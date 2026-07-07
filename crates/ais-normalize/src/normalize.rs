use crate::parse::{
    combine_ais_fragments, parse_ais_sentence_metadata, parse_pghp_timestamp_ms,
    parse_tag_block_field, parse_tag_block_timestamp_ms, split_tag_block, AisSentenceMetadata,
};
use crate::stats::NormalizeStats;
use collect_core::dataset::PartitionKey;
use collect_core::PartitionGranularity;
use rustc_hash::FxHashMap;
use std::sync::Arc;

/// Upper bound on buffered multi-part groups. Malformed data whose groups
/// never complete is evicted oldest-first instead of growing without bound.
const MAX_FRAGMENT_GROUPS: usize = 8192;

pub struct OutputRow {
    pub ts_ms: i64,
    pub partition_rel_dir: Arc<str>,
    pub payload: String,
}

/// Stateless, pre-parsed data from one input row. The time-consuming
/// tag-block and metadata parsing is extracted here so it can run in parallel
/// across rows before the stateful fragment-grouping pass.
pub struct PreprocessedRow {
    /// Byte offset in the original payload of the `!` or `$` that starts
    /// the NMEA sentence (i.e. `tag_prefix.len()`).
    pub sentence_start: usize,
    pub is_pghp: bool,
    pub pghp_ts: Option<i64>,
    pub tag_block_ts: Option<i64>,
    pub meta: AisSentenceMetadata,
}

/// Speed-critical pre-processing that has no cross-row state.
/// Safe to call from a rayon parallel iterator.
pub fn preprocess_row(payload: &str) -> PreprocessedRow {
    let (tag_prefix, sentence) = split_tag_block(payload);
    let pghp_ts = parse_pghp_timestamp_ms(sentence);
    let tag_block_ts = if tag_prefix.is_empty() {
        None
    } else {
        parse_tag_block_timestamp_ms(tag_prefix)
    };
    let meta = parse_ais_sentence_metadata(sentence);
    PreprocessedRow {
        sentence_start: tag_prefix.len(),
        is_pghp: pghp_ts.is_some(),
        pghp_ts,
        tag_block_ts,
        meta,
    }
}

struct FragmentGroup {
    slots: Vec<Option<String>>,
    expected_count: usize,
    received_count: usize,
    first_ts_ms: i64,
    tag_block: Option<String>,
}

/// Stateful processor for a single scan pass over one source partition.
///
/// Maintains a carry-forward timestamp from `$PGHP` lines and a fragment
/// buffer for multi-part AIS sentences.
pub struct PartitionProcessor {
    source: String,
    granularity: PartitionGranularity,
    fragment_groups: FxHashMap<String, FragmentGroup>,
    pending_timestamp_ms: Option<i64>,
    out_cache: Option<(i64, i64, Arc<str>)>,
    pub stats: NormalizeStats,
}

impl PartitionProcessor {
    pub fn new(source: String, granularity: PartitionGranularity) -> Self {
        Self {
            source,
            granularity,
            fragment_groups: FxHashMap::default(),
            pending_timestamp_ms: None,
            out_cache: None,
            stats: NormalizeStats::default(),
        }
    }

    fn output_rel_dir(&mut self, ts_ms: i64) -> Arc<str> {
        let clamped = ts_ms.max(0);
        if let Some((start_ms, end_ms, rel_dir)) = &self.out_cache {
            if clamped >= *start_ms && clamped < *end_ms {
                return rel_dir.clone();
            }
        }
        let key = PartitionKey::from_timestamp_ms(&self.source, ts_ms, self.granularity);
        let (start_ms, end_ms) = self.granularity.period_bounds_ms(ts_ms);
        let rel_dir: Arc<str> = Arc::from(key.relative_dir_time_only());
        self.out_cache = Some((start_ms, end_ms, rel_dir.clone()));
        rel_dir
    }

    fn emit(
        &mut self,
        source_rel_dir: &Arc<str>,
        ts_ms: i64,
        was_retimestamped: bool,
        payload: String,
        out: &mut Vec<OutputRow>,
    ) {
        let rel_dir = self.output_rel_dir(ts_ms);
        self.stats.output_rows += 1;
        if was_retimestamped {
            self.stats.retimestamped_rows += 1;
        }
        if rel_dir.as_ref() != source_rel_dir.as_ref() {
            self.stats.repartitioned_rows += 1;
        }
        out.push(OutputRow {
            ts_ms,
            partition_rel_dir: rel_dir,
            payload,
        });
    }

    /// Process one input row whose tag-block and metadata have already been
    /// parsed by [`preprocess_row`]. This half is stateful (carry-forward
    /// timestamps, fragment-group bookkeeping) and must run sequentially.
    pub fn process_row(
        &mut self,
        source_rel_dir: &Arc<str>,
        ts_ms: i64,
        payload: &str,
        pre: &PreprocessedRow,
        out: &mut Vec<OutputRow>,
    ) {
        self.stats.input_rows += 1;

        let sentence = &payload[pre.sentence_start..];

        if pre.is_pghp {
            self.pending_timestamp_ms = pre.pghp_ts;
            self.emit(source_rel_dir, pre.pghp_ts.unwrap(), true, payload.to_string(), out);
            return;
        }

        if !pre.meta.is_ais {
            self.pending_timestamp_ms = None;
            self.stats.output_rows += 1;
            out.push(OutputRow {
                ts_ms,
                partition_rel_dir: source_rel_dir.clone(),
                payload: payload.to_string(),
            });
            return;
        }

        let (new_ts_ms, was_retimestamped) = if let Some(t) = pre.tag_block_ts {
            if pre.meta.is_fragmented() {
                self.pending_timestamp_ms = Some(t);
            } else {
                self.pending_timestamp_ms = None;
            }
            (t, t != ts_ms)
        } else if let Some(t) = self.pending_timestamp_ms {
            if pre.meta.is_final_fragment() || !pre.meta.is_fragmented() {
                self.pending_timestamp_ms = None;
            }
            (t, t != ts_ms)
        } else {
            (ts_ms, false)
        };

        if !pre.meta.is_fragmented() {
            self.emit(
                source_rel_dir,
                new_ts_ms,
                was_retimestamped,
                payload.to_string(),
                out,
            );
            return;
        }

        let Some(ref group_key) = pre.meta.group_id else {
            self.emit(
                source_rel_dir,
                new_ts_ms,
                was_retimestamped,
                payload.to_string(),
                out,
            );
            return;
        };

        let fragment_count = pre.meta.fragment_count.unwrap_or(1);
        let fragment_number = pre.meta.fragment_number.unwrap_or(1);

        if let Some(group) = self.fragment_groups.get_mut(group_key.as_str()) {
            if fragment_number >= 1
                && fragment_number <= group.expected_count
                && group.slots[fragment_number].is_none()
            {
                group.slots[fragment_number] = Some(sentence.to_string());
                group.received_count += 1;
            }

            if group.received_count < group.expected_count {
                return;
            }

            let group = self
                .fragment_groups
                .remove(group_key.as_str())
                .expect("fragment group just updated");
            let sentences: Vec<String> = group.slots.into_iter().flatten().collect();

            let combined_ts = group.first_ts_ms;
            if let Some(combined_payload) =
                combine_ais_fragments(&sentences, group.tag_block.as_deref())
            {
                self.stats.combined_messages += 1;
                self.emit(
                    source_rel_dir,
                    combined_ts,
                    combined_ts != ts_ms,
                    combined_payload,
                    out,
                );
            } else {
                for (index, sentence_str) in sentences.into_iter().enumerate() {
                    let part_ts = if index == 0 { combined_ts } else { ts_ms };
                    self.emit(
                        source_rel_dir,
                        part_ts,
                        false,
                        sentence_str,
                        out,
                    );
                }
            }
        } else {
            if self.fragment_groups.len() >= MAX_FRAGMENT_GROUPS {
                self.evict_oldest_group(source_rel_dir, out);
            }

            let mut slots = vec![None; fragment_count + 1];
            let mut received_count = 0;
            if fragment_number >= 1 && fragment_number <= fragment_count {
                slots[fragment_number] = Some(sentence.to_string());
                received_count = 1;
            }
            let tag_prefix = &payload[..pre.sentence_start];
            let station = parse_tag_block_field(tag_prefix, "s");
            let tag_block = match (pre.tag_block_ts, station) {
                (Some(t), Some(s)) => Some(format!("c:{},s:{}", t / 1_000, s)),
                (Some(t), None) => Some(format!("c:{}", t / 1_000)),
                (None, Some(s)) => Some(format!("s:{}", s)),
                (None, None) => None,
            };
            self.fragment_groups.insert(
                group_key.clone(),
                FragmentGroup {
                    slots,
                    expected_count: fragment_count,
                    received_count,
                    first_ts_ms: new_ts_ms,
                    tag_block,
                },
            );
        }
    }

    fn evict_oldest_group(&mut self, source_rel_dir: &Arc<str>, out: &mut Vec<OutputRow>) {
        let Some(oldest_key) = self
            .fragment_groups
            .iter()
            .min_by_key(|(_, group)| group.first_ts_ms)
            .map(|(key, _)| key.clone())
        else {
            return;
        };

        if let Some(group) = self.fragment_groups.remove(&oldest_key) {
            self.stats.incomplete_groups += 1;
            let first_ts_ms = group.first_ts_ms;
            for slot in group.slots.into_iter().flatten() {
                self.emit(source_rel_dir, first_ts_ms, false, slot, out);
            }
        }
    }

    pub fn flush_incomplete(
        &mut self,
        source_rel_dir: &Arc<str>,
        out: &mut Vec<OutputRow>,
    ) {
        let groups: Vec<_> = self.fragment_groups.drain().collect();

        for (_, group) in groups {
            self.stats.incomplete_groups += 1;
            let first_ts_ms = group.first_ts_ms;
            for slot in group.slots.into_iter().flatten() {
                self.emit(source_rel_dir, first_ts_ms, false, slot, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::preprocess_row;

    fn source_rel_dir(processor_ts_ms: i64) -> Arc<str> {
        let key =
            PartitionKey::from_timestamp_ms("src", processor_ts_ms, PartitionGranularity::Day);
        Arc::from(key.relative_dir())
    }

    #[test]
    fn combines_two_fragment_rows() {
        let mut processor = PartitionProcessor::new("src".into(), PartitionGranularity::Day);
        let ts = 1_700_000_000_000;
        let rel = source_rel_dir(ts);
        let mut out = Vec::new();

        let pre1 = preprocess_row("!AIVDM,2,1,3,A,55P5TL01VIaAL@7W,0*1C");
        processor.process_row(&rel, ts, "!AIVDM,2,1,3,A,55P5TL01VIaAL@7W,0*1C", &pre1, &mut out);
        assert!(out.is_empty(), "first fragment should be buffered");

        let pre2 = preprocess_row("!AIVDM,2,2,3,A,1@0000000000000,2*55");
        processor.process_row(&rel, ts, "!AIVDM,2,2,3,A,1@0000000000000,2*55", &pre2, &mut out);
        assert_eq!(out.len(), 1, "complete group should emit one combined row");
        assert!(out[0].payload.starts_with("!AIVDM,1,1,,A,"));
        assert_eq!(processor.stats.combined_messages, 1);
        assert_eq!(processor.stats.input_rows, 2);
        assert_eq!(processor.stats.output_rows, 1);
    }

    #[test]
    fn passthrough_rows_borrow_payload_and_partition() {
        let mut processor = PartitionProcessor::new("src".into(), PartitionGranularity::Day);
        let ts = 1_700_000_000_000;
        let rel = source_rel_dir(ts);
        let mut out = Vec::new();

        let pre = preprocess_row("not an ais sentence");
        processor.process_row(&rel, ts, "not an ais sentence", &pre, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload, "not an ais sentence");
        assert_eq!(out[0].partition_rel_dir.as_ref(), rel.as_ref());
    }

    #[test]
    fn caps_buffered_fragment_groups() {
        let mut processor = PartitionProcessor::new("src".into(), PartitionGranularity::Day);
        let ts = 1_700_000_000_000;
        let rel = source_rel_dir(ts);
        let mut evicted_rows = 0usize;

        for index in 0..=MAX_FRAGMENT_GROUPS {
            let line = format!("!AIVDM,2,1,{index},A,PAYLOAD,0*00");
            let mut out = Vec::new();
            let pre = preprocess_row(&line);
            processor.process_row(&rel, ts + index as i64, &line, &pre, &mut out);
            evicted_rows += out.len();
        }

        assert!(processor.fragment_groups.len() <= MAX_FRAGMENT_GROUPS);
        assert_eq!(processor.stats.incomplete_groups, 1, "oldest group evicted");
        assert_eq!(evicted_rows, 1, "evicted fragment emitted as raw row");
    }

    #[test]
    fn flush_incomplete_emits_buffered_fragments() {
        let mut processor = PartitionProcessor::new("src".into(), PartitionGranularity::Day);
        let ts = 1_700_000_000_000;
        let rel = source_rel_dir(ts);
        let mut out = Vec::new();

        let pre = preprocess_row("!AIVDM,2,1,7,A,ORPHAN,0*00");
        processor.process_row(&rel, ts, "!AIVDM,2,1,7,A,ORPHAN,0*00", &pre, &mut out);
        assert!(out.is_empty());

        let mut leftovers = Vec::new();
        processor.flush_incomplete(&rel, &mut leftovers);
        assert_eq!(leftovers.len(), 1);
        assert_eq!(processor.stats.incomplete_groups, 1);
    }
}
