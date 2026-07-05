use crate::parse::{
    combine_ais_fragments, parse_ais_sentence_metadata, parse_pghp_timestamp_ms,
    parse_tag_block_timestamp_ms, split_tag_block,
};
use crate::stats::NormalizeStats;
use collect_core::dataset::PartitionKey;
use collect_core::PartitionGranularity;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

/// Upper bound on buffered multi-part groups. Malformed data whose groups
/// never complete is evicted oldest-first instead of growing without bound.
const MAX_FRAGMENT_GROUPS: usize = 8192;

pub struct OutputRow<'a> {
    pub ts_ms: i64,
    pub partition_rel_dir: Arc<str>,
    pub payload: Cow<'a, str>,
}

struct FragmentGroup {
    /// Slot per fragment number (1-indexed, index 0 unused).
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
    fragment_groups: HashMap<String, FragmentGroup>,
    /// Carry-forward timestamp set by a `$PGHP` line or a tag-block `c:` on a fragmented sentence.
    pending_timestamp_ms: Option<i64>,
    /// `(start_ms, end_ms, rel_dir)` of the most recently used output
    /// partition; avoids per-row PartitionKey construction and string
    /// formatting in the common case of consecutive rows sharing a partition.
    out_cache: Option<(i64, i64, Arc<str>)>,
    pub stats: NormalizeStats,
}

impl PartitionProcessor {
    pub fn new(source: String, granularity: PartitionGranularity) -> Self {
        Self {
            source,
            granularity,
            fragment_groups: HashMap::new(),
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
        // Output is not partitioned by source: several sources merge into one
        // time-only partition, deduplicated by the post-run merge pass.
        let rel_dir: Arc<str> = Arc::from(key.relative_dir_time_only());
        self.out_cache = Some((start_ms, end_ms, rel_dir.clone()));
        rel_dir
    }

    /// Emit one row partitioned by `ts_ms`, updating the shared counters.
    fn emit<'a>(
        &mut self,
        source_rel_dir: &Arc<str>,
        ts_ms: i64,
        was_retimestamped: bool,
        payload: Cow<'a, str>,
        out: &mut Vec<OutputRow<'a>>,
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

    /// Process one input row, appending zero or more output rows to `out`.
    ///
    /// Appends nothing when the row has been buffered as a fragment; appends
    /// one or more rows when a complete group is flushed or a non-fragmented
    /// row is ready.
    pub fn process_row<'a>(
        &mut self,
        source_rel_dir: &Arc<str>,
        ts_ms: i64,
        payload: &'a str,
        out: &mut Vec<OutputRow<'a>>,
    ) {
        self.stats.input_rows += 1;

        let (tag_prefix, sentence) = split_tag_block(payload);

        // Handle $PGHP lines: extract their own timestamp, carry forward for next rows.
        if let Some(pghp_ts) = parse_pghp_timestamp_ms(sentence) {
            self.pending_timestamp_ms = Some(pghp_ts);
            self.emit(source_rel_dir, pghp_ts, true, Cow::Borrowed(payload), out);
            return;
        }

        let tag_block_ts = if tag_prefix.is_empty() {
            None
        } else {
            parse_tag_block_timestamp_ms(tag_prefix)
        };
        let sentence_meta = parse_ais_sentence_metadata(sentence);

        if !sentence_meta.is_ais {
            // Non-AIS line: emit with original timestamp into its original partition.
            self.pending_timestamp_ms = None;
            self.stats.output_rows += 1;
            out.push(OutputRow {
                ts_ms,
                partition_rel_dir: source_rel_dir.clone(),
                payload: Cow::Borrowed(payload),
            });
            return;
        }

        // Resolve the effective timestamp for this AIS sentence.
        let (new_ts_ms, was_retimestamped) = if let Some(t) = tag_block_ts {
            // Explicit tag-block timestamp takes precedence.
            if sentence_meta.is_fragmented() {
                self.pending_timestamp_ms = Some(t);
            } else {
                self.pending_timestamp_ms = None;
            }
            (t, t != ts_ms)
        } else if let Some(t) = self.pending_timestamp_ms {
            // Carry-forward from previous $PGHP or tagged first fragment.
            if sentence_meta.is_final_fragment() || !sentence_meta.is_fragmented() {
                self.pending_timestamp_ms = None;
            }
            (t, t != ts_ms)
        } else {
            (ts_ms, false)
        };

        if !sentence_meta.is_fragmented() {
            // Single-part sentence: emit immediately.
            self.emit(
                source_rel_dir,
                new_ts_ms,
                was_retimestamped,
                Cow::Borrowed(payload),
                out,
            );
            return;
        }

        // Multi-part sentence: buffer until all fragments arrive.
        let Some(group_key) = sentence_meta.group_id else {
            // Fragmented but no group key (missing sequence_id): emit as-is.
            self.emit(
                source_rel_dir,
                new_ts_ms,
                was_retimestamped,
                Cow::Borrowed(payload),
                out,
            );
            return;
        };

        let fragment_count = sentence_meta.fragment_count.unwrap_or(1);
        let fragment_number = sentence_meta.fragment_number.unwrap_or(1);

        if let Some(group) = self.fragment_groups.get_mut(group_key.as_str()) {
            // Store just the bare sentence (strip tag block) for combining.
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

            // All fragments received: combine and emit.
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
                    Cow::Owned(combined_payload),
                    out,
                );
            } else {
                // Combine failed: emit each fragment individually.
                for (index, sentence_str) in sentences.into_iter().enumerate() {
                    let part_ts = if index == 0 { combined_ts } else { ts_ms };
                    self.emit(
                        source_rel_dir,
                        part_ts,
                        false,
                        Cow::Owned(sentence_str),
                        out,
                    );
                }
            }
        } else {
            // First fragment of a new group; `is_fragmented` guarantees
            // expected_count >= 2, so a fresh group can never complete here.
            if self.fragment_groups.len() >= MAX_FRAGMENT_GROUPS {
                self.evict_oldest_group(source_rel_dir, out);
            }

            let mut slots = vec![None; fragment_count + 1];
            let mut received_count = 0;
            if fragment_number >= 1 && fragment_number <= fragment_count {
                slots[fragment_number] = Some(sentence.to_string());
                received_count = 1;
            }
            // The rebuilt tag block for a combined sentence carries only
            // the c: timestamp; g: grouping describes the original
            // fragmentation, which no longer applies once combined.
            let tag_block = tag_block_ts.map(|t| format!("c:{}", t / 1_000));
            self.fragment_groups.insert(
                group_key,
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

    /// Evict the oldest buffered fragment group, emitting its fragments as
    /// individual rows so nothing is silently dropped.
    fn evict_oldest_group(&mut self, source_rel_dir: &Arc<str>, out: &mut Vec<OutputRow<'_>>) {
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
                self.emit(source_rel_dir, first_ts_ms, false, Cow::Owned(slot), out);
            }
        }
    }

    /// Flush all incomplete fragment groups as raw individual rows.
    pub fn flush_incomplete(
        &mut self,
        source_rel_dir: &Arc<str>,
        out: &mut Vec<OutputRow<'static>>,
    ) {
        let groups: Vec<_> = self.fragment_groups.drain().collect();

        for (_, group) in groups {
            self.stats.incomplete_groups += 1;
            let first_ts_ms = group.first_ts_ms;
            for slot in group.slots.into_iter().flatten() {
                self.emit(source_rel_dir, first_ts_ms, false, Cow::Owned(slot), out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        processor.process_row(&rel, ts, "!AIVDM,2,1,3,A,55P5TL01VIaAL@7W,0*1C", &mut out);
        assert!(out.is_empty(), "first fragment should be buffered");

        processor.process_row(&rel, ts, "!AIVDM,2,2,3,A,1@0000000000000,2*55", &mut out);
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

        processor.process_row(&rel, ts, "not an ais sentence", &mut out);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0].payload, Cow::Borrowed(_)));
        assert_eq!(out[0].partition_rel_dir.as_ref(), rel.as_ref());
    }

    #[test]
    fn caps_buffered_fragment_groups() {
        let mut processor = PartitionProcessor::new("src".into(), PartitionGranularity::Day);
        let ts = 1_700_000_000_000;
        let rel = source_rel_dir(ts);
        let mut evicted_rows = 0usize;

        // Insert MAX_FRAGMENT_GROUPS + 1 distinct never-completing groups.
        for index in 0..=MAX_FRAGMENT_GROUPS {
            let line = format!("!AIVDM,2,1,{index},A,PAYLOAD,0*00");
            let mut out = Vec::new();
            processor.process_row(&rel, ts + index as i64, &line, &mut out);
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

        processor.process_row(&rel, ts, "!AIVDM,2,1,7,A,ORPHAN,0*00", &mut out);
        assert!(out.is_empty());

        let mut leftovers = Vec::new();
        processor.flush_incomplete(&rel, &mut leftovers);
        assert_eq!(leftovers.len(), 1);
        assert_eq!(processor.stats.incomplete_groups, 1);
    }
}
