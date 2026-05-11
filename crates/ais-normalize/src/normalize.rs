use crate::dataset::PartitionKey;
use crate::parse::{
    combine_ais_fragments, parse_ais_sentence_metadata, parse_ais_tag_block,
    parse_pghp_timestamp_ms, nmea_sentence,
};
use crate::stats::NormalizeStats;
use collect_core::PartitionGranularity;
use std::collections::HashMap;

pub struct OutputRow {
    pub ts_ms: i64,
    pub partition_rel_dir: String,
    pub payload: String,
    pub was_retimestamped: bool,
    pub was_repartitioned: bool,
    pub is_combined: bool,
}

struct FragmentGroup {
    /// Slot per fragment number (1-indexed, index 0 unused).
    slots: Vec<Option<String>>,
    expected_count: usize,
    received_count: usize,
    first_ts_ms: i64,
    tag_block: Option<String>,
}

/// Stateful processor for a single scan pass over the dataset.
///
/// Maintains a carry-forward timestamp from `$PGHP` lines and a fragment buffer
/// for multi-part AIS sentences.
pub struct PartitionProcessor {
    source: String,
    granularity: PartitionGranularity,
    fragment_groups: HashMap<String, FragmentGroup>,
    /// Carry-forward timestamp set by a `$PGHP` line or a tag-block `c:` on a fragmented sentence.
    pending_timestamp_ms: Option<i64>,
    pub stats: NormalizeStats,
}

impl PartitionProcessor {
    pub fn new(source: String, granularity: PartitionGranularity) -> Self {
        Self {
            source,
            granularity,
            fragment_groups: HashMap::new(),
            pending_timestamp_ms: None,
            stats: NormalizeStats::default(),
        }
    }

    /// Process one input row. Returns zero or more output rows.
    ///
    /// Returns an empty Vec when the row has been buffered as a fragment;
    /// returns one or more rows when a complete group is flushed or a non-fragmented
    /// row is ready.
    pub fn process_row(
        &mut self,
        source_partition: &PartitionKey,
        ts_ms: i64,
        payload: &str,
    ) -> Vec<OutputRow> {
        self.stats.input_rows += 1;

        let sentence = nmea_sentence(payload);

        // Handle $PGHP lines: extract their own timestamp, carry forward for next rows.
        if let Some(pghp_ts) = parse_pghp_timestamp_ms(sentence) {
            self.pending_timestamp_ms = Some(pghp_ts);
            let new_partition = PartitionKey::from_timestamp_ms(&self.source, pghp_ts, self.granularity);
            let was_repartitioned = new_partition.relative_dir() != source_partition.relative_dir();
            self.stats.output_rows += 1;
            self.stats.retimestamped_rows += 1;
            if was_repartitioned {
                self.stats.repartitioned_rows += 1;
            }
            return vec![OutputRow {
                ts_ms: pghp_ts,
                partition_rel_dir: new_partition.relative_dir(),
                payload: payload.to_string(),
                was_retimestamped: true,
                was_repartitioned,
                is_combined: false,
            }];
        }

        let tag_block = parse_ais_tag_block(payload);
        let sentence_meta = parse_ais_sentence_metadata(sentence);

        if !sentence_meta.is_ais {
            // Non-AIS line: emit with original timestamp.
            self.pending_timestamp_ms = None;
            let rel_dir = source_partition.relative_dir();
            self.stats.output_rows += 1;
            return vec![OutputRow {
                ts_ms,
                partition_rel_dir: rel_dir,
                payload: payload.to_string(),
                was_retimestamped: false,
                was_repartitioned: false,
                is_combined: false,
            }];
        }

        // Resolve the effective timestamp for this AIS sentence.
        let (new_ts_ms, was_retimestamped) = if let Some(t) = tag_block.timestamp_ms {
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
            let new_partition =
                PartitionKey::from_timestamp_ms(&self.source, new_ts_ms, self.granularity);
            let was_repartitioned = new_partition.relative_dir() != source_partition.relative_dir();
            self.stats.output_rows += 1;
            if was_retimestamped {
                self.stats.retimestamped_rows += 1;
            }
            if was_repartitioned {
                self.stats.repartitioned_rows += 1;
            }
            return vec![OutputRow {
                ts_ms: new_ts_ms,
                partition_rel_dir: new_partition.relative_dir(),
                payload: payload.to_string(),
                was_retimestamped,
                was_repartitioned,
                is_combined: false,
            }];
        }

        // Multi-part sentence: buffer until all fragments arrive.
        let Some(group_key) = &sentence_meta.group_id else {
            // Fragmented but no group key (missing sequence_id): emit as-is.
            let new_partition =
                PartitionKey::from_timestamp_ms(&self.source, new_ts_ms, self.granularity);
            let was_repartitioned = new_partition.relative_dir() != source_partition.relative_dir();
            self.stats.output_rows += 1;
            if was_retimestamped {
                self.stats.retimestamped_rows += 1;
            }
            if was_repartitioned {
                self.stats.repartitioned_rows += 1;
            }
            return vec![OutputRow {
                ts_ms: new_ts_ms,
                partition_rel_dir: new_partition.relative_dir(),
                payload: payload.to_string(),
                was_retimestamped,
                was_repartitioned,
                is_combined: false,
            }];
        };

        let fragment_count = sentence_meta.fragment_count.unwrap_or(1);
        let fragment_number = sentence_meta.fragment_number.unwrap_or(1);

        let group = self
            .fragment_groups
            .entry(group_key.clone())
            .or_insert_with(|| {
                let tb = tag_block.group_id.as_deref().and_then(|gid| {
                    // reconstruct a minimal tag block body for the combined output if there's a c:
                    tag_block.timestamp_ms.map(|t| {
                        let seconds = t / 1_000;
                        if !gid.is_empty() {
                            format!("c:{}", seconds)
                        } else {
                            format!("c:{}", seconds)
                        }
                    })
                }).or_else(|| {
                    tag_block.timestamp_ms.map(|t| format!("c:{}", t / 1_000))
                });
                FragmentGroup {
                    slots: vec![None; fragment_count + 1],
                    expected_count: fragment_count,
                    received_count: 0,
                    first_ts_ms: new_ts_ms,
                    tag_block: tb,
                }
            });

        // Store just the bare sentence (strip tag block) for combining.
        if fragment_number >= 1 && fragment_number <= group.expected_count {
            if group.slots[fragment_number].is_none() {
                group.slots[fragment_number] = Some(sentence.to_string());
                group.received_count += 1;
            }
        }

        if group.received_count < group.expected_count {
            return vec![];
        }

        // All fragments received: combine and emit.
        let group = self.fragment_groups.remove(group_key).unwrap();
        let sentences: Vec<String> = (1..=group.expected_count)
            .filter_map(|i| group.slots[i].clone())
            .collect();

        let combined_ts = group.first_ts_ms;
        let new_partition =
            PartitionKey::from_timestamp_ms(&self.source, combined_ts, self.granularity);
        let was_repartitioned = new_partition.relative_dir() != source_partition.relative_dir();

        if let Some(combined_payload) = combine_ais_fragments(&sentences, group.tag_block.as_deref()) {
            self.stats.output_rows += 1;
            self.stats.combined_messages += 1;
            if combined_ts != ts_ms {
                self.stats.retimestamped_rows += 1;
            }
            if was_repartitioned {
                self.stats.repartitioned_rows += 1;
            }
            vec![OutputRow {
                ts_ms: combined_ts,
                partition_rel_dir: new_partition.relative_dir(),
                payload: combined_payload,
                was_retimestamped: combined_ts != ts_ms,
                was_repartitioned,
                is_combined: true,
            }]
        } else {
            // Combine failed: emit each fragment individually.
            let mut rows = Vec::new();
            for (i, sentence_str) in sentences.iter().enumerate() {
                let part_ts = if i == 0 { combined_ts } else { ts_ms };
                let part_partition =
                    PartitionKey::from_timestamp_ms(&self.source, part_ts, self.granularity);
                self.stats.output_rows += 1;
                rows.push(OutputRow {
                    ts_ms: part_ts,
                    partition_rel_dir: part_partition.relative_dir(),
                    payload: sentence_str.clone(),
                    was_retimestamped: false,
                    was_repartitioned: part_partition.relative_dir()
                        != source_partition.relative_dir(),
                    is_combined: false,
                });
            }
            rows
        }
    }

    /// Flush all incomplete fragment groups as raw individual rows.
    pub fn flush_incomplete(&mut self, source_partition: &PartitionKey) -> Vec<OutputRow> {
        let mut rows = Vec::new();
        let groups: Vec<_> = self.fragment_groups.drain().collect();

        for (_, group) in groups {
            self.stats.incomplete_groups += 1;
            for slot in group.slots.into_iter().flatten() {
                let new_partition = PartitionKey::from_timestamp_ms(
                    &self.source,
                    group.first_ts_ms,
                    self.granularity,
                );
                let was_repartitioned =
                    new_partition.relative_dir() != source_partition.relative_dir();
                self.stats.output_rows += 1;
                if was_repartitioned {
                    self.stats.repartitioned_rows += 1;
                }
                rows.push(OutputRow {
                    ts_ms: group.first_ts_ms,
                    partition_rel_dir: new_partition.relative_dir(),
                    payload: slot,
                    was_retimestamped: group.first_ts_ms != 0,
                    was_repartitioned,
                    is_combined: false,
                });
            }
        }

        rows
    }
}
