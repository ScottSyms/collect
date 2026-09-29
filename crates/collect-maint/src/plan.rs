//! Deciding what to compact: pure functions over the live-file list, so the
//! policy is unit-testable without a catalog.

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use iceberg::spec::{PrimitiveLiteral, Schema, Struct, Transform};
use iceberg::table::Table;

use crate::rewrite::LiveFile;

/// Prefix of every data file this tool writes; used to tell compacted files
/// from freshly ingested ones.
pub const COMPACT_PREFIX: &str = "compact-";

/// Columns to sort by when `--sort-by` isn't given: `mmsi` then `ts`, keeping
/// only those the table has (so `raw`, which has no `mmsi`, sorts by `ts`).
pub const DEFAULT_SORT: [&str; 2] = ["mmsi", "ts"];

/// Columns that get parquet bloom filters when present.
pub const BLOOM_COLUMNS: [&str; 6] = [
    "mmsi",
    "station",
    "source",
    "imo_number",
    "call_sign",
    "name",
];

pub fn sort_columns(schema: &Schema, overrides: &[String]) -> Result<Vec<String>> {
    if overrides.is_empty() {
        return Ok(DEFAULT_SORT
            .iter()
            .filter(|c| schema.field_by_name(c).is_some())
            .map(|c| c.to_string())
            .collect());
    }
    for c in overrides {
        if schema.field_by_name(c).is_none() {
            bail!("--sort-by column '{c}' is not in the table schema");
        }
    }
    Ok(overrides.to_vec())
}

#[derive(Debug, Clone, Copy)]
pub struct PlanOptions {
    pub now_ms: i64,
    /// A partition must have ended at least this long ago to be touched.
    pub min_age_ms: i64,
    pub target_bytes: u64,
    /// Compaction sorts a partition in memory, so refuse ones larger than this
    /// (compressed input bytes).
    pub max_partition_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct PartitionPlan {
    pub key: String,
    pub label: String,
    pub partition: Option<Struct>,
    pub files: Vec<LiveFile>,
    pub input_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Skip {
    /// Partition hasn't been closed for `min_age` yet.
    Open,
    /// Already compacted and right-sized.
    Fine,
    /// Larger than `max_partition_bytes`.
    TooBig,
}

fn is_compacted(f: &LiveFile) -> bool {
    f.path
        .rsplit('/')
        .next()
        .is_some_and(|n| n.starts_with(COMPACT_PREFIX))
}

/// A partition needs rewriting if any file wasn't written by this tool (new
/// ingest, unsorted), or if two or more files are under half the target size.
/// A lone small tail file is left alone, otherwise every run would rewrite it.
pub fn needs_compaction(files: &[LiveFile], target_bytes: u64) -> bool {
    files.iter().any(|f| !is_compacted(f))
        || files.iter().filter(|f| f.size < target_bytes / 2).count() >= 2
}

pub fn plan(
    table: &Table,
    files: Vec<LiveFile>,
    opts: PlanOptions,
) -> (Vec<PartitionPlan>, Vec<(String, Skip)>) {
    let mut groups: BTreeMap<String, Vec<LiveFile>> = BTreeMap::new();
    for f in files {
        groups
            .entry(format!("{:?}", f.partition))
            .or_default()
            .push(f);
    }
    let mut plans = Vec::new();
    let mut skipped = Vec::new();
    for (key, files) in groups {
        let partition = files[0].partition.clone();
        let label = describe_partition(table, partition.as_ref());
        let closed = partition_range_ms(table, partition.as_ref())
            .is_none_or(|(_, end)| end <= opts.now_ms - opts.min_age_ms);
        if !closed {
            skipped.push((label, Skip::Open));
        } else if !needs_compaction(&files, opts.target_bytes) {
            skipped.push((label, Skip::Fine));
        } else {
            let input_bytes = files.iter().map(|f| f.size).sum();
            if input_bytes > opts.max_partition_bytes {
                skipped.push((label, Skip::TooBig));
            } else {
                plans.push(PartitionPlan {
                    key,
                    label,
                    partition,
                    files,
                    input_bytes,
                });
            }
        }
    }
    (plans, skipped)
}

/// `[start, end)` of the time range a single-time-transform partition covers.
pub fn partition_range_ms(table: &Table, partition: Option<&Struct>) -> Option<(i64, i64)> {
    let spec = table.metadata().default_partition_spec();
    let field = spec.fields().first()?;
    let value = partition?.fields().first()?.as_ref()?;
    let PrimitiveLiteral::Int(v) = value.as_primitive_literal()? else {
        return None;
    };
    range_for(field.transform, v)
}

fn range_for(transform: Transform, v: i32) -> Option<(i64, i64)> {
    let month_start = |m: i32| {
        let date =
            NaiveDate::from_ymd_opt(1970 + m.div_euclid(12), m.rem_euclid(12) as u32 + 1, 1)?;
        Some(
            Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0)?)
                .timestamp_millis(),
        )
    };
    let v64 = i64::from(v);
    match transform {
        Transform::Hour => Some((v64 * 3_600_000, (v64 + 1) * 3_600_000)),
        Transform::Day => Some((v64 * 86_400_000, (v64 + 1) * 86_400_000)),
        Transform::Month => Some((month_start(v)?, month_start(v + 1)?)),
        Transform::Year => Some((month_start(v * 12)?, month_start((v + 1) * 12)?)),
        _ => None,
    }
}

pub fn describe_partition(table: &Table, partition: Option<&Struct>) -> String {
    match partition_range_ms(table, partition) {
        Some((start, _)) => {
            let dt = Utc.timestamp_millis_opt(start).single();
            let spec = table.metadata().default_partition_spec();
            let name = spec
                .fields()
                .first()
                .map(|f| f.name.as_str())
                .unwrap_or("ts");
            dt.map(|d| {
                if matches!(spec.fields()[0].transform, Transform::Hour) {
                    format!("{name}={}", d.format("%Y-%m-%dT%H"))
                } else {
                    format!("{name}={}-{:02}-{:02}", d.year(), d.month(), d.day())
                }
            })
            .unwrap_or_else(|| format!("{partition:?}"))
        }
        None => "(unpartitioned)".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, size: u64) -> LiveFile {
        LiveFile {
            path: format!("s3://b/t/data/{name}"),
            size,
            records: 1,
            partition: None,
        }
    }
    const T: u64 = 512 << 20;

    #[test]
    fn fresh_ingest_files_trigger_compaction() {
        assert!(needs_compaction(&[file("part-1.parquet", T)], T));
    }

    #[test]
    fn compacted_full_files_are_left_alone() {
        assert!(!needs_compaction(
            &[
                file("compact-a-0.parquet", T),
                file("compact-a-1.parquet", T)
            ],
            T
        ));
    }

    #[test]
    fn one_small_tail_is_left_alone_but_two_small_are_merged() {
        let tail = [
            file("compact-a-0.parquet", T),
            file("compact-a-1.parquet", 10 << 20),
        ];
        assert!(!needs_compaction(&tail, T));
        let two = [
            file("compact-a-0.parquet", 10 << 20),
            file("compact-b-0.parquet", 20 << 20),
        ];
        assert!(needs_compaction(&two, T));
    }

    #[test]
    fn transform_ranges() {
        assert_eq!(range_for(Transform::Day, 0), Some((0, 86_400_000)));
        assert_eq!(range_for(Transform::Hour, 1), Some((3_600_000, 7_200_000)));
        // 1970-02-01 = 31 days
        assert_eq!(range_for(Transform::Month, 0), Some((0, 31 * 86_400_000)));
        // 1970 is 365 days
        assert_eq!(range_for(Transform::Year, 0), Some((0, 365 * 86_400_000)));
    }
}
