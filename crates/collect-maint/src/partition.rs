use anyhow::{bail, Result};
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PartitionKey {
    pub source: String,
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
}

impl PartitionKey {
    pub fn parse(rel_path: &str) -> Option<Self> {
        let parent = Path::new(rel_path).parent()?;
        let parts: Vec<String> = parent
            .iter()
            .map(|part| part.to_string_lossy().to_string())
            .collect();

        if parts.len() < 6 {
            return None;
        }

        let start = parts.len().saturating_sub(6);
        parse_partition_parts(&parts[start..])
    }

    pub fn relative_dir(&self) -> String {
        format_partition_dir(self)
    }

    pub fn minute_id(&self) -> Result<u64> {
        let dt = Utc
            .with_ymd_and_hms(self.year, self.month, self.day, self.hour, self.minute, 0)
            .single()
            .ok_or_else(|| anyhow::anyhow!("invalid partition timestamp: {}", self.relative_dir()))?;
        Ok(dt.timestamp().max(0) as u64 / 60)
    }

    pub fn matches_timestamp_ms(&self, timestamp_ms: i64) -> bool {
        minute_id_from_timestamp_ms(timestamp_ms) == self.minute_id().unwrap_or(u64::MAX)
    }
}

pub fn minute_id_from_timestamp_ms(timestamp_ms: i64) -> u64 {
    timestamp_ms.max(0) as u64 / 1_000 / 60
}

fn parse_partition_parts(parts: &[String]) -> Option<PartitionKey> {
    if parts.len() != 6 {
        return None;
    }

    let source = parse_partition_value(&parts[0], "source")?.to_string();
    let year = parse_partition_value(&parts[1], "year")?.parse().ok()?;
    let month = parse_partition_value(&parts[2], "month")?.parse().ok()?;
    let day = parse_partition_value(&parts[3], "day")?.parse().ok()?;
    let hour = parse_partition_value(&parts[4], "hour")?.parse().ok()?;
    let minute = parse_partition_value(&parts[5], "minute")?.parse().ok()?;

    Some(PartitionKey {
        source,
        year,
        month,
        day,
        hour,
        minute,
    })
}

fn parse_partition_value<'a>(segment: &'a str, expected_key: &str) -> Option<&'a str> {
    let (key, value) = segment.split_once('=')?;
    if key == expected_key {
        Some(value)
    } else {
        None
    }
}

pub fn format_partition_dir(partition: &PartitionKey) -> String {
    format!(
        "source={}/year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}",
        partition.source,
        partition.year,
        partition.month,
        partition.day,
        partition.hour,
        partition.minute
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EntryKind {
    Parquet,
    CompactedParquet,
    Manifest,
    Temp,
    Other,
}

pub fn classify_entry(rel_path: &str, size: u64) -> EntryKind {
    if size == 0 {
        return EntryKind::Temp;
    }

    let file_name = Path::new(rel_path)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(rel_path);

    if file_name.ends_with(".manifest.json") {
        EntryKind::Manifest
    } else if file_name.ends_with(".tmp") || file_name.ends_with(".partial") {
        EntryKind::Temp
    } else if file_name.starts_with("compact-") && file_name.ends_with(".parquet") {
        EntryKind::CompactedParquet
    } else if file_name.ends_with(".parquet") {
        EntryKind::Parquet
    } else {
        EntryKind::Other
    }
}

pub fn compaction_output_name(group_index: usize) -> String {
    let timestamp_ms = Utc::now().timestamp_millis();
    let pid = std::process::id();
    format!("compact-{}-{}-{:03}.parquet", timestamp_ms, pid, group_index)
}

pub fn manifest_path_for_output(output_rel_path: &str) -> String {
    format!("{}.manifest.json", output_rel_path)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactionManifest {
    pub partition: String,
    pub output: String,
    pub inputs: Vec<String>,
    pub created_at_ms: i64,
}

impl CompactionManifest {
    pub fn new(partition: &PartitionKey, output: String, inputs: Vec<String>) -> Self {
        Self {
            partition: partition.relative_dir(),
            output,
            inputs,
            created_at_ms: Utc::now().timestamp_millis(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.inputs.is_empty() {
            bail!("manifest has no inputs: {}", self.output);
        }
        if !self.output.ends_with(".parquet") {
            bail!("manifest output is not parquet: {}", self.output);
        }
        Ok(())
    }
}
