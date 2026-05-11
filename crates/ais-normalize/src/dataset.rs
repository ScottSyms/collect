use anyhow::{Context, Result};
use collect_core::PartitionGranularity;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PartitionKey {
    pub source: String,
    pub granularity: PartitionGranularity,
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
}

impl PartitionKey {
    /// Parse from a path relative to the dataset root.
    ///
    /// For Day granularity, expects: `source=X/year=YYYY/month=MM/day=DD/<file>`
    pub fn parse(rel_path: &str, granularity: PartitionGranularity) -> Option<Self> {
        // number of directory segments = depth + 1 (source + time components)
        let dir_depth = granularity.depth() + 1;
        let mut segments = rel_path.split('/');
        let mut source = String::new();
        let mut year = 0i32;
        let mut month = 1u32;
        let mut day = 1u32;
        let mut hour = 0u32;
        let mut minute = 0u32;

        for i in 0..dir_depth {
            let seg = segments.next()?;
            match i {
                0 => source = parse_kv(seg, "source")?.to_string(),
                1 => year = parse_kv(seg, "year")?.parse().ok()?,
                2 => month = parse_kv(seg, "month")?.parse().ok()?,
                3 => day = parse_kv(seg, "day")?.parse().ok()?,
                4 => hour = parse_kv(seg, "hour")?.parse().ok()?,
                5 => minute = parse_kv(seg, "minute")?.parse().ok()?,
                _ => {}
            }
        }

        if source.is_empty() {
            return None;
        }

        Some(PartitionKey {
            source,
            granularity,
            year,
            month,
            day,
            hour,
            minute,
        })
    }

    /// Build a partition key from a UTC millisecond timestamp.
    pub fn from_timestamp_ms(source: &str, ts_ms: i64, granularity: PartitionGranularity) -> Self {
        let (year, month, day, hour, minute) = granularity.components_from_timestamp(ts_ms);
        PartitionKey {
            source: source.to_string(),
            granularity,
            year,
            month,
            day,
            hour,
            minute,
        }
    }

    /// Returns the relative directory path, e.g. `source=foo/year=2024/month=03/day=15`
    pub fn relative_dir(&self) -> String {
        let depth = self.granularity.depth();
        let mut parts = vec![format!("source={}", self.source)];
        // year is always present (depth >= 1)
        parts.push(format!("year={:04}", self.year));
        if depth >= 2 {
            parts.push(format!("month={:02}", self.month));
        }
        if depth >= 3 {
            parts.push(format!("day={:02}", self.day));
        }
        if depth >= 4 {
            parts.push(format!("hour={:02}", self.hour));
        }
        if depth >= 5 {
            parts.push(format!("minute={:02}", self.minute));
        }
        parts.join("/")
    }

    /// Chronological sort key.
    pub fn sort_key(&self) -> (i32, u32, u32, u32, u32, &str) {
        (
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            &self.source,
        )
    }
}

fn parse_kv<'a>(segment: &'a str, key: &str) -> Option<&'a str> {
    segment.strip_prefix(&format!("{}=", key))
}

pub struct DatasetFile {
    pub partition: PartitionKey,
    pub path: PathBuf,
}

/// List all `.parquet` files under `root`, sorted chronologically by partition then path.
pub async fn list_parquet_files(
    root: &Path,
    granularity: PartitionGranularity,
    source_filter: Option<&str>,
) -> Result<Vec<DatasetFile>> {
    let root = root.to_path_buf();
    let source_filter = source_filter.map(str::to_string);

    tokio::task::spawn_blocking(move || {
        let mut files: Vec<DatasetFile> = Vec::new();

        for entry in WalkDir::new(&root).follow_links(false) {
            let entry = entry.context("walking dataset directory")?;
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
                continue;
            }
            // skip temp files written by collect-core
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("tmp-"))
                .unwrap_or(false)
            {
                continue;
            }

            let rel_path = path
                .strip_prefix(&root)
                .context("computing relative path")?
                .to_string_lossy()
                .into_owned();

            let Some(partition) = PartitionKey::parse(&rel_path, granularity) else {
                continue;
            };

            if let Some(ref filter) = source_filter {
                if &partition.source != filter {
                    continue;
                }
            }

            files.push(DatasetFile {
                partition,
                path: path.to_path_buf(),
            });
        }

        files.sort_by(|a, b| {
            a.partition
                .sort_key()
                .cmp(&b.partition.sort_key())
                .then(a.path.cmp(&b.path))
        });

        Ok(files)
    })
    .await
    .context("dataset scan task panicked")?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_day_partition() {
        let key = PartitionKey::parse(
            "source=mydata/year=2024/month=03/day=15/part-abc.parquet",
            PartitionGranularity::Day,
        )
        .expect("should parse");
        assert_eq!(key.source, "mydata");
        assert_eq!(key.year, 2024);
        assert_eq!(key.month, 3);
        assert_eq!(key.day, 15);
    }

    #[test]
    fn parses_hour_partition() {
        let key = PartitionKey::parse(
            "source=ais/year=2024/month=01/day=01/hour=12/part.parquet",
            PartitionGranularity::Hour,
        )
        .expect("should parse");
        assert_eq!(key.hour, 12);
    }

    #[test]
    fn parses_minute_partition() {
        let key = PartitionKey::parse(
            "source=ais/year=2024/month=01/day=01/hour=12/minute=30/part.parquet",
            PartitionGranularity::Minute,
        )
        .expect("should parse");
        assert_eq!(key.hour, 12);
        assert_eq!(key.minute, 30);
    }

    #[test]
    fn relative_dir_day() {
        let key = PartitionKey {
            source: "ais".to_string(),
            granularity: PartitionGranularity::Day,
            year: 2024,
            month: 3,
            day: 15,
            hour: 0,
            minute: 0,
        };
        assert_eq!(key.relative_dir(), "source=ais/year=2024/month=03/day=15");
    }

    #[test]
    fn relative_dir_hour() {
        let key = PartitionKey {
            source: "ais".to_string(),
            granularity: PartitionGranularity::Hour,
            year: 2024,
            month: 3,
            day: 15,
            hour: 7,
            minute: 0,
        };
        assert_eq!(
            key.relative_dir(),
            "source=ais/year=2024/month=03/day=15/hour=07"
        );
    }

    #[test]
    fn from_timestamp_ms_day() {
        // 2023-11-14 22:13:20 UTC
        let ts_ms = 1_700_000_000_000i64;
        let key = PartitionKey::from_timestamp_ms("test", ts_ms, PartitionGranularity::Day);
        assert_eq!(key.year, 2023);
        assert_eq!(key.month, 11);
        assert_eq!(key.day, 14);
    }
}
