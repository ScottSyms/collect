use crate::{PartitionGranularity, S3Storage};
use anyhow::{Context, Result};
use chrono::{Datelike, TimeZone, Utc};
use futures_util::stream::{self, StreamExt};
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
    /// The leading `source=X` segment is optional: bronze datasets written by
    /// the collectors are `source=X/year=YYYY/month=MM/day=DD/<file>`, but the
    /// normalized/decoded datasets ais-normalize and ais-parse write are no
    /// longer partitioned by source (`year=YYYY/month=MM/day=DD/<file>`).
    /// When the source segment is absent, `source` is the empty string.
    pub fn parse(rel_path: &str, granularity: PartitionGranularity) -> Option<Self> {
        let mut segments = rel_path.split('/').peekable();
        let source = if segments.peek().is_some_and(|s| s.starts_with("source=")) {
            parse_kv(segments.next()?, "source")?.to_string()
        } else {
            String::new()
        };

        let mut year = 0i32;
        let mut month = 1u32;
        let mut day = 1u32;
        let mut hour = 0u32;
        let mut minute = 0u32;

        for i in 0..granularity.depth() {
            let seg = segments.next()?;
            match i {
                0 => year = parse_kv(seg, "year")?.parse().ok()?,
                1 => month = parse_kv(seg, "month")?.parse().ok()?,
                2 => day = parse_kv(seg, "day")?.parse().ok()?,
                3 => hour = parse_kv(seg, "hour")?.parse().ok()?,
                4 => minute = parse_kv(seg, "minute")?.parse().ok()?,
                _ => {}
            }
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

    /// Returns the relative directory path. Includes the `source=` segment
    /// only when this key carries a source (e.g. `source=foo/year=2024/month=03/day=15`);
    /// for a source-less key it is the time-only path (`year=2024/month=03/day=15`),
    /// so it round-trips whatever layout `parse` read.
    pub fn relative_dir(&self) -> String {
        if self.source.is_empty() {
            return self.relative_dir_time_only();
        }
        let mut parts = vec![format!("source={}", self.source)];
        parts.extend(self.time_segments());
        parts.join("/")
    }

    /// Returns the time-only relative directory (`year=YYYY[/month=MM[/...]]`),
    /// never including `source=`. Used for the normalized/decoded output
    /// layout, which is not partitioned by source.
    pub fn relative_dir_time_only(&self) -> String {
        self.time_segments().join("/")
    }

    fn time_segments(&self) -> Vec<String> {
        let depth = self.granularity.depth();
        // year is always present (depth >= 1)
        let mut parts = vec![format!("year={:04}", self.year)];
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
        parts
    }

    /// A copy of this key with the source cleared, so several source
    /// partitions collapse to one time-only key (used to group input from
    /// many sources into a single source-less output partition).
    pub fn without_source(&self) -> PartitionKey {
        PartitionKey {
            source: String::new(),
            ..self.clone()
        }
    }

    /// Millisecond bounds `[start, end)` of this partition's time period.
    pub fn period_bounds_ms(&self) -> (i64, i64) {
        // Turn the partition's own components back into the instant at its
        // start, then let the granularity compute the matching period width
        // (calendar-correct for month/year). Fall back to the epoch if the
        // components somehow don't form a valid date.
        let start_ms = Utc
            .with_ymd_and_hms(self.year, self.month, self.day, self.hour, self.minute, 0)
            .single()
            .map(|dt| dt.timestamp_millis())
            .unwrap_or(0);
        self.granularity.period_bounds_ms(start_ms)
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
    segment.strip_prefix(key).and_then(|s| s.strip_prefix('='))
}

/// UTC calendar year of a millisecond timestamp; clamps to `i32::MIN` on the
/// impossible out-of-range case so year pruning stays conservative (keeps).
fn year_of_ms(ms: i64) -> i32 {
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.year())
        .unwrap_or(i32::MIN)
}

fn system_time_to_ms(time: std::time::SystemTime) -> i64 {
    time.duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// Whether a partition contains at least one file modified after `cutoff_ms`
/// — i.e. work an `--incremental` run still has to (re)process. Files with an
/// unknown modification time count as new, so uncertainty errs toward
/// processing (idempotent thanks to dedup).
pub fn partition_is_new(modified: impl IntoIterator<Item = Option<i64>>, cutoff_ms: i64) -> bool {
    modified
        .into_iter()
        .any(|m| m.is_none_or(|ms| ms > cutoff_ms))
}

/// Newest known modification time across files, for the next watermark.
/// Unknown times are skipped — they were processed, but can't advance the
/// watermark without risking skips on the next run.
pub fn max_modified_ms(modified: impl IntoIterator<Item = Option<i64>>) -> Option<i64> {
    modified.into_iter().flatten().max()
}

/// Optional partition-boundary selection: process only partitions whose
/// components match every specified value. Components are hierarchical —
/// specifying `month` without `year` would match that month in every year,
/// which is almost never intended, so `validate` rejects gaps.
///
/// `since_ms` is an alternative, rolling selection: a partition matches when
/// its time period extends past that UTC-millisecond cutoff (i.e. it can hold
/// data from the window `[cutoff, now]`). It is mutually exclusive with the
/// fixed `year`..`minute` components.
#[derive(Clone, Copy, Debug, Default)]
pub struct PartitionFilter {
    pub year: Option<i32>,
    pub month: Option<u32>,
    pub day: Option<u32>,
    pub hour: Option<u32>,
    pub minute: Option<u32>,
    pub since_ms: Option<i64>,
}

impl PartitionFilter {
    pub fn is_empty(&self) -> bool {
        self.year.is_none()
            && self.month.is_none()
            && self.day.is_none()
            && self.hour.is_none()
            && self.minute.is_none()
            && self.since_ms.is_none()
    }

    /// Reject gaps in the hierarchy and components finer than the dataset layout.
    pub fn validate(&self, granularity: PartitionGranularity) -> Result<()> {
        if self.since_ms.is_some()
            && (self.year.is_some()
                || self.month.is_some()
                || self.day.is_some()
                || self.hour.is_some()
                || self.minute.is_some())
        {
            anyhow::bail!(
                "--since selects a rolling time window and cannot be combined with the fixed \
                 --year/--month/--day/--hour/--minute partition components"
            );
        }
        let chain = [
            ("--year", self.year.is_some(), 1usize),
            ("--month", self.month.is_some(), 2),
            ("--day", self.day.is_some(), 3),
            ("--hour", self.hour.is_some(), 4),
            ("--minute", self.minute.is_some(), 5),
        ];
        let depth = granularity.depth();
        let mut deepest_set = 0usize;
        for (name, set, level) in chain {
            if set {
                if level > depth {
                    anyhow::bail!(
                        "{name} is finer than the dataset layout (--partition {granularity})"
                    );
                }
                if level != deepest_set + 1 {
                    anyhow::bail!(
                        "{name} requires every coarser component to be set too (year, then month, then day, ...)"
                    );
                }
                deepest_set = level;
            }
        }
        Ok(())
    }

    pub fn matches(&self, key: &PartitionKey) -> bool {
        self.year.is_none_or(|y| key.year == y)
            && self.month.is_none_or(|m| key.month == m)
            && self.day.is_none_or(|d| key.day == d)
            && self.hour.is_none_or(|h| key.hour == h)
            && self.minute.is_none_or(|m| key.minute == m)
            // A partition is "within the last N hours" when its period ends
            // after the cutoff — that keeps the partition holding the cutoff
            // instant plus every later one.
            && self
                .since_ms
                .is_none_or(|cutoff| key.period_bounds_ms().1 > cutoff)
    }

    /// The Hive path fragment covered by this filter (e.g.
    /// `year=2026/month=07`), used to push the selection down into the S3
    /// LIST prefix and to prune the local directory walk. `validate`
    /// guarantees the components form a contiguous chain from `year`.
    fn hive_path_chain(&self) -> String {
        let mut chain = String::new();
        if let Some(year) = self.year {
            chain.push_str(&format!("year={:04}", year));
            if let Some(month) = self.month {
                chain.push_str(&format!("/month={:02}", month));
                if let Some(day) = self.day {
                    chain.push_str(&format!("/day={:02}", day));
                    if let Some(hour) = self.hour {
                        chain.push_str(&format!("/hour={:02}", hour));
                        if let Some(minute) = self.minute {
                            chain.push_str(&format!("/minute={:02}", minute));
                        }
                    }
                }
            }
        }
        chain
    }

    /// Whether a directory named `segment` (e.g. `year=2024`) can contain
    /// matching partitions. Unknown segment shapes are conservatively kept.
    fn dir_segment_may_match(&self, segment: &str) -> bool {
        if let Some(value) = parse_kv(segment, "year") {
            if let Ok(parsed) = value.parse::<i32>() {
                if let Some(wanted) = self.year {
                    return parsed == wanted;
                }
                // For a rolling --since window we can still prune whole years
                // that ended before the cutoff. Finer segments need their
                // parent's context to prune safely, so `matches` handles those.
                if let Some(cutoff) = self.since_ms {
                    return parsed >= year_of_ms(cutoff);
                }
            }
            return true;
        }
        for (key, wanted) in [
            ("month", self.month),
            ("day", self.day),
            ("hour", self.hour),
            ("minute", self.minute),
        ] {
            if let Some(value) = parse_kv(segment, key) {
                return match (value.parse::<u32>(), wanted) {
                    (Ok(parsed), Some(want)) => parsed == want,
                    _ => true,
                };
            }
        }
        true
    }
}

pub struct DatasetFile {
    pub partition: PartitionKey,
    pub path: PathBuf,
    /// Filesystem mtime (or S3 `LastModified` for downloaded entries), as UTC
    /// milliseconds. `None` when the platform/store didn't report one; such
    /// files are conservatively treated as new by `--incremental` selection.
    pub modified_ms: Option<i64>,
}

/// List all `.parquet` files under `root`, sorted chronologically by partition then path.
///
/// The walk prunes whole directory subtrees that cannot match `filter` or
/// `source_filter`, so selecting one day out of a multi-year archive does not
/// stat every file in it.
pub async fn list_parquet_files(
    root: &Path,
    granularity: PartitionGranularity,
    source_filter: Option<&str>,
    filter: PartitionFilter,
) -> Result<Vec<DatasetFile>> {
    let root = root.to_path_buf();
    let source_filter = source_filter.map(str::to_string);

    tokio::task::spawn_blocking(move || {
        let mut files: Vec<DatasetFile> = Vec::new();

        let source_prune = source_filter.clone();
        let walker = WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_entry(move |entry| {
                if !entry.file_type().is_dir() {
                    return true;
                }
                let Some(name) = entry.file_name().to_str() else {
                    return true;
                };
                if let Some(source) = parse_kv(name, "source") {
                    if let Some(ref wanted) = source_prune {
                        return source == wanted;
                    }
                    return true;
                }
                filter.dir_segment_may_match(name)
            });

        for entry in walker {
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

            if let Some(ref wanted) = source_filter {
                if &partition.source != wanted {
                    continue;
                }
            }
            if !filter.matches(&partition) {
                continue;
            }

            let modified_ms = entry
                .metadata()
                .ok()
                .and_then(|meta| meta.modified().ok())
                .map(system_time_to_ms);

            files.push(DatasetFile {
                partition,
                path: path.to_path_buf(),
                modified_ms,
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

/// One matched `.parquet` object in an S3 input bucket, before it's downloaded.
///
/// `storage_index` records which input bucket the object came from — when a run
/// reads several buckets, entries for the same partition key can come from
/// different buckets, and each must be downloaded through its own storage
/// handle. Listing sets it to `0`; the caller overrides it per bucket.
#[derive(Clone)]
pub struct S3Entry {
    pub key: String,
    pub rel_path: String,
    pub partition: PartitionKey,
    pub storage_index: usize,
    /// Server-side `LastModified` (UTC milliseconds), when reported.
    pub modified_ms: Option<i64>,
}

fn rel_from_key(prefix: &str, key: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        key.to_string()
    } else {
        key.strip_prefix(&format!("{}/", prefix))
            .unwrap_or(key)
            .to_string()
    }
}

/// List `.parquet` objects under `prefix` in an S3 bucket, mirroring the
/// filtering `list_parquet_files` applies locally: skips `tmp-`-prefixed
/// files, requires the key (relative to `prefix`) to match the Hive layout
/// for `granularity`, and optionally restricts to one `source` and to
/// partitions matching `filter`.
///
/// When both a source and a partition filter are given, the selection is
/// pushed down into the S3 LIST prefix so a one-day slice of a multi-year
/// bucket lists only that day's keys instead of the whole bucket.
pub async fn list_s3_parquet_entries(
    storage: &S3Storage,
    prefix: &str,
    granularity: PartitionGranularity,
    source_filter: Option<&str>,
    filter: PartitionFilter,
) -> Result<Vec<S3Entry>> {
    // LIST prefix pushdown needs the source segment, since it comes before
    // the time components in the key layout.
    let list_prefix = match source_filter {
        Some(source) if !filter.is_empty() => {
            let base = prefix.trim_matches('/');
            let chain = filter.hive_path_chain();
            let mut pushed = String::new();
            if !base.is_empty() {
                pushed.push_str(base);
                pushed.push('/');
            }
            pushed.push_str(&format!("source={}/{}", source, chain));
            pushed
        }
        _ => prefix.to_string(),
    };

    let keys = storage
        .list_keys_with_prefix(&list_prefix)
        .await
        .context("listing S3 input bucket")?;

    let mut entries: Vec<S3Entry> = Vec::new();
    for object in keys {
        let key = object.key;
        if !key.ends_with(".parquet") {
            continue;
        }
        let file_name = key.rsplit('/').next().unwrap_or(&key);
        if file_name.starts_with("tmp-") {
            continue;
        }

        let rel_path = rel_from_key(prefix, &key);
        let Some(partition) = PartitionKey::parse(&rel_path, granularity) else {
            continue;
        };

        if let Some(wanted) = source_filter {
            if partition.source != wanted {
                continue;
            }
        }
        if !filter.matches(&partition) {
            continue;
        }

        entries.push(S3Entry {
            key,
            rel_path,
            partition,
            storage_index: 0,
            modified_ms: object.modified_ms,
        });
    }

    entries.sort_by(|a, b| {
        a.partition
            .sort_key()
            .cmp(&b.partition.sort_key())
            .then(a.rel_path.cmp(&b.rel_path))
    });

    Ok(entries)
}

/// Download every matched S3 entry into `scratch_root`, preserving its relative
/// Hive path, so the rest of the pipeline can treat it exactly like a local
/// dataset. Each entry is fetched through `storages[entry.storage_index]`, so a
/// single partition may draw files from several input buckets. Runs up to
/// `concurrency` downloads at once.
///
/// The scratch filename is prefixed with the storage index so files that share
/// a name across buckets cannot overwrite each other; they still live under the
/// partition's `rel_dir`, keeping the caller's per-partition cleanup correct.
pub async fn download_s3_entries(
    storages: &[S3Storage],
    entries: Vec<S3Entry>,
    scratch_root: &Path,
    concurrency: usize,
) -> Result<Vec<DatasetFile>> {
    let scratch_root = scratch_root.to_path_buf();
    let mut files: Vec<DatasetFile> = stream::iter(entries)
        .map(|entry| {
            let storage = storages
                .get(entry.storage_index)
                .expect("entry storage_index within storages")
                .clone();
            let local_path = namespaced_scratch_path(&scratch_root, &entry);
            async move {
                storage
                    .download_to_path(&entry.key, &local_path)
                    .await
                    .with_context(|| format!("downloading s3://{}", entry.key))?;
                Ok::<_, anyhow::Error>(DatasetFile {
                    partition: entry.partition,
                    path: local_path,
                    modified_ms: entry.modified_ms,
                })
            }
        })
        .buffer_unordered(concurrency.max(1))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    files.sort_by(|a, b| {
        a.partition
            .sort_key()
            .cmp(&b.partition.sort_key())
            .then(a.path.cmp(&b.path))
    });

    Ok(files)
}

/// Group a partition-key-sorted list into contiguous runs sharing a key.
///
/// The input MUST already be sorted by partition key — callers that concatenate
/// several individually-sorted lists (e.g. one per input bucket) re-sort first,
/// otherwise a key split across the input would produce multiple groups.
pub fn group_by_partition<T>(
    items: Vec<T>,
    key_of: impl Fn(&T) -> PartitionKey,
) -> Vec<(PartitionKey, Vec<T>)> {
    let mut groups: Vec<(PartitionKey, Vec<T>)> = Vec::new();
    for item in items {
        let key = key_of(&item);
        match groups.last_mut() {
            Some((existing, list)) if *existing == key => list.push(item),
            _ => groups.push((key, vec![item])),
        }
    }
    groups
}

/// `scratch_root/<rel_dir>/in<idx>-<filename>` for an entry, so same-named
/// objects from different buckets never collide on disk.
fn namespaced_scratch_path(scratch_root: &Path, entry: &S3Entry) -> PathBuf {
    let rel = Path::new(&entry.rel_path);
    let file_name = rel
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| format!("part-{}.parquet", entry.storage_index));
    let namespaced = format!("in{}-{}", entry.storage_index, file_name);
    match rel.parent() {
        Some(parent) => scratch_root.join(parent).join(namespaced),
        None => scratch_root.join(namespaced),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn day_key(source: &str, year: i32, month: u32, day: u32) -> PartitionKey {
        PartitionKey {
            source: source.to_string(),
            granularity: PartitionGranularity::Day,
            year,
            month,
            day,
            hour: 0,
            minute: 0,
        }
    }

    #[test]
    fn groups_distinct_sources_separately() {
        // Two sources, already sorted by key: two groups.
        let keys = vec![
            day_key("alpha", 2026, 7, 1),
            day_key("alpha", 2026, 7, 1),
            day_key("beta", 2026, 7, 1),
        ];
        let groups = group_by_partition(keys, |k| k.clone());
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0.source, "alpha");
        assert_eq!(groups[0].1.len(), 2);
        assert_eq!(groups[1].0.source, "beta");
        assert_eq!(groups[1].1.len(), 1);
    }

    #[test]
    fn groups_same_source_from_two_inputs_into_one() {
        // The same partition key contributed by two inputs collapses to one
        // group (once the concatenated list is sorted, they are adjacent).
        let keys = vec![day_key("norway", 2026, 7, 2), day_key("norway", 2026, 7, 2)];
        let groups = group_by_partition(keys, |k| k.clone());
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].1.len(), 2);
    }

    #[test]
    fn group_by_without_source_merges_sources_of_the_same_time() {
        // Sorted by sort_key (time before source), two sources of the same day
        // are contiguous, so grouping by the source-less key merges them into
        // one time-only work item — the source-less output partition.
        let mut keys = vec![
            day_key("alpha", 2026, 7, 1),
            day_key("beta", 2026, 7, 1),
            day_key("alpha", 2026, 7, 2),
        ];
        keys.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        let groups = group_by_partition(keys, |k| k.without_source());
        assert_eq!(groups.len(), 2, "two days -> two groups");
        assert_eq!(groups[0].0.source, "", "grouped key is source-less");
        assert_eq!(groups[0].0.day, 1);
        assert_eq!(groups[0].1.len(), 2, "alpha+beta merged into day 1");
        assert_eq!(groups[1].0.day, 2);
        assert_eq!(groups[1].1.len(), 1);
    }

    #[test]
    fn namespaced_scratch_path_is_per_storage() {
        let entry = |idx| S3Entry {
            key: "k".into(),
            rel_path: "source=x/year=2026/month=07/day=01/part-0.parquet".into(),
            partition: day_key("x", 2026, 7, 1),
            storage_index: idx,
            modified_ms: None,
        };
        let root = Path::new("/scratch");
        let a = namespaced_scratch_path(root, &entry(0));
        let b = namespaced_scratch_path(root, &entry(1));
        assert_ne!(a, b, "same-named objects from two buckets must not collide");
        assert!(a.to_string_lossy().ends_with("in0-part-0.parquet"));
        assert!(b.to_string_lossy().ends_with("in1-part-0.parquet"));
        assert_eq!(a.parent(), b.parent(), "both live under the same rel_dir");
    }

    #[test]
    fn filter_matches_hierarchically() {
        let filter = PartitionFilter {
            year: Some(2026),
            month: Some(7),
            ..Default::default()
        };
        assert!(filter.matches(&day_key("a", 2026, 7, 1)));
        assert!(filter.matches(&day_key("a", 2026, 7, 31)));
        assert!(!filter.matches(&day_key("a", 2026, 6, 1)));
        assert!(!filter.matches(&day_key("a", 2025, 7, 1)));
        assert!(PartitionFilter::default().matches(&day_key("a", 1999, 1, 1)));
    }

    #[test]
    fn since_matches_partitions_whose_period_ends_after_cutoff() {
        // Cutoff at 2026-07-04 12:00:00 UTC.
        let cutoff = Utc
            .with_ymd_and_hms(2026, 7, 4, 12, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        let filter = PartitionFilter {
            since_ms: Some(cutoff),
            ..Default::default()
        };
        // Same day as the cutoff: its period ends 2026-07-05 00:00 > cutoff.
        assert!(filter.matches(&day_key("a", 2026, 7, 4)));
        // Later day: kept.
        assert!(filter.matches(&day_key("a", 2026, 7, 5)));
        // Earlier day: its period ended 2026-07-04 00:00 <= cutoff, dropped.
        assert!(!filter.matches(&day_key("a", 2026, 7, 3)));
    }

    #[test]
    fn since_prunes_years_before_the_cutoff_year() {
        let cutoff = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        let filter = PartitionFilter {
            since_ms: Some(cutoff),
            ..Default::default()
        };
        assert!(!filter.dir_segment_may_match("year=2025"));
        assert!(filter.dir_segment_may_match("year=2026"));
        assert!(filter.dir_segment_may_match("year=2027"));
        // Finer segments can't be pruned without parent context: kept.
        assert!(filter.dir_segment_may_match("month=01"));
    }

    #[test]
    fn since_is_mutually_exclusive_with_fixed_components() {
        let filter = PartitionFilter {
            year: Some(2026),
            since_ms: Some(0),
            ..Default::default()
        };
        assert!(filter.validate(PartitionGranularity::Day).is_err());

        let ok = PartitionFilter {
            since_ms: Some(0),
            ..Default::default()
        };
        assert!(ok.validate(PartitionGranularity::Day).is_ok());
    }

    #[test]
    fn partition_is_new_when_any_file_is_past_cutoff() {
        assert!(partition_is_new([Some(100), Some(300)], 200));
        // The comparison is strictly-after.
        assert!(!partition_is_new([Some(100), Some(200)], 200));
        // Unknown mtimes err toward processing.
        assert!(partition_is_new([Some(100), None], 200));
        assert!(!partition_is_new(std::iter::empty::<Option<i64>>(), 0));
    }

    #[test]
    fn max_modified_skips_unknown_mtimes() {
        assert_eq!(max_modified_ms([Some(100), None, Some(300)]), Some(300));
        assert_eq!(max_modified_ms([None, None]), None);
        assert_eq!(max_modified_ms(std::iter::empty::<Option<i64>>()), None);
    }

    #[test]
    fn local_walk_reports_file_mtimes() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = root.path().join("source=x/year=2026/month=07/day=01");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("part-0.parquet");
        std::fs::write(&path, b"stub").unwrap();
        // Backdate the file so the reported mtime is deterministic.
        let stamp = std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_700_000_000_000);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(stamp)
            .unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let files = rt
            .block_on(list_parquet_files(
                root.path(),
                PartitionGranularity::Day,
                None,
                PartitionFilter::default(),
            ))
            .expect("list");
        assert_eq!(files.len(), 1);
        let modified = files[0].modified_ms.expect("mtime reported");
        // Filesystems may round to whole seconds; allow that much slack.
        assert!((modified - 1_700_000_000_000).abs() < 1_000);
    }

    #[test]
    fn filter_validate_rejects_gaps_and_too_fine() {
        let gap = PartitionFilter {
            month: Some(7),
            ..Default::default()
        };
        assert!(gap.validate(PartitionGranularity::Day).is_err());

        let too_fine = PartitionFilter {
            year: Some(2026),
            month: Some(7),
            day: Some(4),
            hour: Some(12),
            ..Default::default()
        };
        assert!(too_fine.validate(PartitionGranularity::Day).is_err());
        assert!(too_fine.validate(PartitionGranularity::Hour).is_ok());

        let ok = PartitionFilter {
            year: Some(2026),
            month: Some(7),
            ..Default::default()
        };
        assert!(ok.validate(PartitionGranularity::Day).is_ok());
    }

    #[test]
    fn filter_hive_path_chain_stops_at_first_unset() {
        let filter = PartitionFilter {
            year: Some(2026),
            month: Some(7),
            ..Default::default()
        };
        assert_eq!(filter.hive_path_chain(), "year=2026/month=07");
    }

    #[test]
    fn filter_prunes_directory_segments() {
        let filter = PartitionFilter {
            year: Some(2026),
            month: Some(7),
            ..Default::default()
        };
        assert!(filter.dir_segment_may_match("year=2026"));
        assert!(!filter.dir_segment_may_match("year=2025"));
        assert!(filter.dir_segment_may_match("month=07"));
        assert!(!filter.dir_segment_may_match("month=06"));
        // components the filter leaves open, and non-hive names, are kept
        assert!(filter.dir_segment_may_match("day=15"));
        assert!(filter.dir_segment_may_match("random-dir"));
    }

    #[test]
    fn rel_from_key_strips_prefix() {
        assert_eq!(
            rel_from_key(
                "datasets/ais",
                "datasets/ais/source=x/year=2024/day.parquet"
            ),
            "source=x/year=2024/day.parquet"
        );
    }

    #[test]
    fn rel_from_key_no_prefix() {
        assert_eq!(
            rel_from_key("", "source=x/year=2024/day.parquet"),
            "source=x/year=2024/day.parquet"
        );
    }

    #[test]
    fn rel_from_key_ignores_non_matching_prefix() {
        // A key that doesn't actually start with the prefix falls back to itself
        // rather than panicking; callers still filter by PartitionKey::parse.
        assert_eq!(
            rel_from_key("other", "source=x/day.parquet"),
            "source=x/day.parquet"
        );
    }

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
    fn relative_dir_time_only_and_source_less_round_trip() {
        let key = day_key("norway", 2026, 7, 5);
        // With a source, relative_dir keeps it; time_only never does.
        assert_eq!(
            key.relative_dir(),
            "source=norway/year=2026/month=07/day=05"
        );
        assert_eq!(key.relative_dir_time_only(), "year=2026/month=07/day=05");
        assert_eq!(
            key.without_source().relative_dir(),
            "year=2026/month=07/day=05"
        );
    }

    #[test]
    fn parses_source_less_path() {
        // The normalized/decoded layout has no source= segment.
        let key = PartitionKey::parse(
            "year=2026/month=07/day=05/pos-abc.parquet",
            PartitionGranularity::Day,
        )
        .expect("should parse a source-less path");
        assert_eq!(key.source, "");
        assert_eq!(key.year, 2026);
        assert_eq!(key.month, 7);
        assert_eq!(key.day, 5);
        // And still round-trips.
        assert_eq!(key.relative_dir(), "year=2026/month=07/day=05");
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
