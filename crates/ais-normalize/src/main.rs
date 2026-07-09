use anyhow::{bail, Context, Result};
use arrow::array::{StringArray, TimestampMillisecondArray};
use chrono::TimeZone;
use clap::Parser;
use collect_core::{PartitionGranularity, S3ConnectionArgs, S3Storage};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::File as StdFile;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

mod dedup;
mod normalize;
mod output;
mod parse;
mod stats;
mod status;

use collect_core::{dataset, state};

use dataset::{DatasetFile, PartitionKey};
use normalize::{preprocess_row, OutputRow, PartitionProcessor};
use output::OutputWriterPool;
use stats::NormalizeStats;
use status::StatusMode;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Re-timestamp, re-partition, and combine multi-part AIS sentences in a Hive-partitioned Parquet dataset"
)]
struct Args {
    /// Source Hive-partitioned Parquet root directory. Repeatable to merge
    /// several sources in one run (mutually exclusive with --input-s3-bucket)
    #[arg(long)]
    input_dir: Vec<PathBuf>,

    /// Output Hive-partitioned Parquet root directory, may equal input-dir (mutually exclusive with --output-s3-bucket)
    #[arg(long)]
    output_dir: Option<PathBuf>,

    /// S3 bucket to read the input dataset from, instead of --input-dir.
    /// Repeatable to merge several buckets (all on the shared endpoint)
    #[arg(long)]
    input_s3_bucket: Vec<String>,

    /// Key prefix within the input S3 bucket; acts as the dataset root
    #[arg(long, default_value = "")]
    input_s3_prefix: String,

    /// S3 bucket to write normalized output to, instead of --output-dir
    #[arg(long)]
    output_s3_bucket: Option<String>,

    /// Key prefix within the output S3 bucket; acts as the dataset root
    #[arg(long, default_value = "")]
    output_s3_prefix: String,

    #[command(flatten)]
    s3_connection: S3ConnectionArgs,

    /// Partition granularity; must match the dataset layout
    #[arg(long, default_value_t = PartitionGranularity::Day)]
    partition: PartitionGranularity,

    /// Filter to a specific source label (processes all sources if omitted)
    #[arg(long)]
    source: Option<String>,

    /// Process only this year's partitions (narrow further with --month, --day, ...)
    #[arg(long)]
    year: Option<i32>,

    /// Process only this month's partitions; requires --year
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=12))]
    month: Option<u32>,

    /// Process only this day's partitions; requires --month
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=31))]
    day: Option<u32>,

    /// Process only this hour's partitions; requires --day and an hour-or-finer layout
    #[arg(long, value_parser = clap::value_parser!(u32).range(0..=23))]
    hour: Option<u32>,

    /// Process only this minute's partitions; requires --hour and a minute layout
    #[arg(long, value_parser = clap::value_parser!(u32).range(0..=59))]
    minute: Option<u32>,

    /// Process only partitions holding data from the last N hours (rolling
    /// window from now, UTC). Ideal for an hourly cron re-run; mutually
    /// exclusive with the fixed --year/--month/--day/--hour/--minute filters.
    /// With --incremental, acts only as the first run's starting bound
    #[arg(long, value_name = "HOURS")]
    since: Option<u64>,

    /// Track a watermark at the output target and process only partitions
    /// holding files that arrived since the last successful run. Self-heals
    /// after downtime and catches late-arriving files (selection is by file
    /// modification time, not partition period)
    #[arg(long)]
    incremental: bool,

    /// Number of rows per Parquet read batch
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Zstd compression level for output files
    #[arg(long, default_value_t = 5)]
    compression_level: i32,

    /// Number of partitions to process concurrently; auto-selected when omitted
    #[arg(long)]
    concurrency: Option<usize>,

    /// Merge each touched output partition and drop exact (ts, payload)
    /// duplicates, so re-runs are idempotent. Set false to append instead.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    dedup: bool,

    /// Disable the runtime status TUI and print plain progress updates
    #[arg(long)]
    noui: bool,
}

impl Args {
    /// Only the new S3 fields read from the environment — `--input-dir` /
    /// `--output-dir` remain CLI-only, unchanged from before S3 support.
    fn apply_env(&mut self) {
        if self.input_dir.is_empty() && self.input_s3_bucket.is_empty() {
            if let Ok(value) = std::env::var("INPUT_S3_BUCKET") {
                // Comma-separated so several buckets can be given via one env var.
                self.input_s3_bucket = value
                    .split(',')
                    .map(str::trim)
                    .filter(|b| !b.is_empty())
                    .map(str::to_string)
                    .collect();
            }
        }
        if self.input_s3_prefix.is_empty() {
            if let Ok(value) = std::env::var("INPUT_S3_PREFIX") {
                self.input_s3_prefix = value;
            }
        }
        if self.since.is_none() {
            if let Ok(value) = std::env::var("SINCE_HOURS") {
                if let Ok(hours) = value.trim().parse::<u64>() {
                    self.since = Some(hours);
                }
            }
        }
        if !self.incremental {
            if let Ok(value) = std::env::var("INCREMENTAL") {
                self.incremental = value.eq_ignore_ascii_case("true") || value == "1";
            }
        }
        if self.output_dir.is_none() && self.output_s3_bucket.is_none() {
            if let Ok(value) = std::env::var("OUTPUT_S3_BUCKET") {
                if !value.trim().is_empty() {
                    self.output_s3_bucket = Some(value);
                }
            }
        }
        if self.output_s3_prefix.is_empty() {
            if let Ok(value) = std::env::var("OUTPUT_S3_PREFIX") {
                self.output_s3_prefix = value;
            }
        }
        self.s3_connection.apply_env();
        // dedup defaults to true; only an explicit env value flips it off. A
        // command-line --dedup always wins because it is parsed before this.
        if self.dedup {
            if let Ok(value) = std::env::var("DEDUP") {
                if value.eq_ignore_ascii_case("false") || value == "0" {
                    self.dedup = false;
                }
            }
        }
    }

    /// Split `bucket/path` syntax in bucket arguments into separate bucket +
    /// prefix. A bucket like `bronze/norway/dt=2026` becomes bucket=`bronze`
    /// with prefix=`norway/dt=2026`.  The extracted prefix is prepended to any
    /// existing `--*-prefix` value.
    fn split_s3_args(&mut self) {
        let mut merged = self.input_s3_prefix.clone();
        let mut buckets = Vec::with_capacity(self.input_s3_bucket.len());
        for b in self.input_s3_bucket.drain(..) {
            let (bucket, pfx) = S3Storage::split_s3_path(&b, &merged);
            merged = pfx;
            buckets.push(bucket);
        }
        self.input_s3_bucket = buckets;
        self.input_s3_prefix = merged;

        if let Some(b) = self.output_s3_bucket.take() {
            let (bucket, pfx) = S3Storage::split_s3_path(&b, &self.output_s3_prefix);
            self.output_s3_bucket = Some(bucket);
            self.output_s3_prefix = pfx;
        }
    }
}

/// Derive the S3 key for a file written under `scratch_root` (`root/rel/dir/file`
/// becomes `prefix/rel/dir/file`, or just `rel/dir/file` when `prefix` is empty).
fn s3_key_for_output(scratch_root: &Path, local_path: &Path, prefix: &str) -> String {
    let rel = local_path
        .strip_prefix(scratch_root)
        .unwrap_or(local_path)
        .to_string_lossy()
        .replace('\\', "/");
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        rel
    } else {
        format!("{}/{}", prefix, rel)
    }
}

/// True when the run reads and writes the same place — any input directory
/// equal to the output directory, or any input bucket equal to the output
/// bucket at the same prefix. Best-effort: local paths are compared after
/// canonicalization when both resolve, else by raw value.
fn output_equals_input(args: &Args) -> bool {
    if let Some(output) = &args.output_dir {
        let output_canon = std::fs::canonicalize(output).ok();
        return args.input_dir.iter().any(|input| {
            match (std::fs::canonicalize(input).ok(), &output_canon) {
                (Some(a), Some(b)) => a == *b,
                _ => input == output,
            }
        });
    }
    if let Some(out_bucket) = &args.output_s3_bucket {
        let same_prefix =
            args.input_s3_prefix.trim_matches('/') == args.output_s3_prefix.trim_matches('/');
        return same_prefix && args.input_s3_bucket.iter().any(|b| b == out_bucket);
    }
    false
}

const UPLOAD_MAX_ATTEMPTS: u32 = 3;

/// Upload `local_path` to `key`, retrying transient failures with exponential
/// backoff. `S3Storage::upload_file` only deletes the local file on success,
/// so a retry after a failed attempt re-sends the same still-present file.
async fn upload_with_retries(storage: &S3Storage, local_path: &Path, key: &str) -> Result<()> {
    let mut last_error = None;
    for attempt in 1..=UPLOAD_MAX_ATTEMPTS {
        match storage.upload_file(local_path, key).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                if attempt < UPLOAD_MAX_ATTEMPTS {
                    let backoff = std::time::Duration::from_secs(1 << (attempt - 1));
                    eprintln!(
                        "Upload attempt {attempt}/{UPLOAD_MAX_ATTEMPTS} failed for {}: {error}. Retrying in {}s...",
                        local_path.display(),
                        backoff.as_secs()
                    );
                    tokio::time::sleep(backoff).await;
                }
                last_error = Some(error);
            }
        }
    }
    Err(last_error.expect("loop runs at least once and always records an error on failure"))
}

/// RFC 3339 label for a UTC millisecond timestamp, for log lines.
fn format_ms(ms: i64) -> String {
    chrono::Utc
        .timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| format!("{ms}ms"))
}

/// Best-effort raise of the soft open-file limit toward the hard limit. The
/// merge and concurrent S3 downloads can hold many descriptors at once, and
/// some environments default the soft limit low (macOS 256, some containers
/// 1024). Never fails the run.
fn raise_open_file_limit() {
    if let Err(error) = rlimit::increase_nofile_limit(u64::MAX) {
        eprintln!("Warning: could not raise the open-file limit: {error}");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    raise_open_file_limit();

    let mut args = Args::parse();
    args.apply_env();
    args.split_s3_args();
    let status_mode = StatusMode::from_tty(!args.noui && std::io::stdout().is_terminal());

    match (args.input_dir.is_empty(), args.input_s3_bucket.is_empty()) {
        (false, false) => bail!("use either --input-dir or --input-s3-bucket, not both"),
        (true, true) => bail!("one of --input-dir or --input-s3-bucket is required"),
        _ => {}
    }
    match (&args.output_dir, &args.output_s3_bucket) {
        (Some(_), Some(_)) => bail!("use either --output-dir or --output-s3-bucket, not both"),
        (None, None) => bail!("one of --output-dir or --output-s3-bucket is required"),
        _ => {}
    }

    // A --since window is a rolling cutoff relative to "now": include every
    // partition whose period ends after now - N hours. Saturating so an absurd
    // N can't overflow the millisecond arithmetic.
    let since_ms = args.since.map(|hours| {
        let now_ms = chrono::Utc::now().timestamp_millis();
        now_ms.saturating_sub((hours as i64).saturating_mul(3_600_000))
    });

    if args.incremental
        && (args.year.is_some()
            || args.month.is_some()
            || args.day.is_some()
            || args.hour.is_some()
            || args.minute.is_some())
    {
        bail!(
            "--incremental tracks its own rolling watermark and cannot be combined with the \
             fixed --year/--month/--day/--hour/--minute partition components"
        );
    }

    let partition_filter = dataset::PartitionFilter {
        year: args.year,
        month: args.month,
        day: args.day,
        hour: args.hour,
        minute: args.minute,
        // In incremental mode --since only seeds the first run's watermark
        // cutoff (applied to file mtimes below), not the period filter.
        since_ms: if args.incremental { None } else { since_ms },
    };
    partition_filter.validate(args.partition)?;

    if let (false, Some(hours), Some(cutoff)) = (args.incremental, args.since, since_ms) {
        eprintln!(
            "Processing partitions from the last {hours}h (since {}).",
            format_ms(cutoff)
        );
    }

    // Dedup only makes sense when we are actually writing output.
    let dedup_enabled = args.dedup;

    if dedup_enabled && output_equals_input(&args) {
        eprintln!(
            "Warning: output target equals input target. With dedup on, the raw input and \
             normalized rows have different payloads, so they will NOT dedup against each other \
             and the partition will end up holding both. Use a separate output dir/bucket."
        );
    }

    // Set up Ctrl-C handler.
    let _signal = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(_) => return,
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        status::request_cancel();
        eprintln!("\nShutdown requested.");
    });

    // Every input bucket lives on the same S3 endpoint/region/credentials; only
    // the bucket name differs. Build one storage handle per input bucket; their
    // order is the `storage_index` carried by each listed object.
    let mut input_storages: Vec<S3Storage> = Vec::with_capacity(args.input_s3_bucket.len());
    for bucket in &args.input_s3_bucket {
        input_storages.push(
            S3Storage::new(
                bucket.clone(),
                String::new(),
                args.s3_connection.s3_region.clone(),
                args.s3_connection.s3_endpoint.clone(),
                args.s3_connection.s3_access_key.clone(),
                args.s3_connection.s3_secret_key.clone(),
                false,
                args.s3_connection.s3_disable_tls,
            )
            .await
            .with_context(|| format!("connecting to input S3 bucket {bucket}"))?,
        );
    }
    let input_storages = Arc::new(input_storages);
    let input_is_s3 = !input_storages.is_empty();
    let output_storage = match &args.output_s3_bucket {
        Some(bucket) => Some(
            S3Storage::new(
                bucket.clone(),
                String::new(),
                args.s3_connection.s3_region.clone(),
                args.s3_connection.s3_endpoint.clone(),
                args.s3_connection.s3_access_key.clone(),
                args.s3_connection.s3_secret_key.clone(),
                false,
                args.s3_connection.s3_disable_tls,
            )
            .await
            .context("connecting to output S3 bucket")?,
        ),
        None => None,
    };

    // --incremental: the watermark state lives at the output target, because
    // it describes what has been written there. Load it before listing so the
    // cutoff can drive partition selection.
    let state_store = if args.incremental {
        Some(match (&output_storage, &args.output_dir) {
            (Some(storage), _) => {
                state::StateStore::s3(storage, &args.output_s3_prefix, "ais-normalize")
            }
            (None, Some(dir)) => state::StateStore::local(dir, "ais-normalize"),
            _ => unreachable!("validated exactly one output target above"),
        })
    } else {
        None
    };
    let mut prev_watermark_ms: Option<i64> = None;
    // File-mtime cutoff for incremental selection; `None` in incremental mode
    // means "first run, no seed" (process everything).
    let mut incremental_cutoff: Option<i64> = None;
    if let Some(store) = &state_store {
        match store.load().await? {
            Some(prev) => {
                if args.since.is_some() {
                    eprintln!("Note: watermark state exists; --since is ignored in its favor.");
                }
                prev_watermark_ms = Some(prev.watermark_ms);
                incremental_cutoff = Some(prev.cutoff_ms());
                eprintln!(
                    "Incremental: processing files modified after {} (state: {}).",
                    format_ms(prev.cutoff_ms()),
                    store.describe()
                );
            }
            None => match since_ms {
                Some(cutoff) => {
                    incremental_cutoff = Some(cutoff);
                    eprintln!(
                        "Incremental: first run; starting from --since {}h ({}).",
                        args.since.unwrap_or_default(),
                        format_ms(cutoff)
                    );
                }
                None => {
                    eprintln!(
                        "Incremental: first run (no state at {}); processing the full dataset.",
                        store.describe()
                    );
                }
            },
        }
    }

    // The input scratch dir only ever holds copies of data still safely
    // stored in the source bucket, so it's fine to auto-delete on drop
    // regardless of how the run ends.
    let input_scratch = input_is_s3
        .then(|| {
            tempfile::Builder::new()
                .prefix("ais-normalize-input-")
                .tempdir()
        })
        .transpose()
        .context("creating input scratch directory")?;

    // The output scratch dir holds normalized data that exists nowhere else
    // until it's uploaded. `.keep()` detaches it from tempfile's
    // delete-on-drop so a failed upload can't silently destroy work that was
    // already produced; it's removed explicitly once every upload succeeds
    // (see the end of `main`), and left in place — with its path logged — if
    // the run errors out first.
    let output_scratch_path: Option<PathBuf> = if output_storage.is_some() {
        let dir = tempfile::Builder::new()
            .prefix("ais-normalize-output-")
            .tempdir()
            .context("creating output scratch directory")?;
        Some(dir.keep())
    } else {
        None
    };

    let output_root: PathBuf = match (&args.output_dir, &output_scratch_path) {
        (Some(dir), None) => dir.clone(),
        (None, Some(scratch_path)) => scratch_path.clone(),
        _ => unreachable!("validated exactly one output target above"),
    };

    // A unit of work: either files already on local disk, or S3 objects the
    // worker downloads just before processing (and deletes right after), so
    // scratch disk usage stays bounded by `concurrency` partitions instead of
    // the whole dataset.
    enum PartitionWork {
        Local(Vec<DatasetFile>),
        Remote(Vec<dataset::S3Entry>),
    }

    // Which input indices contributed each source label. If any source is fed
    // by more than one input, that source's rows can be merged into a single
    // output file that already holds cross-input duplicates, so dedup must run
    // even on single-file partitions (see `force_dedup` below).
    let mut source_inputs: HashMap<String, HashSet<usize>> = HashMap::new();

    let mut partitions: Vec<(PartitionKey, PartitionWork)> = if input_is_s3 {
        eprintln!("Listing {} input S3 bucket(s)...", input_storages.len());
        if !partition_filter.is_empty() && args.source.is_none() {
            eprintln!(
                "Note: without --source the partition filter is applied after listing; \
                 add --source to push it into the S3 LIST prefix."
            );
        }

        // List each bucket, tag its objects with the bucket's index, and pool
        // them. The per-bucket lists are each sorted, so re-sort the pool by
        // partition key before grouping so identical keys from different buckets
        // land in one group.
        let mut entries: Vec<dataset::S3Entry> = Vec::new();
        for (index, storage) in input_storages.iter().enumerate() {
            let mut bucket_entries = dataset::list_s3_parquet_entries(
                storage,
                &args.input_s3_prefix,
                args.partition,
                args.source.as_deref(),
                partition_filter,
            )
            .await
            .with_context(|| format!("listing input S3 bucket {}", args.input_s3_bucket[index]))?;
            for entry in &mut bucket_entries {
                entry.storage_index = index;
                source_inputs
                    .entry(entry.partition.source.clone())
                    .or_default()
                    .insert(index);
            }
            entries.extend(bucket_entries);
        }

        if entries.is_empty() {
            eprintln!(
                "No matching Parquet objects found across {} bucket(s) under prefix {:?}.",
                input_storages.len(),
                args.input_s3_prefix
            );
            return Ok(());
        }
        eprintln!("Found {} matching object(s).", entries.len());

        entries.sort_by(|a, b| {
            a.partition
                .sort_key()
                .cmp(&b.partition.sort_key())
                .then(a.rel_path.cmp(&b.rel_path))
        });
        dataset::group_by_partition(entries, |entry| entry.partition.clone())
            .into_iter()
            .map(|(key, list)| (key, PartitionWork::Remote(list)))
            .collect()
    } else {
        eprintln!("Scanning {} input dir(s)...", args.input_dir.len());
        let mut files: Vec<DatasetFile> = Vec::new();
        for (index, dir) in args.input_dir.iter().enumerate() {
            let dir_files = dataset::list_parquet_files(
                dir,
                args.partition,
                args.source.as_deref(),
                partition_filter,
            )
            .await
            .with_context(|| format!("scanning input dataset {}", dir.display()))?;
            for file in &dir_files {
                source_inputs
                    .entry(file.partition.source.clone())
                    .or_default()
                    .insert(index);
            }
            files.extend(dir_files);
        }

        if files.is_empty() {
            eprintln!("No matching Parquet files found in the input dir(s).");
            return Ok(());
        }

        files.sort_by(|a, b| {
            a.partition
                .sort_key()
                .cmp(&b.partition.sort_key())
                .then(a.path.cmp(&b.path))
        });
        dataset::group_by_partition(files, |file| file.partition.clone())
            .into_iter()
            .map(|(key, list)| (key, PartitionWork::Local(list)))
            .collect()
    };

    // --incremental: keep only partitions holding at least one file modified
    // after the cutoff, and remember the newest mtime among the kept files —
    // that becomes the next watermark once the run fully succeeds.
    let mut watermark_candidate_ms: Option<i64> = None;
    if args.incremental {
        let work_mtimes = |work: &PartitionWork| -> Vec<Option<i64>> {
            match work {
                PartitionWork::Local(files) => files.iter().map(|f| f.modified_ms).collect(),
                PartitionWork::Remote(entries) => entries.iter().map(|e| e.modified_ms).collect(),
            }
        };
        let before = partitions.len();
        if let Some(cutoff) = incremental_cutoff {
            partitions.retain(|(_, work)| dataset::partition_is_new(work_mtimes(work), cutoff));
        }
        watermark_candidate_ms =
            dataset::max_modified_ms(partitions.iter().flat_map(|(_, work)| work_mtimes(work)));
        eprintln!(
            "Incremental: selected {} of {} partition(s) with new files.",
            partitions.len(),
            before
        );
        if partitions.is_empty() {
            // Nothing will be written, so the detached output scratch dir
            // (kept alive for upload-failure recovery) has no purpose.
            if let Some(scratch_path) = &output_scratch_path {
                let _ = tokio::fs::remove_dir_all(scratch_path).await;
            }
            eprintln!("Nothing new since the watermark; done.");
            return Ok(());
        }
    }

    // When a source is contributed by more than one input, its rows may be
    // merged into a single output file that already contains cross-input
    // duplicates, so the dedup pass must run even on single-file partitions.
    let force_dedup = source_inputs.values().any(|inputs| inputs.len() > 1);
    if dedup_enabled && force_dedup && status_mode.is_plain() {
        eprintln!("A source is fed by multiple inputs; dedup will merge all touched partitions.");
    }

    let total_partitions = partitions.len();
    eprintln!(
        "Found {} partition(s) across {} source(s).",
        total_partitions,
        {
            let mut sources: Vec<_> = partitions.iter().map(|(k, _)| k.source.as_str()).collect();
            sources.dedup();
            sources.len()
        }
    );

    let concurrency = args
        .concurrency
        .unwrap_or_else(default_concurrency)
        .clamp(1, total_partitions.max(1));
    if status_mode.is_plain() && concurrency > 1 {
        eprintln!("Processing partitions with {} worker(s).", concurrency);
    }

    // Partitions are independent (processor state and writer pools are
    // per-partition), so distribute them over a small worker pool.
    type PartitionQueue = Mutex<VecDeque<(PartitionKey, PartitionWork)>>;
    let queue: Arc<PartitionQueue> = Arc::new(Mutex::new(partitions.into()));
    let processed = Arc::new(AtomicUsize::new(0));
    let input_scratch_root: Option<PathBuf> = input_scratch
        .as_ref()
        .map(|scratch| scratch.path().to_path_buf());

    // Per-worker parallelism for downloading one partition's objects; the
    // worker count provides the cross-partition parallelism.
    const PARTITION_DOWNLOAD_CONCURRENCY: usize = 4;

    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let queue = queue.clone();
        let processed = processed.clone();
        let output_root = output_root.clone();
        let output_storage = output_storage.clone();
        let output_s3_prefix = args.output_s3_prefix.clone();
        let input_storages = input_storages.clone();
        let input_scratch_root = input_scratch_root.clone();
        let granularity = args.partition;
        let batch_size = args.batch_size;
        let compression_level = args.compression_level;

        workers.push(tokio::spawn(async move {
            let mut stats = NormalizeStats::default();
            // When dedup runs, S3 upload is deferred to the dedup pass so it
            // uploads a single merged object per partition instead of the raw
            // per-run files.
            let upload_inline = !dedup_enabled;
            let mut partition_rows: BTreeMap<String, u64> = BTreeMap::new();

            loop {
                if status::is_cancelled() {
                    break;
                }

                let next = queue.lock().expect("partition queue lock").pop_front();
                let Some((partition_key, work)) = next else {
                    break;
                };

                // Remote partitions are fetched here, just before processing,
                // and their scratch copies removed right after — the whole
                // dataset is never on local disk at once.
                let downloaded_rel_dir = match &work {
                    PartitionWork::Remote(_) => Some(partition_key.relative_dir()),
                    PartitionWork::Local(_) => None,
                };
                let partition_files = match work {
                    PartitionWork::Local(files) => files,
                    PartitionWork::Remote(entries) => {
                        let scratch_root = input_scratch_root
                            .as_ref()
                            .expect("remote work implies input scratch dir");
                        match dataset::download_s3_entries(
                            &input_storages,
                            entries,
                            scratch_root,
                            PARTITION_DOWNLOAD_CONCURRENCY,
                        )
                        .await
                        .context("downloading input partition from S3")
                        {
                            Ok(files) => files,
                            Err(error) => {
                                status::request_cancel();
                                return Err(error);
                            }
                        }
                    }
                };

                let output_root_for_task = output_root.clone();
                let result = tokio::task::spawn_blocking(move || {
                    process_partition(
                        partition_key,
                        partition_files,
                        output_root_for_task,
                        granularity,
                        batch_size,
                        compression_level,
                    )
                })
                .await
                .context("partition worker panicked")?;

                if let (Some(rel_dir), Some(scratch_root)) =
                    (&downloaded_rel_dir, &input_scratch_root)
                {
                    let _ = tokio::fs::remove_dir_all(scratch_root.join(rel_dir)).await;
                }

                match result {
                    Ok((partition_stats, rows)) => {
                        stats.merge(&partition_stats);
                        for (rel_dir, rows_written, local_path) in rows {
                            *partition_rows.entry(rel_dir).or_default() += rows_written;

                            if let Some(storage) = &output_storage {
                                if rows_written > 0 && upload_inline {
                                    let key = s3_key_for_output(
                                        &output_root,
                                        &local_path,
                                        &output_s3_prefix,
                                    );
                                    if let Err(error) =
                                        upload_with_retries(storage, &local_path, &key)
                                            .await
                                            .with_context(|| {
                                                format!("uploading {} to S3", local_path.display())
                                            })
                                    {
                                        status::request_cancel();
                                        return Err(error);
                                    }
                                }
                            }
                        }
                    }
                    Err(error) => {
                        // Stop the other workers before surfacing the error.
                        status::request_cancel();
                        return Err(error);
                    }
                }

                let done = processed.fetch_add(1, Ordering::SeqCst) + 1;
                if status_mode.is_plain() && status::should_emit_plain_update(done, 10) {
                    status::print_plain_update(done, total_partitions);
                }
            }

            Ok::<_, anyhow::Error>((stats, partition_rows))
        }));
    }

    let mut total_stats = NormalizeStats::default();
    let mut partition_rows: BTreeMap<String, u64> = BTreeMap::new();
    let mut first_error = None;
    for worker in workers {
        match worker.await {
            Ok(Ok((stats, rows))) => {
                total_stats.merge(&stats);
                for (rel_dir, rows_written) in rows {
                    *partition_rows.entry(rel_dir).or_default() += rows_written;
                }
            }
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error.into());
                }
            }
        }
    }

    if let Some(error) = first_error {
        if let Some(scratch_path) = &output_scratch_path {
            eprintln!(
                "Normalized output may remain un-uploaded in {} — inspect and upload it manually, \
                 or re-run once the S3 issue is resolved (the same input regenerates equivalent output).",
                scratch_path.display()
            );
        }
        return Err(error);
    }

    // Merge each touched output partition and drop exact (ts, payload)
    // duplicates, so this run collapses cleanly with any prior runs' output.
    let mut dedup_stats = dedup::DedupStats::default();
    if dedup_enabled && !partition_rows.is_empty() {
        if status_mode.is_plain() {
            eprintln!(
                "Deduplicating {} output partition(s)...",
                partition_rows.len()
            );
        }
        for rel_dir in partition_rows.keys() {
            let stats = if let Some(storage) = &output_storage {
                let new_files_dir = output_root.join(rel_dir);
                let work_dir = output_root.join(".dedup").join(rel_dir);
                dedup::dedup_s3_partition(
                    storage,
                    &args.output_s3_prefix,
                    rel_dir,
                    &new_files_dir,
                    &work_dir,
                    args.compression_level,
                    force_dedup,
                )
                .await
                .with_context(|| format!("deduplicating S3 partition {rel_dir}"))?
            } else {
                let dir = output_root.join(rel_dir);
                let level = args.compression_level;
                tokio::task::spawn_blocking(move || {
                    dedup::dedup_local_partition(&dir, level, force_dedup)
                })
                .await
                .context("dedup task panicked")?
                .with_context(|| format!("deduplicating partition {rel_dir}"))?
            };
            dedup_stats.merge(&stats);
        }
    }

    // Every upload succeeded (or there was nothing to upload): the scratch
    // copies are redundant now, so reclaim the disk space.
    if let Some(scratch_path) = &output_scratch_path {
        let _ = tokio::fs::remove_dir_all(scratch_path).await;
    }

    if status::is_cancelled() {
        eprintln!(
            "Cancelled after {} partition(s).",
            processed.load(Ordering::SeqCst)
        );
    }

    // Advance the watermark only after a fully successful, applied run — a
    // failed, cancelled, or dry run leaves it alone so the next run re-covers
    // the same window (idempotent thanks to dedup). Never move it backwards.
    if let Some(store) = &state_store {
        if !status::is_cancelled() {
            if let Some(candidate) = watermark_candidate_ms {
                let new_watermark = prev_watermark_ms.map_or(candidate, |p| p.max(candidate));
                store
                    .save(state::WatermarkState::new(new_watermark))
                    .await
                    .context("saving watermark state")?;
                eprintln!(
                    "Watermark advanced to {} ({}).",
                    format_ms(new_watermark),
                    store.describe()
                );
            } else {
                eprintln!("Watermark left unchanged: no file reported a modification time.");
            }
        }
    }

    let partitions_written = partition_rows.len();
    total_stats.print_summary();
    dedup_stats.print_summary();

    eprintln!("Done. Wrote {} output partition(s).", partitions_written);

    Ok(())
}

fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4)
}

/// `(output partition rel_dir, rows written, local file path)` per partition
/// file produced by one `process_partition` call.
type PartitionOutputRows = Vec<(String, u64, PathBuf)>;

/// Process one source partition end to end. Files within the partition are
/// processed in parallel via rayon (each file gets its own processor since
/// fragment groups and carry-forward timestamps are file-local). Output rows
/// are collected per-file and then written to the shared pool sequentially.
fn process_partition(
    partition_key: PartitionKey,
    files: Vec<DatasetFile>,
    output_root: PathBuf,
    granularity: PartitionGranularity,
    batch_size: usize,
    compression_level: i32,
) -> Result<(NormalizeStats, PartitionOutputRows)> {
    let source_rel_dir: Arc<str> = Arc::from(partition_key.relative_dir_time_only());
    let source = partition_key.source.clone();

    // Process files in parallel — each gets its own PartitionProcessor.
    let results: Vec<Result<(NormalizeStats, Vec<OutputRow>)>> = files
        .par_iter()
        .map(|file| {
            let mut processor = PartitionProcessor::new(source.clone(), granularity);
            let mut rows = Vec::new();
            process_parquet_file(
                &file.path,
                &source_rel_dir,
                &mut processor,
                &mut rows,
                batch_size,
            )
            .with_context(|| format!("processing {}", file.path.display()))?;
            // Flush any incomplete fragment groups at the end of this file.
            processor.flush_incomplete(&source_rel_dir, &mut rows);
            Ok((processor.stats, rows))
        })
        .collect();

    // Collect stats and write all output rows to a single pool.
    let mut total_stats = NormalizeStats::default();
    let mut pool = OutputWriterPool::new(output_root, &source, compression_level);
    for result in results {
        let (stats, rows) = result?;
        total_stats.merge(&stats);
        for row in rows {
            pool.write_row(&row.partition_rel_dir, row.ts_ms, &row.payload)?;
        }
    }

    total_stats.partitions_processed += 1;
    let file_rows = pool.flush_all().context("flushing output writers")?;
    Ok((total_stats, file_rows))
}

/// Process a single Parquet file, emitting output rows. Tag-block splitting
/// and metadata parsing are parallelised across rows with rayon; the stateful
/// fragment-grouping pass remains sequential within each file.
fn process_parquet_file(
    path: &Path,
    source_rel_dir: &Arc<str>,
    processor: &mut PartitionProcessor,
    output_rows: &mut Vec<OutputRow>,
    batch_size: usize,
) -> Result<()> {
    let file = StdFile::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("read Parquet footer {}", path.display()))?
        .with_batch_size(batch_size)
        .build()
        .with_context(|| format!("build Parquet reader {}", path.display()))?;

    let mut cancel_counter: u32 = 0;

    while let Some(batch) = reader.next().transpose()? {
        let ts_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .context("expected timestamp column at index 0")?;
        let payload_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected string column at index 1")?;

        let n = batch.num_rows();

        // Fast batched cancellation check (~every 1024 rows instead of every row).
        cancel_counter += n as u32;
        if cancel_counter >= 1024 {
            if status::is_cancelled() {
                return Ok(());
            }
            cancel_counter = 0;
        }

        // Phase 1 — stateless parsing in parallel.
        let payloads: Vec<&str> = (0..n).map(|i| payload_col.value(i)).collect();
        let ts_values: Vec<i64> = (0..n).map(|i| ts_col.value(i)).collect();
        let preprocessed: Vec<_> = payloads
            .par_iter()
            .map(|payload| preprocess_row(payload))
            .collect();

        // Phase 2 — stateful fragment processing (sequential).
        for i in 0..n {
            processor.process_row(
                source_rel_dir,
                ts_values[i],
                payloads[i],
                &preprocessed[i],
                output_rows,
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_key_for_output_with_prefix() {
        let root = Path::new("/tmp/scratch");
        let path = root.join("source=x/year=2024/day=01/norm-1.parquet");
        assert_eq!(
            s3_key_for_output(root, &path, "normalized"),
            "normalized/source=x/year=2024/day=01/norm-1.parquet"
        );
    }

    #[test]
    fn s3_key_for_output_without_prefix() {
        let root = Path::new("/tmp/scratch");
        let path = root.join("source=x/year=2024/day=01/norm-1.parquet");
        assert_eq!(
            s3_key_for_output(root, &path, ""),
            "source=x/year=2024/day=01/norm-1.parquet"
        );
    }
}
