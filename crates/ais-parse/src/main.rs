use anyhow::{bail, Context, Result};
use arrow::array::{Array, StringArray, TimestampMillisecondArray};
use chrono::TimeZone;
use clap::Parser;
use collect_core::dataset::{self, DatasetFile, PartitionKey};
use collect_core::state;
use collect_core::{PartitionGranularity, S3ConnectionArgs, S3Storage};
use nmea_parser::NmeaParser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::VecDeque;
use std::fs::File as StdFile;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

mod ais_bits;
mod decode;
mod output;
mod stats;

use decode::{decode_payload, Decoded};
use output::{existing_parquet_files, AtonWriter, BinaryWriter, MeteoWriter, PositionsWriter, StaticsWriter};
use stats::ParseStats;

/// Decoded output lands in sibling hive datasets under the output root.
const POSITIONS_TREE: &str = "positions";
const STATICS_TREE: &str = "statics";
const METEO_TREE: &str = "meteo";
const BINARY_TREE: &str = "binary";
const ATONS_TREE: &str = "atons";
/// Every output dataset tree — used by the partition-replace logic.
const OUTPUT_TREES: [&str; 5] = [POSITIONS_TREE, STATICS_TREE, METEO_TREE, BINARY_TREE, ATONS_TREE];

static CANCELLED: AtomicBool = AtomicBool::new(false);

fn is_cancelled() -> bool {
    CANCELLED.load(Ordering::Relaxed)
}

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Decode normalized AIS sentences into typed Parquet (positions and vessel statics)"
)]
struct Args {
    /// Normalized Hive-partitioned Parquet root directory. Repeatable to
    /// merge several sources in one run (mutually exclusive with --input-s3-bucket)
    #[arg(long)]
    input_dir: Vec<PathBuf>,

    /// Output root; decoded data is written under <root>/positions and
    /// <root>/statics (mutually exclusive with --output-s3-bucket)
    #[arg(long)]
    output_dir: Option<PathBuf>,

    /// S3 bucket to read the input dataset from, instead of --input-dir.
    /// Repeatable to merge several buckets (all on the shared endpoint)
    #[arg(long)]
    input_s3_bucket: Vec<String>,

    /// Key prefix within the input S3 bucket; acts as the dataset root
    #[arg(long, default_value = "")]
    input_s3_prefix: String,

    /// S3 bucket to write decoded output to, instead of --output-dir
    #[arg(long)]
    output_s3_bucket: Option<String>,

    /// Key prefix within the output S3 bucket; acts as the dataset root
    #[arg(long, default_value = "")]
    output_s3_prefix: String,

    #[command(flatten)]
    s3_connection: S3ConnectionArgs,

    /// Partition granularity; must match the input dataset layout (the
    /// output trees mirror it)
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
    /// window from now, UTC); mutually exclusive with the fixed filters.
    /// With --incremental, acts only as the first run's starting bound
    #[arg(long, value_name = "HOURS")]
    since: Option<u64>,

    /// Track a watermark at the output target and process only partitions
    /// holding files that arrived since the last successful run
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
}

impl Args {
    fn apply_env(&mut self) {
        if self.input_dir.is_empty() && self.input_s3_bucket.is_empty() {
            if let Ok(value) = std::env::var("INPUT_S3_BUCKET") {
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
    }
}

/// RFC 3339 label for a UTC millisecond timestamp, for log lines.
fn format_ms(ms: i64) -> String {
    chrono::Utc
        .timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| format!("{ms}ms"))
}

const UPLOAD_MAX_ATTEMPTS: u32 = 3;

/// Upload with exponential backoff on transient failures.
/// `S3Storage::upload_file` deletes the local file only on success, so a
/// retry re-sends the same still-present file.
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

fn s3_join(prefix: &str, rel: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        rel.to_string()
    } else {
        format!("{}/{}", prefix, rel)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Concurrent S3 downloads and open Parquet readers can hold many
    // descriptors at once; raise the soft limit best-effort.
    if let Err(error) = rlimit::increase_nofile_limit(u64::MAX) {
        eprintln!("Warning: could not raise the open-file limit: {error}");
    }

    let mut args = Args::parse();
    args.apply_env();

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

    let since_ms = args.since.map(|hours| {
        let now_ms = chrono::Utc::now().timestamp_millis();
        now_ms.saturating_sub((hours as i64).saturating_mul(3_600_000))
    });

    let partition_filter = dataset::PartitionFilter {
        year: args.year,
        month: args.month,
        day: args.day,
        hour: args.hour,
        minute: args.minute,
        since_ms: if args.incremental { None } else { since_ms },
    };
    partition_filter.validate(args.partition)?;

    if let (false, Some(hours), Some(cutoff)) = (args.incremental, args.since, since_ms) {
        eprintln!(
            "Processing partitions from the last {hours}h (since {}).",
            format_ms(cutoff)
        );
    }

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
        CANCELLED.store(true, Ordering::Relaxed);
        eprintln!("\nShutdown requested.");
    });

    let mut input_storages: Vec<S3Storage> = Vec::with_capacity(args.input_s3_bucket.len());
    for bucket in &args.input_s3_bucket {
        input_storages.push(
            S3Storage::new(
                bucket.clone(),
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

    // Incremental watermark, stored per tool at the output target.
    let state_store = if args.incremental {
        Some(match (&output_storage, &args.output_dir) {
            (Some(storage), _) => {
                state::StateStore::s3(storage, &args.output_s3_prefix, "ais-parse")
            }
            (None, Some(dir)) => state::StateStore::local(dir, "ais-parse"),
            _ => unreachable!("validated exactly one output target above"),
        })
    } else {
        None
    };
    let mut prev_watermark_ms: Option<i64> = None;
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

    // Input scratch holds copies of data still safe in the source bucket:
    // auto-delete on drop. Output scratch holds decoded data that exists
    // nowhere else until uploaded: detached via `.keep()` and removed only
    // after every upload succeeded.
    let input_scratch = input_is_s3
        .then(|| {
            tempfile::Builder::new()
                .prefix("ais-parse-input-")
                .tempdir()
        })
        .transpose()
        .context("creating input scratch directory")?;
    let output_scratch_path: Option<PathBuf> = if output_storage.is_some() {
        let dir = tempfile::Builder::new()
            .prefix("ais-parse-output-")
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
    // Prior-run files are only deleted when the final output target is local
    // disk; for S3 output the replace happens against object keys instead.
    let replace_local = output_storage.is_none();

    enum PartitionWork {
        Local(Vec<DatasetFile>),
        Remote(Vec<dataset::S3Entry>),
    }

    let mut partitions: Vec<(PartitionKey, PartitionWork)> = if input_is_s3 {
        eprintln!("Listing {} input S3 bucket(s)...", input_storages.len());
        if !partition_filter.is_empty() && args.source.is_none() {
            eprintln!(
                "Note: without --source the partition filter is applied after listing; \
                 add --source to push it into the S3 LIST prefix."
            );
        }
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
        // Group by the *time-only* key so every source that lands in a given
        // output partition is one work item: the output is not partitioned by
        // source, and one owner per output partition keeps the partition
        // replace race-free. sort_key orders by time before source, so
        // same-time entries from different sources are already contiguous.
        dataset::group_by_partition(entries, |entry| entry.partition.without_source())
            .into_iter()
            .map(|(key, list)| (key, PartitionWork::Remote(list)))
            .collect()
    } else {
        eprintln!("Scanning {} input dir(s)...", args.input_dir.len());
        let mut files: Vec<DatasetFile> = Vec::new();
        for dir in &args.input_dir {
            let dir_files = dataset::list_parquet_files(
                dir,
                args.partition,
                args.source.as_deref(),
                partition_filter,
            )
            .await
            .with_context(|| format!("scanning input dataset {}", dir.display()))?;
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
        // Group by time-only key — see the S3 branch above.
        dataset::group_by_partition(files, |file| file.partition.without_source())
            .into_iter()
            .map(|(key, list)| (key, PartitionWork::Local(list)))
            .collect()
    };

    // --incremental: keep only partitions with files newer than the cutoff.
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
            if let Some(scratch_path) = &output_scratch_path {
                let _ = tokio::fs::remove_dir_all(scratch_path).await;
            }
            eprintln!("Nothing new since the watermark; done.");
            return Ok(());
        }
    }

    let total_partitions = partitions.len();
    eprintln!("Found {} partition(s).", total_partitions);

    let concurrency = args
        .concurrency
        .unwrap_or_else(default_concurrency)
        .clamp(1, total_partitions.max(1));
    if concurrency > 1 {
        eprintln!("Processing partitions with {} worker(s).", concurrency);
    }

    type PartitionQueue = Mutex<VecDeque<(PartitionKey, PartitionWork)>>;
    let queue: Arc<PartitionQueue> = Arc::new(Mutex::new(partitions.into()));
    let processed = Arc::new(AtomicUsize::new(0));
    let input_scratch_root: Option<PathBuf> = input_scratch
        .as_ref()
        .map(|scratch| scratch.path().to_path_buf());

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
        let batch_size = args.batch_size;
        let compression_level = args.compression_level;

        workers.push(tokio::spawn(async move {
            let mut stats = ParseStats::default();

            loop {
                if is_cancelled() {
                    break;
                }
                let next = queue.lock().expect("partition queue lock").pop_front();
                let Some((partition_key, work)) = next else {
                    break;
                };

                let is_remote = matches!(work, PartitionWork::Remote(_));
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
                                CANCELLED.store(true, Ordering::Relaxed);
                                return Err(error);
                            }
                        }
                    }
                };

                // A time-only work item can pool downloads from several source
                // subtrees, so clean the scratch copies by file path rather
                // than a single partition dir.
                let scratch_files: Vec<PathBuf> = if is_remote {
                    partition_files.iter().map(|f| f.path.clone()).collect()
                } else {
                    Vec::new()
                };

                let rel_dir = partition_key.relative_dir_time_only();
                let output_root_for_task = output_root.clone();
                let result = tokio::task::spawn_blocking(move || {
                    process_partition(
                        partition_key,
                        partition_files,
                        output_root_for_task,
                        batch_size,
                        compression_level,
                        replace_local,
                    )
                })
                .await
                .context("partition worker panicked")?;

                for scratch_file in &scratch_files {
                    let _ = tokio::fs::remove_file(scratch_file).await;
                }

                let outputs = match result {
                    Ok((partition_stats, outputs)) => {
                        stats.merge(&partition_stats);
                        outputs
                    }
                    Err(error) => {
                        CANCELLED.store(true, Ordering::Relaxed);
                        return Err(error);
                    }
                };

                // S3 output: replace the partition's prior objects with this
                // run's file, per tree (positions/statics).
                if let Some(storage) = &output_storage {
                    for tree in OUTPUT_TREES {
                        let tree_rel = format!("{tree}/{rel_dir}");
                        let partition_prefix =
                            format!("{}/", s3_join(&output_s3_prefix, &tree_rel));
                        let prior = storage
                            .list_keys_with_prefix(&partition_prefix)
                            .await
                            .with_context(|| format!("listing {partition_prefix}"))?;
                        let prior_keys: Vec<String> = prior
                            .into_iter()
                            .map(|object| object.key)
                            .filter(|key| key.ends_with(".parquet"))
                            .collect();

                        for (out_rel, local_path, _rows) in
                            outputs.iter().filter(|(r, _, _)| r.starts_with(tree))
                        {
                            let key = s3_join(&output_s3_prefix, out_rel);
                            if let Err(error) = upload_with_retries(storage, local_path, &key)
                                .await
                                .with_context(|| {
                                    format!("uploading {} to S3", local_path.display())
                                })
                            {
                                CANCELLED.store(true, Ordering::Relaxed);
                                return Err(error);
                            }
                        }
                        for key in prior_keys {
                            storage
                                .delete_key(&key)
                                .await
                                .with_context(|| format!("deleting prior object {key}"))?;
                        }
                    }
                }

                let done = processed.fetch_add(1, Ordering::SeqCst) + 1;
                if done == 1 || done.is_multiple_of(10) || done == total_partitions {
                    eprintln!("Processed {done}/{total_partitions} partition(s).");
                }
            }

            Ok::<_, anyhow::Error>(stats)
        }));
    }

    let mut total_stats = ParseStats::default();
    let mut first_error = None;
    for worker in workers {
        match worker.await {
            Ok(Ok(stats)) => total_stats.merge(&stats),
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
                "Decoded output may remain un-uploaded in {} — inspect and upload it manually, \
                 or re-run once the S3 issue is resolved (the same input regenerates equivalent output).",
                scratch_path.display()
            );
        }
        return Err(error);
    }

    if let Some(scratch_path) = &output_scratch_path {
        let _ = tokio::fs::remove_dir_all(scratch_path).await;
    }

    if is_cancelled() {
        eprintln!(
            "Cancelled after {} partition(s).",
            processed.load(Ordering::SeqCst)
        );
    }

    // Advance the watermark only after a fully successful, applied run.
    if let Some(store) = &state_store {
        if !is_cancelled() {
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

    total_stats.print_summary();
    eprintln!(
        "Done. Decoded {} partition(s).",
        total_stats.partitions_processed
    );

    Ok(())
}

fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4)
        .clamp(1, 8)
}

/// `(output rel path like "positions/year=.../pos-....parquet", local file
/// path, rows)` per file this partition produced. The output layout is not
/// partitioned by source.
type PartitionOutputs = Vec<(String, PathBuf, u64)>;

/// Record a finished output file (if the writer produced one) under
/// `<tree>/<rel_dir>/<name>`.
fn push_output(
    outputs: &mut PartitionOutputs,
    tree: &str,
    rel_dir: &str,
    done: Option<(PathBuf, u64)>,
) {
    if let Some((path, rows)) = done {
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        outputs.push((format!("{tree}/{rel_dir}/{name}"), path, rows));
    }
}

/// Decode one output partition end to end into one positions file and one
/// statics file, then — for local output — replace the prior run's files in
/// the two output partitions.
///
/// The output is not partitioned by source, so `files` can pool several
/// sources that map to this time partition. Fragment reassembly is per-source
/// (a fresh parser when the source label changes; `files` is sorted so each
/// source's files are contiguous), which keeps multi-part sequence ids from
/// colliding across sources.
#[allow(clippy::too_many_arguments)]
fn process_partition(
    partition_key: PartitionKey,
    files: Vec<DatasetFile>,
    output_root: PathBuf,
    batch_size: usize,
    compression_level: i32,
    replace_local: bool,
) -> Result<(ParseStats, PartitionOutputs)> {
    let rel_dir = partition_key.relative_dir_time_only();
    let dir_for = |tree: &str| output_root.join(tree).join(&rel_dir);

    // Collected before writing so the new (uniquely-named) files are never in
    // the deletion set.
    let prior_files: Vec<PathBuf> = if replace_local {
        let mut prior = Vec::new();
        for tree in OUTPUT_TREES {
            prior.extend(existing_parquet_files(&dir_for(tree))?);
        }
        prior
    } else {
        Vec::new()
    };

    let mut parser = NmeaParser::new();
    let mut current_source: Option<&str> = None;
    let mut stats = ParseStats::default();
    let mut positions = PositionsWriter::new(dir_for(POSITIONS_TREE), compression_level);
    let mut statics = StaticsWriter::new(dir_for(STATICS_TREE), compression_level);
    let mut meteo = MeteoWriter::new(dir_for(METEO_TREE), compression_level);
    let mut binary = BinaryWriter::new(dir_for(BINARY_TREE), compression_level);
    let mut atons = AtonWriter::new(dir_for(ATONS_TREE), compression_level);

    for file in &files {
        // Reset fragment state at each source boundary.
        if current_source != Some(file.partition.source.as_str()) {
            parser = NmeaParser::new();
            current_source = Some(file.partition.source.as_str());
        }
        process_parquet_file(
            &file.path,
            &file.partition.source,
            &mut parser,
            &mut stats,
            Writers {
                positions: &mut positions,
                statics: &mut statics,
                meteo: &mut meteo,
                binary: &mut binary,
                atons: &mut atons,
            },
            batch_size,
        )
        .with_context(|| format!("processing {}", file.path.display()))?;
    }
    stats.partitions_processed += 1;

    let mut outputs: PartitionOutputs = Vec::new();
    push_output(
        &mut outputs,
        POSITIONS_TREE,
        &rel_dir,
        positions.finish().context("closing positions writer")?,
    );
    push_output(
        &mut outputs,
        STATICS_TREE,
        &rel_dir,
        statics.finish().context("closing statics writer")?,
    );
    push_output(
        &mut outputs,
        METEO_TREE,
        &rel_dir,
        meteo.finish().context("closing meteo writer")?,
    );
    push_output(
        &mut outputs,
        BINARY_TREE,
        &rel_dir,
        binary.finish().context("closing binary writer")?,
    );
    push_output(
        &mut outputs,
        ATONS_TREE,
        &rel_dir,
        atons.finish().context("closing atons writer")?,
    );

    // New files are durable: the prior run's output for this partition is now
    // redundant (decoding is deterministic, so this is a replace, not a loss).
    for prior in prior_files {
        std::fs::remove_file(&prior)
            .with_context(|| format!("removing prior output {}", prior.display()))?;
    }

    Ok((stats, outputs))
}

/// The set of per-partition writers, threaded into the row loop.
struct Writers<'a> {
    positions: &'a mut PositionsWriter,
    statics: &'a mut StaticsWriter,
    meteo: &'a mut MeteoWriter,
    binary: &'a mut BinaryWriter,
    atons: &'a mut AtonWriter,
}

fn process_parquet_file(
    path: &Path,
    path_source: &str,
    parser: &mut NmeaParser,
    stats: &mut ParseStats,
    writers: Writers<'_>,
    batch_size: usize,
) -> Result<()> {
    let file = StdFile::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("read Parquet footer {}", path.display()))?
        .with_batch_size(batch_size)
        .build()
        .with_context(|| format!("build Parquet reader {}", path.display()))?;

    // Columns are matched by name, not position: normalized input is
    // (ts, source, payload) while raw bronze is (ts, payload). The `source`
    // column (present in normalized data, per-row after a dedup merge pooled
    // several sources) takes precedence; bronze has none, so we fall back to
    // the source parsed from the file's partition path.
    while let Some(batch) = reader.next().transpose()? {
        let schema = batch.schema();
        let ts_idx = schema.index_of("ts").unwrap_or(0);
        let payload_idx = schema
            .index_of("payload")
            .map_err(|_| anyhow::anyhow!("input {} has no payload column", path.display()))?;
        let source_idx = schema.index_of("source").ok();

        let ts_col = batch
            .column(ts_idx)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .context("expected a timestamp `ts` column")?;
        let payload_col = batch
            .column(payload_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected a string `payload` column")?;
        let source_col = source_idx
            .map(|idx| {
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("expected a string `source` column")
            })
            .transpose()?;

        for i in 0..batch.num_rows() {
            if is_cancelled() {
                return Ok(());
            }
            stats.rows_in += 1;
            let source = match &source_col {
                Some(col) if !col.is_null(i) => col.value(i),
                _ => path_source,
            };
            match decode_payload(parser, ts_col.value(i), source, payload_col.value(i)) {
                Decoded::Position(row) => {
                    stats.positions_out += 1;
                    writers.positions.write(&row)?;
                }
                Decoded::Static(row) => {
                    stats.statics_out += 1;
                    writers.statics.write(&row)?;
                }
                Decoded::Meteo(row) => {
                    stats.meteo_out += 1;
                    writers.meteo.write(*row)?;
                }
                Decoded::Binary(row) => {
                    stats.binary_out += 1;
                    writers.binary.write(*row)?;
                }
                Decoded::Aton(row) => {
                    stats.atons_out += 1;
                    writers.atons.write(*row)?;
                }
                Decoded::Other => stats.other_decoded += 1,
                Decoded::Incomplete => stats.incomplete += 1,
                Decoded::Failed => stats.failed += 1,
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_join_handles_prefixes() {
        assert_eq!(
            s3_join("", "positions/source=x/f.parquet"),
            "positions/source=x/f.parquet"
        );
        assert_eq!(
            s3_join("/silver/", "positions/source=x/f.parquet"),
            "silver/positions/source=x/f.parquet"
        );
    }
}
