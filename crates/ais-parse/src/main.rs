use anyhow::{bail, Context, Result};
use arrow::array::{Array, StringArray, TimestampMillisecondArray};
use chrono::TimeZone;
use rand::Rng;
use clap::Parser;
use collect_core::ais_consolidate::{AisConsolidator, AisConsolidatorConfig};
use collect_core::dataset::{self, DatasetFile, PartitionKey};
use collect_core::state;
use collect_core::LineTransformer;
use collect_core::{apply_config_file, PartitionGranularity, S3ConnectionArgs, S3Storage};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::collections::{HashSet, VecDeque};
use std::fs::File as StdFile;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use collect_core::iceberg::table_schemas;
use iceberg::Catalog;
use collect_core::iceberg::{
    ensure_namespace, ensure_table, open_catalog, partition_spec_for, IcebergConfig, TABLE_ATONS,
    TABLE_BINARY, TABLE_METEO, TABLE_POSITIONS, TABLE_STATICS,
};
use output_iceberg::{
    commit_table_batches, IcebergAtonWriter, IcebergBinaryWriter, IcebergMeteoWriter,
    IcebergPositionsWriter, IcebergStaticsWriter,
};

mod ais_bits;
mod decode;
mod output;
mod output_iceberg;
mod stats;

use decode::{decode_payload, AtonRow, BinaryRow, Decoded, MeteoRow, OtherRow, PositionRow, StaticRow};
use output::{AtonWriter, BinaryWriter, MeteoWriter, OtherWriter, PositionsWriter, StaticsWriter};
use stats::ParseStats;

/// Decoded output lands in sibling hive datasets under the output root.
const POSITIONS_TREE: &str = "positions";
const STATICS_TREE: &str = "statics";
const METEO_TREE: &str = "meteo";
const BINARY_TREE: &str = "binary";
const ATONS_TREE: &str = "atons";
const OTHER_TREE: &str = "other";

/// Exit code used when there was nothing to process (distinct from success
/// with rows written, and from a hard error).
const EXIT_NOTHING_TO_DO: i32 = 2;

static CANCELLED: AtomicBool = AtomicBool::new(false);

fn is_cancelled() -> bool {
    CANCELLED.load(Ordering::Relaxed)
}

/// Unique key for row-level dedup within a single partition run. The leading
/// discriminator keeps different row kinds from colliding on the same
/// `(ts_ms, mmsi)` (a position and a static for one MMSI at one instant are
/// distinct rows that must not suppress each other).
///
/// Layout: `(variant, ts_ms, mmsi, dac_fid, msg_type)`
/// - variant: 0=Position, 1=Static, 2=Meteo, 3=Binary, 4=Aton
/// - `dac_fid` packs `(dac << 8) | fid` for Meteo/Binary, else 0
/// - `msg_type` carried for Position/Aton, else 0
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct DedupKey(u8, i64, u32, u32, u8);

#[derive(Parser, Debug)]
#[command(
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_COMMIT_HASH"), ")"),
    about = "Decode normalized AIS sentences into typed Parquet (positions and vessel statics)"
)]
struct Args {
    /// Normalized Hive-partitioned Parquet root directory. Repeatable to
    /// merge several sources in one run (mutually exclusive with --input-s3-bucket)
    #[arg(long, env = "INPUT_DIR", value_delimiter = ',')]
    input_dir: Vec<PathBuf>,

    /// Output root; decoded data is written under <root>/positions and
    /// <root>/statics (mutually exclusive with --output-s3-bucket)
    #[arg(long, env = "OUTPUT_DIR")]
    output_dir: Option<PathBuf>,

    /// S3 bucket to read the input dataset from, instead of --input-dir.
    /// Repeatable to merge several buckets (all on the shared endpoint)
    #[arg(long, env = "INPUT_S3_BUCKET", value_delimiter = ',')]
    input_s3_bucket: Vec<String>,

    /// Key prefix within the input S3 bucket; acts as the dataset root
    #[arg(long, env = "INPUT_S3_PREFIX", default_value = "")]
    input_s3_prefix: String,

    /// S3 bucket to write decoded output to, instead of --output-dir
    #[arg(long, env = "OUTPUT_S3_BUCKET")]
    output_s3_bucket: Option<String>,

    /// Key prefix within the output S3 bucket; acts as the dataset root
    #[arg(long, env = "OUTPUT_S3_PREFIX", default_value = "")]
    output_s3_prefix: String,

    #[command(flatten)]
    s3_connection: S3ConnectionArgs,

    /// Partition granularity; must match the input dataset layout (the
    /// output trees mirror it)
    #[arg(long, env = "PARTITION", default_value_t = PartitionGranularity::Day)]
    partition: PartitionGranularity,

    /// Filter to a specific source label (processes all sources if omitted)
    #[arg(
        long = "filter-source",
        visible_alias = "source",
        env = "FILTER_SOURCE"
    )]
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
    #[arg(long, env = "SINCE", value_name = "HOURS")]
    since: Option<u64>,

    /// Track a watermark at the output target and process only partitions
    /// holding files that arrived since the last successful run
    #[arg(long, env = "INCREMENTAL", value_parser = clap::builder::FalseyValueParser::new())]
    incremental: bool,

    /// Number of rows per Parquet read batch
    #[arg(long, env = "BATCH_SIZE", default_value_t = 8192)]
    batch_size: usize,

    /// Zstd compression level for output files
    #[arg(long, env = "COMPRESSION_LEVEL", default_value_t = 5)]
    compression_level: i32,

    /// Number of partitions to process concurrently; auto-selected when omitted
    #[arg(long, env = "CONCURRENCY")]
    concurrency: Option<usize>,

    /// Concurrent S3 downloads per partition (default: 4)
    #[arg(long, env = "DOWNLOAD_CONCURRENCY", default_value_t = 4)]
    download_concurrency: usize,

    /// Output file name prefix (added before tree suffix, e.g. `{prefix}-pos-*.parquet`)
    #[arg(long, env = "OUTPUT_PREFIX", default_value = "ais")]
    output_prefix: String,

    /// Directory for temporary scratch files (S3 downloads, intermediate Parquet).
    /// Defaults to the system temp dir. Set to /dev/shm or a ramdisk for faster I/O.
    #[arg(long, env = "SCRATCH_DIR")]
    scratch_dir: Option<PathBuf>,

    /// Apply AIS multipart consolidation before decoding (reassembles
    /// fragmented NMEA sentences into single sentences before parsing).
    #[arg(long)]
    consolidate_ais: bool,

    /// Process $PGHP timestamp lines and tag-block c: carry-forward to
    /// correct row timestamps. Independent of --consolidate-ais.
    #[arg(long)]
    process_timestamps: bool,

    /// List the partitions that would be processed and exit without
    /// decoding, writing, or touching the output target
    #[arg(long, env = "DRY_RUN")]
    dry_run: bool,

    /// Suppress informational progress lines; warnings and errors still print
    #[arg(short, long, env = "QUIET")]
    quiet: bool,

    #[command(flatten)]
    iceberg: collect_core::iceberg::IcebergCliArgs,

    /// Print shell completions for the given shell to stdout and exit
    #[arg(long, exclusive = true)]
    completions: Option<clap_complete::Shell>,

    /// Load flag defaults from a flat TOML config file (KEY = value, using
    /// the same env var names shown in --help); explicit flags and
    /// already-set env vars still take precedence over the file
    #[arg(long, env = "CONFIG_FILE")]
    config: Option<PathBuf>,
}

impl Args {
    /// Drop empty-string values that reach us via empty env vars (e.g. an
    /// empty `OUTPUT_S3_BUCKET=` in a job spec means "not set").
    fn normalize(&mut self) {
        self.input_s3_bucket.retain(|b| !b.trim().is_empty());
        self.input_dir.retain(|d| !d.as_os_str().is_empty());
        if matches!(&self.output_s3_bucket, Some(b) if b.trim().is_empty()) {
            self.output_s3_bucket = None;
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

/// RFC 3339 label for a UTC millisecond timestamp, for log lines.
fn format_ms(ms: i64) -> String {
    chrono::Utc
        .timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| format!("{ms}ms"))
}

const UPLOAD_MAX_ATTEMPTS: u32 = 3;
const DOWNLOAD_MAX_ATTEMPTS: u32 = 3;

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

    if let Some(shell) = args.completions {
        collect_core::print_completions::<Args>(shell, "ais-parse");
        return Ok(());
    }

    if let Some(config_path) = &args.config {
        apply_config_file(config_path)?;
        args = Args::parse();
    }

    args.normalize();
    args.split_s3_args();
    let quiet = args.quiet;

    match (args.input_dir.is_empty(), args.input_s3_bucket.is_empty()) {
        (false, false) => bail!("use either --input-dir or --input-s3-bucket, not both"),
        (true, true) => bail!("one of --input-dir or --input-s3-bucket is required"),
        _ => {}
    }
    match (&args.output_dir, &args.output_s3_bucket) {
        (Some(_), Some(_)) => bail!("use either --output-dir or --output-s3-bucket, not both"),
        (None, None) if args.incremental && args.iceberg.is_iceberg_mode() => {
            bail!("--output-s3-bucket is required for the incremental watermark with Iceberg output")
        }
        (None, None) if !args.iceberg.is_iceberg_mode() => {
            bail!("one of --output-dir, --output-s3-bucket, or --iceberg-catalog-uri is required")
        }
        _ => {}
    }
    args.iceberg.validate()?;

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
    // --dry-run never touches the output target: connecting can create a
    // missing bucket as a side effect (see S3Storage::ensure_bucket), and a
    // preview should be side-effect-free. Incremental selection therefore
    // falls back to --since (or the full dataset) under --dry-run, since the
    // real watermark lives at the output.
    let output_storage = if args.dry_run {
        None
    } else {
        match &args.output_s3_bucket {
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
        }
    };

    // Incremental watermark, stored per tool at the output target.
    let state_store = if args.incremental && !args.dry_run {
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
    if args.incremental && args.dry_run {
        eprintln!(
            "Note: --dry-run does not connect to the output, so incremental selection here \
             uses --since (or the full dataset) instead of the real watermark."
        );
        incremental_cutoff = since_ms;
    }
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
            match &args.scratch_dir {
                Some(dir) => tempfile::Builder::new()
                    .prefix("ais-parse-input-")
                    .tempdir_in(dir),
                None => tempfile::Builder::new()
                    .prefix("ais-parse-input-")
                    .tempdir(),
            }
        })
        .transpose()
        .context("creating input scratch directory")?;
    let output_scratch_path: Option<PathBuf> = if output_storage.is_some() {
        let dir = match &args.scratch_dir {
            Some(scratch) => tempfile::Builder::new()
                .prefix("ais-parse-output-")
                .tempdir_in(scratch)
                .context("creating output scratch directory")?,
            None => tempfile::Builder::new()
                .prefix("ais-parse-output-")
                .tempdir()
                .context("creating output scratch directory")?,
        };
        Some(dir.keep())
    } else {
        None
    };
    let output_root: PathBuf = match (&args.output_dir, &output_scratch_path) {
        (Some(dir), None) => dir.clone(),
        (None, Some(scratch_path)) => scratch_path.clone(),
        (None, None) => PathBuf::new(),
        (Some(_), Some(_)) => unreachable!("validated above"),
    };

    enum PartitionWork {
        Local(Vec<DatasetFile>),
        Remote(Vec<dataset::S3Entry>),
    }

    let mut partitions: Vec<(PartitionKey, PartitionWork)> = if input_is_s3 {
        if !quiet {
            eprintln!("Listing {} input S3 bucket(s)...", input_storages.len());
        }
        if !partition_filter.is_empty() && args.source.is_none() {
            eprintln!(
                "Note: without --source the partition filter is applied after listing; \
                 add --source to push it into the S3 LIST prefix."
            );
        }
        let listing_futures: Vec<_> = input_storages
            .iter()
            .enumerate()
            .map(|(index, storage)| {
                let bucket = args.input_s3_bucket[index].clone();
                let prefix = args.input_s3_prefix.clone();
                let source = args.source.clone();
                let pf = partition_filter;
                async move {
                    let mut bucket_entries = dataset::list_s3_parquet_entries(
                        storage,
                        &prefix,
                        args.partition,
                        source.as_deref(),
                        pf,
                    )
                    .await
                    .with_context(|| format!("listing input S3 bucket {bucket}"))?;
                    for entry in &mut bucket_entries {
                        entry.storage_index = index;
                    }
                    Ok::<_, anyhow::Error>(bucket_entries)
                }
            })
            .collect();
        let results = futures_util::future::try_join_all(listing_futures).await?;
        let mut entries: Vec<dataset::S3Entry> = results.into_iter().flatten().collect();
        if entries.is_empty() {
            eprintln!(
                "No matching Parquet objects found across {} bucket(s) under prefix {:?}.",
                input_storages.len(),
                args.input_s3_prefix
            );
            if args.dry_run {
                return Ok(());
            }
            std::process::exit(EXIT_NOTHING_TO_DO);
        }
        if !quiet {
            eprintln!("Found {} matching object(s).", entries.len());
        }
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
        if !quiet {
            eprintln!("Scanning {} input dir(s)...", args.input_dir.len());
        }
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
            if args.dry_run {
                return Ok(());
            }
            std::process::exit(EXIT_NOTHING_TO_DO);
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
            if args.dry_run {
                return Ok(());
            }
            std::process::exit(EXIT_NOTHING_TO_DO);
        }
    }

    let total_partitions = partitions.len();
    if !quiet {
        eprintln!("Found {} partition(s).", total_partitions);
    }

    if args.dry_run {
        if let Some(scratch_path) = &output_scratch_path {
            let _ = tokio::fs::remove_dir_all(scratch_path).await;
        }
        for (key, work) in &partitions {
            let (file_count, kind) = match work {
                PartitionWork::Local(files) => (files.len(), "local"),
                PartitionWork::Remote(entries) => (entries.len(), "S3"),
            };
            println!(
                "{}  ({} {} file{})",
                key.relative_dir_time_only(),
                file_count,
                kind,
                if file_count == 1 { "" } else { "s" }
            );
        }
        eprintln!(
            "Dry run: {} partition(s) would be processed. No output was written.",
            total_partitions
        );
        return Ok(());
    }

    let concurrency = args
        .concurrency
        .unwrap_or_else(default_concurrency)
        .clamp(1, total_partitions.max(1));
    if !quiet && concurrency > 1 {
        eprintln!("Processing partitions with {} worker(s).", concurrency);
    }

    type PartitionQueue = Mutex<VecDeque<(PartitionKey, PartitionWork)>>;
    let queue: Arc<PartitionQueue> = Arc::new(Mutex::new(partitions.into()));
    let processed = Arc::new(AtomicUsize::new(0));
    let input_scratch_root: Option<PathBuf> = input_scratch
        .as_ref()
        .map(|scratch| scratch.path().to_path_buf());

    let download_concurrency = args.download_concurrency;
    let output_prefix = args.output_prefix.clone();
    let consolidate_ais = args.consolidate_ais;
    let process_timestamps = args.process_timestamps;
    let compression_level = args.compression_level;
    let mut total_stats = ParseStats::default();
    let mut first_error = None;

    if !args.iceberg.is_iceberg_mode() {
        let mut workers = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let queue = queue.clone();
            let processed = processed.clone();
            let output_root = output_root.clone();
            let output_storage = output_storage.clone();
            let output_s3_prefix = args.output_s3_prefix.clone();
            let output_prefix = output_prefix.clone();
            let input_storages = input_storages.clone();
            let input_scratch_root = input_scratch_root.clone();
            let batch_size = args.batch_size;

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
                    let partition_label = partition_key.relative_dir_time_only();
                    if !quiet {
                        let file_count = match &work {
                            PartitionWork::Local(files) => files.len(),
                            PartitionWork::Remote(entries) => entries.len(),
                        };
                        eprintln!("  starting {} ({} files) ...", partition_label, file_count);
                    }
                    let partition_files = match work {
                        PartitionWork::Local(files) => files,
                        PartitionWork::Remote(entries) => {
                            let scratch_root = input_scratch_root
                                .as_ref()
                                .expect("remote work implies input scratch dir");
                            let mut result = None;
                            for attempt in 1..=DOWNLOAD_MAX_ATTEMPTS {
                                match dataset::download_s3_entries(
                                    &input_storages,
                                    entries.clone(),
                                    scratch_root,
                                    download_concurrency,
                                )
                                .await
                                {
                                    Ok(files) => {
                                        result = Some(files);
                                        break;
                                    }
                                    Err(error) => {
                                        if attempt < DOWNLOAD_MAX_ATTEMPTS {
                                            let base_secs = 5u64 * (1 << (attempt - 1));
                                            let jitter = rand::thread_rng().gen_range(0..base_secs);
                                            let backoff = std::time::Duration::from_secs(base_secs + jitter);
                                            eprintln!(
                                                "Download attempt {attempt}/{DOWNLOAD_MAX_ATTEMPTS} failed for partition, retrying in ~{}s...",
                                                base_secs + jitter / 2
                                            );
                                            tokio::time::sleep(backoff).await;
                                        } else {
                                            CANCELLED.store(true, Ordering::Relaxed);
                                            return Err(error).context("downloading input partition from S3");
                                        }
                                    }
                                }
                            }
                            result.expect("loop runs at least once")
                        }
                    };

                    let scratch_files: Vec<PathBuf> = if is_remote {
                        partition_files.iter().map(|f| f.path.clone()).collect()
                    } else {
                        Vec::new()
                    };

                    let output_root_for_task = output_root.clone();
                    let output_prefix_for_task = output_prefix.clone();
                    let result = tokio::task::spawn_blocking(move || {
                        process_partition(
                            partition_key,
                            partition_files,
                            output_root_for_task,
                            batch_size,
                            compression_level,
                            &output_prefix_for_task,
                            consolidate_ais,
                            process_timestamps,
                        )
                    })
                    .await
                    .context("partition worker panicked")?;

                    for scratch_file in &scratch_files {
                        let _ = tokio::fs::remove_file(scratch_file).await;
                    }

                    let outputs = match result {
                        Ok((partition_stats, outputs)) => {
                            if !quiet {
                                eprintln!(
                                    "  {}: +{} rows (pos={}, stat={}, met={}, bin={}, aton={}, other={}, unparsed={})",
                                    partition_label,
                                    partition_stats.rows_in,
                                    partition_stats.positions_out,
                                    partition_stats.statics_out,
                                    partition_stats.meteo_out,
                                    partition_stats.binary_out,
                                    partition_stats.atons_out,
                                    partition_stats.others_out,
                                    partition_stats.failed,
                                );
                            }
                            stats.merge(&partition_stats);
                            outputs
                        }
                        Err(error) => {
                            CANCELLED.store(true, Ordering::Relaxed);
                            return Err(error);
                        }
                    };

                    // S3 output: append this run's files. Each run writes uniquely
                    // named files per partition; row-level dedup inside the decoder
                    // keeps the union of runs free of duplicate rows. Old files are
                    // left in place and compacted downstream.
                    if let Some(storage) = &output_storage {
                        for (out_rel, local_path, _rows) in &outputs {
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
                    }

                    let done = processed.fetch_add(1, Ordering::SeqCst) + 1;
                    if !quiet && (done == 1 || done.is_multiple_of(10) || done == total_partitions)
                    {
                        eprintln!("Processed {done}/{total_partitions} partition(s).");
                    }
                }

                Ok::<_, anyhow::Error>(stats)
            }));
        }

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
    } else {
        let mut workers = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let queue = queue.clone();
            let processed = processed.clone();
            let input_storages = input_storages.clone();
            let input_scratch_root = input_scratch_root.clone();
            let batch_size = args.batch_size;

            workers.push(tokio::spawn(async move {
                let mut stats = ParseStats::default();
                let mut partition_batches: Vec<IcebergPartitionOutput> = Vec::new();

                loop {
                    if is_cancelled() {
                        break;
                    }
                    let next = queue.lock().expect("partition queue lock").pop_front();
                    let Some((partition_key, work)) = next else {
                        break;
                    };

                    let is_remote = matches!(work, PartitionWork::Remote(_));
                    let partition_label = partition_key.relative_dir_time_only();
                    if !quiet {
                        let file_count = match &work {
                            PartitionWork::Local(files) => files.len(),
                            PartitionWork::Remote(entries) => entries.len(),
                        };
                        eprintln!("  starting {} ({} files) ...", partition_label, file_count);
                    }
                    let partition_files = match work {
                        PartitionWork::Local(files) => files,
                        PartitionWork::Remote(entries) => {
                            let scratch_root = input_scratch_root
                                .as_ref()
                                .expect("remote work implies input scratch dir");
                            let mut result = None;
                            for attempt in 1..=DOWNLOAD_MAX_ATTEMPTS {
                                match dataset::download_s3_entries(
                                    &input_storages,
                                    entries.clone(),
                                    scratch_root,
                                    download_concurrency,
                                )
                                .await
                                {
                                    Ok(files) => {
                                        result = Some(files);
                                        break;
                                    }
                                    Err(error) => {
                                        if attempt < DOWNLOAD_MAX_ATTEMPTS {
                                            let base_secs = 5u64 * (1 << (attempt - 1));
                                            let jitter = rand::thread_rng().gen_range(0..base_secs);
                                            let backoff = std::time::Duration::from_secs(base_secs + jitter);
                                            eprintln!(
                                                "Download attempt {attempt}/{DOWNLOAD_MAX_ATTEMPTS} failed for partition, retrying in ~{}s...",
                                                base_secs + jitter / 2
                                            );
                                            tokio::time::sleep(backoff).await;
                                        } else {
                                            CANCELLED.store(true, Ordering::Relaxed);
                                            return Err(error).context("downloading input partition from S3");
                                        }
                                    }
                                }
                            }
                            result.expect("loop runs at least once")
                        }
                    };

                    let scratch_files: Vec<PathBuf> = if is_remote {
                        partition_files.iter().map(|f| f.path.clone()).collect()
                    } else {
                        Vec::new()
                    };

                    let result = tokio::task::spawn_blocking(move || {
                        process_partition_iceberg(
                            partition_key,
                            partition_files,
                            batch_size,
                            consolidate_ais,
                            process_timestamps,
                        )
                    })
                    .await
                    .context("partition worker panicked")?;

                    for scratch_file in &scratch_files {
                        let _ = tokio::fs::remove_file(scratch_file).await;
                    }

                    match result {
                        Ok((partition_stats, batches)) => {
                            if !quiet {
                                eprintln!(
                                    "  {}: +{} rows (pos={}, stat={}, met={}, bin={}, aton={}, other={}, unparsed={})",
                                    partition_label,
                                    partition_stats.rows_in,
                                    partition_stats.positions_out,
                                    partition_stats.statics_out,
                                    partition_stats.meteo_out,
                                    partition_stats.binary_out,
                                    partition_stats.atons_out,
                                    partition_stats.others_out,
                                    partition_stats.failed,
                                );
                            }
                            stats.merge(&partition_stats);
                            partition_batches.push(batches);
                        }
                        Err(error) => {
                            CANCELLED.store(true, Ordering::Relaxed);
                            return Err(error);
                        }
                    }

                    let done = processed.fetch_add(1, Ordering::SeqCst) + 1;
                    if !quiet && (done == 1 || done.is_multiple_of(10) || done == total_partitions)
                    {
                        eprintln!("Processed {done}/{total_partitions} partition(s).");
                    }
                }

                Ok::<_, anyhow::Error>((stats, partition_batches))
            }));
        }

        // Open catalog and ensure tables before joining workers so each
        // partition commits immediately as its worker finishes.
        let (catalog, positions_table, statics_table, meteo_table, binary_table, atons_table) =
            if first_error.is_none() {
                let config: IcebergConfig = (&args.iceberg).into();
                let catalog = open_catalog(&config)
                    .await
                    .context("connecting to Iceberg catalog")?;
                ensure_namespace(&catalog, &config).await?;

                let partition_granularity = args.partition.as_str();
                let positions_table = ensure_table(
                    &catalog, &config, TABLE_POSITIONS,
                    table_schemas::positions_schema(),
                    partition_spec_for(&table_schemas::positions_schema(), partition_granularity)?,
                ).await?;
                let statics_table = ensure_table(
                    &catalog, &config, TABLE_STATICS,
                    table_schemas::statics_schema(),
                    partition_spec_for(&table_schemas::statics_schema(), partition_granularity)?,
                ).await?;
                let meteo_table = ensure_table(
                    &catalog, &config, TABLE_METEO,
                    table_schemas::meteo_schema(),
                    partition_spec_for(&table_schemas::meteo_schema(), partition_granularity)?,
                ).await?;
                let binary_table = ensure_table(
                    &catalog, &config, TABLE_BINARY,
                    table_schemas::binary_schema(),
                    partition_spec_for(&table_schemas::binary_schema(), partition_granularity)?,
                ).await?;
                let atons_table = ensure_table(
                    &catalog, &config, TABLE_ATONS,
                    table_schemas::atons_schema(),
                    partition_spec_for(&table_schemas::atons_schema(), partition_granularity)?,
                ).await?;
                (Some(catalog), Some(positions_table), Some(statics_table), Some(meteo_table), Some(binary_table), Some(atons_table))
            } else {
                (None, None, None, None, None, None)
            };

        for worker in workers {
            match worker.await {
                Ok(Ok((stats, batches))) => {
                    total_stats.merge(&stats);
                    eprintln!("  [DEBUG] worker finished with {} partition(s) in this batch", batches.len());
                    if let (Some(ref cat), Some(ref pos), Some(ref stat), Some(ref met), Some(ref bin), Some(ref atn)) = (catalog.as_ref(), positions_table.as_ref(), statics_table.as_ref(), meteo_table.as_ref(), binary_table.as_ref(), atons_table.as_ref()) {
                        let cat: &dyn Catalog = &**cat;
                        for (i, output) in batches.into_iter().enumerate() {
                            eprintln!("  Committing partition {} to Iceberg ...", i + 1);
                            commit_table_batches(cat, pos, output.positions, compression_level, TABLE_POSITIONS).await?;
                            commit_table_batches(cat, stat, output.statics, compression_level, TABLE_STATICS).await?;
                            commit_table_batches(cat, met, output.meteo, compression_level, TABLE_METEO).await?;
                            commit_table_batches(cat, bin, output.binary, compression_level, TABLE_BINARY).await?;
                            commit_table_batches(cat, atn, output.atons, compression_level, TABLE_ATONS).await?;
                            eprintln!("  Committed partition {} to Iceberg.", i + 1);
                        }
                    } else {
                        eprintln!("  [DEBUG] WARNING: catalog or table reference is None — skipping commit for partition");
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
    output_prefix: &str,
    consolidate_ais: bool,
    process_timestamps: bool,
) -> Result<(ParseStats, PartitionOutputs)> {
    let rel_dir = partition_key.relative_dir_time_only();
    let dir_for = |tree: &str| output_root.join(tree).join(&rel_dir);

    let mut stats = ParseStats::default();
    let mut positions =
        PositionsWriter::new(dir_for(POSITIONS_TREE), output_prefix, compression_level);
    let mut statics = StaticsWriter::new(dir_for(STATICS_TREE), output_prefix, compression_level);
    let mut meteo = MeteoWriter::new(dir_for(METEO_TREE), output_prefix, compression_level);
    let mut binary = BinaryWriter::new(dir_for(BINARY_TREE), output_prefix, compression_level);
    let mut atons = AtonWriter::new(dir_for(ATONS_TREE), output_prefix, compression_level);
    let mut other = OtherWriter::new(dir_for(OTHER_TREE), output_prefix, compression_level);

    // Row-level dedup keyed on (ts, mmsi, source-kind) so the same AIS message
    // seen in more than one input file for this partition is emitted once.
    let mut seen: HashSet<DedupKey> = HashSet::new();

    for file in &files {
        process_parquet_file(
            &file.path,
            &file.partition.source,
            &mut stats,
            &mut Writers {
                positions: &mut positions,
                statics: &mut statics,
                meteo: &mut meteo,
                binary: &mut binary,
                atons: &mut atons,
                other: &mut other,
            },
            batch_size,
            &mut seen,
            consolidate_ais,
            process_timestamps,
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
    push_output(
        &mut outputs,
        OTHER_TREE,
        &rel_dir,
        other.finish().context("closing other writer")?,
    );

    Ok((stats, outputs))
}

/// The set of per-partition writers, threaded into the row loop.
struct Writers<'a> {
    positions: &'a mut PositionsWriter,
    statics: &'a mut StaticsWriter,
    meteo: &'a mut MeteoWriter,
    binary: &'a mut BinaryWriter,
    atons: &'a mut AtonWriter,
    other: &'a mut OtherWriter,
}

trait WriterSet {
    fn write_position(&mut self, row: &PositionRow, payload: &str) -> Result<()>;
    fn write_static(&mut self, row: &StaticRow, payload: &str) -> Result<()>;
    fn write_meteo(&mut self, row: MeteoRow, payload: &str) -> Result<()>;
    fn write_binary(&mut self, row: BinaryRow, payload: &str) -> Result<()>;
    fn write_aton(&mut self, row: AtonRow, payload: &str) -> Result<()>;
    fn write_other(&mut self, row: OtherRow) -> Result<()>;
}

impl<'a> WriterSet for Writers<'a> {
    fn write_position(&mut self, row: &PositionRow, payload: &str) -> Result<()> {
        self.positions.write(row, payload)
    }
    fn write_static(&mut self, row: &StaticRow, payload: &str) -> Result<()> {
        self.statics.write(row, payload)
    }
    fn write_meteo(&mut self, row: MeteoRow, payload: &str) -> Result<()> {
        self.meteo.write(row, payload)
    }
    fn write_binary(&mut self, row: BinaryRow, payload: &str) -> Result<()> {
        self.binary.write(row, payload)
    }
    fn write_aton(&mut self, row: AtonRow, payload: &str) -> Result<()> {
        self.atons.write(row, payload)
    }
    fn write_other(&mut self, row: OtherRow) -> Result<()> {
        self.other.write(row)
    }
}

struct IcebergWriters {
    positions: IcebergPositionsWriter,
    statics: IcebergStaticsWriter,
    meteo: IcebergMeteoWriter,
    binary: IcebergBinaryWriter,
    atons: IcebergAtonWriter,
}

impl WriterSet for IcebergWriters {
    fn write_position(&mut self, row: &PositionRow, payload: &str) -> Result<()> {
        self.positions.write(row, payload)
    }
    fn write_static(&mut self, row: &StaticRow, payload: &str) -> Result<()> {
        self.statics.write(row, payload)
    }
    fn write_meteo(&mut self, row: MeteoRow, payload: &str) -> Result<()> {
        self.meteo.write(row, payload)
    }
    fn write_binary(&mut self, row: BinaryRow, payload: &str) -> Result<()> {
        self.binary.write(row, payload)
    }
    fn write_aton(&mut self, row: AtonRow, payload: &str) -> Result<()> {
        self.atons.write(row, payload)
    }
    fn write_other(&mut self, _row: OtherRow) -> Result<()> {
        Ok(())
    }
}

struct IcebergPartitionOutput {
    positions: Vec<RecordBatch>,
    statics: Vec<RecordBatch>,
    meteo: Vec<RecordBatch>,
    binary: Vec<RecordBatch>,
    atons: Vec<RecordBatch>,
}

#[allow(clippy::too_many_arguments)]
fn process_partition_iceberg(
    _partition_key: PartitionKey,
    files: Vec<DatasetFile>,
    batch_size: usize,
    consolidate_ais: bool,
    process_timestamps: bool,
) -> Result<(ParseStats, IcebergPartitionOutput)> {
    let mut stats = ParseStats::default();
    let mut writers = IcebergWriters {
        positions: IcebergPositionsWriter::new(),
        statics: IcebergStaticsWriter::new(),
        meteo: IcebergMeteoWriter::new(),
        binary: IcebergBinaryWriter::new(),
        atons: IcebergAtonWriter::new(),
    };

    let mut seen: HashSet<DedupKey> = HashSet::new();

    for file in &files {
        process_parquet_file(
            &file.path,
            &file.partition.source,
            &mut stats,
            &mut writers,
            batch_size,
            &mut seen,
            consolidate_ais,
            process_timestamps,
        )
        .with_context(|| format!("processing {}", file.path.display()))?;
    }
    stats.partitions_processed += 1;

    Ok((
        stats,
        IcebergPartitionOutput {
            positions: writers.positions.finish()?,
            statics: writers.statics.finish()?,
            meteo: writers.meteo.finish()?,
            binary: writers.binary.finish()?,
            atons: writers.atons.finish()?,
        },
    ))
}

fn process_parquet_file<W: WriterSet>(
    path: &Path,
    path_source: &str,
    stats: &mut ParseStats,
    writers: &mut W,
    batch_size: usize,
    seen: &mut HashSet<DedupKey>,
    consolidate_ais: bool,
    process_timestamps: bool,
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
    let mut consolidator = if consolidate_ais || process_timestamps {
        Some(AisConsolidator::new(AisConsolidatorConfig {
            process_timestamps,
            consolidate_multipart: consolidate_ais,
        }))
    } else {
        None
    };

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

        let n = batch.num_rows();

        let sources: Vec<&str> = (0..n)
            .map(|i| match &source_col {
                Some(col) if !col.is_null(i) => col.value(i),
                _ => path_source,
            })
            .collect();

        let (decoded, payloads): (Vec<(usize, Decoded)>, Vec<String>) = if let Some(c) =
            &mut consolidator
        {
            let mut rows: Vec<(i64, String, String)> = Vec::new();
            for i in 0..n {
                let ts = ts_col.value(i);
                let src = sources[i];
                let raw = payload_col.value(i);
                for (new_ts, new_payload) in c.transform(raw, ts) {
                    rows.push((new_ts, src.to_string(), new_payload));
                }
            }
            let payloads: Vec<String> = rows.iter().map(|(_, _, p)| p.clone()).collect();
            let decoded: Vec<(usize, Decoded)> = rows
                .into_par_iter()
                .enumerate()
                .map(|(idx, (ts, src, payload))| (idx, decode_payload(ts, &src, &payload)))
                .collect();
            (decoded, payloads)
        } else {
            let payloads: Vec<String> = (0..n).map(|i| payload_col.value(i).to_string()).collect();
            let decoded: Vec<(usize, Decoded)> = (0..n)
                .into_par_iter()
                .map(|i| {
                    (
                        i,
                        decode_payload(ts_col.value(i), sources[i], payload_col.value(i)),
                    )
                })
                .collect();
            (decoded, payloads)
        };

        stats.rows_in += n as u64;
        for (i, (_, result)) in decoded.into_iter().enumerate() {
            match result {
                Decoded::Position(row) => {
                    let key = DedupKey(0, row.ts_ms, row.mmsi, 0, row.msg_type);
                    if seen.insert(key) {
                        stats.positions_out += 1;
                        writers.write_position(&row, &payloads[i])?;
                    } else {
                        stats.rows_deduped += 1;
                    }
                }
                Decoded::Static(row) => {
                    let key = DedupKey(1, row.ts_ms, row.mmsi, 0, 0);
                    if seen.insert(key) {
                        stats.statics_out += 1;
                        writers.write_static(&row, &payloads[i])?;
                    } else {
                        stats.rows_deduped += 1;
                    }
                }
                Decoded::Meteo(row) => {
                    let key = DedupKey(
                        2,
                        row.ts_ms,
                        row.mmsi,
                        ((row.dac as u32) << 8) | row.fid as u32,
                        0,
                    );
                    if seen.insert(key) {
                        stats.meteo_out += 1;
                        writers.write_meteo(*row, &payloads[i])?;
                    } else {
                        stats.rows_deduped += 1;
                    }
                }
                Decoded::Binary(row) => {
                    let key = DedupKey(
                        3,
                        row.ts_ms,
                        row.mmsi,
                        ((row.dac as u32) << 8) | row.fid as u32,
                        0,
                    );
                    if seen.insert(key) {
                        stats.binary_out += 1;
                        writers.write_binary(*row, &payloads[i])?;
                    } else {
                        stats.rows_deduped += 1;
                    }
                }
                Decoded::Aton(row) => {
                    let key = DedupKey(4, row.ts_ms, row.mmsi, 0, row.msg_type);
                    if seen.insert(key) {
                        stats.atons_out += 1;
                        writers.write_aton(*row, &payloads[i])?;
                    } else {
                        stats.rows_deduped += 1;
                    }
                }
                Decoded::Other(row) => {
                    stats.others_out += 1;
                    writers.write_other(*row)?;
                }
                Decoded::Incomplete => stats.incomplete += 1,
                Decoded::Failed => stats.failed += 1,
            }
        }
    }

    // Flush any buffered AIS fragments at end-of-file.
    if let Some(c) = &mut consolidator {
        for (ts, payload) in c.flush() {
            match decode_payload(ts, path_source, &payload) {
                Decoded::Position(row) => {
                    let key = DedupKey(0, row.ts_ms, row.mmsi, 0, row.msg_type);
                    if seen.insert(key) {
                        stats.positions_out += 1;
                        writers.write_position(&row, &payload)?;
                    } else {
                        stats.rows_deduped += 1;
                    }
                }
                Decoded::Static(row) => {
                    let key = DedupKey(1, row.ts_ms, row.mmsi, 0, 0);
                    if seen.insert(key) {
                        stats.statics_out += 1;
                        writers.write_static(&row, &payload)?;
                    } else {
                        stats.rows_deduped += 1;
                    }
                }
                Decoded::Meteo(row) => {
                    let key = DedupKey(
                        2,
                        row.ts_ms,
                        row.mmsi,
                        ((row.dac as u32) << 8) | row.fid as u32,
                        0,
                    );
                    if seen.insert(key) {
                        stats.meteo_out += 1;
                        writers.write_meteo(*row, &payload)?;
                    } else {
                        stats.rows_deduped += 1;
                    }
                }
                Decoded::Binary(row) => {
                    let key = DedupKey(
                        3,
                        row.ts_ms,
                        row.mmsi,
                        ((row.dac as u32) << 8) | row.fid as u32,
                        0,
                    );
                    if seen.insert(key) {
                        stats.binary_out += 1;
                        writers.write_binary(*row, &payload)?;
                    } else {
                        stats.rows_deduped += 1;
                    }
                }
                Decoded::Aton(row) => {
                    let key = DedupKey(4, row.ts_ms, row.mmsi, 0, row.msg_type);
                    if seen.insert(key) {
                        stats.atons_out += 1;
                        writers.write_aton(*row, &payload)?;
                    } else {
                        stats.rows_deduped += 1;
                    }
                }
                Decoded::Other(row) => {
                    stats.others_out += 1;
                    writers.write_other(*row)?;
                }
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
