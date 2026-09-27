use anyhow::{bail, Context, Result};
use arrow::array::{
    ArrayBuilder, StringBuilder, TimestampMillisecondArray, TimestampMillisecondBuilder,
};
use arrow::compute::{sort_to_indices, take, SortOptions};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client as S3Client, Config as S3Config};
use chrono::{Datelike, TimeZone, Timelike, Utc};
use clap::{Args, ValueEnum};
use futures_util::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use std::collections::VecDeque;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::codec::{FramedRead, LinesCodec};

pub mod ais_consolidate;
pub mod backoff;
pub mod dataset;
pub mod exitcode;
pub mod iceberg;
pub mod iceberg_commit_manifest;
pub mod log;
mod metrics;
pub mod silver;
pub mod state;

pub use async_trait::async_trait;
pub use metrics::{HealthState, IngestMetrics};
pub use tokio_util::codec::LinesCodecError;

const DEFAULT_OUT_DIR: &str = "data";
const DEFAULT_UPLOAD_DRAIN_TIMEOUT_SECONDS: u64 = 60;
const DEFAULT_MAX_LINE_LENGTH: usize = 65_536;
const DEFAULT_MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
/// `pub` so the batch parsers (`ais-parse`/`aisstream-parse`), which don't
/// flatten `CommonCliArgs` and so declare their own `--compression-level`
/// with a concrete `default_value_t`, reference the same constant instead of
/// a duplicated literal `5`.
pub const DEFAULT_COMPRESSION_LEVEL: i32 = 5;
const DEFAULT_S3_REGION: &str = "us-east-1";
const DEFAULT_HEALTH_STALE_WINDOW_SECONDS: u64 = 60;
/// On by default (see Phase 3b of the reliability hardening plan): a
/// collector that has connected fine but received nothing in 5 minutes is
/// almost always stuck or pointed at the wrong upstream, not legitimately
/// idle — an operator should be told rather than have the process sit
/// silently. `--data-drought-seconds 0` disables the check for a feed that
/// really is this quiet.
const DEFAULT_DATA_DROUGHT_SECONDS: u64 = 300;
const DEFAULT_UPLOAD_QUEUE_CAPACITY: usize = 128;
const DEFAULT_UPLOAD_CONCURRENCY: usize = 4;
const DEFAULT_WRITE_QUEUE_CAPACITY: usize = 64;
const DEFAULT_UPLOAD_RETRIES: usize = 3;
const HEALTH_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
static PARQUET_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, ValueEnum)]
#[value(rename_all = "lower")]
pub enum PartitionGranularity {
    Minute,
    Hour,
    Day,
    Month,
    Year,
}

impl Default for PartitionGranularity {
    fn default() -> Self {
        Self::Day
    }
}

impl PartitionGranularity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
            Self::Year => "year",
        }
    }

    pub const fn depth(self) -> usize {
        match self {
            Self::Year => 1,
            Self::Month => 2,
            Self::Day => 3,
            Self::Hour => 4,
            Self::Minute => 5,
        }
    }

    pub fn components_from_timestamp(self, timestamp_ms: i64) -> (i32, u32, u32, u32, u32) {
        let timestamp_ms = timestamp_ms.max(0);
        let dt = Utc
            .timestamp_millis_opt(timestamp_ms)
            .single()
            .unwrap_or_else(|| {
                Utc.timestamp_opt(0, 0)
                    .single()
                    .expect("unix epoch should be valid")
            });

        match self {
            Self::Minute => (dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute()),
            Self::Hour => (dt.year(), dt.month(), dt.day(), dt.hour(), 0),
            Self::Day => (dt.year(), dt.month(), dt.day(), 0, 0),
            Self::Month => (dt.year(), dt.month(), 1, 0, 0),
            Self::Year => (dt.year(), 1, 1, 0, 0),
        }
    }

    /// Millisecond bounds `[start, end)` of the partition period containing
    /// `timestamp_ms`. Negative timestamps clamp to the epoch, matching
    /// `components_from_timestamp`.
    pub fn period_bounds_ms(self, timestamp_ms: i64) -> (i64, i64) {
        let (year, month, day, hour, minute) = self.components_from_timestamp(timestamp_ms);
        let start = Utc
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap_or_else(|| {
                Utc.timestamp_opt(0, 0)
                    .single()
                    .expect("unix epoch should be valid")
            });
        let end = match self {
            Self::Minute => start + chrono::Duration::minutes(1),
            Self::Hour => start + chrono::Duration::hours(1),
            Self::Day => start + chrono::Duration::days(1),
            Self::Month => start
                .checked_add_months(chrono::Months::new(1))
                .unwrap_or(start + chrono::Duration::days(31)),
            Self::Year => start
                .checked_add_months(chrono::Months::new(12))
                .unwrap_or(start + chrono::Duration::days(366)),
        };
        (start.timestamp_millis(), end.timestamp_millis())
    }
}

impl std::fmt::Display for PartitionGranularity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PartitionGranularity {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "minute" => Ok(Self::Minute),
            "hour" => Ok(Self::Hour),
            "day" => Ok(Self::Day),
            "month" => Ok(Self::Month),
            "year" => Ok(Self::Year),
            _ => Err(format!(
                "invalid partition granularity: {value} (expected minute, hour, day, month, or year)"
            )),
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct CommonCliArgs {
    /// Output root directory
    #[arg(long = "output-dir", env = "OUTPUT_DIR", default_value = DEFAULT_OUT_DIR)]
    pub out_dir: PathBuf,

    /// Partition granularity for dataset layout
    #[arg(long, env = "PARTITION", default_value_t = PartitionGranularity::Day)]
    pub partition: PartitionGranularity,

    /// Max rows to buffer per Parquet file before flush (default: flush on the selected partition boundary)
    #[arg(long, env = "MAX_ROWS")]
    pub max_rows: Option<usize>,

    /// Max payload bytes to buffer per Parquet file before flush
    #[arg(long, env = "MAX_BATCH_BYTES")]
    pub max_batch_bytes: Option<usize>,

    /// Zstd compression level for Parquet output (default: 5)
    #[arg(long, env = "COMPRESSION_LEVEL")]
    pub compression_level: Option<i32>,

    /// Max concurrent S3 uploads
    #[arg(long, env = "UPLOAD_CONCURRENCY")]
    pub upload_concurrency: Option<usize>,

    /// Seconds to wait for background uploads on shutdown
    #[arg(long, env = "UPLOAD_DRAIN_TIMEOUT_SECONDS", default_value_t = DEFAULT_UPLOAD_DRAIN_TIMEOUT_SECONDS)]
    pub upload_drain_timeout_seconds: u64,

    /// Maximum bytes allowed per input line before dropping it
    #[arg(long, env = "MAX_LINE_LENGTH", default_value_t = DEFAULT_MAX_LINE_LENGTH)]
    pub max_line_length: usize,

    /// Run health check and exit
    #[arg(long, env = "HEALTH_CHECK", value_parser = clap::builder::FalseyValueParser::new())]
    pub health_check: bool,

    /// Serve Prometheus metrics and /healthz on this address, e.g. 0.0.0.0:9184
    #[arg(long, env = "METRICS_ADDR")]
    pub metrics_addr: Option<String>,

    /// Exit if no row has been ingested for this many seconds, even though
    /// the upstream connection appears fine (0 disables the check)
    #[arg(long, env = "DATA_DROUGHT_SECONDS", default_value_t = DEFAULT_DATA_DROUGHT_SECONDS)]
    pub data_drought_seconds: u64,
}

#[derive(Clone, Debug, Args)]
pub struct S3CliArgs {
    /// S3 bucket name for remote storage (enables S3 upload).  You may include
    /// a key prefix, e.g. `bronze/norway` — the first path segment becomes the
    /// bucket and the remainder is used as `s3_prefix`.
    #[arg(long, env = "S3_BUCKET")]
    pub s3_bucket: Option<String>,

    /// Optional S3 key prefix prepended to every upload key (used alongside
    /// `--s3-bucket`, or contributed by `bucket/path` syntax).
    #[arg(long, env = "S3_PREFIX")]
    pub s3_prefix: Option<String>,

    /// S3 endpoint URL (for MinIO or custom S3-compatible storage)
    #[arg(long, env = "S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    /// S3 region (default: us-east-1)
    #[arg(long, env = "S3_REGION", default_value = DEFAULT_S3_REGION)]
    pub s3_region: String,

    /// S3 access key ID (can also use AWS_ACCESS_KEY_ID env var)
    #[arg(long, env = "S3_ACCESS_KEY")]
    pub s3_access_key: Option<String>,

    /// S3 secret access key (can also use AWS_SECRET_ACCESS_KEY env var)
    #[arg(long, env = "S3_SECRET_KEY", hide_env_values = true)]
    pub s3_secret_key: Option<String>,

    /// Keep local files after S3 upload (default: delete after successful upload)
    #[arg(long, env = "KEEP_LOCAL", value_parser = clap::builder::FalseyValueParser::new())]
    pub keep_local: bool,

    /// Disable TLS/HTTPS for S3 endpoint (use plain HTTP instead)
    #[arg(long, env = "S3_DISABLE_TLS", value_parser = clap::builder::FalseyValueParser::new())]
    pub s3_disable_tls: bool,
}

#[derive(Clone, Debug, Args)]
pub struct S3ConnectionArgs {
    /// S3 endpoint URL (for MinIO or custom S3-compatible storage)
    #[arg(long, env = "S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    /// S3 region (default: us-east-1)
    #[arg(long, env = "S3_REGION", default_value = DEFAULT_S3_REGION)]
    pub s3_region: String,

    /// S3 access key ID (can also use AWS_ACCESS_KEY_ID env var)
    #[arg(long, env = "S3_ACCESS_KEY")]
    pub s3_access_key: Option<String>,

    /// S3 secret access key (can also use AWS_SECRET_ACCESS_KEY env var)
    #[arg(long, env = "S3_SECRET_KEY", hide_env_values = true)]
    pub s3_secret_key: Option<String>,

    /// Disable TLS/HTTPS for S3 endpoint (use plain HTTP instead)
    #[arg(long, env = "S3_DISABLE_TLS", value_parser = clap::builder::FalseyValueParser::new())]
    pub s3_disable_tls: bool,
}

#[derive(Clone, Debug)]
pub struct CommonOptions {
    pub out_dir: PathBuf,
    pub partition: PartitionGranularity,
    pub max_rows: Option<usize>,
    pub max_batch_bytes: usize,
    pub compression_level: i32,
    pub upload_concurrency: usize,
    pub upload_drain_timeout_seconds: u64,
    pub max_line_length: usize,
    pub health_check: bool,
    pub metrics_addr: Option<String>,
    pub data_drought_seconds: u64,
}

#[derive(Clone, Debug)]
pub struct S3Options {
    pub bucket: String,
    pub s3_prefix: String,
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub keep_local: bool,
    pub disable_tls: bool,
}

pub struct IngestOptions {
    pub common: CommonOptions,
    pub s3: Option<S3Options>,
    pub s3_storage: Option<S3Storage>,
    /// When set, each successfully-uploaded batch is registered as a row in
    /// the Iceberg `raw` table (see `crate::iceberg::register_raw_upload`).
    pub iceberg: Option<iceberg::IcebergHandle>,
    pub health_file: PathBuf,
    pub manage_health: bool,
    pub report_progress: bool,
    pub log_writes: bool,
    pub shutdown: Option<Arc<AtomicBool>>,
    /// Number of parquet write workers; defaults to a per-process heuristic.
    /// Callers running many ingests concurrently (e.g. collect-file's
    /// parallel mode) should set this to 1 to avoid multiplying task counts.
    pub write_workers: Option<usize>,
    /// Scan `out_dir` at startup for parquet files a previous run wrote but
    /// never uploaded, and upload them in the background. Only applies when
    /// S3 is configured without keep_local. Callers running many ingests
    /// over one out_dir (collect-file's parallel mode) should sweep once
    /// themselves and set this to false.
    pub sweep_orphans: bool,

    /// Optional line transformer applied to every line before it enters the
    /// batch buffer. The transformer can buffer, combine, or rewrite lines
    /// (e.g. for AIS multipart message reassembly with `AisConsolidator`).
    /// Cloned instances lose the transformer (stateful, not shared).
    pub line_transformer: Option<Box<dyn LineTransformer>>,

    /// Optional inline silver parser (`--parser`). When set, every sealed
    /// bronze batch is additionally decoded into the six typed silver tables
    /// after the bronze Parquet file is durable on disk. Shared by `Arc`
    /// across write workers (implementations are stateless per batch).
    /// Cloned — unlike `line_transformer`, this is shareable.
    pub silver: Option<Arc<dyn silver::SilverCommit>>,
}

// Manual Clone + Debug: skip line_transformer (stateful — not shareable or
// introspectable).
impl Clone for IngestOptions {
    fn clone(&self) -> Self {
        IngestOptions {
            line_transformer: None,
            silver: self.silver.clone(),
            common: self.common.clone(),
            s3: self.s3.clone(),
            s3_storage: self.s3_storage.clone(),
            iceberg: self.iceberg.clone(),
            health_file: self.health_file.clone(),
            manage_health: self.manage_health,
            report_progress: self.report_progress,
            log_writes: self.log_writes,
            shutdown: self.shutdown.clone(),
            write_workers: self.write_workers,
            sweep_orphans: self.sweep_orphans,
        }
    }
}

impl std::fmt::Debug for IngestOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestOptions")
            .field("common", &self.common)
            .field("s3", &self.s3)
            .field("s3_storage", &self.s3_storage)
            .field("iceberg", &self.iceberg)
            .field("health_file", &self.health_file)
            .field("manage_health", &self.manage_health)
            .field("report_progress", &self.report_progress)
            .field("log_writes", &self.log_writes)
            .field("shutdown", &self.shutdown)
            .field("write_workers", &self.write_workers)
            .field("sweep_orphans", &self.sweep_orphans)
            .field(
                "line_transformer",
                &self.line_transformer.as_ref().map(|_| "(set)"),
            )
            .field("silver", &self.silver.as_ref().map(|s| s.describe()))
            .finish()
    }
}

pub type LineReader = FramedRead<Box<dyn AsyncRead + Unpin + Send>, LinesCodec>;

#[derive(Clone, Debug)]
pub struct IngestProgress {
    pub current_input: String,
    pub current_input_index: usize,
    pub input_total: usize,
}

pub enum ReaderTransition {
    Continue(LineReader),
    Stop,
}

#[async_trait]
pub trait LineSource {
    fn source_name(&self) -> &str;

    fn timestamp_for_payload(&mut self, _payload: &str) -> Option<i64> {
        None
    }

    fn normalize_payload(&mut self, payload: String, _timestamp_ms: i64) -> String {
        payload
    }

    /// Called after a batch has been sealed and queued for writing. Every line
    /// previously delivered to this source belongs to some batch with a
    /// sequence number `<= seq` (sequence numbers start at 1). Note that the
    /// line currently being processed may or may not be part of the sealed
    /// batch, so sources tracking consumption progress should be conservative
    /// about the most recent line.
    fn on_batch_sealed(&mut self, _seq: u64) {}

    /// Called once the batch with this sequence number has been durably
    /// written to local disk (written and renamed into place). Notifications
    /// may arrive out of order when multiple write workers are active.
    /// Sources that track upstream progress (e.g. Kafka offsets) should only
    /// advance through contiguous durable sequence numbers.
    fn on_batch_durable(&mut self, _seq: u64) {}

    fn ingest_progress(&self) -> Option<IngestProgress> {
        None
    }

    async fn open(&mut self, max_line_length: usize) -> Result<LineReader>;

    async fn on_stream_end(
        &mut self,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        let _ = (shutdown, max_line_length);
        Ok(ReaderTransition::Stop)
    }

    async fn on_stream_error(
        &mut self,
        error: &LinesCodecError,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        let _ = (shutdown, max_line_length);
        Err(anyhow::anyhow!(error.to_string()))
    }
}

/// Transforms raw input lines before they are written to the Parquet batch.
/// Useful for AIS multipart message reassembly, payload filtering, etc.
/// Each call to `transform` receives the raw line and its arrival timestamp;
/// returns zero or more (timestamp, payload) pairs to write. `flush` is
/// called on partition rollover and shutdown to emit buffered state.
pub trait LineTransformer: Send {
    fn transform(&mut self, line: &str, arrival_ts_ms: i64) -> Vec<(i64, String)>;
    fn flush(&mut self) -> Vec<(i64, String)> {
        Vec::new()
    }
}

impl CommonCliArgs {
    pub fn to_options(&self) -> CommonOptions {
        CommonOptions {
            out_dir: self.out_dir.clone(),
            partition: self.partition,
            max_rows: self.max_rows,
            max_batch_bytes: self.max_batch_bytes.unwrap_or(DEFAULT_MAX_BATCH_BYTES),
            compression_level: self.compression_level.unwrap_or(DEFAULT_COMPRESSION_LEVEL),
            upload_concurrency: self.upload_concurrency.unwrap_or(DEFAULT_UPLOAD_CONCURRENCY),
            upload_drain_timeout_seconds: self.upload_drain_timeout_seconds,
            max_line_length: self.max_line_length,
            health_check: self.health_check,
            metrics_addr: self.metrics_addr.clone(),
            data_drought_seconds: self.data_drought_seconds,
        }
    }
}

impl S3CliArgs {
    pub fn to_options(&self) -> Option<S3Options> {
        let bucket_raw = self.s3_bucket.clone()?;
        let (bucket, path_prefix) = S3Storage::split_s3_path(&bucket_raw, "");
        let s3_prefix = match (&self.s3_prefix, path_prefix.is_empty()) {
            (Some(explicit), false) => format!("{}/{}", path_prefix, explicit.trim_matches('/')),
            (Some(explicit), true) => explicit.clone(),
            (None, false) => path_prefix,
            (None, true) => String::new(),
        };

        Some(S3Options {
            bucket,
            s3_prefix,
            endpoint: self.s3_endpoint.clone(),
            region: self.s3_region.clone(),
            access_key: self.s3_access_key.clone(),
            secret_key: self.s3_secret_key.clone(),
            keep_local: self.keep_local,
            disable_tls: self.s3_disable_tls,
        })
    }
}

impl S3Options {
    pub async fn into_storage(self) -> Result<S3Storage> {
        S3Storage::new(
            self.bucket,
            self.s3_prefix,
            self.region,
            self.endpoint,
            self.access_key,
            self.secret_key,
            self.keep_local,
            self.disable_tls,
        )
        .await
    }
}

pub fn health_file_path(app_name: &str) -> PathBuf {
    PathBuf::from(format!("/tmp/{}.health", app_name))
}

/// Write shell completions for `command` to stdout and return. Call this
/// right after parsing `--completions <shell>` and exit before doing any
/// other work.
pub fn print_completions<C: clap::CommandFactory>(shell: clap_complete::Shell, bin_name: &str) {
    clap_complete::generate(shell, &mut C::command(), bin_name, &mut std::io::stdout());
}

/// Load a flat TOML config file and set each key as a process environment
/// variable, for any key not already present in the environment — so an
/// explicit CLI flag or a pre-set env var always wins over the file.
///
/// Keys are expected to be the same SCREAMING_SNAKE names used by `--flag
/// env = "..."` throughout this workspace (e.g. `S3_BUCKET`, `OUTPUT_DIR`).
/// String, integer, float, and boolean values are converted with their
/// natural `Display`; a string array is joined with `,` to match the
/// comma-separated convention used by repeatable flags like
/// `INPUT_S3_BUCKET`. Nested tables are rejected (config files here are
/// intentionally flat, one file per binary invocation).
///
/// Call this once, immediately after parsing just enough to know the
/// `--config` path was given, and then re-parse `Args` so clap picks up the
/// newly-set environment variables with correct precedence.
pub fn apply_config_file(path: &Path) -> Result<()> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file {}", path.display()))?;
    let table: toml::Table = contents
        .parse()
        .with_context(|| format!("parsing config file {} as TOML", path.display()))?;

    for (key, value) in &table {
        if std::env::var(key).is_ok() {
            continue;
        }
        let rendered = match value {
            toml::Value::String(s) => s.clone(),
            toml::Value::Integer(i) => i.to_string(),
            toml::Value::Float(f) => f.to_string(),
            toml::Value::Boolean(b) => b.to_string(),
            toml::Value::Array(items) => {
                let mut parts = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        toml::Value::String(s) => parts.push(s.clone()),
                        toml::Value::Integer(i) => parts.push(i.to_string()),
                        other => bail!(
                            "config file {}: key '{key}' has an array element of unsupported \
                             type {other:?}; use strings or integers",
                            path.display()
                        ),
                    }
                }
                parts.join(",")
            }
            toml::Value::Datetime(dt) => dt.to_string(),
            toml::Value::Table(_) => bail!(
                "config file {}: key '{key}' is a nested table; config files here must be a \
                 flat list of KEY = value pairs matching the env var names shown in --help",
                path.display()
            ),
        };
        std::env::set_var(key, rendered);
    }

    Ok(())
}

pub fn default_source_from_path(path: &Path) -> String {
    path.file_stem()
        .or_else(|| path.file_name())
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("file")
        .to_string()
}

pub fn format_count(value: usize) -> String {
    let raw = value.to_string();
    let mut grouped = String::with_capacity(raw.len() + raw.len() / 3);

    for (index, ch) in raw.chars().rev().enumerate() {
        if index != 0 && index % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }

    grouped.chars().rev().collect()
}

fn validate_source_name(source: &str) -> Result<()> {
    if source.is_empty() {
        bail!("source name must not be empty");
    }
    if source.contains(['/', '\\', '\0']) {
        bail!("source name must be a single path component: {source:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{format_count, PartKey, PartitionGranularity};
    use chrono::{TimeZone, Utc};

    #[test]
    fn formats_counts_with_thousands_separators() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(1_234), "1,234");
        assert_eq!(format_count(12_345_678), "12,345,678");
    }

    #[test]
    fn source_names_cannot_escape_the_output_directory() {
        assert!(super::validate_source_name("norway-tcp").is_ok());
        assert!(super::validate_source_name("").is_err());
        assert!(super::validate_source_name("../outside").is_err());
        assert!(super::validate_source_name("nested/source").is_err());
        assert!(super::validate_source_name("nested\\source").is_err());
    }

    #[test]
    fn truncates_partition_keys_by_granularity() {
        let ts_one = Utc
            .with_ymd_and_hms(2024, 2, 3, 4, 5, 6)
            .single()
            .expect("valid timestamp")
            .timestamp_millis();
        let ts_two = Utc
            .with_ymd_and_hms(2024, 2, 3, 5, 59, 59)
            .single()
            .expect("valid timestamp")
            .timestamp_millis();

        let day_one = PartKey::from_timestamp("source", ts_one, PartitionGranularity::Day);
        let day_two = PartKey::from_timestamp("source", ts_two, PartitionGranularity::Day);
        assert_eq!(day_one, day_two);
        assert_eq!(
            day_one.relative_dir(),
            "source=source/year=2024/month=02/day=03"
        );

        let hour_one = PartKey::from_timestamp("source", ts_one, PartitionGranularity::Hour);
        let hour_two = PartKey::from_timestamp("source", ts_two, PartitionGranularity::Hour);
        assert_ne!(hour_one, hour_two);
        assert_eq!(
            hour_one.relative_dir(),
            "source=source/year=2024/month=02/day=03/hour=04"
        );

        let month = PartKey::from_timestamp("source", ts_one, PartitionGranularity::Month);
        assert_eq!(month.relative_dir(), "source=source/year=2024/month=02");

        let year = PartKey::from_timestamp("source", ts_one, PartitionGranularity::Year);
        assert_eq!(year.relative_dir(), "source=source/year=2024");
    }

    #[test]
    fn period_bounds_cover_their_timestamp() {
        let ts = Utc
            .with_ymd_and_hms(2024, 2, 29, 23, 59, 58)
            .single()
            .expect("valid timestamp")
            .timestamp_millis();

        for granularity in [
            PartitionGranularity::Minute,
            PartitionGranularity::Hour,
            PartitionGranularity::Day,
            PartitionGranularity::Month,
            PartitionGranularity::Year,
        ] {
            let (start, end) = granularity.period_bounds_ms(ts);
            assert!(
                start <= ts && ts < end,
                "{granularity}: {start}..{end} vs {ts}"
            );
            // A timestamp just past the end must map to a different partition.
            let next = PartKey::from_timestamp("s", end, granularity);
            let current = PartKey::from_timestamp("s", ts, granularity);
            assert_ne!(next, current, "{granularity}");
        }

        let (day_start, day_end) = PartitionGranularity::Day.period_bounds_ms(ts);
        assert_eq!(day_end - day_start, 24 * 3600 * 1000);
    }

    fn count_parquet_rows(dir: &std::path::Path) -> anyhow::Result<i64> {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let mut total = 0;
        let mut stack = vec![dir.to_path_buf()];
        while let Some(current) = stack.pop() {
            for entry in std::fs::read_dir(&current)? {
                let path = entry?.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    let reader = SerializedFileReader::new(std::fs::File::open(&path)?)?;
                    total += reader.metadata().file_metadata().num_rows();
                }
            }
        }
        Ok(total)
    }

    #[tokio::test]
    async fn shutdown_flushes_buffered_rows() -> anyhow::Result<()> {
        use super::{
            async_trait, line_reader_from_async_read, run_ingest, CommonOptions, IngestOptions,
            LineReader, LineSource,
        };
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        // Emits three lines but raises the shutdown flag while the second is
        // being processed, mimicking a SIGTERM landing mid-stream with rows
        // still buffered in memory.
        struct ShutdownMidStream {
            shutdown: Arc<AtomicBool>,
            seen: usize,
        }

        #[async_trait]
        impl LineSource for ShutdownMidStream {
            fn source_name(&self) -> &str {
                "shutdown-test"
            }

            fn normalize_payload(&mut self, payload: String, _timestamp_ms: i64) -> String {
                self.seen += 1;
                if self.seen == 2 {
                    self.shutdown.store(true, Ordering::SeqCst);
                }
                payload
            }

            async fn open(&mut self, max_line_length: usize) -> anyhow::Result<LineReader> {
                Ok(line_reader_from_async_read(
                    std::io::Cursor::new(b"line-1\nline-2\nline-3\n".to_vec()),
                    max_line_length,
                ))
            }
        }

        let dir = tempfile::tempdir()?;
        let out_dir = dir.path().join("out");
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut source = ShutdownMidStream {
            shutdown: shutdown.clone(),
            seen: 0,
        };

        run_ingest(
            &mut source,
            IngestOptions {
                common: CommonOptions {
                    out_dir: out_dir.clone(),
                    partition: PartitionGranularity::Year,
                    max_rows: None,
                    max_batch_bytes: 64 * 1024 * 1024,
                    compression_level: 3,
                    upload_concurrency: 4,
                    upload_drain_timeout_seconds: 1,
                    max_line_length: 1024,
                    health_check: false,
                    metrics_addr: None,
                    data_drought_seconds: 0,
                },
                s3: None,
                s3_storage: None,
                iceberg: None,
                health_file: dir.path().join("health"),
                manage_health: false,
                report_progress: false,
                log_writes: false,
                shutdown: Some(shutdown),
                write_workers: None,
                sweep_orphans: false,
                line_transformer: None,
                silver: None,
            },
        )
        .await?;

        assert_eq!(
            count_parquet_rows(&out_dir)?,
            2,
            "rows buffered when shutdown was requested must be flushed to disk"
        );
        Ok(())
    }

    #[test]
    fn finds_only_hive_layout_parquet_orphans() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let out_dir = dir.path().join("out");
        let partition_dir = out_dir.join("source=ais/year=2024/month=01/day=15");
        std::fs::create_dir_all(&partition_dir)?;

        std::fs::write(partition_dir.join("part-1.parquet"), b"data")?;
        std::fs::write(partition_dir.join("part-2.parquet.tmp"), b"incomplete")?;
        std::fs::write(out_dir.join("stray.parquet"), b"outside layout")?;
        let stray_claim = out_dir.join("stray.parquet.uploading");
        std::fs::write(&stray_claim, b"not a collector claim")?;
        std::fs::write(out_dir.join(".collect-file-completed"), b"manifest")?;

        let orphans = super::find_orphaned_uploads(&out_dir)?;
        assert_eq!(orphans.len(), 1, "only the finished hive-layout file");
        assert_eq!(
            orphans[0].1,
            "source=ais/year=2024/month=01/day=15/part-1.parquet"
        );
        assert_eq!(orphans[0].0, partition_dir.join("part-1.parquet"));
        assert!(
            stray_claim.is_file(),
            "claims outside Hive layout are ignored"
        );

        // Missing out_dir is a clean no-op (first run on an empty node).
        let none = super::find_orphaned_uploads(&dir.path().join("missing"))?;
        assert!(none.is_empty());
        Ok(())
    }

    #[test]
    fn removes_empty_partition_ancestors_but_preserves_output_root() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let out_dir = dir.path().join("out");
        let minute_dir = out_dir.join("source=ais/year=2025/month=12/day=31/hour=23/minute=59");
        std::fs::create_dir_all(&minute_dir)?;

        let pending = super::reconcile_partition_directories(&out_dir, &minute_dir)?;

        assert!(pending.is_empty());
        assert!(out_dir.is_dir(), "the output root must never be removed");
        assert!(
            !out_dir.join("source=ais").exists(),
            "the entire empty Hive directory chain should be removed"
        );
        Ok(())
    }

    #[test]
    fn discovers_pending_files_then_finishes_directory_cleanup() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let out_dir = dir.path().join("out");
        let minute_dir = out_dir.join("source=ais/year=2025/month=12/day=31/hour=23/minute=59");
        let empty_sibling = out_dir.join("source=ais/year=2025/month=11/day=01/hour=00/minute=00");
        std::fs::create_dir_all(&minute_dir)?;
        std::fs::create_dir_all(&empty_sibling)?;
        let parquet = minute_dir.join("part-1.parquet");
        std::fs::write(&parquet, b"complete")?;

        let pending = super::reconcile_partition_directories(&out_dir, &minute_dir)?;
        assert_eq!(
            pending,
            vec![(
                parquet.clone(),
                "source=ais/year=2025/month=12/day=31/hour=23/minute=59/part-1.parquet".to_string(),
            )]
        );
        assert!(empty_sibling.is_dir());

        std::fs::remove_file(parquet)?;
        let pending = super::reconcile_partition_directories(&out_dir, &minute_dir)?;
        assert!(pending.is_empty());
        assert!(out_dir.is_dir());
        assert!(!out_dir.join("source=ais/year=2025").exists());
        Ok(())
    }

    #[test]
    fn cleanup_preserves_temporary_and_unknown_files() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let out_dir = dir.path().join("out");
        let partition_dir = out_dir.join("source=ais/year=2025/month=12");
        std::fs::create_dir_all(&partition_dir)?;
        let temporary = partition_dir.join("part-1.parquet.tmp");
        let metadata = partition_dir.join("metadata.json");
        std::fs::write(&temporary, b"incomplete")?;
        std::fs::write(&metadata, b"{}")?;

        let pending = super::reconcile_partition_directories(&out_dir, &partition_dir)?;

        assert!(pending.is_empty());
        assert!(temporary.is_file());
        assert!(metadata.is_file());
        assert!(partition_dir.is_dir());
        Ok(())
    }

    #[test]
    fn upload_claims_are_exclusive_and_do_not_hide_orphans() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let out_dir = dir.path().join("out");
        let partition_dir = out_dir.join("source=ais/year=2025");
        std::fs::create_dir_all(&partition_dir)?;
        let parquet = partition_dir.join("part-1.parquet");
        std::fs::write(&parquet, b"complete")?;

        let claim = super::claim_upload(&parquet)?.expect("first owner should claim the file");
        assert!(
            super::claim_upload(&parquet)?.is_none(),
            "a second uploader must not acquire the same file"
        );

        let pending = super::find_orphaned_uploads(&out_dir)?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, parquet);
        drop(claim);
        assert!(
            super::claim_upload(&pending[0].0)?.is_some(),
            "the file should be claimable again after its owner exits"
        );
        Ok(())
    }

    #[test]
    fn refuses_to_clean_outside_the_output_root() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let out_dir = dir.path().join("out");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&out_dir)?;
        std::fs::create_dir_all(&outside)?;

        let error = super::reconcile_partition_directories(&out_dir, &outside)
            .expect_err("outside cleanup must be rejected");
        assert!(error.to_string().contains("outside output directory"));
        assert!(outside.is_dir());

        let traversing = out_dir.join("source=ais/../../outside");
        let error = super::reconcile_partition_directories(&out_dir, &traversing)
            .expect_err("parent traversal must be rejected");
        assert!(error.to_string().contains("outside output directory"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn refuses_symlinked_partition_directories() -> anyhow::Result<()> {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir()?;
        let out_dir = dir.path().join("out");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&out_dir)?;
        std::fs::create_dir_all(outside.join("year=2025"))?;
        symlink(&outside, out_dir.join("source=ais"))?;

        let linked_partition = out_dir.join("source=ais/year=2025");
        let error = super::reconcile_partition_directories(&out_dir, &linked_partition)
            .expect_err("symlinked partition cleanup must be rejected");
        assert!(error.to_string().contains("output symlink"));
        assert!(outside.join("year=2025").is_dir());
        Ok(())
    }

    #[tokio::test]
    async fn every_sealed_batch_is_reported_durable() -> anyhow::Result<()> {
        use super::{
            async_trait, line_reader_from_async_read, run_ingest, CommonOptions, IngestOptions,
            LineReader, LineSource,
        };

        struct RecordingSource {
            sealed: Vec<u64>,
            durable: Vec<u64>,
        }

        #[async_trait]
        impl LineSource for RecordingSource {
            fn source_name(&self) -> &str {
                "seal-test"
            }

            fn on_batch_sealed(&mut self, seq: u64) {
                self.sealed.push(seq);
            }

            fn on_batch_durable(&mut self, seq: u64) {
                self.durable.push(seq);
            }

            async fn open(&mut self, max_line_length: usize) -> anyhow::Result<LineReader> {
                let data: String = (0..6).map(|i| format!("line-{i}\n")).collect();
                Ok(line_reader_from_async_read(
                    std::io::Cursor::new(data.into_bytes()),
                    max_line_length,
                ))
            }
        }

        let dir = tempfile::tempdir()?;
        let mut source = RecordingSource {
            sealed: Vec::new(),
            durable: Vec::new(),
        };

        run_ingest(
            &mut source,
            IngestOptions {
                common: CommonOptions {
                    out_dir: dir.path().join("out"),
                    partition: PartitionGranularity::Year,
                    max_rows: Some(2), // 6 lines -> 3 sealed batches
                    max_batch_bytes: 64 * 1024 * 1024,
                    compression_level: 3,
                    upload_concurrency: 4,
                    upload_drain_timeout_seconds: 1,
                    max_line_length: 1024,
                    health_check: false,
                    metrics_addr: None,
                    data_drought_seconds: 0,
                },
                s3: None,
                s3_storage: None,
                iceberg: None,
                health_file: dir.path().join("health"),
                manage_health: false,
                report_progress: false,
                log_writes: false,
                shutdown: None,
                write_workers: None,
                sweep_orphans: false,
                line_transformer: None,
                silver: None,
            },
        )
        .await?;

        assert_eq!(source.sealed, vec![1, 2, 3], "seals arrive in order");
        let mut durable = source.durable.clone();
        durable.sort_unstable();
        assert_eq!(
            durable,
            vec![1, 2, 3],
            "every sealed batch must be reported durable before run_ingest returns"
        );
        Ok(())
    }
}

pub fn line_reader_from_async_read<R>(reader: R, max_line_length: usize) -> LineReader
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let stream: Box<dyn AsyncRead + Unpin + Send> = Box::new(reader);
    FramedRead::new(stream, LinesCodec::new_with_max_length(max_line_length))
}

pub async fn run_ingest<S>(source: &mut S, options: IngestOptions) -> Result<()>
where
    S: LineSource + Send,
{
    validate_source_name(source.source_name())?;

    let IngestOptions {
        common,
        s3,
        s3_storage,
        iceberg,
        health_file,
        manage_health,
        report_progress,
        log_writes,
        shutdown: external_shutdown,
        write_workers,
        sweep_orphans,
        mut line_transformer,
        silver,
    } = options;

    if common.health_check {
        check_health(&health_file)?;
        return Ok(());
    }
    if common.upload_concurrency == 0 {
        bail!("upload concurrency must be greater than zero");
    }

    if (s3_storage.is_some() || s3.is_some()) && silver.as_ref().is_some_and(|s| s.delete_local_after_commit()) {
        bail!(
            "--delete-after-iceberg cannot be combined with S3 upload; \
             uploaded files are already deleted locally after upload"
        );
    }

    let s3_storage = match (s3_storage, s3) {
        (Some(storage), _) => Some(storage),
        (None, Some(s3_options)) => Some(s3_options.into_storage().await?),
        (None, None) => None,
    };

    let metrics = Arc::new(IngestMetrics::default());
    metrics.touch_heartbeat();
    metrics.mark_started();
    metrics.set_drought_seconds(common.data_drought_seconds);
    let metrics_server = match &common.metrics_addr {
        Some(addr) => {
            match metrics::spawn_metrics_server(
                addr,
                source.source_name().to_string(),
                metrics.clone(),
            )
            .await
            {
                Ok((handle, local_addr)) => {
                    log::info(
                        "metrics_endpoint_listening",
                        &format!("metrics endpoint listening on http://{local_addr}/metrics"),
                        &[],
                    );
                    Some(handle)
                }
                Err(error) => {
                    // Never fail ingest because a metrics port is taken (e.g.
                    // several collect-file workers sharing one address).
                    log::warn(
                        "metrics_endpoint_disabled",
                        &format!("metrics endpoint disabled: {error}"),
                        &[],
                    );
                    None
                }
            }
        }
        None => None,
    };

    // Snapshot orphaned files before this run writes anything new. They enter
    // the tracked live upload queue below, so shutdown drains them as well.
    let startup_orphans = if sweep_orphans {
        if let Some(_storage) = s3_storage.clone().filter(|storage| !storage.keeps_local()) {
            let out_dir = common.out_dir.clone();
            match tokio::task::spawn_blocking(move || find_orphaned_uploads(&out_dir)).await {
                Ok(Ok(orphans)) if !orphans.is_empty() => {
                    eprintln!(
                        "♻️  Found {} orphaned parquet file(s) from a previous run; uploading in background",
                        format_count(orphans.len())
                    );
                    metrics
                        .orphan_files_swept
                        .fetch_add(orphans.len() as u64, Ordering::Relaxed);
                    orphans
                }
                Ok(Ok(_)) => Vec::new(),
                Ok(Err(error)) => {
                    eprintln!("⚠️  Orphan upload scan failed: {error}");
                    Vec::new()
                }
                Err(error) => {
                    eprintln!("⚠️  Orphan upload scan failed: {error}");
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let mut reader = source.open(common.max_line_length).await?;

    let shutdown = external_shutdown.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let shutdown_signal = shutdown.clone();
    let _shutdown_task = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(error) => {
                    eprintln!("Failed to register SIGTERM handler: {}", error);
                    return;
                }
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

        shutdown_signal.store(true, Ordering::SeqCst);
        eprintln!("Shutdown signal received. Flushing buffers and draining uploads...");
    });

    let now_ms = now_unix_duration().as_millis() as i64;
    let mut current_key = PartKey::from_timestamp(source.source_name(), now_ms, common.partition);
    // Cache the current partition's time window so the per-row check is two
    // integer comparisons instead of a PartKey allocation plus calendar math.
    let (mut key_start_ms, mut key_end_ms) = common.partition.period_bounds_ms(now_ms);
    let mut buf = BatchBuf::new(common.max_rows, common.max_batch_bytes);
    let mut rows_in_file = 0usize;
    let mut rows_processed = 0usize;
    let mut files_flushed = 0usize;
    let mut batches_sealed = 0u64;

    let (upload_tx, upload_worker) = spawn_upload_worker(
        s3_storage.clone(),
        metrics.clone(),
        common.upload_concurrency,
        common.out_dir.clone(),
        iceberg.clone(),
    );
    if let Some(upload_tx) = upload_tx.as_ref() {
        for orphan in startup_orphans {
            if upload_tx.send(orphan.into()).await.is_err() {
                eprintln!("⚠️  Upload queue closed while adding startup recovery files");
                break;
            }
        }
    }
    let (durable_tx, mut durable_rx) = mpsc::unbounded_channel::<u64>();
    // Bound queued batches by payload bytes, not job count, so memory stays
    // predictable regardless of batch size.
    let write_budget_bytes = common
        .max_batch_bytes
        .saturating_mul(4)
        .max(8 * 1024 * 1024);
    let (write_queue, write_worker) = spawn_write_worker(
        shutdown.clone(),
        upload_tx.clone(),
        durable_tx,
        log_writes,
        write_workers,
        write_budget_bytes,
        iceberg.is_some(),
        silver.clone(),
        metrics.clone(),
    );

    if let Some(silver) = silver.as_ref() {
        log::info(
            "silver_parsing_enabled",
            &format!("inline silver parsing enabled: {}", silver.describe()),
            &[],
        );
    }

    if manage_health {
        update_health_status_async(&health_file, true, None).await?;
    }

    let mut heartbeat = tokio::time::interval(HEALTH_UPDATE_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut progress_heartbeat = tokio::time::interval(Duration::from_secs(5));
    progress_heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    progress_heartbeat.tick().await;

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        tokio::select! {
            _ = heartbeat.tick() => {
                metrics.touch_heartbeat();
                let state = metrics.health_state();
                if manage_health {
                    if let Err(error) =
                        update_health_status_async(&health_file, state.healthy, state.reason.as_deref()).await
                    {
                        log::warn(
                            "health_file_write_failed",
                            &format!("failed to update health status: {error}"),
                            &[],
                        );
                    }
                }
                // The heartbeat was just touched above, so the only way
                // health_state() can be unhealthy here is a data drought
                // (see HealthState's doc comment) — bounded retry doesn't
                // apply to "connected fine but nothing arriving", so this
                // exits directly rather than looping.
                if !state.healthy {
                    let reason = state.reason.as_deref().unwrap_or("unknown");
                    log::error(
                        "data_drought",
                        &format!("giving up: {reason}"),
                        &[("source", source.source_name())],
                    );
                    if manage_health {
                        let _ = update_health_status_async(&health_file, false, Some(reason)).await;
                    }
                    std::process::exit(exitcode::DATA_DROUGHT);
                }
            }
            _ = progress_heartbeat.tick(), if report_progress => {
                emit_ingest_progress(
                    source,
                    rows_processed,
                    rows_in_file,
                    buf.payload_bytes(),
                    files_flushed,
                );
            }
            maybe_seq = durable_rx.recv() => {
                if let Some(seq) = maybe_seq {
                    metrics.batches_durable.fetch_add(1, Ordering::Relaxed);
                    source.on_batch_durable(seq);
                }
            }
            maybe_line = reader.next() => {
                match maybe_line {
                    Some(Ok(payload)) => {
                        let arrival_ts_ms = match source.timestamp_for_payload(&payload) {
                            Some(timestamp_ms) => timestamp_ms,
                            None => now_unix_duration().as_millis() as i64,
                        };
                        let payload = source.normalize_payload(payload, arrival_ts_ms);

                        // Resolve lines to write. An active transformer owns
                        // timestamp resolution and may buffer/combine multiple
                        // input lines (e.g. AIS fragment reassembly).
                        let lines: Vec<(i64, String)> = if let Some(t) = &mut line_transformer {
                            t.transform(&payload, arrival_ts_ms)
                        } else {
                            vec![(arrival_ts_ms, payload)]
                        };

                        for (row_ts_ms, payload) in lines {
                            let payload_len = payload.len();
                            let row_ts_clamped = row_ts_ms.max(0);

                            if row_ts_clamped < key_start_ms || row_ts_clamped >= key_end_ms {
                                if !buf.is_empty() {
                                    flush_batch(
                                        &common.out_dir,
                                        &current_key,
                                        &mut buf,
                                        write_queue.as_ref(),
                                        common.compression_level,
                                        batches_sealed + 1,
                                    )
                                    .await?;
                                    batches_sealed += 1;
                                    source.on_batch_sealed(batches_sealed);
                                    metrics.batches_sealed.store(batches_sealed, Ordering::Relaxed);
                                    rows_in_file = 0;
                                    files_flushed += 1;
                                }
                            current_key = PartKey::from_timestamp(
                                source.source_name(),
                                row_ts_ms,
                                common.partition,
                            );
                            let bounds = common.partition.period_bounds_ms(row_ts_ms);
                            key_start_ms = bounds.0;
                            key_end_ms = bounds.1;
                            }

                            // Keep each batch well below Arrow's string offset limit.
                            if buf.would_exceed_batch_bytes(payload_len, common.max_batch_bytes) {
                                flush_batch(
                                    &common.out_dir,
                                    &current_key,
                                    &mut buf,
                                    write_queue.as_ref(),
                                    common.compression_level,
                                    batches_sealed + 1,
                                )
                                .await?;
                                batches_sealed += 1;
                                source.on_batch_sealed(batches_sealed);
                                metrics.batches_sealed.store(batches_sealed, Ordering::Relaxed);
                                rows_in_file = 0;
                                files_flushed += 1;
                            }

                            buf.push(row_ts_ms, &payload);
                            rows_in_file += 1;
                            rows_processed += 1;
                            metrics.rows_processed.fetch_add(1, Ordering::Relaxed);
                            metrics
                                .buffered_bytes
                                .store(buf.payload_bytes() as u64, Ordering::Relaxed);
                            metrics.touch_last_row();

                            if buf.payload_bytes() >= common.max_batch_bytes {
                                flush_batch(
                                    &common.out_dir,
                                    &current_key,
                                    &mut buf,
                                    write_queue.as_ref(),
                                    common.compression_level,
                                    batches_sealed + 1,
                                )
                                .await?;
                                batches_sealed += 1;
                                source.on_batch_sealed(batches_sealed);
                                metrics.batches_sealed.store(batches_sealed, Ordering::Relaxed);
                                rows_in_file = 0;
                                files_flushed += 1;
                            }

                            if let Some(max_rows) = common.max_rows {
                                if rows_in_file >= max_rows {
                                    flush_batch(
                                        &common.out_dir,
                                        &current_key,
                                        &mut buf,
                                        write_queue.as_ref(),
                                        common.compression_level,
                                        batches_sealed + 1,
                                    )
                                    .await?;
                                    batches_sealed += 1;
                                    source.on_batch_sealed(batches_sealed);
                                    metrics.batches_sealed.store(batches_sealed, Ordering::Relaxed);
                                    rows_in_file = 0;
                                    files_flushed += 1;
                                }
                            }
                        }
                    }
                    Some(Err(LinesCodecError::MaxLineLengthExceeded)) => {
                        eprintln!(
                            "⚠️  Dropped oversized input line (>{} bytes)",
                            format_count(common.max_line_length)
                        );
                    }
                    Some(Err(error)) => {
                        match source.on_stream_error(&error, &shutdown, common.max_line_length).await? {
                            ReaderTransition::Continue(new_reader) => {
                                reader = new_reader;
                                continue;
                            }
                            ReaderTransition::Stop => {
                                if shutdown.load(Ordering::SeqCst) {
                                    break;
                                }
                                return Err(error.into());
                            }
                        }
                    }
                    None => {
                        match source.on_stream_end(&shutdown, common.max_line_length).await? {
                            ReaderTransition::Continue(new_reader) => {
                                reader = new_reader;
                                continue;
                            }
                            ReaderTransition::Stop => break,
                        }
                    }
                }
            }
        }
    }

    // Flush any buffered transformer state (e.g. incomplete AIS fragments).
    if let Some(t) = &mut line_transformer {
        for (flush_ts, flush_payload) in t.flush() {
            let flush_key =
                PartKey::from_timestamp(source.source_name(), flush_ts, common.partition);
            if flush_key != current_key && !buf.is_empty() {
                flush_batch(
                    &common.out_dir,
                    &current_key,
                    &mut buf,
                    write_queue.as_ref(),
                    common.compression_level,
                    batches_sealed + 1,
                )
                .await?;
                batches_sealed += 1;
                source.on_batch_sealed(batches_sealed);
                metrics
                    .batches_sealed
                    .store(batches_sealed, Ordering::Relaxed);
            }
            current_key = flush_key;
            buf.push(flush_ts, &flush_payload);
        }
    }

    if !buf.is_empty() {
        flush_batch(
            &common.out_dir,
            &current_key,
            &mut buf,
            write_queue.as_ref(),
            common.compression_level,
            batches_sealed + 1,
        )
        .await?;
        batches_sealed += 1;
        source.on_batch_sealed(batches_sealed);
        metrics
            .batches_sealed
            .store(batches_sealed, Ordering::Relaxed);
    }

    if let Some(write_queue) = write_queue {
        drop(write_queue);
        if let Some(write_worker) = write_worker {
            match write_worker.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) => return Err(error.into()),
            }
        }
    }

    // All write workers have exited, so every durability notification is
    // buffered in the channel; deliver them so sources can record final
    // progress (e.g. commit Kafka offsets) before we return.
    while let Ok(seq) = durable_rx.try_recv() {
        metrics.batches_durable.fetch_add(1, Ordering::Relaxed);
        source.on_batch_durable(seq);
    }

    if let Some(upload_tx) = upload_tx {
        drop(upload_tx);
        if let Some(mut upload_worker) = upload_worker {
            if log_writes {
                eprintln!("Waiting for background uploads to complete...");
            }
            let drain_timeout = Duration::from_secs(common.upload_drain_timeout_seconds);
            match tokio::time::timeout(drain_timeout, &mut upload_worker).await {
                Ok(join_result) => {
                    if let Err(error) = join_result {
                        eprintln!("Upload worker failed: {}", error);
                    } else {
                        if log_writes {
                            eprintln!("All uploads completed");
                        }
                    }
                }
                Err(_) => {
                    upload_worker.abort();
                    let _ = upload_worker.await;
                    eprintln!(
                        "Upload drain timed out after {} seconds. Exiting.",
                        common.upload_drain_timeout_seconds
                    );
                }
            }
        }
    }

    if let Some(metrics_server) = metrics_server {
        metrics_server.abort();
    }

    if manage_health {
        update_health_status_async(&health_file, false, Some("shutting down")).await?;
    }
    Ok(())
}

/// Write the health-status file an exec-based check (`--health-check`,
/// Docker `HEALTHCHECK`) reads from a separate process invocation — so it
/// carries a `reason` alongside the status, not just a bare bool, to agree
/// with `/healthz` and [`metrics::HealthState`] on *why* a still-running
/// process considers itself unhealthy (e.g. a data drought), not only
/// whether it has stopped updating the file at all.
pub async fn update_health_status_async(
    health_file: &Path,
    healthy: bool,
    reason: Option<&str>,
) -> Result<()> {
    let status = if healthy { "healthy" } else { "unhealthy" };
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let content = format!("{}:{}:{}", status, timestamp, reason.unwrap_or(""));

    tokio::fs::write(health_file, content)
        .await
        .with_context(|| "Failed to write health status file")?;
    Ok(())
}

pub fn check_health(health_file: &Path) -> Result<()> {
    match std::fs::read_to_string(health_file) {
        Ok(content) => {
            let mut parts = content.trim().splitn(3, ':');
            let (Some(status), Some(timestamp_str)) = (parts.next(), parts.next()) else {
                println!("Health check: UNHEALTHY (bad status file)");
                return Err(anyhow::anyhow!("bad health status file"));
            };
            let reason = parts.next().filter(|r| !r.is_empty());

            let timestamp: u64 = timestamp_str.parse().unwrap_or(0);
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

            if status == "healthy"
                && current_time.saturating_sub(timestamp) < DEFAULT_HEALTH_STALE_WINDOW_SECONDS
            {
                println!("Health check: HEALTHY");
                Ok(())
            } else {
                match reason {
                    Some(reason) => println!("Health check: UNHEALTHY ({reason})"),
                    None => println!("Health check: UNHEALTHY (stale or bad status)"),
                }
                Err(anyhow::anyhow!("health check failed"))
            }
        }
        Err(_) => {
            println!("Health check: UNHEALTHY (no status file)");
            Err(anyhow::anyhow!("no health status file"))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PartKey {
    source: String,
    granularity: PartitionGranularity,
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
}

impl PartKey {
    pub fn from_timestamp(
        source: &str,
        timestamp_ms: i64,
        granularity: PartitionGranularity,
    ) -> Self {
        let (year, month, day, hour, minute) = granularity.components_from_timestamp(timestamp_ms);
        PartKey {
            source: source.to_string(),
            granularity,
            year,
            month,
            day,
            hour,
            minute,
        }
    }

    fn relative_dir(&self) -> String {
        match self.granularity {
            PartitionGranularity::Year => {
                format!("source={}/year={:04}", self.source, self.year)
            }
            PartitionGranularity::Month => format!(
                "source={}/year={:04}/month={:02}",
                self.source, self.year, self.month
            ),
            PartitionGranularity::Day => format!(
                "source={}/year={:04}/month={:02}/day={:02}",
                self.source, self.year, self.month, self.day
            ),
            PartitionGranularity::Hour => format!(
                "source={}/year={:04}/month={:02}/day={:02}/hour={:02}",
                self.source, self.year, self.month, self.day, self.hour
            ),
            PartitionGranularity::Minute => format!(
                "source={}/year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}",
                self.source, self.year, self.month, self.day, self.hour, self.minute
            ),
        }
    }

    fn dir_path(&self, root: &Path) -> PathBuf {
        root.join(self.relative_dir())
    }

    fn s3_key(&self, filename: &str) -> String {
        format!("{}/{}", self.relative_dir(), filename)
    }
}

#[derive(Clone)]
pub struct S3Storage {
    client: S3Client,
    bucket: String,
    s3_prefix: String,
    keep_local: bool,
}

impl std::fmt::Debug for S3Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Storage")
            .field("bucket", &self.bucket)
            .field("s3_prefix", &self.s3_prefix)
            .field("keep_local", &self.keep_local)
            .finish_non_exhaustive()
    }
}

impl S3Storage {
    pub async fn new(
        bucket: String,
        s3_prefix: String,
        region: String,
        endpoint: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        keep_local: bool,
        disable_tls: bool,
    ) -> Result<Self> {
        let mut config_builder =
            aws_config::defaults(aws_config::BehaviorVersion::latest()).region(Region::new(region));

        if let Some(mut endpoint_url) = endpoint {
            if disable_tls
                && !endpoint_url.starts_with("http://")
                && !endpoint_url.starts_with("https://")
            {
                endpoint_url = format!("http://{}", endpoint_url);
            }
            config_builder = config_builder.endpoint_url(endpoint_url);
        }

        if let (Some(access), Some(secret)) = (access_key, secret_key) {
            let credentials = Credentials::new(access, secret, None, None, "manual");
            config_builder = config_builder.credentials_provider(credentials);
        }

        let config = config_builder.load().await;
        let s3_config = S3Config::from(&config)
            .to_builder()
            .force_path_style(true)
            .build();

        let client = S3Client::from_conf(s3_config);

        Self::ensure_bucket(&client, &bucket).await?;

        Ok(S3Storage {
            client,
            bucket,
            s3_prefix,
            keep_local,
        })
    }

    /// Create the target bucket if it does not already exist. Safe to call
    /// concurrently: a `BucketAlreadyOwnedByYou`/`BucketAlreadyExists` race is
    /// treated as success. Works against both AWS S3 and S3-compatible stores
    /// (MinIO/rustfs) since it uses a plain `CreateBucket` with no location
    /// constraint under path-style addressing.
    async fn ensure_bucket(client: &S3Client, bucket: &str) -> Result<()> {
        if bucket.contains('/') {
            let (parsed_bucket, path_part) = S3Storage::split_s3_path(bucket, "");
            bail!(
                "'{bucket}' is not a valid S3 bucket name because it contains \
                 a '/' (the bucket would be '{parsed_bucket}' and the key \
                 prefix would be '{path_part}'). Use --s3-bucket \
                 {parsed_bucket} --s3-prefix {path_part} or pass the bucket \
                 and prefix together as --s3-bucket {parsed_bucket}/{path_part} \
                 and the tool will split it automatically."
            );
        }

        match client.head_bucket().bucket(bucket).send().await {
            Ok(_) => return Ok(()),
            Err(err) => {
                // Only a confirmed "no such bucket" falls through to create.
                // Anything else (403 permission denied, network failure, bad
                // credentials, ...) is a real problem and must not be masked
                // by a CreateBucket attempt against a bucket that already
                // exists but we simply can't see.
                let is_not_found = err.as_service_error().is_some_and(|e| e.is_not_found());
                if !is_not_found {
                    let status = err.raw_response().map(|r| r.status().as_u16());
                    if status == Some(403) {
                        bail!(
                            "access denied checking whether S3 bucket '{bucket}' exists \
                             (HeadBucket returned 403 Forbidden). Check --s3-access-key/\
                             --s3-secret-key (or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY) and \
                             that the credentials have permission on this bucket."
                        );
                    }
                    return Err(anyhow::Error::new(err))
                        .with_context(|| format!("checking whether S3 bucket '{bucket}' exists"));
                }
            }
        }

        eprintln!("S3 bucket '{bucket}' does not exist; creating it...");
        match client.create_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(()),
            Err(err) => {
                let service_err = err.into_service_error();
                if service_err.is_bucket_already_owned_by_you()
                    || service_err.is_bucket_already_exists()
                {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!(service_err))
                        .with_context(|| format!("creating S3 bucket {bucket}"))
                }
            }
        }
    }

    /// Split a `bucket/key/path` string into (`bucket`, `prefix`). If there is
    /// no `/`, the prefix is empty. The prefix is returned without a leading
    /// or trailing `/`. The function is idempotent: if `prefix` is already
    /// non-empty, the extracted path is prepended.
    pub fn split_s3_path(bucket: &str, existing_prefix: &str) -> (String, String) {
        match bucket.find('/') {
            Some(slash) => {
                let b = bucket[..slash].to_string();
                let p = bucket[slash + 1..].trim_matches('/').to_string();
                let merged = if existing_prefix.is_empty() {
                    p
                } else {
                    format!("{}/{}", p, existing_prefix.trim_matches('/'))
                };
                (b, merged)
            }
            None => (bucket.to_string(), existing_prefix.to_string()),
        }
    }

    /// True when uploaded files are kept on local disk after upload.
    pub fn keeps_local(&self) -> bool {
        self.keep_local
    }

    pub async fn upload_file(&self, local_path: &Path, s3_key: &str) -> Result<()> {
        let full_key = if self.s3_prefix.is_empty() {
            s3_key.to_string()
        } else {
            format!("{}/{}", self.s3_prefix.trim_matches('/'), s3_key)
        };

        let file_metadata = tokio::fs::metadata(local_path)
            .await
            .with_context(|| format!("Failed to read file metadata: {}", local_path.display()))?;
        let file_size = file_metadata.len();

        let body = ByteStream::from_path(local_path).await.with_context(|| {
            format!(
                "Failed to stream file (size: {} bytes): {}",
                file_size,
                local_path.display()
            )
        })?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                eprintln!("❌ S3 Upload Failed:");
                eprintln!("   File: {}", local_path.display());
                eprintln!(
                    "   Size: {} bytes ({:.2} MB)",
                    file_size,
                    file_size as f64 / 1_048_576.0
                );
                eprintln!("   Bucket: s3://{}", self.bucket);
                eprintln!("   Key: {}", full_key);
                eprintln!("   Error Type: {}", e);
                if let Some(source) = e.source() {
                    eprintln!("   Underlying Cause: {}", source);
                }
                anyhow::anyhow!("S3 upload failed: {}", e)
            })?;

        eprintln!(
            "✅ Uploaded {} to S3: s3://{}/{} ({:.2} MB)",
            local_path.display(),
            self.bucket,
            full_key,
            file_size as f64 / 1_048_576.0
        );

        if !self.keep_local {
            tokio::fs::remove_file(local_path).await.with_context(|| {
                format!("Failed to remove local file: {}", local_path.display())
            })?;
            eprintln!("🗑️  Removed local file: {}", local_path.display());
        }

        Ok(())
    }

    /// List every object under `prefix`, following pagination. Pass `""` to
    /// list the whole bucket. Retries on transient S3 errors.
    pub async fn list_keys_with_prefix(&self, prefix: &str) -> Result<Vec<S3ObjectInfo>> {
        const MAX_ATTEMPTS: usize = 3;
        let mut last_error = None;
        for attempt in 1..=MAX_ATTEMPTS {
            match self.list_keys_with_prefix_inner(prefix).await {
                Ok(keys) => return Ok(keys),
                Err(error) => {
                    if attempt < MAX_ATTEMPTS {
                        let backoff = std::time::Duration::from_secs(1 << (attempt - 1));
                        eprintln!(
                            "List attempt {attempt}/{MAX_ATTEMPTS} failed, retrying in {}s...",
                            backoff.as_secs()
                        );
                        tokio::time::sleep(backoff).await;
                    }
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.expect("loop runs at least once"))
    }

    async fn list_keys_with_prefix_inner(&self, prefix: &str) -> Result<Vec<S3ObjectInfo>> {
        let mut keys = Vec::new();
        let mut continuation_token = None;
        let mut page: u32 = 0;

        loop {
            page += 1;
            let mut request = self.client.list_objects_v2().bucket(&self.bucket);
            if !prefix.is_empty() {
                request = request.prefix(prefix);
            }
            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .with_context(|| format!("listing s3://{}/{}", self.bucket, prefix))?;

            let page_count = response.contents().len();
            for object in response.contents() {
                let Some(key) = object.key() else {
                    continue;
                };
                let size = object.size().unwrap_or(0).max(0) as u64;
                let modified_ms = object.last_modified().map(|when| when.to_millis().ok());
                keys.push(S3ObjectInfo {
                    key: key.to_string(),
                    size,
                    modified_ms: modified_ms.flatten(),
                });
            }

            eprintln!(
                "  listed page {page} (+{page_count} objects, {} total) ...",
                keys.len()
            );

            if response.is_truncated().unwrap_or(false) {
                continuation_token = response
                    .next_continuation_token()
                    .map(|value| value.to_string());
            } else {
                break;
            }
        }

        Ok(keys)
    }

    /// Download an object to `local_path`, creating parent directories as needed.
    pub async fn download_to_path(&self, key: &str, local_path: &Path) -> Result<u64> {
        let mut response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("downloading s3://{}/{}", self.bucket, key))?;

        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("mkdir -p {}", parent.display()))?;
        }

        let file = tokio::fs::File::create(local_path)
            .await
            .with_context(|| format!("create {}", local_path.display()))?;
        // Body chunks are small; coalesce them so each write to the blocking
        // file pool is large instead of one round trip per chunk.
        let mut writer = tokio::io::BufWriter::with_capacity(1024 * 1024, file);
        let mut bytes_written: u64 = 0;
        while let Some(chunk) = response.body.try_next().await.with_context(|| {
            format!("read body of s3://{}/{}", self.bucket, key)
        })? {
            writer.write_all(&chunk).await.with_context(|| {
                format!(
                    "copy s3://{}/{} to {}",
                    self.bucket,
                    key,
                    local_path.display()
                )
            })?;
            bytes_written += chunk.len() as u64;
        }
        writer.flush().await?;
        Ok(bytes_written)
    }

    /// Name of the bucket this handle points at.
    pub fn bucket_name(&self) -> &str {
        &self.bucket
    }

    /// Write a small object from an in-memory buffer (e.g. job state). Unlike
    /// `upload_file` there is no local file involved and nothing is deleted.
    pub async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(bytes.into())
            .send()
            .await
            .with_context(|| format!("writing s3://{}/{}", self.bucket, key))?;
        Ok(())
    }

    /// Read a small object fully into memory. Returns `Ok(None)` when the key
    /// does not exist, so callers can treat a missing object as first-run state.
    pub async fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                if error
                    .as_service_error()
                    .map(|service| service.is_no_such_key())
                    .unwrap_or(false)
                {
                    return Ok(None);
                }
                return Err(error).with_context(|| format!("reading s3://{}/{}", self.bucket, key));
            }
        };
        let bytes = response
            .body
            .collect()
            .await
            .with_context(|| format!("reading body of s3://{}/{}", self.bucket, key))?;
        Ok(Some(bytes.into_bytes().to_vec()))
    }
}

/// One object returned by `S3Storage::list_keys_with_prefix`.
#[derive(Clone, Debug)]
pub struct S3ObjectInfo {
    pub key: String,
    pub size: u64,
    /// Server-side `LastModified`, as UTC milliseconds, when the store
    /// reported one.
    pub modified_ms: Option<i64>,
}

struct BatchBuf {
    ts: TimestampMillisecondBuilder,
    payload: StringBuilder,
    schema: Arc<Schema>,
    row_capacity: usize,
    byte_capacity: usize,
    payload_bytes: usize,
}

impl BatchBuf {
    fn new(max_rows: Option<usize>, max_batch_bytes: usize) -> Self {
        let row_capacity = max_rows.unwrap_or(4096);
        let byte_capacity = row_capacity.saturating_mul(128).min(max_batch_bytes.max(1));
        let fields = vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("payload", DataType::Utf8, false),
        ];
        BatchBuf {
            ts: TimestampMillisecondBuilder::with_capacity(row_capacity),
            payload: StringBuilder::with_capacity(row_capacity, byte_capacity),
            schema: Arc::new(Schema::new(fields)),
            row_capacity,
            byte_capacity,
            payload_bytes: 0,
        }
    }

    fn push(&mut self, ts_ms: i64, payload: &str) {
        self.ts.append_value(ts_ms);
        self.payload.append_value(payload);
        self.payload_bytes = self.payload_bytes.saturating_add(payload.len());
    }

    fn len(&self) -> usize {
        self.ts.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    fn would_exceed_batch_bytes(&self, next_payload_bytes: usize, max_batch_bytes: usize) -> bool {
        !self.is_empty() && self.payload_bytes.saturating_add(next_payload_bytes) > max_batch_bytes
    }

    fn to_record_batch(&mut self) -> Result<RecordBatch> {
        let ts_array = self.ts.finish().with_timezone_opt(Some(Arc::from("UTC")));
        let payload_array = self.payload.finish();

        self.ts = TimestampMillisecondBuilder::with_capacity(self.row_capacity);
        self.payload = StringBuilder::with_capacity(self.row_capacity, self.byte_capacity);
        self.payload_bytes = 0;

        let ts = Arc::new(ts_array) as Arc<dyn arrow::array::Array>;
        let payload = Arc::new(payload_array) as Arc<dyn arrow::array::Array>;
        RecordBatch::try_new(self.schema.clone(), vec![ts, payload]).context("building RecordBatch")
    }
}

/// Sort a `(ts, payload)` record batch by ascending timestamp.
///
/// Returns a cheap clone when the batch is already sorted — the common case
/// for streaming sources, avoiding the O(n log n) sort and the full copy that
/// reordering implies.
pub fn sort_record_batch_by_ts(batch: &RecordBatch) -> Result<RecordBatch> {
    let ts_column = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| anyhow::anyhow!("missing ts column for sorting"))?;

    if ts_column.values().windows(2).all(|pair| pair[0] <= pair[1]) {
        return Ok(batch.clone());
    }

    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };
    let indices = sort_to_indices(ts_column, Some(sort_options), None).context("sort indices")?;

    let sorted_columns: Vec<_> = (0..batch.num_columns())
        .map(|i| take(batch.column(i), &indices, None).context("reorder column"))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(batch.schema(), sorted_columns).context("build sorted batch")
}

fn open_writer(
    out_dir: &Path,
    path: &Path,
    schema: &Arc<Schema>,
    compression_level: i32,
) -> Result<ArrowWriter<File>> {
    let parent = path.parent();
    let file = loop {
        if let Some(parent) = parent {
            match fs::create_dir_all(parent) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error).with_context(|| format!("mkdir -p {}", parent.display()));
                }
            }
            ensure_no_symlink_descendants(out_dir, parent)?;
        }
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(file) => break file,
            Err(error) if error.kind() == ErrorKind::NotFound && parent.is_some() => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("create {}", path.display()));
            }
        }
    };
    let zstd_level =
        ZstdLevel::try_new(compression_level).context("invalid Zstd compression level")?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd_level))
        .set_column_encoding(ColumnPath::from("ts"), Encoding::DELTA_BINARY_PACKED)
        .set_column_encoding(
            ColumnPath::from("payload"),
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        )
        .set_column_dictionary_enabled(ColumnPath::from("ts"), false)
        .set_column_dictionary_enabled(ColumnPath::from("payload"), false)
        .build();
    ArrowWriter::try_new(file, schema.clone(), Some(props))
        .context("creating Parquet ArrowWriter")
}

fn ensure_no_symlink_descendants(root: &Path, path: &Path) -> Result<()> {
    if !is_path_within(root, path) {
        bail!(
            "refusing to access {} outside output directory {}",
            path.display(),
            root.display()
        );
    }

    let relative = path.strip_prefix(root).expect("validated output path");
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            bail!("invalid output path component in {}", path.display());
        };
        current.push(component);
        let metadata = fs::symlink_metadata(&current)
            .with_context(|| format!("reading metadata for {}", current.display()))?;
        if metadata.file_type().is_symlink() {
            bail!("refusing to follow output symlink {}", current.display());
        }
    }

    let canonical_root = fs::canonicalize(root)
        .with_context(|| format!("canonicalizing output directory {}", root.display()))?;
    let canonical_path = fs::canonicalize(path)
        .with_context(|| format!("canonicalizing output path {}", path.display()))?;
    if !canonical_path.starts_with(&canonical_root) {
        bail!(
            "refusing to access {} outside output directory {}",
            path.display(),
            root.display()
        );
    }
    Ok(())
}

fn parquet_file_name() -> String {
    let now = Utc::now();
    let counter = PARQUET_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let random = rand::random::<u64>();
    format!(
        "part-{:04}{:02}{:02}T{:02}{:02}{:02}{:03}-{:016x}-{:06}.parquet",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.timestamp_subsec_millis(),
        random,
        counter
    )
}

async fn upload_with_retry(
    storage: S3Storage,
    job: UploadJob,
    metrics: Arc<IngestMetrics>,
    out_dir: PathBuf,
    iceberg: Option<iceberg::IcebergHandle>,
) {
    let mut pending = VecDeque::from([job]);

    while let Some(UploadJob {
        path,
        s3_key,
        source,
        iceberg_batch,
    }) = pending.pop_front()
    {
        let claim = if storage.keeps_local() {
            None
        } else {
            match tokio::task::spawn_blocking({
                let path = path.clone();
                move || claim_upload(&path)
            })
            .await
            {
                Ok(Ok(Some(claim))) => Some(claim),
                Ok(Ok(None)) => continue,
                Ok(Err(error)) => {
                    metrics.uploads_failed.fetch_add(1, Ordering::Relaxed);
                    eprintln!("⚠️  Failed to claim {} for upload: {error}", path.display());
                    continue;
                }
                Err(error) => {
                    metrics.uploads_failed.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "⚠️  Upload claim task failed for {}: {error}",
                        path.display()
                    );
                    continue;
                }
            }
        };

        let uploaded = upload_one_with_retry(&storage, &path, &s3_key, &metrics).await;
        drop(claim);

        if !uploaded {
            continue;
        }

        if let (Some(handle), Some(batch)) = (iceberg.as_ref(), iceberg_batch) {
            register_raw_upload_with_retry(handle, batch, &source, &s3_key, &metrics).await;
        }

        if storage.keeps_local() {
            continue;
        }

        let cleanup = tokio::task::spawn_blocking({
            let out_dir = out_dir.clone();
            let parent = path.parent().map(Path::to_path_buf);
            move || match parent {
                Some(parent) => reconcile_partition_directories(&out_dir, &parent),
                None => Ok(Vec::new()),
            }
        })
        .await;

        match cleanup {
            Ok(Ok(files)) => pending.extend(files.into_iter().map(UploadJob::from)),
            Ok(Err(error)) => eprintln!(
                "⚠️  Failed to clean local partition directories after uploading {}: {error}",
                path.display()
            ),
            Err(error) => eprintln!(
                "⚠️  Local partition cleanup task failed after uploading {}: {error}",
                path.display()
            ),
        }
    }
}

async fn upload_one_with_retry(
    storage: &S3Storage,
    upload_path: &Path,
    s3_key: &str,
    metrics: &IngestMetrics,
) -> bool {
    // Scale the attempt timeout with file size (60s base + ~2s per MiB,
    // capped at 15 min) so large files on slow links are not doomed to hit
    // the same fixed timeout on every retry.
    let file_size = tokio::fs::metadata(upload_path)
        .await
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let attempt_timeout = Duration::from_secs((60 + 2 * (file_size / (1024 * 1024))).min(900));

    for attempt in 1..=DEFAULT_UPLOAD_RETRIES {
        let upload_result =
            tokio::time::timeout(attempt_timeout, storage.upload_file(upload_path, s3_key)).await;

        match upload_result {
            Ok(Ok(())) => {
                metrics.uploads_succeeded.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            Ok(Err(error)) if attempt < DEFAULT_UPLOAD_RETRIES => {
                metrics.upload_retries.fetch_add(1, Ordering::Relaxed);
                let backoff = Duration::from_secs(1_u64 << (attempt - 1));
                eprintln!(
                    "Upload attempt {attempt}/{DEFAULT_UPLOAD_RETRIES} failed for {}: {}. Retrying in {}s...",
                    upload_path.display(),
                    error,
                    backoff.as_secs()
                );
                tokio::time::sleep(backoff).await;
            }
            Err(_) if attempt < DEFAULT_UPLOAD_RETRIES => {
                metrics.upload_retries.fetch_add(1, Ordering::Relaxed);
                let backoff = Duration::from_secs(1_u64 << (attempt - 1));
                eprintln!(
                    "Upload attempt {attempt}/{DEFAULT_UPLOAD_RETRIES} timed out for {}. Retrying in {}s...",
                    upload_path.display(),
                    backoff.as_secs()
                );
                tokio::time::sleep(backoff).await;
            }
            Ok(Err(error)) => {
                metrics.uploads_failed.fetch_add(1, Ordering::Relaxed);
                eprintln!("⚠️  Background upload failed after retries: {}", error);
                eprintln!(
                    "   📁 File preserved on disk for retry: {}",
                    upload_path.display()
                );
                return false;
            }
            Err(_) => {
                metrics.uploads_failed.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "⚠️  Background upload timed out after retries. File preserved on disk: {}",
                    upload_path.display()
                );
                return false;
            }
        }
    }

    false
}

const ICEBERG_REGISTER_ATTEMPTS: u32 = 3;

/// Register a successfully-uploaded batch with Iceberg, with a short bounded
/// retry. This runs after the S3 upload has already succeeded and must never
/// fail the upload itself: on exhaustion it logs and bumps a failure metric,
/// leaving a re-run of `ais-parse`/`aisstream-parse` in batch mode (or a
/// manual pass) as the out-of-band backstop for anything missed.
async fn register_raw_upload_with_retry(
    handle: &iceberg::IcebergHandle,
    batch: RecordBatch,
    source: &str,
    s3_key: &str,
    metrics: &IngestMetrics,
) {
    for attempt in 1..=ICEBERG_REGISTER_ATTEMPTS {
        match iceberg::register_raw_upload(handle, batch.clone(), source).await {
            Ok(()) => {
                metrics
                    .iceberg_registrations_succeeded
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(error) if attempt < ICEBERG_REGISTER_ATTEMPTS => {
                metrics
                    .iceberg_registration_retries
                    .fetch_add(1, Ordering::Relaxed);
                let backoff = Duration::from_secs(1_u64 << (attempt - 1));
                eprintln!(
                    "Iceberg registration attempt {attempt}/{ICEBERG_REGISTER_ATTEMPTS} failed for {s3_key}: {error:#}. Retrying in {}s...",
                    backoff.as_secs()
                );
                tokio::time::sleep(backoff).await;
            }
            Err(error) => {
                metrics
                    .iceberg_registrations_failed
                    .fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "⚠️  Iceberg registration failed for {s3_key} after {ICEBERG_REGISTER_ATTEMPTS} attempts: {error:#}"
                );
            }
        }
    }
}

struct UploadClaim {
    _file: File,
}

fn claim_upload(path: &Path) -> Result<Option<UploadClaim>> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("opening {} for upload", path.display()));
        }
    };

    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;

        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if matches!(error.kind(), ErrorKind::WouldBlock) {
                return Ok(None);
            }
            return Err(error).with_context(|| format!("locking {} for upload", path.display()));
        }
    }

    Ok(Some(UploadClaim { _file: file }))
}

/// Find parquet files under `out_dir` that a previous run wrote but never
/// uploaded (the uploader deletes local files on success, so with keep_local
/// off, anything still present belongs to a run that died before upload).
/// Returns `(local_path, s3_key)` pairs; only files inside the hive layout
/// (`source=.../...`) are considered, and in-progress `.tmp` files are not.
fn find_orphaned_uploads(out_dir: &Path) -> Result<Vec<(PathBuf, String)>> {
    find_pending_uploads(out_dir, out_dir)
}

fn find_pending_uploads(out_dir: &Path, scan_root: &Path) -> Result<Vec<(PathBuf, String)>> {
    let mut orphans = Vec::new();
    if !scan_root.is_dir() {
        return Ok(orphans);
    }
    if scan_root != out_dir && !is_path_within(out_dir, scan_root) {
        bail!(
            "refusing to scan {} outside output directory {}",
            scan_root.display(),
            out_dir.display()
        );
    }
    ensure_no_symlink_descendants(out_dir, scan_root)?;

    let mut stack = vec![scan_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) => {
                eprintln!(
                    "⚠️  Skipping unreadable directory {}: {}",
                    dir.display(),
                    error
                );
                continue;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }

            let upload_path = path;
            let Ok(rel) = upload_path.strip_prefix(out_dir) else {
                continue;
            };
            let s3_key = rel
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            if !s3_key.starts_with("source=") {
                continue;
            }

            let is_parquet =
                upload_path.extension().and_then(|ext| ext.to_str()) == Some("parquet");
            if !is_parquet {
                continue;
            }

            orphans.push((upload_path, s3_key));
        }
    }

    orphans.sort();
    Ok(orphans)
}

fn reconcile_partition_directories(
    out_dir: &Path,
    start_dir: &Path,
) -> Result<Vec<(PathBuf, String)>> {
    if start_dir == out_dir {
        return Ok(Vec::new());
    }
    if !is_path_within(out_dir, start_dir) {
        bail!(
            "refusing to clean {} outside output directory {}",
            start_dir.display(),
            out_dir.display()
        );
    }
    if start_dir.exists() {
        ensure_no_symlink_descendants(out_dir, start_dir)?;
    }

    let mut current = Some(start_dir.to_path_buf());
    while let Some(dir) = current {
        if dir == out_dir {
            break;
        }

        match fs::remove_dir(&dir) {
            Ok(()) => current = dir.parent().map(Path::to_path_buf),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                current = dir.parent().map(Path::to_path_buf)
            }
            Err(error) if error.kind() == ErrorKind::DirectoryNotEmpty => {
                let files = find_pending_uploads(out_dir, &dir)?;
                prune_empty_descendants(out_dir, &dir)?;
                if !files.is_empty() {
                    return Ok(files);
                }

                match fs::remove_dir(&dir) {
                    Ok(()) => current = dir.parent().map(Path::to_path_buf),
                    Err(error) if matches!(error.kind(), ErrorKind::NotFound) => {
                        current = dir.parent().map(Path::to_path_buf)
                    }
                    Err(error) if error.kind() == ErrorKind::DirectoryNotEmpty => {
                        return Ok(Vec::new());
                    }
                    Err(error) => {
                        return Err(error).with_context(|| format!("removing {}", dir.display()));
                    }
                }
            }
            Err(error) => {
                return Err(error).with_context(|| format!("removing {}", dir.display()));
            }
        }
    }

    Ok(Vec::new())
}

fn is_path_within(root: &Path, path: &Path) -> bool {
    path.strip_prefix(root).is_ok_and(|relative| {
        relative
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    })
}

fn prune_empty_descendants(out_dir: &Path, scan_root: &Path) -> Result<()> {
    let mut stack = vec![scan_root.to_path_buf()];
    let mut directories = Vec::new();

    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("reading {}", dir.display()));
            }
        };

        directories.push(dir);
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() && !file_type.is_symlink() {
                stack.push(entry.path());
            }
        }
    }

    directories.sort_by_key(|dir| std::cmp::Reverse(dir.components().count()));
    for dir in directories {
        if dir == out_dir || !is_path_within(out_dir, &dir) {
            continue;
        }
        match fs::remove_dir(&dir) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::NotFound | ErrorKind::DirectoryNotEmpty
                ) => {}
            Err(error) => {
                return Err(error).with_context(|| format!("removing {}", dir.display()));
            }
        }
    }

    Ok(())
}

/// Upload previously found orphan files with bounded concurrency, reusing the
/// per-file retry/timeout policy of live uploads (successful uploads delete
/// the local file; failures leave it for the next sweep).
async fn upload_orphaned_files(
    storage: S3Storage,
    files: Vec<(PathBuf, String)>,
    metrics: Arc<IngestMetrics>,
    upload_concurrency: usize,
    out_dir: PathBuf,
) {
    let semaphore = Arc::new(Semaphore::new(upload_concurrency));
    let mut uploads = JoinSet::new();

    for (path, s3_key) in files {
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };
        let storage = storage.clone();
        let metrics = metrics.clone();
        let out_dir = out_dir.clone();
        let job = UploadJob::from((path, s3_key));
        uploads.spawn(async move {
            let _permit = permit;
            // Orphan-recovered files have no in-memory batch, so they are
            // never registered with Iceberg here (see `UploadJob` doc comment).
            upload_with_retry(storage, job, metrics, out_dir, None).await;
        });
    }

    while let Some(result) = uploads.join_next().await {
        if let Err(error) = result {
            eprintln!("Orphan upload task failed: {error}");
        }
    }
}

/// Scan `out_dir` for parquet files left behind by a previous run and upload
/// them, waiting for completion. Returns the number of files uploaded (or
/// attempted). No-op when `storage` keeps local files after upload, since
/// already-uploaded files can't be told apart from orphans.
pub async fn sweep_orphaned_uploads(out_dir: PathBuf, storage: S3Storage) -> Result<usize> {
    if storage.keeps_local() {
        return Ok(0);
    }

    let scan_out_dir = out_dir.clone();
    let files = tokio::task::spawn_blocking(move || find_orphaned_uploads(&scan_out_dir))
        .await
        .context("orphan upload scan task")??;
    let count = files.len();
    if count > 0 {
        upload_orphaned_files(
            storage,
            files,
            Arc::new(IngestMetrics::default()),
            DEFAULT_UPLOAD_CONCURRENCY,
            out_dir,
        )
        .await;
    }
    Ok(count)
}

fn spawn_upload_worker(
    s3_storage: Option<S3Storage>,
    metrics: Arc<IngestMetrics>,
    upload_concurrency: usize,
    out_dir: PathBuf,
    iceberg: Option<iceberg::IcebergHandle>,
) -> (
    Option<mpsc::Sender<UploadJob>>,
    Option<tokio::task::JoinHandle<()>>,
) {
    let Some(s3_storage_worker) = s3_storage else {
        return (None, None);
    };

    let (tx, mut rx) = mpsc::channel::<UploadJob>(DEFAULT_UPLOAD_QUEUE_CAPACITY);
    let worker = tokio::spawn(async move {
        let semaphore = Arc::new(Semaphore::new(upload_concurrency));
        let mut uploads = JoinSet::new();

        while let Some(job) = rx.recv().await {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let storage = s3_storage_worker.clone();
            let metrics = metrics.clone();
            let out_dir = out_dir.clone();
            let iceberg = iceberg.clone();
            uploads.spawn(async move {
                let _permit = permit;
                upload_with_retry(storage, job, metrics, out_dir, iceberg).await;
            });
        }

        while let Some(result) = uploads.join_next().await {
            if let Err(error) = result {
                eprintln!("Upload task failed: {}", error);
            }
        }
    });

    (Some(tx), Some(worker))
}

/// Producer half of the write pipeline: a job channel plus a semaphore that
/// bounds the payload bytes allowed in flight (queued or being written).
struct WriteQueueHandle {
    tx: mpsc::Sender<WriteJob>,
    byte_budget: Arc<Semaphore>,
    budget_bytes: usize,
}

#[allow(clippy::too_many_arguments)]
fn spawn_write_worker(
    shutdown: Arc<AtomicBool>,
    upload_tx: Option<mpsc::Sender<UploadJob>>,
    durable_tx: mpsc::UnboundedSender<u64>,
    log_writes: bool,
    worker_limit: Option<usize>,
    budget_bytes: usize,
    iceberg_enabled: bool,
    silver: Option<Arc<dyn silver::SilverCommit>>,
    metrics: Arc<IngestMetrics>,
) -> (
    Option<WriteQueueHandle>,
    Option<tokio::task::JoinHandle<Result<()>>>,
) {
    let (tx, rx) = mpsc::channel::<WriteJob>(DEFAULT_WRITE_QUEUE_CAPACITY);
    let worker_count = worker_limit
        .unwrap_or_else(default_write_worker_limit)
        .max(1);

    let worker = tokio::spawn(async move {
        let mut workers = Vec::with_capacity(worker_count);
        let shared_rx = Arc::new(tokio::sync::Mutex::new(rx));

        for _ in 0..worker_count {
            let shared_rx = shared_rx.clone();
            let shutdown = shutdown.clone();
            let upload_tx = upload_tx.clone();
            let durable_tx = durable_tx.clone();
            let silver = silver.clone();
            let metrics = metrics.clone();
            workers.push(tokio::spawn(async move {
                // Drain until the channel closes: a shutdown signal stops the
                // reader, but queued batches must still reach disk. A write
                // error sets `shutdown` (stopping the reader) and exits this
                // worker; once every worker has failed the channel closes and
                // producers see the error.
                loop {
                    let next_job = {
                        let mut rx = shared_rx.lock().await;
                        rx.recv().await
                    };

                    let Some(job) = next_job else {
                        break;
                    };

                    if let Err(error) = write_batch_job(
                        job,
                        upload_tx.as_ref(),
                        &durable_tx,
                        log_writes,
                        iceberg_enabled,
                        silver.as_ref(),
                        &metrics,
                    )
                    .await
                    {
                        shutdown.store(true, Ordering::SeqCst);
                        return Err(error);
                    }
                }

                Ok::<_, anyhow::Error>(())
            }));
        }

        let mut first_error = None;
        for worker in workers {
            match worker.await {
                Ok(Ok(())) => {}
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
            eprintln!("Write worker failed: {}", error);
            return Err(error);
        }

        Ok(())
    });

    (
        Some(WriteQueueHandle {
            tx,
            byte_budget: Arc::new(Semaphore::new(budget_bytes.min(Semaphore::MAX_PERMITS))),
            budget_bytes,
        }),
        Some(worker),
    )
}

struct WriteJob {
    seq: u64,
    out_dir: PathBuf,
    path: PathBuf,
    s3_key: String,
    /// Source name, carried through to `UploadJob` for Iceberg registration.
    source: String,
    batch: RecordBatch,
    compression_level: i32,
    /// Held while the batch occupies memory; released once written to disk.
    byte_permit: OwnedSemaphorePermit,
}

/// One file queued for S3 upload. `iceberg_batch` carries the sealed
/// `[ts, payload]` batch that was written to `path`, so it can be
/// registered with Iceberg immediately after the upload succeeds — it is
/// `None` for orphan/reconciled files rediscovered from disk (no in-memory
/// batch survives a restart), which are therefore not auto-registered.
struct UploadJob {
    path: PathBuf,
    s3_key: String,
    source: String,
    iceberg_batch: Option<RecordBatch>,
}

impl From<(PathBuf, String)> for UploadJob {
    fn from((path, s3_key): (PathBuf, String)) -> Self {
        UploadJob {
            path,
            s3_key,
            source: String::new(),
            iceberg_batch: None,
        }
    }
}

async fn write_batch_job(
    job: WriteJob,
    upload_tx: Option<&mpsc::Sender<UploadJob>>,
    durable_tx: &mpsc::UnboundedSender<u64>,
    log_writes: bool,
    iceberg_enabled: bool,
    silver: Option<&Arc<dyn silver::SilverCommit>>,
    metrics: &IngestMetrics,
) -> Result<()> {
    let WriteJob {
        seq,
        out_dir,
        path,
        s3_key,
        source,
        batch,
        compression_level,
        byte_permit,
    } = job;
    let schema = batch.schema();
    let batch_rows = batch.num_rows();
    // Cheap: RecordBatch::clone() only clones the Arc-backed Arrow arrays.
    // Taken before the batch is moved into the blocking write task below.
    let iceberg_batch = iceberg_enabled.then(|| batch.clone());
    let silver_batch = silver.is_some().then(|| batch.clone());
    let temp_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("part.parquet")
    ));
    let final_path = path.clone();

    let write_out_dir = out_dir.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut writer = open_writer(&write_out_dir, &temp_path, &schema, compression_level)?;
        writer.write(&batch).context("writing batch to Parquet")?;
        writer.close().context("closing Parquet writer")?;
        fs::rename(&temp_path, &final_path).with_context(|| {
            format!(
                "renaming {} to {}",
                temp_path.display(),
                final_path.display()
            )
        })?;
        Ok(())
    })
    .await
    .context("joining parquet write task")??;

    // The batch memory is released once it is on disk; free its byte budget
    // so the producer can seal the next batch.
    drop(byte_permit);

    // The batch is on local disk under its final name: report durability so
    // sources can advance their upstream progress markers (e.g. Kafka offsets).
    let _ = durable_tx.send(seq);

    // Inline silver parsing runs after bronze durability (so upstream progress
    // markers keep bronze-only semantics) and before the upload is queued.
    // A silver failure never fails the bronze batch: the bronze upload
    // proceeds and the orchestrator backfill stays the repair path.
    let mut silver_committed = false;
    if let (Some(silver), Some(silver_batch)) = (silver, silver_batch) {
        match silver.commit_bronze_batch(&silver_batch, &source).await {
            Ok(stats) => {
                silver_committed = true;
                metrics.record_silver_stats(&stats);
                metrics
                    .silver_commits_succeeded
                    .fetch_add(1, Ordering::Relaxed);
                if log_writes && stats.rows_in > 0 {
                    eprintln!(
                        "🔬 Silver: {} in → {} decoded (pos={} stat={} met={} bin={} aton={} other={} incomplete={} failed={} deduped={})",
                        stats.rows_in,
                        stats.decoded_total(),
                        stats.positions,
                        stats.statics,
                        stats.meteo,
                        stats.binary,
                        stats.atons,
                        stats.other,
                        stats.incomplete,
                        stats.failed,
                        stats.deduped,
                    );
                }
            }
            Err(error) => {
                metrics
                    .silver_commits_failed
                    .fetch_add(1, Ordering::Relaxed);
                eprintln!("⚠️  Silver commit failed for {}: {error:#}", path.display());
            }
        }
    }

    // `--delete-after-iceberg`: the silver rows are committed, so the local
    // bronze file is no longer needed. Only reached on a successful commit.
    let mut deleted_local = false;
    if silver_committed && silver.is_some_and(|s| s.delete_local_after_commit()) {
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                deleted_local = true;
                if let Some(parent) = path.parent().map(Path::to_path_buf) {
                    let out_dir = out_dir.clone();
                    // Best effort: prune now-empty partition directories.
                    let _ = tokio::task::spawn_blocking(move || {
                        reconcile_partition_directories(&out_dir, &parent)
                    })
                    .await;
                }
            }
            Err(error) => eprintln!(
                "⚠️  Failed to delete {} after Iceberg commit: {error}",
                path.display()
            ),
        }
    }

    if log_writes {
        let disposition = if deleted_local {
            "committed to Iceberg, local file deleted"
        } else if upload_tx.is_some() {
            "queued for upload"
        } else {
            "kept locally"
        };
        eprintln!(
            "✅ Wrote {} rows to {} ({disposition})",
            format_count(batch_rows),
            path.display()
        );
    }

    if let Some(upload_tx) = upload_tx {
        upload_tx
            .send(UploadJob {
                path,
                s3_key,
                source,
                iceberg_batch,
            })
            .await
            .context("Failed to queue upload task")?;
    }

    Ok(())
}

fn default_write_worker_limit() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get().saturating_div(2).max(2))
        .unwrap_or(4)
        .clamp(2, 8)
}

async fn flush_batch(
    root: &Path,
    key: &PartKey,
    buf: &mut BatchBuf,
    queue: Option<&WriteQueueHandle>,
    compression_level: i32,
    seq: u64,
) -> Result<()> {
    let dir = key.dir_path(root);
    let filename = parquet_file_name();
    let path = dir.join(&filename);
    let payload_bytes = buf.payload_bytes().max(1);
    let batch = buf.to_record_batch()?;
    let batch = tokio::task::spawn_blocking(move || sort_record_batch_by_ts(&batch))
        .await
        .context("sort task panicked")?
        .context("sorting batch by timestamp")?;
    let s3_key = key.s3_key(&filename);
    let Some(queue) = queue else {
        return Err(anyhow::anyhow!("missing write queue"));
    };

    // Byte-budget backpressure: memory in the write pipeline is bounded by
    // the semaphore budget rather than the queue's job count. Oversized
    // batches clamp to the full budget so they can still proceed alone.
    let permits = payload_bytes.min(queue.budget_bytes).min(u32::MAX as usize) as u32;
    let byte_permit = queue
        .byte_budget
        .clone()
        .acquire_many_owned(permits)
        .await
        .map_err(|_| anyhow::anyhow!("write byte budget closed"))?;

    // Waits for queue capacity even during shutdown: the write workers keep
    // draining until the channel closes, so this send stays bounded. The
    // channel only closes early when every write worker has failed.
    queue
        .tx
        .send(WriteJob {
            seq,
            out_dir: root.to_path_buf(),
            path,
            s3_key,
            source: key.source.clone(),
            batch,
            compression_level,
            byte_permit,
        })
        .await
        .map_err(|_| anyhow::anyhow!("write queue closed (write workers failed)"))?;

    Ok(())
}

fn emit_ingest_progress<S: LineSource>(
    source: &S,
    rows_processed: usize,
    rows_in_file: usize,
    buffered_bytes: usize,
    files_flushed: usize,
) {
    let Some(progress) = source.ingest_progress() else {
        return;
    };

    eprintln!(
        "📈 Processing {}/{} {} | rows={} batch_rows={} buffered_bytes={} flushed_files={}",
        format_count(progress.current_input_index),
        format_count(progress.input_total),
        progress.current_input,
        format_count(rows_processed),
        format_count(rows_in_file),
        format_count(buffered_bytes),
        format_count(files_flushed),
    );
}

fn now_unix_duration() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
}
