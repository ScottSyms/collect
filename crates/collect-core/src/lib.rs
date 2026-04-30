use anyhow::{Context, Result};
use arrow::array::{ArrayBuilder, StringBuilder, TimestampMillisecondBuilder};
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
use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncRead;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_util::codec::{FramedRead, LinesCodec};

pub use async_trait::async_trait;
pub use tokio_util::codec::LinesCodecError;

const DEFAULT_OUT_DIR: &str = "data";
const DEFAULT_UPLOAD_DRAIN_TIMEOUT_SECONDS: u64 = 60;
const DEFAULT_MAX_LINE_LENGTH: usize = 65_536;
const DEFAULT_MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_COMPRESSION_LEVEL: i32 = 5;
const DEFAULT_S3_REGION: &str = "us-east-1";
const DEFAULT_HEALTH_STALE_WINDOW_SECONDS: u64 = 60;
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
    #[arg(short = 'o', long, default_value = "data")]
    pub out_dir: PathBuf,

    /// Partition granularity for dataset layout
    #[arg(long, default_value_t = PartitionGranularity::Day)]
    pub partition: PartitionGranularity,

    /// Max rows to buffer per Parquet file before flush (default: flush on the selected partition boundary)
    #[arg(long)]
    pub max_rows: Option<usize>,

    /// Max payload bytes to buffer per Parquet file before flush
    #[arg(long)]
    pub max_batch_bytes: Option<usize>,

    /// Zstd compression level for Parquet output
    #[arg(long)]
    pub compression_level: Option<i32>,

    /// Seconds to wait for background uploads on shutdown
    #[arg(long, default_value_t = DEFAULT_UPLOAD_DRAIN_TIMEOUT_SECONDS)]
    pub upload_drain_timeout_seconds: u64,

    /// Maximum bytes allowed per input line before dropping it
    #[arg(long, default_value_t = DEFAULT_MAX_LINE_LENGTH)]
    pub max_line_length: usize,

    /// Run health check and exit
    #[arg(long)]
    pub health_check: bool,
}

#[derive(Clone, Debug, Args)]
pub struct S3CliArgs {
    /// S3 bucket name for remote storage (enables S3 upload)
    #[arg(long)]
    pub s3_bucket: Option<String>,

    /// S3 endpoint URL (for MinIO or custom S3-compatible storage)
    #[arg(long)]
    pub s3_endpoint: Option<String>,

    /// S3 region (default: us-east-1)
    #[arg(long, default_value = "us-east-1")]
    pub s3_region: String,

    /// S3 access key ID (can also use AWS_ACCESS_KEY_ID env var)
    #[arg(long)]
    pub s3_access_key: Option<String>,

    /// S3 secret access key (can also use AWS_SECRET_ACCESS_KEY env var)
    #[arg(long)]
    pub s3_secret_key: Option<String>,

    /// Keep local files after S3 upload (default: delete after successful upload)
    #[arg(long)]
    pub keep_local: bool,

    /// Disable TLS/HTTPS for S3 endpoint (use plain HTTP instead)
    #[arg(long)]
    pub s3_disable_tls: bool,
}

#[derive(Clone, Debug)]
pub struct CommonOptions {
    pub out_dir: PathBuf,
    pub partition: PartitionGranularity,
    pub max_rows: Option<usize>,
    pub max_batch_bytes: usize,
    pub compression_level: i32,
    pub upload_drain_timeout_seconds: u64,
    pub max_line_length: usize,
    pub health_check: bool,
}

#[derive(Clone, Debug)]
pub struct S3Options {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub keep_local: bool,
    pub disable_tls: bool,
}

#[derive(Clone, Debug)]
pub struct IngestOptions {
    pub common: CommonOptions,
    pub s3: Option<S3Options>,
    pub health_file: PathBuf,
    pub manage_health: bool,
    pub report_progress: bool,
    pub log_writes: bool,
    pub shutdown: Option<Arc<AtomicBool>>,
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

impl CommonCliArgs {
    pub fn apply_env(&mut self) {
        if self.out_dir == PathBuf::from(DEFAULT_OUT_DIR) {
            if let Ok(value) = std::env::var("OUT_DIR") {
                self.out_dir = PathBuf::from(value);
            }
        }

        if self.partition == PartitionGranularity::Day {
            if let Ok(value) = std::env::var("PARTITION") {
                if let Ok(parsed) = value.parse::<PartitionGranularity>() {
                    self.partition = parsed;
                }
            }
        }

        if self.max_rows.is_none() {
            if let Ok(value) = std::env::var("MAX_ROWS") {
                self.max_rows = value.parse().ok();
            }
        }

        if self.max_batch_bytes.is_none() {
            if let Ok(value) = std::env::var("MAX_BATCH_BYTES") {
                self.max_batch_bytes = value.parse().ok();
            }
        }

        if self.compression_level.is_none() {
            if let Ok(value) = std::env::var("COMPRESSION_LEVEL") {
                self.compression_level = value.parse().ok();
            }
        }

        if self.upload_drain_timeout_seconds == DEFAULT_UPLOAD_DRAIN_TIMEOUT_SECONDS {
            if let Ok(value) = std::env::var("UPLOAD_DRAIN_TIMEOUT_SECONDS") {
                if let Ok(parsed) = value.parse::<u64>() {
                    self.upload_drain_timeout_seconds = parsed;
                }
            }
        }

        if self.max_line_length == DEFAULT_MAX_LINE_LENGTH {
            if let Ok(value) = std::env::var("MAX_LINE_LENGTH") {
                if let Ok(parsed) = value.parse::<usize>() {
                    self.max_line_length = parsed;
                }
            }
        }

        if !self.health_check {
            if let Some(value) = parse_bool_env("HEALTH_CHECK") {
                self.health_check = value;
            }
        }
    }

    pub fn to_options(&self) -> CommonOptions {
        CommonOptions {
            out_dir: self.out_dir.clone(),
            partition: self.partition,
            max_rows: self.max_rows,
            max_batch_bytes: self.max_batch_bytes.unwrap_or(DEFAULT_MAX_BATCH_BYTES),
            compression_level: self.compression_level.unwrap_or(DEFAULT_COMPRESSION_LEVEL),
            upload_drain_timeout_seconds: self.upload_drain_timeout_seconds,
            max_line_length: self.max_line_length,
            health_check: self.health_check,
        }
    }
}

impl S3CliArgs {
    pub fn apply_env(&mut self) {
        if self.s3_bucket.is_none() {
            if let Ok(value) = std::env::var("S3_BUCKET") {
                self.s3_bucket = Some(value);
            }
        }

        if self.s3_endpoint.is_none() {
            if let Ok(value) = std::env::var("S3_ENDPOINT") {
                self.s3_endpoint = Some(value);
            }
        }

        if self.s3_region == DEFAULT_S3_REGION {
            if let Ok(value) = std::env::var("S3_REGION") {
                self.s3_region = value;
            }
        }

        if self.s3_access_key.is_none() {
            if let Ok(value) = std::env::var("S3_ACCESS_KEY") {
                self.s3_access_key = Some(value);
            }
        }

        if self.s3_secret_key.is_none() {
            if let Ok(value) = std::env::var("S3_SECRET_KEY") {
                self.s3_secret_key = Some(value);
            }
        }

        if !self.keep_local {
            if let Some(value) = parse_bool_env("KEEP_LOCAL") {
                self.keep_local = value;
            }
        }

        if !self.s3_disable_tls {
            if let Some(value) = parse_bool_env("S3_DISABLE_TLS") {
                self.s3_disable_tls = value;
            }
        }
    }

    pub fn to_options(&self) -> Option<S3Options> {
        let bucket = self.s3_bucket.clone()?;

        Some(S3Options {
            bucket,
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
    let IngestOptions {
        common,
        s3,
        health_file,
        manage_health,
        report_progress,
        log_writes,
        shutdown: external_shutdown,
    } = options;

    if common.health_check {
        check_health(&health_file)?;
        return Ok(());
    }

    let s3_storage = match s3 {
        Some(s3_options) => Some(s3_options.into_storage().await?),
        None => None,
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

    let now = now_unix_duration();
    let mut current_key = PartKey::from_timestamp(
        source.source_name(),
        now.as_millis() as i64,
        common.partition,
    );
    let mut buf = BatchBuf::new(common.max_rows, common.max_batch_bytes);
    let mut rows_in_file = 0usize;
    let mut rows_processed = 0usize;
    let mut files_flushed = 0usize;

    let (upload_tx, upload_worker) = spawn_upload_worker(s3_storage.clone());
    let (write_tx, write_worker) =
        spawn_write_worker(shutdown.clone(), upload_tx.clone(), log_writes);

    if options.manage_health {
        update_health_status_async(&health_file, true).await?;
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
                if manage_health {
                    if let Err(error) = update_health_status_async(&health_file, true).await {
                        eprintln!("Failed to update health status: {}", error);
                    }
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
            maybe_line = reader.next() => {
                match maybe_line {
                    Some(Ok(payload)) => {
                        let row_ts_ms = match source.timestamp_for_payload(&payload) {
                            Some(timestamp_ms) => timestamp_ms,
                            None => now_unix_duration().as_millis() as i64,
                        };
                        let payload = source.normalize_payload(payload, row_ts_ms);
                        let row_key = PartKey::from_timestamp(
                            source.source_name(),
                            row_ts_ms,
                            common.partition,
                        );
                        let payload_len = payload.len();

                        if row_key != current_key {
                            if !buf.is_empty() {
                                flush_batch(
                                    &common.out_dir,
                                    &current_key,
                                    &mut buf,
                                    write_tx.as_ref(),
                                    common.compression_level,
                                    &shutdown,
                                    log_writes,
                                )
                                .await?;
                                rows_in_file = 0;
                                files_flushed += 1;
                            }
                            current_key = row_key;
                        }

                        // Keep each batch well below Arrow's string offset limit.
                        if buf.would_exceed_batch_bytes(payload_len, common.max_batch_bytes) {
                            flush_batch(
                                &common.out_dir,
                                &current_key,
                                &mut buf,
                                write_tx.as_ref(),
                                common.compression_level,
                                &shutdown,
                                log_writes,
                            )
                            .await?;
                            rows_in_file = 0;
                            files_flushed += 1;
                        }

                        buf.push(row_ts_ms, &payload);
                        rows_in_file += 1;
                        rows_processed += 1;

                        if buf.payload_bytes() >= common.max_batch_bytes {
                            flush_batch(
                                &common.out_dir,
                                &current_key,
                                &mut buf,
                                write_tx.as_ref(),
                                common.compression_level,
                                &shutdown,
                                log_writes,
                            )
                            .await?;
                            rows_in_file = 0;
                            files_flushed += 1;
                        }

                        if let Some(max_rows) = common.max_rows {
                            if rows_in_file >= max_rows {
                                flush_batch(
                                    &common.out_dir,
                                    &current_key,
                                    &mut buf,
                                    write_tx.as_ref(),
                                    common.compression_level,
                                    &shutdown,
                                    log_writes,
                                )
                                .await?;
                                rows_in_file = 0;
                                files_flushed += 1;
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

    if !buf.is_empty() {
        flush_batch(
            &common.out_dir,
            &current_key,
            &mut buf,
            write_tx.as_ref(),
            common.compression_level,
            &shutdown,
            log_writes,
        )
        .await?;
    }

    if let Some(write_tx) = write_tx {
        drop(write_tx);
        if let Some(write_worker) = write_worker {
            match write_worker.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) => return Err(error.into()),
            }
        }
    }

    if let Some(upload_tx) = upload_tx {
        drop(upload_tx);
        if let Some(upload_worker) = upload_worker {
            if log_writes {
                println!("Waiting for background uploads to complete...");
            }
            let drain_timeout = Duration::from_secs(common.upload_drain_timeout_seconds);
            match tokio::time::timeout(drain_timeout, upload_worker).await {
                Ok(join_result) => {
                    if let Err(error) = join_result {
                        eprintln!("Upload worker failed: {}", error);
                    } else {
                        if log_writes {
                            println!("All uploads completed");
                        }
                    }
                }
                Err(_) => {
                    eprintln!(
                        "Upload drain timed out after {} seconds. Exiting.",
                        common.upload_drain_timeout_seconds
                    );
                }
            }
        }
    }

    if options.manage_health {
        update_health_status_async(&health_file, false).await?;
    }
    Ok(())
}

pub async fn update_health_status_async(health_file: &Path, healthy: bool) -> Result<()> {
    let status = if healthy { "healthy" } else { "unhealthy" };
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let content = format!("{}:{}", status, timestamp);

    tokio::fs::write(health_file, content)
        .await
        .with_context(|| "Failed to write health status file")?;
    Ok(())
}

pub fn check_health(health_file: &Path) -> Result<()> {
    match std::fs::read_to_string(health_file) {
        Ok(content) => {
            let Some((status, timestamp_str)) = content.trim().split_once(':') else {
                println!("Health check: UNHEALTHY (bad status file)");
                return Err(anyhow::anyhow!("bad health status file"));
            };

            let timestamp: u64 = timestamp_str.parse().unwrap_or(0);
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

            if status == "healthy"
                && current_time.saturating_sub(timestamp) < DEFAULT_HEALTH_STALE_WINDOW_SECONDS
            {
                println!("Health check: HEALTHY");
                Ok(())
            } else {
                println!("Health check: UNHEALTHY (stale or bad status)");
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

    pub fn from_minute(source: &str, minute_id: u64) -> Self {
        Self::from_timestamp(
            source,
            minute_id.saturating_mul(60_000) as i64,
            PartitionGranularity::Minute,
        )
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
    keep_local: bool,
}

impl S3Storage {
    pub async fn new(
        bucket: String,
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

        Ok(S3Storage {
            client,
            bucket,
            keep_local,
        })
    }

    pub async fn upload_file(&self, local_path: &Path, s3_key: &str) -> Result<()> {
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
            .key(s3_key)
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
                eprintln!("   Key: {}", s3_key);
                eprintln!("   Error Type: {}", e);
                if let Some(source) = e.source() {
                    eprintln!("   Underlying Cause: {}", source);
                }
                anyhow::anyhow!("S3 upload failed: {}", e)
            })?;

        println!(
            "✅ Uploaded {} to S3: s3://{}/{} ({:.2} MB)",
            local_path.display(),
            self.bucket,
            s3_key,
            file_size as f64 / 1_048_576.0
        );

        if !self.keep_local {
            tokio::fs::remove_file(local_path).await.with_context(|| {
                format!("Failed to remove local file: {}", local_path.display())
            })?;
            println!("🗑️  Removed local file: {}", local_path.display());
        }

        Ok(())
    }
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

fn open_writer(
    path: &Path,
    schema: &Arc<Schema>,
    compression_level: i32,
) -> Result<ArrowWriter<File>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
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
    Ok(ArrowWriter::try_new(file, schema.clone(), Some(props))
        .context("creating Parquet ArrowWriter")?)
}

fn parquet_file_name() -> String {
    let now = Utc::now();
    let counter = PARQUET_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "part-{:04}{:02}{:02}T{:02}{:02}{:02}{:03}-{:06}.parquet",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.timestamp_subsec_millis(),
        counter
    )
}

async fn upload_with_retry(storage: S3Storage, path: PathBuf, s3_key: String) {
    for attempt in 1..=DEFAULT_UPLOAD_RETRIES {
        let upload_result =
            tokio::time::timeout(Duration::from_secs(60), storage.upload_file(&path, &s3_key))
                .await;

        match upload_result {
            Ok(Ok(())) => return,
            Ok(Err(error)) if attempt < DEFAULT_UPLOAD_RETRIES => {
                let backoff = Duration::from_secs(1_u64 << (attempt - 1));
                eprintln!(
                    "Upload attempt {attempt}/{DEFAULT_UPLOAD_RETRIES} failed for {}: {}. Retrying in {}s...",
                    path.display(),
                    error,
                    backoff.as_secs()
                );
                tokio::time::sleep(backoff).await;
            }
            Err(_) if attempt < DEFAULT_UPLOAD_RETRIES => {
                let backoff = Duration::from_secs(1_u64 << (attempt - 1));
                eprintln!(
                    "Upload attempt {attempt}/{DEFAULT_UPLOAD_RETRIES} timed out for {}. Retrying in {}s...",
                    path.display(),
                    backoff.as_secs()
                );
                tokio::time::sleep(backoff).await;
            }
            Ok(Err(error)) => {
                eprintln!("⚠️  Background upload failed after retries: {}", error);
                eprintln!("   📁 File preserved on disk for retry: {}", path.display());
                return;
            }
            Err(_) => {
                eprintln!(
                    "⚠️  Background upload timed out after retries. File preserved on disk: {}",
                    path.display()
                );
                return;
            }
        }
    }
}

fn spawn_upload_worker(
    s3_storage: Option<S3Storage>,
) -> (
    Option<mpsc::Sender<(PathBuf, String)>>,
    Option<tokio::task::JoinHandle<()>>,
) {
    let Some(s3_storage_worker) = s3_storage else {
        return (None, None);
    };

    let (tx, mut rx) = mpsc::channel::<(PathBuf, String)>(DEFAULT_UPLOAD_QUEUE_CAPACITY);
    let worker = tokio::spawn(async move {
        let semaphore = Arc::new(Semaphore::new(DEFAULT_UPLOAD_CONCURRENCY));
        let mut uploads = JoinSet::new();

        while let Some((path, s3_key)) = rx.recv().await {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let storage = s3_storage_worker.clone();
            uploads.spawn(async move {
                let _permit = permit;
                upload_with_retry(storage, path, s3_key).await;
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

fn spawn_write_worker(
    shutdown: Arc<AtomicBool>,
    upload_tx: Option<mpsc::Sender<(PathBuf, String)>>,
    log_writes: bool,
) -> (
    Option<mpsc::Sender<WriteJob>>,
    Option<tokio::task::JoinHandle<Result<()>>>,
) {
    let (tx, rx) = mpsc::channel::<WriteJob>(DEFAULT_WRITE_QUEUE_CAPACITY);
    let worker_count = default_write_worker_limit();

    let worker = tokio::spawn(async move {
        let mut workers = Vec::with_capacity(worker_count);
        let shared_rx = Arc::new(tokio::sync::Mutex::new(rx));

        for _ in 0..worker_count {
            let shared_rx = shared_rx.clone();
            let shutdown = shutdown.clone();
            let upload_tx = upload_tx.clone();
            workers.push(tokio::spawn(async move {
                loop {
                    if shutdown.load(Ordering::SeqCst) {
                        break;
                    }

                    let next_job = {
                        let mut rx = shared_rx.lock().await;
                        rx.recv().await
                    };

                    let Some(job) = next_job else {
                        break;
                    };

                    if let Err(error) = write_batch_job(job, upload_tx.as_ref(), log_writes).await {
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

    (Some(tx), Some(worker))
}

struct WriteJob {
    path: PathBuf,
    s3_key: String,
    batch: RecordBatch,
    compression_level: i32,
}

async fn write_batch_job(
    job: WriteJob,
    upload_tx: Option<&mpsc::Sender<(PathBuf, String)>>,
    log_writes: bool,
) -> Result<()> {
    let WriteJob {
        path,
        s3_key,
        batch,
        compression_level,
    } = job;
    let schema = batch.schema();
    let batch_rows = batch.num_rows();
    let temp_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("part.parquet")
    ));
    let final_path = path.clone();

    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut writer = open_writer(&temp_path, &schema, compression_level)?;
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

    if log_writes {
        println!(
            "✅ Wrote {} rows to {} (queued for upload)",
            format_count(batch_rows),
            path.display()
        );
    }

    if let Some(upload_tx) = upload_tx {
        upload_tx
            .send((path, s3_key))
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
    write_tx: Option<&mpsc::Sender<WriteJob>>,
    compression_level: i32,
    shutdown: &AtomicBool,
    _log_writes: bool,
) -> Result<()> {
    let dir = key.dir_path(root);
    let filename = parquet_file_name();
    let path = dir.join(&filename);
    let batch = buf.to_record_batch()?;
    let s3_key = key.s3_key(&filename);
    let Some(write_tx) = write_tx else {
        return Err(anyhow::anyhow!("missing write queue"));
    };

    let mut job = Some(WriteJob {
        path,
        s3_key,
        batch,
        compression_level,
    });

    loop {
        if shutdown.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("write queue interrupted by shutdown"));
        }

        match write_tx.try_send(job.take().expect("write job missing")) {
            Ok(()) => break,
            Err(mpsc::error::TrySendError::Full(returned_job)) => {
                job = Some(returned_job);
                tokio::task::yield_now().await;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(anyhow::anyhow!("write queue closed"));
            }
        }
    }

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

fn parse_bool_env(name: &str) -> Option<bool> {
    std::env::var(name)
        .ok()
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "true" | "1"))
}

fn now_unix_duration() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
}
