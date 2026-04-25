use anyhow::{Context, Result};
use arrow::array::{ArrayBuilder, StringBuilder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client as S3Client, Config as S3Config};
use chrono::{Datelike, Timelike, Utc};
use clap::Args;
use futures_util::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
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
const DEFAULT_S3_REGION: &str = "us-east-1";
const DEFAULT_HEALTH_STALE_WINDOW_SECONDS: u64 = 60;
const DEFAULT_UPLOAD_QUEUE_CAPACITY: usize = 128;
const DEFAULT_UPLOAD_CONCURRENCY: usize = 4;
const DEFAULT_UPLOAD_RETRIES: usize = 3;
const HEALTH_UPDATE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Args)]
pub struct CommonCliArgs {
    /// Output root directory
    #[arg(short = 'o', long, default_value = "data")]
    pub out_dir: PathBuf,

    /// Max rows to buffer per Parquet file before flush (default: flush on minute boundary only)
    #[arg(long)]
    pub max_rows: Option<usize>,

    /// Max payload bytes to buffer per Parquet file before flush
    #[arg(long)]
    pub max_batch_bytes: Option<usize>,

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
    pub max_rows: Option<usize>,
    pub max_batch_bytes: usize,
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

    fn timestamp_for_payload(&self, _payload: &str) -> Option<i64> {
        None
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
            max_rows: self.max_rows,
            max_batch_bytes: self.max_batch_bytes.unwrap_or(DEFAULT_MAX_BATCH_BYTES),
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
    use super::format_count;

    #[test]
    fn formats_counts_with_thousands_separators() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(1_234), "1,234");
        assert_eq!(format_count(12_345_678), "12,345,678");
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

    let shutdown = Arc::new(AtomicBool::new(false));
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
    let mut current_minute_id = minute_id_from_millis(now.as_millis() as i64);
    let mut current_key = PartKey::from_minute(source.source_name(), current_minute_id);
    let mut buf = BatchBuf::new(common.max_rows, common.max_batch_bytes);
    let mut rows_in_file = 0usize;
    let mut rows_processed = 0usize;
    let mut files_flushed = 0usize;

    let (upload_tx, upload_worker) = spawn_upload_worker(s3_storage.clone());

    update_health_status_async(&health_file, true).await?;

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
                if let Err(error) = update_health_status_async(&health_file, true).await {
                    eprintln!("Failed to update health status: {}", error);
                }
            }
            _ = progress_heartbeat.tick() => {
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
                        let duration = now_unix_duration();
                        let now_ms = duration.as_millis() as i64;
                        let row_ts_ms = source.timestamp_for_payload(&payload).unwrap_or(now_ms);
                        let minute_id = minute_id_from_millis(row_ts_ms);
                        let payload_len = payload.len();

                        if minute_id != current_minute_id {
                            if !buf.is_empty() {
                                flush_batch(&common.out_dir, &current_key, &mut buf, upload_tx.as_ref()).await?;
                                rows_in_file = 0;
                                files_flushed += 1;
                            }
                            current_minute_id = minute_id;
                            current_key = PartKey::from_minute(source.source_name(), current_minute_id);
                        }

                        // Keep each batch well below Arrow's string offset limit.
                        if buf.would_exceed_batch_bytes(payload_len, common.max_batch_bytes) {
                            flush_batch(&common.out_dir, &current_key, &mut buf, upload_tx.as_ref()).await?;
                            rows_in_file = 0;
                            files_flushed += 1;
                        }

                        buf.push(row_ts_ms, &payload);
                        rows_in_file += 1;
                        rows_processed += 1;

                        if buf.payload_bytes() >= common.max_batch_bytes {
                            flush_batch(&common.out_dir, &current_key, &mut buf, upload_tx.as_ref()).await?;
                            rows_in_file = 0;
                            files_flushed += 1;
                        }

                        if let Some(max_rows) = common.max_rows {
                            if rows_in_file >= max_rows {
                                flush_batch(&common.out_dir, &current_key, &mut buf, upload_tx.as_ref()).await?;
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
        flush_batch(&common.out_dir, &current_key, &mut buf, upload_tx.as_ref()).await?;
    }

    if let Some(upload_tx) = upload_tx {
        drop(upload_tx);
        if let Some(upload_worker) = upload_worker {
            println!("Waiting for background uploads to complete...");
            let drain_timeout = Duration::from_secs(common.upload_drain_timeout_seconds);
            match tokio::time::timeout(drain_timeout, upload_worker).await {
                Ok(join_result) => {
                    if let Err(error) = join_result {
                        eprintln!("Upload worker failed: {}", error);
                    } else {
                        println!("All uploads completed");
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

    update_health_status_async(&health_file, false).await?;
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
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
}

impl PartKey {
    pub fn from_minute(source: &str, minute_id: u64) -> Self {
        let minute_time = UNIX_EPOCH + Duration::from_secs(minute_id * 60);
        let now: chrono::DateTime<Utc> = minute_time.into();
        PartKey {
            source: source.to_string(),
            year: now.year(),
            month: now.month(),
            day: now.day(),
            hour: now.hour(),
            minute: now.minute(),
        }
    }

    fn dir_path(&self, root: &Path) -> PathBuf {
        root.join(format!("source={}", self.source))
            .join(format!("year={:04}", self.year))
            .join(format!("month={:02}", self.month))
            .join(format!("day={:02}", self.day))
            .join(format!("hour={:02}", self.hour))
            .join(format!("minute={:02}", self.minute))
    }

    fn s3_key(&self, filename: &str) -> String {
        format!(
            "source={}/year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}/{}",
            self.source, self.year, self.month, self.day, self.hour, self.minute, filename
        )
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

fn open_writer(path: &Path, schema: &Arc<Schema>) -> Result<ArrowWriter<File>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let zstd_level = ZstdLevel::try_new(5).unwrap_or_default();
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
    format!(
        "part-{:04}{:02}{:02}T{:02}{:02}{:02}{:03}.parquet",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.timestamp_subsec_millis()
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

async fn flush_batch(
    root: &Path,
    key: &PartKey,
    buf: &mut BatchBuf,
    upload_tx: Option<&mpsc::Sender<(PathBuf, String)>>,
) -> Result<()> {
    let dir = key.dir_path(root);
    let filename = parquet_file_name();
    let path = dir.join(&filename);
    let batch = buf.to_record_batch()?;
    let schema = buf.schema.clone();
    let path_for_write = path.clone();
    let batch_rows = batch.num_rows();

    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut writer = open_writer(&path_for_write, &schema)?;
        writer.write(&batch).context("writing batch to Parquet")?;
        writer.close().context("closing Parquet writer")?;
        Ok(())
    })
    .await
    .context("joining parquet write task")??;

    println!(
        "✅ Wrote {} rows to {} (queued for upload)",
        format_count(batch_rows),
        path.display()
    );

    if let Some(upload_tx) = upload_tx {
        let s3_key = key.s3_key(&filename);
        upload_tx
            .send((path, s3_key))
            .await
            .context("Failed to queue upload task")?;
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

fn minute_id_from_millis(timestamp_ms: i64) -> u64 {
    timestamp_ms.max(0) as u64 / 1_000 / 60
}
