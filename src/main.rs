use anyhow::{Context, Result};
use clap::Parser;
use futures_util::StreamExt;
use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::AsyncRead;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client as S3Client, Config as S3Config};

use chrono::{Datelike, Timelike, Utc};

mod tui;

use arrow::array::ArrayBuilder; // Import trait for .len()
use arrow::array::{StringBuilder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Stream a text file or TCP feed into Hive-partitioned Parquet with Zstd compression"
)]
struct Args {
    /// Input text file (one record per line)
    #[arg(short, long, conflicts_with_all = ["tcp_host", "tcp_port"])]
    input: Option<PathBuf>,

    /// TCP host address to receive data from (e.g., 153.44.253.27)
    #[arg(long, requires = "tcp_port", conflicts_with = "input")]
    tcp_host: Option<String>,

    /// TCP port to receive data from (e.g., 5631)
    #[arg(long, requires = "tcp_host", conflicts_with = "input")]
    tcp_port: Option<u16>,

    /// Logical source label; defaults to input file stem or "tcp" for network input
    #[arg(short, long)]
    source: Option<String>,

    /// Output root directory
    #[arg(short = 'o', long, default_value = "data")]
    out_dir: PathBuf,

    /// Max rows to buffer per Parquet file before flush (default: flush on minute boundary only)
    #[arg(long)]
    max_rows: Option<usize>,

    /// Max payload bytes to buffer per Parquet file before flush
    #[arg(long, default_value_t = DEFAULT_MAX_PAYLOAD_BYTES)]
    max_payload_bytes: usize,

    /// Seconds to wait for background uploads on shutdown
    #[arg(long, default_value_t = 60)]
    upload_drain_timeout_seconds: u64,

    /// Maximum bytes allowed per input line before dropping it
    #[arg(long, default_value_t = 65_536)]
    max_line_length: usize,

    /// Run health check and exit (for Docker HEALTHCHECK)
    #[arg(long)]
    health_check: bool,

    /// S3 bucket name for remote storage (enables S3 upload)
    #[arg(long)]
    s3_bucket: Option<String>,

    /// S3 endpoint URL (for MinIO or custom S3-compatible storage)
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 region (default: us-east-1)
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,

    /// S3 access key ID (can also use AWS_ACCESS_KEY_ID env var)
    #[arg(long)]
    s3_access_key: Option<String>,

    /// S3 secret access key (can also use AWS_SECRET_ACCESS_KEY env var)
    #[arg(long)]
    s3_secret_key: Option<String>,

    /// Keep local files after S3 upload (default: delete after successful upload)
    #[arg(long)]
    keep_local: bool,

    /// Disable TLS/HTTPS for S3 endpoint (use plain HTTP instead)
    #[arg(long)]
    s3_disable_tls: bool,

    /// Launch interactive TUI for configuration
    #[arg(long)]
    tui: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct PartKey {
    source: String,
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
}

impl PartKey {
    fn from_minute(source: &str, minute_id: u64) -> Self {
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

        // Handle custom endpoint (for MinIO)
        if let Some(mut endpoint_url) = endpoint {
            // If TLS is disabled and endpoint doesn't specify protocol, ensure it uses http://
            if disable_tls
                && !endpoint_url.starts_with("http://")
                && !endpoint_url.starts_with("https://")
            {
                endpoint_url = format!("http://{}", endpoint_url);
            }
            config_builder = config_builder.endpoint_url(endpoint_url);
        }

        // Handle custom credentials
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

        // Remove local file if not keeping it
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
    schema: std::sync::Arc<Schema>,
    row_capacity: usize,
    byte_capacity: usize,
    max_payload_bytes: usize,
    payload_bytes: usize,
}

const HEALTH_FILE: &str = "/tmp/app_health";
const HEALTH_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_UPLOAD_QUEUE_CAPACITY: usize = 128;
const DEFAULT_UPLOAD_CONCURRENCY: usize = 4;
const DEFAULT_UPLOAD_RETRIES: usize = 3;
const TCP_RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const TCP_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
enum InputMode {
    File {
        path: PathBuf,
        source: String,
    },
    Tcp {
        host: String,
        port: u16,
        source: String,
    },
}

impl InputMode {
    fn from_args(args: &Args) -> Result<Self> {
        match (&args.input, &args.tcp_host, &args.tcp_port) {
            (Some(input_path), None, None) => Ok(Self::File {
                path: input_path.clone(),
                source: args.source.clone().unwrap_or_else(|| {
                    input_path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("unknown")
                        .to_string()
                }),
            }),
            (None, Some(host), Some(port)) => Ok(Self::Tcp {
                host: host.clone(),
                port: *port,
                source: args.source.clone().unwrap_or_else(|| "tcp".to_string()),
            }),
            _ => Err(anyhow::anyhow!(
                "Must specify either --input <file> or --tcp-host <host> --tcp-port <port>"
            )),
        }
    }

    fn source(&self) -> &str {
        match self {
            Self::File { source, .. } | Self::Tcp { source, .. } => source,
        }
    }

    fn is_tcp(&self) -> bool {
        matches!(self, Self::Tcp { .. })
    }
}

impl BatchBuf {
    fn new(max_rows: Option<usize>, max_payload_bytes: usize) -> Self {
        let row_capacity = max_rows.unwrap_or(4096);
        let byte_capacity = row_capacity.saturating_mul(128).min(max_payload_bytes);
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
            schema: std::sync::Arc::new(Schema::new(fields)),
            row_capacity,
            byte_capacity,
            max_payload_bytes,
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

    fn should_flush_before_push(&self, next_payload_len: usize) -> bool {
        !self.is_empty()
            && self.payload_bytes.saturating_add(next_payload_len) > self.max_payload_bytes
    }

    fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    fn to_record_batch(&mut self) -> Result<RecordBatch> {
        let ts_array = self
            .ts
            .finish()
            .with_timezone_opt(Some(std::sync::Arc::from("UTC")));
        let payload_array = self.payload.finish();

        self.ts = TimestampMillisecondBuilder::with_capacity(self.row_capacity);
        self.payload = StringBuilder::with_capacity(self.row_capacity, self.byte_capacity);
        self.payload_bytes = 0;

        let ts = std::sync::Arc::new(ts_array) as std::sync::Arc<dyn arrow::array::Array>;
        let payload = std::sync::Arc::new(payload_array) as std::sync::Arc<dyn arrow::array::Array>;
        RecordBatch::try_new(self.schema.clone(), vec![ts, payload]).context("building RecordBatch")
    }
}

fn open_writer(path: &Path, schema: &std::sync::Arc<Schema>) -> Result<ArrowWriter<File>> {
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
    // Unique-ish name per flush
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

async fn update_health_status_async(healthy: bool) -> Result<()> {
    let status = if healthy { "healthy" } else { "unhealthy" };
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let content = format!("{}:{}", status, timestamp);

    tokio::fs::write(HEALTH_FILE, content)
        .await
        .with_context(|| "Failed to write health status file")?;
    Ok(())
}

fn check_health() -> Result<()> {
    // Check if health file exists and is recent (within last 60 seconds)
    match std::fs::read_to_string(HEALTH_FILE) {
        Ok(content) => {
            let Some((status, timestamp_str)) = content.trim().split_once(':') else {
                std::process::exit(1);
            };
            let timestamp: u64 = timestamp_str.parse().unwrap_or(0);
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

            // Check if status is healthy and timestamp is recent (within 60 seconds)
            if status == "healthy" && (current_time - timestamp) < 60 {
                println!("Health check: HEALTHY");
                std::process::exit(0);
            } else {
                println!("Health check: UNHEALTHY (stale or bad status)");
                std::process::exit(1);
            }
        }
        Err(_) => {
            println!("Health check: UNHEALTHY (no status file)");
            std::process::exit(1);
        }
    }
}

fn now_unix_duration() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
}

async fn open_input_stream(
    input_mode: &InputMode,
    max_line_length: usize,
) -> Result<FramedRead<Box<dyn AsyncRead + Unpin + Send>, LinesCodec>> {
    match input_mode {
        InputMode::File { path, .. } => {
            let file = tokio::fs::File::open(path)
                .await
                .with_context(|| format!("open input {}", path.display()))?;
            let stream: Box<dyn AsyncRead + Unpin + Send> = Box::new(file);
            Ok(FramedRead::new(
                stream,
                LinesCodec::new_with_max_length(max_line_length),
            ))
        }
        InputMode::Tcp { host, port, .. } => {
            let stream = TcpStream::connect(format!("{}:{}", host, port))
                .await
                .with_context(|| format!("connect to TCP {}:{}", host, port))?;
            stream.set_nodelay(true).context("set TCP nodelay")?;
            let stream: Box<dyn AsyncRead + Unpin + Send> = Box::new(stream);
            Ok(FramedRead::new(
                stream,
                LinesCodec::new_with_max_length(max_line_length),
            ))
        }
    }
}

async fn reconnect_input_stream(
    input_mode: &InputMode,
    shutdown: &AtomicBool,
    max_line_length: usize,
) -> Result<Option<FramedRead<Box<dyn AsyncRead + Unpin + Send>, LinesCodec>>> {
    let InputMode::Tcp { host, port, .. } = input_mode else {
        return Ok(None);
    };

    let mut delay = TCP_RECONNECT_INITIAL_DELAY;
    while !shutdown.load(Ordering::SeqCst) {
        eprintln!(
            "TCP input disconnected. Reconnecting to {}:{} in {}s...",
            host,
            port,
            delay.as_secs()
        );
        tokio::time::sleep(delay).await;

        if shutdown.load(Ordering::SeqCst) {
            return Ok(None);
        }

        match open_input_stream(input_mode, max_line_length).await {
            Ok(reader) => {
                println!("Reconnected to TCP {}:{}", host, port);
                return Ok(Some(reader));
            }
            Err(error) => {
                eprintln!("Reconnect failed for TCP {}:{}: {}", host, port, error);
                delay = std::cmp::min(delay.saturating_mul(2), TCP_RECONNECT_MAX_DELAY);
            }
        }
    }

    Ok(None)
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
        let mut uploads = tokio::task::JoinSet::new();

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

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();

    // If --tui flag is set, launch the TUI
    if args.tui {
        let initial_config = tui::TuiConfig::from_env();

        match tui::run_tui(initial_config)? {
            Some(config) => {
                // User confirmed, convert TuiConfig back to Args
                let cli_args = config.to_cli_args();
                let mut full_args = vec!["capture".to_string()];
                full_args.extend(cli_args);

                // Re-parse with TUI-generated args
                args = Args::parse_from(full_args);
            }
            None => {
                // User quit without running
                println!("Configuration cancelled.");
                return Ok(());
            }
        }
    }

    // Handle environment variables gracefully - only override if not set via CLI and env var exists

    // Input source environment variables
    if args.input.is_none() {
        if let Ok(input_env) = std::env::var("INPUT_FILE") {
            args.input = Some(PathBuf::from(input_env));
        }
    }

    if args.tcp_host.is_none() {
        if let Ok(host_env) = std::env::var("TCP_HOST") {
            args.tcp_host = Some(host_env);
        }
    }

    if args.tcp_port.is_none() {
        if let Ok(port_env) = std::env::var("TCP_PORT") {
            args.tcp_port = port_env.parse().ok();
        }
    }

    if args.source.is_none() {
        if let Ok(source_env) = std::env::var("SOURCE") {
            args.source = Some(source_env);
        }
    }

    // Check OUT_DIR environment variable (only if still default)
    if args.out_dir == PathBuf::from("data") {
        if let Ok(out_dir_env) = std::env::var("OUT_DIR") {
            args.out_dir = PathBuf::from(out_dir_env);
        }
    }

    if args.max_rows.is_none() {
        if let Ok(max_rows_env) = std::env::var("MAX_ROWS") {
            args.max_rows = max_rows_env.parse().ok();
        }
    }

    if args.max_payload_bytes == DEFAULT_MAX_PAYLOAD_BYTES {
        if let Ok(max_payload_bytes_env) = std::env::var("MAX_PAYLOAD_BYTES") {
            if let Ok(parsed) = max_payload_bytes_env.parse::<usize>() {
                args.max_payload_bytes = parsed;
            }
        }
    }

    if args.upload_drain_timeout_seconds == 60 {
        if let Ok(timeout_env) = std::env::var("UPLOAD_DRAIN_TIMEOUT_SECONDS") {
            if let Ok(parsed) = timeout_env.parse::<u64>() {
                args.upload_drain_timeout_seconds = parsed;
            }
        }
    }

    if args.max_line_length == 65_536 {
        if let Ok(max_line_length_env) = std::env::var("MAX_LINE_LENGTH") {
            if let Ok(parsed) = max_line_length_env.parse::<usize>() {
                args.max_line_length = parsed;
            }
        }
    }

    // Health check environment variable
    if !args.health_check {
        if let Ok(health_env) = std::env::var("HEALTH_CHECK") {
            args.health_check = health_env.to_lowercase() == "true" || health_env == "1";
        }
    }

    // S3 configuration environment variables
    if args.s3_bucket.is_none() {
        if let Ok(bucket_env) = std::env::var("S3_BUCKET") {
            args.s3_bucket = Some(bucket_env);
        }
    }

    if args.s3_endpoint.is_none() {
        if let Ok(endpoint_env) = std::env::var("S3_ENDPOINT") {
            args.s3_endpoint = Some(endpoint_env);
        }
    }

    // Check S3_REGION environment variable (only if still default)
    if args.s3_region == "us-east-1" {
        if let Ok(region_env) = std::env::var("S3_REGION") {
            args.s3_region = region_env;
        }
    }

    if args.s3_access_key.is_none() {
        if let Ok(access_key_env) = std::env::var("S3_ACCESS_KEY") {
            args.s3_access_key = Some(access_key_env);
        }
    }

    if args.s3_secret_key.is_none() {
        if let Ok(secret_key_env) = std::env::var("S3_SECRET_KEY") {
            args.s3_secret_key = Some(secret_key_env);
        }
    }

    if !args.keep_local {
        if let Ok(keep_local_env) = std::env::var("KEEP_LOCAL") {
            args.keep_local = keep_local_env.to_lowercase() == "true" || keep_local_env == "1";
        }
    }

    if !args.s3_disable_tls {
        if let Ok(disable_tls_env) = std::env::var("S3_DISABLE_TLS") {
            args.s3_disable_tls =
                disable_tls_env.to_lowercase() == "true" || disable_tls_env == "1";
        }
    }

    // Handle health check mode
    if args.health_check {
        check_health()?;
        return Ok(()); // This line won't be reached due to process::exit in check_health
    }

    // Echo all configuration parameters at startup
    println!("═══════════════════════════════════════════════════════════");
    println!("🚀 Capture Configuration");
    println!("═══════════════════════════════════════════════════════════");

    // Input source configuration
    println!("\n📥 Input Source:");
    if let Some(input) = &args.input {
        println!("   File: {}", input.display());
    } else if let Some(host) = &args.tcp_host {
        println!("   TCP: {}:{}", host, args.tcp_port.unwrap_or(0));
    } else {
        println!("   None specified");
    }

    println!("\n📤 Output Configuration:");
    println!(
        "   Source Name: {}",
        args.source.as_deref().unwrap_or("(auto-detect)")
    );
    println!("   Output Directory: {}", args.out_dir.display());
    if let Some(max) = args.max_rows {
        println!("   Max Rows Per File: {}", max);
    } else {
        println!("   Max Rows Per File: unlimited (flush on minute boundary)");
    }
    println!("   Max Payload Per File: {} bytes", args.max_payload_bytes);
    println!("   Max Line Length: {} bytes", args.max_line_length);
    println!(
        "   Upload Drain Timeout: {}s",
        args.upload_drain_timeout_seconds
    );

    println!("\n☁️  S3 Configuration:");
    if let Some(bucket) = &args.s3_bucket {
        println!("   Bucket: {}", bucket);
        println!("   Region: {}", args.s3_region);
        if let Some(endpoint) = &args.s3_endpoint {
            println!("   Endpoint: {}", endpoint);
        }
        println!(
            "   TLS/HTTPS: {}",
            if args.s3_disable_tls {
                "disabled (HTTP)"
            } else {
                "enabled (HTTPS)"
            }
        );
        if args.s3_access_key.is_some() {
            println!("   Access Key: *** (configured)");
        }
        if args.s3_secret_key.is_some() {
            println!("   Secret Key: *** (configured)");
        }
        println!("   Keep Local Files: {}", args.keep_local);
    } else {
        println!("   Disabled (local storage only)");
    }

    println!("═══════════════════════════════════════════════════════════\n");

    // Initialize S3 storage if bucket is specified
    let s3_storage = if let Some(bucket) = args.s3_bucket.clone() {
        println!("🔄 Initializing S3 storage for bucket: {}", bucket);
        Some(
            S3Storage::new(
                bucket,
                args.s3_region.clone(),
                args.s3_endpoint.clone(),
                args.s3_access_key.clone(),
                args.s3_secret_key.clone(),
                args.keep_local,
                args.s3_disable_tls,
            )
            .await?,
        )
    } else {
        None
    };

    let input_mode = InputMode::from_args(&args)?;
    let source = input_mode.source().to_string();
    let mut reader = open_input_stream(&input_mode, args.max_line_length).await?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(e) => {
                    eprintln!("Failed to register SIGTERM handler: {}", e);
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
    let mut current_minute_id = now.as_secs() / 60;
    let mut current_key = PartKey::from_minute(&source, current_minute_id);
    let mut buf = BatchBuf::new(args.max_rows, args.max_payload_bytes);
    let mut rows_in_file = 0usize;

    let (upload_tx, upload_worker) = spawn_upload_worker(s3_storage.clone());

    // Mark as healthy when starting
    update_health_status_async(true).await?;

    let mut heartbeat = tokio::time::interval(HEALTH_UPDATE_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        tokio::select! {
            _ = heartbeat.tick() => {
                if let Err(error) = update_health_status_async(true).await {
                    eprintln!("Failed to update health status: {}", error);
                }
            }
            maybe_line = reader.next() => {
                match maybe_line {
                    Some(Ok(payload)) => {
                        let duration = now_unix_duration();
                        let now_ms = duration.as_millis() as i64;
                        let minute_id = duration.as_secs() / 60;

                        if minute_id != current_minute_id {
                            if !buf.is_empty() {
                                flush_batch(&args.out_dir, &current_key, &mut buf, upload_tx.as_ref()).await?;
                                rows_in_file = 0;
                            }
                            current_minute_id = minute_id;
                            current_key = PartKey::from_minute(&source, current_minute_id);
                        }

                        if buf.should_flush_before_push(payload.len()) {
                            flush_batch(&args.out_dir, &current_key, &mut buf, upload_tx.as_ref()).await?;
                            rows_in_file = 0;
                        }

                        buf.push(now_ms, &payload);
                        rows_in_file += 1;

                        if buf.payload_bytes() >= args.max_payload_bytes {
                            flush_batch(&args.out_dir, &current_key, &mut buf, upload_tx.as_ref()).await?;
                            rows_in_file = 0;
                            continue;
                        }

                        if let Some(max_rows) = args.max_rows {
                            if rows_in_file >= max_rows {
                                flush_batch(&args.out_dir, &current_key, &mut buf, upload_tx.as_ref()).await?;
                                rows_in_file = 0;
                            }
                        }
                    }
                    Some(Err(LinesCodecError::MaxLineLengthExceeded)) => {
                        eprintln!(
                            "⚠️  Dropped oversized input line (>{} bytes)",
                            args.max_line_length
                        );
                    }
                    Some(Err(error)) => {
                        if input_mode.is_tcp() {
                            match reconnect_input_stream(&input_mode, &shutdown, args.max_line_length).await? {
                                Some(new_reader) => {
                                    reader = new_reader;
                                    continue;
                                }
                                None => break,
                            }
                        } else {
                            return Err(error.into());
                        }
                    }
                    None => {
                        if input_mode.is_tcp() {
                            match reconnect_input_stream(&input_mode, &shutdown, args.max_line_length).await? {
                                Some(new_reader) => {
                                    reader = new_reader;
                                    continue;
                                }
                                None => break,
                            }
                        }
                        break;
                    },
                }
            }
        }
    }

    if !buf.is_empty() {
        flush_batch(&args.out_dir, &current_key, &mut buf, upload_tx.as_ref()).await?;
    }

    // Close the upload channel and wait for all uploads to complete
    if let Some(upload_tx) = upload_tx {
        drop(upload_tx);
        if let Some(upload_worker) = upload_worker {
            println!("Waiting for background uploads to complete...");
            let drain_timeout = Duration::from_secs(args.upload_drain_timeout_seconds);
            match tokio::time::timeout(drain_timeout, upload_worker).await {
                Ok(join_result) => {
                    if let Err(e) = join_result {
                        eprintln!("Upload worker failed: {}", e);
                    } else {
                        println!("All uploads completed");
                    }
                }
                Err(_) => {
                    eprintln!(
                        "Upload drain timed out after {} seconds. Exiting.",
                        args.upload_drain_timeout_seconds
                    );
                }
            }
        }
    }

    update_health_status_async(false).await?;

    Ok(())
}

async fn flush_batch(
    root: &Path,
    key: &PartKey,
    buf: &mut BatchBuf,
    upload_tx: Option<&tokio::sync::mpsc::Sender<(PathBuf, String)>>,
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
        batch_rows,
        path.display()
    );

    // Queue upload to S3 if configured (non-blocking)
    if let Some(upload_tx) = upload_tx {
        let s3_key = key.s3_key(&filename);

        // Send to background worker (non-blocking)
        upload_tx
            .send((path, s3_key))
            .await
            .context("Failed to queue upload task")?;
    }

    Ok(())
}
