use anyhow::{Context, Result};
use clap::Parser;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::net::TcpStream;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::{SystemTime, UNIX_EPOCH, Duration};

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client as S3Client, Config as S3Config};
use aws_sdk_s3::primitives::ByteStream;

use chrono::{Datelike, Timelike, Utc};

mod tui;

use arrow::array::{StringBuilder, TimestampMillisecondBuilder, TimestampMillisecondArray};
use arrow::array::ArrayBuilder; // Import trait for .len()
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::basic::Compression;

#[derive(Parser, Debug)]
#[command(version, about="Stream a text file or TCP feed into Hive-partitioned Parquet with Zstd compression")]
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
    #[arg(short='o', long, default_value = "data")]
    out_dir: PathBuf,

    /// Max rows to buffer per Parquet file before flush (default: flush on minute boundary only)
    #[arg(long)]
    max_rows: Option<usize>,

    /// Seconds to wait for background uploads on shutdown
    #[arg(long, default_value_t = 60)]
    upload_drain_timeout_seconds: u64,

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
    fn from_now(source: &str) -> Self {
        let now = Utc::now();
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
        format!("source={}/year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}/{}", 
                self.source, self.year, self.month, self.day, self.hour, self.minute, filename)
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
        let mut config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(region));

        // Handle custom endpoint (for MinIO)
        if let Some(mut endpoint_url) = endpoint {
            // If TLS is disabled and endpoint doesn't specify protocol, ensure it uses http://
            if disable_tls && !endpoint_url.starts_with("http://") && !endpoint_url.starts_with("https://") {
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
        let s3_config = S3Config::from(&config).to_builder()
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
        let file_metadata = tokio::fs::metadata(local_path).await
            .with_context(|| format!("Failed to read file metadata: {}", local_path.display()))?;
        let file_size = file_metadata.len();
        
        let file_content = tokio::fs::read(local_path).await
            .with_context(|| format!("Failed to read file (size: {} bytes): {}", file_size, local_path.display()))?;

        let body = ByteStream::from(file_content);

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
                eprintln!("   Size: {} bytes ({:.2} MB)", file_size, file_size as f64 / 1_048_576.0);
                eprintln!("   Bucket: s3://{}", self.bucket);
                eprintln!("   Key: {}", s3_key);
                eprintln!("   Error Type: {}", e);
                if let Some(source) = e.source() {
                    eprintln!("   Underlying Cause: {}", source);
                }
                anyhow::anyhow!("S3 upload failed: {}", e)
            })?;

        println!("✅ Uploaded {} to S3: s3://{}/{} ({:.2} MB)", 
                 local_path.display(), self.bucket, s3_key, file_size as f64 / 1_048_576.0);

        // Remove local file if not keeping it
        if !self.keep_local {
            tokio::fs::remove_file(local_path).await
                .with_context(|| format!("Failed to remove local file: {}", local_path.display()))?;
            println!("🗑️  Removed local file: {}", local_path.display());
        }

        Ok(())
    }
}


struct BatchBuf {
    ts: TimestampMillisecondBuilder,
    payload: StringBuilder,
    schema: std::sync::Arc<Schema>,
}

impl BatchBuf {
    fn new() -> Self {
        let fields = vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("payload", DataType::Utf8, false),
        ];
        BatchBuf {
            ts: TimestampMillisecondBuilder::new(),
            payload: StringBuilder::new(),
            schema: std::sync::Arc::new(Schema::new(fields)),
        }
    }

    fn push(&mut self, ts_ms: i64, payload: &str) {
        self.ts.append_value(ts_ms);
        self.payload.append_value(payload);
    }

    fn len(&self) -> usize {
        self.ts.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn to_record_batch(&mut self) -> Result<RecordBatch> {
        let ts_array = std::mem::take(&mut self.ts).finish();
        let ts = std::sync::Arc::new(
            TimestampMillisecondArray::from_iter_values(ts_array.iter().map(|v| v.unwrap()))
                .with_timezone_opt(Some(std::sync::Arc::from("UTC")))
        ) as std::sync::Arc<dyn arrow::array::Array>;
        let payload = std::sync::Arc::new(std::mem::take(&mut self.payload).finish()) as std::sync::Arc<dyn arrow::array::Array>;
        RecordBatch::try_new(self.schema.clone(), vec![ts, payload])
            .context("building RecordBatch")
    }
}

fn open_writer(path: &Path, schema: &std::sync::Arc<Schema>) -> Result<ArrowWriter<File>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::default()))
        .build();
    Ok(ArrowWriter::try_new(file, schema.clone(), Some(props))
        .context("creating Parquet ArrowWriter")?)
}

fn parquet_file_name() -> String {
    // Unique-ish name per flush
    let now = Utc::now();
    format!("part-{:04}{:02}{:02}T{:02}{:02}{:02}{:03}.parquet",
        now.year(), now.month(), now.day(),
        now.hour(), now.minute(), now.second(), now.timestamp_subsec_millis())
}

fn update_health_status(healthy: bool) -> Result<()> {
    let health_file = "/tmp/app_health";
    let status = if healthy { "healthy" } else { "unhealthy" };
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let content = format!("{}:{}", status, timestamp);
    
    std::fs::write(health_file, content)
        .with_context(|| "Failed to write health status file")?;
    Ok(())
}

fn check_health() -> Result<()> {
    let health_file = "/tmp/app_health";
    
    // Check if health file exists and is recent (within last 60 seconds)
    match std::fs::read_to_string(health_file) {
        Ok(content) => {
            let parts: Vec<&str> = content.trim().split(':').collect();
            if parts.len() != 2 {
                std::process::exit(1);
            }
            
            let status = parts[0];
            let timestamp: u64 = parts[1].parse().unwrap_or(0);
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

    if args.upload_drain_timeout_seconds == 60 {
        if let Ok(timeout_env) = std::env::var("UPLOAD_DRAIN_TIMEOUT_SECONDS") {
            if let Ok(parsed) = timeout_env.parse::<u64>() {
                args.upload_drain_timeout_seconds = parsed;
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
            args.s3_disable_tls = disable_tls_env.to_lowercase() == "true" || disable_tls_env == "1";
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
    println!("   Source Name: {}", args.source.as_deref().unwrap_or("(auto-detect)"));
    println!("   Output Directory: {}", args.out_dir.display());
    if let Some(max) = args.max_rows {
        println!("   Max Rows Per File: {}", max);
    } else {
        println!("   Max Rows Per File: unlimited (flush on minute boundary)");
    }
    println!("   Upload Drain Timeout: {}s", args.upload_drain_timeout_seconds);
    
    println!("\n☁️  S3 Configuration:");
    if let Some(bucket) = &args.s3_bucket {
        println!("   Bucket: {}", bucket);
        println!("   Region: {}", args.s3_region);
        if let Some(endpoint) = &args.s3_endpoint {
            println!("   Endpoint: {}", endpoint);
        }
        println!("   TLS/HTTPS: {}", if args.s3_disable_tls { "disabled (HTTP)" } else { "enabled (HTTPS)" });
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
        Some(S3Storage::new(
            bucket,
            args.s3_region.clone(),
            args.s3_endpoint.clone(),
            args.s3_access_key.clone(),
            args.s3_secret_key.clone(),
            args.keep_local,
            args.s3_disable_tls,
        ).await?)
    } else {
        None
    };

    // Determine source name and create appropriate reader for file/TCP
    let (source, reader): (String, Box<dyn BufRead>) = match (&args.input, &args.tcp_host, &args.tcp_port) {
        (Some(input_path), None, None) => {
            // File input
            let source = args.source.unwrap_or_else(|| {
                input_path.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string()
            });
            let file = File::open(input_path)
                .with_context(|| format!("open input {}", input_path.display()))?;
            (source, Box::new(BufReader::new(file)))
        },
        (None, Some(host), Some(port)) => {
            // TCP input
            let source = args.source.unwrap_or_else(|| "tcp".to_string());
            let stream = TcpStream::connect(format!("{}:{}", host, port))
                .with_context(|| format!("connect to TCP {}:{}", host, port))?;
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .context("set TCP read timeout")?;
            (source, Box::new(BufReader::new(stream)))
        },
        _ => {
            return Err(anyhow::anyhow!("Must specify either --input <file> or --tcp-host <host> --tcp-port <port>"));
        }
    };

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
        if let Err(e) = update_health_status(false) {
            eprintln!("Failed to update health status: {}", e);
        }
    });

    let mut current_key = PartKey::from_now(&source);
    let mut buf = BatchBuf::new();
    let mut rows_in_file = 0usize;
    let mut processed_count = 0usize;

    // Create channel for background uploads (buffer up to 10 pending uploads)
    let (upload_tx, mut upload_rx) = tokio::sync::mpsc::channel::<(PathBuf, String, bool)>(10);
    
    // Spawn background upload worker
    let s3_storage_worker = s3_storage.clone();
    let _upload_worker = tokio::spawn(async move {
        while let Some((path, s3_key, should_keep_local)) = upload_rx.recv().await {
            if let Some(s3) = &s3_storage_worker {
                match s3.upload_file(&path, &s3_key).await {
                    Ok(_) => {
                        if !should_keep_local {
                            let _ = std::fs::remove_file(&path);
                            println!("🗑️  Removed local file: {}", path.display());
                        }
                    }
                    Err(e) => {
                        eprintln!("⚠️  Background upload failed: {}", e);
                        eprintln!("   📁 File preserved on disk for retry: {}", path.display());
                    }
                }
            }
        }
    });

    // Mark as healthy when starting
    update_health_status(true)?;

    let mut reader = reader;
    let mut line = String::new();
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        line.clear();
        let read_result = reader.read_line(&mut line);
        let bytes = match read_result {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                if e.kind() == io::ErrorKind::TimedOut || e.kind() == io::ErrorKind::WouldBlock {
                    continue;
                }
                return Err(e.into());
            }
        };

        if bytes == 0 {
            break;
        }

        let payload = line.trim_end_matches(&['\r', '\n'][..]).to_string();
        
        // Echo the payload to stdout as it's received
        // println!("{}", payload);
        
        let now = Utc::now();
        let key = PartKey {
            source: source.clone(),
            year: now.year(),
            month: now.month(),
            day: now.day(),
            hour: now.hour(),
            minute: now.minute(),
        };

        // If the minute boundary (or source) changed, flush.
        if key != current_key && !buf.is_empty() {
            flush_batch(&args.out_dir, &current_key, &mut buf, &s3_storage, &upload_tx).await?;
            rows_in_file = 0;
            current_key = key.clone();
        } else {
            // Keep current key if unchanged
            current_key = key.clone();
        }

        buf.push(now.timestamp_millis(), &payload);
        rows_in_file += 1;
        processed_count += 1;

        // Update health status every 100 processed lines
        if processed_count % 100 == 0 {
            update_health_status(true).unwrap_or_else(|e| eprintln!("Failed to update health status: {}", e));
        }

        if let Some(max_rows) = args.max_rows {
            if rows_in_file >= max_rows {
                flush_batch(&args.out_dir, &current_key, &mut buf, &s3_storage, &upload_tx).await?;
                rows_in_file = 0;
            }
        }
    }

    if !buf.is_empty() {
        flush_batch(&args.out_dir, &current_key, &mut buf, &s3_storage, &upload_tx).await?;
    }

    // Close the upload channel and wait for all uploads to complete
    drop(upload_tx);
    println!("Waiting for background uploads to complete...");
    let drain_timeout = Duration::from_secs(args.upload_drain_timeout_seconds);
    match tokio::time::timeout(drain_timeout, _upload_worker).await {
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

    Ok(())
}

async fn flush_batch(
    root: &Path, 
    key: &PartKey, 
    buf: &mut BatchBuf, 
    s3_storage: &Option<S3Storage>,
    upload_tx: &tokio::sync::mpsc::Sender<(PathBuf, String, bool)>
) -> Result<()> {
    let dir = key.dir_path(root);
    let filename = parquet_file_name();
    let path = dir.join(&filename);
    let mut writer = open_writer(&path, &buf.schema)?;
    let batch = buf.to_record_batch()?;
    writer.write(&batch).context("writing batch to Parquet")?;
    writer.close().context("closing Parquet writer")?;
    
    println!("✅ Wrote {} rows to {} (queued for upload)", batch.num_rows(), path.display());

    // Queue upload to S3 if configured (non-blocking)
    if s3_storage.is_some() {
        let s3_key = key.s3_key(&filename);
        let should_keep_local = s3_storage.as_ref().map(|s| s.keep_local).unwrap_or(true);
        
        // Send to background worker (non-blocking)
        upload_tx.send((path, s3_key, should_keep_local)).await
            .context("Failed to queue upload task")?;
    }

    Ok(())
}
