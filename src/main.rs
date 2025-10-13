use anyhow::{Context, Result};
use clap::Parser;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::net::TcpStream;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client as S3Client, Config as S3Config};
use aws_sdk_s3::primitives::ByteStream;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

use chrono::{Datelike, Timelike, Utc};

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

    /// WebSocket URL to connect to (e.g., wss://stream.aisstream.io/v0/stream)
    #[arg(long, conflicts_with_all = ["input", "tcp_host", "tcp_port"])]
    ws_url: Option<String>,

    /// API key for WebSocket authentication (for AISStream.io)
    #[arg(long, requires = "ws_url")]
    ws_api_key: Option<String>,

    /// Bounding box for WebSocket subscription (format: lat1,lon1,lat2,lon2) 
    /// Can be specified multiple times for multiple boxes. Default: entire world
    #[arg(long, requires = "ws_url")]
    ws_bbox: Vec<String>,

    /// Filter WebSocket messages by MMSI (can be specified multiple times, max 50)
    #[arg(long, requires = "ws_url")]
    ws_mmsi_filter: Vec<String>,

    /// Filter WebSocket messages by message type (e.g., PositionReport)
    #[arg(long, requires = "ws_url")]
    ws_message_type_filter: Vec<String>,

    /// Enable debug mode for WebSocket connections
    #[arg(long)]
    ws_debug: bool,
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
    ) -> Result<Self> {
        let mut config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(region));

        // Handle custom endpoint (for MinIO)
        if let Some(endpoint_url) = endpoint {
            config_builder = config_builder.endpoint_url(endpoint_url);
        }

        // Handle custom credentials
        if let (Some(access), Some(secret)) = (access_key, secret_key) {
            let credentials = Credentials::new(access, secret, None, None, "manual");
            config_builder = config_builder.credentials_provider(credentials);
        }

        let config = config_builder.load().await;
        let s3_config = S3Config::from(&config);
        let client = S3Client::from_conf(s3_config);

        Ok(S3Storage {
            client,
            bucket,
            keep_local,
        })
    }

    pub async fn upload_file(&self, local_path: &Path, s3_key: &str) -> Result<()> {
        let file_content = tokio::fs::read(local_path).await
            .with_context(|| format!("Failed to read file: {}", local_path.display()))?;

        let body = ByteStream::from(file_content);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .body(body)
            .send()
            .await
            .with_context(|| format!("Failed to upload {} to S3", s3_key))?;

        println!("✅ Uploaded {} to S3: s3://{}/{}", local_path.display(), self.bucket, s3_key);

        // Remove local file if not keeping it
        if !self.keep_local {
            tokio::fs::remove_file(local_path).await
                .with_context(|| format!("Failed to remove local file: {}", local_path.display()))?;
            println!("🗑️  Removed local file: {}", local_path.display());
        }

        Ok(())
    }
}

#[derive(Serialize, Debug)]
struct WebSocketSubscription {
    #[serde(rename = "APIKey")]
    api_key: String,
    #[serde(rename = "BoundingBoxes")]
    bounding_boxes: Vec<Vec<Vec<f64>>>,
    #[serde(rename = "FiltersShipMMSI", skip_serializing_if = "Vec::is_empty")]
    filters_ship_mmsi: Vec<String>,
    #[serde(rename = "FilterMessageTypes", skip_serializing_if = "Vec::is_empty")]
    filter_message_types: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct AISMessage {
    #[serde(rename = "MessageType")]
    message_type: String,
    #[serde(rename = "Message")]
    message: serde_json::Value,
    #[serde(rename = "MetaData")]
    metadata: Option<serde_json::Value>,
}

pub struct WebSocketClient {
    url: String,
    subscription: WebSocketSubscription,
    debug: bool,
}

impl WebSocketClient {
    pub fn new(
        url: String,
        api_key: String,
        bounding_boxes: Vec<String>,
        mmsi_filters: Vec<String>,
        message_type_filters: Vec<String>,
        debug: bool,
    ) -> Result<Self> {
        // Parse bounding boxes from "lat1,lon1,lat2,lon2" format
        let parsed_boxes: Result<Vec<Vec<Vec<f64>>>, _> = bounding_boxes
            .iter()
            .map(|bbox| {
                let coords: Vec<f64> = bbox.split(',').map(|s| s.parse()).collect::<Result<Vec<_>, _>>()?;
                if coords.len() != 4 {
                    return Err(anyhow::anyhow!("Bounding box must have 4 coordinates: lat1,lon1,lat2,lon2"));
                }
                Ok(vec![vec![coords[0], coords[1]], vec![coords[2], coords[3]]])
            })
            .collect();

        let bounding_boxes = match parsed_boxes {
            Ok(boxes) if !boxes.is_empty() => boxes,
            _ => {
                println!("⚠️  No valid bounding boxes specified, using entire world: [[-90,-180],[90,180]]");
                vec![vec![vec![-90.0, -180.0], vec![90.0, 180.0]]]
            }
        };

        let subscription = WebSocketSubscription {
            api_key,
            bounding_boxes,
            filters_ship_mmsi: mmsi_filters,
            filter_message_types: message_type_filters,
        };

        Ok(WebSocketClient { url, subscription, debug })
    }

    pub async fn connect_and_stream(&self) -> Result<impl futures_util::Stream<Item = Result<String>>> {
        // Add connection timeout and retry logic
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY: u64 = 5; // seconds

        loop {
            match self.try_connect().await {
                Ok(stream) => return Ok(stream),
                Err(e) => {
                    retry_count += 1;
                    if retry_count > MAX_RETRIES {
                        return Err(e.context(format!("Failed to connect after {} attempts", MAX_RETRIES)));
                    }
                    
                    eprintln!("⚠️  WebSocket connection failed (attempt {}/{}): {}", retry_count, MAX_RETRIES, e);
                    eprintln!("🔄 Retrying in {} seconds...", RETRY_DELAY);
                    
                    tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_DELAY)).await;
                }
            }
        }
    }

    async fn try_connect(&self) -> Result<impl futures_util::Stream<Item = Result<String>>> {
        println!("🔄 Attempting WebSocket connection to: {}", self.url);
        
        // Connect with timeout
        let connect_future = connect_async(&self.url);
        let (ws_stream, response) = tokio::time::timeout(
            tokio::time::Duration::from_secs(30),
            connect_future
        ).await
        .context("WebSocket connection timed out after 30 seconds")?
        .with_context(|| format!("Failed to connect to WebSocket: {}", self.url))?;

        println!("✅ WebSocket connected, response status: {:?}", response.status());

        let (mut write, read) = ws_stream.split();

        // Send subscription message
        let subscription_json = serde_json::to_string(&self.subscription)
            .context("Failed to serialize subscription message")?;
        
        println!("� Sending WebSocket subscription: {}", subscription_json);
        
        // Send subscription with timeout
        let send_future = write.send(Message::Text(subscription_json));
        tokio::time::timeout(
            tokio::time::Duration::from_secs(10),
            send_future
        ).await
        .context("Subscription message send timed out")?
        .context("Failed to send subscription message")?;

        println!("✅ Subscription message sent successfully");

        // Capture debug flag to avoid borrowing self in closure
        let debug = self.debug;

        // Return the read stream with improved error handling
        Ok(read.filter_map(move |msg| async move {
            match msg {
                Ok(Message::Text(text)) => {
                    // Log first few messages for debugging
                    static mut MESSAGE_COUNT: u32 = 0;
                    unsafe {
                        MESSAGE_COUNT += 1;
                        if debug || MESSAGE_COUNT <= 3 {
                            println!("📨 WebSocket message #{}: {}", MESSAGE_COUNT, 
                                if text.len() > 200 { format!("{}...", &text[..200]) } else { text.clone() });
                        }
                    }
                    
                    // Check for authentication/error messages first
                    if text.contains("error") || text.contains("Error") || text.contains("unauthorized") || text.contains("Unauthorized") {
                        eprintln!("❌ WebSocket authentication/error: {}", text);
                        return Some(Err(anyhow::anyhow!("WebSocket authentication error: {}", text)));
                    }
                    
                    // Check for success/acknowledgment messages
                    if text.contains("subscribed") || text.contains("success") || text.contains("acknowledged") {
                        println!("✅ WebSocket subscription acknowledged: {}", text);
                        return None; // Don't process as data
                    }
                    
                    // Try to parse as AIS message
                    match serde_json::from_str::<AISMessage>(&text) {
                        Ok(_ais_msg) => {
                            if debug {
                                println!("📊 Processing valid AIS message");
                            }
                            Some(Ok(text)) // Valid AIS message
                        },
                        Err(parse_error) => {
                            // Log parsing errors for first few messages only to avoid spam
                            static mut ERROR_COUNT: u32 = 0;
                            unsafe {
                                ERROR_COUNT += 1;
                                if debug || ERROR_COUNT <= 5 {
                                    eprintln!("⚠️  Message parsing error #{}: {} | Message: {}", 
                                        ERROR_COUNT, parse_error, 
                                        if text.len() > 100 { format!("{}...", &text[..100]) } else { text });
                                } else if ERROR_COUNT == 6 && !debug {
                                    eprintln!("⚠️  Suppressing further parsing error messages (use --ws-debug for all errors)...");
                                }
                            }
                            None // Skip unparseable messages but continue
                        }
                    }
                },
                Ok(Message::Close(close_frame)) => {
                    let reason = close_frame.as_ref()
                        .map(|cf| format!("code: {}, reason: {}", cf.code, cf.reason))
                        .unwrap_or_else(|| "no reason provided".to_string());
                    eprintln!("🔌 WebSocket connection closed: {}", reason);
                    Some(Err(anyhow::anyhow!("WebSocket connection closed: {}", reason)))
                },
                Ok(Message::Ping(_)) => {
                    // Ping messages are handled automatically by the WebSocket library
                    None
                },
                Ok(Message::Pong(_)) => {
                    // Pong responses
                    None
                },
                Ok(Message::Binary(data)) => {
                    if debug {
                        println!("📦 Received binary WebSocket message ({} bytes)", data.len());
                    }
                    
                    // Try to decode binary data as UTF-8 text
                    match String::from_utf8(data.clone()) {
                        Ok(text) => {
                            if debug {
                                println!("📝 Binary message decoded as text: {}", 
                                    if text.len() > 200 { format!("{}...", &text[..200]) } else { text.clone() });
                            }
                            
                            // Process the decoded text like a regular text message
                            // Check for authentication/error messages first
                            if text.contains("error") || text.contains("Error") || text.contains("unauthorized") || text.contains("Unauthorized") {
                                eprintln!("❌ WebSocket authentication/error (binary): {}", text);
                                return Some(Err(anyhow::anyhow!("WebSocket authentication error: {}", text)));
                            }
                            
                            // Check for success/acknowledgment messages
                            if text.contains("subscribed") || text.contains("success") || text.contains("acknowledged") {
                                println!("✅ WebSocket subscription acknowledged (binary): {}", text);
                                return None; // Don't process as data
                            }
                            
                            // Try to parse as AIS message
                            match serde_json::from_str::<AISMessage>(&text) {
                                Ok(_ais_msg) => {
                                    if debug {
                                        println!("📊 Processing valid AIS message from binary format");
                                    }
                                    Some(Ok(text)) // Valid AIS message
                                },
                                Err(parse_error) => {
                                    if debug {
                                        eprintln!("⚠️  Binary message parsing error: {} | Message: {}", 
                                            parse_error, 
                                            if text.len() > 100 { format!("{}...", &text[..100]) } else { text });
                                    }
                                    None // Skip unparseable messages but continue
                                }
                            }
                        },
                        Err(utf8_error) => {
                            if debug {
                                eprintln!("⚠️  Binary message is not valid UTF-8: {} | First 50 bytes: {:?}", 
                                    utf8_error, &data[..std::cmp::min(50, data.len())]);
                            }
                            // Could be compressed data or other binary format
                            // For now, we'll ignore non-UTF8 binary messages
                            None
                        }
                    }
                },
                Ok(Message::Frame(_)) => {
                    // Raw frame messages - ignore these
                    None
                },
                Err(e) => {
                    eprintln!("❌ WebSocket protocol error: {}", e);
                    Some(Err(anyhow::anyhow!("WebSocket protocol error: {}", e)))
                },
            }
        }))
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
    
    // WebSocket configuration environment variables
    if args.ws_url.is_none() {
        if let Ok(ws_url_env) = std::env::var("WS_URL") {
            args.ws_url = Some(ws_url_env);
        }
    }
    
    if args.ws_api_key.is_none() {
        if let Ok(api_key_env) = std::env::var("WS_API_KEY") {
            args.ws_api_key = Some(api_key_env);
        }
    }
    
    // Handle comma-separated environment variables for Vec fields
    if args.ws_bbox.is_empty() {
        if let Ok(bbox_env) = std::env::var("WS_BBOX") {
            args.ws_bbox = bbox_env.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
        }
    }
    
    if args.ws_mmsi_filter.is_empty() {
        if let Ok(mmsi_env) = std::env::var("WS_MMSI_FILTER") {
            args.ws_mmsi_filter = mmsi_env.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
        }
    }
    
    if args.ws_message_type_filter.is_empty() {
        if let Ok(msg_type_env) = std::env::var("WS_MESSAGE_TYPE_FILTER") {
            args.ws_message_type_filter = msg_type_env.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
        }
    }

    if !args.ws_debug {
        if let Ok(debug_env) = std::env::var("WS_DEBUG") {
            args.ws_debug = debug_env.to_lowercase() == "true" || debug_env == "1";
        }
    }

    // Handle health check mode
    if args.health_check {
        check_health()?;
        return Ok(()); // This line won't be reached due to process::exit in check_health
    }

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
        ).await?)
    } else {
        None
    };

    // Handle WebSocket input separately
    if let Some(ws_url) = &args.ws_url {
        let api_key = args.ws_api_key.clone()
            .ok_or_else(|| anyhow::anyhow!("WebSocket API key is required"))?;
        
        let source = args.source.unwrap_or_else(|| "websocket".to_string());
        
        return handle_websocket_input(
            ws_url.clone(),
            api_key,
            args.ws_bbox.clone(),
            args.ws_mmsi_filter.clone(),
            args.ws_message_type_filter.clone(),
            args.ws_debug,
            source,
            args.out_dir.clone(),
            args.max_rows,
            s3_storage,
        ).await;
    }

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
            (source, Box::new(BufReader::new(stream)))
        },
        _ => {
            return Err(anyhow::anyhow!("Must specify either --input <file>, --tcp-host <host> --tcp-port <port>, or --ws-url <url>"));
        }
    };

    let mut current_key = PartKey::from_now(&source);
    let mut buf = BatchBuf::new();
    let mut rows_in_file = 0usize;
    let mut processed_count = 0usize;

    // Mark as healthy when starting
    update_health_status(true)?;

    for line in reader.lines() {
        let payload = line?;
        
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
            flush_batch(&args.out_dir, &current_key, &mut buf, &s3_storage).await?;
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
                flush_batch(&args.out_dir, &current_key, &mut buf, &s3_storage).await?;
                rows_in_file = 0;
            }
        }
    }

    if !buf.is_empty() {
        flush_batch(&args.out_dir, &current_key, &mut buf, &s3_storage).await?;
    }

    Ok(())
}

async fn flush_batch(root: &Path, key: &PartKey, buf: &mut BatchBuf, s3_storage: &Option<S3Storage>) -> Result<()> {
    let dir = key.dir_path(root);
    let filename = parquet_file_name();
    let path = dir.join(&filename);
    let mut writer = open_writer(&path, &buf.schema)?;
    let batch = buf.to_record_batch()?;
    writer.write(&batch).context("writing batch to Parquet")?;
    writer.close().context("closing Parquet writer")?;
    
    println!("✅ Wrote {} rows to {}", batch.num_rows(), path.display());

    // Upload to S3 if configured
    if let Some(s3) = s3_storage {
        let s3_key = key.s3_key(&filename);
        s3.upload_file(&path, &s3_key).await?;
    }

    Ok(())
}

async fn handle_websocket_input(
    ws_url: String,
    api_key: String,
    bounding_boxes: Vec<String>,
    mmsi_filters: Vec<String>,
    message_type_filters: Vec<String>,
    ws_debug: bool,
    source: String,
    out_dir: PathBuf,
    max_rows: Option<usize>,
    s3_storage: Option<S3Storage>,
) -> Result<()> {
    println!("🔄 Connecting to WebSocket: {}", ws_url);
    
    let ws_client = WebSocketClient::new(
        ws_url,
        api_key,
        bounding_boxes,
        mmsi_filters,
        message_type_filters,
        ws_debug,
    )?;

    let mut current_key = PartKey::from_now(&source);
    let mut buf = BatchBuf::new();
    let mut rows_in_file = 0usize;
    let mut processed_count = 0usize;

    // Mark as healthy when starting
    update_health_status(true)?;

    // Main connection loop with automatic reconnection
    loop {
        let stream = ws_client.connect_and_stream().await?;
        tokio::pin!(stream);
        
        println!("✅ WebSocket connected, processing messages...");

        while let Some(message_result) = StreamExt::next(&mut stream).await {
            let payload = match message_result {
                Ok(payload) => payload,
                Err(e) => {
                    eprintln!("❌ WebSocket stream error: {}", e);
                    
                    // Check if this is a connection error that we should recover from
                    let error_str = format!("{}", e);
                    if error_str.contains("Connection reset") || 
                       error_str.contains("connection closed") ||
                       error_str.contains("protocol error") ||
                       error_str.contains("timed out") {
                        
                        eprintln!("🔄 Connection lost, attempting to reconnect...");
                        break; // Break inner loop, reconnect in outer loop
                    } else {
                        // For other types of errors, propagate them
                        return Err(e);
                    }
                }
            };
        
        // Echo the payload to stdout as it's received (only in debug mode)
        if ws_debug {
            println!("{}", payload);
        }
        
        let now = Utc::now();
        let key = PartKey {
            source: source.clone(),
            year: now.year(),
            month: now.month(),
            day: now.day(),
            hour: now.hour(),
            minute: now.minute(),
        };

        // If the minute boundary changed, flush
        if key != current_key && !buf.is_empty() {
            flush_batch(&out_dir, &current_key, &mut buf, &s3_storage).await?;
            rows_in_file = 0;
            current_key = key.clone();
        } else {
            current_key = key.clone();
        }

        buf.push(now.timestamp_millis(), &payload);
        rows_in_file += 1;
        processed_count += 1;

        // Update health status every 100 processed lines
        if processed_count % 100 == 0 {
            update_health_status(true).unwrap_or_else(|e| eprintln!("Failed to update health status: {}", e));
        }

            if let Some(max_rows) = max_rows {
                if rows_in_file >= max_rows {
                    flush_batch(&out_dir, &current_key, &mut buf, &s3_storage).await?;
                    rows_in_file = 0;
                }
            }
        }
        
        // Connection was lost, wait a moment before reconnecting
        eprintln!("⏳ Waiting 5 seconds before reconnection attempt...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        
        // Flush any buffered data before reconnecting
        if !buf.is_empty() {
            flush_batch(&out_dir, &current_key, &mut buf, &s3_storage).await?;
        }
    }
}