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
}

impl WebSocketClient {
    pub fn new(
        url: String,
        api_key: String,
        bounding_boxes: Vec<String>,
        mmsi_filters: Vec<String>,
        message_type_filters: Vec<String>,
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

        Ok(WebSocketClient { url, subscription })
    }

    pub async fn connect_and_stream(&self) -> Result<impl futures_util::Stream<Item = Result<String>>> {
        let (ws_stream, _) = connect_async(&self.url).await
            .with_context(|| format!("Failed to connect to WebSocket: {}", self.url))?;

        let (mut write, mut read) = ws_stream.split();

        // Send subscription message
        let subscription_json = serde_json::to_string(&self.subscription)
            .context("Failed to serialize subscription message")?;
        
        println!("🔄 Sending WebSocket subscription: {}", subscription_json);
        
        write.send(Message::Text(subscription_json)).await
            .context("Failed to send subscription message")?;

        // Return the read stream
        Ok(read.filter_map(|msg| async move {
            match msg {
                Ok(Message::Text(text)) => {
                    // Try to parse as AIS message first
                    match serde_json::from_str::<AISMessage>(&text) {
                        Ok(_ais_msg) => Some(Ok(text)), // Valid AIS message
                        Err(_) => {
                            // Check if it's an error message
                            if text.contains("error") {
                                eprintln!("❌ WebSocket error: {}", text);
                                Some(Err(anyhow::anyhow!("WebSocket error: {}", text)))
                            } else {
                                // Unknown message format, log but continue
                                eprintln!("⚠️  Unknown WebSocket message format: {}", text);
                                None
                            }
                        }
                    }
                },
                Ok(Message::Close(_)) => {
                    eprintln!("🔌 WebSocket connection closed");
                    Some(Err(anyhow::anyhow!("WebSocket connection closed")))
                },
                Err(e) => Some(Err(anyhow::anyhow!("WebSocket error: {}", e))),
                _ => None, // Ignore other message types (binary, ping, pong)
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
    let args = Args::parse();

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
    )?;

    let stream = ws_client.connect_and_stream().await?;
    tokio::pin!(stream);
    
    let mut current_key = PartKey::from_now(&source);
    let mut buf = BatchBuf::new();
    let mut rows_in_file = 0usize;
    let mut processed_count = 0usize;

    // Mark as healthy when starting
    update_health_status(true)?;

    println!("✅ WebSocket connected, processing messages...");

    while let Some(message_result) = StreamExt::next(&mut stream).await {
        let payload = message_result?;
        
        // Echo the payload to stdout as it's received
        println!("{}", payload);
        
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

    // Flush any remaining data
    if !buf.is_empty() {
        flush_batch(&out_dir, &current_key, &mut buf, &s3_storage).await?;
    }

    Ok(())
}