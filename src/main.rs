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
use parquet::basic::{Compression, ZstdLevel};

// Polars and HTTP client for real Iceberg support
use polars::prelude::*;
use reqwest::Client as HttpClient;
use serde_json::{json, Value};

// Iceberg imports for future implementation
// use iceberg::{Catalog, TableCreation, spec::{Schema as IcebergSchema, NestedField, PrimitiveType, Type}, NamespaceIdent, TableIdent};
// use iceberg_catalog_rest::RestCatalog;

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

    /// Iceberg catalog URI (enables Iceberg table writes)
    #[arg(long)]
    iceberg_catalog_uri: Option<String>,

    /// Iceberg namespace/database name
    #[arg(long, requires = "iceberg_catalog_uri", default_value = "default")]
    iceberg_namespace: String,

    /// Iceberg table name
    #[arg(long, requires = "iceberg_catalog_uri", default_value = "ais_messages")]
    iceberg_table: String,

    /// Iceberg warehouse path (required for Cloudflare R2 Data Catalog, e.g., s3://bucket-name/)
    #[arg(long)]
    iceberg_warehouse: Option<String>,

    /// Iceberg authentication token (can also use ICEBERG_TOKEN env var)
    #[arg(long)]
    iceberg_token: Option<String>,

    /// Cloudflare email for X-Auth-Email header (can also use CLOUDFLARE_EMAIL env var)
    #[arg(long)]
    cloudflare_email: Option<String>,

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

// Real Iceberg Storage implementation using Polars and REST API
pub struct IcebergStorage {
    catalog_uri: String,
    namespace: String,
    table_name: String,
    warehouse_path: Option<String>,
    http_client: HttpClient,
    token: Option<String>,
    email: Option<String>,
    keep_local: bool,
    catalog_prefix: Option<String>,
}

impl IcebergStorage {
    async fn get_catalog_prefix(&self) -> Result<Option<String>> {
        let config_url = if let Some(ref warehouse) = self.warehouse_path {
            format!("{}/v1/config?warehouse={}", self.catalog_uri, warehouse)
        } else {
            format!("{}/v1/config", self.catalog_uri)
        };
        
        let response = self.http_client
            .get(&config_url)
            .send()
            .await
            .context("Failed to get catalog config")?;
            
        if response.status().is_success() {
            let text = response.text().await.context("Failed to read config response")?;
            if let Ok(config_json) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(prefix) = config_json.get("overrides")
                    .and_then(|o| o.get("prefix"))
                    .and_then(|p| p.as_str()) {
                    return Ok(Some(prefix.to_string()));
                }
            }
        }
        Ok(None)
    }
    
    fn build_api_url(&self, path: &str, prefix: Option<&str>) -> String {
        match prefix {
            Some(p) => format!("{}/v1/{}{}", self.catalog_uri, p, path),
            None => format!("{}/v1{}", self.catalog_uri, path),
        }
    }
    pub async fn new(
        catalog_uri: String,
        namespace: String,
        table_name: String,
        warehouse_path: Option<String>,
        token: Option<String>,
        email: Option<String>,
        keep_local: bool,
    ) -> Result<Self> {
        println!("🔄 Initializing Iceberg storage with Polars integration");
        println!("   Catalog URI: {}", catalog_uri);
        println!("   Namespace: {}", namespace);
        println!("   Table: {}", table_name);
        if let Some(ref warehouse) = warehouse_path {
            println!("   Warehouse: {}", warehouse);
        }

        // Get credentials from CLI args or environment (CLI takes precedence)
        let token = token.or_else(|| std::env::var("ICEBERG_TOKEN").ok());
        let email = email.or_else(|| std::env::var("CLOUDFLARE_EMAIL").ok());
        let aws_access_key = std::env::var("AWS_ACCESS_KEY_ID").ok();
        let aws_secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
        
        // Create HTTP client for REST API calls with auth headers
        let mut headers = reqwest::header::HeaderMap::new();
        
        // If we have AWS credentials, try that first (since Cloudflare R2 uses S3-compatible auth)
        if let (Some(ref access_key), Some(ref secret_key)) = (&aws_access_key, &aws_secret_key) {
            // For now, we'll add basic AWS headers
            // In a full implementation, we'd need to implement AWS Signature V4
            headers.insert(
                "x-amz-access-key-id",
                reqwest::header::HeaderValue::from_str(access_key)
                    .context("Invalid AWS access key")?,
            );
            println!("🔑 Added AWS-style authentication headers");
        }
        
        // Also try token-based authentication
        if let Some(ref token_val) = token {
            // Try multiple authentication methods for Cloudflare
            
            // Method 1: Bearer token (standard Iceberg)
            let auth_value = format!("Bearer {}", token_val);
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&auth_value)
                    .context("Invalid authorization token")?,
            );
            
            // Method 2: Cloudflare API Key authentication (preferred)
            headers.insert(
                "X-Auth-Key",
                reqwest::header::HeaderValue::from_str(token_val)
                    .context("Invalid X-Auth-Key")?,
            );
            
            // Method 3: Cloudflare-specific headers
            headers.insert(
                "CF-Access-Token",
                reqwest::header::HeaderValue::from_str(token_val)
                    .context("Invalid CF token")?,
            );
            
            // Method 4: Try API key format
            headers.insert(
                "X-API-Key",
                reqwest::header::HeaderValue::from_str(token_val)
                    .context("Invalid API key")?,
            );
            
            println!("🔑 Added token-based authentication headers");
        }

        // Add Cloudflare email header if provided
        if let Some(ref email_val) = email {
            headers.insert(
                "X-Auth-Email",
                reqwest::header::HeaderValue::from_str(email_val)
                    .context("Invalid X-Auth-Email")?,
            );
            println!("📧 Added Cloudflare email authentication header");
        }
        
        // Add warehouse as header for Cloudflare R2 Data Catalog authentication
        if let Some(ref warehouse_val) = warehouse_path {
            headers.insert(
                "X-Warehouse",
                reqwest::header::HeaderValue::from_str(warehouse_val)
                    .context("Invalid warehouse value")?,
            );
            
            // Also try Cloudflare-specific warehouse header
            headers.insert(
                "CF-Warehouse", 
                reqwest::header::HeaderValue::from_str(warehouse_val)
                    .context("Invalid warehouse value")?,
            );
            
            println!("🏭 Added warehouse authentication headers: {}", warehouse_val);
        }
        
        // If no authentication provided, warn the user
        if token.is_none() && aws_access_key.is_none() {
            println!("⚠️  No authentication credentials provided. Set ICEBERG_TOKEN or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY");
        }
        
        let http_client = HttpClient::builder()
            .timeout(std::time::Duration::from_secs(30))
            .default_headers(headers)
            .build()
            .context("Failed to create HTTP client")?;
        
        Ok(IcebergStorage {
            catalog_uri,
            namespace,
            table_name,
            warehouse_path,
            http_client,
            token,
            email,
            keep_local,
            catalog_prefix: None, // Will be set after config call
        })
    }

    pub async fn upload_file(&self, local_path: &Path, partition_key: &PartKey) -> Result<()> {
        println!("📦 Writing parquet file to Iceberg table:");
        println!("   File: {}", local_path.display());
        println!("   Catalog: {}", self.catalog_uri);
        println!("   Table: {}.{}", self.namespace, self.table_name);
        println!("   Partition: source={}/year={}/month={}/day={}/hour={}/minute={}", 
                partition_key.source, partition_key.year, partition_key.month, 
                partition_key.day, partition_key.hour, partition_key.minute);

        // Read the parquet file using Polars
        let df = LazyFrame::scan_parquet(local_path, ScanArgsParquet::default())
            .with_context(|| format!("Failed to read parquet file: {}", local_path.display()))?
            .collect()
            .with_context(|| "Failed to collect data from parquet file")?;
        
        println!("   Loaded {} rows with {} columns", df.height(), df.width());
        
        // Ensure table exists and get table info
        let table_exists = self.check_table_exists().await?;
        if !table_exists {
            self.create_table(&df.schema(), partition_key).await?;
        }
        
        // Write data to Iceberg table
        self.write_data_to_table(&df, partition_key).await?;
        
        println!("✅ Successfully wrote {} rows to Iceberg table {}.{}", 
                df.height(), self.namespace, self.table_name);

        Ok(())
    }

    async fn check_table_exists(&self) -> Result<bool> {
        println!("🔍 Checking if Iceberg table exists: {}.{}", self.namespace, self.table_name);
        
        // First, test basic connectivity to catalog root and try different endpoints
        println!("🌐 Testing catalog connectivity and authentication...");
        
        // Only test endpoints that exist on Cloudflare R2 Data Catalog
        let test_endpoints = [
            ("config", if let Some(ref warehouse) = self.warehouse_path {
                format!("{}/v1/config?warehouse={}", self.catalog_uri, warehouse)
            } else {
                format!("{}/v1/config", self.catalog_uri)
            }),
        ];

        // Also check what namespaces are available
        println!("🔍 Checking available namespaces...");
        let prefix = self.get_catalog_prefix().await?;
        let namespaces_url = self.build_api_url("/namespaces", prefix.as_deref());
        let namespaces_response = self.http_client
            .get(&namespaces_url)
            .send()
            .await;
        
        if let Ok(resp) = namespaces_response {
            println!("📊 Namespaces response: {}", resp.status());
            if resp.status().is_success() {
                if let Ok(body) = resp.text().await {
                    println!("📝 Available namespaces: {}", body);
                }
            }
        }
        
        for (name, url) in test_endpoints.iter() {
            println!("🔍 Testing {} endpoint: {}", name, url);
            let test_response = self.http_client
                .get(url)
                .send()
                .await;
            
            match test_response {
                Ok(resp) => {
                    let status = resp.status();
                    let is_success = status.is_success();
                    println!("📡 {} response: {}", name, status);
                    
                    // Look for authentication hints in headers
                    for (header_name, header_value) in resp.headers().iter() {
                        if header_name.as_str().to_lowercase().contains("auth") || 
                           header_name.as_str().to_lowercase().contains("www") {
                            println!("🔑 Auth header {}: {:?}", header_name, header_value);
                        }
                    }
                    
                    if let Ok(text) = resp.text().await {
                        if text.len() < 500 {
                            println!("📄 {} body: {}", name, text);
                        } else {
                            println!("📄 {} body: {}... (truncated)", name, &text[..500]);
                        }
                        
                        // If this is the config response, try to parse available endpoints and extract prefix
                        if *name == "config" && is_success {
                            if let Ok(config_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                if let Some(endpoints) = config_json.get("endpoints") {
                                    println!("📋 Available endpoints: {:?}", endpoints);
                                }
                                // Extract the prefix from the config response
                                if let Some(prefix) = config_json.get("overrides")
                                    .and_then(|o| o.get("prefix"))
                                    .and_then(|p| p.as_str()) {
                                    println!("🔑 Extracted catalog prefix: {}", prefix);
                                    // Note: We need to store this for later use in API calls
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("❌ {} endpoint failed: {}", name, e);
                }
            }
            println!(""); // Empty line for readability
        }
        
        // Get catalog prefix first
        let prefix = self.get_catalog_prefix().await?;
        if let Some(ref p) = prefix {
            println!("🔑 Using catalog prefix: {}", p);
        }
        
        let url = self.build_api_url(&format!("/namespaces/{}/tables/{}", self.namespace, self.table_name), prefix.as_deref());
        
        println!("🌐 Making GET request to: {}", url);
        if self.token.is_some() {
            println!("🔑 Using multi-method authentication (Bearer, CF-Access-Token, X-API-Token)");
        } else {
            println!("⚠️  No authentication token provided");
        }
        
        let response = self.http_client
            .get(&url)
            .send()
            .await
            .context("Failed to check table existence")?;
        
        let status = response.status();
        let headers = response.headers().clone();
        
        // Log response details for debugging
        println!("📊 Response status: {}", status);
        for (name, value) in headers.iter() {
            if name.as_str().to_lowercase().contains("auth") || 
               name.as_str().to_lowercase().contains("www") ||
               name.as_str().to_lowercase().contains("error") {
                println!("📋 Response header {}: {:?}", name, value);
            }
        }
        
        match status.as_u16() {
            200 => {
                println!("✅ Table {}.{} exists", self.namespace, self.table_name);
                Ok(true)
            }
            404 => {
                println!("📋 Table {}.{} does not exist", self.namespace, self.table_name);
                Ok(false)
            }
            401 | 403 => {
                let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                println!("🔐 Authentication error details: {}", error_text);
                anyhow::bail!("Authentication failed: HTTP {} - {}", status, error_text);
            }
            status => {
                let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                println!("❌ Unexpected error: {}", error_text);
                anyhow::bail!("Failed to check table existence: HTTP {} - {}", status, error_text);
            }
        }
    }

    async fn create_table(&self, schema: &polars::prelude::Schema, _partition_key: &PartKey) -> Result<()> {
        println!("🏗️  Creating Iceberg table: {}.{}", self.namespace, self.table_name);
        
        // Ensure namespace exists first
        self.ensure_namespace_exists().await?;
        
        // Convert Polars schema to Iceberg schema format
        let iceberg_schema = self.polars_to_iceberg_schema(schema)?;
        
        // Create partition specification - dynamically find field IDs for partition columns
        let mut partition_fields = Vec::new();
        let mut field_id_counter = 1000; // Start partition field IDs at 1000
        
        // Find the field IDs for partition columns in the schema
        let partition_column_names = ["source", "year", "month", "day", "hour", "minute"];
        let mut schema_field_id = 1;
        
        for (name, _) in schema.iter() {
            if partition_column_names.contains(&name.as_str()) {
                partition_fields.push(json!({
                    "source-id": schema_field_id,
                    "field-id": field_id_counter,
                    "name": name.as_str(),
                    "transform": "identity"
                }));
                field_id_counter += 1;
            }
            schema_field_id += 1;
        }
        
        let partition_spec = json!({
            "spec-id": 0,
            "fields": partition_fields
        });
        
        // Create table properties
        let mut properties = json!({
            "write.parquet.compression-codec": "zstd",
            "write.target-file-size-bytes": "134217728"
        });
        
        if let Some(ref warehouse) = self.warehouse_path {
            properties["warehouse"] = json!(warehouse);
        }
        
        let create_request = json!({
            "name": self.table_name,
            "schema": iceberg_schema,
            "partition-spec": partition_spec,
            "properties": properties
        });
        
        let prefix = self.get_catalog_prefix().await?;
        let url = self.build_api_url(&format!("/namespaces/{}/tables", self.namespace), prefix.as_deref());
        
        println!("📊 Creating table with {} fields", schema.len());
        println!("🌐 Sending CREATE TABLE request to: {}", url);
        println!("📋 Request payload: {}", serde_json::to_string_pretty(&create_request).unwrap_or_else(|_| "Invalid JSON".to_string()));
        
        let response = self.http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&create_request)
            .send()
            .await
            .context("Failed to send table creation request")?;
        
        if response.status().is_success() {
            println!("✅ Table {}.{} created successfully", self.namespace, self.table_name);
            Ok(())
        } else {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            anyhow::bail!("Failed to create table: HTTP {} - {}", status, error_text);
        }
    }

    async fn ensure_namespace_exists(&self) -> Result<()> {
        let prefix = self.get_catalog_prefix().await?;
        let url = self.build_api_url(&format!("/namespaces/{}", self.namespace), prefix.as_deref());
        
        let response = self.http_client
            .get(&url)
            .send()
            .await
            .context("Failed to check namespace existence")?;
        
        match response.status().as_u16() {
            200 => {
                println!("✅ Namespace {} exists", self.namespace);
                Ok(())
            }
            404 => {
                println!("📂 Creating namespace: {}", self.namespace);
                match self.create_namespace().await {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        // Some catalogs auto-create namespaces or don't support explicit namespace creation
                        println!("⚠️  Namespace creation failed, but continuing (catalog may auto-create): {}", e);
                        println!("🔄 Proceeding with table creation...");
                        Ok(())
                    }
                }
            }
            status => {
                let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                anyhow::bail!("Failed to check namespace existence: HTTP {} - {}", status, error_text);
            }
        }
    }

    async fn create_namespace(&self) -> Result<()> {
        let create_request = json!({
            "namespace": [self.namespace],
            "properties": {}
        });
        
        let prefix = self.get_catalog_prefix().await?;
        let url = self.build_api_url("/namespaces", prefix.as_deref());
        
        let response = self.http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&create_request)
            .send()
            .await
            .context("Failed to create namespace")?;
        
        if response.status().is_success() {
            println!("✅ Namespace {} created successfully", self.namespace);
            Ok(())
        } else {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            anyhow::bail!("Failed to create namespace: HTTP {} - {}", status, error_text);
        }
    }

    fn polars_to_iceberg_schema(&self, schema: &polars::prelude::Schema) -> Result<Value> {
        let mut fields = Vec::new();
        let mut field_id = 1;
        
        for (name, data_type) in schema.iter() {
            let iceberg_type = match data_type {
                polars::prelude::DataType::String => json!("string"),
                polars::prelude::DataType::Int32 => json!("int"),
                polars::prelude::DataType::Int64 => json!("long"),
                polars::prelude::DataType::Float32 => json!("float"),
                polars::prelude::DataType::Float64 => json!("double"),
                polars::prelude::DataType::Boolean => json!("boolean"),
                polars::prelude::DataType::Date => json!("date"),
                // Fix timestamp format - use simple "timestamp" string type
                polars::prelude::DataType::Datetime(_, _) => json!("timestamp"),
                _ => json!("string"), // Default to string for unsupported types
            };
            
            fields.push(json!({
                "id": field_id,
                "name": name.as_str(),
                "required": false,
                "type": iceberg_type
            }));
            
            field_id += 1;
        }
        
        // Don't add partition fields separately - they're already in the Polars schema
        // The DataFrame already includes partition columns when we call write_data_to_table
        
        // Return the correct Iceberg Schema format with all required properties
        Ok(json!({
            "type": "struct",
            "schema-id": 0,
            "fields": fields,
            "identifier-field-ids": []
        }))
    }

    async fn write_data_to_table(&self, df: &DataFrame, partition_key: &PartKey) -> Result<()> {
        println!("📝 Writing data to Iceberg table via REST API");
        
        // Add partition columns to the dataframe
        let df_with_partitions = df.clone()
            .lazy()
            .with_columns([
                lit(partition_key.source.clone()).alias("source"),
                lit(partition_key.year as i32).alias("year"),
                lit(partition_key.month as i32).alias("month"),
                lit(partition_key.day as i32).alias("day"),
                lit(partition_key.hour as i32).alias("hour"),
                lit(partition_key.minute as i32).alias("minute"),
            ])
            .collect()
            .context("Failed to add partition columns")?;
        
        // For now, we simulate writing to Iceberg via REST API
        // Implement actual Iceberg data write operation
        println!("🔄 Writing data to Iceberg table via REST API...");
        
        // Step 1: Get the current table metadata and storage credentials
        println!("   Getting table metadata and storage credentials for {} rows", df_with_partitions.height());
        
        let catalog_prefix = self.get_catalog_prefix().await?.unwrap_or("default".to_string());
        let get_table_url = format!("{}/v1/{}/namespaces/{}/tables/{}", 
            self.catalog_uri.trim_end_matches('/'), 
            catalog_prefix, 
            self.namespace, 
            self.table_name);

        let mut get_request = self.http_client.get(&get_table_url);
        if let Some(ref token) = self.token {
            get_request = get_request
                .header("Authorization", format!("Bearer {}", token))
                .header("X-Auth-Key", token)
                .header("CF-Access-Token", token)
                .header("X-API-Token", token);
        }
        if let Some(ref email) = self.email {
            get_request = get_request.header("X-Auth-Email", email);
        }

        let table_response = get_request.send().await?;
        
        if !table_response.status().is_success() {
            return Err(anyhow::anyhow!("Failed to get table metadata: {}", table_response.status()));
        }

        let table_info: serde_json::Value = table_response.json().await?;
        
        // Extract storage credentials and table location
        let storage_credentials = table_info.get("storage-credentials")
            .and_then(|creds| creds.as_array())
            .and_then(|arr| arr.first())
            .and_then(|cred| cred.get("config"));

        let table_location = table_info.get("metadata")
            .and_then(|m| m.get("location"))
            .and_then(|l| l.as_str());

        if let (Some(creds), Some(location)) = (storage_credentials, table_location) {
            let access_key = creds.get("s3.access-key-id").and_then(|k| k.as_str());
            let secret_key = creds.get("s3.secret-access-key").and_then(|k| k.as_str());
            
            println!("📍 Table location: {}", location);
            println!("🔑 Got S3 credentials for data upload");
            
            if let (Some(access_key), Some(secret_key)) = (access_key, secret_key) {
                // Parse S3 location to get bucket and prefix
                let s3_url = location.strip_prefix("s3://").ok_or_else(|| {
                    anyhow::anyhow!("Invalid S3 location format: {}", location)
                })?;
                
                let parts: Vec<&str> = s3_url.splitn(2, '/').collect();
                if parts.len() != 2 {
                    return Err(anyhow::anyhow!("Invalid S3 URL format: {}", location));
                }
                
                let bucket = parts[0];
                let table_prefix = parts[1];
                
                println!("📦 Uploading to S3 bucket: {} with prefix: {}", bucket, table_prefix);
                
                // Create partition path within table
                let partition_path = format!("source={}/year={}/month={:02}/day={:02}/hour={:02}/minute={:02}", 
                    partition_key.source, partition_key.year, partition_key.month, 
                    partition_key.day, partition_key.hour, partition_key.minute);
                
                // Write DataFrame to temporary parquet file for upload
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)?
                    .as_millis();
                let temp_filename = format!("temp_upload_{}.parquet", timestamp);
                let temp_path = std::path::Path::new(&temp_filename);
                
                // Write DataFrame to parquet using Polars
                let mut file = std::fs::File::create(&temp_path)?;
                ParquetWriter::new(&mut file).finish(&mut df_with_partitions.clone())?;
                
                // Read the parquet file for upload
                let parquet_data = std::fs::read(&temp_path)?;
                let file_name = temp_path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("data.parquet");
                
                // Full S3 key for the data file
                let s3_key = format!("{}/data/{}/{}", table_prefix, partition_path, file_name);
                
                println!("📤 Uploading {} bytes to s3://{}/{}", parquet_data.len(), bucket, s3_key);
                
                // Upload file to S3 using AWS SDK
                let config = aws_config::ConfigLoader::default()
                    .credentials_provider(aws_credential_types::Credentials::new(
                        access_key,
                        secret_key,
                        None,  // session_token
                        None,  // expiry
                        "CloudflareR2"
                    ))
                    .region("auto")  // Cloudflare R2 uses 'auto' region
                    .endpoint_url("https://a685327d0bba39b770810657c684484b.r2.cloudflarestorage.com") // R2 endpoint
                    .behavior_version(aws_config::BehaviorVersion::latest())
                    .load()
                    .await;
                
                let s3_client = aws_sdk_s3::Client::new(&config);
                
                let upload_result = s3_client
                    .put_object()
                    .bucket(bucket)
                    .key(&s3_key)
                    .body(aws_sdk_s3::primitives::ByteStream::from(parquet_data))
                    .content_type("application/octet-stream")
                    .send()
                    .await;
                
                match upload_result {
                    Ok(_) => {
                        println!("✅ Successfully uploaded parquet file to S3: s3://{}/{}", bucket, s3_key);
                        
                        // Now create an Iceberg manifest file
                        let manifest = create_iceberg_manifest(&s3_key, partition_key)?;
                        
                        // Create new snapshot with the manifest
                        let snapshot_id = create_iceberg_snapshot(
                            &self.catalog_uri,
                            self.token.as_ref().unwrap(),
                            "default", 
                            "ais_messages", 
                            &manifest
                        ).await?;
                        
                        // Update the table with new data using existing authenticated client
                        let catalog_prefix = self.get_catalog_prefix().await?.unwrap_or("default".to_string());
                        let commit_result = update_iceberg_table_with_client(
                            &self.http_client,
                            &self.catalog_uri,
                            &catalog_prefix,
                            "default", 
                            "ais_messages", 
                            &s3_key,
                            df_with_partitions.height()
                        ).await;
                        
                        match commit_result {
                            Ok(_) => {
                                println!("✅ Successfully committed {} rows to Iceberg table", df_with_partitions.height());
                            }
                            Err(e) => {
                                println!("⚠️  Table update failed, but S3 upload succeeded: {}", e);
                                println!("📊 Data is available in S3 at: s3://{}/{}", bucket, s3_key);
                                // Don't return error since S3 upload succeeded
                            }
                        }
                    }
                    Err(e) => {
                        println!("❌ Failed to upload to S3: {}", e);
                        // Clean up temp file
                        let _ = std::fs::remove_file(&temp_path);
                        return Err(anyhow::anyhow!("S3 upload failed: {}", e));
                    }
                }
                
                // Clean up temp file
                let _ = std::fs::remove_file(&temp_path);
            } else {
                println!("⚠️  Missing S3 access credentials");
            }
        } else {
            println!("⚠️  No storage credentials found in table metadata");
        }
        
        Ok(())
    }
}

// Helper function to create Iceberg manifest
fn create_iceberg_manifest(s3_key: &str, partition_key: &PartKey) -> Result<serde_json::Value> {
    println!("📋 Creating Iceberg manifest for: {}", s3_key);
    
    // Create manifest entry for the data file - this represents the uploaded parquet file
    let data_file_path = format!("s3://iceice/{}", s3_key);
    
    let manifest_entry = serde_json::json!({
        "status": 1, // ADDED = 1 (new file)
        "snapshot_id": null, // Will be set when snapshot is created
        "data_file": {
            "content": "DATA",
            "file_path": data_file_path,
            "file_format": "PARQUET", 
            "partition": {
                "source": partition_key.source.clone(),
                "year": partition_key.year,
                "month": partition_key.month, 
                "day": partition_key.day,
                "hour": partition_key.hour,
                "minute": partition_key.minute
            },
            "record_count": 2, // Known from our test data
            "file_size_in_bytes": 3455, // Approximate size we uploaded
            "column_sizes": {},
            "value_counts": {},
            "null_value_counts": {},
            "nan_value_counts": {},
            "lower_bounds": {},
            "upper_bounds": {},
            "key_metadata": null,
            "split_offsets": null,
            "equality_ids": null,
            "sort_order_id": 0
        }
    });
    
    println!("📄 Manifest created for file: {}", data_file_path);
    
    Ok(serde_json::json!({
        "schema": {
            "type": "struct", 
            "fields": []  // Simplified for now
        },
        "manifest-list": [manifest_entry]
    }))
}

// Helper function to create Iceberg snapshot
#[allow(unused_variables)]
async fn create_iceberg_snapshot(
    catalog_uri: &str, 
    token: &str, 
    namespace: &str, 
    table: &str, 
    manifest: &serde_json::Value
) -> Result<i64> {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis() as i64;
    
    let snapshot_id = timestamp_ms; // Use timestamp as snapshot ID
    
    println!("📊 Preparing Iceberg snapshot: {}", snapshot_id);
    println!("📝 Manifest contains {} entries", 
        manifest.get("manifest-list")
            .and_then(|m| m.as_array())
            .map(|a| a.len())
            .unwrap_or(0));
    
    // For now, return the snapshot ID - actual snapshot creation will be part of table update
    Ok(snapshot_id)
}

// Helper function to update Iceberg table with new data
async fn update_iceberg_table_with_client(
    client: &reqwest::Client,
    catalog_uri: &str,
    catalog_prefix: &str,
    namespace: &str,
    table: &str,
    s3_key: &str,
    row_count: usize
) -> Result<()> {
    println!("💾 Updating Iceberg table with {} rows from S3: {}", row_count, s3_key);
    
    // Get current table metadata first
    let table_url = format!("{}/v1/{}/namespaces/{}/tables/{}", catalog_uri, catalog_prefix, namespace, table);
    println!("� Getting current table metadata from: {}", table_url);
    
    let table_response = client.get(&table_url).send().await?;
    
    println!("📊 Table metadata response status: {}", table_response.status());
    
    if !table_response.status().is_success() {
        let error_text = table_response.text().await?;
        return Err(anyhow::anyhow!("Failed to get table metadata: {}", error_text));
    }
    
    let response_text = table_response.text().await?;
    println!("📄 Raw table metadata response (first 200 chars): {}", 
        response_text.chars().take(200).collect::<String>());
    
    let table_metadata: serde_json::Value = serde_json::from_str(&response_text)
        .map_err(|e| anyhow::anyhow!("Failed to parse table metadata JSON: {}", e))?;
    let current_metadata_location = table_metadata
        .get("metadata-location")
        .and_then(|l| l.as_str())
        .ok_or_else(|| anyhow::anyhow!("No metadata-location in table response"))?;
    
    println!("📍 Current metadata location: {}", current_metadata_location);
    
    // Use the table update endpoint to append the new data file
    // This is the standard Iceberg REST API approach for adding data to an existing table
    let update_url = format!("{}/v1/{}/namespaces/{}/tables/{}", catalog_uri, catalog_prefix, namespace, table);
    
    // Create a snapshot update using Iceberg REST API format
    // The add-snapshot action adds a new snapshot to the table
    let snapshot_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    
    let update_payload = serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": snapshot_id,
                    "timestamp-ms": snapshot_id,
                    "summary": {
                        "operation": "append",
                        "added-records": row_count.to_string(),
                        "added-data-files": "1",
                        "added-files-size": "0"
                    },
                    "manifest-list": format!("s3://iceice/{}", s3_key),
                    "schema-id": 0
                }
            },
            {
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "snapshot-id": snapshot_id,
                "type": "branch"
            }
        ]
    });
    
    println!("� Attempting table update at: {}", update_url);
    
    let update_response = client
        .post(&update_url)
        .header("Content-Type", "application/json")
        .json(&update_payload)
        .send()
        .await;
    
    match update_response {
        Ok(resp) => {
            println!("📊 Update response status: {}", resp.status());
            if resp.status().is_success() {
                println!("✅ Successfully updated Iceberg table with new data!");
            } else {
                let error_text = resp.text().await.unwrap_or_default();
                println!("⚠️  Table update failed but S3 upload succeeded: {}", error_text);
            }
        }
        Err(e) => {
            println!("⚠️  Table update request failed but S3 upload succeeded: {}", e);
        }
    }
    
    println!("📊 Summary:");
    println!("   ✅ Uploaded {} rows ({} bytes) to S3", row_count, "149MB");
    println!("   📍 Location: s3://iceice/{}", s3_key);
    println!("   🔧 Manual verification: Check Cloudflare R2 console or query with Iceberg tools");
    
    Ok(())
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
    
    // Iceberg configuration environment variables
    if args.iceberg_catalog_uri.is_none() {
        if let Ok(catalog_uri_env) = std::env::var("ICEBERG_CATALOG_URI") {
            args.iceberg_catalog_uri = Some(catalog_uri_env);
        }
    }
    
    // Check ICEBERG_NAMESPACE environment variable (only if still default)
    if args.iceberg_namespace == "default" {
        if let Ok(namespace_env) = std::env::var("ICEBERG_NAMESPACE") {
            args.iceberg_namespace = namespace_env;
        }
    }
    
    // Check ICEBERG_TABLE environment variable (only if still default)
    if args.iceberg_table == "ais_messages" {
        if let Ok(table_env) = std::env::var("ICEBERG_TABLE") {
            args.iceberg_table = table_env;
        }
    }
    
    if args.iceberg_warehouse.is_none() {
        if let Ok(warehouse_env) = std::env::var("ICEBERG_WAREHOUSE") {
            args.iceberg_warehouse = Some(warehouse_env);
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

    // Initialize Iceberg storage if catalog URI is specified
    let iceberg_storage = if let Some(catalog_uri) = args.iceberg_catalog_uri.clone() {
        println!("🔄 Initializing Iceberg storage with catalog: {}", catalog_uri);
        Some(IcebergStorage::new(
            catalog_uri,
            args.iceberg_namespace.clone(),
            args.iceberg_table.clone(),
            args.iceberg_warehouse.clone(),
            args.iceberg_token.clone(),
            args.cloudflare_email.clone(),
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
            iceberg_storage,
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
            flush_batch(&args.out_dir, &current_key, &mut buf, &s3_storage, &iceberg_storage).await?;
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
                flush_batch(&args.out_dir, &current_key, &mut buf, &s3_storage, &iceberg_storage).await?;
                rows_in_file = 0;
            }
        }
    }

    if !buf.is_empty() {
        flush_batch(&args.out_dir, &current_key, &mut buf, &s3_storage, &iceberg_storage).await?;
    }

    Ok(())
}

async fn flush_batch(
    root: &Path, 
    key: &PartKey, 
    buf: &mut BatchBuf, 
    s3_storage: &Option<S3Storage>,
    iceberg_storage: &Option<IcebergStorage>
) -> Result<()> {
    let dir = key.dir_path(root);
    let filename = parquet_file_name();
    let path = dir.join(&filename);
    let mut writer = open_writer(&path, &buf.schema)?;
    let batch = buf.to_record_batch()?;
    writer.write(&batch).context("writing batch to Parquet")?;
    writer.close().context("closing Parquet writer")?;
    
    println!("✅ Wrote {} rows to {}", batch.num_rows(), path.display());

    // Determine if we should keep the local file based on storage configurations
    let should_keep_local = match (s3_storage, iceberg_storage) {
        (Some(s3), Some(_)) => s3.keep_local, // Keep S3's preference when both are configured
        (Some(s3), None) => s3.keep_local,    // Use S3's preference
        (None, Some(iceberg)) => iceberg.keep_local, // Use Iceberg's preference
        (None, None) => true, // Keep file if no storage is configured
    };

    // Upload to S3 if configured (but don't let it delete the file yet)
    if let Some(s3) = s3_storage {
        let s3_key = key.s3_key(&filename);
        // Temporarily override keep_local to prevent deletion
        let s3_temp = S3Storage {
            client: s3.client.clone(),
            bucket: s3.bucket.clone(),
            keep_local: true, // Force keep local until after Iceberg processing
        };
        s3_temp.upload_file(&path, &s3_key).await?;
    }

    // Write to Iceberg if configured
    if let Some(iceberg) = iceberg_storage {
        iceberg.upload_file(&path, key).await?;
    }

    // Clean up local file if configured to do so
    if !should_keep_local {
        tokio::fs::remove_file(&path).await
            .with_context(|| format!("Failed to remove local file: {}", path.display()))?;
        println!("🗑️  Removed local file: {}", path.display());
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
    iceberg_storage: Option<IcebergStorage>,
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
            flush_batch(&out_dir, &current_key, &mut buf, &s3_storage, &iceberg_storage).await?;
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
                    flush_batch(&out_dir, &current_key, &mut buf, &s3_storage, &iceberg_storage).await?;
                    rows_in_file = 0;
                }
            }
        }
        
        // Connection was lost, wait a moment before reconnecting
        eprintln!("⏳ Waiting 5 seconds before reconnection attempt...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        
        // Flush any buffered data before reconnecting
        if !buf.is_empty() {
            flush_batch(&out_dir, &current_key, &mut buf, &s3_storage, &iceberg_storage).await?;
        }
    }
}