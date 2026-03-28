# capture

A high-performance Rust application for ingesting data streams into Hive-partitioned Parquet files with Zstd compression. Supports file and TCP inputs with optional remote storage (S3/MinIO).

## Features
- **Multiple Input Sources**: File or TCP stream
- **Hive Partitioning**: Automatic partitioning by source, year, month, day, hour, and minute
- **Parquet Format**: Efficient columnar storage with Zstd compression
- **S3 Integration**: Upload to AWS S3 or S3-compatible storage (MinIO) with optional TLS
- **Background Uploads**: Non-blocking S3 uploads to prevent data collection pauses
- **Docker Support**: Full Docker and docker-compose integration with health checks
- **Environment Variables**: Complete environment variable support for containerized deployments
- **Real-time Processing**: Async processing with configurable buffering
- **Health Monitoring**: Built-in health checks for container orchestration
- **Pure Rust TLS**: Uses rustls for secure connections without OpenSSL dependencies

## Quick Start

### Using Environment Variables (Recommended for Docker)

```bash
# TCP stream with S3
export TCP_HOST="153.44.253.27"
export TCP_PORT="5631"
export SOURCE="norway-tcp"
export S3_BUCKET="maritime-data"
export S3_REGION="us-west-2"

./capture
```

### Using Command Line Arguments

```bash
# File input
./capture --input data.txt --source mydata

# TCP stream
./capture --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp

# File input with S3
./capture --input data.txt --source mydata --s3-bucket maritime-data
```

## Environment Variables

All command-line parameters can be configured using environment variables:

| Environment Variable | CLI Argument | Description |
|---------------------|--------------|-------------|
| `INPUT_FILE` | `--input` | Input text file path |
| `TCP_HOST` | `--tcp-host` | TCP host address |
| `TCP_PORT` | `--tcp-port` | TCP port number |
| `SOURCE` | `--source` | Logical source label |
| `OUT_DIR` | `--out-dir` | Output directory |
| `MAX_ROWS` | `--max-rows` | Max rows per file |
| `UPLOAD_DRAIN_TIMEOUT_SECONDS` | `--upload-drain-timeout-seconds` | Max seconds to wait for upload drain |
| `HEALTH_CHECK` | `--health-check` | Run health check |
| `S3_BUCKET` | `--s3-bucket` | S3 bucket name |
| `S3_ENDPOINT` | `--s3-endpoint` | S3 endpoint URL |
| `S3_REGION` | `--s3-region` | S3 region |
| `S3_ACCESS_KEY` | `--s3-access-key` | S3 access key |
| `S3_SECRET_KEY` | `--s3-secret-key` | S3 secret key |
| `KEEP_LOCAL` | `--keep-local` | Keep local files |
| `S3_DISABLE_TLS` | `--s3-disable-tls` | Disable TLS for S3 (use HTTP) |

See [ENVIRONMENT_VARIABLES.md](ENVIRONMENT_VARIABLES.md) for detailed usage examples.

## Docker Usage

### Using docker-compose (Recommended)

```yaml
version: '3.8'

services:
  data-ingest:
    build: .
    image: capture:latest
    environment:
      # TCP Stream
      - TCP_HOST=153.44.253.27
      - TCP_PORT=5631
      - SOURCE=norway-tcp
      
      # S3 Configuration  
      - S3_BUCKET=maritime-data
      - S3_REGION=us-west-2
      - S3_ACCESS_KEY=${AWS_ACCESS_KEY_ID}
      - S3_SECRET_KEY=${AWS_SECRET_ACCESS_KEY}
    volumes:
      - ./output:/data
    healthcheck:
      test: ["CMD", "/usr/local/bin/capture", "--health-check"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### Building the Docker Image

```bash
docker build -t capture .
```

### Running with Docker

```bash
# TCP stream with environment variables
docker run -d \
  --name data-ingest \
  -e TCP_HOST="153.44.253.27" \
  -e TCP_PORT="5631" \
  -e SOURCE="norway-tcp" \
  -v $(pwd)/output:/data \
  capture:latest
```

## Configuration Precedence

Configuration values are applied in the following order (highest to lowest precedence):

1. **Command-line arguments** (highest precedence)
2. **Environment variables**
3. **Default values** (lowest precedence)

This allows you to set base configuration via environment variables and override specific values with command-line arguments when needed.

## Output Structure

Data is organized in Hive-partitioned directories:

```
data/
├── source=file-ingest/
│   └── year=2025/
│       └── month=01/
│           └── day=15/
│               └── hour=14/
│                   └── minute=30/
│                       ├── 20250115_143045_001.parquet
│                       └── 20250115_143145_002.parquet
└── source=norway-tcp/
    └── year=2025/
        └── month=01/
            └── day=15/
                └── hour=14/
                    └── minute=31/
                        └── 20250115_143155_001.parquet
```

## Health Checks

The application includes health check functionality for container orchestration:

```bash
# Check health status
./capture --health-check

# Or using environment variable
HEALTH_CHECK=true ./capture
```

Health status is tracked in `/tmp/health_status` with timestamps and status information.

## S3 Integration

Supports AWS S3 and S3-compatible storage (MinIO) with background uploads to prevent data collection pauses:

### AWS S3
```bash
export S3_BUCKET="my-data-bucket"
export S3_REGION="us-west-2"
export S3_ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
export S3_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

### MinIO (with optional non-TLS mode)
```bash
export S3_BUCKET="data-lake"
export S3_ENDPOINT="http://minio:9000"
export S3_REGION="us-east-1"
export S3_ACCESS_KEY="minioadmin"
export S3_SECRET_KEY="minioadmin"
export S3_DISABLE_TLS="true"  # Use HTTP instead of HTTPS
```

### Features
- **Background Uploads**: Files are queued and uploaded asynchronously to prevent blocking data collection
- **Error Handling**: Failed uploads preserve local files with detailed error reporting
- **TLS Optional**: Can disable TLS for local development or internal networks
- **Pure Rust**: Uses rustls for TLS, no OpenSSL dependencies

See [S3_INTEGRATION.md](S3_INTEGRATION.md) for detailed configuration.

## Building from Source

```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Clone and build
git clone <repository-url>
cd capture
cargo build --release

# Run
./target/release/capture --help
```

## Performance Tuning

- **MAX_ROWS**: Control memory usage vs file size (default: flush on minute boundary)
- **Compression**: Uses Zstd for optimal compression ratio and speed
- **Async Processing**: Leverages Tokio for high-performance async I/O
- **Resource Limits**: Set appropriate memory limits in Docker for large datasets

## Troubleshooting

### Environment Variables Not Working
- Ensure variable names match exactly (case-sensitive)
- Check variable export in shell: `export VARIABLE_NAME=value`
- Verify docker-compose environment syntax

### Build Issues
```bash
# Update Rust toolchain
rustup update

# Clean and rebuild
cargo clean && cargo build --release
```

### Connection Issues
- **TCP**: Verify host/port accessibility: `telnet 153.44.253.27 5631`
- **S3**: Validate credentials and bucket permissions

See individual documentation files for detailed troubleshooting guides.

## License

[Add your license information here]

## Contributing

[Add contributing guidelines here]
