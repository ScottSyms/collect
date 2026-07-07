# collect

A Rust workspace for ingesting positional data into Hive-partitioned Parquet files with Zstd compression — the bronze layer of a medallion pipeline for maritime (AIS) data. It provides:

- **`collect-file`** — recursive file ingestion (plain, gzip, bzip2, zip)
- **`collect-socket`** — TCP line-stream ingestion
- **`collect-kafka`** — Kafka topic ingestion with at-least-once offset commits
- **`collect-aisstream`** — aisstream.io WebSocket ingestion
- **`ais-normalize`** — post-processing: re-timestamp, re-partition, and combine multi-part AIS sentences (input and output can each be local or S3)
- **`ais-parse`** — silver layer: decode AIS sentences into typed Parquet (vessel positions and statics), via [ScottSyms/nmea-parser](https://github.com/ScottSyms/nmea-parser); local or S3 on both sides

All collectors support optional remote storage (S3/MinIO).

## Features
- **Multiple Input Sources**: Files, TCP streams, Kafka topics, and aisstream.io WebSocket
- **Compressed Inputs**: Plain text, gzip, bzip2, and zip files
- **AIS Normalization**: Fragment reassembly, tag-block/`$PGHP` re-timestamping, parallel partition processing, S3-to-S3, multi-source merge into one time-partitioned dataset (source retained as a column), idempotent re-runs (exact `(ts, source, payload)` dedup-merge), and watermark-based incremental scheduling (`--incremental`)
- **AIS Decoding**: Typed Parquet output decoded from AIVDM sentences — positions (MMSI, lat/lon, SOG/COG), vessel statics (name, destination, dimensions), and Type 8 Binary Broadcast including full meteorological/hydrological data (wind, temperature, pressure, waves, currents, tides); unrecognized Type 8 subtypes retained as hex — with idempotent partition-replace re-runs
- **Hive Partitioning**: Automatic partitioning by source and selected time granularity
- **Parquet Format**: Efficient columnar storage with Zstd compression, sorted by timestamp
- **S3 Integration**: Upload to AWS S3 or S3-compatible storage (MinIO) with optional TLS
- **Background Uploads**: Non-blocking S3 uploads to prevent data collection pauses
- **At-Least-Once Delivery**: Graceful-shutdown flush, startup sweep of orphaned files, and Kafka offsets committed only after data is durable on disk
- **Observability**: Optional Prometheus `/metrics` and HTTP `/healthz` endpoint per collector
- **Docker Support**: Full Docker and docker-compose integration with health checks
- **Environment Variables**: Complete environment variable support for containerized deployments
- **Bounded Memory**: Byte-budgeted write pipeline; predictable footprint under backpressure
- **Health Monitoring**: File-based health checks plus an HTTP endpoint for orchestration
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

cargo run -p collect-socket --
```

### Using Command Line Arguments

```bash
# File input
cargo run -p collect-file -- --input-dir data.txt --source mydata

# TCP stream
cargo run -p collect-socket -- --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp

# Kafka topic
cargo run -p collect-kafka -- --kafka-brokers broker:9092 --topic ais-raw --group-id collect

# aisstream.io WebSocket (worldwide bounding box)
cargo run -p collect-aisstream -- --api-key $AISSTREAM_API_KEY --bounding-boxes '[[[-90,-180],[90,180]]]'

# File input with S3
cargo run -p collect-file -- --input-dir data.txt --source mydata --compression-level 1 --s3-bucket maritime-data

# Normalize collected AIS data (combine fragments, re-timestamp/re-partition)
cargo run -p ais-normalize -- --input-dir data --output-dir normalized --partition day

# Normalize straight from one S3 bucket to another (same endpoint/region/credentials)
cargo run -p ais-normalize -- --input-s3-bucket raw-ais --output-s3-bucket normalized-ais --s3-endpoint http://minio:9000 --partition day

# Decode normalized AIS into typed Parquet (positions + vessel statics)
cargo run -p ais-parse -- --input-dir normalized --output-dir silver --partition day
```

See [AIS_NORMALIZE.md](AIS_NORMALIZE.md) for how fragment reassembly and re-timestamping work, the full CLI reference, and deployment as a scheduled batch job. See [AIS_PARSE.md](AIS_PARSE.md) for the decoded (silver) schemas and the bronze → silver pipeline.

`collect-file` auto-detects plain text, gzip, bzip2, and zip inputs. Zip archives are read entry-by-entry in archive order. Hidden dotfiles are skipped silently. `--concurrency` overrides the auto-selected file worker count.

## Environment Variables

Most command-line parameters can be configured using environment variables.

| Environment Variable | CLI Argument | Description |
|---------------------|--------------|-------------|
| `INPUT_PATH` / `INPUT_FILE` | `--input-dir` | Input file or directory path |
| `TCP_HOST` | `--tcp-host` | TCP host address |
| `TCP_PORT` | `--tcp-port` | TCP port number |
| `SOURCE` | `--source` | Logical source label |
| `PARTITION` | `--partition` | Partition granularity for ingest layout |
| `AIS` | `--ais` | Use NMEA `c:<epoch>` tag blocks or `$PGHP` capture timestamps (collect-file only) |
| `OUTPUT_DIR` | `--output-dir` | Output directory |
| `MAX_ROWS` | `--max-rows` | Max rows per file |
| `MAX_BATCH_BYTES` | `--max-batch-bytes` | Max payload bytes per Parquet file |
| `COMPRESSION_LEVEL` | `--compression-level` | Zstd compression level for Parquet output |
| `MAX_LINE_LENGTH` | `--max-line-length` | Max bytes per input line |
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
    image: collect:latest
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
      test: ["CMD", "/usr/local/bin/collect-socket", "--health-check"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### Building the Docker Image

```bash
docker build -t collect .
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
  collect:latest
```

The image defaults to `collect-socket`; use `--entrypoint /usr/local/bin/collect-file` for file ingestion or `--entrypoint /usr/local/bin/collect-maint` for maintenance commands.

## Configuration Precedence

Configuration values are applied in the following order (highest to lowest precedence):

1. **Command-line arguments** (highest precedence)
2. **Environment variables**
3. **Default values** (lowest precedence)

This allows you to set base configuration via environment variables and override specific values with command-line arguments when needed.

## TUI

Both binaries include an interactive console setup flow:

```bash
cargo run -p collect-file -- --tui
cargo run -p collect-socket -- --tui
```

The file binary saves to `collect-file-config.json` by default. The socket binary saves to `collect-socket-config.json` by default.

## Output Structure

Data is organized in Hive-partitioned directories:

```
data/
├── source=file-ingest/
│   └── year=2025/
│       └── month=01/
│           └── day=15/
│               └── part-20250115T000000000-000000.parquet
└── source=norway-tcp/
    └── year=2025/
        └── month=01/
            └── day=15/
                └── part-20250115T000000000-000001.parquet
```

Use `--partition day|hour|minute|month|year` to choose how deep the time hierarchy goes. The default is `day`.

## Health Checks

The application includes health check functionality for container orchestration:

```bash
# Check health status
./target/release/collect-socket --health-check

# Or using environment variable
HEALTH_CHECK=true ./target/release/collect-socket
```

Health status is tracked in `/tmp/collect-socket.health` for the socket binary and `/tmp/collect-file.health` for the file binary.

When `--metrics-addr` is set (see below), an HTTP `GET /healthz` endpoint is also available — it returns `200` while the ingest loop's heartbeat is fresh and `503` once it goes stale (60-second window), so it detects hung loops rather than just live processes. Prefer it for Nomad/Kubernetes HTTP checks.

## Observability

Every collector can serve Prometheus metrics with `--metrics-addr` (or `METRICS_ADDR`):

```bash
cargo run -p collect-socket -- --tcp-host host --tcp-port 5631 --metrics-addr 0.0.0.0:9184
curl localhost:9184/metrics
curl localhost:9184/healthz
```

Exposed metrics (all labeled with `source="..."`):

| Metric | Type | Meaning |
|--------|------|---------|
| `collect_rows_processed_total` | counter | Rows ingested since process start |
| `collect_batches_sealed_total` | counter | Batches queued for Parquet writing |
| `collect_batches_durable_total` | counter | Batches durably written to local disk |
| `collect_buffered_bytes` | gauge | Payload bytes in the open batch |
| `collect_uploads_succeeded_total` | counter | Completed S3 uploads |
| `collect_uploads_failed_total` | counter | Uploads abandoned after retries |
| `collect_upload_retries_total` | counter | Upload attempts retried |
| `collect_orphan_files_swept_total` | counter | Orphaned files queued at startup |
| `collect_last_row_unix_ms` | gauge | Timestamp of most recent row |
| `collect_last_heartbeat_unix_ms` | gauge | Ingest loop heartbeat |

## Delivery Guarantees

- **Graceful shutdown**: on SIGTERM/SIGINT the collectors stop reading, flush the in-memory batch, finish every queued Parquet write, and drain pending S3 uploads for up to `UPLOAD_DRAIN_TIMEOUT_SECONDS` (default 60s). Give your orchestrator a stop grace period longer than that (`kill_timeout` in Nomad, `stop_grace_period` in Docker Compose).
- **Orphan sweep**: at startup each collector scans its output directory for Parquet files a previous run wrote but never uploaded (crash, SIGKILL, expired drain window) and uploads them in the background. Skipped when `KEEP_LOCAL=true`, since uploaded files can't be distinguished from orphans.
- **Kafka offsets**: `collect-kafka` disables auto-commit and commits offsets only after the batch containing a message is durably written to local disk, giving at-least-once delivery — a crash replays at most a few messages instead of losing them.

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
cd collect
cargo build --release --workspace

# Run
./target/release/collect-file --help
./target/release/collect-socket --help
```

## Performance Tuning

- **MAX_ROWS**: Cap rows per file when you want smaller Parquet chunks (default: flush on the partition boundary)
- **MAX_BATCH_BYTES**: Cap buffered payload size per Parquet file (default: 64 MiB). The write pipeline holds at most ~4× this in flight, so worst-case ingest memory is roughly `5 × MAX_BATCH_BYTES` plus a small base.
- **Compression**: Zstd level 5 by default; level 3 is ~40% faster for a few percent more size — a good trade for CPU-constrained live collectors
- **collect-file `--concurrency`**: overrides the auto-selected worker count (defaults to ~1–2× cores; per-worker buffers scale down automatically)
- **ais-normalize `--concurrency`**: partitions are processed in parallel (defaults to up to 8 workers)
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
cargo clean && cargo build --release --workspace
```

### Connection Issues
- **TCP**: Verify host/port accessibility: `telnet 153.44.253.27 5631`
- **S3**: Validate credentials and bucket permissions

See individual documentation files for detailed troubleshooting guides.
