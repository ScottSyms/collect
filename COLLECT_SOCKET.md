# collect-socket — TCP data collector

Reads newline-delimited data from a TCP connection and writes it into
Hive-partitioned Parquet with Zstd compression. Supports AIS multi-part
message consolidation and `$PGHP` timestamp processing.

## Pipeline

```
TCP stream (newline-delimited) → collect-socket → Bronze Parquet
                                                  (ts, payload, source)
```

## Usage

```bash
# Basic TCP ingest
cargo run -p collect-socket -- \
  --tcp-host 153.44.253.27 --tcp-port 5631 \
  --source norway-tcp

# With S3 output
cargo run -p collect-socket -- \
  --tcp-host 153.44.253.27 --tcp-port 5631 \
  --source norway-tcp \
  --s3-bucket bronze --s3-prefix norway-tcp \
  --s3-endpoint http://minio:9000 \
  --s3-access-key minio --s3-secret-key minioadmin --s3-disable-tls

# With AIS multipart consolidation
cargo run -p collect-socket -- \
  --tcp-host 153.44.253.27 --tcp-port 5631 \
  --consolidate-ais \
  --process-timestamps
```

## CLI Reference

### Connection

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--tcp-host` | `TCP_HOST` | — | TCP host address |
| `--tcp-port` | `TCP_PORT` | — | TCP port |

### Source

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--source` / `-s` | `SOURCE` | `"tcp"` | Logical source label |

### AIS Processing

| Flag | Default | Description |
|------|---------|-------------|
| `--consolidate-ais` | off | Reassemble multi-part NMEA fragments into single sentences |
| `--process-timestamps` | off | Extract `$PGHP` and tag-block `c:` timestamps |

See [AIS_PARSE.md](AIS_PARSE.md) for details on the consolidation pipeline.

### Common (from `CommonCliArgs`)

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--output-dir` | `OUTPUT_DIR` | `"data"` | Local output root directory |
| `--partition` | `PARTITION` | `day` | Partition granularity (minute/hour/day/month/year) |
| `--max-rows` | `MAX_ROWS` | — | Max rows per file before flush |
| `--max-batch-bytes` | `MAX_BATCH_BYTES` | — | Max batch bytes before flush |
| `--compression-level` | `COMPRESSION_LEVEL` | `5` | Zstd compression level |
| `--upload-drain-timeout-seconds` | `UPLOAD_DRAIN_TIMEOUT_SECONDS` | `60` | Max seconds to wait for uploads on shutdown |
| `--max-line-length` | `MAX_LINE_LENGTH` | — | Max input line length |
| `--health-check` | `HEALTH_CHECK` | off | Serve health check endpoint |
| `--metrics-addr` | `METRICS_ADDR` | — | Prometheus metrics address |

### S3 (from `S3CliArgs`)

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--s3-bucket` | `S3_BUCKET` | — | S3 bucket (supports `bucket/path` syntax) |
| `--s3-prefix` | `S3_PREFIX` | — | Optional key prefix |
| `--s3-endpoint` | `S3_ENDPOINT` | — | Custom S3 endpoint |
| `--s3-region` | `S3_REGION` | `us-east-1` | AWS region |
| `--s3-access-key` | `S3_ACCESS_KEY` | — | Access key ID |
| `--s3-secret-key` | `S3_SECRET_KEY` | — | Secret access key |
| `--keep-local` | `KEEP_LOCAL` | false | Keep files after S3 upload |
| `--s3-disable-tls` | `S3_DISABLE_TLS` | false | Use HTTP instead of HTTPS |

## Output

- **Schema:** `ts` (timestamp ms UTC), `payload` (utf8), `source` (utf8)
- **Partition:** Hive-style under `--output-dir` (default: `data/`)
- **Compression:** Zstd
- **Reconnect:** Exponential backoff (1s → 5s max) on disconnect

## Reconnection

On TCP disconnect, `collect-socket` automatically reconnects with exponential
backoff starting at 1 second, doubling to a 5-second maximum. The ingest
stream resumes where it left off.
