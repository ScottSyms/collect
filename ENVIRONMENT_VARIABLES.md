# Environment Variable Configuration

Most command-line parameters can be configured using environment variables, making Docker deployments much easier to manage. This document lists the supported environment variables and their usage.

Every environment variable is also shown in each binary's `--help` output as
`[env: NAME=]` next to its flag — `--help` is the authoritative reference.
Each variable is the SCREAMING_SNAKE name of its flag unless noted.

## Environment Variable Reference

### Input Source Configuration (Choose One)

#### File Input
- `INPUT_PATH`: Path to input file or directory (collect-file)
  - Example: `INPUT_PATH=/input/data.txt`
  - Conflicts with TCP options

#### Parquet Input (batch tools)
- `INPUT_DIR`: Local Hive-partitioned Parquet root(s) for ais-parse / aisstream-parse
  - Comma-separated for multiple roots
  - Mutually exclusive with `INPUT_S3_BUCKET`

#### TCP Input  
- `TCP_HOST`: TCP host address to receive data from
  - Example: `TCP_HOST=153.44.253.27`
  - Requires `TCP_PORT`
  - Conflicts with file options

- `TCP_PORT`: TCP port to receive data from
  - Example: `TCP_PORT=5631`
  - Requires `TCP_HOST`
  - Conflicts with file options

### General Configuration

- `SOURCE`: Logical source label for data partitioning
  - Example: `SOURCE=ais-sf-bay`
  - Default: input file stem or "tcp" for network input

- `PARTITION`: Partition granularity for the dataset layout
  - Example: `PARTITION=hour`
  - Default: `day`

- `OUTPUT_DIR`: Output root directory for Parquet files
  - Example: `OUTPUT_DIR=/data`
  - Default: `data` (collectors); no default for the batch tools

- `CONCURRENCY`: Max concurrent workers (collect-file) / partitions processed in parallel (ais-parse, aisstream-parse)
  - Example: `CONCURRENCY=4`
  - Default: auto-selected

- `MAX_ROWS`: Maximum rows to buffer per Parquet file before flush
  - Example: `MAX_ROWS=10000`
  - Default: flush on the selected partition boundary only

- `MAX_BATCH_BYTES`: Maximum payload bytes to buffer per Parquet file before flush
  - Example: `MAX_BATCH_BYTES=67108864`
  - Default: `67108864` (64 MiB)

- `COMPRESSION_LEVEL`: Zstd compression level for Parquet output
  - Example: `COMPRESSION_LEVEL=1`
  - Default: `5`

- `MAX_LINE_LENGTH`: Maximum bytes allowed per input line
  - Example: `MAX_LINE_LENGTH=65536`
  - Default: `65536`

- `HEALTH_CHECK`: Run health check and exit (for Docker HEALTHCHECK)
  - Example: `HEALTH_CHECK=true`
  - Default: `false`

- `METRICS_ADDR`: Serve Prometheus metrics (`/metrics`) and an HTTP health check (`/healthz`) on this address
  - Example: `METRICS_ADDR=0.0.0.0:9184`
  - Default: disabled
  - `/healthz` returns `200` while the ingest loop heartbeat is fresh and `503` once it goes stale (60s window)

- `KEEP_LOCAL`: Keep local files after successful S3 upload
  - Example: `KEEP_LOCAL=true`
  - Default: `false` (delete after successful upload)

- `UPLOAD_DRAIN_TIMEOUT_SECONDS`: Max seconds to wait for background uploads on shutdown
  - Example: `UPLOAD_DRAIN_TIMEOUT_SECONDS=60`
  - Default: `60`
  - On SIGTERM the collectors flush the in-memory batch, finish queued Parquet writes, and drain uploads for up to this long. Make sure your orchestrator's stop grace period (Docker `stop_grace_period`, Nomad `kill_timeout`) exceeds this value.

### S3 Configuration

- `S3_BUCKET`: S3 bucket name for remote storage (enables S3 upload)
  - Example: `S3_BUCKET=maritime-data`
  - Supports `bucket/path` syntax — the path component becomes the key prefix

- `S3_PREFIX`: Optional key prefix for S3 uploads (appended to any path from `S3_BUCKET`)
  - Example: `S3_PREFIX=norway`

- `INPUT_S3_BUCKET`: Input S3 bucket for batch processing tools (ais-parse, aisstream-parse)
  - Example: `INPUT_S3_BUCKET=bronze/norway`
  - Comma-separated for multiple buckets: `INPUT_S3_BUCKET=bronze/norway,duplicate`
  - Supports `bucket/path` syntax per entry

- `INPUT_S3_PREFIX`: Key prefix within the input S3 bucket
  - Example: `INPUT_S3_PREFIX=raw-data`

- `OUTPUT_S3_BUCKET`: Output S3 bucket for batch processing tools
  - Supports `bucket/path` syntax

- `OUTPUT_S3_PREFIX`: Key prefix within the output S3 bucket
  - Example: `OUTPUT_S3_PREFIX=silver`

- `S3_ENDPOINT`: S3 endpoint URL (for MinIO or custom S3-compatible storage)
  - Example: `S3_ENDPOINT=https://minio.example.com`
  - Example: `S3_ENDPOINT=http://localhost:9000`

- `S3_REGION`: S3 region
  - Example: `S3_REGION=us-west-2`
  - Default: `us-east-1`

- `S3_ACCESS_KEY`: S3 access key ID
  - Example: `S3_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE`
  - Can also use standard AWS environment variable `AWS_ACCESS_KEY_ID`

- `S3_SECRET_KEY`: S3 secret access key
  - Example: `S3_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`
  - Can also use standard AWS environment variable `AWS_SECRET_ACCESS_KEY`

### Iceberg Output Options

- `ICEBERG_CATALOG_URI`: Iceberg REST catalog URI
  - Example: `ICEBERG_CATALOG_URI=http://lakekeeper:8181/catalog`

- `ICEBERG_WAREHOUSE`: Iceberg warehouse location
  - Example: `ICEBERG_WAREHOUSE=s3://my-bucket/warehouse`

- `ICEBERG_NAMESPACE`: Iceberg namespace (database)
  - Example: `ICEBERG_NAMESPACE=ais`
  - Default: `ais`

- `ICEBERG_TABLE_PREFIX`: Optional prefix for Iceberg table names
  - Example: `ICEBERG_TABLE_PREFIX=prod`

- `ICEBERG_TOKEN`: Bearer token for Lakekeeper / REST catalog authentication
  - Example: `ICEBERG_TOKEN=lk_demo_xxx`

### Batch Processing Options (ais-parse, aisstream-parse)

- `INCREMENTAL`: Enable watermark-based incremental processing
  - Example: `INCREMENTAL=true`

- `SINCE`: Rolling window in hours (with `INCREMENTAL`, first-run seed only)
  - Example: `SINCE=2`

- `PARTITION`: Partition granularity of the input dataset layout
  - Example: `PARTITION=day`

- `BATCH_SIZE`: Rows per Parquet read batch
  - Default: `8192`

- `FILTER_SOURCE`: Process only this source label
  - Example: `FILTER_SOURCE=norway`

- `OUTPUT_PREFIX`: Output file name prefix
  - Default: `ais` (ais-parse) / `aisstream` (aisstream-parse)

- `DRY_RUN`: List the partitions that would be processed and exit, without decoding, writing, or connecting to the output target
  - Example: `DRY_RUN=true`

### Common to Every Binary

- `QUIET`: Suppress routine progress lines (scanning/listing/per-batch chatter); warnings, errors, and the final summary still print
  - Example: `QUIET=true`
  - Every binary also accepts `-q` / `--quiet` and `--completions <shell>` (bash/zsh/fish/elvish/powershell) as CLI-only flags (no env equivalent for completions, since it's a one-shot action, not a runtime mode)

### Source-Specific Options

- Kafka (collect-kafka): `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `KAFKA_AUTO_OFFSET_RESET`
- AISStream (collect-aisstream): `AISSTREAM_API_KEY`, `BOUNDING_BOXES`, `FILTER_MMSI`, `FILTER_MESSAGE_TYPES` (comma-separated lists)

## Docker Compose Examples

### TCP Stream with S3 Upload

```yaml
version: '3.8'

services:
  data-ingest:
    image: collect:latest
    environment:
      - TCP_HOST=153.44.253.27
      - TCP_PORT=5631
      - SOURCE=norway-tcp
      - S3_BUCKET=maritime-data
      - S3_REGION=us-west-2
      - S3_ACCESS_KEY=${AWS_ACCESS_KEY_ID}
      - S3_SECRET_KEY=${AWS_SECRET_ACCESS_KEY}
      - KEEP_LOCAL=false
    volumes:
      - ./output:/data
```

### TCP Stream Input

```yaml
version: '3.8'

services:
  data-ingest:
    image: collect:latest
    environment:
      - TCP_HOST=153.44.253.27
      - TCP_PORT=5631
      - SOURCE=norway-tcp
      - OUTPUT_DIR=/data
      - MAX_ROWS=5000
    volumes:
      - ./output:/data
```

### File Input with MinIO

```yaml
version: '3.8'

services:
  data-ingest:
    image: collect:latest
    environment:
      - INPUT_PATH=/input/data.txt
      - SOURCE=batch-data
      - S3_BUCKET=data-lake
      - S3_ENDPOINT=http://minio:9000
      - S3_REGION=us-east-1
      - S3_ACCESS_KEY=minioadmin
      - S3_SECRET_KEY=minioadmin
      - KEEP_LOCAL=true
    volumes:
      - ./input:/input:ro
      - ./output:/data
```

## Environment Variable Precedence

1. **Command-line arguments** take highest precedence (an explicit flag always
   wins, even when its value equals the default)
2. **Environment variables** are used if no command-line argument is provided
3. **Default values** are used if neither is specified

Note: for mutually exclusive pairs (`--input-dir` vs `--input-s3-bucket`,
`--output-dir` vs `--output-s3-bucket`), a value supplied via environment
variable counts as "set" — unset the variable rather than relying on a flag to
switch modes.

This allows for flexible configuration where you can:
- Set base configuration via environment variables
- Override specific values with command-line arguments when needed

## Missing Environment Variables

The application gracefully handles missing environment variables:

- **Optional parameters** (like `S3_BUCKET`): Missing environment variables are treated as unset/empty
- **Parameters with defaults** (like `S3_REGION`, `OUTPUT_DIR`, `PARTITION`): Use their default values when environment variable is missing
- **Boolean parameters** (like `HEALTH_CHECK`, `KEEP_LOCAL`): Default to `false` when environment variable is missing
## Docker Health Checks

The application supports Docker health checks via the `HEALTH_CHECK` environment variable or `--health-check` command-line flag:

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=10s \
  CMD ["/usr/local/bin/collect-socket", "--health-check"]
```

Or using environment variables:

```yaml
services:
  data-ingest:
    image: collect:latest
    environment:
      - HEALTH_CHECK=true
    healthcheck:
      test: ["CMD", "/usr/local/bin/collect-socket"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
```

The healthcheck command inherits the service environment, so `HEALTH_CHECK=true` is visible there. Swap `collect-socket` for `collect-file` if the container runs file ingestion.

## Best Practices

1. **Use environment variables for Docker deployments** - easier to manage and more secure
2. **Store sensitive values in secrets** - use Docker secrets or Kubernetes secrets for API keys
3. **Set appropriate resource limits** - especially memory limits for large datasets
4. **Use volume mounts for data persistence** - ensure data survives container restarts
5. **Configure health checks** - enable proper container orchestration
6. **Use explicit regions for S3** - avoid default region assumptions
7. **Test configurations locally** - validate settings before production deployment

## Security Considerations

- Never commit API keys or credentials to version control
- Use environment variable substitution in docker-compose files: `${VARIABLE_NAME}`
- Consider using Docker secrets for production deployments
- Restrict S3 bucket access with appropriate IAM policies
- Validate network access for TCP connections

## Troubleshooting

### Environment Variable Not Being Read
- Ensure the environment variable name matches exactly (case-sensitive)
- Check that the variable is properly exported in your shell
- Verify docker-compose syntax for environment variables

### Conflicting Input Sources
- Only one input method can be specified (file or TCP)
- Remove conflicting environment variables
- Check for both environment variables and command-line arguments

### S3 Upload Issues
- Verify bucket exists and is accessible
- Check S3 credentials and permissions
- Validate endpoint URL format for MinIO/custom S3
- Ensure network connectivity to S3 endpoint
