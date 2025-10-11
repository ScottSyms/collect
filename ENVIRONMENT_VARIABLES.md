# Environment Variable Configuration

All command-line parameters can be configured using environment variables, making Docker deployments much easier to manage. This document lists all available environment variables and their usage.

## Environment Variable Reference

### Input Source Configuration (Choose One)

#### File Input
- `INPUT_FILE`: Path to input text file (one record per line)
  - Example: `INPUT_FILE=/input/data.txt`
  - Conflicts with TCP and WebSocket options

#### TCP Input  
- `TCP_HOST`: TCP host address to receive data from
  - Example: `TCP_HOST=153.44.253.27`
  - Requires `TCP_PORT`
  - Conflicts with file and WebSocket options

- `TCP_PORT`: TCP port to receive data from
  - Example: `TCP_PORT=5631`
  - Requires `TCP_HOST`
  - Conflicts with file and WebSocket options

#### WebSocket Input
- `WS_URL`: WebSocket URL to connect to
  - Example: `WS_URL=wss://stream.aisstream.io/v0/stream`
  - Conflicts with file and TCP options

- `WS_API_KEY`: API key for WebSocket authentication (required for AISStream.io)
  - Example: `WS_API_KEY=your-api-key-here`
  - Requires `WS_URL`

- `WS_BBOX`: Bounding box for WebSocket subscription (comma-separated values)
  - Format: `lat1,lon1,lat2,lon2` for multiple boxes
  - Example: `WS_BBOX=37.9,-122.6,37.6,-122.3,40.7,-74.0,40.6,-73.9`
  - Default: entire world if not specified
  - Requires `WS_URL`

- `WS_MMSI_FILTER`: Filter WebSocket messages by MMSI (comma-separated, max 50)
  - Example: `WS_MMSI_FILTER=123456789,987654321,555666777`
  - Requires `WS_URL`

- `WS_MESSAGE_TYPE_FILTER`: Filter WebSocket messages by message type (comma-separated)
  - Example: `WS_MESSAGE_TYPE_FILTER=PositionReport,StaticAndVoyageRelatedData`
  - Requires `WS_URL`

### General Configuration

- `SOURCE`: Logical source label for data partitioning
  - Example: `SOURCE=ais-sf-bay`
  - Default: input file stem or "tcp" for network input

- `OUT_DIR`: Output root directory for Parquet files
  - Example: `OUT_DIR=/data`
  - Default: `data`

- `MAX_ROWS`: Maximum rows to buffer per Parquet file before flush
  - Example: `MAX_ROWS=10000`
  - Default: flush on minute boundary only

- `HEALTH_CHECK`: Run health check and exit (for Docker HEALTHCHECK)
  - Example: `HEALTH_CHECK=true`
  - Default: `false`

- `KEEP_LOCAL`: Keep local files after S3 upload
  - Example: `KEEP_LOCAL=true`
  - Default: `false` (delete after successful upload)

### S3 Configuration

- `S3_BUCKET`: S3 bucket name for remote storage (enables S3 upload)
  - Example: `S3_BUCKET=maritime-data`

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

## Docker Compose Examples

### WebSocket AIS Stream with S3 Upload

```yaml
version: '3.8'

services:
  data-ingest:
    image: hive-parquet-ingest:latest
    environment:
      - WS_URL=wss://stream.aisstream.io/v0/stream
      - WS_API_KEY=${AIS_API_KEY}
      - WS_BBOX=37.9,-122.6,37.6,-122.3
      - SOURCE=ais-sf-bay
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
    image: hive-parquet-ingest:latest
    environment:
      - TCP_HOST=153.44.253.27
      - TCP_PORT=5631
      - SOURCE=norway-tcp
      - OUT_DIR=/data
      - MAX_ROWS=5000
    volumes:
      - ./output:/data
```

### File Input with MinIO

```yaml
version: '3.8'

services:
  data-ingest:
    image: hive-parquet-ingest:latest
    environment:
      - INPUT_FILE=/input/data.txt
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

1. **Command-line arguments** take highest precedence
2. **Environment variables** are used if no command-line argument is provided
3. **Default values** are used if neither is specified

This allows for flexible configuration where you can:
- Set base configuration via environment variables
- Override specific values with command-line arguments when needed

## Missing Environment Variables

The application gracefully handles missing environment variables:

- **Optional parameters** (like `S3_BUCKET`, `WS_URL`): Missing environment variables are treated as unset/empty
- **Parameters with defaults** (like `S3_REGION`, `OUT_DIR`): Use their default values when environment variable is missing
- **Boolean parameters** (like `HEALTH_CHECK`, `KEEP_LOCAL`): Default to `false` when environment variable is missing
- **Array parameters** (like `WS_BBOX`, `WS_MMSI_FILTER`): Default to empty arrays when environment variable is missing

### Comma-Separated Values

For array parameters, you can use comma-separated values in environment variables:

```bash
# Multiple bounding boxes
export WS_BBOX="37.9,-122.6,37.6,-122.3,40.7,-74.0,40.6,-73.9"

# Multiple MMSI filters  
export WS_MMSI_FILTER="123456789,987654321,555666777"

# Multiple message types
export WS_MESSAGE_TYPE_FILTER="PositionReport,StaticAndVoyageRelatedData"
```

Empty or missing array environment variables will result in empty arrays, which is equivalent to not specifying the parameter.

## Docker Health Checks

The application supports Docker health checks via the `HEALTH_CHECK` environment variable or `--health-check` command-line flag:

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=10s \
  CMD ["/usr/local/bin/hive_parquet_ingest", "--health-check"]
```

Or using environment variables:

```yaml
healthcheck:
  test: ["CMD", "/usr/local/bin/hive_parquet_ingest"]
  environment:
    - HEALTH_CHECK=true
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 10s
```

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
- Use HTTPS endpoints for WebSocket connections
- Validate network access for TCP connections

## Troubleshooting

### Environment Variable Not Being Read
- Ensure the environment variable name matches exactly (case-sensitive)
- Check that the variable is properly exported in your shell
- Verify docker-compose syntax for environment variables

### Conflicting Input Sources
- Only one input method can be specified (file, TCP, or WebSocket)
- Remove conflicting environment variables
- Check for both environment variables and command-line arguments

### S3 Upload Issues
- Verify bucket exists and is accessible
- Check S3 credentials and permissions
- Validate endpoint URL format for MinIO/custom S3
- Ensure network connectivity to S3 endpoint

### WebSocket Connection Problems
- Verify API key is valid and active
- Check WebSocket URL is reachable
- Validate bounding box coordinates format
- Ensure MMSI filter doesn't exceed 50 entries