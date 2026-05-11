# Docker Health Check Setup

This document covers the Docker-friendly health checks implemented for the data ingestion binaries.

## Health Check Features

### 1. Built-in Health Check Command
The application supports a `--health-check` flag that can be used by Docker's HEALTHCHECK instruction.

```bash
# Manual health check
./target/release/collect-socket --health-check
./target/release/collect-file --health-check
```

### 2. Health Status Tracking
- The application maintains a health status file at `/tmp/collect-socket.health` (socket binary) or `/tmp/collect-file.health` (file binary)
- Status is refreshed every second while ingesting
- Health check considers the application healthy if:
  - Status file exists
  - Status is "healthy" 
  - Timestamp is within the last 60 seconds

### 3. Docker Integration

#### Building the Docker Image
```bash
docker build -t collect .
```

#### Running with Docker Compose
```bash
# Start the service
docker-compose up -d

# Check health status
docker-compose ps

# View health check logs
docker inspect --format='{{json .State.Health}}' data-ingest | jq
```

The default image entrypoint is `collect-socket`; use `--entrypoint /usr/local/bin/collect-file` for file ingestion.

#### Manual Docker Run Examples

**File Input:**
```bash
docker run -d \
  --name data-ingest \
  --entrypoint /usr/local/bin/collect-file \
  -v $(pwd)/input:/input:ro \
  -v $(pwd)/output:/data \
  --health-cmd "/usr/local/bin/collect-file --health-check" \
  --health-interval 30s \
  --health-timeout 10s \
  --health-retries 3 \
  collect \
  --input /input/data.txt --source mydata
```

**TCP Input:**
```bash
docker run -d \
  --name data-ingest \
  -v $(pwd)/output:/data \
  --health-cmd "/usr/local/bin/collect-socket --health-check" \
  --health-interval 30s \
  --health-timeout 10s \
  --health-retries 3 \
  collect \
  --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp
```

### 4. Health Check Endpoints

The health check system provides these status codes:
- **Exit 0**: Healthy - application is running and processing data
- **Exit 1**: Unhealthy - application is stalled, crashed, or not processing data

### 5. Troubleshooting

#### Viewing Health Status
```bash
# Check current health status
docker exec data-ingest cat /tmp/collect-socket.health

# Use /tmp/collect-file.health when the container runs the file binary

# View container health history
docker inspect data-ingest | jq '.State.Health.Log'

# Follow application logs
docker-compose logs -f data-ingest
```

#### Common Issues
1. **Health check failing**: Check if the application is actually processing data
2. **Stale timestamps**: Application may be stuck in a loop or blocked on input
3. **Permission issues**: Ensure the application can write to the relevant `/tmp/collect-*.health` file

#### Debug Mode
```bash
# Run with debug output
docker run --rm -it collect --health-check
```

This health check system provides comprehensive monitoring for Docker environments, ensuring your data ingestion pipeline maintains high availability and can be automatically managed by orchestration systems.
