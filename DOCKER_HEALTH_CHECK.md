# Docker Health Check Setup

This document explains the Docker-friendly health checks implemented for the data ingestion application.

## Health Check Features

### 1. Built-in Health Check Command
The application now supports a `--health-check` flag that can be used by Docker's HEALTHCHECK instruction.

```bash
# Manual health check
./hive_parquet_ingest --health-check
```

### 2. Health Status Tracking
- The application maintains a health status file at `/tmp/app_health`
- Status is updated every 100 processed records
- Health check considers the application healthy if:
  - Status file exists
  - Status is "healthy" 
  - Timestamp is within the last 60 seconds

### 3. Docker Integration

#### Building the Docker Image
```bash
docker build -t hive-parquet-ingest .
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

#### Manual Docker Run Examples

**File Input:**
```bash
docker run -d \
  --name data-ingest \
  -v $(pwd)/input:/input:ro \
  -v $(pwd)/output:/data \
  --health-cmd "/usr/local/bin/hive_parquet_ingest --health-check" \
  --health-interval 30s \
  --health-timeout 10s \
  --health-retries 3 \
  hive-parquet-ingest \
  --input /input/data.txt --source mydata
```

**TCP Input:**
```bash
docker run -d \
  --name data-ingest \
  -v $(pwd)/output:/data \
  --health-cmd "/usr/local/bin/hive_parquet_ingest --health-check" \
  --health-interval 30s \
  --health-timeout 10s \
  --health-retries 3 \
  hive-parquet-ingest \
  --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp
```

### 4. Health Monitoring Script

Use the provided `health-monitor.sh` script for additional monitoring:

```bash
# Run health check once
./health-monitor.sh

# Set up as a cron job (every 5 minutes)
echo "*/5 * * * * /path/to/health-monitor.sh" | crontab -
```

### 5. Health Check Endpoints

The health check system provides these status codes:
- **Exit 0**: Healthy - application is running and processing data
- **Exit 1**: Unhealthy - application is stalled, crashed, or not processing data

### 6. Monitoring Integration

#### Prometheus Integration
You can expose health metrics by parsing the health status file:

```bash
# Example metric export
echo "app_healthy{service=\"data-ingest\"} $([ -f /tmp/app_health ] && echo 1 || echo 0)"
```

#### Alerting Examples

**Docker Compose Override for Monitoring:**
```yaml
# docker-compose.override.yml
version: '3.8'
services:
  data-ingest:
    labels:
      - "monitoring.health-check=true"
      - "monitoring.service=data-ingest"
```

**Kubernetes Health Check:**
```yaml
livenessProbe:
  exec:
    command:
    - /usr/local/bin/hive_parquet_ingest
    - --health-check
  initialDelaySeconds: 30
  periodSeconds: 30
  timeoutSeconds: 10
  failureThreshold: 3

readinessProbe:
  exec:
    command:
    - /usr/local/bin/hive_parquet_ingest  
    - --health-check
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 2
```

### 7. Troubleshooting

#### Viewing Health Status
```bash
# Check current health status
docker exec data-ingest cat /tmp/app_health

# View container health history
docker inspect data-ingest | jq '.State.Health.Log'

# Follow application logs
docker-compose logs -f data-ingest
```

#### Common Issues
1. **Health check failing**: Check if the application is actually processing data
2. **Stale timestamps**: Application may be stuck in a loop or blocked on input
3. **Permission issues**: Ensure the application can write to `/tmp/app_health`

#### Debug Mode
```bash
# Run with debug output
docker run --rm -it hive-parquet-ingest --health-check
```

This health check system provides comprehensive monitoring for Docker environments, ensuring your data ingestion pipeline maintains high availability and can be automatically managed by orchestration systems.