# WebSocket Integration for AIS Stream

This application now supports consuming live maritime AIS (Automatic Identification System) data from WebSocket feeds, specifically designed to work with [AISStream.io](https://aisstream.io/).

## AIS Stream WebSocket Support

### Overview
The WebSocket integration allows real-time consumption of maritime vessel tracking data from AIS stations worldwide. The data includes:
- **Position Reports**: Real-time vessel locations, speed, and heading
- **Ship Static Data**: Vessel names, dimensions, and characteristics  
- **Navigation Status**: Anchored, under way, moored, etc.
- **Safety Messages**: Maritime safety broadcasts

### Usage Examples

#### Basic World-Wide AIS Feed
```bash
./hive_parquet_ingest \
    --ws-url wss://stream.aisstream.io/v0/stream \
    --ws-api-key your-api-key-here \
    --source ais-global
```

#### Regional Bounding Box (San Francisco Bay Area)
```bash
./hive_parquet_ingest \
    --ws-url wss://stream.aisstream.io/v0/stream \
    --ws-api-key your-api-key-here \
    --ws-bbox "37.9,-122.6,37.6,-122.3" \
    --source ais-sf-bay
```

#### Multiple Bounding Boxes (LA and Miami Ports)
```bash
./hive_parquet_ingest \
    --ws-url wss://stream.aisstream.io/v0/stream \
    --ws-api-key your-api-key-here \
    --ws-bbox "33.8,-118.4,33.6,-118.1" \
    --ws-bbox "25.8,-80.2,25.6,-79.9" \
    --source ais-ports \
    --ws-message-type-filter PositionReport
```

#### Specific Vessel Tracking (by MMSI)
```bash
./hive_parquet_ingest \
    --ws-url wss://stream.aisstream.io/v0/stream \
    --ws-api-key your-api-key-here \
    --ws-mmsi-filter 368207620 \
    --ws-mmsi-filter 367719770 \
    --source ais-specific-vessels
```

#### With S3 Storage
```bash
./hive_parquet_ingest \
    --ws-url wss://stream.aisstream.io/v0/stream \
    --ws-api-key your-api-key-here \
    --ws-bbox "25.8,-80.2,25.6,-79.9" \
    --source ais-miami \
    --s3-bucket maritime-data \
    --s3-region us-east-1
```

## WebSocket Command Line Options

| Option | Description | Required | Default | Example |
|--------|-------------|----------|---------|---------|
| `--ws-url` | WebSocket endpoint URL | Yes | - | `wss://stream.aisstream.io/v0/stream` |
| `--ws-api-key` | API authentication key | Yes | - | `your-api-key` |
| `--ws-bbox` | Bounding box (lat1,lon1,lat2,lon2) | No | World | `37.9,-122.6,37.6,-122.3` |
| `--ws-mmsi-filter` | Filter by vessel MMSI | No | All vessels | `368207620` |
| `--ws-message-type-filter` | Filter by AIS message type | No | All types | `PositionReport` |

### Bounding Box Format
Specify as comma-separated coordinates: `lat1,lon1,lat2,lon2`
- **lat1,lat2**: Latitude range (-90 to 90)
- **lon1,lon2**: Longitude range (-180 to 180)
- **Multiple boxes**: Use `--ws-bbox` multiple times

**Examples:**
- **San Francisco Bay**: `37.9,-122.6,37.6,-122.3`
- **English Channel**: `51.5,1.0,50.0,-1.0`
- **Entire World**: `-90,-180,90,180` (default if none specified)

### Message Type Filters
Common AIS message types you can filter by:
- `PositionReport` - Vessel position updates (most common)
- `ShipStaticData` - Vessel information and characteristics
- `BaseStationReport` - AIS base station information
- `SafetyBroadcastMessage` - Maritime safety messages
- `AidsToNavigationReport` - Navigation aid status

### MMSI Filtering
- **MMSI**: Maritime Mobile Service Identity (unique vessel identifier)
- **Maximum**: 50 MMSI values per subscription
- **Format**: 9-digit numbers (e.g., `368207620`)
- **Usage**: Specify `--ws-mmsi-filter` multiple times

## AIS Data Format

The application processes JSON messages in AISStream.io format:

```json
{
  "MessageType": "PositionReport",
  "MetaData": {
    "MMSI": 368207620,
    "ShipName": "VESSEL NAME",
    "latitude": 37.7749,
    "longitude": -122.4194,
    "time_utc": "2025-10-11 12:34:56.789 +0000 UTC"
  },
  "Message": {
    "PositionReport": {
      "UserID": 368207620,
      "Latitude": 37.7749,
      "Longitude": -122.4194,
      "Sog": 12.5,
      "Cog": 45.2,
      "TrueHeading": 50,
      "NavigationalStatus": 0
    }
  }
}
```

## Output Structure

Data is stored in Hive-partitioned Parquet files:
```
data/
├── source=ais-sf-bay/
│   └── year=2025/
│       └── month=10/
│           └── day=11/
│               └── hour=14/
│                   └── minute=30/
│                       └── part-20251011T143045123.parquet
```

Each Parquet file contains:
- **ts**: Timestamp (milliseconds since epoch, UTC)
- **payload**: Complete JSON AIS message as string

## Docker Usage

### docker-compose.yml
```yaml
version: '3.8'
services:
  ais-ingest:
    build: .
    command: [
      "--ws-url", "wss://stream.aisstream.io/v0/stream",
      "--ws-api-key", "${AIS_API_KEY}",
      "--ws-bbox", "37.9,-122.6,37.6,-122.3",
      "--source", "ais-sf-bay",
      "--s3-bucket", "maritime-data"
    ]
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
    volumes:
      - ./output:/data
```

### Environment Variables
```bash
export AIS_API_KEY="your-aisstream-api-key"
export AWS_ACCESS_KEY_ID="your-aws-key"
export AWS_SECRET_ACCESS_KEY="your-aws-secret"
docker-compose up -d
```

## API Key Management

### Getting an AISStream.io API Key
1. Visit [AISStream.io](https://aisstream.io/)
2. Sign in via GitHub or supported method
3. Navigate to [API Keys](https://aisstream.io/apikeys)
4. Generate a new API key
5. Copy the key for use in `--ws-api-key`

### Security Best Practices
- **Never commit API keys** to version control
- **Use environment variables** in production
- **Rotate keys regularly** for security
- **Monitor usage** to detect unauthorized access

## Performance Considerations

### Global Feed Volume
- **~300 messages/second** average for worldwide subscription
- **High bandwidth required** for global coverage
- **Consider regional filtering** for better performance

### Resource Requirements
- **CPU**: Moderate for JSON parsing and Parquet writing
- **Memory**: ~100-500MB for buffering
- **Disk I/O**: Depends on flush frequency and S3 upload
- **Network**: Sustained WebSocket connection required

### Optimization Tips
```bash
# Reduce data volume with filtering
--ws-message-type-filter PositionReport

# Increase batch size for efficiency
--max-rows 10000

# Use specific regions instead of global
--ws-bbox "37.9,-122.6,37.6,-122.3"
```

## Monitoring and Health Checks

The application provides the same health check functionality for WebSocket feeds:

```bash
# Check if WebSocket processing is healthy
./hive_parquet_ingest --health-check

# Docker health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD /usr/local/bin/hive_parquet_ingest --health-check
```

## Error Handling

### Connection Issues
- **Automatic reconnection**: Not implemented (process will exit)
- **Network timeouts**: WebSocket will close on timeout
- **Invalid API key**: Connection rejected with error message

### Data Issues
- **Malformed JSON**: Message skipped, processing continues
- **Unknown message types**: Logged as warnings
- **Rate limiting**: Connection may be closed by AISStream.io

## Example Workflows

### Maritime Traffic Analysis
```bash
# Collect data from major shipping lanes
./hive_parquet_ingest \
    --ws-url wss://stream.aisstream.io/v0/stream \
    --ws-api-key $AIS_API_KEY \
    --ws-bbox "25.0,-80.0,26.0,-79.0" \
    --source miami-shipping-lane \
    --s3-bucket maritime-analytics
```

### Port Activity Monitoring
```bash
# Monitor specific port area
./hive_parquet_ingest \
    --ws-url wss://stream.aisstream.io/v0/stream \
    --ws-api-key $AIS_API_KEY \
    --ws-bbox "47.5,-122.4,47.7,-122.2" \
    --source seattle-port \
    --ws-message-type-filter PositionReport \
    --max-rows 5000
```

### Vessel Fleet Tracking
```bash
# Track specific company fleet
./hive_parquet_ingest \
    --ws-url wss://stream.aisstream.io/v0/stream \
    --ws-api-key $AIS_API_KEY \
    --ws-mmsi-filter 123456789 \
    --ws-mmsi-filter 987654321 \
    --source company-fleet \
    --keep-local
```

This WebSocket integration transforms your application into a real-time maritime data collection system, perfect for maritime analytics, vessel tracking, and port monitoring applications!