# WebSocket Connection Troubleshooting Guide

## Common WebSocket Connection Issues

### Connection Reset Error
**Error**: `WebSocket protocol error: Connection reset without closing handshake`

**Possible Causes:**
1. **Authentication Issue**: Invalid or expired API key
2. **Network Issues**: Firewall, proxy, or network instability
3. **Rate Limiting**: Too many connections from your IP
4. **Server-side Issues**: Temporary service problems

### Troubleshooting Steps

#### 1. Enable Debug Mode
Add the `--ws-debug` flag to see detailed WebSocket communication:

```bash
./target/release/hive_parquet_ingest \
  --source aisstore \
  --s3-bucket aisstore \
  --s3-endpoint 'http://192.168.99.107:9000' \
  --s3-region us-east-1 \
  --s3-access-key admin \
  --s3-secret-key 'vishnu'\!'23' \
  --ws-url 'wss://stream.aisstream.io/v0/stream' \
  --ws-api-key YOUR_API_KEY \
  --ws-debug
```

#### 2. Verify API Key
- Check that your API key is valid and active
- Ensure it has the correct permissions for AISStream.io
- Test the API key with a simple curl request:

```bash
curl -H "Authorization: Bearer YOUR_API_KEY" https://stream.aisstream.io/v0/stream
```

#### 3. Test Connection Parameters
Try with minimal parameters first:

```bash
# Basic connection test
./target/release/hive_parquet_ingest \
  --ws-url 'wss://stream.aisstream.io/v0/stream' \
  --ws-api-key YOUR_API_KEY \
  --source test \
  --ws-debug
```

#### 4. Check Network Connectivity
```bash
# Test basic connectivity to AISStream.io
ping stream.aisstream.io

# Test HTTPS connectivity
curl -I https://stream.aisstream.io

# Check if WebSocket port is accessible
nc -z stream.aisstream.io 443
```

#### 5. Try Different Bounding Boxes
Start with a smaller area to reduce data load:

```bash
./target/release/hive_parquet_ingest \
  --ws-url 'wss://stream.aisstream.io/v0/stream' \
  --ws-api-key YOUR_API_KEY \
  --ws-bbox "37.7,-122.5,37.8,-122.4" \
  --source sf-bay-test \
  --ws-debug
```

## New Features in This Version

### Automatic Reconnection
The application now automatically reconnects when the WebSocket connection is lost:
- Detects connection reset, timeout, and protocol errors
- Waits 5 seconds between reconnection attempts
- Retries up to 3 times during initial connection
- Continues processing from where it left off after reconnection

### Enhanced Error Handling
- **Connection Timeout**: 30-second timeout for initial connection
- **Send Timeout**: 10-second timeout for subscription message
- **Detailed Error Messages**: Better error descriptions and context
- **Debug Mode**: Verbose logging with `--ws-debug` flag

### Improved Message Processing
- **Authentication Detection**: Recognizes auth errors and subscription confirmations
- **Message Counting**: Tracks valid AIS messages processed
- **Error Suppression**: Limits spam from parsing errors (unless in debug mode)
- **Progress Indicators**: Shows processing statistics

## Environment Variable Support

You can also configure WebSocket options via environment variables:

```bash
export WS_URL="wss://stream.aisstream.io/v0/stream"
export WS_API_KEY="your-api-key-here"
export WS_BBOX="37.7,-122.5,37.8,-122.4"
export WS_DEBUG=true
export SOURCE="ais-data"

./target/release/hive_parquet_ingest
```

## Expected Behavior

### Successful Connection
```
🔄 Attempting WebSocket connection to: wss://stream.aisstream.io/v0/stream
✅ WebSocket connected, response status: 101 Switching Protocols
📤 Sending WebSocket subscription: {"APIKey":"...","BoundingBoxes":[...],...}
✅ Subscription message sent successfully
✅ WebSocket connected, processing messages...
📨 WebSocket message #1: {"subscribed":true,...}
✅ WebSocket subscription acknowledged: {"subscribed":true}
📨 WebSocket message #2: {"MessageType":"PositionReport",...}
📊 Processed 100 valid AIS messages
✅ Wrote 42 rows to data/source=aisstore/year=2025/month=10/day=12/...
```

### Connection Recovery
```
❌ WebSocket stream error: WebSocket protocol error: Connection reset without closing handshake
🔄 Connection lost, attempting to reconnect...
⏳ Waiting 5 seconds before reconnection attempt...
🔄 Attempting WebSocket connection to: wss://stream.aisstream.io/v0/stream
✅ WebSocket connected, response status: 101 Switching Protocols
✅ WebSocket reconnected successfully
📊 Processed 200 valid AIS messages
```

## Debugging Tips

1. **Check Logs**: Look for authentication acknowledgments and error patterns
2. **Monitor Traffic**: Use debug mode to see all WebSocket messages
3. **Test Incrementally**: Start with basic connection, then add filters
4. **Verify Credentials**: Ensure API key is correct and has permissions
5. **Check Quotas**: Verify you haven't exceeded API rate limits
6. **Network Issues**: Test from different networks if possible

## Performance Optimization

- **Bounding Boxes**: Use specific geographic areas to reduce data volume
- **MMSI Filters**: Filter by specific ship identifiers to limit messages
- **Message Types**: Filter by message type (e.g., only PositionReport)
- **Buffer Size**: Adjust `--max-rows` based on your processing needs

## Getting Help

If connection issues persist:
1. Enable debug mode (`--ws-debug`)
2. Save the debug output to a file
3. Check AISStream.io status and documentation
4. Contact AISStream.io support with your API key details
5. Verify your subscription plan supports real-time streaming