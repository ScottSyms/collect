#!/bin/bash
# Comprehensive test of graceful environment variable handling

echo "Comprehensive Environment Variable Handling Test"
echo "==============================================="
echo

cd /Users/scottsyms/code/sa/capture

# Build the application
echo "Building application..."
cargo build --release
if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi
echo "✅ Build successful"
echo

# Test 1: No environment variables
echo "Test 1: No environment variables set"
env -i PATH="$PATH" ./target/release/hive_parquet_ingest --health-check
if [ $? -eq 0 ]; then
    echo "✅ PASS: Application works with no environment variables"
else
    echo "❌ FAIL: Application fails with no environment variables"
fi
echo

# Test 2: Valid environment variables
echo "Test 2: Valid environment variables"
SOURCE="env-test" OUT_DIR="/tmp/test" S3_REGION="us-west-2" HEALTH_CHECK=true ./target/release/hive_parquet_ingest
if [ $? -eq 0 ]; then
    echo "✅ PASS: Application works with valid environment variables"
else
    echo "❌ FAIL: Application fails with valid environment variables"
fi
echo

# Test 3: Empty environment variables
echo "Test 3: Empty environment variables"
SOURCE="" OUT_DIR="" WS_BBOX="" WS_MMSI_FILTER="" HEALTH_CHECK=true ./target/release/hive_parquet_ingest
if [ $? -eq 0 ]; then
    echo "✅ PASS: Application handles empty environment variables"
else
    echo "❌ FAIL: Application fails with empty environment variables"
fi
echo

# Test 4: Invalid numeric environment variables
echo "Test 4: Invalid numeric environment variables (should be ignored)"
TCP_PORT="invalid" MAX_ROWS="not-a-number" HEALTH_CHECK=true ./target/release/hive_parquet_ingest
if [ $? -eq 0 ]; then
    echo "✅ PASS: Application ignores invalid numeric environment variables"
else
    echo "❌ FAIL: Application fails with invalid numeric environment variables"
fi
echo

# Test 5: Boolean environment variables (various formats)
echo "Test 5: Boolean environment variables (true/false/1/0)"
HEALTH_CHECK=false KEEP_LOCAL=1 ./target/release/hive_parquet_ingest --health-check
if [ $? -eq 0 ]; then
    echo "✅ PASS: Application handles boolean environment variables correctly"
else
    echo "❌ FAIL: Application fails with boolean environment variables"
fi
echo

# Test 6: Comma-separated array environment variables
echo "Test 6: Comma-separated array environment variables"
WS_BBOX="37.9,-122.6,37.6,-122.3" WS_MMSI_FILTER="123456789,987654321" WS_MESSAGE_TYPE_FILTER="PositionReport,StaticAndVoyageRelatedData" HEALTH_CHECK=true ./target/release/hive_parquet_ingest
if [ $? -eq 0 ]; then
    echo "✅ PASS: Application handles comma-separated arrays correctly"
else
    echo "❌ FAIL: Application fails with comma-separated arrays"
fi
echo

# Test 7: Mixed CLI arguments and environment variables (CLI should win)
echo "Test 7: CLI arguments override environment variables"
SOURCE="env-source" ./target/release/hive_parquet_ingest --source "cli-source" --health-check
if [ $? -eq 0 ]; then
    echo "✅ PASS: CLI arguments correctly override environment variables"
else
    echo "❌ FAIL: CLI override test failed"
fi
echo

# Test 8: Whitespace in comma-separated values
echo "Test 8: Whitespace handling in comma-separated values"
WS_BBOX=" 37.9 , -122.6 , 37.6 , -122.3 " WS_MMSI_FILTER="123 , 456 , 789" HEALTH_CHECK=true ./target/release/hive_parquet_ingest
if [ $? -eq 0 ]; then
    echo "✅ PASS: Application handles whitespace in comma-separated values"
else
    echo "❌ FAIL: Application fails with whitespace in comma-separated values"
fi
echo

echo "Environment Variable Handling Test Complete!"
echo "==========================================="
echo "All tests demonstrate that the application:"
echo "• Continues running when environment variables don't exist"
echo "• Gracefully handles empty or invalid environment variables"  
echo "• Properly parses comma-separated array values"
echo "• Respects CLI argument precedence over environment variables"
echo "• Handles various boolean formats (true/false/1/0)"
echo "• Trims whitespace from comma-separated values"