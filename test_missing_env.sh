#!/bin/bash
# Test missing environment variables handling

echo "Testing graceful handling of missing environment variables..."
echo "============================================================="

cd /Users/scottsyms/code/sa/capture

# Test 1: No environment variables set
echo "Test 1: Clean environment (no env vars)"
env -i PATH="$PATH" ./target/debug/hive_parquet_ingest --health-check 2>&1
STATUS1=$?
echo "Exit code: $STATUS1"
echo ""

# Test 2: Some environment variables set, others missing
echo "Test 2: Partial environment variables"
env -i PATH="$PATH" SOURCE="test" OUT_DIR="/tmp" ./target/debug/hive_parquet_ingest --health-check 2>&1
STATUS2=$?
echo "Exit code: $STATUS2"
echo ""

# Test 3: Empty comma-separated environment variables
echo "Test 3: Empty comma-separated env vars"
env -i PATH="$PATH" WS_BBOX="" WS_MMSI_FILTER="" ./target/debug/hive_parquet_ingest --health-check 2>&1
STATUS3=$?
echo "Exit code: $STATUS3"
echo ""

echo "Summary:"
echo "Test 1 (clean env): Exit code $STATUS1"
echo "Test 2 (partial env): Exit code $STATUS2" 
echo "Test 3 (empty env vars): Exit code $STATUS3"

if [ $STATUS1 -eq 0 ] && [ $STATUS2 -eq 0 ] && [ $STATUS3 -eq 0 ]; then
    echo "✅ All tests passed - environment variables handled gracefully"
else
    echo "❌ Some tests failed - check error handling"
fi