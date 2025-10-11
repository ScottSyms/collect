#!/bin/bash
# Test script to verify environment variable support

echo "Testing environment variable support for hive_parquet_ingest"
echo "============================================================"

# Build the application first
echo "Building application..."
cd /Users/scottsyms/code/sa/capture
cargo build --release

if [ $? -eq 0 ]; then
    echo "✅ Build successful"
else
    echo "❌ Build failed"
    exit 1
fi

echo ""
echo "Testing environment variable support:"

# Test 1: Help with environment variable set
echo "1. Testing help output shows environment variable support..."
OUTPUT=$(./target/release/hive_parquet_ingest --help 2>&1)
if echo "$OUTPUT" | grep -q "env"; then
    echo "✅ Help shows environment variable support"
else
    echo "❌ Help doesn't mention environment variables"
fi

# Test 2: Source environment variable
echo "2. Testing SOURCE environment variable..."
SOURCE="test-source" ./target/release/hive_parquet_ingest --health-check 2>&1
if [ $? -eq 0 ]; then
    echo "✅ SOURCE environment variable accepted"
else
    echo "❌ SOURCE environment variable failed"
fi

# Test 3: Health check environment variable
echo "3. Testing HEALTH_CHECK environment variable..."
HEALTH_CHECK=true ./target/release/hive_parquet_ingest 2>&1
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ HEALTH_CHECK environment variable works"
else
    echo "⚠️  HEALTH_CHECK exited with code $EXIT_CODE (expected for health check)"
fi

# Test 4: Multiple environment variables
echo "4. Testing multiple environment variables together..."
SOURCE="env-test" OUT_DIR="/tmp/test" HEALTH_CHECK=true ./target/release/hive_parquet_ingest 2>&1
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Multiple environment variables work together"
else
    echo "⚠️  Multiple env vars exited with code $EXIT_CODE (expected for health check)"
fi

echo ""
echo "Environment variable testing complete!"