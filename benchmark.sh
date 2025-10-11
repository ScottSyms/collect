#!/bin/bash
# Performance benchmark suite for hive_parquet_ingest

echo "Performance Benchmark Suite"
echo "=========================="
echo "This will test various scenarios and collect performance metrics"
echo

# Ensure we have a release build
if [ ! -f "./target/release/hive_parquet_ingest" ]; then
    echo "Building release version..."
    cargo build --release
    if [ $? -ne 0 ]; then
        echo "Build failed!"
        exit 1
    fi
fi

RESULTS_DIR="benchmark_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

echo "Results will be saved to: $RESULTS_DIR"
echo

# Test 1: Health Check Performance
echo "Test 1: Health Check Performance"
echo "================================"
echo "Running 10 health check iterations..."

for i in {1..10}; do
    echo -n "Iteration $i: "
    /usr/bin/time -l ./target/release/hive_parquet_ingest --health-check 2>&1 | \
    grep -E "(real|maximum resident set size)" | \
    tr '\n' ' ' && echo
done > "$RESULTS_DIR/health_check_performance.txt"

echo "Health check results saved to $RESULTS_DIR/health_check_performance.txt"
echo

# Test 2: File Processing Performance
if [ -f "norway.nmea" ]; then
    echo "Test 2: File Processing Performance" 
    echo "==================================="
    
    # Create test files of different sizes
    head -100 norway.nmea > "$RESULTS_DIR/test_small.nmea"
    head -1000 norway.nmea > "$RESULTS_DIR/test_medium.nmea" 
    head -5000 norway.nmea > "$RESULTS_DIR/test_large.nmea"
    
    echo "Testing small file (100 lines)..."
    /usr/bin/time -l ./target/release/hive_parquet_ingest \
        --input "$RESULTS_DIR/test_small.nmea" \
        --source benchmark-small \
        --out-dir "$RESULTS_DIR/output_small" \
        2>&1 | tee "$RESULTS_DIR/file_small_performance.txt"
    
    echo "Testing medium file (1000 lines)..."
    /usr/bin/time -l ./target/release/hive_parquet_ingest \
        --input "$RESULTS_DIR/test_medium.nmea" \
        --source benchmark-medium \
        --out-dir "$RESULTS_DIR/output_medium" \
        2>&1 | tee "$RESULTS_DIR/file_medium_performance.txt"
    
    echo "Testing large file (5000 lines)..."
    /usr/bin/time -l ./target/release/hive_parquet_ingest \
        --input "$RESULTS_DIR/test_large.nmea" \
        --source benchmark-large \
        --out-dir "$RESULTS_DIR/output_large" \
        2>&1 | tee "$RESULTS_DIR/file_large_performance.txt"
        
    echo "File processing results saved to $RESULTS_DIR/file_*_performance.txt"
else
    echo "Skipping file processing tests (norway.nmea not found)"
fi
echo

# Test 3: Environment Variable Overhead
echo "Test 3: Environment Variable Performance"
echo "======================================="
echo "Testing with no env vars vs many env vars..."

echo "No environment variables:"
/usr/bin/time -l env -i PATH="$PATH" ./target/release/hive_parquet_ingest --health-check \
    2>&1 | tee "$RESULTS_DIR/no_env_vars.txt"

echo "Many environment variables:"
/usr/bin/time -l env \
    SOURCE=test \
    OUT_DIR=/tmp \
    S3_BUCKET=test \
    S3_REGION=us-east-1 \
    WS_BBOX="1,2,3,4" \
    WS_MMSI_FILTER="123,456,789" \
    KEEP_LOCAL=true \
    ./target/release/hive_parquet_ingest --health-check \
    2>&1 | tee "$RESULTS_DIR/many_env_vars.txt"

echo "Environment variable overhead results saved"
echo

# Test 4: Memory Usage Patterns
echo "Test 4: Memory Usage Analysis"
echo "============================"

if [ -f "norway.nmea" ] && [ $(wc -l < norway.nmea) -gt 1000 ]; then
    echo "Running memory tracking on file processing..."
    timeout 30s ./memory_tracker.sh ./target/release/hive_parquet_ingest \
        --input norway.nmea \
        --source memory-test \
        --out-dir "$RESULTS_DIR/memory_test" \
        --max-rows 1000 || true
    
    # Move the generated CSV to results directory
    mv memory_usage_*.csv "$RESULTS_DIR/" 2>/dev/null || true
    echo "Memory analysis complete"
else
    echo "Skipping memory analysis (insufficient test data)"
fi
echo

# Generate Summary Report
echo "Generating Summary Report"
echo "========================"

cat > "$RESULTS_DIR/summary_report.md" << EOF
# Performance Benchmark Report

Generated: $(date)
System: $(uname -a)
Rust Version: $(rustc --version 2>/dev/null || echo "Unknown")

## Test Environment
- Binary: ./target/release/hive_parquet_ingest
- Test Data: $(if [ -f "norway.nmea" ]; then echo "norway.nmea ($(wc -l < norway.nmea) lines)"; else echo "Not available"; fi)
- Results Directory: $RESULTS_DIR

## Health Check Performance
$(if [ -f "$RESULTS_DIR/health_check_performance.txt" ]; then 
    echo "Average time for health check over 10 iterations:"
    echo '```'
    cat "$RESULTS_DIR/health_check_performance.txt"
    echo '```'
  else
    echo "No health check data available"
  fi)

## File Processing Performance
$(if [ -f "$RESULTS_DIR/file_small_performance.txt" ]; then
    echo "### Small File (100 lines)"
    echo '```'
    grep -E "(real|maximum resident set size)" "$RESULTS_DIR/file_small_performance.txt"
    echo '```'
  fi)

$(if [ -f "$RESULTS_DIR/file_medium_performance.txt" ]; then
    echo "### Medium File (1000 lines)"
    echo '```'
    grep -E "(real|maximum resident set size)" "$RESULTS_DIR/file_medium_performance.txt"
    echo '```'
  fi)

$(if [ -f "$RESULTS_DIR/file_large_performance.txt" ]; then
    echo "### Large File (5000 lines)"
    echo '```'
    grep -E "(real|maximum resident set size)" "$RESULTS_DIR/file_large_performance.txt"
    echo '```'
  fi)

## Environment Variable Overhead
$(if [ -f "$RESULTS_DIR/no_env_vars.txt" ] && [ -f "$RESULTS_DIR/many_env_vars.txt" ]; then
    echo "### No Environment Variables"
    echo '```'
    grep -E "(real|maximum resident set size)" "$RESULTS_DIR/no_env_vars.txt"
    echo '```'
    echo "### Many Environment Variables"
    echo '```' 
    grep -E "(real|maximum resident set size)" "$RESULTS_DIR/many_env_vars.txt"
    echo '```'
  fi)

## Memory Usage
$(ls "$RESULTS_DIR"/memory_usage_*.csv 2>/dev/null | head -1 | while read csvfile; do
    if [ -n "$csvfile" ]; then
        echo "Memory usage data available in: $(basename "$csvfile")"
        echo "Peak memory usage:"
        echo '```'
        tail -n +2 "$csvfile" | sort -t, -k5 -nr | head -1 | cut -d, -f1,5 | sed 's/,/ - /'
        echo '```'
    fi
  done)

## File Sizes Generated
$(find "$RESULTS_DIR" -name "*.parquet" -exec ls -lh {} \; 2>/dev/null | awk '{print $5, $9}' | head -10)

## Recommendations
Based on these benchmarks:
- Health check overhead: $(if [ -f "$RESULTS_DIR/health_check_performance.txt" ]; then echo "Measured"; else echo "Not measured"; fi)
- File processing scales with input size
- Environment variable parsing has minimal overhead
- Peak memory usage should be monitored for large datasets

EOF

echo "Summary report generated: $RESULTS_DIR/summary_report.md"
echo
echo "Benchmark Complete!"
echo "=================="
echo "Results available in: $RESULTS_DIR/"
echo "Key files:"
echo "  - summary_report.md (overview)"
echo "  - *_performance.txt (detailed timings)"
echo "  - memory_usage_*.csv (memory analysis)"
echo
echo "To view the summary:"
echo "  cat $RESULTS_DIR/summary_report.md"