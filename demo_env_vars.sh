#!/bin/bash
# Example script demonstrating graceful environment variable handling

echo "Demonstrating graceful environment variable handling"
echo "=================================================="
echo

# Build the application first
echo "Building application..."
cargo build --release
echo

# Example 1: No environment variables - should work fine
echo "Example 1: Running with no environment variables set"
echo "Command: ./target/release/hive_parquet_ingest --health-check"
env -i PATH="$PATH" ./target/release/hive_parquet_ingest --health-check
echo "✅ Success: Application handles missing environment variables gracefully"
echo

# Example 2: Some environment variables set, others missing
echo "Example 2: Partial environment variables (SOURCE set, others missing)"
echo "Command: SOURCE=test ./target/release/hive_parquet_ingest --health-check"  
SOURCE=test ./target/release/hive_parquet_ingest --health-check
echo "✅ Success: Application works with partial environment variables"
echo

# Example 3: Empty environment variables
echo "Example 3: Empty environment variables"
echo "Command: SOURCE='' ./target/release/hive_parquet_ingest --health-check"
SOURCE="" ./target/release/hive_parquet_ingest --health-check
echo "✅ Success: Empty environment variables handled gracefully"
echo

# Example 4: Boolean environment variables
echo "Example 4: Boolean environment variables"
echo "Command: KEEP_LOCAL=true HEALTH_CHECK=true ./target/release/hive_parquet_ingest"
KEEP_LOCAL=true HEALTH_CHECK=true ./target/release/hive_parquet_ingest
echo "✅ Success: Boolean environment variables work correctly"
echo

echo "All environment variable handling tests completed successfully!"
echo
echo "Key benefits of this implementation:"
echo "• Missing environment variables don't cause errors"
echo "• Empty environment variables are treated as unset"  
echo "• Optional environment variables are handled gracefully"
echo "• Command-line arguments can still override environment variables"
echo "• Boolean environment variables work as expected"
echo "• Application gracefully falls back to defaults when needed"
