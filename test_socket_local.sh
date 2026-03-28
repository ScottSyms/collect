#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
DURATION_SECONDS=60

if [[ -z "$DATA_DIR" || "$DATA_DIR" == "/" ]]; then
  echo "Refusing to clean invalid data directory: '$DATA_DIR'"
  exit 1
fi

echo "Cleaning data directory: $DATA_DIR"
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

if [[ ! -x "${SCRIPT_DIR}/target/release/capture" ]]; then
  echo "Building release binary..."
  cargo build --release
fi

echo "Starting capture for ${DURATION_SECONDS}s..."
cargo run --release -- \
  --tcp-host 153.44.253.27 \
  --tcp-port 5631 \
  --source test-socket-local \
  --out-dir "$DATA_DIR" &
app_pid=$!

sleep "$DURATION_SECONDS"

if kill -0 "$app_pid" 2>/dev/null; then
  echo "Stopping capture..."
  kill -TERM "$app_pid" 2>/dev/null || true
fi

wait "$app_pid" || true

shopt -s globstar nullglob
parquet_files=("$DATA_DIR"/source=*/**/*.parquet)

if (( ${#parquet_files[@]} > 0 )); then
  echo "PASS: Found ${#parquet_files[@]} parquet file(s) in $DATA_DIR"

  if ! command -v duckdb >/dev/null 2>&1; then
    echo "duckdb not found in PATH; skipping row count"
    exit 0
  fi

  duckdb -c "select count(*) from read_parquet('${DATA_DIR}/**/*.parquet', hive_partitioning = true);"
  exit 0
fi

echo "FAIL: No parquet files found in $DATA_DIR"
exit 1
