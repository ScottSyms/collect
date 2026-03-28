#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
INPUT_FILE="${SCRIPT_DIR}/norway.nmea"

if [[ -z "$DATA_DIR" || "$DATA_DIR" == "/" ]]; then
  echo "Refusing to clean invalid data directory: '$DATA_DIR'"
  exit 1
fi

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE"
  exit 1
fi

echo "Cleaning data directory: $DATA_DIR"
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

if [[ ! -x "${SCRIPT_DIR}/target/release/capture" ]]; then
  echo "Building release binary..."
  cargo build --release
fi

echo "Starting capture with file input: $INPUT_FILE"
cargo run --release -- \
  --input "$INPUT_FILE" \
  --source test-file-local \
  --out-dir "$DATA_DIR"

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
