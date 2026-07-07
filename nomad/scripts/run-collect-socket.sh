#!/bin/bash
set -euo pipefail

export RUST_LOG="${RUST_LOG:-INFO}"
export TCP_HOST="${TCP_HOST:-192.168.99.107}"
export TCP_PORT="${TCP_PORT:-7001}"
export SOURCE="${SOURCE:-norway}"
export PARTITION="${PARTITION:-minute}"
export OUT_DIR="${OUT_DIR:-/tmp/collect-socket-data}"
export MAX_ROWS="${MAX_ROWS:-10000}"
export KEEP_LOCAL="${KEEP_LOCAL:-false}"
export S3_BUCKET="${S3_BUCKET:-norway}"
export S3_ENDPOINT="${S3_ENDPOINT:-http://192.168.99.107:9000}"
export S3_REGION="${S3_REGION:-us-east-1}"
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-WSF8aTTn5oKc4i1fVJvP}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-OqaL2H8UcUJByliObQp9ujZGHVZF1GKd91IqAkYY}"
export S3_DISABLE_TLS="${S3_DISABLE_TLS:-true}"

mkdir -p "$OUT_DIR"

exec "$HOME/code/projects/collect/target/release/collect-socket"
