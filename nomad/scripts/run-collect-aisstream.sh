#!/bin/bash
set -euo pipefail

export RUST_LOG="${RUST_LOG:-INFO}"
export AISSTREAM_API_KEY="${AISSTREAM_API_KEY:-4938596d4f90691fc50c08fcab77b716afb60c3c}"
export BOUNDING_BOXES="${BOUNDING_BOXES:-[[[-90,-180],[90,180]]]}"
export SOURCE="${SOURCE:-aisstream}"
export PARTITION="${PARTITION:-minute}"
export OUT_DIR="${OUT_DIR:-/tmp/collect-aisstream-data}"
export MAX_ROWS="${MAX_ROWS:-10000}"
export KEEP_LOCAL="${KEEP_LOCAL:-false}"
export S3_BUCKET="${S3_BUCKET:-aisstream}"
export S3_ENDPOINT="${S3_ENDPOINT:-http://192.168.99.107:9000}"
export S3_REGION="${S3_REGION:-us-east-1}"
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-WSF8aTTn5oKc4i1fVJvP}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-OqaL2H8UcUJByliObQp9ujZGHVZF1GKd91IqAkYY}"
export S3_DISABLE_TLS="${S3_DISABLE_TLS:-true}"

mkdir -p "$OUT_DIR"

exec "$HOME/code/projects/collect/target/release/collect-aisstream"
