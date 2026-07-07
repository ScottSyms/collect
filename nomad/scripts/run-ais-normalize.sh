#!/bin/bash
set -euo pipefail

export S3_ENDPOINT="${S3_ENDPOINT:-http://192.168.99.107:9000}"
export S3_REGION="${S3_REGION:-us-east-1}"
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-WSF8aTTn5oKc4i1fVJvP}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-OqaL2H8UcUJByliObQp9ujZGHVZF1GKd91IqAkYY}"
export S3_DISABLE_TLS="${S3_DISABLE_TLS:-true}"

exec "$HOME/code/projects/collect/target/release/ais-normalize" \
  --input-s3-bucket "norway" \
  --input-s3-bucket "duplicate" \
  --output-s3-bucket "norway-norm" \
  --incremental \
  --concurrency "4"
