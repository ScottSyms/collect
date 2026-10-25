#!/usr/bin/env bash
#
# Runs vessel-imagery against a local RustFS server's Iceberg catalog.
#
# Usage: ./run_imagery_rustfs.sh <vessel-imagery arguments...>
#
#   ./run_imagery_rustfs.sh init --apply
#   ./run_imagery_rustfs.sh fetch commons --contact you@example.org --limit 50 --apply
#   ./run_imagery_rustfs.sh import --manifest held/manifest.csv --apply
#   ./run_imagery_rustfs.sh describe --limit 20 --apply
#   ./run_imagery_rustfs.sh profile --apply
#   ./run_imagery_rustfs.sh lookup --imo 9321483
#
# Credentials and endpoints work as in run_foundation_rustfs.sh: read from
# .rustfs.env next to this script, overridable in the environment
# (RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_ENDPOINT, RUSTFS_REGION,
# ICEBERG_CATALOG_URI, ICEBERG_WAREHOUSE, ICEBERG_NAMESPACE). The namespace
# defaults to `imagery` (the registry is read from `identity`).

set -euo pipefail

case "${1:-}" in
    ""|-h|--help)
        sed -n '3,18p' "$0" | sed 's/^# \{0,1\}//'
        exit 0
        ;;
esac

here=$(cd "$(dirname "$0")" && pwd)

if [[ -f "$here/.rustfs.env" ]]; then
    while IFS='=' read -r key value; do
        [[ -z "$key" || "$key" == \#* ]] && continue
        [[ -z "${!key:-}" ]] && export "$key=$value"
    done < "$here/.rustfs.env"
fi

: "${RUSTFS_ACCESS_KEY:?set RUSTFS_ACCESS_KEY (or put it in .rustfs.env)}"
: "${RUSTFS_SECRET_KEY:?set RUSTFS_SECRET_KEY (or put it in .rustfs.env)}"
RUSTFS_ENDPOINT=${RUSTFS_ENDPOINT:-http://localhost:9000}
[[ "$RUSTFS_ENDPOINT" == *://* ]] || RUSTFS_ENDPOINT="http://$RUSTFS_ENDPOINT"

export S3_ENDPOINT=$RUSTFS_ENDPOINT
export S3_ACCESS_KEY=$RUSTFS_ACCESS_KEY
export S3_SECRET_KEY=$RUSTFS_SECRET_KEY
export S3_REGION=${RUSTFS_REGION:-us-east-1}
export S3_PATH_STYLE=true
export ICEBERG_CATALOG_URI=${ICEBERG_CATALOG_URI:-$RUSTFS_ENDPOINT/iceberg}
export ICEBERG_SIGV4=true
export ICEBERG_WAREHOUSE=${ICEBERG_WAREHOUSE:-maritimedata}
export ICEBERG_NAMESPACE=${ICEBERG_NAMESPACE:-imagery}

if ! curl -s -o /dev/null --max-time 5 "$RUSTFS_ENDPOINT/"; then
    echo "error: RustFS is not answering at $RUSTFS_ENDPOINT" >&2
    exit 1
fi

# RustFS serves a bucket as an Iceberg warehouse only once it is
# table-enabled; both PUTs are idempotent.
if [[ " $* " == *" --apply "* ]]; then
    curl -sf -o /dev/null -X PUT --user "$RUSTFS_ACCESS_KEY:$RUSTFS_SECRET_KEY" \
        --aws-sigv4 "aws:amz:$S3_REGION:s3" "$RUSTFS_ENDPOINT/$ICEBERG_WAREHOUSE" || true
    if ! curl -sf -o /dev/null -X PUT --user "$RUSTFS_ACCESS_KEY:$RUSTFS_SECRET_KEY" \
        --aws-sigv4 "aws:amz:$S3_REGION:s3" "$RUSTFS_ENDPOINT/iceberg/v1/buckets/$ICEBERG_WAREHOUSE"; then
        echo "error: could not table-enable bucket $ICEBERG_WAREHOUSE (check the credentials)" >&2
        exit 1
    fi
fi

echo "building vessel-imagery..." >&2
cargo build --release --quiet -p vessel-imagery --manifest-path "$here/Cargo.toml"
exec "$here/target/release/vessel-imagery" "$@"
