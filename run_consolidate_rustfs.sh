#!/usr/bin/env bash
#
# Runs `ais-compact consolidate` on every table in the given namespaces of a
# local RustFS server's Iceberg catalog, keeping each under RustFS's limit of
# 10,000 manifests per snapshot. Dry run (counts only) unless --apply.
#
# Usage: ./run_consolidate_rustfs.sh [--apply] [--min-manifests N] [namespace...]
#
#   ./run_consolidate_rustfs.sh                       # report, all namespaces
#   ./run_consolidate_rustfs.sh --apply               # merge, all namespaces
#   ./run_consolidate_rustfs.sh --apply ais identity  # merge, two namespaces
#
# With no namespace, every namespace in the warehouse is processed. Stop the
# writers to a table first, or a concurrent commit may refuse the merge.
# Credentials and endpoints work as in run_foundation_rustfs.sh: read from
# .rustfs.env next to this script, overridable in the environment
# (RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_ENDPOINT, RUSTFS_REGION,
# ICEBERG_CATALOG_URI, ICEBERG_WAREHOUSE). The warehouse defaults to
# `maritimedata`.

set -euo pipefail

consolidate_args=()
namespaces=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            sed -n '3,19p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        --apply) consolidate_args+=("$1"); shift ;;
        --min-manifests) consolidate_args+=("$1" "$2"); shift 2 ;;
        -*) echo "error: unknown option $1" >&2; exit 2 ;;
        *) namespaces+=("$1"); shift ;;
    esac
done

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

if ! curl -s -o /dev/null --max-time 5 "$RUSTFS_ENDPOINT/"; then
    echo "error: RustFS is not answering at $RUSTFS_ENDPOINT" >&2
    exit 1
fi

# GET a catalog path and print one name per line from the JSON list in $2
# ("namespaces" or "identifiers").
catalog_list() {
    curl -sf --user "$RUSTFS_ACCESS_KEY:$RUSTFS_SECRET_KEY" --aws-sigv4 "aws:amz:$S3_REGION:s3" \
        "$ICEBERG_CATALOG_URI/v1/$ICEBERG_WAREHOUSE/$1" |
        python3 -I -c '
import json, sys
for item in json.load(sys.stdin)[sys.argv[1]]:
    print(".".join(item) if isinstance(item, list) else item["name"])' "$2"
}

if [[ ${#namespaces[@]} -eq 0 ]]; then
    while IFS= read -r ns; do namespaces+=("$ns"); done < <(catalog_list namespaces namespaces)
fi

echo "building ais-compact..." >&2
cargo build --release --quiet -p collect-maint --bin ais-compact --manifest-path "$here/Cargo.toml"

failed=0
for ns in "${namespaces[@]}"; do
    tables=()
    while IFS= read -r t; do tables+=(--table "$t"); done < <(catalog_list "namespaces/$ns/tables" identifiers)
    if [[ ${#tables[@]} -eq 0 ]]; then
        echo "== $ns: no tables"
        continue
    fi
    echo "== $ns ($(( ${#tables[@]} / 2 )) tables)"
    ICEBERG_NAMESPACE=$ns "$here/target/release/ais-compact" "${tables[@]}" \
        consolidate ${consolidate_args[@]+"${consolidate_args[@]}"} || failed=1
done
exit $failed
