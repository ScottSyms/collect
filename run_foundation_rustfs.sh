#!/usr/bin/env bash
#
# Runs collect-foundation against a local RustFS server.
# See ./run_foundation_rustfs.sh --help.

set -euo pipefail

USCG_DIR=data/uscg

usage() {
    cat <<'EOF'
Usage: ./run_foundation_rustfs.sh [--dry-run] <target> [extra flags...]

Builds collect-foundation (release) and runs it against a local RustFS
server's Iceberg catalog. Tables are written unless --dry-run is given.

Targets:
  placenames            GeoNames placenames with alternate names (whole world)
  paris-mou <dir>       Paris MoU bulk XML files in <dir>
  uscg                  USCG MISLE deficiency workbooks (from USCG_DIR when set)
  tsb                   TSB Canada MARSIS marine occurrences
  tokyo-mou             Tokyo MoU detention lists, 2000 onward
  iuu-list              Combined IUU Vessel List workbook(s) in IUU_DIR
  sources               Only the evidence_sources table
  evidence              tsb, tokyo-mou, iuu-list, uscg, then paris-mou;
                        a source whose files aren't available is skipped
  all                   placenames, then evidence
  raw <args...>         Pass <args...> straight to collect-foundation

Extra flags go to the subcommand. For example:
  ./run_foundation_rustfs.sh placenames --country NO,CA
  ./run_foundation_rustfs.sh --dry-run tokyo-mou --from 2024-01
  ./run_foundation_rustfs.sh raw placenames load --help

Credentials are read from .rustfs.env next to this script, if it exists, and
can be overridden in the environment:
  RUSTFS_ACCESS_KEY     S3 access key (required)
  RUSTFS_SECRET_KEY     S3 secret key (required)
  RUSTFS_ENDPOINT       S3 endpoint            (default: http://localhost:9000)
  RUSTFS_REGION         SigV4 signing region   (default: us-east-1)
  ICEBERG_CATALOG_URI   Iceberg REST catalog   (default: $RUSTFS_ENDPOINT/iceberg)
  ICEBERG_WAREHOUSE     Warehouse / bucket     (default: maritimedata)
  ICEBERG_NAMESPACE     Namespace              (default: foundation)
  COLLECT_USER_AGENT    User-Agent for the USCG and TSB downloads
                        (default: a desktop browser string; both sites refuse
                        other clients)
  PARIS_MOU_DIR         Paris MoU bulk files, for the evidence and all targets
  USCG_DIR              USCG workbooks saved from the MISLE report page (the
                        site refuses scripted downloads; without it, uscg
                        tries to download and the evidence target skips it)
  IUU_DIR               IUU list Excel file(s)  (default: data/iuu)
  SCRATCH_DIR           Download directory (default: collect-foundation's own)
EOF
}

here=$(cd "$(dirname "$0")" && pwd)

if [[ -f "$here/.rustfs.env" ]]; then
    # Keep anything already set in the environment.
    while IFS='=' read -r key value; do
        [[ -z "$key" || "$key" == \#* ]] && continue
        [[ -z "${!key:-}" ]] && export "$key=$value"
    done < "$here/.rustfs.env"
fi

apply=(--apply)
if [[ "${1:-}" == "--dry-run" ]]; then
    apply=()
    shift
fi

case "${1:-}" in
    ""|-h|--help)
        usage
        exit 0
        ;;
esac
target=$1
shift

: "${RUSTFS_ACCESS_KEY:?set RUSTFS_ACCESS_KEY (or put it in .rustfs.env)}"
: "${RUSTFS_SECRET_KEY:?set RUSTFS_SECRET_KEY (or put it in .rustfs.env)}"
RUSTFS_ENDPOINT=${RUSTFS_ENDPOINT:-http://localhost:9000}
[[ "$RUSTFS_ENDPOINT" == *://* ]] || RUSTFS_ENDPOINT="http://$RUSTFS_ENDPOINT"

# What collect-core's open_catalog and SigV4 signing read.
export S3_ENDPOINT=$RUSTFS_ENDPOINT
export S3_ACCESS_KEY=$RUSTFS_ACCESS_KEY
export S3_SECRET_KEY=$RUSTFS_SECRET_KEY
export S3_REGION=${RUSTFS_REGION:-us-east-1}
export S3_PATH_STYLE=true
export ICEBERG_CATALOG_URI=${ICEBERG_CATALOG_URI:-$RUSTFS_ENDPOINT/iceberg}
export ICEBERG_SIGV4=true
export ICEBERG_WAREHOUSE=${ICEBERG_WAREHOUSE:-maritimedata}
export ICEBERG_NAMESPACE=${ICEBERG_NAMESPACE:-foundation}
export COLLECT_USER_AGENT=${COLLECT_USER_AGENT:-"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"}

if ! curl -s -o /dev/null --max-time 5 "$RUSTFS_ENDPOINT/"; then
    echo "error: RustFS is not answering at $RUSTFS_ENDPOINT" >&2
    exit 1
fi

# RustFS serves a bucket as an Iceberg warehouse only once it is
# table-enabled; the PUT is idempotent. The bucket itself must exist.
if [[ ${#apply[@]} -gt 0 ]]; then
    curl -sf -o /dev/null -X PUT --user "$RUSTFS_ACCESS_KEY:$RUSTFS_SECRET_KEY" \
        --aws-sigv4 "aws:amz:$S3_REGION:s3" "$RUSTFS_ENDPOINT/$ICEBERG_WAREHOUSE" || true
    if ! curl -sf -o /dev/null -X PUT --user "$RUSTFS_ACCESS_KEY:$RUSTFS_SECRET_KEY" \
        --aws-sigv4 "aws:amz:$S3_REGION:s3" "$RUSTFS_ENDPOINT/iceberg/v1/buckets/$ICEBERG_WAREHOUSE"; then
        echo "error: could not table-enable bucket $ICEBERG_WAREHOUSE (check the credentials)" >&2
        exit 1
    fi
fi

IUU_DIR=${IUU_DIR:-$here/data/iuu}

echo "building collect-foundation..." >&2
cargo build --release --quiet -p collect-foundation --manifest-path "$here/Cargo.toml"
bin="$here/target/release/collect-foundation"

scratch=()
[[ -n "${SCRATCH_DIR:-}" ]] && scratch=(--scratch "$SCRATCH_DIR")

run() {
    echo "==> collect-foundation $*" >&2
    "$bin" "$@"
}

# Flags every load takes. `${arr[@]+"${arr[@]}"}` expands an empty array
# safely under `set -u` in macOS's bash 3.2.
common=(${scratch[@]+"${scratch[@]}"} ${apply[@]+"${apply[@]}"})

placenames() { run placenames load --alternate-names ${common[@]+"${common[@]}"} "$@"; }
evidence_source() { local s=$1; shift; run evidence "$s" ${common[@]+"${common[@]}"} "$@"; }
paris() {
    local dir=$1; shift
    [[ -d "$dir" ]] || { echo "error: $dir is not a directory" >&2; exit 2; }
    run evidence paris-mou --source-dir "$dir" ${common[@]+"${common[@]}"} "$@"
}
uscg() {
    if [[ -n "${USCG_DIR:-}" ]]; then
        evidence_source uscg --source-dir "$USCG_DIR" "$@"
    else
        evidence_source uscg "$@"
    fi
}
iuu() {
    [[ -d "$IUU_DIR" ]] || { echo "error: $IUU_DIR is not a directory (set IUU_DIR)" >&2; return 2; }
    evidence_source iuu-list --source-dir "$IUU_DIR" "$@"
}
all_evidence() {
    evidence_source tsb "$@"
    evidence_source tokyo-mou "$@"
    if [[ -d "$IUU_DIR" ]]; then
        iuu "$@"
    else
        echo "skipping iuu-list: no $IUU_DIR (set IUU_DIR)" >&2
    fi
    if [[ -n "${USCG_DIR:-}" ]]; then
        uscg "$@"
    else
        echo "skipping uscg: set USCG_DIR to the saved MISLE workbooks" >&2
    fi
    if [[ -n "${PARIS_MOU_DIR:-}" ]]; then
        paris "$PARIS_MOU_DIR" "$@"
    else
        echo "skipping paris-mou: set PARIS_MOU_DIR to the bulk XML directory" >&2
    fi
}

case "$target" in
    placenames)            placenames "$@" ;;
    paris-mou)
        [[ $# -ge 1 ]] || { echo "usage: $0 paris-mou <dir> [flags]" >&2; exit 2; }
        paris "$@" ;;
    uscg)                  uscg "$@" ;;
    iuu-list)              iuu "$@" ;;
    tsb|tokyo-mou)         evidence_source "$target" "$@" ;;
    sources)               run evidence sources ${apply[@]+"${apply[@]}"} ;;
    evidence)              all_evidence "$@" ;;
    all)                   placenames "$@"; all_evidence ;;
    raw)                   run "$@" ;;
    *)
        echo "error: unknown target '$target'" >&2
        usage >&2
        exit 2 ;;
esac
