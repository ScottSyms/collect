#!/bin/sh

set -eu

usage() {
    cat <<'EOF'
Usage: ./copy_to_rustfs.sh [DESTINATION]

Recursively finds *.parquet under the current directory, groups the files by
their leaf directory (for example month=03/day=29), combines each directory's
files with DuckDB, uploads each combined parquet to RustFS, and appends each
batch to an Apache Iceberg table in the RustFS S3 Tables (Iceberg REST) catalog.

DESTINATION is the object name written inside each batch directory, so a file in
month=03/day=29 is uploaded to s3://mytable/month=03/day=29/<DESTINATION>.
DESTINATION defaults to compact.parquet. When the script runs in a directory
with no parquet subdirectories, the object is s3://mytable/<DESTINATION>.

Optional environment variables:
  RUSTFS_ENDPOINT     RustFS S3 endpoint    (default: localhost:9000)
  RUSTFS_ACCESS_KEY   S3 access key         (default: rustfsadmin)
  RUSTFS_SECRET_KEY   S3 secret key         (default: rustfsadmin)
  RUSTFS_BUCKET       S3 bucket / warehouse (default: mytable)
  DUCKDB_BIN          DuckDB executable     (default: duckdb)

  RUSTFS_COPY_ENABLED Upload each combined directory parquet (default: 1; set
                      0 to only commit the Iceberg table)

  ICEBERG_ENABLED     Register an Iceberg table (default: 1; set 0 to skip)
  ICEBERG_NAMESPACE   Iceberg namespace     (default: space3)
  ICEBERG_TABLE       Iceberg table name    (default: compact)
  ICEBERG_MODE        overwrite | append    (default: overwrite)
  ICEBERG_REGION      SigV4 signing region  (default: us-east-1)
  ICEBERG_BATCH_MB    MiB of parquet read per Iceberg append (default: 512)
  PYTHON_BIN          Python interpreter    (default: python3)

Only the {ts, payload} columns are committed. The month/day directories are
Hive-style path partitions; those columns are not stored in the parquet files
and are deliberately not added to the Iceberg schema.

The Iceberg step needs PyIceberg, PyArrow and boto3. When the `uv` command is
present they are provisioned on the fly; otherwise install them into the
interpreter named by PYTHON_BIN.
EOF
}

case "${1:-}" in
    -h|--help)
        usage
        exit 0
        ;;
esac

if [ "$#" -gt 1 ]; then
    usage >&2
    exit 2
fi

RUSTFS_ENDPOINT=${RUSTFS_ENDPOINT:-localhost:9000}
RUSTFS_ACCESS_KEY=${RUSTFS_ACCESS_KEY:-rustfsadmin}
RUSTFS_SECRET_KEY=${RUSTFS_SECRET_KEY:-rustfsadmin}
RUSTFS_BUCKET=${RUSTFS_BUCKET:-mytable}
DUCKDB_BIN=${DUCKDB_BIN:-duckdb}
DESTINATION=${1:-compact.parquet}

RUSTFS_COPY_ENABLED=${RUSTFS_COPY_ENABLED:-1}

ICEBERG_ENABLED=${ICEBERG_ENABLED:-1}
ICEBERG_NAMESPACE=${ICEBERG_NAMESPACE:-space3}
ICEBERG_TABLE=${ICEBERG_TABLE:-compact}
ICEBERG_MODE=${ICEBERG_MODE:-overwrite}
ICEBERG_REGION=${ICEBERG_REGION:-us-east-1}
ICEBERG_BATCH_MB=${ICEBERG_BATCH_MB:-512}
PYTHON_BIN=${PYTHON_BIN:-python3}

case "$RUSTFS_COPY_ENABLED" in
    0|1) ;;
    *)
        printf 'Error: RUSTFS_COPY_ENABLED must be 0 or 1 (got %s)\n' "$RUSTFS_COPY_ENABLED" >&2
        exit 2
        ;;
esac

case "$ICEBERG_MODE" in
    overwrite|append) ;;
    *)
        printf 'Error: ICEBERG_MODE must be "overwrite" or "append" (got %s)\n' "$ICEBERG_MODE" >&2
        exit 2
        ;;
esac

if [ "$RUSTFS_COPY_ENABLED" = "1" ] && ! command -v "$DUCKDB_BIN" >/dev/null 2>&1; then
    printf 'Error: DuckDB executable not found: %s\n' "$DUCKDB_BIN" >&2
    exit 1
fi

# These values are embedded as SQL literals. Reject quotes rather than risk
# malformed SQL or accidental injection from an environment variable.
for value in "$RUSTFS_ENDPOINT" "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY" \
    "$RUSTFS_BUCKET" "$DESTINATION"; do
    case "$value" in
        *"'"*)
            printf "Error: configuration values cannot contain single quotes\n" >&2
            exit 1
            ;;
    esac
done

batch_file=$(mktemp "${TMPDIR:-/tmp}/copy_to_rustfs_batches.XXXXXX")
sql_file=$(mktemp "${TMPDIR:-/tmp}/copy_to_rustfs_sql.XXXXXX")
iceberg_py=
cleanup() {
    rm -f "$batch_file" "$sql_file" "$iceberg_py"
}
trap cleanup EXIT INT TERM

find . -type f -name '*.parquet' -exec dirname {} + | sort -u >"$batch_file"
if [ ! -s "$batch_file" ]; then
    printf 'Error: no *.parquet files found under %s\n' "$(pwd)" >&2
    exit 1
fi

# The directory paths are embedded as SQL literals too; reject quotes for the
# same reason the configuration values are checked.
while IFS= read -r dir; do
    case "$dir" in
        *"'"*)
            printf 'Error: path contains a single quote: %s\n' "$dir" >&2
            exit 1
            ;;
    esac
done <"$batch_file"

if [ "$RUSTFS_COPY_ENABLED" = "1" ]; then
    {
        printf 'INSTALL httpfs;\n'
        printf 'LOAD httpfs;\n'
        printf 'CREATE SECRET rustfs (\n'
        printf "    TYPE s3,\n"
        printf "    KEY_ID '%s',\n" "$RUSTFS_ACCESS_KEY"
        printf "    SECRET '%s',\n" "$RUSTFS_SECRET_KEY"
        printf "    ENDPOINT '%s',\n" "$RUSTFS_ENDPOINT"
        printf "    URL_STYLE 'path',\n"
        printf "    USE_SSL false\n"
        printf ');\n'
        while IFS= read -r dir; do
            rel=${dir#./}
            if [ -z "$rel" ] || [ "$rel" = "." ]; then
                target="s3://${RUSTFS_BUCKET}/${DESTINATION}"
            else
                target="s3://${RUSTFS_BUCKET}/${rel}/${DESTINATION}"
            fi
            printf 'COPY (\n'
            printf "    SELECT * FROM read_parquet('%s/*.parquet', union_by_name = true, hive_partitioning = false)\n" "$dir"
            printf ") TO '%s' (FORMAT parquet);\n" "$target"
        done <"$batch_file"
    } >"$sql_file"

    "$DUCKDB_BIN" <"$sql_file"

    batch_count=$(wc -l <"$batch_file" | tr -d ' ')
    printf 'Copied %s directory batch(es) to s3://%s\n' "$batch_count" "$RUSTFS_BUCKET"
fi

if [ "$ICEBERG_ENABLED" = "0" ]; then
    exit 0
fi

# --- Register the data as an Iceberg table in the RustFS catalog -------------
#
# A raw object upload never becomes a table: Iceberg tables are catalog-managed
# entities, and RustFS refuses to let ordinary S3 writes create catalog
# metadata. The table has to be created and committed through the Iceberg REST
# catalog, which is what the PyIceberg step below does.
#
# RustFS enforces SigV4 on the catalog and requires x-amz-content-sha256 on
# every request. PyIceberg's built-in SigV4 adapter signs with the generic
# botocore SigV4Auth and omits that header when a request has a body, which
# RustFS rejects. The adapter below mirrors RustFS's own conformance suite
# (scripts/table-catalog/pyiceberg_smoke.py): sign with S3SigV4Auth and always
# set x-amz-content-sha256.
iceberg_py=$(mktemp "${TMPDIR:-/tmp}/copy_to_iceberg.XXXXXX.py")

cat >"$iceberg_py" <<'PY'
import glob
import hashlib
import os
import sys

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from pyiceberg.catalog.rest import RestCatalog
from requests.adapters import HTTPAdapter

endpoint = os.environ["RUSTFS_ENDPOINT"]
if "://" not in endpoint:
    endpoint = "http://" + endpoint
bucket = os.environ["RUSTFS_BUCKET"]
access_key = os.environ["RUSTFS_ACCESS_KEY"]
secret_key = os.environ["RUSTFS_SECRET_KEY"]
namespace = os.environ["ICEBERG_NAMESPACE"]
table_name = os.environ["ICEBERG_TABLE"]
mode = os.environ["ICEBERG_MODE"].lower()
region = os.environ["ICEBERG_REGION"]
max_bytes = int(os.environ.get("ICEBERG_BATCH_MB", "512")) * 1024 * 1024
batch_list = sys.argv[1]


class RustfsSigV4Adapter(HTTPAdapter):
    def add_headers(self, request, **kwargs):
        body = request.body or b""
        if isinstance(body, str):
            body = body.encode("utf-8")
        request.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
        if "connection" in request.headers:
            del request.headers["connection"]
        aws_request = AWSRequest(
            method=request.method,
            url=request.url,
            data=body,
            headers=dict(request.headers),
        )
        S3SigV4Auth(Credentials(access_key, secret_key), "s3", region).add_auth(aws_request)
        request.headers.update(aws_request.headers)


class RustfsRestCatalog(RestCatalog):
    # RestCatalog fetches /config before building its persistent session, so the
    # signing adapter has to be mounted on that throwaway session too.
    def _init_sigv4(self, session):
        session.mount(self.uri, RustfsSigV4Adapter())


properties = {
    "uri": f"{endpoint}/iceberg",
    "warehouse": bucket,
    "rest.sigv4-enabled": "true",
    "rest.signing-name": "s3",
    "s3.endpoint": endpoint,
    "s3.access-key-id": access_key,
    "s3.secret-access-key": secret_key,
    "s3.path-style-access": "true",
    "s3.region": region,
}

catalog = RustfsRestCatalog("rustfs", **properties)

with open(batch_list, "r", encoding="utf-8") as handle:
    directories = [line.strip() for line in handle if line.strip()]


def iter_chunks(directory):
    # Cap each Arrow table by on-disk size so a busy day (some hold thousands
    # of files) is never read whole into memory twice before it is committed.
    files = sorted(glob.glob(os.path.join(directory, "*.parquet")))
    chunk = []
    size = 0
    for path in files:
        try:
            path_size = os.path.getsize(path)
        except OSError:
            path_size = 0
        if chunk and size + path_size > max_bytes:
            yield chunk
            chunk = []
            size = 0
        chunk.append(path)
        size += path_size
    if chunk:
        yield chunk


COLUMNS = ["ts", "payload"]


def read_chunk(files):
    try:
        data = ds.dataset(files, format="parquet", partitioning=None).to_table()
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, TypeError, ValueError):
        tables = [pq.read_table(path) for path in files]
        if len(tables) == 1:
            data = tables[0]
        else:
            try:
                data = pa.concat_tables(tables, promote_options="permissive")
            except TypeError:
                data = pa.concat_tables(tables)
    return data.select(COLUMNS)


try:
    catalog.create_namespace_if_not_exists(namespace)
except AttributeError:
    try:
        catalog.create_namespace(namespace)
    except Exception as error:
        if "already exists" not in str(error).lower():
            raise

identifier = (namespace, table_name)
table = None
rows = 0
appends = 0

for directory in directories:
    for files in iter_chunks(directory):
        data = read_chunk(files)
        if table is None:
            if catalog.table_exists(identifier):
                table = catalog.load_table(identifier)
                if mode == "append":
                    table.append(data)
                else:
                    table.overwrite(data)
            else:
                table = catalog.create_table(identifier, schema=data.schema)
                table.append(data)
        else:
            table.append(data)
        rows += data.num_rows
        appends += 1
        print(
            f"{directory}: committed {data.num_rows} rows "
            f"({rows} rows, {appends} appends total)",
            flush=True,
        )

if table is None:
    print("Error: no parquet files to commit", file=sys.stderr)
    sys.exit(1)

print(
    f"Iceberg table {namespace}.{table_name} -> {table.location()} "
    f"({rows} rows across {appends} appends, mode={mode})"
)
PY

export RUSTFS_ENDPOINT RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY RUSTFS_BUCKET
export ICEBERG_NAMESPACE ICEBERG_TABLE ICEBERG_MODE ICEBERG_REGION ICEBERG_BATCH_MB

if command -v uv >/dev/null 2>&1; then
    uv run --quiet --with 'pyiceberg[pyarrow]' --with boto3 python "$iceberg_py" "$batch_file"
else
    if ! "$PYTHON_BIN" -c 'import pyiceberg, pyarrow, boto3' >/dev/null 2>&1; then
        printf 'Error: %s is missing pyiceberg, pyarrow or boto3.\n' "$PYTHON_BIN" >&2
        printf 'Install them, or install the "uv" command to provision them on the fly.\n' >&2
        exit 1
    fi
    "$PYTHON_BIN" "$iceberg_py" "$batch_file"
fi
