# collect-socket — TCP data collector

Reads newline-delimited data from a TCP connection and writes it into
Hive-partitioned Parquet with Zstd compression. Supports AIS multi-part
message consolidation and `$PGHP` timestamp processing.

## Pipeline

```
TCP stream (newline-delimited) → collect-socket → Bronze Parquet
                                                  (ts, payload, source)
```

## Usage

```bash
# Basic TCP ingest
cargo run -p collect-socket -- \
  --tcp-host 153.44.253.27 --tcp-port 5631 \
  --source norway-tcp

# With S3 output
cargo run -p collect-socket -- \
  --tcp-host 153.44.253.27 --tcp-port 5631 \
  --source norway-tcp \
  --s3-bucket bronze --s3-prefix norway-tcp \
  --s3-endpoint http://minio:9000 \
  --s3-access-key minio --s3-secret-key minioadmin --s3-disable-tls

# With AIS multipart consolidation
cargo run -p collect-socket -- \
  --tcp-host 153.44.253.27 --tcp-port 5631 \
  --consolidate-ais \
  --process-timestamps
```

## CLI Reference

### Connection

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--tcp-host` | `TCP_HOST` | — | TCP host address |
| `--tcp-port` | `TCP_PORT` | — | TCP port |

### Source

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--source` / `-s` | `SOURCE` | `"tcp"` | Logical source label |
| `--quiet` / `-q` | `QUIET` | off | Suppress routine progress lines; warnings/errors still print |
| `--completions <shell>` | — | — | Print shell completions to stdout and exit |
| `--version` | — | — | Prints `<crate version> (<git commit hash>)` |
| `--config <file>` | `CONFIG_FILE` | — | Load flag defaults from a flat TOML file; CLI flags and pre-set env vars still win — see [README.md](README.md#common-cli-features) |

### AIS Processing

| Flag | Default | Description |
|------|---------|-------------|
| `--consolidate-ais` | off | Reassemble multi-part NMEA fragments into single sentences |
| `--process-timestamps` | off | Extract `$PGHP` and tag-block `c:` timestamps |

See [AIS_PARSE.md](AIS_PARSE.md) for details on the consolidation pipeline.

### Inline Parsing (`--parser`)

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--parser` | `PARSER` | `none` | Decode lines into silver tables inline: `ais` (NMEA) or `aisstream` (JSON) |

With `--iceberg-catalog-uri` set, each sealed bronze batch is committed to the six Iceberg tables; otherwise silver is written as Hive-partitioned Parquet siblings (`positions/`, `statics`, …) under `--output-dir` (time-only layout, no `source=` segment). Bronze is always written; silver failures never fail bronze. See [README.md](README.md#inline-silver-parsing).

### Common (from `CommonCliArgs`)

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--output-dir` | `OUTPUT_DIR` | `"data"` | Local output root directory |
| `--partition` | `PARTITION` | `day` | Partition granularity (minute/hour/day/month/year) |
| `--max-rows` | `MAX_ROWS` | — | Max rows per file before flush |
| `--max-batch-bytes` | `MAX_BATCH_BYTES` | — | Max batch bytes before flush |
| `--compression-level` | `COMPRESSION_LEVEL` | `5` | Zstd compression level |
| `--upload-drain-timeout-seconds` | `UPLOAD_DRAIN_TIMEOUT_SECONDS` | `60` | Max seconds to wait for uploads on shutdown |
| `--max-line-length` | `MAX_LINE_LENGTH` | — | Max input line length |
| `--health-check` | `HEALTH_CHECK` | off | Serve health check endpoint |
| `--metrics-addr` | `METRICS_ADDR` | — | Prometheus metrics address |

### S3 (from `S3CliArgs`)

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--s3-bucket` | `S3_BUCKET` | — | S3 bucket (supports `bucket/path` syntax) |
| `--s3-prefix` | `S3_PREFIX` | — | Optional key prefix |
| `--s3-endpoint` | `S3_ENDPOINT` | — | Custom S3 endpoint |
| `--s3-region` | `S3_REGION` | `us-east-1` | AWS region |
| `--s3-access-key` | `S3_ACCESS_KEY` | — | Access key ID |
| `--s3-secret-key` | `S3_SECRET_KEY` | — | Secret access key |
| `--keep-local` | `KEEP_LOCAL` | false | Keep files after S3 upload |
| `--s3-disable-tls` | `S3_DISABLE_TLS` | false | Use HTTP instead of HTTPS |

### Iceberg (from `IcebergCliArgs`) — optional, direct raw registration

Unset by default — no catalog connection is attempted unless `--iceberg-catalog-uri` is set. When set, each bronze Parquet file is registered as a row in a `raw` Iceberg table (`ts`, `source`, `payload`) immediately after its S3 upload succeeds.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--iceberg-catalog-uri` | `ICEBERG_CATALOG_URI` | — | Iceberg REST catalog URI (e.g. `http://lakekeeper:8181/catalog`); enables this feature when set |
| `--iceberg-warehouse` | `ICEBERG_WAREHOUSE` | — | Iceberg warehouse location (required when the catalog URI is set) |
| `--iceberg-namespace` | `ICEBERG_NAMESPACE` | `ais` | Iceberg namespace (database) |
| `--iceberg-table-prefix` | `ICEBERG_TABLE_PREFIX` | — | Optional prefix added to the `raw` table name |
| `--iceberg-token` | `ICEBERG_TOKEN` | — | Bearer token for Lakekeeper / REST catalog auth |
| `--iceberg-sigv4` | `ICEBERG_SIGV4` | false | Sign catalog requests with AWS SigV4 using `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_REGION`; required by RustFS's built-in catalog |

**RustFS catalog.** RustFS serves its Iceberg REST catalog at `<endpoint>/iceberg`, uses the bucket name as the warehouse, and rejects unsigned requests. Because the Rust REST client has no request-signing hook, `--iceberg-sigv4` starts a loopback proxy inside the process that signs each catalog call and forwards it (`collect-core/src/iceberg/sigv4.rs`). Example:

```bash
S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=rustfsadmin S3_SECRET_KEY=rustfsadmin \
S3_REGION=us-east-1 S3_PATH_STYLE=true \
cargo run -p collect-socket -- --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp \
  --parser ais --iceberg-catalog-uri http://localhost:9000/iceberg \
  --iceberg-warehouse ais --iceberg-sigv4
```

**`--delete-after-iceberg`** (env `DELETE_AFTER_ICEBERG`) deletes each local bronze Parquet file (and its empty partition directories) once the silver rows for that batch have committed to Iceberg. It requires `--parser` and `--iceberg-catalog-uri`, and is rejected together with `--s3-bucket` (S3 upload already deletes local files). Raw payloads are not kept anywhere afterwards, so rows that fail to decode are lost; a failed Iceberg commit keeps the file.

Registration is best-effort: a short bounded retry, then a logged warning and a `collect_iceberg_registrations_failed_total` metric bump — it never fails or blocks the upload. Files recovered from a crash (orphaned uploads found on restart) are not registered, since no in-memory batch survives a restart; re-run `ais-parse`/`aisstream-parse` in batch mode against the bronze data to backfill any gaps.

## Output

- **Schema:** `ts` (timestamp ms UTC), `payload` (utf8), `source` (utf8)
- **Partition:** Hive-style under `--output-dir` (default: `data/`)
- **Compression:** Zstd
- **Reconnect:** Exponential backoff (1s → 5s max) on disconnect

## Reconnection

On TCP disconnect, `collect-socket` automatically reconnects with exponential
backoff starting at 1 second, doubling to a 5-second maximum. The ingest
stream resumes where it left off.
