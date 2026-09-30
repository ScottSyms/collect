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
| `--config <file>` | `CONFIG_FILE` | — | Load flag defaults from a flat TOML file; CLI flags and pre-set env vars still win — see [CLI_REFERENCE.md](CLI_REFERENCE.md#shared-across-all-binaries) |

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

With `--iceberg-catalog-uri` set, each sealed bronze batch is committed to the six Iceberg tables; otherwise silver is written as Hive-partitioned Parquet siblings (`positions/`, `statics`, …) under `--output-dir` (time-only layout, no `source=` segment). Bronze is always written; silver failures never fail bronze. See [CLI_REFERENCE.md](CLI_REFERENCE.md#inline-silver-parsing-semantics).

### Common, S3, Iceberg, reconnect, logging

Full flag/env tables now live in one place:
[CLI_REFERENCE.md](CLI_REFERENCE.md#common-to-the-five-collectors-commoncliargs)
(`--output-dir`, `--partition`, buffering/upload flags, `--health-check`,
`--metrics-addr`, `--data-drought-seconds`),
[S3](CLI_REFERENCE.md#s3--collectors-s3cliargs-one-sink),
[Iceberg](CLI_REFERENCE.md#iceberg-icebergcliargs-all-binaries), and
[reconnect](CLI_REFERENCE.md#reconnect-the-four-streaming-collectors)
(`--max-reconnect-seconds`).

Iceberg registration here is optional and direct — unset by default, no
catalog connection is attempted unless `--iceberg-catalog-uri` is set. When
set, each bronze Parquet file is registered as a row in a `raw` Iceberg
table (`ts`, `source`, `payload`) immediately after its S3 upload
succeeds.

**RustFS catalog.** RustFS serves its Iceberg REST catalog at `<endpoint>/iceberg`, uses the bucket name as the warehouse, and rejects unsigned requests. Because the Rust REST client has no request-signing hook, `--iceberg-sigv4` starts a loopback proxy inside the process that signs each catalog call and forwards it (`collect-core/src/iceberg/sigv4.rs`). The target bucket must also be enabled as a "table bucket" first — RustFS's web console has an "Enable this bucket" action for this; there's no documented CLI/REST call. Example:

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
stream resumes where it left off. If the connection can't be re-established
within `--max-reconnect-seconds` (default 300s) of total retrying, the
process gives up and exits with [`UPSTREAM_UNAVAILABLE`](CLI_REFERENCE.md#exit-codes)
so an external restart policy (Nomad, Docker) can take over; set it to `0`
to retry forever instead.
