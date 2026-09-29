# CLI reference

The full flag/env surface for all six binaries. `<binary> --help` is always
the authoritative source for the version you're running — this groups the
same information by the shared structs the flags come from, so it's easier
to see what's common across binaries versus what's bespoke to one.

New to the project? Start with [TUTORIAL.md](TUTORIAL.md) instead — this
page is a reference, not a walkthrough.

## Shared across all six binaries

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--log-format text\|json` | `LOG_FORMAT` | `json` off a TTY, `text` on one | Format for operationally-significant events (startup/shutdown, reconnects, health transitions, failures) — see [Logging](#logging) |
| `-v, --verbose` | `VERBOSE` | off | Also emit debug-level events |
| `--completions <shell>` | — | — | Print shell completions and exit |
| `--config <path>` | `CONFIG_FILE` | — | Load flag defaults from a flat TOML file (same keys as the env var names below); explicit flags and already-set env vars still win |
| `-h, --help` / `-V, --version` | — | — | |

## Common to the four collectors (`CommonCliArgs`)

`collect-socket`, `collect-kafka`, `collect-file`, `collect-aisstream`.

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--output-dir <dir>` | `OUTPUT_DIR` | `data` | Output root |
| `--partition <granularity>` | `PARTITION` | `day` | `minute`\|`hour`\|`day`\|`month`\|`year` |
| `--max-rows <n>` | `MAX_ROWS` | flush on partition boundary | Rows buffered before flush |
| `--max-batch-bytes <n>` | `MAX_BATCH_BYTES` | 64 MiB | Payload bytes buffered before flush |
| `--compression-level <n>` | `COMPRESSION_LEVEL` | `5` | Zstd level |
| `--upload-concurrency <n>` | `UPLOAD_CONCURRENCY` | `4` | Concurrent S3 uploads |
| `--upload-drain-timeout-seconds <n>` | `UPLOAD_DRAIN_TIMEOUT_SECONDS` | `60` | Seconds to drain pending uploads on shutdown |
| `--max-line-length <n>` | `MAX_LINE_LENGTH` | `65536` | Bytes; an oversized line is dropped, not fatal |
| `--health-check` | `HEALTH_CHECK` | off | Run a one-shot health check against the health file and exit — see [DOCKER_HEALTH_CHECK.md](DOCKER_HEALTH_CHECK.md) |
| `--metrics-addr <addr>` | `METRICS_ADDR` | off | Serve Prometheus `/metrics` and `/healthz` on this address |
| `--data-drought-seconds <n>` | `DATA_DROUGHT_SECONDS` | `300` | Exit [`DATA_DROUGHT`](#exit-codes) if no row arrives for this long, even with a healthy connection; `0` disables the check |

## S3 — collectors (`S3CliArgs`, one sink)

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--s3-bucket <bucket[/prefix]>` | `S3_BUCKET` | unset (no S3) | Presence enables S3 upload; a `bucket/prefix` value splits into bucket + `--s3-prefix` |
| `--s3-prefix <prefix>` | `S3_PREFIX` | — | Prepended to every upload key |
| `--s3-endpoint <url>` | `S3_ENDPOINT` | AWS S3 | Set for MinIO/RustFS/any S3-compatible store |
| `--s3-region <region>` | `S3_REGION` | `us-east-1` | |
| `--s3-access-key` / `--s3-secret-key` | `S3_ACCESS_KEY` / `S3_SECRET_KEY` (or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) | — | |
| `--keep-local` | `KEEP_LOCAL` | off (deletes after upload) | |
| `--s3-disable-tls` | `S3_DISABLE_TLS` | off | Plain HTTP |

## S3 — batch parsers (`S3ConnectionArgs`, connection only)

`ais-parse`, `aisstream-parse` read one dataset and write another, so they
take independent, repeatable bucket sets on each side instead of one
`--s3-bucket` — see [their own tables](#ais-parse--aisstream-parse) below.
The connection flags themselves are the same names/envs as the collectors'
table above, minus `--s3-bucket`/`--s3-prefix`/`--keep-local` (not
applicable to a two-sided tool).

## Iceberg (`IcebergCliArgs`, all six binaries)

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--iceberg-catalog-uri <uri>` | `ICEBERG_CATALOG_URI` | unset (no Iceberg) | Presence enables Iceberg; requires `--iceberg-warehouse` |
| `--iceberg-warehouse <location>` | `ICEBERG_WAREHOUSE` | — | e.g. `s3://bucket/warehouse` |
| `--iceberg-namespace <ns>` | `ICEBERG_NAMESPACE` | `ais` | |
| `--iceberg-table-prefix <prefix>` | `ICEBERG_TABLE_PREFIX` | — | e.g. `ais` → `ais_positions` |
| `--iceberg-token <token>` | `ICEBERG_TOKEN` | — | Bearer token for a Lakekeeper-style REST catalog |
| `--iceberg-sigv4` | `ICEBERG_SIGV4` | off | Sign catalog requests with S3 credentials (service `s3`) — needed for a catalog served directly by an S3-compatible store, e.g. RustFS's built-in `/iceberg` endpoint, rather than a separate token-authenticated service |

**Iceberg mode reads its S3 config from environment variables only** —
`S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY`/`AWS_ACCESS_KEY_ID`,
`S3_SECRET_KEY`/`AWS_SECRET_ACCESS_KEY`, and `S3_PATH_STYLE` (`true`/`false`,
Iceberg-only — the non-Iceberg S3 path always uses path style unconditionally
and ignores this var) — independently of the `--s3-*` flags above, which only
configure bronze upload/download. Set both if you're using S3 upload and
Iceberg together. This applies to `--iceberg-sigv4` signing and to the
Iceberg table's underlying data-file storage (the actual Parquet writes a
commit does), not just one or the other.

**RustFS-specific setup**: a bucket must be explicitly enabled as a "table
bucket" before it can serve as `--iceberg-warehouse` — RustFS's web console
has an "Enable this bucket" action for this; there is no documented CLI or
REST call for it, though `PUT /iceberg/v1/buckets/<bucket>` (SigV4-signed)
does it. Skip this for a catalog that isn't RustFS (Lakekeeper, etc.).

## Inline parsing (collectors only, `ParserCliArgs`)

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--parser none\|ais\|aisstream` | `PARSER` | `none` | Decode each ingested row inline into the six silver tables as it arrives; `none` keeps today's bronze-only behavior |
| `--delete-after-iceberg` | `DELETE_AFTER_ICEBERG` | off | Delete each local bronze file once its silver rows commit to Iceberg. Requires `--parser` + `--iceberg-catalog-uri`; rejected together with `--s3-bucket` |

## Reconnect (the three streaming collectors)

`collect-socket`, `collect-kafka`, `collect-aisstream` — not `collect-file`,
which has no live connection to lose.

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--max-reconnect-seconds <n>` | `MAX_RECONNECT_SECONDS` | `300` | Give up and exit [`UPSTREAM_UNAVAILABLE`](#exit-codes) after this much *total* bounded-backoff retrying; `0` retries forever |

## Logging

Every binary emits operationally-significant events (startup/shutdown,
reconnect attempts, health transitions, retry exhaustion, partition/file
failures) either as human-readable text or as one JSON object per line to
stderr:

```json
{"ts":"2026-09-27T11:58:30.918646+00:00","level":"warn","binary":"collect-kafka","event":"reconnect_attempt","message":"Kafka consumer disconnected, attempting to reconnect","topic":"ais-raw"}
```

Routine per-row/per-file progress output is unaffected by `--log-format` —
it stays plain text, gated by `--quiet` instead (see each binary's own
table). `--quiet` and `--log-format`/`--verbose` are independent: quiet
silences routine chatter, log-format/verbose control the structured events.

## Health signals

Three independent surfaces, all backed by the same health computation
(heartbeat freshness, then — while the heartbeat is fresh — data-drought
status):

1. **Process exit code** — authoritative. See [Exit codes](#exit-codes)
   below.
2. **`--health-check`** (collectors only) — a one-shot check against a
   status file the running process maintains; for Docker `HEALTHCHECK`/an
   exec-based orchestrator check. See [DOCKER_HEALTH_CHECK.md](DOCKER_HEALTH_CHECK.md).
3. **`--metrics-addr`** (collectors only, opt-in) — `GET /healthz` returns
   200/503 with the same reason text as (2); `GET /metrics` is Prometheus
   text exposition, all metrics labeled `source="..."`:

   | Metric | Type | Meaning |
   |---|---|---|
   | `collect_rows_processed_total` | counter | Rows ingested since process start |
   | `collect_batches_sealed_total` | counter | Batches queued for Parquet writing |
   | `collect_batches_durable_total` | counter | Batches durably written to local disk |
   | `collect_buffered_bytes` | gauge | Payload bytes in the open batch |
   | `collect_uploads_succeeded_total` / `_failed_total` / `_retries_total` | counter | S3 upload outcomes |
   | `collect_iceberg_registrations_succeeded_total` / `_failed_total` / `_retries_total` | counter | Direct raw-table registration outcomes |
   | `collect_orphan_files_swept_total` | counter | Orphaned files queued at startup |
   | `collect_silver_{positions,statics,meteo,binary,atons,other,incomplete,failed,deduped}_total` | counter | Inline silver decode, per table (only moves with `--parser` set) |
   | `collect_silver_commits_succeeded_total` / `_failed_total` | counter | Inline silver batch commits |
   | `collect_last_row_unix_ms` / `collect_last_heartbeat_unix_ms` | gauge | Feeds the drought/heartbeat health checks above |

## Inline silver parsing semantics

Decode runs in the write worker after the bronze batch is already durable on
local disk (Kafka offsets still commit on bronze durability, independent of
silver). A silver failure is logged and counted
(`collect_silver_commits_failed_total`) but never fails the bronze batch —
bronze is always the authoritative record. Dedup is per-batch, not global,
so a crash-replay can double-append silver rows, and a file recovered as an
orphan at startup has no in-memory batch to decode, so it gets no silver at
all — re-run `ais-parse`/`aisstream-parse` in batch mode against the bronze
data to backfill either gap. Expect higher CPU and roughly 3–7× the memory
bound when `--parser` is set; lower `--max-batch-bytes` if needed.

## Delivery guarantees

- **Graceful shutdown**: on SIGTERM/SIGINT a collector stops reading,
  flushes the in-memory batch, finishes every queued Parquet write, and
  drains pending S3 uploads for up to `--upload-drain-timeout-seconds`
  (default 60s). Give your process manager a stop grace period longer than
  that (`kill_timeout` in Nomad, `stop_grace_period` in Docker Compose).
- **Orphan sweep**: at startup, a collector scans its output directory for
  Parquet files a previous run wrote but never uploaded (crash, SIGKILL,
  an expired drain window) and uploads them in the background. Skipped
  when `--keep-local` is set, since an uploaded file can't then be told
  apart from a genuine orphan.
- **Local cleanup**: without `--keep-local`, a successful upload deletes
  its local Parquet file and recursively removes now-empty Hive partition
  directories, stopping at the output root. Failed, temporary, and unknown
  files are left in place with their directory hierarchy intact.
- **Kafka offsets**: `collect-kafka` disables auto-commit and commits
  offsets only after the batch containing that message is durably written
  to local disk — at-least-once delivery; a crash replays at most a few
  messages rather than losing them.

## Exit codes

Shared across all six binaries (`collect_core::exitcode`):

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Unclassified error (bad configuration, an I/O failure with no more specific code below) |
| `2` | Nothing to do — ran successfully but no input matched (e.g. no partitions, no files) |
| `3` | `UPSTREAM_UNAVAILABLE` — a collector's connection retry window was exhausted |
| `4` | `DATA_DROUGHT` — a collector received no rows within its drought window despite a healthy connection |
| `5` | `PARTIAL_FAILURE_THRESHOLD` — a batch parser skipped more partitions than `--max-partition-failures` allows (or hit one with `--fail-fast`) |

## `collect-socket`

| Flag | Env | Notes |
|---|---|---|
| `--tcp-host <host>` | `TCP_HOST` | Requires `--tcp-port` |
| `--tcp-port <port>` | `TCP_PORT` | Requires `--tcp-host` |
| `-s, --source <label>` | `SOURCE` | Default `tcp` |
| `-q, --quiet` | `QUIET` | Suppress routine progress lines |
| `--consolidate-ais` | `CONSOLIDATE_AIS` | Reassemble fragmented NMEA sentences before writing |
| `--process-timestamps` | `PROCESS_TIMESTAMPS` | Correct row timestamps from `$PGHP`/tag-block `c:` |

Plus [Common](#common-to-the-four-collectors-commoncliargs), [S3](#s3--collectors-s3cliargs-one-sink), [Iceberg](#iceberg-icebergcliargs-all-six-binaries), [inline parsing](#inline-parsing-collectors-only-parsercliargs), [reconnect](#reconnect-the-three-streaming-collectors), and the [shared flags](#shared-across-all-six-binaries).

## `collect-kafka`

| Flag | Env | Notes |
|---|---|---|
| `--kafka-brokers <list>` | `KAFKA_BROKERS` | e.g. `host1:9092,host2:9092` |
| `--kafka-topic <topic>` (alias `--topic`) | `KAFKA_TOPIC` | |
| `--kafka-group-id <id>` (alias `--group-id`) | `KAFKA_GROUP_ID` | |
| `--kafka-auto-offset-reset earliest\|latest` | `KAFKA_AUTO_OFFSET_RESET` | Default `latest` |
| `-s, --source <label>` | `SOURCE` | Defaults to the topic name |
| `-q, --quiet` | `QUIET` | |

Plus [Common](#common-to-the-four-collectors-commoncliargs), [S3](#s3--collectors-s3cliargs-one-sink), [Iceberg](#iceberg-icebergcliargs-all-six-binaries), [inline parsing](#inline-parsing-collectors-only-parsercliargs), [reconnect](#reconnect-the-three-streaming-collectors), and the [shared flags](#shared-across-all-six-binaries).

## `collect-file`

| Flag | Env | Notes |
|---|---|---|
| `--input <path>` | `INPUT_PATH` | File or directory (recursive); auto-detects plain/gzip/bzip2/zip |
| `-s, --source <label>` | `SOURCE` | Defaults to the input's file stem or directory name |
| `--concurrency <n>` | `CONCURRENCY` | Concurrent file workers; auto-selected when omitted |
| `--noui` | — | Disable the runtime status UI, print aggregate updates every 10 files instead |
| `-q, --quiet` | `QUIET` | |
| `--consolidate-ais` | `CONSOLIDATE_AIS` | |
| `--process-timestamps` | `PROCESS_TIMESTAMPS` | |

Plus [Common](#common-to-the-four-collectors-commoncliargs), [S3](#s3--collectors-s3cliargs-one-sink), [Iceberg](#iceberg-icebergcliargs-all-six-binaries), [inline parsing](#inline-parsing-collectors-only-parsercliargs), and the [shared flags](#shared-across-all-six-binaries). No reconnect flags — no live connection to lose.

## `collect-aisstream`

| Flag | Env | Notes |
|---|---|---|
| `--api-key <key>` | `AISSTREAM_API_KEY` | |
| `--bounding-boxes <json>` | `BOUNDING_BOXES` | e.g. `'[[[-90,-180],[90,180]]]'` for the whole world |
| `--filter-mmsi <list>` | `FILTER_MMSI` | Repeatable or comma-separated, max 50 |
| `--filter-message-types <list>` | `FILTER_MESSAGE_TYPES` | Repeatable or comma-separated |
| `-s, --source <label>` | `SOURCE` | Default `aisstream` |
| `-q, --quiet` | `QUIET` | |

Plus [Common](#common-to-the-four-collectors-commoncliargs), [S3](#s3--collectors-s3cliargs-one-sink), [Iceberg](#iceberg-icebergcliargs-all-six-binaries), [inline parsing](#inline-parsing-collectors-only-parsercliargs), [reconnect](#reconnect-the-three-streaming-collectors), and the [shared flags](#shared-across-all-six-binaries).

## `ais-parse` / `aisstream-parse`

Field-for-field identical between the two except where noted; `ais-parse`
also applies AIS-specific pre-processing (`--consolidate-ais`,
`--process-timestamps`) since its NMEA input can carry multi-part
sentences and capture timestamps that `aisstream-parse`'s JSON payloads
don't.

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--input-dir <dir>` | `INPUT_DIR` | — | Repeatable; mutually exclusive with `--input-s3-bucket` |
| `--input-s3-bucket <bucket>` | `INPUT_S3_BUCKET` | — | Repeatable, to merge several buckets on one endpoint |
| `--input-s3-prefix <prefix>` | `INPUT_S3_PREFIX` | `""` | |
| `--output-dir <dir>` | `OUTPUT_DIR` | — | Mutually exclusive with `--output-s3-bucket` |
| `--output-s3-bucket <bucket>` | `OUTPUT_S3_BUCKET` | — | |
| `--output-s3-prefix <prefix>` | `OUTPUT_S3_PREFIX` | `""` | |
| `--partition <granularity>` | `PARTITION` | `day` | Must match the input dataset's actual layout |
| `--filter-source <label>` | `FILTER_SOURCE` | all sources | |
| `--year`/`--month`/`--day`/`--hour`/`--minute` | — | — | Narrow to a fixed window; each requires the coarser one |
| `--since <hours>` | `SINCE` | — | Rolling window from now; mutually exclusive with the fixed filters; with `--incremental`, only the first run's starting bound |
| `--incremental` | `INCREMENTAL` | off | Track a watermark, process only partitions with files newer than the last successful run |
| `--batch-size <n>` | `BATCH_SIZE` | `8192` | Rows per Parquet read batch |
| `--compression-level <n>` | `COMPRESSION_LEVEL` | `5` | |
| `--concurrency <n>` | `CONCURRENCY` | auto | Partitions processed concurrently |
| `--download-concurrency <n>` | `DOWNLOAD_CONCURRENCY` | `4` | Concurrent S3 downloads per partition |
| `--output-prefix <prefix>` | `OUTPUT_PREFIX` | `ais` / `aisstream` | Prepended to output file names |
| `--scratch-dir <dir>` | `SCRATCH_DIR` | system temp | Set to `/dev/shm` or a ramdisk for faster I/O |
| `--consolidate-ais` *(ais-parse only)* | `CONSOLIDATE_AIS` | off | |
| `--process-timestamps` *(ais-parse only)* | `PROCESS_TIMESTAMPS` | off | |
| `--dry-run` | `DRY_RUN` | off | List matching partitions, touch nothing |
| `-q, --quiet` | `QUIET` | | |
| `--fail-fast` | `FAIL_FAST` | off | Abort on the first partition failure instead of skipping it |
| `--max-partition-failures <n>` | `MAX_PARTITION_FAILURES` | `5` | Abort once this many partitions have failed and been skipped |

Plus [S3 connection](#s3--batch-parsers-s3connectionargs-connection-only),
[Iceberg](#iceberg-icebergcliargs-all-six-binaries), and the
[shared flags](#shared-across-all-six-binaries). No `--upload-concurrency`,
`--max-rows`, etc. — these are batch tools, not streaming collectors, so
they don't flatten `CommonCliArgs`.

## `ais-compact`

Table maintenance for Iceberg catalogs with no compactor of their own (RustFS's
built-in catalog). Takes the [Iceberg flags](#iceberg-icebergcliargs-all-six-binaries)
(`--iceberg-catalog-uri`, `--iceberg-warehouse`, `--iceberg-sigv4`, …) plus the
S3 credentials in the environment, and one subcommand. Every command that
changes anything is a **dry run unless `--apply` is given**.

`--table <name>` (repeatable, before or after the subcommand) picks tables by
base name without the prefix; the default is `raw positions statics meteo
binary atons other`.

| Subcommand | Flags | What it does |
|---|---|---|
| `inspect` | `--target-file-mb` (512) | File counts and sizes, partitions needing work, snapshots, small manifests, sort order |
| `compact` | `--apply`, `--min-age-hours` (2), `--target-file-mb` (512), `--max-partition-mb` (1024), `--sort-by a,b` (`mmsi,ts`), `--consolidate-manifests <n>` (20, 0 = off) | Rewrites each *closed* partition that has any freshly ingested file, or two or more files under half the target, into sorted, right-sized zstd files (128Ki-row groups, bloom filters on `mmsi station source imo_number call_sign name`), committed as one Iceberg `replace` snapshot. Registers the sort order on the table. Merges small manifests when at least `n` exist |
| `expire` | `--apply`, `--older-than-days` (7), `--retain-last` (5) | Drops old snapshots (never the current one or a branch/tag head). Metadata only |
| `orphans` | `--apply`, `--older-than-days` (3), S3 connection flags | Deletes objects under the table location that no snapshot references. Run after `expire` to actually free space |

Notes:

- **Sort:** `--sort-by` defaults to `mmsi,ts`, keeping only columns the table
  has, so `raw` (no `mmsi`) sorts by `ts`. Nulls sort last.
- **Memory:** a partition is sorted in memory. `--max-partition-mb` bounds its
  *compressed* input; expect several times that in RAM. Larger partitions are
  skipped with a message, not failed.
- **Idempotent:** files written by the tool are named `compact-*`; a partition of
  full-size `compact-*` files is left alone, and one small tail file is not
  rewritten again.
- **Concurrent ingest:** commits are pinned to the snapshot they were planned
  against. If ingest commits first the catalog answers 409 and the partition is
  reloaded and re-planned (up to 4 attempts).
- **Format:** Iceberg v2 tables without delete files only.
- Exit codes: `0` did something (or `inspect`), `2` nothing to do, `5` a table
  or partition failed.
