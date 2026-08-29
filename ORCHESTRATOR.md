# collect-orchestrator — event-driven per-file parser queue

`collect-orchestrator` is the silver-layer orchestrator: it maintains a Postgres work queue of every bronze Parquet object, receives new-file notifications via an S3-compatible bucket webhook (RustFS/MinIO), and drives bounded-parallel per-file decoding into Iceberg via the `ais-parse` / `aisstream-parse` libraries. Successfully parsed files are archived to `parse_history`; failures remain on the queue for exponential-backoff retry.

```
collect-* (bronze) → S3 PutObject → RustFS/MinIO Bucket Notification → POST /ingest → Postgres parse_queue
                                                                              │
                                                                              └──► worker pool (MAX_INFLIGHT) → library decode → Iceberg REST (Lakekeeper)
                                                                                         │
                                                                                         └──► parse_history (archive)
```

## Why per-file

`ais-parse` and `aisstream-parse` are normally partition-batched (`PartitionKey → Vec<DatasetFile>`), but the orchestrator operates file-granular:

- **Unique row per parquet file:** `parse_queue.s3_key` is the object's full key (e.g. `source=norway/year=2026/month=07/day=15/part-20260715T000000000-000123.parquet`). This is the user-requested unique identifier for every file in the raw bucket, partitioned by `source`.
- **Source-routed parsers:** `source=aisstream*` → `aisstream-parse` library, everything else → `ais-parse` library. Overridden via `--source-map`.
- **Archive not delete:** successes are `DELETE` from `parse_queue` + `INSERT` into `parse_history` (preserves audit; replays remain idempotent via Iceberg commit path).

## Components

| Crate | Role |
|-------|------|
| `collect-orchestrator` | HTTP service + worker pool + CLI backfill |
| `ais-parse` (lib) | NMEA/AIVDM decode (`ais_parse::decode::decode_payload`, `output_iceberg` writers) |
| `aisstream-parse` (lib) | AISStream JSON decode (`aisstream_parse::convert::decode_row`, `ais_stream::AisStreamMessage`, `output_iceberg` writers) |
| `collect-core` | `S3Storage`, `iceberg::{open_catalog, ensure_table, IcebergConfig}`, partitioning helpers |

`ais-parse` and `aisstream-parse` now expose `src/lib.rs` so both their existing `ais-parse`/`aisstream-parse` binaries and `collect-orchestrator` link the same code without forking processes (per refined choice: library, not fork).

## Flow

1. **Webhook ingest (`POST /ingest`):** accepts either:
   - S3 Event Notification JSON (`{ "Records": [{ "eventName": "s3:ObjectCreated:*", "s3": { "bucket": {"name":"..."}, "object": {"key":"source=norway/.../part.parquet"} } }] }`) — keys are URL-decoded, or
   - Flat JSON `{ "s3_bucket": "collections", "s3_key": "source=norway/.../part.parquet" }` / array thereof (manual `curl` / tests).
   Only `.parquet` keys are enqueued. Insert is `ON CONFLICT (s3_key) DO NOTHING` (idempotent redelivery). Auth optional via `Authorization: Bearer $INGEST_TOKEN` (`INGEST_TOKEN`, forwarded as `NOTIFY_WEBHOOK_AUTH_TOKEN` on RustFS).

2. **Queue (`parse_queue` / `parse_history`, `crates/collect-orchestrator/migrations/001_queue.sql`):**

  ```sql
  parse_queue(s3_bucket, s3_key PK, source, parser ∈ {ais-parse,aisstream-parse},
              status ∈ {pending,processing,failed,dead_letter}, attempts, max_attempts=5,
              last_error, next_retry_at, created_at, updated_at, locked_at, locked_by)
  parse_history(s3_bucket, s3_key PK, source, parser, attempts, duration_ms,
                rows_in, positions_out, statics_out, meteo_out, binary_out, atons_out, other_out,
                incomplete, unparsed, deduped, created_at, completed_at)
  ```

   `FOR UPDATE SKIP LOCKED` (`db.rs:fetch_pending`) allows future horizontal replicas; lease reclaim re-queues `processing` rows with `locked_at < now()-10m`.

3. **Worker pool:** semaphore `MAX_INFLIGHT` (default 4, env `MAX_INFLIGHT` / `--max-inflight`). Each slot:
   - marks row `processing` (`locked_at/by`, `attempts++`),
   - downloads single object via `S3Storage::download_to_path` (3 attempts, 1 s→4 s backoff; scratch dir `SCRATCH_DIR` or tempdir),
   - `spawn_blocking` decode: `decode_ais_file` or `decode_aisstream_file` (`decode.rs`) → `FileStats` + `IcebergBatches` (`Vec<RecordBatch>` ×6),
   - commits to Iceberg REST (`open_catalog`, `ensure_namespace`, `ensure_table` for `positions/statics/meteo/binary/atons/other` with `day` partition spec → `DataFileWriter` per table → `fast_append`; empty batches skipped),
   - on success `archive_success` (`DELETE`+`INSERT` tx) after `close().await`; on error `mark_failed` with exponential backoff `5s*2^attempts` capped 1 h + jitter, or `dead_letter` after `max_attempts`.

   Note: per-file decode means multi-part NMEA fragments split across files will be `Incomplete`/`Failed` rather than reassembled — surfaced as `incomplete`/`unparsed` in `parse_history`.

4. **Backfill (`--backfill`):** lists all `.parquet` keys under `--input-s3-prefix` via `S3Storage::list_keys_with_prefix` and enqueues missing rows. Run once after deploy and/or as periodic Nomad batch to catch missed webhooks.

## CLI reference

```
collect-orchestrator [--listen-addr 0.0.0.0:8080] [--database-url $DATABASE_URL]
  [--max-inflight 4] [--batch-size 8192] [--compression-level 5]
  [--scratch-dir /tmp] [--source-map ./source_map.toml] [--ingest-token xxx]
  --input-s3-bucket collections [--input-s3-prefix bronze]
  --s3-endpoint http://rustfs:9000 --s3-region us-east-1 --s3-access-key ... --s3-secret-key ...
  --iceberg-catalog-uri http://lakekeeper:8181/catalog --iceberg-warehouse s3://warehouse
  [--iceberg-namespace ais] [--iceberg-token ...]
  [--backfill] [--config file.toml] [--completions <shell>]
```

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--listen-addr` | `LISTEN_ADDR` | `0.0.0.0:8080` | HTTP listen |
| `--database-url` | `DATABASE_URL` | — | Postgres (also used by Lakekeeper) |
| `--max-inflight` | `MAX_INFLIGHT` | `4` | Bounded parallel parses (answer 8) |
| `--batch-size` | `BATCH_SIZE` | `8192` | Parquet read batch rows |
| `--compression-level` | `COMPRESSION_LEVEL` | `5` | Zstd level for Iceberg data files |
| `--scratch-dir` | `SCRATCH_DIR` | system tmp | S3 download dir (`/dev/shm` for tmpfs) |
| `--source-map` | `SOURCE_MAP` | infer | `source → parser` overrides (`source_map.toml` `[source_map]` table) |
| `--ingest-token` | `INGEST_TOKEN` | — | Bearer token for `POST /ingest` |
| `--input-s3-bucket` | `INPUT_S3_BUCKET` | `collections` | Bronze bucket to download from |
| `--input-s3-prefix` | `INPUT_S3_PREFIX` | `""` | Key prefix under bucket (`bronze`) |
| `--s3-endpoint/region/access-key/secret-key/disable-tls` | `S3_*` | — | Shared S3 connection (RustFS/MinIO/AWS) |
| `--iceberg-catalog-uri` | `ICEBERG_CATALOG_URI` | — | REST catalog URI |
| `--iceberg-warehouse` | `ICEBERG_WAREHOUSE` | — | Warehouse (`s3://warehouse`) |
| `--iceberg-namespace` | `ICEBERG_NAMESPACE` | `ais` | Namespace (tables shared with batch parsers) |
| `--iceberg-token` | `ICEBERG_TOKEN` | — | Lakekeeper bearer token |
| `--backfill` | — | — | List S3 and enqueue then exit |
| `--config` | `CONFIG_FILE` | — | Flat TOML defaults (same precedence as other binaries) |

All six Iceberg tables are written (`positions`, `statics`, `meteo`, `binary`, `atons`, `other`; `h3`/`hilbert` `u64→i64` as in batch parsers). S3 storage for Iceberg is via REST warehouse + env `S3_*` / `S3_PATH_STYLE`, etc. (see `AIS_PARSE.md#iceberg-output`).

## HTTP API

- `POST /ingest` — enqueue (auth optional). Returns `{ "accepted": N, "duplicates": M }`. Example:

  ```bash
  curl -H "Authorization: Bearer $INGEST_TOKEN" -H "Content-Type: application/json" \
    -d '{"s3_bucket":"collections","s3_key":"bronze/source=norway/year=2026/month=07/day=15/part-abc.parquet"}' \
    http://localhost:8080/ingest
  # S3 event passthrough:
  curl -H "Authorization: Bearer $INGEST_TOKEN" -d @s3-event.json http://localhost:8080/ingest
  ```

- `GET /healthz` — `200` if Postgres reachable, else `503`.
- `GET /metrics` — Prometheus `orchestrator_queue_depth{status}` (add `orchestrator_inflight` etc. as needed).
- `GET /queue?status=pending&limit=100` — list queued rows (for debugging/alerts; `dead_letter` signals attention).

Every binary also supports `--version` (`0.1.0 (hash)`) and `--completions <shell>`.

## RustFS webhook wiring

RustFS (MinIO-compatible) bucket notifications replace AWS Lambda:

```bash
# Alias (MinIO `mc` works for RustFS)
mc alias set rustfs http://rustfs:9000 $S3_ACCESS_KEY $S3_SECRET_KEY
mc event add rustfs/collections arn:minio:sqs::1:webhook --event put --prefix bronze/ --suffix .parquet
# RustFS / MinIO notify config (env):
# NOTIFY_WEBHOOK_ENABLE=on NOTIFY_WEBHOOK_ENDPOINT=http://collect-orchestrator:8080/ingest
# NOTIFY_WEBHOOK_AUTH_TOKEN=$INGEST_TOKEN
```

Local `docker-compose.yml` runs `collect-orchestrator` on `8080` dependent on `db`/`lakekeeper` healthy; swap the `minio` service for `rustfs` and keep the `collect-orchestrator` `S3_ENDPOINT=http://rustfs:9000`. `Nomad` job `nomad/collect-orchestrator-prod` is a `service` with `check http /healthz` and webhook comments.

## Failure semantics

- **Retry:** `next_retry_at = now() + 5s*2^attempts + jitter` (capped 1 h). `failed` → retry after `next_retry_at`. `dead_letter` after 5 attempts (manual `GET /queue?status=dead_letter` + alert; re-enqueue via updating `status=pending`).
- **Crash safety:** lease reclaim (`UPDATE parse_queue SET status='pending' WHERE locked_at < now()-10m`) on every worker loop + startup.
- **Idempotency:** idempotent insert + per-table `fast_append` (pure append). Duplicates across runs are not row-deduped (that is partition-batched), but per-file archiving makes a restarted `backfill` a no-op for already-archived keys. A crash between table commits of one file can leave a partial commit (same narrow window as batch parsers' `CommitManifest` note `AIS_PARSE.md#idempotent-re-runs-with-iceberg`).

## Performance

- `MAX_INFLIGHT=4` by default (configurable). Each slot holds one S3 download + one blocking decode + six Iceberg commits. Tune with `download_concurrency` equivalent already inside `S3Storage`; orchestrator serializes per-file Iceberg catalog calls.

## Backfill & repair

```bash
# One-shot backfill (lists bucket prefix, enqueues missing .parquet keys):
DATABASE_URL=postgresql://postgres:postgres@db:5432/postgres \
  collect-orchestrator --input-s3-bucket collections --input-s3-prefix bronze \
    --s3-endpoint http://rustfs:9000 --s3-access-key ... --s3-secret-key ... --s3-disable-tls \
    --iceberg-catalog-uri http://lakekeeper:8181/catalog --iceberg-warehouse s3://warehouse \
    --backfill
```

Schedule also as periodic Nomad batch `collect-orchestrator-backfill` to heal missed webhook deliveries.
