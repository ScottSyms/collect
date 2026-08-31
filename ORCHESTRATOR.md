# collect-orchestrator — event-driven per-file parser queue

`collect-orchestrator` is the silver-layer orchestrator: it maintains a Postgres work queue of every bronze Parquet object, receives new-file notifications via an S3-compatible bucket webhook (RustFS/MinIO), and drives per-file decoding into Iceberg via the `ais-parse` / `aisstream-parse` libraries. Successfully parsed files are archived to `parse_history`; failures remain on the queue for exponential-backoff retry.

Two execution modes share the same queue and HTTP surface:

* **Inline mode (default)** — bounded in-process worker pool (`MAX_INFLIGHT`).
* **Nomad dispatch mode (`ENABLE_DISPATCH=true`)** — each file is dispatched as a Nomad parameterized batch job (`parse-file`) and scheduled across the Nomad cluster; workers callback to `POST /complete`.

```
collect-* (bronze) → S3 PutObject → RustFS/MinIO Bucket Notification → POST /ingest → Postgres parse_queue
                                                                              │
                                   ┌──────────────────────────────────────────┼──────────────────────────────────────────┐
                                   │ inline (ENABLE_DISPATCH=false)           │ dispatch (ENABLE_DISPATCH=true)          │
                                   ▼                                          ▼                                          │
                         worker pool (MAX_INFLIGHT)              dispatcher → Nomad POST /v1/job/parse-file/dispatch │
                           │ library decode → Iceberg REST                     │   └─► parse-file-worker (per file) → Iceberg │
                           │                                                   │                    │                        │
                           └───────────────► parse_history (archive) ◄─────────┴────────────────────┘                        │
```

## Why per-file

`ais-parse` and `aisstream-parse` are normally partition-batched (`PartitionKey → Vec<DatasetFile>`), but the orchestrator operates file-granular:

- **Unique row per parquet file:** `parse_queue.s3_key` is the object's full key (e.g. `source=norway/year=2026/month=07/day=15/part-20260715T000000000-000123.parquet`). This is the user-requested unique identifier for every file in the raw bucket, partitioned by `source`.
- **Source-routed parsers:** `source=aisstream*` → `aisstream-parse` library, everything else → `ais-parse` library. Overridden via `--source-map`.
- **Archive not delete:** successes are `DELETE` from `parse_queue` + `INSERT` into `parse_history` (preserves audit; replays remain idempotent via Iceberg commit path).

## Components

| Crate | Role |
|-------|------|
| `collect-orchestrator` | HTTP service (`POST /ingest`, `POST /complete|/fail`, `GET /healthz|/metrics|/queue`) + queue + dispatcher or inline worker pool + CLI backfill |
| `parse-file-worker` | Nomad batch worker: downloads one bronze object, decodes via same libraries, commits to Iceberg, then callbacks to orchestrator (`crates/parse-file-worker/src/main.rs`) |
| `ais-parse` (lib) | NMEA/AIVDM decode (`ais_parse::decode::decode_payload`, `output_iceberg` writers) |
| `aisstream-parse` (lib) | AISStream JSON decode (`aisstream_parse::convert::decode_row`, `ais_stream::AisStreamMessage`, `output_iceberg` writers) |
| `collect-core` | `S3Storage`, `iceberg::{open_catalog, ensure_table, commit_batches, IcebergConfig}`, partitioning helpers |

`ais-parse` and `aisstream-parse` expose `src/lib.rs` so both their standalone binaries and the orchestrator/worker link the same code without forking processes. `collect-core::iceberg::commit_batches` is the shared Iceberg writer used by both inline and dispatch paths.

## Flow

### 1. Webhook ingest (`POST /ingest`)

Accepts either:

- S3 Event Notification JSON (`{ "Records": [{ "eventName": "s3:ObjectCreated:*", "s3": { "bucket": {"name":"..."}, "object": {"key":"source=norway/.../part.parquet"} } }] }`) — keys are URL-decoded, or
- Flat JSON `{ "s3_bucket": "collections", "s3_key": "source=norway/.../part.parquet" }` / array thereof (manual `curl` / tests).

Only `.parquet` keys are enqueued. Insert is `ON CONFLICT (s3_key) DO NOTHING` (idempotent redelivery). Auth optional via `Authorization: Bearer $INGEST_TOKEN` (`INGEST_TOKEN`, forwarded as `NOTIFY_WEBHOOK_AUTH_TOKEN` on RustFS). Same token (or `CALLBACK_TOKEN`) guards `POST /complete|/fail`.

### 2. Queue (`parse_queue` / `parse_history`, `crates/collect-orchestrator/migrations/001_queue.sql` + `002_dispatch.sql`)

```sql
parse_queue(s3_bucket, s3_key PK, source, parser ∈ {ais-parse,aisstream-parse},
            status ∈ {pending,processing,failed,dead_letter,dispatched}, attempts, max_attempts=5,
            last_error, next_retry_at, created_at, updated_at, locked_at, locked_by,
            dispatched_at, nomad_job_id, nomad_alloc_id)
parse_history(s3_bucket, s3_key PK, source, parser, attempts, duration_ms,
              rows_in, positions_out, statics_out, meteo_out, binary_out, atons_out, other_out,
              incomplete, unparsed, deduped, created_at, completed_at)
```

* `FOR UPDATE SKIP LOCKED` (`db.rs:fetch_pending`, `fetch_pending_for_dispatch`) allows horizontal replicas; inline mode uses `processing` leases, dispatch mode uses `dispatched` leases.
* Lease reclaim: `processing` rows with `locked_at < now()-10m` → `pending` (`db.rs:reclaim_stale`); `dispatched` rows with `dispatched_at < now()-30m` → `pending` (`db.rs:reclaim_stale_dispatched`, configurable via `DISPATCH_RECLAIM_SECS`). Both run every loop iteration.
* `nomad_job_id` records the Nomad `DispatchedJobID`/`EvalID` for debugging; it is cleared on retry/dead_letter.

### 3a. Inline worker pool (default, `ENABLE_DISPATCH=false`)

Semaphore `MAX_INFLIGHT` (default 4, env `MAX_INFLIGHT` / `--max-inflight`). Each slot:

- marks row `processing` (`locked_at/by`, `attempts++`),
- downloads single object via `S3Storage::download_to_path` (3 attempts, 1 s→4 s backoff; scratch dir `SCRATCH_DIR` or tempdir),
- `spawn_blocking` decode: `decode_ais_file` or `decode_aisstream_file` (`decode.rs`) → `FileStats` + `IcebergBatches` (`Vec<RecordBatch>` ×6),
- commits to Iceberg REST (`open_catalog`, `ensure_namespace`, `ensure_table` for `positions/statics/meteo/binary/atons/other` with `day` partition spec → `collect_core::iceberg::commit_batches` → `fast_append`; empty batches skipped),
- on success `archive_success` (`DELETE`+`INSERT` tx); on error `mark_failed` with exponential backoff `5s*2^attempts` capped 1 h + jitter, or `dead_letter` after `max_attempts`.

Note: per-file decode means multi-part NMEA fragments split across files will be `Incomplete`/`Failed` rather than reassembled — surfaced as `incomplete`/`unparsed` in `parse_history`.

### 3b. Nomad dispatch (`ENABLE_DISPATCH=true`)

Dispatcher (`crates/collect-orchestrator/src/dispatcher.rs`) replaces the inline pool:

- Poll loop: `reclaim_stale` + `reclaim_stale_dispatched`, then `fetch_pending_for_dispatch` (`UPDATE ... SET status='dispatched', dispatched_at=now(), attempts++ WHERE s3_key = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *`).
- Parallel dispatch: `Semaphore(DISPATCH_CONCURRENCY)` (default 32) bounds concurrent `POST $NOMAD_ADDR/v1/job/parse-file/dispatch` calls (`dispatcher.rs:dispatch_one`). `Meta {s3_bucket, s3_key, source, parser}` per Nomad parameterized job spec (`nomad/parse-file.nomad.hcl`). `X-Nomad-Token` from `NOMAD_TOKEN` if set.
- On dispatch success: `mark_dispatched(s3_key, nomad_job_id)`; on dispatch transport failure: `mark_failed` with `nomad dispatch failed: ...` so backoff applies.
- Worker (`crates/parse-file-worker`) runs on any Nomad client (new nodes auto-receive work):
  1. Reads `NOMAD_META_*` (or `S3_BUCKET/S3_KEY` env), connects via `S3Storage::new`, downloads with 3 retries to `SCRATCH_DIR`.
  2. `spawn_blocking` same `decode_ais_file`/`decode_aisstream_file` as inline; commits via shared `collect_core::iceberg::commit_batches`.
  3. Callbacks to orchestrator: `POST $CALLBACK_URL/complete {s3_bucket, s3_key, duration_ms, stats{rows_in,positions_out,...}}` with `Authorization: Bearer $CALLBACK_TOKEN` (falls back to `INGEST_TOKEN`). On decode/commit failure, `POST .../fail {s3_key, error}`.
- Orchestrator `POST /complete` → idempotent `archive_success` (checks `parse_history` first); `POST /fail` → `mark_failed` with backoff. Both clear `dispatched_at/nomad_job_id` on failure via `db.rs:mark_failed`.

Single file per Nomad job. Dispatch overhead ~1–3 s; suitable because file decode+commit dominates.

### 4. Backfill (`--backfill`)

Lists all `.parquet` keys under `--input-s3-prefix` via `S3Storage::list_keys_with_prefix` and enqueues missing rows. Run once after deploy and/or as periodic Nomad batch to catch missed webhooks. Works with both modes (backfill only enqueues; dispatcher or inline pool drains).

## CLI reference

```
collect-orchestrator [--listen-addr 0.0.0.0:8080] [--database-url $DATABASE_URL]
  [--max-inflight 4] [--batch-size 8192] [--compression-level 5]
  [--scratch-dir /tmp] [--source-map ./source_map.toml] [--ingest-token xxx] [--callback-token yyy]
  [--enable-dispatch] [--nomad-addr http://nomad.service.consul:4646] [--nomad-token zzz] [--nomad-job parse-file]
  [--dispatch-concurrency 32] [--dispatch-reclaim-secs 1800]
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
| `--max-inflight` | `MAX_INFLIGHT` | `4` | Inline bounded parallel parses (ignored when `ENABLE_DISPATCH=true`) |
| `--batch-size` | `BATCH_SIZE` | `8192` | Parquet read batch rows (passed to worker via env) |
| `--compression-level` | `COMPRESSION_LEVEL` | `5` | Zstd level for Iceberg data files |
| `--scratch-dir` | `SCRATCH_DIR` | system tmp | S3 download dir (`/dev/shm` for tmpfs) |
| `--source-map` | `SOURCE_MAP` | infer | `source → parser` overrides (`source_map.toml` `[source_map]` table) |
| `--ingest-token` | `INGEST_TOKEN` | — | Bearer token for `POST /ingest` (also fallback for `/complete|/fail`) |
| `--callback-token` | `CALLBACK_TOKEN` | `INGEST_TOKEN` | Bearer token for `POST /complete|/fail` (worker → orchestrator) |
| `--enable-dispatch` | `ENABLE_DISPATCH` | `false` | Enable Nomad dispatch mode |
| `--nomad-addr` | `NOMAD_ADDR` | `http://nomad.service.consul:4646` | Nomad HTTP API address |
| `--nomad-token` | `NOMAD_TOKEN` | — | Nomad ACL token (`X-Nomad-Token`) |
| `--nomad-job` | `NOMAD_JOB` | `parse-file` | Parameterized batch job name to dispatch |
| `--dispatch-concurrency` | `DISPATCH_CONCURRENCY` | `32` | Max concurrent Nomad dispatch RPCs |
| `--dispatch-reclaim-secs` | `DISPATCH_RECLAIM_SECS` | `1800` | Reclaim `dispatched` rows after this many seconds (30 min) |
| `--input-s3-bucket` | `INPUT_S3_BUCKET` | `collections` | Bronze bucket to download from |
| `--input-s3-prefix` | `INPUT_S3_PREFIX` | `""` | Key prefix under bucket (`bronze`) |
| `--s3-endpoint/region/access-key/secret-key/disable-tls` | `S3_*` | — | Shared S3 connection (RustFS/MinIO/AWS) |
| `--iceberg-catalog-uri` | `ICEBERG_CATALOG_URI` | — | REST catalog URI |
| `--iceberg-warehouse` | `ICEBERG_WAREHOUSE` | — | Warehouse (`s3://warehouse`) |
| `--iceberg-namespace` | `ICEBERG_NAMESPACE` | `ais` | Namespace (tables shared with batch parsers) |
| `--iceberg-token` | `ICEBERG_TOKEN` | — | Lakekeeper bearer token |
| `--backfill` | — | — | List S3 and enqueue then exit |
| `--config` | `CONFIG_FILE` | — | Flat TOML defaults (same precedence as other binaries) |

All six Iceberg tables are written (`positions`, `statics`, `meteo`, `binary`, `atons`, `other`; `h3`/`hilbert` `u64→i64` as in batch parsers). S3 storage for Iceberg is via REST warehouse + env `S3_*` / `S3_PATH_STYLE`, etc. (see `AIS_PARSE.md#iceberg-output`). Dispatch workers inherit the same Iceberg config via Consul-templated `ICEBERG_CATALOG_URI`.

## HTTP API

- `POST /ingest` — enqueue (auth via `INGEST_TOKEN` if set). Returns `{ "accepted": N, "duplicates": M }`. Example:

  ```bash
  curl -H "Authorization: Bearer $INGEST_TOKEN" -H "Content-Type: application/json" \
    -d '{"s3_bucket":"collections","s3_key":"bronze/source=norway/year=2026/month=07/day=15/part-abc.parquet"}' \
    http://localhost:8080/ingest
  # S3 event passthrough:
  curl -H "Authorization: Bearer $INGEST_TOKEN" -d @s3-event.json http://localhost:8080/ingest
  ```

- `POST /complete` — worker callback on success (auth via `CALLBACK_TOKEN` or `INGEST_TOKEN`). Body `{ "s3_key": "...", "s3_bucket": "...", "duration_ms": 1234, "stats": { "rows_in":..., "positions_out":..., "statics_out":..., "meteo_out":..., "binary_out":..., "atons_out":..., "other_out":..., "incomplete":..., "unparsed":..., "deduped":... } }`. Idempotent (checks `parse_history` first). Returns `{ "status": "archived" | "already_complete" }`.

- `POST /fail` — worker callback on error (same auth). Body `{ "s3_key": "...", "error": "..." }`. Applies `mark_failed` backoff or `dead_letter`.

- `GET /healthz` — `200` if Postgres reachable, else `503`.
- `GET /metrics` — Prometheus `orchestrator_queue_depth{status}` (includes `dispatched`).
- `GET /queue?status=pending&limit=100` — list queued rows (for debugging/alerts; `dead_letter` and `dispatched` signal attention).

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

Local `docker-compose.yml` runs `collect-orchestrator` on `8080` dependent on `db`/`lakekeeper` healthy; swap the `minio` service for `rustfs` and keep the `collect-orchestrator` `S3_ENDPOINT=http://rustfs:9000`. `Nomad` job `nomad/collect-orchestrator-prod` is a `service` with `check http /healthz` and webhook comments. Set `ENABLE_DISPATCH=true` there to switch to Nomad scheduling.

## Nomad dispatch mode (detail)

* **Job:** `nomad/parse-file.nomad.hcl` — `type = "batch"`, `parameterized { payload="forbidden" meta_required=["s3_bucket","s3_key","source","parser"] }`. Single `group "parse" count=1` task `exec` running `local/parse-file-worker` (artifact `http://192.168.99.107:9000/binaries/parse-file-worker`). Dispatch via `POST /v1/job/parse-file/dispatch {"Meta":{...}}`.
* **Scaling:** New Nomad clients automatically receive `parse-file` allocs; `DISPATCH_CONCURRENCY` controls orchestrator-side parallelism, Nomad handles bin-packing.
* **Secrets:** `S3_ACCESS_KEY/SECRET_KEY/CALLBACK_TOKEN` via `template { env=true }` from `nomadVar "nomad/jobs/parse-file"` — populate with `nomad var put nomad/jobs/parse-file S3_ACCESS_KEY=... S3_SECRET_KEY=... CALLBACK_TOKEN=...` or your consolidated `~/code/nomad/vars/prod.json`. `CALLBACK_URL` and `ICEBERG_CATALOG_URI` are Consul-templated (`secrets/consul.env`).
* **One-time setup:** `nomad job run nomad/parse-file.nomad.hcl` (registers the parameterized job; no allocs until dispatched).
* **Rollback:** Set `ENABLE_DISPATCH=false` (job default) and redeploy `collect-orchestrator-prod`; `dispatched` rows will be reclaimed after `DISPATCH_RECLAIM_SECS` and processed inline.

## Failure semantics

- **Retry:** `next_retry_at = now() + 5s*2^attempts + jitter` (capped 1 h). `failed` → retry after `next_retry_at`. `dead_letter` after 5 attempts (manual `GET /queue?status=dead_letter` + alert; re-enqueue via updating `status=pending`). Dispatch transport failures are recorded via `POST /fail` / `mark_failed` same path.
- **Crash safety (inline):** lease reclaim (`UPDATE parse_queue SET status='pending' WHERE locked_at < now()-10m`) on every worker loop.
- **Crash safety (dispatch):** lease reclaim (`UPDATE parse_queue SET status='pending' WHERE dispatched_at < now()-30m`) on every dispatcher loop + dispatch loop also reclaims old `processing` rows. Worker crash before callback leaves row `dispatched` until reclaimed; callback after reclaim is idempotent via `parse_history` check.
- **Idempotency:** idempotent `ON CONFLICT (s3_key) DO NOTHING` enqueue + per-table `fast_append` (pure append). `POST /complete` checks `parse_history` first. Per-file archiving makes a restarted `backfill` a no-op for already-archived keys. A crash between the six per-table commits of one file can leave a partial commit (same narrow window as batch parsers' `CommitManifest` note `AIS_PARSE.md#idempotent-re-runs-with-iceberg`). Worker exits 1 on callback failure so Nomad can surface the failed alloc; orchestrator's backoff still governs retry (Nomad `restart { attempts=0 }` to avoid double retry).

## Performance

- **Inline:** `MAX_INFLIGHT=4` by default. Each slot holds one S3 download + one blocking decode + six Iceberg commits. Tune with `download_concurrency` equivalent already inside `S3Storage`; orchestrator serializes per-file Iceberg catalog calls.
- **Dispatch:** `DISPATCH_CONCURRENCY=32` by default bounds Nomad RPCs, not parses — actual parse parallelism equals Nomad cluster capacity (one alloc per file × client count). Each worker alloc `cpu 500 / memory 1024` as in `parse-file.nomad.hcl`. Tune `DISPATCH_RECLAIM_SECS` lower if workers are short-lived and you want faster retry after client loss.

## Backfill & repair

```bash
# One-shot backfill (lists bucket prefix, enqueues missing .parquet keys):
DATABASE_URL=postgresql://postgres:postgres@db:5432/postgres \
  collect-orchestrator --input-s3-bucket collections --input-s3-prefix bronze \
    --s3-endpoint http://rustfs:9000 --s3-access-key ... --s3-secret-key ... --s3-disable-tls \
    --iceberg-catalog-uri http://lakekeeper:8181/catalog --iceberg-warehouse s3://warehouse \
    --backfill
# Then either mode drains:
#  inline: MAX_INFLIGHT workers pull
#  dispatch: dispatcher dispatches; workers callback
```

Schedule also as periodic Nomad batch `collect-orchestrator-backfill` to heal missed webhook deliveries.
