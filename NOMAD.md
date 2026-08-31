# Nomad Usage

This project includes Nomad job definitions under [nomad/](nomad/) for orchestrating `collect-socket` and other binaries.

## Production pipeline (`nomad/nomad-*-prod`)

The `nomad-*-prod` files are the actual running deployment — static job specs (no `-var` templating), all pointed at one MinIO endpoint (`http://192.168.99.107:9000`) and one Nomad cluster spanning the `duncan` and `dc1` datacenters. This section describes what runs, in what order, and what has to exist before it will work. (The rest of this document, below, covers the separate `collect-socket.nomad` *template* job — a generic starting point, not what's actually deployed.)

### Job run order

```
1. postgres-prod                        (service — Lakekeeper's metadata store)
2. nomad-lakekeeper-prod                (service — Iceberg REST catalog; needs #1)
   → then: register the "ais" warehouse with Lakekeeper — see below, one-time, manual
3. nomad-tcp-prod                       (service — fans out the upstream AIS feed on :7001)
4. nomad-collect-norway-prod            (service — TCP, source=norway; needs #3)
   nomad-collect-duplicate-prod         (service — TCP, source=duplicate, redundant copy; needs #3)
   nomad-collect-aisstream-prod         (service — connects to aisstream.io directly, no dependency on #3)
5. nomad-ais-normalize-prod             (batch/periodic, every minute; needs #4's norway+duplicate to have written something)
6. nomad-ais-parse-prod                 (batch/periodic, every minute; needs #5's output)
   nomad-aisstream-parse-prod           (batch/periodic, every minute; needs #4's aisstream collector)
7. (one-time) historical Iceberg backfill — see AIS_PARSE.md's bulk-backfill guidance; run this
   BEFORE step 8, or with step 8's jobs disabled, to avoid both processes racing to commit the
   same partitions (see the --since 2 note in nomad-ais-parse-iceberg-prod)
8. nomad-ais-parse-iceberg-prod         (batch/periodic, hourly; needs #6/#7 and the Iceberg warehouse from #2)
   nomad-aisstream-parse-iceberg-prod   (batch/periodic, hourly; needs #6/#7 and the Iceberg warehouse from #2)
9. collect-orchestrator (+ parse-file)  (service + parameterized batch — event-driven Iceberg pipeline, alternative/supplement to step 8)
   → 9a. collect-orchestrator-prod      (service — webhook → Postgres queue, needs #1 and #2)
        one-time: nomad job run nomad/parse-file.nomad.hcl  (register parameterized batch job, no allocs yet)
   → 9b. per-file dispatched allocs     (batch — one parse-file per bronze object, auto-scales with Nomad clients)
```

`nomad-victoria-metrics-prod` (Prometheus-compatible metrics store, Consul service discovery) can run any time after Consul is up — nothing else depends on it.

Steps 5-6 (the flat-Parquet silver pipeline, writing to `collections/ais`) and step 8 (the Iceberg periodic pipeline) are independent consumers of the same normalized/collected input — both can run indefinitely side by side; neither depends on the other. Step 9 (the orchestrator event-driven pipeline) is a per-file alternative to step 8 that writes the same six Iceberg tables (`positions`, `statics`, `meteo`, `binary`, `atons`, `other` in namespace `ais`); run it alongside or instead of the hourly periodic Iceberg jobs, but avoid running both orchestrator and periodic Iceberg jobs over the same partitions at the same time without deduplication (see `AIS_PARSE.md#idempotent-re-runs` and `ORCHESTRATOR.md#failure-semantics`).

### S3 buckets needed

Everything here lives under one MinIO bucket, **`collections`**, addressed by prefix (the tools split `bucket/prefix` syntax automatically — see `--s3-bucket`/`--input-s3-bucket` docs). Nothing needs to be created by hand: each tool's `S3Storage::ensure_bucket` creates the `collections` bucket on first use if it's missing.

| Prefix | Written by | Read by |
|--------|-----------|---------|
| `collections/norway` | `nomad-collect-norway-prod` | `nomad-ais-normalize-prod` |
| `collections/duplicate` | `nomad-collect-duplicate-prod` | `nomad-ais-normalize-prod` |
| `collections/aisstream` | `nomad-collect-aisstream-prod` | `nomad-aisstream-parse-prod`, `nomad-aisstream-parse-iceberg-prod` |
| `collections/norway-norm` | `nomad-ais-normalize-prod` | `nomad-ais-parse-prod`, `nomad-ais-parse-iceberg-prod` |
| `collections/ais` | `nomad-ais-parse-prod`, `nomad-aisstream-parse-prod` | (flat-Parquet silver output — external consumers) |
| `collections/ais-iceberg-state` | `nomad-ais-parse-iceberg-prod`, `nomad-aisstream-parse-iceberg-prod` | same two jobs, next run (watermark + [Iceberg commit manifest](AIS_PARSE.md#idempotent-re-runs-with-iceberg) — metadata only, no row data) |

### Iceberg / Lakekeeper setup needed

Unlike the buckets above, **the Iceberg warehouse is not auto-created** — `ensure_namespace`/`ensure_table` (what `ais-parse`/`aisstream-parse` call) only create the namespace and tables *inside* an already-registered warehouse; registering the warehouse itself is a Lakekeeper management operation, done once, by hand:

1. **A storage bucket for the warehouse.** Pick a bucket (e.g. `iceberg-warehouse`) separate from `collections` — Iceberg manages its own Parquet data files and manifests here, not through any flag in this workspace.
2. **Register the warehouse with Lakekeeper**, naming it to match `--iceberg-warehouse ais` (used by both `nomad-ais-parse-iceberg-prod` and `nomad-aisstream-parse-iceberg-prod`) and pointing its storage profile at that bucket/credentials:

   ```bash
   curl -X POST http://<lakekeeper-address>:<port>/management/v1/warehouse \
     -H 'Content-Type: application/json' \
     -d '{
       "warehouse-name": "ais",
       "storage-profile": {
         "type": "s3",
         "bucket": "iceberg-warehouse",
         "endpoint": "http://192.168.99.107:9000",
         "region": "us-east-1",
         "path-style-access": true
       },
       "storage-credential": {
         "type": "s3",
         "credential-type": "access-key",
         "aws-access-key-id": "<access-key>",
         "aws-secret-access-key": "<secret-key>"
       }
     }'
   ```

   Field names vary by Lakekeeper version — `nomad-lakekeeper-prod` sets `LAKEKEEPER__SERVE_SWAGGER_UI = "true"`, so check the running instance's Swagger UI (`http://<lakekeeper-address>:<port>/swagger-ui`) for the exact current schema instead of trusting this verbatim.
3. **Namespace and tables are then auto-created** on first `ais-parse`/`aisstream-parse` Iceberg-mode run — `--iceberg-namespace ais` creates the `ais` namespace, and all six tables (`positions`, `statics`, `meteo`, `binary`, `atons`, `other`) are created with their schemas and partition specs the first time each is written to. Nothing further to set up.

## Orchestrator Nomad dispatch (new)

`collect-orchestrator` can run inline (default, `MAX_INFLIGHT` semaphore) or scatter work via Nomad (`ENABLE_DISPATCH=true`). In dispatch mode every bronze file becomes one `parse-file` parameterized batch alloc that runs `parse-file-worker`, commits to Iceberg, and callbacks to `POST /complete`.

**Register once (no allocs until dispatched):**

```bash
nomad job run nomad/parse-file.nomad.hcl
```

**Vars (Nomad Variables, consolidated via `~/code/nomad/vars/prod.json`):**

```bash
nomad var put nomad/jobs/parse-file S3_ACCESS_KEY=... S3_SECRET_KEY=... CALLBACK_TOKEN=...
# orchestrator uses same token (falls back to INGEST_TOKEN):
# in nomad/collect-orchestrator-prod env: CALLBACK_TOKEN / INGEST_TOKEN, NOMAD_ADDR, NOMAD_TOKEN, NOMAD_JOB=parse-file
```

The job uses `artifact http://192.168.99.107:9000/binaries/parse-file-worker` + Consul templates for `CALLBACK_URL` (`collect-orchestrator` service) and `ICEBERG_CATALOG_URI` (`lakekeeper` service). Secrets (`S3_ACCESS_KEY/SECRET_KEY/CALLBACK_TOKEN`) come from `nomadVar "nomad/jobs/parse-file"` (`secrets/vars.env`). See `nomad/parse-file.nomad.hcl:32` and `ORCHESTRATOR.md#nomad-dispatch-mode-detail` for `DISPATCH_CONCURRENCY` (default 32) and `DISPATCH_RECLAIM_SECS` (default 1800). New Nomad clients automatically receive new `parse-file` allocs.

**Toggle:** `nomad/collect-orchestrator-prod` defaults `ENABLE_DISPATCH=false` (inline). Set `ENABLE_DISPATCH=true` and redeploy to use dispatch; set back to `false` to roll back inline (reclaim converts `dispatched` rows after `DISPATCH_RECLAIM_SECS`).

**Queue visibility:** `GET /queue?status=dispatched|pending|failed|dead_letter` and `GET /metrics` (`orchestrator_queue_depth{status="dispatched"}`). See `ORCHESTRATOR.md` for `POST /complete` / `POST /fail` payloads and failure semantics.

## Prerequisites

- Nomad cluster (1.4+ recommended for Nomad Variables)
- `collect-socket` binary deployed to all Nomad client nodes at `/usr/local/bin/collect-socket`
- Output directory (`/data`) writable by the Nomad task user on client nodes

## Quick Start

```bash
nomad job run nomad/collect-socket.nomad
```

This connects to the Norway TCP feed at `153.44.253.27:5631` with source label `norway-tcp` and writes Hive-partitioned Parquet files to `/data`.

## Configuration

### Job Variables

The job file uses Nomad variables with sensible defaults for the Norway feed. Override any of them at submit time with `-var`:

| Variable | Default | Description |
|----------|---------|-------------|
| `tcp_host` | `153.44.253.27` | TCP host address |
| `tcp_port` | `5631` | TCP port number |
| `source` | `norway-tcp` | Logical source label |
| `rust_log` | `INFO` | Log level |
| `max_rows` | `10000` | Max rows per Parquet file |
| `keep_local` | `false` | Keep local files after S3 upload |
| `s3_bucket` | _(empty)_ | S3 bucket name |
| `s3_region` | _(empty)_ | S3 region |
| `s3_endpoint` | _(empty)_ | S3 endpoint URL |
| `s3_access_key` | _(empty)_ | S3 access key |
| `s3_secret_key` | _(empty)_ | S3 secret key |
| `s3_disable_tls` | `false` | Disable TLS for S3 (use HTTP) |

### Example with S3

```bash
nomad job run \
  -var s3_bucket=maritime-data \
  -var s3_region=us-west-2 \
  -var s3_disable_tls=true \
  nomad/collect-socket.nomad
```

## Secret Management

Avoid putting S3 keys in plain-text `-var` flags or the job file. Use **Nomad Variables** to store secrets securely:

```bash
nomad var put nomad/jobs/collect-socket/s3 \
  access_key=AKIAIOSFODNN7EXAMPLE \
  secret_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Then add a `template` stanza to the job to inject them at runtime:

```hcl
template {
  data        = <<EOH
S3_ACCESS_KEY={{ with nomadVar "nomad/jobs/collect-socket/s3" }}{{ .access_key }}{{ end }}
S3_SECRET_KEY={{ with nomadVar "nomad/jobs/collect-socket/s3" }}{{ .secret_key }}{{ end }}
EOH
  destination = "local/secrets/env"
  env         = true
}
```

## Metrics & Health Endpoint

The job assigns a dynamic `metrics` port and sets `METRICS_ADDR` so the collector serves:

- `GET /metrics` — Prometheus metrics (rows ingested, batches written, upload successes/failures/retries, orphan files swept, heartbeat)
- `GET /healthz` — `200` while the ingest loop heartbeat is fresh, `503` once it goes stale (60s window)

The service is registered with a `prometheus` tag, so a Prometheus server using Consul service discovery can scrape it with a `consul_sd_configs` job matching that tag.

## Graceful Shutdown

The job sets `kill_timeout = "90s"`, which must stay **above** `UPLOAD_DRAIN_TIMEOUT_SECONDS` (default 60s). On stop, the collector flushes its in-memory batch, finishes queued Parquet writes, and drains pending S3 uploads before exiting; a shorter kill_timeout would SIGKILL it mid-drain. Files that still miss the window are picked up by the startup orphan sweep on the next allocation and uploaded then.

## Health Checks & Restart Behaviour

Nomad polls the HTTP health check every 30 seconds:

```
GET http://<alloc>:<metrics-port>/healthz
```

This detects hung ingest loops, not just dead processes — the endpoint goes `503` when the loop stops heartbeating. (The file-based `collect-socket --health-check` script check remains available for setups without a network namespace.)

There are **two independent restart mechanisms**:

| Trigger | Mechanism | Limit |
|---------|-----------|-------|
| **Task crash** (process exits) | `restart` block | 10 attempts per 5 min, 15s delay |
| **Hung / unhealthy task** (process alive but check fails) | `check_restart` on the health check | 3 consecutive failures before restart |

This means the task is resilient to both hard crashes and silent hangs.

## Resource Tuning

Default resource limits in the job file:

- **CPU:** 500 MHz
- **Memory:** 1024 MB

Adjust by editing the `resources` block in `nomad/collect-socket.nomad` to match your workload:

```hcl
resources {
  cpu    = 1000
  memory = 2048
}
```

See the [performance tuning](#performance-tuning) section in the main README for guidance on `MAX_ROWS`, `MAX_BATCH_BYTES`, and compression settings.
