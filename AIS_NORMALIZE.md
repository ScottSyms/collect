# ais-normalize

`ais-normalize` is a batch post-processing pass over a Hive-partitioned Parquet dataset already collected by `collect-file`, `collect-socket`, `collect-kafka`, or `collect-aisstream`. It sits between raw ingestion and compaction in the medallion pipeline:

```
collect-* (bronze, raw)  →  ais-normalize  →  collect-maint compact  →  Iceberg
```

It is a CLI tool that runs to completion and exits — not a long-running service. It has no `--metrics-addr`/`/healthz` endpoint.

## What problem it solves

Raw AIS feeds carry two artifacts that make the bronze layer awkward to query directly:

1. **Fragmented sentences.** Long AIS messages are split by the feed into multiple NMEA sentences (`!AIVDM,2,1,...` / `!AIVDM,2,2,...`). Each fragment lands in the raw dataset as its own row, arriving as its own separate ingest event — sometimes straddling a partition boundary.
2. **Inaccurate timestamps.** The row's timestamp at ingestion is wall-clock arrival time at the collector, not when the AIS transmission actually occurred. Some feeds carry a more accurate time separately: an NMEA tag block (`\c:<epoch>*hh\`) prefixed to the sentence, or a `$PGHP` capture-time sentence that precedes a run of untagged sentences.

`ais-normalize` makes one pass over a dataset and does three things per row:

- **Fragment reassembly** — combines all fragments of a multi-part `!AIVDM`/`!AIVDO` sentence into one sentence with a recomputed NMEA checksum, once every fragment has arrived.
- **Re-timestamping** — replaces the ingest timestamp with the most accurate one available, in priority order: tag-block `c:` epoch (highest) → carried-forward `$PGHP` capture time → original ingest timestamp (unchanged, lowest).
- **Re-partitioning** — since re-timestamping can move a row's true time across a partition boundary, every row's Hive partition is recomputed from its corrected timestamp, even when that differs from the partition it was originally ingested into.

Rows that aren't AIS `VDM`/`VDO` sentences (heartbeats, other NMEA talkers, etc.) pass straight through with their original timestamp and partition.

## How it works

**Dataset scan.** The input directory is walked once; each Parquet file's path is parsed against the `--partition` granularity to recover its `(source, year, month, day, [hour], [minute])` key. `tmp-`-prefixed files (in-progress writer temp files) are skipped. Files that don't match the expected path depth are silently skipped — see [Troubleshooting](#troubleshooting).

**Partition grouping.** Files are grouped by their *input* partition key and sorted chronologically. Each group is handed to a worker as one independent unit of work.

**Concurrency.** A pool of `--concurrency` workers (default: available cores, capped at 8) pulls partitions off a shared queue. Each partition gets its own `PartitionProcessor` (fragment buffer + carry-forward timestamp state) and its own `OutputWriterPool` (one Arrow/Parquet writer per *output* partition the input rows get redistributed into). State never crosses partition boundaries, so partitions can run fully in parallel and memory scales with `concurrency × output partitions touched by one input partition` — not with total dataset size.

**Per-row logic:**

- A `$PGHP` sentence is parsed for its embedded date/time, which becomes the carry-forward timestamp applied to subsequent untagged sentences until the next `$PGHP` or tag-block `c:` value.
- A tag-block `c:<epoch>` value, when present, takes precedence over the carry-forward timestamp for that sentence.
- Multi-part sentences are grouped by a key derived from the sentence's own fields — type, fragment count, sequence ID, channel — not by the tag block's `g:` field, which isn't present on every feed. The `g:` field is parsed but intentionally unused for grouping.
- Buffered fragment groups are capped at 8192 concurrent groups per partition; if a new group would exceed the cap, the oldest incomplete group is evicted and its fragments are emitted individually (counted as an "incomplete group") rather than buffered indefinitely. Malformed or truncated fragment sequences can never grow memory without bound.
- Any fragment groups still incomplete at the end of a partition's files are flushed the same way.

**Output.** Rows are written with the same `(ts: Timestamp(ms, UTC), payload: Utf8)` schema as ingestion, Zstd-compressed, with row groups sized and sorted by timestamp on every flush — this is what lets `collect-maint compact` stream-merge the output later without a full re-sort.

## CLI reference

| Flag | Default | Description |
|------|---------|-------------|
| `--input-dir` | *(one of `--input-dir`/`--input-s3-bucket` required)* | Source Hive-partitioned Parquet root on local disk |
| `--output-dir` | *(one of `--output-dir`/`--output-s3-bucket` required)* | Destination root on local disk (see [output directory](#output-directory-non-destructive) note) |
| `--input-s3-bucket` | *(none)* | Read the input dataset from this S3 bucket instead of `--input-dir` |
| `--input-s3-prefix` | *(empty)* | Key prefix within the input bucket; acts as the dataset root, env `INPUT_S3_PREFIX` |
| `--output-s3-bucket` | *(none)* | Write normalized output to this S3 bucket instead of `--output-dir` |
| `--output-s3-prefix` | *(empty)* | Key prefix within the output bucket; acts as the dataset root, env `OUTPUT_S3_PREFIX` |
| `--s3-endpoint` | AWS default | S3 endpoint URL, shared by both buckets (for MinIO/other S3-compatible storage), env `S3_ENDPOINT` |
| `--s3-region` | `us-east-1` | S3 region, shared by both buckets, env `S3_REGION` |
| `--s3-access-key` | *(none)* | S3 access key, shared by both buckets, env `S3_ACCESS_KEY` or `AWS_ACCESS_KEY_ID` |
| `--s3-secret-key` | *(none)* | S3 secret key, shared by both buckets, env `S3_SECRET_KEY` or `AWS_SECRET_ACCESS_KEY` |
| `--s3-disable-tls` | off | Use plain HTTP instead of HTTPS for the S3 endpoint, env `S3_DISABLE_TLS` |
| `--partition` | `day` | Partition granularity; must match the input dataset's on-disk layout |
| `--source` | *(all sources)* | Restrict to one `source=` label |
| `--apply` | off (dry run) | Actually write output; omit to preview only |
| `--batch-size` | `8192` | Rows per Parquet read batch |
| `--compression-level` | `5` | Zstd level for output files |
| `--concurrency` | cores, clamped `[1, 8]` | Partitions processed concurrently; also bounds S3 download concurrency for `--input-s3-bucket` |
| `--noui` | off | Disable the TTY status display; print plain progress lines instead (auto-enabled when stdout isn't a terminal) |

`--input-dir`/`--input-s3-bucket` are mutually exclusive (exactly one required), and likewise for `--output-dir`/`--output-s3-bucket`. `--input-s3-bucket` and `--output-s3-bucket` always share one endpoint/region/credentials — only the bucket name (and optional prefix) differs between them, matching how the same S3-compatible store (e.g. one MinIO deployment) typically hosts both the raw and normalized datasets side by side as separate buckets.

## S3 input and output

Either side (or both) can be backed by S3 instead of local disk:

```bash
# Both input and output on the same MinIO endpoint, different buckets
cargo run -p ais-normalize -- \
  --input-s3-bucket raw-ais --output-s3-bucket normalized-ais \
  --s3-endpoint http://minio:9000 --s3-region us-east-1 \
  --s3-access-key minioadmin --s3-secret-key minioadmin --s3-disable-tls \
  --partition day --apply

# Local input, S3 output
cargo run -p ais-normalize -- \
  --input-dir data --output-s3-bucket normalized-ais --output-s3-prefix ais \
  --s3-endpoint http://minio:9000 --s3-access-key minioadmin --s3-secret-key minioadmin --s3-disable-tls \
  --partition day --apply
```

When either side is S3, the corresponding files are staged through a local scratch directory (under the OS temp dir) rather than read/written directly:

- **S3 input**: every matched object is listed, then downloaded to the scratch directory up front (concurrency bounded by `--concurrency`) before partition processing starts. Peak local disk usage is roughly the total size of the matched input objects.
- **S3 output**: each partition writes its normalized Parquet file to the scratch directory exactly as it would to a local `--output-dir`, then uploads it to `output_s3_prefix/<rel_dir>/<file>` and deletes the local copy — with up to 3 attempts (exponential backoff) per file. **If an upload ultimately fails, the scratch directory is deliberately not cleaned up** — its path is printed so you can inspect or manually upload the file — rather than silently discarding already-normalized output. On a fully successful run the scratch directory is removed automatically.

## Usage

```bash
# Preview what would happen (no files written)
cargo run -p ais-normalize -- --input-dir data --output-dir normalized --partition day

# Apply, writing normalized output to a separate directory
cargo run -p ais-normalize -- --input-dir data --output-dir normalized --partition day --apply

# Restrict to one source, tune concurrency and compression
cargo run -p ais-normalize -- \
  --input-dir data --output-dir normalized --partition day \
  --source norway-tcp --concurrency 4 --compression-level 3 --apply
```

Ctrl-C / SIGTERM requests a clean stop: in-flight partitions finish their currently-open output files (properly closed and renamed, no leftover `tmp-` files) and processing of further files/partitions stops; already-completed partitions are unaffected.

### Summary output

At the end of a run, a plain-text summary is printed to stderr:

| Field | Meaning |
|-------|---------|
| `partitions processed` | Input partitions scanned |
| `input rows` | Total rows read |
| `output rows` | Total rows written (fragments combine into fewer rows; non-AIS rows pass through 1:1) |
| `re-timestamped` | Rows whose output timestamp differs from their ingest timestamp |
| `re-partitioned` | Rows written to a different partition than the one they were read from |
| `combined messages` | Multi-part sentences successfully reassembled into one sentence |
| `incomplete groups` | Fragment groups that never completed (evicted by the buffer cap or left over at end-of-partition) and were emitted as raw, uncombined fragments |

A high `incomplete groups` count usually means fragments of the same message are landing in different source partitions or being dropped upstream — check the feed and the ingest `--partition` granularity relative to message arrival spacing.

## Output directory (non-destructive)

`ais-normalize` never modifies or deletes the input dataset — it only reads it and writes new files. This applies equally to `--output-dir` and `--output-s3-bucket`:

- **The output target may equal the input target** (same directory, or same bucket/prefix), but if it does, the original raw files remain in place alongside the new normalized files in the same partitions. Re-running the tool will also re-scan and re-normalize any previously-written normalized output sitting under that same root (harmless, since already-normalized rows have nothing left to reassemble or re-timestamp, but wasteful).
- **Recommended pattern:** write to a separate output directory or bucket, verify the result (row counts, spot-check with `collect-maint inspect`), then swap the downstream read path (or move/delete the raw data) once satisfied. Keep the raw bronze data until you're confident in the normalized output — it's the only copy of the original ingest-time data.

## Deployment

This is a batch job, not a service — there's no health endpoint and nothing to keep running. Run it as a scheduled task after each ingestion window, ahead of compaction.

### Nomad periodic batch job

```hcl
job "ais-normalize" {
  datacenters = ["dc1"]
  type        = "batch"

  periodic {
    cron             = "0 */6 * * * *"   # every 6 hours
    prohibit_overlap = true
  }

  group "normalize" {
    task "ais-normalize" {
      driver = "exec"

      config {
        command = "/usr/local/bin/ais-normalize"
        args = [
          "--input-dir", "/data/bronze",
          "--output-dir", "/data/normalized",
          "--partition", "day",
          "--concurrency", "4",
          "--apply",
        ]
      }

      resources {
        cpu    = 1000
        memory = 2048
      }
    }
  }
}
```

`prohibit_overlap` matters: two concurrent runs over the same dataset would race on partition assignment and file naming. Size `memory` for `--concurrency × (number of output partitions a single input partition's rows fan out into) × ~ a few MiB per open writer buffer` — a few hundred MiB is enough for typical AIS retimestamping fan-out; raise it if a single run touches many output partitions per input partition.

With `--input-s3-bucket`/`--output-s3-bucket`, no host volume is needed at all — swap the `args` block for:

```hcl
args = [
  "--input-s3-bucket", "raw-ais",
  "--output-s3-bucket", "normalized-ais",
  "--s3-endpoint", "http://minio.service.consul:9000",
  "--partition", "day",
  "--concurrency", "4",
  "--apply",
]
env {
  S3_ACCESS_KEY = "..."
  S3_SECRET_KEY = "..."
}
```

Size local disk for the S3 scratch space too (under the task's `$TMPDIR`): input needs room for the total size of matched input objects, output needs room for one run's worth of normalized files until each uploads.

### Cron / systemd timer alternative

```bash
# crontab -e
0 */6 * * * /usr/local/bin/ais-normalize --input-dir /data/bronze --output-dir /data/normalized --partition day --apply --noui >> /var/log/ais-normalize.log 2>&1
```

`--noui` is automatic when stdout isn't a TTY (cron, systemd), so it's optional here but explicit is fine.

## Troubleshooting

- **"No Parquet files found"** or fewer files processed than expected — the most common cause is `--partition` not matching the granularity the data was actually ingested with. `PartitionKey` parsing expects an exact number of `key=value` path segments for the given granularity (e.g. `day` expects `source=X/year=Y/month=M/day=D/`); files whose path doesn't match that depth are silently skipped rather than raising an error. Check the ingest job's `--partition`/`PARTITION` setting.
- **High `incomplete groups`** — see the [summary output](#summary-output) note above.
- **Output directory filling up with both raw and normalized files** — see [Output directory](#output-directory-non-destructive).
- **Run failed partway through an S3 upload** — the run exits non-zero and prints the scratch directory path; the already-normalized Parquet file(s) are left there rather than deleted, since they're not yet durable anywhere else. Either upload manually with the printed path and key layout, or just re-run once the underlying S3 issue (network, credentials, bucket) is fixed — the same input regenerates equivalent output, and you can delete the stale scratch directory afterward.
