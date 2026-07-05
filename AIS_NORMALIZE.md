# ais-normalize

`ais-normalize` is a batch post-processing pass over a Hive-partitioned Parquet dataset already collected by `collect-file`, `collect-socket`, `collect-kafka`, or `collect-aisstream`. It sits between raw ingestion and compaction in the medallion pipeline:

```
collect-* (bronze, raw)  →  collect-maint compact  →  ais-normalize  →  ais-parse (silver)  →  Iceberg
```

It is a CLI tool that runs to completion and exits — not a long-running service. It has no `--metrics-addr`/`/healthz` endpoint. The next step of the pipeline — decoding the normalized sentences into typed columns — is [ais-parse](AIS_PARSE.md).

> **Compaction placement.** `collect-maint compact`/`validate` currently expect the bronze two-column `(ts, payload)` schema, so run compaction on the **raw** dataset (before normalize), as shown above. The normalized output carries an extra `source` column; compacting it needs the schema-general compaction update (tracked separately).

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

**Dataset scan.** Each input (one or more directories or buckets — see [merging multiple sources](#merging-multiple-sources)) is walked once; each Parquet file's path is parsed against the `--partition` granularity to recover its `(source, year, month, day, [hour], [minute])` key. `tmp-`-prefixed files (in-progress writer temp files) are skipped. Files that don't match the expected path depth are silently skipped — see [Troubleshooting](#troubleshooting).

**Partition grouping.** Files from all inputs are pooled, sorted by *input* partition key, and grouped by that key (so the same partition contributed by two inputs becomes one work item). Each group is handed to a worker as one independent unit of work.

**Concurrency.** A pool of `--concurrency` workers (default: available cores, capped at 8) pulls partitions off a shared queue. Each partition gets its own `PartitionProcessor` (fragment buffer + carry-forward timestamp state) and its own `OutputWriterPool` (one Arrow/Parquet writer per *output* partition the input rows get redistributed into). State never crosses partition boundaries, so partitions can run fully in parallel and memory scales with `concurrency × open output writers per partition` — not with total dataset size. Parallelism is per input partition; a dataset stored at finer granularity (hour/minute) exposes more parallelism than one giant day partition.

**Per-row logic:**

- A `$PGHP` sentence is parsed for its embedded date/time, which becomes the carry-forward timestamp applied to subsequent untagged sentences until the next `$PGHP` or tag-block `c:` value.
- A tag-block `c:<epoch>` value, when present, takes precedence over the carry-forward timestamp for that sentence.
- Multi-part sentences are grouped by a key derived from the sentence's own fields — type, fragment count, sequence ID, channel — not by the tag block's `g:` field, which isn't present on every feed and is intentionally ignored.
- Buffered fragment groups are capped at 8192 concurrent groups per partition; if a new group would exceed the cap, the oldest incomplete group is evicted and its fragments are emitted individually (counted as an "incomplete group") rather than buffered indefinitely. Malformed or truncated fragment sequences can never grow memory without bound.
- Any fragment groups still incomplete at the end of a partition's files are flushed the same way.

**Output.** Rows are written with the schema `(ts: Timestamp(ms, UTC), source: Utf8, payload: Utf8)` — the same `ts`/`payload` as ingestion plus a `source` column carrying the input's `source=` label (the output itself is not partitioned by source; see [merging multiple sources](#merging-multiple-sources)). Files are Zstd-compressed, with row groups sized and sorted by timestamp on every flush. The `source` value is constant within a file, so it dictionary-encodes to almost nothing. Open writers per pool are hard-capped at 64: if pathologically scattered timestamps fan an input partition out further than that, all open files are closed and affected partitions simply get an additional file, keeping worst-case memory bounded even on hostile data.

## CLI reference

| Flag | Default | Description |
|------|---------|-------------|
| `--input-dir` | *(one of `--input-dir`/`--input-s3-bucket` required)* | Source Hive-partitioned Parquet root on local disk. **Repeatable** — pass it several times to merge multiple sources in one run (see [merging multiple sources](#merging-multiple-sources)) |
| `--output-dir` | *(one of `--output-dir`/`--output-s3-bucket` required)* | Destination root on local disk (see [output directory](#output-directory-non-destructive) note) |
| `--input-s3-bucket` | *(none)* | Read the input dataset from this S3 bucket instead of `--input-dir`. **Repeatable**; comma-separated `INPUT_S3_BUCKET` env also works |
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
| `--year` | *(all years)* | Process only this year's partitions (see [processing a slice](#processing-a-slice-of-the-archive)) |
| `--month` | *(all months)* | Narrow to one month; requires `--year` |
| `--day` | *(all days)* | Narrow to one day; requires `--month` |
| `--hour` | *(all hours)* | Narrow to one hour; requires `--day` and an `hour`-or-finer layout |
| `--minute` | *(all minutes)* | Narrow to one minute; requires `--hour` and a `minute` layout |
| `--since <HOURS>` | *(off)* | Process only partitions holding data from the last N hours (rolling window from now, UTC); env `SINCE_HOURS`. Mutually exclusive with `--year`…`--minute`. See [rolling windows](#rolling-windows---since) |
| `--incremental` | *(off)* | Track a watermark at the output target and process only partitions holding files that arrived since the last successful run; env `INCREMENTAL=true`. Self-healing; preferred for scheduled runs. See [watermark runs](#watermark-runs---incremental) |
| `--apply` | off (dry run) | Actually write output; omit to preview only |
| `--batch-size` | `8192` | Rows per Parquet read batch |
| `--compression-level` | `5` | Zstd level for output files |
| `--concurrency` | cores, clamped `[1, 8]` | Partitions processed concurrently |
| `--dedup` | `true` | Merge touched output partitions and drop exact `(ts, source, payload)` duplicates so re-runs are idempotent; `--dedup false` (env `DEDUP=false`) appends instead. See [idempotent re-runs](#idempotent-re-runs-deduplication) |
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

- **S3 input**: matching objects are listed once up front, but downloaded **per partition, by the worker about to process that partition**, and the partition's scratch copies are deleted as soon as it finishes. Scratch disk usage is therefore bounded by roughly `--concurrency` partitions' worth of data at any moment — not the whole dataset — so a multi-terabyte bucket can be normalized on a machine with modest local disk.
- **S3 output**: each partition writes its normalized Parquet file(s) to the scratch directory exactly as it would to a local `--output-dir`, then uploads each to `output_s3_prefix/<rel_dir>/<file>` and deletes the local copy — with up to 3 attempts (exponential backoff) per file. **If an upload ultimately fails, the scratch directory is deliberately not cleaned up** — its path is printed so you can inspect or manually upload the file — rather than silently discarding already-normalized output. On a fully successful run the scratch directory is removed automatically.

## Processing a slice of the archive

`--year`/`--month`/`--day`/`--hour`/`--minute` select a single partition subtree instead of the whole dataset — e.g. re-normalize one day after an upstream feed problem, or work through a large archive in monthly chunks:

```bash
# One month
cargo run -p ais-normalize -- --input-dir data --output-dir normalized \
  --partition day --year 2026 --month 6 --apply

# One day of one source, straight from S3
cargo run -p ais-normalize -- --input-s3-bucket raw-ais --output-s3-bucket normalized-ais \
  --s3-endpoint http://minio:9000 --source norway-tcp \
  --partition day --year 2026 --month 7 --day 3 --apply
```

Rules and behavior:

- Components are hierarchical: `--month` requires `--year`, `--day` requires `--month`, and so on — a bare `--month 6` (June of *every* year) is rejected as almost certainly unintended.
- A component finer than the dataset layout (e.g. `--hour` on a `--partition day` dataset) is rejected.
- The selection is a *partition* filter, applied to where rows were **read from** — a row inside a selected partition whose corrected timestamp moves it elsewhere is still written to its correct output partition.
- The filter is pushed down as far as possible: on local disk the directory walk prunes non-matching subtrees without descending into them, and on S3 — when `--source` is also given — it is folded into the LIST prefix (`source=X/year=Y/month=M/...`), so slicing one day out of a multi-year bucket lists only that day's keys. Without `--source`, S3 listing covers the whole prefix and filters client-side (the tool prints a hint when this happens).

## Watermark runs (`--incremental`)

`--incremental` is the preferred mode for scheduled runs. The tool keeps a **watermark** — a small JSON document at the output target (`<output-dir>/_ais-normalize/watermark.json`, or the same path as a key under the output S3 prefix) recording the newest input-file modification time it has fully processed. Each run then selects only partitions holding at least one file **modified since the watermark**, and advances the watermark once the run fully succeeds:

```bash
# Hourly cron: process exactly what arrived since the last successful run
cargo run -p ais-normalize -- \
  --input-s3-bucket raw-ais --output-s3-bucket normalized-ais \
  --s3-endpoint http://minio:9000 --s3-access-key … --s3-secret-key … --s3-disable-tls \
  --partition day --incremental --apply

# Env form
INCREMENTAL=true INPUT_S3_BUCKET=raw-ais OUTPUT_S3_BUCKET=normalized-ais … \
  cargo run -p ais-normalize -- --partition day --apply
```

Why this beats a fixed `--since` window for schedulers:

- **Self-healing.** The watermark only advances on a fully successful, applied run. If the host is down for a weekend or three runs in a row fail, the next successful run automatically covers the whole gap — there is no window to outgrow.
- **Catches late arrivals.** Selection is by file modification time (S3 `LastModified` / local mtime), not partition period, so a file uploaded late into an *old* partition (delayed collector flush, orphan sweep) still re-triggers that partition.

Rules and behavior:

- **First run** (no state yet) processes the full dataset. To bound it on a huge archive, add `--since N`: with `--incremental`, `--since` only seeds the first run's cutoff (`now − N hours`, applied to file mtimes) and is ignored once state exists.
- **The watermark lives at the output target** — a different output dir/bucket+prefix is a different job with its own state. All inputs feeding one output share one watermark.
- **A 60-second overlap lap** is applied when comparing mtimes, absorbing `LastModified` jitter around the previous run's listing. Files within the lap re-process; dedup makes that free. (Corollary: an idle feed keeps re-selecting its newest partition — harmless.)
- **Dry runs, failures, and cancels never advance the watermark**, and it never moves backwards.
- Mutually exclusive with `--year`…`--minute`. Combines normally with `--source` and multi-input merging.
- The state directory is metadata, not data: `collect-maint` skips `_`- and `.`-prefixed path segments (the Hive/Spark convention), and Parquet dataset readers ignore non-`.parquet` files.

## Rolling windows (`--since`)

`--since <HOURS>` selects a **rolling** window instead of a fixed calendar slice: it processes every partition whose time period extends past `now − N hours`. It is stateless — nothing is written anywhere — which makes it right for ad-hoc re-processing ("redo the last day") and for schedulers where you'd rather not have state; for scheduled pipelines prefer [`--incremental`](#watermark-runs---incremental), which self-heals across downtime:

```bash
# Every run: normalize whatever arrived in the last 2 hours
cargo run -p ais-normalize -- \
  --input-s3-bucket raw-ais --output-s3-bucket normalized-ais \
  --s3-endpoint http://minio:9000 --s3-access-key … --s3-secret-key … --s3-disable-tls \
  --partition day --since 2 --apply

# Env form (containers / cron)
SINCE_HOURS=2 INPUT_S3_BUCKET=raw-ais OUTPUT_S3_BUCKET=normalized-ais … \
  cargo run -p ais-normalize -- --partition day --apply
```

Rules and behavior:

- **The window is `[now − N hours, now]` in UTC**, evaluated once at startup. A partition matches when its period *end* is after the cutoff, so the partition currently receiving data (and any that ended inside the window) is included.
- **Pick N with a margin over the run interval.** Running hourly with `--since 2` (rather than `--since 1`) overlaps successive runs by an hour so a late-arriving or slow run never leaves a gap. The overlap is free: dedup makes re-processing a partition idempotent, so the doubly-covered hour just collapses back to the same rows.
- **Granularity interacts with the window.** On a `--partition day` layout the smallest unit is a day, so `--since 2` reprocesses the whole current day (and yesterday near midnight) every run — correct, but heavier. For lighter incremental runs use an `hour`-granularity dataset, where `--since 2` touches only the last two or three hour-partitions.
- **Mutually exclusive with `--year`…`--minute`** (a fixed slice and a rolling window can't both apply). `--source` and multi-input merging still combine with it normally.
- **S3 listing note.** `--since` doesn't set a fixed `year=/month=` chain, so it can't be folded into the S3 LIST prefix the way a fixed slice with `--source` can; listing covers the source's prefix and the window is applied client-side (whole-year subtrees before the cutoff year are still pruned). For very large buckets, pair it with `--source` to at least scope the LIST to one feed.

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

## Merging multiple sources

`--input-dir` and `--input-s3-bucket` are both repeatable, so several source datasets can be normalized into one output in a single run (instead of invoking the tool once per feed):

```bash
# Two S3 buckets (one feed each) → one normalized bucket
cargo run -p ais-normalize -- \
  --input-s3-bucket norway --input-s3-bucket aisstream \
  --output-s3-bucket normalized-ais \
  --s3-endpoint http://minio:9000 --s3-access-key … --s3-secret-key … --s3-disable-tls \
  --partition day --apply

# Comma-separated env form
INPUT_S3_BUCKET=norway,aisstream OUTPUT_S3_BUCKET=normalized-ais … cargo run -p ais-normalize -- --partition day --apply

# Two local dirs
cargo run -p ais-normalize -- --input-dir /data/norway --input-dir /data/aisstream --output-dir /data/normalized --partition day --apply
```

Rules and behavior:

- **Output is not partitioned by source, but source is retained as a column.** All inputs are merged into one dataset partitioned by time only (`year=…/month=…/day=…`, no `source=` segment); each row's origin is preserved in the `source` column instead. Rows from `norway` and `aisstream` for the same day land in the same output partition, each tagged with its own `source`.
- **All inputs must be the same kind.** Either several `--input-dir` **or** several `--input-s3-bucket`, not a mix. Every input bucket shares one endpoint/region/credentials and one `--input-s3-prefix` (keep it empty for bucket-root datasets).
- **Overlap is handled.** Exact `(ts, source, payload)` duplicates — from re-runs or two snapshots of one feed — are collapsed by the dedup merge. Because `source` is part of the key, a message that two *different* sources reported byte-identically is kept once per source (provenance intact).
- **`--source` and the partition-slice filters** apply to every input (they still select on the input's `source=`/time layout).

## Idempotent re-runs (deduplication)

By default (`--dedup true`), after the run finishes ais-normalize merges every output partition it wrote to and removes exact `(ts, source, payload)` duplicates. Because normalize is deterministic — the true timestamp comes from the `\c:` tag block or `$PGHP`, and the combined payload is fixed — re-running over the same (or overlapping) input regenerates byte-identical rows, which the merge collapses. **Running the tool any number of times converges each partition to the same deduped set**, so you can safely re-run a day after fixing an upstream feed, or re-process overlapping ranges, without accumulating duplicates.

- **Duplicate = exact `(ts, source, payload)`.** Two rows are duplicates only if the timestamp, the `source`, *and* the full payload all match. Rows that differ in any way are all kept — including the `\s:<station>` receiver tag, and the `source` column, so the same AIS message received by two base stations (or arriving from two sources) is preserved, not collapsed.
- **Why this instead of "overwrite".** Normalize re-partitions rows (a row read from `day=01` can land in `day=02`), so a "clear the partition and rewrite" approach could delete a neighbor partition's real data when you run with `--day`/`--month` filters. Dedup-merge keeps the union of rows and removes only redundant ones, so spillover and overlapping selections merge safely.
- **Scope.** Only partitions this run actually wrote to are merged; untouched partitions are never read or rewritten. A partition with a single fresh file and no prior data is left as-is (first runs stay cheap); the merge kicks in once a partition has 2+ files (a prior run's output, or a generation rollover).
- **Cost.** When a partition is merged it is fully read and rewritten (≈ a compaction pass over that partition; on S3, its prior objects are downloaded, a single merged object is uploaded, and the old objects deleted). This is the price of idempotency — pass `--dedup false` (or `DEDUP=false`) to skip it and append instead (the pre-dedup behavior, where re-runs accumulate files).
- **Only under `--apply`.** Dry runs never merge; they note that dedup will run on apply.
- **`output == input` caveat.** With dedup on and the output target equal to the input target, raw and normalized rows share a partition but have different payloads, so they will **not** dedup against each other and the partition ends up holding both — a warning is printed. Use a separate output dir/bucket.

The end-of-run summary reports `partitions merged`, `rows in`, `rows out`, and `duplicates removed`.

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
    cron             = "0 * * * * *"     # every hour
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
          "--incremental",     # watermark at the output: process only what arrived since the last successful run
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

The `--incremental` above keeps each scheduled run cheap — it only normalizes partitions with files that arrived since the last successful run — and self-heals: after downtime or failed runs, the next success covers the whole gap automatically (see [watermark runs](#watermark-runs---incremental)). The first run processes the full archive; add `--since N` alongside it to bound that first pass. Drop `--incremental` for a one-shot full backfill.

Size local disk for the S3 scratch space too (under the task's `$TMPDIR`): input needs room for about `--concurrency` partitions' worth of downloaded data at a time (partitions are fetched lazily and deleted after processing), and output needs room for the in-flight normalized files until each uploads. For a one-off pass over a very large archive, pair this with the [partition slice flags](#processing-a-slice-of-the-archive) to process one month or day per scheduled run.

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
- **"Too many open files"** during the dedup step of a partition with a large accumulation of files — this is handled: the merge is bounded-fan-in (it never opens more than a fixed number of files at once, spilling to intermediate files in rounds), and the process also raises its soft open-file limit toward the hard limit at startup. If you still hit it in an unusually constrained environment, raise the descriptor limit (`ulimit -n`, or the orchestrator's equivalent).
