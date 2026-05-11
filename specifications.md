# collect Specifications

## Purpose

`collect` is a Rust workspace for ingesting newline-delimited text into Hive-partitioned Parquet datasets and for maintaining those datasets afterward.

It has four user-facing binaries:

- `collect-file`: ingest files and directories
- `collect-socket`: ingest newline-delimited TCP streams
- `collect-maint`: inspect, validate, compact, and vacuum datasets
- `ais-normalize`: re-timestamp, re-partition, and combine multi-part AIS sentences in collected Parquet data

## Workspace Goals

- Ingest data into Parquet with Zstd compression.
- Partition output by `source` plus a chosen time granularity.
- Support local filesystem output and S3-compatible object storage.
- Support interactive setup via TUI and non-interactive CLI/environment usage.
- Make long-running work observable with progress reporting.
- Allow graceful cancellation and clean shutdown.
- Keep maintenance operations safe by default, with dry-run modes where appropriate.

## Shared Concepts

### Partitioning

Output is organized as Hive-style directories:

`source=<label>/year=YYYY/month=MM/day=DD/hour=HH/minute=MM`

Supported granularities:

- `minute`
- `hour`
- `day`
- `month`
- `year`

Default partition granularity is `day`.

### Parquet Output

- Output format is Parquet.
- Compression is Zstd.
- Parquet files are written atomically through temp files then renamed into place.
- Output file names are timestamp/pid based.

### Storage Targets

- Local filesystem output.
- S3 / S3-compatible output with optional TLS disabled.
- S3 uploads run in the background so ingestion does not block on network I/O.

### Health Checks

- Each ingest binary maintains a health file in `/tmp`.
- Health check mode exits early and reports service health.

## collect-file

### Purpose

Recursively ingest files from a path into partitioned Parquet. Rows are timestamped using the source file's modification time.

### Supported Inputs

- Plain text files
- gzip files
- bzip2 files
- zip archives

### Input Rules

- Files and directories are recursively discovered.
- Hidden dotfiles and hidden archive entries are skipped silently.
- Plain files that look tar-like are rejected when they are actually archive-like names without supported content.
- zip entries are processed entry-by-entry in archive order.
- unreadable files are skipped with a warning instead of aborting the run.

### CLI

Required / common flags:

- `--input <path>`: file or directory to ingest
- `--source <label>`: logical source name
- `--concurrency <n>`: override the auto-selected file worker count
- `--tui`: launch the configuration TUI
- `--noui`: disable runtime status TUI and print plain aggregate updates

Inherited ingest options:

- `--out-dir`
- `--partition`
- `--max-rows`
- `--max-batch-bytes`
- `--compression-level`
- `--upload-drain-timeout-seconds`
- `--max-line-length`
- `--health-check`
- S3 options: `--s3-bucket`, `--s3-endpoint`, `--s3-region`, `--s3-access-key`, `--s3-secret-key`, `--keep-local`, `--s3-disable-tls`

### Defaults and Precedence

Precedence order:

1. CLI flags
2. environment variables
3. defaults

If `--input` is omitted, `INPUT_PATH` or `INPUT_FILE` is used.
If `--source` is omitted, `SOURCE` is used.

### Runtime Status

- When stdout is a TTY, a full-screen runtime status view is used automatically.
- `--noui` forces plain text mode.
- Plain mode prints aggregate updates every 10 items.
- TUI mode shows:
  - processed / remaining files
  - aggregate stats
  - current clock and elapsed time
- `Ctrl-C`, `q`, and `Esc` cancel cleanly.

### Progress and Restartability

- File completion is tracked in a completion manifest under the output root.
- Already completed files are skipped on startup.
- File-level errors are recoverable; bad files are skipped and ingestion continues.

### Ingestion Behavior

- Inputs are scanned in parallel.
- File scheduling favors many small files with a bounded worker pool.
- Long lines are bounded by `max-line-length`.
- Writes are decoupled from parsing so ingestion stays responsive under load.
- Health status is updated during ingest.
- All rows use the file modification time as their timestamp. AIS-specific timestamp extraction is handled as a post-processing step by `ais-normalize`.

## collect-socket

### Purpose

Connect to a TCP endpoint, read newline-delimited text, and ingest it into partitioned Parquet.

### CLI

- `--tcp-host <host>`
- `--tcp-port <port>`
- `--source <label>`
- `--tui`: launch configuration TUI

Inherited ingest options are the same as `collect-file`, except there is no runtime `--noui` mode in the current behavior.

### TCP Behavior

- Connects to `host:port`.
- Uses `TCP_NODELAY`.
- On disconnect or codec error, it reconnects with exponential backoff.
- Backoff starts at 1 second and caps at 5 seconds.
- Reconnect attempts stop when shutdown is requested.

### Runtime Status

- Uses the shared ingest progress reporting from `collect-core`.
- Emits periodic progress while ingesting.

## collect-maint

### Purpose

Inspect and repair Hive-partitioned datasets.

### CLI

Global flags:

- `--root <path>` for local datasets
- `--s3-bucket <bucket>` for S3 datasets
- `--s3-prefix <prefix>`
- `--s3-endpoint <url>`
- `--s3-region <region>`
- `--s3-access-key <key>`
- `--s3-secret-key <secret>`
- `--s3-disable-tls`
- `--partition <granularity>`
- `--concurrency <n>`; if omitted, `collect-maint` auto-selects a worker count
- `--noui`: disable runtime status TUI and print plain updates every 10 items
- `--compression-level <level>`

Subcommands:

- `inspect [--verbose]`
- `validate`
- `compact [--target-file-size-bytes N] [--apply]`
- `vacuum [--apply]`

### Dataset Model

`collect-maint` works on leaf partitions where data is actually stored.

Leaf partitions contain Parquet files under the selected partition granularity layout.

### inspect

- Summarizes partition counts, file counts, sizes, and time range.
- Reports recommendations for compact, vacuum, and validate.
- `--verbose` prints per-partition details.
- Non-compactable or non-data files are classified and counted.

### validate

- Reads parquet files and checks partition/timestamp consistency.
- Reports per-file validation issues.
- Fails the command if issues exist.

### compact

- Dry-run by default.
- With `--apply`, compacts small Parquet files within leaf partitions.
- Default compacted output target size is about 512 MiB.
- Only leaf partitions with more than one Parquet file are candidates.
- Compaction groups files by partition, plans output files, writes manifests, materializes inputs, writes compacted Parquet, validates the result, publishes output, and deletes old inputs/manifests.
- Partition writes are isolated so one partition's compaction does not interfere with another.

### vacuum

- Dry-run by default.
- With `--apply`, removes temporary files and interrupted compaction manifests.
- Also cleans up stale maintenance artifacts.

### Runtime Status

- When stdout is a TTY, a full-screen runtime status view is used automatically.
- `--noui` forces plain text mode.
- Plain mode prints aggregate updates every 10 items.
- TUI mode shows:
  - processed / remaining leaf partitions
  - aggregate stats
  - current clock and elapsed time
- `Ctrl-C`, `q`, and `Esc` cancel cleanly.

### Maintenance Progress Semantics

- `processed` means leaf partitions already consolidated to a single Parquet file at startup, plus partitions consolidated during the current run.
- `remaining` means leaf partitions still needing consolidation.
- Progress is updated as partition compactions finish, not per job.

## ais-normalize

### Purpose

Post-process a Hive-partitioned Parquet dataset collected from AIS NMEA feeds. Corrects row timestamps from embedded NMEA metadata, moves rows to the correct partition, and combines multi-part AIS sentences into single records.

### When to Use

Run `ais-normalize` after `collect-file` or `collect-socket` has ingested raw AIS data. The ingest binaries stamp rows with the file modification time or wall clock; `ais-normalize` replaces those with the precise capture timestamps embedded in the payloads.

### What It Does

**Re-timestamp:** For each row, extract the correct capture time from:

- NMEA tag block `c:<epoch>` fields
- `$PGHP` capture timestamp lines (Gatehouse format: `$PGHP,1,YYYY,M,D,H,M,S,ms,...`)
- Carry-forward: a `$PGHP` or tagged first fragment's timestamp propagates to subsequent unfragmented sentences and fragment parts that lack their own `c:` field

If no NMEA timestamp is found, the original `ts` value is kept unchanged.

**Re-partition:** After re-timestamping, the correct partition key is recomputed. Rows whose timestamp moves them to a different partition are written to the new location.

**Combine multi-part sentences:** Multi-sentence AIS messages (e.g. type 5 static/voyage data using two sentences, type 24 class B using two parts) are reassembled into a single output record:

- The 6-bit ASCII payload fields from all parts are concatenated.
- Fill bits are taken from the final fragment.
- The NMEA sentence counter is reset to `1,1` (single sentence).
- The NMEA checksum is recalculated.
- Any tag block with a `c:` timestamp is preserved on the combined sentence.
- The combined record uses the timestamp of the first fragment.

Incomplete groups (fragments missing their partner at end of scan) are emitted as individual raw sentences and counted in stats.

### CLI

- `--input-dir <path>`: source Hive-partitioned Parquet root
- `--output-dir <path>`: destination root (may equal `--input-dir`)
- `--partition <granularity>`: must match the dataset layout (default: `day`)
- `--source <label>`: filter to one source label; all sources if omitted
- `--apply`: write output; dry-run by default
- `--batch-size <n>`: rows per Parquet read batch (default: 8192)
- `--compression-level <n>`: Zstd level for output (default: 5)
- `--noui`: disable TUI and print plain progress updates

### Defaults and Precedence

Dry-run by default; `--apply` is required to write any output.

### Processing Behavior

- Partitions are processed in chronological order.
- Each partition's fragment buffer is independent; cross-partition fragment groups are flushed as-is at the end of each partition.
- `$PGHP` rows are re-timestamped and re-partitioned using their own embedded time; they are preserved in the output.
- Output files use fresh names (`norm-*.parquet`) and do not overwrite input files, so `--output-dir == --input-dir` is safe. Run `collect-maint compact --apply` afterward to consolidate.

### Runtime Status

- Prints a dry-run summary of affected partitions when `--apply` is not set.
- Prints a stats summary on completion: input rows, output rows, re-timestamped, re-partitioned, combined messages, incomplete groups, partitions processed.
- `--noui` prints plain partition progress updates every 10 partitions.
- Cancellable with `Ctrl-C`.

## collect-core

### Purpose

Shared ingest engine and common CLI types.

### Responsibilities

- Parse common ingest CLI arguments.
- Apply environment variable overrides.
- Build ingest options for shared ingestion.
- Normalize partition granularity and defaults.
- Define the `LineSource` trait.
- Run the ingest loop.
- Manage health updates.
- Manage background writes/uploads.
- Support cooperative shutdown from signal handlers and runtime monitors.
- Optionally suppress write/upload chatter for UI-driven runs.

### Ingest Pipeline

The engine consumes a `LineSource` that yields newline-delimited payloads and:

- timestamps rows
- buckets rows into partition keys
- buffers rows into batches
- flushes batches to Parquet
- optionally uploads or keeps local files
- respects shutdown signals

### Shared CLI Defaults

- Output directory: `data`
- Partition: `day`
- Max batch bytes: 64 MiB
- Compression level: `5`
- Upload drain timeout: 60 seconds
- Max line length: 65,536 bytes
- S3 region: `us-east-1`

## collect-tui

### Purpose

Shared interactive setup framework used by `collect-file` and `collect-socket`.

### Responsibilities

- Render a terminal UI in alternate screen mode.
- Support editing fields, toggling booleans, saving/loading JSON config, and launching the configured command.
- Use serde JSON config files.
- Provide reusable `TuiModel` implementations for binaries.

## Environment Variables

Common ingest variables:

- `OUT_DIR`
- `PARTITION`
- `MAX_ROWS`
- `MAX_BATCH_BYTES`
- `COMPRESSION_LEVEL`
- `UPLOAD_DRAIN_TIMEOUT_SECONDS`
- `MAX_LINE_LENGTH`
- `HEALTH_CHECK`

File ingestion:

- `INPUT_PATH`
- `INPUT_FILE`
- `SOURCE`

Socket ingestion:

- `TCP_HOST`
- `TCP_PORT`

S3 configuration:

- `S3_BUCKET`
- `S3_ENDPOINT`
- `S3_REGION`
- `S3_ACCESS_KEY`
- `AWS_ACCESS_KEY_ID`
- `S3_SECRET_KEY`
- `AWS_SECRET_ACCESS_KEY`
- `KEEP_LOCAL`
- `S3_DISABLE_TLS`

## Output Layout

Example:

```text
data/
  source=<source>/
    year=YYYY/
      month=MM/
        day=DD/
          hour=HH/
            minute=MM/
              part.parquet
```

The exact depth depends on partition granularity.

## Behavioral Requirements

- Input errors must be recoverable when possible.
- Terminal UI must not break non-interactive use.
- Cancellation must restore the terminal.
- Plain mode must remain log-friendly.
- Maintenance `compact` and `vacuum` must default to dry-run.
- `ais-normalize` must default to dry-run.
- Shared defaults should prefer `day` partitioning.

## Rebuild Guidance

If recreating this from scratch:

1. Build shared CLI and ingest primitives first.
2. Implement Parquet writing, partitioning, and S3 upload behavior.
3. Add `collect-file` ingestion and manifest-based restartability.
4. Add `collect-socket` reconnecting TCP ingestion.
5. Add `collect-maint` inspect/validate/compact/vacuum flows.
6. Add `ais-normalize` post-processing for AIS datasets.
7. Add TUI/plain runtime status layers and cancellation.
8. Add tests for input decoding, manifest restart, partition planning, maintenance flows, and AIS normalization.
