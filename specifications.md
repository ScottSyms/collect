# collect Specifications

## Purpose

`collect` is a Rust workspace for ingesting newline-delimited text into Hive-partitioned Parquet datasets and for maintaining those datasets afterward.

It has three user-facing binaries:

- `collect-file`: ingest files and directories
- `collect-socket`: ingest newline-delimited TCP streams
- `collect-aisstream`: ingest aisstream.io WebSocket AIS data

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

- `--input-dir <path>`: file or directory to ingest
- `--source <label>`: logical source name
- `--concurrency <n>`: override the auto-selected file worker count
- `--noui`: disable runtime status TUI and print plain aggregate updates

Inherited ingest options:

- `--output-dir`
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

If `--input-dir` is omitted, `INPUT_PATH` or `INPUT_FILE` is used.
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
- All rows use the file modification time as their timestamp. AIS-specific timestamp extraction is handled during ingest via `--process-timestamps` or as a batch step via ais-parse's own `--process-timestamps` flag.

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
- On-ingest AIS consolidation (`--consolidate-ais`) must support multipart reassembly and $PGHP timestamp processing independently.
- Shared defaults should prefer `day` partitioning.

## Rebuild Guidance

If recreating this from scratch:

1. Build shared CLI and ingest primitives first.
2. Implement Parquet writing, partitioning, and S3 upload behavior.
3. Add `collect-file` ingestion and manifest-based restartability.
4. Add `collect-socket` reconnecting TCP ingestion.
5. Add `collect-maint` inspect/validate/compact/vacuum flows.
6. Add on-ingest AIS consolidation (`--consolidate-ais`, `--process-timestamps`) for multipart reassembly and timestamp correction.
7. Add TUI/plain runtime status layers and cancellation.
8. Add tests for input decoding, manifest restart, partition planning, maintenance flows, and AIS normalization.
