# collect-file — File data collector

Recursively ingests plain text, gzip, bzip2, and zip files into
Hive-partitioned Parquet with Zstd compression. Supports AIS multi-part
message consolidation and `$PGHP` timestamp processing.

New to the project? [TUTORIAL.md](TUTORIAL.md) walks through local storage,
partitioning, S3, and Iceberg in order; this page is a reference for this
one binary.

## Pipeline

```
Input files (txt/gz/bz2/zip) → collect-file → Bronze Parquet
                                                (ts, payload, source)
```

## Usage

```bash
# Ingest a single file
cargo run -p collect-file -- \
  --input data.txt \
  --source my-source

# Ingest a directory (recursive)
cargo run -p collect-file -- \
  --input /path/to/data/ \
  --source maritime

# With S3 output
cargo run -p collect-file -- \
  --input data.txt \
  --source ais \
  --s3-bucket bronze --s3-prefix ais \
  --s3-endpoint http://minio:9000 \
  --s3-access-key minio --s3-secret-key minioadmin --s3-disable-tls

# With AIS processing
cargo run -p collect-file -- \
  --input data.txt \
  --consolidate-ais --process-timestamps
```

## CLI Reference

### Input

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--input` | `INPUT_PATH` | — | Input file or directory (recursive) |
| `--source` / `-s` | `SOURCE` | file stem/dir name | Logical source label |

### Processing

| Flag | Default | Description |
|------|---------|-------------|
| `--concurrency` (env `CONCURRENCY`) | auto (2..32) | Max concurrent file workers |
| `--noui` | off | Disable TUI status display |
| `--quiet` / `-q` (env `QUIET`) | off | Suppress routine progress lines; warnings/errors still print |
| `--completions <shell>` | — | Print shell completions to stdout and exit |
| `--version` | — | Prints `<crate version> (<git commit hash>)` |
| `--config <file>` (env `CONFIG_FILE`) | — | Load flag defaults from a flat TOML file; CLI flags and pre-set env vars still win — see [CLI_REFERENCE.md](CLI_REFERENCE.md#shared-across-all-six-binaries) |

Exits [`NOTHING_TO_DO`](CLI_REFERENCE.md#exit-codes) (`2`, instead of `0`) when there were no unfinished input files to ingest — distinct from a hard error (`1`).

### AIS Processing

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--consolidate-ais` | `CONSOLIDATE_AIS` | off | Reassemble multi-part NMEA fragments into single sentences |
| `--process-timestamps` | `PROCESS_TIMESTAMPS` | off | Extract `$PGHP` and tag-block `c:` timestamps |

### Common + S3 + Iceberg

Full flag/env tables: [CLI_REFERENCE.md](CLI_REFERENCE.md#common-to-the-four-collectors-commoncliargs)
(`CommonCliArgs` — includes `--health-check`, `--metrics-addr`,
`--data-drought-seconds`), [S3](CLI_REFERENCE.md#s3--collectors-s3cliargs-one-sink)
(`S3CliArgs`), and [Iceberg](CLI_REFERENCE.md#iceberg-icebergcliargs-all-six-binaries)
(`IcebergCliArgs` — optional, direct `raw`-table registration on
successful upload), plus the same `--parser` inline-parsing flag
([details](CLI_REFERENCE.md#inline-parsing-collectors-only-parsercliargs)).
No reconnect flags — `collect-file` has no live upstream connection to
lose. In parallel mode the Iceberg catalog connection and table handle are
established once in `main()` and shared (cloned) across all workers, the
same way S3 storage is shared — and likewise the silver handler is one
shared `Arc`, with per-batch decode state kept on each worker's stack.

## Output

- **Schema:** `ts` (timestamp ms UTC), `payload` (utf8), `source` (utf8)
- **Partition:** Hive-style under `--output-dir`
- **Compression:** Zstd
- **Completion manifest:** Tracks processed files so re-runs skip finished work

## File support

| Extension | Format |
|-----------|--------|
| `.txt` | Plain text (default) |
| `.gz` | Gzip-compressed |
| `.bz2` | Bzip2-compressed |
| `.zip` | ZIP archive (recursively extracted) |

## Parallel Mode

When multiple input files are discovered, `collect-file` automatically runs
in parallel with auto-scaled worker count. Each worker processes one file at
a time. The completion manifest prevents re-processing already-finished files
on re-runs.
