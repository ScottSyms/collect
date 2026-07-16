# collect-file — File data collector

Recursively ingests plain text, gzip, bzip2, and zip files into
Hive-partitioned Parquet with Zstd compression. Supports AIS multi-part
message consolidation and `$PGHP` timestamp processing.

## Pipeline

```
Input files (txt/gz/bz2/zip) → collect-file → Bronze Parquet
                                                (ts, payload, source)
```

## Usage

```bash
# Ingest a single file
cargo run -p collect-file -- \
  --input-dir data.txt \
  --source my-source

# Ingest a directory (recursive)
cargo run -p collect-file -- \
  --input-dir /path/to/data/ \
  --source maritime

# With S3 output
cargo run -p collect-file -- \
  --input-dir data.txt \
  --source ais \
  --s3-bucket bronze --s3-prefix ais \
  --s3-endpoint http://minio:9000 \
  --s3-access-key minio --s3-secret-key minioadmin --s3-disable-tls

# With AIS processing
cargo run -p collect-file -- \
  --input-dir data.txt \
  --consolidate-ais --process-timestamps
```

## CLI Reference

### Input

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--input` (alias `--input-dir`) | `INPUT_PATH` | — | Input file or directory (recursive) |
| `--source` / `-s` | `SOURCE` | file stem/dir name | Logical source label |

### Processing

| Flag | Default | Description |
|------|---------|-------------|
| `--concurrency` (env `CONCURRENCY`) | auto (2..32) | Max concurrent file workers |
| `--noui` | off | Disable TUI status display |
| `--quiet` / `-q` (env `QUIET`) | off | Suppress routine progress lines; warnings/errors still print |
| `--completions <shell>` | — | Print shell completions to stdout and exit |
| `--version` | — | Prints `<crate version> (<git commit hash>)` |
| `--config <file>` (env `CONFIG_FILE`) | — | Load flag defaults from a flat TOML file; CLI flags and pre-set env vars still win — see [README.md](README.md#common-cli-features) |

Exits `2` (instead of `0`) when there were no unfinished input files to ingest — distinct from a hard error (`1`).

### AIS Processing

| Flag | Default | Description |
|------|---------|-------------|
| `--consolidate-ais` | off | Reassemble multi-part NMEA fragments into single sentences |
| `--process-timestamps` | off | Extract `$PGHP` and tag-block `c:` timestamps |

### Common + S3

Same as [collect-socket](COLLECT_SOCKET.md) — `CommonCliArgs` and `S3CliArgs`
are identical.

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
