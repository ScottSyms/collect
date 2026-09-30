# collect-aisstream — AISStream.io WebSocket collector

Consumes real-time AIS data from the [aisstream.io](https://aisstream.io)
WebSocket API into Hive-partitioned Parquet with Zstd compression. Supports
geographic bounding box filtering and MMSI/message-type whitelists.

New to the project? [TUTORIAL.md](TUTORIAL.md) walks through local storage,
partitioning, S3, and Iceberg in order; this page is a reference for this
one binary.

## Pipeline

```
wss://stream.aisstream.io/v0/stream → collect-aisstream → Bronze Parquet
                                                          (ts, payload, source)
```

## Usage

```bash
# Basic streaming ingest
cargo run -p collect-aisstream -- \
  --api-key YOUR_AISSTREAM_API_KEY \
  --bounding-boxes '[[[-180,-90],[180,90]]]' \
  --source aisstream

# With MMSI filter
cargo run -p collect-aisstream -- \
  --api-key YOUR_AISSTREAM_API_KEY \
  --bounding-boxes '[[[-10,35],[30,60]]]' \
  --filter-mmsi 123456789 --filter-mmsi 987654321 \
  --source aisstream

# With S3 output
cargo run -p collect-aisstream -- \
  --api-key YOUR_AISSTREAM_API_KEY \
  --bounding-boxes '[[[-180,-90],[180,90]]]' \
  --source aisstream \
  --s3-bucket aisstream \
  --s3-endpoint http://minio:9000 \
  --s3-access-key minio --s3-secret-key minioadmin --s3-disable-tls
```

## CLI Reference

### API Connection

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--api-key` | `AISSTREAM_API_KEY` | — | aisstream.io API key |
| `--bounding-boxes` | `BOUNDING_BOXES` | — | Geographic filter JSON |

### Filters

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--filter-mmsi` (repeatable) | `FILTER_MMSI` | — | MMSI whitelist (max 50) |
| `--filter-message-types` (repeatable) | `FILTER_MESSAGE_TYPES` | — | Message type whitelist |

### Source

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--source` / `-s` | `SOURCE` | `"aisstream"` | Logical source label |
| `--quiet` / `-q` | `QUIET` | off | Suppress routine progress lines; warnings/errors still print |
| `--completions <shell>` | — | — | Print shell completions to stdout and exit |
| `--version` | — | — | Prints `<crate version> (<git commit hash>)` |
| `--config <file>` | `CONFIG_FILE` | — | Load flag defaults from a flat TOML file; CLI flags and pre-set env vars still win — see [CLI_REFERENCE.md](CLI_REFERENCE.md#shared-across-all-binaries) |

### Common + S3 + Iceberg + reconnect

Full flag/env tables: [CLI_REFERENCE.md](CLI_REFERENCE.md#common-to-the-five-collectors-commoncliargs)
(`CommonCliArgs`), [S3](CLI_REFERENCE.md#s3--collectors-s3cliargs-one-sink)
(`S3CliArgs`), [Iceberg](CLI_REFERENCE.md#iceberg-icebergcliargs-all-binaries)
(`IcebergCliArgs` — optional, direct `raw`-table registration on
successful upload), and [reconnect](CLI_REFERENCE.md#reconnect-the-four-streaming-collectors)
(`--max-reconnect-seconds`), plus the same `--parser` inline-parsing flag
([details](CLI_REFERENCE.md#inline-parsing-collectors-only-parsercliargs)).
Use `--parser aisstream` for this feed; `--parser ais` warns at startup
since NMEA decoding rejects every JSON row.

## Output

- **Schema:** `ts` (timestamp ms UTC), `payload` (utf8 JSON), `source` (utf8)
- **Partition:** Hive-style under `--output-dir`
- **Compression:** Zstd

## WebSocket

Connects to `wss://stream.aisstream.io/v0/stream` and sends a subscription
JSON with the API key, bounding boxes, and filters. On disconnect, reconnects
with exponential backoff (1s → 5s max). If the connection can't be
re-established within `--max-reconnect-seconds` (default 300s) of total
retrying, the process gives up and exits with
[`UPSTREAM_UNAVAILABLE`](CLI_REFERENCE.md#exit-codes); set it to `0` to
retry forever instead.

## Partitioning

Output is the raw input for [aisstream-parse](AISSTREAM_PARSE.md), which
reads the bronze Parquet and decodes the JSON payloads into typed AIS tables.
