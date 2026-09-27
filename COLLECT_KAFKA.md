# collect-kafka — Kafka data collector

Consumes messages from a Kafka topic into Hive-partitioned Parquet with
Zstd compression. Commits offsets only after each Parquet batch is durably
written.

New to the project? [TUTORIAL.md](TUTORIAL.md) walks through local storage,
partitioning, S3, and Iceberg in order; this page is a reference for this
one binary.

## Pipeline

```
Kafka topic → collect-kafka → Bronze Parquet
                               (ts, payload, source)
```

## Usage

```bash
# Basic Kafka ingest
cargo run -p collect-kafka -- \
  --kafka-brokers broker1:9092,broker2:9092 \
  --kafka-topic ais-data \
  --kafka-group-id ingest-group-1

# With source label and S3 output
cargo run -p collect-kafka -- \
  --kafka-brokers localhost:9092 \
  --kafka-topic ais-data \
  --kafka-group-id ingest-group-1 \
  --source kafka-ais \
  --s3-bucket bronze --s3-prefix kafka \
  --s3-endpoint http://minio:9000 --s3-disable-tls \
  --s3-access-key minio --s3-secret-key minioadmin
```

## CLI Reference

### Kafka Connection

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--kafka-brokers` | `KAFKA_BROKERS` | — | Kafka bootstrap servers |
| `--kafka-topic` (alias `--topic`) | `KAFKA_TOPIC` | — | Kafka topic to consume |
| `--kafka-group-id` (alias `--group-id`) | `KAFKA_GROUP_ID` | — | Consumer group ID |
| `--kafka-auto-offset-reset` | `KAFKA_AUTO_OFFSET_RESET` | `latest` | Start strategy (`earliest` / `latest`) |

### Source

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--source` / `-s` | `SOURCE` | Kafka topic name | Logical source label |
| `--quiet` / `-q` | `QUIET` | off | Suppress routine progress lines; warnings/errors still print |
| `--completions <shell>` | — | — | Print shell completions to stdout and exit |
| `--version` | — | — | Prints `<crate version> (<git commit hash>)` |
| `--config <file>` | `CONFIG_FILE` | — | Load flag defaults from a flat TOML file; CLI flags and pre-set env vars still win — see [CLI_REFERENCE.md](CLI_REFERENCE.md#shared-across-all-six-binaries) |

### Common + S3 + Iceberg + reconnect

Full flag/env tables: [CLI_REFERENCE.md](CLI_REFERENCE.md#common-to-the-four-collectors-commoncliargs)
(`CommonCliArgs`), [S3](CLI_REFERENCE.md#s3--collectors-s3cliargs-one-sink)
(`S3CliArgs`), [Iceberg](CLI_REFERENCE.md#iceberg-icebergcliargs-all-six-binaries)
(`IcebergCliArgs` — optional, direct `raw`-table registration on
successful upload), and [reconnect](CLI_REFERENCE.md#reconnect-the-three-streaming-collectors)
(`--max-reconnect-seconds`, bounds the broker-reconnect backoff below),
plus the same `--parser` inline-parsing flag
([details](CLI_REFERENCE.md#inline-parsing-collectors-only-parsercliargs)).
Offsets still commit on bronze durability, not on silver commits.

## Output

- **Schema:** `ts` (timestamp ms UTC), `payload` (utf8), `source` (utf8)
- **Partition:** Hive-style under `--output-dir`
- **Compression:** Zstd

## Offset Management

Kafka offsets are committed after the corresponding Parquet batch is durably
written to local disk (not before). On restart, the consumer resumes from the
last committed offset, ensuring at-least-once delivery.

## Reconnection

On a broker disconnect, `collect-kafka` re-subscribes with exponential
backoff (1s → 5s max, same as `collect-socket`/`collect-aisstream`). If the
broker can't be reached again within `--max-reconnect-seconds` (default
300s) of total retrying, the process gives up and exits with
[`UPSTREAM_UNAVAILABLE`](CLI_REFERENCE.md#exit-codes); set it to `0` to
retry forever instead.

## Oversized Messages

Messages exceeding `--max-line-length` are dropped and counted as oversized
rather than causing the batch to fail.
