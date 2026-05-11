# collect-kafka

`collect-kafka` consumes messages from a Kafka topic and writes them into Hive-partitioned Parquet files (Zstd compression), using the same storage layout as `collect-socket`.

## Run

### Required

- `--kafka-brokers host1:9092,host2:9092`
- `--topic <topic>`
- `--group-id <consumer-group>`

### Example

```bash
cargo run -p collect-kafka -- \
  --kafka-brokers localhost:9092 \
  --topic telemetry \
  --group-id collect-kafka \
  --source kafka-telemetry
```

## Environment variables

- `KAFKA_BROKERS`
- `KAFKA_TOPIC`
- `KAFKA_GROUP_ID`
- `KAFKA_AUTO_OFFSET_RESET` (`earliest` | `latest`, default: `latest`)

## Timestamps

The project’s ingestion code needs a timestamp to choose the Hive partition.
`collect-kafka` uses a heuristic on each message payload:

- if payload is an integer: treated as seconds or millis (millis if `> 1_000_000_000_000`)
- if payload is JSON: tries common keys like `ts`, `timestamp`, `event_time`, etc.
- if payload starts with `ts=...` / `timestamp=...` / `event_time=...` tokens

If no timestamp is found, it falls back to ingestion time (`now`).

## Testing

`collect-kafka` has an integration-style ingestion harness that exercises the end-to-end Parquet writing pipeline **without requiring a real Kafka cluster**.

### Run the harness

```bash
cargo test -p collect-kafka -- kafka_ingest_harness_writes_expected_parquet
```

### What the harness validates

The test is located at: `crates/collect-kafka/tests/ingest_harness.rs`.

It uses a mocked `LineSource` (Kafka-like line reader) and calls into the shared ingestion pipeline (`collect_core::run_ingest`). It then verifies:

- Parquet files are written under the configured output directory
- Hive partition layout matches the expected prefix format (for this test’s settings):
  - `source=<source_name>/year=...`
- Total Parquet row count across all produced files equals the number of mocked input messages

### Timestamp behavior in the test

The mock source returns `None` from `timestamp_for_payload()`, so partitioning uses the pipeline’s ingestion/arrival time (i.e., no timestamp is parsed from the mocked payloads).
