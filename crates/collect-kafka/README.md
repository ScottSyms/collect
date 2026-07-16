# collect-kafka

`collect-kafka` consumes messages from a Kafka topic and writes them into Hive-partitioned Parquet files (Zstd compression), using the same storage layout as `collect-socket`.

## Run

### Required

- `--kafka-brokers host1:9092,host2:9092`
- `--kafka-topic <topic>`
- `--kafka-group-id <consumer-group>`

### Example

```bash
cargo run -p collect-kafka -- \
  --kafka-brokers localhost:9092 \
  --kafka-topic telemetry \
  --kafka-group-id collect-kafka \
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
