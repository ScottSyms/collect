# collect-barentswatch — BarentsWatch Live AIS collector

Consumes real-time AIS data from the [BarentsWatch Live AIS API](https://developer.barentswatch.no/docs/AIS/live-ais-api) into Hive-partitioned Parquet with Zstd compression. Uses OAuth2 client-credentials flow for authentication.

New to the project? [TUTORIAL.md](TUTORIAL.md) walks through local storage, partitioning, S3, and Iceberg in order; this page is a reference for this one binary.

## Pipeline

```
https://live.ais.barentswatch.no/v1/combined → collect-barentswatch → Bronze Parquet
                                                      (ts, payload, source)
```

## Usage

```bash
# Basic streaming ingest (full model, JSON format)
cargo run -p collect-barentswatch -- \
  --client-id YOUR_CLIENT_ID \
  --client-secret YOUR_CLIENT_SECRET \
  --source barentswatch

# With S3 output
cargo run -p collect-barentswatch -- \
  --client-id YOUR_CLIENT_ID \
  --client-secret YOUR_CLIENT_SECRET \
  --source barentswatch \
  --s3-bucket barentswatch \
  --s3-endpoint http://minio:9000 \
  --s3-access-key minio --s3-secret-key minioadmin --s3-disable-tls

# With inline silver parsing (decodes to typed AIS tables)
cargo run -p collect-barentswatch -- \
  --client-id YOUR_CLIENT_ID \
  --client-secret YOUR_CLIENT_SECRET \
  --source barentswatch \
  --parser aisstream
```

## CLI Reference

### API Connection

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--client-id` | `BARENTSWATCH_CLIENT_ID` | — | BarentsWatch API client ID |
| `--client-secret` | `BARENTSWATCH_CLIENT_SECRET` | — | BarentsWatch API client secret |
| `--endpoint` | `BARENTSWATCH_ENDPOINT` | `https://live.ais.barentswatch.no/v1/combined` | Stream endpoint URL |
| `--model-type` | `MODEL_TYPE` | `Full` | `Standard` or `Full` (Full includes navigational_status, call_sign, destination, eta, imo_number, dimensions, draught) |
| `--model-format` | `MODEL_FORMAT` | `Json` | `Json` (GeoJSON not supported) |

### Source

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--source` / `-s` | `SOURCE` | `"barentswatch"` | Logical source label |
| `--quiet` / `-q` | `QUIET` | off | Suppress routine progress lines; warnings/errors still print |
| `--completions <shell>` | — | — | Print shell completions to stdout and exit |
| `--version` | — | — | Prints `<crate version> (<git commit hash>)` |
| `--config <file>` | `CONFIG_FILE` | — | Load flag defaults from a flat TOML file; CLI flags and pre-set env vars still win — see [CLI_REFERENCE.md](CLI_REFERENCE.md#shared-across-all-six-binaries) |

### Common + S3 + Iceberg + reconnect

Full flag/env tables: [CLI_REFERENCE.md](CLI_REFERENCE.md#common-to-the-five-collectors-commoncliargs)
(`CommonCliArgs`), [S3](CLI_REFERENCE.md#s3--collectors-s3cliargs-one-sink)
(`S3CliArgs`), [Iceberg](CLI_REFERENCE.md#iceberg-icebergcliargs-all-six-binaries)
(`IcebergCliArgs` — optional, direct `raw`-table registration on
successful upload), and [reconnect](CLI_REFERENCE.md#reconnect-the-four-streaming-collectors)
(`--max-reconnect-seconds`), plus the same `--parser` inline-parsing flag
([details](CLI_REFERENCE.md#inline-parsing-collectors-only-parsercliargs)).
Use `--parser aisstream` for this feed — the collector transforms BarentsWatch JSON
into the aisstream.io envelope format before silver parsing.

## Output

- **Schema:** `ts` (timestamp ms UTC), `payload` (utf8 JSON), `source` (utf8)
- **Partition:** Hive-style under `--output-dir`
- **Compression:** Zstd

## Authentication & Streaming

1. **OAuth2 Token**: Obtains an access token via `POST https://id.barentswatch.no/connect/token` with `grant_type=client_credentials` and `scope=ais`. Tokens are cached and refreshed 5 minutes before expiry.
2. **HTTP Stream**: `GET` the endpoint with `Authorization: Bearer <token>` and `Accept: application/x-ndjson`. The response is a newline-delimited JSON stream.
3. **Reconnection**: On disconnect or error, reconnects with exponential backoff (1s → 5s max). If the connection can't be re-established within `--max-reconnect-seconds` (default 300s) of total retrying, the process gives up and exits with [`UPSTREAM_UNAVAILABLE`](CLI_REFERENCE.md#exit-codes); set it to `0` to retry forever instead.
4. **401 Handling**: On HTTP 401, forces an immediate token refresh and retries the request once.

## Data Transformation

Each BarentsWatch JSON line is transformed into the aisstream.io envelope format before being written to bronze Parquet. This allows the existing `--parser aisstream` to decode the data into the standard silver tables (`positions`, `statics`, `other`, `atons`, `meteo`, `binary`).

| BarentsWatch Field | → | aisstream Envelope |
|---|---|---|
| `mmsi` | → | `MetaData.MMSI`, `Message.UserID` |
| `msgtime` | → | `MetaData.time_utc` |
| `latitude` | → | `MetaData.latitude`, `Message.Latitude` |
| `longitude` | → | `MetaData.longitude`, `Message.Longitude` |
| `name` | → | `MetaData.ShipName` |
| `speedOverGround` | → | `Message.Sog` |
| `courseOverGround` | → | `Message.Cog` |
| `trueHeading` | → | `Message.TrueHeading` (511 → null) |
| `rateOfTurn` | → | `Message.RateOfTurn` (-128 → null) |
| `shipType` | → | `Message.MessageID` (inferred: 30-39→18, 60-99→1, etc.) |
| `navigationalStatus` | → | `Message.NavigationalStatus` |
| `callSign` | → | `Message.CallSign` |
| `destination` | → | `Message.Destination` |
| `eta` | → | `Message.Eta` |
| `imoNumber` | → | `Message.ImoNumber` |
| `dimensionA-D` | → | `Message.A/B/C/D` |
| `draught` | → | `Message.MaximumStaticDraught` |
| — | → | `Message.PositionAccuracy=true`, `Message.Raim=false`, `Message.SpecialManoeuvreIndicator=0` |

**MessageType** is always `"PositionReport"` (the live stream only returns position updates).

## Partitioning

Output is the raw input for [aisstream-parse](AISSTREAM_PARSE.md), which reads the bronze Parquet and decodes the JSON payloads into typed AIS tables.