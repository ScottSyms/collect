# aisstream-parse — decode AISStream JSON into typed Parquet

Reads bronze Parquet datasets produced by [collect-aisstream](COLLECT_AISSTREAM.md)
and decodes the AISStream JSON payloads into typed, queryable Parquet
datasets — positions, statics, meteo, binary, and aids to navigation —
using the same schema conventions as [ais-parse](AIS_PARSE.md).

## Pipeline

```
collect-aisstream → aisstream-parse → Silver Parquet
  (bronze JSON)                      (typed tables)
```

## Usage

```bash
# Decode locally
cargo run -p aisstream-parse -- \
  --input-dir aisstream-data \
  --output-dir ais-silver --partition day

# S3 → S3
cargo run -p aisstream-parse -- \
  --input-s3-bucket bronze/aisstream --output-s3-bucket ais \
  --s3-endpoint http://minio:9000 --s3-disable-tls \
  --s3-access-key minio --s3-secret-key minioadmin \
  --partition day

# Incremental run
cargo run -p aisstream-parse -- \
  --input-s3-bucket bronze/aisstream --output-s3-bucket ais \
  --partition day --incremental
```

## Output Tables

Five sibling Hive-partitioned datasets under the output root. Each is
partitioned by time only (not by source).

```
<output>/positions/year=YYYY/month=MM/day=DD/...
<output>/statics/year=YYYY/month=MM/day=DD/...
<output>/meteo/year=YYYY/month=MM/day=DD/...
<output>/binary/year=YYYY/month=MM/day=DD/...
<output>/atons/year=YYYY/month=MM/day=DD/...
```

### positions

| Column | Type | Notes |
|--------|------|-------|
| `ts` | timestamp (ms UTC) | message timestamp |
| `source` | utf8 | origin feed label |
| `msg_type` | uint8 | AIS message type (1/2/3/18) |
| `mmsi` | uint32 | |
| `ais_class` | utf8 | `Class A` / `Class B` |
| `latitude` / `longitude` | float64, nullable | WGS-84 degrees |
| `sog_knots` | float64, nullable | speed over ground |
| `cog` | float64, nullable | course over ground |
| `heading_true` | float64, nullable | |
| `rot` | float64, nullable | rate of turn |
| `altitude_m` | float64, nullable | (SAR aircraft) |
| `h3` | uint64 (local) / bigint (Iceberg) | H3 cell at resolution 10 (signed long in Iceberg) |
| `hilbert` | uint64 (local) / bigint (Iceberg) | Hilbert curve S2 cell id (signed long in Iceberg) |
| `nav_status` | utf8 | navigation status |
| `high_accuracy` | boolean | position accuracy flag |
| `raim` | boolean | |
| `special_manoeuvre` | boolean, nullable | |
| `station` | utf8, nullable | source/base station |

### statics

| Column | Type | Notes |
|--------|------|-------|
| `ts` | timestamp (ms UTC) | message timestamp |
| `source` | utf8 | origin feed label |
| `msg_type` | uint8 | AIS message type (5 or 24) |
| `mmsi` | uint32 | |
| `ais_class` | utf8 | |
| `imo_number` | uint32, nullable | |
| `call_sign` / `name` | utf8, nullable | |
| `ship_type` | utf8 | |
| `dimension_to_bow/stern/port/starboard` | uint16, nullable | metres |
| `draught_m` | float64, nullable | metres |
| `destination` | utf8, nullable | |
| `eta` | timestamp (ms UTC), nullable | |
| `mothership_mmsi` | uint32, nullable | |
| `station` | utf8, nullable | |

### meteo

Type 8 meteorological & hydrological data (DAC=1 FID=31 / FID=11).
~40 typed columns including position, wind, temperature, pressure, waves,
currents, and ice. Every measurement is nullable. See [ais-parse meteo](AIS_PARSE.md)
for the full column list — the schema is identical.

### binary

Other Type 8 messages retained as generic header + hex payload:

| Column | Type | Notes |
|--------|------|-------|
| `ts` | timestamp (ms UTC) | |
| `source` | utf8 | |
| `msg_type` | uint8 | always 8 |
| `mmsi` | uint32 | |
| `dac` | uint16 | Designated Area Code |
| `fid` | uint8 | Functional ID |
| `payload_hex` | utf8 | application payload as hex |
| `payload_bits` | uint32 | payload length in bits |
| `station` | utf8, nullable | |

### atons

| Column | Type | Notes |
|--------|------|-------|
| `ts` | timestamp (ms UTC) | |
| `source` | utf8 | |
| `msg_type` | uint8 | always 21 |
| `mmsi` | uint32 | |
| `ais_class` | utf8 | |
| `aid_type` | utf8 | aid type description |
| `name` / `name_extension` | utf8, nullable | |
| `latitude` / `longitude` | float64, nullable | |
| `h3` | uint64 (local) / bigint (Iceberg) | signed long in Iceberg |
| `hilbert` | uint64 (local) / bigint (Iceberg) | signed long in Iceberg |
| `dimension_to_bow/stern/port/starboard` | uint16, nullable | |
| `off_position` | boolean | |
| `virtual_aid` | boolean | |
| `assigned_mode` | boolean | |
| `high_accuracy` | boolean | |
| `raim` | boolean | |
| `station` | utf8, nullable | |

## CLI Reference

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--input-dir` (repeatable) | — | — | Local input directory |
| `--input-s3-bucket` (repeatable) | `INPUT_S3_BUCKET` | — | Input S3 bucket (supports `bucket/path`) |
| `--input-s3-prefix` | `INPUT_S3_PREFIX` | `""` | Input key prefix |
| `--output-dir` | — | — | Local output directory |
| `--output-s3-bucket` | `OUTPUT_S3_BUCKET` | — | Output S3 bucket (supports `bucket/path`) |
| `--output-s3-prefix` | `OUTPUT_S3_PREFIX` | `""` | Output key prefix |
| `--partition` | — | `day` | Partition granularity |
| `--source` | — | — | Source filter |
| `--year` / `--month` / `--day` / `--hour` / `--minute` | — | — | Partition time filter chain |
| `--since <HOURS>` | `SINCE_HOURS` | — | Rolling window |
| `--incremental` | `INCREMENTAL` | off | Watermark-based incremental |
| `--batch-size` | — | `8192` | Parquet read batch rows |
| `--compression-level` | — | `5` | Zstd level |
| `--concurrency` | — | auto | Partition concurrency |
| `--output-prefix` | — | `aisstream` | Output file name prefix |

S3 connection args: `--s3-endpoint`, `--s3-region`, `--s3-access-key`,
`--s3-secret-key`, `--s3-disable-tls` (env vars: `S3_ENDPOINT`, etc.)

### Iceberg output (REST catalog)

Instead of `--output-dir` / `--output-s3-bucket`, pass `--iceberg-catalog-uri` to write directly into Iceberg tables via a REST catalog. Both aisstream-parse and ais-parse share the same set of tables when writing to the same namespace.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--iceberg-catalog-uri` | `ICEBERG_CATALOG_URI` | — | REST catalog URI |
| `--iceberg-warehouse` | `ICEBERG_WAREHOUSE` | — | Warehouse location |
| `--iceberg-namespace` | `ICEBERG_NAMESPACE` | `ais` | Catalog namespace (database) |
| `--iceberg-table-prefix` | `ICEBERG_TABLE_PREFIX` | — | Prefix for table names |
| `--iceberg-token` | `ICEBERG_TOKEN` | — | Bearer token for auth |

**Type difference — unsigned → signed:** Iceberg has no unsigned integer types. Columns that are `uint64` in the local Parquet output (`h3`, `hilbert`) are stored as Iceberg `long` (signed 64-bit bigint). The H3 cell ID and Hilbert curve values both fit within signed `i64` range, so no precision is lost.

## Watermark / Incremental

Tracks a watermark at the output target (`_aisstream-parse/watermark.json`).
Only processes partitions with files modified after the watermark. Safe to run
periodically — each run appends uniquely-named files and row-level dedup
prevents duplicates.

## Run Summary

| Field | Meaning |
|-------|---------|
| `input rows` | bronze rows read |
| `position rows` | decoded into `positions/` |
| `static rows` | decoded into `statics/` |
| `meteo rows` | Type 8 met/hydro into `meteo/` |
| `binary rows` | other Type 8 into `binary/` |
| `aton rows` | AIS type 21 into `atons/` |
| `other decoded` | valid messages not materialized |
| `unparsed` | unparseable payloads |
| `deduped (dropped)` | duplicate rows suppressed |
