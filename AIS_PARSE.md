# ais-parse — decode AIS sentences into typed Parquet

`ais-parse` is the silver-layer step of the pipeline: it reads the bronze
(ts, payload) Parquet datasets the collectors produce, decodes each AIS
sentence with the
[ScottSyms/nmea-parser](https://github.com/ScottSyms/nmea-parser) library, and
writes **typed, queryable Parquet** — no more NMEA strings between you and the
data.

```
collect-* (ingest)  →  ais-parse (decode)
   bronze                      silver
```

Input and output can each independently be a local directory or an S3/MinIO
bucket.

## What it produces

Sibling hive-partitioned datasets under the output root — `positions/` and
`statics/` (below), plus `meteo/` and `binary/` from [Type 8](#type-8-binary-broadcast).
The output is **not partitioned by source** — every source that falls in a
time partition is decoded into it together, so downstream queries see one
unified dataset — but each row keeps its origin in a `source` column:

```
<output>/positions/year=YYYY/month=MM/day=DD/pos-....parquet
<output>/statics/year=YYYY/month=MM/day=DD/stat-....parquet
<output>/meteo/year=YYYY/month=MM/day=DD/met-....parquet
<output>/binary/year=YYYY/month=MM/day=DD/bin-....parquet
```

`source` is read from the input's `source` column when present, and otherwise
from the input's `source=` partition segment (raw bronze). Either input layout
works.

**`positions`** — one row per position report (AIS types 1–3, 18, 19, 27):

| Column | Type | Notes |
|--------|------|-------|
| `ts` | timestamp (ms, UTC) | the row's corrected bronze timestamp |
| `source` | utf8 | origin feed label (from the input's `source` column, or its `source=` partition when reading raw bronze) |
| `msg_type` | uint8 | the AIS message type that produced the row (1/2/3/18/19/27) |
| `station` | utf8, nullable | source/base station from the NMEA tag block `s:` field, if present |
| `mmsi` | uint32 | |
| `ais_class` | utf8 | `Class A` / `Class B` |
| `latitude`, `longitude` | float64, nullable | WGS-84 degrees |
| `sog_knots` | float64, nullable | speed over ground |
| `cog` | float64, nullable | course over ground, degrees |
| `heading_true` | float64, nullable | |
| `rot` | float64, nullable | rate of turn |
| `altitude_m` | float64, nullable | SAR aircraft altitude |
| `h3` | uint64 (local) / bigint (Iceberg) | H3 cell at resolution 10 (signed long in Iceberg; no unsigned integer types) |
| `hilbert` | uint64 (local) / bigint (Iceberg) | Hilbert curve S2 cell id (signed long in Iceberg) |
| `nav_status` | utf8 | e.g. `under way using engine` |
| `high_accuracy` | boolean | position accuracy flag |
| `raim` | boolean | |
| `special_manoeuvre` | boolean, nullable | |
| `station` | utf8, nullable | source/base station from NMEA tag block `s:` field |
| `payload` | utf8 | the original NMEA sentence that produced this row |

**`statics`** — one row per static/voyage report (AIS types 5 and 24):

| Column | Type | Notes |
|--------|------|-------|
| `ts`, `source`, `station`, `ais_class` | as above | |
| `msg_type` | uint8 | the AIS message type (5 or 24) |
| `mmsi` | uint32 | |
| `imo_number` | uint32, nullable | |
| `call_sign`, `name` | utf8, nullable | |
| `ship_type` | utf8 | |
| `dimension_to_bow/stern/port/starboard` | uint16, nullable | metres |
| `draught_m` | float64, nullable | metres |
| `destination` | utf8, nullable | |
| `eta` | timestamp (ms, UTC), nullable | |
| `mothership_mmsi` | uint32, nullable | |
| `payload` | utf8 | the original NMEA sentence that produced this row |

**`meteo`** and **`binary`** — decoded from Type 8 Binary Broadcast messages;
see [Type 8](#type-8-binary-broadcast) below.

Sentences that decode to any other message class (base-station reports, aids
to navigation, safety messages, GNSS sentences, ...) are counted in the run
summary (`other decoded`) but not materialized. Unparseable payloads
(`$PGHP` wrappers, corrupt sentences) are counted as `unparsed` — the bronze
data still holds them, nothing is lost.

## Type 8 Binary Broadcast

AIS Type 8 carries a generic header — MMSI plus an application identifier
(DAC = Designated Area Code, FID = Functional ID) — followed by an
application-specific binary payload. `nmea-parser` doesn't decode Type 8, so
ais-parse decodes the 6-bit payload itself. Two output datasets result:

- **`meteo/`** — the standardized **meteorological & hydrological** subtypes,
  DAC=1 FID=31 (IMO289, current) and FID=11 (IMO236, deprecated), decoded into
  ~40 typed columns: `mmsi`, `dac`, `fid`, position (`latitude`/`longitude`),
  `day`/`hour`/`minute`, wind (`wind_speed_kn`, `wind_gust_kn`, `wind_dir_deg`,
  `wind_gust_dir_deg`), `air_temp_c`, `humidity_pct`, `dew_point_c`,
  `pressure_hpa`, `pressure_tendency`, `visibility_nm`/`visibility_greater`,
  `water_level_m`/`water_level_trend`, three current layers
  (`surface_current_speed_kn`/`_dir_deg`, `current2_*`, `current3_*` with
  depths), waves (`wave_height_m`, `wave_period_s`, `wave_dir_deg`), swell
  (`swell_*`), `sea_state`, `water_temp_c`, `precipitation_type`,
  `salinity_pct`, and `ice`. Every measurement is **nullable** — AIS transmits
  a per-field "not available" sentinel that decodes to `null` — and each is
  scaled to a natural unit (°C, hPa, knots, metres, degrees true, %). Bit
  offsets, scales, and sentinels follow gpsd's reference decoder
  (`driver_ais.c` / `gps.h`).

- **`binary/`** — every *other* Type 8 (area notices, extended voyage data,
  regional DACs, ...): the generic header (`mmsi`, `dac`, `fid`) plus the
  application payload retained as `payload_hex` (+ `payload_bits`), so nothing
  is lost and unrecognized subtypes can be decoded downstream later.
  Every row also carries a `payload` column with the original NMEA sentence.

Both datasets carry the same `ts`, `source`, `station`, `msg_type`, and
`payload` columns as the others (`msg_type` is 8 for every Type 8 row) and are
partitioned by time only.

### Tag block / base station

Every dataset includes a **`station`** column carrying the NMEA 4.10 tag-block
`s:` field — the source/base station (receiver) that reported the message —
when the feed provides it. It is read from the tag block on single sentences,
and the consolidator preserves it into the rebuilt tag block when it combines
multi-fragment messages, so combined statics keep their station too. Other tag
fields (`d:`, `n:`, `r:`, `g:`, `t:`) are not currently extracted; only
`c:` (used as `ts`) and `s:` are.

Multi-fragment Type 8 messages (up to 5 sentences) are decoded once the
on-ingest consolidator or the `--consolidate-ais` pass has combined them into
a single sentence. ais-parse also decodes Type 8 from a combined/single
sentence via the peeked-type fast path; a raw un-combined fragment falls
through to the NMEA parser.

## Append-only re-runs (no partition replacement)

Decoding never re-partitions: a bronze row's corrected `ts` already places it
in its output time partition. ais-parse **appends** uniquely-named files to each
output partition it touches (files carry a timestamp + counter suffix). Old
files from previous runs are left in place; downstream compaction consolidates
them later. Because each run produces uniquely-named files, concurrent runs
are safe, and a re-run accumulates new files alongside old ones without
data loss. Row-level dedup (keyed on `(ts, mmsi, source-kind)`) prevents the
same AIS message from appearing in multiple files when the watermark lap causes
a partition to be re-read.

In `--incremental` mode a partition is re-read when *any* of its sources has a
new file, but dedup ensures no duplicate rows make it to the output.

Don't run two instances against the same output concurrently (use
`prohibit_overlap` in Nomad, as with the other batch tools).

## Multi-part messages

When processing raw bronze data via `--consolidate-ais` on the collector or on
ais-parse itself, the `AisConsolidator` explicitly reassembles multi-part
fragments into single sentences **before** they reach the NMEA parser. This
reduces `Incomplete` results and produces cleaner output. The consolidator
handles `$PGHP` timestamp lines, tag-block `c:` carry-forward, and `s:`
station preservation, with an 8K-group buffer and oldest-first eviction.

Without `--consolidate-ais`, raw un-normalized bronze data also works: the
parser buffers `Incomplete` fragments internally and completes them when the
matching part arrives (rows within a partition file are time-ordered, so pairs
almost always meet). Fragments whose partner never arrives are counted
`incomplete`.

When a partition pools several sources, fragment state is reset at each source
boundary so multi-part sequence ids can't collide across sources.

## Usage

```bash
# Decode locally
cargo run -p ais-parse -- --input-dir normalized --output-dir silver --partition day

# S3 → S3 on one MinIO endpoint
cargo run -p ais-parse -- \
  --input-s3-bucket normalized-ais --output-s3-bucket silver-ais \
  --s3-endpoint http://minio:9000 --s3-access-key … --s3-secret-key … --s3-disable-tls \
  --partition day

# Hourly scheduled run: only partitions with new bronze files
cargo run -p ais-parse -- --input-s3-bucket normalized-ais --output-s3-bucket silver-ais \
  --s3-endpoint http://minio:9000 --partition day --incremental
```

### CLI reference

| Flag | Default | Purpose |
|------|---------|---------|
| `--input-dir` (repeatable) / `--input-s3-bucket` (repeatable) + `--input-s3-prefix` | — | bronze input root(s); exactly one kind required. Env `INPUT_DIR`, `INPUT_S3_BUCKET` (both comma-separated; buckets support `bucket/path` syntax), `INPUT_S3_PREFIX` |
| `--output-dir` / `--output-s3-bucket` + `--output-s3-prefix` | — | silver output root; exactly one required. Env `OUTPUT_DIR`, `OUTPUT_S3_BUCKET` (supports `bucket/path` syntax), `OUTPUT_S3_PREFIX` |
| `--s3-endpoint`, `--s3-region`, `--s3-access-key`, `--s3-secret-key`, `--s3-disable-tls` | — | shared S3 connection (env `S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DISABLE_TLS`) |
| `--partition` | `day` | input layout granularity; output trees mirror it (env `PARTITION`) |
| `--filter-source` (alias `--source`), `--year`…`--minute` | *(all)* | partition slice filters (env `FILTER_SOURCE`) |
| `--since <HOURS>` | *(off)* | rolling window (env `SINCE`); with `--incremental`, first-run seed only |
| `--incremental` | *(off)* | watermark at the output (`_ais-parse/watermark.json`), env `INCREMENTAL=true`; requires `--output-s3-bucket` when combined with Iceberg output |
| `--batch-size` | `8192` | Parquet read batch rows (env `BATCH_SIZE`) |
| `--compression-level` | `5` | Zstd level for output (env `COMPRESSION_LEVEL`) |
| `--concurrency` | cores, clamped `[1, 8]` | partitions decoded in parallel (env `CONCURRENCY`) |
| `--output-prefix` | `ais` | output file name prefix (added before tree suffix, env `OUTPUT_PREFIX`) |
| `--consolidate-ais` | *(off)* | reassemble fragmented NMEA sentences before decoding |
| `--dry-run` | *(off)* | list the partitions that would be processed and exit; never connects to the output (env `DRY_RUN`) — see [below](#dry-run) |
| `--quiet` / `-q` | *(off)* | suppress routine progress lines; warnings/errors/summary still print (env `QUIET`) |
| `--completions <shell>` | — | print shell completions to stdout and exit |
| `--version` | — | prints `<crate version> (<git commit hash>)` |
| `--config <file>` | — | load flag defaults from a flat TOML file (env `CONFIG_FILE`); CLI flags and pre-set env vars still win — see [README.md](README.md#common-cli-features) |

### Dry run

`--dry-run` lists the partitions a real run would touch and exits without decoding, writing, or connecting to the output target at all — including skipping the S3 connection that would otherwise auto-create a missing output bucket. Output:

```
$ ais-parse --input-dir normalized --output-dir silver --dry-run
Scanning 1 input dir(s)...
Found 3 partition(s).
year=2026/month=07/day=14  (2 local files)
year=2026/month=07/day=15  (4 local files)
year=2026/month=07/day=16  (1 local file)
Dry run: 3 partition(s) would be processed. No output was written.
```

Combined with `--incremental`, the dry run can't see the real watermark (it lives at the output, which `--dry-run` deliberately doesn't connect to), so it falls back to `--since` or the full dataset — a printed note calls this out.

### Iceberg output (REST catalog)

Instead of `--output-dir` / `--output-s3-bucket`, pass `--iceberg-catalog-uri` to write directly into Iceberg tables via a REST catalog (Lakekeeper, Polaris, etc.). Both `ais-parse` and `aisstream-parse` share the same set of tables when writing to the same namespace.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--iceberg-catalog-uri` | `ICEBERG_CATALOG_URI` | — | REST catalog URI |
| `--iceberg-warehouse` | `ICEBERG_WAREHOUSE` | — | Warehouse location |
| `--iceberg-namespace` | `ICEBERG_NAMESPACE` | `ais` | Catalog namespace (database) |
| `--iceberg-table-prefix` | `ICEBERG_TABLE_PREFIX` | — | Prefix for table names |
| `--iceberg-token` | `ICEBERG_TOKEN` | — | Bearer token for auth |

Tables are automatically created if missing with schemas matching the shared Iceberg specification (field IDs stable across tools). All five table types are written: `positions`, `statics`, `meteo`, `binary`, `atons`.

**Type difference — unsigned → signed:** Iceberg has no unsigned integer types. Columns that are `uint64` in the local Parquet output (`h3`, `hilbert`) are stored as Iceberg `long` (signed 64-bit bigint). The H3 cell ID and Hilbert curve values both fit within signed `i64` range, so no precision is lost. The conversion is handled transparently in the Iceberg writer.

#### Incremental mode with Iceberg

`--incremental` is supported with Iceberg output. The watermark is stored in an S3 bucket (specified via `--output-s3-bucket`) so it persists across runs independently of the Iceberg catalog — pass a bucket on your S3/MinIO endpoint:

```bash
S3_ENDPOINT=http://fishbake:9000 \
S3_ACCESS_KEY=qz5mjYcOvTgRIw6aJKgm \
S3_SECRET_KEY=uIDVBTY5eux1xgmiWqnLU6LfnjI1wYgCyrJuYggl \
S3_PATH_STYLE=true \
S3_DISABLE_EC2_METADATA=true \
~/code/projects/collect/target/release/ais-parse \
  --input-dir ./extract/ \
  --output-s3-bucket warehouse-bucket \
  --incremental \
  --since 48 \
  --iceberg-catalog-uri http://fishbake:8181/catalog \
  --iceberg-warehouse test \
  --iceberg-namespace melongoober
```

The watermark is stored at `s3://<bucket>/<prefix>/_ais-parse/watermark.json`. On the first run `--since` provides the initial cutoff; subsequent runs load the persisted watermark and skip partitions whose files haven't changed. The `--output-s3-bucket` is used only for watermark storage — no Parquet data is written to it.

### Run summary

| Field | Meaning |
|-------|---------|
| `input rows` | bronze rows read |
| `position rows` | decoded into `positions/` |
| `static rows` | decoded into `statics/` |
| `meteo rows` | Type 8 met/hydro decoded into `meteo/` |
| `binary rows` | other Type 8 retained in `binary/` (header + hex) |
| `other decoded` | valid messages of classes not materialized |
| `incomplete fragments` | multi-part fragments whose partner never arrived |
| `unparsed` | sentences the parser rejected |

### Exit codes

| Code | Meaning |
|------|---------|
| `0` | processed successfully (or `--dry-run` completed, even with 0 partitions found) |
| `1` | error |
| `2` | nothing to process — no matching input files, or (with `--incremental`) nothing new since the watermark |
| `deduped (dropped)` | duplicate rows suppressed by row-level dedup |

A quick way to eyeball decoded output:

```bash
cargo run -p ais-parse --example dump -- silver/positions/year=2026/month=07/day=05/pos-....parquet
```

## Deployment

A batch job — run it on a schedule. Nomad periodic example:

```hcl
job "ais-parse" {
  datacenters = ["dc1"]
  type        = "batch"

  periodic {
    cron             = "0 * * * * *"   # hourly
    prohibit_overlap = true
  }

  group "parse" {
    task "ais-parse" {
      driver = "exec"
      config {
        command = "/usr/local/bin/ais-parse"
        args = [
          "--input-s3-bucket", "normalized-ais",
          "--output-s3-bucket", "silver-ais",
          "--s3-endpoint", "http://minio.service.consul:9000",
          "--partition", "day",
          "--incremental",
          "--concurrency", "4",
        ]
      }
      env {
        S3_ACCESS_KEY = "..."
        S3_SECRET_KEY = "..."
      }
      resources {
        cpu    = 1000
        memory = 1024
      }
    }
  }
}
```

The Docker image ships `/usr/local/bin/ais-parse` alongside the collectors;
use `--entrypoint /usr/local/bin/ais-parse` with the same image.

## Library note

`ais-parse` pins `nmea-parser` to the fork's library-only 0.11.0 line
(`rev = a4ddb297…`). Later fork commits embed a CLI, S3 I/O, and an
`iceberg 0.3` dependency in the library crate itself; iceberg pins
`parquet 52`, which conflicts with this workspace's `parquet 53` — and that
plumbing is exactly what `ais-parse` provides natively. If the fork later
gates those behind a feature flag, the pin can move forward.
