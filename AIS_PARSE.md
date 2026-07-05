# ais-parse — decode AIS sentences into typed Parquet

`ais-parse` is the silver-layer step of the pipeline: it reads the bronze
(ts, payload) Parquet datasets the collectors and [ais-normalize](AIS_NORMALIZE.md)
produce, decodes each AIS sentence with the
[ScottSyms/nmea-parser](https://github.com/ScottSyms/nmea-parser) library, and
writes **typed, queryable Parquet** — no more NMEA strings between you and the
data.

```
collect-* (ingest)  →  ais-normalize (re-timestamp/combine)  →  ais-parse (decode)
      bronze                      bronze, clean                     silver
```

Input and output can each independently be a local directory or an S3/MinIO
bucket, exactly like ais-normalize.

## What it produces

Two sibling hive-partitioned datasets under the output root. The output is
**not partitioned by source** — every source that falls in a time partition
is decoded into it together, so downstream queries see one unified dataset:

```
<output>/positions/year=YYYY/month=MM/day=DD/pos-....parquet
<output>/statics/year=YYYY/month=MM/day=DD/stat-....parquet
```

(The bronze input may still be `source=X/year=…`; ais-parse reads either
layout. Each output time partition is rebuilt as a whole from all the sources
that land in it, so the partition-replace re-run stays correct and race-free.)

**`positions`** — one row per position report (AIS types 1–3, 18, 19, 27):

| Column | Type | Notes |
|--------|------|-------|
| `ts` | timestamp (ms, UTC) | the row's corrected bronze timestamp |
| `mmsi` | uint32 | |
| `ais_class` | utf8 | `Class A` / `Class B` |
| `latitude`, `longitude` | float64, nullable | WGS-84 degrees |
| `sog_knots` | float64, nullable | speed over ground |
| `cog` | float64, nullable | course over ground, degrees |
| `heading_true` | float64, nullable | |
| `rot` | float64, nullable | rate of turn |
| `nav_status` | utf8 | e.g. `under way using engine` |
| `high_accuracy` | boolean | position accuracy flag |
| `raim` | boolean | |
| `special_manoeuvre` | boolean, nullable | |

**`statics`** — one row per static/voyage report (AIS types 5 and 24):

| Column | Type | Notes |
|--------|------|-------|
| `ts`, `mmsi`, `ais_class` | as above | |
| `imo_number` | uint32, nullable | |
| `call_sign`, `name` | utf8, nullable | |
| `ship_type` | utf8 | |
| `dimension_to_bow/stern/port/starboard` | uint16, nullable | metres |
| `draught_m` | float64, nullable | metres |
| `destination` | utf8, nullable | |
| `eta` | timestamp (ms, UTC), nullable | |
| `mothership_mmsi` | uint32, nullable | |

Sentences that decode to any other message class (base-station reports, aids
to navigation, safety messages, GNSS sentences, ...) are counted in the run
summary (`other decoded`) but not materialized. Unparseable payloads
(`$PGHP` wrappers, corrupt sentences) are counted as `unparsed` — the bronze
data still holds them, nothing is lost.

## Idempotent re-runs (partition replace)

Decoding never re-partitions: a bronze row's corrected `ts` already places it
in its output time partition. ais-parse **replaces** each output partition it
touches — all the sources landing in that partition are decoded together into
one `positions` and one `statics` file, written first, then the prior run's
files in that partition are deleted (objects, for S3 output). Because the
partition is rebuilt as a whole (not per source), and decoding is
deterministic, a re-run converges to the same result instead of accumulating
duplicates. No dedup pass needed. In `--incremental` mode a partition is
rebuilt when *any* of its sources has a new file, re-reading that partition's
other sources so the replaced partition stays complete.

Don't run two instances against the same output concurrently (use
`prohibit_overlap` in Nomad, as with the other batch tools).

## Multi-part messages

ais-normalize has usually already recombined multi-part messages into single
sentences, which decode directly. Raw, un-normalized bronze data works too:
the parser buffers `Incomplete` fragments and completes them when the matching
part arrives (rows within a partition file are time-ordered, so pairs almost
always meet). Fragments whose partner never arrives are counted `incomplete`.
When a partition pools several sources, fragment state is reset at each source
boundary so multi-part sequence ids can't collide across sources.

## Usage

```bash
# Preview (dry run): decode counts only, nothing written
cargo run -p ais-parse -- --input-dir normalized --output-dir silver --partition day

# Apply locally
cargo run -p ais-parse -- --input-dir normalized --output-dir silver --partition day --apply

# S3 → S3 on one MinIO endpoint
cargo run -p ais-parse -- \
  --input-s3-bucket normalized-ais --output-s3-bucket silver-ais \
  --s3-endpoint http://minio:9000 --s3-access-key … --s3-secret-key … --s3-disable-tls \
  --partition day --apply

# Hourly scheduled run: only partitions with new bronze files
cargo run -p ais-parse -- --input-s3-bucket normalized-ais --output-s3-bucket silver-ais \
  --s3-endpoint http://minio:9000 --partition day --incremental --apply
```

### CLI reference

The flag set mirrors ais-normalize; see [AIS_NORMALIZE.md](AIS_NORMALIZE.md)
for the shared semantics of each group:

| Flag | Default | Purpose |
|------|---------|---------|
| `--input-dir` (repeatable) / `--input-s3-bucket` (repeatable) + `--input-s3-prefix` | — | bronze input root(s); exactly one kind required. Env `INPUT_S3_BUCKET` (comma-separated), `INPUT_S3_PREFIX` |
| `--output-dir` / `--output-s3-bucket` + `--output-s3-prefix` | — | silver output root; exactly one required. Env `OUTPUT_S3_BUCKET`, `OUTPUT_S3_PREFIX` |
| `--s3-endpoint`, `--s3-region`, `--s3-access-key`, `--s3-secret-key`, `--s3-disable-tls` | — | shared S3 connection (env equivalents as in ais-normalize) |
| `--partition` | `day` | input layout granularity; output trees mirror it |
| `--source`, `--year`…`--minute` | *(all)* | partition slice, same rules as ais-normalize |
| `--since <HOURS>` | *(off)* | rolling window (env `SINCE_HOURS`); with `--incremental`, first-run seed only |
| `--incremental` | *(off)* | watermark at the output (`_ais-parse/watermark.json`), env `INCREMENTAL=true`. Independent of ais-normalize's watermark, so both tools can share an output tree |
| `--apply` | off (dry run) | actually write |
| `--batch-size` | `8192` | Parquet read batch rows |
| `--compression-level` | `5` | Zstd level for output |
| `--concurrency` | cores, clamped `[1, 8]` | partitions decoded in parallel |

### Run summary

| Field | Meaning |
|-------|---------|
| `input rows` | bronze rows read |
| `position rows` | decoded into `positions/` |
| `static rows` | decoded into `statics/` |
| `other decoded` | valid messages of classes not materialized |
| `incomplete fragments` | multi-part fragments whose partner never arrived |
| `unparsed` | sentences the parser rejected |

A quick way to eyeball decoded output:

```bash
cargo run -p ais-parse --example dump -- silver/positions/year=2026/month=07/day=05/pos-....parquet
```

## Deployment

A batch job, like ais-normalize — run it on a schedule after the normalize
step. Nomad periodic example:

```hcl
job "ais-parse" {
  datacenters = ["dc1"]
  type        = "batch"

  periodic {
    cron             = "0 * * * * *"   # hourly, after ais-normalize
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
          "--apply",
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
