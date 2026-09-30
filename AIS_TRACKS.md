# ais-tracks — vessels, tracks, stops and voyages

`ais-tracks` turns the silver Iceberg tables (`positions`, `statics`) into
tables that answer questions about vessels rather than messages: who is this
vessel, where did it go, where did it stop, and which port did it sail from and
to. It reads with [DataFusion](https://datafusion.apache.org/) SQL and writes
Iceberg tables into a namespace you choose.

```
collect-* (ingest) → ais-parse (decode) → ais-tracks (derive)
      bronze               silver               derived
```

It works with any Iceberg REST catalog, and uses the same catalog and S3
options as the other binaries. New to the project? [TUTORIAL.md](TUTORIAL.md)
covers Iceberg output first; this page is a reference for this one binary.
Flags are also listed in [CLI_REFERENCE.md](CLI_REFERENCE.md#ais-tracks).

> **Status.** Tested on synthetic data at every level: the SQL, the reducer
> (held to row-for-row agreement with the original SQL), the whole chain with
> thinning on and off, and the daily flow end to end against real Iceberg tables
> on the local filesystem (a test catalog and a small stand-in for the REST
> commit endpoint), covering first builds, reruns, late data and forced ranges.
> What has not been exercised is a live REST/S3 catalog such as RustFS or
> Lakekeeper: the commits reuse [`ais-compact`](AIS_COMPACT.md)'s code, but try
> `--apply` on a scratch namespace first.

## Contents

- [Design rules](#design-rules)
- [The tables and how they connect](#the-tables-and-how-they-connect)
- [Quick start](#quick-start)
- [Namespaces and table names](#namespaces-and-table-names)
- [Commands](#commands): [`vessels`](#vessels), [`ports load`](#ports-load),
  [`track-points`](#track-points), [`tracks`](#tracks),
  [`stop-segments`](#stop-segments), [`stops`](#stops), [`voyages`](#voyages), and [`daily` and catch-up](#daily-and-catch-up)
- [Operating it](#operating-it)
- [Querying the results](#querying-the-results)
- [Limits and known gaps](#limits-and-known-gaps)
- [Exit codes](#exit-codes)
- [Source map](#source-map)

## Design rules

- **Annotate first, thin only by rule.** Silver keeps every report. `track_points`
  is built from it by one streaming pass that flags duplicates, gaps, jumps,
  spikes and invalid values, and then keeps only the rows movement makes
  worth keeping (`--no-thin` keeps all). Every kept row carries how many
  reports it stands for and the sums needed to use it, flagged rows and their
  neighbours are always kept, and `sum(n_raw)` equals the reports read, so
  nothing disappears unaccounted. The summary tables (`tracks`, `stops`,
  `voyages`) count what they cover, give distances and bounding boxes both raw
  and with outliers left out, and say when something is uncertain rather than
  guessing.
- **Movement first.** Stops and voyages come from where a vessel actually was.
  What it *declared* (destination, nav status) is kept beside that as evidence,
  never used in place of it.
- **Days in order.** The daily tables (`track_points`, `tracks`,
  `stop_segments`) are built one UTC day at a time, each replacing only its own
  day partition, so a rerun is idempotent and cheap. State that crosses
  midnight is carried explicitly (see [Operating it](#operating-it)).
- **Dry run by default.** Every command computes and reports; nothing is
  written without `--apply`.

## The tables and how they connect

| Table | One row per | Built | Partitioned |
|-------|-------------|-------|-------------|
| `vessel_attributes` | value a vessel ever reported for an identity attribute | rebuilt each run | none |
| `vessels` | MMSI | rebuilt each run | none |
| `ref_ports` | port, per World Port Index release | appended per release | none |
| `track_points` | kept report (each stands for one or more `positions` rows) | daily | day of `ts` |
| `tracks` | continuous segment, per UTC day | daily | day of `ts` (first row) |
| `stop_segments` | stationary run, per UTC day | daily | day of `ts` (first point) |
| `stops` | stop (the day pieces merged) | rebuilt each run | none |
| `voyages` | leg between stops | rebuilt each run | none |
| `vessel_state` | vessel's last positioned report, as of the end of a built day | daily | day it is as of |
| `build_log` | step and day built, with what it was built from | appended after each build | none |

```
statics ─────────────────────────────► vessel_attributes ─► vessels
positions ─► track_points ─┬─► tracks ──────────┐
                           └─► stop_segments ─► stops ─► voyages
                     ref_ports ───────────────────┘        ▲
                     statics (declared destination) ───────┘
```

Silver tables are read from `--iceberg-namespace`; derived tables are written
to `--output-namespace`.

## Quick start

Point it at the catalog and warehouse the silver tables live in, with the same
S3 environment variables the other binaries use:

```bash
export S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=... S3_SECRET_KEY=... \
       S3_REGION=us-east-1 S3_PATH_STYLE=true
CAT="--iceberg-catalog-uri http://localhost:9000/iceberg --iceberg-warehouse data --iceberg-sigv4"

# One-off: reference data (download Pub 150 from the NGA first)
ais-tracks $CAT --output-namespace curated ports load \
  --file UpdatedPub150.csv --release 2026-03-01 --apply

# Vessel identity
ais-tracks $CAT --output-namespace curated vessels --apply

# A day at a time, in this order
D="--from 2026-03-01 --to 2026-03-31"
ais-tracks $CAT --output-namespace curated track-points   $D --apply
ais-tracks $CAT --output-namespace curated tracks         $D --apply
ais-tracks $CAT --output-namespace curated stop-segments  $D --apply

# Rebuilt from the above
ais-tracks $CAT --output-namespace curated stops   --apply
ais-tracks $CAT --output-namespace curated voyages --apply
```

Or let it work out what needs doing, and do only that:

```bash
# Build every completed day that is new or whose input changed since:
ais-tracks $CAT --output-namespace curated daily --catch-up --apply

# Just say which days that would be, and why:
ais-tracks $CAT --output-namespace curated daily --catch-up --plan
```

Drop `--apply` from any line to see what it would do first.

## Namespaces and table names

| Flag | Env | Default | Meaning |
|------|-----|---------|---------|
| `--iceberg-namespace` | `ICEBERG_NAMESPACE` | `ais` | where `positions` and `statics` are read |
| `--output-namespace` | `OUTPUT_NAMESPACE` | the input namespace | where derived tables are written |
| `--output-table-prefix` | `OUTPUT_TABLE_PREFIX` | none | prefix on derived table names (`v2` → `v2_vessels`) |

The remaining catalog flags (`--iceberg-catalog-uri`, `--iceberg-warehouse`,
`--iceberg-token`, `--iceberg-sigv4`, …) are shared with the other binaries;
see [CLI_REFERENCE.md](CLI_REFERENCE.md#iceberg-icebergcliargs-all-seven-binaries).
The namespace is created if it does not exist. Tables carry no fixed prefix, so
the namespace is what separates one set of derived tables from another (say, one
per experiment).

`--iceberg-table-prefix` applies to the silver input tables only.

## Commands

All commands accept the catalog and namespace flags above, and are dry runs
unless `--apply` is given.

### `vessels`

```bash
ais-tracks $CAT vessels [--apply]
```

Vessel identity from `statics` and `positions`, as two tables replaced
atomically on every run (one `replace` snapshot, so readers never see a partial
table).

**`vessel_attributes`** keeps every distinct value a vessel ever reported for
an identity attribute, so nothing is decided away:

| Column | Meaning |
|--------|---------|
| `mmsi`, `attribute`, `value` | `attribute` is one of `name`, `call_sign`, `imo`, `ship_type`, `length_m`, `beam_m` |
| `n_obs`, `first_seen`, `last_seen` | how often and when it was reported |
| `rank`, `is_current` | rank 1 is the current value: for `imo`, one that passes its check digit beats one that does not; then the most often reported value; then the most recent |

`@` padding and blank strings count as "not reported". Length is bow + stern
and beam is port + starboard, from the reported dimensions.

**`vessels`** has one row per MMSI seen in either input:

| Column | Meaning |
|--------|---------|
| `mmsi`, `mmsi_class`, `mid` | class is `ship`, `handheld`, `distress_beacon`, `craft_associated`, `aton`, `sar_aircraft`, `group`, `coast_station`, or `other`; `mid` is the maritime identification digits (flag-state code) where the class has one |
| `vessel_key` | `imo:N` when the IMO is valid, else `mmsi:N` |
| `imo_number`, `call_sign`, `name`, `ship_type`, `length_m`, `beam_m`, `ais_class` | current values |
| `first_seen`, `last_seen`, `n_positions` | from `positions` |
| `first_static_seen`, `last_static_seen`, `n_statics` | from `statics` |
| `mmsi_valid`, `imo_valid` | class is not `other`; IMO passes its check digit |
| `multiple_imos`, `multiple_names`, `multiple_call_signs` | more than one distinct value was reported |
| `computed_at` | |

Type 24 messages arrive in two halves (name; then type, call sign and
dimensions). Each attribute is resolved independently across all rows, so the
halves combine without special handling.

### `ports load`

```bash
ais-tracks $CAT ports load --file UpdatedPub150.csv --release 2026-03-01 [--apply]
```

Appends the NGA World Port Index (Publication 150 CSV) to `ref_ports` under a
release label. Nothing is overwritten and a release cannot be loaded twice, so
a stop matched last year can be reproduced against the release it used.
Matching uses the greatest label, so use sortable labels such as dates.

| Column | Meaning |
|--------|---------|
| `wpi_release`, `loaded_at` | release label and load time |
| `port_id`, `name`, `alt_name`, `unlocode`, `country`, `region` | identity (`UN/LOCODE` blank in the source becomes null) |
| `harbor_size`, `harbor_type`, `harbor_use` | |
| `latitude`, `longitude` | |
| `channel_depth_m`, `max_vessel_draft_m`, `tidal_range_m` | |

Every port in the file is kept, including ones without coordinates. `port_id`
is not unique in the source (two ids appear twice, for a duplicate spelling and
for two different terminals); matching treats each row as its own candidate.

### `track-points`

```bash
ais-tracks $CAT track-points --from 2026-03-01 [--to 2026-03-31] [--apply]
```

Reads a day of silver `positions` and writes the reduced, annotated rows to
`track_points`, partitioned by day on `ts`. It is the memory-bounded reducer
described under [Running at scale](#running-at-scale-reduce-day): one pass routes the day's
reports to on-disk vessel buckets, then each bucket is reduced in turn, and the
rows stream into one Parquet writer and one Iceberg snapshot per day. Peak
memory is one bucket, not the day.

Each vessel's stream continues from the day before: when the previous day is
built in the same run its end-of-day state is carried in memory, otherwise it
is read from the table (`--lookback-days`, default 1). A run for one day
replaces only that day's partition.

| Column | Meaning |
|--------|---------|
| `ts`, `mmsi`, `source`, `station`, `latitude`, `longitude`, `sog_knots`, `cog`, `heading_true`, `nav_status` | the report kept, as received |
| `has_position` | latitude and longitude present and in range |
| `dup_rank`, `n_dups`, `is_duplicate` | Rows with the same mmsi, ts, position, sog, cog, heading and nav status are the same message heard more than once (different `source` / `station`). Rank 1 is the first. With thinning on, duplicates are collapsed into the kept row and counted in `n_collapsed_dups`, so `is_duplicate` is false on every kept row |
| `prev_ts`, `dt_s`, `dist_nm`, `implied_speed_kn` | Movement since the previous *kept* row: `dt_s` is the time since it, `dist_nm` the summed distance of every hop since it (so distance totals are exact, thinned or not), `implied_speed_kn` the speed of this row's own hop |
| `gap_before` | first stream row, or `dt_s` above `--gap-minutes` |
| `is_speed_jump` | implied speed above `--max-speed-kn`, or two positions more than 0.05 nm apart in the same second |
| `is_spike` | a jump *into* the row and a jump *out* of it: an isolated bad fix |
| `is_sog_invalid`, `is_cog_invalid`, `is_heading_invalid`, `is_outlier` | value outside the AIS range; `is_outlier` is a spike or any invalid value |
| `n_raw` | reports this row stands for, itself included |
| `n_collapsed_dups`, `n_no_position`, `n_outliers_raw` | how many of those were duplicates, had no usable position, or were outliers |
| `sum_speed`, `n_speed` | for a count-weighted mean speed (reported speed when valid, else implied) |
| `sum_sog`, `n_sog`, `max_sog` | the same for valid reported speed only |
| `max_dev_nm` | the farthest a collapsed report was from this row |
| `max_hop_speed_kn` | the fastest hop among the reports it stands for |
| `keep_reason` | bitmask of why the row was kept (see the table below) |

The return leg after a spike is flagged `is_speed_jump` but not `is_spike`,
because it is measured from the bad point; the spike is the row to discard.
"Stream" rows are those with a position that are the first of their message;
duplicates and rows without a position are never kept, only counted.

Flags are `--no-thin`, `--keep-distance-nm`, `--keep-interval-s`,
`--keep-turn-deg`, `--keep-speed-kn`, `--max-speed-kn`, `--gap-minutes`,
`--buckets`, `--target-bucket-rows`, `--scratch` and `--keep-scratch` (the same
as `reduce-day`, below). Without `--apply` it reduces and reports without
writing.

### `tracks`

```bash
ais-tracks $CAT tracks --from 2026-03-01 [--to 2026-03-31] [--apply]
```

`track_points` rolled up into continuous segments. A segment is a run of one
vessel's points with no gap. Each UTC day is built alone, so a segment that
crosses midnight is stored as one row per day; the pieces share a `track_id`,
so `GROUP BY track_id` gives the whole segment. A day's first piece inherits
the previous day's last `track_id` when the vessel kept reporting through
midnight (`continues_previous`); otherwise the id is `<mmsi>-<start epoch ms>`.
If the previous day's partition is missing the piece gets an id of its own and
`chain_broken` is set.

| Column | Meaning |
|--------|---------|
| `ts`, `ts_end`, `duration_s`, `mmsi`, `track_id` | the piece and its chain |
| `continues_previous`, `chain_broken` | see above |
| `n_rows`, `n_stream`, `n_duplicates`, `n_no_position` | every report in the piece is counted (through `n_raw`), thinned or not; `n_stream` are the positioned, non-duplicate ones |
| `n_jumps`, `n_spikes`, `n_outliers` | flag counts |
| `start_lat/lon`, `end_lat/lon` | first and last positioned point |
| `distance_nm_raw` | sum of hops between positioned points, outliers included |
| `distance_nm_clean` | the same leaving out every hop flagged `is_speed_jump` (a spike removes both its legs, so it slightly undercounts) |
| `min/max_lat/lon`, `clean_*` | bounding box over the kept positioned points, and over those not flagged `is_outlier` (with thinning it can miss a collapsed report by up to `--keep-distance-nm`) |
| `bbox_wraps` | longitude span over 180°: the piece probably crosses the antimeridian, so min/max longitude are not a usable box |
| `mean_sog_knots`, `max_sog_knots` | reported speed, leaving out invalid values |

Rows in a stretch with no positioned point of their own (a position-less
message between two gaps) fall in no piece. The run prints how many; the
`track_points` rows are untouched. `tracks` reads the thinned rows and weights
by `n_raw`, so it gives the same counts and distances thinned or not. Flags:
`--from`, `--to`, `--shards`.

### `stop-segments`

```bash
ais-tracks $CAT stop-segments --from 2026-03-01 [--to 2026-03-31] [--apply]
```

Where vessels were stationary, one UTC day at a time, chained across midnight
like `tracks` (a shared `stop_id`, `continues_previous`). Stops come from
movement only, never from a vessel's declared status.

A point is stationary when its speed, averaged over a centred window, is under
a threshold. Reported speed is used, and implied speed when that is missing or
invalid. Smoothing stops berth jitter from splitting one stop into many. A
reporting gap does not end a stop if the vessel is still where it was.
Duplicates and `is_spike` fixes are ignored, not deleted.

| Column | Meaning |
|--------|---------|
| `ts`, `ts_end`, `duration_s`, `mmsi`, `stop_id` | the piece and its chain |
| `continues_previous`, `open_at_day_end` | chained from the previous day; run reaches the vessel's last point of the day (it may continue tomorrow) |
| `n_points` | |
| `lat`, `lon` | centroid; longitude is averaged circularly, so stops near the antimeridian are right |
| `radius_nm` | largest distance from the centroid: a slowly drifting vessel shows a large radius |
| `n_moored`, `n_anchored` | points whose reported nav status says so |
| `mean_speed_kn` | |

| Flag | Default | Meaning |
|------|---------|---------|
| `--from`, `--to`, `--shards` | | as above |
| `--slow-kn` | `0.5` | smoothed speed below this is stationary |
| `--smooth-minutes` | `10` | width of the averaging window |
| `--resume-nm` | `1.0` | after a gap, still stopped if within this distance of where it was |
| `--min-stop-minutes` | `30` | ignore shorter runs, except those touching midnight (they may continue); `0` keeps every run |

### `stops`

```bash
ais-tracks $CAT stops [--apply]
```

One row per stop: all `stop_segments` merged by `stop_id`, then matched to the
nearest port in the latest `ref_ports` release within a radius set by the
port's harbour size (Large 15 nm, Medium 10, Small 6, Very Small or unknown 4).
Rebuilt in full on each run.

| Column | Meaning |
|--------|---------|
| `stop_id`, `mmsi`, `arrive_ts`, `depart_ts`, `duration_s` | |
| `n_segments`, `n_points` | how many day pieces and points |
| `lat`, `lon`, `radius_nm` | centroid, and the largest piece radius |
| `n_moored`, `n_anchored` | |
| `is_current` | the vessel has not been seen since the stop ended |
| `port_id`, `port_name`, `port_unlocode`, `port_country`, `port_distance_nm` | the nearest port in range |
| `port2_id`, `port2_distance_nm` | the runner-up |
| `wpi_release` | the release matched against |
| `computed_at` | |

A stop with no port in range (an anchorage, an offshore platform) keeps null
port columns. The `port_*` columns describe proximity, not a port call: check
`duration_s`, `n_moored` and `n_anchored` if you need to tell a berth from an
anchorage.

### `voyages`

```bash
ais-tracks $CAT voyages [--no-declared] [--apply]
```

The legs between a vessel's consecutive stops, rebuilt in full on each run.
Every stretch of a vessel's observed life belongs to exactly one leg:

- the leg **before its first stop** (`origin_known` false), when it was first
  seen moving;
- the legs **between stops**;
- the leg **after its last stop**, once it has moved on (`is_open` true,
  destination unknown);
- for a vessel that **never stopped**, one leg with neither end known.

| Column | Meaning |
|--------|---------|
| `voyage_id`, `mmsi`, `depart_ts`, `arrive_ts`, `duration_s` | `arrive_ts` is null on an open leg |
| `origin_known`, `dest_known`, `is_open` | how much of the leg is certain |
| `origin_stop_id`, `dest_stop_id` | |
| `origin_lat/lon`, `origin_port_*`, `dest_lat/lon`, `dest_port_*` | the stops' centroids and matched ports |
| `distance_nm_raw`, `distance_nm_clean`, `avg_speed_kn`, `max_sog_knots` | from `track_points`, so exact for the leg; raw and clean as in `tracks` |
| `n_points`, `n_gaps`, `n_outliers` | |
| `declared_destination`, `n_declared_destinations`, `declared_eta` | the destination vessels reported most often during the leg (trailing `@` padding removed), how many different ones there were, and the latest ETA reported for it |
| `declared_matches_dest` | text heuristic comparing the declared destination with the reached port's UN/LOCODE and name; null when either side is missing. A hint, not a verdict |
| `computed_at` | |

`--no-declared` skips the scan of the `statics` table.

## Running at scale: `reduce-day`

The SQL steps above sort and window a whole day in DataFusion, which does not
fit in memory at hundreds of millions of reports a day. `reduce-day` is the
memory-bounded route to the same annotations, with optional thinning:

```bash
# See what thinning would keep on a real day (writes nothing):
ais-tracks $CAT reduce-day --day 2026-03-10

# Write the reduced rows as Parquet, or work from a local directory:
ais-tracks reduce-day --source-dir ./silver --day 2026-03-10 --out-dir ./out
```

It makes one sequential pass over the day, writing each report to one of a few
hundred scratch files chosen by a hash of the MMSI (a fixed-width 31-byte
record, zstd-compressed, no payload text). Then it loads one bucket at a time,
sorts it, and reduces each vessel in a single pass. Memory is one bucket plus a
small output chunk, whatever the size of the day, and silver need not be sorted
or compacted. Reports with an implausible MMSI (zero, more than nine digits) are
set aside and counted, so a bogus id cannot form a giant fake vessel.

The reducer computes what `track-points` computes (duplicate ranks, movement,
gap / jump / spike / invalid flags) and a test holds the two to row-for-row
agreement on randomised days when thinning is off. With thinning on (the
default), only some rows are kept, and each kept row carries what it stands for:

| Keep a row when | Flag | Default |
|---|---|---|
| it is the vessel's first or last positioned row of the day | | |
| the silence before it exceeds the gap limit, or it is the row just before one | `--gap-minutes` | 30 |
| it is a jump, spike or invalid value, or is next to one | `--max-speed-kn` | 60 |
| the vessel moved this far since the last kept row | `--keep-distance-nm` | 0.1 |
| this long has passed since the last kept row | `--keep-interval-s` | 120 |
| the course changed this much while moving | `--keep-turn-deg` | 15 |
| the speed changed this much | `--keep-speed-kn` | 2 |
| the navigation status changed | | |

`--no-thin` keeps every row. On a kept row `dist_nm` is the summed raw hop
distance since the previous kept row, so distance totals stay exact; `dt_s` is
the time since the previous kept row; `n_raw` is the number of reports it stands
for; `n_collapsed_dups`, `n_no_position` and `n_outliers_raw` split that count;
`sum_speed` / `n_speed` give a count-weighted mean speed; `max_dev_nm` is how far
a collapsed report was from the kept one; `keep_reason` is a bitmask. Duplicates
(the same message heard by another receiver) are collapsed into the kept row and
counted, not kept; they are only detected within the day.

The report printed at the end shows how many rows each policy keeps and why, and
checks that `represented` equals `routed`. Run it dry on a real day first: how
much thinning saves depends on your traffic mix (many reports are duplicates
from overlapping receivers, and moored vessels are already sparse).

Measured on synthetic days from `gen-day` (a mix of moored, underway and class B
vessels, 40% of reports duplicated across receivers), release build, one machine:

| Reports | Buckets | Kept | Time | Peak memory | Scratch |
|---|---|---|---|---|---|
| 2.4 M | 1 | 26.9% | 3 s | 302 MB | 0.05 GB |
| 61 M | 21 | 27.1% | 44 s | 335 MB | 1.3 GB |
| 183 M | 61 | 27.1% | 131 s | 431 MB | 3.8 GB |

Memory stays flat as the day grows because it is set by the bucket size
(`--target-bucket-rows`, default 3 million reports, about 56 bytes each while
loaded), not by the day. Scratch is about 21 bytes per report, so a 500 M report
day needs roughly 10 GB. These are synthetic numbers; the retention on real
traffic will differ. `gen-day --out DIR --rows N` writes such a day.

`reduce-day` is the same reduction as `track-points`, but reads a local
directory if you want, and writes a Parquet file rather than an Iceberg table:
use it to tune the thinning on a real day before running `track-points`. The
downstream tables read thinned rows: `tracks`, `stop-segments` and `voyages`
weight by `n_raw` and use the carried sums, so counts, sums and distances agree
with the unthinned answer. Stop and leg boundaries blur by up to about half the
speed-smoothing window (5 minutes) with dense reports, and thinning adds up to
one `--keep-interval-s` to that; this is tested (`tests/pipeline.rs`).

## `daily` and catch-up

`track-points`, `tracks` and `stop-segments` take the same day selection, and
`daily` runs the three in order for it:

| Flag | Meaning |
|------|---------|
| `--from D [--to D]` | exactly these days, always rebuilt |
| `--catch-up` | the days that have input and need building (below), up to yesterday |
| `--full` | with `--catch-up`, rebuild every day in range whatever the log says |
| `--include-today` | with `--catch-up`, include today (UTC), which is still filling |
| `--plan` | say which days would be built, and why, without building |

Two small tables make this work:

- **`vessel_state`** holds each vessel's last positioned report as of the end of
  each built day. The next day reads that one small partition instead of
  scanning the previous day's output.
- **`build_log`** gets one row per step and day, appended *after* the day's data
  is committed, so a day counts as built only once its row exists. A crash in
  between just means the day is rebuilt, which is safe because a rebuild
  replaces the day's partition. Each row records an `input_token` for what the
  day was built from.

A day is rebuilt when the token it would have now differs from the logged one.
For `track_points` the token is the day's silver row count plus a digest of the
vessel states it starts from, so late data in silver triggers a rebuild, and a
rebuilt earlier day triggers the next one **only if some vessel's end-of-day
position actually changed**: a late report that leaves every vessel's last
position alone does not ripple forward. For `tracks` and `stop_segments` the
token is the `track_points` build they read plus the previous day's build of the
same step, because their ids chain across midnight; they are rebuilt whenever
either neighbour was, which is conservative but simple. (Snapshot summaries would
be the usual place for this, but they describe the whole table.)

Days are built in date order. Days with no input are reported and skipped.
`--plan` cannot say which later days a rebuilt day will ripple into, since that
depends on the states it produces; it marks them "follows a rebuilt day".

Exit code `2` means nothing needed building, which suits a scheduler.

## Operating it

**Order.** `ports load` once per release. Then, per day, `track-points`,
`tracks`, `stop-segments` (or just `daily`), then `stops` and `voyages`. A
missing input is reported by name ("run track-points first").

**Daily runs.** Schedule `daily --catch-up --apply` after the day ends. It builds
yesterday, and rebuilds any earlier day whose silver data changed, with
everything else skipped. Follow it with `stops` and `voyages` (still full
rebuilds).

**Reruns and backfills.** Rerunning a day replaces only that day's partition,
and `--catch-up` works out what needs it. With explicit `--from/--to`, rebuilding
an earlier day means rebuilding the days after it, or their chains will point at
stale ids (`--catch-up` does this for you). A run of consecutive days carries the
previous day's state in memory; a run that starts mid-history reads it from
`vessel_state`. Where the previous day is absent, the piece is flagged
(`chain_broken`) rather than guessed.

**Memory.** `track-points` holds one bucket at a time (`--target-bucket-rows`);
`tracks` and `stop-segments` split each day's vessels into `--shards` chunks
by `mmsi % shards` and hold one chunk. `stops` and `voyages` read `track_points`,
`tracks` and `stop_segments` in full, so their cost grows with history.

**Maintenance.** The daily tables write a few files per day. Compact them like
any other table, pointing `ais-compact` at the output namespace:

```bash
ais-compact $CAT --iceberg-namespace curated \
  --table track_points --table tracks --table stop_segments compact --apply
```

**Atomicity.** Every write is a single Iceberg snapshot that adds the new files
and removes the old ones, using the same hand-built `replace` commit as
`ais-compact` (iceberg-rust 0.9 has no overwrite action). A concurrent commit
makes it retry, up to four times; on failure the new files are deleted.

## Querying the results

Downsample points, keeping no duplicates or outliers:

```sql
SELECT * FROM track_points
WHERE ts >= TIMESTAMP '2026-03-01 00:00:00' AND ts < TIMESTAMP '2026-03-02 00:00:00'
  AND NOT is_duplicate AND NOT is_outlier AND has_position;
```

A whole segment that crosses midnight:

```sql
SELECT track_id, min(ts) AS start, max(ts_end) AS finish,
       sum(distance_nm_clean) AS nm, sum(n_rows) AS points
FROM tracks WHERE mmsi = 366123456 GROUP BY track_id ORDER BY start;
```

Points of a track (pieces are cut at midnight, so join on time):

```sql
SELECT p.* FROM tracks t
JOIN track_points p ON p.mmsi = t.mmsi AND p.ts BETWEEN t.ts AND t.ts_end
WHERE t.track_id = '366123456-1772668810000';
```

A vessel's port calls, longest first:

```sql
SELECT port_name, port_unlocode, arrive_ts, depart_ts, duration_s / 3600 AS hours
FROM stops WHERE mmsi = 366123456 AND port_id IS NOT NULL ORDER BY duration_s DESC;
```

Completed voyages between two ports, with how the declared destination fared:

```sql
SELECT mmsi, depart_ts, arrive_ts, distance_nm_clean, avg_speed_kn,
       declared_destination, declared_matches_dest
FROM voyages
WHERE origin_port_name = 'Rotterdam' AND dest_port_name = 'New York' AND NOT is_open;
```

Suspect identities:

```sql
SELECT v.mmsi, v.name, a.value AS imo, a.n_obs
FROM vessels v JOIN vessel_attributes a ON a.mmsi = v.mmsi AND a.attribute = 'imo'
WHERE v.multiple_imos ORDER BY v.mmsi, a.rank;
```

## Limits and known gaps

- **Movement-based stops are a heuristic.** The defaults (0.5 kn, 10-minute
  smoothing, 30-minute minimum) are starting values, not tuned on your data.
  Inspect `radius_nm`, `duration_s` and the nav-status counts before trusting a
  stop as a port call. A short stop split across midnight can be missed on one
  side.
- **Duplicates are collapsed within a day only**, so a report whose twin is in the
  neighbouring day is counted twice.
- **The last point of a day cannot be tested for a jump out**, so it is never
  flagged `is_spike`; the same goes for the newest data in an open day.
- **A vessel silent for longer than `--lookback-days`** starts with a null
  `prev_ts` and `gap_before`.
- **Tracks are cut at UTC midnight.** Use `track_id` to see the whole segment.
- **`vessel_key` and identity are not time-aware.** One row per MMSI with the
  candidate history in `vessel_attributes`; there are no intervals for
  identity changes, and `mid` is not yet mapped to a country name.
- **`stops` and `voyages` are full rebuilds**, not incremental.
- **Port matching is proximity, not a port-call record.** Radii are fixed by
  harbour size, and unmatched stops keep null ports.
- **`declared_matches_dest` is text matching.** Declared destinations are free
  text and often stale.
- **Antimeridian.** Stop centroids are correct across it; `tracks` bounding
  boxes are not (see `bbox_wraps`).

Possible later addition: a separate activity label per stop (moored, anchored,
fishing) from a learned model such as AI2's
[Atlantes](https://allenai.org/blog/atlantes), joined in beside the
movement-based definitions rather than replacing them.

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | success |
| `1` | error (bad configuration, catalog failure, missing input table) |
| `2` | nothing to do: no day in the range had data |

## Source map

| Path | Role |
|------|------|
| `crates/ais-tracks/src/main.rs` | CLI and command wiring |
| `src/vessels.rs` | `vessels` and `vessel_attributes` SQL and schemas |
| `src/ports.rs` | World Port Index loader and `ref_ports` schema |
| `src/track_points.rs` | duplicate, movement and outlier SQL |
| `src/tracks.rs` | segment roll-up SQL |
| `src/stops.rs` | `stop_segments` and `stops` SQL, port matching |
| `src/voyages.rs` | leg construction SQL |
| `src/carry.rs` | previous-day state and schema checks |
| `src/output.rs` | create/replace commits (whole table, or one day partition) |
| `tests/pipeline.rs` | the whole chain on synthetic AIS |
