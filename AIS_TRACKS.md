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
> commit endpoint). Those end-to-end tests fold a multi-vessel fleet in day by
> day and compare the incremental `vessels`, `stops` and `voyages` with a
> from-scratch computation after every day, and cover reruns, late data and
> forced ranges. What has not been exercised is a live REST/S3 catalog such as
> RustFS or Lakekeeper: the commits reuse [`ais-compact`](AIS_COMPACT.md)'s code,
> but try `--apply` on a scratch namespace first.

## Contents

- [Design rules](#design-rules)
- [The tables and how they connect](#the-tables-and-how-they-connect)
- [Quick start](#quick-start)
- [Namespaces and table names](#namespaces-and-table-names)
- [Commands](#commands): [`statics-daily` and `vessels`](#statics-daily-and-vessels), [`ports load`](#ports-load),
  [`track-points`](#track-points), [`tracks`](#tracks),
  [`stop-segments`](#stop-segments), [`stops` and `voyages`](#stops-and-voyages),
  [`daily` and catch-up](#daily-and-catch-up), and
  [`reduce-day`](#running-at-scale-reduce-day)
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
  written without `--apply` (`reduce-day` writes only if given `--out-dir`).

## The tables and how they connect

Every column, with types and nullability, is listed in [SCHEMAS.md](SCHEMAS.md#3-derived-ais-tracks).

| Table | One row per | Built | Partitioned |
|-------|-------------|-------|-------------|
| `vessel_attributes` | value a vessel ever reported for an identity attribute | folded in daily | none |
| `vessels` | MMSI | folded in daily | none |
| `vessel_daily` | vessel and day: first/last seen, reports, and the day's movement totals | daily (from `track-points`) | day of `ts` |
| `attribute_daily`, `static_daily` | vessel and day: what its static reports said | daily (`statics-daily`) | day of `ts` |
| `ref_ports` | port, per World Port Index release | appended per release | none |
| `track_points` | kept report (each stands for one or more `positions` rows) | daily | day of `ts` |
| `tracks` | continuous segment, per UTC day | daily | day of `ts` (first row) |
| `stop_segments` | stationary run, per UTC day | daily | day of `ts` (first point) |
| `stops` | stop (the day pieces merged) | folded in daily | day of `depart_ts` |
| `voyages` | leg that has ended | folded in with `stops` | day of `arrive_ts` |
| `open_voyages` | leg still under way | replaced each run | none |
| `voyage_state` | vessel: where its current leg began and its totals so far (internal) | replaced each run | none |
| `destination_daily` | vessel and day: destinations it declared | daily (`statics-daily`) | day of `ts` |
| `vessel_state` | vessel's last positioned report, as of the end of a built day | daily | day it is as of |
| `build_log` | step and day built, with what it was built from | appended after each build | none |

```
positions ─► track_points ─┬─► tracks
   │                       └─► stop_segments ─► stops ─┬─► voyages / open_voyages
   ├─► vessel_daily ──────────────────────────────────┘        ▲   (state: voyage_state)
   └─► vessel_state (carried into the next day)                 │
statics ─┬─► attribute_daily, static_daily ─► vessels, vessel_attributes
         └─► destination_daily ───────────────────────────────────┘ (declared destinations)
ref_ports ─► stops (port match)
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
OUT="--output-namespace curated"

# One-off: reference data (download Pub 150 from the NGA first)
ais-tracks $CAT $OUT ports load --file UpdatedPub150.csv --release 2026-03-01 --apply

# Then everything, for every completed day that is new or whose input changed:
ais-tracks $CAT $OUT daily --catch-up --apply
```

`daily` runs `track-points`, `statics-daily`, `tracks` and `stop-segments`, then
`stops` and `voyages` together, then `vessels`. Say which days that would be,
and why, without building anything:

```bash
ais-tracks $CAT $OUT daily --catch-up --plan
```

Each step is also a command of its own, for a range of days or to rerun one:

```bash
D="--from 2026-03-01 --to 2026-03-31"
ais-tracks $CAT $OUT track-points   $D --apply    # from silver positions
ais-tracks $CAT $OUT statics-daily  $D --apply    # from silver statics
ais-tracks $CAT $OUT tracks         $D --apply
ais-tracks $CAT $OUT stop-segments  $D --apply
ais-tracks $CAT $OUT stops          --apply       # also folds voyages in
ais-tracks $CAT $OUT vessels        --apply       # folds the daily aggregates in
```

Drop `--apply` from any line to see what it would do first. Before pointing it at
a real day's worth of data, `reduce-day` (below) reports how much thinning would
keep without writing anything.

## Namespaces and table names

| Flag | Env | Default | Meaning |
|------|-----|---------|---------|
| `--iceberg-namespace` | `ICEBERG_NAMESPACE` | `ais` | where `positions` and `statics` are read |
| `--output-namespace` | `OUTPUT_NAMESPACE` | the input namespace | where derived tables are written |
| `--output-table-prefix` | `OUTPUT_TABLE_PREFIX` | none | prefix on derived table names (`v2` → `v2_vessels`) |

The remaining catalog flags (`--iceberg-catalog-uri`, `--iceberg-warehouse`,
`--iceberg-token`, `--iceberg-sigv4`, …) are shared with the other binaries;
see [CLI_REFERENCE.md](CLI_REFERENCE.md#iceberg-icebergcliargs-all-binaries).
The namespace is created if it does not exist. Tables carry no fixed prefix, so
the namespace is what separates one set of derived tables from another (say, one
per experiment).

`--iceberg-table-prefix` applies to the silver input tables only.

## Commands

All commands accept the catalog and namespace flags above, and are dry runs
unless `--apply` is given. `track-points`, `statics-daily`, `tracks` and
`stop-segments` also take the day-selection flags described under
[`daily` and catch-up](#daily-and-catch-up).

### `statics-daily` and `vessels`

```bash
ais-tracks $CAT statics-daily --catch-up --apply
ais-tracks $CAT vessels [--full] [--from-silver] [--plan] [--apply]
```

Vessel identity from static and position reports, kept up to date without
rescanning history. Each built day leaves small tables behind:

- **`vessel_daily`**, from the reduce pass (so no extra scan): when each vessel
  was first and last heard that day and how many reports it sent, duplicates
  and position-less reports included; plus the day's movement totals
  (first and last positioned row, points, distance raw and clean, gaps,
  outliers, top speed), which `voyages` uses.
- **`attribute_daily`** and **`static_daily`**, from `statics-daily`, which reads
  only that day's static reports: every distinct value a vessel reported for
  each identity attribute with counts, its static-report summary, and
  (`destination_daily`) the destinations it declared.

`vessels` and `vessel_attributes` are a fold of those. Every measure is a sum,
minimum or maximum, so a new day merges into the existing tables: the cost
follows the number of vessels, not the length of history. Both tables record
`folded_through`, the last day they include. `vessels` falls back to refolding
every daily table when there is nothing to increment from, when the two tables
disagree about how far they are folded (a crash between writing them), when a
daily table that was already folded in has been rebuilt since (late data), or
with `--full`. `--from-silver` builds from the whole silver tables instead: it
reads all of history, so it is for small deployments and for checking the
incremental result (which the tests do). Reports with an implausible MMSI are
set aside by `track-points` and are not counted.

Nothing is dropped or de-duplicated in the merge, and the two routes give the
same answer.

**`vessel_attributes`** keeps every distinct value a vessel ever reported for
an identity attribute, so nothing is decided away:

| Column | Meaning |
|--------|---------|
| `mmsi`, `attribute`, `value` | `attribute` is one of `name`, `call_sign`, `imo`, `ship_type`, `length_m`, `beam_m` |
| `n_obs`, `first_seen`, `last_seen` | how often and when it was reported |
| `rank`, `is_current` | rank 1 is the current value: for `imo`, one that passes its check digit beats one that does not; then the most often reported value; then the most recent |
| `folded_through`, `computed_at` | the last day folded in, and when |

`@` padding and blank strings count as "not reported". Length is bow + stern
and beam is port + starboard, from the reported dimensions.

**`vessels`** has one row per MMSI seen in either input:

| Column | Meaning |
|--------|---------|
| `mmsi`, `mmsi_class`, `mid` | class is `ship`, `handheld`, `distress_beacon`, `craft_associated`, `aton`, `sar_aircraft`, `group`, `coast_station`, or `other`; `mid` is the maritime identification digits (flag-state code) where the class has one |
| `vessel_key` | `imo:N` when the IMO is valid, else `mmsi:N` |
| `imo_number`, `call_sign`, `name`, `ship_type`, `length_m`, `beam_m`, `ais_class` | current values |
| `first_seen`, `last_seen`, `n_positions` | from position reports |
| `first_static_seen`, `last_static_seen`, `n_statics` | from static reports |
| `mmsi_valid`, `imo_valid` | class is not `other`; IMO passes its check digit |
| `multiple_imos`, `multiple_names`, `multiple_call_signs` | more than one distinct value was reported |
| `computed_at`, `folded_through` | when, and the last day folded in |

Type 24 messages arrive in two halves (name; then type, call sign and
dimensions). Each attribute is resolved independently across all rows, so the
halves combine without special handling. Before any static report arrives,
`vessel_attributes` is not written and `vessels` holds positions only.

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
memory is one bucket, not the day. It also writes that day's `vessel_state` and
`vessel_daily`, and logs the day last.

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
| `keep_reason` | bitmask of why the row was kept: 1 first, 2 last, 4 gap, 8 flagged, 16 next to a flagged row, 32 distance, 64 interval, 128 turn, 256 speed change, 512 nav change, 1024 just before a gap (see [`reduce-day`](#running-at-scale-reduce-day)) |

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

### `stops` and `voyages`

```bash
ais-tracks $CAT stops [--full] [--plan] [--apply]     # `voyages` is the same command
```

One row per stop: the `stop_segments` of a stop merged, then matched to the
nearest port in the latest `ref_ports` release within a radius set by the
port's harbour size (Large 15 nm, Medium 10, Small 6, Very Small or unknown 4).

It folds in only the days not yet folded, and it folds `voyages` in with them,
day by day (see below). The table is partitioned by the day
a stop **ends**, so a stop that has finished never moves again, and a stop that
is still going advances one partition a day. Folding day D reads only D's pieces
and the rows of the stops they touch, recomputes those stops from the existing
row plus today's piece, matches ports for them, and rewrites two partitions: D
(the touched stops) and D-1 (without the stops that moved on). Rerunning a day
is harmless: a stop's existing row is left alone if it already includes the
piece. A first run, `--full`, or a rebuilt earlier day's segments **replay**:
`stops` is cleared and every day is folded in again, from the start, through the
same code (see Voyages below for why the two are folded together).

| Column | Meaning |
|--------|---------|
| `stop_id`, `mmsi`, `arrive_ts`, `depart_ts`, `duration_s` | |
| `n_segments`, `n_points` | how many day pieces and points |
| `lat`, `lon`, `radius_nm` | centroid, and the largest piece radius |
| `n_moored`, `n_anchored` | |
| `port_id`, `port_name`, `port_unlocode`, `port_country`, `port_distance_nm` | the nearest port in range |
| `port2_id`, `port2_distance_nm` | the runner-up |
| `wpi_release` | the release matched against |
| `computed_at` | |

There is no "vessel is still here" flag: it would go stale the moment the
vessel moved on. `voyages` derives it from when the vessel was last seen. A stop
with no port in range (an anchorage, an offshore platform) keeps null port
columns. The `port_*` columns describe proximity, not a port call: check
`duration_s`, `n_moored` and `n_anchored` if you need to tell a berth from an
anchorage.

#### Voyages

The legs between a vessel's consecutive stops. Every stretch of a vessel's
observed life belongs to exactly one leg:

- the leg **before its first stop** (`origin_known` false), when it was first
  seen moving;
- the legs **between stops**;
- the leg **after its last stop**, once it has moved on (`open_voyages`,
  destination unknown);
- for a vessel that **never stopped**, one leg with neither end known.

Legs that have ended are in `voyages`, written once into the partition of the day
they end and never changed. Legs still under way are in `open_voyages`, replaced
each run. Both have the same columns.

| Column | Meaning |
|--------|---------|
| `voyage_id`, `mmsi`, `depart_ts`, `arrive_ts`, `duration_s` | `arrive_ts` is null on an open leg |
| `origin_known`, `dest_known`, `is_open` | how much of the leg is certain |
| `origin_stop_id`, `dest_stop_id` | |
| `origin_lat/lon`, `origin_port_*`, `dest_lat/lon`, `dest_port_*` | the stops' centroids and matched ports |
| `distance_nm_raw`, `distance_nm_clean`, `avg_speed_kn`, `max_sog_knots` | over the points between the leg's ends; raw and clean as in `tracks` |
| `n_points`, `n_gaps`, `n_outliers` | |
| `declared_destination`, `n_declared_destinations`, `declared_eta` | the destination vessels reported most often while the leg lasted, how many different ones there were, and the latest ETA reported for it |
| `declared_matches_dest` | text heuristic comparing the declared destination with the reached port's UN/LOCODE and name; null when either side is missing. A hint, not a verdict |
| `computed_at` | |

**How it stays cheap.** Each vessel keeps a small state row in `voyage_state`:
where its current leg began, the last stop it left, and the leg's totals so far.
A day then does only what it changes. A vessel with no stop that day adds the
day's totals (`vessel_daily`) to its running totals and no point is read. A
vessel that reaches a new stop closes its leg there; one that leaves a stop
starts a leg; one seen for the first time starts a leg at its first positioned
row. Those legs begin or end part-way through the day, so their totals need that
day's points split at the exact times, which is one query over one day of
`track_points`, for those vessels only. Every point is read once, on its own
day, however long the leg. The results are exactly the ones a query over all of
history gives (the tests compare them after every day).

Two things follow from folding a day at a time:

- **A leg's destination stop is described as it stood when the leg ended.** Its
  identity (`dest_stop_id`, `dest_port_*`) is exact, but `dest_lat`, `dest_lon`
  and `dest_port_distance_nm` come from the stop's first day, and a stop that
  carries on is refined afterwards. Join `stops` on `dest_stop_id` for the
  final figures.
- **Declared destinations are counted per day**, the departure and arrival days
  in full, from `destination_daily`, and only for legs that have ended:
  `open_voyages` has none. Late static reports for a day that a leg has already
  been closed over do not refresh it.

A first run, `--full`, or a rebuilt earlier day **replays**: it clears `stops` and
folds every day from the start again through the same code.

## `daily` and catch-up

`track-points`, `statics-daily`, `tracks` and `stop-segments` take the same day
selection, and `daily` runs them in order for it (then `stops` and `voyages`
together, if `ref_ports` is loaded, and `vessels`):

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

`stops` and `voyages` are folded together, day by day, and log one row per day.
The token for a `stops` day is the `stop_segments` build it read. They fold in
new days as an increment, and replay every day from the start when an
already-folded day's segments were rebuilt, when nothing is folded yet, or when
the voyage state is missing or out of step. `vessels` folds in new days, and
refolds when a folded day's aggregates were rebuilt since.

The bookkeeping tables (you rarely need to read them, but they are ordinary
tables):

| Table | Columns |
|-------|---------|
| `vessel_state` | `ts` (the day it is as of), `mmsi`, `last_ts`, `lat`, `lon`: the vessel's last positioned report that day |
| `build_log` | `step`, `day`, `input_token`, `output_rows`, `built_at`: one row per step and day built; the latest wins |
| `voyage_state` | one row per vessel: `mmsi`, `first_ts`, `last_ts` (first and last positioned row seen), `from_ts` (where its current leg began: the last stop's departure, or `first_ts`), `origin_*` (the last stop it left), the leg's running totals `dist_nm_raw`, `dist_nm_clean`, `max_sog_knots`, `n_points`, `n_gaps`, `n_outliers`, and `through` (the last day folded in) |
| `vessel_daily` | `ts` (the day), `mmsi`, `first_seen`, `last_seen`, `n_positions`, then the day's movement: `first_stream_ts`, `last_stream_ts`, `n_points`, `dist_nm_raw`, `dist_nm_clean`, `n_gaps`, `n_outliers`, `max_sog_knots` |
| `attribute_daily` | `ts`, `mmsi`, `attribute`, `value`, `n_obs`, `first_seen`, `last_seen` |
| `static_daily` | `ts`, `mmsi`, `first_static_seen`, `last_static_seen`, `n_statics`, `ais_class` |
| `destination_daily` | `ts`, `mmsi`, `destination`, `n` (times declared), `last_ts`, `eta` (the latest ETA given) |

Days are built in date order. Days with no input are reported and skipped.
`--plan` cannot say which later days a rebuilt day will ripple into, since that
depends on the states it produces; it marks them "follows a rebuilt day".

Exit code `2` means nothing needed building, which suits a scheduler.

## Running at scale: `reduce-day`

`track-points` is built for hundreds of millions of reports a day on a small
machine. `reduce-day` is the same reduction as a stand-alone command: run it on
a real day *without* `--out-dir` to see, before anything is written, how much
each thinning rule keeps and why.

```bash
# Report what thinning would keep on a real day (writes nothing):
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

The reducer computes what the original SQL did (duplicate ranks, movement, gap /
jump / spike / invalid flags), and a test holds the two to row-for-row
agreement on randomised days when thinning is off. With thinning on (the
default), only some rows are kept, and each kept row carries what it stands for.
A stream row is kept when any of these hold:

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
distance since the previous kept row, so distance totals stay exact; `n_raw` is
the number of reports it stands for; `n_collapsed_dups`, `n_no_position` and
`n_outliers_raw` split that count; `sum_speed` / `n_speed` and `sum_sog` /
`n_sog` give count-weighted mean speeds; `max_dev_nm` is how far a collapsed
report was from the kept one. Duplicates (the same message heard by another
receiver) are collapsed into the kept row and counted, not kept; they are only
detected within the day.

The report printed at the end shows how many rows each policy keeps and why, and
checks that `represented` equals `routed`. How much thinning saves depends on
your traffic mix (many reports are duplicates from overlapping receivers, and
moored vessels are already sparse), so run it dry on a real day first.

Everything downstream reads thinned rows and weights by `n_raw` and the carried
sums, so counts, sums and distances agree with the unthinned answer. Stop and leg
boundaries blur by up to about half the speed-smoothing window (5 minutes) with
dense reports, and thinning adds up to one `--keep-interval-s` to that; this is
tested (`tests/pipeline.rs`).

Measured on synthetic days from `gen-day` (a mix of moored, underway and class B
vessels, 40% of reports duplicated across receivers), release build, one machine,
reducing only:

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

## Operating it

**Order.** `ports load` once per release. Then, per day, `track-points`,
`statics-daily`, `tracks`, `stop-segments`, then `stops` (which folds in
`voyages`) and `vessels`: or just `daily`. A missing input is reported by name
("run track-points first").

**Daily runs.** Schedule `daily --catch-up --apply` after the day ends. It builds
yesterday, folds it into `stops` and `vessels`, and rebuilds any earlier day
whose silver data changed, with everything else skipped. Nothing in the chain is
a full rebuild any more.

**Reruns and backfills.** Rerunning a day replaces only that day's partition,
and `--catch-up` works out what needs it. With explicit `--from/--to`, rebuilding
an earlier day means rebuilding the days after it, or their chains will point at
stale ids (`--catch-up` does this for you). A run of consecutive days carries the
previous day's state in memory; a run that starts mid-history reads it from
`vessel_state`. Where the previous day is absent, the piece is flagged
(`chain_broken`) rather than guessed.

**Memory.** `track-points` holds one bucket at a time (`--target-bucket-rows`);
`tracks` and `stop-segments` split each day's vessels into `--shards` chunks
by `mmsi % shards` and hold one chunk. `stops`, `voyages` and `vessels` cost about a day plus the number of vessels.
Their replay and refold paths are history-linear but rare, and run with capped
memory (sorts and aggregates spill to `--scratch`; window functions do not).

**Maintenance.** The daily tables write a few files per day. Compact them like
any other table, pointing `ais-compact` at the output namespace:

```bash
ais-compact $CAT --iceberg-namespace curated \
  --table track_points --table tracks --table stop_segments --table stops --table voyages \
  compact --apply
```

**Atomicity.** Every write is a single Iceberg snapshot that adds the new files
and removes the old ones, using the same hand-built `replace` commit as
`ais-compact` (iceberg-rust 0.9 has no overwrite action). A concurrent commit
makes it retry, up to four times; on failure the new files are deleted. A day is
written table by table, and logged last, so a crash part-way is repaired by
rerunning it.

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
- **Stop and leg boundaries blur by up to about 5 minutes** with dense reports:
  stationary is judged on speed averaged over a 10-minute window. Thinning can add
  up to one `--keep-interval-s`.
- **Implausible MMSIs are set aside**, counted in the run's report and left out of
  every derived table.
- **A leg's destination stop is described as it stood the day the leg ended**
  (see Voyages); declared destinations are counted per day and only for closed
  legs, and late static reports for a day already folded do not refresh them.
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

Paths are under `crates/ais-tracks/`.

| Path | Role |
|------|------|
| `src/main.rs` | CLI and command wiring |
| `src/daily.rs` | the daily steps (`track-points`, `statics-daily`, `tracks`, `stop-segments`, `stops` + `voyages`, `vessels`), day selection, logging |
| `src/state.rs` | `vessel_state` and `build_log`, day selection, the state digest |
| `src/reduce.rs`, `src/router.rs`, `src/reduce_day.rs`, `src/source.rs` | the bounded-memory reducer: per-vessel state machine and thinning, on-disk bucket router, driver, silver and local-directory sources |
| `src/params.rs` | definitions shared by the reducer and the SQL |
| `src/track_points.rs` | the original SQL for `track_points`, kept as the test oracle |
| `src/tracks.rs` | segment roll-up SQL |
| `src/stops.rs` | `stop_segments` and `stops` SQL, port matching |
| `src/legs.rs` | voyage legs advanced a day at a time (state machine, interval sums, open and closed rows) |
| `src/voyages.rs` | the from-scratch voyage SQL, kept as the test oracle, and the `voyages` schema |
| `src/vessels.rs` | `vessels` and `vessel_attributes`, the daily aggregates, the merge |
| `src/ports.rs` | World Port Index loader and `ref_ports` schema |
| `src/carry.rs` | previous-day state and schema checks |
| `src/output.rs` | create/replace commits (whole table, or one day partition), streaming day writer |
| `src/bin/gen_day.rs` | `gen-day`, a synthetic day generator |
| `tests/` | `pipeline.rs` (chain, thinning on and off), `reduce_oracle.rs` (reducer vs SQL), `vessels_fold.rs`, `day_writer.rs`, `daily_e2e.rs` (the daily flow on real Iceberg tables, with `support/` holding the test catalog and scenarios) |
