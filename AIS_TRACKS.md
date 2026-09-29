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

> **Status.** The SQL is covered by tests on synthetic data, including a
> two-day run through every table. The Iceberg write paths reuse the commit
> code of [`ais-compact`](AIS_COMPACT.md), but have not yet been exercised
> against a live catalog: try `--apply` on a scratch namespace first.

## Contents

- [Design rules](#design-rules)
- [The tables and how they connect](#the-tables-and-how-they-connect)
- [Quick start](#quick-start)
- [Namespaces and table names](#namespaces-and-table-names)
- [Commands](#commands): [`vessels`](#vessels), [`ports load`](#ports-load),
  [`track-points`](#track-points), [`tracks`](#tracks),
  [`stop-segments`](#stop-segments), [`stops`](#stops), [`voyages`](#voyages)
- [Operating it](#operating-it)
- [Querying the results](#querying-the-results)
- [Limits and known gaps](#limits-and-known-gaps)
- [Exit codes](#exit-codes)
- [Source map](#source-map)

## Design rules

- **Annotate, never drop.** `track_points` has exactly one row per silver
  `positions` row. Duplicates, outliers and gaps are marked in columns, not
  removed, so a later run (downsampling, cleaning) decides what to discard:
  `WHERE NOT is_duplicate AND NOT is_spike`. The summary tables (`tracks`,
  `stops`, `voyages`) count what they cover, give distances and bounding boxes
  both raw and with outliers left out, and say when something is uncertain
  rather than guessing.
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
| `track_points` | silver `positions` row | daily | day of `ts` |
| `tracks` | continuous segment, per UTC day | daily | day of `ts` (first row) |
| `stop_segments` | stationary run, per UTC day | daily | day of `ts` (first point) |
| `stops` | stop (the day pieces merged) | rebuilt each run | none |
| `voyages` | leg between stops | rebuilt each run | none |

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

Each silver `positions` row, one for one, with every silver column unchanged
and these added. Partitioned by day on `ts`, sorted by `mmsi, ts`.

| Column | Meaning |
|--------|---------|
| `has_position` | latitude and longitude present and in range |
| `dup_rank`, `n_dups`, `is_duplicate` | rows with the same mmsi, ts, position, sog, cog, heading and nav status are the same message heard more than once (different `source`, `station` or `payload`). Rank 1 is the first, ordered by source, station, payload |
| `prev_ts`, `dt_s`, `dist_nm`, `implied_speed_kn` | movement since the vessel's previous *stream* point: the previous row that has a position and is not a duplicate. Null for duplicates and rows without a position |
| `gap_before` | first stream point, or `dt_s` above `--gap-minutes` |
| `is_speed_jump` | implied speed above `--max-speed-kn`, or two positions more than 0.05 nm apart in the same second |
| `is_spike` | a jump *into* the point and a jump *out* of it: an isolated bad fix |
| `is_sog_invalid`, `is_cog_invalid`, `is_heading_invalid` | value outside the AIS range |
| `is_outlier` | `is_spike`, or any invalid sog, cog or heading |

The return leg after a spike is flagged `is_speed_jump` but not `is_spike`,
because it is measured from the bad point; the spike is the row to discard.

| Flag | Default | Meaning |
|------|---------|---------|
| `--from`, `--to` | | first and last UTC day (inclusive); `--day` is an alias for `--from` |
| `--shards` | `4` | split each day's vessels by `mmsi % shards` to bound memory; output is identical for any value |
| `--lookback-days` | `2` | how far back to look for a vessel's previous point |
| `--max-speed-kn` | `60` | implied speed above which a point is a jump |
| `--gap-minutes` | `30` | a longer silence sets `gap_before` |

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
| `n_rows`, `n_stream`, `n_duplicates`, `n_no_position` | every `track_points` row in the piece is counted; `n_stream` are the positioned, non-duplicate ones |
| `n_jumps`, `n_spikes`, `n_outliers` | flag counts |
| `start_lat/lon`, `end_lat/lon` | first and last positioned point |
| `distance_nm_raw` | sum of hops between positioned points, outliers included |
| `distance_nm_clean` | the same leaving out every hop flagged `is_speed_jump` (a spike removes both its legs, so it slightly undercounts) |
| `min/max_lat/lon`, `clean_*` | bounding box over all positioned points, and over those not flagged `is_outlier` |
| `bbox_wraps` | longitude span over 180°: the piece probably crosses the antimeridian, so min/max longitude are not a usable box |
| `mean_sog_knots`, `max_sog_knots` | reported speed, leaving out invalid values |

Rows in a stretch with no positioned point of their own (a position-less
message between two gaps) fall in no piece. The run prints how many; the
`track_points` rows are untouched. Flags: `--from`, `--to`, `--shards`.

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

## Operating it

**Order.** `ports load` once per release. Then `track-points`, `tracks` and
`stop-segments` for each day in date order, then `stops` and `voyages`. A
missing input is reported by name ("run track-points first").

**Daily runs.** After a day is complete, run the three daily steps for it, then
`stops` and `voyages`. Days that are not over yet can be built (`track-points` prints a
note) and are rebuilt when run again.

**Reruns and backfills.** Rerunning a day replaces only that day's partition.
Because `tracks` and `stop_segments` inherit ids from the previous day,
rebuilding an earlier day means rebuilding the days after it, or their chains
will point at stale ids. A run of consecutive days carries the previous day's
state in memory; a run that starts mid-history reads it from the table. Where
the previous day is absent, the piece is flagged (`chain_broken`) rather than
guessed.

**Memory.** Each day is split into `--shards` chunks by `mmsi % shards`, and
each chunk is held in memory while it is written. Raise `--shards` for busy
days. `stops` and `voyages` read `track_points`, `tracks` and `stop_segments`
in full, so their cost grows with history.

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
