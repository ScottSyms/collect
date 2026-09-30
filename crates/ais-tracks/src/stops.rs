//! `stop_segments` and `stops`: where and when vessels were stationary.
//!
//! Stops are found from movement, not from what a vessel declares: a stop is a
//! run of a vessel's positioned points whose smoothed speed stays below a
//! threshold. Smoothing keeps berth jitter from splitting one stop into many;
//! a reporting gap does not end a stop if the vessel is still where it was.
//! Duplicates and isolated bad fixes (`is_spike`) are ignored, never deleted.
//!
//! `stop_segments` is built one UTC day at a time (like `tracks`), each piece
//! chained to the previous day's with a shared `stop_id`. `stops` merges the
//! pieces into one row per stop, and matches it to the nearest World Port Index
//! port within a radius set by the port's harbour size.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration, Utc};
use datafusion::prelude::SessionContext;
use iceberg::spec::{NestedField, PrimitiveType, Schema};

use crate::carry::{self, lit};
use crate::track_points::haversine;

pub const TABLE_STOP_SEGMENTS: &str = "stop_segments";
pub const TABLE_STOPS: &str = "stops";
const PREV_STOP_SEGMENTS: &str = "prev_stop_segments";

/// Match radius (nautical miles) around a port, by its harbour size.
const RADIUS_LARGE_NM: f64 = 15.0;
const RADIUS_MEDIUM_NM: f64 = 10.0;
const RADIUS_SMALL_NM: f64 = 6.0;
const RADIUS_OTHER_NM: f64 = 4.0;

fn required(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::required(id, name, ty.into()))
}

fn optional(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::optional(id, name, ty.into()))
}

/// Column order here is the column order of [`segments_sql`]'s result. `ts`,
/// the piece's first point, must stay column 0: the table is partitioned by
/// day on it.
pub fn stop_segments_schema() -> Schema {
    use PrimitiveType::*;
    let fields = vec![
        required(1, "ts", Timestamptz),
        required(2, "mmsi", Long),
        required(3, "stop_id", String),
        required(4, "ts_end", Timestamptz),
        required(5, "duration_s", Double),
        required(6, "continues_previous", Boolean),
        required(7, "open_at_day_end", Boolean),
        required(8, "n_points", Int),
        required(9, "lat", Double),
        required(10, "lon", Double),
        required(11, "radius_nm", Double),
        required(12, "n_moored", Int),
        required(13, "n_anchored", Int),
        optional(14, "mean_speed_kn", Double),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building stop_segments schema")
}

/// Column order here is the column order of [`stops_sql`]'s result.
pub fn stops_schema() -> Schema {
    use PrimitiveType::*;
    let fields = vec![
        required(1, "stop_id", String),
        required(2, "mmsi", Long),
        required(3, "arrive_ts", Timestamptz),
        required(4, "depart_ts", Timestamptz),
        required(5, "duration_s", Double),
        required(6, "n_segments", Int),
        required(7, "n_points", Long),
        required(8, "lat", Double),
        required(9, "lon", Double),
        required(10, "radius_nm", Double),
        required(11, "n_moored", Long),
        required(12, "n_anchored", Long),
        required(13, "is_current", Boolean),
        optional(14, "port_id", Long),
        optional(15, "port_name", String),
        optional(16, "port_unlocode", String),
        optional(17, "port_country", String),
        optional(18, "port_distance_nm", Double),
        optional(19, "port2_id", Long),
        optional(20, "port2_distance_nm", Double),
        optional(21, "wpi_release", String),
        required(22, "computed_at", Timestamptz),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building stops schema")
}

/// What one day of stop detection is parameterised by.
#[derive(Debug, Clone)]
pub struct StopParams {
    /// Inclusive start of the day being built (UTC midnight).
    pub day_start: DateTime<Utc>,
    /// Vessels are split by `mmsi % shards` to bound memory.
    pub shards: u32,
    /// Smoothed speed below this counts as stationary.
    pub slow_kn: f64,
    /// Width of the centred window speed is averaged over.
    pub smooth: Duration,
    /// After a reporting gap, the vessel still counts as stopped if it is
    /// within this distance of where it was.
    pub resume_nm: f64,
    /// Runs shorter than this are not kept, unless they touch a day edge
    /// (they may continue across midnight). Zero keeps every run.
    pub min_stop: Duration,
}

/// The SQL building one shard of one day of `stop_segments`. Reads the
/// registered `track_points` and `prev_stop_segments` tables.
pub fn segments_sql(p: &StopParams, shard: u32) -> String {
    let ds = lit(p.day_start);
    let de = lit(p.day_start + Duration::days(1));
    let n = p.shards;
    let slow = p.slow_kn;
    let half = (p.smooth.num_seconds() / 2).max(0);
    let resume = p.resume_nm;
    let min_s = p.min_stop.num_seconds();
    let order = "ts, latitude, longitude, sog_knots, source";
    let first = format!("ORDER BY {order}");
    let dev = haversine("latitude", "longitude", "c_lat", "c_lon");

    format!(
        "
WITH pts AS (
  SELECT mmsi, ts, latitude, longitude, sog_knots, source, nav_status, prev_ts, dist_nm,
         gap_before, sum_speed, n_speed, max_dev_nm,
         -- how many positioned reports this row stands for (1 when unthinned)
         (n_raw - n_collapsed_dups - n_no_position) AS w
  FROM track_points
  WHERE ts >= '{ds}' AND ts < '{de}' AND mmsi % {n} = {shard}
    AND has_position AND NOT is_duplicate AND NOT is_spike
),
smooth AS (
  SELECT pts.*,
    sum(sum_speed) OVER sw / nullif(sum(n_speed) OVER sw, 0) AS speed_smooth
  FROM pts
  WINDOW sw AS (PARTITION BY mmsi ORDER BY ts
    RANGE BETWEEN INTERVAL '{half} seconds' PRECEDING
              AND INTERVAL '{half} seconds' FOLLOWING)
),
marked AS (
  SELECT smooth.*,
    coalesce(speed_smooth < {slow}, false) AS is_slow,
    (gap_before AND coalesce(dist_nm, 1e9) > {resume}) AS displaced,
    CAST(row_number() OVER (PARTITION BY mmsi ORDER BY {order}) AS INT) AS rn,
    CAST(count(*) OVER (PARTITION BY mmsi) AS INT) AS n_rows
  FROM smooth
),
lagged AS (
  SELECT marked.*,
    lag(is_slow) OVER (PARTITION BY mmsi ORDER BY {order}) AS prev_slow
  FROM marked
),
runs AS (
  SELECT lagged.*, CAST(sum(CASE WHEN is_slow AND (NOT coalesce(prev_slow, false) OR displaced)
                                 THEN 1 ELSE 0 END) OVER (
      PARTITION BY mmsi ORDER BY {order}
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT) AS run_no
  FROM lagged
),
slowrows AS (
  SELECT runs.*,
    sum(latitude * w) OVER cw / sum(w) OVER cw AS c_lat,
    degrees(atan2(sum(sin(radians(longitude)) * w) OVER cw,
                  sum(cos(radians(longitude)) * w) OVER cw)) AS c_lon
  FROM runs WHERE is_slow
  WINDOW cw AS (PARTITION BY mmsi, run_no)
),
agg AS (
  SELECT mmsi, run_no,
    min(ts) AS ts, max(ts) AS ts_end,
    CAST(sum(w) AS INT) AS n_points,
    bool_or(rn = 1) AS at_day_start,
    bool_or(rn = n_rows) AS open_at_day_end,
    first_value(prev_ts {first}) AS first_prev_ts,
    first_value(displaced {first}) AS first_displaced,
    max(c_lat) AS lat, max(c_lon) AS lon,
    -- a thinned row also covers reports up to max_dev_nm away from it
    max({dev} + max_dev_nm) AS radius_nm,
    CAST(coalesce(sum(w) FILTER (WHERE nav_status ILIKE '%moored%'), 0) AS INT) AS n_moored,
    CAST(coalesce(sum(w) FILTER (WHERE nav_status ILIKE '%anchor%'), 0) AS INT) AS n_anchored,
    sum(sum_speed) / nullif(sum(n_speed), 0) AS mean_speed_kn
  FROM slowrows
  GROUP BY mmsi, run_no
)
SELECT a.ts, a.mmsi,
  CASE WHEN p.stop_id IS NOT NULL THEN p.stop_id
       ELSE concat(CAST(a.mmsi AS VARCHAR), '-', CAST(CAST(a.ts AS BIGINT) / 1000 AS VARCHAR))
  END AS stop_id,
  a.ts_end,
  (CAST(a.ts_end AS BIGINT) - CAST(a.ts AS BIGINT)) / 1000000.0 AS duration_s,
  p.stop_id IS NOT NULL AS continues_previous,
  a.open_at_day_end,
  a.n_points, a.lat, a.lon, a.radius_nm, a.n_moored, a.n_anchored, a.mean_speed_kn
FROM agg a
LEFT JOIN {PREV_STOP_SEGMENTS} p
  ON a.mmsi = p.mmsi AND a.at_day_start AND NOT a.first_displaced
 AND a.first_prev_ts = p.ts_end
WHERE {min_s} = 0
   OR (CAST(a.ts_end AS BIGINT) - CAST(a.ts AS BIGINT)) / 1000000.0 >= {min_s}
   OR a.at_day_start OR a.open_at_day_end
ORDER BY a.mmsi, a.ts"
    )
}

/// The SQL merging `stop_segments` into `stops` and matching ports. Reads the
/// registered `stop_segments`, `tracks` and `ref_ports` tables.
pub fn stops_sql() -> String {
    let seg_dist = haversine("s.lat", "s.lon", "p.plat", "p.plon");
    format!(
        "
WITH seg AS (
  SELECT mmsi, stop_id,
    min(ts) AS arrive_ts, max(ts_end) AS depart_ts,
    CAST(count(*) AS INT) AS n_segments, CAST(sum(n_points) AS BIGINT) AS n_points,
    sum(lat * n_points) / sum(n_points) AS lat,
    degrees(atan2(sum(sin(radians(lon)) * n_points), sum(cos(radians(lon)) * n_points))) AS lon,
    max(radius_nm) AS radius_nm,
    CAST(sum(n_moored) AS BIGINT) AS n_moored, CAST(sum(n_anchored) AS BIGINT) AS n_anchored
  FROM stop_segments GROUP BY mmsi, stop_id
),
seen AS (SELECT mmsi, max(ts_end) AS last_seen FROM tracks GROUP BY mmsi),
ports AS (
  SELECT port_id, name, unlocode, country, wpi_release,
    latitude AS plat, longitude AS plon,
    CASE harbor_size WHEN 'Large' THEN {RADIUS_LARGE_NM} WHEN 'Medium' THEN {RADIUS_MEDIUM_NM}
                     WHEN 'Small' THEN {RADIUS_SMALL_NM} ELSE {RADIUS_OTHER_NM} END AS radius_nm
  FROM ref_ports
  WHERE wpi_release = (SELECT max(wpi_release) FROM ref_ports)
    AND latitude IS NOT NULL AND longitude IS NOT NULL
),
-- Each port is copied into the 3x3 block of 2-degree cells around it, so a
-- stop finds its candidates with an equality join on its own cell.
pcells AS (
  SELECT ports.*,
    CAST(floor(plat / 2) AS INT) + dy.d AS cy,
    (CAST(floor(plon / 2) AS INT) + dx.d + 270) % 180 - 90 AS cx
  FROM ports
  CROSS JOIN (VALUES (-1), (0), (1)) AS dy(d)
  CROSS JOIN (VALUES (-1), (0), (1)) AS dx(d)
),
scell AS (
  SELECT seg.*, CAST(floor(lat / 2) AS INT) AS cy, CAST(floor(lon / 2) AS INT) AS cx FROM seg
),
cand AS (
  SELECT s.mmsi, s.stop_id, p.port_id, p.name, p.unlocode, p.country, p.wpi_release,
         p.radius_nm AS match_nm, {seg_dist} AS dist_nm
  FROM scell s JOIN pcells p ON s.cy = p.cy AND s.cx = p.cx
),
ranked AS (
  SELECT *, CAST(row_number() OVER (PARTITION BY mmsi, stop_id ORDER BY dist_nm, port_id) AS INT) AS rk
  FROM cand WHERE dist_nm <= match_nm
),
best AS (
  SELECT mmsi, stop_id,
    max(CASE WHEN rk = 1 THEN port_id END) AS port_id,
    max(CASE WHEN rk = 1 THEN name END) AS port_name,
    max(CASE WHEN rk = 1 THEN unlocode END) AS port_unlocode,
    max(CASE WHEN rk = 1 THEN country END) AS port_country,
    max(CASE WHEN rk = 1 THEN dist_nm END) AS port_distance_nm,
    max(CASE WHEN rk = 2 THEN port_id END) AS port2_id,
    max(CASE WHEN rk = 2 THEN dist_nm END) AS port2_distance_nm,
    max(CASE WHEN rk = 1 THEN wpi_release END) AS wpi_release
  FROM ranked WHERE rk <= 2 GROUP BY mmsi, stop_id
)
SELECT seg.stop_id, seg.mmsi, seg.arrive_ts, seg.depart_ts,
  (CAST(seg.depart_ts AS BIGINT) - CAST(seg.arrive_ts AS BIGINT)) / 1000000.0 AS duration_s,
  seg.n_segments, seg.n_points, seg.lat, seg.lon, seg.radius_nm, seg.n_moored, seg.n_anchored,
  coalesce(seen.last_seen <= seg.depart_ts, true) AS is_current,
  best.port_id, best.port_name, best.port_unlocode, best.port_country, best.port_distance_nm,
  best.port2_id, best.port2_distance_nm, best.wpi_release,
  now() AS computed_at
FROM seg
LEFT JOIN seen ON seg.mmsi = seen.mmsi
LEFT JOIN best ON seg.mmsi = best.mmsi AND seg.stop_id = best.stop_id
ORDER BY seg.mmsi, seg.arrive_ts"
    )
}

/// Registers the previous day's last stop piece per vessel.
pub async fn set_previous(
    ctx: &SessionContext,
    source: Option<(&str, Option<(DateTime<Utc>, DateTime<Utc>)>)>,
) -> Result<()> {
    carry::set_previous(ctx, PREV_STOP_SEGMENTS, "stop_id", source).await
}

/// Computes one shard of one day. `prev_stop_segments` must be registered.
pub async fn build_segments(
    ctx: &SessionContext,
    p: &StopParams,
    shard: u32,
) -> Result<Vec<RecordBatch>> {
    ctx.sql(&segments_sql(p, shard))
        .await
        .with_context(|| format!("planning stop_segments shard {shard}"))?
        .collect()
        .await
        .with_context(|| format!("computing stop_segments shard {shard}"))
}

/// Merges all `stop_segments` and matches ports.
pub async fn build_stops(ctx: &SessionContext) -> Result<Vec<RecordBatch>> {
    ctx.sql(&stops_sql())
        .await
        .context("planning stops")?
        .collect()
        .await
        .context("computing stops")
}
