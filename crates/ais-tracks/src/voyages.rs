//! `voyages`: the legs between a vessel's consecutive stops.
//!
//! Every stretch of a vessel's observed life belongs to exactly one leg: the
//! leg before its first stop (origin unknown), the legs between stops, and the
//! leg after its last stop while the vessel is still moving (destination
//! unknown, `is_open`). A vessel that never stopped gets a single leg with
//! neither end known. Nothing is dropped; `origin_known`, `dest_known` and
//! `is_open` say how much of a leg is certain.
//!
//! Distances come from `track_points`, so they are exact for the leg rather
//! than apportioned from whole `tracks` pieces. What a vessel *declared* as its
//! destination (from AIS static reports) is kept beside the destination it
//! actually reached; `declared_matches_dest` is a text heuristic, not a
//! verdict.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use iceberg::spec::{NestedField, PrimitiveType, Schema};

pub const TABLE_VOYAGES: &str = "voyages";

fn required(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::required(id, name, ty.into()))
}

fn optional(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::optional(id, name, ty.into()))
}

/// Column order here is the column order of [`voyages_sql`]'s result.
pub fn voyages_schema() -> Schema {
    use PrimitiveType::*;
    let fields = vec![
        required(1, "voyage_id", String),
        required(2, "mmsi", Long),
        required(3, "depart_ts", Timestamptz),
        optional(4, "arrive_ts", Timestamptz),
        optional(5, "duration_s", Double),
        required(6, "origin_known", Boolean),
        required(7, "dest_known", Boolean),
        required(8, "is_open", Boolean),
        optional(9, "origin_stop_id", String),
        optional(10, "dest_stop_id", String),
        optional(11, "origin_lat", Double),
        optional(12, "origin_lon", Double),
        optional(13, "origin_port_id", Long),
        optional(14, "origin_port_name", String),
        optional(15, "origin_unlocode", String),
        optional(16, "origin_country", String),
        optional(17, "origin_port_distance_nm", Double),
        optional(18, "dest_lat", Double),
        optional(19, "dest_lon", Double),
        optional(20, "dest_port_id", Long),
        optional(21, "dest_port_name", String),
        optional(22, "dest_unlocode", String),
        optional(23, "dest_country", String),
        optional(24, "dest_port_distance_nm", Double),
        optional(25, "distance_nm_raw", Double),
        optional(26, "distance_nm_clean", Double),
        optional(27, "avg_speed_kn", Double),
        optional(28, "max_sog_knots", Double),
        required(29, "n_points", Long),
        required(30, "n_gaps", Long),
        required(31, "n_outliers", Long),
        optional(32, "declared_destination", String),
        optional(33, "n_declared_destinations", Long),
        optional(34, "declared_eta", Timestamptz),
        optional(35, "declared_matches_dest", Boolean),
        required(36, "computed_at", Timestamptz),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building voyages schema")
}

/// The SQL building `voyages`. Reads the registered `stops`, `tracks` and
/// `track_points` tables, and `statics` when `declared` is true.
pub fn voyages_sql(declared: bool) -> String {
    let decl = if declared {
        "
dagg AS (
  SELECT voyage_id, dest, count(*) AS n, max(st_ts) AS last_ts, max(st_eta) AS eta FROM (
    SELECT l.voyage_id, st.ts AS st_ts, st.eta AS st_eta,
           regexp_replace(trim(st.destination), '[@ ]+$', '') AS dest
    FROM legs l JOIN statics st
      ON st.mmsi = l.mmsi
     AND st.ts >= date_trunc('day', l.depart_ts)
     AND st.ts < date_trunc('day', l.arrive_ts) + INTERVAL '1 day'
    WHERE l.arrive_ts IS NOT NULL AND st.destination IS NOT NULL
  ) x WHERE dest <> '' GROUP BY voyage_id, dest
),
dr AS (
  SELECT dagg.*,
    CAST(row_number() OVER (PARTITION BY voyage_id ORDER BY n DESC, last_ts DESC, dest) AS INT) AS rk,
    count(*) OVER (PARTITION BY voyage_id) AS n_dest
  FROM dagg
),
decl AS (
  SELECT voyage_id,
    max(CASE WHEN rk = 1 THEN dest END) AS declared_destination,
    max(n_dest) AS n_declared,
    max(CASE WHEN rk = 1 THEN eta END) AS declared_eta
  FROM dr GROUP BY voyage_id
)"
    } else {
        "
decl AS (
  SELECT CAST(NULL AS VARCHAR) AS voyage_id, CAST(NULL AS VARCHAR) AS declared_destination,
         CAST(NULL AS BIGINT) AS n_declared, CAST(NULL AS TIMESTAMP) AS declared_eta
  WHERE false
)"
    };
    let dn = "regexp_replace(upper(dc.declared_destination), '[^A-Z0-9]', '')";
    let nn = "regexp_replace(upper(d.port_name), '[^A-Z0-9]', '')";
    let un = "regexp_replace(upper(coalesce(d.port_unlocode, '')), '[^A-Z0-9]', '')";
    format!(
        "
WITH bounds AS (
  SELECT mmsi, min(ts) AS first_ts, max(ts) AS last_ts
  FROM track_points WHERE has_position AND NOT is_duplicate GROUP BY mmsi
),
ordered AS (
  SELECT stops.*,
    lead(stop_id) OVER w AS next_stop_id, lead(arrive_ts) OVER w AS next_arrive_ts,
    CAST(row_number() OVER w AS INT) AS stop_no
  FROM stops WINDOW w AS (PARTITION BY mmsi ORDER BY arrive_ts)
),
-- A vessel has moved on from its last stop if it was seen after the stop ended.
moved AS (
  SELECT o.stop_id, coalesce(b.last_ts > o.depart_ts, false) AS moved_on
  FROM ordered o LEFT JOIN bounds b ON o.mmsi = b.mmsi
),
legs0 AS (
  -- between consecutive stops, and after the last one if the vessel moved on
  SELECT ordered.mmsi, ordered.stop_id AS origin_stop_id, ordered.next_stop_id AS dest_stop_id,
         ordered.depart_ts, ordered.next_arrive_ts AS arrive_ts
  FROM ordered JOIN moved ON ordered.stop_id = moved.stop_id
  WHERE ordered.next_stop_id IS NOT NULL OR moved.moved_on
  UNION ALL
  -- before the first stop, if the vessel was seen moving first
  SELECT o.mmsi, CAST(NULL AS VARCHAR) AS origin_stop_id, o.stop_id AS dest_stop_id,
         b.first_ts AS depart_ts, o.arrive_ts
  FROM ordered o JOIN bounds b ON o.mmsi = b.mmsi
  WHERE o.stop_no = 1 AND o.arrive_ts > b.first_ts
  UNION ALL
  -- a vessel that never stopped
  SELECT b.mmsi, CAST(NULL AS VARCHAR) AS origin_stop_id, CAST(NULL AS VARCHAR) AS dest_stop_id,
         b.first_ts AS depart_ts, CASE WHEN false THEN b.first_ts END AS arrive_ts
  FROM bounds b WHERE NOT EXISTS (SELECT 1 FROM stops s WHERE s.mmsi = b.mmsi)
),
legs AS (
  SELECT legs0.*,
    concat(CAST(mmsi AS VARCHAR), '-', CAST(CAST(depart_ts AS BIGINT) / 1000 AS VARCHAR)) AS voyage_id
  FROM legs0
),
metrics AS (
  SELECT l.voyage_id,
    sum(tp.dist_nm) FILTER (WHERE tp.has_position AND NOT tp.is_duplicate AND NOT tp.gap_before)
      AS distance_nm_raw,
    sum(tp.dist_nm) FILTER (WHERE tp.has_position AND NOT tp.is_duplicate
                              AND NOT tp.gap_before AND NOT tp.is_speed_jump)
      AS distance_nm_clean,
    max(tp.max_sog) FILTER (WHERE tp.has_position AND NOT tp.is_duplicate) AS max_sog_knots,
    CAST(sum(tp.n_raw) AS BIGINT) AS n_points,
    count(*) FILTER (WHERE tp.gap_before) AS n_gaps,
    CAST(sum(tp.n_outliers_raw) AS BIGINT) AS n_outliers
  FROM legs l JOIN track_points tp
    ON tp.mmsi = l.mmsi AND tp.ts > l.depart_ts AND (l.arrive_ts IS NULL OR tp.ts <= l.arrive_ts)
  GROUP BY l.voyage_id
),{decl}
SELECT l.voyage_id, l.mmsi, l.depart_ts, l.arrive_ts,
  (CAST(l.arrive_ts AS BIGINT) - CAST(l.depart_ts AS BIGINT)) / 1000000.0 AS duration_s,
  l.origin_stop_id IS NOT NULL AS origin_known,
  l.dest_stop_id IS NOT NULL AS dest_known,
  l.arrive_ts IS NULL AS is_open,
  l.origin_stop_id, l.dest_stop_id,
  o.lat AS origin_lat, o.lon AS origin_lon, o.port_id AS origin_port_id,
  o.port_name AS origin_port_name, o.port_unlocode AS origin_unlocode,
  o.port_country AS origin_country, o.port_distance_nm AS origin_port_distance_nm,
  d.lat AS dest_lat, d.lon AS dest_lon, d.port_id AS dest_port_id,
  d.port_name AS dest_port_name, d.port_unlocode AS dest_unlocode,
  d.port_country AS dest_country, d.port_distance_nm AS dest_port_distance_nm,
  coalesce(m.distance_nm_raw, 0.0) AS distance_nm_raw,
  coalesce(m.distance_nm_clean, 0.0) AS distance_nm_clean,
  CASE WHEN l.arrive_ts IS NOT NULL AND l.arrive_ts > l.depart_ts
       THEN coalesce(m.distance_nm_clean, 0.0)
            / ((CAST(l.arrive_ts AS BIGINT) - CAST(l.depart_ts AS BIGINT)) / 3600000000.0)
  END AS avg_speed_kn,
  m.max_sog_knots,
  coalesce(m.n_points, 0) AS n_points, coalesce(m.n_gaps, 0) AS n_gaps,
  coalesce(m.n_outliers, 0) AS n_outliers,
  dc.declared_destination, dc.n_declared AS n_declared_destinations, dc.declared_eta,
  CASE WHEN dc.declared_destination IS NULL OR d.port_name IS NULL THEN NULL
       ELSE length({dn}) >= 4
            AND ({dn} = {un}
                 OR (length({nn}) >= 4 AND (strpos({dn}, {nn}) > 0 OR strpos({nn}, {dn}) > 0)))
  END AS declared_matches_dest,
  now() AS computed_at
FROM legs l
LEFT JOIN stops o ON l.origin_stop_id = o.stop_id AND l.mmsi = o.mmsi
LEFT JOIN stops d ON l.dest_stop_id = d.stop_id AND l.mmsi = d.mmsi
LEFT JOIN metrics m ON l.voyage_id = m.voyage_id
LEFT JOIN decl dc ON l.voyage_id = dc.voyage_id
ORDER BY l.mmsi, l.depart_ts"
    )
}

/// Builds every voyage.
pub async fn build(ctx: &SessionContext, declared: bool) -> Result<Vec<RecordBatch>> {
    ctx.sql(&voyages_sql(declared))
        .await
        .context("planning voyages")?
        .collect()
        .await
        .context("computing voyages")
}
