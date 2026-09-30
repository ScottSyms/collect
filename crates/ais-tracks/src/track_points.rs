//! `track_points`: the silver `positions` rows, one for one, annotated with
//! duplicate ranks, movement since the previous point, and outlier flags.
//!
//! No row is removed, merged or altered: the output has exactly as many rows
//! as the input day, every silver column is carried through unchanged, and the
//! extra columns only describe the row. A later run (downsampling, cleaning)
//! chooses what to discard, e.g. `WHERE NOT is_duplicate AND NOT is_spike`.
//!
//! Movement is measured along the vessel's *stream*: rows that have a valid
//! position and are the first occurrence of their message. Duplicates and
//! position-less rows get null movement columns, so a duplicate can't
//! produce a zero-second hop and a bad row can't break the chain.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, BooleanArray};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration, Utc};
use datafusion::prelude::SessionContext;
use iceberg::spec::{NestedField, PrimitiveType, Schema};

pub const TABLE_TRACK_POINTS: &str = "track_points";

use crate::params::{EARTH_RADIUS_NM, SAME_SECOND_JUMP_NM};

/// The silver columns carried through, in silver order.
const POSITION_COLUMNS: [&str; 20] = [
    "ts",
    "source",
    "msg_type",
    "mmsi",
    "ais_class",
    "latitude",
    "longitude",
    "sog_knots",
    "cog",
    "heading_true",
    "rot",
    "altitude_m",
    "h3",
    "hilbert",
    "nav_status",
    "high_accuracy",
    "raim",
    "special_manoeuvre",
    "station",
    "payload",
];

/// Boolean columns summarised in reports, and asserted on in tests.
pub const FLAG_COLUMNS: [&str; 9] = [
    "has_position",
    "is_duplicate",
    "gap_before",
    "is_speed_jump",
    "is_spike",
    "is_sog_invalid",
    "is_cog_invalid",
    "is_heading_invalid",
    "is_outlier",
];

fn required(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::required(id, name, ty.into()))
}

fn optional(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::optional(id, name, ty.into()))
}

/// Column order here is the column order of [`shard_sql`]'s result. `ts` must
/// stay column 0: the table is partitioned by day on it.
pub fn track_points_schema() -> Schema {
    let fields = vec![
        required(1, "ts", PrimitiveType::Timestamptz),
        required(2, "source", PrimitiveType::String),
        required(3, "msg_type", PrimitiveType::Int),
        required(4, "mmsi", PrimitiveType::Long),
        optional(5, "ais_class", PrimitiveType::String),
        optional(6, "latitude", PrimitiveType::Double),
        optional(7, "longitude", PrimitiveType::Double),
        optional(8, "sog_knots", PrimitiveType::Double),
        optional(9, "cog", PrimitiveType::Double),
        optional(10, "heading_true", PrimitiveType::Double),
        optional(11, "rot", PrimitiveType::Double),
        optional(12, "altitude_m", PrimitiveType::Double),
        optional(13, "h3", PrimitiveType::Long),
        optional(14, "hilbert", PrimitiveType::Long),
        optional(15, "nav_status", PrimitiveType::String),
        optional(16, "high_accuracy", PrimitiveType::Boolean),
        optional(17, "raim", PrimitiveType::Boolean),
        optional(18, "special_manoeuvre", PrimitiveType::Boolean),
        optional(19, "station", PrimitiveType::String),
        optional(20, "payload", PrimitiveType::String),
        required(21, "has_position", PrimitiveType::Boolean),
        required(22, "dup_rank", PrimitiveType::Int),
        required(23, "n_dups", PrimitiveType::Int),
        required(24, "is_duplicate", PrimitiveType::Boolean),
        optional(25, "prev_ts", PrimitiveType::Timestamptz),
        optional(26, "dt_s", PrimitiveType::Double),
        optional(27, "dist_nm", PrimitiveType::Double),
        optional(28, "implied_speed_kn", PrimitiveType::Double),
        required(29, "gap_before", PrimitiveType::Boolean),
        required(30, "is_speed_jump", PrimitiveType::Boolean),
        required(31, "is_spike", PrimitiveType::Boolean),
        required(32, "is_sog_invalid", PrimitiveType::Boolean),
        required(33, "is_cog_invalid", PrimitiveType::Boolean),
        required(34, "is_heading_invalid", PrimitiveType::Boolean),
        required(35, "is_outlier", PrimitiveType::Boolean),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building track_points schema")
}

/// What one day's computation is parameterised by.
#[derive(Debug, Clone)]
pub struct Params {
    /// Inclusive start of the day being built (UTC midnight).
    pub day_start: DateTime<Utc>,
    /// How far before `day_start` to look for each vessel's previous point.
    pub lookback: Duration,
    /// Vessels are split by `mmsi % shards` to bound memory; each shard is
    /// computed independently.
    pub shards: u32,
    /// An implied speed above this marks a point as a speed jump.
    pub max_speed_kn: f64,
    /// A gap longer than this starts a new segment (`gap_before`).
    pub gap: Duration,
}

impl Params {
    pub fn day_end(&self) -> DateTime<Utc> {
        self.day_start + Duration::days(1)
    }
}

fn lit(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S+00:00").to_string()
}

/// Great-circle distance in nautical miles between two lat/lon columns pairs.
pub fn haversine(lat1: &str, lon1: &str, lat2: &str, lon2: &str) -> String {
    format!(
        "2 * {EARTH_RADIUS_NM} * asin(least(1.0, sqrt(\
         power(sin(radians({lat2} - {lat1}) / 2), 2) \
         + cos(radians({lat1})) * cos(radians({lat2})) \
         * power(sin(radians({lon2} - {lon1}) / 2), 2))))"
    )
}

/// The SQL computing one shard of one day. Reads the registered `positions`
/// table.
pub fn shard_sql(p: &Params, shard: u32) -> String {
    let (ds, de, ls) = (
        lit(p.day_start),
        lit(p.day_end()),
        lit(p.day_start - p.lookback),
    );
    let n = p.shards;
    let cols = POSITION_COLUMNS.join(", ");
    let max_speed = p.max_speed_kn;
    let gap_s = p.gap.num_seconds();
    let dist = haversine("prev_lat", "prev_lon", "latitude", "longitude");
    // Two rows are the same message when everything the vessel transmitted
    // matches; which receiver heard it (source, station, payload) does not
    // count. `payload` and friends only order the copies.
    let content = "mmsi, ts, latitude, longitude, sog_knots, cog, heading_true, nav_status";
    // Total order within a vessel, so windows are deterministic.
    let order = "ts, latitude, longitude, dup_rank, source, station, payload";
    let rev_order =
        "ts DESC, latitude DESC, longitude DESC, dup_rank DESC, source DESC, station DESC, payload DESC";

    format!(
        "
WITH base AS (
  SELECT {cols},
    coalesce(latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180, false) AS has_position,
    CAST(row_number() OVER (PARTITION BY {content} ORDER BY source, station, payload) AS INT) AS dup_rank,
    CAST(count(*) OVER (PARTITION BY {content}) AS INT) AS n_dups
  FROM positions
  WHERE ts >= '{ds}' AND ts < '{de}' AND mmsi % {n} = {shard}
),
state AS (
  SELECT mmsi, ts AS st_ts, latitude AS st_lat, longitude AS st_lon FROM (
    SELECT mmsi, ts, latitude, longitude,
           row_number() OVER (PARTITION BY mmsi ORDER BY ts DESC) AS rn
    FROM positions
    WHERE ts >= '{ls}' AND ts < '{ds}' AND mmsi % {n} = {shard}
      AND latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180
  ) t WHERE rn = 1
),
joined AS (
  SELECT base.*, (has_position AND dup_rank = 1) AS in_stream,
         state.st_ts, state.st_lat, state.st_lon
  FROM base LEFT JOIN state ON base.mmsi = state.mmsi
),
prev AS (
  SELECT joined.*,
    last_value(CASE WHEN in_stream THEN ts END) IGNORE NULLS OVER pw AS w_ts,
    last_value(CASE WHEN in_stream THEN latitude END) IGNORE NULLS OVER pw AS w_lat,
    last_value(CASE WHEN in_stream THEN longitude END) IGNORE NULLS OVER pw AS w_lon
  FROM joined
  WINDOW pw AS (PARTITION BY mmsi ORDER BY {order}
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
),
hop AS (
  SELECT prev.*,
    CASE WHEN in_stream THEN coalesce(w_ts, st_ts) END AS prev_ts,
    CASE WHEN in_stream THEN coalesce(w_lat, st_lat) END AS prev_lat,
    CASE WHEN in_stream THEN coalesce(w_lon, st_lon) END AS prev_lon
  FROM prev
),
dist AS (
  SELECT hop.*,
    CASE WHEN prev_ts IS NOT NULL
         THEN (CAST(ts AS BIGINT) - CAST(prev_ts AS BIGINT)) / 1000000.0 END AS dt_s,
    CASE WHEN prev_ts IS NOT NULL THEN {dist} END AS dist_nm
  FROM hop
),
speed AS (
  SELECT dist.*,
    CASE WHEN dt_s > 0 THEN dist_nm / (dt_s / 3600.0) END AS implied_speed_kn,
    in_stream AND coalesce(
      CASE WHEN dt_s > 0 THEN dist_nm / (dt_s / 3600.0) > {max_speed}
           ELSE dist_nm > {SAME_SECOND_JUMP_NM} END, false) AS over_limit
  FROM dist
),
nxt AS (
  SELECT speed.*,
    last_value(CASE WHEN in_stream THEN over_limit END) IGNORE NULLS OVER (
      PARTITION BY mmsi ORDER BY {rev_order}
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS next_over_limit
  FROM speed
),
flags AS (
  SELECT nxt.*,
    over_limit AS is_speed_jump,
    over_limit AND coalesce(next_over_limit, false) AS is_spike,
    in_stream AND (prev_ts IS NULL OR dt_s > {gap_s}) AS gap_before,
    coalesce(sog_knots < 0 OR sog_knots > 102.2, false) AS is_sog_invalid,
    coalesce(cog < 0 OR cog >= 360, false) AS is_cog_invalid,
    coalesce(heading_true < 0 OR heading_true > 359, false) AS is_heading_invalid
  FROM nxt
)
SELECT {cols},
  has_position, dup_rank, n_dups, dup_rank > 1 AS is_duplicate,
  prev_ts, dt_s, dist_nm, implied_speed_kn,
  gap_before, is_speed_jump, is_spike,
  is_sog_invalid, is_cog_invalid, is_heading_invalid,
  (is_spike OR is_sog_invalid OR is_cog_invalid OR is_heading_invalid) AS is_outlier
FROM flags
ORDER BY mmsi, ts, dup_rank, source"
    )
}

/// Counts of each flag over some batches, for reporting.
#[derive(Debug, Default, Clone)]
pub struct FlagCounts {
    pub rows: usize,
    pub counts: Vec<(&'static str, usize)>,
}

impl FlagCounts {
    pub fn new() -> Self {
        Self {
            rows: 0,
            counts: FLAG_COLUMNS.iter().map(|c| (*c, 0)).collect(),
        }
    }

    pub fn add(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for b in batches {
            self.rows += b.num_rows();
            for (name, total) in &mut self.counts {
                let col = b
                    .column_by_name(name)
                    .with_context(|| format!("missing column {name}"))?;
                let col = col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .with_context(|| format!("{name} is not boolean"))?;
                *total += col.true_count();
            }
        }
        Ok(())
    }

    pub fn get(&self, name: &str) -> usize {
        self.counts
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, c)| *c)
            .unwrap_or(0)
    }
}

/// Computes one shard of one day.
pub async fn build_shard(ctx: &SessionContext, p: &Params, shard: u32) -> Result<Vec<RecordBatch>> {
    ctx.sql(&shard_sql(p, shard))
        .await
        .with_context(|| format!("planning track_points shard {shard}"))?
        .collect()
        .await
        .with_context(|| format!("computing track_points shard {shard}"))
}

/// Sanity check used by callers and tests: every batch has the table's
/// columns in order, and no required column holds a null.
pub fn check_against_schema(batches: &[RecordBatch]) -> Result<()> {
    let want = iceberg::arrow::schema_to_arrow_schema(&track_points_schema())?;
    for b in batches {
        anyhow::ensure!(
            b.num_columns() == want.fields().len(),
            "expected {} columns, got {}",
            want.fields().len(),
            b.num_columns()
        );
        for (i, (w, g)) in want.fields().iter().zip(b.schema().fields()).enumerate() {
            anyhow::ensure!(w.name() == g.name(), "column {i}: {} != {}", w.name(), g.name());
            anyhow::ensure!(
                w.is_nullable() || b.column(i).null_count() == 0,
                "required column {} contains nulls",
                w.name()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        new_null_array, Float64Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::util::display::array_value_to_string;
    use chrono::TimeZone;
    use datafusion::datasource::MemTable;

    fn day0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 10, 0, 0, 0).unwrap()
    }

    /// (mmsi, seconds from day start, lat, lon, source)
    type Pt = (i64, i64, Option<f64>, Option<f64>, &'static str);

    fn positions_table(rows: &[Pt]) -> MemTable {
        let schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(&collect_core::iceberg::table_schemas::positions_schema())
                .unwrap(),
        );
        let n = rows.len();
        let base = day0().timestamp() * 1_000_000;
        let cols: Vec<Arc<dyn Array>> = schema
            .fields()
            .iter()
            .map(|f| -> Arc<dyn Array> {
                match f.name().as_str() {
                    "ts" => Arc::new(
                        TimestampMicrosecondArray::from_iter_values(
                            rows.iter().map(|r| base + r.1 * 1_000_000),
                        )
                        .with_timezone("+00:00"),
                    ),
                    "source" => Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.4))),
                    "msg_type" => Arc::new(Int32Array::from(vec![1; n])),
                    "mmsi" => Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                    "latitude" => Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.2))),
                    "longitude" => Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.3))),
                    "payload" => Arc::new(StringArray::from_iter_values(
                        rows.iter().enumerate().map(|(i, r)| format!("{}-{i}", r.4)),
                    )),
                    _ => new_null_array(f.data_type(), n),
                }
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
        MemTable::try_new(schema, vec![vec![batch]]).unwrap()
    }

    fn params() -> Params {
        Params {
            day_start: day0(),
            lookback: Duration::days(2),
            shards: 1,
            max_speed_kn: 60.0,
            gap: Duration::minutes(30),
        }
    }

    async fn run(rows: &[Pt]) -> (SessionContext, Vec<RecordBatch>) {
        let ctx = SessionContext::new();
        ctx.register_table("positions", Arc::new(positions_table(rows))).unwrap();
        let out = build_shard(&ctx, &params(), 0).await.unwrap();
        let schema = out[0].schema();
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![out.clone()]).unwrap()))
            .unwrap();
        (ctx, out)
    }

    async fn cell(ctx: &SessionContext, col: &str, where_clause: &str) -> String {
        let b = ctx
            .sql(&format!("SELECT {col} FROM t WHERE {where_clause}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = b.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "WHERE {where_clause} matched {rows} rows");
        if b[0].column(0).is_null(0) {
            "NULL".into()
        } else {
            array_value_to_string(b[0].column(0), 0).unwrap()
        }
    }

    // Day rows for vessel 1, plus one point from the previous day.
    fn fixture() -> Vec<Pt> {
        let s = Some;
        vec![
            (1, -60, s(0.0), s(-0.005), "a"), // previous day: state only
            (1, 10, s(0.0), s(0.0), "a"),
            (1, 70, s(0.0), s(0.01), "a"),
            (1, 70, s(0.0), s(0.01), "b"), // same message heard by a second receiver
            (1, 130, s(10.0), s(0.0), "a"), // isolated bad fix, ~600 nm in a minute
            (1, 190, s(0.0), s(0.03), "a"),
            (1, 5400, s(0.0), s(0.04), "a"), // 90 minutes later
            (1, 5500, None, None, "a"),      // no position
            (2, 100, s(50.0), s(5.0), "a"),  // first sighting, no history
        ]
    }

    #[tokio::test]
    async fn one_output_row_per_input_row_in_the_day() {
        let (_, out) = run(&fixture()).await;
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        // The previous-day point feeds the first hop but is not emitted.
        assert_eq!(rows, 8);
        check_against_schema(&out).unwrap();
    }

    #[tokio::test]
    async fn duplicates_are_ranked_not_removed() {
        let (ctx, _) = run(&fixture()).await;
        let w = "mmsi = 1 AND longitude = 0.01";
        assert_eq!(cell(&ctx, "count(*)", w).await, "2");
        assert_eq!(cell(&ctx, "count(*)", &format!("{w} AND is_duplicate")).await, "1");
        assert_eq!(cell(&ctx, "max(n_dups)", w).await, "2");
        // The copy carries no movement of its own.
        assert_eq!(cell(&ctx, "dt_s", &format!("{w} AND is_duplicate")).await, "NULL");
    }

    #[tokio::test]
    async fn movement_uses_the_previous_days_last_point() {
        let (ctx, _) = run(&fixture()).await;
        let w = "mmsi = 1 AND ts = '2026-03-10T00:00:10Z'";
        assert_eq!(cell(&ctx, "dt_s", w).await, "70.0");
        assert_eq!(cell(&ctx, "gap_before", w).await, "false");
        let v: f64 = cell(&ctx, "dist_nm", w).await.parse().unwrap();
        assert!((v - 0.3).abs() < 0.01, "dist {v}"); // 0.005 deg of longitude at the equator
    }

    #[tokio::test]
    async fn isolated_outlier_is_a_spike_and_the_return_leg_is_only_a_jump() {
        let (ctx, out) = run(&fixture()).await;
        let bad = "mmsi = 1 AND latitude = 10.0";
        assert_eq!(cell(&ctx, "is_spike", bad).await, "true");
        assert_eq!(cell(&ctx, "is_outlier", bad).await, "true");
        let back = "mmsi = 1 AND ts = '2026-03-10T00:03:10Z'";
        assert_eq!(cell(&ctx, "is_speed_jump", back).await, "true");
        assert_eq!(cell(&ctx, "is_spike", back).await, "false");
        let mut c = FlagCounts::new();
        c.add(&out).unwrap();
        assert_eq!(c.get("is_spike"), 1);
    }

    #[tokio::test]
    async fn gaps_and_missing_positions_are_flagged() {
        let (ctx, _) = run(&fixture()).await;
        let late = "mmsi = 1 AND ts = '2026-03-10T01:30:00Z'";
        assert_eq!(cell(&ctx, "gap_before", late).await, "true");
        assert_eq!(cell(&ctx, "is_speed_jump", late).await, "false");
        let none = "mmsi = 1 AND latitude IS NULL";
        assert_eq!(cell(&ctx, "has_position", none).await, "false");
        assert_eq!(cell(&ctx, "gap_before", none).await, "false");
        assert_eq!(cell(&ctx, "dist_nm", none).await, "NULL");
        let first = "mmsi = 2";
        assert_eq!(cell(&ctx, "gap_before", first).await, "true");
        assert_eq!(cell(&ctx, "prev_ts", first).await, "NULL");
    }

    #[tokio::test]
    async fn shards_partition_the_vessels_without_loss() {
        let ctx = SessionContext::new();
        ctx.register_table("positions", Arc::new(positions_table(&fixture()))).unwrap();
        let mut p = params();
        p.shards = 2;
        let mut rows = 0;
        for k in 0..2 {
            rows += build_shard(&ctx, &p, k).await.unwrap().iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(rows, 8);
    }
}
