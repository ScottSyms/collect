//! `tracks`: `track_points` rolled up into continuous track segments.
//!
//! A segment is a run of one vessel's points with no reporting gap
//! (`track_points.gap_before` starts a new one). Each UTC day is built on its
//! own so runs stay incremental, which means a segment that crosses midnight
//! is stored as one row per day. The rows of such a chain share a `track_id`:
//! the first piece of a day that continues the previous day inherits the
//! `track_id` of the previous day's last piece, and is flagged
//! `continues_previous`. Group by `track_id` to get the whole segment.
//!
//! Nothing is filtered. Every `track_points` row in a segment is counted, and
//! distance and bounding box are given both raw and with outliers left out, so
//! neither hides the other. A row belongs to a piece by time; rows that fall in
//! a stretch with no positioned point of their own (say, a position-less
//! message between two gaps) belong to no piece, and the run reports how many.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, Int32Array};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration, Utc};
use datafusion::prelude::SessionContext;
use iceberg::spec::{NestedField, PrimitiveType, Schema};

use crate::carry;

pub const TABLE_TRACKS: &str = "tracks";
/// Name the previous day's last piece per vessel is registered under.
const PREV_TRACKS: &str = "prev_tracks";

fn required(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::required(id, name, ty.into()))
}

fn optional(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::optional(id, name, ty.into()))
}

/// Column order here is the column order of [`shard_sql`]'s result. `ts`, the
/// piece's first row, must stay column 0: the table is partitioned by day on it.
pub fn tracks_schema() -> Schema {
    use PrimitiveType::*;
    let fields = vec![
        required(1, "ts", Timestamptz),
        required(2, "mmsi", Long),
        required(3, "track_id", String),
        required(4, "ts_end", Timestamptz),
        required(5, "duration_s", Double),
        required(6, "continues_previous", Boolean),
        required(7, "chain_broken", Boolean),
        required(8, "n_rows", Int),
        required(9, "n_stream", Int),
        required(10, "n_duplicates", Int),
        required(11, "n_no_position", Int),
        required(12, "n_jumps", Int),
        required(13, "n_spikes", Int),
        required(14, "n_outliers", Int),
        optional(15, "start_lat", Double),
        optional(16, "start_lon", Double),
        optional(17, "end_lat", Double),
        optional(18, "end_lon", Double),
        optional(19, "distance_nm_raw", Double),
        optional(20, "distance_nm_clean", Double),
        optional(21, "min_lat", Double),
        optional(22, "max_lat", Double),
        optional(23, "min_lon", Double),
        optional(24, "max_lon", Double),
        optional(25, "clean_min_lat", Double),
        optional(26, "clean_max_lat", Double),
        optional(27, "clean_min_lon", Double),
        optional(28, "clean_max_lon", Double),
        required(29, "bbox_wraps", Boolean),
        optional(30, "mean_sog_knots", Double),
        optional(31, "max_sog_knots", Double),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building tracks schema")
}

use crate::carry::lit;

/// The SQL building one shard of one day. Reads the registered `track_points`
/// and `prev_tracks` tables.
pub fn shard_sql(day_start: DateTime<Utc>, shards: u32, shard: u32) -> String {
    let ds = lit(day_start);
    let de = lit(day_start + Duration::days(1));
    // Same total order `track_points` uses, so pieces are deterministic.
    let order = "ts, latitude, longitude, dup_rank, source, station, payload";
    let first = "ORDER BY ts, latitude, longitude";
    format!(
        "
WITH pts AS (
  SELECT mmsi, ts, latitude, longitude, sog_knots, dist_nm, has_position, is_duplicate,
         gap_before, is_speed_jump, is_spike, is_outlier, is_sog_invalid,
         dup_rank, source, station, payload,
         (has_position AND NOT is_duplicate) AS in_stream
  FROM track_points
  WHERE ts >= '{ds}' AND ts < '{de}' AND mmsi % {shards} = {shard}
),
frag AS (
  SELECT pts.*, CAST(sum(CASE WHEN gap_before THEN 1 ELSE 0 END) OVER (
      PARTITION BY mmsi ORDER BY {order}
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT) AS frag_no
  FROM pts
),
agg AS (
  SELECT mmsi, frag_no,
    min(ts) AS ts, max(ts) AS ts_end,
    CAST(count(*) AS INT) AS n_rows,
    CAST(count(*) FILTER (WHERE in_stream) AS INT) AS n_stream,
    CAST(count(*) FILTER (WHERE is_duplicate) AS INT) AS n_duplicates,
    CAST(count(*) FILTER (WHERE NOT has_position) AS INT) AS n_no_position,
    CAST(count(*) FILTER (WHERE is_speed_jump) AS INT) AS n_jumps,
    CAST(count(*) FILTER (WHERE is_spike) AS INT) AS n_spikes,
    CAST(count(*) FILTER (WHERE is_outlier) AS INT) AS n_outliers,
    first_value(latitude {first}) FILTER (WHERE in_stream) AS start_lat,
    first_value(longitude {first}) FILTER (WHERE in_stream) AS start_lon,
    last_value(latitude {first}) FILTER (WHERE in_stream) AS end_lat,
    last_value(longitude {first}) FILTER (WHERE in_stream) AS end_lon,
    sum(dist_nm) FILTER (WHERE in_stream AND NOT gap_before) AS distance_nm_raw,
    sum(dist_nm) FILTER (WHERE in_stream AND NOT gap_before AND NOT is_speed_jump)
      AS distance_nm_clean,
    min(latitude) FILTER (WHERE in_stream) AS min_lat,
    max(latitude) FILTER (WHERE in_stream) AS max_lat,
    min(longitude) FILTER (WHERE in_stream) AS min_lon,
    max(longitude) FILTER (WHERE in_stream) AS max_lon,
    min(latitude) FILTER (WHERE in_stream AND NOT is_outlier) AS clean_min_lat,
    max(latitude) FILTER (WHERE in_stream AND NOT is_outlier) AS clean_max_lat,
    min(longitude) FILTER (WHERE in_stream AND NOT is_outlier) AS clean_min_lon,
    max(longitude) FILTER (WHERE in_stream AND NOT is_outlier) AS clean_max_lon,
    avg(sog_knots) FILTER (WHERE in_stream AND NOT is_sog_invalid) AS mean_sog_knots,
    max(sog_knots) FILTER (WHERE in_stream AND NOT is_sog_invalid) AS max_sog_knots
  FROM frag
  GROUP BY mmsi, frag_no
  HAVING count(*) FILTER (WHERE in_stream) > 0
)
SELECT a.ts, a.mmsi,
  CASE WHEN a.frag_no = 0 AND p.track_id IS NOT NULL THEN p.track_id
       ELSE concat(CAST(a.mmsi AS VARCHAR), '-', CAST(CAST(a.ts AS BIGINT) / 1000 AS VARCHAR))
  END AS track_id,
  a.ts_end,
  (CAST(a.ts_end AS BIGINT) - CAST(a.ts AS BIGINT)) / 1000000.0 AS duration_s,
  a.frag_no = 0 AS continues_previous,
  (a.frag_no = 0 AND p.track_id IS NULL) AS chain_broken,
  a.n_rows, a.n_stream, a.n_duplicates, a.n_no_position, a.n_jumps, a.n_spikes, a.n_outliers,
  a.start_lat, a.start_lon, a.end_lat, a.end_lon,
  a.distance_nm_raw, a.distance_nm_clean,
  a.min_lat, a.max_lat, a.min_lon, a.max_lon,
  a.clean_min_lat, a.clean_max_lat, a.clean_min_lon, a.clean_max_lon,
  coalesce(a.max_lon - a.min_lon > 180, false) AS bbox_wraps,
  a.mean_sog_knots, a.max_sog_knots
FROM agg a LEFT JOIN {PREV_TRACKS} p ON a.mmsi = p.mmsi
ORDER BY a.mmsi, a.ts"
    )
}

/// Registers what the next day needs from the previous one: the last piece of
/// each vessel (see [`carry::set_previous`]).
pub async fn set_previous(
    ctx: &SessionContext,
    source: Option<(&str, Option<(DateTime<Utc>, DateTime<Utc>)>)>,
) -> Result<()> {
    carry::set_previous(ctx, PREV_TRACKS, "track_id", source).await
}

pub use carry::register_output;

/// Computes one shard of one day. `prev_tracks` must be registered.
pub async fn build_shard(
    ctx: &SessionContext,
    day_start: DateTime<Utc>,
    shards: u32,
    shard: u32,
) -> Result<Vec<RecordBatch>> {
    ctx.sql(&shard_sql(day_start, shards, shard))
        .await
        .with_context(|| format!("planning tracks shard {shard}"))?
        .collect()
        .await
        .with_context(|| format!("computing tracks shard {shard}"))
}

/// Sum of an `Int32` column by name over batches.
pub fn sum_int(batches: &[RecordBatch], name: &str) -> Result<usize> {
    let mut total = 0usize;
    for b in batches {
        let col = b
            .column_by_name(name)
            .with_context(|| format!("missing column {name}"))?;
        let col = col
            .as_any()
            .downcast_ref::<Int32Array>()
            .with_context(|| format!("{name} is not int"))?;
        total += col.iter().flatten().map(|v| v as usize).sum::<usize>();
    }
    Ok(total)
}

/// Every batch has the table's columns in order, and no required column holds
/// a null.
pub fn check_against_schema(batches: &[RecordBatch]) -> Result<()> {
    carry::check_batches(&tracks_schema(), batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::track_points::{self, Params};
    use arrow::array::{new_null_array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::util::display::array_value_to_string;
    use chrono::TimeZone;
    use datafusion::datasource::MemTable;

    fn day(n: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 10, 0, 0, 0).unwrap() + Duration::days(n)
    }

    /// (mmsi, seconds from day 0 start, lat, lon, source)
    type Pt = (i64, i64, Option<f64>, Option<f64>, &'static str);

    fn positions_table(rows: &[Pt]) -> MemTable {
        let schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(
                &collect_core::iceberg::table_schemas::positions_schema(),
            )
            .unwrap(),
        );
        let n = rows.len();
        let base = day(0).timestamp() * 1_000_000;
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
                    "sog_knots" => Arc::new(Float64Array::from(vec![Some(5.0); n])),
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

    const DAY: i64 = 86_400;

    /// Vessel 1 keeps reporting across midnight, then goes quiet for 2 hours;
    /// its second day also holds a duplicate, a bad fix and a position-less
    /// row. Vessel 2 reports on day 0 only.
    fn fixture() -> Vec<Pt> {
        let s = Some;
        vec![
            (1, DAY - 600, s(0.0), s(0.00), "a"),
            (1, DAY - 60, s(0.0), s(0.01), "a"),
            (1, DAY + 60, s(0.0), s(0.02), "a"),
            (1, DAY + 60, s(0.0), s(0.02), "b"), // duplicate
            (1, DAY + 120, s(10.0), s(0.0), "a"), // bad fix
            (1, DAY + 180, s(0.0), s(0.03), "a"),
            (1, DAY + 200, None, None, "a"), // no position
            (1, DAY + 9_000, s(0.0), s(0.05), "a"), // after a gap
            (2, 100, s(50.0), s(5.0), "a"),
        ]
    }

    /// Runs the real pipeline: positions -> track_points (both days) -> tracks
    /// (day 0, then day 1 chained onto it).
    async fn run() -> (SessionContext, Vec<RecordBatch>, Vec<RecordBatch>, usize) {
        let ctx = SessionContext::new();
        ctx.register_table("positions", Arc::new(positions_table(&fixture()))).unwrap();
        let mut all = Vec::new();
        for n in 0..2 {
            let p = Params {
                day_start: day(n),
                lookback: Duration::days(2),
                shards: 1,
                max_speed_kn: 60.0,
                gap: Duration::minutes(30),
            };
            all.extend(track_points::build_shard(&ctx, &p, 0).await.unwrap());
        }
        let n_points: usize = all.iter().map(|b| b.num_rows()).sum();
        ctx.register_table(
            "track_points",
            Arc::new(MemTable::try_new(all[0].schema(), vec![all]).unwrap()),
        )
        .unwrap();

        set_previous(&ctx, None).await.unwrap();
        let d0 = build_shard(&ctx, day(0), 1, 0).await.unwrap();
        register_output(&ctx, "day0", &d0).unwrap();
        set_previous(&ctx, Some(("day0", None))).await.unwrap();
        let d1 = build_shard(&ctx, day(1), 1, 0).await.unwrap();
        (ctx, d0, d1, n_points)
    }

    async fn cell(ctx: &SessionContext, batches: &[RecordBatch], col: &str, w: &str) -> String {
        let _ = ctx.deregister_table("t");
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(batches[0].schema(), vec![batches.to_vec()]).unwrap()),
        )
        .unwrap();
        let b = ctx
            .sql(&format!("SELECT {col} FROM t WHERE {w}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = b.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "WHERE {w} matched {rows} rows");
        if b[0].column(0).is_null(0) {
            "NULL".into()
        } else {
            array_value_to_string(b[0].column(0), 0).unwrap()
        }
    }

    #[tokio::test]
    async fn output_matches_the_iceberg_schema() {
        let (_, d0, d1, _) = run().await;
        check_against_schema(&d0).unwrap();
        check_against_schema(&d1).unwrap();
    }

    #[tokio::test]
    async fn a_segment_crossing_midnight_shares_one_track_id() {
        let (ctx, d0, d1, _) = run().await;
        let first = cell(&ctx, &d0, "track_id", "mmsi = 1").await;
        assert!(first.starts_with("1-"), "{first}");
        // Day 1's first piece continues it; the piece after the 2 h gap is new.
        let cont = cell(&ctx, &d1, "track_id", "mmsi = 1 AND continues_previous").await;
        assert_eq!(cont, first);
        assert_eq!(cell(&ctx, &d1, "chain_broken", "mmsi = 1 AND continues_previous").await, "false");
        let after_gap = cell(&ctx, &d1, "track_id", "mmsi = 1 AND NOT continues_previous").await;
        assert_ne!(after_gap, first);
        assert_eq!(cell(&ctx, &d1, "count(*)", "mmsi = 1").await, "2");
    }

    #[tokio::test]
    async fn a_missing_previous_day_is_flagged_not_hidden() {
        let (ctx, _, _, _) = run().await;
        set_previous(&ctx, None).await.unwrap();
        let d1 = build_shard(&ctx, day(1), 1, 0).await.unwrap();
        assert_eq!(cell(&ctx, &d1, "chain_broken", "mmsi = 1 AND continues_previous").await, "true");
        // It still gets an id of its own.
        assert!(cell(&ctx, &d1, "track_id", "mmsi = 1 AND continues_previous").await.starts_with("1-"));
    }

    #[tokio::test]
    async fn counts_include_everything_and_distance_is_given_raw_and_clean() {
        let (ctx, _, d1, _) = run().await;
        let w = "mmsi = 1 AND continues_previous";
        // 0:01, dup, bad fix, 0:03, no-position row.
        assert_eq!(cell(&ctx, &d1, "n_rows", w).await, "5");
        assert_eq!(cell(&ctx, &d1, "n_stream", w).await, "3");
        assert_eq!(cell(&ctx, &d1, "n_duplicates", w).await, "1");
        assert_eq!(cell(&ctx, &d1, "n_no_position", w).await, "1");
        assert_eq!(cell(&ctx, &d1, "n_spikes", w).await, "1");
        let raw: f64 = cell(&ctx, &d1, "distance_nm_raw", w).await.parse().unwrap();
        let clean: f64 = cell(&ctx, &d1, "distance_nm_clean", w).await.parse().unwrap();
        assert!(raw > 1000.0, "the bad fix inflates the raw distance: {raw}");
        assert!(clean < 2.0, "clean distance ignores the jump legs: {clean}");
        // The bad fix stretches the raw box but not the clean one.
        assert_eq!(cell(&ctx, &d1, "max_lat", w).await, "10.0");
        assert_eq!(cell(&ctx, &d1, "clean_max_lat", w).await, "0.0");
    }

    #[tokio::test]
    async fn every_track_point_is_accounted_for() {
        let (_, d0, d1, n_points) = run().await;
        let covered = sum_int(&d0, "n_rows").unwrap() + sum_int(&d1, "n_rows").unwrap();
        assert_eq!(covered, n_points, "no row belongs to no piece in this fixture");
    }
}
