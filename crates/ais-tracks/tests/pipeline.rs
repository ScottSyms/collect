//! The whole chain on synthetic AIS: positions -> track_points -> tracks ->
//! stop_segments -> stops (with ports) -> voyages.
//!
//! Vessel 366000001 lies at port Alpha (00:00-06:00), sails 60 nm to port Beta
//! (arrives 11:00), lies there across midnight until 03:00 the next day, then
//! sails on and is last seen underway. Vessel 366000002 never stops.

use std::sync::Arc;

use ais_tracks::{carry, ports, stops, track_points, tracks, voyages};
use arrow::array::{new_null_array, Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use chrono::{DateTime, Duration, TimeZone, Utc};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

const TEN_MIN: i64 = 600;
/// Longitude degrees covered in ten minutes at 12 knots, at latitude 10 degrees.
const STEP: f64 = 2.0 / 59.09;

fn day(n: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 10, 0, 0, 0).unwrap() + Duration::days(n)
}

/// (mmsi, seconds from day 0, lat, lon, sog, nav_status)
type Pt = (i64, i64, f64, f64, f64, &'static str);

fn scenario() -> Vec<Pt> {
    let mut v = Vec::new();
    let h = |hours: f64| (hours * 3600.0) as i64;
    let jitter = |i: i64| if i % 2 == 0 { 0.0005 } else { -0.0005 };
    // Alpha, 00:00-06:00
    for i in 0..=36 {
        v.push((366000001, i * TEN_MIN, 10.0 + jitter(i), 20.0 + jitter(i + 1), 0.1, "moored"));
    }
    // Underway 06:10-10:50
    for i in 1..=29 {
        v.push((366000001, h(6.0) + i * TEN_MIN, 10.0, 20.0 + i as f64 * STEP, 12.0, "under way using engine"));
    }
    let beta = 20.0 + 30.0 * STEP;
    // Beta, 11:00 until 03:00 the next day
    for i in 0..=96 {
        v.push((366000001, h(11.0) + i * TEN_MIN, 10.0 + jitter(i), beta + jitter(i + 1), 0.05, "moored"));
    }
    // Underway again 27:10-33:00, then last seen
    for i in 1..=35 {
        v.push((366000001, h(27.0) + i * TEN_MIN, 10.0, beta + i as f64 * STEP, 12.0, "under way using engine"));
    }
    // Vessel 2 keeps moving throughout
    for i in 0..=100 {
        v.push((366000002, i * TEN_MIN, 30.0, 40.0 + i as f64 * STEP, 12.0, "under way using engine"));
    }
    v
}

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
                "source" => Arc::new(StringArray::from(vec!["a"; n])),
                "msg_type" => Arc::new(Int32Array::from(vec![1; n])),
                "mmsi" => Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                "latitude" => Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.2))),
                "longitude" => Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.3))),
                "sog_knots" => Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.4))),
                "nav_status" => Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.5))),
                "payload" => Arc::new(StringArray::from_iter_values((0..n).map(|i| format!("p{i}")))),
                _ => new_null_array(f.data_type(), n),
            }
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    MemTable::try_new(schema, vec![vec![batch]]).unwrap()
}

/// (mmsi, hours after day 0, destination)
fn statics_table(rows: &[(i64, f64, &str)]) -> MemTable {
    let schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(
            &collect_core::iceberg::table_schemas::statics_schema(),
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
                        rows.iter().map(|r| base + (r.1 * 3600.0) as i64 * 1_000_000),
                    )
                    .with_timezone("+00:00"),
                ),
                "source" => Arc::new(StringArray::from(vec!["a"; n])),
                "msg_type" => Arc::new(Int32Array::from(vec![5; n])),
                "mmsi" => Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                "destination" => Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
                _ => new_null_array(f.data_type(), n),
            }
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    MemTable::try_new(schema, vec![vec![batch]]).unwrap()
}

const PORTS_CSV: &str = "OID_,World Port Index Number,Region Name,Main Port Name,Alternate Port Name,UN/LOCODE,Country Code,Harbor Size,Harbor Type,Harbor Use,Channel Depth (m),Maximum Vessel Draft (m),Tidal Range (m),Latitude,Longitude
1,1.0,Test,Alpha,,AA ALP,Aland,Large,Coastal (Natural),Unknown,,,,10.0,20.0
2,2.0,Test,Beta,,BB BET,Bland,Medium,Coastal (Natural),Unknown,,,,10.0,21.01535
3,3.0,Test,Gamma,,,Cland,Very Small,Coastal (Natural),Unknown,,,,10.0,20.05
";

fn register(ctx: &SessionContext, name: &str, batches: &[RecordBatch]) {
    let _ = ctx.deregister_table(name);
    ctx.register_table(
        name,
        Arc::new(MemTable::try_new(batches[0].schema(), vec![batches.to_vec()]).unwrap()),
    )
    .unwrap();
}

async fn rows(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for b in &batches {
        for r in 0..b.num_rows() {
            out.push(
                (0..b.num_columns())
                    .map(|c| {
                        if b.column(c).is_null(r) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(b.column(c), r).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    out
}

fn num(s: &str) -> f64 {
    s.parse().unwrap()
}

struct Built {
    ctx: SessionContext,
    n_positions: usize,
    n_points: usize,
}

async fn run() -> Built {
    let ctx = SessionContext::new();
    let pts = scenario();
    let n_positions = pts.len();
    ctx.register_table("positions", Arc::new(positions_table(&pts))).unwrap();
    ctx.register_table(
        "statics",
        Arc::new(statics_table(&[
            (366000001, 7.0, "ALPHA"),
            (366000001, 8.0, "BETA@@@@"),
            (366000001, 9.0, "BETA@@@@"),
        ])),
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pub150.csv");
    std::fs::write(&path, PORTS_CSV).unwrap();
    let p = ports::load_csv(path.to_str().unwrap(), "2026-03-01").await.unwrap();
    register(&ctx, "ref_ports", &p);

    // track_points, day by day
    let mut all = Vec::new();
    for n in 0..2 {
        let params = track_points::Params {
            day_start: day(n),
            lookback: Duration::days(2),
            shards: 2,
            max_speed_kn: 60.0,
            gap: Duration::minutes(30),
        };
        for shard in 0..params.shards {
            all.extend(track_points::build_shard(&ctx, &params, shard).await.unwrap());
        }
    }
    let n_points = all.iter().map(|b| b.num_rows()).sum();
    register(&ctx, "track_points", &all);

    // tracks and stop_segments, chained day to day
    let mut tracks_out = Vec::new();
    let mut seg_out = Vec::new();
    tracks::set_previous(&ctx, None).await.unwrap();
    stops::set_previous(&ctx, None).await.unwrap();
    for n in 0..2 {
        let sp = stops::StopParams {
            day_start: day(n),
            shards: 2,
            slow_kn: 0.5,
            smooth: Duration::minutes(10),
            resume_nm: 1.0,
            min_stop: Duration::minutes(30),
        };
        let mut t = Vec::new();
        let mut s = Vec::new();
        for shard in 0..2 {
            t.extend(tracks::build_shard(&ctx, day(n), 2, shard).await.unwrap());
            s.extend(stops::build_segments(&ctx, &sp, shard).await.unwrap());
        }
        tracks::check_against_schema(&t).unwrap();
        carry::check_batches(&stops::stop_segments_schema(), &s).unwrap();
        carry::register_output(&ctx, "prev_day_tracks", &t).unwrap();
        tracks::set_previous(&ctx, Some(("prev_day_tracks", None))).await.unwrap();
        carry::register_output(&ctx, "prev_day_stops", &s).unwrap();
        stops::set_previous(&ctx, Some(("prev_day_stops", None))).await.unwrap();
        tracks_out.extend(t);
        seg_out.extend(s);
    }
    register(&ctx, "tracks", &tracks_out);
    register(&ctx, "stop_segments", &seg_out);

    let st = stops::build_stops(&ctx).await.unwrap();
    carry::check_batches(&stops::stops_schema(), &st).unwrap();
    register(&ctx, "stops", &st);

    let v = voyages::build(&ctx, true).await.unwrap();
    carry::check_batches(&voyages::voyages_schema(), &v).unwrap();
    register(&ctx, "voyages", &v);
    Built { ctx, n_positions, n_points }
}

#[tokio::test]
async fn nothing_is_lost_between_positions_and_track_points() {
    let b = run().await;
    assert_eq!(b.n_points, b.n_positions);
}

#[tokio::test]
async fn a_stop_across_midnight_is_one_stop_matched_to_its_port() {
    let b = run().await;
    let r = rows(
        &b.ctx,
        "SELECT arrive_ts, depart_ts, n_segments, port_name, port_unlocode, is_current, port_distance_nm
         FROM stops WHERE mmsi = 366000001 ORDER BY arrive_ts",
    )
    .await;
    assert_eq!(r.len(), 2, "Alpha and Beta: {r:?}");
    // Alpha
    assert!(r[0][0].starts_with("2026-03-10T00:00:00"), "{:?}", r[0]);
    assert!(r[0][1].starts_with("2026-03-10T06:00:00"), "{:?}", r[0]);
    assert_eq!(r[0][3], "Alpha");
    assert_eq!(r[0][4], "AA ALP");
    assert!(num(&r[0][6]) < 0.1);
    // Beta spans two day pieces yet is one stop, and the vessel left it.
    assert!(r[1][0].starts_with("2026-03-10T11:00:00"), "{:?}", r[1]);
    assert!(r[1][1].starts_with("2026-03-11T03:00:00"), "{:?}", r[1]);
    assert_eq!(r[1][2], "2", "day 0 and day 1 pieces merge");
    assert_eq!(r[1][3], "Beta");
    assert_eq!(r[1][5], "false");
    // Harbour size decides the match radius: Very Small Gamma is 3 nm from
    // Alpha, but Alpha is nearer.
    let ids = rows(&b.ctx, "SELECT port2_id FROM stops WHERE mmsi = 366000001 ORDER BY arrive_ts").await;
    assert_eq!(ids[0][0], "3", "runner-up is kept");
}

#[tokio::test]
async fn a_vessel_that_never_stops_has_no_stops_and_one_open_leg() {
    let b = run().await;
    assert!(rows(&b.ctx, "SELECT 1 FROM stops WHERE mmsi = 366000002").await.is_empty());
    let r = rows(
        &b.ctx,
        "SELECT origin_known, dest_known, is_open, n_points FROM voyages WHERE mmsi = 366000002",
    )
    .await;
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][..3], ["false", "false", "true"]);
    assert_eq!(r[0][3], "100", "every point after the first belongs to the leg");
}

#[tokio::test]
async fn voyages_link_the_stops_with_distance_and_declared_destination() {
    let b = run().await;
    let r = rows(
        &b.ctx,
        "SELECT origin_port_name, dest_port_name, distance_nm_clean, avg_speed_kn, is_open,
                declared_destination, declared_matches_dest, n_declared_destinations
         FROM voyages WHERE mmsi = 366000001 ORDER BY depart_ts",
    )
    .await;
    assert_eq!(r.len(), 2, "Alpha->Beta and the open leg after Beta: {r:?}");
    assert_eq!((r[0][0].as_str(), r[0][1].as_str()), ("Alpha", "Beta"));
    let nm = num(&r[0][2]);
    assert!((nm - 60.0).abs() < 1.5, "distance {nm}");
    let kn = num(&r[0][3]);
    assert!((kn - 12.0).abs() < 0.5, "speed {kn}");
    assert_eq!(r[0][4], "false");
    assert_eq!(r[0][5], "BETA", "padding removed; the most reported value wins");
    assert_eq!(r[0][6], "true");
    assert_eq!(r[0][7], "2");
    // After Beta: origin known, nowhere reached yet.
    assert_eq!(r[1][0], "Beta");
    assert_eq!(r[1][1], "NULL");
    assert_eq!(r[1][4], "true");
}
