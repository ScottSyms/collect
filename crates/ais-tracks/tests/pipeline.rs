//! The whole chain on synthetic AIS: positions -> track_points -> tracks ->
//! stop_segments -> stops (with ports) -> voyages.
//!
//! Vessel 366000001 lies at port Alpha (00:00-06:00), sails 60 nm to port Beta
//! (arrives 11:00), lies there across midnight until 03:00 the next day, then
//! sails on and is last seen underway. Vessel 366000002 never stops.

mod support;

use std::sync::Arc;

use support::scenario::{day, raw_points, scenario, NAVS, H};

use ais_tracks::reduce::{Dicts, Rules, ThinOpts};
use ais_tracks::{carry, ports, stops, tracks, voyages};
use arrow::array::{new_null_array, Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use chrono::{DateTime, Duration};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

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

#[derive(Clone, Copy)]
struct Mode {
    /// Seconds between reports.
    step: i64,
    thin: bool,
}

/// How far a stop boundary and a leg's distance may be off.
///
/// Stops use speed smoothed over a 10-minute window, so with dense reports a
/// boundary blurs by up to half the window (5 minutes) either way; thinning
/// keeps a row only every couple of minutes and adds a little more. With
/// sparse reports no window holds a neighbour, so boundaries are exact. The
/// berth in this scenario jitters by about 0.085 nm per report, so each minute
/// of blur at a boundary adds distance to the leg: (seconds, nautical miles,
/// knots).
fn tolerances(m: Mode) -> (f64, f64, f64) {
    match (m.step >= 600, m.thin) {
        (true, _) => (0.5, 1.5, 0.5),
        (false, false) => (330.0, 8.0, 1.0),
        (false, true) => (450.0, 8.0, 1.0),
    }
}

struct Built {
    ctx: SessionContext,
    n_positions: usize,
    /// Rows in `track_points`, and the reports they stand for.
    kept: usize,
    represented: usize,
}

async fn run(m: Mode) -> Built {
    let ctx = SessionContext::new();
    let pts = scenario(m.step);
    let n_positions = pts.len();
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

    // track_points: reduce day 0, then day 1 carrying each vessel's state, as
    // the daily job does.
    let dicts = Dicts::new(vec!["a".into()], vec![], NAVS.iter().map(|s| s.to_string()).collect());
    let thin = if m.thin { ThinOpts::default() } else { ThinOpts::off() };
    let raw = raw_points(&pts);
    let boundary = day(1).timestamp() * 1_000_000;
    let (d0, d1): (Vec<_>, Vec<_>) = raw.into_iter().partition(|r| r.ts_us < boundary);
    let mut all = Vec::new();
    let (b0, state) = ais_tracks::reduce_day::reduce_all_with_state(
        d0, &dicts, &Default::default(), &Rules::default(), &thin,
    )
    .unwrap();
    all.extend(b0);
    let (b1, _) = ais_tracks::reduce_day::reduce_all_with_state(d1, &dicts, &state, &Rules::default(), &thin)
        .unwrap();
    all.extend(b1);
    let kept = all.iter().map(|b| b.num_rows()).sum();
    register(&ctx, "track_points", &all);
    let sums = rows(&ctx, "SELECT sum(n_raw) FROM track_points").await;
    let represented = sums[0][0].parse::<f64>().unwrap() as usize;

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

    stops::define_parts_from_segments(&ctx).await.unwrap();
    let st = stops::build_stops(&ctx).await.unwrap();
    carry::check_batches(&stops::stops_schema(), &st).unwrap();
    register(&ctx, "stops", &st);

    let v = voyages::build(&ctx, true).await.unwrap();
    carry::check_batches(&voyages::voyages_schema(), &v).unwrap();
    register(&ctx, "voyages", &v);
    Built { ctx, n_positions, kept, represented }
}

fn secs(iso: &str) -> f64 {
    DateTime::parse_from_rfc3339(iso).unwrap().timestamp() as f64
}

fn near(iso: &str, seconds_from_day0: i64, tol_s: f64) {
    let want = day(0).timestamp() as f64 + seconds_from_day0 as f64;
    let got = secs(iso);
    assert!((got - want).abs() <= tol_s, "{iso}: {got} is not within {tol_s} s of {want}");
}

/// Every claim about the pipeline, for one reporting interval and thinning mode.
async fn check_all(m: Mode) {
    let (tol_s, tol_nm, tol_kn) = tolerances(m);
    let b = run(m).await;

    // Nothing is lost: thinned rows stand for every report.
    assert_eq!(b.represented, b.n_positions, "reports represented");
    if m.thin {
        assert!(b.kept < b.n_positions * 2 / 5, "thinning kept {} of {}", b.kept, b.n_positions);
    } else {
        assert_eq!(b.kept, b.n_positions);
    }

    // Stops: Alpha, then Beta across midnight, matched to their ports.
    let r = rows(
        &b.ctx,
        "SELECT arrive_ts, depart_ts, n_segments, port_name, port_unlocode, port_distance_nm
         FROM stops WHERE mmsi = 366000001 ORDER BY arrive_ts",
    )
    .await;
    assert_eq!(r.len(), 2, "Alpha and Beta: {r:?}");
    near(&r[0][0], 0, tol_s);
    near(&r[0][1], 6 * H, tol_s);
    assert_eq!((r[0][3].as_str(), r[0][4].as_str()), ("Alpha", "AA ALP"));
    assert!(num(&r[0][5]) < 0.1);
    near(&r[1][0], 11 * H, tol_s);
    near(&r[1][1], 27 * H, tol_s);
    assert_eq!(r[1][2], "2", "day 0 and day 1 pieces merge: {r:?}");
    assert_eq!(r[1][3], "Beta");
    let ids = rows(&b.ctx, "SELECT port2_id FROM stops WHERE mmsi = 366000001 ORDER BY arrive_ts").await;
    assert_eq!(ids[0][0], "3", "runner-up is kept");

    // A vessel that never stops.
    assert!(rows(&b.ctx, "SELECT 1 FROM stops WHERE mmsi = 366000002").await.is_empty());
    let r = rows(
        &b.ctx,
        "SELECT origin_known, dest_known, is_open, n_points FROM voyages WHERE mmsi = 366000002",
    )
    .await;
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][..3], ["false", "false", "true"]);
    assert_eq!(r[0][3], format!("{}", 1000 * 60 / m.step), "every report after the first belongs to the leg");

    // Voyages: distance, speed, and declared destination.
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
    assert!((nm - 60.0).abs() < tol_nm, "distance {nm}");
    let kn = num(&r[0][3]);
    assert!((kn - 12.0).abs() < tol_kn, "speed {kn}");
    assert_eq!(r[0][4], "false");
    assert_eq!(r[0][5], "BETA", "padding removed; the most reported value wins");
    assert_eq!(r[0][6], "true");
    assert_eq!(r[0][7], "2");
    assert_eq!(r[1][0], "Beta");
    assert_eq!(r[1][1], "NULL");
    assert_eq!(r[1][4], "true");
}

#[tokio::test]
async fn sparse_reports_unthinned() {
    check_all(Mode { step: 600, thin: false }).await;
}

#[tokio::test]
async fn dense_reports_unthinned() {
    check_all(Mode { step: 10, thin: false }).await;
}

#[tokio::test]
async fn dense_reports_thinned_give_the_same_answers() {
    check_all(Mode { step: 10, thin: true }).await;
}

#[tokio::test]
async fn thinning_changes_the_size_not_the_story() {
    // Same distances and counts from the tracks table, thinned or not.
    let off = run(Mode { step: 10, thin: false }).await;
    let on = run(Mode { step: 10, thin: true }).await;
    let q = "SELECT sum(n_rows), sum(n_stream), sum(n_duplicates), sum(n_outliers), sum(distance_nm_raw), \
             min(min_lat), max(max_lat) FROM tracks";
    let (a, b) = (rows(&off.ctx, q).await, rows(&on.ctx, q).await);
    for c in 0..4 {
        assert_eq!(a[0][c], b[0][c], "column {c}: {a:?} vs {b:?}");
    }
    let (da, db) = (num(&a[0][4]), num(&b[0][4]));
    assert!((da - db).abs() < 1e-6 * da.max(1.0), "raw distance {da} vs {db}");
    assert!((num(&a[0][5]) - num(&b[0][5])).abs() < 0.01);
    assert!((num(&a[0][6]) - num(&b[0][6])).abs() < 0.01);
    assert!(on.kept < off.kept / 2);
}
