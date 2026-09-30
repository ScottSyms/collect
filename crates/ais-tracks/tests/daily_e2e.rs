//! The daily flow end to end against real Iceberg tables on the local
//! filesystem: silver in, `track_points` / `tracks` / `stop_segments` /
//! `vessel_state` / `build_log` out, through the real scan, writers and both
//! commit paths. Covers first build, rerun, late data that changes where a
//! vessel's stream ends (which must ripple forward) and late data that does not
//! (which must not).

mod support;

use std::sync::Arc;

use ais_tracks::daily::{self, register, register_as, DailyRun, Env, StopTuning, Summary};
use ais_tracks::output::{commit_day, ensure_day_table, DayWriter};
use ais_tracks::reduce::{Dicts, RawPoint};
use ais_tracks::reduce_day::ReduceOptions;
use ais_tracks::state::{self, date_to_day, DaySelect, Log};
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use collect_core::iceberg::{ensure_namespace, table_ident, IcebergConfig};
use collect_maint::commit::RestClient;
use datafusion::prelude::SessionContext;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::Catalog;
use support::scenario::{
    day, fleet, raw_points, scenario, silver_batch, statics_batch, Static, FLEET_DEST, FLEET_PORTS_CSV, NAVS,
};
use support::MockCatalog;

struct Rig {
    cat: MockCatalog,
    input: IcebergConfig,
    output: IcebergConfig,
    rest: RestClient,
    scratch: std::path::PathBuf,
    dicts: Dicts,
    _dir: tempfile::TempDir,
}

fn cfg(url: &str, ns: &str) -> IcebergConfig {
    IcebergConfig {
        catalog_uri: url.to_string(),
        warehouse: "w".into(),
        namespace: ns.into(),
        table_prefix: None,
        token: None,
        sigv4: false,
    }
}

async fn rig() -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let cat = MockCatalog::new(dir.path());
    let url = cat.serve().await;
    let (input, output) = (cfg(&url, "ais"), cfg(&url, "curated"));
    let rest = RestClient::connect(&output).await.unwrap();
    ensure_namespace(&cat, &input).await.unwrap();
    ensure_day_table(&cat, &input, "positions", collect_core::iceberg::table_schemas::positions_schema())
        .await
        .unwrap();
    ensure_day_table(&cat, &input, "statics", collect_core::iceberg::table_schemas::statics_schema())
        .await
        .unwrap();
    let scratch = dir.path().join("scratch");
    Rig {
        cat,
        input,
        output,
        rest,
        scratch,
        dicts: Dicts::new(vec!["a".into()], vec![], NAVS.iter().map(|s| s.to_string()).collect()),
        _dir: dir,
    }
}

impl Rig {
    fn env(&self) -> Env<'_, MockCatalog> {
        Env { catalog: &self.cat, input: &self.input, output: &self.output, rest: Some(&self.rest) }
    }

    /// Writes `points` as the whole of silver day `d` (0 = 2026-03-10).
    async fn put_silver_day(&self, d: i64, points: &[RawPoint]) {
        let idx = date_to_day(day(d).date_naive());
        let t = self.cat.load_table(&table_ident(&self.input, "positions")).await.unwrap();
        let mut w = DayWriter::new(&t, idx, &["mmsi"]).await.unwrap();
        w.write(&silver_batch(points, &self.dicts)).await.unwrap();
        let rows = w.rows;
        let files = w.finish().await.unwrap();
        commit_day(&self.cat, &self.rest, &self.input, "positions", idx, files, rows).await.unwrap();
    }

    /// Writes `rows` as the whole of silver statics day `d`.
    async fn put_statics_day(&self, d: i64, rows: &[Static]) {
        let idx = date_to_day(day(d).date_naive());
        let t = self.cat.load_table(&table_ident(&self.input, "statics")).await.unwrap();
        let mut w = DayWriter::new(&t, idx, &["mmsi"]).await.unwrap();
        w.write(&statics_batch(d, rows)).await.unwrap();
        let n = w.rows;
        let files = w.finish().await.unwrap();
        commit_day(&self.cat, &self.rest, &self.input, "statics", idx, files, n).await.unwrap();
    }

    /// The vessels tables as text, without the columns that legitimately differ.
    async fn vessels_text(&self, from_silver: bool) -> (String, String) {
        let ctx = SessionContext::new();
        let (v, a) = if from_silver {
            register(&ctx, &self.cat, &self.input, "positions").await.unwrap();
            register(&ctx, &self.cat, &self.input, "statics").await.unwrap();
            let built = ais_tracks::vessels::build(&ctx).await.unwrap();
            let c2 = SessionContext::new();
            c2.register_table("v", Arc::new(datafusion::datasource::MemTable::try_new(built.vessels[0].schema(), vec![built.vessels]).unwrap())).unwrap();
            c2.register_table("a", Arc::new(datafusion::datasource::MemTable::try_new(built.attributes[0].schema(), vec![built.attributes]).unwrap())).unwrap();
            return (Self::text(&c2, "v", "mmsi").await, Self::text(&c2, "a", "mmsi, attribute, rank").await);
        } else {
            register_as(&ctx, &self.cat, &self.output, "vessels", "v").await.unwrap();
            register_as(&ctx, &self.cat, &self.output, "vessel_attributes", "a").await.unwrap();
            ("v", "a")
        };
        (Self::text(&ctx, v, "mmsi").await, Self::text(&ctx, a, "mmsi, attribute, rank").await)
    }

    async fn text(ctx: &SessionContext, table: &str, order: &str) -> String {
        let b = ctx
            .sql(&format!("SELECT * EXCEPT (computed_at, folded_through) FROM {table} ORDER BY {order}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        arrow::util::pretty::pretty_format_batches(&b).unwrap().to_string()
    }

    /// Adds `points` to silver day `d` as a new file, like data arriving late.
    async fn append_silver(&self, d: i64, points: &[RawPoint]) {
        let idx = date_to_day(day(d).date_naive());
        let t = self.cat.load_table(&table_ident(&self.input, "positions")).await.unwrap();
        let mut w = DayWriter::new(&t, idx, &["mmsi"]).await.unwrap();
        w.write(&silver_batch(points, &self.dicts)).await.unwrap();
        let files = w.finish().await.unwrap();
        let txn = Transaction::new(&t);
        let txn = txn.fast_append().add_data_files(files).apply(txn).unwrap();
        txn.commit(&self.cat).await.unwrap();
    }

    async fn daily(&self, select: &DaySelect, plan_only: bool) -> Summary {
        let opts = ReduceOptions {
            scratch: self.scratch.clone(),
            target_bucket_rows: 500,
            ..Default::default()
        };
        let stops = StopTuning {
            shards: 2,
            slow_kn: 0.5,
            smooth: Duration::minutes(10),
            resume_nm: 1.0,
            min_stop: Duration::minutes(30),
        };
        daily::run_daily(
            &self.env(),
            &DailyRun {
                select,
                opts: &opts,
                lookback_days: 1,
                track_shards: 2,
                stops: &stops,
                plan_only,
            },
        )
        .await
        .unwrap()
    }

    async fn log(&self) -> Log {
        let t = self.cat.load_table(&table_ident(&self.output, "build_log")).await.unwrap();
        Log::from_batches(&state::scan_all(&t, None).await.unwrap()).unwrap()
    }

    /// One number from a SQL query over an output table.
    async fn number(&self, table: &str, sql: &str) -> f64 {
        let ctx = SessionContext::new();
        register(&ctx, &self.cat, &self.output, table).await.unwrap();
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let c = arrow::compute::cast(b[0].column(0), &arrow::datatypes::DataType::Float64).unwrap();
        c.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0)
    }
}

fn catch_up() -> DaySelect {
    DaySelect { catch_up: true, ..Default::default() }
}

/// Splits the scenario into its two days of raw points.
fn two_days() -> (Vec<RawPoint>, Vec<RawPoint>) {
    let boundary = day(1).timestamp() * 1_000_000;
    raw_points(&scenario(10)).into_iter().partition(|p| p.ts_us < boundary)
}

fn extra_point(mmsi: u32, sec_from_day0: i64, lat: f64, lon: f64, sog: f64, nav: u16) -> RawPoint {
    RawPoint {
        ts_us: (day(0).timestamp() + sec_from_day0) * 1_000_000,
        mmsi,
        lat_e7: Some((lat * 1e7).round() as i32),
        lon_e7: Some((lon * 1e7).round() as i32),
        sog_dk: Some((sog * 10.0).round() as i16),
        cog_dd: Some(900),
        heading_dd: None,
        nav: Some(nav),
        source: 0,
        station: None,
    }
}

#[tokio::test]
async fn daily_builds_skips_and_ripples_late_data_only_as_far_as_it_matters() {
    let r = rig().await;
    let (d0, d1) = two_days();
    let (n0, n1) = (d0.len(), d1.len());
    r.put_silver_day(0, &d0).await;
    r.put_silver_day(1, &d1).await;

    // ---- first run: everything is built --------------------------------
    let s = r.daily(&catch_up(), false).await;
    assert_eq!((s.built, s.skipped), (7, 0), "2 days x 3 steps, plus the vessels fold: {s:?}");

    // Every silver report is represented in track_points.
    let represented = r.number("track_points", "SELECT sum(n_raw) FROM track_points").await;
    assert_eq!(represented as usize, n0 + n1);
    let kept = r.number("track_points", "SELECT count(*) FROM track_points").await;
    assert!(kept < (n0 + n1) as f64 * 0.7, "thinning kept {kept} of {}", n0 + n1);
    // The chain across midnight produced segments, stops and a state per day.
    assert!(r.number("tracks", "SELECT count(*) FROM tracks").await >= 3.0);
    assert!(r.number("stop_segments", "SELECT count(*) FROM stop_segments").await >= 2.0);
    assert_eq!(r.number("vessel_state", "SELECT count(distinct ts) FROM vessel_state").await, 2.0);
    let log = r.log().await;
    for step in ["track_points", "tracks", "stop_segments"] {
        assert_eq!(log.days(step).len(), 2, "{step} logged for both days");
    }

    // ---- nothing changed: nothing is rebuilt -----------------------------
    let s = r.daily(&catch_up(), false).await;
    assert_eq!((s.built, s.skipped), (0, 7), "{s:?}");

    // ---- --plan reports without building ---------------------------------
    let before = r.log().await.get("track_points", day(0).date_naive()).unwrap().built_at_us;
    let s = r.daily(&catch_up(), true).await;
    assert_eq!(s.built, 0);
    assert_eq!(r.log().await.get("track_points", day(0).date_naive()).unwrap().built_at_us, before);

    // ---- late data that moves a vessel's end-of-day position --------------
    // Vessel 2 reports once more in the last minute of day 0.
    let last = extra_point(366000002, 86_390, 30.0, 40.0 + 86_390.0 * (12.0 / 3600.0 / 59.09), 12.0, 1);
    r.append_silver(0, &[last]).await;
    let log0 = r.log().await;
    let (tp0, tp1) = (
        log0.get("track_points", day(0).date_naive()).unwrap().built_at_us,
        log0.get("track_points", day(1).date_naive()).unwrap().built_at_us,
    );
    let s = r.daily(&catch_up(), false).await;
    assert_eq!((s.built, s.skipped), (7, 0), "the change reaches day 1 through vessel state; vessels refolds: {s:?}");
    let log = r.log().await;
    assert!(log.get("track_points", day(0).date_naive()).unwrap().built_at_us > tp0);
    assert!(log.get("track_points", day(1).date_naive()).unwrap().built_at_us > tp1);
    let represented = r.number("track_points", "SELECT sum(n_raw) FROM track_points").await;
    assert_eq!(represented as usize, n0 + n1 + 1, "the late report is counted");

    // ---- late data that leaves every vessel's end of day unchanged ---------
    // A report in the middle of vessel 1's night at berth.
    let mid = extra_point(366000001, 3 * 3600 + 1, 10.0, 20.0, 0.1, 0);
    r.append_silver(0, &[mid]).await;
    let (tp0, tp1) = {
        let l = r.log().await;
        (
            l.get("track_points", day(0).date_naive()).unwrap().built_at_us,
            l.get("track_points", day(1).date_naive()).unwrap().built_at_us,
        )
    };
    let s = r.daily(&catch_up(), false).await;
    let log = r.log().await;
    assert!(log.get("track_points", day(0).date_naive()).unwrap().built_at_us > tp0, "day 0 rebuilt");
    assert_eq!(
        log.get("track_points", day(1).date_naive()).unwrap().built_at_us,
        tp1,
        "day 1's track_points is untouched: the end state it starts from did not change"
    );
    // tracks and stop_segments chain their ids from the previous day's build,
    // so they are conservatively rebuilt for both days.
    // 1 track_points day, 2 tracks, 2 stop-segments, and a vessels refold because
    // day 0's aggregates were rebuilt.
    assert_eq!((s.built, s.skipped), (6, 1), "{s:?}");
    let represented = r.number("track_points", "SELECT sum(n_raw) FROM track_points").await;
    assert_eq!(represented as usize, n0 + n1 + 2);

    // ---- an explicit range rebuilds whatever the log says ------------------
    let forced = DaySelect { from: Some(day(1).date_naive()), ..Default::default() };
    let s = r.daily(&forced, false).await;
    // 3 daily steps for day 1, plus a vessels refold for its rebuilt aggregates.
    assert_eq!((s.built, s.skipped), (4, 0), "{s:?}");
}

const PORTS_CSV: &str = "OID_,World Port Index Number,Region Name,Main Port Name,Alternate Port Name,UN/LOCODE,Country Code,Harbor Size,Harbor Type,Harbor Use,Channel Depth (m),Maximum Vessel Draft (m),Tidal Range (m),Latitude,Longitude
1,1.0,Test,Alpha,,AA ALP,Aland,Large,Coastal (Natural),Unknown,,,,10.0,20.0
2,2.0,Test,Beta,,BB BET,Bland,Medium,Coastal (Natural),Unknown,,,,10.0,21.01535
";

#[tokio::test]
async fn stops_and_voyages_are_written_then_replaced_whole() {
    use ais_tracks::output::replace_table;
    use ais_tracks::ports::{load_csv, ref_ports_schema};
    use ais_tracks::{stops, voyages};

    let r = rig().await;
    let (d0, d1) = two_days();
    r.put_silver_day(0, &d0).await;
    r.put_silver_day(1, &d1).await;
    r.daily(&catch_up(), false).await;

    // Reference ports, appended the way `ports load` does.
    let csv = r._dir.path().join("pub150.csv");
    std::fs::write(&csv, PORTS_CSV).unwrap();
    let batches = load_csv(csv.to_str().unwrap(), "2026-03-01").await.unwrap();
    let schema = ref_ports_schema();
    let table = collect_core::iceberg::ensure_table(
        &r.cat,
        &r.output,
        "ref_ports",
        schema.clone(),
        iceberg::spec::PartitionSpecBuilder::new(schema),
    )
    .await
    .unwrap();
    collect_core::iceberg::commit_batches(&r.cat, &table, batches, 3, "ref_ports").await.unwrap();

    // Build stops from the real tables, and write them twice.
    let build = || async {
        let ctx = SessionContext::new();
        for t in ["stop_segments", "ref_ports"] {
            register(&ctx, &r.cat, &r.output, t).await.unwrap();
        }
        stops::define_parts_from_segments(&ctx).await.unwrap();
        let built = stops::build_stops(&ctx).await.unwrap();
        ais_tracks::carry::check_batches(&stops::stops_schema(), &built).unwrap();
        built
    };
    let built = build().await;
    let n: usize = built.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 2, "Alpha and Beta");
    let first = replace_table(&r.cat, &r.rest, &r.output, "stops", stops::stops_schema(), &built, &["mmsi"])
        .await
        .unwrap();
    assert!(first.created);
    let second = replace_table(&r.cat, &r.rest, &r.output, "stops", stops::stops_schema(), &build().await, &["mmsi"])
        .await
        .unwrap();
    assert!(!second.created);
    assert_eq!(second.files_removed, first.files_added, "the second write replaced the first");
    assert_eq!(r.number("stops", "SELECT count(*) FROM stops").await, 2.0, "replaced, not appended");

    // Voyages from the real tables.
    let ctx = SessionContext::new();
    for t in ["stops", "tracks", "track_points"] {
        register(&ctx, &r.cat, &r.output, t).await.unwrap();
    }
    let v = voyages::build(&ctx, false).await.unwrap();
    ais_tracks::carry::check_batches(&voyages::voyages_schema(), &v).unwrap();
    replace_table(&r.cat, &r.rest, &r.output, "voyages", voyages::voyages_schema(), &v, &["mmsi"])
        .await
        .unwrap();
    // Alpha -> Beta and the open leg after Beta for vessel 1, one open leg for vessel 2.
    assert_eq!(r.number("voyages", "SELECT count(*) FROM voyages").await, 3.0);
    let nm = r
        .number(
            "voyages",
            "SELECT distance_nm_clean FROM voyages WHERE origin_port_name = 'Alpha' AND dest_port_name = 'Beta'",
        )
        .await;
    assert!((nm - 60.0).abs() < 8.0, "Alpha to Beta is about 60 nm, got {nm}");
}

fn statics_for(d: i64) -> Vec<Static> {
    vec![
        (366000001, 10 + d, Some(9811000), Some("ALPHA1"), Some("ALPHA SHIP@@"), Some("Cargo"), Some(200), Some(50), Some(15), Some(15), "Class A", None),
        (366000001, 20 + d, Some(9811000), Some("ALPHA1"), Some("ALPHA SHIP@@"), Some("Cargo"), Some(200), Some(50), Some(15), Some(15), "Class A", None),
        (366000002, 30 + d, None, None, Some("RUNNER"), Some("Tanker"), None, None, None, None, "Class A", None),
    ]
}

/// A third day for the scenario: the second day's reports, a day later.
fn day_two() -> Vec<RawPoint> {
    two_days()
        .1
        .into_iter()
        .map(|mut p| {
            p.ts_us += 86_400_000_000;
            p
        })
        .collect()
}

#[tokio::test]
async fn vessels_are_folded_incrementally_and_always_match_a_rebuild_from_silver() {
    let r = rig().await;
    let (d0, d1) = two_days();
    r.put_silver_day(0, &d0).await;
    r.put_silver_day(1, &d1).await;
    r.put_statics_day(0, &statics_for(0)).await;
    r.put_statics_day(1, &statics_for(1)).await;

    // First build: nothing to increment from, so it folds every daily table.
    let s = r.daily(&catch_up(), false).await;
    assert_eq!(s.built, 2 + 2 + 2 + 2 + 1, "track-points, statics-daily, tracks, stop-segments, vessels: {s:?}");
    let log = r.log().await;
    let last = log.days("vessels").into_iter().max().unwrap();
    assert!(log.get("vessels", last).unwrap().input_token.starts_with("refold"));
    assert_eq!(last, day(1).date_naive());
    assert_eq!(r.number("vessels", "SELECT max(folded_through) FROM vessels").await as i32, date_to_day(day(1).date_naive()));

    let out = r.vessels_text(false).await;
    let oracle = r.vessels_text(true).await;
    assert_eq!(out.0, oracle.0, "vessels equal a rebuild from silver");
    assert_eq!(out.1, oracle.1, "attributes equal a rebuild from silver");
    let total = (d0.len() + d1.len()) as f64;
    let counted = r.number("vessels", "SELECT sum(n_positions) FROM vessels").await;
    assert_eq!(counted, total, "every silver report is counted once");
    assert!(out.0.contains("ALPHA SHIP") && out.0.contains("imo:9811000"));

    // Nothing new: vessels are up to date.
    let s = r.daily(&catch_up(), false).await;
    assert_eq!((s.built, s.skipped), (0, 2 + 2 + 2 + 2 + 1), "{s:?}");

    // A new day is merged into the tables instead of refolding.
    let d2 = day_two();
    r.put_silver_day(2, &d2).await;
    r.put_statics_day(2, &statics_for(2)).await;
    let s = r.daily(&catch_up(), false).await;
    assert!(s.built >= 5, "{s:?}");
    let log = r.log().await;
    let last = log.days("vessels").into_iter().max().unwrap();
    assert_eq!(last, day(2).date_naive());
    assert!(
        log.get("vessels", last).unwrap().input_token.starts_with("increment"),
        "got {:?}",
        log.get("vessels", last).unwrap().input_token
    );
    let out = r.vessels_text(false).await;
    let oracle = r.vessels_text(true).await;
    assert_eq!(out.0, oracle.0, "after an increment: vessels");
    assert_eq!(out.1, oracle.1, "after an increment: attributes");
    let counted = r.number("vessels", "SELECT sum(n_positions) FROM vessels").await;
    assert_eq!(counted, (d0.len() + d1.len() + d2.len()) as f64);

    // Late data for an already folded day forces a refold, and still matches.
    let late = extra_point(366000002, 100, 30.0, 40.0, 12.0, 1);
    r.append_silver(0, &[late]).await;
    r.daily(&catch_up(), false).await;
    let log = r.log().await;
    let last = log.days("vessels").into_iter().max().unwrap();
    assert!(
        log.get("vessels", last).unwrap().input_token.starts_with("refold"),
        "got {:?}",
        log.get("vessels", last).unwrap().input_token
    );
    let out = r.vessels_text(false).await;
    let oracle = r.vessels_text(true).await;
    assert_eq!(out.0, oracle.0, "after a refold: vessels");
    assert_eq!(out.1, oracle.1, "after a refold: attributes");
    let counted = r.number("vessels", "SELECT sum(n_positions) FROM vessels").await;
    assert_eq!(counted, (d0.len() + d1.len() + d2.len() + 1) as f64);
}

/// Reference ports appended the way `ports load` does.
async fn load_ports(r: &Rig) {
    load_ports_csv(r, PORTS_CSV).await;
}

async fn load_ports_csv(r: &Rig, text: &str) {
    use ais_tracks::ports::{load_csv, ref_ports_schema};
    let csv = r._dir.path().join("pub150.csv");
    std::fs::write(&csv, text).unwrap();
    let batches = load_csv(csv.to_str().unwrap(), "2026-03-01").await.unwrap();
    let schema = ref_ports_schema();
    let table = collect_core::iceberg::ensure_table(
        &r.cat,
        &r.output,
        "ref_ports",
        schema.clone(),
        iceberg::spec::PartitionSpecBuilder::new(schema),
    )
    .await
    .unwrap();
    collect_core::iceberg::commit_batches(&r.cat, &table, batches, 3, "ref_ports").await.unwrap();
}

const STOP_COLS: &str = "stop_id, mmsi, arrive_ts, depart_ts, n_segments, n_points, round(lat, 9) AS lat, \
     round(lon, 9) AS lon, radius_nm, n_moored, n_anchored, port_id, port_name, \
     round(port_distance_nm, 6) AS port_distance_nm";

async fn text_of(ctx: &SessionContext, table: &str) -> String {
    let b = ctx
        .sql(&format!("SELECT {STOP_COLS} FROM {table} ORDER BY stop_id"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    arrow::util::pretty::pretty_format_batches(&b).unwrap().to_string()
}

impl Rig {
    /// The `stops` table as text.
    async fn stops_text(&self) -> String {
        let ctx = SessionContext::new();
        register_as(&ctx, &self.cat, &self.output, "stops", "s").await.unwrap();
        text_of(&ctx, "s").await
    }

    /// What a full rebuild from every stop segment gives, as text.
    async fn full_stops_text(&self) -> String {
        use ais_tracks::stops;
        let ctx = SessionContext::new();
        register(&ctx, &self.cat, &self.output, "stop_segments").await.unwrap();
        register(&ctx, &self.cat, &self.output, "ref_ports").await.unwrap();
        stops::define_parts_from_segments(&ctx).await.unwrap();
        let built = stops::build_stops(&ctx).await.unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "f",
            Arc::new(datafusion::datasource::MemTable::try_new(built[0].schema(), vec![built]).unwrap()),
        )
        .unwrap();
        text_of(&ctx, "f").await
    }

    async fn stops_log_token(&self, d: i64) -> String {
        self.log().await.get("stops", day(d).date_naive()).unwrap().input_token.clone()
    }
}

#[tokio::test]
async fn stops_are_folded_day_by_day_and_always_match_a_full_build() {
    let r = rig().await;
    load_ports(&r).await;
    let (d0, d1) = two_days();

    // Day 0 alone: nothing to increment from, so stops are refolded.
    r.put_silver_day(0, &d0).await;
    r.daily(&catch_up(), false).await;
    let after0 = r.stops_text().await;
    assert_eq!(after0, r.full_stops_text().await, "after day 0");
    assert!(after0.contains("Alpha"), "{after0}");

    // Day 1 is folded in: the Beta stop began on day 0 and ends on day 1, so it
    // moves from day 0's partition to day 1's.
    r.put_silver_day(1, &d1).await;
    let s = r.daily(&catch_up(), false).await;
    assert!(s.built >= 5, "{s:?}");
    let after1 = r.stops_text().await;
    assert_eq!(after1, r.full_stops_text().await, "after folding day 1");
    let beta = r.number("stops", "SELECT count(*) FROM stops WHERE port_name = 'Beta'").await;
    assert_eq!(beta, 1.0, "the stop that crossed midnight is one row, not two");
    let segs = r.number("stops", "SELECT max(n_segments) FROM stops WHERE port_name = 'Beta'").await;
    assert_eq!(segs, 2.0, "it holds both day pieces");
    assert!(r.stops_log_token(1).await.starts_with("seg="));
    // No log row was needed to say it was an increment: day 0's row is untouched.
    let up_to_date = r.daily(&catch_up(), false).await;
    assert_eq!(up_to_date.built, 0, "{up_to_date:?}");

    // Folding a day again changes nothing.
    let env = r.env();
    ais_tracks::daily::increment_stops(&env, &r.rest, &r.scratch, day(1).date_naive()).await.unwrap();
    assert_eq!(r.stops_text().await, after1, "rerunning a day is harmless");

    // Day 2 (day 1's reports a day later).
    r.put_silver_day(2, &day_two()).await;
    r.daily(&catch_up(), false).await;
    let after2 = r.stops_text().await;
    assert_eq!(after2, r.full_stops_text().await, "after folding day 2");
    assert_ne!(after2, after1);

    // Late data for day 0 rebuilds its segments, so stops are refolded from all of them.
    let before = r.stops_log_token(0).await;
    r.append_silver(0, &[extra_point(366000001, 3600, 10.0, 20.0, 0.1, 0)]).await;
    r.daily(&catch_up(), false).await;
    assert_ne!(r.stops_log_token(0).await, before, "day 0 was folded again");
    assert_eq!(r.stops_text().await, r.full_stops_text().await, "after a refold");
}

// ---- voyages -----------------------------------------------------------------------------

const FLEET_STEP: i64 = 30;

/// Every column that must match exactly. The destination stop's position and its
/// distance to the port are left out: a leg records them as the stop stood on the
/// day the leg ended, and a stop that carries on is refined afterwards.
const VOYAGE_COLS: &str = "voyage_id, mmsi, depart_ts, arrive_ts, round(duration_s, 3) AS duration_s, \
    origin_known, dest_known, is_open, origin_stop_id, dest_stop_id, \
    round(origin_lat, 9) AS origin_lat, round(origin_lon, 9) AS origin_lon, origin_port_id, \
    origin_port_name, origin_unlocode, origin_country, round(origin_port_distance_nm, 6) AS origin_port_distance_nm, \
    dest_port_id, dest_port_name, dest_unlocode, dest_country, \
    round(distance_nm_raw, 6) AS distance_nm_raw, round(distance_nm_clean, 6) AS distance_nm_clean, \
    round(avg_speed_kn, 6) AS avg_speed_kn, round(max_sog_knots, 6) AS max_sog_knots, \
    n_points, n_gaps, n_outliers, declared_destination, n_declared_destinations, declared_eta, \
    declared_matches_dest";

fn fleet_day_points(k: i64) -> Vec<RawPoint> {
    let (pts, _) = fleet(FLEET_STEP, 4);
    raw_points(&pts.into_iter().filter(|p| p.1 >= k * 86_400 && p.1 < (k + 1) * 86_400).collect::<Vec<_>>())
}

fn fleet_day_statics(k: i64) -> Vec<Static> {
    let (pts, dests) = fleet(FLEET_STEP, 4);
    let mut seen: Vec<i64> = pts.iter().filter(|p| p.1 / 86_400 == k).map(|p| p.0).collect();
    seen.sort();
    seen.dedup();
    let mut rows: Vec<Static> = seen
        .iter()
        .map(|m| {
            let name: &'static str = Box::leak(format!("SHIP {m}").into_boxed_str());
            (*m, 60, None, None, Some(name), None, None, None, None, None, "Class A", None)
        })
        .collect();
    for (m, t, d) in dests.into_iter().filter(|d| d.1 / 86_400 == k) {
        rows.push((m, t % 86_400, None, None, None, None, None, None, None, None, "Class A", Some(FLEET_DEST[d])));
    }
    rows
}

impl Rig {
    async fn has(&self, base: &str) -> bool {
        self.cat.table_exists(&table_ident(&self.output, base)).await.unwrap()
    }

    /// `voyages` and `open_voyages` together, every column.
    async fn voyages_batches(&self) -> Vec<RecordBatch> {
        let ctx = SessionContext::new();
        let mut parts = Vec::new();
        for (base, alias) in [("voyages", "v"), ("open_voyages", "o")] {
            if !self.has(base).await {
                continue;
            }
            let raw = format!("raw_{alias}");
            register_as(&ctx, &self.cat, &self.output, base, &raw).await.unwrap();
            let t = ais_tracks::vessels::materialize(&ctx, &format!("SELECT * FROM {raw}")).await.unwrap();
            ctx.register_table(alias, t).unwrap();
            parts.push(format!("SELECT * FROM {alias}"));
        }
        if parts.is_empty() {
            return Vec::new();
        }
        ctx.sql(&parts.join(" UNION ALL ")).await.unwrap().collect().await.unwrap()
    }

    /// The full computation over all of stops, track_points and statics.
    async fn oracle_batches(&self) -> Vec<RecordBatch> {
        let ctx = SessionContext::new();
        register(&ctx, &self.cat, &self.output, "stops").await.unwrap();
        register(&ctx, &self.cat, &self.output, "track_points").await.unwrap();
        register(&ctx, &self.cat, &self.input, "statics").await.unwrap();
        ais_tracks::voyages::build(&ctx, true).await.unwrap()
    }
}

/// The incremental voyages must equal the full computation: exactly, except for
/// the destination stop's position, which must agree closely.
async fn assert_voyages_match(r: &Rig, what: &str) {
    let (inc, ora) = (r.voyages_batches().await, r.oracle_batches().await);
    assert!(!ora.is_empty(), "{what}: the full computation produced nothing");
    assert!(!inc.is_empty(), "{what}: the incremental tables are empty");
    let ctx = SessionContext::new();
    for (name, b) in [("inc", inc), ("ora", ora)] {
        ctx.register_table(name, Arc::new(datafusion::datasource::MemTable::try_new(b[0].schema(), vec![b]).unwrap()))
            .unwrap();
    }
    let text = |t: &'static str| {
        let ctx = ctx.clone();
        async move {
            let b = ctx
                .sql(&format!("SELECT {VOYAGE_COLS} FROM {t} ORDER BY mmsi, depart_ts"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            arrow::util::pretty::pretty_format_batches(&b).unwrap().to_string()
        }
    };
    assert_same_text(&text("inc").await, &text("ora").await, what);
    let far = ctx
        .sql(
            "SELECT count(*) FROM inc i JOIN ora o ON i.voyage_id = o.voyage_id
             WHERE abs(coalesce(i.dest_lat - o.dest_lat, 0)) > 0.02
                OR abs(coalesce(i.dest_lon - o.dest_lon, 0)) > 0.02
                OR abs(coalesce(i.dest_port_distance_nm - o.dest_port_distance_nm, 0)) > 0.5
                OR (i.dest_lat IS NULL) <> (o.dest_lat IS NULL)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let far = arrow::compute::cast(far[0].column(0), &arrow::datatypes::DataType::Int64).unwrap();
    assert_eq!(far.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0), 0, "{what}: destination stop far off");
}

fn assert_same_text(got: &str, want: &str, what: &str) {
    if got == want {
        return;
    }
    let (g, w): (Vec<_>, Vec<_>) = (got.lines().collect(), want.lines().collect());
    let at = g.iter().zip(&w).position(|(a, b)| a != b).unwrap_or(g.len().min(w.len()));
    panic!(
        "{what}: incremental ({} lines) differs from the full computation ({} lines), first at line {at}:\n  got:  {}\n  want: {}",
        g.len(),
        w.len(),
        g.get(at).unwrap_or(&"<none>"),
        w.get(at).unwrap_or(&"<none>"),
    );
}

#[tokio::test]
async fn voyages_are_folded_day_by_day_and_always_equal_the_full_computation() {
    let r = rig().await;
    load_ports_csv(&r, FLEET_PORTS_CSV).await;

    let mut day0_built = 0;
    for k in 0..4 {
        r.put_silver_day(k, &fleet_day_points(k)).await;
        r.put_statics_day(k, &fleet_day_statics(k)).await;
        r.daily(&catch_up(), false).await;
        assert_voyages_match(&r, &format!("after day {k}")).await;
        if k == 0 {
            day0_built = r.log().await.get("voyages", day(0).date_naive()).unwrap().built_at_us;
        }
    }
    // Day 0 was built once and never replayed: days 1-3 were increments.
    assert_eq!(r.log().await.get("voyages", day(0).date_naive()).unwrap().built_at_us, day0_built);

    // What the scenario is meant to exercise happened.
    let n = |sql: &str| {
        let sql = sql.to_string();
        let r = &r;
        async move { r.number("voyages", &sql).await }
    };
    assert!(n("SELECT count(*) FROM voyages WHERE mmsi = 366000002").await >= 6.0, "the shuttle made many legs");
    assert!(n("SELECT count(*) FROM voyages WHERE mmsi = 366000003 AND NOT origin_known").await >= 1.0, "first seen at sea");
    assert!(n("SELECT count(*) FROM voyages WHERE declared_matches_dest = false").await >= 1.0, "a wrong declaration is caught");
    assert!(n("SELECT count(*) FROM voyages WHERE declared_matches_dest = true").await >= 1.0);
    let open = r.number("open_voyages", "SELECT count(*) FROM open_voyages").await;
    assert!(open >= 2.0, "the cruiser and others are still under way: {open}");

    // Nothing changed: nothing is rebuilt.
    let s = r.daily(&catch_up(), false).await;
    assert_eq!(s.built, 0, "{s:?}");

    // Late data for day 1 replays the voyages and still agrees.
    let late = extra_point(366000004, 86_400 + 5_000, 10.0, 30.5, 12.0, 1);
    r.append_silver(1, &[late]).await;
    r.daily(&catch_up(), false).await;
    assert_voyages_match(&r, "after late data").await;
}
