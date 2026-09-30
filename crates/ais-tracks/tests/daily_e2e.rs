//! The daily flow end to end against real Iceberg tables on the local
//! filesystem: silver in, `track_points` / `tracks` / `stop_segments` /
//! `vessel_state` / `build_log` out, through the real scan, writers and both
//! commit paths. Covers first build, rerun, late data that changes where a
//! vessel's stream ends (which must ripple forward) and late data that does not
//! (which must not).

mod support;

use std::sync::Arc;

use ais_tracks::daily::{self, register, DailyRun, Env, StopTuning, Summary};
use ais_tracks::output::{commit_day, ensure_day_table, DayWriter};
use ais_tracks::reduce::{Dicts, RawPoint};
use ais_tracks::reduce_day::ReduceOptions;
use ais_tracks::state::{self, date_to_day, DaySelect, Log};
use chrono::Duration;
use collect_core::iceberg::{ensure_namespace, table_ident, IcebergConfig};
use collect_maint::commit::RestClient;
use datafusion::prelude::SessionContext;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::Catalog;
use support::scenario::{day, raw_points, scenario, silver_batch, NAVS};
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
    assert_eq!((s.built, s.skipped), (6, 0), "2 days x 3 steps: {s:?}");

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
    assert_eq!((s.built, s.skipped), (0, 6), "{s:?}");

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
    assert_eq!((s.built, s.skipped), (6, 0), "the change reaches day 1 through vessel state: {s:?}");
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
    assert_eq!((s.built, s.skipped), (5, 1), "{s:?}");
    let represented = r.number("track_points", "SELECT sum(n_raw) FROM track_points").await;
    assert_eq!(represented as usize, n0 + n1 + 2);

    // ---- an explicit range rebuilds whatever the log says ------------------
    let forced = DaySelect { from: Some(day(1).date_naive()), ..Default::default() };
    let s = r.daily(&forced, false).await;
    assert_eq!((s.built, s.skipped), (3, 0), "{s:?}");
    let _ = Arc::new(0); // keep Arc imported for future assertions
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
        for t in ["stop_segments", "tracks", "ref_ports", "track_points"] {
            register(&ctx, &r.cat, &r.output, t).await.unwrap();
        }
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
