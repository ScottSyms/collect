//! The incremental `vessels` fold must agree with building from all of silver:
//! day by day, in one refold, and from scratch give the same tables.

mod support;

use std::collections::HashMap;
use std::sync::Arc;

use ais_tracks::reduce::{Dicts, RawPoint, Rules, ThinOpts};
use ais_tracks::reduce_day::reduce_buckets_in_memory;
use ais_tracks::vessels::{
    self, attr_parts_sql, attribute_daily_schema, define_parts, sta_parts_sql, static_daily_schema,
    vessel_daily_batch, vessel_daily_schema, with_day_ts, Vessels,
};
use arrow::record_batch::RecordBatch;
use chrono::{Duration, TimeZone, Utc};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use support::scenario::{silver_batch, statics_batch, Static};

const DAY_US: i64 = 86_400_000_000;

fn day0_us() -> i64 {
    Utc.with_ymd_and_hms(2026, 3, 10, 0, 0, 0).unwrap().timestamp() * 1_000_000
}

fn day_number(d: i64) -> i32 {
    (day0_us() / DAY_US + d) as i32
}

fn statics_for_day(d: i64) -> Vec<Static> {
    match d {
        0 => vec![
            // a ship whose type 24 arrives in halves, with a padded name
            (366123456, 10, None, None, Some("EVER GIVEN@@@"), None, None, None, None, None, "Class A", None),
            (366123456, 11, None, Some("KABC@@"), None, Some("Cargo"), Some(300), Some(100), Some(20), Some(40), "Class A", None),
            // two IMOs, the more frequent one invalid
            (211000001, 5, Some(1234568), None, Some("TWO IMOS"), None, None, None, None, None, "Class A", None),
            (211000001, 9, Some(1234568), None, Some("TWO IMOS"), None, None, None, None, None, "Class A", None),
            // a placeholder name and "no IMO"
            (538000002, 1, Some(0), None, Some("@@@@@@@@"), None, None, None, None, None, "Class B", None),
        ],
        1 => vec![
            (366123456, 20, None, None, Some("EVER GIVEN@@@"), None, None, None, None, None, "Class A", None),
            (211000001, 30, Some(1234567), None, Some("TWO IMOS"), None, None, None, None, None, "Class A", None),
            (992471234, 40, None, None, Some("BUOY 7"), Some("AtoN"), None, None, None, None, "Class A", None),
        ],
        _ => vec![
            (366123456, 50, None, None, Some("EVER GIVN"), None, None, None, None, None, "Class A", None),
            (211000001, 60, Some(1234567), None, Some("TWO IMOS"), None, None, None, None, None, "Class A", None),
        ],
    }
}

fn point(mmsi: u32, d: i64, sec: i64) -> RawPoint {
    RawPoint {
        ts_us: day0_us() + d * DAY_US + sec * 1_000_000,
        mmsi,
        lat_e7: Some(100_000_000 + (sec as i32) * 10),
        lon_e7: Some(200_000_000),
        sog_dk: Some(100),
        cog_dd: Some(900),
        heading_dd: None,
        nav: Some(0),
        source: 0,
        station: None,
    }
}

fn positions_for_day(d: i64) -> Vec<RawPoint> {
    let mut v = Vec::new();
    for sec in [100, 200, 300, 400] {
        v.push(point(366123456, d, sec));
        v.push(point(211000001, d, sec + 1));
    }
    if d == 1 {
        v.push(point(111111111, d, 7)); // never sends statics
        v.push(point(111111111, d, 7)); // a duplicate report still counts
    }
    v
}

fn dicts() -> Dicts {
    Dicts::new(vec!["a".into()], vec![], vec!["moored".into()])
}

async fn register_mem(ctx: &SessionContext, name: &str, batches: &[RecordBatch]) {
    let _ = ctx.deregister_table(name);
    ctx.register_table(
        name,
        Arc::new(MemTable::try_new(batches[0].schema(), vec![batches.to_vec()]).unwrap()),
    )
    .unwrap();
}

/// Rows of both tables as comparable text, without the columns that
/// legitimately differ (when it was computed, and how it was folded).
async fn snapshot(v: &Vessels) -> (String, String) {
    let ctx = SessionContext::new();
    register_mem(&ctx, "v", &v.vessels).await;
    register_mem(&ctx, "a", &v.attributes).await;
    let text = |sql: &'static str| {
        let ctx = ctx.clone();
        async move {
            let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            arrow::util::pretty::pretty_format_batches(&b).unwrap().to_string()
        }
    };
    (
        text("SELECT * EXCEPT (computed_at, folded_through) FROM v ORDER BY mmsi").await,
        text("SELECT * EXCEPT (computed_at, folded_through) FROM a ORDER BY mmsi, attribute, rank").await,
    )
}

/// One day's aggregates, computed the way the daily runners do.
struct Daily {
    pos: Vec<RecordBatch>,
    sta: Vec<RecordBatch>,
    attr: Vec<RecordBatch>,
}

async fn daily_for(d: i64) -> Daily {
    // positions: through the reducer, as track-points does
    let reduced = reduce_buckets_in_memory(positions_for_day(d), &dicts(), &Rules::default(), &ThinOpts::default());
    let pos = vessel_daily_batch(day0_us() + d * DAY_US, &reduced.days).unwrap().unwrap();
    // statics: through SQL over the day's rows, as statics-daily does
    let ctx = SessionContext::new();
    register_mem(&ctx, "statics_day", &[statics_batch(d, &statics_for_day(d))]).await;
    let ts = day0_us() + d * DAY_US;
    let sta = ctx.sql(&sta_parts_sql("statics_day")).await.unwrap().collect().await.unwrap();
    let attr = ctx.sql(&attr_parts_sql("statics_day")).await.unwrap().collect().await.unwrap();
    Daily {
        pos: vec![pos],
        sta: sta.iter().map(|b| with_day_ts(b, ts, &static_daily_schema()).unwrap()).collect(),
        attr: attr.iter().map(|b| with_day_ts(b, ts, &attribute_daily_schema()).unwrap()).collect(),
    }
}

/// Registers the given days' aggregates as `daily_*`, the way the runner does.
async fn register_dailies(ctx: &SessionContext, days: &[&Daily]) {
    let cat = |f: &dyn Fn(&Daily) -> &Vec<RecordBatch>| -> Vec<RecordBatch> {
        days.iter().flat_map(|d| f(d).iter().cloned()).collect()
    };
    for (raw, alias, cols, batches) in [
        ("raw_pos", "daily_pos", "mmsi, first_seen, last_seen, n_positions", cat(&|d| &d.pos)),
        ("raw_sta", "daily_sta", "mmsi, first_static_seen, last_static_seen, n_statics, ais_class", cat(&|d| &d.sta)),
        ("raw_attr", "daily_attr", "mmsi, attribute, value, n_obs, first_seen, last_seen", cat(&|d| &d.attr)),
    ] {
        register_mem(ctx, raw, &batches).await;
        let view = ctx.sql(&format!("SELECT {cols} FROM {raw}")).await.unwrap().into_view();
        let _ = ctx.deregister_table(alias);
        ctx.register_table(alias, view).unwrap();
    }
}

#[tokio::test]
async fn folding_day_by_day_equals_a_refold_equals_building_from_silver() {
    let n_days = 3;

    // From all of silver.
    let ctx = SessionContext::new();
    let pos: Vec<RawPoint> = (0..n_days).flat_map(positions_for_day).collect();
    register_mem(&ctx, "positions", &[silver_batch(&pos, &dicts())]).await;
    let st: Vec<RecordBatch> = (0..n_days).map(|d| statics_batch(d, &statics_for_day(d))).collect();
    register_mem(&ctx, "statics", &st).await;
    let oracle = snapshot(&vessels::build(&ctx).await.unwrap()).await;
    assert!(oracle.0.contains("366123456") && oracle.0.contains("111111111") && oracle.0.contains("992471234"));

    let dailies: Vec<Daily> = {
        let mut v = Vec::new();
        for d in 0..n_days {
            v.push(daily_for(d).await);
        }
        v
    };

    // A refold: every daily table at once, no prior.
    let ctx = SessionContext::new();
    register_dailies(&ctx, &dailies.iter().collect::<Vec<_>>()).await;
    define_parts(&ctx, false).await.unwrap();
    let refold = snapshot(&vessels::merge(&ctx, day_number(n_days - 1)).await.unwrap()).await;

    // Increments: each day merges into the last result.
    let mut prior: Option<Vessels> = None;
    for (d, daily) in dailies.iter().enumerate() {
        let ctx = SessionContext::new();
        register_dailies(&ctx, &[daily]).await;
        if let Some(p) = &prior {
            register_mem(&ctx, "prior_vessels", &p.vessels).await;
            register_mem(&ctx, "prior_attributes", &p.attributes).await;
        }
        define_parts(&ctx, prior.is_some()).await.unwrap();
        prior = Some(vessels::merge(&ctx, day_number(d as i64)).await.unwrap());
    }
    let increment = snapshot(prior.as_ref().unwrap()).await;

    assert_eq!(refold.0, oracle.0, "refold vs silver: vessels");
    assert_eq!(refold.1, oracle.1, "refold vs silver: attributes");
    assert_eq!(increment.0, oracle.0, "day-by-day vs silver: vessels");
    assert_eq!(increment.1, oracle.1, "day-by-day vs silver: attributes");

    // Spot checks on what the fold is for.
    assert!(oracle.0.contains("EVER GIVEN"), "the padded name is cleaned and the most reported wins");
    assert!(oracle.0.contains("imo:1234567"), "the valid IMO beats the more frequent invalid one");
    let _ = (Duration::days(1), HashMap::<u32, u32>::new(), vessel_daily_schema());
}
