//! The Rust reducer, with thinning off, must agree row for row with the SQL in
//! `track_points.rs` on randomised data full of the awkward cases: duplicates,
//! spikes, gaps, rows without a position, invalid values, and state carried
//! from the previous day.

use std::sync::Arc;

use ais_tracks::reduce::{reduce_vessel, Dicts, OutRow, RawPoint, Rules, StreamState, ThinOpts};
use ais_tracks::track_points::{self, Params};
use arrow::array::{
    new_null_array, Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chrono::{Duration, TimeZone, Utc};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const SOURCES: [&str; 3] = ["a", "b", "c"];
const STATIONS: [&str; 2] = ["s1", "s2"];
const NAVS: [&str; 2] = ["under way", "moored"];

fn day_start_us() -> i64 {
    Utc.with_ymd_and_hms(2026, 3, 10, 0, 0, 0).unwrap().timestamp() * 1_000_000
}

fn dicts() -> Dicts {
    Dicts::new(
        SOURCES.iter().map(|s| s.to_string()).collect(),
        STATIONS.iter().map(|s| s.to_string()).collect(),
        NAVS.iter().map(|s| s.to_string()).collect(),
    )
}

fn q7(deg: f64) -> i32 {
    (deg * 1e7).round() as i32
}

/// One vessel's day of messy reports, plus a valid point from the day before.
fn vessel(mmsi: u32, seed: u64) -> (Vec<RawPoint>, StreamState) {
    let mut rng = StdRng::seed_from_u64(seed);
    let (mut lat, mut lon): (f64, f64) = (rng.gen_range(-60.0..60.0), rng.gen_range(-170.0..170.0));
    let mut speed: f64 = rng.gen_range(0.0..20.0);
    let mut heading: f64 = rng.gen_range(0.0..360.0);

    let prev = StreamState {
        ts_us: day_start_us() - 90 * 1_000_000,
        lat: q7(lat) as f64 / 1e7,
        lon: q7(lon) as f64 / 1e7,
    };
    let mut t = day_start_us() + rng.gen_range(0..600) * 1_000_000i64;
    let end = day_start_us() + 86_400 * 1_000_000;
    let mut out = Vec::new();
    let mk = |t: i64, lat: Option<f64>, lon: Option<f64>, sog: f64, cog: f64, hd: f64, nav: Option<u16>, src: u16, stn: Option<u16>| {
        RawPoint {
            ts_us: t,
            mmsi,
            lat_e7: lat.map(q7),
            lon_e7: lon.map(q7),
            sog_dk: Some((sog * 10.0).round() as i16),
            cog_dd: Some((cog * 10.0).round() as i16),
            heading_dd: Some((hd * 10.0).round() as i16),
            nav,
            source: src,
            station: stn,
        }
    };
    while t < end && out.len() < 900 {
        let dt_s = *[2, 10, 10, 10, 30, 180, 600, 600, 2400]
            .get(rng.gen_range(0..9))
            .unwrap() as f64;
        t += (dt_s * 1e6) as i64;
        if t >= end {
            break;
        }
        if rng.gen_bool(0.1) {
            speed = (speed + rng.gen_range(-5.0..5.0)).clamp(0.0, 25.0);
            heading = (heading + rng.gen_range(-40.0..40.0)).rem_euclid(360.0);
        }
        // Move along the track (a crude flat-earth step is fine for test data).
        let nm = speed * dt_s / 3600.0;
        lat = (lat + nm * heading.to_radians().cos() / 60.0).clamp(-80.0, 80.0);
        lon = (lon + nm * heading.to_radians().sin() / 60.0 / lat.to_radians().cos().max(0.2))
            .clamp(-179.0, 179.0);

        let mut plat = Some(lat);
        let mut plon = Some(lon);
        let mut sog = speed;
        let mut cog = heading;
        let mut hd = heading;
        let r: f64 = rng.gen();
        if r < 0.02 {
            plat = None;
            plon = None;
        } else if r < 0.03 {
            plat = Some(91.0); // out of range: not a position
        } else if r < 0.04 {
            plat = Some((lat + rng.gen_range(3.0..8.0)).min(85.0)); // a bad fix
            plon = Some((lon + rng.gen_range(-8.0..8.0)).clamp(-179.0, 179.0));
        }
        if rng.gen_bool(0.02) {
            sog = 102.3;
        }
        if rng.gen_bool(0.02) {
            cog = 360.0;
        }
        if rng.gen_bool(0.02) {
            hd = 511.0;
        }
        let nav = match rng.gen_range(0..3) {
            0 => None,
            1 => Some(0),
            _ => Some(1),
        };
        let stn = if rng.gen_bool(0.5) { Some(0) } else { None };
        out.push(mk(t, plat, plon, sog, cog, hd, nav, 0, stn));
        if rng.gen_bool(0.08) {
            // The same message heard by another receiver.
            let last = out.last().cloned().unwrap();
            out.push(RawPoint {
                source: 1,
                station: Some(1),
                ..last
            });
        }
    }
    (out, prev)
}

fn positions_table(all: &[(RawPoint, bool)]) -> MemTable {
    let schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(
            &collect_core::iceberg::table_schemas::positions_schema(),
        )
        .unwrap(),
    );
    let d = dicts();
    let n = all.len();
    let cols: Vec<Arc<dyn Array>> = schema
        .fields()
        .iter()
        .map(|f| -> Arc<dyn Array> {
            let opt_f = |g: &dyn Fn(&RawPoint) -> Option<f64>| -> Arc<dyn Array> {
                Arc::new(Float64Array::from_iter(all.iter().map(|(p, _)| g(p))))
            };
            match f.name().as_str() {
                "ts" => Arc::new(
                    TimestampMicrosecondArray::from_iter_values(all.iter().map(|(p, _)| p.ts_us))
                        .with_timezone("+00:00"),
                ),
                "source" => Arc::new(StringArray::from_iter_values(
                    all.iter().map(|(p, _)| d.sources[p.source as usize].as_str()),
                )),
                "msg_type" => Arc::new(Int32Array::from(vec![1; n])),
                "mmsi" => Arc::new(Int64Array::from_iter_values(
                    all.iter().map(|(p, _)| p.mmsi as i64),
                )),
                "latitude" => opt_f(&|p| p.lat()),
                "longitude" => opt_f(&|p| p.lon()),
                "sog_knots" => opt_f(&|p| p.sog()),
                "cog" => opt_f(&|p| p.cog()),
                "heading_true" => opt_f(&|p| p.heading()),
                "nav_status" => Arc::new(StringArray::from_iter(
                    all.iter().map(|(p, _)| p.nav.map(|i| d.navs[i as usize].as_str())),
                )),
                "station" => Arc::new(StringArray::from_iter(
                    all.iter()
                        .map(|(p, _)| p.station.map(|i| d.stations[i as usize].as_str())),
                )),
                "payload" => Arc::new(StringArray::from_iter_values(
                    (0..n).map(|i| format!("row-{i:06}")),
                )),
                _ => new_null_array(f.data_type(), n),
            }
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    MemTable::try_new(schema, vec![vec![batch]]).unwrap()
}

/// What we compare, per row.
#[derive(Debug, Clone)]
struct Cmp {
    mmsi: i64,
    ts: i64,
    lat_e7: Option<i64>,
    lon_e7: Option<i64>,
    dup_rank: i64,
    source: String,
    has_position: bool,
    n_dups: i64,
    prev_ts: Option<i64>,
    dt_s: Option<f64>,
    dist_nm: Option<f64>,
    implied: Option<f64>,
    flags: [bool; 7], // gap, jump, spike, sog, cog, heading, outlier
}

fn f64_col(b: &RecordBatch, name: &str) -> Vec<Option<f64>> {
    let c = cast(b.column_by_name(name).unwrap(), &DataType::Float64).unwrap();
    let a = c.as_any().downcast_ref::<Float64Array>().unwrap();
    (0..a.len()).map(|i| (!a.is_null(i)).then(|| a.value(i))).collect()
}

fn i64_col(b: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let c = cast(b.column_by_name(name).unwrap(), &DataType::Int64).unwrap();
    let a = c.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..a.len()).map(|i| (!a.is_null(i)).then(|| a.value(i))).collect()
}

fn bool_col(b: &RecordBatch, name: &str) -> Vec<bool> {
    let a = b
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    (0..a.len()).map(|i| a.value(i)).collect()
}

fn str_col(b: &RecordBatch, name: &str) -> Vec<String> {
    let c = cast(b.column_by_name(name).unwrap(), &DataType::Utf8).unwrap();
    let a = c.as_any().downcast_ref::<StringArray>().unwrap();
    (0..a.len()).map(|i| a.value(i).to_string()).collect()
}

fn from_sql(batches: &[RecordBatch]) -> Vec<Cmp> {
    let mut out = Vec::new();
    for b in batches {
        let (mmsi, ts) = (i64_col(b, "mmsi"), i64_col(b, "ts"));
        let (lat, lon) = (f64_col(b, "latitude"), f64_col(b, "longitude"));
        let (dup, nd) = (i64_col(b, "dup_rank"), i64_col(b, "n_dups"));
        let src = str_col(b, "source");
        let (hp, prev) = (bool_col(b, "has_position"), i64_col(b, "prev_ts"));
        let (dt, dist, imp) = (f64_col(b, "dt_s"), f64_col(b, "dist_nm"), f64_col(b, "implied_speed_kn"));
        let fl: Vec<Vec<bool>> = [
            "gap_before", "is_speed_jump", "is_spike", "is_sog_invalid", "is_cog_invalid",
            "is_heading_invalid", "is_outlier",
        ]
        .iter()
        .map(|n| bool_col(b, n))
        .collect();
        for i in 0..b.num_rows() {
            out.push(Cmp {
                mmsi: mmsi[i].unwrap(),
                ts: ts[i].unwrap(),
                lat_e7: lat[i].map(|v| (v * 1e7).round() as i64),
                lon_e7: lon[i].map(|v| (v * 1e7).round() as i64),
                dup_rank: dup[i].unwrap(),
                source: src[i].clone(),
                has_position: hp[i],
                n_dups: nd[i].unwrap(),
                prev_ts: prev[i],
                dt_s: dt[i],
                dist_nm: dist[i],
                implied: imp[i],
                flags: [fl[0][i], fl[1][i], fl[2][i], fl[3][i], fl[4][i], fl[5][i], fl[6][i]],
            });
        }
    }
    out
}

fn from_rust(rows: &[OutRow], d: &Dicts) -> Vec<Cmp> {
    rows.iter()
        .map(|r| Cmp {
            mmsi: r.mmsi as i64,
            ts: r.ts_us,
            lat_e7: r.lat_e7.map(|v| v as i64),
            lon_e7: r.lon_e7.map(|v| v as i64),
            dup_rank: r.dup_rank as i64,
            source: d.sources[r.source as usize].clone(),
            has_position: r.has_position,
            n_dups: r.n_dups as i64,
            prev_ts: r.prev_ts_us,
            dt_s: r.dt_s,
            dist_nm: r.dist_nm,
            implied: r.implied_speed_kn,
            flags: [
                r.gap_before, r.is_speed_jump, r.is_spike, r.is_sog_invalid, r.is_cog_invalid,
                r.is_heading_invalid, r.is_outlier,
            ],
        })
        .collect()
}

fn sort_key(c: &Cmp) -> (i64, i64, i64, i64, i64, String) {
    (
        c.mmsi,
        c.ts,
        c.lat_e7.unwrap_or(i64::MAX),
        c.lon_e7.unwrap_or(i64::MAX),
        c.dup_rank,
        c.source.clone(),
    )
}

fn close(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => (x - y).abs() <= 1e-9 * (1.0 + x.abs().max(y.abs())),
        _ => false,
    }
}

async fn compare(n_vessels: u32, seed: u64) -> [usize; 6] {
    let d = dicts();
    let mut sql_input: Vec<(RawPoint, bool)> = Vec::new();
    let mut rust_rows = Vec::new();
    let rules = Rules::default();
    for v in 0..n_vessels {
        let (pts, prev) = vessel(1000 + v, seed * 1000 + v as u64);
        // The day before: one positioned row, which SQL finds by lookback.
        sql_input.push((
            RawPoint {
                ts_us: prev.ts_us,
                mmsi: 1000 + v,
                lat_e7: Some(q7(prev.lat)),
                lon_e7: Some(q7(prev.lon)),
                sog_dk: Some(0),
                cog_dd: Some(0),
                heading_dd: Some(0),
                nav: None,
                source: 2,
                station: None,
            },
            false,
        ));
        for p in &pts {
            sql_input.push((p.clone(), true));
        }
        let r = reduce_vessel(pts, &d, Some(prev), &rules, &ThinOpts::off());
        assert_eq!(r.unaccounted_raw, 0);
        rust_rows.extend(r.rows);
    }

    let ctx = SessionContext::new();
    ctx.register_table("positions", Arc::new(positions_table(&sql_input))).unwrap();
    let params = Params {
        day_start: Utc.timestamp_opt(day_start_us() / 1_000_000, 0).unwrap(),
        lookback: Duration::days(2),
        shards: 1,
        max_speed_kn: rules.max_speed_kn,
        gap: Duration::seconds(rules.gap_s as i64),
    };
    let sql = track_points::build_shard(&ctx, &params, 0).await.unwrap();

    let mut a = from_sql(&sql);
    let mut b = from_rust(&rust_rows, &d);
    a.sort_by_key(sort_key);
    b.sort_by_key(sort_key);
    assert_eq!(a.len(), b.len(), "row counts (seed {seed})");
    for (i, (x, y)) in a.iter().zip(&b).enumerate() {
        let ctx = format!("seed {seed} row {i}: sql {x:?}\n rust {y:?}");
        assert_eq!(sort_key(x), sort_key(y), "{ctx}");
        assert_eq!(x.has_position, y.has_position, "{ctx}");
        assert_eq!(x.n_dups, y.n_dups, "{ctx}");
        assert_eq!(x.prev_ts, y.prev_ts, "{ctx}");
        assert!(close(x.dt_s, y.dt_s), "dt {ctx}");
        assert!(close(x.dist_nm, y.dist_nm), "dist {ctx}");
        assert!(close(x.implied, y.implied), "implied {ctx}");
        assert_eq!(x.flags, y.flags, "flags {ctx}");
    }
    // What the data exercised, so a pass cannot be vacuous.
    [
        a.iter().filter(|c| c.flags[2]).count(),                 // spikes
        a.iter().filter(|c| c.flags[0] && c.prev_ts.is_some()).count(), // gaps after a hop
        a.iter().filter(|c| c.dup_rank > 1).count(),             // duplicates
        a.iter().filter(|c| !c.has_position).count(),            // no usable position
        a.iter().filter(|c| c.flags[3] || c.flags[4] || c.flags[5]).count(), // invalid values
        a.iter().filter(|c| c.flags[1] && !c.flags[2]).count(),  // jumps that are not spikes
    ]
}

#[tokio::test]
async fn rust_reducer_matches_the_sql_on_random_days() {
    let mut seen = [0usize; 6];
    for seed in 1..=8 {
        for (t, n) in seen.iter_mut().zip(compare(6, seed).await) {
            *t += n;
        }
    }
    let names = ["spikes", "gaps", "duplicates", "no position", "invalid values", "jumps"];
    for (n, name) in seen.iter().zip(names) {
        assert!(*n > 0, "the random data never produced {name}: {seen:?}");
    }
    eprintln!("exercised: {seen:?}");
}
