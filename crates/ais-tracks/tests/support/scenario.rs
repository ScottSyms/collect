//! A synthetic vessel that berths, sails and berths again across midnight, and
//! helpers to turn it into silver rows.
#![allow(dead_code)]

use std::sync::Arc;

use ais_tracks::reduce::{Dicts, RawPoint};
use arrow::array::{new_null_array, Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration, TimeZone, Utc};

/// Longitude degrees per second at 12 knots, at latitude 10 degrees.
pub const DEG_PER_S: f64 = 12.0 / 3600.0 / 59.09;
pub const H: i64 = 3600;

pub fn day(n: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 10, 0, 0, 0).unwrap() + Duration::days(n)
}

/// (mmsi, seconds from day 0, lat, lon, sog, nav_status)
pub type Pt = (i64, i64, f64, f64, f64, &'static str);

/// `step` is the reporting interval in seconds.
pub fn scenario(step: i64) -> Vec<Pt> {
    let mut v = Vec::new();
    let jitter = |i: i64| if i % 2 == 0 { 0.0005 } else { -0.0005 };
    // Alpha, 00:00-06:00
    let (mut t, mut i) = (0, 0);
    while t <= 6 * H {
        v.push((366000001, t, 10.0 + jitter(i), 20.0 + jitter(i + 1), 0.1, "moored"));
        t += step;
        i += 1;
    }
    // Underway 06:00-11:00
    let mut t = 6 * H + step;
    while t < 11 * H {
        v.push((366000001, t, 10.0, 20.0 + (t - 6 * H) as f64 * DEG_PER_S, 12.0, "under way using engine"));
        t += step;
    }
    let beta = 20.0 + 5.0 * H as f64 * DEG_PER_S;
    // Beta, 11:00 until 03:00 the next day
    let (mut t, mut i) = (11 * H, 0);
    while t <= 27 * H {
        v.push((366000001, t, 10.0 + jitter(i), beta + jitter(i + 1), 0.05, "moored"));
        t += step;
        i += 1;
    }
    // Underway again 27:00-33:00, then last seen
    let mut t = 27 * H + step;
    while t < 33 * H {
        v.push((366000001, t, 10.0, beta + (t - 27 * H) as f64 * DEG_PER_S, 12.0, "under way using engine"));
        t += step;
    }
    // Vessel 2 keeps moving throughout
    let mut t = 0;
    while t <= 1000 * 60 {
        v.push((366000002, t, 30.0, 40.0 + t as f64 * DEG_PER_S, 12.0, "under way using engine"));
        t += step;
    }
    v
}

pub const NAVS: [&str; 2] = ["moored", "under way using engine"];

pub fn raw_points(rows: &[Pt]) -> Vec<RawPoint> {
    let base = day(0).timestamp() * 1_000_000;
    rows.iter()
        .map(|r| RawPoint {
            ts_us: base + r.1 * 1_000_000,
            mmsi: r.0 as u32,
            lat_e7: Some((r.2 * 1e7).round() as i32),
            lon_e7: Some((r.3 * 1e7).round() as i32),
            sog_dk: Some((r.4 * 10.0).round() as i16),
            cog_dd: Some(900),
            heading_dd: None,
            nav: Some(NAVS.iter().position(|n| *n == r.5).unwrap() as u16),
            source: 0,
            station: None,
        })
        .collect()
}


/// Silver `positions` rows for `points`, in the silver table's column order.
pub fn silver_batch(points: &[RawPoint], dicts: &Dicts) -> RecordBatch {
    let schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(
            &collect_core::iceberg::table_schemas::positions_schema(),
        )
        .unwrap(),
    );
    let n = points.len();
    let cols: Vec<Arc<dyn Array>> = schema
        .fields()
        .iter()
        .map(|f| -> Arc<dyn Array> {
            let opt = |g: &dyn Fn(&RawPoint) -> Option<f64>| -> Arc<dyn Array> {
                Arc::new(Float64Array::from_iter(points.iter().map(g)))
            };
            match f.name().as_str() {
                "ts" => Arc::new(
                    TimestampMicrosecondArray::from_iter_values(points.iter().map(|p| p.ts_us))
                        .with_timezone("+00:00"),
                ),
                "source" => Arc::new(StringArray::from_iter_values(
                    points.iter().map(|p| dicts.sources[p.source as usize].as_str()),
                )),
                "msg_type" => Arc::new(Int32Array::from(vec![1; n])),
                "mmsi" => Arc::new(Int64Array::from_iter_values(points.iter().map(|p| p.mmsi as i64))),
                "latitude" => opt(&|p| p.lat()),
                "longitude" => opt(&|p| p.lon()),
                "sog_knots" => opt(&|p| p.sog()),
                "cog" => opt(&|p| p.cog()),
                "heading_true" => opt(&|p| p.heading()),
                "nav_status" => Arc::new(StringArray::from_iter(
                    points.iter().map(|p| p.nav.map(|i| dicts.navs[i as usize].as_str())),
                )),
                "payload" => Arc::new(StringArray::from_iter_values((0..n).map(|i| format!("p{i}")))),
                _ => new_null_array(f.data_type(), n),
            }
        })
        .collect();
    RecordBatch::try_new(schema, cols).unwrap()
}
