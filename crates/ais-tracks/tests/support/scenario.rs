//! A synthetic vessel that berths, sails and berths again across midnight, and
//! helpers to turn it into silver rows.
#![allow(dead_code)]

use std::sync::Arc;

use ais_tracks::reduce::{Dicts, RawPoint};
use arrow::array::{new_null_array, Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};

const DAY_US: i64 = 86_400_000_000;
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

/// (mmsi, seconds from the start of day `d`, imo, call sign, name, ship type, bow, stern, port, starboard, class)
pub type Static = (i64, i64, Option<i32>, Option<&'static str>, Option<&'static str>, Option<&'static str>,
              Option<i32>, Option<i32>, Option<i32>, Option<i32>, &'static str, Option<&'static str>);

pub fn statics_batch(d: i64, rows: &[Static]) -> RecordBatch {
    let schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(&collect_core::iceberg::table_schemas::statics_schema())
            .unwrap(),
    );
    let n = rows.len();
    let cols: Vec<Arc<dyn Array>> = schema
        .fields()
        .iter()
        .map(|f| -> Arc<dyn Array> {
            let s = |g: &dyn Fn(&Static) -> Option<&'static str>| -> Arc<dyn Array> {
                Arc::new(StringArray::from_iter(rows.iter().map(g)))
            };
            let i = |g: &dyn Fn(&Static) -> Option<i32>| -> Arc<dyn Array> {
                Arc::new(Int32Array::from_iter(rows.iter().map(g)))
            };
            match f.name().as_str() {
                "ts" => Arc::new(
                    TimestampMicrosecondArray::from_iter_values(
                        rows.iter().map(|r| (day(0).timestamp() * 1_000_000) + d * DAY_US + r.1 * 1_000_000),
                    )
                    .with_timezone("+00:00"),
                ),
                "source" => Arc::new(StringArray::from(vec!["a"; n])),
                "msg_type" => Arc::new(Int32Array::from(vec![5; n])),
                "mmsi" => Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                "ais_class" => s(&|r| Some(r.10)),
                "imo_number" => i(&|r| r.2),
                "call_sign" => s(&|r| r.3),
                "name" => s(&|r| r.4),
                "ship_type" => s(&|r| r.5),
                "dimension_to_bow" => i(&|r| r.6),
                "dimension_to_stern" => i(&|r| r.7),
                "dimension_to_port" => i(&|r| r.8),
                "dimension_to_starboard" => i(&|r| r.9),
                "destination" => s(&|r| r.11),
                _ => new_null_array(f.data_type(), n),
            }
        })
        .collect();
    RecordBatch::try_new(schema, cols).unwrap()
}



// ---- a fleet -------------------------------------------------------------------------------

/// Four ports 60 nautical miles apart along latitude 10 degrees.
pub const FLEET_PORT_LONS: [f64; 4] = [20.0, 21.01535, 22.0307, 23.04605];
pub const FLEET_DEST: [&str; 4] = ["ALPHA@@", "BETA@@", "GAMMA@@", "DELTA@@"];
pub const FLEET_PORTS_CSV: &str = "OID_,World Port Index Number,Region Name,Main Port Name,Alternate Port Name,UN/LOCODE,Country Code,Harbor Size,Harbor Type,Harbor Use,Channel Depth (m),Maximum Vessel Draft (m),Tidal Range (m),Latitude,Longitude
1,1.0,Test,Alpha,,AA ALP,Aland,Large,Coastal (Natural),Unknown,,,,10.0,20.0
2,2.0,Test,Beta,,BB BET,Bland,Medium,Coastal (Natural),Unknown,,,,10.0,21.01535
3,3.0,Test,Gamma,,CC GAM,Cland,Large,Coastal (Natural),Unknown,,,,10.0,22.0307
4,4.0,Test,Delta,,DD DEL,Dland,Medium,Coastal (Natural),Unknown,,,,10.0,23.04605
";

/// One stretch of a vessel's life.
#[derive(Clone, Copy, Debug)]
pub enum Seg {
    /// Lie at a port (by index) for some hours, reporting.
    Berth(usize, f64),
    /// Sail from where it is to a port at 12 knots.
    Sail(usize),
    /// Report nothing for some hours, staying put.
    Silent(f64),
    /// Sail east at 12 knots for some hours without going anywhere.
    Cruise(f64),
}

#[derive(Clone, Debug)]
pub struct FleetVessel {
    pub mmsi: i64,
    /// Seconds after the start of day 0 that its first report is made.
    pub start_s: i64,
    pub start_lon: f64,
    pub segs: Vec<Seg>,
    /// Declare this destination while sailing, whatever the real one.
    pub declares: Option<usize>,
}

pub fn fleet_vessels() -> Vec<FleetVessel> {
    use Seg::*;
    let v = |mmsi, start_s, start_lon, segs, declares| FleetVessel { mmsi, start_s, start_lon, segs, declares };
    vec![
        // A long passage through four ports, with a long stay at the second.
        v(366000001, 0, 20.0, vec![Berth(0, 5.0), Sail(1), Berth(1, 30.0), Sail(2), Berth(2, 4.0), Sail(3), Berth(3, 20.0)], None),
        // Shuttles between two ports, so several stops and legs fall in one day,
        // and declares the wrong destination.
        v(366000002, 0, 20.0, vec![
            Berth(0, 1.0), Sail(1), Berth(1, 2.0), Sail(0), Berth(0, 1.5), Sail(1), Berth(1, 2.0), Sail(0),
            Berth(0, 1.0), Sail(1), Berth(1, 2.0), Sail(0), Berth(0, 2.0),
        ], Some(0)),
        // First seen part-way along a passage.
        v(366000003, 0, 20.5, vec![Sail(1), Berth(1, 50.0), Sail(0)], None),
        // Never stops.
        v(366000004, 0, 30.0, vec![Cruise(90.0)], None),
        // First seen already berthed, on day 1.
        v(366000005, 34 * 3600, 22.0307, vec![Berth(2, 40.0), Sail(3)], None),
        // Goes quiet while berthed for 30 hours, then sails.
        v(366000006, 0, 20.0, vec![Berth(0, 3.0), Sail(1), Berth(1, 1.0), Silent(30.0), Sail(2), Berth(2, 10.0)], None),
    ]
}

/// The fleet's reports every `step` seconds over `days` days, and the
/// destinations the vessels declare (mmsi, seconds from day 0, index into
/// [`FLEET_DEST`]).
pub fn fleet(step: i64, days: i64) -> (Vec<Pt>, Vec<(i64, i64, usize)>) {
    let horizon = days * 86_400;
    let (mut pts, mut dests) = (Vec::new(), Vec::new());
    for v in fleet_vessels() {
        let (mut t, mut lon, mut i) = (v.start_s, v.start_lon, 0i64);
        let jitter = |i: i64| if i % 2 == 0 { 0.0005 } else { -0.0005 };
        for seg in &v.segs {
            match *seg {
                Seg::Berth(p, hours) => {
                    lon = FLEET_PORT_LONS[p];
                    let end = t + (hours * 3600.0) as i64;
                    while t < end && t < horizon {
                        pts.push((v.mmsi, t, 10.0 + jitter(i), lon + jitter(i + 1), 0.1, "moored"));
                        t += step;
                        i += 1;
                    }
                    t = end;
                }
                Seg::Sail(p) => {
                    let target = FLEET_PORT_LONS[p];
                    let dur = ((target - lon).abs() * 59.09 / 12.0 * 3600.0) as i64;
                    let (t0, lon0) = (t, lon);
                    while t < t0 + dur && t < horizon {
                        let frac = (t - t0) as f64 / dur as f64;
                        pts.push((v.mmsi, t, 10.0, lon0 + (target - lon0) * frac, 12.0, "under way using engine"));
                        if (t - t0) % 3600 < step {
                            dests.push((v.mmsi, t, v.declares.unwrap_or(p)));
                        }
                        t += step;
                    }
                    t = t0 + dur;
                    lon = target;
                }
                Seg::Silent(hours) => t += (hours * 3600.0) as i64,
                Seg::Cruise(hours) => {
                    let end = t + (hours * 3600.0) as i64;
                    while t < end && t < horizon {
                        lon += DEG_PER_S * step as f64;
                        pts.push((v.mmsi, t, 10.0, lon, 12.0, "under way using engine"));
                        t += step;
                    }
                    t = end;
                }
            }
        }
    }
    pts.retain(|p| p.1 < horizon);
    pts.sort_by_key(|p| (p.1, p.0));
    (pts, dests)
}
