//! The reducer: one vessel's raw reports for a day in, annotated (and
//! optionally thinned) track points out, in a single pass with bounded memory.
//!
//! It computes exactly what `track_points.rs` computes in SQL (duplicate ranks,
//! movement along the stream, gap / jump / spike / invalid flags), but as a
//! streaming state machine so a day of hundreds of millions of reports can be
//! processed one vessel at a time inside a few gigabytes. The SQL stays as the
//! oracle: with thinning off the two agree row for row (see
//! `tests/reduce_oracle.rs`).
//!
//! # Thinning
//!
//! With thinning on, only some *stream* rows are kept (positioned,
//! first-of-their-message rows). Every raw row is still accounted for: a kept
//! row carries the counts and sums of the rows it stands for.
//!
//! A stream row is kept when any of these hold (`keep_reason` bits):
//! - it is the first or last stream row of the vessel-day;
//! - the reporting gap before it exceeds the gap limit, or the row is the one
//!   just before such a gap;
//! - it is flagged (jump, spike, invalid value), or is next to a flagged row, so
//!   outliers and the hops around them stay visible;
//! - it is at least `keep_distance_nm` from the last kept row;
//! - at least `keep_interval_s` have passed since the last kept row;
//! - the course changed by `keep_turn_deg` or more (while moving);
//! - the speed changed by `keep_speed_kn` or more;
//! - the navigation status changed.
//!
//! On a kept row, `dist_nm` is the summed raw hop distance since the previous
//! kept row (so distance totals stay exact), `dt_s` is the time since the
//! previous kept row, `implied_speed_kn` is the row's own hop speed, and
//! `sum_speed` / `n_speed` let a consumer form a count-weighted mean speed over
//! the rows it stands for. `max_dev_nm` is the farthest a collapsed row was from
//! the kept one. Rows that cannot be kept (duplicates, rows without a position)
//! are counted on the next kept row.

use std::cmp::Ordering;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::record_batch::RecordBatch;
use iceberg::spec::{NestedField, PrimitiveType, Schema};

use crate::params::{
    haversine_nm, valid_position, COG_MAX_EXCLUSIVE_DEG, HEADING_MAX_VALID_DEG,
    SAME_SECOND_JUMP_NM, SOG_MAX_VALID_KN,
};

pub const KEEP_FIRST: u16 = 1;
pub const KEEP_LAST: u16 = 2;
pub const KEEP_GAP: u16 = 4;
pub const KEEP_FLAG: u16 = 8;
pub const KEEP_NEIGHBOUR: u16 = 16;
pub const KEEP_DISTANCE: u16 = 32;
pub const KEEP_INTERVAL: u16 = 64;
pub const KEEP_TURN: u16 = 128;
pub const KEEP_SPEED: u16 = 256;
pub const KEEP_NAV: u16 = 512;
pub const KEEP_BEFORE_GAP: u16 = 1024;

/// One raw position report, quantised the way the router stores it: positions
/// in 1e-7 degrees, speed / course / heading in tenths.
#[derive(Debug, Clone, PartialEq)]
pub struct RawPoint {
    pub ts_us: i64,
    pub mmsi: u32,
    pub lat_e7: Option<i32>,
    pub lon_e7: Option<i32>,
    pub sog_dk: Option<i16>,
    pub cog_dd: Option<i16>,
    pub heading_dd: Option<i16>,
    /// Index into [`Dicts::navs`].
    pub nav: Option<u16>,
    /// Index into [`Dicts::sources`].
    pub source: u16,
    /// Index into [`Dicts::stations`].
    pub station: Option<u16>,
}

impl RawPoint {
    pub fn lat(&self) -> Option<f64> {
        self.lat_e7.map(|v| v as f64 / 1e7)
    }
    pub fn lon(&self) -> Option<f64> {
        self.lon_e7.map(|v| v as f64 / 1e7)
    }
    pub fn sog(&self) -> Option<f64> {
        self.sog_dk.map(|v| v as f64 / 10.0)
    }
    pub fn cog(&self) -> Option<f64> {
        self.cog_dd.map(|v| v as f64 / 10.0)
    }
    pub fn heading(&self) -> Option<f64> {
        self.heading_dd.map(|v| v as f64 / 10.0)
    }
}

/// String tables for the ids in [`RawPoint`], with a sort rank per id so
/// tie-breaking between otherwise identical rows does not depend on the order
/// the ids were assigned in.
#[derive(Debug, Clone, Default)]
pub struct Dicts {
    pub sources: Vec<String>,
    pub stations: Vec<String>,
    pub navs: Vec<String>,
    source_rank: Vec<u32>,
    station_rank: Vec<u32>,
}

fn ranks(names: &[String]) -> Vec<u32> {
    let mut idx: Vec<usize> = (0..names.len()).collect();
    idx.sort_by(|&a, &b| names[a].cmp(&names[b]));
    let mut r = vec![0; names.len()];
    for (rank, i) in idx.into_iter().enumerate() {
        r[i] = rank as u32;
    }
    r
}

impl Dicts {
    pub fn new(sources: Vec<String>, stations: Vec<String>, navs: Vec<String>) -> Self {
        let source_rank = ranks(&sources);
        let station_rank = ranks(&stations);
        Self {
            sources,
            stations,
            navs,
            source_rank,
            station_rank,
        }
    }
}

/// The definitions that decide a flag, shared with the SQL through
/// [`crate::params`].
#[derive(Debug, Clone, Copy)]
pub struct Rules {
    /// An implied speed above this (knots) is a jump.
    pub max_speed_kn: f64,
    /// A gap longer than this many seconds starts a new segment.
    pub gap_s: f64,
}

impl Default for Rules {
    fn default() -> Self {
        Self {
            max_speed_kn: 60.0,
            gap_s: 30.0 * 60.0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ThinOpts {
    /// Keep every row (the reducer then equals `track_points.rs`).
    pub off: bool,
    pub keep_distance_nm: f64,
    pub keep_interval_s: f64,
    pub keep_turn_deg: f64,
    pub keep_speed_kn: f64,
}

impl ThinOpts {
    pub fn off() -> Self {
        Self {
            off: true,
            ..Self::default()
        }
    }
}

impl Default for ThinOpts {
    fn default() -> Self {
        Self {
            off: false,
            keep_distance_nm: 0.1,
            keep_interval_s: 120.0,
            keep_turn_deg: 15.0,
            keep_speed_kn: 2.0,
        }
    }
}

/// Where a vessel's stream left off (the previous day's last stream row).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StreamState {
    pub ts_us: i64,
    pub lat: f64,
    pub lon: f64,
}

/// One output row: the raw row plus its annotations and, when thinned, what it
/// stands for.
#[derive(Debug, Clone, PartialEq)]
pub struct OutRow {
    pub ts_us: i64,
    pub mmsi: u32,
    pub source: u16,
    pub station: Option<u16>,
    pub lat_e7: Option<i32>,
    pub lon_e7: Option<i32>,
    pub sog_dk: Option<i16>,
    pub cog_dd: Option<i16>,
    pub heading_dd: Option<i16>,
    pub nav: Option<u16>,
    pub has_position: bool,
    pub dup_rank: i32,
    pub n_dups: i32,
    pub prev_ts_us: Option<i64>,
    pub dt_s: Option<f64>,
    pub dist_nm: Option<f64>,
    pub implied_speed_kn: Option<f64>,
    pub gap_before: bool,
    pub is_speed_jump: bool,
    pub is_spike: bool,
    pub is_sog_invalid: bool,
    pub is_cog_invalid: bool,
    pub is_heading_invalid: bool,
    pub is_outlier: bool,
    /// Raw rows this row stands for, itself included.
    pub n_raw: i32,
    /// Of those, exact duplicates of another row.
    pub n_collapsed_dups: i32,
    /// Of those, rows without a usable position.
    pub n_no_position: i32,
    /// Of those, rows carrying an invalid value or a flag.
    pub n_outliers_raw: i32,
    pub sum_speed: f64,
    pub n_speed: i32,
    pub max_dev_nm: f64,
    pub max_hop_speed_kn: Option<f64>,
    pub keep_reason: u16,
    /// Sum and count of valid reported speeds over the stream rows this row
    /// stands for, and the largest of them.
    pub sum_sog: f64,
    pub n_sog: i32,
    pub max_sog: Option<f64>,
}

#[derive(Debug, Default)]
pub struct Reduced {
    pub rows: Vec<OutRow>,
    /// The vessel's last stream row, to carry into the next day.
    pub state: Option<StreamState>,
    /// Raw rows that no kept row could absorb (a vessel-day with no stream row
    /// at all). Kept so `sum(n_raw) + unaccounted_raw == input rows`.
    pub unaccounted_raw: usize,
    /// When the vessel was first and last heard that day and how many reports
    /// it sent, every one counted (duplicates and rows without a position too).
    pub summary: Option<crate::vessels::VesselDay>,
}

fn key32(v: Option<i32>) -> i64 {
    v.map(|x| x as i64).unwrap_or(i64::MAX)
}

fn key16(v: Option<i16>) -> i64 {
    v.map(|x| x as i64).unwrap_or(i64::MAX)
}

fn keyu(v: Option<u16>) -> i64 {
    v.map(|x| x as i64).unwrap_or(i64::MAX)
}

fn cmp_points(a: &RawPoint, b: &RawPoint, d: &Dicts) -> Ordering {
    a.ts_us
        .cmp(&b.ts_us)
        .then(key32(a.lat_e7).cmp(&key32(b.lat_e7)))
        .then(key32(a.lon_e7).cmp(&key32(b.lon_e7)))
        .then(key16(a.sog_dk).cmp(&key16(b.sog_dk)))
        .then(key16(a.cog_dd).cmp(&key16(b.cog_dd)))
        .then(key16(a.heading_dd).cmp(&key16(b.heading_dd)))
        .then(keyu(a.nav).cmp(&keyu(b.nav)))
        .then(d.source_rank[a.source as usize].cmp(&d.source_rank[b.source as usize]))
        .then({
            let r = |s: Option<u16>| {
                s.map(|i| d.station_rank[i as usize] as i64)
                    .unwrap_or(i64::MAX)
            };
            r(a.station).cmp(&r(b.station))
        })
}

/// Two rows are the same message when everything the vessel transmitted matches.
fn same_content(a: &RawPoint, b: &RawPoint) -> bool {
    a.ts_us == b.ts_us
        && a.lat_e7 == b.lat_e7
        && a.lon_e7 == b.lon_e7
        && a.sog_dk == b.sog_dk
        && a.cog_dd == b.cog_dd
        && a.heading_dd == b.heading_dd
        && a.nav == b.nav
}

fn sog_invalid(p: &RawPoint) -> bool {
    p.sog().is_some_and(|v| !(0.0..=SOG_MAX_VALID_KN).contains(&v))
}

fn cog_invalid(p: &RawPoint) -> bool {
    p.cog()
        .is_some_and(|v| !(0.0..COG_MAX_EXCLUSIVE_DEG).contains(&v))
}

fn heading_invalid(p: &RawPoint) -> bool {
    p.heading()
        .is_some_and(|v| !(0.0..=HEADING_MAX_VALID_DEG).contains(&v))
}

/// A stream row's hop from the previous stream row.
#[derive(Debug, Clone, Copy, Default)]
struct Hop {
    prev_ts_us: Option<i64>,
    dt_s: Option<f64>,
    dist_nm: Option<f64>,
    implied_speed_kn: Option<f64>,
    over_limit: bool,
    gap: bool,
}

/// Reduces one vessel's raw rows for a day. `prev` is where its stream left off
/// the day before, if known.
pub fn reduce_vessel(
    mut pts: Vec<RawPoint>,
    dicts: &Dicts,
    prev: Option<StreamState>,
    rules: &Rules,
    thin: &ThinOpts,
) -> Reduced {
    let n = pts.len();
    if n == 0 {
        return Reduced {
            state: prev,
            ..Default::default()
        };
    }
    pts.sort_by(|a, b| cmp_points(a, b, dicts));
    let summary = Some(crate::vessels::VesselDay {
        mmsi: pts[0].mmsi,
        first_ts_us: pts[0].ts_us,
        last_ts_us: pts[n - 1].ts_us,
        n_reports: n as i64,
    });

    // Duplicate groups: consecutive rows with identical content.
    let mut dup_rank = vec![1i32; n];
    let mut n_dups = vec![1i32; n];
    let mut i = 0;
    while i < n {
        let mut j = i + 1;
        while j < n && same_content(&pts[i], &pts[j]) {
            j += 1;
        }
        for k in i..j {
            dup_rank[k] = (k - i + 1) as i32;
            n_dups[k] = (j - i) as i32;
        }
        i = j;
    }

    let has_pos: Vec<bool> = pts
        .iter()
        .map(|q| match (q.lat(), q.lon()) {
            (Some(a), Some(b)) => valid_position(a, b),
            _ => false,
        })
        .collect();
    let stream: Vec<usize> = (0..n).filter(|&k| has_pos[k] && dup_rank[k] == 1).collect();

    // Movement along the stream.
    let mut hops: Vec<Hop> = Vec::with_capacity(stream.len());
    let mut pv = prev;
    for &k in &stream {
        let q = &pts[k];
        let (lat, lon) = (q.lat().unwrap(), q.lon().unwrap());
        let hop = match pv {
            Some(s) => {
                let dt = (q.ts_us - s.ts_us) as f64 / 1_000_000.0;
                let dist = haversine_nm(s.lat, s.lon, lat, lon);
                let over = if dt > 0.0 {
                    dist / (dt / 3600.0) > rules.max_speed_kn
                } else {
                    dist > SAME_SECOND_JUMP_NM
                };
                Hop {
                    prev_ts_us: Some(s.ts_us),
                    dt_s: Some(dt),
                    dist_nm: Some(dist),
                    implied_speed_kn: (dt > 0.0).then(|| dist / (dt / 3600.0)),
                    over_limit: over,
                    gap: dt > rules.gap_s,
                }
            }
            None => Hop {
                gap: true,
                ..Default::default()
            },
        };
        pv = Some(StreamState {
            ts_us: q.ts_us,
            lat,
            lon,
        });
        hops.push(hop);
    }
    let spike: Vec<bool> = (0..stream.len())
        .map(|j| hops[j].over_limit && hops.get(j + 1).is_some_and(|h| h.over_limit))
        .collect();

    // Position of each row in the stream, if it is on it.
    let mut stream_pos: Vec<Option<usize>> = vec![None; n];
    for (j, &k) in stream.iter().enumerate() {
        stream_pos[k] = Some(j);
    }

    let out_row = |k: usize| -> OutRow {
        let q = &pts[k];
        let j = stream_pos[k];
        let (hop, is_spike) = match j {
            Some(j) => (hops[j], spike[j]),
            None => (Hop::default(), false),
        };
        let (si, ci, hi) = (sog_invalid(q), cog_invalid(q), heading_invalid(q));
        OutRow {
            ts_us: q.ts_us,
            mmsi: q.mmsi,
            source: q.source,
            station: q.station,
            lat_e7: q.lat_e7,
            lon_e7: q.lon_e7,
            sog_dk: q.sog_dk,
            cog_dd: q.cog_dd,
            heading_dd: q.heading_dd,
            nav: q.nav,
            has_position: has_pos[k],
            dup_rank: dup_rank[k],
            n_dups: n_dups[k],
            prev_ts_us: hop.prev_ts_us,
            dt_s: hop.dt_s,
            dist_nm: hop.dist_nm,
            implied_speed_kn: hop.implied_speed_kn,
            gap_before: j.is_some() && hop.gap,
            is_speed_jump: j.is_some() && hop.over_limit,
            is_spike,
            is_sog_invalid: si,
            is_cog_invalid: ci,
            is_heading_invalid: hi,
            is_outlier: is_spike || si || ci || hi,
            n_raw: 1,
            n_collapsed_dups: 0,
            n_no_position: 0,
            n_outliers_raw: 0,
            sum_speed: 0.0,
            n_speed: 0,
            max_dev_nm: 0.0,
            max_hop_speed_kn: hop.implied_speed_kn,
            keep_reason: 0,
            sum_sog: 0.0,
            n_sog: 0,
            max_sog: None,
        }
    };

    // A valid reported speed, as `tracks` averages it.
    let valid_sog = |k: usize, r: &OutRow| -> Option<f64> {
        if r.is_sog_invalid {
            None
        } else {
            pts[k].sog()
        }
    };

    // Speed a row contributes to a smoothed mean: reported speed when valid,
    // else the hop's implied speed (mirrors the SQL in `stops.rs`).
    let speed_of = |k: usize, r: &OutRow| -> Option<f64> {
        if r.is_sog_invalid {
            r.implied_speed_kn
        } else {
            pts[k].sog().or(r.implied_speed_kn)
        }
    };

    let state = pv;

    if thin.off {
        let rows: Vec<OutRow> = (0..n)
            .map(|k| {
                let mut r = out_row(k);
                if let Some(s) = speed_of(k, &r) {
                    r.sum_speed = s;
                    r.n_speed = 1;
                }
                r.n_collapsed_dups = i32::from(dup_rank[k] > 1);
                r.n_no_position = i32::from(!has_pos[k]);
                r.n_outliers_raw = i32::from(r.is_outlier);
                if stream_pos[k].is_some() {
                    if let Some(v) = valid_sog(k, &r) {
                        r.sum_sog = v;
                        r.n_sog = 1;
                        r.max_sog = Some(v);
                    }
                }
                r.keep_reason = KEEP_FIRST | KEEP_LAST;
                r
            })
            .collect();
        return Reduced {
            rows,
            state,
            unaccounted_raw: 0,
            summary,
        };
    }

    // ---- thinning ----------------------------------------------------------
    let s_len = stream.len();
    let flagged: Vec<bool> = (0..s_len)
        .map(|j| {
            let q = &pts[stream[j]];
            hops[j].over_limit
                || spike[j]
                || sog_invalid(q)
                || cog_invalid(q)
                || heading_invalid(q)
        })
        .collect();
    let mut static_reason = vec![0u16; s_len];
    for j in 0..s_len {
        let r = &mut static_reason[j];
        if j == 0 {
            *r |= KEEP_FIRST;
        }
        if j + 1 == s_len {
            *r |= KEEP_LAST;
        }
        if hops[j].gap {
            *r |= KEEP_GAP;
        }
        if flagged[j] {
            *r |= KEEP_FLAG;
        }
        if (j > 0 && flagged[j - 1]) || (j + 1 < s_len && flagged[j + 1]) {
            *r |= KEEP_NEIGHBOUR;
        }
        if j + 1 < s_len && hops[j + 1].gap {
            *r |= KEEP_BEFORE_GAP;
        }
    }

    #[derive(Default)]
    struct Pending {
        n_raw: i32,
        dups: i32,
        no_pos: i32,
        outliers: i32,
        sum_speed: f64,
        n_speed: i32,
        dist: f64,
        has_dist: bool,
        max_hop: Option<f64>,
        collapsed: Vec<(f64, f64)>,
        sum_sog: f64,
        n_sog: i32,
        max_sog: Option<f64>,
    }
    struct LastKept {
        ts_us: i64,
        lat: f64,
        lon: f64,
        sog: Option<f64>,
        cog: Option<f64>,
        nav: Option<u16>,
    }

    let mut rows: Vec<OutRow> = Vec::new();
    let mut pending = Pending::default();
    let mut last: Option<LastKept> = None;

    for k in 0..n {
        let Some(j) = stream_pos[k] else {
            pending.n_raw += 1;
            if dup_rank[k] > 1 {
                pending.dups += 1;
            } else {
                pending.no_pos += 1;
            }
            let q = &pts[k];
            if sog_invalid(q) || cog_invalid(q) || heading_invalid(q) {
                pending.outliers += 1;
            }
            continue;
        };
        let mut row = out_row(k);
        let q = &pts[k];
        let (lat, lon) = (q.lat().unwrap(), q.lon().unwrap());
        pending.n_raw += 1;
        if let Some(s) = speed_of(k, &row) {
            pending.sum_speed += s;
            pending.n_speed += 1;
        }
        if let Some(d) = hops[j].dist_nm {
            pending.dist += d;
            pending.has_dist = true;
        }
        if let Some(v) = hops[j].implied_speed_kn {
            pending.max_hop = Some(pending.max_hop.map_or(v, |m| m.max(v)));
        }
        if row.is_outlier {
            pending.outliers += 1;
        }
        if let Some(v) = valid_sog(k, &row) {
            pending.sum_sog += v;
            pending.n_sog += 1;
            pending.max_sog = Some(pending.max_sog.map_or(v, |m| m.max(v)));
        }

        let mut reason = static_reason[j];
        if let Some(l) = &last {
            if haversine_nm(l.lat, l.lon, lat, lon) >= thin.keep_distance_nm {
                reason |= KEEP_DISTANCE;
            }
            if (q.ts_us - l.ts_us) as f64 / 1_000_000.0 >= thin.keep_interval_s {
                reason |= KEEP_INTERVAL;
            }
            if let (Some(a), Some(b), Some(sog)) = (l.cog, q.cog(), q.sog()) {
                if !cog_invalid(q) && sog >= 1.0 {
                    let d = (a - b).abs() % 360.0;
                    if d.min(360.0 - d) >= thin.keep_turn_deg {
                        reason |= KEEP_TURN;
                    }
                }
            }
            if let (Some(a), Some(b)) = (l.sog, q.sog()) {
                if !sog_invalid(q) && (a - b).abs() >= thin.keep_speed_kn {
                    reason |= KEEP_SPEED;
                }
            }
            if l.nav != q.nav {
                reason |= KEEP_NAV;
            }
        }

        if reason == 0 {
            pending.collapsed.push((lat, lon));
            continue;
        }

        // Keep this row and let it stand for everything pending.
        row.dt_s = last
            .as_ref()
            .map(|l| (q.ts_us - l.ts_us) as f64 / 1_000_000.0)
            .or(hops[j].dt_s);
        row.prev_ts_us = last.as_ref().map(|l| l.ts_us).or(hops[j].prev_ts_us);
        row.dist_nm = pending.has_dist.then_some(pending.dist);
        row.n_raw = pending.n_raw;
        row.n_collapsed_dups = pending.dups;
        row.n_no_position = pending.no_pos;
        row.n_outliers_raw = pending.outliers;
        row.sum_speed = pending.sum_speed;
        row.n_speed = pending.n_speed;
        row.max_hop_speed_kn = pending.max_hop;
        row.max_dev_nm = pending
            .collapsed
            .iter()
            .map(|&(a, b)| haversine_nm(lat, lon, a, b))
            .fold(0.0, f64::max);
        row.keep_reason = reason;
        row.sum_sog = pending.sum_sog;
        row.n_sog = pending.n_sog;
        row.max_sog = pending.max_sog;
        last = Some(LastKept {
            ts_us: q.ts_us,
            lat,
            lon,
            sog: q.sog(),
            cog: q.cog(),
            nav: q.nav,
        });
        rows.push(row);
        pending = Pending::default();
    }

    // Rows after the last kept row (only rows without a usable position, since
    // the last stream row is always kept) are absorbed by it.
    let mut unaccounted_raw = 0;
    if pending.n_raw > 0 {
        match rows.last_mut() {
            Some(r) => {
                r.n_raw += pending.n_raw;
                r.n_collapsed_dups += pending.dups;
                r.n_no_position += pending.no_pos;
                r.n_outliers_raw += pending.outliers;
            }
            None => unaccounted_raw = pending.n_raw as usize,
        }
    }
    Reduced {
        rows,
        state,
        unaccounted_raw,
        summary,
    }
}

// ---- output table ------------------------------------------------------------

fn required(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::required(id, name, ty.into()))
}

fn optional(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::optional(id, name, ty.into()))
}

/// Column order here is the column order of [`to_batch`]. `ts` must stay
/// column 0: the table is partitioned by day on it.
pub fn thin_points_schema() -> Schema {
    use PrimitiveType::*;
    let fields = vec![
        required(1, "ts", Timestamptz),
        required(2, "mmsi", Long),
        required(3, "source", String),
        optional(4, "station", String),
        optional(5, "latitude", Double),
        optional(6, "longitude", Double),
        optional(7, "sog_knots", Double),
        optional(8, "cog", Double),
        optional(9, "heading_true", Double),
        optional(10, "nav_status", String),
        required(11, "has_position", Boolean),
        required(12, "dup_rank", Int),
        required(13, "n_dups", Int),
        required(14, "is_duplicate", Boolean),
        optional(15, "prev_ts", Timestamptz),
        optional(16, "dt_s", Double),
        optional(17, "dist_nm", Double),
        optional(18, "implied_speed_kn", Double),
        required(19, "gap_before", Boolean),
        required(20, "is_speed_jump", Boolean),
        required(21, "is_spike", Boolean),
        required(22, "is_sog_invalid", Boolean),
        required(23, "is_cog_invalid", Boolean),
        required(24, "is_heading_invalid", Boolean),
        required(25, "is_outlier", Boolean),
        required(26, "n_raw", Int),
        required(27, "n_collapsed_dups", Int),
        required(28, "n_no_position", Int),
        required(29, "n_outliers_raw", Int),
        required(30, "sum_speed", Double),
        required(31, "n_speed", Int),
        required(32, "max_dev_nm", Double),
        optional(33, "max_hop_speed_kn", Double),
        required(34, "keep_reason", Int),
        required(35, "sum_sog", Double),
        required(36, "n_sog", Int),
        optional(37, "max_sog", Double),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building thin_points schema")
}

/// Converts rows to a batch in [`thin_points_schema`]'s column order and types.
pub fn to_batch(rows: &[OutRow], dicts: &Dicts) -> Result<RecordBatch> {
    let ts = |v: Vec<Option<i64>>| -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(v).with_timezone("+00:00"))
    };
    let f64s = |f: &dyn Fn(&OutRow) -> Option<f64>| -> ArrayRef {
        Arc::new(Float64Array::from_iter(rows.iter().map(f)))
    };
    let bools = |f: &dyn Fn(&OutRow) -> bool| -> ArrayRef {
        Arc::new(BooleanArray::from_iter(rows.iter().map(|r| Some(f(r)))))
    };
    let i32s = |f: &dyn Fn(&OutRow) -> i32| -> ArrayRef {
        Arc::new(Int32Array::from_iter_values(rows.iter().map(f)))
    };
    let cols: Vec<ArrayRef> = vec![
        ts(rows.iter().map(|r| Some(r.ts_us)).collect()),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.mmsi as i64))),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| dicts.sources[r.source as usize].as_str()),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter()
                .map(|r| r.station.map(|i| dicts.stations[i as usize].as_str())),
        )),
        f64s(&|r| r.lat_e7.map(|v| v as f64 / 1e7)),
        f64s(&|r| r.lon_e7.map(|v| v as f64 / 1e7)),
        f64s(&|r| r.sog_dk.map(|v| v as f64 / 10.0)),
        f64s(&|r| r.cog_dd.map(|v| v as f64 / 10.0)),
        f64s(&|r| r.heading_dd.map(|v| v as f64 / 10.0)),
        Arc::new(StringArray::from_iter(
            rows.iter()
                .map(|r| r.nav.map(|i| dicts.navs[i as usize].as_str())),
        )),
        bools(&|r| r.has_position),
        i32s(&|r| r.dup_rank),
        i32s(&|r| r.n_dups),
        bools(&|r| r.dup_rank > 1),
        ts(rows.iter().map(|r| r.prev_ts_us).collect()),
        f64s(&|r| r.dt_s),
        f64s(&|r| r.dist_nm),
        f64s(&|r| r.implied_speed_kn),
        bools(&|r| r.gap_before),
        bools(&|r| r.is_speed_jump),
        bools(&|r| r.is_spike),
        bools(&|r| r.is_sog_invalid),
        bools(&|r| r.is_cog_invalid),
        bools(&|r| r.is_heading_invalid),
        bools(&|r| r.is_outlier),
        i32s(&|r| r.n_raw),
        i32s(&|r| r.n_collapsed_dups),
        i32s(&|r| r.n_no_position),
        i32s(&|r| r.n_outliers_raw),
        f64s(&|r| Some(r.sum_speed)),
        i32s(&|r| r.n_speed),
        f64s(&|r| Some(r.max_dev_nm)),
        f64s(&|r| r.max_hop_speed_kn),
        i32s(&|r| r.keep_reason as i32),
        f64s(&|r| Some(r.sum_sog)),
        i32s(&|r| r.n_sog),
        f64s(&|r| r.max_sog),
    ];
    let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&thin_points_schema())?);
    Ok(RecordBatch::try_new(schema, cols)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dicts() -> Dicts {
        Dicts::new(
            vec!["a".into(), "b".into()],
            vec!["s1".into()],
            vec!["under way".into(), "moored".into()],
        )
    }

    fn pt(sec: i64, lat: f64, lon: f64, sog: f64, source: u16) -> RawPoint {
        RawPoint {
            ts_us: (1_700_000_000 + sec) * 1_000_000,
            mmsi: 1,
            lat_e7: Some((lat * 1e7).round() as i32),
            lon_e7: Some((lon * 1e7).round() as i32),
            sog_dk: Some((sog * 10.0).round() as i16),
            cog_dd: Some(900),
            heading_dd: Some(900),
            nav: Some(0),
            source,
            station: None,
        }
    }

    /// A vessel sailing east at 12 kn, reporting every 10 s.
    fn sailing(n: i64) -> Vec<RawPoint> {
        let step = 12.0 / 3600.0 * 10.0 / 59.09; // degrees of longitude per 10 s
        (0..n).map(|i| pt(i * 10, 10.0, 20.0 + i as f64 * step, 12.0, 0)).collect()
    }

    fn total_raw(r: &Reduced) -> usize {
        r.rows.iter().map(|x| x.n_raw as usize).sum::<usize>() + r.unaccounted_raw
    }

    #[test]
    fn thinning_off_keeps_every_row_including_duplicates() {
        let mut v = sailing(5);
        v.push(v[2].clone()); // same message, second receiver
        v.last_mut().unwrap().source = 1;
        let r = reduce_vessel(v, &dicts(), None, &Rules::default(), &ThinOpts::off());
        assert_eq!(r.rows.len(), 6);
        assert_eq!(r.rows.iter().filter(|x| x.dup_rank > 1).count(), 1);
    }

    #[test]
    fn steady_sailing_is_thinned_but_every_row_is_accounted_for() {
        let v = sailing(300); // 50 minutes
        let n = v.len();
        let r = reduce_vessel(v, &dicts(), None, &Rules::default(), &ThinOpts::default());
        // At 12 kn a report every 10 s moves 0.033 nm, so the 0.1 nm distance
        // rule keeps about one row in three.
        assert!(r.rows.len() < n / 2, "kept {} of {n}", r.rows.len());
        assert_eq!(total_raw(&r), n);
        assert!(r.rows.first().unwrap().keep_reason & KEEP_FIRST != 0);
        assert!(r.rows.last().unwrap().keep_reason & KEEP_LAST != 0);
        // No two kept rows are further apart than the interval, plus one report.
        for w in r.rows.windows(2) {
            assert!((w[1].ts_us - w[0].ts_us) as f64 / 1e6 <= 130.0);
        }
    }

    #[test]
    fn thinning_conserves_total_distance() {
        let v = sailing(300);
        let off = reduce_vessel(v.clone(), &dicts(), None, &Rules::default(), &ThinOpts::off());
        let on = reduce_vessel(v, &dicts(), None, &Rules::default(), &ThinOpts::default());
        let sum = |r: &Reduced| r.rows.iter().filter_map(|x| x.dist_nm).sum::<f64>();
        assert!((sum(&off) - sum(&on)).abs() < 1e-9, "{} vs {}", sum(&off), sum(&on));
    }

    #[test]
    fn a_bad_fix_and_its_neighbours_are_kept() {
        let mut v = sailing(60);
        v[30].lat_e7 = Some(200_000_000); // 10 degrees north of the track: ~600 nm in 10 s
        let r = reduce_vessel(v, &dicts(), None, &Rules::default(), &ThinOpts::default());
        let spike = r.rows.iter().find(|x| x.is_spike).expect("spike kept");
        assert!(spike.keep_reason & KEEP_FLAG != 0);
        let pos = r.rows.iter().position(|x| x.is_spike).unwrap();
        assert!(pos > 0 && pos + 1 < r.rows.len());
        assert!(r.rows[pos - 1].ts_us < spike.ts_us && r.rows[pos + 1].ts_us > spike.ts_us);
        // Both neighbours are exactly one report away.
        assert_eq!(spike.ts_us - r.rows[pos - 1].ts_us, 10_000_000);
        assert_eq!(r.rows[pos + 1].ts_us - spike.ts_us, 10_000_000);
    }

    #[test]
    fn a_gap_keeps_the_row_before_and_after_it() {
        let mut v = sailing(20);
        for (i, p) in v.iter_mut().enumerate().skip(10) {
            p.ts_us += 3 * 3600 * 1_000_000 + i as i64; // silent for 3 hours
        }
        let r = reduce_vessel(v, &dicts(), None, &Rules::default(), &ThinOpts::default());
        let g = r.rows.iter().position(|x| x.gap_before && x.prev_ts_us.is_some()).unwrap();
        assert!(r.rows[g - 1].keep_reason & KEEP_BEFORE_GAP != 0);
        // The gap hop stands alone on the row after it.
        assert!(r.rows[g].dist_nm.unwrap() > 0.0);
    }

    #[test]
    fn rows_without_a_position_are_counted_not_lost() {
        let mut v = sailing(30);
        v[5].lat_e7 = None;
        v[5].lon_e7 = None;
        v[29].lat_e7 = None;
        v[29].lon_e7 = None; // trailing row without a position
        let n = v.len();
        let r = reduce_vessel(v, &dicts(), None, &Rules::default(), &ThinOpts::default());
        assert_eq!(total_raw(&r), n);
        assert_eq!(r.rows.iter().map(|x| x.n_no_position).sum::<i32>(), 2);
    }

    #[test]
    fn a_vessel_with_no_positioned_row_is_reported_as_unaccounted() {
        let mut v = sailing(3);
        for p in &mut v {
            p.lat_e7 = None;
            p.lon_e7 = None;
        }
        let r = reduce_vessel(v, &dicts(), None, &Rules::default(), &ThinOpts::default());
        assert!(r.rows.is_empty());
        assert_eq!(r.unaccounted_raw, 3);
    }

    #[test]
    fn output_is_independent_of_input_order() {
        let mut a = sailing(50);
        a.push(a[10].clone());
        a.last_mut().unwrap().source = 1;
        let mut b = a.clone();
        b.reverse();
        let ra = reduce_vessel(a, &dicts(), None, &Rules::default(), &ThinOpts::default());
        let rb = reduce_vessel(b, &dicts(), None, &Rules::default(), &ThinOpts::default());
        assert_eq!(ra.rows, rb.rows);
    }

    #[test]
    fn thinning_conserves_counts_and_sums() {
        let mut v = sailing(400);
        v[50].lat_e7 = Some(200_000_000); // spike
        v[80].sog_dk = Some(1023); // invalid speed
        v[120].cog_dd = Some(3600); // invalid course
        v[130].lat_e7 = None;
        v[130].lon_e7 = None;
        v.push(v[200].clone()); // duplicate
        v.last_mut().unwrap().source = 1;
        let off = reduce_vessel(v.clone(), &dicts(), None, &Rules::default(), &ThinOpts::off());
        let on = reduce_vessel(v, &dicts(), None, &Rules::default(), &ThinOpts::default());
        assert!(on.rows.len() < off.rows.len() / 2);
        macro_rules! total {
            ($r:expr, $f:ident) => {
                $r.rows.iter().map(|x| x.$f as f64).sum::<f64>()
            };
        }
        for (name, a, b) in [
            ("n_raw", total!(off, n_raw), total!(on, n_raw)),
            ("dups", total!(off, n_collapsed_dups), total!(on, n_collapsed_dups)),
            ("no position", total!(off, n_no_position), total!(on, n_no_position)),
            ("outliers", total!(off, n_outliers_raw), total!(on, n_outliers_raw)),
            ("n_sog", total!(off, n_sog), total!(on, n_sog)),
            ("sum_sog", total!(off, sum_sog), total!(on, sum_sog)),
        ] {
            assert!((a - b).abs() < 1e-6, "{name}: {a} vs {b}");
        }
        let max = |r: &Reduced| r.rows.iter().filter_map(|x| x.max_sog).fold(0.0, f64::max);
        assert_eq!(max(&off), max(&on));
        // Jumps and spikes are never collapsed, so their counts are exact.
        let flag = |r: &Reduced, f: fn(&OutRow) -> bool| r.rows.iter().filter(|x| f(x)).count();
        assert_eq!(flag(&off, |x| x.is_spike), flag(&on, |x| x.is_spike));
        assert_eq!(flag(&off, |x| x.is_speed_jump), flag(&on, |x| x.is_speed_jump));
    }

    #[test]
    fn batches_match_the_iceberg_schema() {
        let r = reduce_vessel(sailing(40), &dicts(), None, &Rules::default(), &ThinOpts::default());
        let b = to_batch(&r.rows, &dicts()).unwrap();
        crate::carry::check_batches(&thin_points_schema(), &[b]).unwrap();
    }
}
