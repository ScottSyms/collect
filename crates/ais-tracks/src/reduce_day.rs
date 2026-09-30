//! Reducing one day: route the raw reports to buckets, then reduce the buckets
//! one at a time. Peak memory is one bucket (a few million reports) plus a
//! small output chunk, however large the day.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures_util::StreamExt;

use crate::reduce::{
    reduce_vessel, to_batch, OutRow, RawPoint, Rules, StreamState, ThinOpts, KEEP_BEFORE_GAP,
    KEEP_DISTANCE, KEEP_FIRST, KEEP_FLAG, KEEP_GAP, KEEP_INTERVAL, KEEP_LAST, KEEP_NAV,
    KEEP_NEIGHBOUR, KEEP_SPEED, KEEP_TURN,
};
use crate::router::{read_bucket_into, Manifest, Router};
use crate::source::BatchStream;
use crate::vessels::VesselDay;

/// Rows converted to Arrow and handed to the sink at a time.
const OUT_CHUNK_ROWS: usize = 65_536;

pub const KEEP_REASONS: [(&str, u16); 11] = [
    ("first", KEEP_FIRST),
    ("last", KEEP_LAST),
    ("gap", KEEP_GAP),
    ("before_gap", KEEP_BEFORE_GAP),
    ("flagged", KEEP_FLAG),
    ("next_to_flagged", KEEP_NEIGHBOUR),
    ("distance", KEEP_DISTANCE),
    ("interval", KEEP_INTERVAL),
    ("turn", KEEP_TURN),
    ("speed_change", KEEP_SPEED),
    ("nav_change", KEEP_NAV),
];

#[derive(Debug, Clone)]
pub struct ReduceOptions {
    pub rules: Rules,
    pub thin: ThinOpts,
    /// Bucket count; `None` picks one from the row estimate.
    pub buckets: Option<usize>,
    /// Aim for about this many reports per bucket when choosing the count.
    pub target_bucket_rows: u64,
    pub scratch: PathBuf,
}

impl Default for ReduceOptions {
    fn default() -> Self {
        Self {
            rules: Rules::default(),
            thin: ThinOpts::default(),
            buckets: None,
            target_bucket_rows: 3_000_000,
            scratch: std::env::temp_dir().join("ais-tracks"),
        }
    }
}

pub fn choose_buckets(est_rows: u64, target: u64) -> usize {
    (est_rows.div_ceil(target.max(1)) as usize).clamp(1, 1024)
}

#[derive(Debug, Default)]
pub struct ReduceStats {
    pub buckets: usize,
    /// Reports routed (on the day, plausible MMSI).
    pub routed: u64,
    pub quarantined: u64,
    pub outside_day: u64,
    pub vessels: u64,
    pub kept: u64,
    /// Sum of `n_raw` over kept rows; with `unaccounted` it equals `routed`.
    pub represented: u64,
    pub unaccounted: u64,
    pub collapsed_dups: u64,
    pub no_position: u64,
    pub outliers_raw: u64,
    pub reasons: [u64; 11],
    pub largest_bucket: u64,
    pub scratch_bytes: u64,
    pub route_time: Duration,
    pub reduce_time: Duration,
}

impl ReduceStats {
    pub fn retention(&self) -> f64 {
        if self.routed == 0 {
            0.0
        } else {
            self.kept as f64 / self.routed as f64
        }
    }
}

/// Streams `stream` into buckets under `opts.scratch/<label>`.
pub async fn route_day(
    mut stream: BatchStream,
    est_rows: u64,
    day_start_us: i64,
    day_end_us: i64,
    label: &str,
    opts: &ReduceOptions,
) -> Result<Manifest> {
    let buckets = opts
        .buckets
        .unwrap_or_else(|| choose_buckets(est_rows, opts.target_bucket_rows));
    let dir = opts.scratch.join(label);
    let _ = std::fs::remove_dir_all(&dir);
    let mut router = Router::new(&dir, buckets)?;
    while let Some(batch) = stream.next().await {
        router.route_batch(&batch?, day_start_us, day_end_us)?;
    }
    router.finish()
}

/// Reduces every bucket, calling `sink` with batches in
/// [`crate::reduce::thin_points_schema`] form. `prev` holds each vessel's stream
/// state from the day before, where known.
///
/// Returns the stats, each reduced vessel's stream state at the end of the day
/// (to carry into the next one), and each vessel's day summary (for
/// `vessel_daily`).
pub fn reduce_buckets(
    m: &Manifest,
    opts: &ReduceOptions,
    prev: &HashMap<u32, StreamState>,
    sink: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<(ReduceStats, HashMap<u32, StreamState>, Vec<VesselDay>)> {
    let started = Instant::now();
    let mut st = ReduceStats {
        buckets: m.buckets,
        routed: m.routed,
        quarantined: m.quarantined,
        outside_day: m.outside_day,
        scratch_bytes: m.bytes_on_disk,
        ..Default::default()
    };
    let mut next: HashMap<u32, StreamState> = HashMap::new();
    let mut vessel_days: Vec<VesselDay> = Vec::new();
    let mut chunk: Vec<OutRow> = Vec::with_capacity(OUT_CHUNK_ROWS);
    let flush = |chunk: &mut Vec<OutRow>, sink: &mut dyn FnMut(RecordBatch) -> Result<()>| {
        if !chunk.is_empty() {
            sink(to_batch(chunk, &m.dicts)?)?;
            chunk.clear();
        }
        Ok::<_, anyhow::Error>(())
    };

    // One buffer for every bucket, and one for each vessel's reports, so the
    // loop does not allocate and free large vectors.
    let mut pts: Vec<RawPoint> = Vec::new();
    let mut group: Vec<RawPoint> = Vec::new();
    for b in 0..m.buckets {
        st.largest_bucket = st.largest_bucket.max(m.counts[b]);
        read_bucket_into(m, b, &mut pts)?;
        pts.sort_unstable_by_key(|p| p.mmsi);
        let mut i = 0;
        while i < pts.len() {
            let mmsi = pts[i].mmsi;
            let mut j = i;
            while j < pts.len() && pts[j].mmsi == mmsi {
                j += 1;
            }
            group.clear();
            group.extend_from_slice(&pts[i..j]);
            let r = reduce_vessel(std::mem::take(&mut group), &m.dicts, prev.get(&mmsi).copied(), &opts.rules, &opts.thin);
            st.vessels += 1;
            st.unaccounted += r.unaccounted_raw as u64;
            if let Some(state) = r.state {
                next.insert(mmsi, state);
            }
            vessel_days.extend(r.summary);
            for row in &r.rows {
                st.kept += 1;
                st.represented += row.n_raw as u64;
                st.collapsed_dups += row.n_collapsed_dups as u64;
                st.no_position += row.n_no_position as u64;
                st.outliers_raw += row.n_outliers_raw as u64;
                for (k, (_, bit)) in KEEP_REASONS.iter().enumerate() {
                    if row.keep_reason & bit != 0 {
                        st.reasons[k] += 1;
                    }
                }
            }
            chunk.extend(r.rows);
            if chunk.len() >= OUT_CHUNK_ROWS {
                flush(&mut chunk, sink)?;
            }
            i = j;
        }
    }
    flush(&mut chunk, sink)?;
    st.reduce_time = started.elapsed();
    Ok((st, next, vessel_days))
}

/// Carries `next` (the end-of-day states just computed) into `prev`, dropping
/// entries older than `keep_after_us`: a vessel silent that long starts a fresh
/// segment anyway.
pub fn merge_states(
    prev: &mut HashMap<u32, StreamState>,
    next: HashMap<u32, StreamState>,
    keep_after_us: i64,
) {
    prev.extend(next);
    prev.retain(|_, s| s.ts_us >= keep_after_us);
}

/// Reduces `points` (any number of vessels, any order) entirely in memory, with
/// no router. For small inputs and tests; use [`route_day`] and
/// [`reduce_buckets`] for a real day.
pub fn reduce_all(
    points: Vec<RawPoint>,
    dicts: &crate::reduce::Dicts,
    prev: &HashMap<u32, StreamState>,
    rules: &Rules,
    thin: &ThinOpts,
) -> Result<Vec<RecordBatch>> {
    Ok(reduce_all_with_state(points, dicts, prev, rules, thin)?.0)
}

/// [`reduce_all`], also returning each vessel's end-of-input stream state.
pub fn reduce_all_with_state(
    mut points: Vec<RawPoint>,
    dicts: &crate::reduce::Dicts,
    prev: &HashMap<u32, StreamState>,
    rules: &Rules,
    thin: &ThinOpts,
) -> Result<(Vec<RecordBatch>, HashMap<u32, StreamState>)> {
    points.sort_by_key(|p| p.mmsi);
    let mut rows: Vec<OutRow> = Vec::new();
    let mut next = HashMap::new();
    let mut i = 0;
    while i < points.len() {
        let mmsi = points[i].mmsi;
        let mut j = i;
        while j < points.len() && points[j].mmsi == mmsi {
            j += 1;
        }
        let r = reduce_vessel(points[i..j].to_vec(), dicts, prev.get(&mmsi).copied(), rules, thin);
        if let Some(state) = r.state {
            next.insert(mmsi, state);
        }
        rows.extend(r.rows);
        i = j;
    }
    let batches = if rows.is_empty() {
        Vec::new()
    } else {
        vec![to_batch(&rows, dicts)?]
    };
    Ok((batches, next))
}

/// What [`reduce_all_with_state`] produces for a day, plus the per-vessel day
/// summaries `vessel_daily` is made from.
pub struct InMemoryReduced {
    pub batches: Vec<RecordBatch>,
    pub days: Vec<VesselDay>,
}

/// Reduces `points` in memory like [`reduce_all`], also returning the per-vessel
/// day summaries. For tests and small inputs.
pub fn reduce_buckets_in_memory(
    mut points: Vec<RawPoint>,
    dicts: &crate::reduce::Dicts,
    rules: &Rules,
    thin: &ThinOpts,
) -> InMemoryReduced {
    points.sort_by_key(|p| p.mmsi);
    let (mut rows, mut days) = (Vec::new(), Vec::new());
    let mut i = 0;
    while i < points.len() {
        let mmsi = points[i].mmsi;
        let mut j = i;
        while j < points.len() && points[j].mmsi == mmsi {
            j += 1;
        }
        let r = reduce_vessel(points[i..j].to_vec(), dicts, None, rules, thin);
        days.extend(r.summary);
        rows.extend(r.rows);
        i = j;
    }
    let batches = if rows.is_empty() {
        Vec::new()
    } else {
        vec![to_batch(&rows, dicts).expect("valid batch")]
    };
    InMemoryReduced { batches, days }
}

/// Peak resident memory of this process so far, in bytes.
pub fn peak_rss_bytes() -> u64 {
    // SAFETY: getrusage only writes into the struct we hand it.
    unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut u);
        let v = u.ru_maxrss as u64;
        if cfg!(target_os = "macos") {
            v
        } else {
            v * 1024
        }
    }
}

pub fn scratch_label(day: chrono::NaiveDate) -> String {
    format!("day-{day}")
}

impl ReduceStats {
    /// A human-readable report.
    pub fn report(&self, thin_off: bool) -> String {
        let mut s = String::new();
        let pct = |n: u64| {
            if self.routed == 0 {
                0.0
            } else {
                100.0 * n as f64 / self.routed as f64
            }
        };
        s += &format!(
            "routed {} reports ({} vessels) into {} buckets, largest {} reports, {:.2} GB scratch\n",
            self.routed,
            self.vessels,
            self.buckets,
            self.largest_bucket,
            self.scratch_bytes as f64 / 1e9
        );
        if self.quarantined + self.outside_day > 0 {
            s += &format!(
                "set aside: {} with an implausible MMSI or no time, {} outside the day\n",
                self.quarantined, self.outside_day
            );
        }
        s += &format!(
            "kept {} rows = {:.1}% of the day{}\n",
            self.kept,
            100.0 * self.retention(),
            if thin_off { " (thinning off)" } else { "" }
        );
        s += &format!(
            "represented {} of {} reports ({} unaccounted); collapsed {} duplicates ({:.1}%), {} without a position, {} flagged/invalid\n",
            self.represented,
            self.routed,
            self.unaccounted,
            self.collapsed_dups,
            pct(self.collapsed_dups),
            self.no_position,
            self.outliers_raw
        );
        s += "kept because (a row can have several reasons):\n";
        for ((name, _), n) in KEEP_REASONS.iter().zip(self.reasons) {
            s += &format!("  {name:>16} {n:>12}  {:>5.1}% of kept\n", 100.0 * n as f64 / self.kept.max(1) as f64);
        }
        s += &format!(
            "time: route {:.1}s, reduce {:.1}s; peak memory {:.0} MB\n",
            self.route_time.as_secs_f64(),
            self.reduce_time.as_secs_f64(),
            peak_rss_bytes() as f64 / 1e6
        );
        s
    }
}

/// Removes a day's scratch files.
pub fn cleanup(scratch: &Path, label: &str) {
    let _ = std::fs::remove_dir_all(scratch.join(label));
}
