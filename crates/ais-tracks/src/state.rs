//! What a run needs to remember between days: where each vessel's stream left
//! off (`vessel_state`), and which days have been built from what
//! (`build_log`). Together they make daily runs incremental and idempotent.
//!
//! **`vessel_state`** is one small partition per built day: each vessel's last
//! positioned report as of the end of that day. The next day reads it instead
//! of scanning the previous day's whole output.
//!
//! **`build_log`** is append-only: one row per step and day, written *after*
//! that day's data is committed, so a day is "done" only once its row exists.
//! A crash between the data and the row just means the day is rebuilt, which is
//! safe because a rebuild replaces the day's partition. Each row records an
//! `input_token` describing what the day was built from. A day needs rebuilding
//! when the token it would have now differs from the logged one:
//!
//! - `track_points`: the silver row count for the day, plus a digest of the
//!   vessel states it started from. Late data changes the count; a rebuilt
//!   earlier day changes the digest only if some vessel's end state changed, so
//!   the rebuild stops spreading forward when nothing downstream would differ.
//! - `tracks`, `stop_segments`: the upstream `track_points` build, plus the
//!   previous day's build of the same step (their ids chain across midnight).
//!
//! Snapshot summaries would be the usual place for this, but they describe the
//! whole table, and per-day provenance needs per-day rows.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use collect_maint::rewrite::LiveFile;
use futures_util::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Reference;
use iceberg::spec::{Datum, NestedField, PrimitiveLiteral, PrimitiveType, Schema};
use iceberg::table::Table;

use crate::reduce::StreamState;

pub const TABLE_VESSEL_STATE: &str = "vessel_state";
pub const TABLE_BUILD_LOG: &str = "build_log";

pub const STEP_TRACK_POINTS: &str = "track_points";
pub const STEP_TRACKS: &str = "tracks";
pub const STEP_STOP_SEGMENTS: &str = "stop_segments";

fn required(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::required(id, name, ty.into()))
}

/// `ts` is the start of the day the state is as of; the table is partitioned by
/// day on it.
pub fn vessel_state_schema() -> Schema {
    use PrimitiveType::*;
    Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            required(1, "ts", Timestamptz),
            required(2, "mmsi", Long),
            required(3, "last_ts", Timestamptz),
            required(4, "lat", Double),
            required(5, "lon", Double),
        ])
        .build()
        .expect("building vessel_state schema")
}

pub fn build_log_schema() -> Schema {
    use PrimitiveType::*;
    Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            required(1, "step", String),
            required(2, "day", Int),
            required(3, "input_token", String),
            required(4, "output_rows", Long),
            required(5, "built_at", Timestamptz),
        ])
        .build()
        .expect("building build_log schema")
}

pub fn date_to_day(d: NaiveDate) -> i32 {
    d.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"))
        .num_days() as i32
}

pub fn day_to_date(day: i32) -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch") + chrono::Duration::days(day as i64)
}

// ---- vessel state -----------------------------------------------------------

/// A vessel's states as one batch (sorted by mmsi, so output is deterministic),
/// as of the day starting at `as_of_start_us`. `None` when there are none.
pub fn states_batch(
    as_of_start_us: i64,
    states: &HashMap<u32, StreamState>,
) -> Result<Option<RecordBatch>> {
    if states.is_empty() {
        return Ok(None);
    }
    let mut v: Vec<(&u32, &StreamState)> = states.iter().collect();
    v.sort_by_key(|(m, _)| **m);
    let n = v.len();
    let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&vessel_state_schema())?);
    let tz = |it: Vec<i64>| -> Arc<dyn Array> {
        Arc::new(TimestampMicrosecondArray::from(it).with_timezone("+00:00"))
    };
    Ok(Some(RecordBatch::try_new(
        schema,
        vec![
            tz(vec![as_of_start_us; n]),
            Arc::new(Int64Array::from_iter_values(v.iter().map(|(m, _)| **m as i64))),
            tz(v.iter().map(|(_, s)| s.ts_us).collect()),
            Arc::new(Float64Array::from_iter_values(v.iter().map(|(_, s)| s.lat))),
            Arc::new(Float64Array::from_iter_values(v.iter().map(|(_, s)| s.lon))),
        ],
    )?))
}

/// Reads states written by [`states_batch`]. When several as-of days are present
/// the latest one wins, whole: a state map is complete as of its day.
pub fn states_from_batches(batches: &[RecordBatch]) -> Result<HashMap<u32, StreamState>> {
    let mut latest: i64 = i64::MIN;
    for b in batches {
        let ts = cast(b.column_by_name("ts").context("no ts")?, &DataType::Int64)?;
        let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..ts.len() {
            latest = latest.max(ts.value(i));
        }
    }
    let mut out = HashMap::new();
    for b in batches {
        let col = |n: &str, ty: &DataType| -> Result<_> {
            Ok(cast(b.column_by_name(n).with_context(|| format!("no column {n}"))?, ty)?)
        };
        let ts = col("ts", &DataType::Int64)?;
        let mmsi = col("mmsi", &DataType::Int64)?;
        let last = col("last_ts", &DataType::Int64)?;
        let (lat, lon) = (col("lat", &DataType::Float64)?, col("lon", &DataType::Float64)?);
        let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
        let mmsi = mmsi.as_any().downcast_ref::<Int64Array>().unwrap();
        let last = last.as_any().downcast_ref::<Int64Array>().unwrap();
        let lat = lat.as_any().downcast_ref::<Float64Array>().unwrap();
        let lon = lon.as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            if ts.value(i) == latest {
                out.insert(
                    mmsi.value(i) as u32,
                    StreamState {
                        ts_us: last.value(i),
                        lat: lat.value(i),
                        lon: lon.value(i),
                    },
                );
            }
        }
    }
    Ok(out)
}

/// An order-independent digest of a set of states: equal sets give equal
/// digests, and any changed vessel almost surely changes it.
pub fn state_digest(states: &HashMap<u32, StreamState>) -> u64 {
    fn mix(mut z: u64) -> u64 {
        z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    states.iter().fold(0u64, |acc, (m, s)| {
        let h = mix(*m as u64) ^ mix(s.ts_us as u64 ^ 1) ^ mix(s.lat.to_bits() ^ 2) ^ mix(s.lon.to_bits() ^ 3);
        acc.wrapping_add(mix(h))
    })
}

/// Reads the vessel states in force at the start of the day `day_start_us`: the
/// latest as-of day within `lookback_us` before it, with vessels older than the
/// lookback dropped (they start a fresh segment anyway).
pub async fn read_states_before(
    table: &Table,
    day_start_us: i64,
    lookback_us: i64,
) -> Result<HashMap<u32, StreamState>> {
    if table.metadata().current_snapshot().is_none() {
        return Ok(HashMap::new());
    }
    let pred = Reference::new("ts")
        .greater_than_or_equal_to(Datum::timestamptz_micros(day_start_us - lookback_us))
        .and(Reference::new("ts").less_than(Datum::timestamptz_micros(day_start_us)));
    let batches = scan_all(table, Some(pred)).await?;
    let mut states = states_from_batches(&batches)?;
    states.retain(|_, s| s.ts_us >= day_start_us - lookback_us);
    Ok(states)
}

/// Reads a small table (optionally filtered) fully into memory.
pub async fn scan_all(
    table: &Table,
    filter: Option<iceberg::expr::Predicate>,
) -> Result<Vec<RecordBatch>> {
    if table.metadata().current_snapshot().is_none() {
        return Ok(Vec::new());
    }
    let mut scan = table.scan().select_all();
    if let Some(f) = filter {
        scan = scan.with_filter(f);
    }
    let tasks: Vec<_> = scan.build()?.plan_files().await?.try_collect().await?;
    let reader = ArrowReaderBuilder::new(table.file_io().clone()).build();
    let stream = reader.read(futures_util::stream::iter(tasks.into_iter().map(Ok)).boxed())?;
    Ok(stream.try_collect().await?)
}

// ---- silver days --------------------------------------------------------------

/// Rows per day, from a table's live files (metadata only). Files whose
/// partition is not a day number are ignored.
pub fn days_from_files(files: &[LiveFile]) -> BTreeMap<NaiveDate, u64> {
    let mut out = BTreeMap::new();
    for f in files {
        let day = f
            .partition
            .as_ref()
            .and_then(|p| p.fields().first())
            .and_then(|l| l.as_ref())
            .and_then(|l| l.as_primitive_literal());
        if let Some(PrimitiveLiteral::Int(d)) = day {
            *out.entry(day_to_date(d)).or_insert(0) += f.records;
        }
    }
    out
}

// ---- build log ------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct LogRow {
    pub step: String,
    pub day: NaiveDate,
    pub input_token: String,
    pub output_rows: i64,
    /// Microseconds since the epoch.
    pub built_at_us: i64,
}

/// The latest row per step and day.
#[derive(Debug, Default, Clone)]
pub struct Log {
    rows: HashMap<(String, NaiveDate), LogRow>,
}

impl Log {
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self> {
        let mut log = Log::default();
        for b in batches {
            let col = |n: &str, ty: &DataType| -> Result<_> {
                Ok(cast(b.column_by_name(n).with_context(|| format!("no column {n}"))?, ty)?)
            };
            let step = col("step", &DataType::Utf8)?;
            let day = col("day", &DataType::Int32)?;
            let tok = col("input_token", &DataType::Utf8)?;
            let rows = col("output_rows", &DataType::Int64)?;
            let at = col("built_at", &DataType::Int64)?;
            let step = step.as_any().downcast_ref::<StringArray>().unwrap();
            let day = day.as_any().downcast_ref::<Int32Array>().unwrap();
            let tok = tok.as_any().downcast_ref::<StringArray>().unwrap();
            let rows = rows.as_any().downcast_ref::<Int64Array>().unwrap();
            let at = at.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                log.insert(LogRow {
                    step: step.value(i).to_string(),
                    day: day_to_date(day.value(i)),
                    input_token: tok.value(i).to_string(),
                    output_rows: rows.value(i),
                    built_at_us: at.value(i),
                });
            }
        }
        Ok(log)
    }

    /// Records a row, keeping the later one if the step and day already have one.
    pub fn insert(&mut self, r: LogRow) {
        let key = (r.step.clone(), r.day);
        match self.rows.get(&key) {
            Some(old) if old.built_at_us > r.built_at_us => {}
            _ => {
                self.rows.insert(key, r);
            }
        }
    }

    pub fn get(&self, step: &str, day: NaiveDate) -> Option<&LogRow> {
        self.rows.get(&(step.to_string(), day))
    }

    /// Whether `step` was built for `day` from exactly `token`.
    pub fn is_current(&self, step: &str, day: NaiveDate, token: &str) -> bool {
        self.get(step, day).is_some_and(|r| r.input_token == token)
    }

    /// Days with a log row for `step`.
    pub fn days(&self, step: &str) -> Vec<NaiveDate> {
        let mut v: Vec<NaiveDate> = self
            .rows
            .keys()
            .filter(|(s, _)| s == step)
            .map(|(_, d)| *d)
            .collect();
        v.sort();
        v
    }
}

pub fn log_batch(rows: &[LogRow]) -> Result<RecordBatch> {
    let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&build_log_schema())?);
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.step.as_str()))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| date_to_day(r.day)))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.input_token.as_str()))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.output_rows))),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.built_at_us))
                    .with_timezone("+00:00"),
            ),
        ],
    )?)
}

/// The `input_token` for a `track_points` day.
pub fn track_points_token(silver_rows: u64, start_states: &HashMap<u32, StreamState>) -> String {
    format!("rows={silver_rows};state={:016x}", state_digest(start_states))
}

/// The `input_token` for a step that reads `track_points`: the `track_points`
/// build it read, and the previous day's build of the same step.
pub fn downstream_token(upstream: &LogRow, previous_same_step: Option<&LogRow>) -> String {
    format!(
        "tp={};prev={}",
        upstream.built_at_us,
        previous_same_step.map_or(0, |r| r.built_at_us)
    )
}

// ---- choosing days ---------------------------------------------------------------

/// How a command names the days it works on.
#[derive(Debug, Clone, Default)]
pub struct DaySelect {
    pub from: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
    pub catch_up: bool,
    /// With `catch_up`, rebuild every candidate day whatever the log says.
    pub full: bool,
    /// With `catch_up`, include today (UTC), which is still filling.
    pub include_today: bool,
}

/// The days to consider, and whether each is forced (built whatever the log
/// says). Without `catch_up`, that is exactly `from..=to`, all forced. With it,
/// the `candidates` (days that have input) in range, before today unless asked,
/// forced only under `full`.
pub fn select_days(
    sel: &DaySelect,
    candidates: &[NaiveDate],
    today: NaiveDate,
) -> Result<(Vec<NaiveDate>, bool)> {
    if !sel.catch_up {
        let from = sel.from.context("give --from, or --catch-up")?;
        let to = sel.to.unwrap_or(from);
        anyhow::ensure!(to >= from, "--to is before --from");
        let mut v = Vec::new();
        let mut d = from;
        while d <= to {
            v.push(d);
            d = d.succ_opt().context("date overflow")?;
        }
        return Ok((v, true));
    }
    let mut v: Vec<NaiveDate> = candidates
        .iter()
        .copied()
        .filter(|d| sel.from.is_none_or(|f| *d >= f))
        .filter(|d| sel.to.is_none_or(|t| *d <= t))
        .filter(|d| sel.include_today || *d < today)
        .collect();
    v.sort();
    v.dedup();
    Ok((v, sel.full))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{Literal, Struct};

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    fn st(ts: i64, lat: f64) -> StreamState {
        StreamState { ts_us: ts, lat, lon: -lat }
    }

    #[test]
    fn states_round_trip_and_the_latest_as_of_day_wins() {
        let mut a = HashMap::new();
        a.insert(1u32, st(100, 10.0));
        a.insert(2u32, st(200, 20.0));
        let mut b = a.clone();
        b.insert(3, st(300, 30.0));
        b.insert(1, st(150, 11.0));
        let ba = states_batch(1_000, &a).unwrap().unwrap();
        let bb = states_batch(2_000, &b).unwrap().unwrap();
        assert_eq!(states_from_batches(std::slice::from_ref(&ba)).unwrap(), a);
        // Two as-of days in one read: only the later, complete map is taken.
        assert_eq!(states_from_batches(&[ba, bb]).unwrap(), b);
        assert!(states_batch(1, &HashMap::new()).unwrap().is_none());
        crate::carry::check_batches(&vessel_state_schema(), &[states_batch(1, &a).unwrap().unwrap()])
            .unwrap();
    }

    #[test]
    fn the_digest_ignores_order_and_notices_change() {
        let mut a = HashMap::new();
        for i in 0..1000u32 {
            a.insert(i, st(i as i64 * 7, i as f64 * 0.01));
        }
        let b: HashMap<u32, StreamState> =
            (0..1000u32).rev().map(|i| (i, st(i as i64 * 7, i as f64 * 0.01))).collect();
        assert_eq!(state_digest(&a), state_digest(&b));
        let mut c = a.clone();
        c.get_mut(&500).unwrap().lat += 1e-9;
        assert_ne!(state_digest(&a), state_digest(&c));
        let mut e = a.clone();
        e.remove(&3);
        assert_ne!(state_digest(&a), state_digest(&e));
        assert_eq!(state_digest(&HashMap::new()), 0);
    }

    #[test]
    fn silver_days_come_from_partition_values() {
        let file = |day: i32, records: u64| LiveFile {
            path: format!("f{day}-{records}"),
            size: 1,
            records,
            partition: Some(Struct::from_iter([Some(Literal::int(day))])),
        };
        let days = days_from_files(&[file(20_500, 10), file(20_500, 5), file(20_501, 7)]);
        assert_eq!(days.len(), 2);
        assert_eq!(days[&day_to_date(20_500)], 15);
        assert_eq!(days[&day_to_date(20_501)], 7);
        assert_eq!(date_to_day(day_to_date(20_500)), 20_500);
    }

    fn row(step: &str, day: &str, token: &str, at: i64) -> LogRow {
        LogRow {
            step: step.into(),
            day: d(day),
            input_token: token.into(),
            output_rows: 5,
            built_at_us: at,
        }
    }

    #[test]
    fn the_log_keeps_the_latest_row_and_round_trips() {
        let rows = vec![
            row("track_points", "2026-03-10", "old", 10),
            row("track_points", "2026-03-10", "new", 20),
            row("tracks", "2026-03-10", "x", 15),
        ];
        let log = Log::from_batches(&[log_batch(&rows).unwrap()]).unwrap();
        assert!(log.is_current("track_points", d("2026-03-10"), "new"));
        assert!(!log.is_current("track_points", d("2026-03-10"), "old"));
        assert!(!log.is_current("track_points", d("2026-03-11"), "new"));
        assert_eq!(log.days("track_points"), vec![d("2026-03-10")]);
        crate::carry::check_batches(&build_log_schema(), &[log_batch(&rows).unwrap()]).unwrap();
    }

    #[test]
    fn a_downstream_token_changes_when_either_neighbour_is_rebuilt() {
        let up = row("track_points", "2026-03-10", "t", 100);
        let prev = row("tracks", "2026-03-09", "t", 50);
        let base = downstream_token(&up, Some(&prev));
        assert_ne!(base, downstream_token(&row("track_points", "2026-03-10", "t", 101), Some(&prev)));
        assert_ne!(base, downstream_token(&up, Some(&row("tracks", "2026-03-09", "t", 51))));
        assert_ne!(base, downstream_token(&up, None));
    }

    #[test]
    fn explicit_ranges_are_forced_and_catch_up_takes_only_days_with_input() {
        let today = d("2026-03-12");
        let cands = [d("2026-03-08"), d("2026-03-09"), d("2026-03-11"), d("2026-03-12")];
        let explicit = DaySelect { from: Some(d("2026-03-10")), to: Some(d("2026-03-11")), ..Default::default() };
        let (days, forced) = select_days(&explicit, &cands, today).unwrap();
        assert_eq!(days, vec![d("2026-03-10"), d("2026-03-11")]);
        assert!(forced);
        assert!(select_days(&DaySelect::default(), &cands, today).is_err(), "needs --from or --catch-up");

        let catch = DaySelect { catch_up: true, ..Default::default() };
        let (days, forced) = select_days(&catch, &cands, today).unwrap();
        assert_eq!(days, vec![d("2026-03-08"), d("2026-03-09"), d("2026-03-11")], "today is still filling");
        assert!(!forced);
        let with_today = DaySelect { catch_up: true, include_today: true, full: true, from: Some(d("2026-03-09")), ..Default::default() };
        let (days, forced) = select_days(&with_today, &cands, today).unwrap();
        assert_eq!(days, vec![d("2026-03-09"), d("2026-03-11"), d("2026-03-12")]);
        assert!(forced);
    }
}
