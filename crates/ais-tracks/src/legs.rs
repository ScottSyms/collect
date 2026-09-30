//! Voyage legs, advanced one day at a time.
//!
//! A leg is the stretch between a vessel's consecutive stops: the leg before its
//! first stop (origin unknown), the legs between stops, and the leg after its
//! last stop while it is still under way. To build them without rereading
//! history, each vessel keeps a small state row: where its current leg began,
//! the last stop it left, and the leg's running totals. A day then does only
//! what that day changes:
//!
//! - A vessel with no stop today just adds today's totals (`vessel_daily`) to
//!   its running totals. No point is read.
//! - A vessel that reaches a new stop today closes its current leg there, and a
//!   vessel that leaves a stop today starts one. Those legs begin or end
//!   part-way through the day, so their totals need today's points split at the
//!   exact times: one query over one day's `track_points`, for those vessels
//!   only.
//! - A vessel seen for the first time starts a leg at its first positioned row.
//!
//! Every point is therefore read once, on its own day, however long the leg. The
//! per-vessel logic ([`advance`]) is a pure function over plain values; the
//! functions after it move data in and out of DataFusion.
//!
//! Closed legs never change, so they are written once, into the partition of the
//! day they end. The legs still under way are the state rows of vessels that
//! have moved on from their last stop.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use iceberg::spec::{NestedField, PrimitiveType, Schema};

pub const TABLE_VOYAGE_STATE: &str = "voyage_state";
pub const TABLE_OPEN_VOYAGES: &str = "open_voyages";

// ---- plain values ---------------------------------------------------------------------

/// The stop a leg began at (all `None` when the origin is unknown).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Origin {
    pub stop_id: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub port_id: Option<i64>,
    pub port_name: Option<String>,
    pub unlocode: Option<String>,
    pub country: Option<String>,
    pub port_distance_nm: Option<f64>,
}

/// What a leg has added up to, over the points between its ends.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Metrics {
    pub dist_raw: f64,
    pub dist_clean: f64,
    pub max_sog: Option<f64>,
    pub n_points: i64,
    pub n_gaps: i64,
    pub n_outliers: i64,
}

impl Metrics {
    pub fn plus(&self, o: &Metrics) -> Metrics {
        Metrics {
            dist_raw: self.dist_raw + o.dist_raw,
            dist_clean: self.dist_clean + o.dist_clean,
            max_sog: match (self.max_sog, o.max_sog) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            },
            n_points: self.n_points + o.n_points,
            n_gaps: self.n_gaps + o.n_gaps,
            n_outliers: self.n_outliers + o.n_outliers,
        }
    }
}

/// A stop as a leg's end sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct StopEvt {
    pub origin: Origin,
    pub arrive_ts: i64,
    pub depart_ts: i64,
}

/// Where a vessel stands in its voyages, as of the end of a day.
#[derive(Debug, Clone, PartialEq)]
pub struct VesselLeg {
    pub mmsi: u32,
    /// First and last positioned row ever seen.
    pub first_ts: i64,
    pub last_ts: i64,
    /// Where the current leg began: the last stop's departure, or `first_ts`.
    pub from_ts: i64,
    /// The last stop (`stop_id` is `None` until the vessel has stopped).
    pub origin: Origin,
    /// The current leg's totals over the points after `from_ts`.
    pub metrics: Metrics,
}

impl VesselLeg {
    /// Whether the current leg is one to publish: either the vessel has never
    /// stopped, or it has been seen since it left its last stop.
    pub fn is_open(&self) -> bool {
        self.origin.stop_id.is_none() || self.last_ts > self.from_ts
    }
}

/// A leg that ended at a stop today.
#[derive(Debug, Clone, PartialEq)]
pub struct ClosedLeg {
    pub mmsi: u32,
    pub depart_ts: i64,
    pub arrive_ts: i64,
    pub origin: Origin,
    pub dest: Origin,
    pub metrics: Metrics,
}

/// One vessel's day, as `vessel_daily` records it.
#[derive(Debug, Clone, PartialEq)]
pub struct DayTotals {
    pub first_stream_ts: i64,
    pub last_stream_ts: i64,
    pub metrics: Metrics,
}

/// The rows of the day between two times: `(from, to]`, with no upper limit when
/// `to` is `None`.
pub type Interval = (i64, Option<i64>);

/// Advances one vessel through one day.
///
/// `stops` are the stops that end today (they arrived earlier today, or before
/// and are still going), oldest first. `sum` totals today's points in an
/// interval; it is asked only for intervals that start or end part-way through
/// the day. Returns the legs closed today and the vessel's state at the end of
/// the day.
pub fn advance(
    mmsi: u32,
    state: Option<&VesselLeg>,
    stops: &[StopEvt],
    day: &DayTotals,
    day_start_us: i64,
    sum: &mut dyn FnMut(Interval) -> Metrics,
) -> (Vec<ClosedLeg>, VesselLeg) {
    struct Cur {
        from_ts: i64,
        origin: Origin,
        /// Totals over the points before today.
        base: Metrics,
    }
    let (first_ts, mut cur) = match state {
        Some(s) => (
            s.first_ts,
            Cur { from_ts: s.from_ts, origin: s.origin.clone(), base: s.metrics.clone() },
        ),
        None => (
            day.first_stream_ts,
            Cur { from_ts: day.first_stream_ts, origin: Origin::default(), base: Metrics::default() },
        ),
    };
    let last_stop_id = state.and_then(|s| s.origin.stop_id.clone());
    let mut closed = Vec::new();

    for x in stops {
        let continuing = x.arrive_ts < day_start_us || last_stop_id.as_ref() == x.origin.stop_id.as_ref();
        if !continuing {
            // A new stop ends the current leg. A vessel whose first row is inside
            // its first stop never had a leg before it.
            let no_leg_before = cur.origin.stop_id.is_none() && x.arrive_ts <= cur.from_ts;
            if !no_leg_before {
                let today = sum((cur.from_ts, Some(x.arrive_ts)));
                closed.push(ClosedLeg {
                    mmsi,
                    depart_ts: cur.from_ts,
                    arrive_ts: x.arrive_ts,
                    origin: cur.origin.clone(),
                    dest: x.origin.clone(),
                    metrics: cur.base.plus(&today),
                });
            }
        }
        // Either way, the next leg starts when the vessel leaves this stop.
        cur = Cur { from_ts: x.depart_ts, origin: x.origin.clone(), base: Metrics::default() };
    }

    // The current leg runs to the end of today. If it began before today it
    // covers all of today's points; otherwise only those after it began.
    let today = if cur.from_ts < day_start_us {
        day.metrics.clone()
    } else {
        sum((cur.from_ts, None))
    };
    let last_ts = state.map_or(day.last_stream_ts, |s| s.last_ts.max(day.last_stream_ts));
    let next = VesselLeg {
        mmsi,
        first_ts,
        last_ts,
        from_ts: cur.from_ts,
        origin: cur.origin,
        metrics: cur.base.plus(&today),
    };
    (closed, next)
}

// ---- schemas -----------------------------------------------------------------------------

fn required(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::required(id, name, ty.into()))
}

fn optional(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::optional(id, name, ty.into()))
}

/// One row per vessel: the state [`advance`] works on. `through` is the last day
/// folded in.
pub fn voyage_state_schema() -> Schema {
    use PrimitiveType::*;
    Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            required(1, "mmsi", Long),
            required(2, "first_ts", Timestamptz),
            required(3, "last_ts", Timestamptz),
            required(4, "from_ts", Timestamptz),
            optional(5, "origin_stop_id", String),
            optional(6, "origin_lat", Double),
            optional(7, "origin_lon", Double),
            optional(8, "origin_port_id", Long),
            optional(9, "origin_port_name", String),
            optional(10, "origin_unlocode", String),
            optional(11, "origin_country", String),
            optional(12, "origin_port_distance_nm", Double),
            required(13, "dist_nm_raw", Double),
            required(14, "dist_nm_clean", Double),
            optional(15, "max_sog_knots", Double),
            required(16, "n_points", Long),
            required(17, "n_gaps", Long),
            required(18, "n_outliers", Long),
            required(19, "through", Int),
        ])
        .build()
        .expect("building voyage_state schema")
}

// ---- reading and writing batches -----------------------------------------------------------

fn col<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    b.column_by_name(name).with_context(|| format!("no column {name}"))
}

fn i64s(b: &RecordBatch, name: &str) -> Result<Vec<Option<i64>>> {
    let c = cast(col(b, name)?, &DataType::Int64)?;
    let a = c.as_any().downcast_ref::<Int64Array>().context("int64")?;
    Ok((0..a.len()).map(|i| (!a.is_null(i)).then(|| a.value(i))).collect())
}

fn f64s(b: &RecordBatch, name: &str) -> Result<Vec<Option<f64>>> {
    let c = cast(col(b, name)?, &DataType::Float64)?;
    let a = c.as_any().downcast_ref::<Float64Array>().context("float64")?;
    Ok((0..a.len()).map(|i| (!a.is_null(i)).then(|| a.value(i))).collect())
}

fn strs(b: &RecordBatch, name: &str) -> Result<Vec<Option<String>>> {
    let c = cast(col(b, name)?, &DataType::Utf8)?;
    let a = c.as_any().downcast_ref::<StringArray>().context("utf8")?;
    Ok((0..a.len()).map(|i| (!a.is_null(i)).then(|| a.value(i).to_string())).collect())
}

fn ts_array(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(v).with_timezone("+00:00"))
}

/// Reads state rows written by [`state_batch`].
pub fn states_from_batches(batches: &[RecordBatch]) -> Result<Vec<VesselLeg>> {
    let mut out = Vec::new();
    for b in batches {
        let (mmsi, first, last, from) = (i64s(b, "mmsi")?, i64s(b, "first_ts")?, i64s(b, "last_ts")?, i64s(b, "from_ts")?);
        let (sid, lat, lon, pid) = (strs(b, "origin_stop_id")?, f64s(b, "origin_lat")?, f64s(b, "origin_lon")?, i64s(b, "origin_port_id")?);
        let (pname, unl, ctry, pdist) = (strs(b, "origin_port_name")?, strs(b, "origin_unlocode")?, strs(b, "origin_country")?, f64s(b, "origin_port_distance_nm")?);
        let (dr, dc, ms) = (f64s(b, "dist_nm_raw")?, f64s(b, "dist_nm_clean")?, f64s(b, "max_sog_knots")?);
        let (np, ng, no) = (i64s(b, "n_points")?, i64s(b, "n_gaps")?, i64s(b, "n_outliers")?);
        for i in 0..b.num_rows() {
            out.push(VesselLeg {
                mmsi: mmsi[i].context("mmsi")? as u32,
                first_ts: first[i].context("first_ts")?,
                last_ts: last[i].context("last_ts")?,
                from_ts: from[i].context("from_ts")?,
                origin: Origin {
                    stop_id: sid[i].clone(),
                    lat: lat[i],
                    lon: lon[i],
                    port_id: pid[i],
                    port_name: pname[i].clone(),
                    unlocode: unl[i].clone(),
                    country: ctry[i].clone(),
                    port_distance_nm: pdist[i],
                },
                metrics: Metrics {
                    dist_raw: dr[i].unwrap_or(0.0),
                    dist_clean: dc[i].unwrap_or(0.0),
                    max_sog: ms[i],
                    n_points: np[i].unwrap_or(0),
                    n_gaps: ng[i].unwrap_or(0),
                    n_outliers: no[i].unwrap_or(0),
                },
            });
        }
    }
    Ok(out)
}

/// State rows as a batch in [`voyage_state_schema`] form, `through` being the
/// day number they are as of.
pub fn state_batch(states: &[VesselLeg], through: i32) -> Result<Option<RecordBatch>> {
    if states.is_empty() {
        return Ok(None);
    }
    let s = states;
    let strv = |f: &dyn Fn(&VesselLeg) -> Option<&String>| -> ArrayRef {
        Arc::new(StringArray::from_iter(s.iter().map(|x| f(x).map(|v| v.as_str()))))
    };
    let f64v = |f: &dyn Fn(&VesselLeg) -> Option<f64>| -> ArrayRef { Arc::new(Float64Array::from_iter(s.iter().map(f))) };
    let i64v = |f: &dyn Fn(&VesselLeg) -> i64| -> ArrayRef { Arc::new(Int64Array::from_iter_values(s.iter().map(f))) };
    let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&voyage_state_schema())?);
    Ok(Some(RecordBatch::try_new(
        schema,
        vec![
            i64v(&|x| x.mmsi as i64),
            ts_array(s.iter().map(|x| Some(x.first_ts)).collect()),
            ts_array(s.iter().map(|x| Some(x.last_ts)).collect()),
            ts_array(s.iter().map(|x| Some(x.from_ts)).collect()),
            strv(&|x| x.origin.stop_id.as_ref()),
            f64v(&|x| x.origin.lat),
            f64v(&|x| x.origin.lon),
            Arc::new(Int64Array::from_iter(s.iter().map(|x| x.origin.port_id))),
            strv(&|x| x.origin.port_name.as_ref()),
            strv(&|x| x.origin.unlocode.as_ref()),
            strv(&|x| x.origin.country.as_ref()),
            f64v(&|x| x.origin.port_distance_nm),
            f64v(&|x| Some(x.metrics.dist_raw)),
            f64v(&|x| Some(x.metrics.dist_clean)),
            f64v(&|x| x.metrics.max_sog),
            i64v(&|x| x.metrics.n_points),
            i64v(&|x| x.metrics.n_gaps),
            i64v(&|x| x.metrics.n_outliers),
            Arc::new(Int32Array::from(vec![through; s.len()])),
        ],
    )?))
}

/// Stops (rows of the `stops` table) grouped by vessel, oldest first.
pub fn stops_by_vessel(batches: &[RecordBatch]) -> Result<HashMap<u32, Vec<StopEvt>>> {
    let mut out: HashMap<u32, Vec<StopEvt>> = HashMap::new();
    for b in batches {
        let (mmsi, arrive, depart) = (i64s(b, "mmsi")?, i64s(b, "arrive_ts")?, i64s(b, "depart_ts")?);
        let (sid, lat, lon, pid) = (strs(b, "stop_id")?, f64s(b, "lat")?, f64s(b, "lon")?, i64s(b, "port_id")?);
        let (pname, unl, ctry, pdist) = (strs(b, "port_name")?, strs(b, "port_unlocode")?, strs(b, "port_country")?, f64s(b, "port_distance_nm")?);
        for i in 0..b.num_rows() {
            out.entry(mmsi[i].context("mmsi")? as u32).or_default().push(StopEvt {
                origin: Origin {
                    stop_id: sid[i].clone(),
                    lat: lat[i],
                    lon: lon[i],
                    port_id: pid[i],
                    port_name: pname[i].clone(),
                    unlocode: unl[i].clone(),
                    country: ctry[i].clone(),
                    port_distance_nm: pdist[i],
                },
                arrive_ts: arrive[i].context("arrive_ts")?,
                depart_ts: depart[i].context("depart_ts")?,
            });
        }
    }
    for v in out.values_mut() {
        v.sort_by_key(|s| (s.arrive_ts, s.depart_ts));
    }
    Ok(out)
}

/// A day's totals per vessel, from `vessel_daily` rows. Vessels with no
/// positioned row that day are left out.
pub fn day_totals(batches: &[RecordBatch]) -> Result<HashMap<u32, DayTotals>> {
    let mut out = HashMap::new();
    for b in batches {
        let (mmsi, first, last) = (i64s(b, "mmsi")?, i64s(b, "first_stream_ts")?, i64s(b, "last_stream_ts")?);
        let (dr, dc, ms) = (f64s(b, "dist_nm_raw")?, f64s(b, "dist_nm_clean")?, f64s(b, "max_sog_knots")?);
        let (np, ng, no) = (i64s(b, "n_points")?, i64s(b, "n_gaps")?, i64s(b, "n_outliers")?);
        for i in 0..b.num_rows() {
            let (Some(f), Some(l)) = (first[i], last[i]) else { continue };
            out.insert(
                mmsi[i].context("mmsi")? as u32,
                DayTotals {
                    first_stream_ts: f,
                    last_stream_ts: l,
                    metrics: Metrics {
                        dist_raw: dr[i].unwrap_or(0.0),
                        dist_clean: dc[i].unwrap_or(0.0),
                        max_sog: ms[i],
                        n_points: np[i].unwrap_or(0),
                        n_gaps: ng[i].unwrap_or(0),
                        n_outliers: no[i].unwrap_or(0),
                    },
                },
            );
        }
    }
    Ok(out)
}

// ---- a day, over DataFusion ------------------------------------------------------------------

/// What a day produced.
pub struct DayAdvance {
    /// Legs closed today, before their declared destinations are attached.
    pub closed: Vec<ClosedLeg>,
    /// Every vessel's state as of the end of today.
    pub states: Vec<RecordBatch>,
    /// Vessels handled with row-level sums (a stop today, or first seen).
    pub event_vessels: usize,
}

async fn mem(ctx: &SessionContext, name: &str, batch: RecordBatch) -> Result<()> {
    let _ = ctx.deregister_table(name)?;
    ctx.register_table(name, Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?))?;
    Ok(())
}

/// The metrics of today's points inside each requested interval, by slot.
async fn interval_sums(
    ctx: &SessionContext,
    intervals: &[(u32, Interval)],
) -> Result<HashMap<usize, Metrics>> {
    if intervals.is_empty() {
        return Ok(HashMap::new());
    }
    let n = intervals.len();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("slot", DataType::Int64, false),
        arrow::datatypes::Field::new("mmsi", DataType::Int64, false),
        arrow::datatypes::Field::new("from_ts", DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())), false),
        arrow::datatypes::Field::new("to_ts", DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..n as i64)),
            Arc::new(Int64Array::from_iter_values(intervals.iter().map(|(m, _)| *m as i64))),
            ts_array(intervals.iter().map(|(_, (f, _))| Some(*f)).collect()),
            ts_array(intervals.iter().map(|(_, (_, t))| *t).collect()),
        ],
    )?;
    mem(ctx, "leg_intervals", batch).await?;
    let out = ctx
        .sql(
            "SELECT i.slot,
               sum(tp.dist_nm) FILTER (WHERE tp.has_position AND NOT tp.is_duplicate AND NOT tp.gap_before) AS dist_raw,
               sum(tp.dist_nm) FILTER (WHERE tp.has_position AND NOT tp.is_duplicate
                                         AND NOT tp.gap_before AND NOT tp.is_speed_jump) AS dist_clean,
               max(tp.max_sog) FILTER (WHERE tp.has_position AND NOT tp.is_duplicate) AS max_sog,
               CAST(sum(tp.n_raw) AS BIGINT) AS n_points,
               CAST(count(*) FILTER (WHERE tp.gap_before) AS BIGINT) AS n_gaps,
               CAST(sum(tp.n_outliers_raw) AS BIGINT) AS n_outliers
             FROM leg_intervals i JOIN leg_points tp
               ON tp.mmsi = i.mmsi AND tp.ts > i.from_ts AND (i.to_ts IS NULL OR tp.ts <= i.to_ts)
             GROUP BY i.slot",
        )
        .await
        .context("planning interval sums")?
        .collect()
        .await
        .context("computing interval sums")?;
    let mut map = HashMap::new();
    for b in &out {
        let slot = i64s(b, "slot")?;
        let (dr, dc, ms) = (f64s(b, "dist_raw")?, f64s(b, "dist_clean")?, f64s(b, "max_sog")?);
        let (np, ng, no) = (i64s(b, "n_points")?, i64s(b, "n_gaps")?, i64s(b, "n_outliers")?);
        for i in 0..b.num_rows() {
            map.insert(
                slot[i].context("slot")? as usize,
                Metrics {
                    dist_raw: dr[i].unwrap_or(0.0),
                    dist_clean: dc[i].unwrap_or(0.0),
                    max_sog: ms[i],
                    n_points: np[i].unwrap_or(0),
                    n_gaps: ng[i].unwrap_or(0),
                    n_outliers: no[i].unwrap_or(0),
                },
            );
        }
    }
    Ok(map)
}

/// Advances every vessel one day.
///
/// `ctx` must have these registered:
/// - `leg_state`: the previous [`voyage_state_schema`] rows (empty at the start),
/// - `leg_stops`: the `stops` rows that end today,
/// - `leg_daily`: today's `vessel_daily` rows,
/// - `leg_points`: today's `track_points` rows (only the columns the interval
///   sums read).
pub async fn advance_day(ctx: &SessionContext, day_start_us: i64, through: i32) -> Result<DayAdvance> {
    // Today's stops and totals, in memory: both are small (a row per vessel).
    let stops_batches = ctx.sql("SELECT * FROM leg_stops").await?.collect().await?;
    let stops = stops_by_vessel(&stops_batches)?;
    let daily_batches = ctx.sql("SELECT * FROM leg_daily").await?.collect().await?;
    let day = day_totals(&daily_batches)?;

    // Vessels that need row-level sums: a stop today, or no state yet.
    let known: HashSet<u32> = {
        let b = ctx.sql("SELECT mmsi FROM leg_state").await?.collect().await?;
        b.iter().flat_map(|b| i64s(b, "mmsi").unwrap_or_default()).flatten().map(|m| m as u32).collect()
    };
    let mut events: HashSet<u32> = stops.keys().copied().collect();
    events.extend(day.keys().filter(|m| !known.contains(m)));
    // A vessel with stops but no positioned row today cannot be advanced.
    events.retain(|m| day.contains_key(m));

    // Their state rows.
    let event_states: HashMap<u32, VesselLeg> = if events.is_empty() {
        HashMap::new()
    } else {
        let ids = Int64Array::from_iter_values(events.iter().map(|m| *m as i64));
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "mmsi",
            DataType::Int64,
            false,
        )]));
        mem(ctx, "leg_events", RecordBatch::try_new(schema, vec![Arc::new(ids)])?).await?;
        let b = ctx
            .sql("SELECT * FROM leg_state WHERE mmsi IN (SELECT mmsi FROM leg_events)")
            .await?
            .collect()
            .await?;
        states_from_batches(&b)?.into_iter().map(|s| (s.mmsi, s)).collect()
    };

    // First pass: find the intervals each event vessel needs.
    let mut wanted: Vec<(u32, Interval)> = Vec::new();
    let mut order: Vec<u32> = events.iter().copied().collect();
    order.sort_unstable();
    for m in &order {
        let mut record = |iv: Interval| {
            wanted.push((*m, iv));
            Metrics::default()
        };
        let _ = advance(*m, event_states.get(m), stops.get(m).map_or(&[][..], |v| v), &day[m], day_start_us, &mut record);
    }
    let sums = interval_sums(ctx, &wanted).await?;

    // Second pass: the same calls, now with their sums.
    let mut closed = Vec::new();
    let mut next: Vec<VesselLeg> = Vec::new();
    let mut slot = 0usize;
    for m in &order {
        let mut lookup = |_iv: Interval| {
            let r = sums.get(&slot).cloned().unwrap_or_default();
            slot += 1;
            r
        };
        let (c, n) = advance(*m, event_states.get(m), stops.get(m).map_or(&[][..], |v| v), &day[m], day_start_us, &mut lookup);
        closed.extend(c);
        next.push(n);
    }
    closed.sort_by_key(|c| (c.mmsi, c.depart_ts));

    // Everyone else: add today's totals to a vessel that has a leg going, and
    // leave the rest as they were.
    let event_rows = state_batch(&next, through)?;
    let mut parts: Vec<RecordBatch> = Vec::new();
    if let Some(b) = event_rows {
        mem(ctx, "leg_event_rows", b).await?;
    }
    let not_event = if events.is_empty() { "true" } else { "s.mmsi NOT IN (SELECT mmsi FROM leg_events)" };
    let updated = format!(
        "SELECT s.mmsi, s.first_ts,
                CASE WHEN d.last_stream_ts > s.last_ts THEN d.last_stream_ts ELSE s.last_ts END AS last_ts,
                s.from_ts, s.origin_stop_id, s.origin_lat, s.origin_lon, s.origin_port_id,
                s.origin_port_name, s.origin_unlocode, s.origin_country, s.origin_port_distance_nm,
                s.dist_nm_raw + d.dist_nm_raw AS dist_nm_raw,
                s.dist_nm_clean + d.dist_nm_clean AS dist_nm_clean,
                CASE WHEN s.max_sog_knots IS NULL THEN d.max_sog_knots
                     WHEN d.max_sog_knots IS NULL THEN s.max_sog_knots
                     WHEN d.max_sog_knots > s.max_sog_knots THEN d.max_sog_knots
                     ELSE s.max_sog_knots END AS max_sog_knots,
                s.n_points + d.n_points AS n_points, s.n_gaps + d.n_gaps AS n_gaps,
                s.n_outliers + d.n_outliers AS n_outliers,
                CAST({through} AS INT) AS through
         FROM leg_state s JOIN leg_daily d ON s.mmsi = d.mmsi
         WHERE d.last_stream_ts IS NOT NULL AND {not_event}"
    );
    let untouched = format!(
        "SELECT s.mmsi, s.first_ts, s.last_ts, s.from_ts, s.origin_stop_id, s.origin_lat, s.origin_lon,
                s.origin_port_id, s.origin_port_name, s.origin_unlocode, s.origin_country,
                s.origin_port_distance_nm, s.dist_nm_raw, s.dist_nm_clean, s.max_sog_knots,
                s.n_points, s.n_gaps, s.n_outliers, CAST({through} AS INT) AS through
         FROM leg_state s
         WHERE {not_event}
           AND s.mmsi NOT IN (SELECT mmsi FROM leg_daily WHERE last_stream_ts IS NOT NULL)"
    );
    for sql in [updated, untouched] {
        parts.extend(ctx.sql(&sql).await.context("planning state update")?.collect().await?);
    }
    if !next.is_empty() {
        parts.extend(ctx.sql("SELECT * FROM leg_event_rows").await?.collect().await?);
    }
    // Same column types whichever way a batch was made.
    let target = Arc::new(iceberg::arrow::schema_to_arrow_schema(&voyage_state_schema())?);
    let states = parts
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| {
            let cols = b
                .columns()
                .iter()
                .zip(target.fields())
                .map(|(c, f)| if c.data_type() == f.data_type() { Ok(c.clone()) } else { cast(c, f.data_type()) })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(RecordBatch::try_new(target.clone(), cols)?)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DayAdvance { closed, states, event_vessels: order.len() })
}

// ---- the rows that leave a day -----------------------------------------------------------------

/// Closed legs as the table `finish_closed_sql` reads, before their declared
/// destinations are attached.
pub fn closed_core_batch(legs: &[ClosedLeg]) -> Result<Option<RecordBatch>> {
    if legs.is_empty() {
        return Ok(None);
    }
    let strv = |f: &dyn Fn(&ClosedLeg) -> Option<&String>| -> ArrayRef {
        Arc::new(StringArray::from_iter(legs.iter().map(|l| f(l).map(|v| v.as_str()))))
    };
    let f64v = |f: &dyn Fn(&ClosedLeg) -> Option<f64>| -> ArrayRef { Arc::new(Float64Array::from_iter(legs.iter().map(f))) };
    let i64v = |f: &dyn Fn(&ClosedLeg) -> Option<i64>| -> ArrayRef { Arc::new(Int64Array::from_iter(legs.iter().map(f))) };
    let voyage_ids: Vec<String> = legs.iter().map(|l| format!("{}-{}", l.mmsi, l.depart_ts / 1000)).collect();
    let fields: Vec<(&str, ArrayRef)> = vec![
        ("voyage_id", Arc::new(StringArray::from_iter_values(voyage_ids.iter().map(|s| s.as_str())))),
        ("mmsi", i64v(&|l| Some(l.mmsi as i64))),
        ("depart_ts", ts_array(legs.iter().map(|l| Some(l.depart_ts)).collect())),
        ("arrive_ts", ts_array(legs.iter().map(|l| Some(l.arrive_ts)).collect())),
        ("origin_stop_id", strv(&|l| l.origin.stop_id.as_ref())),
        ("origin_lat", f64v(&|l| l.origin.lat)),
        ("origin_lon", f64v(&|l| l.origin.lon)),
        ("origin_port_id", i64v(&|l| l.origin.port_id)),
        ("origin_port_name", strv(&|l| l.origin.port_name.as_ref())),
        ("origin_unlocode", strv(&|l| l.origin.unlocode.as_ref())),
        ("origin_country", strv(&|l| l.origin.country.as_ref())),
        ("origin_port_distance_nm", f64v(&|l| l.origin.port_distance_nm)),
        ("dest_stop_id", strv(&|l| l.dest.stop_id.as_ref())),
        ("dest_lat", f64v(&|l| l.dest.lat)),
        ("dest_lon", f64v(&|l| l.dest.lon)),
        ("dest_port_id", i64v(&|l| l.dest.port_id)),
        ("dest_port_name", strv(&|l| l.dest.port_name.as_ref())),
        ("dest_unlocode", strv(&|l| l.dest.unlocode.as_ref())),
        ("dest_country", strv(&|l| l.dest.country.as_ref())),
        ("dest_port_distance_nm", f64v(&|l| l.dest.port_distance_nm)),
        ("dist_nm_raw", f64v(&|l| Some(l.metrics.dist_raw))),
        ("dist_nm_clean", f64v(&|l| Some(l.metrics.dist_clean))),
        ("max_sog_knots", f64v(&|l| l.metrics.max_sog)),
        ("n_points", i64v(&|l| Some(l.metrics.n_points))),
        ("n_gaps", i64v(&|l| Some(l.metrics.n_gaps))),
        ("n_outliers", i64v(&|l| Some(l.metrics.n_outliers))),
    ];
    let schema = Arc::new(arrow::datatypes::Schema::new(
        fields
            .iter()
            .map(|(n, a)| arrow::datatypes::Field::new(*n, a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    Ok(Some(RecordBatch::try_new(schema, fields.into_iter().map(|(_, a)| a).collect())?))
}

/// The columns of the `voyages` table, in order, for a leg that has ended.
/// Reads the registered `closed_core` (from [`closed_core_batch`]) and
/// `destination_daily` (at least the days from the earliest departure).
///
/// A vessel's declared destination is counted per day, the departure and arrival
/// days in full, and the most often reported wins, then the most recent.
pub fn finish_closed_sql() -> String {
    let dn = "regexp_replace(upper(dc.declared_destination), '[^A-Z0-9]', '')";
    let nn = "regexp_replace(upper(c.dest_port_name), '[^A-Z0-9]', '')";
    let un = "regexp_replace(upper(coalesce(c.dest_unlocode, '')), '[^A-Z0-9]', '')";
    format!(
        "
WITH dagg AS (
  SELECT c.voyage_id, d.destination AS dest, sum(d.n) AS n, max(d.last_ts) AS last_ts, max(d.eta) AS eta
  FROM closed_core c JOIN destination_daily d
    ON d.mmsi = c.mmsi
   AND d.ts >= date_trunc('day', c.depart_ts)
   AND d.ts < date_trunc('day', c.arrive_ts) + INTERVAL '1 day'
  GROUP BY c.voyage_id, d.destination
),
dr AS (
  SELECT dagg.*,
    CAST(row_number() OVER (PARTITION BY voyage_id ORDER BY n DESC, last_ts DESC, dest) AS INT) AS rk,
    count(*) OVER (PARTITION BY voyage_id) AS n_dest
  FROM dagg
),
decl AS (
  SELECT voyage_id,
    max(CASE WHEN rk = 1 THEN dest END) AS declared_destination,
    max(n_dest) AS n_declared,
    max(CASE WHEN rk = 1 THEN eta END) AS declared_eta
  FROM dr GROUP BY voyage_id
)
SELECT c.voyage_id, c.mmsi, c.depart_ts, c.arrive_ts,
  (CAST(c.arrive_ts AS BIGINT) - CAST(c.depart_ts AS BIGINT)) / 1000000.0 AS duration_s,
  c.origin_stop_id IS NOT NULL AS origin_known, true AS dest_known, false AS is_open,
  c.origin_stop_id, c.dest_stop_id,
  c.origin_lat, c.origin_lon, c.origin_port_id, c.origin_port_name, c.origin_unlocode,
  c.origin_country, c.origin_port_distance_nm,
  c.dest_lat, c.dest_lon, c.dest_port_id, c.dest_port_name, c.dest_unlocode,
  c.dest_country, c.dest_port_distance_nm,
  c.dist_nm_raw AS distance_nm_raw, c.dist_nm_clean AS distance_nm_clean,
  CASE WHEN c.arrive_ts > c.depart_ts
       THEN c.dist_nm_clean
            / ((CAST(c.arrive_ts AS BIGINT) - CAST(c.depart_ts AS BIGINT)) / 3600000000.0)
  END AS avg_speed_kn,
  c.max_sog_knots, c.n_points, c.n_gaps, c.n_outliers,
  dc.declared_destination, dc.n_declared AS n_declared_destinations, dc.declared_eta,
  CASE WHEN dc.declared_destination IS NULL OR c.dest_port_name IS NULL THEN NULL
       ELSE length({dn}) >= 4
            AND ({dn} = {un}
                 OR (length({nn}) >= 4 AND (strpos({dn}, {nn}) > 0 OR strpos({nn}, {dn}) > 0)))
  END AS declared_matches_dest,
  now() AS computed_at
FROM closed_core c LEFT JOIN decl dc ON c.voyage_id = dc.voyage_id
ORDER BY c.mmsi, c.depart_ts"
    )
}

/// The legs still under way, in the shape of the `voyages` table, from the
/// registered `leg_state_new`: vessels that have never stopped, or have been seen
/// since they left their last stop. They have no destination yet, so no declared
/// destination either.
pub fn open_voyages_sql() -> String {
    "SELECT concat(CAST(mmsi AS VARCHAR), '-', CAST(CAST(from_ts AS BIGINT) / 1000 AS VARCHAR)) AS voyage_id,
       mmsi, from_ts AS depart_ts, CAST(NULL AS TIMESTAMP) AS arrive_ts, CAST(NULL AS DOUBLE) AS duration_s,
       origin_stop_id IS NOT NULL AS origin_known, false AS dest_known, true AS is_open,
       origin_stop_id, CAST(NULL AS VARCHAR) AS dest_stop_id,
       origin_lat, origin_lon, origin_port_id, origin_port_name, origin_unlocode,
       origin_country, origin_port_distance_nm,
       CAST(NULL AS DOUBLE) AS dest_lat, CAST(NULL AS DOUBLE) AS dest_lon, CAST(NULL AS BIGINT) AS dest_port_id,
       CAST(NULL AS VARCHAR) AS dest_port_name, CAST(NULL AS VARCHAR) AS dest_unlocode,
       CAST(NULL AS VARCHAR) AS dest_country, CAST(NULL AS DOUBLE) AS dest_port_distance_nm,
       dist_nm_raw AS distance_nm_raw, dist_nm_clean AS distance_nm_clean,
       CAST(NULL AS DOUBLE) AS avg_speed_kn, max_sog_knots, n_points, n_gaps, n_outliers,
       CAST(NULL AS VARCHAR) AS declared_destination, CAST(NULL AS BIGINT) AS n_declared_destinations,
       CAST(NULL AS TIMESTAMP) AS declared_eta, CAST(NULL AS BOOLEAN) AS declared_matches_dest,
       now() AS computed_at
     FROM leg_state_new
     WHERE origin_stop_id IS NULL OR last_ts > from_ts
     ORDER BY mmsi"
        .to_string()
}

/// Registers `states` under `leg_state_new` and returns the open legs' rows.
pub async fn open_voyages(ctx: &SessionContext, states: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    if states.is_empty() {
        return Ok(Vec::new());
    }
    let _ = ctx.deregister_table("leg_state_new")?;
    ctx.register_table(
        "leg_state_new",
        Arc::new(MemTable::try_new(states[0].schema(), vec![states.to_vec()])?),
    )?;
    ctx.sql(&open_voyages_sql()).await?.collect().await.context("open voyages")
}

/// Attaches declared destinations to today's closed legs and returns rows in the
/// `voyages` table's shape. `destination_daily` must be registered.
pub async fn finish_closed(ctx: &SessionContext, closed: &[ClosedLeg]) -> Result<Vec<RecordBatch>> {
    let Some(core) = closed_core_batch(closed)? else {
        return Ok(Vec::new());
    };
    mem(ctx, "closed_core", core).await?;
    ctx.sql(&finish_closed_sql()).await.context("planning closed voyages")?.collect().await.context("closed voyages")
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: i64 = 3_600_000_000;
    const DAY: i64 = 24 * H;

    fn stop(id: &str, arrive: i64, depart: i64) -> StopEvt {
        StopEvt {
            origin: Origin { stop_id: Some(id.into()), port_name: Some(format!("port-{id}")), ..Default::default() },
            arrive_ts: arrive,
            depart_ts: depart,
        }
    }

    fn totals(first: i64, last: i64, pts: i64) -> DayTotals {
        DayTotals {
            first_stream_ts: first,
            last_stream_ts: last,
            metrics: Metrics { dist_raw: pts as f64, dist_clean: pts as f64, n_points: pts, ..Default::default() },
        }
    }

    /// Every point is one unit of distance and one point; `sum` counts the points
    /// of a day of one report an hour that fall in the interval.
    fn hourly(day_start: i64) -> impl FnMut(Interval) -> Metrics {
        move |(from, to)| {
            let n = (0..24)
                .map(|h| day_start + h * H)
                .filter(|t| *t > from && to.is_none_or(|u| *t <= u))
                .count() as i64;
            Metrics { dist_raw: n as f64, dist_clean: n as f64, n_points: n, ..Default::default() }
        }
    }

    fn leg(mmsi: u32, first: i64, last: i64, from: i64, origin: Origin, pts: i64) -> VesselLeg {
        VesselLeg {
            mmsi,
            first_ts: first,
            last_ts: last,
            from_ts: from,
            origin,
            metrics: Metrics { dist_raw: pts as f64, dist_clean: pts as f64, n_points: pts, ..Default::default() },
        }
    }

    #[test]
    fn a_new_vessel_starts_a_leg_at_its_first_row() {
        let d0 = 0;
        let (closed, s) = advance(1, None, &[], &totals(2 * H, 20 * H, 10), d0, &mut hourly(d0));
        assert!(closed.is_empty());
        assert_eq!(s.from_ts, 2 * H);
        assert!(s.origin.stop_id.is_none() && s.is_open());
        // Points after the first row: 3h..=23h.
        assert_eq!(s.metrics.n_points, 21);
    }

    #[test]
    fn a_vessel_underway_all_day_just_adds_the_days_totals() {
        let prev = leg(1, 0, DAY - H, 5 * H, Origin { stop_id: Some("A".into()), ..Default::default() }, 100);
        let (closed, s) = advance(1, Some(&prev), &[], &totals(DAY, 2 * DAY - H, 24), DAY, &mut |_| panic!("no row-level sums needed"));
        assert!(closed.is_empty());
        assert_eq!(s.metrics.n_points, 124);
        assert_eq!(s.last_ts, 2 * DAY - H);
        assert_eq!(s.from_ts, 5 * H, "the leg did not change");
    }

    #[test]
    fn reaching_a_new_stop_closes_the_leg_with_the_earlier_days_and_todays_part() {
        let a = Origin { stop_id: Some("A".into()), ..Default::default() };
        let prev = leg(1, 0, DAY - H, 5 * H, a.clone(), 100);
        // Arrives at B at 10:00 today and leaves at 20:00.
        let b = stop("B", DAY + 10 * H, DAY + 20 * H);
        let (closed, s) = advance(1, Some(&prev), &[b], &totals(DAY, 2 * DAY - H, 24), DAY, &mut hourly(DAY));
        assert_eq!(closed.len(), 1);
        let c = &closed[0];
        assert_eq!((c.depart_ts, c.arrive_ts), (5 * H, DAY + 10 * H));
        assert_eq!(c.origin, a);
        assert_eq!(c.dest.stop_id.as_deref(), Some("B"));
        // 100 from before, plus today's reports up to and including 10:00.
        assert_eq!(c.metrics.n_points, 100 + 11);
        // The next leg starts when it leaves B, and covers the reports after 20:00.
        assert_eq!(s.from_ts, DAY + 20 * H);
        assert_eq!(s.origin.stop_id.as_deref(), Some("B"));
        assert_eq!(s.metrics.n_points, 3);
    }

    #[test]
    fn two_stops_in_a_day_make_a_leg_between_them() {
        let prev = leg(1, 0, DAY - H, 5 * H, Origin { stop_id: Some("A".into()), ..Default::default() }, 0);
        let (b, c) = (stop("B", DAY + 2 * H, DAY + 4 * H), stop("C", DAY + 9 * H, DAY + 12 * H));
        let (closed, s) = advance(1, Some(&prev), &[b, c], &totals(DAY, DAY + 23 * H, 24), DAY, &mut hourly(DAY));
        assert_eq!(closed.len(), 2);
        assert_eq!(closed[1].origin.stop_id.as_deref(), Some("B"));
        assert_eq!(closed[1].dest.stop_id.as_deref(), Some("C"));
        assert_eq!(closed[1].metrics.n_points, 5, "the reports at 5h, 6h, 7h, 8h and 9h");
        assert_eq!(s.origin.stop_id.as_deref(), Some("C"));
    }

    #[test]
    fn a_stop_that_carries_on_into_today_starts_no_new_leg_and_moves_the_departure() {
        let a = Origin { stop_id: Some("A".into()), ..Default::default() };
        // Stopped at A since yesterday; its last piece ended at the end of yesterday.
        let prev = leg(1, 0, DAY - H, DAY - H, a, 0);
        let a_now = stop("A", DAY - 20 * H, DAY + 6 * H);
        let (closed, s) = advance(1, Some(&prev), &[a_now], &totals(DAY, DAY + 22 * H, 20), DAY, &mut hourly(DAY));
        assert!(closed.is_empty(), "no leg ends at a stop it was already at");
        assert_eq!(s.from_ts, DAY + 6 * H, "it left at 06:00");
        assert_eq!(s.metrics.n_points, 17, "reports after 06:00: 7h..=23h");
    }

    #[test]
    fn a_vessels_first_stop_at_its_first_row_has_no_leading_leg() {
        let first = DAY + 3 * H;
        let x = stop("A", first, DAY + 8 * H);
        let (closed, s) = advance(1, None, &[x], &totals(first, DAY + 20 * H, 10), DAY, &mut hourly(DAY));
        assert!(closed.is_empty());
        assert_eq!(s.origin.stop_id.as_deref(), Some("A"));
        assert_eq!(s.first_ts, first);
    }

    #[test]
    fn a_stop_after_some_sailing_gives_the_vessel_a_leading_leg() {
        let x = stop("A", DAY + 9 * H, DAY + 12 * H);
        let (closed, _) = advance(1, None, &[x], &totals(DAY + 2 * H, DAY + 20 * H, 10), DAY, &mut hourly(DAY));
        assert_eq!(closed.len(), 1);
        assert!(closed[0].origin.stop_id.is_none(), "origin unknown");
        assert_eq!((closed[0].depart_ts, closed[0].arrive_ts), (DAY + 2 * H, DAY + 9 * H));
        assert_eq!(closed[0].metrics.n_points, 7, "reports after 02:00 up to 09:00");
    }

    #[test]
    fn a_vessel_still_in_its_stop_has_no_open_leg() {
        let a = Origin { stop_id: Some("A".into()), ..Default::default() };
        // Reported nothing after leaving A at 05:00.
        let s = leg(1, 0, 5 * H, 5 * H, a.clone(), 0);
        assert!(!s.is_open());
        assert!(leg(1, 0, 9 * H, 5 * H, a, 0).is_open(), "seen after it left");
        assert!(leg(1, 0, 9 * H, 0, Origin::default(), 0).is_open(), "never stopped");
    }

    #[test]
    fn metrics_add_and_keep_the_larger_speed() {
        let a = Metrics { dist_raw: 1.0, max_sog: Some(5.0), n_points: 2, ..Default::default() };
        let b = Metrics { dist_raw: 2.0, max_sog: None, n_points: 3, n_gaps: 1, ..Default::default() };
        let c = a.plus(&b);
        assert_eq!((c.dist_raw, c.max_sog, c.n_points, c.n_gaps), (3.0, Some(5.0), 5, 1));
        assert_eq!(Metrics::default().plus(&Metrics { max_sog: Some(7.0), ..Default::default() }).max_sog, Some(7.0));
    }

    #[test]
    fn state_rows_round_trip() {
        let s = VesselLeg {
            mmsi: 366_000_001,
            first_ts: 1,
            last_ts: 9,
            from_ts: 5,
            origin: Origin {
                stop_id: Some("366000001-5".into()),
                lat: Some(10.0),
                lon: Some(20.0),
                port_id: Some(3),
                port_name: Some("Alpha".into()),
                unlocode: None,
                country: Some("Aland".into()),
                port_distance_nm: Some(0.5),
            },
            metrics: Metrics { dist_raw: 3.5, dist_clean: 3.0, max_sog: Some(12.0), n_points: 7, n_gaps: 1, n_outliers: 2 },
        };
        let none = leg(2, 4, 4, 4, Origin::default(), 0);
        let b = state_batch(&[s.clone(), none.clone()], 20_500).unwrap().unwrap();
        crate::carry::check_batches(&voyage_state_schema(), std::slice::from_ref(&b)).unwrap();
        assert_eq!(states_from_batches(&[b]).unwrap(), vec![s, none]);
    }
}
