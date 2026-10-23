//! Source availability: when each upstream a collector reads from was up,
//! down or degraded, recorded as closed intervals in the Iceberg table
//! `<namespace>.availability` (namespace `sources` by default, in the same
//! warehouse as the binary's other tables).
//!
//! Two shapes of producer share one table:
//! - streaming collectors install a process-wide tracker with
//!   [`init_stream`]; `run_ingest` then reports connects, stream ends,
//!   reconnects and silences through the free functions below, so a
//!   collector needs no tracking code of its own. Open intervals are split
//!   at each checkpoint so the table is never far behind and a hard kill
//!   loses at most one checkpoint's worth.
//! - batch loaders write one `kind = run` row per run with [`record_run`],
//!   classifying a failure with [`classify_failure`].
//!
//! Rows are buffered and committed at most once per flush interval (plus at
//! shutdown): every commit adds a manifest, and RustFS refuses commits past
//! 10,000 manifests. Recording never fails ingest — a failed write keeps its
//! rows for the next flush and logs a warning.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use clap::Args;
use iceberg::spec::{NestedField, PartitionSpecBuilder, PrimitiveType, Schema};
use iceberg::{Catalog, TableIdent};

use crate::iceberg::{commit_batches, ensure_namespace, ensure_table, open_catalog, IcebergCliArgs, IcebergConfig};
use crate::log;

pub const DEFAULT_NAMESPACE: &str = "sources";
pub const TABLE: &str = "availability";

/// Rows kept in memory while writes keep failing; the oldest go first.
const MAX_PENDING: usize = 10_000;
/// How long a final write at shutdown or exit may take.
const FINAL_WRITE_TIMEOUT: Duration = Duration::from_secs(15);

/// CLI args for availability recording. On by default whenever the binary
/// writes to Iceberg.
#[derive(Clone, Debug, Args)]
pub struct AvailabilityCliArgs {
    /// Record when each upstream source is up, down or degraded into
    /// `<availability-namespace>.availability`. On whenever Iceberg output is
    /// configured; `--availability false` turns it off.
    #[arg(
        long = "availability",
        env = "COLLECT_AVAILABILITY",
        default_value_t = true,
        action = clap::ArgAction::Set,
        value_parser = clap::builder::BoolishValueParser::new(),
        value_name = "BOOL"
    )]
    pub availability: bool,

    /// Iceberg namespace for the availability table (same warehouse).
    #[arg(long, env = "AVAILABILITY_NAMESPACE", default_value = DEFAULT_NAMESPACE)]
    pub availability_namespace: String,

    /// Commit buffered availability rows at most this often, in seconds.
    #[arg(long, env = "AVAILABILITY_FLUSH_SECONDS", default_value_t = 300,
          value_parser = clap::value_parser!(u64).range(1..))]
    pub availability_flush_seconds: u64,

    /// Close and reopen a long-running interval this often, in seconds, so
    /// the table is never further behind than this.
    #[arg(long, env = "AVAILABILITY_CHECKPOINT_SECONDS", default_value_t = 3600,
          value_parser = clap::value_parser!(u64).range(1..))]
    pub availability_checkpoint_seconds: u64,

    /// Mark a connected stream `degraded` after this many seconds without a
    /// row (0 = never).
    #[arg(long, env = "AVAILABILITY_QUIET_SECONDS", default_value_t = 300)]
    pub availability_quiet_seconds: u64,
}

impl Default for AvailabilityCliArgs {
    fn default() -> Self {
        AvailabilityCliArgs {
            availability: true,
            availability_namespace: DEFAULT_NAMESPACE.to_string(),
            availability_flush_seconds: 300,
            availability_checkpoint_seconds: 3600,
            availability_quiet_seconds: 300,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Up,
    Down,
    Degraded,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Up => "up",
            State::Down => "down",
            State::Degraded => "degraded",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Stream,
    Run,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Stream => "stream",
            Kind::Run => "run",
        }
    }
}

/// Why an interval ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndReason {
    /// The source changed state; the next interval starts here.
    StateChange,
    /// Split while still open; the next interval continues the same state.
    Checkpoint,
    /// The collector stopped normally.
    Shutdown,
    /// The collector exited on an error (gave up reconnecting, drought, ...).
    Exit,
    /// A batch run finished.
    RunEnd,
}

impl EndReason {
    pub fn as_str(self) -> &'static str {
        match self {
            EndReason::StateChange => "state_change",
            EndReason::Checkpoint => "checkpoint",
            EndReason::Shutdown => "shutdown",
            EndReason::Exit => "exit",
            EndReason::RunEnd => "run_end",
        }
    }
}

/// Who is reporting on which source.
#[derive(Clone, Debug)]
pub struct Identity {
    /// The upstream itself, e.g. `tcp://10.0.0.5:5631` or a download URL.
    pub source_id: String,
    /// The logical source label (`--source`, an evidence source code).
    pub source_label: String,
    pub binary: String,
    pub host: String,
    /// Random per process start, so restarts are told apart.
    pub instance_id: String,
}

impl Identity {
    pub fn new(binary: &str, source_id: &str, source_label: &str) -> Self {
        Identity {
            source_id: source_id.to_string(),
            source_label: source_label.to_string(),
            binary: binary.to_string(),
            host: hostname(),
            instance_id: format!("{:016x}", rand::random::<u64>()),
        }
    }
}

/// One row of the availability table.
#[derive(Clone, Debug, PartialEq)]
pub struct Interval {
    pub kind: Kind,
    pub state: State,
    pub started_ms: i64,
    pub ended_ms: i64,
    pub rows: Option<i64>,
    pub reason: Option<String>,
    pub end_reason: EndReason,
}

#[derive(Clone, Debug)]
struct Open {
    state: State,
    started_ms: i64,
    rows: i64,
    reason: Option<String>,
}

/// The interval state machine for one streaming source, free of I/O and
/// clocks so it can be tested directly: every call takes `now` in Unix ms.
#[derive(Debug)]
pub struct Recorder {
    checkpoint_ms: i64,
    quiet_ms: i64,
    open: Option<Open>,
    pending: Vec<Interval>,
    last_total_rows: u64,
    last_row_ms: i64,
}

impl Recorder {
    /// `quiet_ms == 0` disables the silence check.
    pub fn new(checkpoint_ms: i64, quiet_ms: i64) -> Self {
        Recorder {
            checkpoint_ms: checkpoint_ms.max(1),
            quiet_ms,
            open: None,
            pending: Vec::new(),
            last_total_rows: 0,
            last_row_ms: 0,
        }
    }

    pub fn state(&self) -> Option<State> {
        self.open.as_ref().map(|o| o.state)
    }

    /// Enter `state` at `now`. Re-entering the current state keeps the
    /// interval open (and its first reason).
    pub fn set_state(&mut self, now: i64, state: State, reason: Option<String>) {
        if let Some(open) = &mut self.open {
            if open.state == state {
                if open.reason.is_none() {
                    open.reason = reason;
                }
                return;
            }
        }
        self.close_at(now, EndReason::StateChange);
        self.open = Some(Open { state, started_ms: now, rows: 0, reason });
    }

    /// Report ingest progress: `total_rows` is the running row count and
    /// `last_row_ms` when the latest row arrived. Rows mean the source is
    /// up; a long enough silence on an up source makes it degraded, from
    /// when the silence began.
    pub fn observe(&mut self, now: i64, total_rows: u64, last_row_ms: i64) {
        let delta = total_rows.saturating_sub(self.last_total_rows);
        self.last_total_rows = total_rows.max(self.last_total_rows);
        if delta > 0 {
            self.last_row_ms = last_row_ms.min(now);
            if self.state() != Some(State::Up) {
                let at = self.last_row_ms.max(self.open.as_ref().map_or(i64::MIN, |o| o.started_ms));
                self.set_state(at, State::Up, None);
            }
            if let Some(open) = &mut self.open {
                open.rows += delta as i64;
            }
        }
        if self.quiet_ms > 0 {
            if let Some(open) = &self.open {
                let silent_since = self.last_row_ms.max(open.started_ms);
                if open.state == State::Up && now - silent_since >= self.quiet_ms {
                    let reason = format!("no rows for {}s", self.quiet_ms / 1000);
                    self.set_state(silent_since, State::Degraded, Some(reason));
                }
            }
        }
    }

    /// Split the open interval if it has run a whole checkpoint period.
    pub fn checkpoint(&mut self, now: i64) {
        let Some(open) = &self.open else { return };
        if now - open.started_ms < self.checkpoint_ms {
            return;
        }
        let (state, reason) = (open.state, open.reason.clone());
        self.close_at(now, EndReason::Checkpoint);
        self.open = Some(Open { state, started_ms: now, rows: 0, reason });
    }

    /// Close the open interval for good.
    pub fn close(&mut self, now: i64, end_reason: EndReason, reason: Option<String>) {
        if let (Some(open), Some(reason)) = (&mut self.open, reason) {
            if open.state != State::Up && open.reason.is_none() {
                open.reason = Some(reason);
            }
        }
        self.close_at(now, end_reason);
    }

    fn close_at(&mut self, now: i64, end_reason: EndReason) {
        let Some(open) = self.open.take() else { return };
        self.pending.push(Interval {
            kind: Kind::Stream,
            state: open.state,
            started_ms: open.started_ms,
            ended_ms: now.max(open.started_ms),
            rows: Some(open.rows),
            reason: open.reason,
            end_reason,
        });
    }

    pub fn take_pending(&mut self) -> Vec<Interval> {
        std::mem::take(&mut self.pending)
    }

    /// Put back rows a write failed on, ahead of newer ones, keeping at
    /// most [`MAX_PENDING`].
    pub fn requeue(&mut self, mut rows: Vec<Interval>) {
        rows.append(&mut self.pending);
        let excess = rows.len().saturating_sub(MAX_PENDING);
        rows.drain(..excess);
        self.pending = rows;
    }
}

/// Where rows are committed.
struct Sink {
    catalog: Arc<dyn Catalog>,
    ident: TableIdent,
    label: String,
    /// Serializes commits from the flusher and the final write.
    write_lock: tokio::sync::Mutex<()>,
}

impl Sink {
    async fn connect(iceberg: &IcebergCliArgs, args: &AvailabilityCliArgs) -> Result<Sink> {
        let mut config = IcebergConfig::from(iceberg);
        config.namespace = args.availability_namespace.clone();
        config.table_prefix = None;
        let catalog = open_catalog(&config).await?;
        ensure_namespace(&catalog, &config).await?;
        let table = ensure_table(
            &catalog,
            &config,
            TABLE,
            schema(),
            PartitionSpecBuilder::new(schema()),
        )
        .await?;
        Ok(Sink {
            catalog: Arc::new(catalog),
            ident: table.identifier().clone(),
            label: format!("{}.{TABLE}", config.namespace),
            write_lock: tokio::sync::Mutex::new(()),
        })
    }

    async fn write(&self, identity: &Identity, rows: &[Interval]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let _guard = self.write_lock.lock().await;
        let batch = to_batch(identity, rows, now_ms())?;
        // Reload so the commit builds on the current snapshot.
        let table = self.catalog.load_table(&self.ident).await.context("loading availability table")?;
        commit_batches(self.catalog.as_ref(), &table, vec![batch], crate::DEFAULT_COMPRESSION_LEVEL, &self.label).await
    }
}

/// The process-wide tracker a streaming collector installs.
struct Tracker {
    identity: Identity,
    recorder: Mutex<Recorder>,
    sink: Sink,
    finished: std::sync::atomic::AtomicBool,
}

static TRACKER: OnceLock<Arc<Tracker>> = OnceLock::new();

impl Tracker {
    fn with<T>(&self, f: impl FnOnce(&mut Recorder) -> T) -> T {
        let mut guard = self.recorder.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut guard)
    }

    async fn flush(&self) {
        let rows = self.with(|r| r.take_pending());
        if rows.is_empty() {
            return;
        }
        if let Err(error) = self.sink.write(&self.identity, &rows).await {
            log::warn(
                "availability_write_failed",
                &format!("could not record source availability (kept for the next flush): {error:#}"),
                &[("source", &self.identity.source_id)],
            );
            self.with(|r| r.requeue(rows));
        }
    }

    async fn finish(&self, end_reason: EndReason, reason: Option<String>) {
        if self.finished.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        self.with(|r| r.close(now_ms(), end_reason, reason));
        if tokio::time::timeout(FINAL_WRITE_TIMEOUT, self.flush()).await.is_err() {
            log::warn(
                "availability_write_timeout",
                "timed out recording the final source availability interval",
                &[("source", &self.identity.source_id)],
            );
        }
    }
}

/// Install the tracker for a streaming collector's upstream, unless
/// availability is off or Iceberg output isn't configured. A catalog that
/// can't be reached only disables tracking; ingest carries on.
pub async fn init_stream(
    iceberg: &IcebergCliArgs,
    args: &AvailabilityCliArgs,
    binary: &str,
    source_id: &str,
    source_label: &str,
) {
    if !args.availability || !iceberg.is_iceberg_mode() || TRACKER.get().is_some() {
        return;
    }
    let sink = match Sink::connect(iceberg, args).await {
        Ok(sink) => sink,
        Err(error) => {
            log::warn(
                "availability_disabled",
                &format!("source availability will not be recorded: {error:#}"),
                &[("source", source_id)],
            );
            return;
        }
    };
    let tracker = Arc::new(Tracker {
        identity: Identity::new(binary, source_id, source_label),
        recorder: Mutex::new(Recorder::new(
            (args.availability_checkpoint_seconds * 1000) as i64,
            (args.availability_quiet_seconds * 1000) as i64,
        )),
        sink,
        finished: std::sync::atomic::AtomicBool::new(false),
    });
    if TRACKER.set(tracker.clone()).is_err() {
        return;
    }
    let every = Duration::from_secs(args.availability_flush_seconds);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            if tracker.finished.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            tracker.with(|r| r.checkpoint(now_ms()));
            tracker.flush().await;
        }
    });
}

fn tracker() -> Option<&'static Arc<Tracker>> {
    TRACKER.get().filter(|t| !t.finished.load(std::sync::atomic::Ordering::SeqCst))
}

/// The upstream is connected.
pub fn mark_up() {
    if let Some(t) = tracker() {
        t.with(|r| r.set_state(now_ms(), State::Up, None));
    }
}

/// The upstream is gone (a failed connect, a closed or broken stream).
pub fn mark_down(reason: &str) {
    if let Some(t) = tracker() {
        t.with(|r| r.set_state(now_ms(), State::Down, Some(reason.to_string())));
    }
}

/// Ingest progress, for the silence check (see [`Recorder::observe`]).
pub fn observe(total_rows: u64, last_row_ms: i64) {
    if let Some(t) = tracker() {
        t.with(|r| r.observe(now_ms(), total_rows, last_row_ms));
    }
}

/// Close the open interval and write everything buffered. Later calls do
/// nothing.
pub async fn finish(end_reason: EndReason, reason: Option<String>) {
    if let Some(t) = tracker() {
        t.finish(end_reason, reason).await;
    }
}

/// [`finish`] from synchronous code about to `process::exit`. Needs a
/// multi-threaded Tokio runtime (`#[tokio::main]`'s default); elsewhere the
/// interval is left for the visualiser to show as a gap.
pub fn finish_blocking(end_reason: EndReason, reason: Option<String>) {
    let Some(t) = tracker() else { return };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(t.finish(end_reason, reason)));
        }
        _ => log::warn(
            "availability_not_flushed",
            "exiting without recording the final source availability interval",
            &[("source", &t.identity.source_id)],
        ),
    }
}

/// A network fault reaching a source, for [`classify_failure`]: wrap an
/// error in this when the source answered but refused (an HTTP error status).
#[derive(Debug)]
pub struct SourceUnavailable(pub String);

impl std::fmt::Display for SourceUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SourceUnavailable {}

/// How a failed batch run reflects on its source: `down` when the source
/// couldn't be reached or refused (a `reqwest` error or
/// [`SourceUnavailable`] anywhere in the chain), otherwise `degraded` — it
/// answered, but what came back couldn't be loaded.
pub fn classify_failure(error: &anyhow::Error) -> State {
    let unreachable = error
        .chain()
        .any(|c| c.is::<reqwest::Error>() || c.is::<SourceUnavailable>());
    if unreachable {
        State::Down
    } else {
        State::Degraded
    }
}

/// One batch run against one source.
#[derive(Clone, Debug)]
pub struct Run {
    pub source_id: String,
    pub source_label: String,
    pub started_ms: i64,
}

impl Run {
    /// Start timing a run now.
    pub fn start(source_id: &str, source_label: &str) -> Self {
        Run { source_id: source_id.to_string(), source_label: source_label.to_string(), started_ms: now_ms() }
    }

    /// The row for this run ending now with `outcome`.
    pub fn interval<T>(&self, outcome: &Result<T>, rows: Option<i64>) -> Interval {
        let (state, reason) = match outcome {
            Ok(_) => (State::Up, None),
            Err(e) => (classify_failure(e), Some(format!("{e:#}"))),
        };
        Interval {
            kind: Kind::Run,
            state,
            started_ms: self.started_ms,
            ended_ms: now_ms().max(self.started_ms),
            rows,
            reason,
            end_reason: EndReason::RunEnd,
        }
    }
}

/// Record a finished batch run, unless availability is off or Iceberg isn't
/// configured. Failing to record is logged, never returned: the run's own
/// outcome is what the caller reports.
pub async fn record_run<T>(
    iceberg: &IcebergCliArgs,
    args: &AvailabilityCliArgs,
    binary: &str,
    run: &Run,
    outcome: &Result<T>,
    rows: Option<i64>,
) {
    if !args.availability || !iceberg.is_iceberg_mode() {
        return;
    }
    let interval = run.interval(outcome, rows);
    let identity = Identity::new(binary, &run.source_id, &run.source_label);
    let written = async {
        let sink = Sink::connect(iceberg, args).await?;
        sink.write(&identity, &[interval]).await
    };
    if let Err(error) = written.await {
        log::warn(
            "availability_write_failed",
            &format!("could not record source availability: {error:#}"),
            &[("source", &run.source_id)],
        );
    }
}

fn required(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::required(id, name, ty.into()))
}

fn optional(id: i32, name: &'static str, ty: PrimitiveType) -> Arc<NestedField> {
    Arc::new(NestedField::optional(id, name, ty.into()))
}

/// Schema of `sources.availability`, unpartitioned (it stays small).
pub fn schema() -> Schema {
    let fields = vec![
        required(1, "started_at", PrimitiveType::Timestamptz),
        required(2, "ended_at", PrimitiveType::Timestamptz),
        required(3, "duration_ms", PrimitiveType::Long),
        required(4, "source_id", PrimitiveType::String),
        required(5, "source_label", PrimitiveType::String),
        required(6, "state", PrimitiveType::String),
        required(7, "kind", PrimitiveType::String),
        optional(8, "reason", PrimitiveType::String),
        required(9, "end_reason", PrimitiveType::String),
        optional(10, "rows", PrimitiveType::Long),
        required(11, "binary", PrimitiveType::String),
        required(12, "host", PrimitiveType::String),
        required(13, "instance_id", PrimitiveType::String),
        required(14, "recorded_at", PrimitiveType::Timestamptz),
    ];
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building availability schema")
}

/// Arrow batch of `rows`, columns in [`schema`] order.
pub fn to_batch(identity: &Identity, rows: &[Interval], recorded_ms: i64) -> Result<RecordBatch> {
    let ts = || DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
    let fields = vec![
        Field::new("started_at", ts(), false),
        Field::new("ended_at", ts(), false),
        Field::new("duration_ms", DataType::Int64, false),
        Field::new("source_id", DataType::Utf8, false),
        Field::new("source_label", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("reason", DataType::Utf8, true),
        Field::new("end_reason", DataType::Utf8, false),
        Field::new("rows", DataType::Int64, true),
        Field::new("binary", DataType::Utf8, false),
        Field::new("host", DataType::Utf8, false),
        Field::new("instance_id", DataType::Utf8, false),
        Field::new("recorded_at", ts(), false),
    ];
    let n = rows.len();
    let timestamps = |f: fn(&Interval) -> i64| -> ArrayRef {
        Arc::new(TimestampMillisecondArray::from(rows.iter().map(f).collect::<Vec<_>>()).with_timezone("UTC"))
    };
    let repeat = |s: &str| -> ArrayRef { Arc::new(StringArray::from(vec![s; n])) };
    let columns: Vec<ArrayRef> = vec![
        timestamps(|r| r.started_ms),
        timestamps(|r| r.ended_ms),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.ended_ms - r.started_ms))),
        repeat(&identity.source_id),
        repeat(&identity.source_label),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.state.as_str()))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.kind.as_str()))),
        Arc::new(StringArray::from(rows.iter().map(|r| r.reason.as_deref()).collect::<Vec<_>>())),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.end_reason.as_str()))),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.rows).collect::<Vec<_>>())),
        repeat(&identity.binary),
        repeat(&identity.host),
        repeat(&identity.instance_id),
        Arc::new(TimestampMillisecondArray::from(vec![recorded_ms; n]).with_timezone("UTC")),
    ];
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).context("building availability batch")
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: the buffer is valid for its whole length.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    let name = if rc == 0 {
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        String::from_utf8_lossy(&buf[..end]).into_owned()
    } else {
        String::new()
    };
    if name.is_empty() {
        std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string())
    } else {
        name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: i64 = 1000;

    fn states(rows: &[Interval]) -> Vec<(State, i64, i64, EndReason)> {
        rows.iter().map(|r| (r.state, r.started_ms, r.ended_ms, r.end_reason)).collect()
    }

    #[test]
    fn connect_drop_reconnect_shutdown() {
        let mut r = Recorder::new(3600 * S, 0);
        r.set_state(0, State::Up, None);
        r.observe(10 * S, 5, 9 * S);
        r.set_state(20 * S, State::Down, Some("stream ended".into()));
        r.set_state(25 * S, State::Down, Some("reconnect failed".into()));
        r.set_state(30 * S, State::Up, None);
        r.close(40 * S, EndReason::Shutdown, None);
        let rows = r.take_pending();
        assert_eq!(
            states(&rows),
            vec![
                (State::Up, 0, 20 * S, EndReason::StateChange),
                (State::Down, 20 * S, 30 * S, EndReason::StateChange),
                (State::Up, 30 * S, 40 * S, EndReason::Shutdown),
            ]
        );
        assert_eq!(rows[0].rows, Some(5));
        // The first reason for a down interval is kept.
        assert_eq!(rows[1].reason.as_deref(), Some("stream ended"));
        assert!(r.take_pending().is_empty());
    }

    #[test]
    fn silence_degrades_from_the_last_row_and_rows_restore_up() {
        let mut r = Recorder::new(3600 * S, 60 * S);
        r.set_state(0, State::Up, None);
        r.observe(5 * S, 3, 5 * S);
        r.observe(64 * S, 3, 5 * S);
        assert_eq!(r.state(), Some(State::Up));
        r.observe(65 * S, 3, 5 * S);
        assert_eq!(r.state(), Some(State::Degraded));
        r.observe(100 * S, 4, 99 * S);
        assert_eq!(r.state(), Some(State::Up));
        let rows = r.take_pending();
        assert_eq!(
            states(&rows),
            vec![
                (State::Up, 0, 5 * S, EndReason::StateChange),
                (State::Degraded, 5 * S, 99 * S, EndReason::StateChange),
            ]
        );
        assert_eq!(rows[1].reason.as_deref(), Some("no rows for 60s"));
    }

    #[test]
    fn silence_counts_from_connect_before_the_first_row() {
        let mut r = Recorder::new(3600 * S, 60 * S);
        r.set_state(100 * S, State::Up, None);
        r.observe(150 * S, 0, 0);
        assert_eq!(r.state(), Some(State::Up));
        r.observe(160 * S, 0, 0);
        assert_eq!(r.state(), Some(State::Degraded));
        assert_eq!(states(&r.take_pending()), vec![(State::Up, 100 * S, 100 * S, EndReason::StateChange)]);
    }

    #[test]
    fn down_sources_never_degrade() {
        let mut r = Recorder::new(3600 * S, 10 * S);
        r.set_state(0, State::Down, Some("refused".into()));
        r.observe(100 * S, 0, 0);
        assert_eq!(r.state(), Some(State::Down));
    }

    #[test]
    fn checkpoint_splits_and_keeps_the_state() {
        let mut r = Recorder::new(60 * S, 0);
        r.set_state(0, State::Down, Some("refused".into()));
        r.checkpoint(59 * S);
        assert!(r.take_pending().is_empty());
        r.checkpoint(61 * S);
        r.close(70 * S, EndReason::Exit, Some("gave up".into()));
        let rows = r.take_pending();
        assert_eq!(
            states(&rows),
            vec![(State::Down, 0, 61 * S, EndReason::Checkpoint), (State::Down, 61 * S, 70 * S, EndReason::Exit)]
        );
        assert_eq!(rows[1].reason.as_deref(), Some("refused"));
    }

    #[test]
    fn requeue_keeps_order_and_bounds_memory() {
        let mut r = Recorder::new(S, 0);
        for i in 0..(MAX_PENDING as i64 + 5) {
            r.set_state(i * 10, if i % 2 == 0 { State::Up } else { State::Down }, None);
        }
        let failed = r.take_pending();
        r.set_state(i64::MAX / 2, State::Up, None);
        r.requeue(failed);
        let rows = r.take_pending();
        assert_eq!(rows.len(), MAX_PENDING);
        assert!(rows.windows(2).all(|w| w[0].started_ms < w[1].started_ms));
    }

    #[test]
    fn failures_split_into_down_and_degraded() {
        let refused = anyhow::Error::new(SourceUnavailable("HTTP 503".into())).context("fetching x");
        assert_eq!(classify_failure(&refused), State::Down);
        let parse = anyhow::anyhow!("bad header").context("reading x");
        assert_eq!(classify_failure(&parse), State::Degraded);
        let run = Run::start("https://example.org/", "example");
        let ok: Result<()> = Ok(());
        let row = run.interval(&ok, Some(7));
        assert_eq!((row.state, row.kind, row.rows, row.reason), (State::Up, Kind::Run, Some(7), None));
        let row = run.interval::<()>(&Err(refused), None);
        assert_eq!(row.state, State::Down);
        assert_eq!(row.reason.as_deref(), Some("fetching x: HTTP 503"));
    }

    #[test]
    fn batch_matches_the_iceberg_schema() {
        let id = Identity::new("collect-socket", "tcp://h:1", "tcp");
        let rows = vec![Interval {
            kind: Kind::Stream,
            state: State::Up,
            started_ms: 1_000,
            ended_ms: 4_000,
            rows: Some(3),
            reason: None,
            end_reason: EndReason::Shutdown,
        }];
        let batch = to_batch(&id, &rows, 5_000).unwrap();
        let arrow = iceberg::arrow::schema_to_arrow_schema(&schema()).unwrap();
        assert_eq!(batch.num_columns(), arrow.fields().len());
        for (a, b) in batch.schema().fields().iter().zip(arrow.fields()) {
            assert_eq!(a.name(), b.name());
            assert_eq!(a.is_nullable(), b.is_nullable());
        }
        let dur = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(dur.value(0), 3_000);
    }

    #[test]
    fn flag_parses_false_and_defaults_on() {
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            a: AvailabilityCliArgs,
        }
        use clap::Parser;
        assert!(Cli::try_parse_from(["x"]).unwrap().a.availability);
        assert!(!Cli::try_parse_from(["x", "--availability", "false"]).unwrap().a.availability);
        let cli = Cli::try_parse_from(["x", "--availability-namespace", "ops"]).unwrap();
        assert_eq!(cli.a.availability_namespace, "ops");
    }
}
