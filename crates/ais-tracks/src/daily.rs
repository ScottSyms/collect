//! The daily steps, `track-points`, `tracks` and `stop-segments`, each choosing
//! its days, skipping the ones already built from the same input, building the
//! rest, and only then logging them as done.
//!
//! Every step follows the same order for a day, so a crash at any point leaves
//! something a rerun repairs: write the data files, commit the day's partition
//! (a replace, so rebuilding is idempotent), write the day's `vessel_state`,
//! and last append the `build_log` row. A day counts as built only once its row
//! exists. See [`crate::state`] for what the rows record.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use collect_core::iceberg::{
    commit_batches, ensure_namespace, ensure_table, table_ident, IcebergConfig, TABLE_POSITIONS,
};
use collect_maint::commit::RestClient;
use collect_maint::rewrite::live_files;
use datafusion::prelude::SessionContext;
use iceberg::spec::PartitionSpecBuilder;
use iceberg::Catalog;
use iceberg_datafusion::IcebergStaticTableProvider;

use crate::carry;
use crate::output::{commit_day, ensure_day_table, write_day_shard, DayWriter};
use crate::reduce::StreamState;
use crate::reduce_day::{self, ReduceOptions};
use crate::source;
use crate::state::{
    build_log_schema, date_to_day, days_from_files, downstream_token, log_batch,
    read_states_before, scan_all, select_days, states_batch, track_points_token,
    vessel_state_schema, DaySelect, Log, LogRow, STEP_STOP_SEGMENTS, STEP_TRACKS,
    STEP_TRACK_POINTS, TABLE_BUILD_LOG, TABLE_VESSEL_STATE,
};
use crate::stops::{self, StopParams, TABLE_STOP_SEGMENTS};
use crate::track_points::TABLE_TRACK_POINTS;
use crate::tracks::{self, TABLE_TRACKS};

/// Where a run reads and writes. `rest` is present only when writing.
pub struct Env<'a, C: Catalog> {
    pub catalog: &'a C,
    pub input: &'a IcebergConfig,
    pub output: &'a IcebergConfig,
    pub rest: Option<&'a RestClient>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Summary {
    pub built: usize,
    pub skipped: usize,
    pub empty: usize,
}

impl std::ops::AddAssign for Summary {
    fn add_assign(&mut self, o: Self) {
        self.built += o.built;
        self.skipped += o.skipped;
        self.empty += o.empty;
    }
}

/// Registers an Iceberg table with DataFusion under its base name.
pub async fn register(
    ctx: &SessionContext,
    catalog: &impl Catalog,
    config: &IcebergConfig,
    base: &str,
) -> Result<()> {
    let ident = table_ident(config, base);
    let table = catalog
        .load_table(&ident)
        .await
        .with_context(|| format!("loading table {ident}"))?;
    let provider = IcebergStaticTableProvider::try_new_from_table(table).await?;
    ctx.register_table(base, Arc::new(provider))?;
    Ok(())
}

fn now_us() -> i64 {
    Utc::now().timestamp_micros()
}

fn start_of(day: NaiveDate) -> DateTime<Utc> {
    Utc.from_utc_datetime(&day.and_hms_opt(0, 0, 0).expect("midnight"))
}

async fn load_log<C: Catalog>(env: &Env<'_, C>) -> Result<Log> {
    let ident = table_ident(env.output, TABLE_BUILD_LOG);
    if !env.catalog.table_exists(&ident).await? {
        return Ok(Log::default());
    }
    let table = env.catalog.load_table(&ident).await?;
    Log::from_batches(&scan_all(&table, None).await?)
}

/// Appends a log row. Called only after the day's data is committed.
async fn append_log<C: Catalog>(env: &Env<'_, C>, row: &LogRow) -> Result<()> {
    ensure_namespace(env.catalog, env.output).await?;
    let schema = build_log_schema();
    let table = ensure_table(
        env.catalog,
        env.output,
        TABLE_BUILD_LOG,
        schema.clone(),
        PartitionSpecBuilder::new(schema),
    )
    .await?;
    commit_batches(env.catalog, &table, vec![log_batch(std::slice::from_ref(row))?], 3, TABLE_BUILD_LOG)
        .await
}

fn log_row(step: &str, day: NaiveDate, token: String, rows: usize) -> LogRow {
    LogRow {
        step: step.to_string(),
        day,
        input_token: token,
        output_rows: rows as i64,
        built_at_us: now_us(),
    }
}

/// Writes the vessel states as of the end of `day`.
async fn write_vessel_state<C: Catalog>(
    env: &Env<'_, C>,
    rest: &RestClient,
    day: NaiveDate,
    states: &HashMap<u32, StreamState>,
) -> Result<()> {
    let start_us = start_of(day).timestamp_micros();
    let Some(batch) = states_batch(start_us, states)? else {
        return Ok(());
    };
    ensure_day_table(env.catalog, env.output, TABLE_VESSEL_STATE, vessel_state_schema()).await?;
    let table = env
        .catalog
        .load_table(&table_ident(env.output, TABLE_VESSEL_STATE))
        .await?;
    let days = date_to_day(day);
    let mut w = DayWriter::new(&table, days, &["mmsi"]).await?;
    w.write(&batch).await?;
    let rows = w.rows;
    let files = w.finish().await?;
    commit_day(env.catalog, rest, env.output, TABLE_VESSEL_STATE, days, files, rows).await?;
    Ok(())
}

/// The vessel states in force at the start of `day`: from `vessel_state`, or,
/// for a table built before that existed, from the previous day's output.
async fn start_states<C: Catalog>(
    env: &Env<'_, C>,
    day: NaiveDate,
    lookback_us: i64,
) -> Result<HashMap<u32, StreamState>> {
    let start_us = start_of(day).timestamp_micros();
    let vs = table_ident(env.output, TABLE_VESSEL_STATE);
    if env.catalog.table_exists(&vs).await? {
        let t = env.catalog.load_table(&vs).await?;
        let states = read_states_before(&t, start_us, lookback_us).await?;
        if !states.is_empty() {
            return Ok(states);
        }
    }
    let tp = table_ident(env.output, TABLE_TRACK_POINTS);
    if env.catalog.table_exists(&tp).await? {
        let t = env.catalog.load_table(&tp).await?;
        return source::thin_states(&t, start_us - lookback_us, start_us).await;
    }
    Ok(HashMap::new())
}

// ---- track-points -----------------------------------------------------------------

pub struct TrackPointsRun<'a> {
    pub select: &'a DaySelect,
    pub opts: &'a ReduceOptions,
    pub lookback_days: i64,
    /// Report which days would be built and why, without reducing anything.
    pub plan_only: bool,
}

pub async fn run_track_points<C: Catalog>(env: &Env<'_, C>, run: &TrackPointsRun<'_>) -> Result<Summary> {
    let positions = env
        .catalog
        .load_table(&table_ident(env.input, TABLE_POSITIONS))
        .await
        .context("loading the silver positions table")?;
    let silver = days_from_files(&live_files(&positions).await?);
    let candidates: Vec<NaiveDate> = silver.keys().copied().collect();
    let today = Utc::now().date_naive();
    let (days, force) = select_days(run.select, &candidates, today)?;
    let mut log = load_log(env).await?;
    if let Some(rest) = env.rest {
        ensure_day_table(
            env.catalog,
            env.output,
            TABLE_TRACK_POINTS,
            crate::reduce::thin_points_schema(),
        )
        .await?;
        let _ = rest;
    }
    let lookback_us = run.lookback_days * 86_400_000_000;
    let mut sum = Summary::default();

    // Where each vessel's stream left off: carried from the day just built, else
    // read back.
    let mut states: HashMap<u32, StreamState> = HashMap::new();
    let mut states_after: Option<NaiveDate> = None;
    let mut rebuilt_upstream = false;

    for day in days {
        if day >= today {
            eprintln!("note: {day} is not over yet; it will be rebuilt as data arrives");
        }
        let n_silver = silver.get(&day).copied().unwrap_or(0);
        if n_silver == 0 {
            println!("{day}: no positions; leaving the partition untouched");
            states_after = None;
            sum.empty += 1;
            continue;
        }
        let (start_us, end_us) = source::day_bounds_us(day);
        if states_after.and_then(|d| d.succ_opt()) != Some(day) {
            states = start_states(env, day, lookback_us).await?;
        }
        let token = track_points_token(n_silver, &states);
        if !force && log.is_current(STEP_TRACK_POINTS, day, &token) {
            println!("{day}: up to date");
            states_after = None;
            sum.skipped += 1;
            continue;
        }
        if run.plan_only {
            let why = match log.get(STEP_TRACK_POINTS, day) {
                None => "never built",
                Some(_) if rebuilt_upstream => "follows a rebuilt day",
                Some(_) => "input changed",
            };
            println!("{day}: would build ({why})");
            rebuilt_upstream = true;
            states_after = None;
            sum.built += 1;
            continue;
        }

        let label = reduce_day::scratch_label(day);
        let started = std::time::Instant::now();
        let (stream, est) = source::iceberg_day(&positions, start_us, end_us).await?;
        let manifest = reduce_day::route_day(stream, est, start_us, end_us, &label, run.opts).await?;
        let route_time = started.elapsed();

        // Reduce on a blocking thread and stream the batches to the writer
        // through a small channel, so memory stays bounded.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);
        let (opts2, prev) = (run.opts.clone(), states.clone());
        let handle = tokio::task::spawn_blocking(move || {
            let mut sink = |b| {
                tx.blocking_send(b)
                    .map_err(|_| anyhow::anyhow!("the writer stopped"))
            };
            let r = reduce_day::reduce_buckets(&manifest, &opts2, &prev, &mut sink);
            (r, manifest)
        });
        let days_idx = date_to_day(day);
        let mut writer = if env.rest.is_some() {
            let t = env
                .catalog
                .load_table(&table_ident(env.output, TABLE_TRACK_POINTS))
                .await?;
            Some(DayWriter::new(&t, days_idx, &["mmsi"]).await?)
        } else {
            None
        };
        while let Some(batch) = rx.recv().await {
            if let Some(w) = writer.as_mut() {
                w.write(&batch).await?;
            }
        }
        let (result, manifest) = handle.await?;
        manifest.cleanup();
        let (mut stats, next) = result?;
        stats.route_time = route_time;
        println!(
            "{day}: {} reports -> {} rows ({:.1}%), {} vessels; {} duplicates collapsed, {} set aside; {:.0}s, peak memory {:.0} MB",
            stats.routed,
            stats.kept,
            100.0 * stats.retention(),
            stats.vessels,
            stats.collapsed_dups,
            stats.quarantined,
            started.elapsed().as_secs_f64(),
            reduce_day::peak_rss_bytes() as f64 / 1e6,
        );
        if stats.unaccounted > 0 {
            println!(
                "  {} reports belong to vessels with no positioned report that day and are not represented",
                stats.unaccounted
            );
        }
        let kept_rows = stats.kept as usize;
        reduce_day::merge_states(&mut states, next, end_us - lookback_us);

        if let (Some(w), Some(rest)) = (writer, env.rest) {
            let rows = w.rows;
            let files = w.finish().await?;
            let r = commit_day(env.catalog, rest, env.output, TABLE_TRACK_POINTS, days_idx, files, rows)
                .await?;
            println!(
                "  {}.{TABLE_TRACK_POINTS}: {} files written, {} replaced",
                env.output.namespace, r.files_added, r.files_removed
            );
            write_vessel_state(env, rest, day, &states).await?;
            let row = log_row(STEP_TRACK_POINTS, day, token, kept_rows);
            append_log(env, &row).await?;
            log.insert(row);
        }
        rebuilt_upstream = true;
        states_after = Some(day);
        sum.built += 1;
    }
    Ok(sum)
}

// ---- tracks -----------------------------------------------------------------------

pub struct TracksRun<'a> {
    pub select: &'a DaySelect,
    pub shards: u32,
    pub plan_only: bool,
}

pub async fn run_tracks<C: Catalog>(env: &Env<'_, C>, run: &TracksRun<'_>) -> Result<Summary> {
    anyhow::ensure!(run.shards >= 1, "--shards must be at least 1");
    let mut log = load_log(env).await?;
    let candidates = log.days(STEP_TRACK_POINTS);
    let (days, force) = select_days(run.select, &candidates, Utc::now().date_naive())?;

    let ctx = SessionContext::new();
    register(&ctx, env.catalog, env.output, TABLE_TRACK_POINTS)
        .await
        .context("run track-points first")?;
    if env.rest.is_some() {
        ensure_day_table(env.catalog, env.output, TABLE_TRACKS, tracks::tracks_schema()).await?;
    }
    let tracks_ident = table_ident(env.output, TABLE_TRACKS);
    let mut sum = Summary::default();
    let mut prev_day_registered: Option<NaiveDate> = None;

    for day in days {
        let Some(up) = log.get(STEP_TRACK_POINTS, day).cloned() else {
            println!("{day}: no track_points; run track-points first");
            prev_day_registered = None;
            sum.empty += 1;
            continue;
        };
        let prev_same = day.pred_opt().and_then(|p| log.get(STEP_TRACKS, p)).cloned();
        let token = downstream_token(&up, prev_same.as_ref());
        if !force && log.is_current(STEP_TRACKS, day, &token) {
            println!("{day}: up to date");
            prev_day_registered = None;
            sum.skipped += 1;
            continue;
        }
        if run.plan_only {
            println!("{day}: would build");
            prev_day_registered = None;
            sum.built += 1;
            continue;
        }
        let day_start = start_of(day);
        let days_idx = date_to_day(day);

        // What the previous day ended with: this run's own output when it just
        // built that day, else whatever the table holds.
        if prev_day_registered.and_then(|d| d.succ_opt()) == Some(day) {
            tracks::set_previous(&ctx, Some(("prev_day", None))).await?;
        } else if env.catalog.table_exists(&tracks_ident).await? {
            let _ = ctx.deregister_table(TABLE_TRACKS)?;
            register(&ctx, env.catalog, env.output, TABLE_TRACKS).await?;
            tracks::set_previous(
                &ctx,
                Some((TABLE_TRACKS, Some((day_start - Duration::days(1), day_start)))),
            )
            .await?;
        } else {
            tracks::set_previous(&ctx, None).await?;
        }

        let table = if env.rest.is_some() {
            Some(env.catalog.load_table(&tracks_ident).await?)
        } else {
            None
        };
        let mut all = Vec::new();
        let mut files = Vec::new();
        for shard in 0..run.shards {
            let batches = tracks::build_shard(&ctx, day_start, run.shards, shard).await?;
            tracks::check_against_schema(&batches)?;
            if let Some(t) = &table {
                files.extend(write_day_shard(t, days_idx, &batches, &["mmsi", "track_id"]).await?);
            }
            all.extend(batches);
        }
        let pieces: usize = all.iter().map(|b| b.num_rows()).sum();
        prev_day_registered = if carry::register_output(&ctx, "prev_day", &all)? {
            Some(day)
        } else {
            None
        };
        if pieces == 0 {
            println!("{day}: no track segments; leaving the partition untouched");
            sum.empty += 1;
            continue;
        }

        // Every report the day's rows stand for should be in some segment.
        let (from, to) = (
            day_start.format("%Y-%m-%dT%H:%M:%S+00:00"),
            (day_start + Duration::days(1)).format("%Y-%m-%dT%H:%M:%S+00:00"),
        );
        let in_day = ctx
            .sql(&format!(
                "SELECT sum(n_raw) FROM track_points WHERE ts >= '{from}' AND ts < '{to}'"
            ))
            .await?
            .collect()
            .await?;
        let points = in_day
            .first()
            .and_then(|b| {
                arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Int64)
                    .ok()
                    .and_then(|c| {
                        c.as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .map(|a| a.value(0) as usize)
                    })
            })
            .unwrap_or(0);
        let covered = tracks::sum_int(&all, "n_rows")?;
        println!(
            "{day}: {pieces} segments covering {covered} of {points} reports; {} continue the previous day, {} chains broken",
            count_true(&all, "continues_previous"),
            count_true(&all, "chain_broken"),
        );
        if points > covered {
            println!(
                "  {} reports sit in stretches with no positioned point and belong to no segment",
                points - covered
            );
        }
        if let Some(rest) = env.rest {
            let r = commit_day(env.catalog, rest, env.output, TABLE_TRACKS, days_idx, files, pieces).await?;
            println!(
                "  {}.{TABLE_TRACKS}: {} files written, {} replaced",
                env.output.namespace, r.files_added, r.files_removed
            );
            let row = log_row(STEP_TRACKS, day, token, pieces);
            append_log(env, &row).await?;
            log.insert(row);
        }
        sum.built += 1;
    }
    Ok(sum)
}

// ---- stop-segments ----------------------------------------------------------------

/// The stop-detection settings that do not depend on the day.
#[derive(Debug, Clone)]
pub struct StopTuning {
    pub shards: u32,
    pub slow_kn: f64,
    pub smooth: Duration,
    pub resume_nm: f64,
    pub min_stop: Duration,
}

impl StopTuning {
    fn for_day(&self, day_start: DateTime<Utc>) -> StopParams {
        StopParams {
            day_start,
            shards: self.shards,
            slow_kn: self.slow_kn,
            smooth: self.smooth,
            resume_nm: self.resume_nm,
            min_stop: self.min_stop,
        }
    }
}

pub struct StopSegmentsRun<'a> {
    pub select: &'a DaySelect,
    pub tuning: &'a StopTuning,
    pub plan_only: bool,
}

pub async fn run_stop_segments<C: Catalog>(env: &Env<'_, C>, run: &StopSegmentsRun<'_>) -> Result<Summary> {
    anyhow::ensure!(run.tuning.shards >= 1, "--shards must be at least 1");
    let mut log = load_log(env).await?;
    let candidates = log.days(STEP_TRACK_POINTS);
    let (days, force) = select_days(run.select, &candidates, Utc::now().date_naive())?;

    let ctx = SessionContext::new();
    register(&ctx, env.catalog, env.output, TABLE_TRACK_POINTS)
        .await
        .context("run track-points first")?;
    if env.rest.is_some() {
        ensure_day_table(env.catalog, env.output, TABLE_STOP_SEGMENTS, stops::stop_segments_schema()).await?;
    }
    let seg_ident = table_ident(env.output, TABLE_STOP_SEGMENTS);
    let mut sum = Summary::default();
    let mut prev_day_registered: Option<NaiveDate> = None;

    for day in days {
        let Some(up) = log.get(STEP_TRACK_POINTS, day).cloned() else {
            println!("{day}: no track_points; run track-points first");
            prev_day_registered = None;
            sum.empty += 1;
            continue;
        };
        let prev_same = day.pred_opt().and_then(|p| log.get(STEP_STOP_SEGMENTS, p)).cloned();
        let token = downstream_token(&up, prev_same.as_ref());
        if !force && log.is_current(STEP_STOP_SEGMENTS, day, &token) {
            println!("{day}: up to date");
            prev_day_registered = None;
            sum.skipped += 1;
            continue;
        }
        if run.plan_only {
            println!("{day}: would build");
            prev_day_registered = None;
            sum.built += 1;
            continue;
        }
        let day_start = start_of(day);
        let days_idx = date_to_day(day);
        let params = run.tuning.for_day(day_start);

        if prev_day_registered.and_then(|d| d.succ_opt()) == Some(day) {
            stops::set_previous(&ctx, Some(("prev_day_stops", None))).await?;
        } else if env.catalog.table_exists(&seg_ident).await? {
            let _ = ctx.deregister_table(TABLE_STOP_SEGMENTS)?;
            register(&ctx, env.catalog, env.output, TABLE_STOP_SEGMENTS).await?;
            stops::set_previous(
                &ctx,
                Some((TABLE_STOP_SEGMENTS, Some((day_start - Duration::days(1), day_start)))),
            )
            .await?;
        } else {
            stops::set_previous(&ctx, None).await?;
        }

        let table = if env.rest.is_some() {
            Some(env.catalog.load_table(&seg_ident).await?)
        } else {
            None
        };
        let mut all = Vec::new();
        let mut files = Vec::new();
        for shard in 0..run.tuning.shards {
            let batches = stops::build_segments(&ctx, &params, shard).await?;
            carry::check_batches(&stops::stop_segments_schema(), &batches)?;
            if let Some(t) = &table {
                files.extend(write_day_shard(t, days_idx, &batches, &["mmsi", "stop_id"]).await?);
            }
            all.extend(batches);
        }
        let pieces: usize = all.iter().map(|b| b.num_rows()).sum();
        prev_day_registered = if carry::register_output(&ctx, "prev_day_stops", &all)? {
            Some(day)
        } else {
            None
        };
        if pieces == 0 {
            println!("{day}: no stops; leaving the partition untouched");
            sum.empty += 1;
            continue;
        }
        println!(
            "{day}: {pieces} stop segments; {} continue the previous day, {} still open at day end",
            count_true(&all, "continues_previous"),
            count_true(&all, "open_at_day_end"),
        );
        if let Some(rest) = env.rest {
            let r = commit_day(env.catalog, rest, env.output, TABLE_STOP_SEGMENTS, days_idx, files, pieces)
                .await?;
            println!(
                "  {}.{TABLE_STOP_SEGMENTS}: {} files written, {} replaced",
                env.output.namespace, r.files_added, r.files_removed
            );
            let row = log_row(STEP_STOP_SEGMENTS, day, token, pieces);
            append_log(env, &row).await?;
            log.insert(row);
        }
        sum.built += 1;
    }
    Ok(sum)
}

/// Number of `true`s in a boolean column across batches.
pub fn count_true(batches: &[RecordBatch], name: &str) -> usize {
    batches
        .iter()
        .filter_map(|b| b.column_by_name(name))
        .filter_map(|c| c.as_any().downcast_ref::<arrow::array::BooleanArray>())
        .map(|c| c.true_count())
        .sum()
}

// ---- all three -----------------------------------------------------------------------

pub struct DailyRun<'a> {
    pub select: &'a DaySelect,
    pub opts: &'a ReduceOptions,
    pub lookback_days: i64,
    pub track_shards: u32,
    pub stops: &'a StopTuning,
    pub plan_only: bool,
}

/// `track-points`, then `tracks`, then `stop-segments` for the same selection.
pub async fn run_daily<C: Catalog>(env: &Env<'_, C>, run: &DailyRun<'_>) -> Result<Summary> {
    let mut total = Summary::default();
    println!("== track-points");
    total += run_track_points(
        env,
        &TrackPointsRun {
            select: run.select,
            opts: run.opts,
            lookback_days: run.lookback_days,
            plan_only: run.plan_only,
        },
    )
    .await?;
    if run.plan_only {
        println!("(tracks and stop-segments are planned once track-points has run)");
        return Ok(total);
    }
    println!("== tracks");
    total += run_tracks(
        env,
        &TracksRun {
            select: run.select,
            shards: run.track_shards,
            plan_only: false,
        },
    )
    .await?;
    println!("== stop-segments");
    total += run_stop_segments(
        env,
        &StopSegmentsRun {
            select: run.select,
            tuning: run.stops,
            plan_only: false,
        },
    )
    .await?;
    Ok(total)
}
