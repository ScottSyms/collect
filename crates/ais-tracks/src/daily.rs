//! The daily steps, `track-points`, `tracks` and `stop-segments`, each choosing
//! its days, skipping the ones already built from the same input, building the
//! rest, and only then logging them as done.
//!
//! Every step follows the same order for a day, so a crash at any point leaves
//! something a rerun repairs: write the data files, commit the day's partition
//! (a replace, so rebuilding is idempotent), write the day's `vessel_state`,
//! and last append the `build_log` row. A day counts as built only once its row
//! exists. See [`crate::state`] for what the rows record.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use collect_core::iceberg::{
    commit_batches, ensure_namespace, ensure_table, table_ident, IcebergConfig, TABLE_POSITIONS,
    TABLE_STATICS,
};
use collect_maint::commit::RestClient;
use datafusion::prelude::SessionContext;
use iceberg::spec::PartitionSpecBuilder;
use iceberg::Catalog;
use iceberg_datafusion::IcebergStaticTableProvider;

use crate::carry;
use crate::output::{commit_day, ensure_day_table, ensure_day_table_on, write_day_shard, DayWriter};
use crate::reduce::StreamState;
use crate::reduce_day::{self, ReduceOptions};
use crate::source;
use crate::state::{
    build_log_schema, STEP_STATICS_DAILY, STEP_STOPS, STEP_VESSELS, STEP_VESSEL_DAILY, STEP_VOYAGES, date_to_day, days_from_table, downstream_token, log_batch,
    read_states_before, scan_all, select_days, states_batch, track_points_token,
    vessel_state_schema, DaySelect, Log, LogRow, STEP_STOP_SEGMENTS, STEP_TRACKS,
    STEP_TRACK_POINTS, TABLE_BUILD_LOG, TABLE_VESSEL_STATE,
};
use crate::stops::{self, StopParams, TABLE_STOPS, TABLE_STOP_SEGMENTS};
use crate::ports::TABLE_REF_PORTS;
use crate::legs::{self, voyage_state_schema, TABLE_OPEN_VOYAGES, TABLE_VOYAGE_STATE};
use crate::voyages::{voyages_schema, TABLE_VOYAGES};
use crate::track_points::TABLE_TRACK_POINTS;
use crate::vessels::{
    self, attr_parts_sql, attribute_daily_schema, define_parts, dest_parts_sql, destination_daily_schema,
    sta_parts_sql, static_daily_schema,
    vessel_daily_batch, vessel_daily_schema, vessel_attributes_schema, vessels_schema,
    with_day_ts, TABLE_ATTRIBUTE_DAILY, TABLE_DESTINATION_DAILY, TABLE_STATIC_DAILY, TABLE_VESSELS, TABLE_VESSEL_ATTRIBUTES,
    TABLE_VESSEL_DAILY,
};
use crate::output::{replace_table, replace_table_or_clear};
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
    register_as(ctx, catalog, config, base, base).await
}

/// Registers an Iceberg table with DataFusion under `alias`.
pub async fn register_as(
    ctx: &SessionContext,
    catalog: &impl Catalog,
    config: &IcebergConfig,
    base: &str,
    alias: &str,
) -> Result<()> {
    let ident = table_ident(config, base);
    let table = catalog
        .load_table(&ident)
        .await
        .with_context(|| format!("loading table {ident}"))?;
    let provider = IcebergStaticTableProvider::try_new_from_table(table).await?;
    ctx.register_table(alias, Arc::new(provider))?;
    Ok(())
}

/// A DataFusion context whose memory is capped at `bytes` and which spills
/// sorts and aggregates to `spill_dir` rather than growing without bound. (Window
/// functions and hash joins do not spill, so queries using them must be sized to
/// fit.)
pub fn bounded_context(spill_dir: &Path, bytes: usize) -> Result<SessionContext> {
    use datafusion::execution::memory_pool::FairSpillPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::SessionConfig;
    std::fs::create_dir_all(spill_dir)?;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(bytes)))
        .with_temp_file_path(spill_dir)
        .build_arc()?;
    Ok(SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(2).with_batch_size(8192),
        runtime,
    ))
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

/// Writes `batches` as the whole content of `day`'s partition of `base`, creating
/// the day-partitioned table if needed. Returns the rows written; nothing is
/// touched when there are none.
async fn write_day_batches<C: Catalog>(
    env: &Env<'_, C>,
    rest: &RestClient,
    base: &str,
    schema: iceberg::spec::Schema,
    day: NaiveDate,
    batches: &[RecordBatch],
) -> Result<usize> {
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if rows == 0 {
        return Ok(0);
    }
    ensure_day_table(env.catalog, env.output, base, schema).await?;
    let table = env.catalog.load_table(&table_ident(env.output, base)).await?;
    let days = date_to_day(day);
    let mut w = DayWriter::new(&table, days, &["mmsi"]).await?;
    for b in batches {
        w.write(b).await?;
    }
    let files = w.finish().await?;
    commit_day(env.catalog, rest, env.output, base, days, files, rows).await?;
    Ok(rows)
}

/// Writes the vessel states as of the end of `day`.
async fn write_vessel_state<C: Catalog>(
    env: &Env<'_, C>,
    rest: &RestClient,
    day: NaiveDate,
    states: &HashMap<u32, StreamState>,
) -> Result<()> {
    let start_us = start_of(day).timestamp_micros();
    if let Some(batch) = states_batch(start_us, states)? {
        write_day_batches(env, rest, TABLE_VESSEL_STATE, vessel_state_schema(), day, &[batch]).await?;
    }
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
    let silver = days_from_table(&positions).await?;
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
        let (mut stats, next, vessel_days) = result?;
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
            // Per-vessel first/last seen and report count, for `vessels`.
            if let Some(b) = vessel_daily_batch(start_us, &vessel_days)? {
                write_day_batches(env, rest, TABLE_VESSEL_DAILY, vessel_daily_schema(), day, &[b]).await?;
                let row = log_row(STEP_VESSEL_DAILY, day, token.clone(), vessel_days.len());
                append_log(env, &row).await?;
                log.insert(row);
            }
            // Last, so a day counts as built only once everything it feeds is written.
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

// ---- statics-daily ------------------------------------------------------------------

pub struct StaticsRun<'a> {
    pub select: &'a DaySelect,
    /// Where the query may spill to disk.
    pub scratch: &'a Path,
    pub plan_only: bool,
}

/// Per-day identity aggregates from that day's static reports: `attribute_daily`
/// (every distinct value a vessel reported, with counts) and `static_daily`.
/// Only the day's own static reports are read.
pub async fn run_statics_daily<C: Catalog>(env: &Env<'_, C>, run: &StaticsRun<'_>) -> Result<Summary> {
    let ident = table_ident(env.input, TABLE_STATICS);
    let mut sum = Summary::default();
    if !env.catalog.table_exists(&ident).await? {
        println!("no silver statics table; skipping identity aggregates");
        return Ok(sum);
    }
    let statics = env.catalog.load_table(&ident).await?;
    let silver = days_from_table(&statics).await?;
    let candidates: Vec<NaiveDate> = silver.keys().copied().collect();
    let (days, force) = select_days(run.select, &candidates, Utc::now().date_naive())?;
    let mut log = load_log(env).await?;

    for day in days {
        let n = silver.get(&day).copied().unwrap_or(0);
        if n == 0 {
            println!("{day}: no static reports");
            sum.empty += 1;
            continue;
        }
        let token = format!("rows={n}");
        if !force && log.is_current(STEP_STATICS_DAILY, day, &token) {
            println!("{day}: statics up to date");
            sum.skipped += 1;
            continue;
        }
        if run.plan_only {
            println!("{day}: would build identity aggregates");
            sum.built += 1;
            continue;
        }
        let start = start_of(day);
        let ctx = bounded_context(run.scratch, 1 << 30)?;
        register(&ctx, env.catalog, env.input, TABLE_STATICS).await?;
        let view = ctx
            .sql(&format!(
                "SELECT * FROM {TABLE_STATICS} WHERE ts >= '{}' AND ts < '{}'",
                carry::lit(start),
                carry::lit(start + Duration::days(1))
            ))
            .await?
            .into_view();
        ctx.register_table("statics_day", view)?;
        let attr = ctx.sql(&attr_parts_sql("statics_day")).await?.collect().await?;
        let sta = ctx.sql(&sta_parts_sql("statics_day")).await?.collect().await?;
        let dest = ctx.sql(&dest_parts_sql("statics_day")).await?.collect().await?;
        let start_us = start.timestamp_micros();
        let dest: Vec<RecordBatch> = dest
            .iter()
            .map(|b| with_day_ts(b, start_us, &destination_daily_schema()))
            .collect::<Result<_>>()?;
        let attr: Vec<RecordBatch> = attr
            .iter()
            .map(|b| with_day_ts(b, start_us, &attribute_daily_schema()))
            .collect::<Result<_>>()?;
        let sta: Vec<RecordBatch> = sta
            .iter()
            .map(|b| with_day_ts(b, start_us, &static_daily_schema()))
            .collect::<Result<_>>()?;
        let (n_attr, n_sta) = (
            attr.iter().map(|b| b.num_rows()).sum::<usize>(),
            sta.iter().map(|b| b.num_rows()).sum::<usize>(),
        );
        println!("{day}: {n} static reports -> {n_sta} vessels, {n_attr} attribute values");
        if let Some(rest) = env.rest {
            write_day_batches(env, rest, TABLE_ATTRIBUTE_DAILY, attribute_daily_schema(), day, &attr).await?;
            write_day_batches(env, rest, TABLE_STATIC_DAILY, static_daily_schema(), day, &sta).await?;
            write_day_batches(env, rest, TABLE_DESTINATION_DAILY, destination_daily_schema(), day, &dest).await?;
            let row = log_row(STEP_STATICS_DAILY, day, token, n_sta);
            append_log(env, &row).await?;
            log.insert(row);
        }
        sum.built += 1;
    }
    Ok(sum)
}

// ---- vessels ------------------------------------------------------------------------

pub struct VesselsRun<'a> {
    /// Refold from every daily table even if an increment would do.
    pub full: bool,
    pub scratch: &'a Path,
    pub plan_only: bool,
}

/// What the cumulative tables already hold.
struct Prior {
    folded_through: i32,
    computed_at_us: i64,
    /// Whether `vessel_attributes` exists. Before any static report arrives it
    /// is never written, which is the same as being empty.
    has_attributes: bool,
}

fn first_i64(batches: &[RecordBatch], col: usize) -> Option<i64> {
    use arrow::array::Array;
    let b = batches.first()?;
    let c = arrow::compute::cast(b.column(col), &arrow::datatypes::DataType::Int64).ok()?;
    let a = c.as_any().downcast_ref::<arrow::array::Int64Array>()?;
    (!a.is_empty() && !a.is_null(0)).then(|| a.value(0))
}

/// What a cumulative table says about how far it is folded: nothing (it does not
/// exist or has no rows), that it is inconsistent, or `(folded_through, computed_at)`.
enum Folded {
    Absent,
    Broken,
    At(i64, i64),
}

async fn read_folded<C: Catalog>(env: &Env<'_, C>, scratch: &Path, base: &str) -> Result<Folded> {
    let ident = table_ident(env.output, base);
    if !env.catalog.table_exists(&ident).await? {
        return Ok(Folded::Absent);
    }
    if env.catalog.load_table(&ident).await?.metadata().current_snapshot().is_none() {
        return Ok(Folded::Absent);
    }
    let ctx = bounded_context(scratch, 256 << 20)?;
    register(&ctx, env.catalog, env.output, base).await?;
    let b = ctx
        .sql(&format!(
            "SELECT min(folded_through), max(folded_through), CAST(max(computed_at) AS BIGINT) FROM {base}"
        ))
        .await?
        .collect()
        .await?;
    Ok(match (first_i64(&b, 0), first_i64(&b, 1), first_i64(&b, 2)) {
        // 0 marks tables built straight from silver (`vessels --from-silver`),
        // which say nothing about which daily aggregates they include.
        (Some(lo), Some(hi), Some(at)) if lo == hi && hi != 0 => Folded::At(hi, at),
        _ => Folded::Broken,
    })
}

/// What the cumulative tables say they include, or `None` if there is nothing
/// safe to increment from: no `vessels`, or the two tables disagree about how
/// far they are folded (a crash between writing them).
async fn read_prior<C: Catalog>(env: &Env<'_, C>, scratch: &Path) -> Result<Option<Prior>> {
    let Folded::At(through, at) = read_folded(env, scratch, TABLE_VESSELS).await? else {
        return Ok(None);
    };
    Ok(match read_folded(env, scratch, TABLE_VESSEL_ATTRIBUTES).await? {
        Folded::Absent => Some(Prior { folded_through: through as i32, computed_at_us: at, has_attributes: false }),
        Folded::At(t, a) if t == through => Some(Prior {
            folded_through: through as i32,
            computed_at_us: at.min(a),
            has_attributes: true,
        }),
        _ => None,
    })
}

/// Registers `alias` as the columns `cols` of a daily table (or an empty table
/// of the right shape if it does not exist yet), optionally limited to
/// `[from, to)` days.
async fn register_daily<C: Catalog>(
    ctx: &SessionContext,
    env: &Env<'_, C>,
    base: &str,
    schema: &iceberg::spec::Schema,
    alias: &str,
    cols: &str,
    range: Option<(NaiveDate, NaiveDate)>,
) -> Result<()> {
    let raw = format!("raw_{base}");
    if env.catalog.table_exists(&table_ident(env.output, base)).await? {
        register_as(ctx, env.catalog, env.output, base, &raw).await?;
    } else {
        let arrow = Arc::new(iceberg::arrow::schema_to_arrow_schema(schema)?);
        ctx.register_table(&raw, Arc::new(datafusion::datasource::MemTable::try_new(arrow, vec![vec![]])?))?;
    }
    let filter = range
        .map(|(a, b)| {
            format!(
                " WHERE ts >= '{}' AND ts < '{}'",
                carry::lit(start_of(a)),
                carry::lit(start_of(b))
            )
        })
        .unwrap_or_default();
    let sql = format!("SELECT {cols} FROM {raw}{filter}");
    if range.is_some() {
        // Feeds a UNION with the prior tables: see `vessels::materialize`.
        ctx.register_table(alias, vessels::materialize(ctx, &sql).await?)?;
    } else {
        ctx.register_table(alias, ctx.sql(&sql).await?.into_view())?;
    }
    Ok(())
}

/// Brings `vessels` and `vessel_attributes` up to date from the daily tables.
///
/// Normally that is an increment: the new days merge into the existing tables.
/// It falls back to refolding every daily table when there is nothing to
/// increment from, when the two tables disagree about how far they are folded,
/// when a daily table that was already folded in has been rebuilt since, or
/// when asked to (`full`).
pub async fn run_vessels<C: Catalog>(env: &Env<'_, C>, run: &VesselsRun<'_>) -> Result<Summary> {
    let mut sum = Summary::default();
    let log = load_log(env).await?;
    let mut days: BTreeSet<NaiveDate> = log.days(STEP_VESSEL_DAILY).into_iter().collect();
    days.extend(log.days(STEP_STATICS_DAILY));
    let Some(&through) = days.iter().max() else {
        println!("no daily aggregates yet; run track-points (and statics-daily) first");
        sum.empty += 1;
        return Ok(sum);
    };
    let prior = if run.full { None } else { read_prior(env, run.scratch).await? };

    // Decide how to build.
    enum Mode {
        Refold,
        Increment { after: NaiveDate },
    }
    let mode = match &prior {
        None => Mode::Refold,
        Some(p) => {
            let folded = crate::state::day_to_date(p.folded_through);
            let rebuilt = [STEP_VESSEL_DAILY, STEP_STATICS_DAILY].iter().any(|step| {
                log.days(step).into_iter().any(|d| {
                    d <= folded && log.get(step, d).is_some_and(|r| r.built_at_us > p.computed_at_us)
                })
            });
            if rebuilt {
                Mode::Refold
            } else if through <= folded {
                println!("vessels up to date through {folded}");
                sum.skipped += 1;
                return Ok(sum);
            } else {
                Mode::Increment { after: folded }
            }
        }
    };
    let (label, range) = match &mode {
        Mode::Refold => ("refold from every daily table".to_string(), None),
        Mode::Increment { after } => (
            format!("increment after {after}"),
            Some((after.succ_opt().context("date overflow")?, through.succ_opt().context("date overflow")?)),
        ),
    };
    if run.plan_only {
        println!("would update vessels through {through}: {label}");
        sum.built += 1;
        return Ok(sum);
    }

    let ctx = bounded_context(run.scratch, 1536 << 20)?;
    register_daily(&ctx, env, TABLE_VESSEL_DAILY, &vessel_daily_schema(), "daily_pos",
        "mmsi, first_seen, last_seen, n_positions", range).await?;
    register_daily(&ctx, env, TABLE_STATIC_DAILY, &static_daily_schema(), "daily_sta",
        "mmsi, first_static_seen, last_static_seen, n_statics, ais_class", range).await?;
    register_daily(&ctx, env, TABLE_ATTRIBUTE_DAILY, &attribute_daily_schema(), "daily_attr",
        "mmsi, attribute, value, n_obs, first_seen, last_seen", range).await?;
    let incremental = matches!(mode, Mode::Increment { .. });
    if incremental {
        register_as(&ctx, env.catalog, env.output, TABLE_VESSELS, "raw_prior_vessels").await?;
        ctx.register_table("prior_vessels", vessels::materialize(&ctx, "SELECT * FROM raw_prior_vessels").await?)?;
        if prior.as_ref().is_some_and(|p| p.has_attributes) {
            register_as(&ctx, env.catalog, env.output, TABLE_VESSEL_ATTRIBUTES, "raw_prior_attributes").await?;
            ctx.register_table("prior_attributes", vessels::materialize(&ctx, "SELECT * FROM raw_prior_attributes").await?)?;
        } else {
            let arrow = Arc::new(iceberg::arrow::schema_to_arrow_schema(&vessel_attributes_schema())?);
            ctx.register_table("prior_attributes", Arc::new(datafusion::datasource::MemTable::try_new(arrow, vec![vec![]])?))?;
        }
    }
    define_parts(&ctx, incremental).await?;
    let built = vessels::merge(&ctx, date_to_day(through)).await?;
    carry::check_batches(&vessels_schema(), &built.vessels)?;
    carry::check_batches(&vessel_attributes_schema(), &built.attributes)?;
    let n_vessels: usize = built.vessels.iter().map(|b| b.num_rows()).sum();
    let n_attrs: usize = built.attributes.iter().map(|b| b.num_rows()).sum();
    println!("vessels through {through} ({label}): {n_vessels} vessels, {n_attrs} attribute values");

    if let Some(rest) = env.rest {
        // Attributes first: if only one table is replaced the two disagree about
        // `folded_through`, which the next run reads as "refold".
        if n_attrs > 0 {
            replace_table(env.catalog, rest, env.output, TABLE_VESSEL_ATTRIBUTES, vessel_attributes_schema(),
                &built.attributes, &["mmsi"]).await?;
        } else {
            // No static reports yet. There is nothing to write, and refusing to
            // replace a table with an empty result is right, so leave it.
            println!("no identity attributes yet (no static reports); vessel_attributes not written");
        }
        replace_table(env.catalog, rest, env.output, TABLE_VESSELS, vessels_schema(),
            &built.vessels, &["mmsi"]).await?;
        append_log(env, &log_row(STEP_VESSELS, through, label, n_vessels)).await?;
    }
    sum.built += 1;
    Ok(sum)
}

// ---- stops --------------------------------------------------------------------------

pub struct StopsRun<'a> {
    /// Refold from every stop segment even if an increment would do.
    pub full: bool,
    pub scratch: &'a Path,
    pub plan_only: bool,
}

/// Appends several log rows in one commit.
async fn append_logs<C: Catalog>(env: &Env<'_, C>, rows: &[LogRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    ensure_namespace(env.catalog, env.output).await?;
    let schema = build_log_schema();
    let table = ensure_table(env.catalog, env.output, TABLE_BUILD_LOG, schema.clone(), PartitionSpecBuilder::new(schema))
        .await?;
    commit_batches(env.catalog, &table, vec![log_batch(rows)?], 3, TABLE_BUILD_LOG).await
}

/// Replaces `day`'s partition of `base`, partitioned by day of `column`, with
/// `batches`. An empty `batches` clears the partition.
async fn replace_day_partition<C: Catalog>(
    env: &Env<'_, C>,
    rest: &RestClient,
    base: &str,
    schema: iceberg::spec::Schema,
    column: &str,
    day: NaiveDate,
    batches: &[RecordBatch],
) -> Result<usize> {
    ensure_day_table_on(env.catalog, env.output, base, schema, column).await?;
    let table = env.catalog.load_table(&table_ident(env.output, base)).await?;
    let days = date_to_day(day);
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let files = if rows == 0 {
        Vec::new()
    } else {
        let mut w = DayWriter::new(&table, days, &["mmsi"]).await?;
        for b in batches {
            w.write(b).await?;
        }
        w.finish().await?
    };
    commit_day(env.catalog, rest, env.output, base, days, files, rows).await?;
    Ok(rows)
}

/// Replaces `day`'s partition of the stops table (partitioned on `depart_ts`).
async fn replace_stops_day<C: Catalog>(
    env: &Env<'_, C>,
    rest: &RestClient,
    day: NaiveDate,
    batches: &[RecordBatch],
) -> Result<usize> {
    replace_day_partition(env, rest, TABLE_STOPS, stops::stops_schema(), "depart_ts", day, batches).await
}

/// Folds one new day of stop segments into `stops`: recomputes the stops it
/// touches from their existing row and today's pieces, and rewrites the two
/// partitions involved.
pub async fn increment_stops<C: Catalog>(
    env: &Env<'_, C>,
    rest: &RestClient,
    scratch: &Path,
    day: NaiveDate,
) -> Result<usize> {
    let ctx = bounded_context(scratch, 1 << 30)?;
    register_as(&ctx, env.catalog, env.output, TABLE_STOP_SEGMENTS, "raw_segments").await?;
    let (d_prev, d_start, d_end) = (
        start_of(day.pred_opt().context("date underflow")?),
        start_of(day),
        start_of(day.succ_opt().context("date overflow")?),
    );
    let range = |a: DateTime<Utc>, b: DateTime<Utc>, col: &str| {
        format!("{col} >= '{}' AND {col} < '{}'", carry::lit(a), carry::lit(b))
    };
    let pieces = vessels::materialize(
        &ctx,
        &format!(
            "SELECT stop_id, mmsi, ts, ts_end, n_points, lat, lon, radius_nm, n_moored, n_anchored \
             FROM raw_segments WHERE {}",
            range(d_start, d_end, "ts")
        ),
    )
    .await?;
    ctx.register_table("day_pieces", pieces)?;

    // Existing rows: only the previous day's partition and today's can hold a
    // stop that today's pieces touch.
    let exists = env.catalog.table_exists(&table_ident(env.output, TABLE_STOPS)).await?;
    if exists {
        register_as(&ctx, env.catalog, env.output, TABLE_STOPS, "raw_stops").await?;
    } else {
        let arrow = Arc::new(iceberg::arrow::schema_to_arrow_schema(&stops::stops_schema())?);
        ctx.register_table("raw_stops", Arc::new(datafusion::datasource::MemTable::try_new(arrow, vec![vec![]])?))?;
    }
    let touched = "stop_id IN (SELECT stop_id FROM day_pieces)";
    let base = vessels::materialize(
        &ctx,
        &format!(
            "SELECT * EXCEPT (rn) FROM (SELECT b.*, row_number() OVER \
               (PARTITION BY stop_id ORDER BY depart_ts DESC) AS rn \
             FROM raw_stops b WHERE {} AND {touched}) WHERE rn = 1",
            range(d_prev, d_end, "depart_ts")
        ),
    )
    .await?;
    ctx.register_table("stop_base", base)?;
    let (_, keep_batches) = vessels::materialize_batches(
        &ctx,
        &format!(
            "SELECT * FROM raw_stops WHERE {} AND NOT ({touched})",
            range(d_prev, d_start, "depart_ts")
        ),
    )
    .await?;

    let parts = ctx.sql(&stops::parts_with_base_sql()).await?.into_view();
    ctx.register_table("stop_parts", parts)?;
    register(&ctx, env.catalog, env.output, TABLE_REF_PORTS).await?;
    let merged = stops::build_stops(&ctx).await?;
    carry::check_batches(&stops::stops_schema(), &merged)?;

    // Today's partition first, then remove the moved stops from yesterday's:
    // a crash in between leaves a stop in both, which the next run resolves.
    let rows = replace_stops_day(env, rest, day, &merged).await?;
    if exists {
        replace_stops_day(env, rest, day.pred_opt().context("date underflow")?, &keep_batches).await?;
    }
    Ok(rows)
}

/// Removes every row of `stops`, partition by partition.
async fn clear_stops<C: Catalog>(env: &Env<'_, C>, rest: &RestClient) -> Result<()> {
    let ident = table_ident(env.output, TABLE_STOPS);
    if !env.catalog.table_exists(&ident).await? {
        return Ok(());
    }
    let table = env.catalog.load_table(&ident).await?;
    for day in days_from_table(&table).await?.keys() {
        commit_day(env.catalog, rest, env.output, TABLE_STOPS, date_to_day(*day), Vec::new(), 0).await?;
    }
    Ok(())
}

/// Brings `stops`, `voyages` and `open_voyages` up to date from the stop
/// segments, one day at a time.
///
/// The two are folded together because a day's voyage step needs `stops` as it
/// stood when that day was folded in: the stops that arrived or carried on that
/// day, in that day's partition. (A stop that carries on into the next day moves
/// to the next day's partition, so `stops` cannot be read back day by day
/// afterwards.)
///
/// Normally that is an increment for each new day. It replays every day from the
/// start, clearing `stops` first, when there is nothing to increment from, when an
/// already folded day's stop segments were rebuilt, or when the voyage state is
/// missing or out of step.
pub async fn run_stops<C: Catalog>(env: &Env<'_, C>, run: &StopsRun<'_>) -> Result<Summary> {
    let mut sum = Summary::default();
    let mut log = load_log(env).await?;
    let seg_days = log.days(STEP_STOP_SEGMENTS);
    if seg_days.is_empty() {
        println!("no stop segments yet; run stop-segments first");
        sum.empty += 1;
        return Ok(sum);
    }
    anyhow::ensure!(
        env.catalog.table_exists(&table_ident(env.output, TABLE_REF_PORTS)).await?,
        "run `ports load` first"
    );
    let token_of = |log: &Log, d: NaiveDate| format!("seg={}", log.get(STEP_STOP_SEGMENTS, d).map_or(0, |r| r.built_at_us));
    let current = |log: &Log, d: NaiveDate| log.is_current(STEP_STOPS, d, &token_of(log, d));
    let folded: Vec<NaiveDate> = seg_days.iter().copied().filter(|d| current(&log, *d)).collect();
    let todo: Vec<NaiveDate> = seg_days.iter().copied().filter(|d| !current(&log, *d)).collect();
    let stale = todo.iter().any(|d| log.get(STEP_STOPS, *d).is_some());
    let out_of_order = matches!((folded.last(), todo.first()), (Some(f), Some(t)) if t < f);
    let through = voyage_state_through(env, run.scratch).await?;
    let out_of_step = folded.last().map(|d| date_to_day(*d)) != through;

    enum Mode {
        Replay,
        Increment,
    }
    let (mode, why) = if run.full {
        (Mode::Replay, "asked to")
    } else if folded.is_empty() {
        (Mode::Replay, "nothing folded yet")
    } else if stale {
        (Mode::Replay, "a folded day's stop segments were rebuilt")
    } else if out_of_order {
        (Mode::Replay, "a day before the latest folded one is new")
    } else if out_of_step {
        (Mode::Replay, "the voyage state is missing or out of step")
    } else if todo.is_empty() {
        println!("stops and voyages up to date through {}", folded.last().expect("non-empty"));
        sum.skipped += 1;
        return Ok(sum);
    } else {
        (Mode::Increment, "new days")
    };
    let replay = matches!(mode, Mode::Replay);
    let work: Vec<NaiveDate> = if replay { seg_days.clone() } else { todo };

    if run.plan_only || env.rest.is_none() {
        println!(
            "would {} stops and voyages over {} day(s) {}",
            if replay { "replay" } else { "fold into" },
            work.len(),
            if replay { format!("({why})") } else { format!("({}..{})", work[0], work[work.len() - 1]) },
        );
        sum.built += 1;
        return Ok(sum);
    }
    let rest = env.rest.expect("checked");

    if replay {
        println!("replaying stops and voyages over {} days ({why})", work.len());
        // Mark every day as not folded first, so a crash part-way is repaired by
        // the next run instead of being mistaken for a finished one.
        let marks: Vec<LogRow> = seg_days.iter().map(|d| log_row(STEP_STOPS, *d, "replaying".to_string(), 0)).collect();
        append_logs(env, &marks).await?;
        for r in marks {
            log.insert(r);
        }
        clear_stops(env, rest).await?;
    }
    for (i, d) in work.iter().copied().enumerate() {
        let n_stops = increment_stops(env, rest, run.scratch, d).await?;
        // A previous run may have written this day's voyage state and stopped
        // before logging it: the closed legs and the state are then in place.
        let already = !replay && through == Some(date_to_day(d));
        let (n_closed, n_open) = if already {
            (0, 0)
        } else {
            voyage_day(env, rest, run.scratch, d, !(replay && i == 0)).await?
        };
        println!("{d}: {n_stops} stops folded; {n_closed} legs closed, {n_open} under way");
        let stops_row = log_row(STEP_STOPS, d, token_of(&log, d), n_stops);
        let voyages_row = log_row(STEP_VOYAGES, d, token_of(&log, d), n_closed);
        append_logs(env, &[voyages_row.clone(), stops_row.clone()]).await?;
        log.insert(voyages_row);
        log.insert(stops_row);
        sum.built += 1;
    }
    Ok(sum)
}

// ---- voyages (folded with stops, above) -----------------------------------------------

/// Registers the Iceberg table `base` as `alias`, or an empty table of `schema`'s
/// shape if it does not exist yet.
async fn register_or_empty<C: Catalog>(
    ctx: &SessionContext,
    env: &Env<'_, C>,
    base: &str,
    alias: &str,
    schema: &iceberg::spec::Schema,
) -> Result<()> {
    if env.catalog.table_exists(&table_ident(env.output, base)).await? {
        register_as(ctx, env.catalog, env.output, base, alias).await
    } else {
        let arrow = Arc::new(iceberg::arrow::schema_to_arrow_schema(schema)?);
        ctx.register_table(alias, Arc::new(datafusion::datasource::MemTable::try_new(arrow, vec![vec![]])?))?;
        Ok(())
    }
}

/// Registers `alias` as `SELECT cols FROM raw_alias WHERE range` over `raw_alias`.
async fn view_of(ctx: &SessionContext, alias: &str, cols: &str, filter: &str) -> Result<()> {
    let raw = format!("raw_{alias}");
    let _ = ctx.deregister_table(alias)?;
    let view = ctx.sql(&format!("SELECT {cols} FROM {raw} WHERE {filter}")).await?.into_view();
    ctx.register_table(alias, view)?;
    Ok(())
}

/// Advances the voyage state one day and writes what the day produced. With
/// `prior_state` false the vessels start from nothing (the first day of a replay).
async fn voyage_day<C: Catalog>(
    env: &Env<'_, C>,
    rest: &RestClient,
    scratch: &Path,
    day: NaiveDate,
    prior_state: bool,
) -> Result<(usize, usize)> {
    let ctx = bounded_context(scratch, 1 << 30)?;
    let start = start_of(day);
    let end = start + Duration::days(1);
    let in_day = |col: &str| format!("{col} >= '{}' AND {col} < '{}'", carry::lit(start), carry::lit(end));

    if prior_state {
        register_or_empty(&ctx, env, TABLE_VOYAGE_STATE, "raw_leg_state", &voyage_state_schema()).await?;
        view_of(&ctx, "leg_state", "*", "true").await?;
    } else {
        let arrow = Arc::new(iceberg::arrow::schema_to_arrow_schema(&voyage_state_schema())?);
        ctx.register_table("leg_state", Arc::new(datafusion::datasource::MemTable::try_new(arrow, vec![vec![]])?))?;
    }
    register_as(&ctx, env.catalog, env.output, TABLE_STOPS, "raw_leg_stops").await?;
    view_of(&ctx, "leg_stops", "*", &in_day("depart_ts")).await?;
    register_as(&ctx, env.catalog, env.output, TABLE_VESSEL_DAILY, "raw_leg_daily").await?;
    view_of(&ctx, "leg_daily", "*", &in_day("ts")).await?;
    register_as(&ctx, env.catalog, env.output, TABLE_TRACK_POINTS, "raw_leg_points").await?;
    view_of(
        &ctx,
        "leg_points",
        "mmsi, ts, dist_nm, has_position, is_duplicate, gap_before, is_speed_jump, max_sog, n_raw, n_outliers_raw",
        &in_day("ts"),
    )
    .await?;

    let adv = legs::advance_day(&ctx, start.timestamp_micros(), date_to_day(day)).await?;

    // Declared destinations for the legs that closed, from the daily counts of
    // every day they spanned.
    let mut closed_rows = Vec::new();
    if let Some(min_depart) = adv.closed.iter().map(|c| c.depart_ts).min() {
        let from = Utc.timestamp_micros(min_depart).single().context("time")?;
        let from = start_of(from.date_naive());
        register_or_empty(&ctx, env, TABLE_DESTINATION_DAILY, "raw_destination_daily", &destination_daily_schema()).await?;
        let _ = ctx.deregister_table("destination_daily")?;
        let view = ctx
            .sql(&format!(
                "SELECT * FROM raw_destination_daily WHERE ts >= '{}' AND ts < '{}'",
                carry::lit(from),
                carry::lit(end)
            ))
            .await?
            .into_view();
        ctx.register_table("destination_daily", view)?;
        closed_rows = legs::finish_closed(&ctx, &adv.closed).await?;
        carry::check_batches(&voyages_schema(), &closed_rows)?;
    }
    let n_closed: usize = closed_rows.iter().map(|b| b.num_rows()).sum();
    let open_rows = legs::open_voyages(&ctx, &adv.states).await?;
    carry::check_batches(&voyages_schema(), &open_rows)?;
    let n_open: usize = open_rows.iter().map(|b| b.num_rows()).sum();

    // Closed legs first (a partition replace, so idempotent), then the state that
    // says the day is folded in, then the legs still under way.
    replace_day_partition(env, rest, TABLE_VOYAGES, voyages_schema(), "arrive_ts", day, &closed_rows).await?;
    if adv.states.iter().map(|b| b.num_rows()).sum::<usize>() > 0 {
        replace_table(env.catalog, rest, env.output, TABLE_VOYAGE_STATE, voyage_state_schema(), &adv.states, &["mmsi"]).await?;
    }
    replace_table_or_clear(env.catalog, rest, env.output, TABLE_OPEN_VOYAGES, voyages_schema(), &open_rows, &["mmsi"]).await?;
    Ok((n_closed, n_open))
}

/// The `through` day recorded in `voyage_state`, if the table is consistent.
async fn voyage_state_through<C: Catalog>(env: &Env<'_, C>, scratch: &Path) -> Result<Option<i32>> {
    let ident = table_ident(env.output, TABLE_VOYAGE_STATE);
    if !env.catalog.table_exists(&ident).await?
        || env.catalog.load_table(&ident).await?.metadata().current_snapshot().is_none()
    {
        return Ok(None);
    }
    let ctx = bounded_context(scratch, 256 << 20)?;
    register(&ctx, env.catalog, env.output, TABLE_VOYAGE_STATE).await?;
    let b = ctx
        .sql(&format!("SELECT min(through), max(through) FROM {TABLE_VOYAGE_STATE}"))
        .await?
        .collect()
        .await?;
    Ok(match (first_i64(&b, 0), first_i64(&b, 1)) {
        (Some(lo), Some(hi)) if lo == hi => Some(hi as i32),
        _ => None,
    })
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

/// `track-points`, `statics-daily`, `tracks` and `stop-segments` for the same
/// selection, then `vessels`.
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
    println!("== statics-daily");
    total += run_statics_daily(
        env,
        &StaticsRun { select: run.select, scratch: &run.opts.scratch, plan_only: run.plan_only },
    )
    .await?;
    if run.plan_only {
        println!("(tracks, stop-segments and vessels are planned once track-points has run)");
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
    if env.catalog.table_exists(&table_ident(env.output, TABLE_REF_PORTS)).await? {
        println!("== stops and voyages");
        total += run_stops(
            env,
            &StopsRun { full: false, scratch: &run.opts.scratch, plan_only: false },
        )
        .await?;
    } else {
        println!("== stops (skipped: run `ports load` first)");
    }
    println!("== vessels");
    total += run_vessels(
        env,
        &VesselsRun { full: false, scratch: &run.opts.scratch, plan_only: false },
    )
    .await?;
    Ok(total)
}
