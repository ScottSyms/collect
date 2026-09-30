//! `ais-tracks`: builds derived Iceberg tables (vessel identity, track points,
//! tracks, stops and voyages) from the silver `positions` and `statics` tables.
//! Every command is a dry run unless `--apply` is given.

use anyhow::{Context, Result};
use ais_tracks::output::{commit_day, ensure_day_table, replace_table, write_day_shard, DayWriter};
use ais_tracks::track_points::TABLE_TRACK_POINTS;
use ais_tracks::ports::{self, TABLE_REF_PORTS};
use ais_tracks::reduce::{Rules, StreamState, ThinOpts};
use ais_tracks::reduce_day::{self, ReduceOptions};
use ais_tracks::source;
use ais_tracks::stops::{self, StopParams, TABLE_STOPS, TABLE_STOP_SEGMENTS};
use ais_tracks::tracks::{self, TABLE_TRACKS};
use ais_tracks::voyages::{self, TABLE_VOYAGES};
use ais_tracks::vessels::{
    self, TABLE_VESSELS, TABLE_VESSEL_ATTRIBUTES,
};
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use clap::{Args, Parser, Subcommand};
use collect_core::exitcode;
use collect_core::iceberg::ensure_namespace;
use collect_core::iceberg::{
    open_catalog, table_ident, IcebergCliArgs, IcebergConfig, TABLE_POSITIONS, TABLE_STATICS,
};
use collect_maint::commit::RestClient;
use datafusion::prelude::SessionContext;
use iceberg::Catalog;
use iceberg_datafusion::IcebergStaticTableProvider;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(
    name = "ais-tracks",
    version,
    about = "Build vessel, track and voyage tables from silver AIS data (dry run unless --apply)"
)]
struct Cli {
    /// Silver tables are read from `--iceberg-namespace`.
    #[command(flatten)]
    iceberg: IcebergCliArgs,

    /// Namespace the derived tables are written to. Default: the input
    /// namespace.
    #[arg(long, env = "OUTPUT_NAMESPACE", global = true)]
    output_namespace: Option<String>,

    /// Optional prefix for derived table names (e.g. "v2" -> "v2_vessels").
    #[arg(long, env = "OUTPUT_TABLE_PREFIX", global = true)]
    output_table_prefix: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Rebuild `vessels` and `vessel_attributes` from `statics` and `positions`.
    Vessels(ApplyArgs),
    /// Reduce each day of `positions` with bounded memory (duplicates,
    /// movement, outlier flags, optional thinning) into `track_points`, one
    /// day partition at a time.
    TrackPoints(TrackPointsArgs),
    /// Roll `track_points` up into continuous track segments, one day at a
    /// time. Days must be built in order (run track-points first).
    Tracks(TracksArgs),
    /// Load the NGA World Port Index (Pub 150 CSV) as reference data.
    Ports(PortsArgs),
    /// Find where vessels were stationary, one day at a time (run
    /// track-points first; build days in order).
    StopSegments(StopSegmentsArgs),
    /// Merge stop segments into one row per stop and match them to ports.
    Stops(ApplyArgs),
    /// Build the legs between consecutive stops.
    Voyages(VoyagesArgs),
    /// Route and reduce one day of raw reports with bounded memory: duplicates,
    /// movement and outlier flags, and optional thinning. Reads the silver
    /// `positions` table, or `--source-dir`; writes Parquet under `--out-dir`,
    /// or with no `--out-dir` only reports what would be kept.
    ReduceDay(ReduceDayArgs),
    /// Print shell completions to stdout.
    Completions { shell: clap_complete::Shell },
}

#[derive(Args, Debug)]
struct ApplyArgs {
    /// Write the tables. Without it, compute and report only.
    #[arg(long)]
    apply: bool,
}

/// How a day of raw reports is routed and reduced; shared by `reduce-day` and
/// `track-points`.
#[derive(Args, Debug, Clone)]
struct ReduceTuning {
    /// Scratch space for the bucket files (about 21 bytes per raw report,
    /// compressed).
    #[arg(long, default_value_os_t = std::env::temp_dir().join("ais-tracks"))]
    scratch: std::path::PathBuf,

    /// Keep the scratch files afterwards.
    #[arg(long)]
    keep_scratch: bool,

    /// Number of buckets. Default: about one per --target-bucket-rows reports.
    #[arg(long)]
    buckets: Option<usize>,

    /// Reports per bucket to aim for; a bucket is held in memory (about 56
    /// bytes per report) while it is reduced.
    #[arg(long, default_value_t = 3_000_000)]
    target_bucket_rows: u64,

    /// Keep every row instead of thinning.
    #[arg(long)]
    no_thin: bool,

    /// Keep a row when the vessel has moved this far (nautical miles) since the
    /// last kept row.
    #[arg(long, default_value_t = 0.1)]
    keep_distance_nm: f64,

    /// Keep a row when this many seconds have passed since the last kept row.
    #[arg(long, default_value_t = 120.0)]
    keep_interval_s: f64,

    /// Keep a row when the course changed by this many degrees while moving.
    #[arg(long, default_value_t = 15.0)]
    keep_turn_deg: f64,

    /// Keep a row when the speed changed by this many knots.
    #[arg(long, default_value_t = 2.0)]
    keep_speed_kn: f64,

    /// Implied speed above which a point is a speed jump.
    #[arg(long, default_value_t = 60.0)]
    max_speed_kn: f64,

    /// A longer silence than this many minutes marks a gap.
    #[arg(long, default_value_t = 30)]
    gap_minutes: i64,
}

impl ReduceTuning {
    fn options(&self) -> ReduceOptions {
        ReduceOptions {
            rules: Rules {
                max_speed_kn: self.max_speed_kn,
                gap_s: (self.gap_minutes * 60) as f64,
            },
            thin: if self.no_thin {
                ThinOpts::off()
            } else {
                ThinOpts {
                    off: false,
                    keep_distance_nm: self.keep_distance_nm,
                    keep_interval_s: self.keep_interval_s,
                    keep_turn_deg: self.keep_turn_deg,
                    keep_speed_kn: self.keep_speed_kn,
                }
            },
            buckets: self.buckets,
            target_bucket_rows: self.target_bucket_rows,
            scratch: self.scratch.clone(),
        }
    }
}

#[derive(Args, Debug)]
struct TrackPointsArgs {
    /// First UTC day to build (YYYY-MM-DD).
    #[arg(long, alias = "day", value_parser = clap::value_parser!(NaiveDate))]
    from: NaiveDate,

    /// Last UTC day to build, inclusive. Default: the same as --from.
    #[arg(long, value_parser = clap::value_parser!(NaiveDate))]
    to: Option<NaiveDate>,

    /// How many days before each day to look for a vessel's previous point
    /// when the day before is not being built in the same run.
    #[arg(long, default_value_t = 1)]
    lookback_days: i64,

    #[command(flatten)]
    tuning: ReduceTuning,

    /// Write the partitions. Without it, reduce and report only.
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct TracksArgs {
    /// First UTC day to build (YYYY-MM-DD).
    #[arg(long, alias = "day", value_parser = clap::value_parser!(NaiveDate))]
    from: NaiveDate,

    /// Last UTC day to build, inclusive. Default: the same as --from.
    #[arg(long, value_parser = clap::value_parser!(NaiveDate))]
    to: Option<NaiveDate>,

    /// Split each day's vessels into this many independent chunks to bound
    /// memory. Output is identical for any value.
    #[arg(long, default_value_t = 4)]
    shards: u32,

    /// Write the partitions. Without it, compute and report only.
    #[arg(long)]
    apply: bool,
}

/// Number of `true`s in a boolean column across batches.
fn count_true(batches: &[arrow::record_batch::RecordBatch], name: &str) -> usize {
    batches
        .iter()
        .filter_map(|b| b.column_by_name(name))
        .filter_map(|c| c.as_any().downcast_ref::<arrow::array::BooleanArray>())
        .map(|c| c.true_count())
        .sum()
}

#[derive(Args, Debug)]
struct PortsArgs {
    #[command(subcommand)]
    command: PortsCommand,
}

#[derive(Subcommand, Debug)]
enum PortsCommand {
    /// Append a World Port Index release to `ref_ports`.
    Load {
        /// The downloaded UpdatedPub150.csv.
        #[arg(long)]
        file: String,
        /// Label for this release (e.g. its publication date, 2026-03-01).
        /// Matching uses the greatest label, so use sortable dates.
        #[arg(long)]
        release: String,
        /// Write the table. Without it, parse and report only.
        #[arg(long)]
        apply: bool,
    },
}

#[derive(Args, Debug)]
struct StopSegmentsArgs {
    /// First UTC day to build (YYYY-MM-DD).
    #[arg(long, alias = "day", value_parser = clap::value_parser!(NaiveDate))]
    from: NaiveDate,

    /// Last UTC day to build, inclusive. Default: the same as --from.
    #[arg(long, value_parser = clap::value_parser!(NaiveDate))]
    to: Option<NaiveDate>,

    /// Split each day's vessels into this many independent chunks to bound
    /// memory. Output is identical for any value.
    #[arg(long, default_value_t = 4)]
    shards: u32,

    /// Smoothed speed below this many knots counts as stationary.
    #[arg(long, default_value_t = 0.5)]
    slow_kn: f64,

    /// Width in minutes of the window speed is averaged over.
    #[arg(long, default_value_t = 10)]
    smooth_minutes: i64,

    /// After a reporting gap a vessel still counts as stopped if it is within
    /// this many nautical miles of where it was.
    #[arg(long, default_value_t = 1.0)]
    resume_nm: f64,

    /// Ignore stationary runs shorter than this (0 keeps every run). Runs
    /// touching midnight are kept, as they may continue.
    #[arg(long, default_value_t = 30)]
    min_stop_minutes: i64,

    /// Write the partitions. Without it, compute and report only.
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct VoyagesArgs {
    /// Skip comparing with declared destinations. This avoids scanning the
    /// whole `statics` table.
    #[arg(long)]
    no_declared: bool,

    /// Write the table. Without it, compute and report only.
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct ReduceDayArgs {
    /// The UTC day to reduce, YYYY-MM-DD.
    #[arg(long, value_parser = clap::value_parser!(NaiveDate))]
    day: NaiveDate,

    /// Read Parquet files under this directory instead of the silver table (no
    /// catalog needed).
    #[arg(long)]
    source_dir: Option<std::path::PathBuf>,

    /// Write the reduced rows as a Parquet file under this directory.
    #[arg(long)]
    out_dir: Option<std::path::PathBuf>,

    #[command(flatten)]
    tuning: ReduceTuning,
}

async fn reduce_day_cmd(cli: &Cli, a: &ReduceDayArgs) -> Result<i32> {
    let (start_us, end_us) = source::day_bounds_us(a.day);
    let opts = a.tuning.options();

    let (stream, est) = match &a.source_dir {
        Some(dir) => source::parquet_dir_day(dir, start_us, end_us).await?,
        None => {
            cli.iceberg.validate()?;
            anyhow::ensure!(
                cli.iceberg.is_iceberg_mode(),
                "give --source-dir, or --iceberg-catalog-uri to read the silver table"
            );
            let input = IcebergConfig::from(&cli.iceberg);
            let catalog = open_catalog(&input).await?;
            let table = catalog
                .load_table(&table_ident(&input, TABLE_POSITIONS))
                .await
                .context("loading the silver positions table")?;
            source::iceberg_day(&table, start_us, end_us).await?
        }
    };
    println!("{}: about {est} reports", a.day);

    let label = reduce_day::scratch_label(a.day);
    let started = std::time::Instant::now();
    let manifest = reduce_day::route_day(stream, est, start_us, end_us, &label, &opts).await?;
    let route_time = started.elapsed();
    println!("routing done in {:.1}s; peak memory so far {:.0} MB", route_time.as_secs_f64(), reduce_day::peak_rss_bytes() as f64 / 1e6);

    let mut writer: Option<parquet::arrow::ArrowWriter<std::fs::File>> = None;
    let mut sink = |batch: arrow::record_batch::RecordBatch| -> Result<()> {
        if let Some(dir) = &a.out_dir {
            if writer.is_none() {
                std::fs::create_dir_all(dir)?;
                let props = parquet::file::properties::WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::ZSTD(
                        parquet::basic::ZstdLevel::try_new(3)?,
                    ))
                    .set_max_row_group_size(128 * 1024)
                    .build();
                let file = std::fs::File::create(dir.join(format!("thin-{}.parquet", a.day)))?;
                writer = Some(parquet::arrow::ArrowWriter::try_new(file, batch.schema(), Some(props))?);
            }
            writer.as_mut().expect("writer").write(&batch)?;
        }
        Ok(())
    };
    let result = reduce_day::reduce_buckets(&manifest, &opts, &Default::default(), &mut sink)
        .map(|(stats, _states)| stats);
    if let Some(w) = writer {
        w.close()?;
    }
    if !a.tuning.keep_scratch {
        manifest.cleanup();
    }
    let mut stats = result?;
    stats.route_time = route_time;
    print!("{}", stats.report(opts.thin.off));
    match &a.out_dir {
        Some(d) => println!("wrote {}", d.join(format!("thin-{}.parquet", a.day)).display()),
        None => println!("dry run; pass --out-dir to write the reduced rows"),
    }
    if stats.routed == 0 {
        return Ok(exitcode::NOTHING_TO_DO);
    }
    Ok(exitcode::SUCCESS)
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::exit(exitcode::UNCLASSIFIED_ERROR);
        }
    }
}

/// Registers a silver Iceberg table with DataFusion under its base name.
async fn register(
    ctx: &SessionContext,
    catalog: &impl Catalog,
    config: &IcebergConfig,
    base: &str,
) -> Result<()> {
    let ident = table_ident(config, base);
    let table = catalog
        .load_table(&ident)
        .await
        .with_context(|| format!("loading silver table {ident}"))?;
    let provider = IcebergStaticTableProvider::try_new_from_table(table).await?;
    ctx.register_table(base, Arc::new(provider))?;
    Ok(())
}

async fn run() -> Result<i32> {
    let cli = Cli::parse();
    if let Command::Completions { shell } = &cli.command {
        collect_core::print_completions::<Cli>(*shell, "ais-tracks");
        return Ok(exitcode::SUCCESS);
    }
    if let Command::ReduceDay(a) = &cli.command {
        return reduce_day_cmd(&cli, a).await;
    }
    cli.iceberg.validate()?;
    anyhow::ensure!(
        cli.iceberg.is_iceberg_mode(),
        "--iceberg-catalog-uri is required"
    );
    let input = IcebergConfig::from(&cli.iceberg);
    let output = IcebergConfig {
        namespace: cli
            .output_namespace
            .clone()
            .unwrap_or_else(|| input.namespace.clone()),
        table_prefix: cli.output_table_prefix.clone(),
        ..IcebergConfig::from(&cli.iceberg)
    };
    let catalog = open_catalog(&input).await?;

    match &cli.command {
        Command::Vessels(a) => {
            let ctx = SessionContext::new();
            register(&ctx, &catalog, &input, TABLE_STATICS).await?;
            register(&ctx, &catalog, &input, TABLE_POSITIONS).await?;
            let built = vessels::build(&ctx).await?;
            let n_vessels: usize = built.vessels.iter().map(|b| b.num_rows()).sum();
            let n_attrs: usize = built.attributes.iter().map(|b| b.num_rows()).sum();
            println!("{n_vessels} vessels, {n_attrs} attribute values");
            if !a.apply {
                println!("dry run; pass --apply to write to namespace '{}'", output.namespace);
                return Ok(exitcode::SUCCESS);
            }
            let rest = RestClient::connect(&output).await?;
            for (name, schema, batches, bloom) in [
                (
                    TABLE_VESSELS,
                    vessels::vessels_schema(),
                    &built.vessels,
                    &["mmsi"][..],
                ),
                (
                    TABLE_VESSEL_ATTRIBUTES,
                    vessels::vessel_attributes_schema(),
                    &built.attributes,
                    &["mmsi"][..],
                ),
            ] {
                let r = replace_table(&catalog, &rest, &output, name, schema, batches, bloom)
                    .await?;
                println!(
                    "{}.{name}: {} rows, {} files written, {} replaced{}",
                    output.namespace,
                    r.rows,
                    r.files_added,
                    r.files_removed,
                    if r.created { " (created)" } else { "" }
                );
            }
        }
        Command::TrackPoints(a) => {
            let to = a.to.unwrap_or(a.from);
            anyhow::ensure!(to >= a.from, "--to is before --from");
            anyhow::ensure!(a.lookback_days >= 1, "--lookback-days must be at least 1");
            let opts = a.tuning.options();
            let positions = catalog
                .load_table(&table_ident(&input, TABLE_POSITIONS))
                .await
                .context("loading the silver positions table")?;
            let rest = if a.apply {
                Some(RestClient::connect(&output).await?)
            } else {
                None
            };
            if a.apply {
                ensure_day_table(
                    &catalog,
                    &output,
                    TABLE_TRACK_POINTS,
                    ais_tracks::reduce::thin_points_schema(),
                )
                .await?;
            }
            let tp_ident = table_ident(&output, TABLE_TRACK_POINTS);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            let today = Utc::now().date_naive();
            let lookback_us = a.lookback_days * 86_400_000_000;

            // Where each vessel's stream left off. Carried in memory from the
            // day just built, else read from the table.
            let mut states: HashMap<u32, StreamState> = HashMap::new();
            let mut states_after: Option<NaiveDate> = None;
            let mut built_any = false;
            let mut day = a.from;
            while day <= to {
                if day >= today {
                    eprintln!("note: {day} is not over yet; it will be rebuilt as data arrives");
                }
                let (start_us, end_us) = source::day_bounds_us(day);
                let days = day.signed_duration_since(epoch).num_days() as i32;
                if states_after.and_then(|d| d.succ_opt()) != Some(day) {
                    states.clear();
                    if catalog.table_exists(&tp_ident).await? {
                        let t = catalog.load_table(&tp_ident).await?;
                        states = source::thin_states(&t, start_us - lookback_us, start_us).await?;
                    }
                }

                let label = reduce_day::scratch_label(day);
                let started = std::time::Instant::now();
                let (stream, est) = source::iceberg_day(&positions, start_us, end_us).await?;
                let manifest = reduce_day::route_day(stream, est, start_us, end_us, &label, &opts).await?;
                let route_time = started.elapsed();

                // Reduce on a blocking thread and stream the batches to the
                // writer through a small channel, so memory stays bounded.
                let (tx, mut rx) = tokio::sync::mpsc::channel::<arrow::record_batch::RecordBatch>(4);
                let (opts2, prev) = (opts.clone(), states.clone());
                let handle = tokio::task::spawn_blocking(move || {
                    let mut sink = |b| {
                        tx.blocking_send(b)
                            .map_err(|_| anyhow::anyhow!("the writer stopped"))
                    };
                    let r = reduce_day::reduce_buckets(&manifest, &opts2, &prev, &mut sink);
                    (r, manifest)
                });
                let mut writer = if a.apply {
                    let t = catalog.load_table(&tp_ident).await?;
                    Some(DayWriter::new(&t, days, &["mmsi"]).await?)
                } else {
                    None
                };
                while let Some(batch) = rx.recv().await {
                    if let Some(w) = writer.as_mut() {
                        w.write(&batch).await?;
                    }
                }
                let (result, manifest) = handle.await?;
                if !a.tuning.keep_scratch {
                    manifest.cleanup();
                }
                let (mut stats, next) = result?;
                stats.route_time = route_time;

                if stats.routed == 0 {
                    println!("{day}: no positions; leaving the partition untouched");
                    states_after = None;
                    day = day.succ_opt().expect("date overflow");
                    continue;
                }
                built_any = true;
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
                if let (Some(w), Some(rest)) = (writer, &rest) {
                    let rows = w.rows;
                    let files = w.finish().await?;
                    let r = commit_day(&catalog, rest, &output, TABLE_TRACK_POINTS, days, files, rows).await?;
                    println!(
                        "  {}.{TABLE_TRACK_POINTS}: {} files written, {} replaced",
                        output.namespace, r.files_added, r.files_removed
                    );
                }
                reduce_day::merge_states(&mut states, next, end_us - lookback_us);
                states_after = Some(day);
                day = day.succ_opt().expect("date overflow");
            }
            if !a.apply {
                println!("dry run; pass --apply to write to namespace '{}'", output.namespace);
            }
            if !built_any {
                return Ok(exitcode::NOTHING_TO_DO);
            }
        }
        Command::Tracks(a) => {
            anyhow::ensure!(a.shards >= 1, "--shards must be at least 1");
            let to = a.to.unwrap_or(a.from);
            anyhow::ensure!(to >= a.from, "--to is before --from");
            let ctx = SessionContext::new();
            register(&ctx, &catalog, &output, TABLE_TRACK_POINTS)
                .await
                .context("run track-points first")?;
            let rest = if a.apply {
                Some(RestClient::connect(&output).await?)
            } else {
                None
            };
            let table = if a.apply {
                Some(
                    ensure_day_table(&catalog, &output, TABLE_TRACKS, tracks::tracks_schema())
                        .await?,
                )
            } else {
                None
            };
            let tracks_ident = table_ident(&output, TABLE_TRACKS);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            let mut prev_day_registered: Option<NaiveDate> = None;
            let mut built_any = false;
            let mut day = a.from;
            while day <= to {
                let day_start =
                    Utc.from_utc_datetime(&day.and_hms_opt(0, 0, 0).expect("midnight"));
                let days = day.signed_duration_since(epoch).num_days() as i32;

                // What the previous day ended with: this run's own output when
                // it just built that day, else whatever the table holds.
                let carried = prev_day_registered.and_then(|d| d.succ_opt()) == Some(day);
                if carried {
                    tracks::set_previous(&ctx, Some(("prev_day", None))).await?;
                } else if catalog.table_exists(&tracks_ident).await? {
                    let _ = ctx.deregister_table(TABLE_TRACKS)?;
                    register(&ctx, &catalog, &output, TABLE_TRACKS).await?;
                    tracks::set_previous(
                        &ctx,
                        Some((TABLE_TRACKS, Some((day_start - Duration::days(1), day_start)))),
                    )
                    .await?;
                } else {
                    tracks::set_previous(&ctx, None).await?;
                }

                let mut all = Vec::new();
                let mut files = Vec::new();
                for shard in 0..a.shards {
                    let batches = tracks::build_shard(&ctx, day_start, a.shards, shard).await?;
                    tracks::check_against_schema(&batches)?;
                    if let Some(table) = &table {
                        files.extend(
                            write_day_shard(table, days, &batches, &["mmsi", "track_id"]).await?,
                        );
                    }
                    all.extend(batches);
                }
                let pieces: usize = all.iter().map(|b| b.num_rows()).sum();
                prev_day_registered = if tracks::register_output(&ctx, "prev_day", &all)? {
                    Some(day)
                } else {
                    None
                };
                if pieces == 0 {
                    println!("{day}: no track segments; leaving the partition untouched");
                    day = day.succ_opt().expect("date overflow");
                    continue;
                }
                built_any = true;

                let in_day = ctx
                    .sql(&format!(
                        "SELECT count(*) FROM track_points WHERE ts >= '{}' AND ts < '{}'",
                        day_start.format("%Y-%m-%dT%H:%M:%S+00:00"),
                        (day_start + Duration::days(1)).format("%Y-%m-%dT%H:%M:%S+00:00"),
                    ))
                    .await?
                    .collect()
                    .await?;
                let points = in_day
                    .first()
                    .and_then(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .map(|c| c.value(0) as usize)
                    })
                    .unwrap_or(0);
                let covered = tracks::sum_int(&all, "n_rows")?;
                println!(
                    "{day}: {pieces} segments covering {covered} of {points} points; \
                     {} continue the previous day, {} chains broken",
                    count_true(&all, "continues_previous"),
                    count_true(&all, "chain_broken"),
                );
                if points > covered {
                    println!(
                        "  {} points sit in stretches with no positioned point and belong to no segment",
                        points - covered
                    );
                }
                if let Some(rest) = &rest {
                    let r = commit_day(&catalog, rest, &output, TABLE_TRACKS, days, files, pieces)
                        .await?;
                    println!(
                        "  {}.{TABLE_TRACKS}: {} files written, {} replaced",
                        output.namespace, r.files_added, r.files_removed
                    );
                }
                day = day.succ_opt().expect("date overflow");
            }
            if !a.apply {
                println!("dry run; pass --apply to write to namespace '{}'", output.namespace);
            }
            if !built_any {
                return Ok(exitcode::NOTHING_TO_DO);
            }
        }
        Command::Ports(PortsArgs {
            command: PortsCommand::Load { file, release, apply },
        }) => {
            let batches = ports::load_csv(file, release).await?;
            ais_tracks::carry::check_batches(&ports::ref_ports_schema(), &batches)?;
            let n: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!("{n} ports in {file}, release '{release}'");
            if !apply {
                println!("dry run; pass --apply to write to namespace '{}'", output.namespace);
                return Ok(exitcode::SUCCESS);
            }
            ensure_namespace(&catalog, &output).await?;
            let table = collect_core::iceberg::ensure_table(
                &catalog,
                &output,
                TABLE_REF_PORTS,
                ports::ref_ports_schema(),
                iceberg::spec::PartitionSpecBuilder::new(ports::ref_ports_schema()),
            )
            .await?;
            if table.metadata().current_snapshot().is_some() {
                let ctx = SessionContext::new();
                register(&ctx, &catalog, &output, TABLE_REF_PORTS).await?;
                let existing = ctx
                    .sql(&format!(
                        "SELECT count(*) FROM {TABLE_REF_PORTS} WHERE wpi_release = '{}'",
                        release.replace('\'', "''")
                    ))
                    .await?
                    .collect()
                    .await?;
                let have = existing
                    .first()
                    .and_then(|b| b.column(0).as_any().downcast_ref::<arrow::array::Int64Array>())
                    .map(|c| c.value(0))
                    .unwrap_or(0);
                anyhow::ensure!(
                    have == 0,
                    "release '{release}' is already loaded ({have} ports); pick a new label"
                );
            }
            collect_core::iceberg::commit_batches(&catalog, &table, batches, 3, TABLE_REF_PORTS)
                .await?;
            println!("{}.{TABLE_REF_PORTS}: appended {n} ports", output.namespace);
        }
        Command::StopSegments(a) => {
            anyhow::ensure!(a.shards >= 1, "--shards must be at least 1");
            let to = a.to.unwrap_or(a.from);
            anyhow::ensure!(to >= a.from, "--to is before --from");
            let ctx = SessionContext::new();
            register(&ctx, &catalog, &output, TABLE_TRACK_POINTS)
                .await
                .context("run track-points first")?;
            let rest = if a.apply {
                Some(RestClient::connect(&output).await?)
            } else {
                None
            };
            let table = if a.apply {
                Some(
                    ensure_day_table(
                        &catalog,
                        &output,
                        TABLE_STOP_SEGMENTS,
                        stops::stop_segments_schema(),
                    )
                    .await?,
                )
            } else {
                None
            };
            let seg_ident = table_ident(&output, TABLE_STOP_SEGMENTS);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            let mut prev_day_registered: Option<NaiveDate> = None;
            let mut built_any = false;
            let mut day = a.from;
            while day <= to {
                let day_start =
                    Utc.from_utc_datetime(&day.and_hms_opt(0, 0, 0).expect("midnight"));
                let days = day.signed_duration_since(epoch).num_days() as i32;
                let params = StopParams {
                    day_start,
                    shards: a.shards,
                    slow_kn: a.slow_kn,
                    smooth: Duration::minutes(a.smooth_minutes),
                    resume_nm: a.resume_nm,
                    min_stop: Duration::minutes(a.min_stop_minutes),
                };

                let carried = prev_day_registered.and_then(|d| d.succ_opt()) == Some(day);
                if carried {
                    stops::set_previous(&ctx, Some(("prev_day_stops", None))).await?;
                } else if catalog.table_exists(&seg_ident).await? {
                    let _ = ctx.deregister_table(TABLE_STOP_SEGMENTS)?;
                    register(&ctx, &catalog, &output, TABLE_STOP_SEGMENTS).await?;
                    stops::set_previous(
                        &ctx,
                        Some((
                            TABLE_STOP_SEGMENTS,
                            Some((day_start - Duration::days(1), day_start)),
                        )),
                    )
                    .await?;
                } else {
                    stops::set_previous(&ctx, None).await?;
                }

                let mut all = Vec::new();
                let mut files = Vec::new();
                for shard in 0..a.shards {
                    let batches = stops::build_segments(&ctx, &params, shard).await?;
                    ais_tracks::carry::check_batches(&stops::stop_segments_schema(), &batches)?;
                    if let Some(table) = &table {
                        files.extend(
                            write_day_shard(table, days, &batches, &["mmsi", "stop_id"]).await?,
                        );
                    }
                    all.extend(batches);
                }
                let pieces: usize = all.iter().map(|b| b.num_rows()).sum();
                prev_day_registered = if ais_tracks::carry::register_output(&ctx, "prev_day_stops", &all)? {
                    Some(day)
                } else {
                    None
                };
                if pieces == 0 {
                    println!("{day}: no stops; leaving the partition untouched");
                    day = day.succ_opt().expect("date overflow");
                    continue;
                }
                built_any = true;
                println!(
                    "{day}: {pieces} stop segments; {} continue the previous day, {} still open at day end",
                    count_true(&all, "continues_previous"),
                    count_true(&all, "open_at_day_end"),
                );
                if let Some(rest) = &rest {
                    let r = commit_day(&catalog, rest, &output, TABLE_STOP_SEGMENTS, days, files, pieces)
                        .await?;
                    println!(
                        "  {}.{TABLE_STOP_SEGMENTS}: {} files written, {} replaced",
                        output.namespace, r.files_added, r.files_removed
                    );
                }
                day = day.succ_opt().expect("date overflow");
            }
            if !a.apply {
                println!("dry run; pass --apply to write to namespace '{}'", output.namespace);
            }
            if !built_any {
                return Ok(exitcode::NOTHING_TO_DO);
            }
        }
        Command::Stops(a) => {
            let ctx = SessionContext::new();
            register(&ctx, &catalog, &output, TABLE_STOP_SEGMENTS)
                .await
                .context("run stop-segments first")?;
            register(&ctx, &catalog, &output, TABLE_TRACKS)
                .await
                .context("run tracks first")?;
            register(&ctx, &catalog, &output, TABLE_REF_PORTS)
                .await
                .context("run `ports load` first")?;
            let built = stops::build_stops(&ctx).await?;
            ais_tracks::carry::check_batches(&stops::stops_schema(), &built)?;
            let n: usize = built.iter().map(|b| b.num_rows()).sum();
            let matched: usize = built
                .iter()
                .filter_map(|b| b.column_by_name("port_id"))
                .map(|c| c.len() - c.null_count())
                .sum();
            println!("{n} stops, {matched} matched to a port");
            if !a.apply {
                println!("dry run; pass --apply to write to namespace '{}'", output.namespace);
                return Ok(exitcode::SUCCESS);
            }
            let rest = RestClient::connect(&output).await?;
            let r = replace_table(
                &catalog,
                &rest,
                &output,
                TABLE_STOPS,
                stops::stops_schema(),
                &built,
                &["mmsi", "stop_id"],
            )
            .await?;
            println!(
                "{}.{TABLE_STOPS}: {} rows, {} files written, {} replaced{}",
                output.namespace,
                r.rows,
                r.files_added,
                r.files_removed,
                if r.created { " (created)" } else { "" }
            );
        }
        Command::Voyages(a) => {
            let ctx = SessionContext::new();
            for name in [TABLE_STOPS, TABLE_TRACKS, TABLE_TRACK_POINTS] {
                register(&ctx, &catalog, &output, name)
                    .await
                    .with_context(|| format!("{name} is missing; build it first"))?;
            }
            if !a.no_declared {
                register(&ctx, &catalog, &input, TABLE_STATICS).await?;
            }
            let built = voyages::build(&ctx, !a.no_declared).await?;
            ais_tracks::carry::check_batches(&voyages::voyages_schema(), &built)?;
            let n: usize = built.iter().map(|b| b.num_rows()).sum();
            println!("{n} voyages");
            if !a.apply {
                println!("dry run; pass --apply to write to namespace '{}'", output.namespace);
                return Ok(exitcode::SUCCESS);
            }
            let rest = RestClient::connect(&output).await?;
            let r = replace_table(
                &catalog,
                &rest,
                &output,
                TABLE_VOYAGES,
                voyages::voyages_schema(),
                &built,
                &["mmsi", "voyage_id"],
            )
            .await?;
            println!(
                "{}.{TABLE_VOYAGES}: {} rows, {} files written, {} replaced{}",
                output.namespace,
                r.rows,
                r.files_added,
                r.files_removed,
                if r.created { " (created)" } else { "" }
            );
        }
        Command::ReduceDay(_) | Command::Completions { .. } => unreachable!(),
    }
    Ok(exitcode::SUCCESS)
}
