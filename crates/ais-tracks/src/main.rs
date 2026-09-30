//! `ais-tracks`: builds derived Iceberg tables (vessel identity, track points,
//! tracks, stops and voyages) from the silver `positions` and `statics` tables.
//! Every command is a dry run unless `--apply` is given.

use anyhow::{Context, Result};
use ais_tracks::daily::{self, register, DailyRun, Env, StopSegmentsRun, StopTuning, TrackPointsRun, TracksRun};
use ais_tracks::output::replace_table;
use ais_tracks::state::DaySelect;
use ais_tracks::track_points::TABLE_TRACK_POINTS;
use ais_tracks::ports::{self, TABLE_REF_PORTS};
use ais_tracks::reduce::{Rules, ThinOpts};
use ais_tracks::reduce_day::{self, ReduceOptions};
use ais_tracks::source;
use ais_tracks::stops::TABLE_STOPS;
use ais_tracks::tracks::TABLE_TRACKS;
use ais_tracks::voyages::{self, TABLE_VOYAGES};
use ais_tracks::vessels::{
    self, TABLE_VESSELS, TABLE_VESSEL_ATTRIBUTES,
};
use chrono::{Duration, NaiveDate};
use clap::{Args, Parser, Subcommand};
use collect_core::exitcode;
use collect_core::iceberg::ensure_namespace;
use collect_core::iceberg::{
    open_catalog, table_ident, IcebergCliArgs, IcebergConfig, TABLE_POSITIONS, TABLE_STATICS,
};
use collect_maint::commit::RestClient;
use datafusion::prelude::SessionContext;
use iceberg::Catalog;

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
    Vessels(VesselsArgs),
    /// Per-day identity aggregates from each day's static reports (feeds `vessels`).
    StaticsDaily(StaticsDailyArgs),
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
    /// Merge stop segments into one row per stop and match them to ports:
    /// folds in only the days not yet folded.
    Stops(StopsArgs),
    /// Build the legs between consecutive stops.
    Voyages(VoyagesArgs),
    /// Route and reduce one day of raw reports with bounded memory: duplicates,
    /// movement and outlier flags, and optional thinning. Reads the silver
    /// `positions` table, or `--source-dir`; writes Parquet under `--out-dir`,
    /// or with no `--out-dir` only reports what would be kept.
    ReduceDay(ReduceDayArgs),
    /// Run track-points, tracks and stop-segments in order for the same days:
    /// with --catch-up, everything that needs building, skipping what is
    /// already up to date.
    Daily(DailyArgs),
    /// Print shell completions to stdout.
    Completions { shell: clap_complete::Shell },
}

#[derive(Args, Debug)]
struct StopsArgs {
    /// Refold every stop segment instead of folding in only the new days.
    #[arg(long)]
    full: bool,

    /// Where queries may spill to disk.
    #[arg(long, default_value_os_t = std::env::temp_dir().join("ais-tracks"))]
    scratch: std::path::PathBuf,

    /// Say what would be done, without doing it.
    #[arg(long)]
    plan: bool,

    /// Write the table. Without it, report only.
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

/// Which days a daily step works on.
#[derive(Args, Debug, Clone)]
struct DaysArgs {
    /// First UTC day (YYYY-MM-DD). Required unless --catch-up.
    #[arg(long, alias = "day", value_parser = clap::value_parser!(NaiveDate))]
    from: Option<NaiveDate>,

    /// Last UTC day, inclusive. Default: --from, or with --catch-up the last
    /// day with input.
    #[arg(long, value_parser = clap::value_parser!(NaiveDate))]
    to: Option<NaiveDate>,

    /// Build only the days that need it: days with input that were never
    /// built, or whose input changed since (late data, or an earlier day that
    /// was rebuilt). Each build is logged in `build_log`.
    #[arg(long)]
    catch_up: bool,

    /// With --catch-up, rebuild every day in range whatever the log says.
    #[arg(long, requires = "catch_up")]
    full: bool,

    /// With --catch-up, include today (UTC), which is still filling.
    #[arg(long, requires = "catch_up")]
    include_today: bool,

    /// Say which days would be built and why, without building anything.
    #[arg(long)]
    plan: bool,
}

impl DaysArgs {
    fn select(&self) -> DaySelect {
        DaySelect {
            from: self.from,
            to: self.to,
            catch_up: self.catch_up,
            full: self.full,
            include_today: self.include_today,
        }
    }
}

/// Stop-detection settings.
#[derive(Args, Debug, Clone)]
struct StopFlags {
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
}

impl StopFlags {
    fn tuning(&self, shards: u32) -> StopTuning {
        StopTuning {
            shards,
            slow_kn: self.slow_kn,
            smooth: Duration::minutes(self.smooth_minutes),
            resume_nm: self.resume_nm,
            min_stop: Duration::minutes(self.min_stop_minutes),
        }
    }
}

#[derive(Args, Debug)]
struct TrackPointsArgs {
    #[command(flatten)]
    days: DaysArgs,

    /// How many days before each day to look for a vessel's previous point
    /// when no state was saved for it.
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
    #[command(flatten)]
    days: DaysArgs,

    /// Split each day's vessels into this many independent chunks to bound
    /// memory. Output is identical for any value.
    #[arg(long, default_value_t = 4)]
    shards: u32,

    /// Write the partitions. Without it, compute and report only.
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct StopSegmentsArgs {
    #[command(flatten)]
    days: DaysArgs,

    /// Split each day's vessels into this many independent chunks to bound
    /// memory. Output is identical for any value.
    #[arg(long, default_value_t = 4)]
    shards: u32,

    #[command(flatten)]
    stops: StopFlags,

    /// Write the partitions. Without it, compute and report only.
    #[arg(long)]
    apply: bool,
}

/// `track-points`, `tracks` and `stop-segments`, in order, for the same days.
#[derive(Args, Debug)]
struct DailyArgs {
    #[command(flatten)]
    days: DaysArgs,

    /// How many days before each day to look for a vessel's previous point
    /// when no state was saved for it.
    #[arg(long, default_value_t = 1)]
    lookback_days: i64,

    #[command(flatten)]
    tuning: ReduceTuning,

    /// Shards for the tracks and stop-segments steps.
    #[arg(long, default_value_t = 4)]
    shards: u32,

    #[command(flatten)]
    stops: StopFlags,

    /// Write the partitions. Without it, reduce and report only.
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct VesselsArgs {
    /// Build from the whole silver `positions` and `statics` tables instead of
    /// the daily aggregates. Reads all of history, so it is for small
    /// deployments and for checking the incremental result.
    #[arg(long)]
    from_silver: bool,

    /// Refold every daily aggregate instead of adding only the new days.
    #[arg(long, conflicts_with = "from_silver")]
    full: bool,

    /// Where queries may spill to disk.
    #[arg(long, default_value_os_t = std::env::temp_dir().join("ais-tracks"))]
    scratch: std::path::PathBuf,

    /// Say what would be done, without doing it.
    #[arg(long)]
    plan: bool,

    /// Write the tables. Without it, compute and report only.
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct StaticsDailyArgs {
    #[command(flatten)]
    days: DaysArgs,

    /// Where queries may spill to disk.
    #[arg(long, default_value_os_t = std::env::temp_dir().join("ais-tracks"))]
    scratch: std::path::PathBuf,

    /// Write the partitions. Without it, compute and report only.
    #[arg(long)]
    apply: bool,
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
        .map(|(stats, _states, _days)| stats);
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


/// Connects to the catalog for committing, only when the run will write.
async fn connect_if_writing(apply: bool, plan: bool, output: &IcebergConfig) -> Result<Option<RestClient>> {
    anyhow::ensure!(!(apply && plan), "--plan only reports; drop --apply");
    if apply {
        Ok(Some(RestClient::connect(output).await?))
    } else {
        Ok(None)
    }
}

/// Prints what a daily step did and picks the exit code.
fn finish(sum: daily::Summary, apply: bool, namespace: &str) -> i32 {
    println!(
        "{} built, {} already up to date, {} without input",
        sum.built, sum.skipped, sum.empty
    );
    if !apply && sum.built > 0 {
        println!("dry run; pass --apply to write to namespace '{namespace}'");
    }
    if sum.built == 0 {
        exitcode::NOTHING_TO_DO
    } else {
        exitcode::SUCCESS
    }
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
        Command::Vessels(a) if a.from_silver => {
            let ctx = SessionContext::new();
            register(&ctx, &catalog, &input, TABLE_STATICS).await?;
            register(&ctx, &catalog, &input, TABLE_POSITIONS).await?;
            let built = vessels::build(&ctx).await?;
            let n_vessels: usize = built.vessels.iter().map(|b| b.num_rows()).sum();
            let n_attrs: usize = built.attributes.iter().map(|b| b.num_rows()).sum();
            println!("{n_vessels} vessels, {n_attrs} attribute values (from all of silver)");
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
        Command::Vessels(a) => {
            let rest = connect_if_writing(a.apply, a.plan, &output).await?;
            let env = Env { catalog: &catalog, input: &input, output: &output, rest: rest.as_ref() };
            let sum = daily::run_vessels(
                &env,
                &daily::VesselsRun { full: a.full, scratch: &a.scratch, plan_only: a.plan },
            )
            .await?;
            return Ok(finish(sum, a.apply, &output.namespace));
        }
        Command::StaticsDaily(a) => {
            let rest = connect_if_writing(a.apply, a.days.plan, &output).await?;
            let env = Env { catalog: &catalog, input: &input, output: &output, rest: rest.as_ref() };
            let sum = daily::run_statics_daily(
                &env,
                &daily::StaticsRun { select: &a.days.select(), scratch: &a.scratch, plan_only: a.days.plan },
            )
            .await?;
            return Ok(finish(sum, a.apply, &output.namespace));
        }
        Command::TrackPoints(a) => {
            let rest = connect_if_writing(a.apply, a.days.plan, &output).await?;
            let env = Env { catalog: &catalog, input: &input, output: &output, rest: rest.as_ref() };
            let opts = a.tuning.options();
            let sum = daily::run_track_points(
                &env,
                &TrackPointsRun {
                    select: &a.days.select(),
                    opts: &opts,
                    lookback_days: a.lookback_days,
                    plan_only: a.days.plan,
                },
            )
            .await?;
            return Ok(finish(sum, a.apply, &output.namespace));
        }
        Command::Tracks(a) => {
            let rest = connect_if_writing(a.apply, a.days.plan, &output).await?;
            let env = Env { catalog: &catalog, input: &input, output: &output, rest: rest.as_ref() };
            let sum = daily::run_tracks(
                &env,
                &TracksRun { select: &a.days.select(), shards: a.shards, plan_only: a.days.plan },
            )
            .await?;
            return Ok(finish(sum, a.apply, &output.namespace));
        }
        Command::Daily(a) => {
            let rest = connect_if_writing(a.apply, a.days.plan, &output).await?;
            let env = Env { catalog: &catalog, input: &input, output: &output, rest: rest.as_ref() };
            let opts = a.tuning.options();
            let stops = a.stops.tuning(a.shards);
            let sum = daily::run_daily(
                &env,
                &DailyRun {
                    select: &a.days.select(),
                    opts: &opts,
                    lookback_days: a.lookback_days,
                    track_shards: a.shards,
                    stops: &stops,
                    plan_only: a.days.plan,
                },
            )
            .await?;
            return Ok(finish(sum, a.apply, &output.namespace));
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
            let rest = connect_if_writing(a.apply, a.days.plan, &output).await?;
            let env = Env { catalog: &catalog, input: &input, output: &output, rest: rest.as_ref() };
            let tuning = a.stops.tuning(a.shards);
            let sum = daily::run_stop_segments(
                &env,
                &StopSegmentsRun { select: &a.days.select(), tuning: &tuning, plan_only: a.days.plan },
            )
            .await?;
            return Ok(finish(sum, a.apply, &output.namespace));
        }
        Command::Stops(a) => {
            let rest = connect_if_writing(a.apply, a.plan, &output).await?;
            let env = Env { catalog: &catalog, input: &input, output: &output, rest: rest.as_ref() };
            let sum = daily::run_stops(
                &env,
                &daily::StopsRun { full: a.full, scratch: &a.scratch, plan_only: a.plan },
            )
            .await?;
            return Ok(finish(sum, a.apply, &output.namespace));
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
