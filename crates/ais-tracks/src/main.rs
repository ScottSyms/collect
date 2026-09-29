//! `ais-tracks`: builds derived Iceberg tables (vessel identity, track points,
//! tracks, stops and voyages) from the silver `positions` and `statics` tables.
//! Every command is a dry run unless `--apply` is given.

use anyhow::{Context, Result};
use ais_tracks::output::{commit_day, ensure_day_table, replace_table, write_day_shard};
use ais_tracks::track_points::{self, FlagCounts, Params, TABLE_TRACK_POINTS};
use ais_tracks::ports::{self, TABLE_REF_PORTS};
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
    /// Annotate each `positions` row (duplicates, movement, outlier flags),
    /// one day partition at a time.
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
    /// Print shell completions to stdout.
    Completions { shell: clap_complete::Shell },
}

#[derive(Args, Debug)]
struct ApplyArgs {
    /// Write the tables. Without it, compute and report only.
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct TrackPointsArgs {
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

    /// How many days before each day to search for a vessel's previous point.
    #[arg(long, default_value_t = 2)]
    lookback_days: i64,

    /// Implied speed above which a point is flagged as a speed jump.
    #[arg(long, default_value_t = 60.0)]
    max_speed_kn: f64,

    /// A gap longer than this many minutes marks `gap_before`.
    #[arg(long, default_value_t = 30)]
    gap_minutes: i64,

    /// Write the partitions. Without it, compute and report only.
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
            anyhow::ensure!(a.shards >= 1, "--shards must be at least 1");
            let to = a.to.unwrap_or(a.from);
            anyhow::ensure!(to >= a.from, "--to is before --from");
            let ctx = SessionContext::new();
            register(&ctx, &catalog, &input, TABLE_POSITIONS).await?;
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
                        TABLE_TRACK_POINTS,
                        track_points::track_points_schema(),
                    )
                    .await?,
                )
            } else {
                None
            };
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            let today = Utc::now().date_naive();
            let mut built_any = false;
            let mut day = a.from;
            while day <= to {
                if day >= today {
                    eprintln!("note: {day} is not over yet; it will be rebuilt as data arrives");
                }
                let params = Params {
                    day_start: Utc.from_utc_datetime(&day.and_hms_opt(0, 0, 0).expect("midnight")),
                    lookback: Duration::days(a.lookback_days),
                    shards: a.shards,
                    max_speed_kn: a.max_speed_kn,
                    gap: Duration::minutes(a.gap_minutes),
                };
                let mut counts = FlagCounts::new();
                let mut files = Vec::new();
                for shard in 0..a.shards {
                    let batches = track_points::build_shard(&ctx, &params, shard).await?;
                    counts.add(&batches)?;
                    if let Some(table) = &table {
                        let days = day.signed_duration_since(epoch).num_days() as i32;
                        files.extend(write_day_shard(table, days, &batches, &["mmsi"]).await?);
                    }
                }
                if counts.rows == 0 {
                    println!("{day}: no positions; leaving the partition untouched");
                    day = day.succ_opt().expect("date overflow");
                    continue;
                }
                built_any = true;
                let summary: Vec<String> = counts
                    .counts
                    .iter()
                    .filter(|(_, n)| *n > 0)
                    .map(|(name, n)| format!("{name}={n}"))
                    .collect();
                println!("{day}: {} rows; {}", counts.rows, summary.join(" "));
                if let Some(rest) = &rest {
                    let days = day.signed_duration_since(epoch).num_days() as i32;
                    let r = commit_day(
                        &catalog,
                        rest,
                        &output,
                        TABLE_TRACK_POINTS,
                        days,
                        files,
                        counts.rows,
                    )
                    .await?;
                    println!(
                        "  {}.{TABLE_TRACK_POINTS}: {} files written, {} replaced",
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
        Command::Completions { .. } => unreachable!(),
    }
    Ok(exitcode::SUCCESS)
}
