//! `ais-compact`: compaction, sorting, snapshot expiry and orphan cleanup for
//! the Iceberg tables `collect-*` and `*-parse` write, for catalogs (such as
//! RustFS's) that provide no maintenance service. Every mutating command is a
//! dry run unless `--apply` is given.

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use collect_core::iceberg::{
    open_catalog, table_ident, IcebergCliArgs, IcebergConfig, TABLE_ATONS, TABLE_BINARY,
    TABLE_METEO, TABLE_OTHER, TABLE_POSITIONS, TABLE_RAW, TABLE_STATICS,
};
use collect_core::{exitcode, S3ConnectionArgs, S3Storage};
use collect_maint::commit::{expire_commit, snapshots_to_expire, RestClient};
use collect_maint::compact::{compact_table, count_small_manifests, mib, CompactOptions};
use collect_maint::orphans;
use collect_maint::plan::{self, PlanOptions};
use collect_maint::rewrite::live_files;
use iceberg::Catalog;

const DAY_MS: i64 = 86_400_000;

#[derive(Parser, Debug)]
#[command(
    name = "ais-compact",
    version,
    about = "Compact, sort, expire and clean Iceberg tables (dry run unless --apply)"
)]
struct Cli {
    #[command(flatten)]
    iceberg: IcebergCliArgs,

    /// Table to operate on, by base name without the table prefix (repeatable).
    /// Default: raw, positions, statics, meteo, binary, atons, other.
    #[arg(long = "table", global = true)]
    tables: Vec<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Report file counts, sizes, snapshots and manifests per table.
    Inspect(InspectArgs),
    /// Merge small files per closed partition into sorted, right-sized files.
    Compact(CompactArgs),
    /// Drop old snapshots (metadata only; run `orphans` afterwards to free space).
    Expire(ExpireArgs),
    /// Delete objects under the table location that no snapshot references.
    Orphans(OrphanArgs),
    /// Print shell completions to stdout.
    Completions { shell: clap_complete::Shell },
}

#[derive(Args, Debug)]
struct InspectArgs {
    /// Target file size in MiB, used to count undersized files.
    #[arg(long, default_value_t = 512)]
    target_file_mb: u64,
}

#[derive(Args, Debug)]
struct CompactArgs {
    /// Actually rewrite and commit. Without this, only report the plan.
    #[arg(long)]
    apply: bool,
    /// Only touch partitions that ended at least this many hours ago.
    #[arg(long, default_value_t = 2)]
    min_age_hours: i64,
    /// Target data file size in MiB.
    #[arg(long, default_value_t = 512)]
    target_file_mb: u64,
    /// Skip partitions whose compressed input exceeds this many MiB; a
    /// partition is sorted in memory, which needs several times this.
    #[arg(long, default_value_t = 1024)]
    max_partition_mb: u64,
    /// Sort columns, comma-separated. Default: mmsi,ts (those the table has).
    #[arg(long, value_delimiter = ',')]
    sort_by: Vec<String>,
    /// Consolidate small manifests when at least this many exist (0 = never).
    #[arg(long, default_value_t = 20)]
    consolidate_manifests: usize,
}

#[derive(Args, Debug)]
struct ExpireArgs {
    #[arg(long)]
    apply: bool,
    /// Expire snapshots older than this many days.
    #[arg(long, default_value_t = 7)]
    older_than_days: i64,
    /// Always keep at least this many of the newest snapshots.
    #[arg(long, default_value_t = 5)]
    retain_last: usize,
}

#[derive(Args, Debug)]
struct OrphanArgs {
    #[arg(long)]
    apply: bool,
    /// Only delete unreferenced objects older than this many days, so an
    /// in-flight write (uploaded, not yet committed) is never removed.
    #[arg(long, default_value_t = 3)]
    older_than_days: i64,
    #[command(flatten)]
    s3: S3ConnectionArgs,
}

fn table_names(cli: &Cli) -> Vec<String> {
    if cli.tables.is_empty() {
        [
            TABLE_RAW,
            TABLE_POSITIONS,
            TABLE_STATICS,
            TABLE_METEO,
            TABLE_BINARY,
            TABLE_ATONS,
            TABLE_OTHER,
        ]
        .map(String::from)
        .to_vec()
    } else {
        cli.tables.clone()
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
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

async fn run() -> Result<i32> {
    let cli = Cli::parse();
    if let Command::Completions { shell } = &cli.command {
        collect_core::print_completions::<Cli>(*shell, "ais-compact");
        return Ok(exitcode::SUCCESS);
    }
    cli.iceberg.validate()?;
    anyhow::ensure!(
        cli.iceberg.is_iceberg_mode(),
        "--iceberg-catalog-uri is required"
    );
    let config = IcebergConfig::from(&cli.iceberg);
    let catalog = open_catalog(&config).await?;
    let rest = RestClient::connect(&config).await?;

    let mut failed = 0usize;
    let mut worked = 0usize;
    for name in table_names(&cli) {
        let ident = table_ident(&config, &name);
        let table = match catalog.load_table(&ident).await {
            Ok(t) => t,
            Err(e) => {
                eprintln!("{name}: cannot load ({e}); skipping");
                continue;
            }
        };
        println!("{name}:");
        let result: Result<usize> = match &cli.command {
            Command::Inspect(a) => inspect(&table, a).await.map(|_| 0),
            Command::Compact(a) => {
                let opts = CompactOptions {
                    plan: PlanOptions {
                        now_ms: now_ms(),
                        min_age_ms: a.min_age_hours * 3_600_000,
                        target_bytes: a.target_file_mb << 20,
                        max_partition_bytes: a.max_partition_mb << 20,
                    },
                    sort_by: a.sort_by.clone(),
                    apply: a.apply,
                    consolidate_min_manifests: a.consolidate_manifests,
                };
                compact_table(&catalog, &rest, &config, &name, &opts).await.map(|r| {
                    let change = if a.apply {
                        format!(
                            "{} files -> {} files ({:.1} -> {:.1} MiB)",
                            r.files_in, r.files_out, mib(r.bytes_in), mib(r.bytes_out)
                        )
                    } else {
                        format!("{} files ({:.1} MiB) as input", r.files_in, mib(r.bytes_in))
                    };
                    println!(
                        "  {} partitions {}, {change}; skipped: {} open, {} already fine, {} too big{}{}",
                        r.partitions_rewritten,
                        if a.apply { "rewritten" } else { "to rewrite" },
                        r.skipped_open, r.skipped_fine, r.skipped_too_big,
                        if r.sort_order_registered { "; sort order registered" } else { "" },
                        if r.manifests_replaced > 0 {
                            format!("; {} small manifests {}", r.manifests_replaced, if a.apply { "consolidated" } else { "to consolidate" })
                        } else { String::new() },
                    );
                    failed += r.partitions_failed;
                    r.partitions_rewritten + r.manifests_replaced
                })
            }
            Command::Expire(a) => expire(&rest, &ident, &table, a).await,
            Command::Orphans(a) => orphans_cmd(&table, a).await,
            Command::Completions { .. } => unreachable!(),
        };
        match result {
            Ok(n) => worked += n,
            Err(e) => {
                eprintln!("  FAILED: {e:#}");
                failed += 1;
            }
        }
    }

    Ok(if failed > 0 {
        exitcode::PARTIAL_FAILURE_THRESHOLD
    } else if worked == 0 && !matches!(cli.command, Command::Inspect(_)) {
        exitcode::NOTHING_TO_DO
    } else {
        exitcode::SUCCESS
    })
}

async fn inspect(table: &iceberg::table::Table, a: &InspectArgs) -> Result<()> {
    let files = live_files(table).await?;
    let total: u64 = files.iter().map(|f| f.size).sum();
    let records: u64 = files.iter().map(|f| f.records).sum();
    let target = a.target_file_mb << 20;
    let small = files.iter().filter(|f| f.size < target / 2).count();
    let plan_opts = PlanOptions {
        now_ms: now_ms(),
        min_age_ms: 0,
        target_bytes: target,
        max_partition_bytes: u64::MAX,
    };
    let (needs, fine) = plan::plan(table, files.clone(), plan_opts);
    let sort = table.metadata().default_sort_order();
    println!(
        "  {} data files, {:.1} MiB, {} rows; {} under {} MiB",
        files.len(),
        mib(total),
        records,
        small,
        a.target_file_mb / 2
    );
    println!(
        "  {} partitions need compaction, {} are fine; {} snapshots, {} small manifests; sort order: {}",
        needs.len(),
        fine.len(),
        table.metadata().snapshots().count(),
        count_small_manifests(table).await?,
        if sort.is_unsorted() { "unsorted".to_string() } else { format!("{} field(s)", sort.fields.len()) },
    );
    Ok(())
}

async fn expire(
    rest: &RestClient,
    ident: &iceberg::TableIdent,
    table: &iceberg::table::Table,
    a: &ExpireArgs,
) -> Result<usize> {
    let ids = snapshots_to_expire(table, now_ms() - a.older_than_days * DAY_MS, a.retain_last);
    if ids.is_empty() {
        println!("  no snapshots to expire");
        return Ok(0);
    }
    if !a.apply {
        println!(
            "  would expire {} of {} snapshots",
            ids.len(),
            table.metadata().snapshots().count()
        );
        return Ok(ids.len());
    }
    let n = ids.len();
    let (req, upd) = expire_commit(table, ids);
    anyhow::ensure!(
        rest.commit(ident, &req, &upd).await?,
        "table changed; rerun"
    );
    println!("  expired {n} snapshots");
    Ok(n)
}

async fn orphans_cmd(table: &iceberg::table::Table, a: &OrphanArgs) -> Result<usize> {
    let (bucket, _) = orphans::split_location(table.metadata().location())?;
    let storage = S3Storage::new(
        bucket,
        String::new(),
        a.s3.s3_region.clone(),
        a.s3.s3_endpoint.clone(),
        a.s3.s3_access_key.clone(),
        a.s3.s3_secret_key.clone(),
        true,
        a.s3.s3_disable_tls,
    )
    .await?;
    let found = orphans::find(table, &storage, now_ms() - a.older_than_days * DAY_MS).await?;
    let bytes: u64 = found.iter().map(|o| o.size).sum();
    if !a.apply {
        println!(
            "  would delete {} orphaned objects ({:.1} MiB)",
            found.len(),
            mib(bytes)
        );
        return Ok(found.len());
    }
    let (bucket, _) = orphans::split_location(table.metadata().location())?;
    for o in &found {
        table
            .file_io()
            .delete(format!("s3://{bucket}/{}", o.key))
            .await
            .with_context(|| format!("deleting {}", o.key))?;
    }
    println!(
        "  deleted {} orphaned objects ({:.1} MiB)",
        found.len(),
        mib(bytes)
    );
    Ok(found.len())
}
