//! Compaction driver: plan once, then per closed partition read → sort →
//! write (several partitions in parallel) → replace (one at a time).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use collect_core::iceberg::{table_ident, IcebergConfig};
use futures_util::StreamExt;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFile, NullOrder, SortDirection};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};
use tokio::sync::Semaphore;

use crate::commit::{
    merge_small_manifests, prepare_replace_indexed, seal_consolidation, ManifestIndex, RestClient,
    SMALL_MANIFEST_BYTES,
};
use crate::plan::{self, PartitionPlan, PlanOptions, Skip, BLOOM_COLUMNS};
use crate::progress::{fmt_bytes, fmt_duration, Heartbeat, Throughput};
use crate::rewrite::{read_tasks, scan_tasks, sort_batches, write_partition, LiveFile};

/// Concurrent-ingest commits race ours; each retry reloads the table and
/// rebuilds the commit.
const MAX_COMMIT_ATTEMPTS: usize = 4;

#[derive(Debug, Clone)]
pub struct CompactOptions {
    pub plan: PlanOptions,
    pub sort_by: Vec<String>,
    pub apply: bool,
    /// Consolidate small manifests when at least this many exist (0 = never).
    pub consolidate_min_manifests: usize,
    /// Partitions rewritten at once.
    pub jobs: usize,
    /// Compressed input of the partitions being rewritten at once; bounds
    /// memory, since each is sorted in memory at several times this size.
    pub max_inflight_bytes: u64,
}

#[derive(Debug, Default, Clone)]
pub struct CompactReport {
    pub partitions_rewritten: usize,
    pub partitions_failed: usize,
    pub files_in: usize,
    pub files_out: usize,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub skipped_open: usize,
    pub skipped_fine: usize,
    pub skipped_too_big: usize,
    pub sort_order_registered: bool,
    pub manifests_replaced: usize,
}

pub fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// Registers `columns` (ascending, nulls last) as the table's default sort
/// order when it isn't already. Returns whether it changed anything.
async fn ensure_sort_order(
    catalog: &impl Catalog,
    table: &Table,
    columns: &[String],
) -> Result<bool> {
    if columns.is_empty() {
        return Ok(false);
    }
    let schema = table.metadata().current_schema();
    let want: Vec<i32> = columns
        .iter()
        .map(|c| {
            schema
                .field_id_by_name(c)
                .with_context(|| format!("no column '{c}'"))
        })
        .collect::<Result<_>>()?;
    let have: Vec<i32> = table
        .metadata()
        .default_sort_order()
        .fields
        .iter()
        .filter(|f| f.direction == SortDirection::Ascending)
        .map(|f| f.source_id)
        .collect();
    if have == want {
        return Ok(false);
    }
    let txn = Transaction::new(table);
    let mut action = txn.replace_sort_order();
    for c in columns {
        action = action.asc(c, NullOrder::Last);
    }
    let txn = action.apply(txn)?;
    txn.commit(catalog)
        .await
        .context("registering sort order")?;
    Ok(true)
}

async fn discard(table: &Table, files: &[iceberg::spec::DataFile]) {
    for f in files {
        let _ = table.file_io().delete(f.file_path()).await;
    }
}

/// A partition rewritten into new files that aren't committed yet.
struct Rewritten {
    added: Vec<DataFile>,
    rows: usize,
    decoded: u64,
    t_read: Duration,
    t_sort: Duration,
    t_write: Duration,
}

/// Reads, sorts and writes one partition from scan tasks planned up front.
/// Runs as its own tokio task so partitions rewrite in parallel, with the
/// sort on the blocking pool. Nothing is committed (see [`commit_rewrite`]).
async fn rewrite_partition(
    table: &Table,
    p: &PartitionPlan,
    tasks: Vec<FileScanTask>,
    sort: Arc<Vec<String>>,
    target_bytes: u64,
) -> Result<Rewritten> {
    anyhow::ensure!(
        tasks.len() == p.files.len(),
        "planned {} of {} files to read",
        tasks.len(),
        p.files.len()
    );
    let hb = Heartbeat::start(&p.label);

    hb.set("reading");
    let t = Instant::now();
    let batches = read_tasks(table, tasks).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let decoded = batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum();
    let t_read = t.elapsed();

    hb.set("sorting");
    let t = Instant::now();
    // Moving `batches` in frees them as soon as the sorted copy exists.
    let sorted = tokio::task::spawn_blocking(move || sort_batches(&batches, &sort)).await??;
    let t_sort = t.elapsed();

    hb.set("writing");
    let t = Instant::now();
    let added = write_partition(
        table,
        p.partition.clone(),
        sorted,
        target_bytes,
        &BLOOM_COLUMNS,
    )
    .await?;
    Ok(Rewritten {
        added,
        rows,
        decoded,
        t_read,
        t_sort,
        t_write: t.elapsed(),
    })
}

/// Commits a rewritten partition as a `replace` of exactly the files it
/// read. A concurrent commit (409) only invalidates the snapshot the commit
/// was built on, not the rewritten files, so a retry reloads the table and
/// rebuilds the commit without rewriting. If the input files are no longer
/// live (someone else rewrote them), the new files are deleted.
async fn commit_rewrite(
    catalog: &impl Catalog,
    rest: &RestClient,
    ident: &TableIdent,
    p: &PartitionPlan,
    added: &[DataFile],
    index: &mut ManifestIndex,
) -> Result<()> {
    let paths: HashSet<String> = p.files.iter().map(|f| f.path.clone()).collect();
    let mut table = catalog.load_table(ident).await?;
    for attempt in 1..=MAX_COMMIT_ATTEMPTS {
        if attempt > 1 {
            table = catalog.load_table(ident).await?;
        }
        let prepared = match prepare_replace_indexed(&table, &paths, added.to_vec(), index).await {
            Ok(v) => v,
            Err(e) => {
                discard(&table, added).await;
                return Err(e);
            }
        };
        if rest
            .commit(ident, &prepared.requirements, &prepared.updates)
            .await?
        {
            return Ok(());
        }
        eprintln!(
            "  {}: table changed before commit, retrying ({attempt}/{MAX_COMMIT_ATTEMPTS})",
            p.label
        );
    }
    discard(&table, added).await;
    bail!("gave up after {MAX_COMMIT_ATTEMPTS} conflicting commits")
}

pub async fn compact_table(
    catalog: &impl Catalog,
    rest: &RestClient,
    config: &IcebergConfig,
    base_name: &str,
    opts: &CompactOptions,
) -> Result<CompactReport> {
    let ident = table_ident(config, base_name);
    let mut table = catalog.load_table(&ident).await?;
    let sort = plan::sort_columns(table.metadata().current_schema(), &opts.sort_by)?;
    let mut report = CompactReport::default();

    if opts.apply && ensure_sort_order(catalog, &table, &sort).await? {
        report.sort_order_registered = true;
        table = catalog.load_table(&ident).await?;
    }

    // Plan once: scanning reads every manifest, which on a table built from
    // one commit per ingested file is the most expensive step there is.
    let t = Instant::now();
    let tasks = scan_tasks(&table).await?;
    let live = tasks.iter().map(LiveFile::from).collect();
    let (plans, skipped) = plan::plan(&table, live, opts.plan);
    let total_bytes: u64 = plans.iter().map(|p| p.input_bytes).sum();
    println!(
        "  planned in {}: {} partitions need work ({}), {} skipped",
        fmt_duration(t.elapsed()),
        plans.len(),
        fmt_bytes(total_bytes),
        skipped.len()
    );
    for (label, why) in &skipped {
        match why {
            Skip::Open => report.skipped_open += 1,
            Skip::Fine => report.skipped_fine += 1,
            Skip::TooBig => {
                report.skipped_too_big += 1;
                eprintln!("  {label}: skipped, exceeds --max-partition-mb");
            }
        }
    }

    if !opts.apply {
        for p in plans {
            println!(
                "  would rewrite {}: {} files, {:.1} MiB, sorted by [{}]",
                p.label,
                p.files.len(),
                mib(p.input_bytes),
                sort.join(", ")
            );
            report.partitions_rewritten += 1;
            report.files_in += p.files.len();
            report.bytes_in += p.input_bytes;
        }
    } else {
        rewrite_all(
            catalog,
            rest,
            &ident,
            &table,
            tasks,
            plans,
            sort,
            total_bytes,
            opts,
            &mut report,
        )
        .await;
    }

    if opts.consolidate_min_manifests > 0 {
        if opts.apply {
            match consolidate_manifests(catalog, rest, &ident, opts.consolidate_min_manifests).await
            {
                Ok(replaced) => report.manifests_replaced = replaced,
                Err(e) => {
                    eprintln!("  manifest consolidation FAILED: {e:#}");
                    report.partitions_failed += 1;
                }
            }
        } else {
            let table = catalog.load_table(&ident).await?;
            let small = manifest_counts(&table).await?.small;
            if small >= opts.consolidate_min_manifests {
                report.manifests_replaced = small;
            }
        }
    }
    Ok(report)
}

/// Rewrites `plans` with up to `opts.jobs` partitions in flight and at most
/// `opts.max_inflight_bytes` of compressed input among them (a partition
/// larger than that runs alone), committing each as it finishes. Commits are
/// serial: each builds on the snapshot the previous one made.
#[allow(clippy::too_many_arguments)]
async fn rewrite_all(
    catalog: &impl Catalog,
    rest: &RestClient,
    ident: &TableIdent,
    table: &Table,
    tasks: Vec<FileScanTask>,
    plans: Vec<PartitionPlan>,
    sort: Vec<String>,
    total_bytes: u64,
    opts: &CompactOptions,
    report: &mut CompactReport,
) {
    let n = plans.len();
    let sort = Arc::new(sort);
    let budget_mib = (opts.max_inflight_bytes >> 20).clamp(1, u32::MAX as u64) as u32;
    let budget = Arc::new(Semaphore::new(budget_mib as usize));
    let target_bytes = opts.plan.target_bytes;
    let mut by_path: HashMap<String, FileScanTask> = tasks
        .into_iter()
        .map(|t| (t.data_file_path.clone(), t))
        .collect();

    let spawn = |(i, p): (usize, PartitionPlan)| {
        let tasks: Vec<_> = p
            .files
            .iter()
            .filter_map(|f| by_path.remove(&f.path))
            .collect();
        let (table, sort, budget) = (table.clone(), sort.clone(), budget.clone());
        tokio::spawn(async move {
            let permits = (p.input_bytes.div_ceil(1 << 20) as u32).clamp(1, budget_mib);
            let _held = budget
                .acquire_many_owned(permits)
                .await
                .expect("budget semaphore is never closed");
            println!(
                "  [{}/{n}] {}: {} files, {}",
                i + 1,
                p.label,
                p.files.len(),
                fmt_bytes(p.input_bytes)
            );
            let result = rewrite_partition(&table, &p, tasks, sort, target_bytes).await;
            (p, result)
        })
    };
    let mut finished = futures_util::stream::iter(plans.into_iter().enumerate())
        .map(spawn)
        .buffer_unordered(opts.jobs.max(1));

    let mut index = ManifestIndex::default();
    let mut progress = Throughput::new(total_bytes);
    while let Some(joined) = finished.next().await {
        let (p, result) = match joined {
            Ok(v) => v,
            Err(e) => {
                eprintln!("  a partition rewrite panicked: {e}");
                report.partitions_failed += 1;
                continue;
            }
        };
        let committed = match result {
            Ok(r) => {
                let hb = Heartbeat::start(&p.label);
                hb.set("committing");
                let t = Instant::now();
                commit_rewrite(catalog, rest, ident, &p, &r.added, &mut index)
                    .await
                    .map(|()| (r, t.elapsed()))
            }
            Err(e) => Err(e),
        };
        match committed {
            Ok((r, t_commit)) => {
                let out_bytes = r.added.iter().map(|f| f.file_size_in_bytes()).sum();
                println!(
                    "  rewrote {}: {} files ({:.1} MiB) -> {} files ({:.1} MiB)",
                    p.label,
                    p.files.len(),
                    mib(p.input_bytes),
                    r.added.len(),
                    mib(out_bytes)
                );
                println!(
                    "    read {} ({} rows, {} decoded) · sort {} · write {} · commit {}",
                    fmt_duration(r.t_read),
                    r.rows,
                    fmt_bytes(r.decoded),
                    fmt_duration(r.t_sort),
                    fmt_duration(r.t_write),
                    fmt_duration(t_commit),
                );
                report.partitions_rewritten += 1;
                report.files_in += p.files.len();
                report.bytes_in += p.input_bytes;
                report.files_out += r.added.len();
                report.bytes_out += out_bytes;
            }
            Err(e) => {
                eprintln!("  {}: FAILED: {e:#}", p.label);
                report.partitions_failed += 1;
            }
        }
        progress.add(p.input_bytes);
        println!("    {}", progress.line());
    }
}

/// Manifests in a table's current snapshot.
#[derive(Debug, Default, Clone, Copy)]
pub struct ManifestCounts {
    pub total: usize,
    /// Data manifests under [`SMALL_MANIFEST_BYTES`]: what consolidation merges.
    pub small: usize,
}

pub async fn manifest_counts(table: &Table) -> Result<ManifestCounts> {
    let Some(snap) = table.metadata().current_snapshot() else {
        return Ok(ManifestCounts::default());
    };
    let list = snap
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await?;
    Ok(ManifestCounts {
        total: list.entries().len(),
        small: list
            .entries()
            .iter()
            .filter(|m| {
                m.content == iceberg::spec::ManifestContentType::Data
                    && m.manifest_length < SMALL_MANIFEST_BYTES
            })
            .count(),
    })
}

/// Merges the table's small manifests and publishes the result. Merging
/// reads every small manifest, which takes long enough that live ingest
/// usually commits meanwhile, so each attempt publishes on the freshly
/// reloaded table (keeping manifests appended since) without redoing the
/// merge. Returns how many manifests were replaced: 0 when fewer than
/// `min_manifests` small ones exist.
pub async fn consolidate_manifests(
    catalog: &impl Catalog,
    rest: &RestClient,
    ident: &TableIdent,
    min_manifests: usize,
) -> Result<usize> {
    let table = catalog.load_table(ident).await?;
    let hb = Heartbeat::start(&format!("{} manifests", ident.name()));
    hb.set("merging");
    let t = Instant::now();
    let Some(merged) = merge_small_manifests(&table, min_manifests).await? else {
        return Ok(0);
    };
    hb.set("committing");
    for attempt in 1..=MAX_COMMIT_ATTEMPTS {
        let table = catalog.load_table(ident).await?;
        let (req, upd) = seal_consolidation(&table, &merged, attempt as u32).await?;
        if rest.commit(ident, &req, &upd).await? {
            println!(
                "  consolidated {} small manifests in {}",
                merged.replaced(),
                fmt_duration(t.elapsed())
            );
            return Ok(merged.replaced());
        }
        eprintln!(
            "  manifest consolidation: table changed before commit, retrying \
             ({attempt}/{MAX_COMMIT_ATTEMPTS})"
        );
    }
    bail!("manifest consolidation gave up after {MAX_COMMIT_ATTEMPTS} conflicting commits")
}
