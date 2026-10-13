//! Compaction driver: per closed partition, read → sort → write → replace.

use std::time::Instant;

use anyhow::{bail, Context, Result};
use collect_core::iceberg::{table_ident, IcebergConfig};
use iceberg::spec::{NullOrder, SortDirection};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};

use crate::commit::{prepare_consolidate, prepare_replace, RestClient, SMALL_MANIFEST_BYTES};
use crate::plan::{self, PartitionPlan, PlanOptions, Skip, BLOOM_COLUMNS};
use crate::progress::{fmt_bytes, fmt_duration, Heartbeat, Throughput};
use crate::rewrite::{live_files, read_files, sort_batches, write_partition};

/// Concurrent-ingest commits race ours; each retry reloads and re-plans.
const MAX_COMMIT_ATTEMPTS: usize = 4;

#[derive(Debug, Clone)]
pub struct CompactOptions {
    pub plan: PlanOptions,
    pub sort_by: Vec<String>,
    pub apply: bool,
    /// Consolidate small manifests when at least this many exist (0 = never).
    pub consolidate_min_manifests: usize,
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

/// Rewrites one partition. `Ok(None)` means it no longer needs work (someone
/// else compacted it, or it vanished) after a reload. Prints a line of phase
/// timings when it commits, and a heartbeat while any phase runs long.
async fn rewrite_partition(
    catalog: &impl Catalog,
    rest: &RestClient,
    ident: &TableIdent,
    key: &str,
    label: &str,
    sort: &[String],
    opts: &CompactOptions,
) -> Result<Option<(PartitionPlan, usize, u64)>> {
    let hb = Heartbeat::start(label);
    for attempt in 1..=MAX_COMMIT_ATTEMPTS {
        hb.set("planning");
        let t = Instant::now();
        let table = catalog.load_table(ident).await?;
        let (plans, _) = plan::plan(&table, live_files(&table).await?, opts.plan);
        let Some(p) = plans.into_iter().find(|p| p.key == key) else {
            return Ok(None);
        };
        let t_plan = t.elapsed();

        hb.set("reading");
        let t = Instant::now();
        let paths = p.files.iter().map(|f| f.path.clone()).collect();
        let batches = read_files(&table, &paths).await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let decoded: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        let t_read = t.elapsed();

        hb.set("sorting");
        let t = Instant::now();
        let sorted = sort_batches(&batches, sort)?;
        drop(batches);
        let t_sort = t.elapsed();

        hb.set("writing");
        let t = Instant::now();
        let added = write_partition(
            &table,
            p.partition.clone(),
            sorted,
            opts.plan.target_bytes,
            &BLOOM_COLUMNS,
        )
        .await?;
        let t_write = t.elapsed();
        let out_bytes = added.iter().map(|f| f.file_size_in_bytes()).sum();
        let out_files = added.len();

        hb.set("committing");
        let t = Instant::now();
        let prepared = match prepare_replace(&table, &paths, added.clone()).await {
            Ok(v) => v,
            Err(e) => {
                discard(&table, &added).await;
                return Err(e);
            }
        };
        if rest
            .commit(ident, &prepared.requirements, &prepared.updates)
            .await?
        {
            println!(
                "    plan {} · read {} ({rows} rows, {} decoded) · sort {} · write {} · commit {}",
                fmt_duration(t_plan),
                fmt_duration(t_read),
                fmt_bytes(decoded as u64),
                fmt_duration(t_sort),
                fmt_duration(t_write),
                fmt_duration(t.elapsed()),
            );
            return Ok(Some((p, out_files, out_bytes)));
        }
        discard(&table, &added).await;
        eprintln!(
            "  {}: table changed during rewrite, retrying ({attempt}/{MAX_COMMIT_ATTEMPTS})",
            p.label
        );
    }
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

    let t = Instant::now();
    let (plans, skipped) = plan::plan(&table, live_files(&table).await?, opts.plan);
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

    let n = plans.len();
    let mut progress = Throughput::new(total_bytes);
    for (i, p) in plans.into_iter().enumerate() {
        if !opts.apply {
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
            continue;
        }
        println!(
            "  [{}/{n}] {}: {} files, {}",
            i + 1,
            p.label,
            p.files.len(),
            fmt_bytes(p.input_bytes)
        );
        let planned_bytes = p.input_bytes;
        match rewrite_partition(catalog, rest, &ident, &p.key, &p.label, &sort, opts).await {
            Ok(Some((p, out_files, out_bytes))) => {
                println!(
                    "  rewrote {}: {} files ({:.1} MiB) -> {} files ({:.1} MiB)",
                    p.label,
                    p.files.len(),
                    mib(p.input_bytes),
                    out_files,
                    mib(out_bytes)
                );
                report.partitions_rewritten += 1;
                report.files_in += p.files.len();
                report.bytes_in += p.input_bytes;
                report.files_out += out_files;
                report.bytes_out += out_bytes;
            }
            Ok(None) => println!("    {}: no longer needs work, skipped", p.label),
            Err(e) => {
                eprintln!("  {}: FAILED: {e:#}", p.label);
                report.partitions_failed += 1;
            }
        }
        progress.add(planned_bytes);
        println!("    {}", progress.line());
    }

    if opts.consolidate_min_manifests > 0 {
        let table = catalog.load_table(&ident).await?;
        if opts.apply {
            println!("  checking manifests to consolidate...");
            if let Some((req, upd, replaced)) =
                prepare_consolidate(&table, opts.consolidate_min_manifests).await?
            {
                if rest.commit(&ident, &req, &upd).await? {
                    report.manifests_replaced = replaced;
                    println!("  consolidated {replaced} small manifests");
                } else {
                    eprintln!("  manifest consolidation skipped: table changed (rerun)");
                }
            }
        } else {
            report.manifests_replaced = count_small_manifests(&table).await?;
            if report.manifests_replaced < opts.consolidate_min_manifests {
                report.manifests_replaced = 0;
            }
        }
    }
    Ok(report)
}

pub async fn count_small_manifests(table: &Table) -> Result<usize> {
    let Some(snap) = table.metadata().current_snapshot() else {
        return Ok(0);
    };
    let list = snap
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await?;
    Ok(list
        .entries()
        .iter()
        .filter(|m| {
            m.content == iceberg::spec::ManifestContentType::Data
                && m.manifest_length < SMALL_MANIFEST_BYTES
        })
        .count())
}
