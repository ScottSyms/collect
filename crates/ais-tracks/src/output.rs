//! Writing a derived table: create it when missing, otherwise atomically
//! replace its contents with a single `replace` snapshot, so readers never see
//! a half-written table and a rerun is idempotent.

use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use collect_core::iceberg::{
    ensure_namespace, ensure_table, partition_spec_for, table_ident, IcebergConfig,
};
use collect_maint::commit::{prepare_replace, RestClient};
use collect_maint::rewrite::{live_files, write_partition};
use iceberg::spec::{DataFile, Literal, PartitionSpecBuilder, Schema, Struct};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::Catalog;

/// Same bound `ais-compact` uses when it has no reason to pick another.
const TARGET_FILE_BYTES: u64 = 512 << 20;
const MAX_COMMIT_ATTEMPTS: usize = 4;

#[derive(Debug, Default, Clone, Copy)]
pub struct WriteReport {
    pub rows: usize,
    pub files_added: usize,
    pub files_removed: usize,
    pub created: bool,
}

async fn discard(table: &Table, files: &[DataFile]) {
    for f in files {
        let _ = table.file_io().delete(f.file_path()).await;
    }
}

/// Replaces `base_name`'s contents with `batches` (already in the desired sort
/// order, columns in `schema`'s order), creating the unpartitioned table
/// first when it doesn't exist. `bloom` names columns to build bloom filters
/// on.
pub async fn replace_table(
    catalog: &impl Catalog,
    rest: &RestClient,
    config: &IcebergConfig,
    base_name: &str,
    schema: Schema,
    batches: &[RecordBatch],
    bloom: &[&str],
) -> Result<WriteReport> {
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // An empty result almost always means an empty or unreadable input, and
    // replacing a populated table with it would silently wipe the table.
    if rows == 0 {
        bail!("{base_name}: computed 0 rows; leaving the table untouched");
    }
    ensure_namespace(catalog, config).await?;
    let spec = PartitionSpecBuilder::new(schema.clone());
    let ident = table_ident(config, base_name);
    ensure_table(catalog, config, base_name, schema, spec).await?;

    let first = &batches[0];
    let all = concat_batches(&first.schema(), batches).context("concatenating batches")?;

    for attempt in 1..=MAX_COMMIT_ATTEMPTS {
        let table = catalog.load_table(&ident).await?;
        let live = live_files(&table).await?;
        let remove: HashSet<String> = live.iter().map(|f| f.path.clone()).collect();
        let added = write_partition(&table, None, all.clone(), TARGET_FILE_BYTES, bloom).await?;
        let mut report = WriteReport {
            rows,
            files_added: added.len(),
            files_removed: remove.len(),
            created: remove.is_empty(),
        };

        if remove.is_empty() {
            // Nothing to replace: a plain append is all iceberg-rust needs.
            let txn = Transaction::new(&table);
            let txn = txn.fast_append().add_data_files(added.clone()).apply(txn)?;
            match txn.commit(catalog).await {
                Ok(_) => return Ok(report),
                Err(e) => {
                    discard(&table, &added).await;
                    if attempt == MAX_COMMIT_ATTEMPTS {
                        return Err(e).context("committing first write");
                    }
                    eprintln!("  {base_name}: append failed ({e}), retrying ({attempt}/{MAX_COMMIT_ATTEMPTS})");
                    continue;
                }
            }
        }

        let prepared = match prepare_replace(&table, &remove, added.clone()).await {
            Ok(p) => p,
            Err(e) => {
                discard(&table, &added).await;
                return Err(e);
            }
        };
        if rest
            .commit(&ident, &prepared.requirements, &prepared.updates)
            .await?
        {
            report.created = false;
            return Ok(report);
        }
        discard(&table, &added).await;
        eprintln!("  {base_name}: table changed during write, retrying ({attempt}/{MAX_COMMIT_ATTEMPTS})");
    }
    bail!("{base_name}: gave up after {MAX_COMMIT_ATTEMPTS} conflicting commits")
}

/// Creates the namespace and a day-partitioned (on `ts`) table if missing.
pub async fn ensure_day_table(
    catalog: &impl Catalog,
    config: &IcebergConfig,
    base_name: &str,
    schema: Schema,
) -> Result<Table> {
    ensure_namespace(catalog, config).await?;
    let spec = partition_spec_for(&schema, "day")?;
    ensure_table(catalog, config, base_name, schema, spec).await
}

/// The Iceberg partition value for a day: days since 1970-01-01.
pub fn day_partition(days_since_epoch: i32) -> Struct {
    Struct::from_iter([Some(Literal::int(days_since_epoch))])
}

/// Writes one shard of a day into that day's partition and returns the files;
/// nothing is visible until [`commit_day`].
pub async fn write_day_shard(
    table: &Table,
    days_since_epoch: i32,
    batches: &[RecordBatch],
    bloom: &[&str],
) -> Result<Vec<DataFile>> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let all = concat_batches(&first.schema(), batches).context("concatenating batches")?;
    write_partition(
        table,
        Some(day_partition(days_since_epoch)),
        all,
        TARGET_FILE_BYTES,
        bloom,
    )
    .await
}

/// Publishes `added` as the whole content of one day's partition, replacing
/// whatever files it held, in a single snapshot. Other days are untouched, and
/// a rerun of the same day is idempotent. On failure the new files are deleted.
pub async fn commit_day(
    catalog: &impl Catalog,
    rest: &RestClient,
    config: &IcebergConfig,
    base_name: &str,
    days_since_epoch: i32,
    added: Vec<DataFile>,
    rows: usize,
) -> Result<WriteReport> {
    let ident = table_ident(config, base_name);
    let partition = day_partition(days_since_epoch);
    for attempt in 1..=MAX_COMMIT_ATTEMPTS {
        let table = catalog.load_table(&ident).await?;
        let remove: HashSet<String> = live_files(&table)
            .await?
            .into_iter()
            .filter(|f| f.partition.as_ref() == Some(&partition))
            .map(|f| f.path)
            .collect();
        let report = WriteReport {
            rows,
            files_added: added.len(),
            files_removed: remove.len(),
            created: remove.is_empty(),
        };

        if remove.is_empty() || table.metadata().current_snapshot().is_none() {
            let txn = Transaction::new(&table);
            let txn = txn.fast_append().add_data_files(added.clone()).apply(txn)?;
            match txn.commit(catalog).await {
                Ok(_) => return Ok(report),
                Err(e) if attempt < MAX_COMMIT_ATTEMPTS => {
                    eprintln!("  {base_name}: append failed ({e}), retrying ({attempt}/{MAX_COMMIT_ATTEMPTS})");
                    continue;
                }
                Err(e) => {
                    discard(&table, &added).await;
                    return Err(e).context("committing day");
                }
            }
        }

        let prepared = match prepare_replace(&table, &remove, added.clone()).await {
            Ok(p) => p,
            Err(e) => {
                discard(&table, &added).await;
                return Err(e);
            }
        };
        if rest
            .commit(&ident, &prepared.requirements, &prepared.updates)
            .await?
        {
            return Ok(report);
        }
        eprintln!("  {base_name}: table changed during commit, retrying ({attempt}/{MAX_COMMIT_ATTEMPTS})");
    }
    discard(&catalog.load_table(&ident).await?, &added).await;
    bail!("{base_name}: gave up after {MAX_COMMIT_ATTEMPTS} conflicting commits")
}
