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

/// Writes one day's batches into that day's partition as they arrive, rolling
/// to a new file at the target size. Nothing is visible until the files it
/// returns are passed to [`commit_day`].
///
/// [`write_day_shard`] needs the whole shard in one batch; this takes any
/// number of small ones, so a day can be streamed through in bounded memory.
pub struct DayWriter {
    writer: Box<dyn iceberg::writer::IcebergWriter>,
    target: std::sync::Arc<arrow::datatypes::Schema>,
    pub rows: usize,
}

impl DayWriter {
    pub async fn new(table: &Table, days_since_epoch: i32, bloom: &[&str]) -> Result<Self> {
        use iceberg::spec::{DataFileFormat, PartitionKey};
        use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
        use iceberg::writer::file_writer::location_generator::{
            DefaultFileNameGenerator, DefaultLocationGenerator,
        };
        use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
        use iceberg::writer::file_writer::ParquetWriterBuilder;
        use iceberg::writer::IcebergWriterBuilder;
        use parquet::basic::{Compression, ZstdLevel};
        use parquet::file::properties::WriterProperties;
        use parquet::schema::types::ColumnPath;

        let metadata = table.metadata();
        let schema = metadata.current_schema();
        let location_gen = DefaultLocationGenerator::new(metadata.clone())?;
        let name_gen = DefaultFileNameGenerator::new(
            format!("reduce-{}", uuid::Uuid::new_v4().simple()),
            Some("iceberg".to_string()),
            DataFileFormat::Parquet,
        );
        let mut props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_size(collect_maint::rewrite::MAX_ROW_GROUP_ROWS);
        for col in bloom {
            if schema.field_by_name(col).is_some() {
                props = props.set_column_bloom_filter_enabled(ColumnPath::from(*col), true);
            }
        }
        let rolling = RollingFileWriterBuilder::new(
            ParquetWriterBuilder::new(props.build(), schema.clone()),
            TARGET_FILE_BYTES as usize,
            table.file_io().clone(),
            location_gen,
            name_gen,
        );
        let key = PartitionKey::new(
            metadata.default_partition_spec().as_ref().clone(),
            schema.clone(),
            day_partition(days_since_epoch),
        );
        let writer = DataFileWriterBuilder::new(rolling)
            .build(Some(key))
            .await
            .context("build writer")?;
        Ok(Self {
            writer: Box::new(writer),
            target: std::sync::Arc::new(iceberg::arrow::schema_to_arrow_schema(schema)?),
            rows: 0,
        })
    }

    /// Writes one batch (columns in the table's order; types are cast to it).
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let cols = batch
            .columns()
            .iter()
            .zip(self.target.fields())
            .map(|(c, f)| {
                if c.data_type() == f.data_type() {
                    Ok(c.clone())
                } else {
                    arrow::compute::cast(c, f.data_type()).map_err(anyhow::Error::from)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let projected = RecordBatch::try_new(self.target.clone(), cols)?;
        self.rows += projected.num_rows();
        self.writer.write(projected).await.context("write")?;
        Ok(())
    }

    /// Closes the writer and returns the files, ready for [`commit_day`].
    pub async fn finish(mut self) -> Result<Vec<DataFile>> {
        self.writer.close().await.context("close")
    }
}
