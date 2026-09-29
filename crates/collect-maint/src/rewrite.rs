//! Reading a set of live data files, sorting them, and writing them back out
//! as right-sized files — without committing (see [`crate::commit`]).

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::compute::{concat_batches, lexsort_to_indices, take_record_batch, SortColumn};
use arrow::record_batch::RecordBatch;
use futures_util::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::spec::{DataFile, DataFileFormat, PartitionKey, Struct};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

/// Row-group cap, matching `ais-parse`'s `writer_props`.
pub const MAX_ROW_GROUP_ROWS: usize = 128 * 1024;

/// A live data file in the table's current snapshot.
#[derive(Debug, Clone)]
pub struct LiveFile {
    pub path: String,
    pub size: u64,
    pub records: u64,
    pub partition: Option<Struct>,
}

/// All data files in the current snapshot (no delete-file support: this
/// project never writes deletes, and a table that has them is refused).
pub async fn live_files(table: &Table) -> Result<Vec<LiveFile>> {
    if table.metadata().current_snapshot().is_none() {
        return Ok(Vec::new());
    }
    let mut tasks = table.scan().select_all().build()?.plan_files().await?;
    let mut out = Vec::new();
    while let Some(task) = tasks.try_next().await? {
        anyhow::ensure!(
            task.deletes.is_empty(),
            "{} has delete files; compaction does not apply them",
            task.data_file_path
        );
        out.push(LiveFile {
            path: task.data_file_path,
            size: task.file_size_in_bytes,
            records: task.record_count.unwrap_or(0),
            partition: task.partition,
        });
    }
    Ok(out)
}

/// Reads exactly the named files into memory.
pub async fn read_files(table: &Table, paths: &HashSet<String>) -> Result<Vec<RecordBatch>> {
    let tasks: Vec<_> = table
        .scan()
        .select_all()
        .build()?
        .plan_files()
        .await?
        .try_filter(|t| futures_util::future::ready(paths.contains(&t.data_file_path)))
        .try_collect()
        .await?;
    anyhow::ensure!(
        tasks.len() == paths.len(),
        "planned {} of {} files to read",
        tasks.len(),
        paths.len()
    );
    let reader = ArrowReaderBuilder::new(table.file_io().clone()).build();
    let stream = futures_util::stream::iter(tasks.into_iter().map(Ok)).boxed();
    Ok(reader.read(stream)?.try_collect().await?)
}

/// Sorts all rows by `columns` (ascending, nulls last), returning one batch.
/// No columns means concatenate only.
pub fn sort_batches(batches: &[RecordBatch], columns: &[String]) -> Result<RecordBatch> {
    let schema = batches
        .first()
        .context("no batches to sort")?
        .schema();
    let all = concat_batches(&schema, batches)?;
    if columns.is_empty() {
        return Ok(all);
    }
    let sort_cols = columns
        .iter()
        .map(|name| {
            Ok(SortColumn {
                values: all
                    .column_by_name(name)
                    .with_context(|| format!("sort column '{name}' not in table"))?
                    .clone(),
                options: Some(arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let indices = lexsort_to_indices(&sort_cols, None)?;
    Ok(take_record_batch(&all, &indices)?)
}

/// Writes `batch` (already sorted) into one partition as rolling ~512 MiB
/// zstd files with row groups capped at [`MAX_ROW_GROUP_ROWS`] and bloom
/// filters on `bloom_columns`. The partition value comes from the input files
/// rather than the data, so it cannot be mislabelled.
pub async fn write_partition(
    table: &Table,
    partition: Option<Struct>,
    batch: RecordBatch,
    bloom_columns: &[&str],
) -> Result<Vec<DataFile>> {
    let metadata = table.metadata();
    let schema = metadata.current_schema();
    let location_gen = DefaultLocationGenerator::new(metadata.clone())?;
    let name_gen = DefaultFileNameGenerator::new(
        format!("compact-{}", uuid::Uuid::new_v4().simple()),
        Some("iceberg".to_string()),
        DataFileFormat::Parquet,
    );
    let mut props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_size(MAX_ROW_GROUP_ROWS);
    for col in bloom_columns {
        if schema.field_by_name(col).is_some() {
            props = props.set_column_bloom_filter_enabled(ColumnPath::from(*col), true);
        }
    }
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        ParquetWriterBuilder::new(props.build(), schema.clone()),
        table.file_io().clone(),
        location_gen,
        name_gen,
    );
    let key = partition.map(|p| {
        PartitionKey::new(
            metadata.default_partition_spec().as_ref().clone(),
            schema.clone(),
            p,
        )
    });
    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(key)
        .await
        .context("build writer")?;

    // Re-project onto the table's Arrow schema so field-id metadata matches
    // what the parquet writer expects.
    let target = Arc::new(iceberg::arrow::schema_to_arrow_schema(schema)?);
    let cols = batch
        .columns()
        .iter()
        .zip(target.fields())
        .map(|(c, f)| {
            if c.data_type() == f.data_type() {
                Ok(c.clone())
            } else {
                arrow::compute::cast(c, f.data_type()).map_err(anyhow::Error::from)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(target, cols)?;

    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = MAX_ROW_GROUP_ROWS.min(batch.num_rows() - offset);
        writer.write(batch.slice(offset, len)).await.context("write")?;
        offset += len;
    }
    Ok(writer.close().await.context("close")?)
}
