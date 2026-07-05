use anyhow::{Context, Result};
use arrow::array::{StringBuilder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use collect_core::sort_record_batch_by_ts;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) const FLUSH_BATCH_SIZE: usize = 65_536;

/// Hard cap on simultaneously open partition writers per pool. Rows with
/// badly scattered timestamps could otherwise fan one input partition out
/// across an unbounded number of output partitions, each holding builders
/// and an open Parquet writer. When the cap is hit every open writer is
/// closed (a "generation"); a partition seen again later simply starts a
/// new file, which the Hive layout already permits.
const MAX_OPEN_WRITERS: usize = 64;

pub(crate) fn parquet_file_name() -> String {
    let now = Utc::now();
    use chrono::{Datelike, Timelike};
    let counter = FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "norm-{:04}{:02}{:02}T{:02}{:02}{:02}{:03}-{:06}.parquet",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.timestamp_subsec_millis(),
        counter
    )
}

pub(crate) fn build_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        // Provenance retained as a column (the dataset is no longer
        // partitioned by source). Constant within one output file, so it
        // dictionary-encodes to almost nothing.
        Field::new("source", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

pub(crate) fn open_writer(
    path: &Path,
    schema: &Arc<Schema>,
    compression_level: i32,
) -> Result<ArrowWriter<File>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let zstd_level = ZstdLevel::try_new(compression_level).context("invalid Zstd level")?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd_level))
        // One row group per flushed (sorted) batch: keeps every row group
        // sorted by ts so collect-maint compaction can stream-merge these
        // files, and bounds writer memory to one batch per open partition.
        .set_max_row_group_size(FLUSH_BATCH_SIZE)
        .set_column_encoding(ColumnPath::from("ts"), Encoding::DELTA_BINARY_PACKED)
        .set_column_encoding(
            ColumnPath::from("payload"),
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        )
        .set_column_dictionary_enabled(ColumnPath::from("ts"), false)
        .set_column_dictionary_enabled(ColumnPath::from("payload"), false)
        .build();
    ArrowWriter::try_new(file, schema.clone(), Some(props)).context("creating ArrowWriter")
}

struct PartitionWriter {
    temp_path: PathBuf,
    final_path: PathBuf,
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    source: Arc<str>,
    ts: TimestampMillisecondBuilder,
    source_col: StringBuilder,
    payload: StringBuilder,
    total_rows: u64,
    batch_rows: usize,
}

impl PartitionWriter {
    fn new(
        output_root: &Path,
        rel_dir: &str,
        schema: &Arc<Schema>,
        source: &Arc<str>,
        compression_level: i32,
    ) -> Result<Self> {
        let dir = output_root.join(rel_dir);
        fs::create_dir_all(&dir).with_context(|| format!("mkdir -p {}", dir.display()))?;
        let file_name = parquet_file_name();
        let final_path = dir.join(&file_name);
        let temp_path = dir.join(format!("tmp-{}", file_name));
        let writer = open_writer(&temp_path, schema, compression_level)?;
        Ok(Self {
            temp_path,
            final_path,
            writer,
            schema: schema.clone(),
            source: source.clone(),
            // Modest initial capacities: retimestamping can fan one input
            // partition out across many output partitions, and each open
            // writer holds its own builders. They grow geometrically as
            // needed, so undersizing costs a few reallocs, not throughput.
            ts: TimestampMillisecondBuilder::with_capacity(1024),
            source_col: StringBuilder::with_capacity(1024, 1024),
            payload: StringBuilder::with_capacity(1024, 64 * 1024),
            total_rows: 0,
            batch_rows: 0,
        })
    }

    fn push(&mut self, ts_ms: i64, payload: &str) -> Result<()> {
        self.ts.append_value(ts_ms);
        self.source_col.append_value(&self.source);
        self.payload.append_value(payload);
        self.total_rows += 1;
        self.batch_rows += 1;
        if self.batch_rows >= FLUSH_BATCH_SIZE {
            self.flush_batch()?;
        }
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.batch_rows == 0 {
            return Ok(());
        }
        let ts_array = self.ts.finish().with_timezone_opt(Some(Arc::from("UTC")));
        let source_array = self.source_col.finish();
        let payload_array = self.payload.finish();
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(ts_array) as Arc<dyn arrow::array::Array>,
                Arc::new(source_array) as Arc<dyn arrow::array::Array>,
                Arc::new(payload_array) as Arc<dyn arrow::array::Array>,
            ],
        )
        .context("building RecordBatch")?;
        // Retimestamping reorders rows; sort each batch so every row group
        // (one per batch, see open_writer) is sorted by ts.
        let batch = sort_record_batch_by_ts(&batch)?;
        self.writer.write(&batch).context("writing Parquet batch")?;
        self.batch_rows = 0;
        Ok(())
    }

    fn close(mut self) -> Result<(u64, PathBuf)> {
        self.flush_batch()?;
        self.writer.close().context("closing Parquet writer")?;
        if self.total_rows > 0 {
            fs::rename(&self.temp_path, &self.final_path).with_context(|| {
                format!(
                    "rename {} -> {}",
                    self.temp_path.display(),
                    self.final_path.display()
                )
            })?;
        } else {
            let _ = fs::remove_file(&self.temp_path);
        }
        Ok((self.total_rows, self.final_path))
    }
}

/// Accumulates output rows per partition, writing them to Parquet on `flush_all()`.
///
/// In dry-run mode, rows are counted but no files are written.
pub struct OutputWriterPool {
    output_root: PathBuf,
    compression_level: i32,
    schema: Arc<Schema>,
    /// Provenance for every row this pool writes — one input partition is a
    /// single source, so it is constant for the pool's lifetime.
    source: Arc<str>,
    dry_run: bool,
    /// Active writers keyed by partition `relative_dir`.
    writers: HashMap<String, PartitionWriter>,
    /// Files already closed by a generation rollover (see `MAX_OPEN_WRITERS`).
    finished: Vec<(String, u64, PathBuf)>,
    /// Dry-run row counts keyed by partition `relative_dir`.
    dry_run_counts: HashMap<String, u64>,
    pub total_rows_written: u64,
}

impl OutputWriterPool {
    pub fn new(output_root: PathBuf, source: &str, compression_level: i32, dry_run: bool) -> Self {
        Self {
            output_root,
            compression_level,
            schema: build_schema(),
            source: Arc::from(source),
            dry_run,
            writers: HashMap::new(),
            finished: Vec::new(),
            dry_run_counts: HashMap::new(),
            total_rows_written: 0,
        }
    }

    /// Write a single row to the appropriate output partition.
    ///
    /// Consecutive rows overwhelmingly share a partition, so the map hit path
    /// must not allocate; the key `String` is only built the first time a
    /// partition is seen.
    pub fn write_row(&mut self, partition_rel_dir: &str, ts_ms: i64, payload: &str) -> Result<()> {
        self.total_rows_written += 1;

        if self.dry_run {
            if let Some(count) = self.dry_run_counts.get_mut(partition_rel_dir) {
                *count += 1;
            } else {
                self.dry_run_counts.insert(partition_rel_dir.to_string(), 1);
            }
            return Ok(());
        }

        if let Some(writer) = self.writers.get_mut(partition_rel_dir) {
            return writer.push(ts_ms, payload);
        }

        if self.writers.len() >= MAX_OPEN_WRITERS {
            self.close_open_writers()?;
        }

        let writer = PartitionWriter::new(
            &self.output_root,
            partition_rel_dir,
            &self.schema,
            &self.source,
            self.compression_level,
        )?;
        self.writers.insert(partition_rel_dir.to_string(), writer);
        self.writers
            .get_mut(partition_rel_dir)
            .expect("writer just inserted")
            .push(ts_ms, payload)
    }

    /// Close every open writer, moving completed files into `finished`.
    fn close_open_writers(&mut self) -> Result<()> {
        for (rel_dir, writer) in self.writers.drain() {
            let (rows, path) = writer.close()?;
            if rows > 0 {
                self.finished.push((rel_dir, rows, path));
            }
        }
        Ok(())
    }

    /// Close all writers and rename temp files to final paths.
    /// Returns `(partition_rel_dir, rows, local_path)` per file written
    /// (or that would be written in dry-run mode, with an empty path); a
    /// partition can appear more than once if a generation rollover closed
    /// it mid-run. Callers aggregate for reporting and, for S3 output,
    /// upload each `local_path` under a key derived from it.
    pub fn flush_all(mut self) -> Result<Vec<(String, u64, PathBuf)>> {
        if self.dry_run {
            return Ok(self
                .dry_run_counts
                .into_iter()
                .map(|(rel_dir, rows)| (rel_dir, rows, PathBuf::new()))
                .collect());
        }

        self.close_open_writers()?;
        Ok(self.finished)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_cap_rolls_over_generations_without_losing_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut pool = OutputWriterPool::new(dir.path().to_path_buf(), "norway", 1, false);

        // Touch more partitions than the cap allows, then revisit the first
        // one so it gets a second file in a new generation.
        let total = MAX_OPEN_WRITERS + 1;
        for index in 0..total {
            let rel_dir = format!("source=test/year={:04}", 1000 + index);
            pool.write_row(&rel_dir, index as i64, "payload")
                .expect("write");
        }
        pool.write_row("source=test/year=1000", 9999, "payload")
            .expect("write");

        let outputs = pool.flush_all().expect("flush");
        let total_rows: u64 = outputs.iter().map(|(_, rows, _)| rows).sum();
        assert_eq!(total_rows, (total + 1) as u64);

        // The revisited partition produced two files (one per generation).
        let first_partition_files = outputs
            .iter()
            .filter(|(rel_dir, _, _)| rel_dir == "source=test/year=1000")
            .count();
        assert_eq!(first_partition_files, 2);

        for (_, _, path) in &outputs {
            assert!(path.exists(), "output file missing: {}", path.display());
            assert!(
                !path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("tmp-"),
                "temp file left behind: {}",
                path.display()
            );
        }

        // Every written file carries the pool's source in a `source` column.
        let sample = outputs
            .iter()
            .find(|(_, rows, _)| *rows > 0)
            .map(|(_, _, path)| path)
            .expect("a non-empty output file");
        let file = File::open(sample).expect("open");
        let batch = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("builder")
            .build()
            .expect("build")
            .next()
            .expect("one batch")
            .expect("batch ok");
        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "ts");
        assert_eq!(schema.field(1).name(), "source");
        assert_eq!(schema.field(2).name(), "payload");
        let source = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("source col");
        assert_eq!(source.value(0), "norway");
    }
}
