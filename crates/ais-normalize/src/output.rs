use anyhow::{Context, Result};
use arrow::array::{StringBuilder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

static FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn parquet_file_name() -> String {
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

fn build_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn open_writer(path: &Path, schema: &Arc<Schema>, compression_level: i32) -> Result<ArrowWriter<File>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let zstd_level = ZstdLevel::try_new(compression_level).context("invalid Zstd level")?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd_level))
        .set_column_encoding(ColumnPath::from("ts"), Encoding::DELTA_BINARY_PACKED)
        .set_column_encoding(ColumnPath::from("payload"), Encoding::DELTA_LENGTH_BYTE_ARRAY)
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
    ts: TimestampMillisecondBuilder,
    payload: StringBuilder,
    rows: u64,
}

impl PartitionWriter {
    fn new(
        output_root: &Path,
        rel_dir: &str,
        schema: &Arc<Schema>,
        compression_level: i32,
    ) -> Result<Self> {
        let dir = output_root.join(rel_dir);
        fs::create_dir_all(&dir)
            .with_context(|| format!("mkdir -p {}", dir.display()))?;
        let file_name = parquet_file_name();
        let final_path = dir.join(&file_name);
        let temp_path = dir.join(format!("tmp-{}", file_name));
        let writer = open_writer(&temp_path, schema, compression_level)?;
        Ok(Self {
            temp_path,
            final_path,
            writer,
            schema: schema.clone(),
            ts: TimestampMillisecondBuilder::with_capacity(4096),
            payload: StringBuilder::with_capacity(4096, 4096 * 64),
            rows: 0,
        })
    }

    fn push(&mut self, ts_ms: i64, payload: &str) {
        self.ts.append_value(ts_ms);
        self.payload.append_value(payload);
        self.rows += 1;
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let ts_array = self.ts.finish().with_timezone_opt(Some(Arc::from("UTC")));
        let payload_array = self.payload.finish();
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(ts_array) as Arc<dyn arrow::array::Array>,
                Arc::new(payload_array) as Arc<dyn arrow::array::Array>,
            ],
        )
        .context("building RecordBatch")?;
        self.writer.write(&batch).context("writing Parquet batch")?;
        Ok(())
    }

    fn close(mut self) -> Result<u64> {
        self.flush_batch()?;
        self.writer.close().context("closing Parquet writer")?;
        if self.rows > 0 {
            fs::rename(&self.temp_path, &self.final_path)
                .with_context(|| format!("rename {} -> {}", self.temp_path.display(), self.final_path.display()))?;
        } else {
            let _ = fs::remove_file(&self.temp_path);
        }
        Ok(self.rows)
    }
}

/// Accumulates output rows per partition, writing them to Parquet on `flush_all()`.
///
/// In dry-run mode, rows are counted but no files are written.
pub struct OutputWriterPool {
    output_root: PathBuf,
    compression_level: i32,
    schema: Arc<Schema>,
    dry_run: bool,
    /// Active writers keyed by partition `relative_dir`.
    writers: HashMap<String, PartitionWriter>,
    /// Dry-run row counts keyed by partition `relative_dir`.
    dry_run_counts: HashMap<String, u64>,
    pub total_rows_written: u64,
}

impl OutputWriterPool {
    pub fn new(output_root: PathBuf, compression_level: i32, dry_run: bool) -> Self {
        Self {
            output_root,
            compression_level,
            schema: build_schema(),
            dry_run,
            writers: HashMap::new(),
            dry_run_counts: HashMap::new(),
            total_rows_written: 0,
        }
    }

    /// Write a single row to the appropriate output partition.
    pub fn write_row(&mut self, partition_rel_dir: &str, ts_ms: i64, payload: &str) -> Result<()> {
        if self.dry_run {
            *self.dry_run_counts.entry(partition_rel_dir.to_string()).or_default() += 1;
            self.total_rows_written += 1;
            return Ok(());
        }

        let writer = self.writers.entry(partition_rel_dir.to_string()).or_insert_with(|| {
            PartitionWriter::new(
                &self.output_root,
                partition_rel_dir,
                &self.schema,
                self.compression_level,
            )
            .expect("failed to create partition writer")
        });

        writer.push(ts_ms, payload);
        self.total_rows_written += 1;
        Ok(())
    }

    /// Close all writers and rename temp files to final paths.
    /// Returns the number of partitions written.
    pub fn flush_all(self) -> Result<usize> {
        if self.dry_run {
            let count = self.dry_run_counts.len();
            if count > 0 {
                eprintln!("Dry run — would write to {} partition(s):", count);
                let mut dirs: Vec<_> = self.dry_run_counts.iter().collect();
                dirs.sort_by_key(|(k, _)| k.as_str());
                for (dir, rows) in dirs {
                    eprintln!("  {} ({} rows)", dir, rows);
                }
            }
            return Ok(count);
        }

        let count = self.writers.len();
        for (_, writer) in self.writers {
            writer.close()?;
        }
        Ok(count)
    }
}
