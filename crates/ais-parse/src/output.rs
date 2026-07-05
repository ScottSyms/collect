//! Typed Parquet writers for decoded AIS rows.
//!
//! Each processed partition writes at most one `positions` file and one
//! `statics` file (files are created lazily on the first row, so a partition
//! with no static reports produces no empty statics file). Files are written
//! under a `tmp-` name and renamed into place on close, matching the
//! convention the rest of the workspace uses for in-flight Parquet output.

use crate::decode::{PositionRow, StaticRow};
use anyhow::{Context, Result};
use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, StringBuilder,
    TimestampMillisecondBuilder, UInt16Builder, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Rows buffered in the builders before a RecordBatch is handed to the
/// ArrowWriter (which itself accumulates batches into row groups).
const FLUSH_BATCH_ROWS: usize = 8192;

static FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_file_name(prefix: &str) -> String {
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%S%3f");
    let seq = FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{stamp}-{:06}.parquet", seq)
}

fn writer_props(compression_level: i32) -> Result<WriterProperties> {
    let level = ZstdLevel::try_new(compression_level).context("invalid zstd level")?;
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(level))
        .build())
}

fn ts_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        nullable,
    )
}

fn positions_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ts_field("ts", false),
        Field::new("mmsi", DataType::UInt32, false),
        Field::new("ais_class", DataType::Utf8, false),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
        Field::new("sog_knots", DataType::Float64, true),
        Field::new("cog", DataType::Float64, true),
        Field::new("heading_true", DataType::Float64, true),
        Field::new("rot", DataType::Float64, true),
        Field::new("nav_status", DataType::Utf8, false),
        Field::new("high_accuracy", DataType::Boolean, false),
        Field::new("raim", DataType::Boolean, false),
        Field::new("special_manoeuvre", DataType::Boolean, true),
    ]))
}

fn statics_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ts_field("ts", false),
        Field::new("mmsi", DataType::UInt32, false),
        Field::new("ais_class", DataType::Utf8, false),
        Field::new("imo_number", DataType::UInt32, true),
        Field::new("call_sign", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("ship_type", DataType::Utf8, false),
        Field::new("dimension_to_bow", DataType::UInt16, true),
        Field::new("dimension_to_stern", DataType::UInt16, true),
        Field::new("dimension_to_port", DataType::UInt16, true),
        Field::new("dimension_to_starboard", DataType::UInt16, true),
        Field::new("draught_m", DataType::Float64, true),
        Field::new("destination", DataType::Utf8, true),
        ts_field("eta", true),
        Field::new("mothership_mmsi", DataType::UInt32, true),
    ]))
}

/// Shared open-file plumbing: lazily created tmp file + ArrowWriter, renamed
/// into place on `finish`.
struct FileSink {
    dir: PathBuf,
    prefix: &'static str,
    schema: Arc<Schema>,
    compression_level: i32,
    open: Option<(ArrowWriter<File>, PathBuf, PathBuf)>,
    rows_written: u64,
}

impl FileSink {
    fn new(dir: PathBuf, prefix: &'static str, schema: Arc<Schema>, level: i32) -> Self {
        FileSink {
            dir,
            prefix,
            schema,
            compression_level: level,
            open: None,
            rows_written: 0,
        }
    }

    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.open.is_none() {
            std::fs::create_dir_all(&self.dir)
                .with_context(|| format!("creating {}", self.dir.display()))?;
            let final_name = unique_file_name(self.prefix);
            let final_path = self.dir.join(&final_name);
            let tmp_path = self.dir.join(format!("tmp-{final_name}"));
            let file = File::create(&tmp_path)
                .with_context(|| format!("creating {}", tmp_path.display()))?;
            let writer = ArrowWriter::try_new(
                file,
                self.schema.clone(),
                Some(writer_props(self.compression_level)?),
            )
            .context("creating Parquet writer")?;
            self.open = Some((writer, tmp_path, final_path));
        }
        let (writer, _, _) = self.open.as_mut().expect("writer just ensured");
        self.rows_written += batch.num_rows() as u64;
        writer.write(&batch).context("writing Parquet batch")?;
        Ok(())
    }

    /// Close and rename into place; returns the final path if any rows were
    /// written.
    fn finish(self) -> Result<Option<(PathBuf, u64)>> {
        let Some((writer, tmp_path, final_path)) = self.open else {
            return Ok(None);
        };
        writer.close().context("closing Parquet writer")?;
        std::fs::rename(&tmp_path, &final_path).with_context(|| {
            format!(
                "renaming {} to {}",
                tmp_path.display(),
                final_path.display()
            )
        })?;
        Ok(Some((final_path, self.rows_written)))
    }
}

pub struct PositionsWriter {
    sink: FileSink,
    ts: TimestampMillisecondBuilder,
    mmsi: UInt32Builder,
    ais_class: StringBuilder,
    latitude: Float64Builder,
    longitude: Float64Builder,
    sog_knots: Float64Builder,
    cog: Float64Builder,
    heading_true: Float64Builder,
    rot: Float64Builder,
    nav_status: StringBuilder,
    high_accuracy: BooleanBuilder,
    raim: BooleanBuilder,
    special_manoeuvre: BooleanBuilder,
}

impl PositionsWriter {
    pub fn new(dir: PathBuf, compression_level: i32) -> Self {
        PositionsWriter {
            sink: FileSink::new(dir, "pos", positions_schema(), compression_level),
            ts: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            mmsi: UInt32Builder::new(),
            ais_class: StringBuilder::new(),
            latitude: Float64Builder::new(),
            longitude: Float64Builder::new(),
            sog_knots: Float64Builder::new(),
            cog: Float64Builder::new(),
            heading_true: Float64Builder::new(),
            rot: Float64Builder::new(),
            nav_status: StringBuilder::new(),
            high_accuracy: BooleanBuilder::new(),
            raim: BooleanBuilder::new(),
            special_manoeuvre: BooleanBuilder::new(),
        }
    }

    pub fn write(&mut self, row: &PositionRow) -> Result<()> {
        self.ts.append_value(row.ts_ms);
        self.mmsi.append_value(row.mmsi);
        self.ais_class.append_value(&row.ais_class);
        self.latitude.append_option(row.latitude);
        self.longitude.append_option(row.longitude);
        self.sog_knots.append_option(row.sog_knots);
        self.cog.append_option(row.cog);
        self.heading_true.append_option(row.heading_true);
        self.rot.append_option(row.rot);
        self.nav_status.append_value(&row.nav_status);
        self.high_accuracy.append_value(row.high_accuracy);
        self.raim.append_value(row.raim);
        self.special_manoeuvre.append_option(row.special_manoeuvre);
        if self.ts.len() >= FLUSH_BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.ts.len() == 0 {
            return Ok(());
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.ts.finish()),
            Arc::new(self.mmsi.finish()),
            Arc::new(self.ais_class.finish()),
            Arc::new(self.latitude.finish()),
            Arc::new(self.longitude.finish()),
            Arc::new(self.sog_knots.finish()),
            Arc::new(self.cog.finish()),
            Arc::new(self.heading_true.finish()),
            Arc::new(self.rot.finish()),
            Arc::new(self.nav_status.finish()),
            Arc::new(self.high_accuracy.finish()),
            Arc::new(self.raim.finish()),
            Arc::new(self.special_manoeuvre.finish()),
        ];
        // The builder was created without a timezone after finish(); rebuild
        // consistency by constructing the batch against the schema.
        let batch = RecordBatch::try_new(self.sink.schema.clone(), columns)
            .context("assembling positions batch")?;
        self.sink.write_batch(batch)
    }

    pub fn finish(mut self) -> Result<Option<(PathBuf, u64)>> {
        self.flush()?;
        self.sink.finish()
    }
}

pub struct StaticsWriter {
    sink: FileSink,
    ts: TimestampMillisecondBuilder,
    mmsi: UInt32Builder,
    ais_class: StringBuilder,
    imo_number: UInt32Builder,
    call_sign: StringBuilder,
    name: StringBuilder,
    ship_type: StringBuilder,
    dimension_to_bow: UInt16Builder,
    dimension_to_stern: UInt16Builder,
    dimension_to_port: UInt16Builder,
    dimension_to_starboard: UInt16Builder,
    draught_m: Float64Builder,
    destination: StringBuilder,
    eta: TimestampMillisecondBuilder,
    mothership_mmsi: UInt32Builder,
}

impl StaticsWriter {
    pub fn new(dir: PathBuf, compression_level: i32) -> Self {
        StaticsWriter {
            sink: FileSink::new(dir, "stat", statics_schema(), compression_level),
            ts: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            mmsi: UInt32Builder::new(),
            ais_class: StringBuilder::new(),
            imo_number: UInt32Builder::new(),
            call_sign: StringBuilder::new(),
            name: StringBuilder::new(),
            ship_type: StringBuilder::new(),
            dimension_to_bow: UInt16Builder::new(),
            dimension_to_stern: UInt16Builder::new(),
            dimension_to_port: UInt16Builder::new(),
            dimension_to_starboard: UInt16Builder::new(),
            draught_m: Float64Builder::new(),
            destination: StringBuilder::new(),
            eta: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            mothership_mmsi: UInt32Builder::new(),
        }
    }

    pub fn write(&mut self, row: &StaticRow) -> Result<()> {
        self.ts.append_value(row.ts_ms);
        self.mmsi.append_value(row.mmsi);
        self.ais_class.append_value(&row.ais_class);
        self.imo_number.append_option(row.imo_number);
        self.call_sign.append_option(row.call_sign.as_deref());
        self.name.append_option(row.name.as_deref());
        self.ship_type.append_value(&row.ship_type);
        self.dimension_to_bow.append_option(row.dimension_to_bow);
        self.dimension_to_stern
            .append_option(row.dimension_to_stern);
        self.dimension_to_port.append_option(row.dimension_to_port);
        self.dimension_to_starboard
            .append_option(row.dimension_to_starboard);
        self.draught_m.append_option(row.draught_m);
        self.destination.append_option(row.destination.as_deref());
        self.eta.append_option(row.eta_ms);
        self.mothership_mmsi.append_option(row.mothership_mmsi);
        if self.ts.len() >= FLUSH_BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.ts.len() == 0 {
            return Ok(());
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.ts.finish()),
            Arc::new(self.mmsi.finish()),
            Arc::new(self.ais_class.finish()),
            Arc::new(self.imo_number.finish()),
            Arc::new(self.call_sign.finish()),
            Arc::new(self.name.finish()),
            Arc::new(self.ship_type.finish()),
            Arc::new(self.dimension_to_bow.finish()),
            Arc::new(self.dimension_to_stern.finish()),
            Arc::new(self.dimension_to_port.finish()),
            Arc::new(self.dimension_to_starboard.finish()),
            Arc::new(self.draught_m.finish()),
            Arc::new(self.destination.finish()),
            Arc::new(self.eta.finish()),
            Arc::new(self.mothership_mmsi.finish()),
        ];
        let batch = RecordBatch::try_new(self.sink.schema.clone(), columns)
            .context("assembling statics batch")?;
        self.sink.write_batch(batch)
    }

    pub fn finish(mut self) -> Result<Option<(PathBuf, u64)>> {
        self.flush()?;
        self.sink.finish()
    }
}

/// List the `.parquet` files already present in a local partition directory —
/// the prior run's output, replaced atomically-ish by the caller after the
/// new files are in place.
pub fn existing_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(files),
        Err(error) => {
            return Err(error).with_context(|| format!("reading {}", dir.display()));
        }
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.ends_with(".parquet") && !name.starts_with("tmp-") {
            files.push(path);
        }
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, StringArray, UInt32Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn sample_position(ts_ms: i64, mmsi: u32) -> PositionRow {
        PositionRow {
            ts_ms,
            mmsi,
            ais_class: "ClassA".into(),
            latitude: Some(60.5),
            longitude: Some(4.25),
            sog_knots: Some(12.3),
            cog: Some(180.0),
            heading_true: None,
            rot: None,
            nav_status: "UnderWayUsingEngine".into(),
            high_accuracy: true,
            raim: false,
            special_manoeuvre: None,
        }
    }

    #[test]
    fn positions_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer = PositionsWriter::new(dir.path().to_path_buf(), 3);
        for i in 0..10 {
            writer
                .write(&sample_position(
                    1_700_000_000_000 + i,
                    257_000_000 + i as u32,
                ))
                .expect("write");
        }
        let (path, rows) = writer.finish().expect("finish").expect("file written");
        assert_eq!(rows, 10);
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("pos-"));

        let file = File::open(&path).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("footer")
            .build()
            .expect("reader");
        let batches: Vec<_> = reader.collect::<std::result::Result<_, _>>().expect("read");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
        let first = &batches[0];
        let mmsi = first
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("mmsi col");
        assert_eq!(mmsi.value(0), 257_000_000);
        let lat = first
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("lat col");
        assert!((lat.value(0) - 60.5).abs() < f64::EPSILON);
    }

    #[test]
    fn no_rows_means_no_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let writer = StaticsWriter::new(dir.path().join("statics"), 3);
        assert!(writer.finish().expect("finish").is_none());
        assert!(!dir.path().join("statics").exists());
    }

    #[test]
    fn statics_round_trip_with_nulls() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer = StaticsWriter::new(dir.path().to_path_buf(), 3);
        writer
            .write(&StaticRow {
                ts_ms: 1_700_000_000_000,
                mmsi: 366_998_410,
                ais_class: "ClassA".into(),
                imo_number: None,
                call_sign: Some("WDD7294".into()),
                name: Some("EXAMPLE VESSEL".into()),
                ship_type: "Tug".into(),
                dimension_to_bow: Some(12),
                dimension_to_stern: Some(8),
                dimension_to_port: Some(3),
                dimension_to_starboard: Some(3),
                draught_m: Some(4.2),
                destination: None,
                eta_ms: None,
                mothership_mmsi: None,
            })
            .expect("write");
        let (path, rows) = writer.finish().expect("finish").expect("file written");
        assert_eq!(rows, 1);

        let file = File::open(&path).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("footer")
            .build()
            .expect("reader");
        let batch = reader
            .into_iter()
            .next()
            .expect("one batch")
            .expect("batch ok");
        let name = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name col");
        assert_eq!(name.value(0), "EXAMPLE VESSEL");
        let imo = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("imo col");
        assert!(imo.is_null(0));
    }

    #[test]
    fn existing_parquet_files_skips_tmp() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("pos-1.parquet"), b"x").unwrap();
        std::fs::write(dir.path().join("tmp-pos-2.parquet"), b"x").unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"x").unwrap();
        let files = existing_parquet_files(dir.path()).expect("list");
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("pos-1.parquet"));
        // Missing dir is fine.
        assert!(existing_parquet_files(&dir.path().join("nope"))
            .expect("list")
            .is_empty());
    }
}
