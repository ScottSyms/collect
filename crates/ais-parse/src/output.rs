//! Typed Parquet writers for decoded AIS rows.
//!
//! Each processed partition writes at most one `positions` file and one
//! `statics` file (files are created lazily on the first row, so a partition
//! with no static reports produces no empty statics file). Files are written
//! under a `tmp-` name and renamed into place on close, matching the
//! convention the rest of the workspace uses for in-flight Parquet output.

use crate::decode::{AtonRow, BinaryRow, MeteoRow, PositionRow, StaticRow};
use anyhow::{Context, Result};
use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder,
    StringArray, StringBuilder, TimestampMillisecondArray, TimestampMillisecondBuilder,
    UInt16Array, UInt16Builder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder, UInt8Array, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;
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
    use parquet::schema::types::ColumnPath;
    let level = ZstdLevel::try_new(compression_level).context("invalid zstd level")?;
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(level))
        .set_column_bloom_filter_enabled(ColumnPath::from("mmsi"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("station"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("source"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("imo_number"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("call_sign"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("name"), true)
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
        Field::new("source", DataType::Utf8, false),
        Field::new("msg_type", DataType::UInt8, false),
        Field::new("mmsi", DataType::UInt32, false),
        Field::new("ais_class", DataType::Utf8, false),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
        Field::new("sog_knots", DataType::Float64, true),
        Field::new("cog", DataType::Float64, true),
        Field::new("heading_true", DataType::Float64, true),
        Field::new("rot", DataType::Float64, true),
        Field::new("altitude_m", DataType::Float64, true),
        Field::new("h3", DataType::UInt64, true),
        Field::new("hilbert", DataType::UInt64, true),
        Field::new("nav_status", DataType::Utf8, false),
        Field::new("high_accuracy", DataType::Boolean, false),
        Field::new("raim", DataType::Boolean, false),
        Field::new("special_manoeuvre", DataType::Boolean, true),
        Field::new("station", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn statics_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ts_field("ts", false),
        Field::new("source", DataType::Utf8, false),
        Field::new("msg_type", DataType::UInt8, false),
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
        Field::new("station", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn f64n(name: &str) -> Field {
    Field::new(name, DataType::Float64, true)
}
fn u16n(name: &str) -> Field {
    Field::new(name, DataType::UInt16, true)
}
fn u8n(name: &str) -> Field {
    Field::new(name, DataType::UInt8, true)
}
fn booln(name: &str) -> Field {
    Field::new(name, DataType::Boolean, true)
}

fn meteo_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ts_field("ts", false),
        Field::new("source", DataType::Utf8, false),
        Field::new("msg_type", DataType::UInt8, false),
        Field::new("mmsi", DataType::UInt32, false),
        Field::new("dac", DataType::UInt16, false),
        Field::new("fid", DataType::UInt8, false),
        f64n("latitude"),
        f64n("longitude"),
        Field::new("hilbert", DataType::UInt64, true),
        booln("position_accuracy"),
        u8n("day"),
        u8n("hour"),
        u8n("minute"),
        u16n("wind_speed_kn"),
        u16n("wind_gust_kn"),
        u16n("wind_dir_deg"),
        u16n("wind_gust_dir_deg"),
        f64n("air_temp_c"),
        u8n("humidity_pct"),
        f64n("dew_point_c"),
        u16n("pressure_hpa"),
        u8n("pressure_tendency"),
        f64n("visibility_nm"),
        booln("visibility_greater"),
        f64n("water_level_m"),
        u8n("water_level_trend"),
        f64n("surface_current_speed_kn"),
        u16n("surface_current_dir_deg"),
        f64n("current2_speed_kn"),
        u16n("current2_dir_deg"),
        f64n("current2_depth_m"),
        f64n("current3_speed_kn"),
        u16n("current3_dir_deg"),
        f64n("current3_depth_m"),
        f64n("wave_height_m"),
        u16n("wave_period_s"),
        u16n("wave_dir_deg"),
        f64n("swell_height_m"),
        u16n("swell_period_s"),
        u16n("swell_dir_deg"),
        u8n("sea_state"),
        f64n("water_temp_c"),
        u8n("precipitation_type"),
        f64n("salinity_pct"),
        u8n("ice"),
        Field::new("station", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn binary_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ts_field("ts", false),
        Field::new("source", DataType::Utf8, false),
        Field::new("msg_type", DataType::UInt8, false),
        Field::new("mmsi", DataType::UInt32, false),
        Field::new("dac", DataType::UInt16, false),
        Field::new("fid", DataType::UInt8, false),
        Field::new("payload_hex", DataType::Utf8, false),
        Field::new("payload_bits", DataType::UInt32, false),
        Field::new("station", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Shared open-file plumbing: lazily created tmp file + ArrowWriter, renamed
/// into place on `finish`.
struct FileSink {
    dir: PathBuf,
    prefix: String,
    schema: Arc<Schema>,
    compression_level: i32,
    open: Option<(ArrowWriter<File>, PathBuf, PathBuf)>,
    rows_written: u64,
}

impl FileSink {
    fn new(dir: PathBuf, prefix: &str, schema: Arc<Schema>, level: i32) -> Self {
        FileSink {
            dir,
            prefix: prefix.to_string(),
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
            let final_name = unique_file_name(&self.prefix);
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
    source: StringBuilder,
    msg_type: UInt8Builder,
    mmsi: UInt32Builder,
    ais_class: StringBuilder,
    latitude: Float64Builder,
    longitude: Float64Builder,
    sog_knots: Float64Builder,
    cog: Float64Builder,
    heading_true: Float64Builder,
    rot: Float64Builder,
    altitude_m: Float64Builder,
    h3: UInt64Builder,
    hilbert: UInt64Builder,
    nav_status: StringBuilder,
    high_accuracy: BooleanBuilder,
    raim: BooleanBuilder,
    special_manoeuvre: BooleanBuilder,
    station: StringBuilder,
    payload: StringBuilder,
}

impl PositionsWriter {
    pub fn new(dir: PathBuf, output_prefix: &str, compression_level: i32) -> Self {
        let prefix = format!("{output_prefix}-pos");
        PositionsWriter {
            sink: FileSink::new(dir, &prefix, positions_schema(), compression_level),
            ts: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            source: StringBuilder::new(),
            msg_type: UInt8Builder::new(),
            mmsi: UInt32Builder::new(),
            ais_class: StringBuilder::new(),
            latitude: Float64Builder::new(),
            longitude: Float64Builder::new(),
            sog_knots: Float64Builder::new(),
            cog: Float64Builder::new(),
            heading_true: Float64Builder::new(),
            rot: Float64Builder::new(),
            altitude_m: Float64Builder::new(),
            h3: UInt64Builder::new(),
            hilbert: UInt64Builder::new(),
            nav_status: StringBuilder::new(),
            high_accuracy: BooleanBuilder::new(),
            raim: BooleanBuilder::new(),
            special_manoeuvre: BooleanBuilder::new(),
            station: StringBuilder::new(),
            payload: StringBuilder::new(),
        }
    }

    pub fn write(&mut self, row: &PositionRow, payload: &str) -> Result<()> {
        self.ts.append_value(row.ts_ms);
        self.source.append_value(&row.source);
        self.msg_type.append_value(row.msg_type);
        self.mmsi.append_value(row.mmsi);
        self.ais_class.append_value(&row.ais_class);
        self.latitude.append_option(row.latitude);
        self.longitude.append_option(row.longitude);
        self.sog_knots.append_option(row.sog_knots);
        self.cog.append_option(row.cog);
        self.heading_true.append_option(row.heading_true);
        self.rot.append_option(row.rot);
        self.altitude_m.append_option(row.altitude_m);
        self.h3.append_option(row.h3);
        self.hilbert.append_option(row.hilbert);
        self.nav_status.append_value(&row.nav_status);
        self.high_accuracy.append_value(row.high_accuracy);
        self.raim.append_value(row.raim);
        self.special_manoeuvre.append_option(row.special_manoeuvre);
        self.station.append_option(row.station.as_deref());
        self.payload.append_value(payload);
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
            Arc::new(self.source.finish()),
            Arc::new(self.msg_type.finish()),
            Arc::new(self.mmsi.finish()),
            Arc::new(self.ais_class.finish()),
            Arc::new(self.latitude.finish()),
            Arc::new(self.longitude.finish()),
            Arc::new(self.sog_knots.finish()),
            Arc::new(self.cog.finish()),
            Arc::new(self.heading_true.finish()),
            Arc::new(self.rot.finish()),
            Arc::new(self.altitude_m.finish()),
            Arc::new(self.h3.finish()),
            Arc::new(self.hilbert.finish()),
            Arc::new(self.nav_status.finish()),
            Arc::new(self.high_accuracy.finish()),
            Arc::new(self.raim.finish()),
            Arc::new(self.special_manoeuvre.finish()),
            Arc::new(self.station.finish()),
            Arc::new(self.payload.finish()),
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
    source: StringBuilder,
    msg_type: UInt8Builder,
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
    station: StringBuilder,
    payload: StringBuilder,
}

impl StaticsWriter {
    pub fn new(dir: PathBuf, output_prefix: &str, compression_level: i32) -> Self {
        let prefix = format!("{output_prefix}-stat");
        StaticsWriter {
            sink: FileSink::new(dir, &prefix, statics_schema(), compression_level),
            ts: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            source: StringBuilder::new(),
            msg_type: UInt8Builder::new(),
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
            station: StringBuilder::new(),
            payload: StringBuilder::new(),
        }
    }

    pub fn write(&mut self, row: &StaticRow, payload: &str) -> Result<()> {
        self.ts.append_value(row.ts_ms);
        self.source.append_value(&row.source);
        self.msg_type.append_value(row.msg_type);
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
        self.station.append_option(row.station.as_deref());
        self.payload.append_value(payload);
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
            Arc::new(self.source.finish()),
            Arc::new(self.msg_type.finish()),
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
            Arc::new(self.station.finish()),
            Arc::new(self.payload.finish()),
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

/// Type 8 met/hydro writer. Met/hydro messages are low-rate, so rows are
/// buffered and columns built at flush time (simpler than 40 incremental
/// builders); flushing every `FLUSH_BATCH_ROWS` bounds memory regardless.
pub struct MeteoWriter {
    sink: FileSink,
    rows: Vec<MeteoRow>,
    payloads: Vec<String>,
}

impl MeteoWriter {
    pub fn new(dir: PathBuf, output_prefix: &str, compression_level: i32) -> Self {
        let prefix = format!("{output_prefix}-met");
        MeteoWriter {
            sink: FileSink::new(dir, &prefix, meteo_schema(), compression_level),
            rows: Vec::new(),
            payloads: Vec::new(),
        }
    }

    pub fn write(&mut self, row: MeteoRow, payload: &str) -> Result<()> {
        self.rows.push(row);
        self.payloads.push(payload.to_string());
        if self.rows.len() >= FLUSH_BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.rows.sort_by_key(|r| r.hilbert.unwrap_or(0));
        let r = &self.rows;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampMillisecondArray::from(r.iter().map(|x| x.ts_ms).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(
                r.iter().map(|x| x.source.as_ref()).collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                r.iter().map(|x| x.msg_type).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                r.iter().map(|x| x.mmsi).collect::<Vec<_>>(),
            )),
            Arc::new(UInt16Array::from(
                r.iter().map(|x| x.dac).collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                r.iter().map(|x| x.fid).collect::<Vec<_>>(),
            )),
            f64_col(r, |x| x.latitude),
            f64_col(r, |x| x.longitude),
            Arc::new(UInt64Array::from(
                r.iter().map(|x| x.hilbert).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                r.iter().map(|x| x.position_accuracy).collect::<Vec<_>>(),
            )),
            u8_col(r, |x| x.day),
            u8_col(r, |x| x.hour),
            u8_col(r, |x| x.minute),
            u16_col(r, |x| x.wind_speed_kn),
            u16_col(r, |x| x.wind_gust_kn),
            u16_col(r, |x| x.wind_dir_deg),
            u16_col(r, |x| x.wind_gust_dir_deg),
            f64_col(r, |x| x.air_temp_c),
            u8_col(r, |x| x.humidity_pct),
            f64_col(r, |x| x.dew_point_c),
            u16_col(r, |x| x.pressure_hpa),
            u8_col(r, |x| x.pressure_tendency),
            f64_col(r, |x| x.visibility_nm),
            Arc::new(BooleanArray::from(
                r.iter().map(|x| x.visibility_greater).collect::<Vec<_>>(),
            )),
            f64_col(r, |x| x.water_level_m),
            u8_col(r, |x| x.water_level_trend),
            f64_col(r, |x| x.surface_current_speed_kn),
            u16_col(r, |x| x.surface_current_dir_deg),
            f64_col(r, |x| x.current2_speed_kn),
            u16_col(r, |x| x.current2_dir_deg),
            f64_col(r, |x| x.current2_depth_m),
            f64_col(r, |x| x.current3_speed_kn),
            u16_col(r, |x| x.current3_dir_deg),
            f64_col(r, |x| x.current3_depth_m),
            f64_col(r, |x| x.wave_height_m),
            u16_col(r, |x| x.wave_period_s),
            u16_col(r, |x| x.wave_dir_deg),
            f64_col(r, |x| x.swell_height_m),
            u16_col(r, |x| x.swell_period_s),
            u16_col(r, |x| x.swell_dir_deg),
            u8_col(r, |x| x.sea_state),
            f64_col(r, |x| x.water_temp_c),
            u8_col(r, |x| x.precipitation_type),
            f64_col(r, |x| x.salinity_pct),
            u8_col(r, |x| x.ice),
            Arc::new(StringArray::from(
                r.iter().map(|x| x.station.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                self.payloads.iter().map(|x| x.as_str()).collect::<Vec<_>>(),
            )),
        ];
        let batch = RecordBatch::try_new(self.sink.schema.clone(), columns)
            .context("assembling meteo batch")?;
        self.rows.clear();
        self.payloads.clear();
        self.sink.write_batch(batch)
    }

    pub fn finish(mut self) -> Result<Option<(PathBuf, u64)>> {
        self.flush()?;
        self.sink.finish()
    }
}

/// Type 8 generic writer: header + application payload retained as hex.
pub struct BinaryWriter {
    sink: FileSink,
    rows: Vec<BinaryRow>,
    payloads: Vec<String>,
}

impl BinaryWriter {
    pub fn new(dir: PathBuf, output_prefix: &str, compression_level: i32) -> Self {
        let prefix = format!("{output_prefix}-bin");
        BinaryWriter {
            sink: FileSink::new(dir, &prefix, binary_schema(), compression_level),
            rows: Vec::new(),
            payloads: Vec::new(),
        }
    }

    pub fn write(&mut self, row: BinaryRow, payload: &str) -> Result<()> {
        self.rows.push(row);
        self.payloads.push(payload.to_string());
        if self.rows.len() >= FLUSH_BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let r = &self.rows;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampMillisecondArray::from(r.iter().map(|x| x.ts_ms).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(
                r.iter().map(|x| x.source.as_ref()).collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                r.iter().map(|x| x.msg_type).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                r.iter().map(|x| x.mmsi).collect::<Vec<_>>(),
            )),
            Arc::new(UInt16Array::from(
                r.iter().map(|x| x.dac).collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                r.iter().map(|x| x.fid).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                r.iter().map(|x| x.payload_hex.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                r.iter().map(|x| x.payload_bits).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                r.iter().map(|x| x.station.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                self.payloads.iter().map(|x| x.as_str()).collect::<Vec<_>>(),
            )),
        ];
        let batch = RecordBatch::try_new(self.sink.schema.clone(), columns)
            .context("assembling binary batch")?;
        self.rows.clear();
        self.payloads.clear();
        self.sink.write_batch(batch)
    }

    pub fn finish(mut self) -> Result<Option<(PathBuf, u64)>> {
        self.flush()?;
        self.sink.finish()
    }
}

fn atons_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ts_field("ts", false),
        Field::new("source", DataType::Utf8, false),
        Field::new("msg_type", DataType::UInt8, false),
        Field::new("mmsi", DataType::UInt32, false),
        Field::new("ais_class", DataType::Utf8, false),
        Field::new("aid_type", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("name_extension", DataType::Utf8, true),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
        Field::new("h3", DataType::UInt64, true),
        Field::new("hilbert", DataType::UInt64, true),
        Field::new("dimension_to_bow", DataType::UInt16, true),
        Field::new("dimension_to_stern", DataType::UInt16, true),
        Field::new("dimension_to_port", DataType::UInt16, true),
        Field::new("dimension_to_starboard", DataType::UInt16, true),
        booln("off_position"),
        booln("virtual_aid"),
        booln("assigned_mode"),
        Field::new("high_accuracy", DataType::Boolean, false),
        Field::new("raim", DataType::Boolean, false),
        Field::new("station", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

pub struct AtonWriter {
    sink: FileSink,
    rows: Vec<AtonRow>,
    payloads: Vec<String>,
}

impl AtonWriter {
    pub fn new(dir: PathBuf, output_prefix: &str, compression_level: i32) -> Self {
        let prefix = format!("{output_prefix}-aton");
        AtonWriter {
            sink: FileSink::new(dir, &prefix, atons_schema(), compression_level),
            rows: Vec::new(),
            payloads: Vec::new(),
        }
    }

    pub fn write(&mut self, row: AtonRow, payload: &str) -> Result<()> {
        self.rows.push(row);
        self.payloads.push(payload.to_string());
        if self.rows.len() >= FLUSH_BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.rows.sort_by_key(|r| r.hilbert.unwrap_or(0));
        let r = &self.rows;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMillisecondArray::from(r.iter().map(|x| x.ts_ms).collect::<Vec<_>>()).with_timezone("UTC")),
            Arc::new(StringArray::from(r.iter().map(|x| x.source.as_ref()).collect::<Vec<_>>())),
            Arc::new(UInt8Array::from(r.iter().map(|x| x.msg_type).collect::<Vec<_>>())),
            Arc::new(UInt32Array::from(r.iter().map(|x| x.mmsi).collect::<Vec<_>>())),
            Arc::new(StringArray::from(r.iter().map(|x| x.ais_class.as_ref()).collect::<Vec<_>>())),
            Arc::new(StringArray::from(r.iter().map(|x| x.aid_type.as_str()).collect::<Vec<_>>())),
            Arc::new(StringArray::from(r.iter().map(|x| x.name.as_deref()).collect::<Vec<_>>())),
            Arc::new(StringArray::from(r.iter().map(|x| x.name_extension.as_deref()).collect::<Vec<_>>())),
            f64_aton_col(r, |x| x.latitude),
            f64_aton_col(r, |x| x.longitude),
            Arc::new(UInt64Array::from(r.iter().map(|x| x.h3).collect::<Vec<_>>())),
            Arc::new(UInt64Array::from(r.iter().map(|x| x.hilbert).collect::<Vec<_>>())),
            u16_aton_col(r, |x| x.dimension_to_bow),
            u16_aton_col(r, |x| x.dimension_to_stern),
            u16_aton_col(r, |x| x.dimension_to_port),
            u16_aton_col(r, |x| x.dimension_to_starboard),
            Arc::new(BooleanArray::from(r.iter().map(|x| Some(x.off_position)).collect::<Vec<_>>())),
            Arc::new(BooleanArray::from(r.iter().map(|x| Some(x.virtual_aid)).collect::<Vec<_>>())),
            Arc::new(BooleanArray::from(r.iter().map(|x| Some(x.assigned_mode)).collect::<Vec<_>>())),
            Arc::new(BooleanArray::from(r.iter().map(|x| Some(x.high_accuracy)).collect::<Vec<_>>())),
            Arc::new(BooleanArray::from(r.iter().map(|x| Some(x.raim)).collect::<Vec<_>>())),
            Arc::new(StringArray::from(r.iter().map(|x| x.station.as_deref()).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                self.payloads.iter().map(|x| x.as_str()).collect::<Vec<_>>(),
            )),
        ];
        let batch = RecordBatch::try_new(self.sink.schema.clone(), columns)
            .context("assembling atons batch")?;
        self.rows.clear();
        self.payloads.clear();
        self.sink.write_batch(batch)
    }

    pub fn finish(mut self) -> Result<Option<(PathBuf, u64)>> {
        self.flush()?;
        self.sink.finish()
    }
}

fn f64_col(rows: &[MeteoRow], get: impl Fn(&MeteoRow) -> Option<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(rows.iter().map(get).collect::<Vec<_>>()))
}
fn u16_col(rows: &[MeteoRow], get: impl Fn(&MeteoRow) -> Option<u16>) -> ArrayRef {
    Arc::new(UInt16Array::from(rows.iter().map(get).collect::<Vec<_>>()))
}
fn u8_col(rows: &[MeteoRow], get: impl Fn(&MeteoRow) -> Option<u8>) -> ArrayRef {
    Arc::new(UInt8Array::from(rows.iter().map(get).collect::<Vec<_>>()))
}

/// List the `.parquet` files already present in a local partition directory —
/// the prior run's output, replaced atomically-ish by the caller after the
/// new files are in place.
fn f64_aton_col(rows: &[AtonRow], get: impl Fn(&AtonRow) -> Option<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(rows.iter().map(get).collect::<Vec<_>>()))
}

fn u16_aton_col(rows: &[AtonRow], get: impl Fn(&AtonRow) -> Option<u16>) -> ArrayRef {
    Arc::new(UInt16Array::from(rows.iter().map(get).collect::<Vec<_>>()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, StringArray, UInt32Array, UInt8Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn sample_position(ts_ms: i64, mmsi: u32) -> PositionRow {
        PositionRow {
            ts_ms,
            source: "norway".into(),
            station: Some("AIS_NOR".into()),
            msg_type: 1,
            mmsi,
            ais_class: "Class A".into(),
            latitude: Some(60.5),
            longitude: Some(4.25),
            sog_knots: Some(12.3),
            cog: Some(180.0),
            heading_true: None,
            rot: None,
            altitude_m: None,
            h3: None,
            hilbert: None,
            nav_status: "under way using engine".into(),
            high_accuracy: true,
            raim: false,
            special_manoeuvre: None,
        }
    }

    #[test]
    fn positions_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer = PositionsWriter::new(dir.path().to_path_buf(), "test", 3);
        for i in 0..10 {
            writer
                .write(&sample_position(
                    1_700_000_000_000 + i,
                    257_000_000 + i as u32,
                ), "")
                .expect("write");
        }
        let (path, rows) = writer.finish().expect("finish").expect("file written");
        assert_eq!(rows, 10);
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("test-pos-"));

        let file = File::open(&path).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("footer")
            .build()
            .expect("reader");
        let batches: Vec<_> = reader.collect::<std::result::Result<_, _>>().expect("read");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
        let first = &batches[0];
        assert_eq!(first.schema().field(1).name(), "source");
        assert_eq!(first.schema().field(2).name(), "msg_type");
        let source = first
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("source col");
        assert_eq!(source.value(0), "norway");
        let msg_type = first
            .column(2)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("msg_type col");
        assert_eq!(msg_type.value(0), 1);
        let mmsi = first
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("mmsi col");
        assert_eq!(mmsi.value(0), 257_000_000);
        let lat = first
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("lat col");
        assert!((lat.value(0) - 60.5).abs() < f64::EPSILON);
    }

    #[test]
    fn no_rows_means_no_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let writer = StaticsWriter::new(dir.path().join("statics"), "test", 3);
        assert!(writer.finish().expect("finish").is_none());
        assert!(!dir.path().join("statics").exists());
    }

    #[test]
    fn statics_round_trip_with_nulls() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer = StaticsWriter::new(dir.path().to_path_buf(), "test", 3);
        writer
            .write(&StaticRow {
                ts_ms: 1_700_000_000_000,
                source: "norway".into(),
                station: None,
                msg_type: 5,
                mmsi: 366_998_410,
                ais_class: "ClassA".into(),
                imo_number: None,
                call_sign: Some("WDD7294".into()),
                name: Some("EXAMPLE VESSEL".into()),
                ship_type: "tug".into(),
                dimension_to_bow: Some(12),
                dimension_to_stern: Some(8),
                dimension_to_port: Some(3),
                dimension_to_starboard: Some(3),
                draught_m: Some(4.2),
                destination: None,
                eta_ms: None,
                mothership_mmsi: None,
            }, "")
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
        // Schema order: ts, source, msg_type, mmsi, ais_class, imo_number,
        // call_sign, name, ...
        assert_eq!(batch.schema().field(1).name(), "source");
        assert_eq!(batch.schema().field(2).name(), "msg_type");
        let source = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("source col");
        assert_eq!(source.value(0), "norway");
        let msg_type = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("msg_type col");
        assert_eq!(msg_type.value(0), 5);
        let name = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name col");
        assert_eq!(name.value(0), "EXAMPLE VESSEL");
        let imo = batch
            .column(5)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("imo col");
        assert!(imo.is_null(0));
    }
}
