use crate::convert::{AtonRow, BinaryRow, MeteoRow, PositionRow, StaticRow};
use anyhow::{Context, Result};
use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder,
    StringArray, StringBuilder, TimestampMillisecondArray, TimestampMillisecondBuilder,
    Int64Array, Int64Builder, UInt16Array, UInt16Builder, UInt32Array, UInt32Builder,
    UInt8Array, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, TimeZone, Timelike};
use iceberg::io::FileIO;
use iceberg::spec::{DataFileFormat, PartitionKey};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::Catalog;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

const FLUSH_BATCH_ROWS: usize = 8192;

fn ts_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        nullable,
    )
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
        Field::new("h3", DataType::Int64, true),
        Field::new("hilbert", DataType::Int64, true),
        Field::new("nav_status", DataType::Utf8, false),
        Field::new("high_accuracy", DataType::Boolean, false),
        Field::new("raim", DataType::Boolean, false),
        Field::new("special_manoeuvre", DataType::Boolean, true),
        Field::new("station", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, true),
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
        Field::new("payload", DataType::Utf8, true),
    ]))
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
        Field::new("hilbert", DataType::Int64, true),
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
        Field::new("payload", DataType::Utf8, true),
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
        Field::new("payload", DataType::Utf8, true),
    ]))
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
        Field::new("h3", DataType::Int64, true),
        Field::new("hilbert", DataType::Int64, true),
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
        Field::new("payload", DataType::Utf8, true),
    ]))
}

pub struct PositionsWriter {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
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
    h3: Int64Builder,
    hilbert: Int64Builder,
    nav_status: StringBuilder,
    high_accuracy: BooleanBuilder,
    raim: BooleanBuilder,
    special_manoeuvre: BooleanBuilder,
    station: StringBuilder,
    payload: StringBuilder,
}

impl PositionsWriter {
    pub fn new() -> Self {
        PositionsWriter {
            schema: positions_schema(),
            batches: Vec::new(),
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
            h3: Int64Builder::new(),
            hilbert: Int64Builder::new(),
            nav_status: StringBuilder::new(),
            high_accuracy: BooleanBuilder::new(),
            raim: BooleanBuilder::new(),
            special_manoeuvre: BooleanBuilder::new(),
            station: StringBuilder::new(),
            payload: StringBuilder::new(),
        }
    }

    pub fn write(&mut self, row: &PositionRow, payload: Option<&str>) -> Result<()> {
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
        self.h3.append_option(row.h3.and_then(|v| i64::try_from(v).ok()));
        self.hilbert.append_option(row.hilbert.and_then(|v| i64::try_from(v).ok()));
        self.nav_status.append_value(&row.nav_status);
        self.high_accuracy.append_value(row.high_accuracy);
        self.raim.append_value(row.raim);
        self.special_manoeuvre.append_option(row.special_manoeuvre);
        self.station.append_option(row.station.as_deref());
        self.payload.append_option(payload);
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
        let batch =
            RecordBatch::try_new(self.schema.clone(), columns).context("assembling positions batch")?;
        self.batches.push(batch);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        self.flush()?;
        Ok(std::mem::take(&mut self.batches))
    }
}

pub struct StaticsWriter {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
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
    pub fn new() -> Self {
        StaticsWriter {
            schema: statics_schema(),
            batches: Vec::new(),
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

    pub fn write(&mut self, row: &StaticRow, payload: Option<&str>) -> Result<()> {
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
        self.dimension_to_stern.append_option(row.dimension_to_stern);
        self.dimension_to_port.append_option(row.dimension_to_port);
        self.dimension_to_starboard
            .append_option(row.dimension_to_starboard);
        self.draught_m.append_option(row.draught_m);
        self.destination.append_option(row.destination.as_deref());
        self.eta.append_option(row.eta_ms);
        self.mothership_mmsi.append_option(row.mothership_mmsi);
        self.station.append_option(row.station.as_deref());
        self.payload.append_option(payload);
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
        let batch =
            RecordBatch::try_new(self.schema.clone(), columns).context("assembling statics batch")?;
        self.batches.push(batch);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        self.flush()?;
        Ok(std::mem::take(&mut self.batches))
    }
}

pub struct MeteoWriter {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    rows: Vec<MeteoRow>,
    payloads: Vec<Option<String>>,
}

impl MeteoWriter {
    pub fn new() -> Self {
        MeteoWriter {
            schema: meteo_schema(),
            batches: Vec::new(),
            rows: Vec::new(),
            payloads: Vec::new(),
        }
    }

    pub fn write(&mut self, row: MeteoRow, payload: Option<&str>) -> Result<()> {
        self.rows.push(row);
        self.payloads.push(payload.map(|p| p.to_string()));
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
        let p = &self.payloads;
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
            Arc::new(Int64Array::from(
                r.iter().map(|x| x.hilbert.and_then(|v| i64::try_from(v).ok())).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                r.iter()
                    .map(|x| x.position_accuracy)
                    .collect::<Vec<_>>(),
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
                r.iter()
                    .map(|x| x.visibility_greater)
                    .collect::<Vec<_>>(),
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
                r.iter()
                    .map(|x| x.station.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                p.iter()
                    .map(|x| x.as_deref())
                    .collect::<Vec<_>>(),
            )),
        ];
        let batch =
            RecordBatch::try_new(self.schema.clone(), columns).context("assembling meteo batch")?;
        self.batches.push(batch);
        self.rows.clear();
        self.payloads.clear();
        Ok(())
    }

    pub fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        self.flush()?;
        Ok(std::mem::take(&mut self.batches))
    }
}

pub struct BinaryWriter {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    rows: Vec<BinaryRow>,
    payloads: Vec<Option<String>>,
}

impl BinaryWriter {
    pub fn new() -> Self {
        BinaryWriter {
            schema: binary_schema(),
            batches: Vec::new(),
            rows: Vec::new(),
            payloads: Vec::new(),
        }
    }

    pub fn write(&mut self, row: BinaryRow, payload: Option<&str>) -> Result<()> {
        self.rows.push(row);
        self.payloads.push(payload.map(|p| p.to_string()));
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
        let p = &self.payloads;
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
                r.iter()
                    .map(|x| x.payload_hex.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                r.iter().map(|x| x.payload_bits).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                r.iter()
                    .map(|x| x.station.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                p.iter()
                    .map(|x| x.as_deref())
                    .collect::<Vec<_>>(),
            )),
        ];
        let batch =
            RecordBatch::try_new(self.schema.clone(), columns).context("assembling binary batch")?;
        self.batches.push(batch);
        self.rows.clear();
        self.payloads.clear();
        Ok(())
    }

    pub fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        self.flush()?;
        Ok(std::mem::take(&mut self.batches))
    }
}

pub struct AtonWriter {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    rows: Vec<AtonRow>,
    payloads: Vec<Option<String>>,
}

impl AtonWriter {
    pub fn new() -> Self {
        AtonWriter {
            schema: atons_schema(),
            batches: Vec::new(),
            rows: Vec::new(),
            payloads: Vec::new(),
        }
    }

    pub fn write(&mut self, row: AtonRow, payload: Option<&str>) -> Result<()> {
        self.rows.push(row);
        self.payloads.push(payload.map(|p| p.to_string()));
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
        let p = &self.payloads;
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
            Arc::new(StringArray::from(
                r.iter()
                    .map(|x| x.ais_class.as_ref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                r.iter()
                    .map(|x| x.aid_type.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                r.iter()
                    .map(|x| x.name.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                r.iter()
                    .map(|x| x.name_extension.as_deref())
                    .collect::<Vec<_>>(),
            )),
            f64_aton_col(r, |x| x.latitude),
            f64_aton_col(r, |x| x.longitude),
            i64_aton_col(r, |x| x.h3),
            i64_aton_col(r, |x| x.hilbert),
            u16_aton_col(r, |x| x.dimension_to_bow),
            u16_aton_col(r, |x| x.dimension_to_stern),
            u16_aton_col(r, |x| x.dimension_to_port),
            u16_aton_col(r, |x| x.dimension_to_starboard),
            Arc::new(BooleanArray::from(
                r.iter().map(|x| x.off_position).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                r.iter().map(|x| x.virtual_aid).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                r.iter().map(|x| x.assigned_mode).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                r.iter()
                    .map(|x| Some(x.high_accuracy))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                r.iter().map(|x| Some(x.raim)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                r.iter()
                    .map(|x| x.station.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                p.iter()
                    .map(|x| x.as_deref())
                    .collect::<Vec<_>>(),
            )),
        ];
        let batch =
            RecordBatch::try_new(self.schema.clone(), columns).context("assembling atons batch")?;
        self.batches.push(batch);
        self.rows.clear();
        self.payloads.clear();
        Ok(())
    }

    pub fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        self.flush()?;
        Ok(std::mem::take(&mut self.batches))
    }
}

fn f64_col(rows: &[MeteoRow], get: impl Fn(&MeteoRow) -> Option<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(
        rows.iter().map(get).collect::<Vec<_>>(),
    ))
}

fn u16_col(rows: &[MeteoRow], get: impl Fn(&MeteoRow) -> Option<u16>) -> ArrayRef {
    Arc::new(UInt16Array::from(
        rows.iter().map(get).collect::<Vec<_>>(),
    ))
}

fn u8_col(rows: &[MeteoRow], get: impl Fn(&MeteoRow) -> Option<u8>) -> ArrayRef {
    Arc::new(UInt8Array::from(
        rows.iter().map(get).collect::<Vec<_>>(),
    ))
}

fn f64_aton_col(rows: &[AtonRow], get: impl Fn(&AtonRow) -> Option<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(
        rows.iter().map(get).collect::<Vec<_>>(),
    ))
}

fn u16_aton_col(rows: &[AtonRow], get: impl Fn(&AtonRow) -> Option<u16>) -> ArrayRef {
    Arc::new(UInt16Array::from(
        rows.iter().map(get).collect::<Vec<_>>(),
    ))
}

fn i64_aton_col(rows: &[AtonRow], get: impl Fn(&AtonRow) -> Option<u64>) -> ArrayRef {
    Arc::new(Int64Array::from(
        rows.iter().map(|r| get(r).and_then(|v| i64::try_from(v).ok())).collect::<Vec<_>>(),
    ))
}

fn batch_for_writer(
    batch: &RecordBatch,
    iceberg_schema: &iceberg::spec::Schema,
) -> Result<RecordBatch> {
    let target_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
            .context("converting Iceberg schema to Arrow schema")?,
    );
    let columns: Result<Vec<ArrayRef>> = target_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, target_field)| {
            let source_col = batch.column(i);
            if source_col.data_type() == target_field.data_type() {
                Ok(source_col.clone())
            } else {
                arrow::compute::cast(source_col, target_field.data_type())
                    .context(format!(
                        "casting column '{}' from {:?} to {:?}",
                        target_field.name(),
                        source_col.data_type(),
                        target_field.data_type()
                    ))
            }
        })
        .collect();
    Ok(RecordBatch::try_new(target_schema, columns?)?)
}

pub(crate) async fn commit_batches_to_iceberg(
    batches: Vec<RecordBatch>,
    table: &Table,
    catalog: &dyn Catalog,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let file_io: FileIO = table.file_io().clone();
    let metadata = table.metadata();
    let iceberg_schema = metadata.current_schema();
    let location_gen = DefaultLocationGenerator::new(metadata.clone())
        .context("creating location generator")?;
    let file_name_gen =
        DefaultFileNameGenerator::new("batch".to_string(), None, DataFileFormat::Parquet);

    let level = ZstdLevel::try_new(3).context("invalid zstd level")?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(level))
        .build();

    let parquet_writer_builder =
        ParquetWriterBuilder::new(props, iceberg_schema.clone());
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        file_io,
        location_gen,
        file_name_gen,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    let partition_key = compute_partition_key(table, &batches)?;
    let mut data_file_writer = data_file_writer_builder
        .build(partition_key)
        .await
        .context("building data file writer")?;

    for batch in &batches {
        let projected = batch_for_writer(batch, &iceberg_schema)
            .context("converting batch to Iceberg-compatible schema")?;
        data_file_writer
            .write(projected)
            .await
            .context("writing batch to iceberg")?;
    }

    let data_files = data_file_writer
        .close()
        .await
        .context("closing data file writer")?;

    if data_files.is_empty() {
        return Ok(());
    }

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).context("applying fast append")?;
    tx.commit(catalog)
        .await
        .context("committing iceberg transaction")?;

    Ok(())
}

fn compute_partition_key(
    table: &Table,
    batches: &[RecordBatch],
) -> Result<Option<PartitionKey>> {
    let spec = table.metadata().default_partition_spec();
    if spec.fields().is_empty() {
        return Ok(None);
    }

    let metadata = table.metadata();
    let ts_col = &batches[0].column(0);
    let ts_array = ts_col
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .context("expected timestamp column at index 0")?;
    let first_ts = ts_array.value(0);
    let dt = chrono::Utc
        .timestamp_millis_opt(first_ts)
        .single()
        .context("invalid timestamp in partition computation")?;

    let mut partition_values: Vec<i32> = Vec::new();
    for field in spec.fields() {
        match field.transform {
            iceberg::spec::Transform::Year => partition_values.push(dt.year()),
            iceberg::spec::Transform::Month => partition_values.push(dt.month() as i32),
            iceberg::spec::Transform::Day => partition_values.push(dt.day() as i32),
            iceberg::spec::Transform::Hour => partition_values.push(dt.hour() as i32),
            _ => {}
        }
    }

    let partition_data = iceberg::spec::Struct::from_iter(
        partition_values.into_iter().map(|v| Some(iceberg::spec::Literal::int(v))),
    );

    let pk = PartitionKey::new(
        spec.as_ref().clone(),
        metadata.current_schema().clone(),
        partition_data,
    );

    Ok(Some(pk))
}
