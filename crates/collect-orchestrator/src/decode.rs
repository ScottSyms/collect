use anyhow::{Context, Result};
use arrow::array::{Array, StringArray, TimestampMillisecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashSet;
use std::fs::File as StdFile;
use std::path::Path;

use ais_parse::decode::{decode_payload, Decoded};
use ais_parse::output_iceberg::{
    IcebergAtonWriter, IcebergBinaryWriter, IcebergMeteoWriter, IcebergOtherWriter,
    IcebergPositionsWriter, IcebergStaticsWriter,
};


#[derive(Default, Debug, Clone)]
pub struct FileStats {
    pub rows_in: u64,
    pub positions_out: u64,
    pub statics_out: u64,
    pub meteo_out: u64,
    pub binary_out: u64,
    pub atons_out: u64,
    pub other_out: u64,
    pub incomplete: u64,
    pub failed: u64,
    pub deduped: u64,
}

pub struct IcebergBatches {
    pub positions: Vec<arrow::record_batch::RecordBatch>,
    pub statics: Vec<arrow::record_batch::RecordBatch>,
    pub meteo: Vec<arrow::record_batch::RecordBatch>,
    pub binary: Vec<arrow::record_batch::RecordBatch>,
    pub atons: Vec<arrow::record_batch::RecordBatch>,
    pub others: Vec<arrow::record_batch::RecordBatch>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct DedupKey(u8, i64, u32, u32, u8);

pub fn decode_ais_file(
    local_path: &Path,
    source: &str,
    batch_size: usize,
) -> Result<(FileStats, IcebergBatches)> {
    let mut stats = FileStats::default();
    let mut pos_w = IcebergPositionsWriter::new();
    let mut stat_w = IcebergStaticsWriter::new();
    let mut meteo_w = IcebergMeteoWriter::new();
    let mut bin_w = IcebergBinaryWriter::new();
    let mut aton_w = IcebergAtonWriter::new();
    let mut other_w = IcebergOtherWriter::new();
    let mut seen: HashSet<DedupKey> = HashSet::new();

    let file = StdFile::open(local_path).with_context(|| format!("open {}", local_path.display()))?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("read footer {}", local_path.display()))?
        .with_batch_size(batch_size)
        .build()
        .with_context(|| format!("build reader {}", local_path.display()))?;

    while let Some(batch) = reader.next().transpose()? {
        let schema = batch.schema();
        let ts_idx = schema.index_of("ts").unwrap_or(0);
        let payload_idx = schema
            .index_of("payload")
            .map_err(|_| anyhow::anyhow!("no payload column in {}", local_path.display()))?;
        let source_idx = schema.index_of("source").ok();

        let ts_col = batch
            .column(ts_idx)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .context("ts column")?;
        let payload_col = batch
            .column(payload_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("payload column")?;
        let source_col = source_idx
            .map(|idx| {
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("source column")
            })
            .transpose()?;

        let n = batch.num_rows();
        stats.rows_in += n as u64;
        for i in 0..n {
            let ts = ts_col.value(i);
            let src = match &source_col {
                Some(col) if !col.is_null(i) => col.value(i),
                _ => source,
            };
            let payload = payload_col.value(i);
            match decode_payload(ts, src, payload) {
                Decoded::Position(row) => {
                    let key = DedupKey(0, row.ts_ms, row.mmsi, 0, row.msg_type);
                    if seen.insert(key) {
                        stats.positions_out += 1;
                        pos_w.write(&row, payload)?;
                    } else {
                        stats.deduped += 1;
                    }
                }
                Decoded::Static(row) => {
                    let key = DedupKey(1, row.ts_ms, row.mmsi, 0, 0);
                    if seen.insert(key) {
                        stats.statics_out += 1;
                        stat_w.write(&row, payload)?;
                    } else {
                        stats.deduped += 1;
                    }
                }
                Decoded::Meteo(row) => {
                    let key = DedupKey(2, row.ts_ms, row.mmsi, ((row.dac as u32) << 8) | row.fid as u32, 0);
                    if seen.insert(key) {
                        stats.meteo_out += 1;
                        meteo_w.write(*row, payload)?;
                    } else {
                        stats.deduped += 1;
                    }
                }
                Decoded::Binary(row) => {
                    let key = DedupKey(3, row.ts_ms, row.mmsi, ((row.dac as u32) << 8) | row.fid as u32, 0);
                    if seen.insert(key) {
                        stats.binary_out += 1;
                        bin_w.write(*row, payload)?;
                    } else {
                        stats.deduped += 1;
                    }
                }
                Decoded::Aton(row) => {
                    let key = DedupKey(4, row.ts_ms, row.mmsi, 0, row.msg_type);
                    if seen.insert(key) {
                        stats.atons_out += 1;
                        aton_w.write(*row, payload)?;
                    } else {
                        stats.deduped += 1;
                    }
                }
                Decoded::Other(row) => {
                    stats.other_out += 1;
                    other_w.write(*row)?;
                }
                Decoded::Incomplete => stats.incomplete += 1,
                Decoded::Failed => stats.failed += 1,
            }
        }
    }

    Ok((
        stats,
        IcebergBatches {
            positions: pos_w.finish()?,
            statics: stat_w.finish()?,
            meteo: meteo_w.finish()?,
            binary: bin_w.finish()?,
            atons: aton_w.finish()?,
            others: other_w.finish()?,
        },
    ))
}

pub fn decode_aisstream_file(
    local_path: &Path,
    source: &str,
    batch_size: usize,
) -> Result<(FileStats, IcebergBatches)> {
    use aisstream_parse::ais_stream::AisStreamMessage;
    use aisstream_parse::output_iceberg::{
        AtonWriter as SAtonW, BinaryWriter as SBinW, MeteoWriter as SMetW, OtherWriter as SOthW,
        PositionsWriter as SPosW, StaticsWriter as SStatW,
    };
    let mut stats = FileStats::default();
    let mut pos_w = SPosW::new();
    let mut stat_w = SStatW::new();
    let mut meteo_w = SMetW::new();
    let mut bin_w = SBinW::new();
    let mut aton_w = SAtonW::new();
    let mut other_w = SOthW::new();
    let mut seen: HashSet<DedupKey> = HashSet::new();

    let file = StdFile::open(local_path).with_context(|| format!("open {}", local_path.display()))?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("read footer {}", local_path.display()))?
        .with_batch_size(batch_size)
        .build()?;

    while let Some(batch) = reader.next().transpose()? {
        let schema = batch.schema();
        let ts_idx = schema.index_of("ts").unwrap_or(0);
        let payload_idx = schema
            .index_of("payload")
            .map_err(|_| anyhow::anyhow!("no payload column"))?;
        let source_idx = schema.index_of("source").ok();
        let ts_col = batch
            .column(ts_idx)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .context("ts col")?;
        let payload_col = batch
            .column(payload_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("payload col")?;
        let source_col = source_idx
            .map(|idx| batch.column(idx).as_any().downcast_ref::<StringArray>().context("source col"))
            .transpose()?;
        let n = batch.num_rows();
        stats.rows_in += n as u64;
        for i in 0..n {
            let ts = ts_col.value(i);
            let src = match &source_col {
                Some(col) if !col.is_null(i) => col.value(i),
                _ => source,
            };
            let payload_str = payload_col.value(i);
            let mut buf = payload_str.to_string();
            let mut ais_msg: AisStreamMessage = match unsafe { simd_json::serde::from_str(&mut buf) } {
                Ok(m) => m,
                Err(_) => {
                    stats.failed += 1;
                    continue;
                }
            };
            let msg_type = ais_msg.MessageType.clone();
            let decoded = aisstream_parse::convert::decode_row(
                ts,
                src,
                &msg_type,
                payload_str,
                &mut ais_msg.Message,
            );
            let payload_opt = Some(payload_str);
            match decoded {
                aisstream_parse::convert::Decoded::Position(row) => {
                    let key = DedupKey(0, row.ts_ms, row.mmsi, 0, row.msg_type);
                    if seen.insert(key) {
                        stats.positions_out += 1;
                        pos_w.write(&row, payload_opt)?;
                    } else { stats.deduped += 1; }
                }
                aisstream_parse::convert::Decoded::Static(row) => {
                    let key = DedupKey(1, row.ts_ms, row.mmsi, 0, 0);
                    if seen.insert(key) {
                        stats.statics_out += 1;
                        stat_w.write(&row, payload_opt)?;
                    } else { stats.deduped += 1; }
                }
                aisstream_parse::convert::Decoded::Meteo(row) => {
                    let key = DedupKey(2, row.ts_ms, row.mmsi, ((row.dac as u32)<<8)| row.fid as u32, 0);
                    if seen.insert(key) { stats.meteo_out += 1; meteo_w.write(row, payload_opt)?; } else { stats.deduped+=1; }
                }
                aisstream_parse::convert::Decoded::Binary(row) => {
                    let key = DedupKey(3, row.ts_ms, row.mmsi, ((row.dac as u32)<<8)| row.fid as u32, 0);
                    if seen.insert(key) { stats.binary_out+=1; bin_w.write(row, payload_opt)?; } else { stats.deduped+=1; }
                }
                aisstream_parse::convert::Decoded::Aton(row) => {
                    let key = DedupKey(4, row.ts_ms, row.mmsi, 0, 21);
                    if seen.insert(key) { stats.atons_out+=1; aton_w.write(row, payload_opt)?; } else { stats.deduped+=1; }
                }
                aisstream_parse::convert::Decoded::Other(row) => {
                    stats.other_out+=1;
                    other_w.write(row)?;
                }
                aisstream_parse::convert::Decoded::Failed => stats.failed+=1,
            }
        }
    }

    Ok((
        stats,
        IcebergBatches {
            positions: pos_w.finish()?,
            statics: stat_w.finish()?,
            meteo: meteo_w.finish()?,
            binary: bin_w.finish()?,
            atons: aton_w.finish()?,
            others: other_w.finish()?,
        },
    ))
}
