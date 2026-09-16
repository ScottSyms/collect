//! Inline silver decoding for collectors (`--parser`).
//!
//! Implements [`collect_core::silver::SilverCommit`] for the two parse
//! libraries. One instance is shared by all write workers via `Arc` —
//! implementations are stateless across batches (per-batch decode state such
//! as the dedup set lives on the stack of each `commit_bronze_batch` call).
//!
//! Two targets, selected by the existing sink flags (no new sink flag):
//!
//! - [`IcebergSilver`] — when `--iceberg-catalog-uri` is set: decode into
//!   the six Iceberg tables via `collect_core::iceberg::commit_batches`.
//! - [`HiveSilver`] — otherwise: decode into Hive-partitioned Parquet
//!   siblings (`positions/`, `statics`, …) under the output root, using the
//!   same time-only layout as the `ais-parse` batch binaries.
//!
//! Row dispatch mirrors `collect-orchestrator`'s `decode.rs` (same writers,
//! same per-batch `DedupKey` scheme), but operates on the in-memory bronze
//! batch instead of a Parquet file on disk.

use anyhow::{Context, Result};
use arrow::array::{StringArray, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
use collect_core::dataset::PartitionKey;
use collect_core::iceberg::{
    IcebergCliArgs, TABLE_ATONS, TABLE_BINARY, TABLE_METEO, TABLE_OTHER, TABLE_POSITIONS,
    TABLE_STATICS,
};
use collect_core::silver::{ParserKind, SilverCommit, SilverStats};
use collect_core::PartitionGranularity;
use iceberg::table::Table;
use iceberg::Catalog;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct DedupKey(u8, i64, u32, u32, u8);

/// Borrow the `[ts, payload]` columns out of a sealed bronze batch.
fn bronze_columns(batch: &RecordBatch) -> Result<(&TimestampMillisecondArray, &StringArray)> {
    anyhow::ensure!(
        batch.num_columns() >= 2,
        "expected a [ts, payload] bronze batch, got {} columns",
        batch.num_columns()
    );
    let ts = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .context("bronze batch ts column")?;
    let payload = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("bronze batch payload column")?;
    Ok((ts, payload))
}

// ── Iceberg decode ──────────────────────────────────────────────────────────

struct IcebergDecoded {
    positions: Vec<RecordBatch>,
    statics: Vec<RecordBatch>,
    meteo: Vec<RecordBatch>,
    binary: Vec<RecordBatch>,
    atons: Vec<RecordBatch>,
    other: Vec<RecordBatch>,
    stats: SilverStats,
}

fn decode_ais_iceberg(batch: &RecordBatch, source: &str) -> Result<IcebergDecoded> {
    use ais_parse::decode::{decode_payload, Decoded};
    use ais_parse::output_iceberg::{
        IcebergAtonWriter, IcebergBinaryWriter, IcebergMeteoWriter, IcebergOtherWriter,
        IcebergPositionsWriter, IcebergStaticsWriter,
    };

    let (ts_col, payload_col) = bronze_columns(batch)?;
    let mut stats = SilverStats {
        rows_in: batch.num_rows() as u64,
        ..Default::default()
    };
    let mut pos_w = IcebergPositionsWriter::new();
    let mut stat_w = IcebergStaticsWriter::new();
    let mut meteo_w = IcebergMeteoWriter::new();
    let mut bin_w = IcebergBinaryWriter::new();
    let mut aton_w = IcebergAtonWriter::new();
    let mut other_w = IcebergOtherWriter::new();
    let mut seen: HashSet<DedupKey> = HashSet::new();

    for i in 0..batch.num_rows() {
        let ts = ts_col.value(i);
        let payload = payload_col.value(i);
        match decode_payload(ts, source, payload) {
            Decoded::Position(row) => {
                if seen.insert(DedupKey(0, row.ts_ms, row.mmsi, 0, row.msg_type)) {
                    stats.positions += 1;
                    pos_w.write(&row, payload)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Static(row) => {
                if seen.insert(DedupKey(1, row.ts_ms, row.mmsi, 0, 0)) {
                    stats.statics += 1;
                    stat_w.write(&row, payload)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Meteo(row) => {
                if seen.insert(DedupKey(
                    2,
                    row.ts_ms,
                    row.mmsi,
                    ((row.dac as u32) << 8) | row.fid as u32,
                    0,
                )) {
                    stats.meteo += 1;
                    meteo_w.write(*row, payload)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Binary(row) => {
                if seen.insert(DedupKey(
                    3,
                    row.ts_ms,
                    row.mmsi,
                    ((row.dac as u32) << 8) | row.fid as u32,
                    0,
                )) {
                    stats.binary += 1;
                    bin_w.write(*row, payload)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Aton(row) => {
                if seen.insert(DedupKey(4, row.ts_ms, row.mmsi, 0, row.msg_type)) {
                    stats.atons += 1;
                    aton_w.write(*row, payload)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Other(row) => {
                stats.other += 1;
                other_w.write(*row)?;
            }
            Decoded::Incomplete => stats.incomplete += 1,
            Decoded::Failed => stats.failed += 1,
        }
    }

    Ok(IcebergDecoded {
        positions: pos_w.finish()?,
        statics: stat_w.finish()?,
        meteo: meteo_w.finish()?,
        binary: bin_w.finish()?,
        atons: aton_w.finish()?,
        other: other_w.finish()?,
        stats,
    })
}

fn decode_aisstream_iceberg(batch: &RecordBatch, source: &str) -> Result<IcebergDecoded> {
    use aisstream_parse::ais_stream::AisStreamMessage;
    use aisstream_parse::convert::Decoded;
    use aisstream_parse::output_iceberg::{
        AtonWriter as SPosAtonW, BinaryWriter as SBinW, MeteoWriter as SMetW, OtherWriter as SOthW,
        PositionsWriter as SPosW, StaticsWriter as SStatW,
    };

    let (ts_col, payload_col) = bronze_columns(batch)?;
    let mut stats = SilverStats {
        rows_in: batch.num_rows() as u64,
        ..Default::default()
    };
    let mut pos_w = SPosW::new();
    let mut stat_w = SStatW::new();
    let mut meteo_w = SMetW::new();
    let mut bin_w = SBinW::new();
    let mut aton_w = SPosAtonW::new();
    let mut other_w = SOthW::new();
    let mut seen: HashSet<DedupKey> = HashSet::new();

    for i in 0..batch.num_rows() {
        let ts = ts_col.value(i);
        let payload = payload_col.value(i);
        let mut msg: AisStreamMessage = match serde_json::from_str(payload) {
            Ok(msg) => msg,
            Err(_) => {
                stats.failed += 1;
                continue;
            }
        };
        let msg_type = msg.MessageType.clone();
        let decoded =
            aisstream_parse::convert::decode_row(ts, source, &msg_type, payload, &mut msg.Message);
        match decoded {
            Decoded::Position(row) => {
                if seen.insert(DedupKey(0, row.ts_ms, row.mmsi, 0, row.msg_type)) {
                    stats.positions += 1;
                    pos_w.write(&row, Some(payload))?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Static(row) => {
                if seen.insert(DedupKey(1, row.ts_ms, row.mmsi, 0, 0)) {
                    stats.statics += 1;
                    stat_w.write(&row, Some(payload))?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Meteo(row) => {
                if seen.insert(DedupKey(
                    2,
                    row.ts_ms,
                    row.mmsi,
                    ((row.dac as u32) << 8) | row.fid as u32,
                    0,
                )) {
                    stats.meteo += 1;
                    meteo_w.write(row, Some(payload))?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Binary(row) => {
                if seen.insert(DedupKey(
                    3,
                    row.ts_ms,
                    row.mmsi,
                    ((row.dac as u32) << 8) | row.fid as u32,
                    0,
                )) {
                    stats.binary += 1;
                    bin_w.write(row, Some(payload))?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Aton(row) => {
                if seen.insert(DedupKey(4, row.ts_ms, row.mmsi, 0, 21)) {
                    stats.atons += 1;
                    aton_w.write(row, Some(payload))?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Other(row) => {
                stats.other += 1;
                other_w.write(row)?;
            }
            Decoded::Failed => stats.failed += 1,
        }
    }

    Ok(IcebergDecoded {
        positions: pos_w.finish()?,
        statics: stat_w.finish()?,
        meteo: meteo_w.finish()?,
        binary: bin_w.finish()?,
        atons: aton_w.finish()?,
        other: other_w.finish()?,
        stats,
    })
}

// ── Hive decode ─────────────────────────────────────────────────────────────

const HIVE_FILE_PREFIX: &str = "silver";
const POSITIONS_TREE: &str = "positions";
const STATICS_TREE: &str = "statics";
const METEO_TREE: &str = "meteo";
const BINARY_TREE: &str = "binary";
const ATONS_TREE: &str = "atons";
const OTHER_TREE: &str = "other";

/// Time-only output directory for one bronze batch: every row in a sealed
/// batch shares its bronze partition window, so the first row's timestamp
/// determines the silver time partition.
fn hive_rel_dir(partition: PartitionGranularity, batch: &RecordBatch) -> Result<String> {
    let (ts_col, _) = bronze_columns(batch)?;
    anyhow::ensure!(
        batch.num_rows() > 0,
        "empty bronze batch has no silver partition"
    );
    let first_ts = ts_col.value(0);
    Ok(PartitionKey::from_timestamp_ms("", first_ts, partition).relative_dir_time_only())
}

fn decode_ais_hive(
    batch: &RecordBatch,
    source: &str,
    out_dir: &Path,
    rel_dir: &str,
    compression_level: i32,
) -> Result<SilverStats> {
    use ais_parse::decode::{decode_payload, Decoded};
    use ais_parse::output::{
        AtonWriter, BinaryWriter, MeteoWriter, OtherWriter, PositionsWriter, StaticsWriter,
    };

    let dir_for = |tree: &str| out_dir.join(tree).join(rel_dir);
    let (ts_col, payload_col) = bronze_columns(batch)?;
    let mut stats = SilverStats {
        rows_in: batch.num_rows() as u64,
        ..Default::default()
    };
    let mut positions =
        PositionsWriter::new(dir_for(POSITIONS_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut statics =
        StaticsWriter::new(dir_for(STATICS_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut meteo = MeteoWriter::new(dir_for(METEO_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut binary = BinaryWriter::new(dir_for(BINARY_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut atons = AtonWriter::new(dir_for(ATONS_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut other = OtherWriter::new(dir_for(OTHER_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut seen: HashSet<DedupKey> = HashSet::new();

    for i in 0..batch.num_rows() {
        let ts = ts_col.value(i);
        let payload = payload_col.value(i);
        match decode_payload(ts, source, payload) {
            Decoded::Position(row) => {
                if seen.insert(DedupKey(0, row.ts_ms, row.mmsi, 0, row.msg_type)) {
                    stats.positions += 1;
                    positions.write(&row, payload)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Static(row) => {
                if seen.insert(DedupKey(1, row.ts_ms, row.mmsi, 0, 0)) {
                    stats.statics += 1;
                    statics.write(&row, payload)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Meteo(row) => {
                if seen.insert(DedupKey(
                    2,
                    row.ts_ms,
                    row.mmsi,
                    ((row.dac as u32) << 8) | row.fid as u32,
                    0,
                )) {
                    stats.meteo += 1;
                    meteo.write(*row, payload)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Binary(row) => {
                if seen.insert(DedupKey(
                    3,
                    row.ts_ms,
                    row.mmsi,
                    ((row.dac as u32) << 8) | row.fid as u32,
                    0,
                )) {
                    stats.binary += 1;
                    binary.write(*row, payload)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Aton(row) => {
                if seen.insert(DedupKey(4, row.ts_ms, row.mmsi, 0, row.msg_type)) {
                    stats.atons += 1;
                    atons.write(*row, payload)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Other(row) => {
                stats.other += 1;
                other.write(*row)?;
            }
            Decoded::Incomplete => stats.incomplete += 1,
            Decoded::Failed => stats.failed += 1,
        }
    }

    // Writers create files lazily on the first row; empty writers are no-ops.
    // A `None` finish (no rows for that table) is fine, not an error.
    let _ = positions.finish()?;
    let _ = statics.finish()?;
    let _ = meteo.finish()?;
    let _ = binary.finish()?;
    let _ = atons.finish()?;
    let _ = other.finish()?;
    Ok(stats)
}

fn decode_aisstream_hive(
    batch: &RecordBatch,
    source: &str,
    out_dir: &Path,
    rel_dir: &str,
    compression_level: i32,
) -> Result<SilverStats> {
    use aisstream_parse::ais_stream::AisStreamMessage;
    use aisstream_parse::convert::Decoded;
    use aisstream_parse::output::{
        AtonWriter, BinaryWriter, MeteoWriter, OtherWriter, PositionsWriter, StaticsWriter,
    };

    let dir_for = |tree: &str| out_dir.join(tree).join(rel_dir);
    let (ts_col, payload_col) = bronze_columns(batch)?;
    let mut stats = SilverStats {
        rows_in: batch.num_rows() as u64,
        ..Default::default()
    };
    let mut positions =
        PositionsWriter::new(dir_for(POSITIONS_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut statics =
        StaticsWriter::new(dir_for(STATICS_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut meteo = MeteoWriter::new(dir_for(METEO_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut binary = BinaryWriter::new(dir_for(BINARY_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut atons = AtonWriter::new(dir_for(ATONS_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut other = OtherWriter::new(dir_for(OTHER_TREE), HIVE_FILE_PREFIX, compression_level);
    let mut seen: HashSet<DedupKey> = HashSet::new();

    for i in 0..batch.num_rows() {
        let ts = ts_col.value(i);
        let payload = payload_col.value(i);
        let mut msg: AisStreamMessage = match serde_json::from_str(payload) {
            Ok(msg) => msg,
            Err(_) => {
                stats.failed += 1;
                continue;
            }
        };
        let msg_type = msg.MessageType.clone();
        let decoded =
            aisstream_parse::convert::decode_row(ts, source, &msg_type, payload, &mut msg.Message);
        match decoded {
            Decoded::Position(row) => {
                if seen.insert(DedupKey(0, row.ts_ms, row.mmsi, 0, row.msg_type)) {
                    stats.positions += 1;
                    positions.write(&row)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Static(row) => {
                if seen.insert(DedupKey(1, row.ts_ms, row.mmsi, 0, 0)) {
                    stats.statics += 1;
                    statics.write(&row)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Meteo(row) => {
                if seen.insert(DedupKey(
                    2,
                    row.ts_ms,
                    row.mmsi,
                    ((row.dac as u32) << 8) | row.fid as u32,
                    0,
                )) {
                    stats.meteo += 1;
                    meteo.write(row)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Binary(row) => {
                if seen.insert(DedupKey(
                    3,
                    row.ts_ms,
                    row.mmsi,
                    ((row.dac as u32) << 8) | row.fid as u32,
                    0,
                )) {
                    stats.binary += 1;
                    binary.write(row)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Aton(row) => {
                if seen.insert(DedupKey(4, row.ts_ms, row.mmsi, 0, 21)) {
                    stats.atons += 1;
                    atons.write(row)?;
                } else {
                    stats.deduped += 1;
                }
            }
            Decoded::Other(row) => {
                stats.other += 1;
                other.write(row)?;
            }
            Decoded::Failed => stats.failed += 1,
        }
    }

    let _ = positions.finish()?;
    let _ = statics.finish()?;
    let _ = meteo.finish()?;
    let _ = binary.finish()?;
    let _ = atons.finish()?;
    let _ = other.finish()?;
    Ok(stats)
}

// ── SilverCommit implementations ────────────────────────────────────────────

/// Decode + commit into the six Iceberg tables.
pub struct IcebergSilver {
    parser: ParserKind,
    namespace: String,
    catalog: Arc<dyn Catalog>,
    positions: Table,
    statics: Table,
    meteo: Table,
    binary: Table,
    atons: Table,
    other: Table,
    compression_level: i32,
}

#[async_trait::async_trait]
impl SilverCommit for IcebergSilver {
    async fn commit_bronze_batch(&self, batch: &RecordBatch, source: &str) -> Result<SilverStats> {
        if batch.num_rows() == 0 {
            return Ok(SilverStats::default());
        }
        // Decode is CPU-bound: keep it off the async runtime threads.
        let batch_owned = batch.clone();
        let source_owned = source.to_string();
        let parser = self.parser;
        let decoded = tokio::task::spawn_blocking(move || match parser {
            ParserKind::Ais => decode_ais_iceberg(&batch_owned, &source_owned),
            ParserKind::Aisstream => decode_aisstream_iceberg(&batch_owned, &source_owned),
            ParserKind::None => Ok(IcebergDecoded {
                positions: Vec::new(),
                statics: Vec::new(),
                meteo: Vec::new(),
                binary: Vec::new(),
                atons: Vec::new(),
                other: Vec::new(),
                stats: SilverStats::default(),
            }),
        })
        .await
        .context("silver decode task panicked")?
        .context("decoding bronze batch into silver")?;

        // Pure append (`fast_append`); empty table batches are skipped inside.
        let catalog = self.catalog.as_ref();
        collect_core::iceberg::commit_batches(
            catalog,
            &self.positions,
            decoded.positions,
            self.compression_level,
            TABLE_POSITIONS,
        )
        .await?;
        collect_core::iceberg::commit_batches(
            catalog,
            &self.statics,
            decoded.statics,
            self.compression_level,
            TABLE_STATICS,
        )
        .await?;
        collect_core::iceberg::commit_batches(
            catalog,
            &self.meteo,
            decoded.meteo,
            self.compression_level,
            TABLE_METEO,
        )
        .await?;
        collect_core::iceberg::commit_batches(
            catalog,
            &self.binary,
            decoded.binary,
            self.compression_level,
            TABLE_BINARY,
        )
        .await?;
        collect_core::iceberg::commit_batches(
            catalog,
            &self.atons,
            decoded.atons,
            self.compression_level,
            TABLE_ATONS,
        )
        .await?;
        collect_core::iceberg::commit_batches(
            catalog,
            &self.other,
            decoded.other,
            self.compression_level,
            TABLE_OTHER,
        )
        .await?;
        Ok(decoded.stats)
    }

    fn describe(&self) -> String {
        format!(
            "parser={} → iceberg namespace `{}`",
            self.parser, self.namespace
        )
    }
}

/// Decode + write Hive-partitioned Parquet siblings under the output root.
pub struct HiveSilver {
    parser: ParserKind,
    out_dir: PathBuf,
    partition: PartitionGranularity,
    compression_level: i32,
}

#[async_trait::async_trait]
impl SilverCommit for HiveSilver {
    async fn commit_bronze_batch(&self, batch: &RecordBatch, source: &str) -> Result<SilverStats> {
        if batch.num_rows() == 0 {
            return Ok(SilverStats::default());
        }
        let batch_owned = batch.clone();
        let source_owned = source.to_string();
        let out_dir = self.out_dir.clone();
        let partition = self.partition;
        let compression_level = self.compression_level;
        let parser = self.parser;
        tokio::task::spawn_blocking(move || {
            let rel_dir = hive_rel_dir(partition, &batch_owned)?;
            match parser {
                ParserKind::Ais => decode_ais_hive(
                    &batch_owned,
                    &source_owned,
                    &out_dir,
                    &rel_dir,
                    compression_level,
                ),
                ParserKind::Aisstream => decode_aisstream_hive(
                    &batch_owned,
                    &source_owned,
                    &out_dir,
                    &rel_dir,
                    compression_level,
                ),
                ParserKind::None => Ok(SilverStats::default()),
            }
        })
        .await
        .context("silver decode task panicked")?
        .context("decoding bronze batch into hive silver")
    }

    fn describe(&self) -> String {
        format!(
            "parser={} → hive-parquet under {}",
            self.parser,
            self.out_dir.display()
        )
    }
}

// ── Constructor ─────────────────────────────────────────────────────────────

/// Build the inline silver handler for a collector, or `None` when `--parser
/// none` (default).
///
/// Target selection reuses the existing sink flags: Iceberg when
/// `--iceberg-catalog-uri` is set, otherwise Hive-Parquet under `out_dir`.
/// The six Iceberg tables are ensured once here (not per batch); the Hive
/// path needs no setup.
pub async fn init_silver(
    parser: ParserKind,
    iceberg_args: &IcebergCliArgs,
    out_dir: &Path,
    partition: PartitionGranularity,
    compression_level: i32,
) -> Result<Option<Arc<dyn SilverCommit>>> {
    if !parser.is_enabled() {
        return Ok(None);
    }
    iceberg_args.validate()?;

    if iceberg_args.is_iceberg_mode() {
        let config = collect_core::iceberg::IcebergConfig::from(iceberg_args);
        let catalog = collect_core::iceberg::open_catalog(&config).await?;
        collect_core::iceberg::ensure_namespace(&catalog, &config).await?;
        let granularity = partition.as_str();
        let positions = collect_core::iceberg::ensure_table(
            &catalog,
            &config,
            TABLE_POSITIONS,
            collect_core::iceberg::table_schemas::positions_schema(),
            collect_core::iceberg::partition_spec_for(
                &collect_core::iceberg::table_schemas::positions_schema(),
                granularity,
            )?,
        )
        .await?;
        let statics = collect_core::iceberg::ensure_table(
            &catalog,
            &config,
            TABLE_STATICS,
            collect_core::iceberg::table_schemas::statics_schema(),
            collect_core::iceberg::partition_spec_for(
                &collect_core::iceberg::table_schemas::statics_schema(),
                granularity,
            )?,
        )
        .await?;
        let meteo = collect_core::iceberg::ensure_table(
            &catalog,
            &config,
            TABLE_METEO,
            collect_core::iceberg::table_schemas::meteo_schema(),
            collect_core::iceberg::partition_spec_for(
                &collect_core::iceberg::table_schemas::meteo_schema(),
                granularity,
            )?,
        )
        .await?;
        let binary = collect_core::iceberg::ensure_table(
            &catalog,
            &config,
            TABLE_BINARY,
            collect_core::iceberg::table_schemas::binary_schema(),
            collect_core::iceberg::partition_spec_for(
                &collect_core::iceberg::table_schemas::binary_schema(),
                granularity,
            )?,
        )
        .await?;
        let atons = collect_core::iceberg::ensure_table(
            &catalog,
            &config,
            TABLE_ATONS,
            collect_core::iceberg::table_schemas::atons_schema(),
            collect_core::iceberg::partition_spec_for(
                &collect_core::iceberg::table_schemas::atons_schema(),
                granularity,
            )?,
        )
        .await?;
        let other = collect_core::iceberg::ensure_table(
            &catalog,
            &config,
            TABLE_OTHER,
            collect_core::iceberg::table_schemas::other_schema(),
            collect_core::iceberg::partition_spec_for(
                &collect_core::iceberg::table_schemas::other_schema(),
                granularity,
            )?,
        )
        .await?;
        Ok(Some(Arc::new(IcebergSilver {
            parser,
            namespace: config.namespace.clone(),
            catalog: Arc::new(catalog),
            positions,
            statics,
            meteo,
            binary,
            atons,
            other,
            compression_level,
        })))
    } else {
        Ok(Some(Arc::new(HiveSilver {
            parser,
            out_dir: out_dir.to_path_buf(),
            partition,
            compression_level,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use arrow::array::TimestampMillisecondArray;

    fn bronze_batch(rows: &[(i64, &str)]) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let ts = TimestampMillisecondArray::from(
            rows.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
        )
        .with_timezone_opt(Some(Arc::from("UTC")));
        let payload = StringArray::from(rows.iter().map(|(_, p)| *p).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(payload)])
            .expect("build bronze batch")
    }

    #[test]
    fn ais_decode_routes_positions_statics_and_failures() {
        let batch = bronze_batch(&[
            // AIS type 1 position report.
            (
                1_700_000_000_000,
                "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A",
            ),
            // AIS type 18 Class B position report.
            (
                1_700_000_001_000,
                "!AIVDM,1,1,,A,B52K>;h00Fc>jpUlNV@ikwpUoP06,0*4C",
            ),
            // Not a sentence at all.
            (1_700_000_002_000, "GARBAGE LINE THAT WILL NOT PARSE"),
        ]);
        let decoded = decode_ais_iceberg(&batch, "test").expect("decode");
        assert_eq!(decoded.stats.rows_in, 3);
        assert_eq!(decoded.stats.positions, 2);
        assert_eq!(decoded.stats.failed, 1);
        assert_eq!(decoded.positions.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        assert!(decoded.statics.iter().all(|b| b.num_rows() == 0));
    }

    #[test]
    fn ais_decode_dedups_identical_rows_within_a_batch() {
        let batch = bronze_batch(&[
            (
                1_700_000_000_000,
                "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A",
            ),
            (
                1_700_000_000_000,
                "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A",
            ),
        ]);
        let decoded = decode_ais_iceberg(&batch, "test").expect("decode");
        assert_eq!(decoded.stats.positions, 1);
        assert_eq!(decoded.stats.deduped, 1);
    }

    #[test]
    fn aisstream_decode_routes_positions_and_rejects_garbage() {
        let position = r#"{"MessageType":"PositionReport","MetaData":{},"Message":{"PositionReport":{"MessageID":1,"UserID":123456789,"Latitude":48.38,"Longitude":-123.39,"Sog":12.3,"Cog":224.0,"TrueHeading":215,"RateOfTurn":0,"NavigationalStatus":0,"PositionAccuracy":true,"Raim":false,"SpecialManoeuvreIndicator":0}}}"#;
        let batch = bronze_batch(&[
            (1_700_000_000_000, position),
            (1_700_000_001_000, "not json at all"),
        ]);
        let decoded = decode_aisstream_iceberg(&batch, "test").expect("decode");
        assert_eq!(decoded.stats.rows_in, 2);
        assert_eq!(decoded.stats.positions, 1);
        assert_eq!(decoded.stats.failed, 1);
        let mmsi: Vec<u32> = decoded
            .positions
            .iter()
            .flat_map(|b| {
                b.column(3)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(mmsi, vec![123456789]);
    }

    #[test]
    fn hive_rel_dir_is_time_only_at_every_granularity() {
        // 2024-03-15T10:30:00Z.
        let ts = 1_710_498_600_000;
        let batch = bronze_batch(&[(ts, "!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A")]);
        assert_eq!(
            hive_rel_dir(PartitionGranularity::Year, &batch).unwrap(),
            "year=2024"
        );
        assert_eq!(
            hive_rel_dir(PartitionGranularity::Month, &batch).unwrap(),
            "year=2024/month=03"
        );
        assert_eq!(
            hive_rel_dir(PartitionGranularity::Day, &batch).unwrap(),
            "year=2024/month=03/day=15"
        );
        assert_eq!(
            hive_rel_dir(PartitionGranularity::Hour, &batch).unwrap(),
            "year=2024/month=03/day=15/hour=10"
        );
        assert_eq!(
            hive_rel_dir(PartitionGranularity::Minute, &batch).unwrap(),
            "year=2024/month=03/day=15/hour=10/minute=30"
        );
    }
}
