use anyhow::{Context, Result};
use chrono::{Datelike, TimeZone, Timelike};
use collect_core::iceberg::{ensure_namespace, ensure_table, open_catalog, partition_spec_for, IcebergConfig, TABLE_ATONS, TABLE_BINARY, TABLE_METEO, TABLE_OTHER, TABLE_POSITIONS, TABLE_STATICS};
use collect_core::iceberg::table_schemas;
use collect_core::S3Storage;
use iceberg::Catalog;
use sqlx::PgPool;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

use crate::db::{self, ArchiveStats};
use crate::decode::{decode_ais_file, decode_aisstream_file};

pub struct WorkerContext {
    pub pool: PgPool,
    pub s3_storages: Arc<Vec<S3Storage>>,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub iceberg_config: IcebergConfig,
    pub scratch_dir: Option<PathBuf>,
    pub batch_size: usize,
    pub compression_level: i32,
    pub hostname: String,
}

pub async fn run_worker_loop(ctx: Arc<WorkerContext>, sem: Arc<Semaphore>) {
    loop {
        // reclaim stale leases every iteration occasionally
        let _ = db::reclaim_stale(&ctx.pool, 600).await;
        let row = match db::fetch_pending(&ctx.pool, &ctx.hostname).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("worker fetch error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };
        let Some(row) = row else {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        };
        let permit = match sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break,
        };
        let ctx2 = ctx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = process_one(row.clone(), ctx2.clone()).await {
                eprintln!("failed to process {}: {e:#}", row.s3_key);
                let _ = db::mark_failed(&ctx2.pool, &row.s3_key, &format!("{e:#}"), row.max_attempts, row.attempts).await;
            }
        });
    }
}

async fn process_one(row: db::QueueRow, ctx: Arc<WorkerContext>) -> Result<()> {
    let start = Instant::now();
    // Download single file from S3
    let storage = ctx.s3_storages.first().context("no s3 storage configured")?.clone();
    let scratch_root = match &ctx.scratch_dir {
        Some(d) => tempfile::Builder::new().prefix("orchestrator-").tempdir_in(d)?.keep(),
        None => tempfile::Builder::new().prefix("orchestrator-").tempdir()?.keep(),
    };
    // For bucket notification the s3_key is the full object key (including source= prefix)
    let key = if ctx.s3_prefix.is_empty() {
        row.s3_key.clone()
    } else {
        format!("{}/{}", ctx.s3_prefix.trim_matches('/'), row.s3_key)
    };
    // Local path under scratch
    let local_path = scratch_root.join("input.parquet");
    if let Some(parent) = local_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    // Retry download
    let mut last_err = None;
    for attempt in 1..=3 {
        match storage.download_to_path(&key, &local_path).await {
            Ok(_) => { last_err = None; break; }
            Err(e) => {
                last_err = Some(e);
                if attempt < 3 {
                    tokio::time::sleep(std::time::Duration::from_secs(1 << attempt)).await;
                }
            }
        }
    }
    if let Some(e) = last_err {
        anyhow::bail!("download failed for s3://{}/{}: {e:#}", storage.bucket_name(), key);
    }

    // Decode per-file
    let (stats, batches) = tokio::task::spawn_blocking({
        let local_path = local_path.clone();
        let source = row.source.clone();
        let parser = row.parser.clone();
        let batch_size = ctx.batch_size;
        move || {
            if parser == "aisstream-parse" {
                decode_aisstream_file(&local_path, &source, batch_size)
            } else {
                decode_ais_file(&local_path, &source, batch_size)
            }
        }
    })
    .await
    .context("decode panicked")??;

    // Commit to Iceberg
    let catalog = open_catalog(&ctx.iceberg_config).await.context("open catalog")?;
    ensure_namespace(&catalog, &ctx.iceberg_config).await?;
    let pos_table = ensure_table(&catalog, &ctx.iceberg_config, TABLE_POSITIONS, table_schemas::positions_schema(), partition_spec_for(&table_schemas::positions_schema(), "day")?).await?;
    let stat_table = ensure_table(&catalog, &ctx.iceberg_config, TABLE_STATICS, table_schemas::statics_schema(), partition_spec_for(&table_schemas::statics_schema(), "day")?).await?;
    let meteo_table = ensure_table(&catalog, &ctx.iceberg_config, TABLE_METEO, table_schemas::meteo_schema(), partition_spec_for(&table_schemas::meteo_schema(), "day")?).await?;
    let bin_table = ensure_table(&catalog, &ctx.iceberg_config, TABLE_BINARY, table_schemas::binary_schema(), partition_spec_for(&table_schemas::binary_schema(), "day")?).await?;
    let aton_table = ensure_table(&catalog, &ctx.iceberg_config, TABLE_ATONS, table_schemas::atons_schema(), partition_spec_for(&table_schemas::atons_schema(), "day")?).await?;
    let other_table = ensure_table(&catalog, &ctx.iceberg_config, TABLE_OTHER, table_schemas::other_schema(), partition_spec_for(&table_schemas::other_schema(), "day")?).await?;

    let catalog_ref: &dyn Catalog = &catalog;
    // Use the per-parser iceberg writers' commit helpers via direct DataFileWriter
    // For ais-parse we use ais_parse::output_iceberg::commit_table_batches, for aisstream similar.
    // To keep unified, we write via generic commit helper duplicated here:
    commit_batches(catalog_ref, &pos_table, batches.positions, ctx.compression_level, TABLE_POSITIONS).await?;
    commit_batches(catalog_ref, &stat_table, batches.statics, ctx.compression_level, TABLE_STATICS).await?;
    commit_batches(catalog_ref, &meteo_table, batches.meteo, ctx.compression_level, TABLE_METEO).await?;
    commit_batches(catalog_ref, &bin_table, batches.binary, ctx.compression_level, TABLE_BINARY).await?;
    commit_batches(catalog_ref, &aton_table, batches.atons, ctx.compression_level, TABLE_ATONS).await?;
    commit_batches(catalog_ref, &other_table, batches.others, ctx.compression_level, TABLE_OTHER).await?;

    let duration_ms = start.elapsed().as_millis() as i64;
    let archive = ArchiveStats {
        rows_in: stats.rows_in as i64,
        positions_out: stats.positions_out as i64,
        statics_out: stats.statics_out as i64,
        meteo_out: stats.meteo_out as i64,
        binary_out: stats.binary_out as i64,
        atons_out: stats.atons_out as i64,
        other_out: stats.other_out as i64,
        incomplete: stats.incomplete as i64,
        unparsed: stats.failed as i64,
        deduped: stats.deduped as i64,
    };
    db::archive_success(&ctx.pool, &row, duration_ms, &archive).await?;

    // Cleanup scratch
    let _ = tokio::fs::remove_dir_all(&scratch_root).await;
    eprintln!("✅ {} -> iceberg ({} rows, {} pos) in {}ms", row.s3_key, stats.rows_in, stats.positions_out, duration_ms);
    Ok(())
}

async fn commit_batches(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    batches: Vec<arrow::record_batch::RecordBatch>,
    compression_level: i32,
    table_name: &str,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    // Reuse ais_parse commit logic by calling its internal helper via duplication.
    // We duplicate the DataFileWriter logic here to stay generic.
    use iceberg::spec::{DataFileFormat, PartitionKey};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let metadata = table.metadata();
    let iceberg_schema = metadata.current_schema();
    let location_gen = DefaultLocationGenerator::new(metadata.clone())?;
    let file_name_gen = DefaultFileNameGenerator::new("part".to_string(), Some("iceberg".to_string()), DataFileFormat::Parquet);
    let level = ZstdLevel::try_new(compression_level).context("invalid zstd")?;
    let props = WriterProperties::builder().set_compression(Compression::ZSTD(level)).build();
    let writer_builder = ParquetWriterBuilder::new(props, iceberg_schema.clone());
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(writer_builder, table.file_io().clone(), location_gen, file_name_gen);
    let builder = DataFileWriterBuilder::new(rolling);

    // Compute partition key from first batch ts if partitioned
    let pk = {
        let spec = metadata.default_partition_spec();
        if spec.fields().is_empty() {
            None
        } else {
            let first = &batches[0];
            let ts_col = first.column(0).as_any().downcast_ref::<arrow::array::TimestampMillisecondArray>().context("ts col")?;
            let first_ts = ts_col.value(0);
            let dt = chrono::Utc.timestamp_millis_opt(first_ts).single().context("invalid ts")?;
            let epoch_days = (first_ts / (86400000)) as i32;
            let mut vals: Vec<i32> = Vec::new();
            for f in spec.fields() {
                match f.transform {
                    iceberg::spec::Transform::Year => vals.push(dt.year()),
                    iceberg::spec::Transform::Month => vals.push((dt.year()-1970)*12 + dt.month() as i32 -1),
                    iceberg::spec::Transform::Day => vals.push(epoch_days),
                    iceberg::spec::Transform::Hour => vals.push(epoch_days*24 + dt.hour() as i32),
                    _ => {}
                }
            }
            let data = iceberg::spec::Struct::from_iter(vals.into_iter().map(|v| Some(iceberg::spec::Literal::int(v))));
            Some(PartitionKey::new(spec.as_ref().clone(), metadata.current_schema().clone(), data))
        }
    };

    let mut writer = builder.build(pk).await.context("build writer")?;
    let target_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).context("arrow schema")?);
    for batch in &batches {
        let projected = {
            let cols: Result<Vec<_>, _> = target_schema.fields().iter().enumerate().map(|(i,f)| {
                let col = batch.column(i);
                if col.data_type() == f.data_type() { Ok(col.clone()) } else { arrow::compute::cast(col, f.data_type()).context("cast") }
            }).collect();
            arrow::record_batch::RecordBatch::try_new(target_schema.clone(), cols?)?
        };
        writer.write(projected).await.context("write")?;
    }
    let files = writer.close().await.context("close")?;
    if files.is_empty() { return Ok(()); }
    let txn = Transaction::new(table);
    let txn = txn.fast_append().add_data_files(files).apply(txn)?;
    txn.commit(catalog).await.context("commit")?;
    eprintln!("  committed {} to {}", batches.iter().map(|b| b.num_rows()).sum::<usize>(), table_name);
    Ok(())
}
