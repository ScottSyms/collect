use anyhow::{Context, Result};
use clap::Parser;
use collect_core::iceberg::{
    commit_batches, ensure_namespace, ensure_table, open_catalog, partition_spec_for,
    table_schemas, IcebergConfig, TABLE_ATONS, TABLE_BINARY, TABLE_METEO, TABLE_OTHER,
    TABLE_POSITIONS, TABLE_STATICS,
};
use collect_core::S3Storage;
use iceberg::Catalog;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(name="parse-file-worker", about="Single-file AIS parse worker for Nomad batch dispatch")]
struct Args {
    #[arg(long, env="S3_BUCKET")]
    s3_bucket: String,

    #[arg(long, env="S3_KEY")]
    s3_key: String,

    #[arg(long, env="SOURCE")]
    source: String,

    #[arg(long, env="PARSER", default_value="ais-parse")]
    parser: String,

    #[arg(long, env="S3_PREFIX", default_value="")]
    s3_prefix: String,

    #[arg(long, env="S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    #[arg(long, env="S3_REGION", default_value="us-east-1")]
    s3_region: String,

    #[arg(long, env="S3_ACCESS_KEY")]
    s3_access_key: Option<String>,

    #[arg(long, env="S3_SECRET_KEY")]
    s3_secret_key: Option<String>,

    #[arg(long, env="S3_DISABLE_TLS", value_parser=clap::builder::FalseyValueParser::new())]
    s3_disable_tls: bool,

    #[arg(long, env="BATCH_SIZE", default_value_t=8192)]
    batch_size: usize,

    #[arg(long, env="COMPRESSION_LEVEL", default_value_t=5)]
    compression_level: i32,

    #[arg(long, env="SCRATCH_DIR")]
    scratch_dir: Option<PathBuf>,

    #[arg(long, env="CALLBACK_URL")]
    callback_url: String,

    #[arg(long, env="CALLBACK_TOKEN")]
    callback_token: Option<String>,

    #[command(flatten)]
    iceberg: collect_core::iceberg::IcebergCliArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();

    // Nomad parameterized meta arrives as NOMAD_META_* env vars
    if let Ok(v) = std::env::var("NOMAD_META_s3_bucket") { if args.s3_bucket.is_empty() { args.s3_bucket = v; } }
    if let Ok(v) = std::env::var("NOMAD_META_s3_key") { if args.s3_key.is_empty() { args.s3_key = v; } }
    if let Ok(v) = std::env::var("NOMAD_META_source") { if args.source.is_empty() { args.source = v; } }
    if let Ok(v) = std::env::var("NOMAD_META_parser") { args.parser = v; }
    if let Ok(v) = std::env::var("NOMAD_META_s3_prefix") { args.s3_prefix = v; }

    // Fallback: if still empty try uppercase
    if args.s3_bucket.is_empty() { if let Ok(v) = std::env::var("NOMAD_META_S3_BUCKET") { args.s3_bucket = v; } }
    if args.s3_key.is_empty() { if let Ok(v) = std::env::var("NOMAD_META_S3_KEY") { args.s3_key = v; } }

    anyhow::ensure!(!args.s3_bucket.is_empty(), "S3_BUCKET / NOMAD_META_s3_bucket required");
    anyhow::ensure!(!args.s3_key.is_empty(), "S3_KEY / NOMAD_META_s3_key required");
    args.iceberg.validate()?;

    let result = run_once(&args).await;
    match result {
        Ok((duration_ms, stats)) => {
            if let Err(e) = post_complete(&args, duration_ms, &stats).await {
                eprintln!("callback complete failed: {e:#}; exiting 1 to trigger Nomad retry");
                std::process::exit(1);
            }
            eprintln!("✅ worker done s3://{}/{} in {}ms", args.s3_bucket, args.s3_key, duration_ms);
            Ok(())
        }
        Err(e) => {
            let msg = format!("{e:#}");
            eprintln!("worker failed s3://{}/{}: {msg}", args.s3_bucket, args.s3_key);
            let _ = post_fail(&args, &msg).await;
            std::process::exit(1);
        }
    }
}

async fn run_once(args: &Args) -> Result<(i64, WorkerStats)> {
    let start = Instant::now();
    let storage = S3Storage::new(
        args.s3_bucket.clone(),
        String::new(),
        args.s3_region.clone(),
        args.s3_endpoint.clone(),
        args.s3_access_key.clone(),
        args.s3_secret_key.clone(),
        false,
        args.s3_disable_tls,
    )
    .await
    .context("connecting to S3")?;

    let scratch_root = match &args.scratch_dir {
        Some(d) => tempfile::Builder::new().prefix("parse-worker-").tempdir_in(d)?.keep(),
        None => tempfile::Builder::new().prefix("parse-worker-").tempdir()?.keep(),
    };
    let key = if args.s3_prefix.is_empty() {
        args.s3_key.clone()
    } else {
        format!("{}/{}", args.s3_prefix.trim_matches('/'), args.s3_key)
    };
    let local_path = scratch_root.join("input.parquet");
    if let Some(parent) = local_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
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

    let (stats, batches) = tokio::task::spawn_blocking({
        let local_path = local_path.clone();
        let source = args.source.clone();
        let parser = args.parser.clone();
        let batch_size = args.batch_size;
        move || decode_file(&local_path, &source, &parser, batch_size)
    })
    .await
    .context("decode panicked")??;

    // Open catalog and commit
    let cfg: IcebergConfig = (&args.iceberg).into();
    let catalog = open_catalog(&cfg).await.context("open catalog")?;
    ensure_namespace(&catalog, &cfg).await?;
    let pos_table = ensure_table(&catalog, &cfg, TABLE_POSITIONS, table_schemas::positions_schema(), partition_spec_for(&table_schemas::positions_schema(), "day")?).await?;
    let stat_table = ensure_table(&catalog, &cfg, TABLE_STATICS, table_schemas::statics_schema(), partition_spec_for(&table_schemas::statics_schema(), "day")?).await?;
    let meteo_table = ensure_table(&catalog, &cfg, TABLE_METEO, table_schemas::meteo_schema(), partition_spec_for(&table_schemas::meteo_schema(), "day")?).await?;
    let bin_table = ensure_table(&catalog, &cfg, TABLE_BINARY, table_schemas::binary_schema(), partition_spec_for(&table_schemas::binary_schema(), "day")?).await?;
    let aton_table = ensure_table(&catalog, &cfg, TABLE_ATONS, table_schemas::atons_schema(), partition_spec_for(&table_schemas::atons_schema(), "day")?).await?;
    let other_table = ensure_table(&catalog, &cfg, TABLE_OTHER, table_schemas::other_schema(), partition_spec_for(&table_schemas::other_schema(), "day")?).await?;

    let catalog_ref: &dyn Catalog = &catalog;
    commit_batches(catalog_ref, &pos_table, batches.positions, args.compression_level, TABLE_POSITIONS).await?;
    commit_batches(catalog_ref, &stat_table, batches.statics, args.compression_level, TABLE_STATICS).await?;
    commit_batches(catalog_ref, &meteo_table, batches.meteo, args.compression_level, TABLE_METEO).await?;
    commit_batches(catalog_ref, &bin_table, batches.binary, args.compression_level, TABLE_BINARY).await?;
    commit_batches(catalog_ref, &aton_table, batches.atons, args.compression_level, TABLE_ATONS).await?;
    commit_batches(catalog_ref, &other_table, batches.others, args.compression_level, TABLE_OTHER).await?;

    let _ = tokio::fs::remove_dir_all(&scratch_root).await;
    let duration_ms = start.elapsed().as_millis() as i64;
    let ws = WorkerStats {
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
    Ok((duration_ms, ws))
}

// Reuse orchestrator decode logic inline to avoid crate circular deps
use std::collections::HashSet;
use std::path::Path;
use std::fs::File as StdFile;
use arrow::array::{Array, StringArray, TimestampMillisecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(Default, Debug, Clone)]
struct FileStats {
    rows_in: u64,
    positions_out: u64,
    statics_out: u64,
    meteo_out: u64,
    binary_out: u64,
    atons_out: u64,
    other_out: u64,
    incomplete: u64,
    failed: u64,
    deduped: u64,
}
struct IcebergBatches {
    positions: Vec<arrow::record_batch::RecordBatch>,
    statics: Vec<arrow::record_batch::RecordBatch>,
    meteo: Vec<arrow::record_batch::RecordBatch>,
    binary: Vec<arrow::record_batch::RecordBatch>,
    atons: Vec<arrow::record_batch::RecordBatch>,
    others: Vec<arrow::record_batch::RecordBatch>,
}
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct DedupKey(u8, i64, u32, u32, u8);

fn decode_file(local_path: &Path, source: &str, parser: &str, batch_size: usize) -> Result<(FileStats, IcebergBatches)> {
    if parser == "aisstream-parse" { decode_aisstream_file(local_path, source, batch_size) } else { decode_ais_file(local_path, source, batch_size) }
}

fn decode_ais_file(local_path: &Path, source: &str, batch_size: usize) -> Result<(FileStats, IcebergBatches)> {
    use ais_parse::decode::{decode_payload, Decoded};
    use ais_parse::output_iceberg::{IcebergAtonWriter, IcebergBinaryWriter, IcebergMeteoWriter, IcebergOtherWriter, IcebergPositionsWriter, IcebergStaticsWriter};
    let mut stats = FileStats::default();
    let mut pos_w = IcebergPositionsWriter::new();
    let mut stat_w = IcebergStaticsWriter::new();
    let mut meteo_w = IcebergMeteoWriter::new();
    let mut bin_w = IcebergBinaryWriter::new();
    let mut aton_w = IcebergAtonWriter::new();
    let mut other_w = IcebergOtherWriter::new();
    let mut seen: HashSet<DedupKey> = HashSet::new();
    let file = StdFile::open(local_path).with_context(|| format!("open {}", local_path.display()))?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file).with_context(|| format!("read footer {}", local_path.display()))?.with_batch_size(batch_size).build().with_context(|| format!("build reader {}", local_path.display()))?;
    while let Some(batch) = reader.next().transpose()? {
        let schema = batch.schema();
        let ts_idx = schema.index_of("ts").unwrap_or(0);
        let payload_idx = schema.index_of("payload").map_err(|_| anyhow::anyhow!("no payload column in {}", local_path.display()))?;
        let source_idx = schema.index_of("source").ok();
        let ts_col = batch.column(ts_idx).as_any().downcast_ref::<TimestampMillisecondArray>().context("ts column")?;
        let payload_col = batch.column(payload_idx).as_any().downcast_ref::<StringArray>().context("payload column")?;
        let source_col = source_idx.map(|idx| batch.column(idx).as_any().downcast_ref::<StringArray>().context("source column")).transpose()?;
        let n = batch.num_rows();
        stats.rows_in += n as u64;
        for i in 0..n {
            let ts = ts_col.value(i);
            let src = match &source_col { Some(col) if !col.is_null(i) => col.value(i), _ => source };
            let payload = payload_col.value(i);
            match decode_payload(ts, src, payload) {
                Decoded::Position(row) => { let key = DedupKey(0, row.ts_ms, row.mmsi, 0, row.msg_type); if seen.insert(key) { stats.positions_out+=1; pos_w.write(&row, payload)?; } else { stats.deduped+=1; } }
                Decoded::Static(row) => { let key = DedupKey(1, row.ts_ms, row.mmsi, 0, 0); if seen.insert(key) { stats.statics_out+=1; stat_w.write(&row, payload)?; } else { stats.deduped+=1; } }
                Decoded::Meteo(row) => { let key = DedupKey(2, row.ts_ms, row.mmsi, ((row.dac as u32)<<8)|row.fid as u32, 0); if seen.insert(key) { stats.meteo_out+=1; meteo_w.write(*row, payload)?; } else { stats.deduped+=1; } }
                Decoded::Binary(row) => { let key = DedupKey(3, row.ts_ms, row.mmsi, ((row.dac as u32)<<8)|row.fid as u32, 0); if seen.insert(key) { stats.binary_out+=1; bin_w.write(*row, payload)?; } else { stats.deduped+=1; } }
                Decoded::Aton(row) => { let key = DedupKey(4, row.ts_ms, row.mmsi, 0, row.msg_type); if seen.insert(key) { stats.atons_out+=1; aton_w.write(*row, payload)?; } else { stats.deduped+=1; } }
                Decoded::Other(row) => { stats.other_out+=1; other_w.write(*row)?; }
                Decoded::Incomplete => stats.incomplete+=1,
                Decoded::Failed => stats.failed+=1,
            }
        }
    }
    Ok((stats, IcebergBatches { positions: pos_w.finish()?, statics: stat_w.finish()?, meteo: meteo_w.finish()?, binary: bin_w.finish()?, atons: aton_w.finish()?, others: other_w.finish()? }))
}

fn decode_aisstream_file(local_path: &Path, source: &str, batch_size: usize) -> Result<(FileStats, IcebergBatches)> {
    use aisstream_parse::ais_stream::AisStreamMessage;
    use aisstream_parse::output_iceberg::{AtonWriter as SAtonW, BinaryWriter as SBinW, MeteoWriter as SMetW, OtherWriter as SOthW, PositionsWriter as SPosW, StaticsWriter as SStatW};
    let mut stats = FileStats::default();
    let mut pos_w = SPosW::new(); let mut stat_w = SStatW::new(); let mut meteo_w = SMetW::new(); let mut bin_w = SBinW::new(); let mut aton_w = SAtonW::new(); let mut other_w = SOthW::new();
    let mut seen: HashSet<DedupKey> = HashSet::new();
    let file = StdFile::open(local_path).with_context(|| format!("open {}", local_path.display()))?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file).with_context(|| format!("read footer {}", local_path.display()))?.with_batch_size(batch_size).build()?;
    while let Some(batch) = reader.next().transpose()? {
        let schema = batch.schema();
        let ts_idx = schema.index_of("ts").unwrap_or(0);
        let payload_idx = schema.index_of("payload").map_err(|_| anyhow::anyhow!("no payload column"))?;
        let source_idx = schema.index_of("source").ok();
        let ts_col = batch.column(ts_idx).as_any().downcast_ref::<TimestampMillisecondArray>().context("ts col")?;
        let payload_col = batch.column(payload_idx).as_any().downcast_ref::<StringArray>().context("payload col")?;
        let source_col = source_idx.map(|idx| batch.column(idx).as_any().downcast_ref::<StringArray>().context("source col")).transpose()?;
        let n = batch.num_rows(); stats.rows_in+= n as u64;
        for i in 0..n {
            let ts = ts_col.value(i);
            let src = match &source_col { Some(col) if !col.is_null(i) => col.value(i), _ => source };
            let payload_str = payload_col.value(i);
            let mut buf = payload_str.to_string();
            let mut ais_msg: AisStreamMessage = match unsafe { simd_json::serde::from_str(&mut buf) } { Ok(m) => m, Err(_) => { stats.failed+=1; continue; } };
            let msg_type = ais_msg.MessageType.clone();
            let decoded = aisstream_parse::convert::decode_row(ts, src, &msg_type, payload_str, &mut ais_msg.Message);
            let payload_opt = Some(payload_str);
            match decoded {
                aisstream_parse::convert::Decoded::Position(row) => { let key = DedupKey(0, row.ts_ms, row.mmsi, 0, row.msg_type); if seen.insert(key) { stats.positions_out+=1; pos_w.write(&row, payload_opt)?; } else { stats.deduped+=1; } }
                aisstream_parse::convert::Decoded::Static(row) => { let key = DedupKey(1, row.ts_ms, row.mmsi, 0, 0); if seen.insert(key) { stats.statics_out+=1; stat_w.write(&row, payload_opt)?; } else { stats.deduped+=1; } }
                aisstream_parse::convert::Decoded::Meteo(row) => { let key = DedupKey(2, row.ts_ms, row.mmsi, ((row.dac as u32)<<8)|row.fid as u32, 0); if seen.insert(key) { stats.meteo_out+=1; meteo_w.write(row, payload_opt)?; } else { stats.deduped+=1; } }
                aisstream_parse::convert::Decoded::Binary(row) => { let key = DedupKey(3, row.ts_ms, row.mmsi, ((row.dac as u32)<<8)|row.fid as u32, 0); if seen.insert(key) { stats.binary_out+=1; bin_w.write(row, payload_opt)?; } else { stats.deduped+=1; } }
                aisstream_parse::convert::Decoded::Aton(row) => { let key = DedupKey(4, row.ts_ms, row.mmsi, 0, 21); if seen.insert(key) { stats.atons_out+=1; aton_w.write(row, payload_opt)?; } else { stats.deduped+=1; } }
                aisstream_parse::convert::Decoded::Other(row) => { stats.other_out+=1; other_w.write(row)?; }
                aisstream_parse::convert::Decoded::Failed => stats.failed+=1,
            }
        }
    }
    Ok((stats, IcebergBatches { positions: pos_w.finish()?, statics: stat_w.finish()?, meteo: meteo_w.finish()?, binary: bin_w.finish()?, atons: aton_w.finish()?, others: other_w.finish()? }))
}

#[derive(serde::Serialize, Clone, Debug)]
struct WorkerStats {
    rows_in: i64,
    positions_out: i64,
    statics_out: i64,
    meteo_out: i64,
    binary_out: i64,
    atons_out: i64,
    other_out: i64,
    incomplete: i64,
    unparsed: i64,
    deduped: i64,
}

async fn post_complete(args: &Args, duration_ms: i64, stats: &WorkerStats) -> Result<()> {
    let client = reqwest::Client::new();
    let url = if args.callback_url.ends_with("/complete") || args.callback_url.ends_with("/fail") {
        args.callback_url.clone()
    } else {
        format!("{}/complete", args.callback_url.trim_end_matches('/'))
    };
    let mut req = client.post(&url).json(&serde_json::json!({
        "s3_bucket": args.s3_bucket,
        "s3_key": args.s3_key,
        "duration_ms": duration_ms,
        "stats": stats,
    }));
    if let Some(tok) = &args.callback_token {
        req = req.header("Authorization", format!("Bearer {tok}"));
    }
    let resp = req.send().await.context("callback POST")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("callback {url} returned {status}: {body}");
    }
    Ok(())
}

async fn post_fail(args: &Args, error: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let base = args.callback_url.trim_end_matches('/').trim_end_matches("/complete").to_string();
    let url = format!("{}/fail", base.trim_end_matches('/'));
    let mut req = client.post(&url).json(&serde_json::json!({
        "s3_bucket": args.s3_bucket,
        "s3_key": args.s3_key,
        "error": error,
    }));
    if let Some(tok) = &args.callback_token {
        req = req.header("Authorization", format!("Bearer {tok}"));
    }
    let resp = req.send().await.context("fail callback POST")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("fail callback {url} returned {status}: {body}");
    }
    Ok(())
}
