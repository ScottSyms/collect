use anyhow::{Context, Result};
use arrow::array::{StringArray, TimestampMillisecondArray};
use clap::Parser;
use collect_core::PartitionGranularity;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File as StdFile;
use std::io::IsTerminal;
use std::path::PathBuf;

mod dataset;
mod normalize;
mod output;
mod parse;
mod stats;
mod status;

use dataset::{DatasetFile, PartitionKey};
use normalize::PartitionProcessor;
use output::OutputWriterPool;
use stats::NormalizeStats;
use status::StatusMode;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Re-timestamp, re-partition, and combine multi-part AIS sentences in a Hive-partitioned Parquet dataset"
)]
struct Args {
    /// Source Hive-partitioned Parquet root directory
    #[arg(long)]
    input_dir: PathBuf,

    /// Output Hive-partitioned Parquet root directory (may equal input-dir)
    #[arg(long)]
    output_dir: PathBuf,

    /// Partition granularity; must match the dataset layout
    #[arg(long, default_value_t = PartitionGranularity::Day)]
    partition: PartitionGranularity,

    /// Filter to a specific source label (processes all sources if omitted)
    #[arg(long)]
    source: Option<String>,

    /// Apply changes; dry-run by default
    #[arg(long)]
    apply: bool,

    /// Number of rows per Parquet read batch
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Zstd compression level for output files
    #[arg(long, default_value_t = 5)]
    compression_level: i32,

    /// Disable the runtime status TUI and print plain progress updates
    #[arg(long)]
    noui: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let dry_run = !args.apply;
    let status_mode = StatusMode::from_tty(!args.noui && std::io::stdout().is_terminal());

    if dry_run {
        eprintln!("ais-normalize: dry run (pass --apply to write output)");
    }

    // Set up Ctrl-C handler.
    let _signal = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(_) => return,
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        status::request_cancel();
        eprintln!("\nShutdown requested.");
    });

    eprintln!("Scanning input dataset...");
    let files = dataset::list_parquet_files(
        &args.input_dir,
        args.partition,
        args.source.as_deref(),
    )
    .await
    .context("scanning input dataset")?;

    if files.is_empty() {
        eprintln!("No Parquet files found in {}.", args.input_dir.display());
        return Ok(());
    }

    // Group files by partition.
    let mut partitions: Vec<(PartitionKey, Vec<DatasetFile>)> = Vec::new();
    for file in files {
        match partitions.last_mut() {
            Some((key, list)) if *key == file.partition => list.push(file),
            _ => partitions.push((file.partition.clone(), vec![file])),
        }
    }

    let total_partitions = partitions.len();
    eprintln!(
        "Found {} partition(s) across {} source(s).",
        total_partitions,
        {
            let mut sources: Vec<_> = partitions.iter().map(|(k, _)| k.source.as_str()).collect();
            sources.dedup();
            sources.len()
        }
    );

    let mut pool = OutputWriterPool::new(
        args.output_dir.clone(),
        args.compression_level,
        dry_run,
    );

    let mut total_stats = NormalizeStats::default();

    for (idx, (partition_key, partition_files)) in partitions.into_iter().enumerate() {
        if status::is_cancelled() {
            eprintln!("Cancelled after {} partition(s).", idx);
            break;
        }

        let source = partition_key.source.clone();
        let mut processor = PartitionProcessor::new(source, args.partition);

        for file in &partition_files {
            process_parquet_file(
                &file.path,
                &partition_key,
                &mut processor,
                &mut pool,
                args.batch_size,
            )
            .with_context(|| format!("processing {}", file.path.display()))?;
        }

        // Flush any incomplete fragment groups at the end of this partition.
        let leftovers = processor.flush_incomplete(&partition_key);
        for row in leftovers {
            pool.write_row(&row.partition_rel_dir, row.ts_ms, &row.payload)?;
        }

        processor.stats.partitions_processed += 1;
        total_stats.merge(&processor.stats);

        let processed = idx + 1;
        if status_mode.is_plain() && status::should_emit_plain_update(processed, 10) {
            status::print_plain_update(processed, total_partitions);
        }
    }

    let partitions_written = pool.flush_all().context("flushing output writers")?;
    total_stats.print_summary();

    if dry_run {
        eprintln!(
            "Dry run complete. Pass --apply to write {} output partition(s).",
            partitions_written
        );
    } else {
        eprintln!("Done. Wrote {} output partition(s).", partitions_written);
    }

    Ok(())
}

fn process_parquet_file(
    path: &std::path::Path,
    partition_key: &PartitionKey,
    processor: &mut PartitionProcessor,
    pool: &mut OutputWriterPool,
    batch_size: usize,
) -> Result<()> {
    let file = StdFile::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("read Parquet footer {}", path.display()))?
        .with_batch_size(batch_size)
        .build()
        .with_context(|| format!("build Parquet reader {}", path.display()))?;

    while let Some(batch) = reader.next().transpose()? {
        let ts_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .context("expected timestamp column at index 0")?;
        let payload_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected string column at index 1")?;

        for i in 0..batch.num_rows() {
            if status::is_cancelled() {
                return Ok(());
            }
            let ts_ms = ts_col.value(i);
            let payload = payload_col.value(i);

            let rows = processor.process_row(partition_key, ts_ms, payload);
            for row in rows {
                pool.write_row(&row.partition_rel_dir, row.ts_ms, &row.payload)?;
            }
        }
    }

    Ok(())
}
