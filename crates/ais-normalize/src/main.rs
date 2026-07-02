use anyhow::{Context, Result};
use arrow::array::{StringArray, TimestampMillisecondArray};
use clap::Parser;
use collect_core::PartitionGranularity;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File as StdFile;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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

    /// Number of partitions to process concurrently; auto-selected when omitted
    #[arg(long)]
    concurrency: Option<usize>,

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

    let concurrency = args
        .concurrency
        .unwrap_or_else(default_concurrency)
        .clamp(1, total_partitions.max(1));
    if status_mode.is_plain() && concurrency > 1 {
        eprintln!("Processing partitions with {} worker(s).", concurrency);
    }

    // Partitions are independent (processor state and writer pools are
    // per-partition), so distribute them over a small worker pool.
    type PartitionQueue = Mutex<VecDeque<(PartitionKey, Vec<DatasetFile>)>>;
    let queue: Arc<PartitionQueue> = Arc::new(Mutex::new(partitions.into()));
    let processed = Arc::new(AtomicUsize::new(0));

    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let queue = queue.clone();
        let processed = processed.clone();
        let output_dir = args.output_dir.clone();
        let granularity = args.partition;
        let batch_size = args.batch_size;
        let compression_level = args.compression_level;

        workers.push(tokio::spawn(async move {
            let mut stats = NormalizeStats::default();
            let mut partition_rows: BTreeMap<String, u64> = BTreeMap::new();

            loop {
                if status::is_cancelled() {
                    break;
                }

                let next = queue.lock().expect("partition queue lock").pop_front();
                let Some((partition_key, partition_files)) = next else {
                    break;
                };

                let output_dir = output_dir.clone();
                let result = tokio::task::spawn_blocking(move || {
                    process_partition(
                        partition_key,
                        partition_files,
                        output_dir,
                        granularity,
                        batch_size,
                        compression_level,
                        dry_run,
                    )
                })
                .await
                .context("partition worker panicked")?;

                match result {
                    Ok((partition_stats, rows)) => {
                        stats.merge(&partition_stats);
                        for (rel_dir, rows_written) in rows {
                            *partition_rows.entry(rel_dir).or_default() += rows_written;
                        }
                    }
                    Err(error) => {
                        // Stop the other workers before surfacing the error.
                        status::request_cancel();
                        return Err(error);
                    }
                }

                let done = processed.fetch_add(1, Ordering::SeqCst) + 1;
                if status_mode.is_plain() && status::should_emit_plain_update(done, 10) {
                    status::print_plain_update(done, total_partitions);
                }
            }

            Ok::<_, anyhow::Error>((stats, partition_rows))
        }));
    }

    let mut total_stats = NormalizeStats::default();
    let mut partition_rows: BTreeMap<String, u64> = BTreeMap::new();
    let mut first_error = None;
    for worker in workers {
        match worker.await {
            Ok(Ok((stats, rows))) => {
                total_stats.merge(&stats);
                for (rel_dir, rows_written) in rows {
                    *partition_rows.entry(rel_dir).or_default() += rows_written;
                }
            }
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error.into());
                }
            }
        }
    }

    if let Some(error) = first_error {
        return Err(error);
    }

    if status::is_cancelled() {
        eprintln!(
            "Cancelled after {} partition(s).",
            processed.load(Ordering::SeqCst)
        );
    }

    let partitions_written = partition_rows.len();
    total_stats.print_summary();

    if dry_run {
        if !partition_rows.is_empty() {
            eprintln!("Dry run — would write to {} partition(s):", partitions_written);
            for (rel_dir, rows) in &partition_rows {
                eprintln!("  {} ({} rows)", rel_dir, rows);
            }
        }
        eprintln!(
            "Dry run complete. Pass --apply to write {} output partition(s).",
            partitions_written
        );
    } else {
        eprintln!("Done. Wrote {} output partition(s).", partitions_written);
    }

    Ok(())
}

fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4)
        .clamp(1, 8)
}

/// Process one source partition end to end with its own processor and writer
/// pool, so partitions can run concurrently and writer memory stays bounded
/// by the output partitions a single input partition touches.
fn process_partition(
    partition_key: PartitionKey,
    files: Vec<DatasetFile>,
    output_dir: PathBuf,
    granularity: PartitionGranularity,
    batch_size: usize,
    compression_level: i32,
    dry_run: bool,
) -> Result<(NormalizeStats, Vec<(String, u64)>)> {
    let source_rel_dir: Arc<str> = Arc::from(partition_key.relative_dir());
    let mut processor = PartitionProcessor::new(partition_key.source.clone(), granularity);
    let mut pool = OutputWriterPool::new(output_dir, compression_level, dry_run);

    for file in &files {
        process_parquet_file(
            &file.path,
            &source_rel_dir,
            &mut processor,
            &mut pool,
            batch_size,
        )
        .with_context(|| format!("processing {}", file.path.display()))?;
    }

    // Flush any incomplete fragment groups at the end of this partition.
    let mut leftovers = Vec::new();
    processor.flush_incomplete(&source_rel_dir, &mut leftovers);
    for row in leftovers {
        pool.write_row(&row.partition_rel_dir, row.ts_ms, &row.payload)?;
    }

    let mut stats = processor.stats;
    stats.partitions_processed += 1;
    let rows = pool.flush_all().context("flushing output writers")?;
    Ok((stats, rows))
}

fn process_parquet_file(
    path: &std::path::Path,
    source_rel_dir: &Arc<str>,
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

        let mut rows = Vec::with_capacity(4);
        for i in 0..batch.num_rows() {
            if status::is_cancelled() {
                return Ok(());
            }
            let ts_ms = ts_col.value(i);
            let payload = payload_col.value(i);

            processor.process_row(source_rel_dir, ts_ms, payload, &mut rows);
            for row in rows.drain(..) {
                pool.write_row(&row.partition_rel_dir, row.ts_ms, &row.payload)?;
            }
        }
    }

    Ok(())
}
