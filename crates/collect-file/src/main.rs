use anyhow::{Context, Result};
use clap::Parser;
use collect_core::ais_consolidate::{AisConsolidator, AisConsolidatorConfig};
use collect_core::{
    default_source_from_path, health_file_path, print_completions, run_ingest,
    update_health_status_async, CommonCliArgs, IngestOptions, S3CliArgs,
};
use std::collections::VecDeque;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

mod completion_manifest;
mod input;
mod status;

/// Exit code used when there was nothing to ingest (distinct from success
/// with rows written, and from a hard error).
const EXIT_NOTHING_TO_DO: i32 = 2;

#[derive(Parser, Debug)]
#[command(
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_COMMIT_HASH"), ")"),
    about = "Recursively ingest plain, gzip, bzip2, and zip files into Hive-partitioned Parquet with Zstd compression"
)]
struct Args {
    /// Input file or directory to ingest
    #[arg(long = "input", visible_alias = "input-dir", env = "INPUT_PATH")]
    input: Option<PathBuf>,

    /// Logical source label; defaults to input file stem or directory name
    #[arg(short, long, env = "SOURCE")]
    source: Option<String>,

    /// Maximum number of concurrent file ingest workers; auto-selected when omitted
    #[arg(long, env = "CONCURRENCY")]
    concurrency: Option<usize>,

    #[command(flatten)]
    common: CommonCliArgs,

    #[command(flatten)]
    s3: S3CliArgs,

    /// Disable the runtime status UI and print aggregate updates every 10 files
    #[arg(long)]
    noui: bool,

    /// Suppress informational progress lines; warnings and errors still print
    #[arg(short, long, env = "QUIET")]
    quiet: bool,

    /// Enable AIS multi-part message consolidation (reassembles fragmented
    /// NMEA sentences in-line before writing to the Parquet batch).
    #[arg(long)]
    consolidate_ais: bool,

    /// Process $PGHP timestamp lines and tag-block c: carry-forward to
    /// correct row timestamps. Independent of --consolidate-ais.
    #[arg(long)]
    process_timestamps: bool,

    /// Print shell completions for the given shell to stdout and exit
    #[arg(long, exclusive = true)]
    completions: Option<clap_complete::Shell>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(shell) = args.completions {
        print_completions::<Args>(shell, "collect-file");
        return Ok(());
    }

    let quiet = args.quiet;
    let status_mode = status::StatusMode::from_tty(!args.noui && std::io::stdout().is_terminal());

    let input = args
        .input
        .context("missing input path; set --input or INPUT_PATH")?;
    let source_name = args
        .source
        .unwrap_or_else(|| default_source_from_path(&input));

    let health_file = health_file_path("collect-file");
    if !quiet {
        eprintln!("🔎 Scanning input files...");
    }
    let source = input::FileInputSource::new_parallel(input, source_name).await?;
    if !quiet && status_mode.is_plain() {
        eprintln!("📦 Discovered {} input job(s)", source.job_count());
    }
    run_file_ingest(
        source,
        args.common.to_options(),
        args.s3.to_options(),
        health_file,
        status_mode,
        args.concurrency,
        args.consolidate_ais,
        args.process_timestamps,
        quiet,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_file_ingest(
    source: input::FileInputSource,
    common: collect_core::CommonOptions,
    s3: Option<collect_core::S3Options>,
    health_file: PathBuf,
    status_mode: status::StatusMode,
    concurrency: Option<usize>,
    consolidate_ais: bool,
    process_timestamps: bool,
    quiet: bool,
) -> Result<()> {
    let manifest_path = completion_manifest::manifest_path(&common.out_dir);
    let completed = match completion_manifest::load_completed(&manifest_path) {
        Ok(completed) => completed,
        Err(error) => {
            eprintln!("⚠️  Failed to load completion manifest: {}", error);
            Default::default()
        }
    };
    let stop = Arc::new(AtomicBool::new(false));
    let mut sources: Vec<_> = source
        .into_job_sources()
        .into_iter()
        .filter(|source| match source.completion_key() {
            Ok(key) => !completed.contains(&key),
            Err(error) => {
                eprintln!("⚠️  Skipping file without completion key: {}", error);
                true
            }
        })
        .collect();

    if sources.is_empty() {
        if !quiet {
            eprintln!("✅ No unfinished input files found");
        }
        std::process::exit(EXIT_NOTHING_TO_DO);
    }

    let total_files = sources.len();
    let total_bytes = sources
        .iter()
        .map(input::FileInputSource::estimated_size_bytes)
        .sum();
    let worker_limit = concurrency
        .map(|value| value.max(1))
        .unwrap_or_else(|| default_worker_limit(total_files, total_bytes))
        .min(sources.len().max(1));
    let (s3_options, s3_storage) = if common.health_check {
        (s3, None)
    } else {
        let s3_storage = match s3 {
            Some(s3_options) => Some(s3_options.into_storage().await?),
            None => None,
        };
        (None, s3_storage)
    };
    let mut common = common;
    let parallel = sources.len() > 1;
    if parallel {
        // Each parallel source runs its own ingest pipeline: scale the batch
        // buffer down by worker count (floor 4 MiB) and use one write worker
        // per source, so total memory and task counts stay near the
        // single-source configuration instead of multiplying by workers.
        common.max_batch_bytes = (common.max_batch_bytes / worker_limit)
            .max(4 * 1024 * 1024)
            .min(common.max_batch_bytes);
    }
    let options = IngestOptions {
        common,
        s3: s3_options,
        s3_storage,
        health_file: health_file.clone(),
        manage_health: false,
        report_progress: false,
        log_writes: false,
        shutdown: Some(stop.clone()),
        write_workers: if parallel { Some(1) } else { None },
        // The parallel path sweeps once below; per-worker sweeps would race
        // over the same out_dir.
        sweep_orphans: false,
        line_transformer: if consolidate_ais || process_timestamps {
            Some(Box::new(AisConsolidator::new(AisConsolidatorConfig {
                process_timestamps,
                consolidate_multipart: consolidate_ais,
            })))
        } else {
            None
        },
    };
    if parallel {
        if let Some(storage) = options.s3_storage.clone().filter(|s| !s.keeps_local()) {
            let out_dir = options.common.out_dir.clone();
            tokio::spawn(async move {
                match collect_core::sweep_orphaned_uploads(out_dir, storage).await {
                    Ok(0) => {}
                    Ok(count) => {
                        if !quiet {
                            eprintln!(
                                "♻️  Uploaded {} orphaned parquet file(s) from a previous run",
                                count
                            );
                        }
                    }
                    Err(error) => eprintln!("⚠️  Orphan upload sweep failed: {}", error),
                }
            });
        }
    }
    if sources.len() == 1 {
        if !quiet && status_mode.is_plain() {
            eprintln!("▶️  Starting single-worker ingest");
        }
        let mut source = sources.pop().expect("single source");
        let completed_files = Arc::new(AtomicUsize::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let started_at = Instant::now();
        let status_task = if status_mode.is_tui() {
            Some(status::spawn_status_tui(
                total_files,
                completed_files.clone(),
                running.clone(),
                stop.clone(),
                started_at,
            ))
        } else {
            None
        };
        let result = run_ingest(
            &mut source,
            IngestOptions {
                common: options.common.clone(),
                s3: options.s3.clone(),
                s3_storage: options.s3_storage.clone(),
                health_file: health_file.clone(),
                manage_health: true,
                report_progress: false,
                log_writes: false,
                shutdown: Some(stop.clone()),
                write_workers: None,
                sweep_orphans: true,
                line_transformer: if consolidate_ais || process_timestamps {
                    Some(Box::new(AisConsolidator::new(AisConsolidatorConfig {
                        process_timestamps,
                        consolidate_multipart: consolidate_ais,
                    })))
                } else {
                    None
                },
            },
        )
        .await;

        if result.is_ok() {
            completed_files.store(1, Ordering::SeqCst);
            if let Ok(key) = source.completion_key() {
                if let Err(error) = completion_manifest::append_completed(&manifest_path, &key) {
                    eprintln!("⚠️  Failed to update completion manifest: {}", error);
                }
            }

            if status_mode.is_plain() {
                status::print_plain_update(1, total_files, started_at);
            }
        }

        running.store(false, Ordering::SeqCst);
        if let Some(status_task) = status_task {
            let _ = status_task.join();
        }

        return result;
    }

    let use_consolidator = consolidate_ais;
    let use_timestamps = process_timestamps;
    run_parallel_file_ingest(
        sources,
        options,
        health_file,
        total_files,
        worker_limit,
        manifest_path,
        status_mode,
        stop,
        use_consolidator,
        use_timestamps,
        quiet,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_parallel_file_ingest(
    sources: Vec<input::FileInputSource>,
    options: IngestOptions,
    health_file: PathBuf,
    total_files: usize,
    worker_limit: usize,
    manifest_path: PathBuf,
    status_mode: status::StatusMode,
    stop: Arc<AtomicBool>,
    consolidate_ais: bool,
    process_timestamps: bool,
    quiet: bool,
) -> Result<()> {
    let running = Arc::new(AtomicBool::new(true));
    update_health_status_async(&health_file, true).await?;
    let completed_files = Arc::new(AtomicUsize::new(0));
    let started_at = Instant::now();

    let health_running = running.clone();
    let health_file_for_task = health_file.clone();
    let health_task = tokio::spawn(async move {
        let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;

        while health_running.load(Ordering::SeqCst) {
            heartbeat.tick().await;
            if let Err(error) = update_health_status_async(&health_file_for_task, true).await {
                eprintln!("Failed to update health status: {}", error);
            }
        }
    });

    let status_task = if status_mode.is_tui() {
        Some(status::spawn_status_tui(
            total_files,
            completed_files.clone(),
            running.clone(),
            stop.clone(),
            started_at,
        ))
    } else {
        None
    };

    let queue = Arc::new(Mutex::new(VecDeque::from(sources)));

    let stop_for_signal = stop.clone();
    let running_for_signal = running.clone();
    let _signal_task = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(error) => {
                    eprintln!("Failed to register SIGTERM handler: {}", error);
                    return;
                }
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

        stop_for_signal.store(true, Ordering::SeqCst);
        running_for_signal.store(false, Ordering::SeqCst);
        status::request_cancel();
        eprintln!("🛑 Shutdown signal received. Stopping file workers...");
    });

    if !quiet && status_mode.is_plain() {
        eprintln!(
            "🧵 Starting parallel ingest with {} worker(s)",
            worker_limit
        );
    }
    let mut first_error = None;

    let mut workers = Vec::with_capacity(worker_limit);
    for _ in 0..worker_limit {
        let queue = queue.clone();
        let stop = stop.clone();
        let completed_files = completed_files.clone();
        let manifest_path = manifest_path.clone();
        let mut worker_options = options.clone();
        worker_options.line_transformer = if consolidate_ais || process_timestamps {
            Some(Box::new(AisConsolidator::new(AisConsolidatorConfig {
                process_timestamps,
                consolidate_multipart: consolidate_ais,
            })))
        } else {
            None
        };
        workers.push(tokio::spawn(async move {
            loop {
                if stop.load(Ordering::SeqCst) || status::is_cancelled() {
                    stop.store(true, Ordering::SeqCst);
                    break;
                }

                let next_source = {
                    let mut queue = queue.lock().await;
                    queue.pop_front()
                };

                let Some(mut source) = next_source else {
                    break;
                };

                if status::is_cancelled() {
                    stop.store(true, Ordering::SeqCst);
                    break;
                }

                if let Err(error) = run_ingest(&mut source, worker_options.clone()).await {
                    stop.store(true, Ordering::SeqCst);
                    return Err(error);
                }

                let completed = completed_files.fetch_add(1, Ordering::SeqCst) + 1;

                if let Ok(key) = source.completion_key() {
                    if let Err(error) = completion_manifest::append_completed(&manifest_path, &key)
                    {
                        eprintln!("⚠️  Failed to update completion manifest: {}", error);
                    }
                }

                if status_mode.is_plain()
                    && status::should_emit_plain_update(completed, total_files)
                {
                    status::print_plain_update(completed, total_files, started_at);
                }
            }

            Ok::<_, anyhow::Error>(())
        }));
    }

    for worker in workers {
        match worker.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
                stop.store(true, Ordering::SeqCst);
            }
            Err(error) => return Err(error.into()),
        }
    }

    running.store(false, Ordering::SeqCst);
    let _ = health_task.await;
    if let Some(status_task) = status_task {
        let _ = status_task.join();
    }

    if status::is_cancelled() {
        return Err(anyhow::anyhow!("ingest cancelled"));
    }

    update_health_status_async(&health_file, false).await?;

    if let Some(error) = first_error {
        return Err(error);
    }

    Ok(())
}

fn default_worker_limit(total_files: usize, total_bytes: u64) -> usize {
    let cores = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4);
    let avg_file_size = if total_files == 0 {
        0
    } else {
        total_bytes / total_files as u64
    };

    // Ingest is compression-bound (zstd on write, gzip/bzip2 on read), so
    // oversubscribing cores mostly thrashes. Small files get modest
    // oversubscription to hide per-file open/close latency.
    let worker_count = if avg_file_size < 16 * 1024 * 1024 {
        cores.saturating_mul(2)
    } else if avg_file_size < 128 * 1024 * 1024 {
        cores.saturating_mul(3) / 2
    } else {
        cores
    };

    worker_count.clamp(2, 32)
}
