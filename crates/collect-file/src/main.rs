use anyhow::{Context, Result};
use clap::Parser;
use collect_core::{
    default_source_from_path, health_file_path, run_ingest, update_health_status_async,
    CommonCliArgs, IngestOptions, S3CliArgs,
};
use collect_tui::{run_tui, TuiModel};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::Mutex;

mod completion_manifest;
mod input;

mod tui;

const PHASE_SCANNING: usize = 0;
const PHASE_DISPATCHING: usize = 1;
const PHASE_INGESTING: usize = 2;
const PHASE_DRAINING: usize = 3;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Recursively ingest plain, gzip, bzip2, and zip files into Hive-partitioned Parquet with Zstd compression, with optional AIS capture timestamps"
)]
struct Args {
    /// Input file or directory to ingest
    #[arg(short, long)]
    input: Option<PathBuf>,

    /// Logical source label; defaults to input file stem or directory name
    #[arg(short, long)]
    source: Option<String>,

    /// Use AIS capture timestamps when present, including NMEA c:<epoch> tag blocks, grouped \g fragments, and $PGHP capture lines
    #[arg(long)]
    ais: bool,

    #[command(flatten)]
    common: CommonCliArgs,

    #[command(flatten)]
    s3: S3CliArgs,

    /// Launch interactive TUI for configuration
    #[arg(long)]
    tui: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();

    if args.tui {
        let initial_config = tui::TuiConfig::load_from_env();

        match run_tui(initial_config)? {
            Some(config) => {
                let mut full_args = vec!["collect-file".to_string()];
                full_args.extend(config.to_cli_args());
                args = Args::parse_from(full_args);
            }
            None => {
                println!("Configuration cancelled.");
                return Ok(());
            }
        }
    }

    if args.input.is_none() {
        if let Ok(value) = std::env::var("INPUT_PATH") {
            args.input = Some(PathBuf::from(value));
        } else if let Ok(value) = std::env::var("INPUT_FILE") {
            args.input = Some(PathBuf::from(value));
        }
    }

    if args.source.is_none() {
        if let Ok(value) = std::env::var("SOURCE") {
            args.source = Some(value);
        }
    }

    if !args.ais {
        if let Ok(value) = std::env::var("AIS") {
            args.ais = matches!(value.to_ascii_lowercase().as_str(), "true" | "1");
        }
    }

    args.common.apply_env();
    args.s3.apply_env();

    let input = args
        .input
        .context("missing input path; set --input or INPUT_FILE")?;
    let source_name = args
        .source
        .unwrap_or_else(|| default_source_from_path(&input));

    let health_file = health_file_path("collect-file");
    eprintln!("📍 phase=scanning files=0/0 (0%)");
    eprintln!("🔎 Scanning input files...");
    let source = input::FileInputSource::new_parallel(input, source_name, args.ais).await?;
    eprintln!("📦 Discovered {} input job(s)", source.job_count());
    run_file_ingest(source, args.common.to_options(), args.s3.to_options(), health_file).await
}

async fn run_file_ingest(
    source: input::FileInputSource,
    common: collect_core::CommonOptions,
    s3: Option<collect_core::S3Options>,
    health_file: PathBuf,
) -> Result<()> {
    let manifest_path = completion_manifest::manifest_path(&common.out_dir);
    let completed = match completion_manifest::load_completed(&manifest_path) {
        Ok(completed) => completed,
        Err(error) => {
            eprintln!("⚠️  Failed to load completion manifest: {}", error);
            Default::default()
        }
    };
    let options = IngestOptions {
        common,
        s3,
        health_file: health_file.clone(),
        manage_health: false,
    };
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
        eprintln!("✅ No unfinished input files found");
        return Ok(());
    }

    let total_files = sources.len();
    let total_bytes = sources
        .iter()
        .map(input::FileInputSource::estimated_size_bytes)
        .sum();
    let worker_limit = default_worker_limit(total_files, total_bytes).min(sources.len().max(1));
    eprintln!(
        "🧵 Starting parallel ingest with {} worker(s)",
        worker_limit
    );
    if sources.len() == 1 {
        eprintln!("▶️  Starting single-worker ingest");
        let mut source = sources.pop().expect("single source");
        let result = run_ingest(
            &mut source,
            IngestOptions {
                common: options.common.clone(),
                s3: options.s3.clone(),
                health_file: health_file.clone(),
                manage_health: true,
            },
        )
        .await;

        if result.is_ok() {
            if let Ok(key) = source.completion_key() {
                if let Err(error) = completion_manifest::append_completed(&manifest_path, &key) {
                    eprintln!("⚠️  Failed to update completion manifest: {}", error);
                }
            }
        }

        return result;
    }

    run_parallel_file_ingest(sources, options, health_file, total_files, worker_limit, manifest_path)
        .await
}

async fn run_parallel_file_ingest(
    sources: Vec<input::FileInputSource>,
    options: IngestOptions,
    health_file: PathBuf,
    total_files: usize,
    worker_limit: usize,
    manifest_path: PathBuf,
) -> Result<()> {
    let running = Arc::new(AtomicBool::new(true));
    update_health_status_async(&health_file, true).await?;
    let phase = Arc::new(AtomicUsize::new(PHASE_DISPATCHING));
    let completed_files = Arc::new(AtomicUsize::new(0));

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

    let progress_running = running.clone();
    let progress_phase = phase.clone();
    let progress_completed = completed_files.clone();
    let progress_task = tokio::spawn(async move {
        let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;

        loop {
            heartbeat.tick().await;
            let phase_label = phase_label(progress_phase.load(Ordering::SeqCst));
            let completed = progress_completed.load(Ordering::SeqCst);
            if progress_running.load(Ordering::SeqCst) {
                eprintln!(
                    "📊 phase={} done={}/{}",
                    phase_label,
                    completed,
                    total_files,
                );
            } else {
                eprintln!(
                    "📊 phase={} done={}/{}",
                    phase_label,
                    completed,
                    total_files,
                );
                break;
            }
        }
    });

    let stop = Arc::new(AtomicBool::new(false));
    let queue = Arc::new(Mutex::new(VecDeque::from(sources)));

    let stop_for_signal = stop.clone();
    let running_for_signal = running.clone();
    let phase_for_signal = phase.clone();
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
        phase_for_signal.store(PHASE_DRAINING, Ordering::SeqCst);
        eprintln!("🛑 Shutdown signal received. Stopping file workers...");
    });

    eprintln!("🚚 phase=dispatching file jobs to workers...");
    phase.store(PHASE_DISPATCHING, Ordering::SeqCst);
    let mut first_error = None;

    let mut workers = Vec::with_capacity(worker_limit);
    for _ in 0..worker_limit {
        let queue = queue.clone();
        let stop = stop.clone();
        let completed_files = completed_files.clone();
        let phase = phase.clone();
        let manifest_path = manifest_path.clone();
        let worker_options = options.clone();
        workers.push(tokio::spawn(async move {
            loop {
                if stop.load(Ordering::SeqCst) {
                    break;
                }

                let next_source = {
                    let mut queue = queue.lock().await;
                    queue.pop_front()
                };

                let Some(mut source) = next_source else {
                    break;
                };

                phase.store(PHASE_INGESTING, Ordering::SeqCst);
                if let Err(error) = run_ingest(&mut source, worker_options.clone()).await {
                    stop.store(true, Ordering::SeqCst);
                    return Err(error);
                }

                let completed = completed_files.fetch_add(1, Ordering::SeqCst) + 1;
                if completed >= total_files {
                    phase.store(PHASE_DRAINING, Ordering::SeqCst);
                }

                if let Ok(key) = source.completion_key() {
                    if let Err(error) = completion_manifest::append_completed(&manifest_path, &key) {
                        eprintln!("⚠️  Failed to update completion manifest: {}", error);
                    }
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
    phase.store(PHASE_DRAINING, Ordering::SeqCst);
    let _ = health_task.await;
    let _ = progress_task.await;
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

    let worker_count = if avg_file_size < 16 * 1024 * 1024 {
        cores.saturating_mul(4)
    } else if avg_file_size < 128 * 1024 * 1024 {
        cores.saturating_mul(3)
    } else {
        cores.saturating_mul(2)
    };

    worker_count.clamp(4, 64)
}

fn phase_label(phase: usize) -> &'static str {
    match phase {
        PHASE_SCANNING => "scan",
        PHASE_DISPATCHING => "dispatch",
        PHASE_INGESTING => "ingest",
        PHASE_DRAINING => "drain",
        _ => "unknown",
    }
}
