use anyhow::{Context, Result};
use clap::Parser;
use collect_core::{
    default_source_from_path, health_file_path, run_ingest, update_health_status_async,
    CommonCliArgs, IngestOptions, S3CliArgs,
};
use collect_tui::{run_tui, TuiModel};
use futures_util::stream::{self, StreamExt};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

mod input;

mod tui;

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
    if source.job_count() <= 1 {
        eprintln!("▶️  Starting single-worker ingest");
        let mut source = source;
        return run_ingest(
            &mut source,
            IngestOptions {
                common,
                s3,
                health_file,
                manage_health: true,
            },
        )
        .await;
    }

    let options = IngestOptions {
        common,
        s3,
        health_file: health_file.clone(),
        manage_health: false,
    };
    let total_files = source.job_count();
    let sources = source.into_job_sources();
    eprintln!(
        "🧵 Starting parallel ingest with {} worker(s)",
        sources.len().min(default_worker_limit())
    );
    run_parallel_file_ingest(sources, options, health_file, total_files).await
}

async fn run_parallel_file_ingest(
    sources: Vec<input::FileInputSource>,
    options: IngestOptions,
    health_file: PathBuf,
    total_files: usize,
) -> Result<()> {
    let running = Arc::new(AtomicBool::new(true));
    update_health_status_async(&health_file, true).await?;
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
    let progress_completed = completed_files.clone();
    let progress_task = tokio::spawn(async move {
        let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;

        loop {
            heartbeat.tick().await;
            let completed = progress_completed.load(Ordering::SeqCst);
            let percent = if total_files == 0 {
                100
            } else {
                (completed.saturating_mul(100)) / total_files
            };
            if progress_running.load(Ordering::SeqCst) {
                eprintln!("📊 Files processed: {}/{} ({}%)", completed, total_files, percent);
            } else {
                eprintln!("📊 Draining: {}/{} files complete ({}%)", completed, total_files, percent);
                break;
            }
        }
    });

    let stop = Arc::new(AtomicBool::new(false));

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
        eprintln!("🛑 Shutdown signal received. Stopping file workers...");
    });

    let worker_limit = default_worker_limit().min(sources.len().max(1));
    eprintln!("🚚 Dispatching file jobs to workers...");
    let mut tasks = stream::iter(sources.into_iter())
        .map(|source| {
            let stop = stop.clone();
            let completed_files = completed_files.clone();
            let worker_options = options.clone();
            async move {
                if stop.load(Ordering::SeqCst) {
                    return Ok::<_, anyhow::Error>(());
                }

                let mut source = source;
                if let Err(error) = run_ingest(&mut source, worker_options).await {
                    stop.store(true, Ordering::SeqCst);
                    return Err(error);
                }

                completed_files.fetch_add(1, Ordering::SeqCst);

                Ok(())
            }
        })
        .buffer_unordered(worker_limit);

    let mut first_error = None;
    while let Some(result) = tasks.next().await {
        if let Err(error) = result {
            first_error = Some(error);
            stop.store(true, Ordering::SeqCst);
        }
    }

    running.store(false, Ordering::SeqCst);
    let _ = health_task.await;
    let _ = progress_task.await;
    update_health_status_async(&health_file, false).await?;

    if let Some(error) = first_error {
        return Err(error);
    }

    Ok(())
}

fn default_worker_limit() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get().saturating_mul(2))
        .unwrap_or(8)
        .clamp(4, 32)
}
