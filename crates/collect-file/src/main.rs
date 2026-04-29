use anyhow::{Context, Result};
use clap::Parser;
use collect_core::{
    default_source_from_path, health_file_path, run_ingest, update_health_status_async,
    CommonCliArgs, IngestOptions, S3CliArgs,
};
use collect_tui::{run_tui, TuiModel};
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

    /// Disable the runtime status UI and print aggregate updates every 10 files
    #[arg(long)]
    noui: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    let noui = args.noui;

    if args.tui {
        let initial_config = tui::TuiConfig::load_from_env();

        match run_tui(initial_config)? {
            Some(config) => {
                let mut full_args = vec!["collect-file".to_string()];
                full_args.extend(config.to_cli_args());
                args = Args::parse_from(full_args);
                args.noui = noui;
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

    args.noui = noui;

    args.common.apply_env();
    args.s3.apply_env();

    let status_mode = status::StatusMode::from_tty(!args.noui && std::io::stdout().is_terminal());

    let input = args
        .input
        .context("missing input path; set --input or INPUT_FILE")?;
    let source_name = args
        .source
        .unwrap_or_else(|| default_source_from_path(&input));

    let health_file = health_file_path("collect-file");
    eprintln!("🔎 Scanning input files...");
    let source = input::FileInputSource::new_parallel(input, source_name, args.ais).await?;
    if status_mode.is_plain() {
        eprintln!("📦 Discovered {} input job(s)", source.job_count());
    }
    run_file_ingest(
        source,
        args.common.to_options(),
        args.s3.to_options(),
        health_file,
        status_mode,
    )
    .await
}

async fn run_file_ingest(
    source: input::FileInputSource,
    common: collect_core::CommonOptions,
    s3: Option<collect_core::S3Options>,
    health_file: PathBuf,
    status_mode: status::StatusMode,
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
    let options = IngestOptions {
        common,
        s3,
        health_file: health_file.clone(),
        manage_health: false,
        report_progress: false,
        log_writes: false,
        shutdown: Some(stop.clone()),
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
    if sources.len() == 1 {
        if status_mode.is_plain() {
            eprintln!(
                "🧵 Starting parallel ingest with {} worker(s)",
                worker_limit
            );
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
                health_file: health_file.clone(),
                manage_health: true,
                report_progress: false,
                log_writes: false,
                shutdown: Some(stop.clone()),
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

    run_parallel_file_ingest(
        sources,
        options,
        health_file,
        total_files,
        worker_limit,
        manifest_path,
        status_mode,
        stop,
    )
    .await
}

async fn run_parallel_file_ingest(
    sources: Vec<input::FileInputSource>,
    options: IngestOptions,
    health_file: PathBuf,
    total_files: usize,
    worker_limit: usize,
    manifest_path: PathBuf,
    status_mode: status::StatusMode,
    _stop: Arc<AtomicBool>,
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

    let stop = Arc::new(AtomicBool::new(false));
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

    if status_mode.is_plain() {
        eprintln!(
            "🧵 Starting parallel ingest with {} worker(s)",
            worker_limit
        );
    }
    let mut first_error = None;

    let mut workers = Vec::with_capacity(worker_limit);
    for _ in 0..worker_limit {
        let queue = queue.clone();
        let stop = _stop.clone();
        let completed_files = completed_files.clone();
        let manifest_path = manifest_path.clone();
        let worker_options = options.clone();
        let status_mode = status_mode;
        let started_at = started_at;
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

    let worker_count = if avg_file_size < 16 * 1024 * 1024 {
        cores.saturating_mul(4)
    } else if avg_file_size < 128 * 1024 * 1024 {
        cores.saturating_mul(3)
    } else {
        cores.saturating_mul(2)
    };

    worker_count.clamp(4, 64)
}
