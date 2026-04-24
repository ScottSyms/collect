use anyhow::{Context, Result};
use clap::Parser;
use collect_core::{
    default_source_from_path, health_file_path, run_ingest, CommonCliArgs, IngestOptions, S3CliArgs,
};
use collect_tui::{run_tui, TuiModel};
use std::path::PathBuf;

mod input;

mod tui;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Recursively ingest plain, gzip, bzip2, and zip files into Hive-partitioned Parquet with Zstd compression"
)]
struct Args {
    /// Input file or directory to ingest
    #[arg(short, long)]
    input: Option<PathBuf>,

    /// Logical source label; defaults to input file stem or directory name
    #[arg(short, long)]
    source: Option<String>,

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

    args.common.apply_env();
    args.s3.apply_env();

    let input = args
        .input
        .context("missing input path; set --input or INPUT_FILE")?;
    let source_name = args
        .source
        .unwrap_or_else(|| default_source_from_path(&input));

    let health_file = health_file_path("collect-file");
    let mut source = input::FileInputSource::new(input, source_name)?;

    run_ingest(
        &mut source,
        IngestOptions {
            common: args.common.to_options(),
            s3: args.s3.to_options(),
            health_file,
        },
    )
    .await
}
