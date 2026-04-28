mod commands;
mod partition;
mod progress;
mod storage;

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use collect_core::PartitionGranularity;
use commands::{compact, inspect, vacuum, validate};
use progress::{count, report};
use storage::{StorageConfig, StorageLocation};

fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get().saturating_mul(2))
        .unwrap_or(8)
        .clamp(4, 32)
}

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Maintain hive-partitioned Parquet collections stored locally or on S3",
    after_help = "Examples:\n  collect-maint --root data --partition minute inspect\n  collect-maint --root data --partition hour compact\n  collect-maint --root data --partition day compact --apply\n  collect-maint --root data --partition month vacuum\n  collect-maint --root data --partition year vacuum --apply\n\nNotes:\n  partition is required and must match the dataset layout. compact and vacuum are dry-run by default. Re-run with --apply to make changes."
)]
struct Cli {
    #[command(flatten)]
    storage: StorageArgs,

    /// Partition granularity of the dataset layout
    #[arg(long, value_enum)]
    partition: PartitionGranularity,

    /// Maximum number of concurrent maintenance workers
    #[arg(long, global = true, default_value_t = default_concurrency())]
    concurrency: usize,

    /// Zstd compression level for compacted Parquet output
    #[arg(long, global = true, default_value_t = 5)]
    compression_level: i32,

    #[command(subcommand)]
    command: Command,
}

#[derive(Args, Debug, Clone)]
struct StorageArgs {
    /// Local dataset root (default: data)
    #[arg(long)]
    root: Option<std::path::PathBuf>,

    /// S3 bucket containing the dataset
    #[arg(long)]
    s3_bucket: Option<String>,

    /// Optional dataset prefix inside the bucket
    #[arg(long, default_value = "")]
    s3_prefix: String,

    /// S3 endpoint URL (for MinIO or other S3-compatible storage)
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 region (default: us-east-1)
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,

    /// S3 access key ID
    #[arg(long)]
    s3_access_key: Option<String>,

    /// S3 secret access key
    #[arg(long)]
    s3_secret_key: Option<String>,

    /// Disable TLS/HTTPS for the S3 endpoint
    #[arg(long)]
    s3_disable_tls: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Summarize partitions, file counts, and sizes
    Inspect {
        /// Print per-partition details
        #[arg(long)]
        verbose: bool,
    },

    /// Validate parquet files and partition timestamps
    Validate,

    /// Compact small parquet files within a single partition (dry run by default; use --apply to execute)
    Compact {
        /// Target maximum bytes per compacted file
        #[arg(long, default_value_t = 268_435_456)]
        target_file_size_bytes: u64,

        /// Actually apply changes (dry run by default)
        #[arg(long)]
        apply: bool,
    },

    /// Clean up temporary files and interrupted compaction manifests (dry run by default; use --apply to execute)
    Vacuum {
        /// Actually apply changes (dry run by default)
        #[arg(long)]
        apply: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let storage = cli.storage.into_location().await?;
    let concurrency = cli.concurrency.max(1);
    report(
        "collect-maint",
        format!(
            "loading dataset entries from {} with {} workers",
            storage.dataset_label(),
            count(concurrency)
        ),
    );
    let entries = storage
        .list_entries(cli.partition, concurrency, |listed| {
            if listed > 0 {
                report(
                    "collect-maint",
                    format!("listed {} entries so far", count(listed)),
                );
            }
        })
        .await
        .context("listing dataset entries")?;
    report(
        "collect-maint",
        format!("loaded {} entries", count(entries.len())),
    );

    match cli.command {
        Command::Inspect { verbose } => inspect(&storage, &entries, concurrency, verbose).await,
        Command::Validate => validate(&storage, &entries, concurrency).await,
        Command::Compact {
            target_file_size_bytes,
            apply,
        } => {
            compact(
                &storage,
                &entries,
                target_file_size_bytes,
                apply,
                concurrency,
                cli.compression_level,
            )
            .await
        }
        Command::Vacuum { apply } => {
            vacuum(&storage, &entries, cli.partition, apply, concurrency).await
        }
    }
}

impl StorageArgs {
    async fn into_location(self) -> Result<StorageLocation> {
        if self.root.is_some() && self.s3_bucket.is_some() {
            bail!("use either --root for local storage or --s3-bucket for S3, not both");
        }

        let config = StorageConfig {
            root: self.root,
            s3_bucket: self.s3_bucket,
            s3_prefix: self.s3_prefix,
            s3_endpoint: self.s3_endpoint,
            s3_region: self.s3_region,
            s3_access_key: self.s3_access_key,
            s3_secret_key: self.s3_secret_key,
            s3_disable_tls: self.s3_disable_tls,
        };

        config.into_location().await
    }
}
