use anyhow::{Context, Result};
use clap::Parser;
use collect_core::ais_consolidate::{AisConsolidator, AisConsolidatorConfig};
use collect_core::{
    health_file_path, line_reader_from_async_read, run_ingest, CommonCliArgs, IngestOptions,
    LineReader, LineSource, ReaderTransition, S3CliArgs,
};
use std::cmp::min;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::net::TcpStream;

const TCP_RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const TCP_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Consume newline-delimited TCP data into Hive-partitioned Parquet with Zstd compression"
)]
struct Args {
    /// TCP host address to receive data from
    #[arg(long, env = "TCP_HOST", requires = "tcp_port")]
    tcp_host: Option<String>,

    /// TCP port to receive data from
    #[arg(long, env = "TCP_PORT", requires = "tcp_host")]
    tcp_port: Option<u16>,

    /// Logical source label; defaults to "tcp"
    #[arg(short, long, env = "SOURCE")]
    source: Option<String>,

    /// Enable AIS multi-part message reassembly (combines fragmented NMEA
    /// sentences into single sentences before writing).
    #[arg(long)]
    consolidate_ais: bool,

    /// Process $PGHP timestamp lines and tag-block c: carry-forward to
    /// correct row timestamps. Independent of --consolidate-ais.
    #[arg(long)]
    process_timestamps: bool,

    #[command(flatten)]
    common: CommonCliArgs,

    #[command(flatten)]
    s3: S3CliArgs,
}

struct TcpInputSource {
    host: String,
    port: u16,
    source: String,
}

impl TcpInputSource {
    fn new(host: String, port: u16, source: String) -> Self {
        Self { host, port, source }
    }

    async fn connect(&self, max_line_length: usize) -> Result<LineReader> {
        let stream = TcpStream::connect(format!("{}:{}", self.host, self.port))
            .await
            .with_context(|| format!("connect to TCP {}:{}", self.host, self.port))?;
        stream.set_nodelay(true).context("set TCP nodelay")?;
        Ok(line_reader_from_async_read(stream, max_line_length))
    }

    async fn reconnect(
        &self,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        let mut delay = TCP_RECONNECT_INITIAL_DELAY;

        while !shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            eprintln!(
                "TCP input disconnected. Reconnecting to {}:{} in {}s...",
                self.host,
                self.port,
                delay.as_secs()
            );
            tokio::time::sleep(delay).await;

            if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(ReaderTransition::Stop);
            }

            match self.connect(max_line_length).await {
                Ok(reader) => {
                    println!("Reconnected to TCP {}:{}", self.host, self.port);
                    return Ok(ReaderTransition::Continue(reader));
                }
                Err(error) => {
                    eprintln!(
                        "Reconnect failed for TCP {}:{}: {}",
                        self.host, self.port, error
                    );
                    delay = min(delay.saturating_mul(2), TCP_RECONNECT_MAX_DELAY);
                }
            }
        }

        Ok(ReaderTransition::Stop)
    }
}

#[collect_core::async_trait]
impl LineSource for TcpInputSource {
    fn source_name(&self) -> &str {
        &self.source
    }

    async fn open(&mut self, max_line_length: usize) -> Result<LineReader> {
        self.connect(max_line_length).await
    }

    async fn on_stream_end(
        &mut self,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        self.reconnect(shutdown, max_line_length).await
    }

    async fn on_stream_error(
        &mut self,
        _error: &collect_core::LinesCodecError,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        self.reconnect(shutdown, max_line_length).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let host = args
        .tcp_host
        .context("missing TCP host; set --tcp-host or TCP_HOST")?;
    let port = args
        .tcp_port
        .context("missing TCP port; set --tcp-port or TCP_PORT")?;
    let source_name = args.source.unwrap_or_else(|| "tcp".to_string());

    let health_file = health_file_path("collect-socket");
    let mut source = TcpInputSource::new(host, port, source_name);

    run_ingest(
        &mut source,
        IngestOptions {
            common: args.common.to_options(),
            s3: args.s3.to_options(),
            s3_storage: None,
            health_file,
            manage_health: true,
            report_progress: true,
            log_writes: true,
            shutdown: None,
            write_workers: None,
            sweep_orphans: true,
            line_transformer: if args.consolidate_ais || args.process_timestamps {
                Some(Box::new(AisConsolidator::new(AisConsolidatorConfig {
                    process_timestamps: args.process_timestamps,
                    consolidate_multipart: args.consolidate_ais,
                })))
            } else {
                None
            },
        },
    )
    .await
}
