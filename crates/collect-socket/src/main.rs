use anyhow::{Context, Result};
use clap::Parser;
use collect_core::ais_consolidate::{AisConsolidator, AisConsolidatorConfig};
use collect_core::backoff::{Backoff, ReconnectCliArgs};
use collect_core::{
    apply_config_file, health_file_path, line_reader_from_async_read, log, print_completions,
    run_ingest, CommonCliArgs, IngestOptions, LineReader, LineSource, ReaderTransition, S3CliArgs,
    log::LoggingCliArgs,
};
use collect_core::availability::{self, AvailabilityCliArgs};
use collect_core::iceberg::{init_raw_handle, IcebergCliArgs};
use collect_core::silver::ParserCliArgs;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::{Instant, Sleep};

const TCP_RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const TCP_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);
/// Well under the 300s data-drought exit, so a half-open or stalled upstream
/// gets several reconnect attempts before the process gives up entirely.
const DEFAULT_IDLE_RECONNECT_SECONDS: u64 = 60;

#[derive(Parser, Debug)]
#[command(
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_COMMIT_HASH"), ")"),
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

    /// Suppress informational progress lines; warnings and errors still print
    #[arg(short, long, env = "QUIET")]
    quiet: bool,

    /// Enable AIS multi-part message reassembly (combines fragmented NMEA
    /// sentences into single sentences before writing).
    #[arg(long, env = "CONSOLIDATE_AIS", value_parser = clap::builder::FalseyValueParser::new())]
    consolidate_ais: bool,

    /// Process $PGHP timestamp lines and tag-block c: carry-forward to
    /// correct row timestamps. Independent of --consolidate-ais.
    #[arg(long, env = "PROCESS_TIMESTAMPS", value_parser = clap::builder::FalseyValueParser::new())]
    process_timestamps: bool,

    /// Use the MSSIS receiver epoch appended after the NMEA checksum
    /// (`!AIVDM,...*CS,<epoch>` or `...*CS,<station>,<epoch>`) as the row
    /// timestamp, and keep the station. Independent of --process-timestamps.
    #[arg(long, env = "MSSIS", value_parser = clap::builder::FalseyValueParser::new())]
    mssis: bool,

    /// Drop and re-establish the TCP connection after this many seconds with
    /// no bytes received from upstream (0 = never; rely on the OS to notice)
    #[arg(long, env = "IDLE_RECONNECT_SECONDS", default_value_t = DEFAULT_IDLE_RECONNECT_SECONDS)]
    idle_reconnect_seconds: u64,

    #[command(flatten)]
    common: CommonCliArgs,

    #[command(flatten)]
    logging: LoggingCliArgs,

    #[command(flatten)]
    reconnect: ReconnectCliArgs,

    #[command(flatten)]
    s3: S3CliArgs,

    #[command(flatten)]
    iceberg: IcebergCliArgs,

    #[command(flatten)]
    availability: AvailabilityCliArgs,

    #[command(flatten)]
    parser: ParserCliArgs,

    /// Print shell completions for the given shell to stdout and exit
    #[arg(long, exclusive = true)]
    completions: Option<clap_complete::Shell>,

    /// Load flag defaults from a flat TOML config file (KEY = value, using
    /// the same env var names shown in --help); explicit flags and
    /// already-set env vars still take precedence over the file
    #[arg(long, env = "CONFIG_FILE")]
    config: Option<PathBuf>,
}

/// Wraps a reader so that a read pending for longer than `timeout` with no
/// bytes arriving fails with `TimedOut`. A silently dead upstream (half-open
/// socket, stalled feed) otherwise never produces EOF or an error, so the
/// ingest loop would wait forever and only the data-drought exit would fire.
struct IdleTimeoutRead<R> {
    inner: R,
    timeout: Duration,
    deadline: Pin<Box<Sleep>>,
}

impl<R> IdleTimeoutRead<R> {
    fn new(inner: R, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            deadline: Box::pin(tokio::time::sleep(timeout)),
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for IdleTimeoutRead<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        let before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(result) => {
                if buf.filled().len() > before {
                    let next = Instant::now() + this.timeout;
                    this.deadline.as_mut().reset(next);
                }
                Poll::Ready(result)
            }
            Poll::Pending => match this.deadline.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("no data received for {}s", this.timeout.as_secs()),
                ))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

struct TcpInputSource {
    host: String,
    port: u16,
    source: String,
    max_reconnect_seconds: u64,
    idle_timeout: Option<Duration>,
}

impl TcpInputSource {
    fn new(
        host: String,
        port: u16,
        source: String,
        max_reconnect_seconds: u64,
        idle_reconnect_seconds: u64,
    ) -> Self {
        Self {
            host,
            port,
            source,
            max_reconnect_seconds,
            idle_timeout: (idle_reconnect_seconds > 0)
                .then(|| Duration::from_secs(idle_reconnect_seconds)),
        }
    }

    async fn connect(&self, max_line_length: usize) -> Result<LineReader> {
        let stream = TcpStream::connect(format!("{}:{}", self.host, self.port))
            .await
            .with_context(|| format!("connect to TCP {}:{}", self.host, self.port))?;
        stream.set_nodelay(true).context("set TCP nodelay")?;
        Ok(match self.idle_timeout {
            Some(timeout) => {
                line_reader_from_async_read(IdleTimeoutRead::new(stream, timeout), max_line_length)
            }
            None => line_reader_from_async_read(stream, max_line_length),
        })
    }

    async fn reconnect(
        &self,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        let upstream = format!("TCP {}:{}", self.host, self.port);
        let mut backoff = Backoff::new(
            TCP_RECONNECT_INITIAL_DELAY,
            TCP_RECONNECT_MAX_DELAY,
            self.max_reconnect_seconds,
        );

        loop {
            if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(ReaderTransition::Stop);
            }

            log::warn(
                "reconnect_attempt",
                &format!("{upstream} disconnected, attempting to reconnect"),
                &[("upstream", &upstream)],
            );
            match self.connect(max_line_length).await {
                Ok(reader) => {
                    log::info(
                        "reconnected",
                        &format!("reconnected to {upstream}"),
                        &[("upstream", &upstream)],
                    );
                    return Ok(ReaderTransition::Continue(reader));
                }
                Err(error) => {
                    log::warn(
                        "reconnect_failed",
                        &format!("reconnect to {upstream} failed: {error}"),
                        &[("upstream", &upstream)],
                    );
                }
            }

            if !backoff.wait(shutdown).await {
                if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                    return Ok(ReaderTransition::Stop);
                }
                return Ok(collect_core::backoff::give_up(
                    &upstream,
                    self.max_reconnect_seconds,
                ));
            }
        }
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
        error: &collect_core::LinesCodecError,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        let upstream = format!("TCP {}:{}", self.host, self.port);
        log::warn(
            "stream_error",
            &format!("{upstream} read failed: {error}"),
            &[("upstream", &upstream)],
        );
        self.reconnect(shutdown, max_line_length).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();

    if let Some(shell) = args.completions {
        print_completions::<Args>(shell, "collect-socket");
        return Ok(());
    }

    if let Some(config_path) = &args.config {
        apply_config_file(config_path)?;
        args = Args::parse();
    }

    args.logging.init("collect-socket");

    let host = args
        .tcp_host
        .context("missing TCP host; set --tcp-host or TCP_HOST")?;
    let port = args
        .tcp_port
        .context("missing TCP port; set --tcp-port or TCP_PORT")?;
    let source_name = args.source.unwrap_or_else(|| "tcp".to_string());

    if !args.common.health_check {
        availability::init_stream(
            &args.iceberg,
            &args.availability,
            "collect-socket",
            &format!("tcp://{host}:{port}"),
            &source_name,
        )
        .await;
    }

    let health_file = health_file_path("collect-socket");
    let mut source = TcpInputSource::new(
        host,
        port,
        source_name,
        args.reconnect.max_reconnect_seconds,
        args.idle_reconnect_seconds,
    );

    let common_options = args.common.to_options();
    let iceberg = if common_options.health_check {
        None
    } else {
        init_raw_handle(
            &args.iceberg,
            common_options.partition.as_str(),
            common_options.compression_level,
        )
        .await?
    };
    let silver = if common_options.health_check {
        None
    } else {
        collect_silver::init_silver(
            &args.parser,
            &args.iceberg,
            &common_options.out_dir,
            common_options.partition,
            common_options.compression_level,
        )
        .await?
    };

    let result = run_ingest(
        &mut source,
        IngestOptions {
            common: common_options,
            s3: args.s3.to_options(),
            s3_storage: None,
            iceberg,
            health_file,
            manage_health: true,
            report_progress: !args.quiet,
            log_writes: !args.quiet,
            shutdown: None,
            write_workers: None,
            sweep_orphans: true,
            line_transformer: if args.consolidate_ais || args.process_timestamps || args.mssis {
                Some(Box::new(AisConsolidator::new(AisConsolidatorConfig {
                    process_timestamps: args.process_timestamps,
                    mssis_timestamps: args.mssis,
                    consolidate_multipart: args.consolidate_ais,
                })))
            } else {
                None
            },
            silver,
        },
    )
    .await;
    // Exits UPSTREAM_UNAVAILABLE / DATA_DROUGHT only after run_ingest has
    // flushed and drained every buffered row.
    collect_core::exitcode::exit_if_requested(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test(start_paused = true)]
    async fn idle_read_times_out_when_upstream_goes_quiet() {
        let (mut tx, rx) = tokio::io::duplex(64);
        let mut reader = IdleTimeoutRead::new(rx, Duration::from_secs(60));
        let mut buf = [0u8; 16];

        tx.write_all(b"hello\n").await.unwrap();
        assert_eq!(reader.read(&mut buf).await.unwrap(), 6);

        // Keep the writer open but send nothing: the read must fail rather
        // than hang, so the source reconnects.
        let error = reader.read(&mut buf).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        drop(tx);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_deadline_resets_on_each_read() {
        let (mut tx, rx) = tokio::io::duplex(64);
        let mut reader = IdleTimeoutRead::new(rx, Duration::from_secs(60));
        let mut buf = [0u8; 16];

        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(45)).await;
            tx.write_all(b"x").await.unwrap();
            assert_eq!(reader.read(&mut buf).await.unwrap(), 1);
        }
    }
}
