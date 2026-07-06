use anyhow::{Context as _, Result};
use clap::Parser;
use collect_core::{
    health_file_path, line_reader_from_async_read, run_ingest, CommonCliArgs, IngestOptions,
    LineReader, LineSource, ReaderTransition, S3CliArgs,
};
use futures_util::{SinkExt, StreamExt};
use std::cmp::min;
use std::io;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const WS_URL: &str = "wss://stream.aisstream.io/v0/stream";
const WS_RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const WS_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Consume AIS data from aisstream.io WebSocket API into Hive-partitioned Parquet with Zstd compression"
)]
struct Args {
    /// AISStream API key
    #[arg(long)]
    api_key: Option<String>,

    /// Bounding boxes as JSON matching the AISStream API format, e.g.
    /// '[[[-90,-180],[90,180]]]' for the entire world
    #[arg(long)]
    bounding_boxes: Option<String>,

    /// Only receive messages from these MMSI values (repeatable, max 50)
    #[arg(long)]
    filter_mmsi: Vec<String>,

    /// Only receive these message types (repeatable), e.g. "PositionReport"
    #[arg(long)]
    filter_message_types: Vec<String>,

    /// Logical source label; defaults to "aisstream"
    #[arg(short, long)]
    source: Option<String>,

    #[command(flatten)]
    common: CommonCliArgs,

    #[command(flatten)]
    s3: S3CliArgs,
}

struct WebSocketReadAdapter {
    stream: Pin<Box<dyn futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Send>>,
    buffer: Vec<u8>,
    pos: usize,
    _write: Pin<Box<dyn futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Send>>,
}

impl WebSocketReadAdapter {
    fn new(
        stream: impl futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Send + 'static,
        write: impl futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Send + 'static,
    ) -> Self {
        Self {
            stream: Box::pin(stream),
            buffer: Vec::new(),
            pos: 0,
            _write: Box::pin(write),
        }
    }
}

impl AsyncRead for WebSocketReadAdapter {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.pos < self.buffer.len() {
            let avail = self.buffer.len() - self.pos;
            let n = min(buf.remaining(), avail);
            buf.put_slice(&self.buffer[self.pos..self.pos + n]);
            self.pos += n;
            if self.pos >= self.buffer.len() {
                self.buffer.clear();
                self.pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        loop {
            match self.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(Message::Text(text)))) => {
                    self.buffer = text.into_bytes();
                    self.buffer.push(b'\n');
                    self.pos = 0;
                    let avail = self.buffer.len();
                    let n = min(buf.remaining(), avail);
                    buf.put_slice(&self.buffer[..n]);
                    self.pos = n;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Ok(Message::Binary(data)))) => {
                    self.buffer = data;
                    self.buffer.push(b'\n');
                    self.pos = 0;
                    let avail = self.buffer.len();
                    let n = min(buf.remaining(), avail);
                    buf.put_slice(&self.buffer[..n]);
                    self.pos = n;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)))) => {
                    continue;
                }
                Poll::Ready(Some(Ok(Message::Close(_)))) => {
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e)));
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

struct AisStreamSource {
    api_key: String,
    bounding_boxes: String,
    filter_mmsi: Vec<String>,
    filter_message_types: Vec<String>,
    source: String,
}

impl AisStreamSource {
    fn new(
        api_key: String,
        bounding_boxes: String,
        filter_mmsi: Vec<String>,
        filter_message_types: Vec<String>,
        source: String,
    ) -> Self {
        Self {
            api_key,
            bounding_boxes,
            filter_mmsi,
            filter_message_types,
            source,
        }
    }

    async fn connect(&self, max_line_length: usize) -> Result<LineReader> {
        let (ws_stream, _) = connect_async(WS_URL)
            .await
            .context("WebSocket connection to aisstream.io")?;

        let mut subscribe = serde_json::Map::new();
        subscribe.insert("APIKey".into(), serde_json::Value::String(self.api_key.clone()));

        let bboxes: serde_json::Value =
            serde_json::from_str(&self.bounding_boxes)
                .context("Failed to parse --bounding-boxes as JSON")?;
        subscribe.insert("BoundingBoxes".into(), bboxes);

        if !self.filter_mmsi.is_empty() {
            let mmsi: Vec<serde_json::Value> = self
                .filter_mmsi
                .iter()
                .map(|v| serde_json::Value::String(v.clone()))
                .collect();
            subscribe.insert("FiltersShipMMSI".into(), serde_json::Value::Array(mmsi));
        }

        if !self.filter_message_types.is_empty() {
            let types: Vec<serde_json::Value> = self
                .filter_message_types
                .iter()
                .map(|v| serde_json::Value::String(v.clone()))
                .collect();
            subscribe.insert("FilterMessageTypes".into(), serde_json::Value::Array(types));
        }

        let (mut write, read) = ws_stream.split();
        let subscribe_json = serde_json::to_string(&subscribe)?;

        let msg = Message::Text(subscribe_json);
        if let Err(e) = write.send(msg).await {
            anyhow::bail!("Failed to send subscription message: {}", e);
        }

        let adapter = WebSocketReadAdapter::new(read, write);
        Ok(line_reader_from_async_read(adapter, max_line_length))
    }

    async fn reconnect(
        &self,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        let mut delay = WS_RECONNECT_INITIAL_DELAY;

        while !shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            eprintln!(
                "AISStream disconnected. Reconnecting in {}s...",
                delay.as_secs()
            );
            tokio::time::sleep(delay).await;

            if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(ReaderTransition::Stop);
            }

            match self.connect(max_line_length).await {
                Ok(reader) => {
                    println!("Reconnected to AISStream");
                    return Ok(ReaderTransition::Continue(reader));
                }
                Err(error) => {
                    eprintln!("Reconnect failed: {}", error);
                    delay = min(delay.saturating_mul(2), WS_RECONNECT_MAX_DELAY);
                }
            }
        }

        Ok(ReaderTransition::Stop)
    }
}

#[collect_core::async_trait]
impl LineSource for AisStreamSource {
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
    let mut args = Args::parse();

    args.common.apply_env();
    args.s3.apply_env();

    if args.api_key.is_none() {
        if let Ok(value) = std::env::var("AISSTREAM_API_KEY") {
            args.api_key = Some(value);
        }
    }

    if args.bounding_boxes.is_none() {
        if let Ok(value) = std::env::var("BOUNDING_BOXES") {
            args.bounding_boxes = Some(value);
        }
    }

    if args.source.is_none() {
        if let Ok(value) = std::env::var("SOURCE") {
            args.source = Some(value);
        }
    }

    if args.filter_mmsi.is_empty() {
        if let Ok(value) = std::env::var("FILTER_MMSI") {
            args.filter_mmsi = value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
    }

    if args.filter_message_types.is_empty() {
        if let Ok(value) = std::env::var("FILTER_MESSAGE_TYPES") {
            args.filter_message_types = value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
    }

    let api_key = args
        .api_key
        .context("missing API key; set --api-key or AISSTREAM_API_KEY")?;
    let bounding_boxes = args
        .bounding_boxes
        .context("missing bounding boxes; set --bounding-boxes or BOUNDING_BOXES")?;
    let source_name = args.source.unwrap_or_else(|| "aisstream".to_string());

    let health_file = health_file_path("collect-aisstream");
    let mut source = AisStreamSource::new(
        api_key,
        bounding_boxes,
        args.filter_mmsi,
        args.filter_message_types,
        source_name,
    );

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
        },
    )
    .await
}
