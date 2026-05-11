use anyhow::{Context, Result};
use clap::Parser;
use collect_core::{
    health_file_path, line_reader_from_async_read, run_ingest, CommonCliArgs, IngestOptions,
    LineReader, LineSource, ReaderTransition, S3CliArgs,
};
use collect_tui::{run_tui, TuiModel};
use futures_util::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use std::io;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;

mod tui;

const DEFAULT_KAFKA_AUTO_OFFSET_RESET: &str = "latest";
const KAFKA_FORWARDER_CHANNEL_CAPACITY: usize = 1024;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Consume messages from a Kafka topic into Hive-partitioned Parquet with Zstd compression"
)]
struct Args {
    /// Kafka bootstrap servers, e.g. host1:9092,host2:9092
    #[arg(long)]
    kafka_brokers: Option<String>,

    /// Kafka topic to consume from
    #[arg(long)]
    topic: Option<String>,

    /// Kafka consumer group id
    #[arg(long)]
    group_id: Option<String>,

    /// Where to start when no offset is committed: earliest | latest
    #[arg(long, default_value = DEFAULT_KAFKA_AUTO_OFFSET_RESET)]
    kafka_auto_offset_reset: String,

    /// Logical source label (defaults to kafka topic)
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

struct KafkaInputSource {
    brokers: String,
    topic: String,
    group_id: String,
    auto_offset_reset: String,
    source: String,
    shutdown: Arc<AtomicBool>,
}

impl KafkaInputSource {
    fn new(
        brokers: String,
        topic: String,
        group_id: String,
        auto_offset_reset: String,
        source: String,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            brokers,
            topic,
            group_id,
            auto_offset_reset,
            source,
            shutdown,
        }
    }

    fn make_consumer(&self) -> Result<StreamConsumer> {
        Ok(ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", &self.auto_offset_reset)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("heartbeat.interval.ms", "2000")
            .create()?)
    }
}

#[collect_core::async_trait]
impl LineSource for KafkaInputSource {
    fn source_name(&self) -> &str {
        &self.source
    }

    fn timestamp_for_payload(&mut self, _payload: &str) -> Option<i64> {
        // Generic: use ingestion/arrival time.
        // collect-core will fall back to "now" when this returns None.
        None
    }

    fn normalize_payload(&mut self, payload: String, _timestamp_ms: i64) -> String {
        payload
    }

    async fn open(&mut self, max_line_length: usize) -> Result<LineReader> {
        let consumer = self.make_consumer()?;
        consumer.subscribe(&[&self.topic])?;

        let shutdown = self.shutdown.clone();
        let (tx, rx) = mpsc::channel::<io::Result<bytes::Bytes>>(
            KAFKA_FORWARDER_CHANNEL_CAPACITY,
        );

        // Forward Kafka messages as newline-delimited payloads into a byte stream.
        tokio::spawn(async move {
            let mut stream = consumer.stream();
            while !shutdown.load(Ordering::SeqCst) {
                match stream.next().await {
                    Some(Ok(msg)) => {
                        if let Some(payload) = msg.payload() {
                            let mut v = payload.to_vec();
                            v.push(b'\n');
                            if tx.send(Ok(bytes::Bytes::from(v))).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("Kafka stream error: {e:?}");
                    }
                    None => break,
                }
            }
        });

        let reader = StreamReader::new(ReceiverStream::new(rx));
        Ok(line_reader_from_async_read(reader, max_line_length))
    }

    async fn on_stream_end(
        &mut self,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        if shutdown.load(Ordering::SeqCst) {
            Ok(ReaderTransition::Stop)
        } else {
            Ok(ReaderTransition::Continue(
                self.open(max_line_length).await?,
            ))
        }
    }

    async fn on_stream_error(
        &mut self,
        _error: &collect_core::LinesCodecError,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        if shutdown.load(Ordering::SeqCst) {
            Ok(ReaderTransition::Stop)
        } else {
            Ok(ReaderTransition::Continue(
                self.open(max_line_length).await?,
            ))
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();

    if args.tui {
        let initial_config = tui::TuiConfig::load_from_env();
        match run_tui(initial_config)? {
            Some(config) => {
                let mut full_args = vec!["collect-kafka".to_string()];
                full_args.extend(config.to_cli_args());
                args = Args::parse_from(full_args);
            }
            None => {
                println!("Configuration cancelled.");
                return Ok(());
            }
        }
    }

    // Apply env fallbacks (consistent with other binaries)
    if args.kafka_brokers.is_none() {
        if let Ok(v) = std::env::var("KAFKA_BROKERS") {
            args.kafka_brokers = Some(v);
        }
    }
    if args.topic.is_none() {
        if let Ok(v) = std::env::var("KAFKA_TOPIC") {
            args.topic = Some(v);
        }
    }
    if args.group_id.is_none() {
        if let Ok(v) = std::env::var("KAFKA_GROUP_ID") {
            args.group_id = Some(v);
        }
    }
    if args.source.is_none() {
        if let Ok(v) = std::env::var("SOURCE") {
            args.source = Some(v);
        }
    }
    if let Ok(v) = std::env::var("KAFKA_AUTO_OFFSET_RESET") {
        args.kafka_auto_offset_reset = v;
    }

    args.common.apply_env();
    args.s3.apply_env();

    let brokers = args
        .kafka_brokers
        .context("missing Kafka brokers; set --kafka-brokers or KAFKA_BROKERS")?;
    let topic = args
        .topic
        .context("missing Kafka topic; set --topic or KAFKA_TOPIC")?;
    let group_id = args
        .group_id
        .context("missing Kafka group id; set --group-id or KAFKA_GROUP_ID")?;

    let source_name = args.source.unwrap_or_else(|| topic.clone());

    let shutdown = Arc::new(AtomicBool::new(false));
    let health_file = health_file_path("collect-kafka");

    let mut source = KafkaInputSource::new(
        brokers,
        topic,
        group_id,
        args.kafka_auto_offset_reset,
        source_name,
        shutdown.clone(),
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
            shutdown: Some(shutdown.clone()),
        },
    )
    .await
}
