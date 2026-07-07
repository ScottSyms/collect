use anyhow::{Context, Result};
use clap::Parser;
use collect_core::{
    health_file_path, line_reader_from_async_read, run_ingest, CommonCliArgs, IngestOptions,
    LineReader, LineSource, ReaderTransition, S3CliArgs,
};
use futures_util::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::io;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;

mod offsets;

use offsets::{OffsetTracker, PendingLines};

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
}

struct KafkaInputSource {
    brokers: String,
    topic: String,
    group_id: String,
    auto_offset_reset: String,
    source: String,
    shutdown: Arc<AtomicBool>,
    /// Consumer of the most recently opened stream; used to commit offsets.
    consumer: Option<Arc<StreamConsumer>>,
    tracker: OffsetTracker,
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
            consumer: None,
            tracker: OffsetTracker::new(PendingLines::default()),
        }
    }

    fn make_consumer(&self) -> Result<StreamConsumer> {
        Ok(ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group_id)
            // Offsets are committed manually once the batch containing them
            // has been written to disk (see offsets.rs); auto-commit would
            // advance offsets for data that only exists in memory.
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", &self.auto_offset_reset)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("heartbeat.interval.ms", "2000")
            .create()?)
    }

    fn commit_offsets(&self, offsets: &HashMap<i32, i64>) {
        let Some(consumer) = &self.consumer else {
            return;
        };

        let mut tpl = TopicPartitionList::new();
        for (&partition, &offset) in offsets {
            // Kafka commits the *next* offset to consume.
            if let Err(error) =
                tpl.add_partition_offset(&self.topic, partition, Offset::Offset(offset + 1))
            {
                eprintln!("⚠️  Failed to build Kafka offset commit list: {error}");
                return;
            }
        }

        // Sync keeps the commit ordered ahead of process exit; commits happen
        // at batch-flush frequency, so the blocking round trip is negligible.
        if let Err(error) = consumer.commit(&tpl, CommitMode::Sync) {
            eprintln!("⚠️  Failed to commit Kafka offsets: {error}");
        }
    }
}

#[collect_core::async_trait]
impl LineSource for KafkaInputSource {
    fn source_name(&self) -> &str {
        &self.source
    }

    fn timestamp_for_payload(&mut self, _payload: &str) -> Option<i64> {
        // Called exactly once per consumed line: drive offset accounting here.
        // Generic: use ingestion/arrival time.
        // collect-core will fall back to "now" when this returns None.
        self.tracker.on_line();
        None
    }

    fn normalize_payload(&mut self, payload: String, _timestamp_ms: i64) -> String {
        payload
    }

    fn on_batch_sealed(&mut self, seq: u64) {
        self.tracker.seal(seq);
    }

    fn on_batch_durable(&mut self, seq: u64) {
        if let Some(offsets) = self.tracker.durable(seq) {
            self.commit_offsets(&offsets);
        }
    }

    async fn open(&mut self, max_line_length: usize) -> Result<LineReader> {
        let consumer = self.make_consumer()?;
        consumer.subscribe(&[&self.topic])?;
        let consumer = Arc::new(consumer);
        self.consumer = Some(consumer.clone());

        let pending = PendingLines::default();
        self.tracker.reset_stream(pending.clone());

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
                            // Drop messages the line codec would reject, so
                            // every forwarded message maps to a known number
                            // of consumed lines and offset accounting stays
                            // exact.
                            let longest_segment = payload
                                .split(|&byte| byte == b'\n')
                                .map(<[u8]>::len)
                                .max()
                                .unwrap_or(0);
                            if longest_segment >= max_line_length {
                                eprintln!(
                                    "⚠️  Dropping oversized Kafka message ({} bytes) at partition {} offset {}",
                                    payload.len(),
                                    msg.partition(),
                                    msg.offset()
                                );
                                continue;
                            }

                            let line_count =
                                payload.iter().filter(|&&byte| byte == b'\n').count() as u32 + 1;
                            pending.push(msg.partition(), msg.offset(), line_count);

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
            write_workers: None,
            sweep_orphans: true,
        },
    )
    .await
}
