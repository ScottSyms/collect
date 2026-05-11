use anyhow::Result;
use collect_core::{
    async_trait,
    health_file_path,
    line_reader_from_async_read,
    run_ingest,
    CommonOptions,
    IngestOptions,
    LineReader,
    LineSource,
    PartitionGranularity,
    ReaderTransition,
};
use bytes::Bytes;
use tokio_util::io::StreamReader;
use tokio_stream::iter;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{
    atomic::AtomicBool,
    Arc,
};
use std::io;

struct MockLineSource {
    source_name: String,
    payloads: Vec<String>,
    cursor: usize,
}

impl MockLineSource {
    fn new(source_name: impl Into<String>, payloads: Vec<String>) -> Self {
        Self {
            source_name: source_name.into(),
            payloads,
            cursor: 0,
        }
    }
}

#[async_trait]
impl LineSource for MockLineSource {
    fn source_name(&self) -> &str {
        &self.source_name
    }

    fn timestamp_for_payload(&mut self, _payload: &str) -> Option<i64> {
        // Match the generic Kafka behavior: use ingestion/arrival time.
        None
    }

    fn normalize_payload(&mut self, payload: String, _timestamp_ms: i64) -> String {
        payload
    }

    async fn open(&mut self, max_line_length: usize) -> Result<LineReader> {
        // Encode all remaining payloads as newline-delimited lines.
        let rest = self.payloads[self.cursor..].join("\n");
        self.cursor = self.payloads.len();
        let s = if rest.is_empty() {
            "".to_string()
        } else {
            format!("{}\n", rest)
        };

        // StreamReader provides an AsyncRead over a byte stream.
        let stream = iter(vec![Ok::<Bytes, io::Error>(Bytes::from(s.into_bytes()))]);
        let reader = StreamReader::new(stream);
        Ok(line_reader_from_async_read(reader, max_line_length))
    }

    async fn on_stream_end(
        &mut self,
        _shutdown: &AtomicBool,
        _max_line_length: usize,
    ) -> Result<ReaderTransition> {
        Ok(ReaderTransition::Stop)
    }

    async fn on_stream_error(
        &mut self,
        _error: &collect_core::LinesCodecError,
        _shutdown: &AtomicBool,
        _max_line_length: usize,
    ) -> Result<ReaderTransition> {
        Ok(ReaderTransition::Stop)
    }
}

fn list_parquet_files(root: &PathBuf) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(root).into_iter().flatten() {
        let p = entry.path();
        if p.extension().and_then(|x| x.to_str()) == Some("parquet") {
            out.push(p.to_path_buf());
        }
    }
    out.sort();
    out
}

fn read_total_rows(path: &PathBuf) -> Result<i64> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    // Sum row counts across row groups.
    let mut total = 0i64;
    for rg in metadata.row_groups() {
        total += rg.num_rows() as i64;
    }
    Ok(total)
}

#[tokio::test]
async fn kafka_ingest_harness_writes_expected_parquet() -> Result<()> {
    // Use a partition granularity that makes expected paths stable across the test run.
    let partition = PartitionGranularity::Year;

    let tmp = tempfile::tempdir()?;
    let out_dir = tmp.path().to_path_buf();

    let source_name = "kafka-test";
    let payloads = vec!["m1".to_string(), "m2".to_string(), "m3".to_string(), "m4".to_string()];

    let mut mock = MockLineSource::new(source_name, payloads.clone());

    let shutdown = Arc::new(AtomicBool::new(false));

    let common = CommonOptions {
        out_dir: out_dir.clone(),
        partition,
        max_rows: Some(2), // force multiple parquet files
        max_batch_bytes: 1024 * 1024,
        compression_level: 3,
        upload_drain_timeout_seconds: 1,
        max_line_length: 1024,
        health_check: false,
    };

    let opts = IngestOptions {
        common,
        s3: None,
        s3_storage: None,
        health_file: health_file_path("collect-kafka-test"),
        manage_health: false,
        report_progress: false,
        log_writes: false,
        shutdown: Some(shutdown),
    };

    run_ingest(&mut mock, opts).await?;

    let files = list_parquet_files(&out_dir);
    assert!(
        !files.is_empty(),
        "expected at least one parquet file under {}",
        out_dir.display()
    );

    // Ensure output is written under the expected Hive partition layout.
    let expected_part_prefix = format!("source={}/year=", source_name);
    for f in &files {
        let s = f.to_string_lossy();
        assert!(
            s.contains(&expected_part_prefix),
            "expected parquet file {} to contain partition prefix {}",
            s,
            expected_part_prefix
        );
    }

    // Validate row count equals input rows.
    let total_rows: i64 = files
        .iter()
        .map(read_total_rows)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum();

    assert_eq!(total_rows, payloads.len() as i64);

    Ok(())
}
