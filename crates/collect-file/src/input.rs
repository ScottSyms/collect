use anyhow::{bail, Context, Result};
use async_compression::tokio::bufread::{BzDecoder, GzipDecoder};
use bytes::Bytes;
use collect_core::{
    format_count, line_reader_from_async_read, IngestProgress, LineReader, LineSource,
    ReaderTransition,
};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use tokio::io::BufReader;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;
use walkdir::WalkDir;

#[derive(Debug)]
pub(crate) struct FileInputSource {
    source: String,
    ais: bool,
    ais_group_timestamps: HashMap<String, i64>,
    jobs: Vec<InputJob>,
    cursor: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum InputKind {
    Plain,
    Gzip,
    Bzip2,
    ZipEntry {
        entry_index: usize,
        entry_name: String,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct InputJob {
    path: PathBuf,
    display: String,
    sort_index: usize,
    kind: InputKind,
}

impl FileInputSource {
    pub(crate) fn new(input: PathBuf, source: String, ais: bool) -> Result<Self> {
        let input_display = input.display().to_string();
        let mut jobs = if input.is_file() {
            expand_input_path(&input)?
        } else if input.is_dir() {
            collect_input_jobs(&input)?
        } else {
            return Err(anyhow::anyhow!(
                "input path does not exist or is not readable: {}",
                input_display
            ));
        };

        jobs.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then(left.sort_index.cmp(&right.sort_index))
        });

        Ok(Self {
            source,
            ais,
            ais_group_timestamps: HashMap::new(),
            jobs,
            cursor: 0,
        })
    }

    async fn open_next(&mut self, max_line_length: usize) -> Result<LineReader> {
        if self.jobs.is_empty() {
            return Ok(line_reader_from_async_read(
                tokio::io::empty(),
                max_line_length,
            ));
        }

        let job = self
            .jobs
            .get(self.cursor)
            .cloned()
            .context("no more input files to open")?;

        if self.cursor > 0 {
            self.ais_group_timestamps.clear();
        }

        let current_index = self.cursor + 1;
        let total = self.jobs.len();
        self.cursor += 1;
        println!(
            "📄 Reading {}/{} {}",
            format_count(current_index),
            format_count(total),
            job.display
        );

        job.open(max_line_length).await
    }
}

#[collect_core::async_trait]
impl LineSource for FileInputSource {
    fn source_name(&self) -> &str {
        &self.source
    }

    fn ingest_progress(&self) -> Option<IngestProgress> {
        if self.cursor == 0 || self.cursor > self.jobs.len() {
            return None;
        }

        let job = self.jobs.get(self.cursor - 1)?;
        Some(IngestProgress {
            current_input: job.display.clone(),
            current_input_index: self.cursor,
            input_total: self.jobs.len(),
        })
    }

    fn timestamp_for_payload(&mut self, payload: &str) -> Option<i64> {
        if !self.ais {
            return None;
        }

        let tag_block = parse_ais_tag_block(payload);

        if let Some(timestamp_ms) = tag_block.timestamp_ms {
            if let Some(group_id) = tag_block.group_id {
                self.ais_group_timestamps.insert(group_id, timestamp_ms);
            }
            return Some(timestamp_ms);
        }

        tag_block
            .group_id
            .and_then(|group_id| self.ais_group_timestamps.get(&group_id).copied())
    }

    async fn open(&mut self, max_line_length: usize) -> Result<LineReader> {
        self.open_next(max_line_length).await
    }

    async fn on_stream_end(
        &mut self,
        _shutdown: &std::sync::atomic::AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        if self.cursor < self.jobs.len() {
            Ok(ReaderTransition::Continue(
                self.open_next(max_line_length).await?,
            ))
        } else {
            Ok(ReaderTransition::Stop)
        }
    }
}

impl InputJob {
    fn plain(path: PathBuf) -> Self {
        let display = path.display().to_string();
        Self {
            path,
            display,
            sort_index: 0,
            kind: InputKind::Plain,
        }
    }

    fn gzip(path: PathBuf) -> Self {
        let display = path.display().to_string();
        Self {
            path,
            display,
            sort_index: 0,
            kind: InputKind::Gzip,
        }
    }

    fn bzip2(path: PathBuf) -> Self {
        let display = path.display().to_string();
        Self {
            path,
            display,
            sort_index: 0,
            kind: InputKind::Bzip2,
        }
    }

    fn zip_entry(path: PathBuf, entry_index: usize, entry_name: String) -> Self {
        let display = format!("{}::{}", path.display(), entry_name);
        Self {
            path,
            display,
            sort_index: entry_index + 1,
            kind: InputKind::ZipEntry {
                entry_index,
                entry_name,
            },
        }
    }

    async fn open(self, max_line_length: usize) -> Result<LineReader> {
        let InputJob { path, kind, .. } = self;

        match kind {
            InputKind::Plain => {
                let file = tokio::fs::File::open(&path)
                    .await
                    .with_context(|| format!("open input file {}", path.display()))?;
                Ok(line_reader_from_async_read(file, max_line_length))
            }
            InputKind::Gzip => {
                let file = tokio::fs::File::open(&path)
                    .await
                    .with_context(|| format!("open gzip file {}", path.display()))?;
                let decoder = GzipDecoder::new(BufReader::new(file));
                Ok(line_reader_from_async_read(decoder, max_line_length))
            }
            InputKind::Bzip2 => {
                let file = tokio::fs::File::open(&path)
                    .await
                    .with_context(|| format!("open bzip2 file {}", path.display()))?;
                let decoder = BzDecoder::new(BufReader::new(file));
                Ok(line_reader_from_async_read(decoder, max_line_length))
            }
            InputKind::ZipEntry {
                entry_index,
                entry_name,
            } => open_zip_entry_reader(path, entry_index, entry_name, max_line_length),
        }
    }
}

impl Ord for InputJob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.path
            .cmp(&other.path)
            .then(self.sort_index.cmp(&other.sort_index))
    }
}

impl PartialOrd for InputJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn collect_input_jobs(root: &Path) -> Result<Vec<InputJob>> {
    let mut jobs = Vec::new();

    for entry in WalkDir::new(root) {
        let entry = entry.context("walking input directory")?;
        if entry.file_type().is_file() {
            if is_hidden_file(entry.path()) {
                continue;
            }
            jobs.extend(expand_input_path(entry.path())?);
        }
    }

    Ok(jobs)
}

fn expand_input_path(path: &Path) -> Result<Vec<InputJob>> {
    if is_hidden_file(path) {
        return Ok(Vec::new());
    }

    match detect_input_format(path)? {
        InputFormat::Plain => {
            if is_tar_like_path(path) {
                bail!("tar archives are not supported: {}", path.display());
            }

            Ok(vec![InputJob::plain(path.to_path_buf())])
        }
        InputFormat::Gzip => Ok(vec![InputJob::gzip(path.to_path_buf())]),
        InputFormat::Bzip2 => Ok(vec![InputJob::bzip2(path.to_path_buf())]),
        InputFormat::Zip => collect_zip_jobs(path),
    }
}

fn collect_zip_jobs(path: &Path) -> Result<Vec<InputJob>> {
    let file =
        StdFile::open(path).with_context(|| format!("open zip archive {}", path.display()))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("read zip archive {}", path.display()))?;
    let mut jobs = Vec::new();

    for entry_index in 0..archive.len() {
        let entry = archive
            .by_index(entry_index)
            .with_context(|| format!("read zip entry {} in {}", entry_index, path.display()))?;
        let entry_name = entry.name().to_string();
        if entry_name.ends_with('/') {
            continue;
        }
        if is_hidden_entry_name(&entry_name) {
            continue;
        }
        jobs.push(InputJob::zip_entry(
            path.to_path_buf(),
            entry_index,
            entry_name,
        ));
    }

    Ok(jobs)
}

enum InputFormat {
    Plain,
    Gzip,
    Bzip2,
    Zip,
}

fn detect_input_format(path: &Path) -> Result<InputFormat> {
    let prefix = file_prefix(path, 6)?;
    if is_zip_magic(&prefix) {
        return Ok(InputFormat::Zip);
    }
    if is_gzip_magic(&prefix) {
        return Ok(InputFormat::Gzip);
    }
    if is_bzip2_magic(&prefix) {
        return Ok(InputFormat::Bzip2);
    }

    match file_extension(path).as_deref() {
        Some("zip") => Ok(InputFormat::Zip),
        Some("gz") => Ok(InputFormat::Gzip),
        Some("bz2") => Ok(InputFormat::Bzip2),
        _ => Ok(InputFormat::Plain),
    }
}

fn file_prefix(path: &Path, len: usize) -> Result<Vec<u8>> {
    let mut file =
        StdFile::open(path).with_context(|| format!("open input file {}", path.display()))?;
    let mut buf = vec![0u8; len];
    let read = file
        .read(&mut buf)
        .with_context(|| format!("read input file {}", path.display()))?;
    buf.truncate(read);
    Ok(buf)
}

fn file_extension(path: &Path) -> Option<String> {
    path.extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
}

fn is_tar_like_path(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };

    let name = name.to_ascii_lowercase();
    matches!(
        name.as_str(),
        n if n.ends_with(".tar")
            || n.ends_with(".tar.gz")
            || n.ends_with(".tgz")
            || n.ends_with(".tar.bz2")
            || n.ends_with(".tbz")
            || n.ends_with(".tbz2")
            || n.ends_with(".tar.xz")
            || n.ends_with(".txz")
            || n.ends_with(".tar.lzma")
            || n.ends_with(".tar.zst")
            || n.ends_with(".tzst")
    )
}

fn is_zip_magic(prefix: &[u8]) -> bool {
    prefix.starts_with(b"PK\x03\x04")
        || prefix.starts_with(b"PK\x05\x06")
        || prefix.starts_with(b"PK\x07\x08")
}

fn is_gzip_magic(prefix: &[u8]) -> bool {
    prefix.starts_with(&[0x1f, 0x8b])
}

fn is_bzip2_magic(prefix: &[u8]) -> bool {
    prefix.starts_with(b"BZh")
}

fn is_hidden_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(is_hidden_file_name)
        .unwrap_or(false)
}

fn is_hidden_file_name(name: &str) -> bool {
    matches!(name.as_bytes().first(), Some(b'.')) && name != "." && name != ".."
}

fn is_hidden_entry_name(entry_name: &str) -> bool {
    Path::new(entry_name)
        .file_name()
        .and_then(|value| value.to_str())
        .map(is_hidden_file_name)
        .unwrap_or(false)
}

#[derive(Debug, Default)]
struct AisTagBlock {
    timestamp_ms: Option<i64>,
    group_id: Option<String>,
}

fn parse_ais_tag_block(payload: &str) -> AisTagBlock {
    let sentence_start = payload
        .char_indices()
        .find(|(_, ch)| *ch == '!' || *ch == '$')
        .map(|(idx, _)| idx)
        .unwrap_or(payload.len());
    let prefix = &payload[..sentence_start];
    let mut tag_block = AisTagBlock::default();

    for raw_field in prefix.split(',') {
        let field = raw_field.trim_matches('\\');
        let field = field.split('*').next().unwrap_or(field).trim();

        if let Some(value) = field.strip_prefix("c:") {
            if tag_block.timestamp_ms.is_none() {
                tag_block.timestamp_ms = parse_ais_timestamp_ms(value);
            }
        } else if let Some(value) = field.strip_prefix("g:") {
            if tag_block.group_id.is_none() {
                tag_block.group_id = parse_ais_group_id(value);
            }
        }
    }

    tag_block
}

fn parse_ais_timestamp_ms(value: &str) -> Option<i64> {
    let digits = value
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();

    if digits.is_empty() {
        return None;
    }

    let seconds = digits.parse::<u64>().ok()?;
    Some(seconds.saturating_mul(1_000) as i64)
}

fn parse_ais_group_id(value: &str) -> Option<String> {
    let candidate = value
        .split('-')
        .filter(|part| !part.is_empty())
        .last()
        .unwrap_or(value)
        .trim();

    if candidate.is_empty() {
        None
    } else {
        Some(candidate.to_string())
    }
}

fn open_zip_entry_reader(
    path: PathBuf,
    entry_index: usize,
    entry_name: String,
    max_line_length: usize,
) -> Result<LineReader> {
    let (tx, rx) = mpsc::channel::<io::Result<Bytes>>(8);

    let _ = tokio::task::spawn_blocking(move || {
        let result: io::Result<()> = (|| {
            let file = StdFile::open(&path)?;
            let mut archive = zip::ZipArchive::new(file)
                .map_err(|error| zip_error_to_io(&path, &entry_name, error))?;
            let mut entry = archive
                .by_index(entry_index)
                .map_err(|error| zip_error_to_io(&path, &entry_name, error))?;
            if entry.name().ends_with('/') {
                return Ok(());
            }

            let mut buffer = vec![0u8; 16 * 1024];
            loop {
                let read = entry.read(&mut buffer)?;
                if read == 0 {
                    break;
                }

                if tx
                    .blocking_send(Ok(Bytes::copy_from_slice(&buffer[..read])))
                    .is_err()
                {
                    return Ok(());
                }
            }

            Ok(())
        })();

        if let Err(error) = result {
            let _ = tx.blocking_send(Err(error));
        }
    });

    let stream = ReceiverStream::new(rx);
    let reader = StreamReader::new(stream);
    Ok(line_reader_from_async_read(reader, max_line_length))
}

fn zip_error_to_io(path: &Path, entry_name: &str, error: zip::result::ZipError) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "failed reading zip entry {} in {}: {}",
            entry_name,
            path.display(),
            error
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use chrono::{Datelike, TimeZone, Timelike, Utc};
    use collect_core::{run_ingest, CommonOptions, IngestOptions};
    use futures_util::StreamExt;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::HashMap;
    use std::fs::File as StdFile;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use walkdir::WalkDir;

    #[tokio::test]
    async fn reads_plain_gzip_bzip2_and_zip_entries_in_order() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path();

        write_text_file(&root.join("a_plain.txt"), "plain-1\nplain-2\n")?;
        write_gzip_file(&root.join("b_gzip.gz"), "gzip-1\n")
            .await
            .context("write gzip fixture")?;
        write_bzip2_file(&root.join("c_bzip.bz2"), "bzip-1\n")
            .await
            .context("write bzip2 fixture")?;
        write_zip_file(&root.join("d_archive.zip"))?;

        let mut source = FileInputSource::new(root.to_path_buf(), "source".to_string(), false)?;
        let lines = collect_all_lines(&mut source, 1024).await?;

        assert_eq!(
            lines,
            vec!["plain-1", "plain-2", "gzip-1", "bzip-1", "zip-1", "zip-2"]
        );

        Ok(())
    }

    #[tokio::test]
    async fn silently_ignores_hidden_input_file() -> Result<()> {
        let dir = tempdir()?;
        let hidden = dir.path().join(".hidden.txt");
        write_text_file(&hidden, "hidden-1\nhidden-2\n")?;

        let mut source = FileInputSource::new(hidden, "source".to_string(), false)?;
        let lines = collect_all_lines(&mut source, 1024).await?;

        assert!(lines.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn silently_ignores_hidden_files_in_directories() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path();

        write_text_file(&root.join(".hidden.txt"), "hidden-1\n")?;
        write_text_file(&root.join("visible.txt"), "visible-1\n")?;

        let mut source = FileInputSource::new(root.to_path_buf(), "source".to_string(), false)?;
        let lines = collect_all_lines(&mut source, 1024).await?;

        assert_eq!(lines, vec!["visible-1"]);
        Ok(())
    }

    #[tokio::test]
    async fn flushes_when_batch_bytes_are_small() -> Result<()> {
        let dir = tempdir()?;
        let input_path = dir.path().join("big.txt");
        let out_dir = dir.path().join("out");
        let health_file = dir.path().join("health");

        let mut contents = String::new();
        for i in 0..200 {
            contents.push_str(&format!("line-{i:04} {}\n", "x".repeat(80)));
        }
        write_text_file(&input_path, &contents)?;

        let mut source = FileInputSource::new(input_path, "source".to_string(), false)?;
        run_ingest(
            &mut source,
            IngestOptions {
                common: CommonOptions {
                    out_dir: out_dir.clone(),
                    partition: collect_core::PartitionGranularity::Minute,
                    max_rows: None,
                    max_batch_bytes: 1024,
                    upload_drain_timeout_seconds: 1,
                    max_line_length: 1024,
                    health_check: false,
                },
                s3: None,
                health_file,
            },
        )
        .await?;

        let parquet_files = WalkDir::new(&out_dir)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_type().is_file()
                    && entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet")
            })
            .count();

        assert!(
            parquet_files > 1,
            "expected multiple Parquet files, found {parquet_files}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn accepts_gzip_with_tar_like_name() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("archive.tar.gz");
        write_gzip_file(&path, "gzip-1\n").await?;

        let mut source = FileInputSource::new(path, "source".to_string(), false)?;
        let lines = collect_all_lines(&mut source, 1024).await?;

        assert_eq!(lines, vec!["gzip-1"]);
        Ok(())
    }

    #[tokio::test]
    async fn accepts_zip_with_tar_like_name() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("archive.tar.gz");
        write_zip_file(&path)?;

        let mut source = FileInputSource::new(path, "source".to_string(), false)?;
        let lines = collect_all_lines(&mut source, 1024).await?;

        assert_eq!(lines, vec!["zip-1", "zip-2"]);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_plain_tar_like_paths() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("archive.tar");
        write_text_file(&path, "not used\n").expect("write file");

        let error = FileInputSource::new(path, "source".to_string(), false)
            .expect_err("tar files should be rejected");
        assert!(error.to_string().contains("tar archives are not supported"));
    }

    #[tokio::test]
    async fn parses_ais_timestamp_when_enabled() {
        let mut source = FileInputSource {
            source: "source".to_string(),
            ais: true,
            ais_group_timestamps: HashMap::new(),
            jobs: vec![],
            cursor: 0,
        };

        assert_eq!(
            source.timestamp_for_payload("c:1643588424!AIVDM,1,1,,A,HELLO,0"),
            Some(1_643_588_424_000)
        );
        assert_eq!(source.timestamp_for_payload("!AIVDM,1,1,,A,HELLO,0"), None);
        assert_eq!(
            source.timestamp_for_payload("c:bad!AIVDM,1,1,,A,HELLO,0"),
            None
        );
    }

    #[test]
    fn reuses_ais_group_timestamp_for_follow_on_fragments() {
        let mut source = FileInputSource {
            source: "source".to_string(),
            ais: true,
            ais_group_timestamps: HashMap::new(),
            jobs: vec![],
            cursor: 0,
        };

        let first = r"\g:1-2-6287,c:1609459200*56\!AIVDM,2,1,0,A,P0,4*72";
        let second = r"\g:2-2-6287*56\!AIVDM,2,2,0,A,P0,4*72";

        assert_eq!(source.timestamp_for_payload(first), Some(1_609_459_200_000));
        assert_eq!(
            source.timestamp_for_payload(second),
            Some(1_609_459_200_000)
        );
    }

    #[test]
    fn reuses_ais_group_timestamp_for_three_fragment_messages() {
        let mut source = FileInputSource {
            source: "source".to_string(),
            ais: true,
            ais_group_timestamps: HashMap::new(),
            jobs: vec![],
            cursor: 0,
        };

        let first = r"\g:1-3-2655,c:1609459200*56\!AIVDM,3,1,9,A,first,0*00";
        let second = r"\g:2-3-2655*58\!AIVDM,3,2,9,A,second,0*00";
        let third = r"\g:3-3-2655*58\!AIVDM,3,3,9,A,third,0*00";

        assert_eq!(source.timestamp_for_payload(first), Some(1_609_459_200_000));
        assert_eq!(
            source.timestamp_for_payload(second),
            Some(1_609_459_200_000)
        );
        assert_eq!(source.timestamp_for_payload(third), Some(1_609_459_200_000));
    }

    #[test]
    fn reports_current_input_progress() {
        let source = FileInputSource {
            source: "source".to_string(),
            ais: false,
            ais_group_timestamps: HashMap::new(),
            jobs: vec![InputJob::plain(PathBuf::from("/tmp/input.txt"))],
            cursor: 1,
        };

        let progress = source
            .ingest_progress()
            .expect("expected progress snapshot");
        assert_eq!(progress.current_input, "/tmp/input.txt");
        assert_eq!(progress.current_input_index, 1);
        assert_eq!(progress.input_total, 1);
    }

    #[tokio::test]
    async fn uses_ais_timestamp_for_partitioning_and_keeps_payload() -> Result<()> {
        let dir = tempdir()?;
        let input_path = dir.path().join("ais.txt");
        let out_dir = dir.path().join("out");
        let health_file = dir.path().join("health");
        let ais_line = "c:1609459200!AIVDM,1,1,,A,HELLO,0\n";

        write_text_file(&input_path, ais_line)?;

        let mut source = FileInputSource::new(input_path, "ais-source".to_string(), true)?;
        assert_eq!(
            source.timestamp_for_payload(ais_line.trim_end()),
            Some(1_609_459_200_000)
        );

        run_ingest(
            &mut source,
            IngestOptions {
                common: CommonOptions {
                    out_dir: out_dir.clone(),
                    partition: collect_core::PartitionGranularity::Minute,
                    max_rows: Some(1),
                    max_batch_bytes: 1024 * 1024,
                    upload_drain_timeout_seconds: 1,
                    max_line_length: 1024,
                    health_check: false,
                },
                s3: None,
                health_file,
            },
        )
        .await?;

        let expected = Utc
            .timestamp_opt(1_609_459_200, 0)
            .single()
            .expect("valid AIS timestamp");
        let expected_partition = format!(
            "source=ais-source/year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}",
            expected.year(),
            expected.month(),
            expected.day(),
            expected.hour(),
            expected.minute()
        );
        let expected_dir = out_dir.join(expected_partition);

        let parquet_file = WalkDir::new(&expected_dir)
            .into_iter()
            .filter_map(Result::ok)
            .find(|entry| {
                entry.file_type().is_file()
                    && entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet")
            })
            .map(|entry| entry.into_path())
            .context("missing parquet output")?;

        let file = StdFile::open(&parquet_file)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let batch = reader
            .next()
            .transpose()?
            .context("missing parquet batch")?;
        let payload = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("missing payload column")?;

        assert_eq!(payload.value(0), ais_line.trim_end());
        Ok(())
    }

    async fn collect_all_lines(
        source: &mut FileInputSource,
        max_line_length: usize,
    ) -> Result<Vec<String>> {
        let shutdown = AtomicBool::new(false);
        let mut reader = source.open(max_line_length).await?;
        let mut lines = Vec::new();

        loop {
            match reader.next().await {
                Some(Ok(line)) => lines.push(line),
                Some(Err(error)) => match source
                    .on_stream_error(&error, &shutdown, max_line_length)
                    .await?
                {
                    ReaderTransition::Continue(next_reader) => {
                        reader = next_reader;
                    }
                    ReaderTransition::Stop => return Err(error.into()),
                },
                None => match source.on_stream_end(&shutdown, max_line_length).await? {
                    ReaderTransition::Continue(next_reader) => {
                        reader = next_reader;
                    }
                    ReaderTransition::Stop => break,
                },
            }
        }

        Ok(lines)
    }

    fn write_text_file(path: &Path, contents: &str) -> Result<()> {
        let mut file = StdFile::create(path)
            .with_context(|| format!("create plain fixture {}", path.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("write plain fixture {}", path.display()))?;
        Ok(())
    }

    async fn write_gzip_file(path: &Path, contents: &str) -> Result<()> {
        let file = tokio::fs::File::create(path)
            .await
            .with_context(|| format!("create gzip fixture {}", path.display()))?;
        let mut encoder = async_compression::tokio::write::GzipEncoder::new(file);
        encoder
            .write_all(contents.as_bytes())
            .await
            .with_context(|| format!("write gzip fixture {}", path.display()))?;
        encoder
            .shutdown()
            .await
            .with_context(|| format!("finish gzip fixture {}", path.display()))?;
        Ok(())
    }

    async fn write_bzip2_file(path: &Path, contents: &str) -> Result<()> {
        let file = tokio::fs::File::create(path)
            .await
            .with_context(|| format!("create bzip2 fixture {}", path.display()))?;
        let mut encoder = async_compression::tokio::write::BzEncoder::new(file);
        encoder
            .write_all(contents.as_bytes())
            .await
            .with_context(|| format!("write bzip2 fixture {}", path.display()))?;
        encoder
            .shutdown()
            .await
            .with_context(|| format!("finish bzip2 fixture {}", path.display()))?;
        Ok(())
    }

    fn write_zip_file(path: &Path) -> Result<()> {
        let file = StdFile::create(path)
            .with_context(|| format!("create zip fixture {}", path.display()))?;
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);

        zip.start_file("first.txt", options)
            .with_context(|| format!("start zip entry in {}", path.display()))?;
        zip.write_all(b"zip-1\n")
            .with_context(|| format!("write zip entry in {}", path.display()))?;

        zip.start_file("nested/second.txt", options)
            .with_context(|| format!("start zip entry in {}", path.display()))?;
        zip.write_all(b"zip-2\n")
            .with_context(|| format!("write zip entry in {}", path.display()))?;

        zip.finish()
            .with_context(|| format!("finish zip fixture {}", path.display()))?;
        Ok(())
    }
}
