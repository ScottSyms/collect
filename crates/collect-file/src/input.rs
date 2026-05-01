use anyhow::{bail, Context, Result};
use async_compression::tokio::bufread::{BzDecoder, GzipDecoder};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use collect_core::{
    line_reader_from_async_read, IngestProgress, LineReader, LineSource, ReaderTransition,
};
use futures_util::stream::{self, StreamExt};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
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
    ais_pending_timestamp_ms: Option<i64>,
    ais_payload_timestamp_ms: Option<i64>,
    current_job_timestamp_ms: Option<i64>,
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
    estimated_size: u64,
    sort_index: usize,
    kind: InputKind,
}

impl FileInputSource {
    #[cfg(test)]
    pub(crate) fn new(input: PathBuf, source: String, ais: bool) -> Result<Self> {
        let input_display = input.display().to_string();
        let mut jobs = if input.is_file() {
            let metadata = std::fs::metadata(&input)
                .with_context(|| format!("reading file metadata {}", input.display()))?;
            expand_input_path(&input, metadata.len())?
        } else if input.is_dir() {
            collect_input_jobs(&input)?
        } else {
            return Err(anyhow::anyhow!(
                "input path does not exist or is not readable: {}",
                input_display
            ));
        };

        jobs.sort_unstable_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then(left.sort_index.cmp(&right.sort_index))
        });
        Ok(Self::from_jobs(source, ais, jobs))
    }

    pub(crate) async fn new_parallel(input: PathBuf, source: String, ais: bool) -> Result<Self> {
        let input_display = input.display().to_string();
        let mut jobs = if input.is_file() {
            let metadata = std::fs::metadata(&input)
                .with_context(|| format!("reading file metadata {}", input.display()))?;
            expand_input_path(&input, metadata.len())?
        } else if input.is_dir() {
            collect_input_jobs_parallel(&input, default_scan_concurrency()).await?
        } else {
            return Err(anyhow::anyhow!(
                "input path does not exist or is not readable: {}",
                input_display
            ));
        };

        jobs.sort_unstable_by(|left, right| {
            right
                .estimated_size
                .cmp(&left.estimated_size)
                .then(left.path.cmp(&right.path))
                .then(left.sort_index.cmp(&right.sort_index))
        });

        Ok(Self::from_jobs(source, ais, jobs))
    }

    fn from_jobs(source: String, ais: bool, jobs: Vec<InputJob>) -> Self {
        Self {
            source,
            ais,
            ais_group_timestamps: HashMap::new(),
            ais_pending_timestamp_ms: None,
            ais_payload_timestamp_ms: None,
            current_job_timestamp_ms: None,
            jobs,
            cursor: 0,
        }
    }

    pub(crate) fn job_count(&self) -> usize {
        self.jobs.len()
    }

    pub(crate) fn estimated_size_bytes(&self) -> u64 {
        self.jobs.iter().map(|job| job.estimated_size).sum()
    }

    pub(crate) fn completion_key(&self) -> Result<String> {
        let job = self
            .jobs
            .first()
            .context("missing input job for completion key")?;
        let metadata = std::fs::metadata(&job.path)
            .with_context(|| format!("reading file metadata {}", job.path.display()))?;
        let modified_ms = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis())
            .unwrap_or(0);

        Ok(format!(
            "{:?}|{}|{}|{}",
            job.display, job.estimated_size, modified_ms, job.sort_index
        ))
    }

    pub(crate) fn into_job_sources(self) -> Vec<Self> {
        let FileInputSource {
            source, ais, jobs, ..
        } = self;

        jobs.into_iter()
            .map(|job| FileInputSource::from_jobs(source.clone(), ais, vec![job]))
            .collect()
    }

    async fn open_next(&mut self, max_line_length: usize) -> Result<LineReader> {
        while self.cursor < self.jobs.len() {
            let job = self
                .jobs
                .get(self.cursor)
                .cloned()
                .context("no more input files to open")?;

            if self.cursor > 0 {
                self.ais_group_timestamps.clear();
                self.ais_pending_timestamp_ms = None;
                self.ais_payload_timestamp_ms = None;
            }

            let display = job.display.clone();
            self.current_job_timestamp_ms =
                Some(file_timestamp_ms(&job.path).unwrap_or_else(|_| now_unix_millis()));
            self.cursor += 1;

            match job.open(max_line_length).await {
                Ok(reader) => return Ok(reader),
                Err(error) => {
                    eprintln!("⚠️  Skipping unreadable file {}: {}", display, error);
                }
            }
        }

        self.current_job_timestamp_ms = None;
        Ok(line_reader_from_async_read(
            tokio::io::empty(),
            max_line_length,
        ))
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
        self.ais_payload_timestamp_ms = None;

        if !self.ais {
            return self.current_job_timestamp_ms;
        }

        let sentence = nmea_sentence(payload);

        if let Some(timestamp_ms) = parse_pghp_timestamp_ms(sentence) {
            self.ais_pending_timestamp_ms = Some(timestamp_ms);
            self.ais_payload_timestamp_ms = Some(timestamp_ms);
            return Some(timestamp_ms);
        }

        let tag_block = parse_ais_tag_block(payload);
        let sentence = parse_ais_sentence_metadata(sentence);

        if !sentence.is_ais {
            return None;
        }

        if let Some(timestamp_ms) = tag_block.timestamp_ms {
            self.ais_pending_timestamp_ms = None;
            self.ais_payload_timestamp_ms = Some(timestamp_ms);
            if sentence.is_fragmented() {
                cache_timestamp_for_groups(
                    &mut self.ais_group_timestamps,
                    timestamp_ms,
                    tag_block.group_id.as_deref(),
                    sentence.group_id.as_deref(),
                );
            }
            return Some(timestamp_ms);
        }

        if let Some(timestamp_ms) = lookup_timestamp_for_groups(
            &self.ais_group_timestamps,
            tag_block.group_id.as_deref(),
            sentence.group_id.as_deref(),
        ) {
            self.ais_pending_timestamp_ms = None;
            self.ais_payload_timestamp_ms = Some(timestamp_ms);
            if sentence.is_final_fragment() {
                remove_timestamp_for_groups(
                    &mut self.ais_group_timestamps,
                    tag_block.group_id.as_deref(),
                    sentence.group_id.as_deref(),
                );
            }
            return Some(timestamp_ms);
        }

        let Some(timestamp_ms) = self.ais_pending_timestamp_ms else {
            return None;
        };

        if tag_block
            .group_id
            .as_deref()
            .or(sentence.group_id.as_deref())
            .is_some()
        {
            if sentence.is_fragmented() && sentence.fragment_number == Some(1) {
                cache_timestamp_for_groups(
                    &mut self.ais_group_timestamps,
                    timestamp_ms,
                    tag_block.group_id.as_deref(),
                    sentence.group_id.as_deref(),
                );
                self.ais_pending_timestamp_ms = None;
            } else if sentence.is_final_fragment() || !sentence.is_fragmented() {
                self.ais_pending_timestamp_ms = None;
            }

            self.ais_payload_timestamp_ms = Some(timestamp_ms);
            return Some(timestamp_ms);
        }

        if sentence.is_fragmented() {
            if sentence.is_final_fragment() {
                self.ais_pending_timestamp_ms = None;
            }
            self.ais_payload_timestamp_ms = Some(timestamp_ms);
            return Some(timestamp_ms);
        }

        self.ais_pending_timestamp_ms = None;
        self.ais_payload_timestamp_ms = Some(timestamp_ms);
        Some(timestamp_ms)
    }

    fn normalize_payload(&mut self, payload: String, timestamp_ms: i64) -> String {
        if !self.ais || self.ais_payload_timestamp_ms != Some(timestamp_ms) {
            return payload;
        }

        normalize_ais_payload(&payload, timestamp_ms).unwrap_or(payload)
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

    async fn on_stream_error(
        &mut self,
        error: &collect_core::LinesCodecError,
        _shutdown: &std::sync::atomic::AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        if let Some(job) = self.jobs.get(self.cursor.saturating_sub(1)) {
            eprintln!("⚠️  Skipping failed file {}: {}", job.display, error);
        } else {
            eprintln!("⚠️  Skipping failed input: {}", error);
        }

        Ok(ReaderTransition::Continue(
            self.open_next(max_line_length).await?,
        ))
    }
}

impl InputJob {
    fn plain(path: PathBuf, estimated_size: u64) -> Self {
        let display = path.display().to_string();
        Self {
            path,
            display,
            estimated_size,
            sort_index: 0,
            kind: InputKind::Plain,
        }
    }

    fn gzip(path: PathBuf, estimated_size: u64) -> Self {
        let display = path.display().to_string();
        Self {
            path,
            display,
            estimated_size,
            sort_index: 0,
            kind: InputKind::Gzip,
        }
    }

    fn bzip2(path: PathBuf, estimated_size: u64) -> Self {
        let display = path.display().to_string();
        Self {
            path,
            display,
            estimated_size,
            sort_index: 0,
            kind: InputKind::Bzip2,
        }
    }

    fn zip_entry(
        path: PathBuf,
        estimated_size: u64,
        entry_index: usize,
        entry_name: String,
    ) -> Self {
        let display = format!("{}::{}", path.display(), entry_name);
        Self {
            path,
            display,
            estimated_size,
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
            InputKind::Plain => open_plain_file_reader(path, max_line_length),
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

#[cfg(test)]
fn collect_input_jobs(root: &Path) -> Result<Vec<InputJob>> {
    let mut jobs = Vec::new();

    for entry in WalkDir::new(root) {
        let entry = entry.context("walking input directory")?;
        if entry.file_type().is_file() {
            if is_hidden_file(entry.path()) {
                continue;
            }
            let metadata = entry.metadata().context("reading file metadata")?;
            match expand_input_path(entry.path(), metadata.len()) {
                Ok(expanded) => jobs.extend(expanded),
                Err(error) if should_skip_input_error(&error) => {
                    eprintln!(
                        "⚠️  Skipping unreadable file {}: {}",
                        entry.path().display(),
                        error
                    );
                }
                Err(error) => return Err(error),
            }
        }
    }

    Ok(jobs)
}

async fn collect_input_jobs_parallel(root: &Path, concurrency: usize) -> Result<Vec<InputJob>> {
    let mut paths = Vec::new();

    for entry in WalkDir::new(root) {
        let entry = entry.context("walking input directory")?;
        if entry.file_type().is_file() {
            if is_hidden_file(entry.path()) {
                continue;
            }
            paths.push(entry.path().to_path_buf());
        }
    }

    let mut jobs = Vec::new();
    let mut stream = stream::iter(paths.into_iter())
        .map(|path| async move {
            let metadata = tokio::fs::metadata(&path)
                .await
                .with_context(|| format!("reading file metadata {}", path.display()))?;
            expand_input_path(&path, metadata.len())
        })
        .buffer_unordered(concurrency.max(1));

    while let Some(result) = stream.next().await {
        match result {
            Ok(expanded) => jobs.extend(expanded),
            Err(error) if should_skip_input_error(&error) => {
                eprintln!("⚠️  Skipping unreadable file: {}", error);
            }
            Err(error) => return Err(error),
        }
    }

    Ok(jobs)
}

fn expand_input_path(path: &Path, estimated_size: u64) -> Result<Vec<InputJob>> {
    if is_hidden_file(path) {
        return Ok(Vec::new());
    }

    match detect_input_format(path) {
        Err(error) if should_skip_input_error(&error) => {
            eprintln!("⚠️  Skipping unreadable file {}: {}", path.display(), error);
            Ok(Vec::new())
        }
        Err(error) => Err(error),
        Ok(InputFormat::Plain) => {
            if is_tar_like_path(path) {
                bail!("tar archives are not supported: {}", path.display());
            }

            Ok(vec![InputJob::plain(path.to_path_buf(), estimated_size)])
        }
        Ok(InputFormat::Gzip) => Ok(vec![InputJob::gzip(path.to_path_buf(), estimated_size)]),
        Ok(InputFormat::Bzip2) => Ok(vec![InputJob::bzip2(path.to_path_buf(), estimated_size)]),
        Ok(InputFormat::Zip) => match collect_zip_jobs(path) {
            Ok(jobs) => Ok(jobs),
            Err(error) if should_skip_input_error(&error) => {
                eprintln!("⚠️  Skipping unreadable file {}: {}", path.display(), error);
                Ok(Vec::new())
            }
            Err(error) => Err(error),
        },
    }
}

fn should_skip_input_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.is::<io::Error>() || cause.is::<zip::result::ZipError>())
}

fn collect_zip_jobs(path: &Path) -> Result<Vec<InputJob>> {
    let file =
        StdFile::open(path).with_context(|| format!("open zip archive {}", path.display()))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("read zip archive {}", path.display()))?;
    let mut jobs = Vec::new();
    let estimated_size = std::fs::metadata(path)
        .with_context(|| format!("reading file metadata {}", path.display()))?
        .len();

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
            estimated_size,
            entry_index,
            entry_name,
        ));
    }

    Ok(jobs)
}

fn default_scan_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get().saturating_mul(2))
        .unwrap_or(8)
        .clamp(4, 32)
}

fn file_timestamp_ms(path: &Path) -> Result<i64> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("reading file metadata {}", path.display()))?;
    let modified = metadata
        .modified()
        .with_context(|| format!("reading modified time {}", path.display()))?;
    let duration = modified
        .duration_since(UNIX_EPOCH)
        .context("file modified time predates unix epoch")?;

    Ok(duration.as_millis() as i64)
}

fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_millis() as i64
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

fn nmea_sentence(payload: &str) -> &str {
    let sentence_start = payload
        .char_indices()
        .find(|(_, ch)| *ch == '!' || *ch == '$')
        .map(|(idx, _)| idx)
        .unwrap_or(payload.len());

    &payload[sentence_start..]
}

#[derive(Debug, Default)]
struct AisTagBlock {
    timestamp_ms: Option<i64>,
    group_id: Option<String>,
}

#[derive(Debug, Default)]
struct AisSentenceMetadata {
    is_ais: bool,
    group_id: Option<String>,
    fragment_count: Option<usize>,
    fragment_number: Option<usize>,
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

fn parse_ais_sentence_metadata(sentence: &str) -> AisSentenceMetadata {
    let mut fields = sentence.split(',').map(normalize_sentence_field);
    let Some(sentence_type) = fields.next() else {
        return AisSentenceMetadata::default();
    };

    if !is_ais_sentence_type(sentence_type) {
        return AisSentenceMetadata::default();
    }

    let fragment_count = fields.next().and_then(|value| value.parse::<usize>().ok());
    let fragment_number = fields.next().and_then(|value| value.parse::<usize>().ok());
    let sequence_id = fields
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let channel = fields.next().map(str::trim).unwrap_or_default();

    let group_id = match (fragment_count, sequence_id) {
        (Some(fragment_count), Some(sequence_id)) if fragment_count > 1 => Some(format!(
            "{}:{}:{}:{}",
            sentence_type, fragment_count, sequence_id, channel
        )),
        _ => None,
    };

    AisSentenceMetadata {
        is_ais: true,
        group_id,
        fragment_count,
        fragment_number,
    }
}

impl AisSentenceMetadata {
    fn is_fragmented(&self) -> bool {
        matches!(self.fragment_count, Some(count) if count > 1)
    }

    fn is_final_fragment(&self) -> bool {
        matches!((self.fragment_count, self.fragment_number), (Some(count), Some(number)) if count == number)
    }
}

fn parse_pghp_timestamp_ms(sentence: &str) -> Option<i64> {
    let mut fields = sentence.split(',').map(normalize_sentence_field);
    let sentence_type = fields.next()?;

    if sentence_type != "$PGHP" {
        return None;
    }

    let _message_type = fields.next()?;
    let year = fields.next()?.parse::<i32>().ok()?;
    let month = fields.next()?.parse::<u32>().ok()?;
    let day = fields.next()?.parse::<u32>().ok()?;
    let hour = fields.next()?.parse::<u32>().ok()?;
    let minute = fields.next()?.parse::<u32>().ok()?;
    let second = fields.next()?.parse::<u32>().ok()?;
    let millisecond = fields.next()?.parse::<u32>().ok()?;

    if millisecond >= 1_000 {
        return None;
    }

    let timestamp_ms = Utc
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()?
        .timestamp_millis();

    Some(timestamp_ms.saturating_add(i64::from(millisecond)))
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

fn is_ais_sentence_type(sentence_type: &str) -> bool {
    sentence_type.ends_with("VDM") || sentence_type.ends_with("VDO")
}

fn normalize_sentence_field(field: &str) -> &str {
    field.split('*').next().unwrap_or(field).trim()
}

fn cache_timestamp_for_groups(
    timestamps: &mut HashMap<String, i64>,
    timestamp_ms: i64,
    tag_group_id: Option<&str>,
    sentence_group_id: Option<&str>,
) {
    if let Some(group_id) = tag_group_id {
        timestamps.insert(group_id.to_string(), timestamp_ms);
    }
    if let Some(group_id) = sentence_group_id {
        timestamps.insert(group_id.to_string(), timestamp_ms);
    }
}

fn lookup_timestamp_for_groups(
    timestamps: &HashMap<String, i64>,
    tag_group_id: Option<&str>,
    sentence_group_id: Option<&str>,
) -> Option<i64> {
    tag_group_id
        .and_then(|group_id| timestamps.get(group_id).copied())
        .or_else(|| sentence_group_id.and_then(|group_id| timestamps.get(group_id).copied()))
}

fn remove_timestamp_for_groups(
    timestamps: &mut HashMap<String, i64>,
    tag_group_id: Option<&str>,
    sentence_group_id: Option<&str>,
) {
    if let Some(group_id) = tag_group_id {
        timestamps.remove(group_id);
    }
    if let Some(group_id) = sentence_group_id {
        timestamps.remove(group_id);
    }
}

fn normalize_ais_payload(payload: &str, timestamp_ms: i64) -> Option<String> {
    let timestamp_seconds = timestamp_ms.div_euclid(1_000);

    if let Some(malformed) = malformed_leading_epoch_tag_block(payload) {
        return Some(format_tagged_ais_payload(
            malformed.tag_body,
            malformed.sentence,
            timestamp_seconds,
        ));
    }

    let tagged = ais_tag_block(payload)?;
    if tagged
        .tag_body
        .split(',')
        .map(|field| field.split('*').next().unwrap_or(field).trim())
        .any(|field| field.starts_with("c:"))
    {
        return None;
    }

    if !tagged
        .tag_body
        .split(',')
        .map(|field| field.split('*').next().unwrap_or(field).trim())
        .any(|field| field.starts_with("g:"))
    {
        return None;
    }

    Some(format_tagged_ais_payload(
        tagged.tag_body,
        tagged.sentence,
        timestamp_seconds,
    ))
}

fn format_tagged_ais_payload(tag_body: &str, sentence: &str, timestamp_seconds: i64) -> String {
    let mut fields = tag_body
        .split(',')
        .filter(|field| !field.is_empty())
        .filter(|field| !field.starts_with("c:"))
        .map(str::to_string)
        .collect::<Vec<_>>();
    fields.push(format!("c:{}", timestamp_seconds));
    let tag_body = fields.join(",");
    let checksum = nmea_tag_block_checksum(&tag_body);

    format!("\\{}*{:02X}\\{}", tag_body, checksum, sentence)
}

struct MalformedTagBlock<'a> {
    tag_body: &'a str,
    sentence: &'a str,
}

struct AisTaggedPayload<'a> {
    tag_body: &'a str,
    sentence: &'a str,
}

fn ais_tag_block(payload: &str) -> Option<AisTaggedPayload<'_>> {
    let prefix_end = payload
        .char_indices()
        .find(|(_, ch)| *ch == '!' || *ch == '$')
        .map(|(idx, _)| idx)?;
    let prefix = &payload[..prefix_end];
    let sentence = &payload[prefix_end..];

    if !prefix.starts_with('\\') {
        return None;
    }

    let tag_block_end = prefix[1..].find('\\').map(|idx| idx + 1)?;
    let tag_block = &prefix[1..tag_block_end];
    let tag_body = tag_block.split('*').next().unwrap_or(tag_block).trim();
    if tag_body.is_empty() {
        return None;
    }

    Some(AisTaggedPayload { tag_body, sentence })
}

fn malformed_leading_epoch_tag_block(payload: &str) -> Option<MalformedTagBlock<'_>> {
    let sentence_start = payload
        .char_indices()
        .find(|(_, ch)| *ch == '!' || *ch == '$')
        .map(|(idx, _)| idx)?;
    let prefix = &payload[..sentence_start];
    let sentence = &payload[sentence_start..];
    let tag_start = prefix.find('\\')?;
    let leading = prefix[..tag_start].trim();
    if leading.is_empty() || !leading.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    let tag_block = &prefix[tag_start + 1..];
    let tag_body = tag_block.split('*').next().unwrap_or(tag_block).trim();
    if tag_body.is_empty() {
        return None;
    }

    Some(MalformedTagBlock { tag_body, sentence })
}

fn nmea_tag_block_checksum(tag_body: &str) -> u8 {
    tag_body.bytes().fold(0u8, |checksum, byte| checksum ^ byte)
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

fn open_plain_file_reader(path: PathBuf, max_line_length: usize) -> Result<LineReader> {
    let (tx, rx) = mpsc::channel::<io::Result<Bytes>>(8);

    let _ = tokio::task::spawn_blocking(move || {
        let result: io::Result<()> = (|| {
            let file = StdFile::open(&path)?;
            let mut reader = io::BufReader::new(file);
            let mut buffer = vec![0u8; 32 * 1024];

            loop {
                let read = reader.read(&mut buffer)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, TimestampMillisecondArray};
    use chrono::{Datelike, TimeZone, Timelike, Utc};
    use collect_core::{run_ingest, CommonOptions, IngestOptions};
    use futures_util::StreamExt;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::HashMap;
    use std::fs::File as StdFile;
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
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
                    compression_level: 5,
                    upload_drain_timeout_seconds: 1,
                    max_line_length: 1024,
                    health_check: false,
                },
                s3: None,
                health_file,
                manage_health: true,
                report_progress: true,
                log_writes: true,
                shutdown: None,
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

    #[cfg(unix)]
    #[tokio::test]
    async fn skips_unreadable_files_in_directories() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path();

        write_text_file(&root.join("a_readable.txt"), "good-1\n")?;
        let unreadable = root.join("b_unreadable.txt");
        write_text_file(&unreadable, "bad-1\n")?;
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0))?;

        let mut source = FileInputSource::new(root.to_path_buf(), "source".to_string(), false)?;
        let lines = collect_all_lines(&mut source, 1024).await?;

        assert_eq!(lines, vec!["good-1"]);
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
            ais_pending_timestamp_ms: None,
            ais_payload_timestamp_ms: None,
            current_job_timestamp_ms: None,
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
            ais_pending_timestamp_ms: None,
            ais_payload_timestamp_ms: None,
            current_job_timestamp_ms: None,
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
    fn normalizes_malformed_leading_epoch_tag_block() {
        let payload = r"1564038609\g:2-2-0013*5F\!AIVDM,2,2,3,A,8P?EDQ0,2*20";

        assert_eq!(
            normalize_ais_payload(payload, 1_564_038_609_000).as_deref(),
            Some(r"\g:2-2-0013,c:1564038609*28\!AIVDM,2,2,3,A,8P?EDQ0,2*20")
        );
    }

    #[test]
    fn leaves_valid_tag_block_payload_unchanged() {
        let payload = r"\g:1-2-73874,s:r003669945,c:1241544035*4A\!AIVDM,1,1,,B,15N4cJ`005Jrek0H@9n`DW5608EP,0*13";

        assert_eq!(normalize_ais_payload(payload, 1_241_544_035_000), None);
    }

    #[test]
    fn adds_cached_timestamp_to_later_grouped_sentence_tag_block() {
        let payload = r"\g:2-2-4447*00\!AIVDM,2,2,0,A,P0,4*72";

        assert_eq!(
            normalize_ais_payload(payload, 1_604_189_030_000).as_deref(),
            Some(r"\g:2-2-4447,c:1604189030*2B\!AIVDM,2,2,0,A,P0,4*72")
        );
    }

    #[test]
    fn leaves_uncached_later_grouped_sentence_payload_unchanged() {
        let mut source = FileInputSource {
            source: "source".to_string(),
            ais: true,
            ais_group_timestamps: HashMap::new(),
            ais_pending_timestamp_ms: None,
            ais_payload_timestamp_ms: None,
            current_job_timestamp_ms: None,
            jobs: vec![],
            cursor: 0,
        };
        let payload = r"\g:2-2-4447*00\!AIVDM,2,2,0,A,P0,4*72";

        assert_eq!(source.timestamp_for_payload(payload), None);
        assert_eq!(
            source.normalize_payload(payload.to_string(), 1_604_189_030_000),
            payload
        );
    }

    #[test]
    fn reuses_ais_group_timestamp_for_three_fragment_messages() {
        let mut source = FileInputSource {
            source: "source".to_string(),
            ais: true,
            ais_group_timestamps: HashMap::new(),
            ais_pending_timestamp_ms: None,
            ais_payload_timestamp_ms: None,
            current_job_timestamp_ms: None,
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
    fn uses_tag_group_id_as_cache_key() {
        let mut source = FileInputSource {
            source: "source".to_string(),
            ais: true,
            ais_group_timestamps: HashMap::new(),
            ais_pending_timestamp_ms: None,
            ais_payload_timestamp_ms: None,
            current_job_timestamp_ms: None,
            jobs: vec![],
            cursor: 0,
        };

        let three_part_first = r"\g:1-3-42,c:1609459200*00\!AIVDM,3,1,9,A,first,0*00";
        let two_part_second = r"\g:2-2-42*00\!AIVDM,2,2,8,A,second,0*00";
        let three_part_second = r"\g:2-3-42*00\!AIVDM,3,2,9,A,third,0*00";

        assert_eq!(
            source.timestamp_for_payload(three_part_first),
            Some(1_609_459_200_000)
        );
        assert_eq!(
            source.timestamp_for_payload(two_part_second),
            Some(1_609_459_200_000)
        );
        assert_eq!(
            source.timestamp_for_payload(three_part_second),
            Some(1_609_459_200_000)
        );
    }

    #[test]
    fn reports_current_input_progress() {
        let source = FileInputSource {
            source: "source".to_string(),
            ais: false,
            ais_group_timestamps: HashMap::new(),
            ais_pending_timestamp_ms: None,
            ais_payload_timestamp_ms: None,
            current_job_timestamp_ms: None,
            jobs: vec![InputJob::plain(PathBuf::from("/tmp/input.txt"), 0)],
            cursor: 1,
        };

        let progress = source
            .ingest_progress()
            .expect("expected progress snapshot");
        assert_eq!(progress.current_input, "/tmp/input.txt");
        assert_eq!(progress.current_input_index, 1);
        assert_eq!(progress.input_total, 1);
    }

    #[test]
    fn uses_file_timestamp_for_plain_payloads() {
        let mut source = FileInputSource {
            source: "source".to_string(),
            ais: false,
            ais_group_timestamps: HashMap::new(),
            ais_pending_timestamp_ms: None,
            ais_payload_timestamp_ms: None,
            current_job_timestamp_ms: Some(1_700_000_000_000),
            jobs: vec![],
            cursor: 0,
        };

        assert_eq!(
            source.timestamp_for_payload("plain payload"),
            Some(1_700_000_000_000)
        );
    }

    #[tokio::test]
    async fn uses_pghp_timestamp_for_partitioning_and_keeps_payload() -> Result<()> {
        let dir = tempdir()?;
        let input_path = dir.path().join("ais.txt");
        let out_dir = dir.path().join("out");
        let health_file = dir.path().join("health");
        let lines = vec![
            "$PGHP,1,2013,1,9,4,37,45,298,,110,,1,26*19",
            "!AIVDM,1,1,0,A,13aER4hjP00D@FLMU<r@0?wB0000,0*26",
            "$PGHP,1,2013,1,9,4,37,46,120,,110,,1,75*18",
            "!AIVDM,2,1,3,A,569qcJP000000000000P4V1QDr3777800000000o0p=220DP03888888,0*49",
            "!AIVDM,2,2,3,A,88881CRR@CACP08,2*3C",
        ];
        let ais_input = format!("{}\n", lines.join("\n"));

        write_text_file(&input_path, &ais_input)?;

        let mut source = FileInputSource::new(input_path, "ais-source".to_string(), true)?;

        let expected_ts_one = Utc
            .with_ymd_and_hms(2013, 1, 9, 4, 37, 45)
            .single()
            .expect("valid AIS timestamp")
            .timestamp_millis()
            .saturating_add(298);
        let expected_ts_two = Utc
            .with_ymd_and_hms(2013, 1, 9, 4, 37, 46)
            .single()
            .expect("valid AIS timestamp")
            .timestamp_millis()
            .saturating_add(120);

        assert_eq!(
            source.timestamp_for_payload(lines[0]),
            Some(expected_ts_one)
        );
        assert_eq!(
            source.timestamp_for_payload(lines[1]),
            Some(expected_ts_one)
        );
        assert_eq!(
            source.timestamp_for_payload(lines[2]),
            Some(expected_ts_two)
        );
        assert_eq!(
            source.timestamp_for_payload(lines[3]),
            Some(expected_ts_two)
        );
        assert_eq!(
            source.timestamp_for_payload(lines[4]),
            Some(expected_ts_two)
        );

        run_ingest(
            &mut source,
            IngestOptions {
                common: CommonOptions {
                    out_dir: out_dir.clone(),
                    partition: collect_core::PartitionGranularity::Minute,
                    max_rows: Some(10),
                    max_batch_bytes: 1024 * 1024,
                    compression_level: 5,
                    upload_drain_timeout_seconds: 1,
                    max_line_length: 1024,
                    health_check: false,
                },
                s3: None,
                health_file,
                manage_health: true,
                report_progress: true,
                log_writes: true,
                shutdown: None,
            },
        )
        .await?;

        let expected = Utc
            .timestamp_opt(expected_ts_one / 1_000, 0)
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
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .context("missing timestamp column")?;
        let payload = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("missing payload column")?;

        assert_eq!(batch.num_rows(), lines.len());
        assert_eq!(timestamps.value(0), expected_ts_one);
        assert_eq!(timestamps.value(1), expected_ts_one);
        assert_eq!(timestamps.value(2), expected_ts_two);
        assert_eq!(timestamps.value(3), expected_ts_two);
        assert_eq!(timestamps.value(4), expected_ts_two);
        assert_eq!(payload.value(0), lines[0]);
        assert_eq!(payload.value(1), lines[1]);
        assert_eq!(payload.value(2), lines[2]);
        assert_eq!(payload.value(3), lines[3]);
        assert_eq!(payload.value(4), lines[4]);
        Ok(())
    }

    #[tokio::test]
    async fn skips_invalid_gzip_inputs_without_failing() -> Result<()> {
        let dir = tempdir()?;
        let input_path = dir.path().join("broken.gz");
        write_text_file(&input_path, "this is not gzip\n")?;

        let mut source = FileInputSource::new(input_path, "broken".to_string(), false)?;
        let lines = collect_all_lines(&mut source, 1024).await?;

        assert!(lines.is_empty());
        Ok(())
    }

    #[test]
    fn reuses_pghp_capture_timestamp_for_follow_on_fragments() {
        let mut source = FileInputSource {
            source: "source".to_string(),
            ais: true,
            ais_group_timestamps: HashMap::new(),
            ais_pending_timestamp_ms: None,
            ais_payload_timestamp_ms: None,
            current_job_timestamp_ms: None,
            jobs: vec![],
            cursor: 0,
        };

        let timestamp = "$PGHP,1,2013,1,9,4,37,45,298,,110,,1,26*19";
        let first_fragment =
            "!AIVDM,2,1,3,A,569qcJP000000000000P4V1QDr3777800000000o0p=220DP03888888,0*49";
        let second_fragment = "!AIVDM,2,2,3,A,88881CRR@CACP08,2*3C";

        let expected_ts = Utc
            .with_ymd_and_hms(2013, 1, 9, 4, 37, 45)
            .single()
            .expect("valid AIS timestamp")
            .timestamp_millis()
            .saturating_add(298);

        assert_eq!(source.timestamp_for_payload(timestamp), Some(expected_ts));
        assert_eq!(
            source.timestamp_for_payload(first_fragment),
            Some(expected_ts)
        );
        assert_eq!(
            source.timestamp_for_payload(second_fragment),
            Some(expected_ts)
        );
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
