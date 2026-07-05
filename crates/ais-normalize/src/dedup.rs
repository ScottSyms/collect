//! Merge-and-deduplicate output partitions so repeated runs are idempotent.
//!
//! Re-running normalize over the same (or overlapping) input regenerates
//! byte-identical rows, and each run writes new uniquely-named files, so
//! duplicates would otherwise accumulate. After a run finishes, we merge every
//! parquet file in each touched output partition and drop exact `(ts, payload)`
//! duplicates. Two rows are duplicates only if both the timestamp and the full
//! payload match — rows that differ in any way (including the `\s:` receiver
//! tag) are all kept.
//!
//! The merge streams: collectors write row groups already sorted by `ts`, so we
//! treat each row group as a sorted run and k-way merge them, deduplicating
//! within each equal-`ts` group with a small per-timestamp payload set. Peak
//! memory is one read batch per run plus one second's worth of payloads — not
//! the whole partition. (This mirrors `collect-maint`'s compaction merge in
//! `crates/collect-maint/src/commands.rs`; kept separate here to avoid coupling
//! the two crates.)

use crate::output::{build_schema, open_writer, parquet_file_name};
use anyhow::{Context, Result};
use arrow::array::{
    Array, StringArray, StringBuilder, TimestampMillisecondArray, TimestampMillisecondBuilder,
};
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use collect_core::{sort_record_batch_by_ts, S3Storage};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MERGE_BATCH_ROWS: usize = 8192;

#[derive(Clone, Copy, Debug, Default)]
pub struct DedupStats {
    pub partitions_merged: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub duplicates_removed: u64,
}

impl DedupStats {
    pub fn merge(&mut self, other: &DedupStats) {
        self.partitions_merged += other.partitions_merged;
        self.rows_in += other.rows_in;
        self.rows_out += other.rows_out;
        self.duplicates_removed += other.duplicates_removed;
    }

    pub fn print_summary(&self) {
        if self.partitions_merged == 0 {
            return;
        }
        eprintln!("--- dedup summary ---");
        eprintln!("  partitions merged    : {}", self.partitions_merged);
        eprintln!("  rows in              : {}", self.rows_in);
        eprintln!("  rows out             : {}", self.rows_out);
        eprintln!("  duplicates removed   : {}", self.duplicates_removed);
    }
}

/// One sorted run over a single parquet row group.
struct MergeRun {
    reader: Option<ParquetRecordBatchReader>,
    ts: TimestampMillisecondArray,
    source: StringArray,
    payload: StringArray,
    pos: usize,
}

impl MergeRun {
    fn streaming(
        path: &Path,
        metadata: &ArrowReaderMetadata,
        row_group: usize,
    ) -> Result<Option<Self>> {
        let reader = open_row_group_reader(path, metadata, row_group, None)?;
        let mut run = Self {
            reader: Some(reader),
            ts: TimestampMillisecondArray::from(Vec::<i64>::new()),
            source: StringArray::from(Vec::<&str>::new()),
            payload: StringArray::from(Vec::<&str>::new()),
            pos: 0,
        };
        Ok(if run.load_next_batch()? {
            Some(run)
        } else {
            None
        })
    }

    fn buffered(
        path: &Path,
        metadata: &ArrowReaderMetadata,
        row_group: usize,
    ) -> Result<Option<Self>> {
        let mut reader = open_row_group_reader(path, metadata, row_group, None)?;
        let mut batches = Vec::new();
        while let Some(batch) = reader.next().transpose()? {
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }
        if batches.is_empty() {
            return Ok(None);
        }
        let schema = batches[0].schema();
        let combined = concat_batches(&schema, &batches).context("concatenate record batches")?;
        let sorted = sort_record_batch_by_ts(&combined)?;
        let (ts, source, payload) = split_columns(&sorted)?;
        Ok(Some(Self {
            reader: None,
            ts,
            source,
            payload,
            pos: 0,
        }))
    }

    fn current_ts(&self) -> i64 {
        self.ts.value(self.pos)
    }

    fn current_source(&self) -> &str {
        self.source.value(self.pos)
    }

    fn current_payload(&self) -> &str {
        self.payload.value(self.pos)
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        while let Some(reader) = self.reader.as_mut() {
            match reader.next().transpose().context("reading parquet batch")? {
                Some(batch) if batch.num_rows() > 0 => {
                    let (ts, source, payload) = split_columns(&batch)?;
                    self.ts = ts;
                    self.source = source;
                    self.payload = payload;
                    self.pos = 0;
                    return Ok(true);
                }
                Some(_) => continue,
                None => self.reader = None,
            }
        }
        Ok(false)
    }

    fn advance(&mut self) -> Result<bool> {
        self.pos += 1;
        if self.pos < self.ts.len() {
            return Ok(true);
        }
        self.load_next_batch()
    }
}

fn open_row_group_reader(
    path: &Path,
    metadata: &ArrowReaderMetadata,
    row_group: usize,
    projection: Option<ProjectionMask>,
) -> Result<ParquetRecordBatchReader> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata.clone())
        .with_row_groups(vec![row_group])
        .with_batch_size(MERGE_BATCH_ROWS);
    if let Some(projection) = projection {
        builder = builder.with_projection(projection);
    }
    builder
        .build()
        .with_context(|| format!("build parquet reader {}", path.display()))
}

/// Streams only the ts column, so checking costs far less than a full decode.
fn row_group_is_sorted_by_ts(
    path: &Path,
    metadata: &ArrowReaderMetadata,
    row_group: usize,
) -> Result<bool> {
    let ts_only = ProjectionMask::leaves(metadata.metadata().file_metadata().schema_descr(), [0]);
    let reader = open_row_group_reader(path, metadata, row_group, Some(ts_only))?;
    let mut last = i64::MIN;
    for batch in reader {
        let batch = batch.context("reading ts column")?;
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| anyhow::anyhow!("missing ts column in {}", path.display()))?;
        for index in 0..ts.len() {
            let value = ts.value(index);
            if value < last {
                return Ok(false);
            }
            last = value;
        }
    }
    Ok(true)
}

fn split_columns(
    batch: &RecordBatch,
) -> Result<(TimestampMillisecondArray, StringArray, StringArray)> {
    if batch.num_columns() < 3 {
        anyhow::bail!("unexpected column count: {}", batch.num_columns());
    }
    let ts = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| anyhow::anyhow!("missing ts column"))?
        .clone();
    let source = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("missing source column"))?
        .clone();
    let payload = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("missing payload column"))?
        .clone();
    Ok((ts, source, payload))
}

/// Buffers merged rows and writes them out in fixed-size chunks. Rows arrive
/// already ts-sorted from the merge, so no per-batch sort is needed.
struct MergedWriter {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    ts: TimestampMillisecondBuilder,
    source: StringBuilder,
    payload: StringBuilder,
    chunk_rows: usize,
}

impl MergedWriter {
    fn new(output_path: &Path, compression_level: i32) -> Result<Self> {
        let schema = build_schema();
        let writer = open_writer(output_path, &schema, compression_level)?;
        Ok(Self {
            writer,
            schema,
            ts: TimestampMillisecondBuilder::with_capacity(MERGE_BATCH_ROWS),
            source: StringBuilder::with_capacity(MERGE_BATCH_ROWS, MERGE_BATCH_ROWS * 8),
            payload: StringBuilder::with_capacity(MERGE_BATCH_ROWS, MERGE_BATCH_ROWS * 64),
            chunk_rows: 0,
        })
    }

    fn push(&mut self, ts_ms: i64, source: &str, payload: &str) -> Result<()> {
        self.ts.append_value(ts_ms);
        self.source.append_value(source);
        self.payload.append_value(payload);
        self.chunk_rows += 1;
        if self.chunk_rows >= MERGE_BATCH_ROWS {
            self.flush_chunk()?;
        }
        Ok(())
    }

    fn flush_chunk(&mut self) -> Result<()> {
        if self.chunk_rows == 0 {
            return Ok(());
        }
        let ts_array = self.ts.finish().with_timezone_opt(Some(Arc::from("UTC")));
        let source_array = self.source.finish();
        let payload_array = self.payload.finish();
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(ts_array) as Arc<dyn Array>,
                Arc::new(source_array) as Arc<dyn Array>,
                Arc::new(payload_array) as Arc<dyn Array>,
            ],
        )
        .context("building merged RecordBatch")?;
        self.writer.write(&batch).context("writing merged batch")?;
        self.chunk_rows = 0;
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        self.flush_chunk()?;
        self.writer
            .close()
            .context("closing merged parquet writer")?;
        Ok(())
    }
}

/// Upper bound on parquet readers (≈ open file descriptors) held at once by a
/// single merge. A partition with more row groups than this is merged in rounds
/// through intermediate files so descriptor use never scales with file count.
const MERGE_MAX_OPEN_READERS: usize = 128;

/// Count non-empty row groups in a parquet file. Opens and closes one handle.
fn nonempty_row_group_count(path: &Path) -> Result<usize> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())
        .with_context(|| format!("read parquet footer {}", path.display()))?;
    let mut count = 0;
    for row_group in 0..metadata.metadata().num_row_groups() {
        if metadata.metadata().row_group(row_group).num_rows() > 0 {
            count += 1;
        }
    }
    Ok(count)
}

/// Pack `inputs` into batches whose combined row-group count (≈ concurrent open
/// readers) stays under `MERGE_MAX_OPEN_READERS`. Every batch except the last
/// holds at least two files, so a multi-round merge always makes progress even
/// when individual files already exceed the budget.
fn plan_merge_batches(inputs: &[PathBuf]) -> Result<Vec<Vec<PathBuf>>> {
    let mut batches: Vec<Vec<PathBuf>> = Vec::new();
    let mut current: Vec<PathBuf> = Vec::new();
    let mut current_rgs = 0usize;
    for input in inputs {
        let rgs = nonempty_row_group_count(input)?.max(1);
        if current.len() >= 2 && current_rgs + rgs > MERGE_MAX_OPEN_READERS {
            batches.push(std::mem::take(&mut current));
            current_rgs = 0;
        }
        current.push(input.clone());
        current_rgs += rgs;
    }
    if !current.is_empty() {
        batches.push(current);
    }
    Ok(batches)
}

/// K-way merge `inputs` into `output_path`, dropping exact `(ts, payload)`
/// duplicates. Bounds concurrently-open readers to `MERGE_MAX_OPEN_READERS` by
/// merging in rounds through intermediate files when a partition holds more
/// files than fit at once — so it never runs out of file descriptors regardless
/// of file count. Blocking; call from `spawn_blocking`.
fn merge_dedup_files(
    inputs: &[PathBuf],
    output_path: &Path,
    compression_level: i32,
) -> Result<DedupStats> {
    let batches = plan_merge_batches(inputs)?;
    // Common case: everything fits in one pass — write straight to the output.
    if batches.len() <= 1 {
        return merge_run_batch(inputs, output_path, compression_level);
    }

    let scratch = tempfile::Builder::new()
        .prefix("ais-normalize-merge-")
        .tempdir()
        .context("creating merge scratch directory")?;

    // Round 0 consumes the original inputs; its combined rows_in is the true
    // input-row count (later rounds re-merge already-deduped intermediates).
    let mut total_input_rows = 0u64;
    let mut round_files: Vec<PathBuf> = Vec::new();
    for (index, batch) in batches.iter().enumerate() {
        let out = scratch.path().join(format!("r0-{index:06}.parquet"));
        let stats = merge_run_batch(batch, &out, compression_level)?;
        total_input_rows += stats.rows_in;
        round_files.push(out);
    }

    let mut round = 1u32;
    loop {
        let batches = plan_merge_batches(&round_files)?;
        if batches.len() <= 1 {
            let final_stats = merge_run_batch(&round_files, output_path, compression_level)?;
            return Ok(DedupStats {
                partitions_merged: 0,
                rows_in: total_input_rows,
                rows_out: final_stats.rows_out,
                duplicates_removed: total_input_rows.saturating_sub(final_stats.rows_out),
            });
        }

        let mut next_files = Vec::with_capacity(batches.len());
        for (index, batch) in batches.iter().enumerate() {
            let out = scratch.path().join(format!("r{round}-{index:06}.parquet"));
            merge_run_batch(batch, &out, compression_level)?;
            next_files.push(out);
        }
        // The previous round's intermediates are fully consumed; reclaim them.
        for file in &round_files {
            let _ = fs::remove_file(file);
        }
        round_files = next_files;
        round += 1;
    }
}

/// Single-pass k-way merge of `inputs` (one sorted run per row group) into
/// `output_path`, dropping exact `(ts, payload)` duplicates. Holds one reader
/// per row group open for the duration, so callers bound the batch size.
fn merge_run_batch(
    inputs: &[PathBuf],
    output_path: &Path,
    compression_level: i32,
) -> Result<DedupStats> {
    let mut runs: Vec<MergeRun> = Vec::new();
    for input in inputs {
        let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
        let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())
            .with_context(|| format!("read parquet footer {}", input.display()))?;
        for row_group in 0..metadata.metadata().num_row_groups() {
            if metadata.metadata().row_group(row_group).num_rows() == 0 {
                continue;
            }
            let run = if row_group_is_sorted_by_ts(input, &metadata, row_group)? {
                MergeRun::streaming(input, &metadata, row_group)?
            } else {
                MergeRun::buffered(input, &metadata, row_group)?
            };
            runs.extend(run);
        }
    }

    let mut writer = MergedWriter::new(output_path, compression_level)?;
    let mut stats = DedupStats::default();

    let mut heap: BinaryHeap<Reverse<(i64, usize)>> = runs
        .iter()
        .enumerate()
        .map(|(index, run)| Reverse((run.current_ts(), index)))
        .collect();

    // All rows sharing a timestamp are emitted consecutively (heap orders by
    // ts), so a set holding one timestamp's `(source, payload)` keys is enough
    // to dedup; it is cleared when the timestamp advances. Source is part of
    // the key so a message that two sources reported byte-identically is kept
    // once per source (provenance is retained), while a re-run of the same
    // source still collapses to one row.
    let mut current_ts = i64::MIN;
    let mut seen: HashSet<(String, String)> = HashSet::new();
    while let Some(Reverse((ts_ms, index))) = heap.pop() {
        stats.rows_in += 1;
        let source = runs[index].current_source();
        let payload = runs[index].current_payload();
        if ts_ms != current_ts {
            current_ts = ts_ms;
            seen.clear();
        }
        let key = (source.to_string(), payload.to_string());
        if seen.contains(&key) {
            stats.duplicates_removed += 1;
        } else {
            writer.push(ts_ms, source, payload)?;
            seen.insert(key);
            stats.rows_out += 1;
        }
        if runs[index].advance()? {
            heap.push(Reverse((runs[index].current_ts(), index)));
        }
    }

    writer.close()?;
    Ok(stats)
}

/// `.parquet` files directly inside `dir`, excluding in-progress `tmp-` files.
fn parquet_files_in_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(files),
        Err(error) => return Err(error).with_context(|| format!("read dir {}", dir.display())),
    };
    for entry in entries {
        let entry = entry.with_context(|| format!("read dir entry in {}", dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.ends_with(".parquet") && !name.starts_with("tmp-") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// Merge-dedup a local partition directory in place. By default only rewrites
/// when the partition has 2+ files (a prior run's output or a generation
/// rollover could have introduced cross-file duplicates); a lone fresh file is
/// left untouched to keep first runs cheap. When `force` is set — used when a
/// single source is fed by multiple inputs, so one output file can already
/// contain cross-input duplicates — a single file is merged too. Blocking;
/// call from `spawn_blocking`.
pub fn dedup_local_partition(
    dir: &Path,
    compression_level: i32,
    force: bool,
) -> Result<DedupStats> {
    let inputs = parquet_files_in_dir(dir)?;
    let threshold = if force { 1 } else { 2 };
    if inputs.len() < threshold {
        return Ok(DedupStats::default());
    }

    // Write the merged file, then delete the originals: on a crash we are left
    // with the merged file plus the originals (duplicates), which the next run
    // cleans — never with missing data.
    let file_name = parquet_file_name();
    let tmp_path = dir.join(format!("tmp-{}", file_name));
    let final_path = dir.join(&file_name);

    let mut stats = merge_dedup_files(&inputs, &tmp_path, compression_level)?;
    fs::rename(&tmp_path, &final_path)
        .with_context(|| format!("rename {} -> {}", tmp_path.display(), final_path.display()))?;
    for input in &inputs {
        if *input != final_path {
            fs::remove_file(input).with_context(|| format!("remove {}", input.display()))?;
        }
    }
    stats.partitions_merged = 1;
    Ok(stats)
}

fn s3_join(prefix: &str, rel: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        rel.to_string()
    } else {
        format!("{}/{}", prefix, rel)
    }
}

/// Merge-dedup one output partition on S3. `new_files_dir` holds this run's
/// freshly-written (not-yet-uploaded) files for the partition; existing objects
/// under the partition prefix are the prior runs' data. Uploads a single merged
/// object and deletes the prior objects. `work_dir` is scratch space for
/// downloads and the merged file.
pub async fn dedup_s3_partition(
    storage: &S3Storage,
    output_prefix: &str,
    rel_dir: &str,
    new_files_dir: &Path,
    work_dir: &Path,
    compression_level: i32,
    force: bool,
) -> Result<DedupStats> {
    let new_files = parquet_files_in_dir(new_files_dir)?;
    let partition_prefix = format!("{}/", s3_join(output_prefix, rel_dir));
    let prior_keys: Vec<String> = storage
        .list_keys_with_prefix(&partition_prefix)
        .await
        .with_context(|| format!("listing s3 partition {}", partition_prefix))?
        .into_iter()
        .filter_map(|object| {
            let key = object.key;
            let name = key.rsplit('/').next().unwrap_or(&key);
            (name.ends_with(".parquet") && !name.starts_with("tmp-")).then_some(key)
        })
        .collect();

    // Fast path: exactly one new file and no prior data — just upload it.
    // Skipped when `force`, so a single output file that may hold cross-input
    // duplicates still gets merged.
    if !force && new_files.len() == 1 && prior_keys.is_empty() {
        let key = s3_join(
            output_prefix,
            &format!("{}/{}", rel_dir, file_name_of(&new_files[0])?),
        );
        storage
            .upload_file(&new_files[0], &key)
            .await
            .with_context(|| format!("uploading {} to s3", new_files[0].display()))?;
        return Ok(DedupStats::default());
    }

    if new_files.is_empty() && prior_keys.is_empty() {
        return Ok(DedupStats::default());
    }

    // Download prior objects alongside the new files, merge, upload, then delete
    // the prior objects (the new files were never uploaded).
    fs::create_dir_all(work_dir).with_context(|| format!("mkdir -p {}", work_dir.display()))?;
    let download_dir = work_dir.join("download");
    fs::create_dir_all(&download_dir)
        .with_context(|| format!("mkdir -p {}", download_dir.display()))?;

    let mut inputs = new_files.clone();
    for (index, key) in prior_keys.iter().enumerate() {
        let local = download_dir.join(format!("prior-{index:06}.parquet"));
        storage
            .download_to_path(key, &local)
            .await
            .with_context(|| format!("downloading s3://{key}"))?;
        inputs.push(local);
    }

    let merged_name = parquet_file_name();
    let merged_path = work_dir.join(&merged_name);
    let level = compression_level;
    let inputs_for_merge = inputs.clone();
    let merged_for_task = merged_path.clone();
    let mut stats = tokio::task::spawn_blocking(move || {
        merge_dedup_files(&inputs_for_merge, &merged_for_task, level)
    })
    .await
    .context("dedup merge task panicked")??;

    let merged_key = s3_join(output_prefix, &format!("{}/{}", rel_dir, merged_name));
    storage
        .upload_file(&merged_path, &merged_key)
        .await
        .with_context(|| format!("uploading merged {} to s3", merged_path.display()))?;

    for key in &prior_keys {
        storage
            .delete_key(key)
            .await
            .with_context(|| format!("deleting prior s3://{key}"))?;
    }

    let _ = fs::remove_dir_all(&download_dir);
    let _ = fs::remove_file(&merged_path);
    stats.partitions_merged = 1;
    Ok(stats)
}

fn file_name_of(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.to_string())
        .ok_or_else(|| anyhow::anyhow!("no file name for {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rows are `(ts, source, payload)`.
    fn write_parquet(path: &Path, rows: &[(i64, &str, &str)]) {
        let schema = build_schema();
        let mut w = open_writer(path, &schema, 1).expect("open");
        let ts =
            TimestampMillisecondArray::from(rows.iter().map(|(t, _, _)| *t).collect::<Vec<_>>())
                .with_timezone_opt(Some(Arc::from("UTC")));
        let source = StringArray::from(rows.iter().map(|(_, s, _)| *s).collect::<Vec<_>>());
        let payload = StringArray::from(rows.iter().map(|(_, _, p)| *p).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ts) as Arc<dyn Array>,
                Arc::new(source) as Arc<dyn Array>,
                Arc::new(payload) as Arc<dyn Array>,
            ],
        )
        .expect("batch");
        w.write(&batch).expect("write");
        w.close().expect("close");
    }

    fn read_all(path: &Path) -> Vec<(i64, String, String)> {
        let file = File::open(path).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("builder")
            .build()
            .expect("build");
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.expect("batch");
            let (ts, source, payload) = split_columns(&batch).expect("cols");
            for i in 0..ts.len() {
                out.push((
                    ts.value(i),
                    source.value(i).to_string(),
                    payload.value(i).to_string(),
                ));
            }
        }
        out
    }

    #[test]
    fn merges_and_drops_exact_duplicates() {
        let dir = tempfile::tempdir().expect("tmp");
        let a = dir.path().join("a.parquet");
        let b = dir.path().join("b.parquet");
        // b fully duplicates a; both have an internal-distinct row at ts=100.
        write_parquet(&a, &[(100, "s", "X"), (100, "s", "Y"), (200, "s", "Z")]);
        write_parquet(&b, &[(100, "s", "X"), (100, "s", "Y"), (200, "s", "Z")]);
        let out = dir.path().join("out.parquet");
        let stats = merge_dedup_files(&[a, b], &out, 1).expect("merge");
        assert_eq!(stats.rows_in, 6);
        assert_eq!(stats.rows_out, 3);
        assert_eq!(stats.duplicates_removed, 3);
        let rows = read_all(&out);
        assert_eq!(rows.len(), 3);
        // sorted by ts, all distinct
        assert_eq!(rows[0].0, 100);
        assert_eq!(rows[2], (200, "s".to_string(), "Z".to_string()));
    }

    #[test]
    fn keeps_same_ts_and_payload_from_distinct_sources() {
        // Two sources reported a byte-identical message at the same ts. Source
        // is part of the dedup key, so both survive (provenance retained),
        // while the within-source repeat is dropped.
        let dir = tempfile::tempdir().expect("tmp");
        let a = dir.path().join("a.parquet");
        write_parquet(
            &a,
            &[
                (100, "norway", "M"),
                (100, "sweden", "M"),
                (100, "norway", "M"),
            ],
        );
        let out = dir.path().join("out.parquet");
        let stats = merge_dedup_files(&[a], &out, 1).expect("merge");
        assert_eq!(stats.rows_out, 2);
        assert_eq!(stats.duplicates_removed, 1);
        let mut sources: Vec<String> = read_all(&out).into_iter().map(|(_, s, _)| s).collect();
        sources.sort();
        assert_eq!(sources, vec!["norway".to_string(), "sweden".to_string()]);
    }

    #[test]
    fn merges_far_more_files_than_the_open_reader_budget() {
        // More files than MERGE_MAX_OPEN_READERS forces the multi-round path;
        // it must succeed (no descriptor exhaustion) and dedup globally.
        let dir = tempfile::tempdir().expect("tmp");
        let n = MERGE_MAX_OPEN_READERS * 3 + 7;
        let mut inputs = Vec::new();
        for i in 0..n {
            let path = dir.path().join(format!("in-{i:05}.parquet"));
            // Every file is identical, so the whole pile collapses to 2 rows.
            write_parquet(&path, &[(100, "s", "X"), (200, "s", "Y")]);
            inputs.push(path);
        }
        let out = dir.path().join("out.parquet");
        let stats = merge_dedup_files(&inputs, &out, 1).expect("multi-round merge");
        assert_eq!(stats.rows_in, (n * 2) as u64);
        assert_eq!(stats.rows_out, 2);
        assert_eq!(stats.duplicates_removed, (n * 2 - 2) as u64);
        let rows = read_all(&out);
        assert_eq!(
            rows,
            vec![
                (100, "s".to_string(), "X".to_string()),
                (200, "s".to_string(), "Y".to_string()),
            ]
        );
    }

    #[test]
    fn multi_round_preserves_distinct_rows() {
        // Each file contributes a distinct ts; the multi-round merge must keep
        // them all and stay ts-sorted.
        let dir = tempfile::tempdir().expect("tmp");
        let n = MERGE_MAX_OPEN_READERS * 2 + 1;
        let mut inputs = Vec::new();
        for i in 0..n {
            let path = dir.path().join(format!("in-{i:05}.parquet"));
            write_parquet(&path, &[(i as i64, "s", "p")]);
            inputs.push(path);
        }
        let out = dir.path().join("out.parquet");
        let stats = merge_dedup_files(&inputs, &out, 1).expect("multi-round merge");
        assert_eq!(stats.rows_out, n as u64);
        assert_eq!(stats.duplicates_removed, 0);
        let rows = read_all(&out);
        assert_eq!(rows.len(), n);
        assert!(rows.windows(2).all(|w| w[0].0 <= w[1].0), "ts-sorted");
    }

    #[test]
    fn keeps_same_ts_distinct_payloads() {
        let dir = tempfile::tempdir().expect("tmp");
        let a = dir.path().join("a.parquet");
        // Same timestamp, different payloads (e.g. different \s: stations) are kept.
        write_parquet(
            &a,
            &[
                (100, "s", "station-A"),
                (100, "s", "station-B"),
                (100, "s", "station-A"),
            ],
        );
        let out = dir.path().join("out.parquet");
        let stats = merge_dedup_files(&[a], &out, 1).expect("merge");
        assert_eq!(stats.rows_out, 2);
        assert_eq!(stats.duplicates_removed, 1);
    }

    #[test]
    fn local_partition_single_file_is_untouched() {
        let dir = tempfile::tempdir().expect("tmp");
        let only = dir.path().join("norm-1.parquet");
        write_parquet(&only, &[(100, "s", "X"), (200, "s", "Y")]);
        let stats = dedup_local_partition(dir.path(), 1, false).expect("dedup");
        assert_eq!(stats.partitions_merged, 0);
        assert!(only.exists(), "single file should be left in place");
    }

    #[test]
    fn local_partition_force_dedups_single_file() {
        let dir = tempfile::tempdir().expect("tmp");
        let only = dir.path().join("norm-1.parquet");
        // One file that already holds an internal duplicate (e.g. two inputs of
        // the same source merged into one output file).
        write_parquet(&only, &[(100, "s", "X"), (100, "s", "X"), (200, "s", "Y")]);
        let stats = dedup_local_partition(dir.path(), 1, true).expect("dedup");
        assert_eq!(stats.partitions_merged, 1);
        assert_eq!(stats.duplicates_removed, 1);
        let files = parquet_files_in_dir(dir.path()).expect("list");
        assert_eq!(files.len(), 1);
        assert_eq!(read_all(&files[0]).len(), 2);
    }

    #[test]
    fn local_partition_merges_multiple_files_into_one() {
        let dir = tempfile::tempdir().expect("tmp");
        write_parquet(
            &dir.path().join("norm-1.parquet"),
            &[(100, "s", "X"), (200, "s", "Y")],
        );
        write_parquet(
            &dir.path().join("norm-2.parquet"),
            &[(100, "s", "X"), (300, "s", "Z")],
        );
        let stats = dedup_local_partition(dir.path(), 1, false).expect("dedup");
        assert_eq!(stats.partitions_merged, 1);
        assert_eq!(stats.duplicates_removed, 1);
        let files = parquet_files_in_dir(dir.path()).expect("list");
        assert_eq!(files.len(), 1, "partition collapses to one file");
        let rows = read_all(&files[0]);
        assert_eq!(rows.len(), 3);
    }
}
