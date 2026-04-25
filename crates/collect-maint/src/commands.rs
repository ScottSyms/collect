use crate::partition::{
    compaction_output_name, manifest_path_for_output, CompactionManifest, EntryKind, PartitionKey,
};
use crate::progress::{report, report_step, should_report, SCAN_REPORT_INTERVAL};
use crate::storage::{DatasetEntry, StorageLocation};
use anyhow::{bail, Context, Result};
use arrow::array::{Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use std::collections::BTreeMap;
use std::fs::File as StdFile;
use std::path::{Path, PathBuf};

struct FileScan {
    rows: usize,
    min_ts_ms: Option<i64>,
    max_ts_ms: Option<i64>,
    issues: Vec<String>,
}

struct PartitionSummary {
    parquet_files: usize,
    compacted_files: usize,
    manifests: usize,
    temp_files: usize,
    other_files: usize,
    bytes: u64,
    rows: usize,
    min_ts_ms: Option<i64>,
    max_ts_ms: Option<i64>,
}

impl PartitionSummary {
    fn new() -> Self {
        Self {
            parquet_files: 0,
            compacted_files: 0,
            manifests: 0,
            temp_files: 0,
            other_files: 0,
            bytes: 0,
            rows: 0,
            min_ts_ms: None,
            max_ts_ms: None,
        }
    }

    fn update_ts(&mut self, scan: &FileScan) {
        self.rows += scan.rows;
        if let Some(min_ts) = scan.min_ts_ms {
            self.min_ts_ms = Some(self.min_ts_ms.map_or(min_ts, |cur| cur.min(min_ts)));
        }
        if let Some(max_ts) = scan.max_ts_ms {
            self.max_ts_ms = Some(self.max_ts_ms.map_or(max_ts, |cur| cur.max(max_ts)));
        }
    }
}

#[derive(Clone)]
struct CompactionJob {
    partition: PartitionKey,
    inputs: Vec<DatasetEntry>,
    output_rel: String,
    manifest_rel: String,
}

pub async fn inspect(storage: &StorageLocation, entries: &[DatasetEntry]) -> Result<()> {
    let workspace = tempfile::tempdir()?;
    let mut summary = BTreeMap::<String, PartitionSummary>::new();

    report("inspect", format!("scanning {} entries", entries.len()));

    for (index, entry) in entries.iter().enumerate() {
        report_step("inspect", index + 1, entries.len(), &entry.rel_path);
        let label = partition_label(entry);
        let bucket = summary.entry(label).or_insert_with(PartitionSummary::new);
        bucket.bytes += entry.size;

        match entry.kind {
            EntryKind::Parquet | EntryKind::CompactedParquet => {
                if let Some(path) = materialize_entry(storage, entry, workspace.path()).await? {
                    let scan = scan_parquet_file(&path, entry.partition.as_ref())?;
                    bucket.update_ts(&scan);
                }

                match entry.kind {
                    EntryKind::Parquet => bucket.parquet_files += 1,
                    EntryKind::CompactedParquet => bucket.compacted_files += 1,
                    _ => {}
                }
            }
            EntryKind::Manifest => bucket.manifests += 1,
            EntryKind::Temp => bucket.temp_files += 1,
            EntryKind::Other => bucket.other_files += 1,
        }
    }

    println!("Dataset: {}", storage.dataset_label());
    for (partition, bucket) in summary {
        let min_ts = bucket
            .min_ts_ms
            .and_then(|value| DateTime::<Utc>::from_timestamp_millis(value))
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string());
        let max_ts = bucket
            .max_ts_ms
            .and_then(|value| DateTime::<Utc>::from_timestamp_millis(value))
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string());

        println!(
            "{} | parquet={} compacted={} manifests={} temp={} other={} rows={} bytes={} min_ts={} max_ts={}",
            partition,
            bucket.parquet_files,
            bucket.compacted_files,
            bucket.manifests,
            bucket.temp_files,
            bucket.other_files,
            bucket.rows,
            bucket.bytes,
            min_ts,
            max_ts,
        );
    }

    report("inspect", "complete");

    Ok(())
}

pub async fn validate(storage: &StorageLocation, entries: &[DatasetEntry]) -> Result<()> {
    let workspace = tempfile::tempdir()?;
    let mut issues = Vec::new();

    report("validate", format!("scanning {} entries", entries.len()));

    for (index, entry) in entries.iter().enumerate() {
        report_step("validate", index + 1, entries.len(), &entry.rel_path);
        match entry.kind {
            EntryKind::Parquet | EntryKind::CompactedParquet => {
                let Some(path) = materialize_entry(storage, entry, workspace.path()).await? else {
                    issues.push(format!("{}: unable to materialize", entry.rel_path));
                    continue;
                };

                let scan = scan_parquet_file(&path, entry.partition.as_ref())?;
                for issue in scan.issues {
                    issues.push(format!("{}: {}", entry.rel_path, issue));
                }
            }
            EntryKind::Manifest => issues.push(format!("{}: manifest present", entry.rel_path)),
            EntryKind::Temp => issues.push(format!("{}: temporary file present", entry.rel_path)),
            EntryKind::Other => issues.push(format!("{}: unrecognized file", entry.rel_path)),
        }
    }

    if issues.is_empty() {
        println!("Validation passed");
        report("validate", "complete");
        Ok(())
    } else {
        for issue in &issues {
            eprintln!("✗ {}", issue);
        }
        report("validate", format!("failed with {} issue(s)", issues.len()));
        bail!("validation failed with {} issue(s)", issues.len())
    }
}

pub async fn compact(
    storage: &StorageLocation,
    entries: &[DatasetEntry],
    target_file_size_bytes: u64,
    apply: bool,
) -> Result<()> {
    let jobs = plan_compaction(entries, target_file_size_bytes);
    if jobs.is_empty() {
        println!("No compactable partitions found");
        report("compact", "no compactable partitions found");
        return Ok(());
    }

    report("compact", format!("planned {} job(s)", jobs.len()));

    if !apply {
        for (index, job) in jobs.iter().enumerate() {
            report_step(
                "compact",
                index + 1,
                jobs.len(),
                format!("would compact {} -> {}", job.partition.relative_dir(), job.output_rel),
            );
            println!(
                "Would compact {} file(s) in {} -> {}",
                job.inputs.len(),
                job.partition.relative_dir(),
                job.output_rel
            );
        }
        report("compact", "dry run complete");
        return Ok(());
    }

    let workspace = tempfile::tempdir()?;
    for (index, job) in jobs.iter().enumerate() {
        report_step(
            "compact",
            index + 1,
            jobs.len(),
            format!("compacting {} -> {}", job.partition.relative_dir(), job.output_rel),
        );
        compact_job(storage, job, workspace.path(), index + 1, jobs.len()).await?;
    }

    report("compact", "complete");

    Ok(())
}

pub async fn vacuum(storage: &StorageLocation, entries: &[DatasetEntry], apply: bool) -> Result<()> {
    let mut issues = Vec::new();
    let entry_map: BTreeMap<String, DatasetEntry> = entries
        .iter()
        .cloned()
        .map(|entry| (entry.rel_path.clone(), entry))
        .collect();

    report("vacuum", format!("scanning {} entries", entries.len()));

    for (index, entry) in entries.iter().enumerate() {
        report_step("vacuum", index + 1, entries.len(), &entry.rel_path);
        match entry.kind {
            EntryKind::Temp => {
                if apply {
                    report("vacuum", format!("deleting temp {}", entry.rel_path));
                    storage.delete_rel_path(&entry.rel_path).await?;
                } else {
                    issues.push(format!("would remove temp file {}", entry.rel_path));
                }
            }
            EntryKind::Manifest => {
                let manifest_bytes = storage.read_bytes(&entry.rel_path).await?;
                let manifest: CompactionManifest = serde_json::from_slice(&manifest_bytes)
                    .with_context(|| format!("parse manifest {}", entry.rel_path))?;
                manifest.validate()?;

                report(
                    "vacuum",
                    format!("checking manifest {} ({} input(s))", entry.rel_path, manifest.inputs.len()),
                );

                let output_entry = entry_map.get(&manifest.output);
                let output_exists = output_entry.is_some();
                if output_exists {
                    let partition = PartitionKey::parse(&format!("{}/dummy.parquet", manifest.partition))
                        .with_context(|| format!("parse manifest partition {}", manifest.partition))?;
                    let workspace = tempfile::tempdir()?;
                    let output_path = materialize_entry(
                        storage,
                        output_entry.expect("checked above"),
                        workspace.path(),
                    )
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("unable to materialize compacted output"))?;
                    let scan = scan_parquet_file(&output_path, Some(&partition))?;
                    if !scan.issues.is_empty() {
                        issues.push(format!(
                            "manifest {} output {} failed validation: {}",
                            entry.rel_path,
                            manifest.output,
                            scan.issues.join(", ")
                        ));
                        continue;
                    }

                    if apply {
                        report(
                            "vacuum",
                            format!(
                                "finalizing manifest {} and deleting {} input(s)",
                                entry.rel_path,
                                manifest.inputs.len()
                            ),
                        );
                        storage.delete_rel_paths(&manifest.inputs).await?;
                        storage.delete_rel_path(&entry.rel_path).await?;
                    } else {
                        issues.push(format!(
                            "would finalize manifest {} and delete {} input file(s)",
                            entry.rel_path,
                            manifest.inputs.len()
                        ));
                    }
                } else if apply {
                    report("vacuum", format!("removing stale manifest {}", entry.rel_path));
                    storage.delete_rel_path(&entry.rel_path).await?;
                } else {
                    issues.push(format!("would remove stale manifest {}", entry.rel_path));
                }
            }
            EntryKind::Parquet | EntryKind::CompactedParquet | EntryKind::Other => {
                if entry.size == 0 {
                    if apply {
                        report("vacuum", format!("deleting zero-byte file {}", entry.rel_path));
                        storage.delete_rel_path(&entry.rel_path).await?;
                    } else {
                        issues.push(format!("would remove zero-byte file {}", entry.rel_path));
                    }
                }
            }
        }
    }

    if apply {
        println!("Vacuum complete");
        report("vacuum", "complete");
    } else {
        for issue in &issues {
            println!("{}", issue);
        }
        report("vacuum", "dry run complete");
    }

    Ok(())
}

fn plan_compaction(entries: &[DatasetEntry], target_file_size_bytes: u64) -> Vec<CompactionJob> {
    let mut by_partition: BTreeMap<PartitionKey, Vec<DatasetEntry>> = BTreeMap::new();

    for entry in entries {
        if entry.kind != EntryKind::Parquet {
            continue;
        }
        let Some(partition) = entry.partition.clone() else {
            continue;
        };
        by_partition.entry(partition).or_default().push(entry.clone());
    }

    let mut jobs = Vec::new();

    for (partition, mut files) in by_partition {
        let has_non_parquet_artifacts = entries.iter().any(|entry| {
            entry.partition.as_ref() == Some(&partition)
                && matches!(entry.kind, EntryKind::Manifest | EntryKind::Temp | EntryKind::CompactedParquet | EntryKind::Other)
        });
        if has_non_parquet_artifacts || files.len() < 2 {
            continue;
        }

        files.sort_by(|left, right| {
            left.size
                .cmp(&right.size)
                .then(left.rel_path.cmp(&right.rel_path))
        });

        let mut current = Vec::new();
        let mut current_size = 0u64;
        let mut group_index = 0usize;

        for file in files {
            if current.is_empty() {
                current_size = file.size;
                current.push(file);
                continue;
            }

            if current_size.saturating_add(file.size) <= target_file_size_bytes {
                current_size = current_size.saturating_add(file.size);
                current.push(file);
                continue;
            }

            if current.len() > 1 {
                jobs.push(make_job(&partition, group_index, current.clone()));
                group_index += 1;
            }

            current = vec![file];
            current_size = current[0].size;
        }

        if current.len() > 1 {
            jobs.push(make_job(&partition, group_index, current));
        }
    }

    jobs
}

fn make_job(partition: &PartitionKey, group_index: usize, inputs: Vec<DatasetEntry>) -> CompactionJob {
    let output_rel = format!(
        "{}/{}",
        partition.relative_dir(),
        compaction_output_name(group_index)
    );

    CompactionJob {
        partition: partition.clone(),
        manifest_rel: manifest_path_for_output(&output_rel),
        output_rel,
        inputs,
    }
}

async fn compact_job(
    storage: &StorageLocation,
    job: &CompactionJob,
    workspace: &Path,
    job_index: usize,
    job_total: usize,
) -> Result<()> {
    let manifest = CompactionManifest::new(
        &job.partition,
        job.output_rel.clone(),
        job.inputs.iter().map(|entry| entry.rel_path.clone()).collect(),
    );

    report(
        "compact",
        format!(
            "{}/{} writing manifest {}",
            job_index, job_total, job.manifest_rel
        ),
    );
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    storage.write_bytes(&job.manifest_rel, &manifest_bytes).await?;

    let materialized = materialize_group(storage, &job.inputs, workspace, job_index, job_total).await?;
    let temp_output_path = storage.temp_output_path(workspace, &job.output_rel);
    if let Some(parent) = temp_output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    report(
        "compact",
        format!("{}/{} writing compacted output {}", job_index, job_total, job.output_rel),
    );
    write_compacted_output(&materialized, &temp_output_path)?;
    report(
        "compact",
        format!("{}/{} validating compacted output {}", job_index, job_total, job.output_rel),
    );
    let scan = scan_parquet_file(&temp_output_path, Some(&job.partition))?;
    if !scan.issues.is_empty() {
        bail!(
            "compacted output validation failed for {}: {}",
            job.output_rel,
            scan.issues.join(", ")
        );
    }

    report(
        "compact",
        format!(
            "{}/{} publishing {} and deleting {} source file(s)",
            job_index,
            job_total,
            job.output_rel,
            job.inputs.len()
        ),
    );
    storage.publish_file(&temp_output_path, &job.output_rel).await?;
    storage.delete_rel_paths(&job.inputs.iter().map(|entry| entry.rel_path.clone()).collect::<Vec<_>>()).await?;
    storage.delete_rel_path(&job.manifest_rel).await?;

    println!("Compacted {} file(s) into {}", job.inputs.len(), job.output_rel);
    report(
        "compact",
        format!("{}/{} complete", job_index, job_total),
    );

    Ok(())
}

async fn materialize_group(
    storage: &StorageLocation,
    inputs: &[DatasetEntry],
    workspace: &Path,
    job_index: usize,
    job_total: usize,
) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for (index, input) in inputs.iter().enumerate() {
        if should_report(index + 1, inputs.len(), SCAN_REPORT_INTERVAL) {
            report(
                "compact",
                format!(
                    "{}/{} downloading {}/{} {}",
                    job_index,
                    job_total,
                    index + 1,
                    inputs.len(),
                    input.rel_path
                ),
            );
        }
        if let Some(path) = materialize_entry(storage, input, workspace).await? {
            paths.push(path);
        }
    }
    Ok(paths)
}

async fn materialize_entry(
    storage: &StorageLocation,
    entry: &DatasetEntry,
    workspace: &Path,
) -> Result<Option<PathBuf>> {
    match entry.kind {
        EntryKind::Parquet | EntryKind::CompactedParquet => {
            if let Some(path) = storage.final_local_path(&entry.rel_path) {
                Ok(Some(path))
            } else {
                Ok(Some(storage.materialize(entry, workspace).await?))
            }
        }
        _ => Ok(None),
    }
}

fn write_compacted_output(input_paths: &[PathBuf], output_path: &Path) -> Result<()> {
    if input_paths.is_empty() {
        bail!("no input files to compact");
    }

    let zstd_level = ZstdLevel::try_new(5).unwrap_or_default();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd_level))
        .set_column_encoding(ColumnPath::from("ts"), Encoding::DELTA_BINARY_PACKED)
        .set_column_encoding(
            ColumnPath::from("payload"),
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        )
        .set_column_dictionary_enabled(ColumnPath::from("ts"), false)
        .set_column_dictionary_enabled(ColumnPath::from("payload"), false)
        .build();

    let mut output_file = Some(
        StdFile::create(output_path).with_context(|| format!("create {}", output_path.display()))?,
    );
    let mut writer: Option<ArrowWriter<StdFile>> = None;

    for input_path in input_paths {
        let input_file = StdFile::open(input_path)
            .with_context(|| format!("open {}", input_path.display()))?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(input_file)
            .with_context(|| format!("read parquet footer {}", input_path.display()))?
            .build()
            .with_context(|| format!("build parquet reader {}", input_path.display()))?;

        while let Some(batch) = reader.next().transpose()? {
            if writer.is_none() {
                let schema = batch.schema();
                let file = output_file
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("missing compacted output file handle"))?;
                writer = Some(
                    ArrowWriter::try_new(file, schema, Some(props.clone()))
                        .context("create compacted parquet writer")?,
                );
            }

            if let Some(writer) = writer.as_mut() {
                writer.write(&batch).context("write compacted batch")?;
            }
        }
    }

    let Some(writer) = writer else {
        drop(output_file);
        let _ = std::fs::remove_file(output_path);
        bail!("no batches read while compacting {}", output_path.display());
    };
    writer.close().context("close compacted parquet writer")?;

    Ok(())
}

fn scan_parquet_file(path: &Path, expected_partition: Option<&PartitionKey>) -> Result<FileScan> {
    let file = StdFile::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("read parquet footer {}", path.display()))?
        .build()
        .with_context(|| format!("build parquet reader {}", path.display()))?;

    let mut rows = 0usize;
    let mut min_ts_ms: Option<i64> = None;
    let mut max_ts_ms: Option<i64> = None;
    let mut issues = Vec::new();
    let mut saw_any_batch = false;

    while let Some(batch) = reader.next().transpose()? {
        saw_any_batch = true;
        if batch.num_columns() < 2 {
            issues.push(format!("unexpected column count: {}", batch.num_columns()));
            continue;
        }
        if batch.num_columns() != 2 {
            issues.push(format!("unexpected column count: {}", batch.num_columns()));
        }

        let schema = batch.schema();
        if schema.fields().len() >= 2 {
            let ts_field = schema.field(0);
            let payload_field = schema.field(1);
            if ts_field.name() != "ts"
                || payload_field.name() != "payload"
                || !matches!(ts_field.data_type(), DataType::Timestamp(TimeUnit::Millisecond, Some(_)))
                || !matches!(payload_field.data_type(), DataType::Utf8)
            {
                issues.push("unexpected schema".to_string());
            }
        }

        let Some(ts_column) = batch.column(0).as_any().downcast_ref::<TimestampMillisecondArray>() else {
            issues.push("missing ts column".to_string());
            continue;
        };
        let Some(_payload_column) = batch.column(1).as_any().downcast_ref::<StringArray>() else {
            issues.push("missing payload column".to_string());
            continue;
        };

        for row in 0..batch.num_rows() {
            let ts_ms = ts_column.value(row);
            rows += 1;
            min_ts_ms = Some(match min_ts_ms {
                Some(current) => current.min(ts_ms),
                None => ts_ms,
            });
            max_ts_ms = Some(match max_ts_ms {
                Some(current) => current.max(ts_ms),
                None => ts_ms,
            });

            if let Some(partition) = expected_partition {
                if !partition.matches_timestamp_ms(ts_ms) {
                    issues.push(format!(
                        "timestamp {} does not match partition {}",
                        ts_ms,
                        partition.relative_dir()
                    ));
                    break;
                }
            }
        }
    }

    if !saw_any_batch {
        issues.push("file contains no rows".to_string());
    }

    Ok(FileScan {
        rows,
        min_ts_ms,
        max_ts_ms,
        issues,
    })
}

fn partition_label(entry: &DatasetEntry) -> String {
    entry
        .partition
        .as_ref()
        .map(|partition| partition.relative_dir())
        .unwrap_or_else(|| format!("unpartitioned:{}", entry.rel_path))
}
