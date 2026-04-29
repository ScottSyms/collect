use crate::partition::{
    compaction_output_name, manifest_path_for_output, CompactionManifest, EntryKind, PartitionKey,
};
use crate::progress::{count, decimal, report, report_step, should_report, SCAN_REPORT_INTERVAL};
use crate::storage::{DatasetEntry, StorageLocation};
use anyhow::{bail, Context, Result};
use arrow::array::{Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Utc};
use collect_core::PartitionGranularity;
use futures_util::stream::{self, StreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File as StdFile;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const SMALL_PARQUET_FILE_BYTES: u64 = 256 * 1024 * 1024;

struct FileScan {
    rows: usize,
    min_ts_ms: Option<i64>,
    max_ts_ms: Option<i64>,
    issues: Vec<String>,
}

impl FileScan {
    fn empty() -> Self {
        Self {
            rows: 0,
            min_ts_ms: None,
            max_ts_ms: None,
            issues: Vec::new(),
        }
    }
}

struct PartitionSummary {
    parquet_files: usize,
    compacted_files: usize,
    small_parquet_files: usize,
    manifests: usize,
    temp_files: usize,
    other_files: usize,
    scan_issues: usize,
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
            small_parquet_files: 0,
            manifests: 0,
            temp_files: 0,
            other_files: 0,
            scan_issues: 0,
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

pub async fn inspect(
    storage: &StorageLocation,
    entries: &[DatasetEntry],
    concurrency: usize,
    verbose: bool,
) -> Result<()> {
    let workspace = tempfile::tempdir()?;
    let total = entries.len();
    let mut summary = BTreeMap::<String, PartitionSummary>::new();
    let mut completed = 0usize;

    report(
        "inspect",
        format!(
            "scanning {} entries with {} workers",
            count(total),
            count(concurrency.max(1))
        ),
    );

    let mut stream = stream::iter(entries.iter().cloned())
        .enumerate()
        .map(|(index, entry)| {
            let workspace = workspace.path().to_path_buf();
            async move {
                let scan = inspect_entry(storage, &entry, &workspace).await?;
                Ok::<_, anyhow::Error>((index, entry, scan))
            }
        })
        .buffer_unordered(concurrency.max(1));

    while let Some(result) = stream.next().await {
        let (_index, entry, scan) = result?;
        completed += 1;
        report_step("inspect", completed, total, &entry.rel_path);

        let label = partition_label(&entry);
        let bucket = summary.entry(label).or_insert_with(PartitionSummary::new);
        bucket.bytes += entry.size;

        match entry.kind {
            EntryKind::Parquet => bucket.parquet_files += 1,
            EntryKind::CompactedParquet => bucket.compacted_files += 1,
            EntryKind::Manifest => bucket.manifests += 1,
            EntryKind::Temp => bucket.temp_files += 1,
            EntryKind::Other => bucket.other_files += 1,
        }

        if matches!(entry.kind, EntryKind::Parquet | EntryKind::CompactedParquet) {
            bucket.update_ts(&scan);
            bucket.scan_issues += scan.issues.len();
        }

        if entry.kind == EntryKind::Parquet && entry.size < SMALL_PARQUET_FILE_BYTES {
            bucket.small_parquet_files += 1;
        }
    }

    print_inspection_report(&storage.dataset_label(), &summary, verbose);

    report("inspect", "complete");

    Ok(())
}

fn print_inspection_report(
    dataset_label: &str,
    summary: &BTreeMap<String, PartitionSummary>,
    verbose: bool,
) {
    let mut partitions = 0usize;
    let mut parquet_files = 0usize;
    let mut compacted_files = 0usize;
    let mut manifests = 0usize;
    let mut temp_files = 0usize;
    let mut other_files = 0usize;
    let mut scan_issues = 0usize;
    let mut bytes = 0u64;
    let mut rows = 0usize;
    let mut min_ts_ms: Option<i64> = None;
    let mut max_ts_ms: Option<i64> = None;
    let mut compact_candidates = 0usize;
    let mut compact_majority_small = 0usize;
    let mut vacuum_candidates = 0usize;
    let mut validate_candidates = 0usize;
    let mut verbose_lines = Vec::new();

    for (partition, bucket) in summary {
        partitions += 1;
        parquet_files += bucket.parquet_files;
        compacted_files += bucket.compacted_files;
        manifests += bucket.manifests;
        temp_files += bucket.temp_files;
        other_files += bucket.other_files;
        scan_issues += bucket.scan_issues;
        bytes += bucket.bytes;
        rows += bucket.rows;
        if let Some(value) = bucket.min_ts_ms {
            min_ts_ms = Some(min_ts_ms.map_or(value, |current| current.min(value)));
        }
        if let Some(value) = bucket.max_ts_ms {
            max_ts_ms = Some(max_ts_ms.map_or(value, |current| current.max(value)));
        }

        let compact_needed = can_compact(bucket);
        let vacuum_needed = bucket.manifests > 0 || bucket.temp_files > 0;
        let validate_needed = bucket.other_files > 0 || bucket.scan_issues > 0;

        if compact_needed {
            compact_candidates += 1;
            if bucket.small_parquet_files * 2 > bucket.parquet_files {
                compact_majority_small += 1;
            }
        }
        if vacuum_needed {
            vacuum_candidates += 1;
        }
        if validate_needed {
            validate_candidates += 1;
        }

        if verbose {
            verbose_lines.push(format_partition_status(partition, bucket));
        }
    }

    println!("Dataset: {}", dataset_label);
    println!(
        "Summary: {} partition(s), {} file(s), {} rows, size {}",
        count(partitions),
        count(parquet_files + compacted_files + manifests + temp_files + other_files),
        count(rows),
        human_bytes(bytes),
    );
    println!(
        "Files: parquet={} compacted={} manifests={} temp={} other={}",
        count(parquet_files),
        count(compacted_files),
        count(manifests),
        count(temp_files),
        count(other_files)
    );

    let min_ts = min_ts_ms
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| "-".to_string());
    let max_ts = max_ts_ms
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| "-".to_string());
    println!("Time range: {} -> {}", min_ts, max_ts);

    if compact_candidates == 0 && vacuum_candidates == 0 && validate_candidates == 0 {
        println!("No maintenance needed.");
    } else {
        println!("Recommendations:");
        if compact_candidates > 0 {
            let majority = if compact_majority_small > 0 {
                format!(
                    "; {} of those partitions ({}%) are dominated by files under 256 MiB",
                    count(compact_majority_small),
                    count(percent(compact_majority_small, compact_candidates))
                )
            } else {
                String::new()
            };
            println!(
                "- compact: {} partition(s) have more than one parquet file{}",
                count(compact_candidates),
                majority
            );
        }
        if vacuum_candidates > 0 {
            println!(
                "- vacuum: {} partition(s) have temp or manifest files",
                count(vacuum_candidates)
            );
        }
        if validate_candidates > 0 {
            println!(
                "- validate: {} partition(s) have scan issues or unrecognized files",
                count(validate_candidates)
            );
        }
        if scan_issues > 0 {
            println!("  scan issues detected: {}", count(scan_issues));
        }
        if !verbose {
            println!("Use --verbose for per-partition detail.");
        }
    }

    if verbose {
        println!("Partitions:");
        for line in verbose_lines {
            println!("{}", line);
        }
    }
}

pub async fn validate(
    storage: &StorageLocation,
    entries: &[DatasetEntry],
    concurrency: usize,
) -> Result<()> {
    let workspace = tempfile::tempdir()?;
    let total = entries.len();
    let mut completed = 0usize;
    let mut results = Vec::with_capacity(entries.len());

    report(
        "validate",
        format!(
            "scanning {} entries with {} workers",
            count(total),
            count(concurrency.max(1))
        ),
    );

    let mut stream = stream::iter(entries.iter().cloned())
        .enumerate()
        .map(|(index, entry)| {
            let workspace = workspace.path().to_path_buf();
            async move {
                let issues = validate_entry(storage, &entry, &workspace).await?;
                Ok::<_, anyhow::Error>((index, entry.rel_path, issues))
            }
        })
        .buffer_unordered(concurrency.max(1));

    while let Some(result) = stream.next().await {
        let (index, rel_path, issues) = result?;
        completed += 1;
        report_step("validate", completed, total, &rel_path);
        results.push((index, rel_path, issues));
    }

    results.sort_by_key(|(index, _, _)| *index);

    let mut issues = Vec::new();
    for (_, rel_path, entry_issues) in results {
        for issue in entry_issues {
            issues.push(format!("{}: {}", rel_path, issue));
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
        report(
            "validate",
            format!("failed with {} issue(s)", count(issues.len())),
        );
        bail!("validation failed with {} issue(s)", count(issues.len()))
    }
}

pub async fn compact(
    storage: &StorageLocation,
    entries: &[DatasetEntry],
    target_file_size_bytes: u64,
    apply: bool,
    concurrency: usize,
    compression_level: i32,
) -> Result<()> {
    report(
        "compact",
        format!("planning compaction for {} entries", count(entries.len())),
    );
    let jobs = plan_compaction(entries, target_file_size_bytes);
    if jobs.is_empty() {
        println!("No compactable partitions found");
        report("compact", "no compactable partitions found");
        return Ok(());
    }

    report(
        "compact",
        format!(
            "planned {} job(s) with {} workers",
            count(jobs.len()),
            count(concurrency.max(1))
        ),
    );

    if apply {
        report("compact", "applying changes (--apply)");
    } else {
        report("compact", "dry run only; re-run with --apply to execute");
    }

    if !apply {
        for (index, job) in jobs.iter().enumerate() {
            report_step(
                "compact",
                index + 1,
                jobs.len(),
                format!(
                    "would compact {} -> {}",
                    job.partition.relative_dir(),
                    job.output_rel
                ),
            );
            println!(
                "Would compact {} file(s) in {} -> {}",
                count(job.inputs.len()),
                job.partition.relative_dir(),
                job.output_rel
            );
        }
        report("compact", "dry run complete");
        return Ok(());
    }

    let workspace = tempfile::tempdir()?;
    let total = jobs.len();
    let mut completed = 0usize;
    let partition_groups = group_compaction_jobs_by_partition(jobs);
    let mut stream = stream::iter(partition_groups.into_iter())
        .map(|(partition, jobs)| {
            let workspace = workspace.path().to_path_buf();
            async move {
                compact_partition_jobs(
                    storage,
                    partition,
                    jobs,
                    &workspace,
                    concurrency,
                    compression_level,
                )
                .await
            }
        })
        .buffer_unordered(concurrency.max(1));

    while let Some(result) = stream.next().await {
        let outputs = result?;
        for output_rel in outputs {
            completed += 1;
            report_step("compact", completed, total, &output_rel);
        }
    }

    report("compact", "complete");

    Ok(())
}

async fn compact_partition_jobs(
    storage: &StorageLocation,
    partition: PartitionKey,
    jobs: Vec<CompactionJob>,
    workspace: &Path,
    concurrency: usize,
    compression_level: i32,
) -> Result<Vec<String>> {
    let partition_jobs = jobs.len();
    let mut outputs = Vec::with_capacity(jobs.len());

    for (index, job) in jobs.into_iter().enumerate() {
        let output_rel = job.output_rel.clone();
        compact_job(
            storage,
            &job,
            workspace,
            index + 1,
            partition_jobs,
            concurrency,
            compression_level,
        )
        .await?;
        outputs.push(output_rel);
    }

    report(
        "compact",
        format!("partition {} complete", partition.relative_dir()),
    );

    Ok(outputs)
}

fn group_compaction_jobs_by_partition(
    jobs: Vec<CompactionJob>,
) -> Vec<(PartitionKey, Vec<CompactionJob>)> {
    let mut grouped: BTreeMap<PartitionKey, Vec<CompactionJob>> = BTreeMap::new();

    for job in jobs {
        grouped
            .entry(job.partition.clone())
            .or_default()
            .push(job);
    }

    grouped.into_iter().collect()
}

pub async fn vacuum(
    storage: &StorageLocation,
    entries: &[DatasetEntry],
    partition: PartitionGranularity,
    apply: bool,
    concurrency: usize,
) -> Result<()> {
    let entry_map: BTreeMap<String, DatasetEntry> = entries
        .iter()
        .cloned()
        .map(|entry| (entry.rel_path.clone(), entry))
        .collect();

    report(
        "vacuum",
        format!(
            "scanning {} entries with {} workers",
            count(entries.len()),
            count(concurrency.max(1))
        ),
    );

    if apply {
        report("vacuum", "applying changes (--apply)");
    } else {
        report("vacuum", "dry run only; re-run with --apply to execute");
    }

    let total = entries.len();
    let mut completed = 0usize;
    let mut results = Vec::with_capacity(entries.len());
    let mut stream = stream::iter(entries.iter().cloned())
        .enumerate()
        .map(|(index, entry)| {
            let entry_map = &entry_map;
            async move {
                let issues =
                    vacuum_entry(storage, &entry, entry_map, partition, apply, concurrency).await?;
                Ok::<_, anyhow::Error>((index, entry.rel_path, issues))
            }
        })
        .buffer_unordered(concurrency.max(1));

    while let Some(result) = stream.next().await {
        let (index, rel_path, issues) = result?;
        completed += 1;
        report_step("vacuum", completed, total, &rel_path);
        results.push((index, rel_path, issues));
    }

    results.sort_by_key(|(index, _, _)| *index);

    if apply {
        println!("Vacuum complete");
        report("vacuum", "complete");
    } else {
        for (_, _, entry_issues) in results {
            for issue in entry_issues {
                println!("{}", issue);
            }
        }
        report("vacuum", "dry run complete");
    }

    Ok(())
}

fn plan_compaction(entries: &[DatasetEntry], target_file_size_bytes: u64) -> Vec<CompactionJob> {
    let mut by_partition: BTreeMap<PartitionKey, Vec<DatasetEntry>> = BTreeMap::new();
    let mut blocked_partitions: BTreeSet<PartitionKey> = BTreeSet::new();
    let total_entries = entries.len();
    let heartbeat_interval = Duration::from_secs(5);
    let mut last_report = Instant::now();

    for (index, entry) in entries.iter().enumerate() {
        if entry.kind != EntryKind::Parquet {
            if let Some(partition) = entry.partition.clone() {
                blocked_partitions.insert(partition);
            }
            if index == 0
                || index + 1 == total_entries
                || last_report.elapsed() >= heartbeat_interval
            {
                report(
                    "compact",
                    format!(
                        "planning compaction: indexed {} / {} entries across {} partition(s)",
                        count(index + 1),
                        count(total_entries),
                        count(by_partition.len())
                    ),
                );
                last_report = Instant::now();
            }
            continue;
        }
        let Some(partition) = entry.partition.clone() else {
            if index == 0
                || index + 1 == total_entries
                || last_report.elapsed() >= heartbeat_interval
            {
                report(
                    "compact",
                    format!(
                        "planning compaction: indexed {} / {} entries across {} partition(s)",
                        count(index + 1),
                        count(total_entries),
                        count(by_partition.len())
                    ),
                );
                last_report = Instant::now();
            }
            continue;
        };
        by_partition
            .entry(partition)
            .or_default()
            .push(entry.clone());

        if index == 0 || index + 1 == total_entries || last_report.elapsed() >= heartbeat_interval {
            report(
                "compact",
                format!(
                    "planning compaction: indexed {} / {} entries across {} partition(s)",
                    count(index + 1),
                    count(total_entries),
                    count(by_partition.len())
                ),
            );
            last_report = Instant::now();
        }
    }

    report(
        "compact",
        format!(
            "planning compaction: partition index ready with {} partition(s)",
            count(by_partition.len())
        ),
    );

    let mut jobs = Vec::new();
    let total_partitions = by_partition.len();
    let mut partition_report = Instant::now();

    for (partition_index, (partition, mut files)) in by_partition.into_iter().enumerate() {
        if partition_index == 0
            || partition_index + 1 == total_partitions
            || partition_report.elapsed() >= heartbeat_interval
        {
            report(
                "compact",
                format!(
                    "planning compaction: evaluated {} / {} partition(s), planned {} job(s)",
                    count(partition_index + 1),
                    count(total_partitions),
                    count(jobs.len())
                ),
            );
            partition_report = Instant::now();
        }

        if blocked_partitions.contains(&partition) || files.len() < 2 {
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

fn make_job(
    partition: &PartitionKey,
    group_index: usize,
    inputs: Vec<DatasetEntry>,
) -> CompactionJob {
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
    concurrency: usize,
    compression_level: i32,
) -> Result<()> {
    let manifest = CompactionManifest::new(
        &job.partition,
        job.output_rel.clone(),
        job.inputs
            .iter()
            .map(|entry| entry.rel_path.clone())
            .collect(),
    );

    report(
        "compact",
        format!(
            "{}/{} writing manifest {}",
            count(job_index),
            count(job_total),
            job.manifest_rel
        ),
    );
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    storage
        .write_bytes(&job.manifest_rel, &manifest_bytes)
        .await?;

    let materialized = materialize_group(
        storage,
        &job.inputs,
        workspace,
        job_index,
        job_total,
        concurrency,
    )
    .await?;
    let temp_output_path = storage.temp_output_path(workspace, &job.output_rel);
    if let Some(parent) = temp_output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    report(
        "compact",
        format!(
            "{}/{} writing compacted output {}",
            count(job_index),
            count(job_total),
            job.output_rel
        ),
    );
    write_compacted_output(&materialized, &temp_output_path, compression_level)?;
    report(
        "compact",
        format!(
            "{}/{} validating compacted output {}",
            count(job_index),
            count(job_total),
            job.output_rel
        ),
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
            count(job_index),
            count(job_total),
            job.output_rel,
            count(job.inputs.len())
        ),
    );
    storage
        .publish_file(&temp_output_path, &job.output_rel)
        .await?;
    let input_paths = job
        .inputs
        .iter()
        .map(|entry| entry.rel_path.clone())
        .collect::<Vec<_>>();
    storage.delete_rel_paths(&input_paths, concurrency).await?;
    storage.delete_rel_path(&job.manifest_rel).await?;

    println!(
        "Compacted {} file(s) into {}",
        count(job.inputs.len()),
        job.output_rel
    );
    report(
        "compact",
        format!("{}/{} complete", count(job_index), count(job_total)),
    );

    Ok(())
}

async fn materialize_group(
    storage: &StorageLocation,
    inputs: &[DatasetEntry],
    workspace: &Path,
    job_index: usize,
    job_total: usize,
    concurrency: usize,
) -> Result<Vec<PathBuf>> {
    let total = inputs.len();
    let mut materialized = Vec::with_capacity(total);
    let mut stream = stream::iter(inputs.iter().cloned())
        .enumerate()
        .map(|(index, input)| {
            let workspace = workspace.to_path_buf();
            async move {
                if should_report(index + 1, total, SCAN_REPORT_INTERVAL) {
                    report(
                        "compact",
                        format!(
                            "{}/{} downloading {}/{} {}",
                            count(job_index),
                            count(job_total),
                            count(index + 1),
                            count(total),
                            input.rel_path
                        ),
                    );
                }

                let path = materialize_entry(storage, &input, &workspace)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("unable to materialize {}", input.rel_path))?;
                Ok::<_, anyhow::Error>((index, path))
            }
        })
        .buffer_unordered(concurrency.max(1));

    while let Some(result) = stream.next().await {
        materialized.push(result?);
    }

    materialized.sort_by_key(|(index, _)| *index);
    Ok(materialized.into_iter().map(|(_, path)| path).collect())
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

fn write_compacted_output(
    input_paths: &[PathBuf],
    output_path: &Path,
    compression_level: i32,
) -> Result<()> {
    if input_paths.is_empty() {
        bail!("no input files to compact");
    }

    let zstd_level = ZstdLevel::try_new(compression_level)
        .context("invalid Zstd compression level")?;
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
        StdFile::create(output_path)
            .with_context(|| format!("create {}", output_path.display()))?,
    );
    let mut writer: Option<ArrowWriter<StdFile>> = None;

    for input_path in input_paths {
        let input_file =
            StdFile::open(input_path).with_context(|| format!("open {}", input_path.display()))?;
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
            issues.push(format!(
                "unexpected column count: {}",
                count(batch.num_columns())
            ));
            continue;
        }
        if batch.num_columns() != 2 {
            issues.push(format!(
                "unexpected column count: {}",
                count(batch.num_columns())
            ));
        }

        let schema = batch.schema();
        if schema.fields().len() >= 2 {
            let ts_field = schema.field(0);
            let payload_field = schema.field(1);
            if ts_field.name() != "ts"
                || payload_field.name() != "payload"
                || !matches!(
                    ts_field.data_type(),
                    DataType::Timestamp(TimeUnit::Millisecond, Some(_))
                )
                || !matches!(payload_field.data_type(), DataType::Utf8)
            {
                issues.push("unexpected schema".to_string());
            }
        }

        let Some(ts_column) = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
        else {
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
                        count(ts_ms),
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

async fn inspect_entry(
    storage: &StorageLocation,
    entry: &DatasetEntry,
    workspace: &Path,
) -> Result<FileScan> {
    match entry.kind {
        EntryKind::Parquet | EntryKind::CompactedParquet => {
            let Some(path) = materialize_entry(storage, entry, workspace).await? else {
                return Ok(FileScan::empty());
            };

            let partition = entry.partition.clone();
            Ok(
                tokio::task::spawn_blocking(move || scan_parquet_file(&path, partition.as_ref()))
                    .await??,
            )
        }
        _ => Ok(FileScan::empty()),
    }
}

async fn validate_entry(
    storage: &StorageLocation,
    entry: &DatasetEntry,
    workspace: &Path,
) -> Result<Vec<String>> {
    match entry.kind {
        EntryKind::Parquet | EntryKind::CompactedParquet => {
            let Some(path) = materialize_entry(storage, entry, workspace).await? else {
                return Ok(vec!["unable to materialize".to_string()]);
            };

            let partition = entry.partition.clone();
            let scan =
                tokio::task::spawn_blocking(move || scan_parquet_file(&path, partition.as_ref()))
                    .await??;
            Ok(scan.issues)
        }
        EntryKind::Manifest => Ok(vec!["manifest present".to_string()]),
        EntryKind::Temp => Ok(vec!["temporary file present".to_string()]),
        EntryKind::Other => Ok(vec!["unrecognized file".to_string()]),
    }
}

async fn vacuum_entry(
    storage: &StorageLocation,
    entry: &DatasetEntry,
    entry_map: &BTreeMap<String, DatasetEntry>,
    partition: PartitionGranularity,
    apply: bool,
    concurrency: usize,
) -> Result<Vec<String>> {
    let mut issues = Vec::new();

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
                format!(
                    "checking manifest {} ({} input(s))",
                    entry.rel_path,
                    count(manifest.inputs.len())
                ),
            );

            let output_entry = entry_map.get(&manifest.output);
            if let Some(output_entry) = output_entry {
                let partition = PartitionKey::parse(
                    &format!("{}/dummy.parquet", manifest.partition),
                    partition,
                )
                .with_context(|| format!("parse manifest partition {}", manifest.partition))?;
                let workspace = tempfile::tempdir()?;
                let output_path = materialize_entry(storage, output_entry, workspace.path())
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("unable to materialize compacted output"))?;
                let partition = partition;
                let scan = tokio::task::spawn_blocking(move || {
                    scan_parquet_file(&output_path, Some(&partition))
                })
                .await??;

                if !scan.issues.is_empty() {
                    issues.push(format!(
                        "manifest {} output {} failed validation: {}",
                        entry.rel_path,
                        manifest.output,
                        scan.issues.join(", ")
                    ));
                } else if apply {
                    report(
                        "vacuum",
                        format!(
                            "finalizing manifest {} and deleting {} input(s)",
                            entry.rel_path,
                            count(manifest.inputs.len())
                        ),
                    );
                    storage
                        .delete_rel_paths(&manifest.inputs, concurrency)
                        .await?;
                    storage.delete_rel_path(&entry.rel_path).await?;
                } else {
                    issues.push(format!(
                        "would finalize manifest {} and delete {} input file(s)",
                        entry.rel_path,
                        count(manifest.inputs.len())
                    ));
                }
            } else if apply {
                report(
                    "vacuum",
                    format!("removing stale manifest {}", entry.rel_path),
                );
                storage.delete_rel_path(&entry.rel_path).await?;
            } else {
                issues.push(format!("would remove stale manifest {}", entry.rel_path));
            }
        }
        EntryKind::Parquet | EntryKind::CompactedParquet | EntryKind::Other => {
            if entry.size == 0 {
                if apply {
                    report(
                        "vacuum",
                        format!("deleting zero-byte file {}", entry.rel_path),
                    );
                    storage.delete_rel_path(&entry.rel_path).await?;
                } else {
                    issues.push(format!("would remove zero-byte file {}", entry.rel_path));
                }
            }
        }
    }

    Ok(issues)
}

fn partition_label(entry: &DatasetEntry) -> String {
    entry
        .partition
        .as_ref()
        .map(|partition| partition.relative_dir())
        .unwrap_or_else(|| format!("unpartitioned:{}", entry.rel_path))
}

fn format_partition_status(partition: &str, bucket: &PartitionSummary) -> String {
    let mut details = vec![
        format!("parquet={}", count(bucket.parquet_files)),
        format!("compacted={}", count(bucket.compacted_files)),
        format!("manifests={}", count(bucket.manifests)),
        format!("temp={}", count(bucket.temp_files)),
        format!("other={}", count(bucket.other_files)),
        format!("rows={}", count(bucket.rows)),
        format!("bytes={}", human_bytes(bucket.bytes)),
    ];

    if bucket.parquet_files > 0 && bucket.small_parquet_files > 0 {
        details.push(format!(
            "small_parquet={}/{} ({}%)",
            count(bucket.small_parquet_files),
            count(bucket.parquet_files),
            count(percent(bucket.small_parquet_files, bucket.parquet_files)),
        ));
    }

    if bucket.scan_issues > 0 {
        details.push(format!("scan_issues={}", count(bucket.scan_issues)));
    }

    let mut actions = Vec::new();
    if can_compact(bucket) {
        actions.push("compact");
    }
    if bucket.manifests > 0 || bucket.temp_files > 0 {
        actions.push("vacuum");
    }
    if bucket.other_files > 0 || bucket.scan_issues > 0 {
        actions.push("validate");
    }

    if !actions.is_empty() {
        details.push(format!("actions={}", actions.join(",")));
    }

    format!("{} | {}", partition, details.join(" "))
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;

    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{} {}", count(bytes), UNITS[unit])
    } else {
        format!("{} {}", decimal(value, 1), UNITS[unit])
    }
}

fn percent(numerator: usize, denominator: usize) -> usize {
    if denominator == 0 {
        0
    } else {
        (numerator * 100) / denominator
    }
}

fn can_compact(bucket: &PartitionSummary) -> bool {
    bucket.parquet_files > 1
        && bucket.compacted_files == 0
        && bucket.manifests == 0
        && bucket.temp_files == 0
        && bucket.other_files == 0
}
