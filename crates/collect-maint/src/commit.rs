//! Hand-built Iceberg `replace` commit.
//!
//! iceberg-rust 0.9.1 exposes no rewrite/overwrite action and keeps
//! `TableCommit`'s builder crate-private, so this module writes the manifests
//! and manifest list with the crate's public spec writers and posts the
//! resulting `add-snapshot` / `set-snapshot-ref` updates straight to the REST
//! catalog (through the same SigV4 proxy `open_catalog` uses).

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, bail, Context, Result};
use collect_core::iceberg::{sigv4, IcebergConfig};
use iceberg::spec::{
    DataFile, FormatVersion, ManifestContentType, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder,
    Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::{TableIdent, TableRequirement, TableUpdate};

const MAIN_BRANCH: &str = "main";

/// What a replace commit did, for reporting.
#[derive(Debug, Clone, Copy)]
pub struct ReplaceOutcome {
    pub snapshot_id: i64,
    pub removed_files: usize,
    pub added_files: usize,
    pub removed_records: u64,
    pub added_records: u64,
}

/// A commit ready to post: the requirements pin the branch to the snapshot the
/// rewrite was planned against, so a concurrent commit makes the POST fail
/// with 409 and the caller re-plans on a fresh table.
pub struct PreparedReplace {
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
    pub outcome: ReplaceOutcome,
}

/// Writes the new manifests and manifest list for a snapshot that removes the
/// data files whose paths are in `remove` and adds `add`, and returns the
/// catalog updates that would publish it.
///
/// Fails if any path in `remove` is not live in the table's current snapshot
/// (someone else already rewrote it), which is the signal to re-plan.
pub async fn prepare_replace(
    table: &Table,
    remove: &HashSet<String>,
    add: Vec<DataFile>,
) -> Result<PreparedReplace> {
    let metadata = table.metadata();
    if metadata.format_version() != FormatVersion::V2 {
        bail!(
            "replace commits support Iceberg format v2 only, table is {:?}",
            metadata.format_version()
        );
    }
    let parent = metadata
        .current_snapshot()
        .ok_or_else(|| anyhow!("table has no current snapshot to replace files in"))?;
    let file_io = table.file_io();
    let snapshot_id = new_snapshot_id(table);
    let commit_uuid = uuid::Uuid::new_v4();
    let meta_dir = format!("{}/metadata", metadata.location());
    let mut manifest_no = 0u32;
    let mut next_manifest_path = || {
        let p = format!("{meta_dir}/{commit_uuid}-m{manifest_no}.avro");
        manifest_no += 1;
        p
    };

    let list = parent
        .load_manifest_list(file_io, &metadata.clone())
        .await
        .context("loading current manifest list")?;

    let mut manifests = Vec::new();
    let mut removed_seen: HashSet<String> = HashSet::new();
    let (mut removed_records, mut removed_bytes) = (0u64, 0u64);

    for mf in list.entries() {
        if mf.content != ManifestContentType::Data {
            manifests.push(mf.clone());
            continue;
        }
        let manifest = mf.load_manifest(file_io).await?;
        let touched = manifest
            .entries()
            .iter()
            .any(|e| e.is_alive() && remove.contains(e.file_path()));
        if !touched {
            manifests.push(mf.clone());
            continue;
        }

        // Rewrite this manifest: survivors become `existing`, removals `deleted`.
        let spec = metadata
            .partition_spec_by_id(mf.partition_spec_id)
            .ok_or_else(|| anyhow!("unknown partition spec {}", mf.partition_spec_id))?;
        let mut writer = ManifestWriterBuilder::new(
            file_io.new_output(next_manifest_path())?,
            Some(snapshot_id),
            None,
            metadata.current_schema().clone(),
            spec.as_ref().clone(),
        )
        .build_v2_data();
        for e in manifest.entries() {
            if !e.is_alive() {
                continue;
            }
            let seq_no = e
                .sequence_number()
                .ok_or_else(|| anyhow!("manifest entry without a sequence number"))?;
            let file_seq = e.file_sequence_number;
            if remove.contains(e.file_path()) {
                removed_seen.insert(e.file_path().to_string());
                removed_records += e.record_count();
                removed_bytes += e.file_size_in_bytes();
                writer.add_delete_file(e.data_file().clone(), seq_no, file_seq)?;
            } else {
                let snap = e
                    .snapshot_id()
                    .ok_or_else(|| anyhow!("manifest entry without a snapshot id"))?;
                writer.add_existing_file(e.data_file().clone(), snap, seq_no, file_seq)?;
            }
        }
        manifests.push(writer.write_manifest_file().await?);
    }

    if removed_seen.len() != remove.len() {
        let missing: Vec<_> = remove.difference(&removed_seen).take(3).collect();
        bail!(
            "{} of {} files to replace are no longer live (e.g. {:?}); table changed under the plan",
            remove.len() - removed_seen.len(),
            remove.len(),
            missing
        );
    }

    let added_records: u64 = add.iter().map(|f| f.record_count()).sum();
    let added_bytes: u64 = add.iter().map(|f| f.file_size_in_bytes()).sum();
    let added_files = add.len();
    if !add.is_empty() {
        let spec = metadata.default_partition_spec();
        let mut writer = ManifestWriterBuilder::new(
            file_io.new_output(next_manifest_path())?,
            Some(snapshot_id),
            None,
            metadata.current_schema().clone(),
            spec.as_ref().clone(),
        )
        .build_v2_data();
        for f in add {
            // -1: sequence number is inherited from the snapshot at commit.
            writer.add_file(f, -1)?;
        }
        manifests.push(writer.write_manifest_file().await?);
    }

    let summary = summary(
        parent.summary(),
        remove.len(),
        added_files,
        removed_records,
        added_records,
        removed_bytes,
        added_bytes,
    );
    let (requirements, updates) =
        seal(table, snapshot_id, commit_uuid, manifests, summary).await?;
    Ok(PreparedReplace {
        requirements,
        updates,
        outcome: ReplaceOutcome {
            snapshot_id,
            removed_files: remove.len(),
            added_files,
            removed_records,
            added_records,
        },
    })
}

/// Writes the manifest list for `manifests` and returns the catalog updates
/// (plus branch-pinning requirements) that publish it as a new snapshot on
/// top of the table's current one.
async fn seal(
    table: &Table,
    snapshot_id: i64,
    commit_uuid: uuid::Uuid,
    manifests: Vec<ManifestFile>,
    summary: Summary,
) -> Result<(Vec<TableRequirement>, Vec<TableUpdate>)> {
    let metadata = table.metadata();
    let parent = metadata
        .current_snapshot()
        .ok_or_else(|| anyhow!("table has no current snapshot"))?;
    let list_path = format!(
        "{}/metadata/snap-{snapshot_id}-0-{commit_uuid}.avro",
        metadata.location()
    );
    let mut list_writer = ManifestListWriter::v2(
        table.file_io().new_output(&list_path)?,
        snapshot_id,
        Some(parent.snapshot_id()),
        metadata.next_sequence_number(),
    );
    list_writer.add_manifests(manifests.into_iter())?;
    list_writer.close().await?;

    let snapshot = Snapshot::builder()
        .with_manifest_list(list_path)
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(Some(parent.snapshot_id()))
        .with_sequence_number(metadata.next_sequence_number())
        .with_summary(summary)
        .with_schema_id(metadata.current_schema_id())
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
        .build();
    Ok((
        vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: Some(parent.snapshot_id()),
            },
        ],
        vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ],
    ))
}

/// Data manifests smaller than this are candidates for consolidation.
pub const SMALL_MANIFEST_BYTES: i64 = 8 * 1024 * 1024;

/// A manifest-only rewrite: merges the many tiny manifests that one-commit-
/// per-file ingest leaves behind into one per partition spec, changing no
/// data files. Returns `None` when fewer than `min_manifests` small data
/// manifests exist (not worth a snapshot).
pub async fn prepare_consolidate(
    table: &Table,
    min_manifests: usize,
) -> Result<Option<(Vec<TableRequirement>, Vec<TableUpdate>, usize)>> {
    let metadata = table.metadata();
    anyhow::ensure!(
        metadata.format_version() == FormatVersion::V2,
        "manifest consolidation supports Iceberg format v2 only"
    );
    let Some(parent) = metadata.current_snapshot() else {
        return Ok(None);
    };
    let file_io = table.file_io();
    let list = parent
        .load_manifest_list(file_io, &metadata.clone())
        .await
        .context("loading current manifest list")?;

    let small = |m: &ManifestFile| {
        m.content == ManifestContentType::Data && m.manifest_length < SMALL_MANIFEST_BYTES
    };
    if list.entries().iter().filter(|m| small(m)).count() < min_manifests {
        return Ok(None);
    }

    let snapshot_id = new_snapshot_id(table);
    let commit_uuid = uuid::Uuid::new_v4();
    let mut kept = Vec::new();
    let mut replaced = 0usize;
    let mut writers: HashMap<i32, (iceberg::spec::ManifestWriter, usize)> = HashMap::new();
    let mut n = 0u32;
    for mf in list.entries() {
        if !small(mf) {
            kept.push(mf.clone());
            continue;
        }
        replaced += 1;
        let manifest = mf.load_manifest(file_io).await?;
        if !writers.contains_key(&mf.partition_spec_id) {
            let spec = metadata
                .partition_spec_by_id(mf.partition_spec_id)
                .ok_or_else(|| anyhow!("unknown partition spec {}", mf.partition_spec_id))?;
            let path = format!(
                "{}/metadata/{commit_uuid}-m{n}.avro",
                metadata.location()
            );
            n += 1;
            writers.insert(
                mf.partition_spec_id,
                (
                    ManifestWriterBuilder::new(
                        file_io.new_output(path)?,
                        Some(snapshot_id),
                        None,
                        metadata.current_schema().clone(),
                        spec.as_ref().clone(),
                    )
                    .build_v2_data(),
                    0,
                ),
            );
        }
        let (writer, entries) = writers.get_mut(&mf.partition_spec_id).unwrap();
        for e in manifest.entries() {
            // Deleted entries only matter to incremental readers of the
            // snapshot that deleted them; a merge may drop them.
            if !e.is_alive() {
                continue;
            }
            writer.add_existing_file(
                e.data_file().clone(),
                e.snapshot_id()
                    .ok_or_else(|| anyhow!("manifest entry without a snapshot id"))?,
                e.sequence_number()
                    .ok_or_else(|| anyhow!("manifest entry without a sequence number"))?,
                e.file_sequence_number,
            )?;
            *entries += 1;
        }
    }
    let mut created = 0usize;
    for (_, (writer, entries)) in writers {
        // A group whose entries were all deleted merges into nothing.
        if entries > 0 {
            kept.push(writer.write_manifest_file().await?);
            created += 1;
        }
    }

    // Carry only the running totals: the parent's added-/deleted-* counters
    // describe its own change, and RustFS rejects a snapshot whose counters
    // and operation disagree. A manifest-only rewrite changes no data files,
    // so it is published as an `append` that adds and deletes none.
    let mut props: HashMap<String, String> = parent
        .summary()
        .additional_properties
        .iter()
        .filter(|(k, _)| k.starts_with("total-"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    props.insert("manifests-created".into(), created.to_string());
    props.insert("manifests-replaced".into(), replaced.to_string());
    let summary = Summary {
        operation: Operation::Append,
        additional_properties: props,
    };
    let (req, upd) = seal(table, snapshot_id, commit_uuid, kept, summary).await?;
    Ok(Some((req, upd, replaced)))
}

/// Snapshot ids to expire: everything older than `older_than_ms` except the
/// newest `retain_last`, the current snapshot, and any branch/tag head.
pub fn snapshots_to_expire(table: &Table, older_than_ms: i64, retain_last: usize) -> Vec<i64> {
    let metadata = table.metadata();
    let mut all: Vec<_> = metadata.snapshots().collect();
    all.sort_by_key(|s| std::cmp::Reverse(s.timestamp_ms()));
    // TableMetadata keeps its refs private; the serialized form lists them.
    let ref_names: Vec<String> = serde_json::to_value(metadata)
        .ok()
        .and_then(|v| v.get("refs").and_then(|r| r.as_object().cloned()))
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    let pinned: HashSet<i64> = metadata
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .into_iter()
        .chain(
            ref_names
                .iter()
                .filter_map(|n| metadata.snapshot_for_ref(n).map(|s| s.snapshot_id())),
        )
        .collect();
    all.iter()
        .skip(retain_last)
        .filter(|s| s.timestamp_ms() < older_than_ms && !pinned.contains(&s.snapshot_id()))
        .map(|s| s.snapshot_id())
        .collect()
}

pub fn expire_commit(
    table: &Table,
    snapshot_ids: Vec<i64>,
) -> (Vec<TableRequirement>, Vec<TableUpdate>) {
    (
        vec![TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        }],
        vec![TableUpdate::RemoveSnapshots { snapshot_ids }],
    )
}

fn new_snapshot_id(table: &Table) -> i64 {
    loop {
        let (a, b) = uuid::Uuid::new_v4().as_u64_pair();
        let id = ((a ^ b) as i64).abs();
        if id != 0 && !table.metadata().snapshots().any(|s| s.snapshot_id() == id) {
            return id;
        }
    }
}

/// Snapshot summary for a `replace`, carrying forward the parent's running
/// totals so `total-*` stay correct for engines that read them.
fn summary(
    parent: &Summary,
    removed_files: usize,
    added_files: usize,
    removed_records: u64,
    added_records: u64,
    removed_bytes: u64,
    added_bytes: u64,
) -> Summary {
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert("added-data-files".into(), added_files.to_string());
    props.insert("deleted-data-files".into(), removed_files.to_string());
    props.insert("added-records".into(), added_records.to_string());
    props.insert("deleted-records".into(), removed_records.to_string());
    props.insert("added-files-size".into(), added_bytes.to_string());
    props.insert("removed-files-size".into(), removed_bytes.to_string());
    let carry = |key: &str, add: u64, sub: u64| {
        parent
            .additional_properties
            .get(key)
            .and_then(|v| v.parse::<u64>().ok())
            .map(|total| (total + add).saturating_sub(sub).to_string())
    };
    for (key, add, sub) in [
        ("total-data-files", added_files as u64, removed_files as u64),
        ("total-records", added_records, removed_records),
        ("total-files-size", added_bytes, removed_bytes),
    ] {
        if let Some(v) = carry(key, add, sub) {
            props.insert(key.into(), v);
        }
    }
    Summary {
        operation: Operation::Replace,
        additional_properties: props,
    }
}

/// Minimal Iceberg REST client for the one call iceberg-rust won't make.
pub struct RestClient {
    http: reqwest::Client,
    base: String,
    prefix: String,
    token: Option<String>,
}

impl RestClient {
    pub async fn connect(config: &IcebergConfig) -> Result<Self> {
        let base = if config.sigv4 {
            sigv4::signed_catalog_uri(&config.catalog_uri, sigv4::SigV4Credentials::from_env()?)
                .await?
        } else {
            config.catalog_uri.trim_end_matches('/').to_string()
        };
        let http = reqwest::Client::new();
        let mut req = http
            .get(format!("{base}/v1/config"))
            .query(&[("warehouse", config.warehouse.as_str())]);
        if let Some(t) = &config.token {
            req = req.bearer_auth(t);
        }
        let cfg: serde_json::Value = req
            .send()
            .await?
            .error_for_status()
            .context("GET /v1/config")?
            .json()
            .await?;
        let prefix = cfg["overrides"]["prefix"]
            .as_str()
            .or_else(|| cfg["defaults"]["prefix"].as_str())
            .unwrap_or_default()
            .to_string();
        Ok(Self {
            http,
            base,
            prefix,
            token: config.token.clone(),
        })
    }

    /// Posts a commit. `Ok(false)` means the catalog answered 409 (the branch
    /// moved); the caller should reload the table and re-plan.
    pub async fn commit(
        &self,
        ident: &TableIdent,
        requirements: &[TableRequirement],
        updates: &[TableUpdate],
    ) -> Result<bool> {
        let prefix = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("/{}", self.prefix)
        };
        let url = format!(
            "{}/v1{prefix}/namespaces/{}/tables/{}",
            self.base,
            ident.namespace().to_url_string(),
            ident.name()
        );
        let body = serde_json::json!({
            "identifier": { "namespace": ident.namespace().clone().inner(), "name": ident.name() },
            "requirements": requirements,
            "updates": updates,
        });
        let mut req = self.http.post(url).json(&body);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if status == reqwest::StatusCode::CONFLICT {
            return Ok(false);
        }
        if !status.is_success() {
            bail!(
                "catalog rejected commit: {status}: {}",
                resp.text().await.unwrap_or_default()
            );
        }
        Ok(true)
    }
}
