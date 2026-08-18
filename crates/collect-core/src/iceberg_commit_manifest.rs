//! Per-partition record of source object keys already committed to Iceberg.
//!
//! Iceberg writes in this workspace are pure `fast_append` — there is no
//! equality-delete/overwrite/upsert action available in the `iceberg` crate
//! today (see the `ais-parse`/`aisstream-parse` Iceberg output path), so
//! nothing prevents the same source row from being committed twice if a
//! partition is reprocessed. This manifest closes that gap at the source-file
//! level: before decoding a partition, the caller drops any input file whose
//! key is already recorded here; after a partition's Iceberg commit fully
//! succeeds, the caller records the keys it just committed.
//!
//! One small file per partition — `_<tool>/committed/<partition-rel-dir>.log`
//! — rather than one global manifest, because exactly one process ever owns a
//! given partition (disjoint backfill shards; a single scheduled incremental
//! run; the existing "don't run two instances against the same output
//! concurrently" rule), so a load-modify-put on S3 has no concurrent-writer
//! race to guard against.
//!
//! Owns its target (rather than borrowing, like [`crate::state::StateStore`])
//! so it can be cheaply cloned into the per-partition worker tasks that need
//! to consult and update it — `S3Storage` is itself `Clone`.

use crate::S3Storage;
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::PathBuf;

#[derive(Clone)]
enum ManifestTarget {
    Local { output_root: PathBuf },
    S3 { storage: S3Storage, prefix: String },
}

#[derive(Clone)]
pub struct CommitManifest {
    target: ManifestTarget,
    tool: String,
}

impl CommitManifest {
    pub fn local(output_root: impl Into<PathBuf>, tool: impl Into<String>) -> Self {
        CommitManifest {
            target: ManifestTarget::Local {
                output_root: output_root.into(),
            },
            tool: tool.into(),
        }
    }

    pub fn s3(storage: S3Storage, prefix: impl Into<String>, tool: impl Into<String>) -> Self {
        CommitManifest {
            target: ManifestTarget::S3 {
                storage,
                prefix: prefix.into(),
            },
            tool: tool.into(),
        }
    }

    fn rel_path(&self, partition_rel_dir: &str) -> String {
        format!("_{}/committed/{}.log", self.tool, partition_rel_dir)
    }

    /// Load the set of source object keys already committed for this
    /// partition. An empty set means nothing has been committed yet (first
    /// time this partition has been processed, or a corrupt/unreadable
    /// manifest — downgraded to a warning so a bad manifest can't wedge a
    /// scheduled job).
    pub async fn load(&self, partition_rel_dir: &str) -> Result<HashSet<String>> {
        let rel_path = self.rel_path(partition_rel_dir);
        let bytes = match &self.target {
            ManifestTarget::Local { output_root } => {
                match std::fs::read(output_root.join(&rel_path)) {
                    Ok(bytes) => Some(bytes),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => {
                        return Err(error).context("reading Iceberg commit manifest")
                    }
                }
            }
            ManifestTarget::S3 { storage, prefix } => storage
                .get_bytes(&Self::s3_key(prefix, &rel_path))
                .await
                .context("reading Iceberg commit manifest object")?,
        };
        let Some(bytes) = bytes else {
            return Ok(HashSet::new());
        };
        match std::str::from_utf8(&bytes) {
            Ok(text) => Ok(text
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(str::to_string)
                .collect()),
            Err(error) => {
                eprintln!(
                    "Warning: ignoring unreadable Iceberg commit manifest at {} ({error}); \
                     treating this partition as not yet committed.",
                    rel_path
                );
                Ok(HashSet::new())
            }
        }
    }

    /// Record newly-committed source object keys for this partition. Merges
    /// with whatever is already recorded (a partition can be revisited many
    /// times as new files land in it), then rewrites the manifest whole —
    /// safe because exactly one process ever owns a given partition.
    pub async fn record(&self, partition_rel_dir: &str, new_keys: &[String]) -> Result<()> {
        if new_keys.is_empty() {
            return Ok(());
        }
        let mut all_keys = self.load(partition_rel_dir).await?;
        all_keys.extend(new_keys.iter().cloned());

        let mut sorted: Vec<&str> = all_keys.iter().map(String::as_str).collect();
        sorted.sort_unstable();
        let mut contents = sorted.join("\n");
        contents.push('\n');

        let rel_path = self.rel_path(partition_rel_dir);
        match &self.target {
            ManifestTarget::Local { output_root } => {
                let path = output_root.join(&rel_path);
                let parent = path
                    .parent()
                    .expect("manifest path always has a parent directory");
                std::fs::create_dir_all(parent)
                    .context("creating Iceberg commit manifest directory")?;
                let tmp = parent.join(format!(
                    "{}.tmp",
                    path.file_name()
                        .expect("manifest path always has a file name")
                        .to_string_lossy()
                ));
                std::fs::write(&tmp, contents.as_bytes())
                    .context("writing Iceberg commit manifest temp file")?;
                std::fs::rename(&tmp, &path)
                    .context("renaming Iceberg commit manifest into place")?;
            }
            ManifestTarget::S3 { storage, prefix } => {
                storage
                    .put_bytes(&Self::s3_key(prefix, &rel_path), contents.into_bytes())
                    .await
                    .context("writing Iceberg commit manifest object")?;
            }
        }
        Ok(())
    }

    fn s3_key(prefix: &str, rel_path: &str) -> String {
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            rel_path.to_string()
        } else {
            format!("{prefix}/{rel_path}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime")
    }

    #[test]
    fn missing_manifest_is_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = CommitManifest::local(dir.path(), "ais-parse");
        let loaded = rt()
            .block_on(manifest.load("year=2026/month=07/day=16"))
            .expect("load");
        assert!(loaded.is_empty());
    }

    #[test]
    fn record_then_load_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = CommitManifest::local(dir.path(), "ais-parse");
        let part = "year=2026/month=07/day=16";
        rt().block_on(manifest.record(part, &["a.parquet".to_string(), "b.parquet".to_string()]))
            .expect("record");

        let loaded = rt().block_on(manifest.load(part)).expect("load");
        assert_eq!(loaded.len(), 2);
        assert!(loaded.contains("a.parquet"));
        assert!(loaded.contains("b.parquet"));
    }

    #[test]
    fn record_merges_with_existing_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = CommitManifest::local(dir.path(), "ais-parse");
        let part = "year=2026/month=07/day=16";
        rt().block_on(manifest.record(part, &["a.parquet".to_string()]))
            .expect("first record");
        rt().block_on(manifest.record(part, &["b.parquet".to_string()]))
            .expect("second record");

        let loaded = rt().block_on(manifest.load(part)).expect("load");
        assert_eq!(loaded.len(), 2);
        assert!(loaded.contains("a.parquet"));
        assert!(loaded.contains("b.parquet"));
    }

    #[test]
    fn empty_new_keys_is_a_noop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = CommitManifest::local(dir.path(), "ais-parse");
        let part = "year=2026/month=07/day=16";
        rt().block_on(manifest.record(part, &[])).expect("record");
        assert!(!dir
            .path()
            .join("_ais-parse/committed")
            .join(part)
            .with_extension("log")
            .exists());
    }

    #[test]
    fn tools_do_not_share_manifests() {
        let dir = tempfile::tempdir().expect("tempdir");
        let part = "year=2026/month=07/day=16";
        let ais_parse = CommitManifest::local(dir.path(), "ais-parse");
        rt().block_on(ais_parse.record(part, &["a.parquet".to_string()]))
            .expect("record");

        let aisstream_parse = CommitManifest::local(dir.path(), "aisstream-parse");
        let loaded = rt().block_on(aisstream_parse.load(part)).expect("load");
        assert!(loaded.is_empty());
    }

    #[test]
    fn corrupt_manifest_is_treated_as_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let part = "year=2026/month=07/day=16";
        let path = dir
            .path()
            .join(format!("_ais-parse/committed/{part}.log"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, [0xFF, 0xFE, 0x00, 0xFF]).unwrap();

        let manifest = CommitManifest::local(dir.path(), "ais-parse");
        let loaded = rt().block_on(manifest.load(part)).expect("load");
        assert!(loaded.is_empty());
    }

    #[test]
    fn clone_shares_the_same_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = CommitManifest::local(dir.path(), "ais-parse");
        let cloned = manifest.clone();
        let part = "year=2026/month=07/day=16";
        rt().block_on(manifest.record(part, &["a.parquet".to_string()]))
            .expect("record via original");
        let loaded = rt().block_on(cloned.load(part)).expect("load via clone");
        assert!(loaded.contains("a.parquet"));
    }
}
