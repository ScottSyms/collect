//! Persisted watermark state for `--incremental` runs.
//!
//! The state is one small JSON document stored at the output target —
//! `<output-dir>/_<tool>/watermark.json` locally (e.g.
//! `_ais-normalize/watermark.json`), or the same path as a key under the
//! output prefix on S3. Dataset listings only match `*.parquet`, so the state
//! never shows up as data, and `collect-maint` skips `_`-prefixed segments.
//!
//! Reading is deliberately forgiving: a missing state means "first run", and
//! a corrupt state is downgraded to a warning plus "first run" rather than
//! failing a scheduled job that could otherwise self-heal.

use crate::S3Storage;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Comparison lap applied when selecting files against the watermark, to
/// absorb `LastModified` jitter around the previous run's listing.
pub const WATERMARK_LAP_MS: i64 = 60_000;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct WatermarkState {
    pub version: u32,
    /// Newest file-modification time processed by the last successful run
    /// (UTC milliseconds).
    pub watermark_ms: i64,
    /// Wall-clock time the state was written (UTC milliseconds); informational.
    pub updated_at_ms: i64,
}

impl WatermarkState {
    pub fn new(watermark_ms: i64) -> Self {
        WatermarkState {
            version: 1,
            watermark_ms,
            updated_at_ms: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Cutoff used for file selection: modification times strictly newer than
    /// this are considered unprocessed.
    pub fn cutoff_ms(&self) -> i64 {
        self.watermark_ms.saturating_sub(WATERMARK_LAP_MS)
    }
}

enum StateTarget<'a> {
    Local {
        output_root: &'a Path,
    },
    S3 {
        storage: &'a S3Storage,
        prefix: &'a str,
    },
}

/// Where the state lives — always tied to the output target, because the
/// watermark describes what has been *written*, and a different output is a
/// different job. Each tool keeps its own document (`_<tool>/watermark.json`),
/// so e.g. ais-normalize and ais-parse can share an output tree without
/// clobbering each other's progress.
pub struct StateStore<'a> {
    target: StateTarget<'a>,
    /// `_<tool>/watermark.json`
    rel_path: String,
}

impl<'a> StateStore<'a> {
    pub fn local(output_root: &'a Path, tool: &str) -> Self {
        StateStore {
            target: StateTarget::Local { output_root },
            rel_path: Self::rel_path_for(tool),
        }
    }

    pub fn s3(storage: &'a S3Storage, prefix: &'a str, tool: &str) -> Self {
        StateStore {
            target: StateTarget::S3 { storage, prefix },
            rel_path: Self::rel_path_for(tool),
        }
    }

    fn rel_path_for(tool: &str) -> String {
        format!("_{tool}/watermark.json")
    }

    fn local_path(&self, output_root: &Path) -> PathBuf {
        output_root.join(&self.rel_path)
    }

    fn s3_key(&self, prefix: &str) -> String {
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            self.rel_path.clone()
        } else {
            format!("{prefix}/{}", self.rel_path)
        }
    }

    /// Human-readable location, for log lines.
    pub fn describe(&self) -> String {
        match &self.target {
            StateTarget::Local { output_root } => {
                self.local_path(output_root).display().to_string()
            }
            StateTarget::S3 { storage, prefix } => {
                format!("s3://{}/{}", storage.bucket_name(), self.s3_key(prefix))
            }
        }
    }

    /// Load the previous run's state. `Ok(None)` means first run (no state,
    /// or state that could not be parsed — reported as a warning).
    pub async fn load(&self) -> Result<Option<WatermarkState>> {
        let bytes = match &self.target {
            StateTarget::Local { output_root } => {
                match std::fs::read(self.local_path(output_root)) {
                    Ok(bytes) => Some(bytes),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => {
                        return Err(error).context("reading watermark state file");
                    }
                }
            }
            StateTarget::S3 { storage, prefix } => storage
                .get_bytes(&self.s3_key(prefix))
                .await
                .context("reading watermark state object")?,
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        match serde_json::from_slice::<WatermarkState>(&bytes) {
            Ok(state) => Ok(Some(state)),
            Err(error) => {
                eprintln!(
                    "Warning: ignoring unreadable watermark state at {} ({error}); \
                     treating this as a first run.",
                    self.describe()
                );
                Ok(None)
            }
        }
    }

    /// Persist the new state. Local writes go through a temp file + rename so
    /// a crash mid-write can't leave a truncated document.
    pub async fn save(&self, state: WatermarkState) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(&state).context("serializing watermark state")?;
        match &self.target {
            StateTarget::Local { output_root } => {
                let path = self.local_path(output_root);
                let parent = path
                    .parent()
                    .expect("state path always has a parent directory");
                std::fs::create_dir_all(parent).context("creating watermark state directory")?;
                let tmp = parent.join("watermark.json.tmp");
                std::fs::write(&tmp, &bytes).context("writing watermark state temp file")?;
                std::fs::rename(&tmp, &path).context("renaming watermark state into place")?;
            }
            StateTarget::S3 { storage, prefix } => {
                storage
                    .put_bytes(&self.s3_key(prefix), bytes)
                    .await
                    .context("writing watermark state object")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = StateStore::local(dir.path(), "ais-normalize");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");

        // Missing state → first run.
        assert!(rt.block_on(store.load()).expect("load").is_none());

        let state = WatermarkState::new(1_750_000_000_000);
        rt.block_on(store.save(state)).expect("save");
        let loaded = rt
            .block_on(store.load())
            .expect("load")
            .expect("state present");
        assert_eq!(loaded.watermark_ms, 1_750_000_000_000);
        assert_eq!(loaded.version, 1);
    }

    #[test]
    fn corrupt_state_is_treated_as_first_run() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("_ais-normalize/watermark.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"not json at all").unwrap();

        let store = StateStore::local(dir.path(), "ais-normalize");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        assert!(rt.block_on(store.load()).expect("load").is_none());
    }

    #[test]
    fn cutoff_applies_the_lap() {
        let state = WatermarkState::new(1_000_000);
        assert_eq!(state.cutoff_ms(), 1_000_000 - WATERMARK_LAP_MS);
    }

    #[test]
    fn s3_key_handles_prefixes_and_tool_names() {
        let dir = tempfile::tempdir().expect("tempdir");
        let normalize = StateStore::local(dir.path(), "ais-normalize");
        assert_eq!(normalize.s3_key(""), "_ais-normalize/watermark.json");
        assert_eq!(
            normalize.s3_key("/datasets/ais/"),
            "datasets/ais/_ais-normalize/watermark.json"
        );
        let parse = StateStore::local(dir.path(), "ais-parse");
        assert_eq!(parse.s3_key(""), "_ais-parse/watermark.json");
    }

    #[test]
    fn tools_do_not_share_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let normalize = StateStore::local(dir.path(), "ais-normalize");
        rt.block_on(normalize.save(WatermarkState::new(42)))
            .expect("save");
        let parse = StateStore::local(dir.path(), "ais-parse");
        assert!(
            rt.block_on(parse.load()).expect("load").is_none(),
            "ais-parse must not read ais-normalize's watermark"
        );
    }
}
