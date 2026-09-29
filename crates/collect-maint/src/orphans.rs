//! Finding objects under a table's location that no snapshot references.

use std::collections::HashSet;

use anyhow::{Context, Result};
use collect_core::{S3ObjectInfo, S3Storage};
use iceberg::table::Table;

/// Splits `s3://bucket/some/path` into (`bucket`, `some/path/`).
pub fn split_location(location: &str) -> Result<(String, String)> {
    let rest = location
        .split_once("://")
        .with_context(|| format!("table location has no scheme: {location}"))?
        .1;
    let (bucket, path) = rest.split_once('/').unwrap_or((rest, ""));
    let path = path.trim_matches('/');
    Ok((bucket.to_string(), if path.is_empty() { String::new() } else { format!("{path}/") }))
}

fn key_of(path: &str) -> String {
    path.split_once("://")
        .and_then(|(_, r)| r.split_once('/'))
        .map(|(_, k)| k.to_string())
        .unwrap_or_else(|| path.to_string())
}

/// Every object key the table's metadata reaches: the metadata file and log,
/// every snapshot's manifest list, every manifest, and every data file those
/// list (deleted entries included, to stay conservative).
pub async fn referenced_keys(table: &Table) -> Result<HashSet<String>> {
    let metadata = table.metadata();
    let mut keys = HashSet::new();
    if let Some(loc) = table.metadata_location() {
        keys.insert(key_of(loc));
    }
    // Statistics, partition statistics and metadata-log paths all appear as
    // strings under the table location in the serialized metadata.
    let mut stack = vec![serde_json::to_value(metadata)?];
    let location = metadata.location().to_string();
    while let Some(v) = stack.pop() {
        match v {
            serde_json::Value::String(s) if s.starts_with(&location) || s.contains("://") => {
                keys.insert(key_of(&s));
            }
            serde_json::Value::Array(a) => stack.extend(a),
            serde_json::Value::Object(o) => stack.extend(o.into_values()),
            _ => {}
        }
    }
    for snap in metadata.snapshots() {
        let list = snap
            .load_manifest_list(table.file_io(), &metadata.clone())
            .await
            .with_context(|| format!("loading manifest list of snapshot {}", snap.snapshot_id()))?;
        for mf in list.entries() {
            keys.insert(key_of(&mf.manifest_path));
            let manifest = mf.load_manifest(table.file_io()).await?;
            for e in manifest.entries() {
                keys.insert(key_of(e.file_path()));
            }
        }
    }
    Ok(keys)
}

/// Objects under the table location that no snapshot references and that are
/// older than `cutoff_ms` (writers upload before they commit, so a young
/// unreferenced object may be an in-flight write).
pub async fn find(
    table: &Table,
    storage: &S3Storage,
    cutoff_ms: i64,
) -> Result<Vec<S3ObjectInfo>> {
    let (_, prefix) = split_location(table.metadata().location())?;
    let referenced = referenced_keys(table).await?;
    anyhow::ensure!(!referenced.is_empty(), "no referenced objects found; refusing to list orphans");
    let listed = storage.list_keys_with_prefix(&prefix).await?;
    Ok(listed
        .into_iter()
        .filter(|o| !referenced.contains(&o.key) && o.modified_ms.is_some_and(|m| m < cutoff_ms))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_locations() {
        assert_eq!(split_location("s3://b/a/b").unwrap(), ("b".into(), "a/b/".into()));
        assert_eq!(split_location("s3://b").unwrap(), ("b".into(), String::new()));
        assert_eq!(key_of("s3://b/a/b.parquet"), "a/b.parquet");
    }
}
