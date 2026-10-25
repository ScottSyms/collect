//! The source catalogue: one TOML file per source, kept apart from the code.
//! The files under `config/sources` are built in; `--config-dir` replaces
//! them with a directory of the same shape, so a deployment can add, disable
//! or re-license a source without a rebuild.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const BUILT_IN: &[(&str, &str)] = &[
    ("tc_large_vessels.toml", include_str!("../config/sources/tc_large_vessels.toml")),
    ("tc_small_vessels.toml", include_str!("../config/sources/tc_small_vessels.toml")),
    ("uscg_psix.toml", include_str!("../config/sources/uscg_psix.toml")),
    ("iacs_vessels_in_class.toml", include_str!("../config/sources/iacs_vessels_in_class.toml")),
    ("uscg_misle_deficiencies.toml", include_str!("../config/sources/uscg_misle_deficiencies.toml")),
    ("gfw_vessel_identity.toml", include_str!("../config/sources/gfw_vessel_identity.toml")),
];

/// What a source's licence or agreement lets us do with it. Collection,
/// retention and combination are checked before ingesting.
pub const USES: &[&str] = &[
    "access",
    "automated_download",
    "retain_history",
    "combine",
    "redistribute",
    "commercial",
];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub source_id: String,
    pub name: String,
    pub publisher: String,
    /// The parser adapter: `tc_large`, `tc_small`, `iacs`, `psix`.
    pub adapter: String,
    pub source_url: String,
    #[serde(default)]
    pub dataset_url: Option<String>,
    /// `http_download`, `api`, `authenticated_api`, `manual`.
    pub access_method: String,
    pub data_format: String,
    /// `daily`, `weekly`, `monthly` or `manual`.
    pub refresh_policy: String,
    pub license: String,
    pub permitted_uses: Vec<String>,
    /// `approved`, `pending`, `restricted`, `suspended`.
    pub access_status: String,
    pub authorization_basis: String,
    /// `none`, or `env:<VAR>` naming where a key is kept (never the key).
    pub authentication: String,
    pub enabled: bool,
    pub priority: i32,
    /// `registry` or `self_reported`.
    pub trust: String,
    /// Identifier types this source issues itself (`NATIONAL_REGISTRY`).
    #[serde(default)]
    pub issues: Vec<String>,
    /// ISO 3166 alpha-2 of the issuing jurisdiction, for national registers.
    #[serde(default)]
    pub jurisdiction: Option<String>,
    /// `snapshot` (each fetch is the whole dataset, so absence means removal)
    /// or `incremental` (absence means nothing).
    pub removal_semantics: String,
    /// A run that would remove more than this share of the source's records
    /// is quarantined instead of applied.
    pub max_removal_fraction: f64,
    /// Whether the fetched files may be archived.
    pub retain_raw: bool,
    #[serde(default)]
    pub options: toml::Table,
}

impl SourceConfig {
    pub fn parse(text: &str, origin: &str) -> Result<Self> {
        let c: SourceConfig = toml::from_str(text).with_context(|| format!("parsing {origin}"))?;
        c.check().with_context(|| format!("checking {origin}"))?;
        Ok(c)
    }

    fn check(&self) -> Result<()> {
        let id_ok = !self.source_id.is_empty()
            && self.source_id.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        anyhow::ensure!(id_ok, "source_id '{}' must be lowercase letters, digits and _", self.source_id);
        for u in &self.permitted_uses {
            anyhow::ensure!(USES.contains(&u.as_str()), "unknown permitted use '{u}' (expected one of {USES:?})");
        }
        let one_of = |what: &str, v: &str, allowed: &[&str]| -> Result<()> {
            anyhow::ensure!(allowed.contains(&v), "{what} '{v}' is not one of {allowed:?}");
            Ok(())
        };
        one_of("access_status", &self.access_status, &["approved", "pending", "restricted", "suspended"])?;
        one_of("refresh_policy", &self.refresh_policy, &["daily", "weekly", "monthly", "manual"])?;
        one_of("trust", &self.trust, &["registry", "self_reported"])?;
        one_of("removal_semantics", &self.removal_semantics, &["snapshot", "incremental"])?;
        one_of(
            "access_method",
            &self.access_method,
            &["http_download", "api", "authenticated_api", "manual"],
        )?;
        anyhow::ensure!(
            self.authentication == "none" || self.authentication.starts_with("env:"),
            "authentication must be 'none' or 'env:<VAR>' (keys are never kept in config)"
        );
        anyhow::ensure!(
            (0.0..=1.0).contains(&self.max_removal_fraction),
            "max_removal_fraction must be within 0..1"
        );
        if self.issues.iter().any(|i| i == "NATIONAL_REGISTRY") {
            anyhow::ensure!(
                self.jurisdiction.as_deref().is_some_and(|j| j.len() == 2),
                "a source issuing NATIONAL_REGISTRY identifiers needs a two-letter jurisdiction"
            );
        }
        Ok(())
    }

    pub fn permits(&self, used: &str) -> bool {
        self.permitted_uses.iter().any(|u| u == used)
    }

    /// Why automatic collection is refused, or `None` when it may go ahead.
    /// Ingesting means downloading automatically, keeping the records'
    /// history and combining them with other sources.
    pub fn refusal(&self) -> Option<String> {
        if !self.enabled {
            return Some("disabled in its configuration".into());
        }
        if self.access_status != "approved" {
            return Some(format!("access_status is '{}', not 'approved'", self.access_status));
        }
        if self.access_method == "manual" {
            return Some("access_method is manual; pass the files with --source-dir".into());
        }
        for used in ["access", "automated_download", "retain_history", "combine"] {
            if !self.permits(used) {
                return Some(format!("permitted_uses lacks '{used}'"));
            }
        }
        None
    }

    /// Like [`refusal`](Self::refusal), for files an operator fetched by hand:
    /// automated download doesn't apply, the rest does.
    pub fn refusal_for_local_files(&self) -> Option<String> {
        if !self.enabled {
            return Some("disabled in its configuration".into());
        }
        if self.access_status != "approved" {
            return Some(format!("access_status is '{}', not 'approved'", self.access_status));
        }
        for used in ["access", "retain_history", "combine"] {
            if !self.permits(used) {
                return Some(format!("permitted_uses lacks '{used}'"));
            }
        }
        None
    }

    /// Whether a source last fetched at `last` is due again at `now`.
    pub fn is_due(&self, last: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
        let every = match self.refresh_policy.as_str() {
            "daily" => Duration::hours(20),
            "weekly" => Duration::days(6),
            "monthly" => Duration::days(27),
            _ => return false,
        };
        last.is_none_or(|t| now - t >= every)
    }

    /// The SHA-256 of the configuration as loaded, recorded with every run.
    pub fn version_hash(&self, text: &str) -> String {
        hex::encode(Sha256::digest(text.as_bytes()))[..16].to_string()
    }

    pub fn option_u64(&self, key: &str, default: u64) -> u64 {
        self.options.get(key).and_then(|v| v.as_integer()).map_or(default, |v| v.max(0) as u64)
    }

    pub fn option_strings(&self, key: &str) -> Vec<String> {
        self.options
            .get(key)
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default()
    }
}

/// The loaded catalogue, keyed by `source_id`, each with its config hash.
pub struct Catalogue {
    pub sources: BTreeMap<String, (SourceConfig, String)>,
}

impl Catalogue {
    /// The built-in sources, or every `*.toml` in `dir`.
    pub fn load(dir: Option<&Path>) -> Result<Self> {
        let mut files: Vec<(String, String)> = Vec::new();
        match dir {
            None => files.extend(BUILT_IN.iter().map(|(n, t)| (n.to_string(), t.to_string()))),
            Some(dir) => {
                for entry in std::fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))? {
                    let path = entry?.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("toml") {
                        let text = std::fs::read_to_string(&path)
                            .with_context(|| format!("reading {}", path.display()))?;
                        files.push((path.display().to_string(), text));
                    }
                }
            }
        }
        let mut sources = BTreeMap::new();
        for (origin, text) in files {
            let c = SourceConfig::parse(&text, &origin)?;
            let hash = c.version_hash(&text);
            if sources.insert(c.source_id.clone(), (c, hash)).is_some() {
                bail!("source_id defined twice (in {origin})");
            }
        }
        anyhow::ensure!(!sources.is_empty(), "no sources configured");
        Ok(Catalogue { sources })
    }

    pub fn get(&self, id: &str) -> Result<&(SourceConfig, String)> {
        self.sources.get(id).with_context(|| {
            format!(
                "unknown source '{id}' (configured: {})",
                self.sources.keys().cloned().collect::<Vec<_>>().join(", ")
            )
        })
    }

    pub fn priority(&self, id: &str) -> i32 {
        self.sources.get(id).map_or(0, |(c, _)| c.priority)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_in_sources_load() {
        let c = Catalogue::load(None).unwrap();
        assert!(c.sources.contains_key("tc_large_vessels"));
        let (tc, _) = c.get("tc_large_vessels").unwrap();
        assert_eq!(tc.refusal(), None);
        let (psix, _) = c.get("uscg_psix").unwrap();
        assert!(psix.refusal().unwrap().contains("pending"));
    }

    #[test]
    fn rejects_keys_in_config_and_unknown_uses() {
        let base = include_str!("../config/sources/tc_large_vessels.toml");
        let bad = base.replace("authentication = \"none\"", "authentication = \"abc123\"");
        assert!(SourceConfig::parse(&bad, "x").is_err());
        let bad = base.replace("\"commercial\"]", "\"commercial\", \"resell\"]");
        assert!(SourceConfig::parse(&bad, "x").is_err());
    }

    #[test]
    fn refusal_follows_the_licence() {
        let base = include_str!("../config/sources/tc_large_vessels.toml");
        let c = SourceConfig::parse(&base.replace("\"combine\", ", ""), "x").unwrap();
        assert!(c.refusal().unwrap().contains("combine"));
        let c = SourceConfig::parse(&base.replace("enabled = true", "enabled = false"), "x").unwrap();
        assert!(c.refusal().is_some());
    }

    #[test]
    fn due_by_policy() {
        let c = Catalogue::load(None).unwrap();
        let (tc, _) = c.get("tc_small_vessels").unwrap();
        let now = Utc::now();
        assert!(tc.is_due(None, now));
        assert!(!tc.is_due(Some(now - Duration::days(2)), now));
        assert!(tc.is_due(Some(now - Duration::days(7)), now));
    }
}
