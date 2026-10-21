//! Bronze: source records and what changed since the last ingestion.

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};

use crate::columns::Reader;
use crate::config::SourceConfig;
use crate::fetch::sha256_hex;
use crate::sources::{Adapter, Parsed};
use crate::tables::{change, RejectedRow, SourceRecordRow};

/// One stored version of a source record, as read back from bronze.
#[derive(Clone, Debug)]
pub struct Version {
    pub record_id: String,
    pub source_record_id: String,
    pub subject_id: String,
    pub change_type: String,
    pub record_hash: Option<String>,
    pub raw_content: Option<String>,
    pub ingested_at: DateTime<Utc>,
    pub source_published_at: Option<DateTime<Utc>>,
}

pub fn read_versions(batches: &[RecordBatch]) -> Result<Vec<Version>> {
    let mut out = Vec::new();
    for b in batches {
        let r = Reader::new(b);
        let (rid, srid, subj, ct, hash, raw) = (
            r.str("record_id")?,
            r.str("source_record_id")?,
            r.str("subject_id")?,
            r.str("change_type")?,
            r.str("record_hash")?,
            r.str("raw_content")?,
        );
        let (ing, publ) = (r.ts("ingested_at")?, r.ts("source_published_at")?);
        for i in 0..r.rows() {
            out.push(Version {
                record_id: rid(i).unwrap_or_default(),
                source_record_id: srid(i).unwrap_or_default(),
                subject_id: subj(i).unwrap_or_default(),
                change_type: ct(i).unwrap_or_default(),
                record_hash: hash(i),
                raw_content: raw(i),
                ingested_at: ing(i).expect("ingested_at is required"),
                source_published_at: publ(i),
            });
        }
    }
    sort_versions(&mut out);
    Ok(out)
}

/// Versions in the order they happened: by record, then ingestion time.
pub fn sort_versions(v: &mut [Version]) {
    v.sort_by(|a, b| {
        (&a.source_record_id, a.ingested_at, &a.record_id).cmp(&(&b.source_record_id, b.ingested_at, &b.record_id))
    });
}

/// The fields as canonical JSON (keys sorted) and its SHA-256.
pub fn canonical(fields: &BTreeMap<String, String>) -> (String, String) {
    let json = serde_json::to_string(fields).expect("fields serialize");
    let hash = sha256_hex(json.as_bytes());
    (json, hash)
}

pub fn fields_of(raw: &str) -> BTreeMap<String, String> {
    serde_json::from_str(raw).unwrap_or_default()
}

/// What an ingestion adds to bronze.
#[derive(Default)]
pub struct Delta {
    pub rows: Vec<SourceRecordRow>,
    pub rejected: Vec<RejectedRow>,
    pub read: usize,
    pub new: usize,
    pub modified: usize,
    pub unchanged: usize,
    pub removed: usize,
    /// Records the source held before this run (not removed).
    pub present_before: usize,
    pub partial: usize,
}

pub struct RunInfo<'a> {
    pub run_id: &'a str,
    pub source_version: Option<&'a str>,
    pub source_published_at: Option<DateTime<Utc>>,
    pub retrieved_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub object_uris: &'a [Option<String>],
    /// Whether the fetch saw the whole source.
    pub complete: bool,
}

/// Compares what was parsed with the latest stored version of each record.
pub fn diff(cfg: &SourceConfig, adapter: Adapter, run: &RunInfo, parsed: &Parsed, stored: &[Version]) -> Delta {
    let mut latest: HashMap<&str, &Version> = HashMap::new();
    for v in stored {
        latest.insert(v.source_record_id.as_str(), v); // sorted, so the last wins
    }
    let mut d = Delta { read: parsed.records.len(), ..Default::default() };
    d.present_before = latest.values().filter(|v| v.change_type != change::REMOVED).count();
    let mut n = 0usize;
    let mut next_id = || {
        n += 1;
        format!("{}:{n}", run.run_id)
    };
    let uri = |i: usize| run.object_uris.get(i).cloned().flatten();

    let mut seen: HashSet<&str> = HashSet::new();
    for rec in &parsed.records {
        seen.insert(rec.source_record_id.as_str());
        let (json, hash) = canonical(&rec.fields);
        let change_type = match latest.get(rec.source_record_id.as_str()) {
            None => change::NEW,
            Some(v) if v.change_type == change::REMOVED => change::NEW,
            Some(v) if v.record_hash.as_deref() == Some(hash.as_str()) => {
                d.unchanged += 1;
                continue;
            }
            Some(_) => change::MODIFIED,
        };
        if change_type == change::NEW {
            d.new += 1;
        } else {
            d.modified += 1;
        }
        let problems: Vec<String> = adapter.map(cfg, &rec.fields).iter().filter_map(|x| x.problem()).collect();
        if !problems.is_empty() {
            d.partial += 1;
        }
        d.rows.push(SourceRecordRow {
            record_id: next_id(),
            source_id: cfg.source_id.clone(),
            source_record_id: rec.source_record_id.clone(),
            subject_id: rec.subject_id.clone(),
            source_version: run.source_version.map(String::from),
            ingestion_run_id: run.run_id.to_string(),
            source_published_at: rec.published_at.or(run.source_published_at),
            retrieved_at: run.retrieved_at,
            ingested_at: run.ingested_at,
            change_type: change_type.to_string(),
            record_hash: Some(hash),
            raw_content: Some(json),
            source_object_uri: uri(rec.object),
            parser_version: adapter.parser_version().to_string(),
            validation_status: if problems.is_empty() { "valid" } else { "partial" }.to_string(),
            validation_errors: problems,
        });
    }

    // Absence means removal only when the fetch saw everything and the
    // source publishes whole snapshots.
    if run.complete && cfg.removal_semantics == "snapshot" {
        let mut gone: Vec<&&Version> = latest
            .values()
            .filter(|v| v.change_type != change::REMOVED && !seen.contains(v.source_record_id.as_str()))
            .collect();
        gone.sort_by(|a, b| a.source_record_id.cmp(&b.source_record_id));
        for v in gone {
            d.removed += 1;
            d.rows.push(SourceRecordRow {
                record_id: next_id(),
                source_id: cfg.source_id.clone(),
                source_record_id: v.source_record_id.clone(),
                subject_id: v.subject_id.clone(),
                source_version: run.source_version.map(String::from),
                ingestion_run_id: run.run_id.to_string(),
                source_published_at: run.source_published_at,
                retrieved_at: run.retrieved_at,
                ingested_at: run.ingested_at,
                change_type: change::REMOVED.to_string(),
                record_hash: None,
                raw_content: None,
                source_object_uri: None,
                parser_version: adapter.parser_version().to_string(),
                validation_status: "valid".to_string(),
                validation_errors: Vec::new(),
            });
        }
    }

    for rej in &parsed.rejected {
        d.rejected.push(RejectedRow {
            record_id: next_id(),
            source_id: cfg.source_id.clone(),
            ingestion_run_id: run.run_id.to_string(),
            source_object_uri: uri(rej.object),
            location: rej.location.clone(),
            raw_content: rej.raw.clone(),
            reasons: rej.reasons.clone(),
            ingested_at: run.ingested_at,
        });
    }
    d
}

impl Delta {
    /// Why the delta should not be applied: too much of the source would
    /// disappear at once, which usually means a truncated or broken file.
    pub fn quarantine_reason(&self, cfg: &SourceConfig) -> Option<String> {
        if self.present_before >= 20 {
            let share = self.removed as f64 / self.present_before as f64;
            if share > cfg.max_removal_fraction {
                return Some(format!(
                    "{} of {} records ({:.0}%) would be removed, above max_removal_fraction {:.0}%",
                    self.removed,
                    self.present_before,
                    share * 100.0,
                    cfg.max_removal_fraction * 100.0
                ));
            }
        }
        None
    }

    /// The bronze rows as stored versions, for deriving assertions.
    pub fn versions(&self) -> Vec<Version> {
        self.rows
            .iter()
            .map(|r| Version {
                record_id: r.record_id.clone(),
                source_record_id: r.source_record_id.clone(),
                subject_id: r.subject_id.clone(),
                change_type: r.change_type.clone(),
                record_hash: r.record_hash.clone(),
                raw_content: r.raw_content.clone(),
                ingested_at: r.ingested_at,
                source_published_at: r.source_published_at,
            })
            .collect()
    }
}
