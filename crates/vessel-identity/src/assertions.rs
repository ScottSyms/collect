//! Silver: assertions, derived from a source's whole bronze history.
//!
//! Each version of a record makes some claims. A claim the next version no
//! longer makes is superseded at that version's ingestion time; a record
//! removed from the source withdraws its open claims. A claim repeated by
//! later versions keeps the time and provenance of the version that first
//! made it. Because the result depends only on bronze, rebuilding a source's
//! partition is idempotent, and an ingestion interrupted after its bronze
//! commit is completed by running it again.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::config::SourceConfig;
use crate::fetch::sha256_hex;
use crate::records::{fields_of, Version};
use crate::sources::{Adapter, Draft};
use crate::tables::{change, AssertionRow};

struct Open {
    assertion_id: String,
    recorded_at: DateTime<Utc>,
    published_at: Option<DateTime<Utc>>,
    provenance: String,
}

fn ts(t: Option<DateTime<Utc>>) -> String {
    t.map(|t| t.to_rfc3339()).unwrap_or_default()
}

fn assertion_id(source_id: &str, source_record_id: &str, provenance: &str, d: &Draft) -> String {
    let key = [
        source_id,
        source_record_id,
        provenance,
        d.attribute,
        d.value_original.as_deref().unwrap_or(""),
        d.value_normalized.as_deref().unwrap_or(""),
        d.qualifier.as_deref().unwrap_or(""),
        &ts(d.valid_from),
        &ts(d.valid_to),
    ]
    .join("\u{1f}");
    sha256_hex(key.as_bytes())[..32].to_string()
}

fn row(cfg: &SourceConfig, v: &Version, d: &Draft, o: &Open, superseded: Option<(DateTime<Utc>, &str)>) -> AssertionRow {
    AssertionRow {
        assertion_id: o.assertion_id.clone(),
        source_id: cfg.source_id.clone(),
        source_record_id: v.source_record_id.clone(),
        subject_id: v.subject_id.clone(),
        attribute: d.attribute.to_string(),
        value_original: d.value_original.clone(),
        value_normalized: d.value_normalized.clone(),
        value_type: d.value_type.to_string(),
        qualifier: d.qualifier.clone(),
        check_status: d.check_status.map(String::from),
        valid_from: d.valid_from,
        valid_to: d.valid_to,
        recorded_at: o.recorded_at,
        superseded_at: superseded.map(|s| s.0),
        source_published_at: o.published_at,
        assertion_status: superseded.map_or("active", |s| s.1).to_string(),
        provenance_record_id: o.provenance.clone(),
    }
}

/// Every assertion `versions` (one source's bronze rows, sorted by record
/// then time) imply, active and historical.
pub fn derive(cfg: &SourceConfig, adapter: Adapter, versions: &[Version]) -> Vec<AssertionRow> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < versions.len() {
        let id = &versions[i].source_record_id;
        let mut j = i;
        while j < versions.len() && &versions[j].source_record_id == id {
            j += 1;
        }
        let mut open: BTreeMap<Draft, Open> = BTreeMap::new();
        // The version that opened each claim, to report its subject.
        let mut last = &versions[i];
        for v in &versions[i..j] {
            last = v;
            let drafts: Vec<Draft> = match (&v.change_type[..], &v.raw_content) {
                (change::REMOVED, _) | (_, None) => Vec::new(),
                (_, Some(raw)) => adapter.map(cfg, &fields_of(raw)),
            };
            let status = if v.change_type == change::REMOVED { "withdrawn" } else { "superseded" };
            let keep: std::collections::BTreeSet<&Draft> = drafts.iter().collect();
            let closing: Vec<Draft> = open.keys().filter(|d| !keep.contains(d)).cloned().collect();
            for d in closing {
                let o = open.remove(&d).expect("open claim");
                out.push(row(cfg, v, &d, &o, Some((v.ingested_at, status))));
            }
            for d in drafts {
                open.entry(d.clone()).or_insert_with(|| Open {
                    assertion_id: assertion_id(&cfg.source_id, &v.source_record_id, &v.record_id, &d),
                    recorded_at: v.ingested_at,
                    published_at: v.source_published_at,
                    provenance: v.record_id.clone(),
                });
            }
        }
        for (d, o) in &open {
            out.push(row(cfg, last, d, o, None));
        }
        i = j;
    }
    out.sort_by(|a, b| (&a.subject_id, &a.attribute, &a.assertion_id).cmp(&(&b.subject_id, &b.attribute, &b.assertion_id)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::canonical;

    fn cfg() -> SourceConfig {
        crate::config::Catalogue::load(None).unwrap().get("tc_large_vessels").unwrap().0.clone()
    }

    fn version(n: u32, change: &str, fields: &[(&str, &str)]) -> Version {
        let f: BTreeMap<String, String> = fields.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let (json, hash) = canonical(&f);
        Version {
            record_id: format!("run{n}:1"),
            source_record_id: "834567".into(),
            subject_id: "834567".into(),
            change_type: change.into(),
            record_hash: (change != change::REMOVED).then_some(hash),
            raw_content: (change != change::REMOVED).then_some(json),
            ingested_at: DateTime::from_timestamp(1_700_000_000 + n as i64 * 86_400, 0).unwrap(),
            source_published_at: None,
        }
    }

    fn named<'a>(rows: &'a [AssertionRow], attribute: &str) -> Vec<&'a AssertionRow> {
        rows.iter().filter(|r| r.attribute == attribute).collect()
    }

    #[test]
    fn a_rename_supersedes_the_old_name_and_keeps_it() {
        let v = vec![
            version(1, change::NEW, &[("Official Number", "834567"), ("Vessel Name", "ATLANTIC STAR")]),
            version(2, change::MODIFIED, &[("Official Number", "834567"), ("Vessel Name", "NORDIC STAR")]),
        ];
        let rows = derive(&cfg(), Adapter::TcLarge, &v);
        let names = named(&rows, "name");
        assert_eq!(names.len(), 2);
        let old = names.iter().find(|r| r.value_normalized.as_deref() == Some("ATLANTIC STAR")).unwrap();
        assert_eq!(old.assertion_status, "superseded");
        assert_eq!(old.superseded_at, Some(v[1].ingested_at));
        let new = names.iter().find(|r| r.value_normalized.as_deref() == Some("NORDIC STAR")).unwrap();
        assert_eq!((new.assertion_status.as_str(), new.recorded_at), ("active", v[1].ingested_at));
        // The registration didn't change: one assertion, from the first version.
        let nr = named(&rows, "national_registry");
        assert_eq!(nr.len(), 1);
        assert_eq!((nr[0].recorded_at, nr[0].provenance_record_id.as_str()), (v[0].ingested_at, "run1:1"));
    }

    #[test]
    fn removal_withdraws_and_reappearance_reopens() {
        let f = [("Official Number", "834567"), ("Vessel Name", "ATLANTIC STAR")];
        let v = vec![version(1, change::NEW, &f), version(2, change::REMOVED, &[]), version(3, change::NEW, &f)];
        let rows = derive(&cfg(), Adapter::TcLarge, &v);
        let names = named(&rows, "name");
        assert_eq!(names.len(), 2);
        assert!(names.iter().any(|r| r.assertion_status == "withdrawn" && r.superseded_at == Some(v[1].ingested_at)));
        assert!(names.iter().any(|r| r.assertion_status == "active" && r.recorded_at == v[2].ingested_at));
        assert_ne!(names[0].assertion_id, names[1].assertion_id);
    }

    #[test]
    fn derivation_is_deterministic() {
        let v = vec![version(1, change::NEW, &[("Official Number", "834567"), ("IMO Vessel Number", "9321483")])];
        assert_eq!(derive(&cfg(), Adapter::TcLarge, &v), derive(&cfg(), Adapter::TcLarge, &v));
    }
}
