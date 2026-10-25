//! Running reconciliation against the store, recording manual decisions,
//! and looking vessels up.

use std::collections::{HashMap, HashSet};

use anyhow::{bail, Result};
use chrono::Utc;
use iceberg::Catalog;

use crate::columns::Reader;
use crate::config::Catalogue;
use crate::load;
use crate::normalize;
use crate::reconcile::{self, Inputs, SourceInfo, Stats, ALGORITHM_VERSION};
use crate::tables::{
    DecisionRow, ReconcileRunRow, Store, ASSERTIONS, CONFLICTS, CONFLICT_COLUMNS, DECISIONS, DECISION_COLUMNS, HISTORY_COLUMNS,
    IDENTIFIER_COLUMNS, PROPOSAL_COLUMNS, RECONCILE_RUN_COLUMNS, RECONCILIATION_RUNS, REDIRECTS, REDIRECT_COLUMNS, REVIEW_QUEUE,
    SUBJECT_COLUMNS, VESSELS, VESSEL_ATTRIBUTES, VESSEL_ATTRIBUTE_COLUMNS, VESSEL_COLUMNS, VESSEL_HISTORY, VESSEL_IDENTIFIERS,
    VESSEL_SUBJECTS,
};

/// What reconciliation knows about each source, and the sources whose
/// assertions may be combined with others at all.
fn source_infos(cat: &Catalogue) -> (HashMap<String, SourceInfo>, HashSet<String>) {
    let mut infos = HashMap::new();
    let mut combinable = HashSet::new();
    for (c, _) in cat.sources.values() {
        infos.insert(
            c.source_id.clone(),
            SourceInfo {
                priority: c.priority,
                registry: c.trust == "registry",
                issues_national_registry: c.issues.iter().any(|i| i == "NATIONAL_REGISTRY"),
                jurisdiction: c.jurisdiction.clone(),
                publisher: c.publisher.clone(),
            },
        );
        if c.permits("combine") && !matches!(c.access_status.as_str(), "suspended" | "restricted") {
            combinable.insert(c.source_id.clone());
        }
    }
    (infos, combinable)
}

pub async fn reconcile<C: Catalog>(store: &Store<C>, cat: &Catalogue, apply: bool) -> Result<Stats> {
    let started = Utc::now();
    let (infos, combinable) = source_infos(cat);
    let mut assertions = load::assertions(&store.read(ASSERTIONS, None).await?)?;
    let before = assertions.len();
    let excluded: HashSet<String> = assertions.iter().filter(|a| !combinable.contains(&a.source_id)).map(|a| a.source_id.clone()).collect();
    assertions.retain(|a| combinable.contains(&a.source_id));
    if !excluded.is_empty() {
        println!(
            "excluding {} assertion(s) from sources not configured for combination: {}",
            before - assertions.len(),
            excluded.into_iter().collect::<Vec<_>>().join(", ")
        );
    }
    anyhow::ensure!(!assertions.is_empty(), "no assertions to reconcile; ingest a source first");
    let inputs = Inputs {
        assertions,
        subjects: load::subjects(&store.read(VESSEL_SUBJECTS, None).await?)?,
        redirects: load::redirects(&store.read(REDIRECTS, None).await?)?,
        decisions: load::decisions(&store.read(DECISIONS, None).await?)?,
        conflicts: load::conflicts(&store.read(CONFLICTS, None).await?)?,
        proposals: load::proposals(&store.read(REVIEW_QUEUE, None).await?)?,
    };
    let now = Utc::now();
    let out = tokio::task::spawn_blocking(move || {
        reconcile::reconcile(&inputs, &infos, now, &mut || uuid::Uuid::new_v4().to_string())
    })
    .await?;
    let st = &out.stats;
    println!(
        "{} assertions, {} subjects -> {} vessels ({} new, {} merged, {} split, {} restored); {} merges refused; {} open conflicts; {} proposals",
        st.assertions, st.subjects, st.vessels, st.created, st.merges, st.splits, st.restores, st.blocked, st.conflicts_open, st.proposals
    );
    let mut by_status: std::collections::BTreeMap<&str, usize> = Default::default();
    for v in &out.vessels {
        *by_status.entry(v.identity_status.as_str()).or_default() += 1;
    }
    println!("  identity status: {by_status:?}");
    if !apply {
        return Ok(out.stats);
    }

    // Identity state first (decisions, redirects, the subject map), so an
    // interruption never loses which ids were retired; then the gold
    // tables, which are rebuilt from it.
    store.append(DECISIONS, DECISION_COLUMNS, &out.decisions, None).await?;
    store.replace(REDIRECTS, REDIRECT_COLUMNS, &out.redirects, true).await?;
    store.replace(VESSEL_SUBJECTS, SUBJECT_COLUMNS, &out.subjects, false).await?;
    store.replace(VESSELS, VESSEL_COLUMNS, &out.vessels, false).await?;
    store.replace(VESSEL_ATTRIBUTES, VESSEL_ATTRIBUTE_COLUMNS, &out.attributes, true).await?;
    store.replace(VESSEL_IDENTIFIERS, IDENTIFIER_COLUMNS, &out.identifiers, true).await?;
    store.replace(VESSEL_HISTORY, HISTORY_COLUMNS, &out.history, true).await?;
    store.replace(CONFLICTS, CONFLICT_COLUMNS, &out.conflicts, true).await?;
    store.replace(REVIEW_QUEUE, PROPOSAL_COLUMNS, &out.proposals, true).await?;
    let st = &out.stats;
    let run = ReconcileRunRow {
        run_id: format!("reconcile-{}", started.format("%Y%m%dT%H%M%SZ")),
        started_at: started,
        finished_at: Utc::now(),
        algorithm_version: ALGORITHM_VERSION.into(),
        assertions_read: st.assertions as i64,
        subjects: st.subjects as i64,
        vessels: st.vessels as i64,
        vessels_created: st.created as i64,
        merges: st.merges as i64,
        splits: st.splits as i64,
        restores: st.restores as i64,
        blocked_merges: st.blocked as i64,
        conflicts_open: st.conflicts_open as i64,
        proposals: st.proposals as i64,
        vessels_snapshot_id: store.snapshot_id(VESSELS).await?,
    };
    store.append(RECONCILIATION_RUNS, RECONCILE_RUN_COLUMNS, &[run], None).await?;
    println!("  written: vessels, vessel_attributes, vessel_identifiers, vessel_history, vessel_subjects, vessel_id_redirects, identity_conflicts, identity_review_queue");
    Ok(out.stats)
}

/// A manual decision: `KEEP_APART` or `LINK` two subjects
/// (`<source_id>/<subject_id>`), or `REVOKE` an earlier decision. Takes
/// effect at the next reconcile.
pub async fn decide<C: Catalog>(
    store: Option<&Store<C>>,
    kind: &str,
    subjects: &[String],
    revokes: Option<&str>,
    actor: &str,
    reason: &str,
) -> Result<DecisionRow> {
    if kind != "REVOKE" {
        anyhow::ensure!(subjects.len() == 2 && subjects[0] != subjects[1], "{kind} takes two different subjects");
        for s in subjects {
            anyhow::ensure!(s.split_once('/').is_some_and(|(a, b)| !a.is_empty() && !b.is_empty()), "subject '{s}' is not <source_id>/<subject_id>");
        }
    }
    let now = Utc::now();
    let d = DecisionRow {
        decision_id: format!("{}-{}", kind.to_ascii_lowercase(), uuid::Uuid::new_v4().simple()),
        decision_type: kind.into(),
        decided_at: now,
        actor: format!("user:{actor}"),
        subjects: subjects.to_vec(),
        vessel_ids: Vec::new(),
        evidence: Vec::new(),
        reason: Some(reason.to_string()),
        revokes_decision_id: revokes.map(String::from),
    };
    if let Some(s) = store {
        let known = load::subjects(&s.read(VESSEL_SUBJECTS, None).await?)?;
        let keys: HashSet<String> = known.iter().map(|k| reconcile::subject_key(&k.source_id, &k.subject_id)).collect();
        for x in subjects {
            if !keys.contains(x) {
                bail!("subject '{x}' is not in vessel_subjects (run reconcile first, or check the id)");
            }
        }
        if let Some(r) = revokes {
            let all = load::decisions(&s.read(DECISIONS, None).await?)?;
            anyhow::ensure!(all.iter().any(|d| d.decision_id == r), "no decision '{r}'");
        }
        s.append(DECISIONS, DECISION_COLUMNS, std::slice::from_ref(&d), None).await?;
    }
    Ok(d)
}

/// What to look a vessel up by.
pub enum Query {
    Imo(String),
    Mmsi(String),
    Callsign(String),
    Registration(String),
    Name(String),
    VesselId(String),
}

/// Vessels matching `q` (including through former identifiers and retired
/// ids), printed with their identifiers.
/// The IMO numbers (check-digit valid) and MMSIs in `vessel_identifiers`
/// that some source other than `asking` supports, for a source that is
/// asked about known vessels. Its own earlier answers are left out, so they
/// don't widen the next run's lookups.
pub async fn known_identifiers<C: Catalog>(store: &Store<C>, asking: &str) -> Result<Vec<crate::sources::gfw::Lookup>> {
    let mut out = std::collections::BTreeSet::new();
    for b in &store.read(VESSEL_IDENTIFIERS, None).await? {
        let r = Reader::new(b);
        let (t, v, check, sources) = (r.str("identifier_type")?, r.str("identifier_value")?, r.str("check_status")?, r.list("source_ids")?);
        for i in 0..r.rows() {
            let (Some(kind), Some(value)) = (t(i), v(i)) else { continue };
            let usable = match kind.as_str() {
                "IMO" => check(i).as_deref() == Some("valid"),
                "MMSI" => true,
                _ => false,
            };
            if usable && sources(i).iter().any(|s| s != asking) {
                out.insert(crate::sources::gfw::Lookup { kind, value });
            }
        }
    }
    Ok(out.into_iter().collect())
}

pub async fn lookup<C: Catalog>(store: &Store<C>, q: &Query) -> Result<usize> {
    let ids = store.read(VESSEL_IDENTIFIERS, None).await?;
    let mut hits: HashSet<String> = HashSet::new();
    let want = |kind: &str, value: &str, t: &str, v: &str| t == kind && v == value;
    let (kind, value) = match q {
        Query::Imo(v) => ("IMO", normalize::imo(v).map(|x| x.0).unwrap_or_else(|| v.clone())),
        Query::Mmsi(v) => ("MMSI", v.trim().to_string()),
        Query::Callsign(v) => ("CALLSIGN", normalize::call_sign(v).unwrap_or_else(|| v.clone())),
        Query::Registration(v) => ("NATIONAL_REGISTRY", v.trim().to_ascii_uppercase()),
        Query::Name(v) => ("NAME", normalize::name_key(v).unwrap_or_default()),
        Query::VesselId(v) => ("ID", v.clone()),
    };
    if kind == "ID" {
        let redirects = load::redirects(&store.read(REDIRECTS, None).await?)?;
        let mut cur = value.clone();
        for _ in 0..64 {
            match redirects.iter().find(|r| r.retired_vessel_id == cur && r.active) {
                Some(r) => {
                    println!("{cur} was merged into {}", r.surviving_vessel_id);
                    cur = r.surviving_vessel_id.clone();
                }
                None => break,
            }
        }
        hits.insert(cur);
    } else if kind == "NAME" {
        for b in &store.read(VESSEL_HISTORY, None).await? {
            let r = Reader::new(b);
            let (vid, at, val) = (r.str("vessel_id")?, r.str("attribute")?, r.str("value")?);
            for i in 0..r.rows() {
                let is_name = matches!(at(i).as_deref(), Some("name" | "former_name"));
                if is_name && val(i).and_then(|v| normalize::name_key(&v)).as_deref() == Some(value.as_str()) {
                    hits.extend(vid(i));
                }
            }
        }
    } else {
        for b in &ids {
            let r = Reader::new(b);
            let (vid, t, v) = (r.str("vessel_id")?, r.str("identifier_type")?, r.str("identifier_value")?);
            for i in 0..r.rows() {
                if want(kind, &value, t(i).as_deref().unwrap_or(""), v(i).as_deref().unwrap_or("")) {
                    hits.extend(vid(i));
                }
            }
        }
    }

    let mut found = 0;
    for b in &store.read(VESSELS, None).await? {
        let r = Reader::new(b);
        let vid = r.str("vessel_id")?;
        for i in 0..r.rows() {
            let id = vid(i).unwrap_or_default();
            if !hits.contains(&id) {
                continue;
            }
            found += 1;
            let s = |c: &str| r.str(c).ok().and_then(|f| f(i)).unwrap_or_else(|| "-".into());
            println!(
                "{id}  {}  IMO {}  flag {}  type {}  built {}  {} / {}  sources: {}",
                s("preferred_name"),
                s("imo_number"),
                s("flag_state"),
                s("vessel_type"),
                s("year_built"),
                s("identity_status"),
                s("operational_status"),
                r.list("source_ids").map(|f| f(i).join(",")).unwrap_or_default()
            );
            for ib in &ids {
                let ir = Reader::new(ib);
                let (v2, t, val, st, cur) = (
                    ir.str("vessel_id")?,
                    ir.str("identifier_type")?,
                    ir.str("identifier_value")?,
                    ir.str("identity_status")?,
                    ir.str("is_current")?,
                );
                for j in 0..ir.rows() {
                    if v2(j).as_deref() == Some(id.as_str()) {
                        println!(
                            "    {:<18} {:<14} {}{}",
                            t(j).unwrap_or_default(),
                            val(j).unwrap_or_default(),
                            st(j).unwrap_or_default(),
                            if cur(j).as_deref() == Some("true") { "" } else { " (former)" }
                        );
                    }
                }
            }
        }
    }
    if found == 0 {
        println!("no vessel found");
    }
    Ok(found)
}
