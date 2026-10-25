//! Deterministic identity reconciliation: which source subjects are the same
//! physical vessel, which permanent id each vessel keeps, and the gold
//! tables derived from that.
//!
//! **Linking.** Subjects are joined only on independently supported,
//! persistent identifiers: a check-digit-valid IMO number from a registry
//! source, or a national registration within its issuing jurisdiction, plus
//! manual `LINK` decisions. A join that would put two different valid IMO
//! numbers on one vessel, or two subjects a `KEEP_APART` decision separates,
//! is refused and recorded as a conflict. Names, call signs and MMSIs never
//! join anything on their own; they only produce proposals for review.
//!
//! **Permanent ids.** Each vessel id is a random UUID. A cluster keeps the
//! id its subjects had; when a cluster holds subjects of several vessels,
//! the one with the most keeps its id and the others are retired with a
//! redirect (MERGE). When a vessel's subjects come apart (a KEEP_APART, a
//! source correcting an IMO number), the piece holding the subjects a
//! retired id once had gets that id back (RESTORE); a piece with none gets
//! a new id (SPLIT). So merges are reversible, and an old id always resolves.
//!
//! Everything here is a pure function of its inputs, for testing.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};

use crate::fetch::sha256_hex;
use crate::sources::attr;
use crate::tables::{
    AssertionRow, ConflictRow, DecisionRow, HistoryRow, IdentifierRow, ProposalRow, RedirectRow, SubjectRow,
    VesselAttributeRow, VesselRow,
};

pub const ALGORITHM_VERSION: &str = "deterministic/1";

/// What reconciliation needs to know about a source.
#[derive(Clone, Debug)]
pub struct SourceInfo {
    pub priority: i32,
    /// `trust = "registry"`: its identifiers are independently supported.
    pub registry: bool,
    /// It issues national registrations itself.
    pub issues_national_registry: bool,
    /// Where it issues them; a registration it reports for another
    /// jurisdiction is not its own. `None` takes every one as its own.
    pub jurisdiction: Option<String>,
    pub publisher: String,
}

impl SourceInfo {
    /// Whether this source issued the national registration `a` reports.
    fn issued(&self, a: &AssertionRow) -> bool {
        self.issues_national_registry
            && self.jurisdiction.as_deref().is_none_or(|j| a.qualifier.as_deref().is_some_and(|q| q.eq_ignore_ascii_case(j)))
    }
}

#[derive(Default)]
pub struct Inputs {
    pub assertions: Vec<AssertionRow>,
    pub subjects: Vec<SubjectRow>,
    pub redirects: Vec<RedirectRow>,
    pub decisions: Vec<DecisionRow>,
    pub conflicts: Vec<ConflictRow>,
    pub proposals: Vec<ProposalRow>,
}

#[derive(Default, Debug)]
pub struct Stats {
    pub assertions: usize,
    pub subjects: usize,
    pub vessels: usize,
    pub created: usize,
    pub merges: usize,
    pub splits: usize,
    pub restores: usize,
    pub blocked: usize,
    pub conflicts_open: usize,
    pub proposals: usize,
}

#[derive(Default)]
pub struct Output {
    pub vessels: Vec<VesselRow>,
    pub attributes: Vec<VesselAttributeRow>,
    pub identifiers: Vec<IdentifierRow>,
    pub history: Vec<HistoryRow>,
    pub subjects: Vec<SubjectRow>,
    pub redirects: Vec<RedirectRow>,
    pub conflicts: Vec<ConflictRow>,
    pub proposals: Vec<ProposalRow>,
    /// Automated decisions made by this run, not yet in the decision log.
    pub decisions: Vec<DecisionRow>,
    pub stats: Stats,
}

pub fn subject_key(source_id: &str, subject_id: &str) -> String {
    format!("{source_id}/{subject_id}")
}

fn hash_id(parts: &[&str]) -> String {
    sha256_hex(parts.join("\u{1f}").as_bytes())[..32].to_string()
}

/// Claims that describe a record's latest state: still asserted, or the
/// last word of a record since removed. Superseded claims are history.
fn is_latest(a: &AssertionRow) -> bool {
    a.assertion_status == "active" || a.assertion_status == "withdrawn"
}

// ---------------------------------------------------------------------------
// Union-find with the merge guards.

struct Sets {
    parent: Vec<usize>,
    imos: Vec<BTreeSet<String>>,
    /// (decision index, side) for each KEEP_APART a member is part of.
    apart: Vec<BTreeSet<(usize, u8)>>,
    linked_by: Vec<Option<&'static str>>,
}

enum Blocked {
    Imo(BTreeSet<String>, BTreeSet<String>),
    KeepApart,
}

impl Sets {
    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize, reason: &'static str, check_imo: bool) -> Result<(), Blocked> {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return Ok(());
        }
        if self.apart[ra].iter().any(|&(d, s)| self.apart[rb].contains(&(d, 1 - s))) {
            return Err(Blocked::KeepApart);
        }
        if check_imo && !self.imos[ra].is_empty() && !self.imos[rb].is_empty() && self.imos[ra] != self.imos[rb] {
            return Err(Blocked::Imo(self.imos[ra].clone(), self.imos[rb].clone()));
        }
        let (keep, gone) = if ra < rb { (ra, rb) } else { (rb, ra) };
        self.parent[gone] = keep;
        let imos = std::mem::take(&mut self.imos[gone]);
        self.imos[keep].extend(imos);
        let apart = std::mem::take(&mut self.apart[gone]);
        self.apart[keep].extend(apart);
        for x in [a, b] {
            self.linked_by[x].get_or_insert(reason);
        }
        Ok(())
    }
}

struct BlockedMerge {
    kind: &'static str,
    value: String,
    a: usize,
    b: usize,
    imos_a: Vec<String>,
    imos_b: Vec<String>,
}

// ---------------------------------------------------------------------------

fn ordering_key<'a>(a: &'a AssertionRow, sources: &HashMap<String, SourceInfo>) -> impl Ord + 'a {
    use std::cmp::Reverse;
    let priority = sources.get(&a.source_id).map_or(0, |s| s.priority);
    (
        Reverse(a.assertion_status == "active"),
        Reverse(priority),
        Reverse(a.source_published_at),
        Reverse(a.valid_from),
        Reverse(a.recorded_at),
        &a.assertion_id,
    )
}

/// The best-supported value of `attribute` among `rows`: from active claims
/// before withdrawn ones, then higher-priority sources, then newer.
fn pick<'a>(rows: &[&'a AssertionRow], attribute: &str, sources: &HashMap<String, SourceInfo>) -> Option<&'a AssertionRow> {
    rows.iter()
        .copied()
        .filter(|a| a.attribute == attribute && is_latest(a) && a.value_normalized.is_some())
        .filter(|a| a.attribute != attr::IMO || a.check_status.as_deref() == Some("valid"))
        .min_by(|x, y| ordering_key(x, sources).cmp(&ordering_key(y, sources)))
}

fn display(a: &AssertionRow) -> String {
    match a.attribute.as_str() {
        attr::NAME | attr::FORMER_NAME => a.value_original.clone().or_else(|| a.value_normalized.clone()),
        _ => a.value_normalized.clone().or_else(|| a.value_original.clone()),
    }
    .unwrap_or_default()
}

fn operational_status(rows: &[&AssertionRow], sources: &HashMap<String, SourceInfo>) -> &'static str {
    let latest = |attribute: &'static str| {
        rows.iter().filter(move |a| a.attribute == attribute && a.assertion_status == "active").filter_map(|a| a.value_normalized.as_deref())
    };
    let scrapped = |v: &str| ["SCRAP", "RECYCLING", "DEMOLISH", "BROKEN UP"].iter().any(|w| v.contains(w));
    if latest(attr::CLASS_STATUS_REASON).chain(latest(attr::REGISTRY_STATUS)).any(scrapped) {
        return "scrapped";
    }
    if let Some(s) = pick(rows, attr::REGISTRY_STATUS, sources).and_then(|a| a.value_normalized.as_deref()) {
        match s {
            "ACTIVE" | "REGISTERED" => return "active",
            s if s.contains("INACTIVE") || s.contains("CLOSED") || s.contains("DELETED") => return "inactive",
            _ => {}
        }
    }
    match pick(rows, attr::CLASS_STATUS, sources).and_then(|a| a.value_normalized.as_deref()) {
        Some("DELIVERED" | "REINSTATED" | "REASSIGNED") => "active",
        _ => "unknown",
    }
}

fn identifier_type(attribute: &str) -> &'static str {
    match attribute {
        attr::IMO => "IMO",
        attr::MMSI => "MMSI",
        attr::CALLSIGN => "CALLSIGN",
        attr::NATIONAL_REGISTRY => "NATIONAL_REGISTRY",
        attr::PSIX_VESSEL_ID => "PSIX_VESSEL_ID",
        _ => "HULL_NUMBER",
    }
}

const HISTORY_ATTRIBUTES: &[&str] = &[
    attr::NAME,
    attr::FORMER_NAME,
    attr::FLAG,
    attr::PORT_OF_REGISTRY,
    attr::VESSEL_TYPE,
    attr::REGISTRY_STATUS,
    attr::CLASS_SOCIETY,
    attr::CLASS_STATUS,
];

/// Values compared across sources for conflicts: (attribute, conflict type,
/// severity, how far apart counts as different).
const COMPARED: &[(&str, &str, &str)] = &[
    (attr::NAME, "NAME", "informational"),
    (attr::FLAG, "FLAG", "moderate"),
    (attr::YEAR_BUILT, "YEAR_BUILT", "moderate"),
    (attr::LENGTH_M, "DIMENSIONS", "informational"),
];

fn differ(attribute: &str, a: &str, b: &str) -> bool {
    match attribute {
        attr::YEAR_BUILT => match (a.parse::<i32>(), b.parse::<i32>()) {
            (Ok(x), Ok(y)) => (x - y).abs() > 1,
            _ => a != b,
        },
        attr::LENGTH_M => match (a.parse::<f64>(), b.parse::<f64>()) {
            // Sources measure length differently (overall, registered,
            // between perpendiculars), so only a wide gap counts.
            (Ok(x), Ok(y)) => (x - y).abs() / x.max(y) > 0.10,
            _ => a != b,
        },
        _ => a != b,
    }
}

// ---------------------------------------------------------------------------

pub fn reconcile(inputs: &Inputs, sources: &HashMap<String, SourceInfo>, now: DateTime<Utc>, new_id: &mut dyn FnMut() -> String) -> Output {
    let actor = format!("reconcile/{ALGORITHM_VERSION}");
    let mut out = Output::default();
    out.stats.assertions = inputs.assertions.len();

    // Subjects, in a fixed order.
    let mut by_subject: BTreeMap<String, Vec<&AssertionRow>> = BTreeMap::new();
    for a in &inputs.assertions {
        by_subject.entry(subject_key(&a.source_id, &a.subject_id)).or_default().push(a);
    }
    let keys: Vec<String> = by_subject.keys().cloned().collect();
    let index: HashMap<&str, usize> = keys.iter().enumerate().map(|(i, k)| (k.as_str(), i)).collect();
    let rows: Vec<&Vec<&AssertionRow>> = by_subject.values().collect();
    let n = keys.len();
    out.stats.subjects = n;

    // Linking keys.
    let registry = |s: &str| sources.get(s).is_some_and(|i| i.registry);
    let mut imo_keys: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut nr_keys: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut sets = Sets {
        parent: (0..n).collect(),
        imos: vec![BTreeSet::new(); n],
        apart: vec![BTreeSet::new(); n],
        linked_by: vec![None; n],
    };
    for (i, rs) in rows.iter().enumerate() {
        for a in rs.iter().filter(|a| is_latest(a)) {
            let Some(v) = a.value_normalized.as_deref() else { continue };
            if a.attribute == attr::IMO && a.check_status.as_deref() == Some("valid") && registry(&a.source_id) {
                imo_keys.entry(v.to_string()).or_default().push(i);
                sets.imos[i].insert(v.to_string());
            } else if a.attribute == attr::NATIONAL_REGISTRY {
                let j = a.qualifier.as_deref().unwrap_or("?");
                nr_keys.entry(format!("{j}:{v}")).or_default().push(i);
            }
        }
    }

    // Manual decisions still in force.
    let revoked: HashSet<&str> = inputs
        .decisions
        .iter()
        .filter(|d| d.decision_type == "REVOKE")
        .filter_map(|d| d.revokes_decision_id.as_deref())
        .collect();
    let mut manual: Vec<&DecisionRow> = inputs
        .decisions
        .iter()
        .filter(|d| matches!(d.decision_type.as_str(), "KEEP_APART" | "LINK") && !revoked.contains(d.decision_id.as_str()))
        .collect();
    manual.sort_by(|a, b| (a.decided_at, &a.decision_id).cmp(&(b.decided_at, &b.decision_id)));
    let pair = |d: &DecisionRow| -> Option<(usize, usize)> {
        let [a, b] = d.subjects.as_slice() else { return None };
        Some((*index.get(a.as_str())?, *index.get(b.as_str())?))
    };
    let mut apart_pairs: Vec<(usize, usize)> = Vec::new();
    for (di, d) in manual.iter().enumerate() {
        if d.decision_type == "KEEP_APART" {
            if let Some((a, b)) = pair(d) {
                sets.apart[a].insert((di, 0));
                sets.apart[b].insert((di, 1));
                apart_pairs.push((a, b));
            }
        }
    }

    // Join: IMO numbers, then national registrations, then manual links.
    let mut blocked: Vec<BlockedMerge> = Vec::new();
    for (kind, reason, keys) in [("imo", "IMO", &imo_keys), ("national_registry", "NATIONAL_REGISTRY", &nr_keys)] {
        for (value, members) in keys {
            for &m in &members[1..] {
                match sets.union(members[0], m, reason, true) {
                    Ok(()) => {}
                    Err(Blocked::Imo(x, y)) => blocked.push(BlockedMerge {
                        kind,
                        value: value.clone(),
                        a: members[0],
                        b: m,
                        imos_a: x.into_iter().collect(),
                        imos_b: y.into_iter().collect(),
                    }),
                    Err(Blocked::KeepApart) => {}
                }
            }
        }
    }
    for d in &manual {
        if d.decision_type == "LINK" {
            if let Some((a, b)) = pair(d) {
                let _ = sets.union(a, b, "DECISION", false);
            }
        }
    }
    out.stats.blocked = blocked.len();

    // Clusters, ordered by their first subject.
    let mut clusters: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for i in 0..n {
        let r = sets.find(i);
        clusters.entry(r).or_default().push(i);
    }
    let mut clusters: Vec<Vec<usize>> = clusters.into_values().collect();
    clusters.sort_by_key(|c| c[0]);

    // ---- permanent ids ------------------------------------------------------
    let mut redirects: BTreeMap<String, RedirectRow> =
        inputs.redirects.iter().map(|r| (r.retired_vessel_id.clone(), r.clone())).collect();
    let resolve = |redirects: &BTreeMap<String, RedirectRow>, id: &str| -> String {
        let mut cur = id.to_string();
        for _ in 0..64 {
            match redirects.get(&cur) {
                Some(r) if r.active => cur = r.surviving_vessel_id.clone(),
                _ => break,
            }
        }
        cur
    };
    let prev_of: HashMap<String, &SubjectRow> =
        inputs.subjects.iter().map(|s| (subject_key(&s.source_id, &s.subject_id), s)).collect();
    // cur: the subject's id now; orig: the id it had before any merge.
    let mut cur: Vec<Option<String>> = vec![None; n];
    let mut orig: Vec<Option<String>> = vec![None; n];
    let mut retired_by_subject: HashMap<&str, Vec<&RedirectRow>> = HashMap::new();
    for r in inputs.redirects.iter().filter(|r| r.active) {
        for s in &r.retired_subjects {
            retired_by_subject.entry(s.as_str()).or_default().push(r);
        }
    }
    for i in 0..n {
        let Some(p) = prev_of.get(&keys[i]) else { continue };
        let c = resolve(&redirects, &p.vessel_id);
        let o = retired_by_subject
            .get(keys[i].as_str())
            .and_then(|rs| {
                rs.iter()
                    .filter(|r| resolve(&redirects, &r.surviving_vessel_id) == c)
                    .max_by_key(|r| (r.retired_at, r.retired_vessel_id.clone()))
                    .map(|r| r.retired_vessel_id.clone())
            })
            .unwrap_or_else(|| c.clone());
        cur[i] = Some(c);
        orig[i] = Some(o);
    }
    let live: BTreeSet<String> = cur.iter().flatten().cloned().collect();

    // How strongly each cluster is identified: its distinct linking keys.
    let strength: Vec<usize> = clusters
        .iter()
        .map(|c| {
            let mut k: HashSet<String> = HashSet::new();
            for &i in c {
                for a in rows[i].iter().filter(|a| is_latest(a)) {
                    let strong = (a.attribute == attr::IMO && a.check_status.as_deref() == Some("valid")) || a.attribute == attr::NATIONAL_REGISTRY;
                    if strong {
                        k.insert(format!("{}={}", a.attribute, a.value_normalized.as_deref().unwrap_or("")));
                    }
                }
            }
            k.len()
        })
        .collect();

    // Who owns each live id: the cluster with most of its original subjects,
    // then most of its current ones, then the most strongly identified.
    let mut owner: HashMap<String, usize> = HashMap::new();
    {
        let mut best: HashMap<&str, (usize, usize, usize, usize)> = HashMap::new(); // id -> (orig, cur, strength, cluster)
        for (ci, c) in clusters.iter().enumerate() {
            let mut counts: HashMap<&str, (usize, usize)> = HashMap::new();
            for &i in c {
                if let Some(o) = orig[i].as_deref().filter(|o| live.contains(*o)) {
                    counts.entry(o).or_default().0 += 1;
                }
                if let Some(x) = cur[i].as_deref() {
                    counts.entry(x).or_default().1 += 1;
                }
            }
            for (id, (oc, cc)) in counts {
                let e = best.entry(id).or_insert((oc, cc, strength[ci], ci));
                if (oc, cc, strength[ci]) > (e.0, e.1, e.2) {
                    *e = (oc, cc, strength[ci], ci);
                }
            }
        }
        for (id, (_, _, _, ci)) in best {
            owner.insert(id.to_string(), ci);
        }
    }

    let mut vessel_of_cluster: Vec<String> = Vec::with_capacity(clusters.len());
    let mut used_retired: HashSet<String> = HashSet::new();
    let mut seen_decisions: HashSet<String> = inputs.decisions.iter().map(|d| d.decision_id.clone()).collect();
    let mut decide = |out: &mut Output, d: DecisionRow| {
        if seen_decisions.insert(d.decision_id.clone()) {
            out.decisions.push(d);
        }
    };
    for (ci, c) in clusters.iter().enumerate() {
        let subj: Vec<String> = c.iter().map(|&i| keys[i].clone()).collect();
        let mut owned: Vec<(usize, String)> = Vec::new();
        {
            let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
            for &i in c {
                if let Some(x) = cur[i].as_deref() {
                    if owner.get(x) == Some(&ci) {
                        *counts.entry(x).or_default() += 1;
                    }
                }
            }
            for (id, k) in counts {
                owned.push((k, id.to_string()));
            }
            owned.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        }
        let id = if let Some((_, survivor)) = owned.first() {
            for (_, retired) in &owned[1..] {
                let retired_subjects: Vec<String> =
                    c.iter().filter(|&&i| orig[i].as_deref() == Some(retired)).map(|&i| keys[i].clone()).collect();
                // Includes when the id was last restored, so merging again
                // after a restore is a new decision, but a rerun isn't.
                let restored = redirects.get(retired).and_then(|r| r.restored_at).map(|t| t.to_rfc3339()).unwrap_or_default();
                let decision_id = hash_id(&["MERGE", retired, survivor, &restored]);
                let evidence = link_evidence(c, &rows, &sets.linked_by);
                decide(
                    &mut out,
                    DecisionRow {
                        decision_id: decision_id.clone(),
                        decision_type: "MERGE".into(),
                        decided_at: now,
                        actor: actor.clone(),
                        subjects: retired_subjects.clone(),
                        vessel_ids: vec![retired.clone(), survivor.clone()],
                        evidence,
                        reason: Some(format!("{retired} joined {survivor} on a shared persistent identifier")),
                        revokes_decision_id: None,
                    },
                );
                redirects.insert(
                    retired.clone(),
                    RedirectRow {
                        retired_vessel_id: retired.clone(),
                        surviving_vessel_id: survivor.clone(),
                        decision_id,
                        retired_at: now,
                        retired_subjects,
                        active: true,
                        restored_at: None,
                    },
                );
                out.stats.merges += 1;
            }
            survivor.clone()
        } else {
            // No live id of its own: a retired id whose subjects these are,
            // or a new one.
            let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
            for &i in c {
                if let Some(o) = orig[i].as_deref().filter(|o| !live.contains(*o) && !used_retired.contains(*o)) {
                    *counts.entry(o).or_default() += 1;
                }
            }
            let restore = counts.into_iter().max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(a.0))).map(|(id, _)| id.to_string());
            match restore {
                Some(r) => {
                    used_retired.insert(r.clone());
                    let from = redirects.get(&r).map(|x| x.surviving_vessel_id.clone()).unwrap_or_default();
                    let decision_id = hash_id(&["RESTORE", &r, &from, &now.to_rfc3339()]);
                    if let Some(x) = redirects.get_mut(&r) {
                        x.active = false;
                        x.restored_at = Some(now);
                    }
                    decide(
                        &mut out,
                        DecisionRow {
                            decision_id,
                            decision_type: "RESTORE".into(),
                            decided_at: now,
                            actor: actor.clone(),
                            subjects: subj.clone(),
                            vessel_ids: vec![r.clone(), from],
                            evidence: Vec::new(),
                            reason: Some("subjects of a retired vessel id no longer link to the vessel it was merged into".into()),
                            revokes_decision_id: None,
                        },
                    );
                    out.stats.restores += 1;
                    r
                }
                None => {
                    let id = new_id();
                    let before: Vec<String> = c.iter().filter_map(|&i| cur[i].clone()).collect::<BTreeSet<_>>().into_iter().collect();
                    if before.is_empty() {
                        out.stats.created += 1;
                    } else {
                        let mut ids = before.clone();
                        ids.push(id.clone());
                        decide(
                            &mut out,
                            DecisionRow {
                                decision_id: hash_id(&["SPLIT", &before.join(","), &id]),
                                decision_type: "SPLIT".into(),
                                decided_at: now,
                                actor: actor.clone(),
                                subjects: subj.clone(),
                                vessel_ids: ids,
                                evidence: Vec::new(),
                                reason: Some("subjects no longer link to the rest of their vessel".into()),
                                revokes_decision_id: None,
                            },
                        );
                        out.stats.splits += 1;
                    }
                    id
                }
            }
        };
        vessel_of_cluster.push(id);
    }
    let mut vessel_of: Vec<String> = vec![String::new(); n];
    for (ci, c) in clusters.iter().enumerate() {
        for &i in c {
            vessel_of[i] = vessel_of_cluster[ci].clone();
        }
    }

    for (i, k) in keys.iter().enumerate() {
        let (source_id, subject_id) = k.split_once('/').expect("subject key");
        let first = prev_of
            .get(k)
            .filter(|p| resolve(&redirects, &p.vessel_id) == vessel_of[i])
            .map_or(now, |p| p.first_linked_at);
        out.subjects.push(SubjectRow {
            source_id: source_id.to_string(),
            subject_id: subject_id.to_string(),
            vessel_id: vessel_of[i].clone(),
            linked_by: sets.linked_by[i].unwrap_or("NONE").to_string(),
            first_linked_at: first,
            updated_at: now,
        });
    }
    out.redirects = redirects.values().cloned().collect();

    // ---- conflicts found this run ----------------------------------------------
    let mut found: Vec<ConflictRow> = Vec::new();
    let mut disputed: HashSet<String> = HashSet::new();
    let mut disputed_values: HashSet<(String, String)> = HashSet::new();
    for b in &blocked {
        let (va, vb) = (&vessel_of[b.a], &vessel_of[b.b]);
        let ids: Vec<String> = [b.a, b.b]
            .iter()
            .flat_map(|&i| rows[i].iter().filter(|a| is_latest(a) && (a.attribute == attr::IMO || a.attribute == b.kind)))
            .map(|a| a.assertion_id.clone())
            .collect();
        let mut values = vec![format!("{}={}", b.kind, b.value)];
        values.extend(b.imos_a.iter().chain(&b.imos_b).map(|i| format!("imo={i}")));
        for (mine, other) in [(va, vb), (vb, va)] {
            disputed.insert(mine.clone());
            found.push(ConflictRow {
                conflict_id: hash_id(&["BLOCKED_MERGE", b.kind, &b.value, &keys[b.a.min(b.b)], &keys[b.a.max(b.b)], mine]),
                vessel_id: mine.clone(),
                conflict_type: "BLOCKED_MERGE".into(),
                attribute: b.kind.into(),
                conflicting_values: crate::tables::sorted(values.clone()),
                assertion_ids: crate::tables::sorted(ids.clone()),
                source_ids: crate::tables::sorted([b.a, b.b].iter().map(|&i| keys[i].split('/').next().unwrap_or("").to_string())),
                related_vessel_ids: vec![other.clone()],
                severity: "significant".into(),
                resolution_status: "unresolved".into(),
                resolution_decision_id: None,
                detected_at: now,
                last_seen_at: now,
                cleared_at: None,
            });
        }
    }

    // ---- gold rows per vessel ------------------------------------------------
    for (ci, c) in clusters.iter().enumerate() {
        let vid = &vessel_of_cluster[ci];
        let all: Vec<&AssertionRow> = c.iter().flat_map(|&i| rows[i].iter().copied()).collect();

        // Several valid IMO numbers on one vessel (a manual link, or a
        // record carrying two).
        let imos: BTreeMap<&str, Vec<&AssertionRow>> = all
            .iter()
            .filter(|a| a.attribute == attr::IMO && a.assertion_status == "active" && a.check_status.as_deref() == Some("valid"))
            .fold(BTreeMap::new(), |mut m, a| {
                m.entry(a.value_normalized.as_deref().unwrap_or("")).or_insert_with(Vec::new).push(a);
                m
            });
        if imos.len() > 1 {
            disputed.insert(vid.clone());
            let ids = crate::tables::sorted(imos.values().flatten().map(|a| a.assertion_id.clone()));
            for v in imos.keys() {
                disputed_values.insert((vid.clone(), format!("IMO:{v}")));
            }
            found.push(ConflictRow {
                conflict_id: hash_id(&["IMO", "imo", &ids.join(",")]),
                vessel_id: vid.clone(),
                conflict_type: "IMO".into(),
                attribute: attr::IMO.into(),
                conflicting_values: imos.keys().map(|s| s.to_string()).collect(),
                assertion_ids: ids,
                source_ids: crate::tables::sorted(imos.values().flatten().map(|a| a.source_id.clone())),
                related_vessel_ids: Vec::new(),
                severity: "significant".into(),
                resolution_status: "unresolved".into(),
                resolution_decision_id: None,
                detected_at: now,
                last_seen_at: now,
                cleared_at: None,
            });
        }

        // Sources disagreeing on a compared value: each source's own pick.
        let source_ids: BTreeSet<&str> = all.iter().map(|a| a.source_id.as_str()).collect();
        for (attribute, kind, severity) in COMPARED {
            let picks: Vec<&AssertionRow> = source_ids
                .iter()
                .filter_map(|s| {
                    let mine: Vec<&AssertionRow> = all.iter().copied().filter(|a| a.source_id == *s && a.assertion_status == "active").collect();
                    pick(&mine, attribute, sources)
                })
                .collect();
            let differs = picks.iter().enumerate().any(|(i, x)| {
                picks[i + 1..].iter().any(|y| differ(attribute, x.value_normalized.as_deref().unwrap_or(""), y.value_normalized.as_deref().unwrap_or("")))
            });
            if differs {
                let ids = crate::tables::sorted(picks.iter().map(|a| a.assertion_id.clone()));
                found.push(ConflictRow {
                    conflict_id: hash_id(&[kind, attribute, &ids.join(",")]),
                    vessel_id: vid.clone(),
                    conflict_type: kind.to_string(),
                    attribute: attribute.to_string(),
                    conflicting_values: crate::tables::sorted(picks.iter().map(|a| format!("{}: {}", a.source_id, display(a)))),
                    assertion_ids: ids,
                    source_ids: crate::tables::sorted(picks.iter().map(|a| a.source_id.clone())),
                    related_vessel_ids: Vec::new(),
                    severity: severity.to_string(),
                    resolution_status: "unresolved".into(),
                    resolution_decision_id: None,
                    detected_at: now,
                    last_seen_at: now,
                    cleared_at: None,
                });
            }
        }

        // Preferred values.
        let mut row = VesselRow { vessel_id: vid.clone(), last_updated_at: now, ..Default::default() };
        let mut chosen = |attribute: &'static str| -> Option<String> {
            let a = pick(&all, attribute, sources)?;
            let value = display(a);
            let norm = a.value_normalized.clone();
            let supporting = crate::tables::sorted(
                all.iter().filter(|x| x.attribute == attribute && is_latest(x) && x.value_normalized == norm).map(|x| x.assertion_id.clone()),
            );
            let alternatives = crate::tables::sorted(
                all.iter()
                    .filter(|x| x.attribute == attribute && is_latest(x) && x.value_normalized.is_some() && x.value_normalized != norm)
                    .map(|x| display(x)),
            );
            out.attributes.push(VesselAttributeRow {
                vessel_id: vid.clone(),
                attribute: attribute.to_string(),
                value: value.clone(),
                assertion_id: a.assertion_id.clone(),
                source_id: a.source_id.clone(),
                supporting_assertions: supporting,
                alternative_values: alternatives,
            });
            Some(value)
        };
        let num = |v: Option<String>| v.and_then(|s| s.parse::<f64>().ok());
        row.preferred_name = chosen(attr::NAME);
        row.imo_number = chosen(attr::IMO);
        row.preferred_mmsi = chosen(attr::MMSI);
        row.preferred_callsign = chosen(attr::CALLSIGN);
        row.flag_state = chosen(attr::FLAG);
        row.vessel_type = chosen(attr::VESSEL_TYPE);
        row.year_built = chosen(attr::YEAR_BUILT).and_then(|s| s.parse().ok());
        row.length_m = num(chosen(attr::LENGTH_M));
        row.beam_m = num(chosen(attr::BEAM_M));
        row.depth_m = num(chosen(attr::DEPTH_M));
        row.gross_tonnage = num(chosen(attr::GROSS_TONNAGE));
        row.net_tonnage = num(chosen(attr::NET_TONNAGE));
        row.deadweight_tonnes = num(chosen(attr::DEADWEIGHT));
        row.classification_society = chosen(attr::CLASS_SOCIETY);
        row.operational_status = operational_status(&all, sources).to_string();

        let imo_sources: BTreeSet<&str> = all
            .iter()
            .filter(|a| a.attribute == attr::IMO && a.value_normalized == row.imo_number && is_latest(a) && registry(&a.source_id))
            .map(|a| a.source_id.as_str())
            .collect();
        let authoritative = all.iter().any(|a| {
            a.attribute == attr::NATIONAL_REGISTRY
                && a.assertion_status == "active"
                && sources.get(&a.source_id).is_some_and(|s| s.issued(a))
        });
        row.identity_status = if disputed.contains(vid) {
            "disputed"
        } else if (row.imo_number.is_some() && imo_sources.len() >= 2) || authoritative {
            "confirmed"
        } else if row.imo_number.is_some() {
            "probable"
        } else {
            "provisional"
        }
        .to_string();
        row.first_known_at = all.iter().filter_map(|a| a.valid_from).min().or_else(|| all.iter().map(|a| a.recorded_at).min());
        row.last_updated_at = all.iter().map(|a| a.superseded_at.unwrap_or(a.recorded_at)).max().unwrap_or(now);
        row.source_ids = crate::tables::sorted(all.iter().map(|a| a.source_id.clone()));
        row.source_count = row.source_ids.len() as i32;
        row.subject_count = c.len() as i32;

        // Identifiers and history: every value, current and past.
        let mut groups: BTreeMap<(String, String, Option<String>), Vec<&AssertionRow>> = BTreeMap::new();
        for a in &all {
            let Some(v) = a.value_normalized.clone() else { continue };
            if attr::IDENTIFIERS.contains(&a.attribute.as_str()) || HISTORY_ATTRIBUTES.contains(&a.attribute.as_str()) {
                let scope = matches!(a.attribute.as_str(), attr::NATIONAL_REGISTRY | attr::PSIX_VESSEL_ID).then(|| a.qualifier.clone()).flatten();
                groups.entry((a.attribute.clone(), v, scope)).or_default().push(a);
            }
        }
        for ((attribute, value, scope), g) in groups {
            let current = g.iter().any(|a| a.assertion_status == "active");
            let valid_from = g.iter().filter_map(|a| a.valid_from).min();
            let valid_to = if current || g.iter().any(|a| a.valid_to.is_none()) { None } else { g.iter().filter_map(|a| a.valid_to).max() };
            let first = g.iter().map(|a| a.recorded_at).min().unwrap_or(now);
            let last = g.iter().map(|a| a.superseded_at.unwrap_or(a.recorded_at)).max().unwrap_or(now);
            let source_ids = crate::tables::sorted(g.iter().map(|a| a.source_id.clone()));
            let supporting = crate::tables::sorted(g.iter().map(|a| a.assertion_id.clone()));
            if attr::IDENTIFIERS.contains(&attribute.as_str()) {
                let kind = identifier_type(&attribute);
                let check = g.iter().filter_map(|a| a.check_status.clone()).min_by_key(|c| c != "valid");
                let verified = g.iter().any(|a| registry(&a.source_id) && a.check_status.as_deref().is_none_or(|c| c == "valid"));
                let status = if disputed_values.contains(&(vid.clone(), format!("{kind}:{value}"))) {
                    "disputed"
                } else if verified && kind != "HULL_NUMBER" {
                    "verified"
                } else {
                    "reported"
                };
                let authority = match kind {
                    "IMO" => Some("IMO".to_string()),
                    "PSIX_VESSEL_ID" => Some("USCG".to_string()),
                    "NATIONAL_REGISTRY" => g
                        .iter()
                        .find_map(|a| sources.get(&a.source_id).filter(|s| s.issued(a)).map(|s| s.publisher.clone())),
                    _ => None,
                };
                out.identifiers.push(IdentifierRow {
                    vessel_id: vid.clone(),
                    identifier_type: kind.to_string(),
                    identifier_value: value,
                    issuing_authority: authority,
                    issuing_jurisdiction: scope,
                    valid_from,
                    valid_to,
                    identity_status: status.to_string(),
                    check_status: check,
                    is_current: current,
                    first_recorded_at: first,
                    last_recorded_at: last,
                    source_ids,
                    supporting_assertions: supporting,
                });
            } else {
                let latest = g.iter().max_by_key(|a| (a.recorded_at, a.assertion_id.clone())).expect("non-empty");
                out.history.push(HistoryRow {
                    vessel_id: vid.clone(),
                    attribute,
                    value: display(latest),
                    valid_from,
                    valid_to,
                    is_current: current,
                    first_recorded_at: first,
                    last_recorded_at: last,
                    source_ids,
                    supporting_assertions: supporting,
                });
            }
        }
        out.vessels.push(row);
    }
    out.vessels.sort_by(|a, b| a.vessel_id.cmp(&b.vessel_id));

    // ---- conflicts: keep every one ever found ---------------------------------
    let mut prior: BTreeMap<String, ConflictRow> = inputs.conflicts.iter().map(|c| (c.conflict_id.clone(), c.clone())).collect();
    let mut now_ids = HashSet::new();
    for mut f in found {
        if let Some(p) = prior.remove(&f.conflict_id) {
            f.detected_at = p.detected_at;
            if matches!(p.resolution_status.as_str(), "reviewed" | "resolved") {
                f.resolution_status = p.resolution_status;
                f.resolution_decision_id = p.resolution_decision_id;
            }
        }
        now_ids.insert(f.conflict_id.clone());
        out.conflicts.push(f);
    }
    for (_, mut p) in prior {
        p.vessel_id = resolve(&redirects, &p.vessel_id);
        if p.resolution_status == "unresolved" {
            p.resolution_status = "cleared".into();
            p.cleared_at = Some(now);
        }
        out.conflicts.push(p);
    }
    out.conflicts.sort_by(|a, b| a.conflict_id.cmp(&b.conflict_id));
    out.stats.conflicts_open = out.conflicts.iter().filter(|c| c.resolution_status == "unresolved").count();

    // ---- proposals --------------------------------------------------------------
    out.proposals = proposals(&out.vessels, &clusters, &vessel_of_cluster, &rows, &apart_pairs, &vessel_of, &inputs.proposals, now);
    out.stats.proposals = out.proposals.len();
    out.stats.vessels = out.vessels.len();
    out
}

fn link_evidence(c: &[usize], rows: &[&Vec<&AssertionRow>], linked_by: &[Option<&'static str>]) -> Vec<String> {
    let mut ev = BTreeSet::new();
    for &i in c {
        if let Some(reason) = linked_by[i] {
            for a in rows[i].iter().filter(|a| is_latest(a)) {
                let hit = (reason == "IMO" && a.attribute == attr::IMO) || (reason == "NATIONAL_REGISTRY" && a.attribute == attr::NATIONAL_REGISTRY);
                if hit {
                    ev.insert(format!("{} {} ({})", a.attribute, a.value_normalized.as_deref().unwrap_or(""), a.assertion_id));
                }
            }
        }
    }
    ev.into_iter().take(20).collect()
}

/// Buckets with more vessels than this are too ambiguous to propose from.
const MAX_BUCKET: usize = 5;

#[allow(clippy::too_many_arguments)]
fn proposals(
    vessels: &[VesselRow],
    clusters: &[Vec<usize>],
    vessel_of_cluster: &[String],
    rows: &[&Vec<&AssertionRow>],
    apart_pairs: &[(usize, usize)],
    vessel_of: &[String],
    prior: &[ProposalRow],
    now: DateTime<Utc>,
) -> Vec<ProposalRow> {
    let by_id: HashMap<&str, &VesselRow> = vessels.iter().map(|v| (v.vessel_id.as_str(), v)).collect();
    let mut buckets: BTreeMap<String, BTreeSet<&str>> = BTreeMap::new();
    for (ci, c) in clusters.iter().enumerate() {
        let vid = vessel_of_cluster[ci].as_str();
        let v = by_id[vid];
        for a in c.iter().flat_map(|&i| rows[i].iter()).filter(|a| a.assertion_status == "active") {
            let Some(val) = a.value_normalized.as_deref() else { continue };
            match a.attribute.as_str() {
                attr::CALLSIGN => buckets.entry(format!("call sign {val}")).or_default().insert(vid),
                attr::MMSI => buckets.entry(format!("MMSI {val}")).or_default().insert(vid),
                attr::NAME => match v.year_built {
                    Some(y) => buckets.entry(format!("name '{val}' built {y}")).or_default().insert(vid),
                    None => false,
                },
                _ => false,
            };
        }
    }
    let apart: HashSet<(&str, &str)> = apart_pairs
        .iter()
        .map(|&(a, b)| {
            let (x, y) = (vessel_of[a].as_str(), vessel_of[b].as_str());
            if x < y { (x, y) } else { (y, x) }
        })
        .collect();
    let mut evidence: BTreeMap<(&str, &str), Vec<String>> = BTreeMap::new();
    for (key, members) in &buckets {
        if members.len() < 2 || members.len() > MAX_BUCKET {
            continue;
        }
        let m: Vec<&str> = members.iter().copied().collect();
        for i in 0..m.len() {
            for j in i + 1..m.len() {
                let (a, b) = (by_id[m[i]], by_id[m[j]]);
                // Two different valid IMO numbers: two ships.
                if a.imo_number.is_some() && b.imo_number.is_some() && a.imo_number != b.imo_number {
                    continue;
                }
                if apart.contains(&(m[i], m[j])) {
                    continue;
                }
                evidence.entry((m[i], m[j])).or_default().push(format!("same {key}"));
            }
        }
    }
    let prior: HashMap<&str, &ProposalRow> = prior.iter().map(|p| (p.proposal_id.as_str(), p)).collect();
    evidence
        .into_iter()
        .map(|((a, b), ev)| {
            let id = hash_id(&["PROPOSE_MATCH", a, b]);
            let (va, vb) = (by_id[a], by_id[b]);
            let compatible_flag = va.flag_state.is_none() || vb.flag_state.is_none() || va.flag_state == vb.flag_state;
            let category = if ev.len() >= 2 && compatible_flag { "probable" } else { "possible" };
            ProposalRow {
                first_proposed_at: prior.get(id.as_str()).map_or(now, |p| p.first_proposed_at),
                proposal_id: id,
                proposal_type: "PROPOSE_MATCH".into(),
                vessel_id_a: a.to_string(),
                vessel_id_b: b.to_string(),
                category: category.into(),
                evidence: ev,
                last_seen_at: now,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(day: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_760_000_000 + day * 86_400, 0).unwrap()
    }

    fn sources() -> HashMap<String, SourceInfo> {
        let mut m = HashMap::new();
        for (id, prio, natreg) in [("tc", 90, true), ("psix", 80, true), ("iacs", 70, false)] {
            m.insert(id.to_string(), SourceInfo { priority: prio, registry: true, issues_national_registry: natreg, jurisdiction: None, publisher: id.to_uppercase() });
        }
        m
    }

    fn a(source: &str, subject: &str, attribute: &str, value: &str) -> AssertionRow {
        let check = match attribute {
            attr::IMO => crate::normalize::imo(value).map(|x| x.1.as_str().to_string()),
            _ => None,
        };
        AssertionRow {
            assertion_id: hash_id(&[source, subject, attribute, value]),
            source_id: source.into(),
            source_record_id: subject.into(),
            subject_id: subject.into(),
            attribute: attribute.into(),
            value_original: Some(value.into()),
            value_normalized: Some(match attribute {
                attr::NAME => crate::normalize::name_key(value).unwrap(),
                _ => value.into(),
            }),
            value_type: "string".into(),
            qualifier: (attribute == attr::NATIONAL_REGISTRY).then(|| "CA".to_string()),
            check_status: check,
            recorded_at: t(0),
            assertion_status: "active".into(),
            provenance_record_id: "r".into(),
            ..Default::default()
        }
    }

    struct Ids(u32);
    impl Ids {
        fn next(&mut self) -> String {
            self.0 += 1;
            format!("v{:03}", self.0)
        }
    }

    fn run(inputs: &Inputs, ids: &mut Ids, day: i64) -> Output {
        reconcile(inputs, &sources(), t(day), &mut || ids.next())
    }

    /// Carries a run's state into the next run's inputs.
    fn carry(prev: &Output, assertions: Vec<AssertionRow>, decisions: &[DecisionRow]) -> Inputs {
        let mut d = decisions.to_vec();
        d.extend(prev.decisions.iter().cloned());
        Inputs {
            assertions,
            subjects: prev.subjects.clone(),
            redirects: prev.redirects.clone(),
            decisions: d,
            conflicts: prev.conflicts.clone(),
            proposals: prev.proposals.clone(),
        }
    }

    fn vessel_of<'a>(o: &'a Output, source: &str, subject: &str) -> &'a str {
        &o.subjects.iter().find(|s| s.source_id == source && s.subject_id == subject).unwrap().vessel_id
    }

    #[test]
    fn a_registration_from_another_jurisdiction_links_but_does_not_confirm() {
        let mut srcs = sources();
        srcs.insert(
            "misle".into(),
            SourceInfo { priority: 60, registry: true, issues_national_registry: true, jurisdiction: Some("US".into()), publisher: "USCG".into() },
        );
        // A Canadian official number, as the Coast Guard reports it.
        let foreign = a("misle", "ON:822480", attr::NATIONAL_REGISTRY, "822480");
        let alone = reconcile(&Inputs { assertions: vec![foreign.clone()], ..Default::default() }, &srcs, t(1), &mut || "v1".into());
        assert_eq!(alone.vessels[0].identity_status, "provisional");
        assert_eq!(alone.identifiers[0].issuing_authority, None);

        let mut own = a("misle", "USON:1091257", attr::NATIONAL_REGISTRY, "1091257");
        own.qualifier = Some("US".into());
        let o = reconcile(&Inputs { assertions: vec![own], ..Default::default() }, &srcs, t(1), &mut || "v2".into());
        assert_eq!(o.vessels[0].identity_status, "confirmed");
        assert_eq!(o.identifiers[0].issuing_authority.as_deref(), Some("USCG"));

        // The Canadian register's own entry for that number joins it.
        let tc = a("tc", "822480", attr::NATIONAL_REGISTRY, "822480");
        let mut ids = Ids(0);
        let both = reconcile(&Inputs { assertions: vec![foreign, tc], ..Default::default() }, &srcs, t(1), &mut || ids.next());
        assert_eq!(both.vessels.len(), 1);
        assert_eq!(both.vessels[0].identity_status, "confirmed");
    }

    #[test]
    fn same_vessel_in_two_registries_is_one_vessel() {
        let rows = vec![
            a("tc", "834567", attr::NATIONAL_REGISTRY, "834567"),
            a("tc", "834567", attr::IMO, "9321483"),
            a("tc", "834567", attr::NAME, "ATLANTIC STAR"),
            a("tc", "834567", attr::FLAG, "CAN"),
            a("iacs", "9321483", attr::IMO, "9321483"),
            a("iacs", "9321483", attr::NAME, "ATLANTIC STAR"),
            a("iacs", "9321483", attr::CLASS_SOCIETY, "LRS"),
        ];
        let o = run(&Inputs { assertions: rows.clone(), ..Default::default() }, &mut Ids(0), 1);
        assert_eq!(o.vessels.len(), 1);
        let v = &o.vessels[0];
        assert_eq!(v.imo_number.as_deref(), Some("9321483"));
        assert_eq!(v.identity_status, "confirmed");
        assert_eq!(v.source_count, 2);
        assert_eq!(v.classification_society.as_deref(), Some("LRS"));
        assert_eq!(o.stats.created, 1);
        // Every original assertion is still reachable through the subjects.
        assert_eq!(o.subjects.len(), 2);
        let imo = o.identifiers.iter().find(|i| i.identifier_type == "IMO").unwrap();
        assert_eq!((imo.identity_status.as_str(), imo.source_ids.len()), ("verified", 2));
        // Same input again: same ids, nothing new.
        let again = run(&carry(&o, rows, &[]), &mut Ids(100), 2);
        assert_eq!(again.vessels[0].vessel_id, v.vessel_id);
        assert_eq!((again.stats.created, again.stats.merges, again.decisions.len()), (0, 0, 0));
    }

    #[test]
    fn two_vessels_with_the_same_name_stay_apart() {
        let rows = vec![
            a("tc", "1", attr::NATIONAL_REGISTRY, "1"),
            a("tc", "1", attr::NAME, "MISS JENNY"),
            a("tc", "2", attr::NATIONAL_REGISTRY, "2"),
            a("tc", "2", attr::NAME, "MISS JENNY"),
        ];
        let o = run(&Inputs { assertions: rows, ..Default::default() }, &mut Ids(0), 1);
        assert_eq!(o.vessels.len(), 2);
        assert!(o.vessels.iter().all(|v| v.identity_status == "confirmed"));
    }

    #[test]
    fn a_shared_registration_does_not_join_two_imo_numbers() {
        // Two records with one official number but different valid IMOs.
        let rows = vec![
            a("tc", "1", attr::NATIONAL_REGISTRY, "77"),
            a("tc", "1", attr::IMO, "9321483"),
            a("psix", "x", attr::NATIONAL_REGISTRY, "77"),
            a("psix", "x", attr::IMO, "9074729"),
        ];
        let o = run(&Inputs { assertions: rows, ..Default::default() }, &mut Ids(0), 1);
        assert_eq!(o.vessels.len(), 2);
        assert_eq!(o.stats.blocked, 1);
        assert!(o.vessels.iter().all(|v| v.identity_status == "disputed"));
        let c: Vec<&ConflictRow> = o.conflicts.iter().filter(|c| c.conflict_type == "BLOCKED_MERGE").collect();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].related_vessel_ids, [c[1].vessel_id.clone()]);
    }

    #[test]
    fn conflicting_values_are_kept_and_reported() {
        let rows = vec![
            a("tc", "1", attr::IMO, "9321483"),
            a("tc", "1", attr::NATIONAL_REGISTRY, "1"),
            a("tc", "1", attr::FLAG, "CAN"),
            a("tc", "1", attr::YEAR_BUILT, "2005"),
            a("psix", "x", attr::IMO, "9321483"),
            a("psix", "x", attr::FLAG, "PAN"),
            a("psix", "x", attr::YEAR_BUILT, "2006"),
        ];
        let o = run(&Inputs { assertions: rows.clone(), ..Default::default() }, &mut Ids(0), 1);
        assert_eq!(o.vessels.len(), 1);
        let v = &o.vessels[0];
        assert_eq!(v.flag_state.as_deref(), Some("CAN"), "the higher-priority source wins");
        let flag = o.attributes.iter().find(|x| x.attribute == attr::FLAG).unwrap();
        assert_eq!(flag.alternative_values, ["PAN"]);
        let kinds: Vec<&str> = o.conflicts.iter().map(|c| c.conflict_type.as_str()).collect();
        assert_eq!(kinds, ["FLAG"], "a one-year build difference is not a conflict");

        // When the sources agree again, the conflict is cleared, not deleted.
        let mut fixed = rows;
        fixed.retain(|r| !(r.source_id == "psix" && r.attribute == attr::FLAG));
        fixed.push(a("psix", "x", attr::FLAG, "CAN"));
        let o2 = run(&carry(&o, fixed, &[]), &mut Ids(10), 2);
        assert_eq!(o2.conflicts.len(), 1);
        assert_eq!(o2.conflicts[0].resolution_status, "cleared");
        assert_eq!(o2.conflicts[0].cleared_at, Some(t(2)));
    }

    #[test]
    fn an_mmsi_change_keeps_both_identifiers() {
        let mut old = a("psix", "x", attr::MMSI, "316001111");
        old.assertion_status = "superseded".into();
        old.superseded_at = Some(t(5));
        let rows = vec![a("psix", "x", attr::PSIX_VESSEL_ID, "x"), old, a("psix", "x", attr::MMSI, "316002222")];
        let o = run(&Inputs { assertions: rows, ..Default::default() }, &mut Ids(0), 6);
        let mmsi: Vec<&IdentifierRow> = o.identifiers.iter().filter(|i| i.identifier_type == "MMSI").collect();
        assert_eq!(mmsi.len(), 2);
        assert!(mmsi.iter().any(|i| i.identifier_value == "316001111" && !i.is_current && i.last_recorded_at == t(5)));
        assert_eq!(o.vessels[0].preferred_mmsi.as_deref(), Some("316002222"));
    }

    #[test]
    fn merges_are_reversible() {
        // Day 1: two vessels, nothing in common.
        let tc = vec![a("tc", "1", attr::NATIONAL_REGISTRY, "1"), a("tc", "1", attr::NAME, "ATLANTIC STAR")];
        let iacs = vec![a("iacs", "9321483", attr::IMO, "9321483"), a("iacs", "9321483", attr::NAME, "ATLANTIC STAR")];
        let mut rows = tc.clone();
        rows.extend(iacs.clone());
        let mut ids = Ids(0);
        let d1 = run(&Inputs { assertions: rows, ..Default::default() }, &mut ids, 1);
        assert_eq!(d1.vessels.len(), 2);
        assert_eq!(d1.proposals.len(), 0, "a name alone, without a build year, proposes nothing");
        let (v_tc, v_iacs) = (vessel_of(&d1, "tc", "1").to_string(), vessel_of(&d1, "iacs", "9321483").to_string());

        // Day 2: TC adds the IMO number, so the two are one vessel.
        let mut rows = tc.clone();
        rows.push(a("tc", "1", attr::IMO, "9321483"));
        rows.extend(iacs.clone());
        let d2 = run(&carry(&d1, rows.clone(), &[]), &mut ids, 2);
        assert_eq!(d2.vessels.len(), 1);
        assert_eq!(d2.stats.merges, 1);
        let survivor = d2.vessels[0].vessel_id.clone();
        let retired = if survivor == v_tc { &v_iacs } else { &v_tc };
        let r = d2.redirects.iter().find(|r| &r.retired_vessel_id == retired).unwrap();
        assert!(r.active);
        assert_eq!(r.surviving_vessel_id, survivor);
        assert_eq!(d2.decisions.iter().filter(|d| d.decision_type == "MERGE").count(), 1);

        // Day 3: someone decides they are different ships after all.
        let keep_apart = DecisionRow {
            decision_id: "k1".into(),
            decision_type: "KEEP_APART".into(),
            decided_at: t(3),
            actor: "user:test".into(),
            subjects: vec!["tc/1".into(), "iacs/9321483".into()],
            ..Default::default()
        };
        let d3 = run(&carry(&d2, rows.clone(), std::slice::from_ref(&keep_apart)), &mut ids, 3);
        assert_eq!(d3.vessels.len(), 2);
        assert_eq!(vessel_of(&d3, "tc", "1"), v_tc, "each side gets its original id back");
        assert_eq!(vessel_of(&d3, "iacs", "9321483"), v_iacs);
        assert_eq!(d3.stats.restores, 1);
        assert!(!d3.redirects.iter().find(|r| &r.retired_vessel_id == retired).unwrap().active);

        // Day 4: the decision is revoked; they merge again into the same survivor.
        let revoke = DecisionRow {
            decision_id: "r1".into(),
            decision_type: "REVOKE".into(),
            decided_at: t(4),
            actor: "user:test".into(),
            revokes_decision_id: Some("k1".into()),
            ..Default::default()
        };
        let d4 = run(&carry(&d3, rows, &[keep_apart, revoke]), &mut ids, 4);
        assert_eq!(d4.vessels.len(), 1);
        assert_eq!(d4.vessels[0].vessel_id, survivor);
    }

    #[test]
    fn an_identifier_correction_splits_a_vessel() {
        let mut ids = Ids(0);
        let rows = vec![
            a("tc", "1", attr::NATIONAL_REGISTRY, "1"),
            a("tc", "1", attr::IMO, "9321483"),
            a("psix", "x", attr::IMO, "9321483"),
            a("psix", "x", attr::NAME, "OTHER SHIP"),
        ];
        let d1 = run(&Inputs { assertions: rows, ..Default::default() }, &mut ids, 1);
        assert_eq!(d1.vessels.len(), 1);
        // PSIX corrects its record: a different IMO number.
        let rows = vec![
            a("tc", "1", attr::NATIONAL_REGISTRY, "1"),
            a("tc", "1", attr::IMO, "9321483"),
            a("psix", "x", attr::IMO, "9074729"),
            a("psix", "x", attr::NAME, "OTHER SHIP"),
        ];
        let d2 = run(&carry(&d1, rows, &[]), &mut ids, 2);
        assert_eq!(d2.vessels.len(), 2);
        assert_eq!(d2.stats.splits, 1);
        assert_eq!(vessel_of(&d2, "tc", "1"), vessel_of(&d1, "tc", "1"));
    }

    #[test]
    fn shared_call_sign_and_name_propose_a_match() {
        let rows = vec![
            a("tc", "1", attr::NATIONAL_REGISTRY, "1"),
            a("tc", "1", attr::NAME, "ATLANTIC STAR"),
            a("tc", "1", attr::YEAR_BUILT, "1999"),
            a("psix", "x", attr::PSIX_VESSEL_ID, "x"),
            a("psix", "x", attr::NAME, "ATLANTIC STAR"),
            a("psix", "x", attr::YEAR_BUILT, "1999"),
            a("psix", "x", attr::CALLSIGN, "CFA1234"),
        ];
        let o = run(&Inputs { assertions: rows, ..Default::default() }, &mut Ids(0), 1);
        assert_eq!(o.vessels.len(), 2, "proposals never merge");
        assert_eq!(o.proposals.len(), 1);
        assert_eq!(o.proposals[0].category, "possible");
        assert!(o.proposals[0].evidence[0].contains("ATLANTIC STAR"));
    }

    #[test]
    fn a_bad_check_digit_never_links() {
        let rows = vec![
            a("tc", "1", attr::NATIONAL_REGISTRY, "1"),
            a("tc", "1", attr::IMO, "9321487"),
            a("iacs", "9321487", attr::IMO, "9321487"),
        ];
        let o = run(&Inputs { assertions: rows, ..Default::default() }, &mut Ids(0), 1);
        assert_eq!(o.vessels.len(), 2);
        assert!(o.vessels.iter().all(|v| v.imo_number.is_none()));
    }
}
