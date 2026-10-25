//! Reading the registry's own tables back into rows.

use anyhow::Result;
use arrow::record_batch::RecordBatch;

use crate::columns::Reader;
use crate::tables::{AssertionRow, ConflictRow, DecisionRow, ProposalRow, RedirectRow, RunRow, SubjectRow};

macro_rules! each_row {
    ($batches:expr, |$r:ident, $i:ident| $body:expr) => {{
        let mut out = Vec::new();
        for b in $batches {
            let $r = Reader::new(b);
            for $i in 0..$r.rows() {
                out.push($body);
            }
        }
        out
    }};
}

pub fn assertions(batches: &[RecordBatch]) -> Result<Vec<AssertionRow>> {
    let mut out = Vec::new();
    for b in batches {
        let r = Reader::new(b);
        let s = |c: &str| r.str(c);
        let (id, src, srid, subj, attr, vo, vn, vt, q, cs, st, prov) = (
            s("assertion_id")?,
            s("source_id")?,
            s("source_record_id")?,
            s("subject_id")?,
            s("attribute")?,
            s("value_original")?,
            s("value_normalized")?,
            s("value_type")?,
            s("qualifier")?,
            s("check_status")?,
            s("assertion_status")?,
            s("provenance_record_id")?,
        );
        let (vf, vto, rec, sup, publ) = (r.ts("valid_from")?, r.ts("valid_to")?, r.ts("recorded_at")?, r.ts("superseded_at")?, r.ts("source_published_at")?);
        for i in 0..r.rows() {
            out.push(AssertionRow {
                assertion_id: id(i).unwrap_or_default(),
                source_id: src(i).unwrap_or_default(),
                source_record_id: srid(i).unwrap_or_default(),
                subject_id: subj(i).unwrap_or_default(),
                attribute: attr(i).unwrap_or_default(),
                value_original: vo(i),
                value_normalized: vn(i),
                value_type: vt(i).unwrap_or_default(),
                qualifier: q(i),
                check_status: cs(i),
                valid_from: vf(i),
                valid_to: vto(i),
                recorded_at: rec(i).expect("recorded_at is required"),
                superseded_at: sup(i),
                source_published_at: publ(i),
                assertion_status: st(i).unwrap_or_default(),
                provenance_record_id: prov(i).unwrap_or_default(),
            });
        }
    }
    Ok(out)
}

pub fn subjects(batches: &[RecordBatch]) -> Result<Vec<SubjectRow>> {
    let mut out = Vec::new();
    for b in batches {
        let r = Reader::new(b);
        let (src, subj, vid, by) = (r.str("source_id")?, r.str("subject_id")?, r.str("vessel_id")?, r.str("linked_by")?);
        let (first, upd) = (r.ts("first_linked_at")?, r.ts("updated_at")?);
        for i in 0..r.rows() {
            out.push(SubjectRow {
                source_id: src(i).unwrap_or_default(),
                subject_id: subj(i).unwrap_or_default(),
                vessel_id: vid(i).unwrap_or_default(),
                linked_by: by(i).unwrap_or_default(),
                first_linked_at: first(i).expect("required"),
                updated_at: upd(i).expect("required"),
            });
        }
    }
    Ok(out)
}

pub fn redirects(batches: &[RecordBatch]) -> Result<Vec<RedirectRow>> {
    let mut out = Vec::new();
    for b in batches {
        let r = Reader::new(b);
        let (ret, sur, dec, subj) = (r.str("retired_vessel_id")?, r.str("surviving_vessel_id")?, r.str("decision_id")?, r.list("retired_subjects")?);
        let (at, restored) = (r.ts("retired_at")?, r.ts("restored_at")?);
        let active = r.str("active")?;
        for i in 0..r.rows() {
            out.push(RedirectRow {
                retired_vessel_id: ret(i).unwrap_or_default(),
                surviving_vessel_id: sur(i).unwrap_or_default(),
                decision_id: dec(i).unwrap_or_default(),
                retired_at: at(i).expect("required"),
                retired_subjects: subj(i),
                active: active(i).as_deref() == Some("true"),
                restored_at: restored(i),
            });
        }
    }
    Ok(out)
}

pub fn decisions(batches: &[RecordBatch]) -> Result<Vec<DecisionRow>> {
    let mut out = Vec::new();
    for b in batches {
        let r = Reader::new(b);
        let (id, ty, actor, reason, revokes) =
            (r.str("decision_id")?, r.str("decision_type")?, r.str("actor")?, r.str("reason")?, r.str("revokes_decision_id")?);
        let (subj, vids, ev) = (r.list("subjects")?, r.list("vessel_ids")?, r.list("evidence")?);
        let at = r.ts("decided_at")?;
        for i in 0..r.rows() {
            out.push(DecisionRow {
                decision_id: id(i).unwrap_or_default(),
                decision_type: ty(i).unwrap_or_default(),
                decided_at: at(i).expect("required"),
                actor: actor(i).unwrap_or_default(),
                subjects: subj(i),
                vessel_ids: vids(i),
                evidence: ev(i),
                reason: reason(i),
                revokes_decision_id: revokes(i),
            });
        }
    }
    Ok(out)
}

pub fn conflicts(batches: &[RecordBatch]) -> Result<Vec<ConflictRow>> {
    let mut out = Vec::new();
    for b in batches {
        let r = Reader::new(b);
        let s = |c: &str| r.str(c);
        let (id, vid, ty, at, sev, st, dec) = (
            s("conflict_id")?,
            s("vessel_id")?,
            s("conflict_type")?,
            s("attribute")?,
            s("severity")?,
            s("resolution_status")?,
            s("resolution_decision_id")?,
        );
        let (vals, aids, sids, rel) = (r.list("conflicting_values")?, r.list("assertion_ids")?, r.list("source_ids")?, r.list("related_vessel_ids")?);
        let (det, seen, cleared) = (r.ts("detected_at")?, r.ts("last_seen_at")?, r.ts("cleared_at")?);
        for i in 0..r.rows() {
            out.push(ConflictRow {
                conflict_id: id(i).unwrap_or_default(),
                vessel_id: vid(i).unwrap_or_default(),
                conflict_type: ty(i).unwrap_or_default(),
                attribute: at(i).unwrap_or_default(),
                conflicting_values: vals(i),
                assertion_ids: aids(i),
                source_ids: sids(i),
                related_vessel_ids: rel(i),
                severity: sev(i).unwrap_or_default(),
                resolution_status: st(i).unwrap_or_default(),
                resolution_decision_id: dec(i),
                detected_at: det(i).expect("required"),
                last_seen_at: seen(i).expect("required"),
                cleared_at: cleared(i),
            });
        }
    }
    Ok(out)
}

pub fn proposals(batches: &[RecordBatch]) -> Result<Vec<ProposalRow>> {
    let mut out = Vec::new();
    for b in batches {
        let r = Reader::new(b);
        let (id, ty, a, bb, cat, ev) =
            (r.str("proposal_id")?, r.str("proposal_type")?, r.str("vessel_id_a")?, r.str("vessel_id_b")?, r.str("category")?, r.list("evidence")?);
        let (first, last) = (r.ts("first_proposed_at")?, r.ts("last_seen_at")?);
        for i in 0..r.rows() {
            out.push(ProposalRow {
                proposal_id: id(i).unwrap_or_default(),
                proposal_type: ty(i).unwrap_or_default(),
                vessel_id_a: a(i).unwrap_or_default(),
                vessel_id_b: bb(i).unwrap_or_default(),
                category: cat(i).unwrap_or_default(),
                evidence: ev(i),
                first_proposed_at: first(i).expect("required"),
                last_seen_at: last(i).expect("required"),
            });
        }
    }
    Ok(out)
}

/// The fields of `ingestion_runs` the collector needs back.
pub fn runs(batches: &[RecordBatch]) -> Result<Vec<RunRow>> {
    Ok(each_row!(batches, |r, i| {
        RunRow {
            run_id: r.str("run_id")?(i).unwrap_or_default(),
            source_id: r.str("source_id")?(i).unwrap_or_default(),
            status: r.str("status")?(i).unwrap_or_default(),
            started_at: r.ts("started_at")?(i).expect("required"),
            finished_at: r.ts("finished_at")?(i),
            source_version: r.str("source_version")?(i),
            source_published_at: r.ts("source_published_at")?(i),
            content_hash: r.str("content_hash")?(i),
            parser_version: r.str("parser_version")?(i).unwrap_or_default(),
            config_hash: r.str("config_hash")?(i).unwrap_or_default(),
            records_read: r.long("records_read")?(i),
            records_new: r.long("records_new")?(i),
            records_modified: r.long("records_modified")?(i),
            records_removed: r.long("records_removed")?(i),
            message: r.str("message")?(i),
            ..Default::default()
        }
    }))
}
