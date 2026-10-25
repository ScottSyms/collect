//! The registry's tables, all in one namespace (`identity` by default). The
//! layers the specification calls bronze, silver and gold are kept as
//! logical layers only, without name prefixes:
//!
//! | Layer      | Table                   | Written by | How                                  |
//! |------------|-------------------------|------------|--------------------------------------|
//! | control    | `source_catalog`        | ingest     | replaced (current configuration)     |
//! | control    | `ingestion_runs`        | ingest     | appended                             |
//! | bronze     | `source_records`        | ingest     | appended, by source and month        |
//! | bronze     | `rejected_records`      | ingest     | appended, by source                  |
//! | silver     | `identity_assertions`   | ingest     | source's partition recomputed        |
//! | gold       | `vessels`               | reconcile  | replaced (materialized current state)|
//! | gold       | `vessel_attributes`     | reconcile  | replaced                             |
//! | gold       | `vessel_identifiers`    | reconcile  | replaced                             |
//! | gold       | `vessel_history`        | reconcile  | replaced                             |
//! | resolution | `vessel_subjects`       | reconcile  | replaced (source record -> vessel)   |
//! | resolution | `vessel_id_redirects`   | reconcile  | replaced, never shrinks              |
//! | resolution | `identity_conflicts`    | reconcile  | replaced, never shrinks              |
//! | resolution | `identity_review_queue` | reconcile  | replaced                             |
//! | resolution | `identity_decisions`    | both       | appended                             |
//! | control    | `reconciliation_runs`   | reconcile  | appended                             |
//!
//! Everything a gold table holds can be rebuilt from the assertions and the
//! decisions; the assertions can be rebuilt from `source_records`.

use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, Utc};
use collect_core::iceberg::{ensure_namespace, ensure_table, table_ident, IcebergConfig};
use collect_maint::commit::RestClient;
use collect_maint::rewrite::write_partition;
use iceberg::expr::Reference;
use iceberg::spec::{Datum, Literal, PartitionSpecBuilder, Schema, Struct, Transform};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::Catalog;

use crate::columns::{col, schema, to_batch, Column};

pub const SOURCE_CATALOG: &str = "source_catalog";
pub const INGESTION_RUNS: &str = "ingestion_runs";
pub const SOURCE_RECORDS: &str = "source_records";
pub const REJECTED_RECORDS: &str = "rejected_records";
pub const ASSERTIONS: &str = "identity_assertions";
pub const VESSELS: &str = "vessels";
pub const VESSEL_ATTRIBUTES: &str = "vessel_attributes";
pub const VESSEL_IDENTIFIERS: &str = "vessel_identifiers";
pub const VESSEL_HISTORY: &str = "vessel_history";
pub const VESSEL_SUBJECTS: &str = "vessel_subjects";
pub const REDIRECTS: &str = "vessel_id_redirects";
pub const CONFLICTS: &str = "identity_conflicts";
pub const REVIEW_QUEUE: &str = "identity_review_queue";
pub const DECISIONS: &str = "identity_decisions";
pub const RECONCILIATION_RUNS: &str = "reconciliation_runs";

const TARGET_FILE_BYTES: u64 = 256 << 20;
const MAX_COMMIT_ATTEMPTS: usize = 4;

// ---------------------------------------------------------------------------
// Rows.

#[derive(Clone, Debug, Default)]
pub struct CatalogRow {
    pub source_id: String,
    pub name: String,
    pub publisher: String,
    pub adapter: String,
    pub source_url: String,
    pub dataset_url: Option<String>,
    pub access_method: String,
    pub data_format: String,
    pub refresh_policy: String,
    pub license: String,
    pub permitted_uses: Vec<String>,
    pub access_status: String,
    pub authorization_basis: String,
    pub authentication: String,
    pub enabled: Option<bool>,
    pub priority: Option<i32>,
    pub trust: String,
    pub jurisdiction: Option<String>,
    pub removal_semantics: String,
    pub config_hash: String,
    pub parser_version: String,
    pub last_successful_fetch: Option<DateTime<Utc>>,
    pub last_successful_ingestion: Option<DateTime<Utc>>,
    pub last_source_publication: Option<DateTime<Utc>>,
    pub last_content_hash: Option<String>,
    pub last_run_status: Option<String>,
    pub updated_at: DateTime<Utc>,
}

pub const CATALOG_COLUMNS: &[Column<CatalogRow>] = &[
    col!(source_id, Str, req),
    col!(name, Str, req),
    col!(publisher, Str, req),
    col!(adapter, Str, req),
    col!(source_url, Str, req),
    col!(dataset_url, Str),
    col!(access_method, Str, req),
    col!(data_format, Str, req),
    col!(refresh_policy, Str, req),
    col!(license, Str, req),
    col!(permitted_uses, StrList),
    col!(access_status, Str, req),
    col!(authorization_basis, Str, req),
    col!(authentication, Str, req),
    col!(enabled, Bool),
    col!(priority, Int),
    col!(trust, Str, req),
    col!(jurisdiction, Str),
    col!(removal_semantics, Str, req),
    col!(config_hash, Str, req),
    col!(parser_version, Str, req),
    col!(last_successful_fetch, Ts),
    col!(last_successful_ingestion, Ts),
    col!(last_source_publication, Ts),
    col!(last_content_hash, Str),
    col!(last_run_status, Str),
    col!(updated_at, Ts, req),
];

/// One ingestion of one source.
#[derive(Clone, Debug, Default)]
pub struct RunRow {
    pub run_id: String,
    pub source_id: String,
    /// `succeeded`, `unchanged` (content already ingested), `quarantined`,
    /// `refused` (not authorized) or `failed`.
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub source_version: Option<String>,
    pub source_published_at: Option<DateTime<Utc>>,
    pub content_hash: Option<String>,
    pub source_object_uris: Vec<String>,
    pub parser_version: String,
    pub config_hash: String,
    /// Whether the run saw the whole source (so absence means removal).
    pub complete: Option<bool>,
    pub records_read: Option<i64>,
    pub records_new: Option<i64>,
    pub records_modified: Option<i64>,
    pub records_unchanged: Option<i64>,
    pub records_removed: Option<i64>,
    pub records_rejected: Option<i64>,
    pub assertions_active: Option<i64>,
    pub assertions_total: Option<i64>,
    pub source_records_snapshot_id: Option<i64>,
    pub assertions_snapshot_id: Option<i64>,
    pub warnings: Vec<String>,
    pub message: Option<String>,
}

pub const RUN_COLUMNS: &[Column<RunRow>] = &[
    col!(run_id, Str, req),
    col!(source_id, Str, req),
    col!(status, Str, req),
    col!(started_at, Ts, req),
    col!(finished_at, Ts),
    col!(source_version, Str),
    col!(source_published_at, Ts),
    col!(content_hash, Str),
    col!(source_object_uris, StrList),
    col!(parser_version, Str, req),
    col!(config_hash, Str, req),
    col!(complete, Bool),
    col!(records_read, Long),
    col!(records_new, Long),
    col!(records_modified, Long),
    col!(records_unchanged, Long),
    col!(records_removed, Long),
    col!(records_rejected, Long),
    col!(assertions_active, Long),
    col!(assertions_total, Long),
    col!(source_records_snapshot_id, Long),
    col!(assertions_snapshot_id, Long),
    col!(warnings, StrList),
    col!(message, Str),
];

/// The change a source record row records.
pub mod change {
    pub const NEW: &str = "NEW";
    pub const MODIFIED: &str = "MODIFIED";
    pub const REMOVED: &str = "REMOVED_FROM_SOURCE";
}

/// One version of one source record (bronze): appended when a record first
/// appears, when its content changes, and when it disappears from a complete
/// snapshot (`change_type` REMOVED_FROM_SOURCE, no content). Unchanged
/// records add nothing, so re-ingesting the same publication is a no-op.
#[derive(Clone, Debug, Default)]
pub struct SourceRecordRow {
    /// `<run_id>:<n>`.
    pub record_id: String,
    pub source_id: String,
    pub source_record_id: String,
    /// What the source describes: a vessel as the source knows it.
    pub subject_id: String,
    pub source_version: Option<String>,
    pub ingestion_run_id: String,
    pub source_published_at: Option<DateTime<Utc>>,
    pub retrieved_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub change_type: String,
    /// SHA-256 of the canonical JSON in `raw_content`.
    pub record_hash: Option<String>,
    /// The source record's fields as published, as a JSON object.
    pub raw_content: Option<String>,
    pub source_object_uri: Option<String>,
    pub parser_version: String,
    /// `valid` or `partial`.
    pub validation_status: String,
    pub validation_errors: Vec<String>,
}

pub const SOURCE_RECORD_COLUMNS: &[Column<SourceRecordRow>] = &[
    col!(record_id, Str, req),
    col!(source_id, Str, req),
    col!(source_record_id, Str, req),
    col!(subject_id, Str, req),
    col!(source_version, Str),
    col!(ingestion_run_id, Str, req),
    col!(source_published_at, Ts),
    col!(retrieved_at, Ts, req),
    col!(ingested_at, Ts, req),
    col!(change_type, Str, req),
    col!(record_hash, Str),
    col!(raw_content, Str),
    col!(source_object_uri, Str),
    col!(parser_version, Str, req),
    col!(validation_status, Str, req),
    col!(validation_errors, StrList),
];

#[derive(Clone, Debug, Default)]
pub struct RejectedRow {
    pub record_id: String,
    pub source_id: String,
    pub ingestion_run_id: String,
    pub source_object_uri: Option<String>,
    /// Where in the file: `row 12`, `slice 1987`.
    pub location: String,
    pub raw_content: Option<String>,
    pub reasons: Vec<String>,
    pub ingested_at: DateTime<Utc>,
}

pub const REJECTED_COLUMNS: &[Column<RejectedRow>] = &[
    col!(record_id, Str, req),
    col!(source_id, Str, req),
    col!(ingestion_run_id, Str, req),
    col!(source_object_uri, Str),
    col!(location, Str, req),
    col!(raw_content, Str),
    col!(reasons, StrList),
    col!(ingested_at, Ts, req),
];

/// One claim a source makes about one attribute of one subject (silver).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AssertionRow {
    /// A hash of where the claim came from and what it says: the same claim
    /// always gets the same id.
    pub assertion_id: String,
    pub source_id: String,
    pub source_record_id: String,
    pub subject_id: String,
    pub attribute: String,
    pub value_original: Option<String>,
    pub value_normalized: Option<String>,
    /// `identifier`, `string`, `code`, `number`, `integer`, `date`.
    pub value_type: String,
    /// Scope of the value: the issuing jurisdiction of a registration, the
    /// unit a measurement was published in, the role of an organization.
    pub qualifier: Option<String>,
    /// What validation found: `valid`, `bad_check_digit`, `unverifiable`,
    /// `unrecognised`, or null when there is nothing to check.
    pub check_status: Option<String>,
    /// When the claim was true in the world, as the source says; null when
    /// it doesn't say (never invented).
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
    /// When the registry first learned the claim.
    pub recorded_at: DateTime<Utc>,
    /// When a later version of the same source record replaced it.
    pub superseded_at: Option<DateTime<Utc>>,
    pub source_published_at: Option<DateTime<Utc>>,
    /// `active`, `superseded`, `withdrawn` (record removed from the source).
    pub assertion_status: String,
    /// The `source_records.record_id` it was first read from.
    pub provenance_record_id: String,
}

pub const ASSERTION_COLUMNS: &[Column<AssertionRow>] = &[
    col!(assertion_id, Str, req),
    col!(source_id, Str, req),
    col!(source_record_id, Str, req),
    col!(subject_id, Str, req),
    col!(attribute, Str, req),
    col!(value_original, Str),
    col!(value_normalized, Str),
    col!(value_type, Str, req),
    col!(qualifier, Str),
    col!(check_status, Str),
    col!(valid_from, Ts),
    col!(valid_to, Ts),
    col!(recorded_at, Ts, req),
    col!(superseded_at, Ts),
    col!(source_published_at, Ts),
    col!(assertion_status, Str, req),
    col!(provenance_record_id, Str, req),
];

/// One resolved physical vessel, as currently best supported (gold).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VesselRow {
    pub vessel_id: String,
    pub preferred_name: Option<String>,
    pub imo_number: Option<String>,
    pub preferred_mmsi: Option<String>,
    pub preferred_callsign: Option<String>,
    /// ISO 3166 alpha-3.
    pub flag_state: Option<String>,
    pub vessel_type: Option<String>,
    pub year_built: Option<i32>,
    pub length_m: Option<f64>,
    pub beam_m: Option<f64>,
    pub depth_m: Option<f64>,
    pub gross_tonnage: Option<f64>,
    pub net_tonnage: Option<f64>,
    pub deadweight_tonnes: Option<f64>,
    pub classification_society: Option<String>,
    /// `active`, `inactive`, `scrapped`, `unknown`.
    pub operational_status: String,
    /// `confirmed`, `probable`, `provisional`, `disputed`.
    pub identity_status: String,
    pub first_known_at: Option<DateTime<Utc>>,
    pub last_updated_at: DateTime<Utc>,
    pub source_count: i32,
    pub subject_count: i32,
    pub source_ids: Vec<String>,
}

pub const VESSEL_COLUMNS: &[Column<VesselRow>] = &[
    col!(vessel_id, Str, req),
    col!(preferred_name, Str),
    col!(imo_number, Str),
    col!(preferred_mmsi, Str),
    col!(preferred_callsign, Str),
    col!(flag_state, Str),
    col!(vessel_type, Str),
    col!(year_built, Int),
    col!(length_m, Double),
    col!(beam_m, Double),
    col!(depth_m, Double),
    col!(gross_tonnage, Double),
    col!(net_tonnage, Double),
    col!(deadweight_tonnes, Double),
    col!(classification_society, Str),
    col!(operational_status, Str, req),
    col!(identity_status, Str, req),
    col!(first_known_at, Ts),
    col!(last_updated_at, Ts, req),
    col!(source_count, Int, req),
    col!(subject_count, Int, req),
    col!(source_ids, StrList),
];

/// Which assertion each preferred value in `vessels` came from, and how many
/// other values the sources offer.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VesselAttributeRow {
    pub vessel_id: String,
    pub attribute: String,
    pub value: String,
    pub assertion_id: String,
    pub source_id: String,
    pub supporting_assertions: Vec<String>,
    pub alternative_values: Vec<String>,
}

pub const VESSEL_ATTRIBUTE_COLUMNS: &[Column<VesselAttributeRow>] = &[
    col!(vessel_id, Str, req),
    col!(attribute, Str, req),
    col!(value, Str, req),
    col!(assertion_id, Str, req),
    col!(source_id, Str, req),
    col!(supporting_assertions, StrList),
    col!(alternative_values, StrList),
];

/// One identifier a vessel has carried (gold). A vessel can have several
/// rows of one type, current and historical.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IdentifierRow {
    pub vessel_id: String,
    /// `IMO`, `MMSI`, `CALLSIGN`, `NATIONAL_REGISTRY`, `PSIX_VESSEL_ID`, ...
    pub identifier_type: String,
    pub identifier_value: String,
    pub issuing_authority: Option<String>,
    pub issuing_jurisdiction: Option<String>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
    /// `verified` (a registry issued or checked it), `reported`, `disputed`.
    pub identity_status: String,
    pub check_status: Option<String>,
    /// Whether some source still asserts it.
    pub is_current: bool,
    pub first_recorded_at: DateTime<Utc>,
    pub last_recorded_at: DateTime<Utc>,
    pub source_ids: Vec<String>,
    pub supporting_assertions: Vec<String>,
}

pub const IDENTIFIER_COLUMNS: &[Column<IdentifierRow>] = &[
    col!(vessel_id, Str, req),
    col!(identifier_type, Str, req),
    col!(identifier_value, Str, req),
    col!(issuing_authority, Str),
    col!(issuing_jurisdiction, Str),
    col!(valid_from, Ts),
    col!(valid_to, Ts),
    col!(identity_status, Str, req),
    col!(check_status, Str),
    col!(is_current, Bool, req),
    col!(first_recorded_at, Ts, req),
    col!(last_recorded_at, Ts, req),
    col!(source_ids, StrList),
    col!(supporting_assertions, StrList),
];

/// One value a non-identifier attribute (name, flag, port of registry,
/// class) has had, with when it held and when the registry knew it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HistoryRow {
    pub vessel_id: String,
    pub attribute: String,
    pub value: String,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
    pub is_current: bool,
    pub first_recorded_at: DateTime<Utc>,
    pub last_recorded_at: DateTime<Utc>,
    pub source_ids: Vec<String>,
    pub supporting_assertions: Vec<String>,
}

pub const HISTORY_COLUMNS: &[Column<HistoryRow>] = &[
    col!(vessel_id, Str, req),
    col!(attribute, Str, req),
    col!(value, Str, req),
    col!(valid_from, Ts),
    col!(valid_to, Ts),
    col!(is_current, Bool, req),
    col!(first_recorded_at, Ts, req),
    col!(last_recorded_at, Ts, req),
    col!(source_ids, StrList),
    col!(supporting_assertions, StrList),
];

/// Which vessel each source subject resolved to.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SubjectRow {
    pub source_id: String,
    pub subject_id: String,
    pub vessel_id: String,
    /// What tied it to the vessel's other subjects: `IMO`,
    /// `NATIONAL_REGISTRY`, `DECISION`, or `NONE` (alone).
    pub linked_by: String,
    pub first_linked_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub const SUBJECT_COLUMNS: &[Column<SubjectRow>] = &[
    col!(source_id, Str, req),
    col!(subject_id, Str, req),
    col!(vessel_id, Str, req),
    col!(linked_by, Str, req),
    col!(first_linked_at, Ts, req),
    col!(updated_at, Ts, req),
];

/// A vessel id retired by a merge, and where it now points. Kept forever,
/// so an old id always resolves; `active` turns false when the merge is
/// undone and the old id is in use again.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RedirectRow {
    pub retired_vessel_id: String,
    pub surviving_vessel_id: String,
    pub decision_id: String,
    pub retired_at: DateTime<Utc>,
    /// The subjects the retired id had, `<source_id>/<subject_id>`.
    pub retired_subjects: Vec<String>,
    pub active: bool,
    pub restored_at: Option<DateTime<Utc>>,
}

pub const REDIRECT_COLUMNS: &[Column<RedirectRow>] = &[
    col!(retired_vessel_id, Str, req),
    col!(surviving_vessel_id, Str, req),
    col!(decision_id, Str, req),
    col!(retired_at, Ts, req),
    col!(retired_subjects, StrList),
    col!(active, Bool, req),
    col!(restored_at, Ts),
];

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConflictRow {
    pub conflict_id: String,
    pub vessel_id: String,
    /// `IMO`, `NAME`, `FLAG`, `YEAR_BUILT`, `DIMENSIONS`, `IDENTIFIER_SHARED`.
    pub conflict_type: String,
    pub attribute: String,
    pub conflicting_values: Vec<String>,
    pub assertion_ids: Vec<String>,
    pub source_ids: Vec<String>,
    /// Other vessels involved (a blocked merge names the other side).
    pub related_vessel_ids: Vec<String>,
    /// `informational`, `moderate`, `significant`.
    pub severity: String,
    /// `unresolved`, `cleared` (the sources no longer disagree), `reviewed`,
    /// `resolved`.
    pub resolution_status: String,
    pub resolution_decision_id: Option<String>,
    pub detected_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub cleared_at: Option<DateTime<Utc>>,
}

pub const CONFLICT_COLUMNS: &[Column<ConflictRow>] = &[
    col!(conflict_id, Str, req),
    col!(vessel_id, Str, req),
    col!(conflict_type, Str, req),
    col!(attribute, Str, req),
    col!(conflicting_values, StrList),
    col!(assertion_ids, StrList),
    col!(source_ids, StrList),
    col!(related_vessel_ids, StrList),
    col!(severity, Str, req),
    col!(resolution_status, Str, req),
    col!(resolution_decision_id, Str),
    col!(detected_at, Ts, req),
    col!(last_seen_at, Ts, req),
    col!(cleared_at, Ts),
];

/// A candidate match the deterministic rules would not make on their own.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProposalRow {
    pub proposal_id: String,
    /// `PROPOSE_MATCH`.
    pub proposal_type: String,
    pub vessel_id_a: String,
    pub vessel_id_b: String,
    /// `possible` (partial evidence) or `probable` (several compatible
    /// identifiers, nothing contradicting).
    pub category: String,
    pub evidence: Vec<String>,
    pub first_proposed_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

pub const PROPOSAL_COLUMNS: &[Column<ProposalRow>] = &[
    col!(proposal_id, Str, req),
    col!(proposal_type, Str, req),
    col!(vessel_id_a, Str, req),
    col!(vessel_id_b, Str, req),
    col!(category, Str, req),
    col!(evidence, StrList),
    col!(first_proposed_at, Ts, req),
    col!(last_seen_at, Ts, req),
];

/// A manual decision, or an automated merge, split or restore.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DecisionRow {
    pub decision_id: String,
    /// Manual: `KEEP_APART`, `LINK`, `REVOKE`. Automated: `MERGE`, `SPLIT`,
    /// `RESTORE` (a retired id back in use).
    pub decision_type: String,
    pub decided_at: DateTime<Utc>,
    /// `reconcile/<algorithm version>` or `user:<name>`.
    pub actor: String,
    /// For KEEP_APART and LINK: the two subjects, `<source_id>/<subject_id>`.
    pub subjects: Vec<String>,
    pub vessel_ids: Vec<String>,
    pub evidence: Vec<String>,
    pub reason: Option<String>,
    pub revokes_decision_id: Option<String>,
}

pub const DECISION_COLUMNS: &[Column<DecisionRow>] = &[
    col!(decision_id, Str, req),
    col!(decision_type, Str, req),
    col!(decided_at, Ts, req),
    col!(actor, Str, req),
    col!(subjects, StrList),
    col!(vessel_ids, StrList),
    col!(evidence, StrList),
    col!(reason, Str),
    col!(revokes_decision_id, Str),
];

#[derive(Clone, Debug, Default)]
pub struct ReconcileRunRow {
    pub run_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub algorithm_version: String,
    pub assertions_read: i64,
    pub subjects: i64,
    pub vessels: i64,
    pub vessels_created: i64,
    pub merges: i64,
    pub splits: i64,
    pub restores: i64,
    pub blocked_merges: i64,
    pub conflicts_open: i64,
    pub proposals: i64,
    pub vessels_snapshot_id: Option<i64>,
}

pub const RECONCILE_RUN_COLUMNS: &[Column<ReconcileRunRow>] = &[
    col!(run_id, Str, req),
    col!(started_at, Ts, req),
    col!(finished_at, Ts, req),
    col!(algorithm_version, Str, req),
    col!(assertions_read, Long, req),
    col!(subjects, Long, req),
    col!(vessels, Long, req),
    col!(vessels_created, Long, req),
    col!(merges, Long, req),
    col!(splits, Long, req),
    col!(restores, Long, req),
    col!(blocked_merges, Long, req),
    col!(conflicts_open, Long, req),
    col!(proposals, Long, req),
    col!(vessels_snapshot_id, Long),
];

// ---------------------------------------------------------------------------
// Partitioning.

/// How a table is partitioned. Identities are not spatial, so nothing here
/// partitions by place; the gold tables are small enough to stay whole.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Layout {
    Unpartitioned,
    /// Identity on `source_id`.
    BySource,
    /// Identity on `source_id`, then month of `ingested_at`.
    BySourceMonth,
}

pub struct TableDef {
    pub name: &'static str,
    pub schema: fn() -> Schema,
    pub layout: Layout,
    pub bloom: &'static [&'static str],
}

macro_rules! def {
    ($name:expr, $cols:expr, $layout:ident, $bloom:expr) => {
        TableDef { name: $name, schema: || schema($cols), layout: Layout::$layout, bloom: $bloom }
    };
}

pub const TABLES: &[TableDef] = &[
    def!(SOURCE_CATALOG, CATALOG_COLUMNS, Unpartitioned, &[]),
    def!(INGESTION_RUNS, RUN_COLUMNS, Unpartitioned, &[]),
    def!(SOURCE_RECORDS, SOURCE_RECORD_COLUMNS, BySourceMonth, &["source_record_id", "subject_id"]),
    def!(REJECTED_RECORDS, REJECTED_COLUMNS, BySource, &[]),
    def!(ASSERTIONS, ASSERTION_COLUMNS, BySource, &["subject_id", "value_normalized"]),
    def!(VESSELS, VESSEL_COLUMNS, Unpartitioned, &["vessel_id", "imo_number", "preferred_mmsi", "preferred_callsign"]),
    def!(VESSEL_ATTRIBUTES, VESSEL_ATTRIBUTE_COLUMNS, Unpartitioned, &["vessel_id"]),
    def!(VESSEL_IDENTIFIERS, IDENTIFIER_COLUMNS, Unpartitioned, &["vessel_id", "identifier_value"]),
    def!(VESSEL_HISTORY, HISTORY_COLUMNS, Unpartitioned, &["vessel_id"]),
    def!(VESSEL_SUBJECTS, SUBJECT_COLUMNS, Unpartitioned, &["subject_id", "vessel_id"]),
    def!(REDIRECTS, REDIRECT_COLUMNS, Unpartitioned, &["retired_vessel_id"]),
    def!(CONFLICTS, CONFLICT_COLUMNS, Unpartitioned, &["vessel_id"]),
    def!(REVIEW_QUEUE, PROPOSAL_COLUMNS, Unpartitioned, &[]),
    def!(DECISIONS, DECISION_COLUMNS, Unpartitioned, &[]),
    def!(RECONCILIATION_RUNS, RECONCILE_RUN_COLUMNS, Unpartitioned, &[]),
];

pub fn def(name: &str) -> &'static TableDef {
    TABLES.iter().find(|t| t.name == name).expect("known table")
}

fn spec(d: &TableDef) -> Result<PartitionSpecBuilder> {
    let s = (d.schema)();
    let b = PartitionSpecBuilder::new(s);
    Ok(match d.layout {
        Layout::Unpartitioned => b,
        Layout::BySource => b.add_partition_field("source_id", "source_id", Transform::Identity)?,
        Layout::BySourceMonth => b
            .add_partition_field("source_id", "source_id", Transform::Identity)?
            .add_partition_field("ingested_at", "ingested_at_month", Transform::Month)?,
    })
}

/// Months since 1970-01, the value of a `month` partition.
pub fn months_since_epoch(t: DateTime<Utc>) -> i32 {
    (t.year() - 1970) * 12 + t.month0() as i32
}

pub fn source_partition(source_id: &str) -> Struct {
    Struct::from_iter([Some(Literal::string(source_id))])
}

pub fn source_month_partition(source_id: &str, at: DateTime<Utc>) -> Struct {
    Struct::from_iter([Some(Literal::string(source_id)), Some(Literal::int(months_since_epoch(at)))])
}

// ---------------------------------------------------------------------------
// The store.

/// The registry's tables in one catalog and namespace.
pub struct Store<C> {
    pub catalog: C,
    pub rest: RestClient,
    pub config: IcebergConfig,
}

impl<C: Catalog> Store<C> {
    /// Creates the namespace and every table that's missing.
    pub async fn init(&self) -> Result<Vec<&'static str>> {
        ensure_namespace(&self.catalog, &self.config).await?;
        let mut created = Vec::new();
        for d in TABLES {
            let ident = table_ident(&self.config, d.name);
            if self.catalog.table_exists(&ident).await.unwrap_or(false) {
                continue;
            }
            ensure_table(&self.catalog, &self.config, d.name, (d.schema)(), spec(d)?).await?;
            created.push(d.name);
        }
        Ok(created)
    }

    pub async fn table(&self, name: &str) -> Result<Table> {
        let d = def(name);
        ensure_namespace(&self.catalog, &self.config).await?;
        let ident = table_ident(&self.config, name);
        if let Ok(t) = self.catalog.load_table(&ident).await {
            return Ok(t);
        }
        ensure_table(&self.catalog, &self.config, d.name, (d.schema)(), spec(d)?).await
    }

    /// The table, or `None` when it hasn't been created yet.
    pub async fn existing(&self, name: &str) -> Result<Option<Table>> {
        let ident = table_ident(&self.config, name);
        if !self.catalog.table_exists(&ident).await? {
            return Ok(None);
        }
        Ok(Some(self.catalog.load_table(&ident).await?))
    }

    /// All rows of `name` (optionally only `source_id = source`), or none
    /// when the table doesn't exist.
    pub async fn read(&self, name: &str, source: Option<&str>) -> Result<Vec<RecordBatch>> {
        let Some(table) = self.existing(name).await? else {
            return Ok(Vec::new());
        };
        let filter = source.map(|s| Reference::new("source_id").equal_to(Datum::string(s)));
        ais_tracks::state::scan_all(&table, filter).await.with_context(|| format!("reading {name}"))
    }

    pub async fn snapshot_id(&self, name: &str) -> Result<Option<i64>> {
        Ok(self.existing(name).await?.and_then(|t| t.metadata().current_snapshot_id()))
    }

    /// Appends `rows` to `name` (into `partition` when the table has one) in
    /// one snapshot. Nothing to append is a no-op.
    pub async fn append<T>(&self, name: &str, cols: &[Column<T>], rows: &[T], partition: Option<Struct>) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let d = def(name);
        let batch = to_batch(cols, rows)?;
        let ident = table_ident(&self.config, name);
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            let table = self.table(name).await?;
            let files = write_partition(&table, partition.clone(), batch.clone(), TARGET_FILE_BYTES, d.bloom).await?;
            let txn = Transaction::new(&table);
            let txn = txn.fast_append().add_data_files(files.clone()).apply(txn)?;
            match txn.commit(&self.catalog).await {
                Ok(_) => return Ok(rows.len()),
                Err(e) => {
                    for f in &files {
                        let _ = table.file_io().delete(f.file_path()).await;
                    }
                    if attempt == MAX_COMMIT_ATTEMPTS {
                        return Err(e).with_context(|| format!("appending to {ident}"));
                    }
                    eprintln!("  {name}: append failed ({e}), retrying ({attempt}/{MAX_COMMIT_ATTEMPTS})");
                }
            }
        }
        unreachable!()
    }

    /// Replaces the whole of an unpartitioned table. Refuses to empty a
    /// table unless `allow_empty` (an empty result usually means bad input).
    pub async fn replace<T>(&self, name: &str, cols: &[Column<T>], rows: &[T], allow_empty: bool) -> Result<()> {
        let d = def(name);
        debug_assert_eq!(d.layout, Layout::Unpartitioned);
        let batch = to_batch(cols, rows)?;
        if rows.is_empty() {
            if !allow_empty {
                bail!("{name}: computed 0 rows; leaving the table untouched");
            }
            self.table(name).await?;
            ais_tracks::output::replace_table_or_clear(&self.catalog, &self.rest, &self.config, name, (d.schema)(), &[], d.bloom)
                .await?;
            return Ok(());
        }
        ais_tracks::output::replace_table(&self.catalog, &self.rest, &self.config, name, (d.schema)(), &[batch], d.bloom)
            .await
            .with_context(|| format!("replacing {name}"))?;
        Ok(())
    }

    /// Replaces one source's partition of a table partitioned by source.
    pub async fn replace_source<T>(&self, name: &str, cols: &[Column<T>], source: &str, rows: &[T]) -> Result<()> {
        let d = def(name);
        debug_assert_eq!(d.layout, Layout::BySource);
        self.table(name).await?;
        let batch = to_batch(cols, rows)?;
        ais_tracks::output::replace_string_partition(
            &self.catalog,
            &self.rest,
            &self.config,
            name,
            (d.schema)(),
            "source_id",
            source,
            &[batch],
            d.bloom,
        )
        .await
        .with_context(|| format!("replacing {name} for {source}"))?;
        Ok(())
    }
}

/// Distinct values, sorted.
pub fn sorted(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let set: HashSet<String> = values.into_iter().collect();
    let mut v: Vec<String> = set.into_iter().collect();
    v.sort();
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemas_build_and_partition() {
        for d in TABLES {
            let s = (d.schema)();
            assert!(s.as_struct().fields().len() > 2, "{}", d.name);
            spec(d).unwrap().build().unwrap();
        }
    }

    #[test]
    fn batches_cast_to_the_iceberg_types() {
        // write_partition casts each column to the table's Arrow type; list
        // columns differ in their element field, so check that cast works.
        let now = Utc::now();
        let row = DecisionRow {
            decision_id: "d".into(),
            decision_type: "KEEP_APART".into(),
            decided_at: now,
            actor: "user:x".into(),
            subjects: vec!["a/1".into(), "b/2".into()],
            ..Default::default()
        };
        let b = to_batch(DECISION_COLUMNS, &[row]).unwrap();
        let target = iceberg::arrow::schema_to_arrow_schema(&schema(DECISION_COLUMNS)).unwrap();
        for (c, f) in b.columns().iter().zip(target.fields()) {
            arrow::compute::cast(c, f.data_type()).unwrap();
        }
    }

    #[test]
    fn month_partition_values() {
        let t = DateTime::parse_from_rfc3339("2026-10-17T00:00:00Z").unwrap().with_timezone(&Utc);
        assert_eq!(months_since_epoch(t), 56 * 12 + 9);
    }
}
