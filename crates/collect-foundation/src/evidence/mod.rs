//! Vessel evidence: port state control inspections and detentions, marine
//! occurrences and enforcement actions (IUU listings), from several
//! publishers, in
//! two shared tables:
//!
//! - `vessel_events`: one row per event per vessel (an inspection, a
//!   detention, an accident a vessel was involved in), with the vessel as the
//!   source identified it.
//! - `vessel_event_findings`: what the event found (PSC deficiencies), one row
//!   each, joined to `vessel_events` on `event_id`.
//!
//! Every row carries `source` (who published it) and `evidence_category`
//! (what kind of evidence it is). The categories are kept apart on purpose:
//! a PSC detention is evidence about regulatory compliance, a fisheries
//! boarding is evidence about enforcement, a grounding is evidence about
//! safety. They describe different things about a vessel and shouldn't be
//! summed into one score. See [`Category`] and [`EventType`].
//!
//! Both tables are partitioned by `source`, and a load replaces only its own
//! source's rows, so sources load independently and a rerun is idempotent.
//! `evidence_sources` describes each source. Some sources also keep their
//! full records in their own tables (`marsis_*`, `paris_mou_*`).

pub mod iuu;
pub mod paris;
pub mod tokyo;
pub mod tsb;
pub mod uscg;

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Utc};
use iceberg::spec::{NestedField, PrimitiveType, Schema};

pub const EVENTS_TABLE: &str = "vessel_events";
pub const FINDINGS_TABLE: &str = "vessel_event_findings";
pub const SOURCES_TABLE: &str = "evidence_sources";
/// The column both shared tables are partitioned on.
pub const PARTITION_COLUMN: &str = "source";

/// What kind of evidence an event is. Stored as its `as_str` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Category {
    /// A port state inspecting a foreign-flagged ship against international
    /// conventions (SOLAS, MARPOL, MLC, ...): Paris MoU, Tokyo MoU, USCG.
    PortStateControl,
    /// A flag state inspecting its own ships (USCG on US-flagged vessels).
    FlagStateControl,
    /// Law-enforcement and fisheries actions: IUU listings, boardings,
    /// seizures, sanctions.
    Enforcement,
    /// Accidents and incidents reported to a safety investigator (TSB).
    MarineOccurrence,
}

impl Category {
    pub fn as_str(self) -> &'static str {
        match self {
            Category::PortStateControl => "port_state_control",
            Category::FlagStateControl => "flag_state_control",
            Category::Enforcement => "enforcement",
            Category::MarineOccurrence => "marine_occurrence",
        }
    }
}

/// What happened, within a category. Stored as its `as_str` value; the
/// source's own, finer classification goes in `event_subtype`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventType {
    /// An inspection that ended without the ship being held.
    Inspection,
    /// An inspection that ended with the ship detained.
    Detention,
    /// The ship refused entry to port (USCG "denied entry", Paris MoU ban).
    RefusalOfAccess,
    /// The ship ordered out of port.
    Expulsion,
    /// A marine accident (TSB: loss of life, serious injury, loss of the
    /// vessel, collision, grounding, ...).
    Accident,
    /// A marine incident (TSB: a dangerous situation without an accident's
    /// consequences).
    Incident,
    /// An occurrence the source doesn't class as either.
    Occurrence,
    /// A vessel put on an RFMO's IUU vessel list.
    IuuListing,
    /// A vessel taken off an RFMO's IUU vessel list.
    IuuDelisting,
}

impl EventType {
    pub fn as_str(self) -> &'static str {
        match self {
            EventType::Inspection => "inspection",
            EventType::Detention => "detention",
            EventType::RefusalOfAccess => "refusal_of_access",
            EventType::Expulsion => "expulsion",
            EventType::Accident => "accident",
            EventType::Incident => "incident",
            EventType::Occurrence => "occurrence",
            EventType::IuuListing => "iuu_listing",
            EventType::IuuDelisting => "iuu_delisting",
        }
    }
}

/// One row of `vessel_events`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Event {
    /// `<source>:<the source's own id>`, unique within the table.
    pub event_id: String,
    pub source: String,
    pub evidence_category: String,
    pub event_type: String,
    /// The source's classification: PSC inspection type, accident type, ...
    pub event_subtype: Option<String>,
    /// The source's severity grading (TSB: IMO casualty class).
    pub severity: Option<String>,
    /// The regime or agency: "Paris MoU", "Tokyo MoU", "USCG", "TSB Canada".
    pub regime: String,
    /// Who acted: the inspecting state, Coast Guard unit or TSB region.
    pub authority: Option<String>,
    /// The source's own record id.
    pub source_record_id: String,
    /// Groups rows of one case (TSB: the occurrence number shared by every
    /// vessel involved).
    pub case_id: Option<String>,
    /// Inspection, detention or occurrence date.
    pub event_date: Option<NaiveDate>,
    /// Last visit or release date.
    pub event_end_date: Option<NaiveDate>,
    /// Occurrence time, when the source gives it in UTC.
    pub event_time: Option<DateTime<Utc>>,
    pub place_name: Option<String>,
    /// UN/LOCODE when the source gives one.
    pub place_code: Option<String>,
    pub place_country: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub imo_number: Option<i32>,
    pub mmsi: Option<i64>,
    pub call_sign: Option<String>,
    pub vessel_name: Option<String>,
    /// Another identifier the source used for the vessel (official number,
    /// state number, ...), typed by `vessel_id_type`.
    pub vessel_id: Option<String>,
    pub vessel_id_type: Option<String>,
    /// Flag state as the source wrote it (name or code).
    pub flag: Option<String>,
    /// ISO 3166 alpha-2 flag, when the source gives a code.
    pub flag_code: Option<String>,
    pub ship_type: Option<String>,
    pub gross_tonnage: Option<f64>,
    pub year_built: Option<i32>,
    pub company_imo: Option<String>,
    pub company_name: Option<String>,
    pub classification_society: Option<String>,
    pub detained: Option<bool>,
    pub detention_days: Option<i32>,
    pub deficiency_count: Option<i32>,
    pub detainable_deficiency_count: Option<i32>,
    pub deaths: Option<i32>,
    pub injuries: Option<i32>,
    pub missing_persons: Option<i32>,
    pub pollution: Option<bool>,
    pub summary: Option<String>,
    pub source_file: String,
}

/// One row of `vessel_event_findings`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Finding {
    pub event_id: String,
    pub source: String,
    /// Position within the event, from 1.
    pub finding_seq: i32,
    /// The source's own id for the finding, if it has one.
    pub finding_id: Option<String>,
    pub finding_date: Option<NaiveDate>,
    /// Deficiency code (Paris/Tokyo MoU defective item, USCG component).
    pub finding_code: Option<String>,
    /// The area the code belongs to ("FIRE SAFETY", "07 - Fire Safety").
    pub finding_area: Option<String>,
    pub finding_subarea: Option<String>,
    /// The item ("Fire-dampers").
    pub finding_item: Option<String>,
    pub nature_of_defect_code: Option<String>,
    /// Required action code (USCG "30" = ship detained).
    pub action_code: Option<String>,
    pub action: Option<String>,
    pub ground_for_detention: Option<bool>,
    /// Whether the recognized organization (class) was held responsible.
    pub ro_related: Option<bool>,
    pub responsible_ro: Option<String>,
    pub accidental_damage: Option<bool>,
    pub due_date: Option<NaiveDate>,
    pub resolved_date: Option<NaiveDate>,
    pub description: Option<String>,
    pub source_file: String,
}

/// What a source load produced for the shared tables.
#[derive(Default)]
pub struct Evidence {
    pub events: Vec<Event>,
    pub findings: Vec<Finding>,
}

// ---------------------------------------------------------------------------
// Column definitions: one list per table drives both the Iceberg schema and
// the Arrow batch, so the two can't drift apart.

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    Str,
    Int,
    Long,
    Double,
    Bool,
    Date,
    Ts,
}

pub enum Val<'a> {
    Str(Option<&'a str>),
    Int(Option<i32>),
    Long(Option<i64>),
    Double(Option<f64>),
    Bool(Option<bool>),
    Date(Option<NaiveDate>),
    Ts(Option<DateTime<Utc>>),
}

pub struct Column<T> {
    pub name: &'static str,
    pub kind: Kind,
    pub required: bool,
    pub get: for<'a> fn(&'a T) -> Val<'a>,
}

macro_rules! col {
    ($name:ident, Str, req) => {
        Column { name: stringify!($name), kind: Kind::Str, required: true, get: |r| Val::Str(Some(r.$name.as_str())) }
    };
    ($name:ident, Str) => {
        Column { name: stringify!($name), kind: Kind::Str, required: false, get: |r| Val::Str(r.$name.as_deref()) }
    };
    ($name:ident, Int, req) => {
        Column { name: stringify!($name), kind: Kind::Int, required: true, get: |r| Val::Int(Some(r.$name)) }
    };
    ($name:ident, $kind:ident) => {
        Column { name: stringify!($name), kind: Kind::$kind, required: false, get: |r| Val::$kind(r.$name) }
    };
}
pub(crate) use col;

pub const EVENT_COLUMNS: &[Column<Event>] = &[
    col!(event_id, Str, req),
    col!(source, Str, req),
    col!(evidence_category, Str, req),
    col!(event_type, Str, req),
    col!(event_subtype, Str),
    col!(severity, Str),
    col!(regime, Str, req),
    col!(authority, Str),
    col!(source_record_id, Str, req),
    col!(case_id, Str),
    col!(event_date, Date),
    col!(event_end_date, Date),
    col!(event_time, Ts),
    col!(place_name, Str),
    col!(place_code, Str),
    col!(place_country, Str),
    col!(latitude, Double),
    col!(longitude, Double),
    col!(imo_number, Int),
    col!(mmsi, Long),
    col!(call_sign, Str),
    col!(vessel_name, Str),
    col!(vessel_id, Str),
    col!(vessel_id_type, Str),
    col!(flag, Str),
    col!(flag_code, Str),
    col!(ship_type, Str),
    col!(gross_tonnage, Double),
    col!(year_built, Int),
    col!(company_imo, Str),
    col!(company_name, Str),
    col!(classification_society, Str),
    col!(detained, Bool),
    col!(detention_days, Int),
    col!(deficiency_count, Int),
    col!(detainable_deficiency_count, Int),
    col!(deaths, Int),
    col!(injuries, Int),
    col!(missing_persons, Int),
    col!(pollution, Bool),
    col!(summary, Str),
    col!(source_file, Str, req),
];

pub const FINDING_COLUMNS: &[Column<Finding>] = &[
    col!(event_id, Str, req),
    col!(source, Str, req),
    col!(finding_seq, Int, req),
    col!(finding_id, Str),
    col!(finding_date, Date),
    col!(finding_code, Str),
    col!(finding_area, Str),
    col!(finding_subarea, Str),
    col!(finding_item, Str),
    col!(nature_of_defect_code, Str),
    col!(action_code, Str),
    col!(action, Str),
    col!(ground_for_detention, Bool),
    col!(ro_related, Bool),
    col!(responsible_ro, Str),
    col!(accidental_damage, Bool),
    col!(due_date, Date),
    col!(resolved_date, Date),
    col!(description, Str),
    col!(source_file, Str, req),
];

/// Columns to build bloom filters on, for lookups by vessel.
pub const EVENT_BLOOM: &[&str] = &["event_id", "imo_number", "mmsi", "call_sign"];
pub const FINDING_BLOOM: &[&str] = &["event_id"];

fn iceberg_type(kind: Kind) -> PrimitiveType {
    match kind {
        Kind::Str => PrimitiveType::String,
        Kind::Int => PrimitiveType::Int,
        Kind::Long => PrimitiveType::Long,
        Kind::Double => PrimitiveType::Double,
        Kind::Bool => PrimitiveType::Boolean,
        Kind::Date => PrimitiveType::Date,
        Kind::Ts => PrimitiveType::Timestamptz,
    }
}

fn arrow_type(kind: Kind) -> DataType {
    match kind {
        Kind::Str => DataType::Utf8,
        Kind::Int => DataType::Int32,
        Kind::Long => DataType::Int64,
        Kind::Double => DataType::Float64,
        Kind::Bool => DataType::Boolean,
        Kind::Date => DataType::Date32,
        Kind::Ts => DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
    }
}

/// `cols`, then `loaded_at`.
pub fn schema<T>(cols: &[Column<T>]) -> Schema {
    let mut fields: Vec<Arc<NestedField>> = cols
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let id = i as i32 + 1;
            let ty = iceberg_type(c.kind).into();
            Arc::new(if c.required {
                NestedField::required(id, c.name, ty)
            } else {
                NestedField::optional(id, c.name, ty)
            })
        })
        .collect();
    let n = cols.len() as i32;
    fields.push(Arc::new(NestedField::required(n + 1, "loaded_at", PrimitiveType::Timestamptz.into())));
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("building evidence table schema")
}

fn epoch_days(d: NaiveDate) -> i32 {
    (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
}

/// `rows` as one batch with `schema(cols)`'s columns.
pub fn to_batch<T>(cols: &[Column<T>], rows: &[T], loaded_at: DateTime<Utc>) -> Result<RecordBatch> {
    let mut fields: Vec<Field> = cols
        .iter()
        .map(|c| Field::new(c.name, arrow_type(c.kind), !c.required))
        .collect();
    fields.push(Field::new("loaded_at", arrow_type(Kind::Ts), false));

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for c in cols {
        macro_rules! gather {
            ($variant:ident) => {
                rows.iter().map(|r| match (c.get)(r) {
                    Val::$variant(v) => v,
                    _ => panic!("column {} is not {:?}", c.name, c.kind),
                })
            };
        }
        let array: ArrayRef = match c.kind {
            Kind::Str => Arc::new(gather!(Str).collect::<StringArray>()),
            Kind::Int => Arc::new(gather!(Int).collect::<Int32Array>()),
            Kind::Long => Arc::new(gather!(Long).collect::<Int64Array>()),
            Kind::Double => Arc::new(gather!(Double).collect::<Float64Array>()),
            Kind::Bool => Arc::new(gather!(Bool).collect::<BooleanArray>()),
            Kind::Date => Arc::new(gather!(Date).map(|d| d.map(epoch_days)).collect::<Date32Array>()),
            Kind::Ts => Arc::new(
                gather!(Ts)
                    .map(|t| t.map(|t| t.timestamp_micros()))
                    .collect::<TimestampMicrosecondArray>()
                    .with_timezone("+00:00"),
            ),
        };
        arrays.push(array);
    }
    arrays.push(Arc::new(
        TimestampMicrosecondArray::from(vec![loaded_at.timestamp_micros(); rows.len()]).with_timezone("+00:00"),
    ));
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays).context("building evidence batch")
}

/// A table whose columns are all optional strings named as in the source
/// file, plus `loaded_at` and `source_file`: a source's records kept as
/// published, alongside the normalized rows.
pub struct RawTable {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub source_file: String,
}

impl RawTable {
    pub fn schema(&self) -> Schema {
        let mut fields: Vec<Arc<NestedField>> = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| Arc::new(NestedField::optional(i as i32 + 1, c, PrimitiveType::String.into())))
            .collect();
        let n = self.columns.len() as i32;
        fields.push(Arc::new(NestedField::required(n + 1, "loaded_at", PrimitiveType::Timestamptz.into())));
        fields.push(Arc::new(NestedField::required(n + 2, "source_file", PrimitiveType::String.into())));
        Schema::builder()
            .with_schema_id(1)
            .with_fields(fields)
            .build()
            .expect("building raw table schema")
    }

    pub fn to_batch(&self, loaded_at: DateTime<Utc>) -> Result<RecordBatch> {
        let mut fields: Vec<Field> = self.columns.iter().map(|c| Field::new(c, DataType::Utf8, true)).collect();
        fields.push(Field::new("loaded_at", arrow_type(Kind::Ts), false));
        fields.push(Field::new("source_file", DataType::Utf8, false));
        let mut arrays: Vec<ArrayRef> = (0..self.columns.len())
            .map(|i| {
                Arc::new(self.rows.iter().map(|r| r.get(i).and_then(|v| v.as_deref())).collect::<StringArray>())
                    as ArrayRef
            })
            .collect();
        arrays.push(Arc::new(
            TimestampMicrosecondArray::from(vec![loaded_at.timestamp_micros(); self.rows.len()])
                .with_timezone("+00:00"),
        ));
        arrays.push(Arc::new(StringArray::from(vec![self.source_file.as_str(); self.rows.len()])));
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays)
            .with_context(|| format!("building {}", self.table))
    }
}

// ---------------------------------------------------------------------------
// The sources.

/// One row of `evidence_sources`.
pub struct SourceInfo {
    pub source: &'static str,
    pub name: &'static str,
    pub regime: &'static str,
    pub evidence_categories: &'static str,
    pub publisher: &'static str,
    pub url: &'static str,
    pub access: &'static str,
    pub coverage: &'static str,
    pub notes: &'static str,
}

pub const SOURCES: &[SourceInfo] = &[
    paris::INFO,
    uscg::INFO,
    tsb::INFO,
    tokyo::INFO,
    iuu::INFO,
];

macro_rules! info_col {
    ($name:ident) => {
        Column { name: stringify!($name), kind: Kind::Str, required: true, get: |r: &SourceInfo| Val::Str(Some(r.$name)) }
    };
}

pub const SOURCE_COLUMNS: &[Column<SourceInfo>] = &[
    info_col!(source),
    info_col!(name),
    info_col!(regime),
    info_col!(evidence_categories),
    info_col!(publisher),
    info_col!(url),
    info_col!(access),
    info_col!(coverage),
    info_col!(notes),
];

// ---------------------------------------------------------------------------
// Parsing helpers shared by the sources.

/// `s` trimmed, or `None` when that leaves nothing.
pub fn text(s: &str) -> Option<String> {
    let t = s.trim();
    (!t.is_empty()).then(|| t.to_string())
}

/// A date at the start of `s`: `yyyy-mm-dd` (with anything after it, such as
/// a time or a `Z`) or `dd.mm.yyyy`.
pub fn date(s: &str) -> Option<NaiveDate> {
    let s = s.trim();
    if s.len() >= 10 {
        if let Ok(d) = NaiveDate::parse_from_str(&s[..10], "%Y-%m-%d") {
            return Some(d);
        }
        if let Ok(d) = NaiveDate::parse_from_str(&s[..10], "%d.%m.%Y") {
            return Some(d);
        }
    }
    None
}

/// The leading year of `s` (`1993`, `2004-11-19`).
pub fn year(s: &str) -> Option<i32> {
    let s = s.trim();
    let y: i32 = s.get(..4)?.parse().ok()?;
    (1800..=2200).contains(&y).then_some(y)
}

pub fn int(s: &str) -> Option<i32> {
    let s = s.trim();
    s.parse().ok().or_else(|| s.parse::<f64>().ok().filter(|f| f.fract() == 0.0).map(|f| f as i32))
}

pub fn double(s: &str) -> Option<f64> {
    s.trim().parse().ok().filter(|f: &f64| f.is_finite())
}

/// A 7-digit IMO number (leading zeros and an `IMO` prefix tolerated).
pub fn imo(s: &str) -> Option<i32> {
    let s = s.trim();
    let s = s.strip_prefix("IMO").unwrap_or(s).trim();
    let n: i32 = s.parse().ok()?;
    (1_000_000..=9_999_999).contains(&n).then_some(n)
}

/// A 9-digit MMSI.
pub fn mmsi(s: &str) -> Option<i64> {
    let n: i64 = s.trim().parse().ok()?;
    (100_000_000..=999_999_999).contains(&n).then_some(n)
}

/// Splits Paris/Tokyo MoU style deficiency text, `07109 - FIRE SAFETY -
/// Fixed fire extinguishing installation (no responsibility of RO)`, into
/// code, area, item and the trailing parenthesised RO note.
pub fn split_deficiency(s: &str) -> (Option<String>, Option<String>, Option<String>, Option<String>) {
    let s = s.trim();
    let (body, note) = trailing_parens(s);
    let mut parts = body.splitn(3, " - ");
    let first = parts.next().unwrap_or("").trim();
    let looks_like_code = !first.is_empty() && first.chars().all(|c| c.is_ascii_digit());
    if !looks_like_code {
        return (None, None, text(body), note);
    }
    let area = parts.next().and_then(text);
    let item = parts.next().and_then(text);
    (Some(first.to_string()), area, item, note)
}

/// `s` without a trailing balanced `( ... )`, and what was inside it.
fn trailing_parens(s: &str) -> (&str, Option<String>) {
    if !s.ends_with(')') {
        return (s, None);
    }
    let mut depth = 0;
    for (i, c) in s.char_indices().rev() {
        match c {
            ')' => depth += 1,
            '(' => {
                depth -= 1;
                if depth == 0 {
                    return (s[..i].trim_end(), text(&s[i + 1..s.len() - 1]));
                }
            }
            _ => {}
        }
    }
    (s, None)
}

/// What a deficiency's RO note says about the classification society:
/// `(no responsibility of RO)` means not related, `(Other)` says nothing,
/// anything else names the responsible RO.
pub fn ro_note(note: Option<&str>) -> (Option<bool>, Option<String>) {
    match note {
        None => (None, None),
        Some(n) if n.eq_ignore_ascii_case("no responsibility of RO") => (Some(false), None),
        Some(n) if n.eq_ignore_ascii_case("other") => (None, Some(n.to_string())),
        Some(n) => (Some(true), Some(n.to_string())),
    }
}

/// Counts for the dry-run report.
pub fn summarize(label: &str, ev: &Evidence) {
    let mut by_type: std::collections::BTreeMap<(&str, &str), usize> = Default::default();
    for e in &ev.events {
        *by_type.entry((e.evidence_category.as_str(), e.event_type.as_str())).or_default() += 1;
    }
    let (first, last) = ev
        .events
        .iter()
        .filter_map(|e| e.event_date)
        .fold((None, None), |(lo, hi): (Option<NaiveDate>, Option<NaiveDate>), d| {
            (Some(lo.map_or(d, |x| x.min(d))), Some(hi.map_or(d, |x| x.max(d))))
        });
    let span = match (first, last) {
        (Some(a), Some(b)) => format!(", {a} to {b}"),
        _ => String::new(),
    };
    let with_imo = ev.events.iter().filter(|e| e.imo_number.is_some()).count();
    println!(
        "{label}: {} events ({with_imo} with an IMO number), {} findings{span}",
        ev.events.len(),
        ev.findings.len()
    );
    for ((cat, ty), n) in by_type {
        println!("  {cat}/{ty}: {n}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemas_match_batches() {
        let now = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let ev = Event {
            event_id: "x:1".into(),
            source: "x".into(),
            evidence_category: Category::PortStateControl.as_str().into(),
            event_type: EventType::Detention.as_str().into(),
            regime: "X".into(),
            source_record_id: "1".into(),
            event_date: NaiveDate::from_ymd_opt(2024, 2, 29),
            event_time: Some(now),
            imo_number: Some(9_296_793),
            detained: Some(true),
            source_file: "f".into(),
            ..Default::default()
        };
        let b = to_batch(EVENT_COLUMNS, &[ev.clone(), ev], now).unwrap();
        ais_tracks::carry::check_batches(&schema(EVENT_COLUMNS), std::slice::from_ref(&b)).unwrap();
        assert_eq!(b.num_rows(), 2);
        let d = b.column_by_name("event_date").unwrap().as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(d.value_as_date(0), NaiveDate::from_ymd_opt(2024, 2, 29));

        let f = Finding { event_id: "x:1".into(), source: "x".into(), finding_seq: 1, source_file: "f".into(), ..Default::default() };
        let b = to_batch(FINDING_COLUMNS, &[f], now).unwrap();
        ais_tracks::carry::check_batches(&schema(FINDING_COLUMNS), &[b]).unwrap();

        let b = to_batch(SOURCE_COLUMNS, SOURCES, now).unwrap();
        ais_tracks::carry::check_batches(&schema(SOURCE_COLUMNS), &[b]).unwrap();
    }

    #[test]
    fn raw_tables_match_their_schema() {
        let now = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let t = RawTable {
            table: "t".into(),
            columns: vec!["A".into(), "B".into()],
            rows: vec![vec![Some("1".into()), None], vec![Some("2".into())]],
            source_file: "f.csv".into(),
        };
        let b = t.to_batch(now).unwrap();
        ais_tracks::carry::check_batches(&t.schema(), std::slice::from_ref(&b)).unwrap();
        assert_eq!(b.num_rows(), 2);
    }

    #[test]
    fn parses_values() {
        assert_eq!(date("2020-07-23Z"), NaiveDate::from_ymd_opt(2020, 7, 23));
        assert_eq!(date("2026-09-13 00:00:00.0000000"), NaiveDate::from_ymd_opt(2026, 9, 13));
        assert_eq!(date("01.09.2026"), NaiveDate::from_ymd_opt(2026, 9, 1));
        assert_eq!(date("2026"), None);
        assert_eq!(year("1993"), Some(1993));
        assert_eq!(year("2004-11-19"), Some(2004));
        assert_eq!(imo("0771929"), None, "ISM company numbers aren't ship IMO numbers when too short");
        assert_eq!(imo("9296793"), Some(9_296_793));
        assert_eq!(imo("IMO 9296793"), Some(9_296_793));
        assert_eq!(int("4.0"), Some(4));
        assert_eq!(mmsi("316001234"), Some(316_001_234));
        assert_eq!(mmsi("12345"), None);
    }

    #[test]
    fn splits_deficiencies() {
        let (code, area, item, note) =
            split_deficiency("07109 - FIRE SAFETY - Fixed fire extinguishing installation (no responsibility of RO)");
        assert_eq!(code.as_deref(), Some("07109"));
        assert_eq!(area.as_deref(), Some("FIRE SAFETY"));
        assert_eq!(item.as_deref(), Some("Fixed fire extinguishing installation"));
        assert_eq!(ro_note(note.as_deref()), (Some(false), None));

        let (_, area, item, note) =
            split_deficiency("07101 - FIRE SAFETY - Fire prevention structural integrity (Nippon Kaiji Kyokai (NKK))");
        assert_eq!(area.as_deref(), Some("FIRE SAFETY"));
        assert_eq!(item.as_deref(), Some("Fire prevention structural integrity"));
        assert_eq!(ro_note(note.as_deref()), (Some(true), Some("Nippon Kaiji Kyokai (NKK)".into())));

        // Items can themselves contain " - " and parentheses.
        let (_, area, item, note) = split_deficiency(
            "14608 - POLLUTION PREVENTION - MARPOL ANNEX VI - Incinerator incl.operations and operating manual  (no responsibility of RO)",
        );
        assert_eq!(area.as_deref(), Some("POLLUTION PREVENTION"));
        assert_eq!(item.as_deref(), Some("MARPOL ANNEX VI - Incinerator incl.operations and operating manual"));
        assert_eq!(note.as_deref(), Some("no responsibility of RO"));

        let (_, _, _, note) = split_deficiency("10105 - SAFETY OF NAVIGATION - Magnetic compass (Other)");
        assert_eq!(ro_note(note.as_deref()), (None, Some("Other".into())));
    }
}
