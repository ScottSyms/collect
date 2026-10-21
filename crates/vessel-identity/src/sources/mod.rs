//! Source adapters. Each one turns what a publisher serves into source
//! records (fields as published, keyed by the source's own record id), and
//! maps a record's fields to assertions. Mapping runs on the stored fields,
//! not on the file, so assertions can always be rebuilt from bronze.
//!
//! A new source is a TOML file under `config/sources`, an adapter module
//! here, and one line in [`Adapter::from_name`].

pub mod gfw;
pub mod iacs;
pub mod misle;
pub mod psix;
pub mod tc;

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Result};
use chrono::{DateTime, NaiveDate, Utc};

use crate::config::SourceConfig;
use crate::countries;
use crate::fetch::{Http, RawObject};
use crate::normalize::{self, ImoCheck};

/// Attribute names used in `identity_assertions.attribute`.
pub mod attr {
    pub const IMO: &str = "imo";
    pub const MMSI: &str = "mmsi";
    pub const CALLSIGN: &str = "callsign";
    pub const NATIONAL_REGISTRY: &str = "national_registry";
    pub const PSIX_VESSEL_ID: &str = "psix_vessel_id";
    pub const HULL_NUMBER: &str = "hull_number";
    pub const NAME: &str = "name";
    pub const FORMER_NAME: &str = "former_name";
    pub const FLAG: &str = "flag_state";
    pub const PORT_OF_REGISTRY: &str = "port_of_registry";
    pub const REGISTRATION_DATE: &str = "registration_date";
    pub const REGISTRY_STATUS: &str = "registry_status";
    pub const VESSEL_TYPE: &str = "vessel_type";
    pub const VESSEL_SUBTYPE: &str = "vessel_subtype";
    pub const YEAR_BUILT: &str = "year_built";
    pub const YEAR_REBUILT: &str = "year_rebuilt";
    pub const LENGTH_M: &str = "length_m";
    pub const BEAM_M: &str = "beam_m";
    pub const DEPTH_M: &str = "depth_m";
    pub const GROSS_TONNAGE: &str = "gross_tonnage";
    pub const NET_TONNAGE: &str = "net_tonnage";
    pub const DEADWEIGHT: &str = "deadweight_tonnes";
    pub const ALL_UP_WEIGHT: &str = "all_up_weight_kg";
    pub const HULL_MATERIAL: &str = "hull_material";
    pub const CONSTRUCTION_TYPE: &str = "construction_type";
    pub const ENGINE_TYPE: &str = "engine_type";
    pub const ENGINE_COUNT: &str = "engine_count";
    pub const PROPULSION_TYPE: &str = "propulsion_type";
    pub const PROPULSION_METHOD: &str = "propulsion_method";
    pub const PROPULSION_POWER: &str = "propulsion_power";
    pub const SPEED_KNOTS: &str = "speed_knots";
    pub const CLASS_SOCIETY: &str = "class_society";
    pub const CLASS_STATUS: &str = "class_status";
    pub const CLASS_STATUS_REASON: &str = "class_status_reason";
    pub const LAST_SURVEY: &str = "last_survey_date";
    pub const NEXT_SURVEY_DUE: &str = "next_survey_due";

    /// Attributes that identify a vessel (go to `vessel_identifiers`).
    pub const IDENTIFIERS: &[&str] = &[IMO, MMSI, CALLSIGN, NATIONAL_REGISTRY, PSIX_VESSEL_ID, HULL_NUMBER];
}

/// One record as a source published it.
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub source_record_id: String,
    /// The vessel as this source knows it. Several records can describe one
    /// subject (IACS keeps one row per class society per vessel).
    pub subject_id: String,
    /// Non-empty fields, by the source's own column names.
    pub fields: BTreeMap<String, String>,
    /// Index into the fetched objects.
    pub object: usize,
    /// When the source dated this record, if it does.
    pub published_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct Rejected {
    pub location: String,
    pub raw: Option<String>,
    pub reasons: Vec<String>,
    pub object: usize,
}

#[derive(Default, Debug)]
pub struct Parsed {
    pub records: Vec<Record>,
    pub rejected: Vec<Rejected>,
    /// Notes for the run (data age hints, skipped rows).
    pub warnings: Vec<String>,
}

/// What a fetch produced.
pub struct Fetched {
    pub objects: Vec<RawObject>,
    pub source_version: Option<String>,
    pub source_published_at: Option<DateTime<Utc>>,
    /// Whether the objects hold the whole source; an incomplete fetch never
    /// removes records.
    pub complete: bool,
    pub warnings: Vec<String>,
}

/// One claim about one attribute, before it gets its provenance.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Draft {
    pub attribute: &'static str,
    pub value_original: Option<String>,
    pub value_normalized: Option<String>,
    pub value_type: &'static str,
    pub qualifier: Option<String>,
    pub check_status: Option<&'static str>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
}

impl Draft {
    pub fn new(attribute: &'static str, value_type: &'static str, original: &str, normalized: Option<String>) -> Self {
        Draft {
            attribute,
            value_original: Some(original.trim().to_string()),
            value_normalized: normalized,
            value_type,
            qualifier: None,
            check_status: None,
            valid_from: None,
            valid_to: None,
        }
    }

    pub fn qualified(mut self, q: impl Into<String>) -> Self {
        self.qualifier = Some(q.into());
        self
    }

    pub fn checked(mut self, c: &'static str) -> Self {
        self.check_status = Some(c);
        self
    }

    pub fn from(mut self, t: Option<DateTime<Utc>>) -> Self {
        self.valid_from = t;
        self
    }

    /// A validation problem worth recording on the source record.
    pub fn problem(&self) -> Option<String> {
        match self.check_status {
            Some(s @ ("bad_check_digit" | "unrecognised" | "invalid")) => Some(format!(
                "{} '{}': {s}",
                self.attribute,
                self.value_original.as_deref().unwrap_or("")
            )),
            _ => None,
        }
    }
}

/// Builds drafts from a record's fields with the shared normalizations.
pub struct Mapper<'a> {
    pub fields: &'a BTreeMap<String, String>,
    pub out: Vec<Draft>,
}

impl<'a> Mapper<'a> {
    pub fn new(fields: &'a BTreeMap<String, String>) -> Self {
        Mapper { fields, out: Vec::new() }
    }

    pub fn get(&self, key: &str) -> Option<&'a str> {
        self.fields.get(key).map(|s| s.as_str()).filter(|s| !s.trim().is_empty())
    }

    pub fn push(&mut self, d: Draft) {
        self.out.push(d);
    }

    pub fn imo(&mut self, key: &str) -> Option<ImoCheck> {
        let v = self.get(key)?;
        match normalize::imo(v) {
            Some((n, check)) => {
                self.push(Draft::new(attr::IMO, "identifier", v, Some(n)).checked(check.as_str()));
                Some(check)
            }
            None if is_placeholder(v) => None,
            None => {
                self.push(Draft::new(attr::IMO, "identifier", v, None).checked("invalid"));
                None
            }
        }
    }

    pub fn name(&mut self, attribute: &'static str, key: &str) {
        if let Some(v) = self.get(key) {
            self.name_value(attribute, v);
        }
    }

    pub fn name_value(&mut self, attribute: &'static str, v: &str) {
        if let Some(t) = normalize::name_text(v) {
            self.push(Draft::new(attribute, "string", &t, normalize::name_key(&t)));
        }
    }

    pub fn callsign(&mut self, key: &str) {
        if let Some(v) = self.get(key) {
            if let Some(c) = normalize::call_sign(v) {
                self.push(Draft::new(attr::CALLSIGN, "identifier", v, Some(c)));
            }
        }
    }

    pub fn flag(&mut self, key: &str) {
        if let Some(v) = self.get(key) {
            match countries::alpha3(v) {
                Some(a3) => self.push(Draft::new(attr::FLAG, "code", v, Some(a3.to_string()))),
                None if is_placeholder(v) || v.eq_ignore_ascii_case("unknown") => {}
                None => self.push(Draft::new(attr::FLAG, "code", v, None).checked("unrecognised")),
            }
        }
    }

    /// Free text kept as published, upper-cased for matching.
    pub fn text(&mut self, attribute: &'static str, key: &str) {
        if let Some(v) = self.get(key) {
            if !is_placeholder(v) {
                let t = v.split_whitespace().collect::<Vec<_>>().join(" ");
                self.push(Draft::new(attribute, "string", v, Some(t.to_uppercase())));
            }
        }
    }

    pub fn year(&mut self, attribute: &'static str, key: &str) {
        if let Some(v) = self.get(key) {
            if let Some(y) = normalize::build_year(v) {
                self.push(Draft::new(attribute, "integer", v, Some(y.to_string())));
            }
        }
    }

    pub fn integer(&mut self, attribute: &'static str, key: &str) {
        if let Some(v) = self.get(key) {
            if let Some(x) = normalize::positive(v).filter(|x| x.fract() == 0.0) {
                self.push(Draft::new(attribute, "integer", v, Some((x as i64).to_string())));
            }
        }
    }

    /// A measurement, converted to the unit `attribute` names by `factor`
    /// (1 when the source already uses it); `unit` records the source's unit.
    pub fn measure(&mut self, attribute: &'static str, key: &str, unit: &str, factor: f64) {
        if let Some(v) = self.get(key) {
            if let Some(x) = normalize::positive(v) {
                self.push(Draft::new(attribute, "number", v, Some(normalize::fmt_number(x * factor))).qualified(unit));
            }
        }
    }

    pub fn date(&mut self, attribute: &'static str, key: &str) -> Option<DateTime<Utc>> {
        let v = self.get(key)?;
        let d = parse_date(v)?;
        let t = midnight(d);
        self.push(Draft::new(attribute, "date", v, Some(d.to_string())));
        Some(t)
    }
}

pub fn is_placeholder(v: &str) -> bool {
    matches!(v.trim().to_ascii_uppercase().as_str(), "" | "-" | "N/A" | "NA" | "NONE" | "0" | "NULL")
}

/// `yyyy-mm-dd` (with anything after), `yyyymmdd`, or `dd/mm/yy`.
pub fn parse_date(s: &str) -> Option<NaiveDate> {
    let s = s.trim();
    if s.len() >= 10 {
        if let Ok(d) = NaiveDate::parse_from_str(&s[..10], "%Y-%m-%d") {
            return Some(d);
        }
    }
    if s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()) {
        return NaiveDate::parse_from_str(s, "%Y%m%d").ok();
    }
    if s.len() == 8 && s.as_bytes()[2] == b'/' {
        return NaiveDate::parse_from_str(s, "%d/%m/%y").ok();
    }
    None
}

pub fn midnight(d: NaiveDate) -> DateTime<Utc> {
    d.and_hms_opt(0, 0, 0).expect("midnight").and_utc()
}

/// The adapters, by the name a source's config gives.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Adapter {
    TcLarge,
    TcSmall,
    Iacs,
    Psix,
    UscgMisle,
    Gfw,
}

/// Where a fetch gets its files and puts its working state.
pub struct FetchCtx<'a> {
    pub http: &'a Http,
    /// Read the source's files from here instead of fetching.
    pub local_dir: Option<&'a Path>,
    /// Working space that survives a restart (PSIX keeps fetched slices).
    pub scratch: &'a Path,
    /// Identifiers the registry already holds, for sources that are asked
    /// about known vessels (GFW); empty for the others.
    pub lookups: &'a [gfw::Lookup],
}

impl Adapter {
    pub fn from_name(name: &str) -> Result<Self> {
        Ok(match name {
            "tc_large" => Adapter::TcLarge,
            "tc_small" => Adapter::TcSmall,
            "iacs" => Adapter::Iacs,
            "psix" => Adapter::Psix,
            "uscg_misle" => Adapter::UscgMisle,
            "gfw" => Adapter::Gfw,
            _ => bail!("unknown adapter '{name}'"),
        })
    }

    /// Recorded with every source record; bump when parsing or mapping
    /// changes what a record yields.
    pub fn parser_version(self) -> &'static str {
        match self {
            Adapter::TcLarge => "tc_large/1",
            Adapter::TcSmall => "tc_small/1",
            Adapter::Iacs => "iacs/1",
            Adapter::Psix => "psix/1",
            Adapter::UscgMisle => "uscg_misle/1",
            Adapter::Gfw => "gfw/1",
        }
    }

    pub async fn fetch(self, cfg: &SourceConfig, ctx: &FetchCtx<'_>) -> Result<Fetched> {
        match self {
            Adapter::TcLarge | Adapter::TcSmall => tc::fetch(cfg, ctx).await,
            Adapter::Iacs => iacs::fetch(cfg, ctx).await,
            Adapter::Psix => psix::fetch(cfg, ctx).await,
            Adapter::UscgMisle => misle::fetch(cfg, ctx).await,
            Adapter::Gfw => gfw::fetch(cfg, ctx).await,
        }
    }

    pub fn parse(self, cfg: &SourceConfig, fetched: &Fetched) -> Result<Parsed> {
        match self {
            Adapter::TcLarge => tc::parse(tc::Register::Large, &fetched.objects),
            Adapter::TcSmall => tc::parse(tc::Register::Small, &fetched.objects),
            Adapter::Iacs => iacs::parse(&fetched.objects),
            Adapter::Psix => psix::parse(cfg, &fetched.objects),
            Adapter::UscgMisle => misle::parse(&fetched.objects),
            Adapter::Gfw => gfw::parse(&fetched.objects),
        }
    }

    /// The assertions a record's fields make.
    pub fn map(self, cfg: &SourceConfig, fields: &BTreeMap<String, String>) -> Vec<Draft> {
        let mut drafts = match self {
            Adapter::TcLarge => tc::map(tc::Register::Large, cfg, fields),
            Adapter::TcSmall => tc::map(tc::Register::Small, cfg, fields),
            Adapter::Iacs => iacs::map(fields),
            Adapter::Psix => psix::map(fields),
            Adapter::UscgMisle => misle::map(fields),
            Adapter::Gfw => gfw::map(fields),
        };
        drafts.sort();
        drafts.dedup();
        drafts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates() {
        assert_eq!(parse_date("2026-10-17"), NaiveDate::from_ymd_opt(2026, 10, 17));
        assert_eq!(parse_date("2026-10-17T00:00:00"), NaiveDate::from_ymd_opt(2026, 10, 17));
        assert_eq!(parse_date("20250724"), NaiveDate::from_ymd_opt(2025, 7, 24));
        assert_eq!(parse_date("25/07/24"), NaiveDate::from_ymd_opt(2024, 7, 25));
        assert_eq!(parse_date("x"), None);
    }
}
