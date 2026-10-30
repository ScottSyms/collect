//! USCG MISLE Deficiency Data Report: one Excel workbook per year from 2018,
//! listing every deficiency the Coast Guard closed on foreign and US
//! vessels. The same workbooks feed `collect-foundation evidence uscg`; here
//! they are read for who the vessels were.
//!
//! Each row names the inspected vessel by its primary VIN and VIN type (an
//! IMO number for most foreign ships, the US official number for documented
//! US vessels, a state number, a HIN, ...), with its name, flag and class as
//! MISLE recorded them that day. The VIN is the subject. One record is kept
//! per subject per distinct set of particulars, with the dates it was first
//! and last observed, so a renamed or reflagged vessel has one record per
//! identity it was seen under, and the newest wins.
//!
//! These are observations, not register entries: the record says when MISLE
//! saw a value, not when it started or stopped holding, so assertions carry
//! no validity dates; the observation window goes in the qualifier.
//!
//! The Coast Guard's site refuses non-browser downloads, so the workbooks
//! are saved by hand and ingested with `--source-dir`.

use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;

use anyhow::{bail, Context, Result};
use calamine::{Data, Reader, Xlsx};
use chrono::NaiveDate;
use sha2::{Digest, Sha256};

use super::{attr, is_placeholder, midnight, parse_date, Draft, FetchCtx, Fetched, Mapper, Parsed, Record, Rejected};
use crate::config::SourceConfig;
use crate::countries;
use crate::fetch::{files_in, read_local, RawObject};
use crate::normalize;

// Field names in the stored records.
const VIN: &str = "Primary VIN";
const VIN_TYPE: &str = "Primary VIN Type";
const NAME: &str = "Vessel Name";
const CLASS: &str = "Vessel Class";
const TYPE: &str = "Vessel Type";
const FLAG: &str = "Flag";
const CLASS_DOCUMENT: &str = "Class Document";
const MISLE_ID: &str = "Vessel MISLE ID";
const FIRST: &str = "First Observed";
const LAST: &str = "Last Observed";
const ACTIVITIES: &str = "Activities";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Col {
    Date,
    Activity,
    Vin,
    VinType,
    Name,
    Class,
    Type,
    Flag,
    ClassDocument,
    MisleId,
}

/// Header names (lowercase, letters and digits only) in every layout:
/// 2018-2022, 2023-2024 with class and certificate issuers, 2025 on with
/// underscored names and MISLE's own vessel id.
fn column(header: &str) -> Option<Col> {
    let h: String = header.chars().filter(|c| c.is_ascii_alphanumeric()).collect::<String>().to_ascii_lowercase();
    Some(match h.as_str() {
        "inspectiondate" | "activitystartdt" => Col::Date,
        "activityid" => Col::Activity,
        "primaryvin" => Col::Vin,
        "primaryvintype" | "vintype" => Col::VinType,
        "vesselname" => Col::Name,
        "vesselclass" => Col::Class,
        "vesseltype" => Col::Type,
        "flag" => Col::Flag,
        "classdocument" => Col::ClassDocument,
        "vesselmisleid" => Col::MisleId,
        _ => return None,
    })
}

pub async fn fetch(_cfg: &SourceConfig, ctx: &FetchCtx<'_>) -> Result<Fetched> {
    let Some(dir) = ctx.local_dir else {
        bail!("the Coast Guard's site refuses non-browser downloads; save the MISLE workbooks and pass --source-dir");
    };
    let objects: Vec<RawObject> = files_in(dir, &["xlsx"])?.iter().map(|f| read_local(f)).collect::<Result<_>>()?;
    anyhow::ensure!(!objects.is_empty(), "no .xlsx in {}", dir.display());
    let version = format!(
        "{} workbooks: {}",
        objects.len(),
        objects.iter().map(|o| o.name.as_str()).collect::<Vec<_>>().join(", ")
    );
    Ok(Fetched {
        source_version: Some(version),
        source_published_at: None,
        objects,
        // Incremental: a vessel missing from these files is not removed.
        complete: true,
        warnings: Vec::new(),
    })
}

/// A cell as text: whole numbers without `.0`.
fn cell_text(c: &Data) -> Option<String> {
    let s = match c {
        Data::String(s) | Data::DateTimeIso(s) | Data::DurationIso(s) => s.trim().to_string(),
        Data::Int(i) => i.to_string(),
        Data::Float(f) if f.fract() == 0.0 && f.abs() < 1e15 => format!("{}", *f as i64),
        Data::Float(f) => f.to_string(),
        Data::Bool(b) => b.to_string(),
        Data::DateTime(dt) => dt.as_f64().to_string(),
        Data::Error(_) | Data::Empty => return None,
    };
    (!s.is_empty()).then_some(s)
}

/// An Excel serial day number (1900 system) as a date.
fn excel_date(serial: f64) -> Option<NaiveDate> {
    if !(1.0..2_958_466.0).contains(&serial) {
        return None;
    }
    NaiveDate::from_ymd_opt(1899, 12, 30)?.checked_add_days(chrono::Days::new(serial.floor() as u64))
}

fn cell_date(c: &Data) -> Option<NaiveDate> {
    match c {
        Data::DateTime(dt) => excel_date(dt.as_f64()),
        Data::Float(f) => excel_date(*f),
        Data::Int(i) => excel_date(*i as f64),
        Data::String(s) | Data::DateTimeIso(s) => {
            parse_date(s).or_else(|| NaiveDate::parse_from_str(s.trim(), "%m/%d/%Y").ok())
        }
        _ => None,
    }
}

/// The subject id: a short code for the VIN type, and the VIN.
fn subject(vin_type: &str, vin: &str) -> String {
    let code = match vin_type.trim().to_ascii_uppercase().as_str() {
        "IMO NUMBER" => "IMO".to_string(),
        "OFFICIAL NUMBER (U.S.)" => "USON".to_string(),
        "OFFICIAL NUMBER (FOREIGN)" => "ON".to_string(),
        "STATE NUMBER" => "STATE".to_string(),
        "COAST GUARD NUMBER" => "CG".to_string(),
        "MMSI NUMBER" => "MMSI".to_string(),
        other => other.chars().filter(|c| c.is_ascii_alphanumeric()).collect(),
    };
    format!("{code}:{}", vin.trim().to_ascii_uppercase())
}

/// One vessel identity as observed, before it becomes a record.
struct Seen {
    subject: String,
    fields: BTreeMap<String, String>,
    first: Option<NaiveDate>,
    last: Option<NaiveDate>,
    activities: std::collections::BTreeSet<String>,
    object: usize,
}

pub fn parse(objects: &[RawObject]) -> Result<Parsed> {
    let mut parsed = Parsed::default();
    // (subject, particulars) -> what was seen.
    let mut seen: BTreeMap<(String, String), Seen> = BTreeMap::new();
    for (oi, object) in objects.iter().enumerate() {
        let mut wb: Xlsx<_> = Xlsx::new(Cursor::new(object.bytes.as_ref()))
            .with_context(|| format!("{}: opening the workbook", object.name))?;
        let range = wb
            .worksheet_range_at(0)
            .with_context(|| format!("{}: no worksheet", object.name))?
            .with_context(|| format!("{}: reading the first worksheet", object.name))?;
        let mut rows = range.rows();
        let header: Vec<Option<Col>> = rows
            .next()
            .with_context(|| format!("{}: empty worksheet", object.name))?
            .iter()
            .map(|h| cell_text(h).and_then(|h| column(&h)))
            .collect();
        for need in [Col::Date, Col::Vin, Col::VinType, Col::Name, Col::Flag] {
            anyhow::ensure!(header.contains(&Some(need)), "{}: no {need:?} column; has the layout changed?", object.name);
        }
        for (ri, row) in rows.enumerate() {
            let cells: HashMap<Col, &Data> = header.iter().zip(row).filter_map(|(c, d)| c.map(|c| (c, d))).collect();
            let get = |c: Col| cells.get(&c).and_then(|d| cell_text(d));
            if cells.values().all(|d| matches!(d, Data::Empty)) {
                continue;
            }
            let (vin, vin_type) = (get(Col::Vin), get(Col::VinType));
            let usable = |v: &str| !is_placeholder(v) && !v.eq_ignore_ascii_case("NONUMBERFOUND") && !v.eq_ignore_ascii_case("UNKNOWN");
            let (Some(vin), Some(vin_type)) = (vin.filter(|v| usable(v)), vin_type) else {
                parsed.rejected.push(Rejected {
                    location: format!("{} row {}", object.name, ri + 2),
                    raw: serde_json::to_string(&cells.iter().map(|(k, v)| (format!("{k:?}"), cell_text(v))).collect::<BTreeMap<_, _>>()).ok(),
                    reasons: vec!["no usable primary VIN".into()],
                    object: oi,
                });
                continue;
            };
            let subject = subject(&vin_type, &vin);
            let mut fields = BTreeMap::new();
            fields.insert(VIN.to_string(), vin.trim().to_string());
            fields.insert(VIN_TYPE.to_string(), vin_type.trim().to_string());
            for (col, key) in [
                (Col::Name, NAME),
                (Col::Class, CLASS),
                (Col::Type, TYPE),
                (Col::Flag, FLAG),
                (Col::ClassDocument, CLASS_DOCUMENT),
                (Col::MisleId, MISLE_ID),
            ] {
                if let Some(v) = get(col) {
                    fields.insert(key.to_string(), v);
                }
            }
            // Particulars: everything but the observation dates. Class and
            // type columns exist only in some years, so they aren't part of
            // the identity a record stands for.
            let particulars = [NAME, FLAG].iter().map(|k| fields.get(*k).map_or("", |s| s.as_str())).collect::<Vec<_>>().join("|");
            let date = cells.get(&Col::Date).and_then(|d| cell_date(d));
            let s = seen.entry((subject.clone(), particulars)).or_insert_with(|| Seen {
                subject,
                fields: BTreeMap::new(),
                first: None,
                last: None,
                activities: Default::default(),
                object: oi,
            });
            if date >= s.last || s.last.is_none() {
                s.object = oi;
            }
            // Fields present in any observation; the newest one's win.
            for (k, v) in fields {
                if date >= s.last || !s.fields.contains_key(&k) {
                    s.fields.insert(k, v);
                }
            }
            s.first = match (s.first, date) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            s.last = s.last.max(date);
            if let Some(a) = get(Col::Activity) {
                s.activities.insert(a);
            }
        }
    }

    for ((_, particulars), mut s) in seen {
        if let Some(d) = s.first {
            s.fields.insert(FIRST.into(), d.to_string());
        }
        if let Some(d) = s.last {
            s.fields.insert(LAST.into(), d.to_string());
        }
        if !s.activities.is_empty() {
            s.fields.insert(ACTIVITIES.into(), s.activities.len().to_string());
        }
        let digest = hex::encode(&Sha256::digest(particulars.as_bytes())[..6]);
        parsed.records.push(Record {
            source_record_id: format!("{}#{digest}", s.subject),
            subject_id: s.subject,
            fields: s.fields,
            object: s.object,
            published_at: s.last.map(midnight),
        });
    }
    if let Some(newest) = parsed.records.iter().filter_map(|r| r.published_at).max() {
        parsed.warnings.push(format!("newest inspection in the workbooks: {}", newest.date_naive()));
    }
    Ok(parsed)
}

pub fn map(fields: &BTreeMap<String, String>) -> Vec<Draft> {
    let mut m = Mapper::new(fields);
    let window = match (m.get(FIRST), m.get(LAST)) {
        (Some(a), Some(b)) if a == b => Some(format!("observed {a}")),
        (Some(a), Some(b)) => Some(format!("observed {a} to {b}")),
        (a, b) => a.or(b).map(|d| format!("observed {d}")),
    };
    let observed = |d: Draft| match &window {
        Some(w) if d.qualifier.is_none() => d.qualified(w.clone()),
        _ => d,
    };
    let flag = m.get(FLAG).and_then(countries::alpha3);

    if let (Some(vin), Some(vin_type)) = (m.get(VIN), m.get(VIN_TYPE)) {
        let vin = vin.trim();
        match vin_type.trim().to_ascii_uppercase().as_str() {
            "IMO NUMBER" => {
                m.imo(VIN);
            }
            // The Coast Guard issues US official numbers itself.
            "OFFICIAL NUMBER (U.S.)" => m.push(
                Draft::new(attr::NATIONAL_REGISTRY, "identifier", vin, Some(vin.to_ascii_uppercase()))
                    .qualified("US")
                    .checked("valid"),
            ),
            // A registration issued by the flag state; scoped to it only when
            // the flag is known.
            "OFFICIAL NUMBER (FOREIGN)" => {
                if let Some(a2) = flag.and_then(countries::alpha2_of_alpha3) {
                    m.push(Draft::new(attr::NATIONAL_REGISTRY, "identifier", vin, Some(vin.to_ascii_uppercase())).qualified(a2));
                }
            }
            "MMSI NUMBER" => match normalize::mmsi(vin) {
                Some(n) => m.push(Draft::new(attr::MMSI, "identifier", vin, Some(n)).qualified(normalize::mmsi_kind(vin))),
                None => m.push(Draft::new(attr::MMSI, "identifier", vin, None).checked("invalid")),
            },
            "HIN" | "HULL NUMBER" => m.push(Draft::new(attr::HULL_NUMBER, "identifier", vin, Some(vin.to_ascii_uppercase())).qualified(vin_type.trim())),
            // State numbers, Coast Guard numbers, ABS and EPIRB numbers stay
            // in the subject id and the record; nothing links on them.
            _ => {}
        }
    }
    if let Some(id) = m.get(MISLE_ID) {
        m.push(Draft::new(attr::PSIX_VESSEL_ID, "identifier", id, Some(id.trim().to_string())).qualified("US"));
    }
    // A name that is just the VIN (common for state-numbered boats) or
    // UNNAMED says nothing.
    if let Some(name) = m.get(NAME) {
        let n = name.trim();
        if !n.eq_ignore_ascii_case("UNNAMED") && Some(n) != m.get(VIN).map(str::trim) {
            m.name_value(attr::NAME, n);
        }
    }
    m.flag(FLAG);
    m.text(attr::VESSEL_TYPE, CLASS);
    m.text(attr::VESSEL_SUBTYPE, TYPE);
    m.text(attr::CLASS_SOCIETY, CLASS_DOCUMENT);
    let mut out = m.out;
    for d in &mut out {
        if d.attribute != attr::NATIONAL_REGISTRY && d.attribute != attr::PSIX_VESSEL_ID && d.attribute != attr::MMSI && d.attribute != attr::HULL_NUMBER {
            *d = observed(d.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    enum Cell {
        S(&'static str),
        N(f64),
        D(u16, u8, u8),
        E,
    }

    /// A workbook laid out as the Coast Guard's are.
    fn workbook(name: &str, header: &[&str], rows: &[Vec<Cell>]) -> RawObject {
        use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};
        let mut wb = Workbook::new();
        let ws = wb.add_worksheet();
        let date_fmt = Format::new().set_num_format("m/d/yyyy");
        for (c, h) in header.iter().enumerate() {
            ws.write_string(0, c as u16, *h).unwrap();
        }
        for (r, row) in rows.iter().enumerate() {
            let r = r as u32 + 1;
            for (c, cell) in row.iter().enumerate() {
                let c = c as u16;
                match cell {
                    Cell::S(s) => ws.write_string(r, c, *s).map(|_| ()).unwrap(),
                    Cell::N(n) => ws.write_number(r, c, *n).map(|_| ()).unwrap(),
                    Cell::D(y, m, d) => {
                        ws.write_datetime_with_format(r, c, ExcelDateTime::from_ymd(*y, *m, *d).unwrap(), &date_fmt).map(|_| ()).unwrap()
                    }
                    Cell::E => {}
                }
            }
        }
        RawObject::new(name, wb.save_to_buffer().unwrap().into())
    }

    fn objects() -> Vec<RawObject> {
        use Cell::*;
        let old = ["Inspection Date", "Activity ID", "Unit", "Primary VIN", "Primary VIN Type", "Vessel Name", "Vessel Class", "Flag", "System", "Component", "Resolution Action", "Description"];
        let y2019 = workbook(
            "U.S.C.G-DeficiencyData 2019.xlsx",
            &old,
            &[
                vec![D(2019, 3, 2), N(1.0), S("Sector X"), S("9194957"), S("IMO Number"), S("SANTA MARIA"), S("Refrigerated Cargo Ship"), S("PANAMA"), S("s"), S("c"), S("a"), S("d")],
                vec![D(2019, 5, 1), N(2.0), S("Sector X"), S("9194957"), S("IMO Number"), S("SANTA MARIA"), S("Refrigerated Cargo Ship"), S("PANAMA"), S("s"), S("c"), S("a"), S("d")],
                vec![D(2019, 6, 1), N(3.0), S("Sector Y"), S("FL6076PU"), S("State Number"), S("FL6076PU"), S("Recreational"), S("UNITED STATES"), S("s"), S("c"), S("a"), S("d")],
                vec![D(2019, 6, 2), N(4.0), S("Sector Y"), S("NONUMBERFOUND"), S("Other"), S("UNNAMED"), S("Recreational"), S("UNITED STATES"), S("s"), S("c"), S("a"), S("d")],
                vec![D(2019, 7, 1), N(5.0), S("Sector Y"), S("822480"), S("Official Number (Foreign)"), S("PML 9000"), S("Passenger Ship"), S("CANADA"), S("s"), S("c"), S("a"), S("d")],
            ],
        );
        let y2025 = workbook(
            "Website_Deficiency_Data_Nov_2025.xlsx",
            &["ActivityID", "ActivityStartDt", "Vessel_MISLE_ID", "VesselName", "Primary_VIN", "VIN_Type", "Vessel_Class", "Vessel_Type", "Flag", "DeficiencyID"],
            &[
                vec![N(10.0), D(2025, 4, 3), N(555.0), S("KANO REEFER"), S("9194957"), S("IMO Number"), S("Refrigerated Cargo Ship"), S("General Cargo"), S("LIBERIA"), N(1.0)],
                vec![N(11.0), D(2025, 4, 3), N(37.0), S("FRANK SINATRA"), S("1091257"), S("Official Number (U.S.)"), S("Passenger Ship"), S("Ferry"), S("UNITED STATES"), N(2.0)],
                vec![N(11.0), D(2025, 4, 3), N(37.0), S("FRANK SINATRA"), S("1091257"), S("Official Number (U.S.)"), S("Passenger Ship"), S("Ferry"), S("UNITED STATES"), N(3.0)],
                vec![N(12.0), D(2025, 5, 1), E, S("SEA ROVER"), S("366123450"), S("MMSI Number"), S("Fishing Vessel"), E, S("UNITED STATES"), N(4.0)],
            ],
        );
        vec![y2019, y2025]
    }

    #[test]
    fn one_record_per_identity_seen() {
        let p = parse(&objects()).unwrap();
        assert_eq!(p.rejected.len(), 1, "NONUMBERFOUND is no vessel id");
        let ids: Vec<(&str, &str)> = p.records.iter().map(|r| (r.subject_id.as_str(), r.fields.get(NAME).unwrap().as_str())).collect();
        assert_eq!(
            ids,
            [
                ("IMO:9194957", "KANO REEFER"),
                ("IMO:9194957", "SANTA MARIA"),
                ("MMSI:366123450", "SEA ROVER"),
                ("ON:822480", "PML 9000"),
                ("STATE:FL6076PU", "FL6076PU"),
                ("USON:1091257", "FRANK SINATRA"),
            ]
        );
        let santa = &p.records[1];
        assert_eq!(santa.fields.get(FIRST).unwrap(), "2019-03-02");
        assert_eq!(santa.fields.get(LAST).unwrap(), "2019-05-01");
        assert_eq!(santa.fields.get(ACTIVITIES).unwrap(), "2");
        assert_eq!(santa.published_at, Some(midnight(NaiveDate::from_ymd_opt(2019, 5, 1).unwrap())));
        assert_ne!(santa.source_record_id, p.records[0].source_record_id);
        assert_eq!(p.records[5].fields.get(ACTIVITIES).unwrap(), "1", "one activity, two deficiencies");
        assert_eq!(p.records[5].fields.get(MISLE_ID).unwrap(), "37");

        // Parsing is stable: the same files give the same record ids.
        let again = parse(&objects()).unwrap();
        assert_eq!(p.records, again.records);
    }

    fn drafts(p: &Parsed, subject: &str) -> Vec<(String, Option<String>, Option<String>)> {
        p.records
            .iter()
            .filter(|r| r.subject_id == subject)
            .flat_map(|r| map(&r.fields))
            .map(|d| (d.attribute.to_string(), d.value_normalized, d.qualifier))
            .collect()
    }

    #[test]
    fn maps_identifiers_by_vin_type() {
        let p = parse(&objects()).unwrap();
        let us = drafts(&p, "USON:1091257");
        assert!(us.contains(&("national_registry".into(), Some("1091257".into()), Some("US".into()))));
        assert!(us.contains(&("psix_vessel_id".into(), Some("37".into()), Some("US".into()))));
        assert!(us.contains(&("flag_state".into(), Some("USA".into()), Some("observed 2025-04-03".into()))));
        assert!(us.contains(&("vessel_subtype".into(), Some("FERRY".into()), Some("observed 2025-04-03".into()))));

        let imo = drafts(&p, "IMO:9194957");
        assert!(imo.contains(&("imo".into(), Some("9194957".into()), Some("observed 2019-03-02 to 2019-05-01".into()))));
        assert!(imo.contains(&("flag_state".into(), Some("LBR".into()), Some("observed 2025-04-03".into()))));
        assert!(imo.iter().any(|d| d.0 == "name" && d.1.is_some()));

        let ca = drafts(&p, "ON:822480");
        assert!(ca.contains(&("national_registry".into(), Some("822480".into()), Some("CA".into()))));

        let mmsi = drafts(&p, "MMSI:366123450");
        assert!(mmsi.iter().any(|d| d.0 == "mmsi" && d.1.as_deref() == Some("366123450")));

        let state = drafts(&p, "STATE:FL6076PU");
        assert!(!state.iter().any(|d| d.0 == "name"), "a name that is just the VIN says nothing");
        assert!(!state.iter().any(|d| d.0 == "national_registry"));
    }

    #[test]
    fn maps_headers_of_every_layout() {
        assert_eq!(column("Inspection Date"), Some(Col::Date));
        assert_eq!(column("ActivityStartDt"), Some(Col::Date));
        assert_eq!(column("Primary_VIN"), Some(Col::Vin));
        assert_eq!(column("VIN_Type"), Some(Col::VinType));
        assert_eq!(column("Vessel_MISLE_ID"), Some(Col::MisleId));
        assert_eq!(column("Class Document"), Some(Col::ClassDocument));
        assert_eq!(column("Description"), None);
    }
}
