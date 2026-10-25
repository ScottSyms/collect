//! Transport Canada's vessel registers, as the XLSX files open.canada.ca
//! publishes: the Canadian Register of Large Vessels and the Small
//! Commercial Vessel Registry. Each row is one registered vessel, keyed by
//! its official number (the national registration, issued in `CA`).
//!
//! The files are regenerated daily, so their `Last-Modified` is not the age
//! of the data; the newest registration date in a file is reported instead
//! as a hint. Neither file has call signs or MMSIs, and the small register
//! has no names.

use std::collections::BTreeMap;
use std::io::Cursor;

use anyhow::{bail, Context, Result};
use calamine::{Data, Reader, Xlsx};

use super::{attr, parse_date, Draft, FetchCtx, Fetched, Mapper, Parsed, Record, Rejected};
use crate::config::SourceConfig;
use crate::fetch::{files_in, read_local, RawObject};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Register {
    Large,
    Small,
}

const OFFICIAL_NUMBER: &str = "Official Number";

impl Register {
    /// Columns that must be present, or the layout has changed.
    fn required(self) -> &'static [&'static str] {
        match self {
            Register::Large => &[OFFICIAL_NUMBER, "Vessel Name", "IMO Vessel Number", "Port of Registry", "Length"],
            Register::Small => &[OFFICIAL_NUMBER, "Certificate First Registry Date", "Main Descriptor", "Vessel Length"],
        }
    }

    fn registration_column(self) -> &'static str {
        match self {
            Register::Large => "Certificate Issuing date or Registration date",
            Register::Small => "Certificate First Registry Date",
        }
    }
}

pub async fn fetch(cfg: &SourceConfig, ctx: &FetchCtx<'_>) -> Result<Fetched> {
    let object = match ctx.local_dir {
        Some(dir) => {
            let files = files_in(dir, &["xlsx"])?;
            let [file] = files.as_slice() else {
                bail!("expected exactly one .xlsx in {}, found {}", dir.display(), files.len());
            };
            read_local(file)?
        }
        None => {
            let name = cfg.source_url.rsplit('/').next().unwrap_or("register.xlsx");
            ctx.http.get(&cfg.source_url, name).await?
        }
    };
    let version = object.last_modified.clone().or_else(|| object.etag.clone());
    Ok(Fetched {
        source_version: version.map(|v| format!("http {v}")),
        // The file's own date is its regeneration, not its data's age.
        source_published_at: None,
        objects: vec![object],
        complete: true,
        warnings: Vec::new(),
    })
}

/// A cell as text: dates as `yyyy-mm-dd`, whole numbers without `.0`.
fn cell_text(c: &Data) -> Option<String> {
    let s = match c {
        Data::String(s) | Data::DateTimeIso(s) | Data::DurationIso(s) => s.trim().to_string(),
        Data::Int(i) => i.to_string(),
        Data::Float(f) if f.fract() == 0.0 && f.abs() < 1e15 => format!("{}", *f as i64),
        Data::Float(f) => f.to_string(),
        Data::Bool(b) => b.to_string(),
        Data::DateTime(dt) => excel_date(dt.as_f64())?.to_string(),
        Data::Error(_) | Data::Empty => return None,
    };
    (!s.is_empty()).then_some(s)
}

/// An Excel serial date (days since 1899-12-30, the 1900 date system).
fn excel_date(serial: f64) -> Option<chrono::NaiveDate> {
    let base = chrono::NaiveDate::from_ymd_opt(1899, 12, 30)?;
    (serial.is_finite() && serial >= 1.0).then(|| base + chrono::Duration::days(serial.floor() as i64))
}

pub fn parse(register: Register, objects: &[RawObject]) -> Result<Parsed> {
    let object = objects.first().context("no file fetched")?;
    let mut wb: Xlsx<_> = Xlsx::new(Cursor::new(object.bytes.as_ref())).context("opening the workbook")?;
    let range = wb
        .worksheet_range_at(0)
        .context("the workbook has no sheet")?
        .context("reading the first sheet")?;
    let rows: Vec<&[Data]> = range.rows().collect();
    let header_at = rows
        .iter()
        .take(20)
        .position(|r| r.iter().any(|c| cell_text(c).as_deref() == Some(OFFICIAL_NUMBER)))
        .with_context(|| format!("no '{OFFICIAL_NUMBER}' header in the first 20 rows; has the layout changed?"))?;
    let header: Vec<Option<String>> = rows[header_at].iter().map(cell_text).collect();
    for want in register.required() {
        anyhow::ensure!(
            header.iter().any(|h| h.as_deref() == Some(*want)),
            "column '{want}' is missing; has the layout changed?"
        );
    }

    let mut parsed = Parsed::default();
    let mut newest_registration = None;
    // A few official numbers appear on more than one row with different
    // particulars; each row is its own record of the one subject.
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (i, row) in rows.iter().enumerate().skip(header_at + 1) {
        let mut fields = BTreeMap::new();
        for (h, c) in header.iter().zip(row.iter()) {
            if let (Some(h), Some(v)) = (h, cell_text(c)) {
                fields.insert(h.clone(), v);
            }
        }
        if fields.is_empty() {
            continue;
        }
        let Some(official) = fields.get(OFFICIAL_NUMBER).map(|s| s.trim().to_ascii_uppercase()) else {
            parsed.rejected.push(Rejected {
                location: format!("row {}", i + 1),
                raw: serde_json::to_string(&fields).ok(),
                reasons: vec!["no official number".into()],
                object: 0,
            });
            continue;
        };
        if let Some(d) = fields.get(register.registration_column()).and_then(|s| parse_date(s)) {
            newest_registration = newest_registration.max(Some(d));
        }
        let n = seen.entry(official.clone()).or_default();
        *n += 1;
        let record_id = if *n == 1 { official.clone() } else { format!("{official}#{n}") };
        parsed.records.push(Record {
            source_record_id: record_id,
            subject_id: official,
            fields,
            object: 0,
            published_at: None,
        });
    }
    if let Some(d) = newest_registration {
        parsed.warnings.push(format!("newest registration date in the file: {d} (a hint at the data's age)"));
    }
    Ok(parsed)
}

pub fn map(register: Register, cfg: &SourceConfig, fields: &BTreeMap<String, String>) -> Vec<Draft> {
    let jurisdiction = cfg.jurisdiction.clone().unwrap_or_else(|| "CA".into());
    let flag = crate::countries::alpha3_of_alpha2(&jurisdiction).unwrap_or("CAN");
    let mut m = Mapper::new(fields);
    let registered = m.date(attr::REGISTRATION_DATE, register.registration_column());
    if let Some(v) = m.get(OFFICIAL_NUMBER) {
        m.push(
            Draft::new(attr::NATIONAL_REGISTRY, "identifier", v, Some(v.trim().to_ascii_uppercase()))
                .qualified(jurisdiction.clone())
                .checked("valid")
                .from(registered),
        );
    }
    // Being on the register is itself a claim about flag and status.
    m.push(Draft::new(attr::FLAG, "code", &jurisdiction, Some(flag.to_string())).qualified("implied by register").from(registered));
    m.push(Draft::new(attr::REGISTRY_STATUS, "code", "REGISTERED", Some("REGISTERED".into())).qualified("implied by register"));
    m.text(attr::HULL_NUMBER, "Hull Number");
    match register {
        Register::Large => {
            m.name(attr::NAME, "Vessel Name");
            m.imo("IMO Vessel Number");
            m.year(attr::YEAR_BUILT, "Year of Build");
            m.year(attr::YEAR_REBUILT, "Year of Latest Rebuild");
            if let Some(port) = m.get("Port of Registry") {
                m.push(Draft::new(attr::PORT_OF_REGISTRY, "string", port, Some(port.trim().to_uppercase())).from(registered));
            }
            m.text(attr::VESSEL_TYPE, "Vessel Descriptor");
            m.measure(attr::GROSS_TONNAGE, "Gross Tonnage", "GT", 1.0);
            m.measure(attr::NET_TONNAGE, "Net Tonnage", "NT", 1.0);
            m.text(attr::CONSTRUCTION_TYPE, "Construction Type");
            m.text(attr::HULL_MATERIAL, "Construction Material");
            m.measure(attr::LENGTH_M, "Length", "m", 1.0);
            m.measure(attr::BEAM_M, "Breadth", "m", 1.0);
            m.measure(attr::DEPTH_M, "Depth", "m", 1.0);
            m.text(attr::ENGINE_TYPE, "Engine Type");
            m.integer(attr::ENGINE_COUNT, "Number of Engines");
            m.text(attr::PROPULSION_TYPE, "Propulsion Type");
            m.measure(attr::SPEED_KNOTS, "Speed (Knots)", "kn", 1.0);
            m.text(attr::PROPULSION_METHOD, "Propulsion Method");
            m.measure(attr::PROPULSION_POWER, "Engine Propulsion Power", "as published", 1.0);
        }
        Register::Small => {
            m.text(attr::VESSEL_TYPE, "Main Descriptor");
            m.text(attr::VESSEL_SUBTYPE, "Sub Descriptor 1");
            m.text(attr::VESSEL_SUBTYPE, "Sub Descriptor 2");
            m.text(attr::HULL_MATERIAL, "Hull Material");
            m.measure(attr::ALL_UP_WEIGHT, "All Up Weight", "kg", 1.0);
            m.measure(attr::GROSS_TONNAGE, "Vessel Gross Tonnage", "GT", 1.0);
            m.measure(attr::LENGTH_M, "Vessel Length", "m", 1.0);
            m.measure(attr::BEAM_M, "Vessel Breadth", "m", 1.0);
            m.measure(attr::DEPTH_M, "Vessel Depth", "m", 1.0);
            m.text(attr::PROPULSION_TYPE, "Propulsion Type");
            m.measure(attr::PROPULSION_POWER, "Power HP", "hp", 1.0);
        }
    }
    m.out
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::sources::midnight;
    use bytes::Bytes;
    use rust_xlsxwriter::Workbook;

    /// An XLSX laid out like Transport Canada's: a blank first row, the
    /// header in the second, data from the third, all from column B.
    pub fn workbook(header: &[&str], rows: &[Vec<&str>]) -> Bytes {
        let mut wb = Workbook::new();
        let ws = wb.add_worksheet();
        for (j, h) in header.iter().enumerate() {
            ws.write_string(1, j as u16 + 1, *h).unwrap();
        }
        for (i, r) in rows.iter().enumerate() {
            for (j, v) in r.iter().enumerate() {
                if v.is_empty() {
                    continue;
                }
                match v.parse::<f64>() {
                    Ok(n) => ws.write_number(i as u32 + 2, j as u16 + 1, n).unwrap(),
                    Err(_) => ws.write_string(i as u32 + 2, j as u16 + 1, *v).unwrap(),
                };
            }
        }
        Bytes::from(wb.save_to_buffer().unwrap())
    }

    pub const LARGE_HEADER: &[&str] = &[
        "Official Number", "Vessel Name", "IMO Vessel Number", "Hull Number", "Year of Build", "Year of Latest Rebuild",
        "Port of Registry", "Certificate Issuing date or Registration date", "Vessel Descriptor", "Gross Tonnage",
        "Net Tonnage", "Construction Type", "Construction Material", "Length", "Breadth", "Depth", "Engine Type",
        "Number of Engines", "Propulsion Type", "Speed (Knots)", "Propulsion Method", "Engine Propulsion Power",
    ];

    pub fn large_row<'a>(official: &'a str, name: &'a str, imo: &'a str, year: &'a str, length: &'a str) -> Vec<&'a str> {
        vec![
            official, name, imo, "", year, "", "HALIFAX", "2019-05-01", "BULK CARRIER", "24000", "11000", "", "STEEL",
            length, "32.2", "14", "DIESEL", "1", "", "14", "SINGLE SCREW", "9000",
        ]
    }

    fn cfg() -> SourceConfig {
        crate::config::Catalogue::load(None).unwrap().get("tc_large_vessels").unwrap().0.clone()
    }

    #[test]
    fn parses_the_large_register() {
        let bytes = workbook(
            LARGE_HEADER,
            &[
                large_row("92443", "SEGWUN", "", "188700", "36.82"),
                large_row("834567", "M.V. ATLANTIC STAR", "9321483", "2005", "199.9"),
                large_row("", "NO NUMBER", "", "", ""),
                large_row("92443", "SEGWUN", "", "188700", "36.9"),
            ],
        );
        let parsed = parse(Register::Large, &[RawObject::new("f.xlsx", bytes)]).unwrap();
        assert_eq!(parsed.records.len(), 3);
        assert_eq!(parsed.records[2].source_record_id, "92443#2", "a repeated official number is a second record");
        assert_eq!(parsed.records[2].subject_id, "92443");
        assert_eq!(parsed.rejected.len(), 1);
        assert!(parsed.warnings[0].contains("2019-05-01"));
        let r = &parsed.records[1];
        assert_eq!(r.source_record_id, "834567");
        assert_eq!(r.fields["Year of Build"], "2005");

        let drafts = map(Register::Large, &cfg(), &r.fields);
        let get = |a: &str| drafts.iter().find(|d| d.attribute == a).unwrap();
        assert_eq!(get(attr::IMO).value_normalized.as_deref(), Some("9321483"));
        assert_eq!(get(attr::IMO).check_status, Some("valid"));
        assert_eq!(get(attr::NAME).value_normalized.as_deref(), Some("ATLANTIC STAR"));
        assert_eq!(get(attr::NAME).value_original.as_deref(), Some("M.V. ATLANTIC STAR"));
        assert_eq!(get(attr::FLAG).value_normalized.as_deref(), Some("CAN"));
        let nr = get(attr::NATIONAL_REGISTRY);
        assert_eq!((nr.value_normalized.as_deref(), nr.qualifier.as_deref()), (Some("834567"), Some("CA")));
        assert_eq!(nr.valid_from, Some(midnight(chrono::NaiveDate::from_ymd_opt(2019, 5, 1).unwrap())));
        assert_eq!(get(attr::LENGTH_M).value_normalized.as_deref(), Some("199.9"));

        let old = map(Register::Large, &cfg(), &parsed.records[0].fields);
        assert_eq!(old.iter().find(|d| d.attribute == attr::YEAR_BUILT).unwrap().value_normalized.as_deref(), Some("1887"));
        assert!(old.iter().all(|d| d.attribute != attr::IMO), "no IMO number, no IMO assertion");
    }

    #[test]
    fn reads_excel_dates() {
        assert_eq!(excel_date(45292.0), chrono::NaiveDate::from_ymd_opt(2024, 1, 1));
        assert_eq!(cell_text(&Data::Float(188700.0)).as_deref(), Some("188700"));
    }

    #[test]
    fn refuses_a_changed_layout() {
        let bytes = workbook(&["Official Number", "Name"], &[vec!["1", "X"]]);
        let e = parse(Register::Large, &[RawObject::new("f.xlsx", bytes)]).unwrap_err();
        assert!(format!("{e:#}").contains("layout"), "{e:#}");
    }
}
