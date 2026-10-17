//! US Coast Guard inspection deficiencies, from the MISLE Deficiency Data
//! Report (<https://www.dco.uscg.mil/Our-Organization/Assistant-Commandant-for-Prevention-Policy-CG-5P/Inspections-Compliance-CG-5PC-/Commercial-Vessel-Compliance/MISLE-DEF-DATA-REPORT/>):
//! one Excel workbook per year from 2018, refreshed monthly, holding every
//! deficiency the Coast Guard closed, on foreign and US vessels. It is the
//! data PSIX shows per vessel, in bulk.
//!
//! Each MISLE activity (an inspection or exam) becomes one `vessel_events`
//! row, `port_state_control` for foreign-flagged vessels and
//! `flag_state_control` for US-flagged ones, typed by the most severe
//! required action among its deficiencies (ship detained, denied entry or
//! expelled). Each deficiency is a `vessel_event_findings` row. Activities
//! that found nothing aren't in the report.
//!
//! The workbooks have had three layouts (2018-2020, 2021-2024 with class
//! and certificate issuers, and from 2025 with deficiency ids and dates);
//! columns are matched by name.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use calamine::{open_workbook_auto, Data, Reader};
use chrono::NaiveDate;

use super::{date, imo, mmsi, text, Category, Event, EventType, Evidence, Finding, SourceInfo};

pub const SOURCE: &str = "uscg_misle";
pub const PAGE: &str = "https://www.dco.uscg.mil/Our-Organization/Assistant-Commandant-for-Prevention-Policy-CG-5P/Inspections-Compliance-CG-5PC-/Commercial-Vessel-Compliance/MISLE-DEF-DATA-REPORT/";
const BASE: &str = "https://www.dco.uscg.mil/Portals/9/DCO%20Documents/5p/CG-5PC/CG-CVC/Guidance/MISLEDefs/";

/// The workbooks listed on the report page as of October 2026.
pub const DEFAULT_FILES: [&str; 8] = [
    "U.S.C.G-DeficiencyData%202018%20.xlsx",
    "U.S.C.G-DeficiencyData%202019.xlsx",
    "U.S.C.G-DeficiencyData%202020.xlsx",
    "Website-DeficiencyData-20211231.xlsx",
    "Website-DeficiencyData-20221231.xlsx",
    "Website-DeficiencyData-20231231.xlsx",
    "Website-DeficiencyData-20241231.xlsx",
    "Website_Deficiency_Data_Nov_2025.xlsx",
];

pub const INFO: SourceInfo = SourceInfo {
    source: SOURCE,
    name: "USCG MISLE Deficiency Data Report",
    regime: "USCG",
    evidence_categories: "port_state_control,flag_state_control",
    publisher: "US Coast Guard Office of Commercial Vessel Compliance (CG-CVC)",
    url: PAGE,
    access: "Public yearly Excel workbooks, refreshed monthly (the site refuses non-browser downloads)",
    coverage: "Closed deficiencies on foreign and US vessels, 2018 onward; the same MISLE data PSIX publishes per vessel",
    notes: "Only activities with deficiencies appear. Excludes security, self-reported and worklist items.",
};

/// The download URL of a default file.
pub fn url(file: &str) -> String {
    format!("{BASE}{file}")
}

/// A file's name on disk: the URL name with `%20` decoded.
pub fn local_name(file: &str) -> String {
    file.replace("%20", " ")
}

/// Every `.xlsx` in `dir`, sorted.
pub fn files_in(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("listing {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()).is_some_and(|e| e.eq_ignore_ascii_case("xlsx")))
        .collect();
    out.sort();
    Ok(out)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Col {
    Date,
    Activity,
    Unit,
    Vin,
    VinType,
    Name,
    Class,
    VesselType,
    Flag,
    ClassDocument,
    Smc,
    Issc,
    System,
    SubSystem,
    Component,
    Action,
    Description,
    DeficiencyId,
    DateIssued,
    DueDate,
    ResolvedDate,
    VesselMisleId,
}

/// Header names (lowercase, letters and digits only) for each column.
fn column(header: &str) -> Option<Col> {
    let h: String = header.chars().filter(|c| c.is_ascii_alphanumeric()).collect::<String>().to_ascii_lowercase();
    Some(match h.as_str() {
        "inspectiondate" | "activitystartdt" => Col::Date,
        "activityid" => Col::Activity,
        "unit" | "issuingunit" => Col::Unit,
        "primaryvin" => Col::Vin,
        "primaryvintype" | "vintype" => Col::VinType,
        "vesselname" => Col::Name,
        "vesselclass" => Col::Class,
        "vesseltype" => Col::VesselType,
        "flag" => Col::Flag,
        "classdocument" => Col::ClassDocument,
        "smc" => Col::Smc,
        "issc" => Col::Issc,
        "system" => Col::System,
        "subsystem" => Col::SubSystem,
        "component" => Col::Component,
        "resolutionaction" | "action" => Col::Action,
        "description" => Col::Description,
        "deficiencyid" => Col::DeficiencyId,
        "dateissued" => Col::DateIssued,
        "duedate" => Col::DueDate,
        "resolveddate" => Col::ResolvedDate,
        "vesselmisleid" => Col::VesselMisleId,
        _ => return None,
    })
}

/// A cell as text; whole numbers without a decimal point.
pub(super) fn cell_text(d: &Data) -> Option<String> {
    match d {
        Data::String(s) | Data::DateTimeIso(s) | Data::DurationIso(s) => text(s),
        Data::Int(i) => Some(i.to_string()),
        Data::Float(f) if f.fract() == 0.0 && f.abs() < 1e15 => Some(format!("{}", *f as i64)),
        Data::Float(f) => Some(f.to_string()),
        Data::Bool(b) => Some(b.to_string()),
        Data::DateTime(dt) => Some(dt.as_f64().to_string()),
        Data::Error(_) | Data::Empty => None,
    }
}

/// An Excel serial day number (1900 system) as a date.
fn excel_date(serial: f64) -> Option<NaiveDate> {
    if !(1.0..2_958_466.0).contains(&serial) {
        return None;
    }
    NaiveDate::from_ymd_opt(1899, 12, 30)?.checked_add_days(chrono::Days::new(serial.floor() as u64))
}

fn cell_date(d: &Data) -> Option<NaiveDate> {
    match d {
        Data::DateTime(dt) => excel_date(dt.as_f64()),
        Data::Float(f) => excel_date(*f),
        Data::Int(i) => excel_date(*i as f64),
        Data::String(s) | Data::DateTimeIso(s) => date(s)
            .or_else(|| NaiveDate::parse_from_str(s.trim(), "%m/%d/%Y").ok()),
        _ => None,
    }
}

/// `"30 - Ship detained"` -> (`"30"`, the whole text).
fn coded(s: Option<String>) -> (Option<String>, Option<String>) {
    let code = s.as_deref().and_then(|s| s.split_once(" - ")).map(|(c, _)| c.trim().to_string());
    (code, s)
}

/// The event type a required action implies, if more than an inspection.
fn action_event(code: Option<&str>) -> EventType {
    match code {
        Some("20") => EventType::Expulsion,
        Some("25") => EventType::RefusalOfAccess,
        Some("30") => EventType::Detention,
        _ => EventType::Inspection,
    }
}

fn severity_rank(t: EventType) -> u8 {
    match t {
        EventType::Expulsion => 3,
        EventType::RefusalOfAccess => 2,
        EventType::Detention => 1,
        _ => 0,
    }
}

/// One workbook's rows as (column -> cell) maps.
pub struct Sheet {
    pub source_file: String,
    rows: Vec<HashMap<Col, Data>>,
}

pub fn read(path: &Path, source_file: &str) -> Result<Sheet> {
    let mut wb = open_workbook_auto(path).with_context(|| format!("opening {}", path.display()))?;
    let range = wb
        .worksheet_range_at(0)
        .with_context(|| format!("{source_file}: no worksheet"))?
        .with_context(|| format!("{source_file}: reading the first worksheet"))?;
    let mut rows = range.rows();
    let header: Vec<Option<Col>> = rows
        .next()
        .with_context(|| format!("{source_file}: empty worksheet"))?
        .iter()
        .map(|h| cell_text(h).and_then(|h| column(&h)))
        .collect();
    for need in [Col::Activity, Col::Date, Col::Vin, Col::Action] {
        anyhow::ensure!(header.contains(&Some(need)), "{source_file}: no {need:?} column");
    }
    let rows = rows
        .map(|r| {
            header
                .iter()
                .zip(r)
                .filter_map(|(c, d)| c.map(|c| (c, d.clone())))
                .collect()
        })
        .collect();
    Ok(Sheet { source_file: source_file.to_string(), rows })
}

#[derive(Default)]
pub struct Parsed {
    pub evidence: Evidence,
    /// Rows without an activity id.
    pub rejected: usize,
    /// Deficiencies that appeared more than once (same deficiency id, or the
    /// same row in two workbooks); only the first is kept.
    pub duplicates: usize,
}

/// Events and findings from every workbook, grouped by MISLE activity
/// across workbooks.
pub fn events(sheets: &[Sheet]) -> Parsed {
    let mut out = Parsed::default();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut rank: Vec<u8> = Vec::new();
    let mut seen: std::collections::HashSet<String> = Default::default();

    for sheet in sheets {
        for row in &sheet.rows {
            let get = |c: Col| row.get(&c).and_then(cell_text);
            let get_date = |c: Col| row.get(&c).and_then(cell_date);
            let Some(activity) = get(Col::Activity) else {
                out.rejected += 1;
                continue;
            };
            let deficiency_id = get(Col::DeficiencyId);
            let fingerprint = match &deficiency_id {
                Some(id) => format!("id:{id}"),
                None => format!(
                    "row:{activity}|{:?}|{:?}|{:?}",
                    get(Col::Component),
                    get(Col::Action),
                    get(Col::Description)
                ),
            };
            if !seen.insert(fingerprint) {
                out.duplicates += 1;
                continue;
            }

            let event_id = format!("{SOURCE}:{activity}");
            let event_date = get_date(Col::Date);
            let (action_code, action) = coded(get(Col::Action));
            let kind = action_event(action_code.as_deref());

            let i = *index.entry(activity.clone()).or_insert_with(|| {
                let flag = get(Col::Flag);
                // Fall back to MISLE's own vessel id when there's no VIN.
                let (vin, vin_type) = match (get(Col::Vin), get(Col::VesselMisleId)) {
                    (None, Some(id)) => (Some(id), Some("MISLE Vessel ID".to_string())),
                    (vin, _) => (vin, get(Col::VinType)),
                };
                let vt = vin_type.as_deref().unwrap_or("").to_ascii_lowercase();
                let category = if flag.as_deref().is_some_and(|f| f.eq_ignore_ascii_case("UNITED STATES")) {
                    Category::FlagStateControl
                } else {
                    Category::PortStateControl
                };
                let ship_type = match (get(Col::Class), get(Col::VesselType)) {
                    (Some(c), Some(t)) => Some(format!("{c} / {t}")),
                    (c, t) => c.or(t),
                };
                out.evidence.events.push(Event {
                    event_id: event_id.clone(),
                    source: SOURCE.into(),
                    evidence_category: category.as_str().into(),
                    event_type: EventType::Inspection.as_str().into(),
                    regime: INFO.regime.into(),
                    authority: get(Col::Unit),
                    source_record_id: activity.clone(),
                    event_date,
                    place_country: Some("US".into()),
                    imo_number: if vt.contains("imo") { vin.as_deref().and_then(imo) } else { None },
                    mmsi: if vt.contains("mmsi") { vin.as_deref().and_then(mmsi) } else { None },
                    vessel_name: get(Col::Name),
                    vessel_id: vin,
                    vessel_id_type: vin_type,
                    flag,
                    ship_type,
                    classification_society: get(Col::ClassDocument),
                    detained: Some(false),
                    deficiency_count: Some(0),
                    detainable_deficiency_count: Some(0),
                    summary: match (get(Col::Smc), get(Col::Issc)) {
                        (None, None) => None,
                        (smc, issc) => Some(format!(
                            "SMC issuer: {}; ISSC issuer: {}",
                            smc.as_deref().unwrap_or("-"),
                            issc.as_deref().unwrap_or("-")
                        )),
                    },
                    source_file: sheet.source_file.clone(),
                    ..Default::default()
                });
                rank.push(0);
                out.evidence.events.len() - 1
            });

            let ev = &mut out.evidence.events[i];
            let seq = ev.deficiency_count.unwrap_or(0) + 1;
            ev.deficiency_count = Some(seq);
            if kind == EventType::Detention {
                ev.detained = Some(true);
                ev.detainable_deficiency_count = Some(ev.detainable_deficiency_count.unwrap_or(0) + 1);
            }
            if severity_rank(kind) > rank[i] {
                rank[i] = severity_rank(kind);
                ev.event_type = kind.as_str().into();
            }
            if let (Some(d), Some(cur)) = (event_date, ev.event_date) {
                ev.event_date = Some(cur.min(d));
            } else if ev.event_date.is_none() {
                ev.event_date = event_date;
            }

            let component = get(Col::Component);
            let (finding_code, finding_item) = match component.as_deref().and_then(|c| c.split_once(" - ")) {
                Some((code, item)) => (text(code), text(item)),
                None => (None, component.clone()),
            };
            out.evidence.findings.push(Finding {
                event_id,
                source: SOURCE.into(),
                finding_seq: seq,
                finding_id: deficiency_id,
                finding_date: get_date(Col::DateIssued).or(event_date),
                finding_code,
                finding_area: get(Col::System),
                finding_subarea: get(Col::SubSystem),
                finding_item,
                ground_for_detention: Some(action_code.as_deref() == Some("30")),
                action_code,
                action,
                due_date: get_date(Col::DueDate),
                resolved_date: get_date(Col::ResolvedDate),
                description: get(Col::Description),
                source_file: sheet.source_file.clone(),
                ..Default::default()
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Data {
        Data::String(v.into())
    }

    fn sheet(name: &str, rows: Vec<Vec<(Col, Data)>>) -> Sheet {
        Sheet { source_file: name.into(), rows: rows.into_iter().map(|r| r.into_iter().collect()).collect() }
    }

    /// Writes a workbook the way the Coast Guard's are laid out: a header
    /// row, dates as date-formatted serial numbers, ids as numbers.
    fn workbook(dir: &Path, name: &str, header: &[&str], rows: &[Vec<Cell>]) -> PathBuf {
        use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};
        let mut wb = Workbook::new();
        let ws = wb.add_worksheet().set_name("DeficiencyData").unwrap();
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
                    Cell::D(y, m, d) => ws
                        .write_datetime_with_format(r, c, ExcelDateTime::from_ymd(*y, *m, *d).unwrap(), &date_fmt)
                        .map(|_| ())
                        .unwrap(),
                    Cell::E => {}
                }
            }
        }
        let path = dir.join(name);
        wb.save(&path).unwrap();
        path
    }

    enum Cell {
        S(&'static str),
        N(f64),
        D(u16, u8, u8),
        E,
    }

    #[test]
    fn reads_every_workbook_layout() {
        use Cell::*;
        let dir = tempfile::tempdir().unwrap();
        // 2018-2020: "Activity ID", no issuer columns.
        let y2018 = workbook(
            dir.path(),
            "U.S.C.G-DeficiencyData 2018 .xlsx",
            &["Inspection Date", "Activity ID", "Unit", "Primary VIN", "Primary VIN Type", "Vessel Name", "Vessel Class", "Flag", "System", "Component", "Resolution Action", "Description"],
            &[vec![D(2018, 1, 2), N(6_300_001.0), S("Sector Houston-Galveston"), S("9100001"), S("IMO Number"), S("OLD SHIP"), S("Bulk Carrier"), S("PANAMA"), S("07 - Fire Safety"), S("07109 - Fixed fire extinguishing installation"), S("30 - Ship detained"), S("CO2 system inoperable")]],
        );
        // 2021-2024: "ActivityId", plus class and certificate issuers.
        let y2024 = workbook(
            dir.path(),
            "Website-DeficiencyData-20241231.xlsx",
            &["Inspection Date", "ActivityId", "Unit", "Primary VIN", "Primary VIN Type", "Vessel Name", "Vessel Class", "Flag", "Class Document", "SMC", "ISSC", "System", "Component", "Resolution Action", "Description"],
            &[vec![D(2024, 1, 1), N(7_844_019.0), S("Sector New York"), S("9838668"), S("IMO Number"), S("TEAL RAY"), S("Tank Ship"), S("LIBERIA"), E, S("NIPPON KAIJI KYOKAI"), S("NIPPON KAIJI KYOKAI"), S("02 - Structural Conditions"), S("02103 - Stability/strength/loading information and instruments"), S("17 - Rectify deficiencies prior to departure"), S("Broken stiffeners")]],
        );
        // 2025 on: underscored names, deficiency ids and dates.
        let y2025 = workbook(
            dir.path(),
            "Website_Deficiency_Data_Nov_2025.xlsx",
            &["ActivityID", "ActivityStartDt", "Vessel_MISLE_ID", "VesselName", "Primary_VIN", "VIN_Type", "Vessel_Class", "Vessel_Type", "Flag", "DeficiencyID", "DateIssued", "Issuing_Unit", "System", "SubSystem", "Component", "Action", "Description", "DueDate", "ResolvedDate"],
            &[vec![N(8_104_002.0), D(2025, 4, 3), N(37.0), S("FRANK SINATRA"), S("1091257"), S("Official Number (U.S.)"), S("Passenger Ship"), S("Ferry"), S("UNITED STATES"), N(2_859_016.0), D(2025, 4, 3), S("Sector New York"), S("03 - Water/Weathertight Conditions"), S("N/A - No Subsystem"), S("03110 - Manholes/flush scuttles"), S("705 - Other - as specified"), S("Cracked hatch"), E, D(2025, 7, 14)]],
        );

        let files = files_in(dir.path()).unwrap();
        assert_eq!(files.len(), 3);
        let sheets: Vec<Sheet> = [y2018, y2024, y2025]
            .iter()
            .map(|p| read(p, p.file_name().unwrap().to_str().unwrap()).unwrap())
            .collect();
        let p = events(&sheets);
        assert_eq!((p.rejected, p.duplicates), (0, 0));
        let ev = &p.evidence.events;
        assert_eq!(ev.len(), 3);

        assert_eq!(ev[0].event_id, "uscg_misle:6300001");
        assert_eq!(ev[0].event_type, "detention");
        assert_eq!(ev[0].event_date, NaiveDate::from_ymd_opt(2018, 1, 2));
        assert_eq!(ev[0].imo_number, Some(9_100_001));

        assert_eq!(ev[1].event_id, "uscg_misle:7844019");
        assert_eq!(ev[1].event_date, NaiveDate::from_ymd_opt(2024, 1, 1));
        assert_eq!(ev[1].summary.as_deref(), Some("SMC issuer: NIPPON KAIJI KYOKAI; ISSC issuer: NIPPON KAIJI KYOKAI"));
        assert_eq!(ev[1].classification_society, None);

        assert_eq!(ev[2].event_id, "uscg_misle:8104002");
        assert_eq!(ev[2].evidence_category, "flag_state_control");
        assert_eq!(ev[2].authority.as_deref(), Some("Sector New York"));
        assert_eq!(ev[2].event_date, NaiveDate::from_ymd_opt(2025, 4, 3));

        let f = &p.evidence.findings;
        assert_eq!(f[2].finding_id.as_deref(), Some("2859016"));
        assert_eq!(f[2].finding_code.as_deref(), Some("03110"));
        assert_eq!(f[2].finding_subarea.as_deref(), Some("N/A - No Subsystem"));
        assert_eq!(f[2].resolved_date, NaiveDate::from_ymd_opt(2025, 7, 14));
        assert_eq!(f[2].due_date, None);
    }

    #[test]
    fn maps_headers_of_every_layout() {
        assert_eq!(column("Inspection Date"), Some(Col::Date));
        assert_eq!(column("ActivityStartDt"), Some(Col::Date));
        assert_eq!(column("Activity ID"), Some(Col::Activity));
        assert_eq!(column("ActivityId"), Some(Col::Activity));
        assert_eq!(column("Primary_VIN"), Some(Col::Vin));
        assert_eq!(column("VIN_Type"), Some(Col::VinType));
        assert_eq!(column("Resolution Action"), Some(Col::Action));
        assert_eq!(column("Issuing_Unit"), Some(Col::Unit));
        assert_eq!(column("Something"), None);
    }

    #[test]
    fn converts_excel_dates() {
        assert_eq!(excel_date(45292.0), NaiveDate::from_ymd_opt(2024, 1, 1));
        assert_eq!(excel_date(45750.0), NaiveDate::from_ymd_opt(2025, 4, 3));
        assert_eq!(cell_date(&s("04/03/2025")), NaiveDate::from_ymd_opt(2025, 4, 3));
        assert_eq!(cell_text(&Data::Float(7844019.0)).as_deref(), Some("7844019"));
    }

    #[test]
    fn groups_deficiencies_into_activities() {
        let old = sheet(
            "2024.xlsx",
            vec![
                vec![
                    (Col::Date, Data::Float(45292.0)),
                    (Col::Activity, Data::Float(7844019.0)),
                    (Col::Unit, s("Sector New York")),
                    (Col::Vin, s("9838668")),
                    (Col::VinType, s("IMO Number")),
                    (Col::Name, s("TEAL RAY")),
                    (Col::Class, s("Tank Ship")),
                    (Col::Flag, s("LIBERIA")),
                    (Col::Smc, s("NIPPON KAIJI KYOKAI")),
                    (Col::System, s("02 - Structural Conditions")),
                    (Col::Component, s("02103 - Stability/strength/loading information and instruments")),
                    (Col::Action, s("17 - Rectify deficiencies prior to departure")),
                    (Col::Description, s("Broken stiffeners")),
                ],
                vec![
                    (Col::Date, Data::Float(45293.0)),
                    (Col::Activity, Data::Float(7844019.0)),
                    (Col::Vin, s("9838668")),
                    (Col::VinType, s("IMO Number")),
                    (Col::Flag, s("LIBERIA")),
                    (Col::Component, s("07109 - Fixed fire extinguishing installation")),
                    (Col::Action, s("30 - Ship detained")),
                ],
                vec![(Col::Vin, s("1"))],
            ],
        );
        let new = sheet(
            "2025.xlsx",
            vec![
                vec![
                    (Col::Activity, Data::Int(8104002)),
                    (Col::Date, Data::Float(45750.0)),
                    (Col::Vin, s("1091257")),
                    (Col::VinType, s("Official Number (U.S.)")),
                    (Col::Name, s("FRANK SINATRA")),
                    (Col::Class, s("Passenger Ship")),
                    (Col::VesselType, s("Ferry")),
                    (Col::Flag, s("UNITED STATES")),
                    (Col::DeficiencyId, Data::Int(2859016)),
                    (Col::DateIssued, Data::Float(45751.0)),
                    (Col::SubSystem, s("N/A - No Subsystem")),
                    (Col::Action, s("705 - Other - as specified")),
                    (Col::ResolvedDate, Data::Float(45852.0)),
                ],
                // The same deficiency again, as in an overlapping workbook.
                vec![
                    (Col::Activity, Data::Int(8104002)),
                    (Col::DeficiencyId, Data::Int(2859016)),
                    (Col::Action, s("705 - Other - as specified")),
                ],
            ],
        );
        let p = events(&[old, new]);
        assert_eq!((p.rejected, p.duplicates), (1, 1));
        let ev = &p.evidence.events;
        assert_eq!(ev.len(), 2);

        let a = &ev[0];
        assert_eq!(a.event_id, "uscg_misle:7844019");
        assert_eq!((a.evidence_category.as_str(), a.event_type.as_str()), ("port_state_control", "detention"));
        assert_eq!(a.imo_number, Some(9_838_668));
        assert_eq!(a.event_date, NaiveDate::from_ymd_opt(2024, 1, 1));
        assert_eq!((a.deficiency_count, a.detainable_deficiency_count, a.detained), (Some(2), Some(1), Some(true)));
        assert_eq!(a.authority.as_deref(), Some("Sector New York"));

        let b = &ev[1];
        assert_eq!((b.evidence_category.as_str(), b.event_type.as_str()), ("flag_state_control", "inspection"));
        assert_eq!(b.imo_number, None);
        assert_eq!(b.vessel_id_type.as_deref(), Some("Official Number (U.S.)"));
        assert_eq!(b.ship_type.as_deref(), Some("Passenger Ship / Ferry"));

        let f = &p.evidence.findings;
        assert_eq!(f.len(), 3);
        assert_eq!(f[0].finding_code.as_deref(), Some("02103"));
        assert_eq!(f[0].finding_area.as_deref(), Some("02 - Structural Conditions"));
        assert_eq!(f[1].action_code.as_deref(), Some("30"));
        assert_eq!(f[1].ground_for_detention, Some(true));
        assert_eq!(f[1].finding_seq, 2);
        assert_eq!(f[2].finding_id.as_deref(), Some("2859016"));
        assert_eq!(f[2].finding_date, NaiveDate::from_ymd_opt(2025, 4, 4));
        assert_eq!(f[2].resolved_date, NaiveDate::from_ymd_opt(2025, 7, 14));
        assert_eq!(f[2].action_code.as_deref(), Some("705"));
    }
}
