//! IUU fishing vessel listings, from the Combined IUU Vessel List that Trygg
//! Mat Tracking (TMT) keeps (<https://iuu-vessels.org/>): every vessel a
//! regional fisheries management organisation (RFMO) has put on its IUU
//! vessel list, current and delisted, with TMT's research on the vessel's
//! current identity.
//!
//! Each listing of a vessel by one RFMO becomes an `iuu_listing` event in
//! the `enforcement` category, dated when the RFMO listed it, with the
//! listing reason in `summary` and in `event_subtype` whether the RFMO
//! listed the vessel on its own findings or cross-listed it from another
//! RFMO's list. A listing the RFMO has since lifted also gets an
//! `iuu_delisting` event, dated when it was lifted, and closes the listing
//! (`event_end_date`); the two share a `case_id`. Delisted vessels stay in
//! the list, so the table holds the full history.
//!
//! The list is only downloadable as an Excel file after a form asking who
//! wants it and why, so this reads files saved by hand. The `IUUList` sheet
//! has one row per vessel; each RFMO has a column (`IOTC`, `ICCAT`, ...)
//! holding `<listed> - <delisted>`, each a year, month or day, followed by a
//! `Reason` column. The file gives no reason for delistings.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use calamine::{open_workbook_auto, Data, Reader};
use chrono::NaiveDate;

use super::uscg::cell_text;
use super::{date, double, imo, mmsi, text, year, Category, Event, EventType, Evidence, SourceInfo};

pub const SOURCE: &str = "iuu_list";
pub const PAGE: &str = "https://iuu-vessels.org/Home/Download";

pub const INFO: SourceInfo = SourceInfo {
    source: SOURCE,
    name: "Combined IUU Vessel List",
    regime: "RFMO IUU vessel lists",
    evidence_categories: "enforcement",
    publisher: "Trygg Mat Tracking (TMT)",
    url: PAGE,
    access: "Free Excel download after a form asking for name, organisation and intended use; saved by hand",
    coverage: "Vessels on the IUU lists of CCAMLR, CCSBT, GFCM, IATTC, ICCAT, IOTC, NAFO, NEAFC, NPFC, SEAFO, SIOFA, SPRFMO and WCPFC, current and delisted, 2002 onward",
    notes: "One iuu_listing event per vessel per RFMO listing, plus an iuu_delisting event when it was lifted (no delisting reason given). regime is the listing RFMO. Dates given only as a year or month are stored as its first day, noted in summary. Vessel identity is TMT's current research, which can be newer than the RFMO's own list. No vessel id in the file: event_id uses the IMO number, else name and call sign.",
};

/// The RFMOs that keep IUU vessel lists, as the file names their columns.
pub const RFMOS: [&str; 13] = [
    "CCAMLR", "CCSBT", "GFCM", "IATTC", "ICCAT", "IOTC", "NAFO", "NEAFC", "NPFC", "SEAFO", "SIOFA", "SPRFMO", "WCPFC",
];

/// Every `.xls` or `.xlsx` in `dir`, sorted by name.
pub fn files_in(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("listing {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| e.eq_ignore_ascii_case("xlsx") || e.eq_ignore_ascii_case("xls"))
        })
        .filter(|p| !p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with("~$")))
        .collect();
    out.sort();
    Ok(out)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Col {
    CurrentlyListed,
    Name,
    RfmoName,
    Imo,
    RegNo,
    VesselType,
    GearType,
    Flag,
    Ircs,
    Mmsi,
    GrossTonnage,
    YearBuilt,
    Owner,
    Operator,
}

/// Header names (lowercase, letters and digits only) for each vessel column.
fn column(header: &str) -> Option<Col> {
    Some(match norm(header).as_str() {
        "currentlylisted" => Col::CurrentlyListed,
        "name" => Col::Name,
        "rfmoname" => Col::RfmoName,
        "imo" => Col::Imo,
        "nnr" => Col::RegNo,
        "vesseltype" => Col::VesselType,
        "geartype" => Col::GearType,
        "flag" => Col::Flag,
        "ircs" => Col::Ircs,
        "mmsi" => Col::Mmsi,
        "gt" => Col::GrossTonnage,
        "yearofbuild" => Col::YearBuilt,
        "ownername" => Col::Owner,
        "operatorname" => Col::Operator,
        _ => return None,
    })
}

/// Lowercase letters and digits only.
fn norm(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_alphanumeric()).collect::<String>().to_ascii_lowercase()
}

/// One RFMO's listing of a vessel, as published.
#[derive(Clone, Debug, Default, PartialEq)]
struct Listing {
    rfmo: &'static str,
    /// `<listed> - <delisted>`.
    period: String,
    reason: Option<String>,
}

/// One row of the list: a vessel and its listings.
#[derive(Clone, Debug, Default)]
pub struct Vessel {
    fields: HashMap<Col, String>,
    listings: Vec<Listing>,
    source_file: String,
}

impl Vessel {
    fn get(&self, c: Col) -> Option<String> {
        self.fields.get(&c).cloned()
    }

    /// The first value of a field that may list several (`735.000, 499.000`).
    fn first(&self, c: Col) -> Option<String> {
        self.fields.get(&c).and_then(|s| s.split([',', ';', '\n']).find_map(text))
    }

    fn imo(&self) -> Option<i32> {
        self.fields.get(&Col::Imo).and_then(|s| s.split([',', ';', '/', '\n']).find_map(imo))
    }

    /// An id for the vessel within the list, which has none of its own: the
    /// IMO number, else name and call sign. Not always unique: see `events`.
    fn key(&self) -> Option<String> {
        if let Some(n) = self.imo() {
            return Some(format!("imo{n}"));
        }
        let mut key = norm(&self.get(Col::Name).or_else(|| self.get(Col::RfmoName))?);
        if let Some(cs) = self.first(Col::Ircs) {
            key.push('-');
            key.push_str(&norm(&cs));
        }
        (!key.is_empty()).then_some(key)
    }
}

/// Every vessel in a workbook, from the sheet whose first row names vessel
/// and RFMO columns (`IUUList`; the other sheet is a disclaimer).
pub fn read(path: &Path, source_file: &str) -> Result<Vec<Vessel>> {
    let mut wb = open_workbook_auto(path).with_context(|| format!("opening {}", path.display()))?;
    for name in wb.sheet_names() {
        let range = wb.worksheet_range(&name).with_context(|| format!("{source_file}: reading sheet {name}"))?;
        let rows: Vec<&[Data]> = range.rows().collect();
        if let Some(v) = read_sheet(&rows, source_file)? {
            return Ok(v);
        }
    }
    anyhow::bail!("{source_file}: no sheet with Name and RFMO columns ({})", RFMOS.join(", "))
}

/// What a header row's column holds.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Slot {
    Vessel(Col),
    Period(&'static str),
    Reason(&'static str),
}

/// The vessels on a sheet, or `None` if its first row isn't the list's
/// header.
fn read_sheet(rows: &[&[Data]], source_file: &str) -> Result<Option<Vec<Vessel>>> {
    let Some(header) = rows.first() else { return Ok(None) };
    let mut slots: Vec<Option<Slot>> = Vec::with_capacity(header.len());
    let mut last_rfmo = None;
    for h in header.iter() {
        let h = cell_text(h).unwrap_or_default();
        let slot = if let Some(r) = RFMOS.iter().find(|r| r.eq_ignore_ascii_case(h.trim())) {
            last_rfmo = Some(*r);
            Some(Slot::Period(r))
        } else if norm(&h) == "reason" {
            // Each `Reason` column follows its RFMO's column.
            last_rfmo.take().map(Slot::Reason)
        } else {
            last_rfmo = None;
            column(&h).map(Slot::Vessel)
        };
        slots.push(slot);
    }
    let has = |s: Slot| slots.contains(&Some(s));
    if !has(Slot::Vessel(Col::Name)) || !slots.iter().any(|s| matches!(s, Some(Slot::Period(_)))) {
        return Ok(None);
    }
    for r in RFMOS {
        anyhow::ensure!(has(Slot::Period(r)), "{source_file}: no {r} column; the list's layout has changed");
    }

    let mut out = Vec::new();
    for row in &rows[1..] {
        let mut v = Vessel { source_file: source_file.to_string(), ..Default::default() };
        for (slot, cell) in slots.iter().zip(row.iter()) {
            let (Some(slot), Some(value)) = (slot, cell_text(cell)) else { continue };
            match *slot {
                Slot::Vessel(c) => {
                    v.fields.insert(c, value);
                }
                Slot::Period(r) => v.listings.push(Listing { rfmo: r, period: value, reason: None }),
                Slot::Reason(r) => {
                    if let Some(l) = v.listings.iter_mut().find(|l| l.rfmo == r) {
                        l.reason = Some(value);
                    }
                }
            }
        }
        if v.fields.contains_key(&Col::Name) || v.fields.contains_key(&Col::Imo) || !v.listings.is_empty() {
            out.push(v);
        }
    }
    Ok(Some(out))
}

/// A listing or delisting date: `2023-10-11`, or `2019-12` / `2008` taken
/// as the first day, with the text as published to say so.
fn period_date(s: &str) -> Option<(NaiveDate, Option<String>)> {
    let s = s.trim();
    if let Some(d) = date(s).filter(|_| s.len() == 10) {
        return Some((d, None));
    }
    if s.len() == 7 {
        return NaiveDate::parse_from_str(&format!("{s}-01"), "%Y-%m-%d").ok().map(|d| (d, Some(s.to_string())));
    }
    let y = year(s).filter(|_| s.len() == 4)?;
    NaiveDate::from_ymd_opt(y, 1, 1).map(|d| (d, Some(s.to_string())))
}

/// `"2007-10-31 - 2009-11-13"` -> (listed, delisted). Either half may be
/// empty or a bare year or month.
fn split_period(s: &str) -> (&str, &str) {
    match s.split_once(" - ") {
        Some((a, b)) => (a.trim(), b.trim()),
        None => (s.trim().trim_end_matches('-').trim(), ""),
    }
}

#[derive(Default)]
pub struct Parsed {
    pub evidence: Evidence,
    /// Distinct vessels with at least one listing.
    pub vessels: usize,
    /// Rows without any listing, or without a name or IMO number (skipped).
    pub skipped: usize,
    /// Listings already read from another row or file (skipped).
    pub duplicates: usize,
    /// Listing or delisting dates given only as a year or month.
    pub partial_dates: usize,
    /// Periods neither half of which is a date.
    pub undated: usize,
    /// Rows the list marks not currently listed though a listing on them is
    /// open, or the reverse.
    pub status_mismatches: usize,
}

/// `summary` text for a date given only as a year or month.
fn partial_note(what: &str, partial: &Option<String>) -> Option<String> {
    partial.as_ref().map(|p| format!("{what} {p}; day not given"))
}

fn join(parts: impl IntoIterator<Item = Option<String>>) -> Option<String> {
    let parts: Vec<String> = parts.into_iter().flatten().collect();
    (!parts.is_empty()).then(|| parts.join(". "))
}

/// Events for every listing of every vessel. With several files (snapshots
/// of the list), pass the newest first: a listing that appears in more than
/// one keeps the newest file's details.
pub fn events(vessels: &[Vessel]) -> Parsed {
    let mut out = Parsed::default();
    let mut seen: HashSet<String> = HashSet::new();
    let mut vessel_keys: HashSet<String> = HashSet::new();
    let mut rows_per_key: HashMap<(&str, String), usize> = HashMap::new();
    for v in vessels {
        let Some(mut key) = v.key().filter(|_| !v.listings.is_empty()) else {
            out.skipped += 1;
            continue;
        };
        // Different vessels can share a name and have no IMO number or call
        // sign (two LU RONG YUAN YU 108 rows); number repeats within a file.
        let n = rows_per_key.entry((v.source_file.as_str(), key.clone())).or_default();
        *n += 1;
        if *n > 1 {
            key = format!("{key}~{n}");
        }
        if vessel_keys.insert(key.clone()) {
            out.vessels += 1;
        }

        let ship_type = match (v.get(Col::VesselType), v.get(Col::GearType)) {
            (Some(t), Some(g)) => Some(format!("{t} (gear: {g})")),
            (t, None) => t,
            (None, Some(g)) => Some(format!("gear: {g}")),
        };
        let base = Event {
            source: SOURCE.into(),
            evidence_category: Category::Enforcement.as_str().into(),
            imo_number: v.imo(),
            mmsi: v.first(Col::Mmsi).as_deref().and_then(mmsi),
            call_sign: v.first(Col::Ircs),
            vessel_name: v.get(Col::Name).or_else(|| v.get(Col::RfmoName)),
            vessel_id: v.first(Col::RegNo),
            vessel_id_type: v.get(Col::RegNo).map(|_| "National registration number".into()),
            flag: v.get(Col::Flag),
            ship_type,
            gross_tonnage: v.first(Col::GrossTonnage).as_deref().and_then(double),
            year_built: v.first(Col::YearBuilt).as_deref().and_then(year),
            company_name: v.get(Col::Owner).or_else(|| v.get(Col::Operator)),
            source_file: v.source_file.clone(),
            ..Default::default()
        };

        let mut any_open = false;
        for (n, l) in v.listings.iter().enumerate() {
            let (listed_s, delisted_s) = split_period(&l.period);
            let listed = period_date(listed_s);
            let delisted = period_date(delisted_s);
            if listed.is_none() && delisted.is_none() {
                out.undated += 1;
            }
            let lifted = !delisted_s.is_empty();
            any_open |= !lifted;
            out.partial_dates += [&listed, &delisted].iter().filter(|d| d.as_ref().is_some_and(|(_, p)| p.is_some())).count();

            let when = listed.as_ref().map(|(d, _)| d.to_string()).unwrap_or_else(|| format!("{}", n + 1));
            let case = format!("{key}:{}:{when}", l.rfmo);
            if !seen.insert(case.clone()) {
                out.duplicates += 1;
                continue;
            }
            let cross = l.reason.as_deref().is_some_and(|r| {
                let r = norm(r);
                r.starts_with("crosslist") || r.starts_with("following")
            });
            let listed_note = listed.as_ref().and_then(|(_, p)| partial_note("Listed", p));
            let delisted_note = delisted.as_ref().and_then(|(_, p)| partial_note("Delisted", p));
            out.evidence.events.push(Event {
                event_id: format!("{SOURCE}:{case}:listing"),
                event_type: EventType::IuuListing.as_str().into(),
                event_subtype: Some(if cross { "cross_listing" } else { "rfmo_listing" }.into()),
                regime: l.rfmo.into(),
                authority: Some(l.rfmo.into()),
                source_record_id: format!("{key}:{}", l.rfmo),
                case_id: Some(case.clone()),
                event_date: listed.as_ref().map(|(d, _)| *d),
                event_end_date: delisted.as_ref().map(|(d, _)| *d),
                summary: join([l.reason.clone(), listed_note]),
                ..base.clone()
            });
            if lifted {
                out.evidence.events.push(Event {
                    event_id: format!("{SOURCE}:{case}:delisting"),
                    event_type: EventType::IuuDelisting.as_str().into(),
                    regime: l.rfmo.into(),
                    authority: Some(l.rfmo.into()),
                    source_record_id: format!("{key}:{}", l.rfmo),
                    case_id: Some(case),
                    event_date: delisted.as_ref().map(|(d, _)| *d),
                    summary: delisted_note,
                    ..base.clone()
                });
            }
        }
        let currently = v.get(Col::CurrentlyListed).map(|s| s.eq_ignore_ascii_case("true"));
        if currently.is_some_and(|c| c != any_open) {
            out.status_mismatches += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: [&str; 45] = [
        "CurrentlyListed", "Name", "RFMOName", "IMO", "NNR", "VesselType", "GearType", "Flag", "IRCS", "MMSI", "GT", "DWT",
        "Length", "YearOfBuild", "Depth", "BuiltIn", "OwnerName", "OperatorName", "VesselStatus", "IOTC", "Reason",
        "ICCAT", "Reason", "IATTC", "Reason", "CCAMLR", "Reason", "WCPFC", "Reason", "SEAFO", "Reason", "NEAFC",
        "Reason", "NAFO", "Reason", "SPRFMO", "Reason", "CCSBT", "Reason", "GFCM", "Reason", "NPFC", "Reason", "SIOFA",
        "Reason",
    ];

    /// A row with the vessel columns given and `(RFMO, period, reason)`
    /// listings; every cell is text, as in TMT's export.
    fn row(vessel: &[(&str, &str)], listings: &[(&str, &str, &str)]) -> Vec<String> {
        HEADER
            .iter()
            .enumerate()
            .map(|(i, h)| {
                if let Some((_, v)) = vessel.iter().find(|(k, _)| k == h) {
                    return v.to_string();
                }
                if *h == "Reason" {
                    return listings.iter().find(|(r, _, _)| *r == HEADER[i - 1]).map(|l| l.2.to_string()).unwrap_or_default();
                }
                listings.iter().find(|(r, _, _)| r == h).map(|l| l.1.to_string()).unwrap_or_default()
            })
            .collect()
    }

    /// Writes the list the way TMT's export lays it out: a disclaimer sheet,
    /// then `IUUList` with a header row and every value as text.
    fn workbook(dir: &Path, name: &str, rows: &[Vec<String>]) -> PathBuf {
        use rust_xlsxwriter::Workbook;
        let mut wb = Workbook::new();
        let d = wb.add_worksheet().set_name("Disclaimer").unwrap();
        d.write_string(2, 1, "Date: 2026-10-17").unwrap();
        let ws = wb.add_worksheet().set_name("IUUList").unwrap();
        for (c, h) in HEADER.iter().enumerate() {
            ws.write_string(0, c as u16, *h).unwrap();
        }
        for (r, row) in rows.iter().enumerate() {
            for (c, v) in row.iter().enumerate() {
                if !v.is_empty() {
                    ws.write_string(r as u32 + 1, c as u16, v).unwrap();
                }
            }
        }
        let path = dir.join(name);
        wb.save(&path).unwrap();
        path
    }

    /// Rows modelled on the 17 October 2026 list: ABDI BABA 1 (listed by
    /// ICCAT, cross-listed with month and year-only dates), ALFA (delisted
    /// everywhere, a total loss) and a vessel known only by name and call
    /// sign.
    fn fixture() -> Vec<Vec<String>> {
        vec![
            row(
                &[("CurrentlyListed", "True"), ("Name", "ABDI BABA 1"), ("RFMOName", "Sharon 1"), ("IMO", "8692299"), ("VesselType", "Fishing Vessel"), ("GearType", "Purse seine"), ("Flag", "Unknown"), ("GT", "143.000"), ("YearOfBuild", "197401"), ("OwnerName", "Rptd Sold Undisclosed Interest"), ("VesselStatus", "In Service/Commission")],
                &[("IOTC", "2019-12 - ", "Cross-listing"), ("ICCAT", "2008-11-24 - ", "Use of fraudulent documentation"), ("NEAFC", "2008 - ", "Cross-listing")],
            ),
            row(
                &[("CurrentlyListed", "False"), ("Name", "ALFA"), ("RFMOName", "Alfa"), ("IMO", "7436533"), ("NNR", "C-00951"), ("VesselType", "Reefer"), ("Flag", "Georgia"), ("IRCS", "4LPN"), ("GT", "669.000"), ("OwnerName", "Stratford Vale Corp"), ("OperatorName", "JSC Fishery Group"), ("VesselStatus", "Total Loss")],
                &[("NEAFC", "2007-10-31 - 2009-11-13", ""), ("NAFO", "2007-06-12 - 2009-11-13", "OBS by NOR TRA in RA SCH 07/37"), ("SPRFMO", "2008-01-05 - 2009", "")],
            ),
            row(
                &[("CurrentlyListed", "True"), ("Name", "ABISHAK PUTHA 3"), ("IRCS", "4SFXXXX"), ("MMSI", "417000878")],
                &[("SIOFA", "2020-08-14 - ", "Engaged in fishing for fishery resources in the Agreement Area and  are not on the SIOFA Record of Authorized Vessels")],
            ),
            row(&[("CurrentlyListed", "True"), ("Name", "NO LISTINGS")], &[]),
        ]
    }

    #[test]
    fn reads_the_published_layout() {
        let dir = tempfile::tempdir().unwrap();
        let path = workbook(dir.path(), "IUUList-20261017.xlsx", &fixture());
        assert_eq!(files_in(dir.path()).unwrap(), vec![path.clone()]);
        let vessels = read(&path, "IUUList-20261017.xlsx").unwrap();
        assert_eq!(vessels.len(), 4);
        let p = events(&vessels);
        assert_eq!((p.vessels, p.skipped, p.duplicates, p.undated, p.status_mismatches), (3, 1, 0, 0, 0));
        assert_eq!(p.partial_dates, 3);
        let ev = &p.evidence.events;
        let got: Vec<(&str, &str, Option<NaiveDate>)> =
            ev.iter().map(|e| (e.regime.as_str(), e.event_type.as_str(), e.event_date)).collect();
        let d = |y, m, d| NaiveDate::from_ymd_opt(y, m, d);
        assert_eq!(
            got,
            [
                ("IOTC", "iuu_listing", d(2019, 12, 1)),
                ("ICCAT", "iuu_listing", d(2008, 11, 24)),
                ("NEAFC", "iuu_listing", d(2008, 1, 1)),
                ("NEAFC", "iuu_listing", d(2007, 10, 31)),
                ("NEAFC", "iuu_delisting", d(2009, 11, 13)),
                ("NAFO", "iuu_listing", d(2007, 6, 12)),
                ("NAFO", "iuu_delisting", d(2009, 11, 13)),
                ("SPRFMO", "iuu_listing", d(2008, 1, 5)),
                ("SPRFMO", "iuu_delisting", d(2009, 1, 1)),
                ("SIOFA", "iuu_listing", d(2020, 8, 14)),
            ]
        );

        let a = &ev[0];
        assert_eq!(a.event_id, "iuu_list:imo8692299:IOTC:2019-12-01:listing");
        assert_eq!(a.evidence_category, "enforcement");
        assert_eq!(a.event_subtype.as_deref(), Some("cross_listing"));
        assert_eq!(a.summary.as_deref(), Some("Cross-listing. Listed 2019-12; day not given"));
        assert_eq!(a.event_end_date, None);
        assert_eq!(a.imo_number, Some(8_692_299));
        assert_eq!(a.vessel_name.as_deref(), Some("ABDI BABA 1"));
        assert_eq!(a.ship_type.as_deref(), Some("Fishing Vessel (gear: Purse seine)"));
        assert_eq!(a.gross_tonnage, Some(143.0));
        assert_eq!(a.year_built, Some(1974));

        let b = &ev[1];
        assert_eq!(b.event_subtype.as_deref(), Some("rfmo_listing"));
        assert_eq!(b.summary.as_deref(), Some("Use of fraudulent documentation"));

        // ALFA: each listing closed by its delisting, sharing a case id.
        let (l, x) = (&ev[5], &ev[6]);
        assert_eq!(l.event_end_date, d(2009, 11, 13));
        assert_eq!(l.case_id, x.case_id);
        assert_eq!(x.event_id, "iuu_list:imo7436533:NAFO:2007-06-12:delisting");
        assert_eq!(l.summary.as_deref(), Some("OBS by NOR TRA in RA SCH 07/37"));
        assert_eq!(ev[3].summary, None);
        assert_eq!(ev[8].summary.as_deref(), Some("Delisted 2009; day not given"));
        assert_eq!((l.vessel_id.as_deref(), l.vessel_id_type.as_deref()), (Some("C-00951"), Some("National registration number")));
        assert_eq!(l.call_sign.as_deref(), Some("4LPN"));
        assert_eq!(l.company_name.as_deref(), Some("Stratford Vale Corp"));

        // No IMO number: keyed by name and call sign.
        let s = &ev[9];
        assert_eq!(s.event_id, "iuu_list:abishakputha3-4sfxxxx:SIOFA:2020-08-14:listing");
        assert_eq!(s.mmsi, Some(417_000_878));
    }

    #[test]
    fn keeps_the_first_copy_of_a_listing_across_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let rows = |reason: &'static str| {
            vec![row(&[("Name", "ABDI BABA 1"), ("IMO", "8692299")], &[("ICCAT", "2008-11-24 - ", reason)])]
        };
        let mut vessels = read(&workbook(dir.path(), "new.xlsx", &rows("Newer wording")), "new.xlsx").unwrap();
        vessels.extend(read(&workbook(dir.path(), "old.xlsx", &rows("Older wording")), "old.xlsx").unwrap());
        let p = events(&vessels);
        assert_eq!((p.evidence.events.len(), p.duplicates, p.vessels), (1, 1, 1));
        assert_eq!(p.evidence.events[0].source_file, "new.xlsx");
        assert_eq!(p.evidence.events[0].summary.as_deref(), Some("Newer wording"));
    }

    #[test]
    fn keeps_same_named_vessels_apart() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![
            row(&[("Name", "LU RONG YUAN YU 108")], &[("IOTC", "2019-12 -", "Cross-listing"), ("NPFC", "2018-01 -", "")]),
            row(&[("Name", "LU RONG YUAN YU 108")], &[("IOTC", "2019-12 -", "Cross-listing"), ("NEAFC", "2019 -", "Cross-listing")]),
        ];
        let p = events(&read(&workbook(dir.path(), "f.xlsx", &rows), "f.xlsx").unwrap());
        assert_eq!((p.vessels, p.duplicates, p.evidence.events.len()), (2, 0, 4));
        assert_eq!(p.evidence.events[2].event_id, "iuu_list:lurongyuanyu108~2:IOTC:2019-12-01:listing");
    }

    #[test]
    fn rejects_a_changed_layout() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.xlsx");
        let mut wb = rust_xlsxwriter::Workbook::new();
        let ws = wb.add_worksheet();
        for (c, h) in ["Name", "IMO", "IOTC", "Reason"].iter().enumerate() {
            ws.write_string(0, c as u16, *h).unwrap();
        }
        wb.save(&path).unwrap();
        let err = read(&path, "x.xlsx").unwrap_err().to_string();
        assert!(err.contains("no CCAMLR column"), "{err}");
    }

    #[test]
    fn parses_periods() {
        assert_eq!(split_period("2007-10-31 - 2009-11-13"), ("2007-10-31", "2009-11-13"));
        assert_eq!(split_period("2023-10-11 - "), ("2023-10-11", ""));
        assert_eq!(split_period("2023-10-11 -"), ("2023-10-11", ""));
        let d = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).unwrap();
        assert_eq!(period_date("2023-10-11"), Some((d(2023, 10, 11), None)));
        assert_eq!(period_date("2019-12"), Some((d(2019, 12, 1), Some("2019-12".into()))));
        assert_eq!(period_date("2008"), Some((d(2008, 1, 1), Some("2008".into()))));
        assert_eq!(period_date(""), None);
        assert_eq!(period_date("unknown"), None);
    }
}
