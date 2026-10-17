//! Transportation Safety Board of Canada marine occurrence data (MARSIS),
//! January 1975 onward, published monthly as six CSV files
//! (<https://www.tsb.gc.ca/eng/stats/marine/data-6.html>). Each file loads
//! as published into a `marsis_*` table, and each vessel involved in an
//! occurrence becomes one `vessel_events` row (category
//! `marine_occurrence`). Every load is the whole history.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{NaiveTime, TimeZone, Utc};

use super::{date, double, imo, int, mmsi, text, year, Category, Event, EventType, Evidence, RawTable, SourceInfo};

pub const SOURCE: &str = "tsb_marsis";
pub const DEFAULT_URL: &str = "https://www.tsb.gc.ca/sites/default/files/stats/";

pub const INFO: SourceInfo = SourceInfo {
    source: SOURCE,
    name: "TSB Marine occurrence data (MARSIS)",
    regime: "TSB Canada",
    evidence_categories: "marine_occurrence",
    publisher: "Transportation Safety Board of Canada",
    url: "https://www.tsb.gc.ca/eng/stats/marine/data-6.html",
    access: "Open Government Licence - Canada; six CSV files, refreshed monthly around the 15th",
    coverage: "Marine accidents and incidents reported to the TSB, 1975 onward (complete from 1995)",
    notes: "One vessel_events row per vessel involved; case_id is the occurrence number. Some data is withheld from the public files for privacy.",
};

/// The files, and the raw table each loads into.
pub const FILES: [(&str, &str); 6] = [
    ("MARSISdb_MDOTW_VW_OCCURRENCE_PUBLIC.csv", "marsis_occurrence"),
    ("MARSISdb_MDOTW_VW_OCCURRENCE_VESSEL_PUBLIC.csv", "marsis_occurrence_vessel"),
    ("MARSISdb_MDOTW_VW_INJURIES_PUBLIC.csv", "marsis_injuries"),
    ("MARSISdb_MDOTW_VW_OCCURRENCE_VESSEL_LSA_EQUIPMENT_PUBLIC.csv", "marsis_vessel_lsa_equipment"),
    ("MARSISdb_MDOTW_VW_OCCURRENCE_VESSEL_NAV_EQUIPMENT_PUBLIC.csv", "marsis_vessel_nav_equipment"),
    ("MARSISdb_MDOTW_VW_OCCURRENCE_VESSEL_REC_EQUIPMENT_PUBLIC.csv", "marsis_vessel_rec_equipment"),
];

/// Reads one MARSIS CSV as published. The files are UTF-8 with one or more
/// byte-order marks and quoted fields that can span lines.
pub fn read_raw(path: &Path, table: &str, source_file: &str) -> Result<RawTable> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .with_context(|| format!("opening {}", path.display()))?
        .read_to_end(&mut bytes)?;
    parse_raw(&bytes, table, source_file)
}

pub fn parse_raw(bytes: &[u8], table: &str, source_file: &str) -> Result<RawTable> {
    let mut start = 0;
    while bytes[start..].starts_with(b"\xEF\xBB\xBF") {
        start += 3;
    }
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(&bytes[start..]);
    let columns: Vec<String> = rdr
        .headers()
        .with_context(|| format!("{source_file}: reading header"))?
        .iter()
        .map(|h| h.trim_matches('\u{feff}').trim().to_string())
        .collect();
    let mut rows = Vec::new();
    for (n, rec) in rdr.records().enumerate() {
        let rec = rec.with_context(|| format!("{source_file}: record {}", n + 1))?;
        rows.push(rec.iter().map(text).collect());
    }
    Ok(RawTable { table: table.to_string(), columns, rows, source_file: source_file.to_string() })
}

/// Field access by column name.
struct Rows<'a> {
    t: &'a RawTable,
    idx: HashMap<&'a str, usize>,
}

impl<'a> Rows<'a> {
    fn new(t: &'a RawTable) -> Self {
        Rows { t, idx: t.columns.iter().enumerate().map(|(i, c)| (c.as_str(), i)).collect() }
    }

    fn get(&self, row: &'a [Option<String>], col: &str) -> Option<&'a str> {
        self.idx.get(col).and_then(|&i| row.get(i)).and_then(|v| v.as_deref())
    }

    fn require(&self, cols: &[&str]) -> Result<()> {
        for c in cols {
            anyhow::ensure!(self.idx.contains_key(c), "{}: no column {c}", self.t.source_file);
        }
        Ok(())
    }
}

/// A signed coordinate from MARSIS's unsigned value and N/S/E/W bearing,
/// or `None` when out of range.
fn coordinate(value: Option<&str>, bearing: Option<&str>, limit: f64) -> Option<f64> {
    let v = double(value?)?;
    let v = match bearing.map(str::trim) {
        Some("S") | Some("W") => -v.abs(),
        _ => v,
    };
    (v.abs() <= limit && v != 0.0).then_some(v)
}

fn yes_no(s: Option<&str>) -> Option<bool> {
    match s?.trim() {
        "Yes" => Some(true),
        "No" => Some(false),
        _ => None,
    }
}

fn event_type(s: Option<&str>) -> EventType {
    match s.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("accident") => EventType::Accident,
        Some("incident") => EventType::Incident,
        _ => EventType::Occurrence,
    }
}

fn sum(a: Option<i32>, b: Option<i32>) -> Option<i32> {
    match (a, b) {
        (None, None) => None,
        (a, b) => Some(a.unwrap_or(0) + b.unwrap_or(0)),
    }
}

/// `vessel_events` rows from the occurrence and occurrence-vessel tables:
/// one per (occurrence, vessel), or one with no vessel for an occurrence
/// that lists none. The occurrence file repeats an occurrence once per
/// notification, and the vessel file repeats a vessel once per cargo, damage
/// and tow record; the first row (for vessels, preferring the primary
/// accident type) stands for each, and the repeats stay in the `marsis_*`
/// tables.
pub fn events(occ: &RawTable, ves: &RawTable) -> Result<Evidence> {
    let o = Rows::new(occ);
    let v = Rows::new(ves);
    o.require(&["OccID", "OccNo", "OccDate"])?;
    v.require(&["OccID", "VesselID", "VesselName"])?;

    // (OccID, vessel key) -> row index, first seen order.
    let mut order: Vec<(String, String)> = Vec::new();
    let mut chosen: HashMap<(String, String), usize> = HashMap::new();
    for (i, row) in ves.rows.iter().enumerate() {
        let Some(occ_id) = v.get(row, "OccID") else { continue };
        let vessel = v
            .get(row, "VesselID")
            .map(|id| format!("v{id}"))
            .or_else(|| v.get(row, "VesselName").map(|n| format!("n{n}")))
            .unwrap_or_default();
        let key = (occ_id.to_string(), vessel);
        match chosen.get(&key) {
            None => {
                order.push(key.clone());
                chosen.insert(key, i);
            }
            Some(&j) => {
                let primary = |r: &[Option<String>]| v.get(r, "OccurrencePrimaryAccIncIND") == Some("1");
                if !primary(&ves.rows[j]) && primary(row) {
                    chosen.insert(key, i);
                }
            }
        }
    }
    let mut by_occ: HashMap<&str, Vec<usize>> = HashMap::new();
    for key in &order {
        by_occ.entry(key.0.as_str()).or_default().push(chosen[key]);
    }

    let mut out = Evidence::default();
    let mut done: std::collections::HashSet<&str> = Default::default();
    for orow in &occ.rows {
        let Some(occ_id) = o.get(orow, "OccID") else { continue };
        if !done.insert(occ_id) {
            continue;
        }
        let occ_no = o.get(orow, "OccNo").unwrap_or(occ_id);
        let event_date = o.get(orow, "OccDate").and_then(date);
        let event_time = match (event_date, o.get(orow, "OccTime"), o.get(orow, "TimeZoneDisplayEng")) {
            (Some(d), Some(t), Some("UTC")) => NaiveTime::parse_from_str(t, "%H:%M:%S")
                .ok()
                .map(|t| Utc.from_utc_datetime(&d.and_time(t))),
            _ => None,
        };
        let base = Event {
            source: SOURCE.into(),
            evidence_category: Category::MarineOccurrence.as_str().into(),
            event_type: event_type(o.get(orow, "OccTypeDisplayEng")).as_str().into(),
            event_subtype: o.get(orow, "AccIncTypeDisplayEng").map(Into::into),
            severity: o.get(orow, "ImoClasslevelDisplayEng").map(Into::into),
            regime: INFO.regime.into(),
            authority: o.get(orow, "RegionResponsibilityDisplayEng").map(Into::into),
            source_record_id: occ_no.into(),
            case_id: Some(occ_no.into()),
            event_date,
            event_time,
            place_name: o
                .get(orow, "NearestLocationDescription")
                .or_else(|| o.get(orow, "ProvinceDisplayEng"))
                .map(Into::into),
            latitude: coordinate(o.get(orow, "Latitude"), o.get(orow, "LatEnum_Bearing_DisplayEng"), 90.0),
            longitude: coordinate(o.get(orow, "Longitude"), o.get(orow, "LongEnum_Bearing_DisplayEng"), 180.0),
            deaths: o.get(orow, "TotalDeaths").and_then(int),
            injuries: sum(
                o.get(orow, "TotalMinorInjuries").and_then(int),
                o.get(orow, "TotalSeriousInjuries").and_then(int),
            ),
            missing_persons: o.get(orow, "TotalMissingIndividuals").and_then(int),
            pollution: yes_no(o.get(orow, "PollutionIND_DisplayEng")),
            summary: o.get(orow, "Summary").map(Into::into),
            source_file: occ.source_file.clone(),
            ..Default::default()
        };

        let vessels = by_occ.get(occ_id).map(Vec::as_slice).unwrap_or(&[]);
        if vessels.is_empty() {
            out.events.push(Event { event_id: format!("{SOURCE}:{occ_id}"), ..base });
            continue;
        }
        for (n, &i) in vessels.iter().enumerate() {
            let vrow = &ves.rows[i];
            let key = v.get(vrow, "VesselID").map(str::to_string).unwrap_or_else(|| format!("x{}", n + 1));
            let pollution = match v.get(vrow, "SeaPollutionDegreeDisplayEng") {
                Some("NONE") => Some(false),
                Some("UNKNOWN") | None => base.pollution,
                Some(_) => Some(true),
            };
            let official = v.get(vrow, "OfficialNo");
            out.events.push(Event {
                event_id: format!("{SOURCE}:{occ_id}:{key}"),
                event_type: v
                    .get(vrow, "OccTypeDisplayEng")
                    .map(|t| event_type(Some(t)).as_str().to_string())
                    .unwrap_or_else(|| base.event_type.clone()),
                event_subtype: v.get(vrow, "AccIncTypeDisplayEng").map(Into::into).or_else(|| base.event_subtype.clone()),
                imo_number: v.get(vrow, "IMO").and_then(imo),
                mmsi: v.get(vrow, "MMSI").and_then(mmsi),
                call_sign: v.get(vrow, "CallSign").map(Into::into),
                vessel_name: v.get(vrow, "VesselName").map(Into::into),
                vessel_id: official.or_else(|| v.get(vrow, "VRN")).map(Into::into),
                vessel_id_type: match (official, v.get(vrow, "VRN")) {
                    (Some(_), _) => Some("official_number".into()),
                    (None, Some(_)) => Some("vessel_registration_number".into()),
                    _ => None,
                },
                flag: v.get(vrow, "VesselFlagDisplayEng").map(Into::into),
                ship_type: v.get(vrow, "VesselTypeDisplayEng").map(Into::into),
                gross_tonnage: v.get(vrow, "GrossTonnage").and_then(double).filter(|t| *t > 0.0),
                year_built: v.get(vrow, "YearBuilt").and_then(year),
                classification_society: v.get(vrow, "ClassSocietyDisplayEng").map(Into::into),
                deaths: v.get(vrow, "TotalVesselDeaths").and_then(int),
                injuries: sum(
                    v.get(vrow, "TotalVesselMinorInjuries").and_then(int),
                    v.get(vrow, "TotalVesselSeriousInjuries").and_then(int),
                ),
                missing_persons: v.get(vrow, "TotalVesselMissingIndividuals").and_then(int),
                pollution,
                ..base.clone()
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const OCC: &str = "\u{feff}\u{feff}OccID,OccNo,OccDate,OccTime,TimeZoneDisplayEng,OccTypeDisplayEng,AccIncTypeDisplayEng,ImoClasslevelDisplayEng,Summary,PollutionIND_DisplayEng,Latitude,LatEnum_Bearing_DisplayEng,Longitude,LongEnum_Bearing_DisplayEng,NearestLocationDescription,RegionResponsibilityDisplayEng,TotalDeaths,TotalMinorInjuries,TotalSeriousInjuries,TotalMissingIndividuals
54616,\"M26A0299\",2026-09-13 00:00:00.0000000,13:30:00,UTC,Incident,TOTAL FAILURE,MARINE CASUALTY,\"Disabled, towed
to Twillingate.\",Unknown,49.74200000,N,054.82566667,W,Long Point,ATLANTIC REGION,0,0,0,0
20785,M92M4022,1992-06-02 00:00:00.0000000,09:22:00,LOCAL,Accident,COLLISION,,Collision in fog,Yes,44.35,N,63.2333,W,,MARITIMES REGION,0,2,0,0
20785,M92M4022,1992-06-02 00:00:00.0000000,09:22:00,LOCAL,Accident,COLLISION,,Collision in fog (second notification),Yes,44.35,N,63.2333,W,,MARITIMES REGION,0,2,0,0
99,X1,1999-01-01 00:00:00.0000000,,,,,,No vessels,,,,,,,,,,,
";

    const VES: &str = "OccNo,OccID,VesselID,VesselName,IMO,OfficialNo,VRN,MMSI,CallSign,VesselTypeDisplayEng,VesselFlagDisplayEng,GrossTonnage,YearBuilt,ClassSocietyDisplayEng,OccurrencePrimaryAccIncIND,OccTypeDisplayEng,AccIncTypeDisplayEng,SeaPollutionDegreeDisplayEng,TotalVesselDeaths,TotalVesselMinorInjuries,TotalVesselSeriousInjuries,TotalVesselMissingIndividuals
M26A0299,54616,32039,SEA SHEPARD II,,C31434NL,125798,,,FISHING,CANADA,4.99,,,1,Incident,TOTAL FAILURE,UNKNOWN,0,0,0,0
M92M4022,20785,9103,LAKE MICHIGAN,7045695,,,,3EBT6,CARGO - SOLID,PANAMA,11502.0,1971,,0,Accident,COLLISION,NONE,0,1,0,0
M92M4022,20785,9103,LAKE MICHIGAN,7045695,,,,3EBT6,CARGO - SOLID,PANAMA,11502.0,1971,,1,Accident,STRIKING,NONE,0,1,0,0
M92M4022,20785,9104,CHARLENE & SHERYL,,,,316001234,,FISHING,CANADA,0,,,1,Accident,COLLISION,MINOR,0,1,0,0
";

    fn tables() -> (RawTable, RawTable) {
        (
            parse_raw(OCC.as_bytes(), "marsis_occurrence", "occ.csv").unwrap(),
            parse_raw(VES.as_bytes(), "marsis_occurrence_vessel", "ves.csv").unwrap(),
        )
    }

    #[test]
    fn reads_files_as_published() {
        let (occ, ves) = tables();
        assert_eq!(occ.columns[0], "OccID", "byte-order marks stripped");
        assert_eq!(occ.rows.len(), 4, "a quoted summary spanning lines is one record");
        assert_eq!(occ.rows[0][8].as_deref(), Some("Disabled, towed\nto Twillingate."));
        assert_eq!(ves.rows.len(), 4, "repeated vessel rows kept");
    }

    #[test]
    fn one_event_per_vessel_per_occurrence() {
        let (occ, ves) = tables();
        let ev = events(&occ, &ves).unwrap();
        let ids: Vec<&str> = ev.events.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(
            ids,
            ["tsb_marsis:54616:32039", "tsb_marsis:20785:9103", "tsb_marsis:20785:9104", "tsb_marsis:99"],
            "a repeated occurrence row (another notification) adds no events"
        );

        let e = &ev.events[0];
        assert_eq!((e.evidence_category.as_str(), e.event_type.as_str()), ("marine_occurrence", "incident"));
        assert_eq!(e.case_id.as_deref(), Some("M26A0299"));
        assert_eq!(e.event_time.unwrap().to_rfc3339(), "2026-09-13T13:30:00+00:00");
        assert_eq!(e.latitude, Some(49.742));
        assert!((e.longitude.unwrap() + 54.82566667).abs() < 1e-9, "west is negative");
        assert_eq!(e.vessel_id.as_deref(), Some("C31434NL"));
        assert_eq!(e.vessel_id_type.as_deref(), Some("official_number"));
        assert_eq!(e.pollution, None, "UNKNOWN degree falls back to the occurrence's Unknown");

        let lake = &ev.events[1];
        assert_eq!(lake.summary.as_deref(), Some("Collision in fog"), "first notification's row");
        assert_eq!(lake.event_subtype.as_deref(), Some("STRIKING"), "primary accident row preferred");
        assert_eq!(lake.imo_number, Some(7_045_695));
        assert_eq!(lake.event_time, None, "local times aren't converted");
        assert_eq!(lake.pollution, Some(false));
        assert_eq!(lake.injuries, Some(1));
        assert_eq!(lake.year_built, Some(1971));

        let small = &ev.events[2];
        assert_eq!(small.mmsi, Some(316_001_234));
        assert_eq!(small.pollution, Some(true));
        assert_eq!(small.gross_tonnage, None, "zero tonnage means unknown");
        assert_eq!(small.case_id, lake.case_id);

        let lone = &ev.events[3];
        assert_eq!(lone.vessel_name, None);
        assert_eq!(lone.event_type, "occurrence");
    }
}
