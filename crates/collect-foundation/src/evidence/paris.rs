//! Paris MoU port state control inspections, from the bulk XML files
//! registered users download from the Paris MoU data-sharing page
//! (<https://parismou.org/Inspection-Database/data-sharing>). The files are
//! THETIS `InspectionResultsReceipt` documents
//! (`urn:getPublicInspections.xmlData.business.thetis.emsa.europa.eu`), one
//! `Inspection` element per inspection. Access needs an account, so this
//! reads the files from a directory (`.xml`, or `.zip` holding `.xml`); a
//! load takes every file there, so keep the whole history in it.
//!
//! Besides `vessel_events` and `vessel_event_findings`, certificates and
//! charterers recorded at inspection go to `paris_mou_certificates` and
//! `paris_mou_charterers`.

use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use quick_xml::events::{BytesStart, Event as Xml};
use quick_xml::Reader;

use super::{col, date, double, imo, int, text, year, Category, Column, Event, EventType, Evidence, Finding, Kind, SourceInfo, Val};

pub const SOURCE: &str = "paris_mou";
pub const CERTIFICATES_TABLE: &str = "paris_mou_certificates";
pub const CHARTERERS_TABLE: &str = "paris_mou_charterers";

pub const INFO: SourceInfo = SourceInfo {
    source: SOURCE,
    name: "Paris MoU inspection data (bulk XML)",
    regime: "Paris MoU",
    evidence_categories: "port_state_control",
    publisher: "Paris MoU Secretariat / EMSA (THETIS)",
    url: "https://parismou.org/Inspection-Database/data-sharing",
    access: "Bulk XML for registered users (apply on the data-sharing page; API access on request)",
    coverage: "Every PSC inspection by Paris MoU member states in the files supplied",
    notes: "Codes as THETIS gives them: flag, authority and company country ISO alpha-2, place UN/LOCODE, ship type and RO numeric codes.",
};

/// One certificate recorded at an inspection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Certificate {
    pub event_id: String,
    pub inspection_id: String,
    /// `class` or `statutory`.
    pub certificate_kind: String,
    pub certificate_code: Option<String>,
    pub issuing_authority_type: Option<String>,
    pub issuing_authority: Option<String>,
    pub class_status: Option<String>,
    pub date_of_status: Option<NaiveDate>,
    pub date_of_issue: Option<NaiveDate>,
    pub date_of_expiry: Option<NaiveDate>,
    pub date_of_last_survey: Option<NaiveDate>,
    pub place_of_last_survey: Option<String>,
    pub surveying_authority_type: Option<String>,
    pub surveying_authority: Option<String>,
    pub source_file: String,
}

/// One charterer recorded at an inspection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Charterer {
    pub event_id: String,
    pub inspection_id: String,
    pub name: Option<String>,
    pub address: Option<String>,
    pub city: Option<String>,
    pub country: Option<String>,
    /// Charter type (`TIME`, `VOYAGE`, ...).
    pub charter_type: Option<String>,
    pub source_file: String,
}

pub const CERTIFICATE_COLUMNS: &[Column<Certificate>] = &[
    col!(event_id, Str, req),
    col!(inspection_id, Str, req),
    col!(certificate_kind, Str, req),
    col!(certificate_code, Str),
    col!(issuing_authority_type, Str),
    col!(issuing_authority, Str),
    col!(class_status, Str),
    col!(date_of_status, Date),
    col!(date_of_issue, Date),
    col!(date_of_expiry, Date),
    col!(date_of_last_survey, Date),
    col!(place_of_last_survey, Str),
    col!(surveying_authority_type, Str),
    col!(surveying_authority, Str),
    col!(source_file, Str, req),
];

pub const CHARTERER_COLUMNS: &[Column<Charterer>] = &[
    col!(event_id, Str, req),
    col!(inspection_id, Str, req),
    col!(name, Str),
    col!(address, Str),
    col!(city, Str),
    col!(country, Str),
    col!(charter_type, Str),
    col!(source_file, Str, req),
];

#[derive(Default)]
pub struct Parsed {
    pub evidence: Evidence,
    pub certificates: Vec<Certificate>,
    pub charterers: Vec<Charterer>,
    /// `Inspection` elements skipped for having no `InspectionID`.
    pub rejected: usize,
}

impl Parsed {
    pub fn extend(&mut self, other: Parsed) {
        self.evidence.events.extend(other.evidence.events);
        self.evidence.findings.extend(other.evidence.findings);
        self.certificates.extend(other.certificates);
        self.charterers.extend(other.charterers);
        self.rejected += other.rejected;
    }
}

/// An element's attributes by local name (THETIS qualifies them).
fn attrs(e: &BytesStart) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for a in e.attributes() {
        let a = a?;
        let key = String::from_utf8_lossy(a.key.local_name().as_ref()).into_owned();
        if a.key.as_ref().starts_with(b"xmlns") {
            continue;
        }
        let value = a.normalized_value(quick_xml::XmlVersion::Implicit1_0)?.into_owned();
        out.push((key, value));
    }
    Ok(out)
}

fn attr<'a>(a: &'a [(String, String)], name: &str) -> Option<&'a str> {
    a.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str()).filter(|v| !v.trim().is_empty())
}

fn attr_text(a: &[(String, String)], name: &str) -> Option<String> {
    attr(a, name).and_then(text)
}

fn attr_bool(a: &[(String, String)], name: &str) -> Option<bool> {
    match attr(a, name)?.trim() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

/// One `Inspection` while it is being read.
#[derive(Default)]
struct Current {
    event: Event,
    detention: Option<Vec<(String, String)>>,
    findings: Vec<Finding>,
    certificates: Vec<Certificate>,
    charterers: Vec<Charterer>,
}

/// Parses one `InspectionResultsReceipt` document.
pub fn parse(reader: impl BufRead, source_file: &str) -> Result<Parsed> {
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut out = Parsed::default();
    let mut cur: Option<Current> = None;
    let mut in_particulars = false;

    loop {
        let ev = xml
            .read_event_into(&mut buf)
            .with_context(|| format!("{source_file}: XML error at byte {}", xml.buffer_position()))?;
        match ev {
            Xml::Eof => break,
            Xml::Start(ref e) | Xml::Empty(ref e) => {
                let empty = matches!(ev, Xml::Empty(_));
                let name = e.local_name();
                let a = attrs(e)?;
                match name.as_ref() {
                    b"Inspection" => {
                        let mut c = Current::default();
                        let ev = &mut c.event;
                        let id = attr_text(&a, "InspectionID").unwrap_or_default();
                        ev.event_id = format!("{SOURCE}:{id}");
                        ev.source_record_id = id;
                        ev.source = SOURCE.into();
                        ev.evidence_category = Category::PortStateControl.as_str().into();
                        ev.regime = INFO.regime.into();
                        ev.authority = attr_text(&a, "ReportingAuthority");
                        ev.place_code = attr_text(&a, "PlaceOfInspection");
                        ev.place_country = ev.place_code.as_deref().and_then(|p| p.get(..2)).map(Into::into);
                        ev.event_date = attr(&a, "DateOfFirstVisit").and_then(date);
                        ev.event_end_date = attr(&a, "DateOfFinalVisit").and_then(date);
                        ev.event_subtype = attr_text(&a, "PSCInspectionType");
                        ev.source_file = source_file.into();
                        if empty {
                            finish(c, &mut out);
                        } else {
                            cur = Some(c);
                        }
                    }
                    _ if cur.is_none() => {}
                    b"ShipParticulars" => {
                        let ev = &mut cur.as_mut().unwrap().event;
                        ev.imo_number = attr(&a, "IMO").and_then(imo);
                        in_particulars = !empty;
                    }
                    field if in_particulars => {
                        let ev = &mut cur.as_mut().unwrap().event;
                        let value = attr_text(&a, "Value");
                        match field {
                            b"Name" => ev.vessel_name = value,
                            b"Flag" => ev.flag_code = value,
                            b"ShipType" => ev.ship_type = value,
                            b"KeelDate" => ev.year_built = value.as_deref().and_then(year),
                            b"CallSign" => ev.call_sign = value,
                            b"GrossTonnage" => ev.gross_tonnage = value.as_deref().and_then(double),
                            _ => {}
                        }
                    }
                    b"ISMCompany" => {
                        let ev = &mut cur.as_mut().unwrap().event;
                        ev.company_imo = attr_text(&a, "IMO");
                        ev.company_name = attr_text(&a, "Name");
                    }
                    b"Charterer" => {
                        let c = cur.as_mut().unwrap();
                        c.charterers.push(Charterer {
                            event_id: c.event.event_id.clone(),
                            inspection_id: c.event.source_record_id.clone(),
                            name: attr_text(&a, "Name"),
                            address: attr_text(&a, "Address"),
                            city: attr_text(&a, "City"),
                            country: attr_text(&a, "Country"),
                            charter_type: attr_text(&a, "Type"),
                            source_file: source_file.into(),
                        });
                    }
                    kind @ (b"ClassCertificate" | b"StatutoryCertificate") => {
                        let c = cur.as_mut().unwrap();
                        let class = kind == b"ClassCertificate";
                        c.certificates.push(Certificate {
                            event_id: c.event.event_id.clone(),
                            inspection_id: c.event.source_record_id.clone(),
                            certificate_kind: if class { "class" } else { "statutory" }.into(),
                            certificate_code: attr_text(&a, "CertificateCode"),
                            // THETIS spells it "IssuingAutorityType".
                            issuing_authority_type: attr_text(&a, "IssuingAutorityType")
                                .or_else(|| attr_text(&a, "IssuingAuthorityType")),
                            issuing_authority: attr_text(&a, "IssuingAuthority"),
                            class_status: attr_text(&a, "ClassStatus"),
                            date_of_status: attr(&a, "DateOfStatus").and_then(date),
                            date_of_issue: attr(&a, "DateOfIssue").and_then(date),
                            date_of_expiry: attr(&a, "DateOfExpiry").and_then(date),
                            date_of_last_survey: attr(&a, "DateOfLastSurvey").and_then(date),
                            place_of_last_survey: attr_text(&a, "PlaceOfLastSurvey"),
                            surveying_authority_type: attr_text(&a, "SurveyingAuthorityType"),
                            surveying_authority: attr_text(&a, "SurveyingAuthority"),
                            source_file: source_file.into(),
                        });
                    }
                    b"Deficiency" => {
                        let c = cur.as_mut().unwrap();
                        c.findings.push(Finding {
                            event_id: c.event.event_id.clone(),
                            source: SOURCE.into(),
                            finding_seq: c.findings.len() as i32 + 1,
                            finding_date: c.event.event_date,
                            finding_code: attr_text(&a, "DefectiveItemCode"),
                            nature_of_defect_code: attr_text(&a, "NatureOfDefectCode"),
                            ground_for_detention: attr_bool(&a, "isGroundDetention"),
                            ro_related: attr_bool(&a, "isRORelated"),
                            accidental_damage: attr_bool(&a, "isAccidentalDamage"),
                            source_file: source_file.into(),
                            ..Default::default()
                        });
                    }
                    b"Detention" => cur.as_mut().unwrap().detention = Some(a),
                    _ => {}
                }
            }
            Xml::End(ref e) => match e.local_name().as_ref() {
                b"ShipParticulars" => in_particulars = false,
                b"Inspection" => {
                    if let Some(c) = cur.take() {
                        finish(c, &mut out);
                    }
                }
                _ => {}
            },
            _ => {}
        }
        buf.clear();
    }
    Ok(out)
}

fn finish(mut c: Current, out: &mut Parsed) {
    if c.event.source_record_id.is_empty() {
        out.rejected += 1;
        return;
    }
    let ev = &mut c.event;
    ev.deficiency_count = Some(c.findings.len() as i32);
    ev.detainable_deficiency_count =
        Some(c.findings.iter().filter(|f| f.ground_for_detention == Some(true)).count() as i32);
    ev.classification_society = c
        .certificates
        .iter()
        .find(|x| x.certificate_kind == "class")
        .and_then(|x| x.issuing_authority.clone());
    match &c.detention {
        Some(d) => {
            ev.event_type = EventType::Detention.as_str().into();
            ev.detained = Some(true);
            ev.detention_days = attr(d, "DurationOfDetention").and_then(int);
            ev.summary = attr_text(d, "Type").map(|t| format!("Detention type: {t}"));
        }
        None => {
            ev.event_type = EventType::Inspection.as_str().into();
            ev.detained = Some(false);
        }
    }
    out.evidence.events.push(c.event);
    out.evidence.findings.extend(c.findings);
    out.certificates.extend(c.certificates);
    out.charterers.extend(c.charterers);
}

/// Parses a `.xml` file, or every `.xml` entry of a `.zip`.
pub fn read(path: &Path, label: &str) -> Result<Parsed> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let is_zip = path.extension().and_then(|e| e.to_str()).is_some_and(|e| e.eq_ignore_ascii_case("zip"));
    if !is_zip {
        return parse(BufReader::with_capacity(1 << 20, file), label);
    }
    let mut zip = zip::ZipArchive::new(file).with_context(|| format!("reading {}", path.display()))?;
    let mut out = Parsed::default();
    for i in 0..zip.len() {
        let entry = zip.by_index(i)?;
        let name = entry.name().to_string();
        if !name.to_ascii_lowercase().ends_with(".xml") {
            continue;
        }
        let mut text = Vec::with_capacity(entry.size() as usize);
        BufReader::new(entry).read_to_end(&mut text)?;
        out.extend(parse(&text[..], &format!("{label}/{name}"))?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rebuilt from the Paris MoU's published example (inspection
    /// 2397763648) plus a detained inspection.
    const SAMPLE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<InspectionResultsReceipt xmlns:ns1="urn:getPublicInspections.xmlData.business.thetis.emsa.europa.eu">
  <ns1:Inspection ns1:InspectionID="2397763648" ns1:ReportingAuthority="CA" ns1:PlaceOfInspection="CAPRR" ns1:DateOfFirstVisit="2020-07-23Z" ns1:DateOfFinalVisit="2020-07-24Z" ns1:PSCInspectionType="EXPANDED_INSPECTION">
    <ns1:ShipParticulars ns1:IMO="9296793">
      <ns1:Name ns1:Value="APOLLO" ns1:EffectDate="2020-07-22Z"/>
      <ns1:Flag ns1:Value="MH" ns1:EffectDate="2020-07-22Z"/>
      <ns1:ShipType ns1:Value="340" ns1:EffectDate="2020-07-22Z"/>
      <ns1:KeelDate ns1:Value="2003-10-29Z" ns1:EffectDate="2020-07-22Z"/>
      <ns1:CallSign ns1:Value="V7JF8" ns1:EffectDate="2020-07-22Z"/>
      <ns1:GrossTonnage ns1:Value="40219" ns1:EffectDate="2020-07-22Z"/>
    </ns1:ShipParticulars>
    <ns1:ISMCompany ns1:IMO="0771929" ns1:Name="Transocean Maritime Agencies" ns1:Country="MC"/>
    <ns1:Charterers>
      <ns1:Charterer ns1:Name="DS NORDEN A/S" ns1:Address="52, Strandvejen, DK-2900" ns1:City="Hellerup" ns1:Country="DK" ns1:Type="TIME"/>
    </ns1:Charterers>
    <ns1:ClassCertificates>
      <ns1:ClassCertificate ns1:IssuingAuthority="278" ns1:ClassStatus="DELIVERED" ns1:DateOfStatus="2018-02-19Z"/>
    </ns1:ClassCertificates>
    <ns1:StatutoryCertificates>
      <ns1:StatutoryCertificate ns1:CertificateCode="523" ns1:IssuingAutorityType="RO" ns1:IssuingAuthority="128" ns1:DateOfIssue="2006-01-02Z"/>
      <ns1:StatutoryCertificate ns1:CertificateCode="507" ns1:IssuingAutorityType="RO" ns1:IssuingAuthority="278" ns1:DateOfIssue="2018-01-05Z" ns1:DateOfExpiry="2021-01-31Z"/>
    </ns1:StatutoryCertificates>
    <ns1:Deficiencies/>
  </ns1:Inspection>
  <ns1:Inspection ns1:InspectionID="2400000001" ns1:ReportingAuthority="NL" ns1:PlaceOfInspection="NLRTM" ns1:DateOfFirstVisit="2021-03-01Z" ns1:DateOfFinalVisit="2021-03-05Z" ns1:PSCInspectionType="MORE_DETAILED_INSPECTION">
    <ns1:ShipParticulars ns1:IMO="9100001">
      <ns1:Name ns1:Value="SEA &amp; SKY"/>
      <ns1:Flag ns1:Value="TG"/>
    </ns1:ShipParticulars>
    <ns1:Deficiencies>
      <ns1:Deficiency ns1:DefectiveItemCode="07109" ns1:NatureOfDefectCode="13" ns1:isGroundDetention="true" ns1:isRORelated="false" ns1:isAccidentalDamage="false"/>
      <ns1:Deficiency ns1:DefectiveItemCode="11101" ns1:NatureOfDefectCode="2" ns1:isGroundDetention="false" ns1:isRORelated="true" ns1:isAccidentalDamage="false"/>
    </ns1:Deficiencies>
    <ns1:Detention ns1:Type="DETENTION" ns1:DurationOfDetention="4"/>
  </ns1:Inspection>
  <ns1:Inspection ns1:ReportingAuthority="NL"/>
</InspectionResultsReceipt>"#;

    #[test]
    fn parses_inspections() {
        let p = parse(SAMPLE.as_bytes(), "sample.xml").unwrap();
        assert_eq!(p.rejected, 1, "no InspectionID");
        let ev = &p.evidence.events;
        assert_eq!(ev.len(), 2);

        let a = &ev[0];
        assert_eq!(a.event_id, "paris_mou:2397763648");
        assert_eq!((a.evidence_category.as_str(), a.event_type.as_str()), ("port_state_control", "inspection"));
        assert_eq!(a.event_subtype.as_deref(), Some("EXPANDED_INSPECTION"));
        assert_eq!(a.authority.as_deref(), Some("CA"));
        assert_eq!(a.place_code.as_deref(), Some("CAPRR"));
        assert_eq!(a.place_country.as_deref(), Some("CA"));
        assert_eq!(a.event_date, NaiveDate::from_ymd_opt(2020, 7, 23));
        assert_eq!(a.event_end_date, NaiveDate::from_ymd_opt(2020, 7, 24));
        assert_eq!(a.imo_number, Some(9_296_793));
        assert_eq!(a.vessel_name.as_deref(), Some("APOLLO"));
        assert_eq!(a.flag_code.as_deref(), Some("MH"));
        assert_eq!(a.year_built, Some(2003));
        assert_eq!(a.gross_tonnage, Some(40219.0));
        assert_eq!(a.company_imo.as_deref(), Some("0771929"));
        assert_eq!(a.classification_society.as_deref(), Some("278"));
        assert_eq!((a.detained, a.deficiency_count), (Some(false), Some(0)));

        let b = &ev[1];
        assert_eq!(b.event_type, "detention");
        assert_eq!(b.vessel_name.as_deref(), Some("SEA & SKY"));
        assert_eq!((b.detained, b.detention_days), (Some(true), Some(4)));
        assert_eq!((b.deficiency_count, b.detainable_deficiency_count), (Some(2), Some(1)));

        let f = &p.evidence.findings;
        assert_eq!(f.len(), 2);
        assert_eq!((f[0].event_id.as_str(), f[0].finding_seq), ("paris_mou:2400000001", 1));
        assert_eq!(f[0].finding_code.as_deref(), Some("07109"));
        assert_eq!(f[0].ground_for_detention, Some(true));
        assert_eq!(f[1].ro_related, Some(true));

        assert_eq!(p.certificates.len(), 3);
        assert_eq!(p.certificates[1].issuing_authority_type.as_deref(), Some("RO"));
        assert_eq!(p.certificates[2].date_of_expiry, NaiveDate::from_ymd_opt(2021, 1, 31));
        assert_eq!(p.charterers.len(), 1);
        assert_eq!(p.charterers[0].charter_type.as_deref(), Some("TIME"));
    }

    #[test]
    fn tables_match_their_schemas() {
        let p = parse(SAMPLE.as_bytes(), "sample.xml").unwrap();
        let now = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let b = super::super::to_batch(CERTIFICATE_COLUMNS, &p.certificates, now).unwrap();
        ais_tracks::carry::check_batches(&super::super::schema(CERTIFICATE_COLUMNS), &[b]).unwrap();
        let b = super::super::to_batch(CHARTERER_COLUMNS, &p.charterers, now).unwrap();
        ais_tracks::carry::check_batches(&super::super::schema(CHARTERER_COLUMNS), &[b]).unwrap();
    }

    #[test]
    fn reads_zipped_files() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bulk.zip");
        let mut zip = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
        let opts = zip::write::SimpleFileOptions::default();
        zip.start_file("readme.txt", opts).unwrap();
        zip.write_all(b"ignored").unwrap();
        zip.start_file("2021/part1.xml", opts).unwrap();
        zip.write_all(SAMPLE.as_bytes()).unwrap();
        zip.finish().unwrap();
        let p = read(&path, "bulk.zip").unwrap();
        assert_eq!(p.evidence.events.len(), 2);
        assert_eq!(p.evidence.events[0].source_file, "bulk.zip/2021/part1.xml");
    }
}
