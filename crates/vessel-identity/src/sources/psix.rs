//! USCG Port State Information Exchange (PSIX), through its SOAP web service
//! (`getVesselSummaryXMLString`). The service has no bulk export, so the
//! register is fetched in slices by build year (plus one for an unknown
//! year), slowly: it answers 503 when pressed. Each slice's response is kept
//! in the scratch directory, so an interrupted fetch resumes where it
//! stopped, and the slices are archived together as one zip.
//!
//! A row is one vessel: `VesselId` (PSIX's own id), name, call sign, service
//! type, build year, status, flag and `Identification`, which is the IMO
//! number for foreign vessels and the official (documentation) number for
//! US-flagged ones. A slice that still fails after retries makes the fetch
//! incomplete, and an incomplete fetch never removes records.

use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chrono::{Datelike, Utc};
use quick_xml::events::Event;
use quick_xml::Reader;

use super::{attr, Draft, FetchCtx, Fetched, Mapper, Parsed, Record, Rejected};
use crate::config::SourceConfig;
use crate::fetch::{decode_text, files_in, read_local, unzip, RawObject};

const SOAP_ACTION: &str = "https://cgmix.uscg.mil/getVesselSummaryXMLString";
const RESULT_TAG: &str = "getVesselSummaryXMLStringResult";
/// The slice holding vessels whose build year PSIX doesn't know.
const UNKNOWN_YEAR: &str = "0";

fn envelope(build_year: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getVesselSummaryXMLString xmlns="https://cgmix.uscg.mil">
      <VesselID></VesselID><VesselName></VesselName><CallSign></CallSign><VIN></VIN><HIN></HIN><Flag></Flag><Service></Service><BuildYear>{build_year}</BuildYear>
    </getVesselSummaryXMLString>
  </soap:Body>
</soap:Envelope>"#
    )
}

fn slices(cfg: &SourceConfig) -> Vec<String> {
    let first = cfg.option_u64("first_build_year", 1850) as i32;
    let last = Utc::now().year() + 1;
    let mut out: Vec<String> = (first..=last).rev().map(|y| y.to_string()).collect();
    out.push(UNKNOWN_YEAR.into());
    out
}

fn fresh(path: &Path, max_age: Duration) -> bool {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .is_ok_and(|t| SystemTime::now().duration_since(t).unwrap_or_default() < max_age)
}

pub async fn fetch(cfg: &SourceConfig, ctx: &FetchCtx<'_>) -> Result<Fetched> {
    if let Some(dir) = ctx.local_dir {
        let mut objects = Vec::new();
        for f in files_in(dir, &["zip", "xml"])? {
            objects.push(read_local(&f)?);
        }
        anyhow::ensure!(!objects.is_empty(), "no .zip or .xml in {}", dir.display());
        return Ok(Fetched { objects, source_version: None, source_published_at: None, complete: true, warnings: Vec::new() });
    }

    let dir = ctx.scratch.join(&cfg.source_id).join("slices");
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let delay = Duration::from_millis(cfg.option_u64("request_delay_ms", 3000));
    let max_age = Duration::from_secs(3600 * cfg.option_u64("slice_max_age_hours", 72));
    let attempts = cfg.option_u64("max_attempts", 6) as u32;
    let mut failed = Vec::new();
    let all = slices(cfg);
    let mut reused = 0;
    for (i, year) in all.iter().enumerate() {
        let path = dir.join(format!("build_year_{year}.xml"));
        if fresh(&path, max_age) {
            reused += 1;
            continue;
        }
        if i > 0 {
            tokio::time::sleep(delay).await;
        }
        let body = envelope(year);
        let req = || {
            ctx.http
                .client()
                .post(&cfg.source_url)
                .header("Content-Type", "text/xml; charset=utf-8")
                .header("SOAPAction", format!("\"{SOAP_ACTION}\""))
                .body(body.clone())
        };
        // Retried here rather than by `Http::send`: this source's budget is
        // larger and its backoff longer.
        let result = {
            let mut last = None;
            let mut got = None;
            for attempt in 1..=attempts {
                if attempt > 1 {
                    let wait = Duration::from_secs((2u64 << attempt.min(6)).min(120));
                    tokio::time::sleep(wait).await;
                }
                match req().send().await {
                    Ok(r) if r.status().is_success() => match r.bytes().await {
                        Ok(b) => {
                            got = Some(b);
                            break;
                        }
                        Err(e) => last = Some(e.to_string()),
                    },
                    Ok(r) => last = Some(format!("HTTP {}", r.status())),
                    Err(e) => last = Some(e.to_string()),
                }
            }
            got.ok_or_else(|| anyhow::anyhow!(last.unwrap_or_default()))
        };
        match result.and_then(|b| summaries(&b).map(|_| b)) {
            Ok(bytes) => {
                let tmp = path.with_extension("part");
                std::fs::write(&tmp, &bytes)?;
                std::fs::rename(&tmp, &path)?;
                if (i + 1) % 20 == 0 {
                    eprintln!("  psix: {}/{} slices", i + 1, all.len());
                }
            }
            Err(e) => {
                eprintln!("  psix: build year {year} failed: {e:#}");
                failed.push(year.clone());
            }
        }
    }
    if reused > 0 {
        eprintln!("  psix: reused {reused} slices fetched in the last {}h", max_age.as_secs() / 3600);
    }
    if failed.len() * 2 > all.len() {
        bail!("{} of {} PSIX slices failed; the service may be down", failed.len(), all.len());
    }

    // Archive the slices as one zip, entries in a fixed order with a fixed
    // timestamp, so the same responses always give the same bytes.
    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let opts = zip::write::SimpleFileOptions::default().last_modified_time(zip::DateTime::default());
        for year in &all {
            let path = dir.join(format!("build_year_{year}.xml"));
            if let Ok(bytes) = std::fs::read(&path) {
                zip.start_file(format!("build_year_{year}.xml"), opts)?;
                zip.write_all(&bytes)?;
            }
        }
        zip.finish()?;
    }
    let mut warnings = Vec::new();
    if !failed.is_empty() {
        warnings.push(format!("incomplete: build years {} failed", failed.join(", ")));
    }
    Ok(Fetched {
        objects: vec![RawObject::new(format!("psix_{}.zip", Utc::now().format("%Y%m%d")), Bytes::from(buf))],
        source_version: Some(format!("{} slices", all.len() - failed.len())),
        source_published_at: None,
        complete: failed.is_empty(),
        warnings,
    })
}

/// The `VesselSummary` rows in one SOAP response (or in the bare
/// `NewDataSet` XML), each as element name -> text.
pub fn summaries(response: &[u8]) -> Result<Vec<BTreeMap<String, String>>> {
    let text = decode_text(response);
    let inner = match (text.find(&format!("<{RESULT_TAG}>")), text.find(&format!("</{RESULT_TAG}>"))) {
        (Some(a), Some(b)) => {
            let raw = &text[a + RESULT_TAG.len() + 2..b];
            quick_xml::escape::unescape(raw).context("unescaping the result")?.into_owned()
        }
        _ if text.contains("<NewDataSet") || text.contains(&format!("<{RESULT_TAG} />")) => text.clone(),
        _ => bail!("not a getVesselSummaryXMLString response"),
    };
    let mut reader = Reader::from_str(&inner);
    let mut rows = Vec::new();
    let mut row: Option<BTreeMap<String, String>> = None;
    let mut field: Option<(String, String)> = None;
    loop {
        match reader.read_event().context("reading PSIX XML")? {
            Event::Start(e) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if name == "VesselSummary" {
                    row = Some(BTreeMap::new());
                } else if row.is_some() {
                    field = Some((name, String::new()));
                }
            }
            Event::Text(t) => {
                if let Some((_, buf)) = field.as_mut() {
                    buf.push_str(&t.decode()?);
                }
            }
            Event::CData(t) => {
                if let Some((_, buf)) = field.as_mut() {
                    buf.push_str(&String::from_utf8_lossy(&t));
                }
            }
            Event::GeneralRef(r) => {
                if let Some((_, buf)) = field.as_mut() {
                    let entity = format!("&{};", r.decode()?);
                    buf.push_str(&quick_xml::escape::unescape(&entity).map(|c| c.into_owned()).unwrap_or(entity));
                }
            }
            Event::End(e) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if name == "VesselSummary" {
                    if let Some(r) = row.take() {
                        rows.push(r);
                    }
                } else if let (Some(r), Some((f, v))) = (row.as_mut(), field.take()) {
                    let v = v.trim();
                    if f == name && !v.is_empty() {
                        r.insert(f, v.to_string());
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(rows)
}

pub fn parse(cfg: &SourceConfig, objects: &[RawObject]) -> Result<Parsed> {
    let excluded: HashSet<String> = cfg.option_strings("exclude_service_types").into_iter().map(|s| s.to_uppercase()).collect();
    let mut parsed = Parsed::default();
    let mut seen = HashSet::new();
    let mut skipped = 0usize;
    for (oi, o) in objects.iter().enumerate() {
        let docs: Vec<(String, Vec<u8>)> = if o.name.to_ascii_lowercase().ends_with(".zip") {
            unzip(&o.bytes, ".xml")?
        } else {
            vec![(o.name.clone(), o.bytes.to_vec())]
        };
        for (name, bytes) in docs {
            let rows = summaries(&bytes).with_context(|| format!("parsing {name}"))?;
            for (i, fields) in rows.into_iter().enumerate() {
                let service = fields.get("ServiceType").map(|s| s.to_uppercase()).unwrap_or_default();
                if excluded.contains(&service) {
                    skipped += 1;
                    continue;
                }
                let Some(id) = fields.get("VesselId").cloned() else {
                    parsed.rejected.push(Rejected {
                        location: format!("{name} row {}", i + 1),
                        raw: serde_json::to_string(&fields).ok(),
                        reasons: vec!["no VesselId".into()],
                        object: oi,
                    });
                    continue;
                };
                if !seen.insert(id.clone()) {
                    continue;
                }
                parsed.records.push(Record { source_record_id: id.clone(), subject_id: id, fields, object: oi, published_at: None });
            }
        }
    }
    if skipped > 0 {
        parsed.warnings.push(format!("{skipped} rows skipped by exclude_service_types"));
    }
    Ok(parsed)
}

pub fn map(fields: &BTreeMap<String, String>) -> Vec<Draft> {
    let mut m = Mapper::new(fields);
    if let Some(id) = m.get("VesselId") {
        m.push(Draft::new(attr::PSIX_VESSEL_ID, "identifier", id, Some(id.trim().to_string())).qualified("US").checked("valid"));
    }
    m.name(attr::NAME, "VesselName");
    m.callsign("VesselCallSign");
    m.text(attr::VESSEL_TYPE, "ServiceType");
    m.year(attr::YEAR_BUILT, "ConstructionCompletedYear");
    m.text(attr::REGISTRY_STATUS, "StatusLookupName");
    m.text(attr::HULL_NUMBER, "ManufacturerHullNumber");
    m.flag("CountryLookupName");
    let us = m.get("CountryLookupName").and_then(crate::countries::alpha3) == Some("USA");
    if let Some(ident) = m.get("Identification") {
        if us {
            // US vessels are identified by their documentation number.
            m.push(
                Draft::new(attr::NATIONAL_REGISTRY, "identifier", ident, Some(ident.trim().to_ascii_uppercase()))
                    .qualified("US")
                    .checked("valid"),
            );
        } else {
            match crate::normalize::imo(ident) {
                Some((n, check)) => m.push(Draft::new(attr::IMO, "identifier", ident, Some(n)).checked(check.as_str())),
                None if super::is_placeholder(ident) => {}
                // Not shaped like an IMO number: keep it, typed as unknown.
                None => m.push(Draft::new(attr::HULL_NUMBER, "identifier", ident, Some(ident.trim().to_uppercase())).qualified("PSIX Identification")),
            }
        }
    }
    m.out
}

#[cfg(test)]
mod tests {
    use super::*;

    pub const RESPONSE: &str = r#"<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><getVesselSummaryXMLStringResponse xmlns="https://cgmix.uscg.mil"><getVesselSummaryXMLStringResult>&lt;NewDataSet&gt;
  &lt;VesselSummary&gt;
    &lt;VesselId&gt;275677&lt;/VesselId&gt;
    &lt;VesselName&gt;EVER GIVEN&lt;/VesselName&gt;
    &lt;VesselCallSign&gt;HPBJ&lt;/VesselCallSign&gt;
    &lt;ServiceType&gt;Freight Ship&lt;/ServiceType&gt;
    &lt;ConstructionCompletedYear&gt;1986&lt;/ConstructionCompletedYear&gt;
    &lt;StatusLookupName&gt;Active&lt;/StatusLookupName&gt;
    &lt;Identification&gt;8320901&lt;/Identification&gt;
    &lt;CountryLookupName&gt;PANAMA&lt;/CountryLookupName&gt;
  &lt;/VesselSummary&gt;
  &lt;VesselSummary&gt;
    &lt;VesselId&gt;1684185&lt;/VesselId&gt;
    &lt;VesselName&gt;NEVER GIVEN&lt;/VesselName&gt;
    &lt;VesselCallSign&gt;N/A&lt;/VesselCallSign&gt;
    &lt;ServiceType&gt;Recreational&lt;/ServiceType&gt;
    &lt;Identification&gt;1317522&lt;/Identification&gt;
    &lt;CountryLookupName&gt;UNITED STATES&lt;/CountryLookupName&gt;
  &lt;/VesselSummary&gt;
  &lt;VesselSummary&gt;
    &lt;VesselId&gt;900001&lt;/VesselId&gt;
    &lt;VesselName&gt;SALT &amp;amp; PEPPER&lt;/VesselName&gt;
    &lt;ServiceType&gt;Towing Vessel&lt;/ServiceType&gt;
    &lt;ConstructionCompletedYear&gt;0&lt;/ConstructionCompletedYear&gt;
    &lt;Identification&gt;1234567&lt;/Identification&gt;
    &lt;CountryLookupName&gt;UNITED STATES&lt;/CountryLookupName&gt;
  &lt;/VesselSummary&gt;
&lt;/NewDataSet&gt;</getVesselSummaryXMLStringResult></getVesselSummaryXMLStringResponse></soap:Body></soap:Envelope>"#;

    fn cfg() -> SourceConfig {
        crate::config::Catalogue::load(None).unwrap().get("uscg_psix").unwrap().0.clone()
    }

    #[test]
    fn reads_soap_responses() {
        let rows = summaries(RESPONSE.as_bytes()).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["VesselName"], "EVER GIVEN");
        assert_eq!(rows[2]["VesselName"], "SALT & PEPPER");
        assert!(summaries(b"<html>Service Unavailable</html>").is_err());
    }

    #[test]
    fn skips_excluded_services_and_types_identification() {
        let parsed = parse(&cfg(), &[RawObject::new("build_year_1986.xml", Bytes::from_static(RESPONSE.as_bytes()))]).unwrap();
        assert_eq!(parsed.records.len(), 2, "the recreational boat is skipped");
        let foreign = map(&parsed.records[0].fields);
        let imo = foreign.iter().find(|d| d.attribute == attr::IMO).unwrap();
        assert_eq!((imo.value_normalized.as_deref(), imo.check_status), (Some("8320901"), Some("valid")));
        assert_eq!(foreign.iter().find(|d| d.attribute == attr::FLAG).unwrap().value_normalized.as_deref(), Some("PAN"));
        assert_eq!(foreign.iter().find(|d| d.attribute == attr::CALLSIGN).unwrap().value_normalized.as_deref(), Some("HPBJ"));

        let us = map(&parsed.records[1].fields);
        assert!(us.iter().all(|d| d.attribute != attr::IMO), "a US documentation number is not an IMO number");
        let nr = us.iter().find(|d| d.attribute == attr::NATIONAL_REGISTRY).unwrap();
        assert_eq!((nr.value_normalized.as_deref(), nr.qualifier.as_deref()), (Some("1234567"), Some("US")));
        assert!(us.iter().all(|d| d.attribute != attr::YEAR_BUILT), "year 0 is unknown");
    }
}
