//! IACS Vessels in Class: the weekly CSV the IACS members send to Equasis,
//! published as `EquasisToIACS_<yyyymmdd>_<n>.zip` on the IACS site. One
//! row per vessel per classification society record, `;`-separated:
//!
//! `IMO;SHIP NAME;CLASS;DATE OF SURVEY;DATE OF NEXT SURVEY;DATE OF LATEST STATUS;STATUS;REASON FOR THE STATUS`
//!
//! The name carries the date the society reported it, `TRANQUILLITY(25/07/24)`;
//! a vessel can have several rows (a newer report, a change of class). The
//! file has no record id, so a record is keyed by IMO, society and the
//! name's report date (or the survey date when the name has none). Dates
//! are `yyyymmdd`.

use std::collections::{BTreeMap, HashMap};

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;

use super::{attr, midnight, parse_date, Draft, FetchCtx, Fetched, Mapper, Parsed, Record, Rejected};
use crate::config::SourceConfig;
use crate::fetch::{decode_text, files_in, read_local, unzip, RawObject};

const COLUMNS: [&str; 8] = [
    "IMO",
    "SHIP NAME",
    "CLASS",
    "DATE OF SURVEY",
    "DATE OF NEXT SURVEY",
    "DATE OF LATEST STATUS",
    "STATUS",
    "REASON FOR THE STATUS",
];

/// IACS member codes as the file writes them.
const SOCIETIES: &[(&str, &str)] = &[
    ("ABS", "American Bureau of Shipping"),
    ("BV", "Bureau Veritas"),
    ("CCS", "China Classification Society"),
    ("CRS", "Croatian Register of Shipping"),
    ("IRS", "Indian Register of Shipping"),
    ("KR", "Korean Register"),
    ("LRS", "Lloyd's Register"),
    ("NKK", "Nippon Kaiji Kyokai (ClassNK)"),
    ("NV", "DNV"),
    ("PRS", "Polski Rejestr Statkow"),
    ("RINA", "RINA"),
    ("TLV", "Turk Loydu"),
];

/// The newest `EquasisToIACS_<date>_<n>.zip` linked from `html`, with its
/// date and sequence number.
pub fn newest_link(html: &str) -> Option<(String, NaiveDate, u32)> {
    let html = html.replace("\\u002F", "/").replace("\\/", "/");
    let mut best: Option<(String, NaiveDate, u32)> = None;
    for (i, _) in html.match_indices("EquasisToIACS_") {
        let tail = &html[i + "EquasisToIACS_".len()..];
        let Some(end) = tail.find(".zip") else { continue };
        let stem = &tail[..end];
        let Some((date, seq)) = stem.split_once('_') else { continue };
        let (Ok(date), Ok(seq)) = (NaiveDate::parse_from_str(date, "%Y%m%d"), seq.parse::<u32>()) else {
            continue;
        };
        // Walk back to the start of the URL.
        let start = html[..i].rfind(|c: char| c == '"' || c == '\'' || c.is_whitespace() || c == '(').map_or(0, |p| p + 1);
        let url = &html[start..i + "EquasisToIACS_".len() + end + 4];
        if !url.starts_with("http") {
            continue;
        }
        if best.as_ref().is_none_or(|(_, d, s)| (date, seq) > (*d, *s)) {
            best = Some((url.to_string(), date, seq));
        }
    }
    best
}

fn stem_date(name: &str) -> Option<(NaiveDate, u32)> {
    let s = name.rsplit('/').next()?.strip_prefix("EquasisToIACS_")?;
    let s = s.split('.').next()?;
    let (d, n) = s.split_once('_')?;
    Some((NaiveDate::parse_from_str(d, "%Y%m%d").ok()?, n.parse().ok()?))
}

pub async fn fetch(cfg: &SourceConfig, ctx: &FetchCtx<'_>) -> Result<Fetched> {
    let object = match ctx.local_dir {
        Some(dir) => {
            let files = files_in(dir, &["zip", "csv"])?;
            // The newest weekly file, by the date in its name.
            let file = files
                .iter()
                .max_by_key(|p| stem_date(&p.to_string_lossy()))
                .with_context(|| format!("no .zip or .csv in {}", dir.display()))?;
            read_local(file)?
        }
        None => {
            let page = ctx.http.get(&cfg.source_url, "vessels-in-class.html").await?;
            let html = decode_text(&page.bytes);
            let Some((url, _, _)) = newest_link(&html) else {
                bail!("no EquasisToIACS_<date>_<n>.zip link on {}; has the page changed?", cfg.source_url);
            };
            let name = url.rsplit('/').next().unwrap_or("iacs.zip").to_string();
            ctx.http.get(&url, &name).await?
        }
    };
    let dated = stem_date(&object.name);
    Ok(Fetched {
        source_version: dated.map(|(d, n)| format!("{}_{n}", d.format("%Y%m%d"))),
        source_published_at: dated.map(|(d, _)| midnight(d)),
        objects: vec![object],
        complete: true,
        warnings: Vec::new(),
    })
}

/// `TRANQUILLITY(25/07/24)` -> (`TRANQUILLITY`, 2024-07-25).
pub fn split_name(s: &str) -> (&str, Option<NaiveDate>) {
    let t = s.trim();
    if let Some(open) = t.rfind('(') {
        if t.ends_with(')') {
            if let Some(d) = parse_date(&t[open + 1..t.len() - 1]) {
                return (t[..open].trim(), Some(d));
            }
        }
    }
    (t, None)
}

pub fn parse(objects: &[RawObject]) -> Result<Parsed> {
    let object = objects.first().context("no file fetched")?;
    let text = if object.name.to_ascii_lowercase().ends_with(".zip") {
        let entries = unzip(&object.bytes, ".csv")?;
        let [(_, bytes)] = entries.as_slice() else {
            bail!("expected one .csv in {}, found {}", object.name, entries.len());
        };
        decode_text(bytes)
    } else {
        decode_text(&object.bytes)
    };
    let mut rdr = csv::ReaderBuilder::new().delimiter(b';').flexible(true).from_reader(text.as_bytes());
    let header: Vec<String> = rdr.headers()?.iter().map(|h| h.trim().to_string()).collect();
    for want in COLUMNS {
        anyhow::ensure!(header.iter().any(|h| h == want), "column '{want}' is missing; has the layout changed?");
    }

    let mut parsed = Parsed::default();
    let mut seen: HashMap<String, usize> = HashMap::new();
    for (i, row) in rdr.records().enumerate() {
        let line = i + 2;
        let row = match row {
            Ok(r) => r,
            Err(e) => {
                parsed.rejected.push(Rejected { location: format!("line {line}"), raw: None, reasons: vec![e.to_string()], object: 0 });
                continue;
            }
        };
        let fields: BTreeMap<String, String> = header
            .iter()
            .zip(row.iter())
            .filter(|(_, v)| !v.trim().is_empty())
            .map(|(h, v)| (h.clone(), v.trim().to_string()))
            .collect();
        let Some(imo) = fields.get("IMO").cloned() else {
            parsed.rejected.push(Rejected {
                location: format!("line {line}"),
                raw: serde_json::to_string(&fields).ok(),
                reasons: vec!["no IMO number".into()],
                object: 0,
            });
            continue;
        };
        let reported = fields.get("SHIP NAME").and_then(|n| split_name(n).1);
        let when = reported
            .map(|d| d.format("%Y%m%d").to_string())
            .or_else(|| fields.get("DATE OF SURVEY").cloned())
            .unwrap_or_else(|| "-".into());
        let class = fields.get("CLASS").cloned().unwrap_or_else(|| "-".into());
        let mut id = format!("{imo}:{class}:{when}");
        let n = seen.entry(id.clone()).or_default();
        *n += 1;
        if *n > 1 {
            id = format!("{id}#{n}");
        }
        parsed.records.push(Record { source_record_id: id, subject_id: imo, fields, object: 0, published_at: reported.map(midnight) });
    }
    Ok(parsed)
}

pub fn map(fields: &BTreeMap<String, String>) -> Vec<Draft> {
    let mut m = Mapper::new(fields);
    m.imo("IMO");
    if let Some(raw) = m.get("SHIP NAME") {
        let (name, reported) = split_name(raw);
        if let Some(t) = crate::normalize::name_text(name) {
            // The `(dd/mm/yy)` suffix is IACS's report date, not part of the
            // name: it goes in the qualifier (bronze keeps the raw value).
            let mut d = Draft::new(attr::NAME, "string", &t, crate::normalize::name_key(&t));
            if let Some(r) = reported {
                d = d.qualified(format!("reported {r}"));
            }
            m.push(d);
        }
    }
    if let Some(code) = m.get("CLASS") {
        let code = code.trim().to_ascii_uppercase();
        let full = SOCIETIES.iter().find(|(c, _)| *c == code).map(|(_, n)| *n);
        let mut d = Draft::new(attr::CLASS_SOCIETY, "code", &code, Some(code.clone()));
        d = match full {
            Some(n) => d.qualified(n),
            None => d.checked("unrecognised"),
        };
        m.push(d);
    }
    m.date(attr::LAST_SURVEY, "DATE OF SURVEY");
    m.date(attr::NEXT_SURVEY_DUE, "DATE OF NEXT SURVEY");
    let status_from = m.get("DATE OF LATEST STATUS").and_then(parse_date).map(midnight);
    if let Some(s) = m.get("STATUS") {
        m.push(Draft::new(attr::CLASS_STATUS, "code", s, Some(s.trim().to_uppercase())).from(status_from));
    }
    if let Some(s) = m.get("REASON FOR THE STATUS") {
        m.push(Draft::new(attr::CLASS_STATUS_REASON, "string", s, Some(s.trim().to_uppercase())).from(status_from));
    }
    m.out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    pub const SAMPLE: &str = "\
IMO;SHIP NAME;CLASS;DATE OF SURVEY;DATE OF NEXT SURVEY;DATE OF LATEST STATUS;STATUS;REASON FOR THE STATUS
1000021;;LRS;20200803;20250724;19960202;Delivered;
1002093;TRANQUILLITY(25/07/24);LRS;20191110;20241109;20191110;Delivered;
1002093;TRANQUILLITY(11/10/26);LRS;20240726;20290725;20191110;Delivered;
9321483;ATLANTIC STAR(11/10/26);NV;20250101;20300101;20260901;Withdrawn;Sold for recycling (scrap)
;NO IMO(11/10/26);BV;20250101;;;;
";

    #[test]
    fn finds_the_newest_weekly_file() {
        let html = r#"..."https://iacs.s3.af-south-1.amazonaws.com/wp-content/uploads/2026/10/09114507/EquasisToIACS_20261009_980.zip" x
          <a href="https://iacs.s3.af-south-1.amazonaws.com/wp-content/uploads/2026/10/16155113/EquasisToIACS_20261016_981.zip">"#;
        let (url, d, n) = newest_link(html).unwrap();
        assert!(url.ends_with("/16155113/EquasisToIACS_20261016_981.zip"), "{url}");
        assert!(url.starts_with("https://iacs.s3"));
        assert_eq!((d, n), (NaiveDate::from_ymd_opt(2026, 10, 16).unwrap(), 981));
        assert_eq!(newest_link("nothing here"), None);
    }

    #[test]
    fn parses_rows_and_keys_records() {
        let parsed = parse(&[RawObject::new("EquasisToIACS_20261016_981.csv", Bytes::from_static(SAMPLE.as_bytes()))]).unwrap();
        assert_eq!(parsed.records.len(), 4);
        assert_eq!(parsed.rejected.len(), 1);
        let ids: Vec<&str> = parsed.records.iter().map(|r| r.source_record_id.as_str()).collect();
        assert_eq!(ids, ["1000021:LRS:20200803", "1002093:LRS:20240725", "1002093:LRS:20261011", "9321483:NV:20261011"]);
        assert_eq!(parsed.records[1].subject_id, "1002093");
        assert_eq!(parsed.records[1].published_at, Some(midnight(NaiveDate::from_ymd_opt(2024, 7, 25).unwrap())));

        let drafts = map(&parsed.records[3].fields);
        let get = |a: &str| drafts.iter().find(|d| d.attribute == a).unwrap();
        assert_eq!(get(attr::NAME).value_normalized.as_deref(), Some("ATLANTIC STAR"));
        assert_eq!(get(attr::NAME).value_original.as_deref(), Some("ATLANTIC STAR"));
        assert_eq!(get(attr::NAME).qualifier.as_deref(), Some("reported 2026-10-11"));
        assert_eq!(get(attr::CLASS_SOCIETY).qualifier.as_deref(), Some("DNV"));
        assert_eq!(get(attr::CLASS_STATUS).value_normalized.as_deref(), Some("WITHDRAWN"));
        assert!(get(attr::CLASS_STATUS).valid_from.is_some());
        assert_eq!(get(attr::CLASS_STATUS_REASON).value_normalized.as_deref(), Some("SOLD FOR RECYCLING (SCRAP)"));
    }

    #[test]
    fn reads_the_published_zip() {
        use std::io::Write;
        let mut buf = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            zip.start_file("EquasisToIACS_20261016_981.csv", zip::write::SimpleFileOptions::default()).unwrap();
            zip.write_all(SAMPLE.as_bytes()).unwrap();
            zip.finish().unwrap();
        }
        let parsed = parse(&[RawObject::new("EquasisToIACS_20261016_981.zip", Bytes::from(buf))]).unwrap();
        assert_eq!(parsed.records.len(), 4);
    }

    #[test]
    fn refuses_a_changed_layout() {
        let e = parse(&[RawObject::new("x.csv", Bytes::from_static(b"IMO;NAME\n1;x\n"))]).unwrap_err();
        assert!(format!("{e:#}").contains("layout"));
    }
}
