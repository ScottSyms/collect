//! Tokyo MoU (Asia-Pacific) port state control detentions, from the monthly
//! detention lists APCIS publishes
//! (<https://www.tokyo-mou.org/inspections-detentions/detention-list/>).
//! The Tokyo MoU has no public bulk feed of all inspections; until an
//! authorized one is arranged, this loads every detention, month by month,
//! from January 2000. The list's "Excel" export is an HTML table.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{Datelike, NaiveDate};

use super::{date, double, imo, ro_note, split_deficiency, text, year, Category, Event, EventType, Evidence, Finding, SourceInfo};
use crate::download::{save, Source};

pub const SOURCE: &str = "tokyo_mou";
pub const DEFAULT_URL: &str = "https://apcis.tmou.org/isss/public_apcis.php";
/// The first month APCIS offers.
pub const FIRST_MONTH: (i32, u32) = (2000, 1);

pub const INFO: SourceInfo = SourceInfo {
    source: SOURCE,
    name: "Tokyo MoU detention list",
    regime: "Tokyo MoU",
    evidence_categories: "port_state_control",
    publisher: "Tokyo MOU Secretariat (APCIS, hosted by the Russian ICC on PSC)",
    url: "https://www.tokyo-mou.org/inspections-detentions/detention-list/",
    access: "Public monthly detention lists; no bulk feed of all inspections",
    coverage: "Detentions by Asia-Pacific member authorities, 2000 onward",
    notes: "Detentions only, not every inspection. Findings are the grounds for detention; the list doesn't give the total deficiency count. No native id: event_id is IMO, detention date and place.",
};

/// The cache name of one month's list.
pub fn file_name(year: i32, month: u32) -> String {
    format!("tokyo_mou/detlist-{year:04}-{month:02}.xls")
}

/// Every month from `from` through `to`, inclusive.
pub fn months(from: (i32, u32), to: (i32, u32)) -> Vec<(i32, u32)> {
    let mut out = Vec::new();
    let (mut y, mut m) = from;
    while (y, m) <= to {
        out.push((y, m));
        m += 1;
        if m == 13 {
            y += 1;
            m = 1;
        }
    }
    out
}

/// One month's list file: from `source.local_dir`, or downloaded into scratch.
pub async fn fetch_month(source: &Source, url: &str, year: i32, month: u32) -> Result<PathBuf> {
    let name = file_name(year, month);
    if let Some(dir) = &source.local_dir {
        let path = dir.join(&name);
        anyhow::ensure!(path.is_file(), "{} not found", path.display());
        return Ok(path);
    }
    let path = source.scratch.join(&name);
    std::fs::create_dir_all(path.parent().unwrap())?;
    let form = [
        ("Mode", "SaveDetList".to_string()),
        ("MOU", "TMOU".to_string()),
        ("Auth", String::new()),
        ("Src", "online".to_string()),
        ("Type", "Auth".to_string()),
        ("Month", format!("{month:02}")),
        ("Year", year.to_string()),
        ("SaveFile", "1".to_string()),
    ];
    let full = format!("{url}?Mode=DetList");
    let resp = source
        .http()
        .post(&full)
        .form(&form)
        .send()
        .await
        .with_context(|| format!("requesting {name}"))?;
    save(resp, &full, &name, &path, Instant::now()).await?;
    Ok(path)
}

/// Decodes the handful of HTML entities the lists use.
fn unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        rest = &rest[i..];
        let Some(end) = rest.find(';').filter(|&e| e <= 10) else {
            out.push('&');
            rest = &rest[1..];
            continue;
        };
        let entity = &rest[1..end];
        let decoded = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            "nbsp" => Some(' '),
            e if e.starts_with("#x") || e.starts_with("#X") => u32::from_str_radix(&e[2..], 16).ok().and_then(char::from_u32),
            e if e.starts_with('#') => e[1..].parse().ok().and_then(char::from_u32),
            _ => None,
        };
        match decoded {
            Some(c) => {
                out.push(c);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// A cell's text: `<br>` becomes a newline, other tags go, entities decode.
fn cell_text(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut rest = html;
    while let Some(i) = rest.find('<') {
        out.push_str(&rest[..i]);
        let Some(end) = rest[i..].find('>') else {
            rest = "";
            break;
        };
        let tag = rest[i + 1..i + end].trim().to_ascii_lowercase();
        if tag == "br" || tag == "br/" || tag == "br /" {
            out.push('\n');
        }
        rest = &rest[i + end + 1..];
    }
    out.push_str(rest);
    unescape(&out).trim().to_string()
}

/// Splits `s` at each case-insensitive occurrence of `tag` (e.g. `<tr`),
/// returning what follows each.
fn split_tag<'a>(s: &'a str, tag: &str) -> Vec<&'a str> {
    let lower = s.to_ascii_lowercase();
    let starts: Vec<usize> = lower.match_indices(tag).map(|(i, _)| i).collect();
    starts
        .iter()
        .enumerate()
        .map(|(n, &i)| {
            let body = &s[i..starts.get(n + 1).copied().unwrap_or(s.len())];
            // Skip the rest of the opening tag.
            body.find('>').map(|e| &body[e + 1..]).unwrap_or("")
        })
        .collect()
}

/// The table's rows as cell text, header row included.
pub fn table_rows(html: &str) -> Vec<Vec<String>> {
    split_tag(html, "<tr")
        .into_iter()
        .map(|row| split_tag(row, "<td").into_iter().map(cell_text).collect::<Vec<_>>())
        .filter(|cells| !cells.is_empty())
        .collect()
}

const HEADER: [&str; 13] = [
    "IMO No.",
    "Ship Name",
    "Ship Flag",
    "Year of build",
    "Gross Tonnage",
    "Ship Type",
    "Classification society",
    "Related ROs",
    "Company",
    "Place of detention",
    "Date of detention",
    "Date of release",
    "Nature of deficiencies",
];

/// Member authorities whose names contain a comma, longest first, so
/// "Masan, Korea, Republic of" isn't read as "Republic of".
const COMMA_COUNTRIES: [&str; 3] = ["Korea, Republic of", "Hong Kong, China", "Macao, China"];

/// The country part of "Port, Country".
fn place_country(place: &str) -> Option<String> {
    for c in COMMA_COUNTRIES {
        if place == c || place.ends_with(&format!(", {c}")) {
            return Some(c.to_string());
        }
    }
    place.rsplit_once(',').and_then(|(_, c)| text(c))
}

/// Keys already used, so `event_id`s stay unique across months: the lists
/// have no id of their own, and a few rows share IMO, date and place (mostly
/// rows without an IMO number). Repeats get `#2`, `#3`, ... in the order
/// read. Nothing is dropped.
pub type Seen = std::collections::HashMap<String, u32>;

/// Parses one month's list.
pub fn parse(html: &str, source_file: &str, seen: &mut Seen) -> Result<Evidence> {
    let rows = table_rows(html);
    let mut out = Evidence::default();
    let Some(header) = rows.first() else {
        // An empty month comes back with no table at all.
        return Ok(out);
    };
    let header: Vec<&str> = header.iter().map(|h| h.trim()).collect();
    anyhow::ensure!(
        header == HEADER,
        "{source_file}: unexpected columns {header:?}"
    );
    for cells in &rows[1..] {
        let get = |i: usize| cells.get(i).map(String::as_str).unwrap_or("");
        let imo_text = get(0).trim();
        let detained = date(get(10));
        let released = date(get(11));
        let place = text(get(9));
        let mut key = format!(
            "{imo_text}:{}:{}",
            detained.map(|d| d.format("%Y%m%d").to_string()).unwrap_or_default(),
            place.as_deref().unwrap_or("")
        );
        let n = seen.entry(key.clone()).or_insert(0);
        *n += 1;
        if *n > 1 {
            key = format!("{key}#{n}");
        }
        let event_id = format!("{SOURCE}:{key}");
        let lines: Vec<&str> = get(12).lines().map(str::trim).filter(|l| !l.is_empty()).collect();
        for (n, line) in lines.iter().enumerate() {
            let (code, area, item, note) = split_deficiency(line);
            let (ro_related, responsible_ro) = ro_note(note.as_deref());
            out.findings.push(Finding {
                event_id: event_id.clone(),
                source: SOURCE.into(),
                finding_seq: n as i32 + 1,
                finding_date: detained,
                finding_code: code,
                finding_area: area,
                finding_item: item,
                ground_for_detention: Some(true),
                ro_related,
                responsible_ro,
                description: Some(line.to_string()),
                source_file: source_file.into(),
                ..Default::default()
            });
        }
        out.events.push(Event {
            event_id,
            source: SOURCE.into(),
            evidence_category: Category::PortStateControl.as_str().into(),
            event_type: EventType::Detention.as_str().into(),
            regime: INFO.regime.into(),
            // "Shanghai, China": the detaining authority is the port's country.
            authority: place.as_deref().and_then(place_country),
            source_record_id: key,
            event_date: detained,
            event_end_date: released,
            place_name: place.clone(),
            place_country: place.as_deref().and_then(place_country),
            imo_number: imo(imo_text),
            vessel_name: text(get(1)),
            flag: text(get(2)),
            ship_type: text(get(5)),
            gross_tonnage: double(get(4)).filter(|t| *t > 0.0),
            year_built: year(get(3)),
            company_name: text(get(8)),
            classification_society: text(get(6)),
            detained: Some(true),
            detention_days: match (detained, released) {
                (Some(a), Some(b)) if b >= a => Some((b - a).num_days() as i32),
                _ => None,
            },
            detainable_deficiency_count: Some(lines.len() as i32),
            summary: text(get(7)).map(|ros| format!("Related ROs: {ros}")),
            source_file: source_file.into(),
            ..Default::default()
        });
    }
    Ok(out)
}

/// Reads one month's file (UTF-8, falling back to Latin-1).
pub fn read(path: &Path, source_file: &str, seen: &mut Seen) -> Result<Evidence> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let html = match String::from_utf8(bytes) {
        Ok(s) => s,
        Err(e) => e.into_bytes().iter().map(|&b| b as char).collect(),
    };
    parse(&html, source_file, seen)
}

/// The current month, the default end of a load.
pub fn this_month(today: NaiveDate) -> (i32, u32) {
    (today.year(), today.month())
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIST: &str = "<table border=1><tr><td align='center'><font> IMO No.</font></td><td><font> Ship Name</font></td><td><font> Ship Flag</font></td><td><font> Year of build</font></td><td><font> Gross Tonnage</font></td><td><font> Ship Type</font></td><td><font> Classification society</font></td><td><font> Related ROs</font></td><td><font> Company</font></td><td><font> Place of detention</font></td><td><font> Date of detention</font></td><td><font> Date of release</font></td><td><font> Nature of deficiencies</font></td></tr>\
<tr class=\"tr_drk\"><td align='rigth' valign='top'><font >9291377</font></td><td><font >DG TREASURE</font></td><td><font >Bahamas</font></td><td><font >2004-11-19</font></td><td><font >30704</font></td><td><font >Bulk carrier</font></td><td><font >Indian Register of Shipping (IRS)</font></td><td><font >Indian Register of Shipping (IRS);RINA Services S.p.A. (RINA);</font></td><td><font >OCEAN PIONEER SHIPPING CO LTD</font></td><td><font >Shanghai, China</font></td><td><font >01.09.2026</font></td><td><font >07.09.2026</font></td><td><font >03108 - WATER/WEATHERTIGHT CONDITIONS - Ventilators, air pipes, casings (no responsibility of RO)<br>07101 - FIRE SAFETY - Fire prevention structural integrity (Nippon Kaiji Kyokai (NKK))</font></td></tr>\
<tr><td><font >8405189</font></td><td><font >RONG PING</font></td><td><font >Cambodia</font></td><td><font >1984</font></td><td><font >1236</font></td><td><font >General cargo/multipurpose</font></td><td><font >UNION BUREAU OF SHIPPING</font></td><td><font >Other;</font></td><td><font >SHANDONG &amp; CO.</font></td><td><font >Masan, Korea, Republic of</font></td><td><font >02.03.2005</font></td><td><font ></font></td><td><font >03105 - WATER/WEATHERTIGHT CONDITIONS - Covers (Other)</font></td></tr>\
</table>";

    #[test]
    fn parses_a_month() {
        let ev = parse(LIST, "tokyo_mou/detlist-2026-09.xls", &mut Seen::new()).unwrap();
        assert_eq!(ev.events.len(), 2);
        let e = &ev.events[0];
        assert_eq!(e.event_id, "tokyo_mou:9291377:20260901:Shanghai, China");
        assert_eq!((e.evidence_category.as_str(), e.event_type.as_str()), ("port_state_control", "detention"));
        assert_eq!(e.imo_number, Some(9_291_377));
        assert_eq!(e.authority.as_deref(), Some("China"));
        assert_eq!(e.detention_days, Some(6));
        assert_eq!(e.year_built, Some(2004));
        assert_eq!(e.detainable_deficiency_count, Some(2));

        let open = &ev.events[1];
        assert_eq!(open.event_end_date, None, "not yet released");
        assert_eq!(open.detention_days, None);
        assert_eq!(open.company_name.as_deref(), Some("SHANDONG & CO."));
        assert_eq!(open.place_country.as_deref(), Some("Korea, Republic of"));
        assert_eq!(place_country("Hong Kong, China").as_deref(), Some("Hong Kong, China"));
        assert_eq!(place_country("Port Walcott, WA, Australia").as_deref(), Some("Australia"));
        assert_eq!(open.year_built, Some(1984));

        assert_eq!(ev.findings.len(), 3);
        let f = &ev.findings[1];
        assert_eq!((f.event_id.as_str(), f.finding_seq), (e.event_id.as_str(), 2));
        assert_eq!(f.finding_code.as_deref(), Some("07101"));
        assert_eq!(f.responsible_ro.as_deref(), Some("Nippon Kaiji Kyokai (NKK)"));
        assert_eq!(f.ro_related, Some(true));
        assert_eq!(f.ground_for_detention, Some(true));
    }

    #[test]
    fn repeated_keys_get_suffixes() {
        let mut seen = Seen::new();
        let mut ev = parse(LIST, "a", &mut seen).unwrap();
        let more = parse(LIST, "b", &mut seen).unwrap();
        ev.events.extend(more.events);
        ev.findings.extend(more.findings);
        let ids: Vec<&str> = ev.events.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(ids[2], "tokyo_mou:9291377:20260901:Shanghai, China#2");
        assert_eq!(ids[3], "tokyo_mou:8405189:20050302:Masan, Korea, Republic of#2");
        let ids: std::collections::HashSet<&str> = ids.into_iter().collect();
        assert_eq!(ids.len(), 4);
        let f: Vec<&str> = ev.findings.iter().map(|f| f.event_id.as_str()).collect();
        assert_eq!(&f[3..], [ev.events[2].event_id.as_str(), ev.events[2].event_id.as_str(), ev.events[3].event_id.as_str()]);
        assert_eq!(&f[..3], [ev.events[0].event_id.as_str(), ev.events[0].event_id.as_str(), ev.events[1].event_id.as_str()]);
    }

    #[test]
    fn empty_month_and_bad_header() {
        assert!(parse("<html>no detentions</html>", "x", &mut Seen::new()).unwrap().events.is_empty());
        assert!(parse("<table><tr><td>Something else</td></tr></table>", "x", &mut Seen::new()).is_err());
    }

    #[test]
    fn month_ranges() {
        assert_eq!(months((2025, 11), (2026, 2)), [(2025, 11), (2025, 12), (2026, 1), (2026, 2)]);
        assert!(months((2026, 2), (2026, 1)).is_empty());
        assert_eq!(file_name(2005, 3), "tokyo_mou/detlist-2005-03.xls");
    }

    #[test]
    fn unescapes_entities() {
        assert_eq!(unescape("A &amp; B &#39;C&#x27; &bogus; &"), "A & B 'C' &bogus; &");
    }
}
