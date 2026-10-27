//! Wikidata: ship items with an IMO number (P458) or MMSI (P587) and an
//! image (P18). The image is a Commons file, so its licence comes from
//! Commons as for `fetch commons`; Wikidata only supplies the link from
//! identifier to file, in a few paged SPARQL queries for the whole set.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::commons::Commons;
use crate::identity::{valid_imo, Target, Targets};

pub const SPARQL: &str = "https://query.wikidata.org/sparql";
/// Rows per query; the query service stops queries at 60 s.
pub const PAGE: usize = 5000;

/// One Wikidata item, with its identifiers and image files.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Item {
    /// `Q...`
    pub qid: String,
    pub imos: Vec<String>,
    pub mmsis: Vec<String>,
    /// Commons titles (`File:...`).
    pub files: Vec<String>,
}

impl Item {
    /// The registry vessel this item is, and how: by a valid IMO number
    /// first, then by an MMSI only one vessel has carried.
    pub fn resolve<'a>(&self, targets: &'a Targets) -> Option<(&'a Target, &'static str)> {
        self.imos
            .iter()
            .filter(|i| valid_imo(i))
            .find_map(|i| targets.by_imo(i))
            .map(|t| (t, "wikidata_imo"))
            .or_else(|| self.mmsis.iter().find_map(|m| targets.by_mmsi(m)).map(|t| (t, "wikidata_mmsi")))
    }
}

pub fn query(limit: usize, offset: usize) -> String {
    format!(
        "SELECT ?item ?imo ?mmsi ?image WHERE {{ {{ ?item wdt:P458 ?imo }} UNION {{ ?item wdt:P587 ?mmsi }} ?item wdt:P18 ?image . }} \
         ORDER BY ?item ?imo ?mmsi ?image LIMIT {limit} OFFSET {offset}"
    )
}

/// `%XX` escapes decoded (the query service returns file paths escaped).
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = ((b[i + 1] as char).to_digit(16), (b[i + 2] as char).to_digit(16)) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `http://commons.wikimedia.org/wiki/Special:FilePath/Foo%20bar.jpg` as
/// `File:Foo bar.jpg`.
pub fn file_title(image: &str) -> Option<String> {
    let name = image.rsplit_once("Special:FilePath/")?.1;
    let name = percent_decode(name).replace('_', " ");
    (!name.trim().is_empty()).then(|| format!("File:{}", name.trim()))
}

/// A Commons page URL (`.../wiki/File:Foo_bar.jpg`) as its title.
pub fn title_from_page_url(url: &str) -> Option<String> {
    let name = url.rsplit_once("/wiki/")?.1;
    Some(percent_decode(name).replace('_', " "))
}

/// The bindings of one result page, folded into items. Returns the items
/// and the number of rows read.
pub fn parse(body: &Value, into: &mut BTreeMap<String, Item>) -> usize {
    let rows = body.pointer("/results/bindings").and_then(Value::as_array).cloned().unwrap_or_default();
    let get = |r: &Value, k: &str| r.pointer(&format!("/{k}/value")).and_then(Value::as_str).map(|s| s.trim().to_string());
    for r in &rows {
        let Some(qid) = get(r, "item").and_then(|u| u.rsplit('/').next().map(String::from)) else { continue };
        let item = into.entry(qid.clone()).or_insert_with(|| Item { qid, ..Default::default() });
        if let Some(imo) = get(r, "imo").map(|s| s.trim_start_matches("IMO").trim().to_string()) {
            if !item.imos.contains(&imo) {
                item.imos.push(imo);
            }
        }
        if let Some(m) = get(r, "mmsi") {
            if !item.mmsis.contains(&m) {
                item.mmsis.push(m);
            }
        }
        if let Some(f) = get(r, "image").and_then(|i| file_title(&i)) {
            if !item.files.contains(&f) {
                item.files.push(f);
            }
        }
    }
    rows.len()
}

/// Every item with an IMO number or MMSI and an image, through `commons`'
/// rate-limited client (the query service asks for the same etiquette).
pub async fn items(commons: &mut Commons, endpoint: Option<&str>) -> Result<Vec<Item>> {
    let endpoint = endpoint.unwrap_or(SPARQL);
    let mut found = BTreeMap::new();
    let mut offset = 0;
    loop {
        let q = query(PAGE, offset);
        let body: Value = commons
            .get(endpoint, &[("query", q.as_str()), ("format", "json")])
            .await?
            .json()
            .await
            .context("parsing the Wikidata query service's response")?;
        let n = parse(&body, &mut found);
        println!("  wikidata: {} row(s) at offset {offset}", n);
        if n < PAGE {
            return Ok(found.into_values().collect());
        }
        offset += PAGE;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn decodes_file_paths() {
        assert_eq!(file_title("http://commons.wikimedia.org/wiki/Special:FilePath/ELBE%206523.jpg").as_deref(), Some("File:ELBE 6523.jpg"));
        assert_eq!(
            file_title("http://commons.wikimedia.org/wiki/Special:FilePath/M%C3%BCller%20%282019%29.jpg").as_deref(),
            Some("File:Müller (2019).jpg")
        );
        assert_eq!(title_from_page_url("https://commons.wikimedia.org/wiki/File:M%C3%BCller_(2019).jpg").as_deref(), Some("File:Müller (2019).jpg"));
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("a%2"), "a%2");
    }

    #[test]
    fn folds_rows_into_items() {
        let body: Value = serde_json::from_str(include_str!("../tests/fixtures/wikidata_sparql.json")).unwrap();
        let mut items = BTreeMap::new();
        assert_eq!(parse(&body, &mut items), 5);
        let items: Vec<Item> = items.into_values().collect();
        assert_eq!(items.len(), 3);
        let a = &items[0];
        assert_eq!(a.qid, "Q100144448");
        assert_eq!(a.imos, vec!["9074729"]);
        assert_eq!(a.mmsis, vec!["316001325"]);
        assert_eq!(a.files, vec!["File:ELBE 6523.jpg"]);
        assert_eq!(items[1].files.len(), 2);
    }

    #[test]
    fn resolves_by_imo_then_unique_mmsi() {
        let t = Targets::new(
            vec![
                Target { vessel_id: "a".into(), imos: vec!["9074729".into()], mmsis: vec!["316001325".into()], ..Default::default() },
                Target { vessel_id: "b".into(), mmsis: vec!["316001325".into(), "235000001".into()], ..Default::default() },
            ],
            HashMap::new(),
        );
        let item = |imos: &[&str], mmsis: &[&str]| Item {
            qid: "Q1".into(),
            imos: imos.iter().map(|s| s.to_string()).collect(),
            mmsis: mmsis.iter().map(|s| s.to_string()).collect(),
            files: vec![],
        };
        let (v, basis) = item(&["9074729"], &["235000001"]).resolve(&t).unwrap();
        assert_eq!((v.vessel_id.as_str(), basis), ("a", "wikidata_imo"));
        let (v, basis) = item(&["1234567"], &["235000001"]).resolve(&t).unwrap();
        assert_eq!((v.vessel_id.as_str(), basis), ("b", "wikidata_mmsi"));
        assert!(item(&[], &["316001325"]).resolve(&t).is_none(), "shared MMSI ties to nothing");
        assert!(item(&["9074730"], &[]).resolve(&t).is_none());
    }
}
