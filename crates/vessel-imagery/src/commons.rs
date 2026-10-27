//! Wikimedia Commons: freely licensed ship photos, found through the
//! `Category:IMO <number>` categories Commons keeps for ships. Files sit in
//! the ship's own category (`Category:Foo (ship, 2005)`), which is a member
//! of the IMO category, so the walk goes down a level or two.
//!
//! Wikimedia's API etiquette: a User-Agent with contact details, one request
//! at a time, `maxlag`, and backing off when told to.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

pub const API: &str = "https://commons.wikimedia.org/w/api.php";
const EXTMETADATA: &str = "LicenseShortName|LicenseUrl|Artist|Credit|AttributionRequired|DateTimeOriginal|Copyrighted|UsageTerms";
const RASTER: &[&str] = &["image/jpeg", "image/png", "image/tiff", "image/webp"];

/// One file on Commons, with its licence as Commons records it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CommonsFile {
    pub title: String,
    pub page_url: String,
    pub original_url: String,
    /// A thumbnail at most `thumb_width` wide (the original when smaller).
    pub thumb_url: String,
    pub mime: String,
    pub sha1: Option<String>,
    pub licence_short: Option<String>,
    pub licence_url: Option<String>,
    pub attribution_required: bool,
    pub author: Option<String>,
    pub credit: Option<String>,
    pub captured_at: Option<DateTime<Utc>>,
    pub copyrighted: Option<bool>,
    /// The category the file was found in.
    pub category: String,
}

impl CommonsFile {
    pub fn licence_class(&self) -> &'static str {
        licence_class(self.licence_short.as_deref(), self.copyrighted)
    }
}

/// `public_domain`, `cc0`, `cc_by`, `cc_by_sa`, `other_free` or `unknown`,
/// from Commons' short licence name.
pub fn licence_class(short: Option<&str>, copyrighted: Option<bool>) -> &'static str {
    let Some(s) = short.map(|s| s.trim().to_ascii_lowercase()) else {
        return if copyrighted == Some(false) { "public_domain" } else { "unknown" };
    };
    if s.starts_with("cc0") || s.contains("cc-zero") {
        "cc0"
    } else if s.starts_with("public domain") || s == "pd" || s.starts_with("pd-") || s.starts_with("pd ") {
        "public_domain"
    } else if s.starts_with("cc by-sa") || s.starts_with("cc-by-sa") {
        "cc_by_sa"
    } else if s.starts_with("cc by") || s.starts_with("cc-by") {
        if s.contains("-nc") || s.contains("-nd") || s.contains(" nc") || s.contains(" nd") {
            "unknown"
        } else {
            "cc_by"
        }
    } else if s.starts_with("gfdl") || s.starts_with("fal") || s.starts_with("ogl") || s == "attribution" || s.starts_with("copyrighted free use") {
        "other_free"
    } else if copyrighted == Some(false) {
        "public_domain"
    } else {
        "unknown"
    }
}

/// Text without HTML tags or entities (Commons' `Artist` is often a link).
pub fn strip_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            c if !in_tag => out.push(c),
            _ => {}
        }
    }
    let out = out.replace("&amp;", "&").replace("&nbsp;", " ").replace("&quot;", "\"").replace("&#039;", "'").replace("&lt;", "<").replace("&gt;", ">");
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// A credit cut to its first part: some `Artist` fields carry the whole
/// licence notice after the name.
fn short(s: &str) -> String {
    const MAX: usize = 120;
    let cut = ["This file is licensed", " http", "I'd appreciate"].iter().filter_map(|m| s.find(m)).min().unwrap_or(s.len());
    let s = s[..cut].trim();
    if s.chars().count() <= MAX {
        return s.to_string();
    }
    let t: String = s.chars().take(MAX).collect();
    format!("{}…", t.rsplit_once(' ').map_or(t.as_str(), |(a, _)| a))
}

/// Commons' `DateTimeOriginal` comes as `2019-06-02 14:31:07`, `2019-06-02`,
/// or HTML around either.
fn parse_date(s: &str) -> Option<DateTime<Utc>> {
    let s = strip_html(s);
    let head: String = s.chars().take(19).collect();
    if let Ok(t) = NaiveDateTime::parse_from_str(&head, "%Y-%m-%d %H:%M:%S") {
        return Some(t.and_utc());
    }
    let day: String = s.chars().take(10).collect();
    NaiveDate::parse_from_str(&day, "%Y-%m-%d").ok().map(|d| d.and_hms_opt(0, 0, 0).expect("midnight").and_utc())
}

#[derive(Deserialize)]
struct Page {
    title: String,
    #[serde(default)]
    imageinfo: Vec<Info>,
}

#[derive(Deserialize)]
struct Info {
    url: String,
    descriptionurl: String,
    #[serde(default)]
    thumburl: Option<String>,
    mime: String,
    #[serde(default)]
    sha1: Option<String>,
    #[serde(default)]
    extmetadata: BTreeMap<String, Value>,
}

fn meta(m: &BTreeMap<String, Value>, key: &str) -> Option<String> {
    let v = m.get(key)?.get("value")?;
    let s = match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let s = strip_html(&s);
    (!s.is_empty()).then_some(s)
}

/// The files in one page of an `imageinfo` response.
pub fn parse_files(body: &Value, category: &str) -> Vec<CommonsFile> {
    let pages: Vec<Page> = body
        .pointer("/query/pages")
        .and_then(|p| serde_json::from_value(p.clone()).ok())
        .unwrap_or_default();
    pages
        .into_iter()
        .filter_map(|p| {
            let i = p.imageinfo.into_iter().next()?;
            let m = &i.extmetadata;
            Some(CommonsFile {
                title: p.title,
                page_url: i.descriptionurl,
                thumb_url: i.thumburl.unwrap_or_else(|| i.url.clone()),
                original_url: i.url,
                mime: i.mime,
                sha1: i.sha1,
                licence_short: meta(m, "LicenseShortName"),
                licence_url: meta(m, "LicenseUrl"),
                attribution_required: meta(m, "AttributionRequired").is_none_or(|v| v != "false"),
                author: meta(m, "Artist").map(|a| short(&a)),
                credit: meta(m, "Credit").map(|c| short(&c)),
                captured_at: meta(m, "DateTimeOriginal").and_then(|d| parse_date(&d)),
                copyrighted: meta(m, "Copyrighted").map(|v| v != "False" && v != "false"),
                category: category.to_string(),
            })
        })
        .collect()
}

pub struct Commons {
    http: reqwest::Client,
    api: String,
    delay: Duration,
    thumb_width: u32,
    pub requests: usize,
    pub max_requests: usize,
}

impl Commons {
    /// `user_agent` must name a way to reach the operator (Wikimedia's
    /// User-Agent policy); requests without one are refused here.
    pub fn new(user_agent: &str, api: Option<&str>, delay: Duration, thumb_width: u32, max_requests: usize) -> Result<Self> {
        if !(user_agent.contains('@') || user_agent.contains("http")) {
            bail!("the Commons User-Agent needs contact details (an email address or URL); set --contact");
        }
        let http = reqwest::Client::builder().user_agent(user_agent).timeout(Duration::from_secs(120)).build()?;
        Ok(Commons { http, api: api.unwrap_or(API).to_string(), delay, thumb_width, requests: 0, max_requests })
    }

    /// A GET with Wikimedia's etiquette: the request cap, the pause between
    /// requests, and backing off on 429, 5xx or replication lag.
    pub(crate) async fn get(&mut self, url: &str, query: &[(&str, &str)]) -> Result<reqwest::Response> {
        for attempt in 1..=5u32 {
            if self.requests >= self.max_requests {
                bail!("request cap reached ({}); run again to continue", self.max_requests);
            }
            if self.requests > 0 {
                tokio::time::sleep(self.delay).await;
            }
            self.requests += 1;
            let resp = self.http.get(url).query(query).send().await.with_context(|| format!("requesting {url}"))?;
            let status = resp.status();
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            let lagged = resp.headers().get("x-database-lag").is_some();
            if status.is_success() && !lagged {
                return Ok(resp);
            }
            if !(status.as_u16() == 429 || status.is_server_error() || lagged) {
                bail!("{url}: HTTP {status}");
            }
            let wait = retry_after.unwrap_or(5 * u64::from(attempt)).min(300);
            eprintln!("  commons: {} (HTTP {status}), waiting {wait}s", if lagged { "replication lag" } else { "asked to back off" });
            tokio::time::sleep(Duration::from_secs(wait)).await;
        }
        bail!("{url}: still refused after 5 attempts")
    }

    async fn api(&mut self, params: &[(&str, &str)]) -> Result<Value> {
        let mut q: Vec<(&str, &str)> = vec![("action", "query"), ("format", "json"), ("formatversion", "2"), ("maxlag", "5")];
        q.extend_from_slice(params);
        let api = self.api.clone();
        let v: Value = self.get(&api, &q).await?.json().await.context("parsing Commons response")?;
        if let Some(e) = v.get("error") {
            bail!("Commons API error: {e}");
        }
        Ok(v)
    }

    /// Subcategories of `category` (full titles), continuing as needed.
    async fn subcategories(&mut self, category: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut cont: Option<String> = None;
        loop {
            let mut p = vec![("list", "categorymembers"), ("cmtitle", category), ("cmtype", "subcat"), ("cmlimit", "50")];
            if let Some(c) = cont.as_deref() {
                p.push(("cmcontinue", c));
            }
            let v = self.api(&p).await?;
            for m in v.pointer("/query/categorymembers").and_then(Value::as_array).into_iter().flatten() {
                if let Some(t) = m.get("title").and_then(Value::as_str) {
                    out.push(t.to_string());
                }
            }
            cont = v.pointer("/continue/cmcontinue").and_then(Value::as_str).map(String::from);
            if cont.is_none() || out.len() >= 200 {
                return Ok(out);
            }
        }
    }

    /// Raster files directly in `category`, up to `limit`.
    async fn files_in(&mut self, category: &str, limit: usize) -> Result<Vec<CommonsFile>> {
        let width = self.thumb_width.to_string();
        let mut out = Vec::new();
        let mut cont: Option<String> = None;
        loop {
            let mut p = vec![
                ("generator", "categorymembers"),
                ("gcmtitle", category),
                ("gcmtype", "file"),
                ("gcmlimit", "50"),
                ("prop", "imageinfo"),
                ("iiprop", "url|mime|sha1|extmetadata"),
                ("iiextmetadatafilter", EXTMETADATA),
                ("iiurlwidth", &width),
            ];
            if let Some(c) = cont.as_deref() {
                p.push(("gcmcontinue", c));
            }
            let v = self.api(&p).await?;
            out.extend(parse_files(&v, category).into_iter().filter(|f| RASTER.contains(&f.mime.as_str())));
            cont = v.pointer("/continue/gcmcontinue").and_then(Value::as_str).map(String::from);
            if cont.is_none() || out.len() >= limit {
                out.truncate(limit);
                return Ok(out);
            }
        }
    }

    /// Raster files by title (`File:...`), 50 to a request. Titles Commons
    /// doesn't have are left out.
    pub async fn files_by_title(&mut self, titles: &[String], label: &str) -> Result<Vec<CommonsFile>> {
        let width = self.thumb_width.to_string();
        let mut out = Vec::new();
        for chunk in titles.chunks(50) {
            let joined = chunk.join("|");
            let p = vec![
                ("titles", joined.as_str()),
                ("prop", "imageinfo"),
                ("iiprop", "url|mime|sha1|extmetadata"),
                ("iiextmetadatafilter", EXTMETADATA),
                ("iiurlwidth", &width),
            ];
            let v = self.api(&p).await?;
            out.extend(parse_files(&v, label).into_iter().filter(|f| RASTER.contains(&f.mime.as_str())));
        }
        Ok(out)
    }

    /// Photos filed under `Category:IMO <imo>`, walking at most `depth`
    /// levels of subcategories (but never into another IMO category), up to
    /// `limit` files.
    pub async fn files_for_imo(&mut self, imo: &str, depth: usize, limit: usize) -> Result<Vec<CommonsFile>> {
        let root = format!("Category:IMO {imo}");
        let mut seen_cats = BTreeSet::from([root.clone()]);
        let mut level = vec![root];
        let mut files: Vec<CommonsFile> = Vec::new();
        let mut seen_files = BTreeSet::new();
        for d in 0..=depth {
            let mut next = Vec::new();
            for cat in &level {
                for f in self.files_in(cat, limit - files.len()).await? {
                    if seen_files.insert(f.title.clone()) {
                        files.push(f);
                    }
                }
                if files.len() >= limit {
                    return Ok(files);
                }
                if d < depth {
                    for sub in self.subcategories(cat).await? {
                        if !sub.starts_with("Category:IMO ") && seen_cats.len() < 12 && seen_cats.insert(sub.clone()) {
                            next.push(sub);
                        }
                    }
                }
            }
            level = next;
        }
        Ok(files)
    }

    pub async fn download(&mut self, url: &str) -> Result<Bytes> {
        let resp = self.get(url, &[]).await?;
        resp.bytes().await.with_context(|| format!("downloading {url}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_licences() {
        assert_eq!(licence_class(Some("CC BY-SA 4.0"), Some(true)), "cc_by_sa");
        assert_eq!(licence_class(Some("CC BY 2.0"), Some(true)), "cc_by");
        assert_eq!(licence_class(Some("CC BY-NC 2.0"), Some(true)), "unknown");
        assert_eq!(licence_class(Some("CC0"), Some(false)), "cc0");
        assert_eq!(licence_class(Some("Public domain"), Some(false)), "public_domain");
        assert_eq!(licence_class(Some("PD-USGov"), None), "public_domain");
        assert_eq!(licence_class(Some("GFDL"), Some(true)), "other_free");
        assert_eq!(licence_class(None, Some(false)), "public_domain");
        assert_eq!(licence_class(None, None), "unknown");
        assert_eq!(licence_class(Some("Some custom thing"), Some(true)), "unknown");
    }

    #[test]
    fn strips_html() {
        assert_eq!(strip_html(r#"<a href="//commons.wikimedia.org/wiki/User:X">X &amp; Y</a>"#), "X & Y");
        assert_eq!(short("Kolforn (Kolforn) I'd appreciate if you could mail me"), "Kolforn (Kolforn)");
        assert_eq!(parse_date("2019-06-02 14:31:07").unwrap().to_rfc3339(), "2019-06-02T14:31:07+00:00");
        assert_eq!(parse_date("<span>2019-06-02</span>").unwrap().to_rfc3339(), "2019-06-02T00:00:00+00:00");
    }

    #[test]
    fn parses_imageinfo() {
        let body: Value = serde_json::from_str(include_str!("../tests/fixtures/commons_imageinfo.json")).unwrap();
        let f = parse_files(&body, "Category:Example (ship, 2005)");
        assert_eq!(f.len(), 2);
        assert_eq!(f[0].licence_short.as_deref(), Some("CC BY-SA 4.0"));
        assert_eq!(f[0].licence_class(), "cc_by_sa");
        assert_eq!(f[0].author.as_deref(), Some("Jane Shipspotter"));
        assert!(f[0].thumb_url.contains("1280px"));
        assert!(f[0].attribution_required);
        assert_eq!(f[1].licence_class(), "public_domain");
        assert!(!f[1].attribution_required);
        assert_eq!(f[1].thumb_url, f[1].original_url);
    }
}
