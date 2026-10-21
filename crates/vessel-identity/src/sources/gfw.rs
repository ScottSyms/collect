//! Global Fishing Watch vessel identity, through the Vessels API
//! (`/v3/vessels/search`, dataset `public-global-vessel-identity`). Each
//! result is one vessel as GFW resolved it: the registry records GFW holds
//! for it (`registryInfo`, compiled from over 40 public registries), the AIS
//! identities it transmitted (`selfReportedInfo`), owners and fishing
//! authorizations.
//!
//! The API has no bulk export, and its search pages can't be followed
//! reliably (the `since` cursor repeats and a repeated request comes back
//! cached), so the collector asks about vessels the registry already knows:
//! the IMO numbers and MMSIs other sources gave it, OR-ed together in
//! batches of `batch_size`, each batch one page of at most 50 vessels (split
//! in half until its matches fit). Each finished batch is kept in the
//! scratch directory, so an interrupted fetch resumes, and the batches are
//! archived together as one zip. GFW's usage limits are enforced across
//! runs by a request ledger ([`Usage`]): a run stops before the daily or
//! monthly cap, or while a 429 block lasts, and is then incomplete; the
//! next run resumes from the finished batches.
//!
//! The subject is GFW's registry vessel id when it has registry records, or
//! else its AIS vessel id. Only registry records may claim an IMO number;
//! the IMO a ship broadcasts is kept in the record but asserts nothing. MMSIs
//! carry the period GFW saw them transmitted, so AIS positions can be joined
//! to the vessel that used the MMSI at the time; the newest one is open-ended.
//!
//! Access needs a personal API token (`authentication = "env:<VAR>"`). GFW's
//! terms are CC BY-NC 4.0 plus its API Terms of Use: noncommercial use, and
//! "Powered by Global Fishing Watch" attribution on anything built from it.

use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde_json::Value;

use super::{attr, Draft, FetchCtx, Fetched, Mapper, Parsed, Record, Rejected};
use crate::config::SourceConfig;
use crate::fetch::{files_in, read_local, unzip, RawObject};
use crate::normalize;

const DATASET: &str = "public-global-vessel-identity:latest";
const PAGE_SIZE: u64 = 50;
/// GFW's published limits per user, across all of that user's tokens.
const GFW_DAILY_LIMIT: u64 = 50_000;
const GFW_MONTHLY_LIMIT: u64 = 1_500_000;

const SELF_REPORTED: &str = "selfReportedInfo";
const REGISTRY: &str = "registryInfo";
const OWNERS: &str = "registryOwners";
const AUTHORIZATIONS: &str = "registryPublicAuthorizations";
const COMBINED: &str = "combinedSourcesInfo";

/// One lookup: a `where` term for an identifier the registry already holds.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lookup {
    pub kind: String,
    pub value: String,
}

impl Lookup {
    /// The `where` term, or `None` for a kind GFW can't be asked about or a
    /// value that isn't a plain number (nothing is quoted into the query).
    fn term(&self) -> Option<String> {
        if self.value.is_empty() || !self.value.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        match self.kind.as_str() {
            "IMO" => Some(format!("imo = '{}'", self.value)),
            "MMSI" => Some(format!("ssvid = '{}'", self.value)),
            _ => None,
        }
    }
}

/// The `where` clauses a fetch sends: the lookup terms in batches of
/// `batch_size`, OR-ed together, then any raw `queries`.
fn batches(cfg: &SourceConfig, lookups: &[Lookup]) -> Vec<Vec<String>> {
    let size = cfg.option_u64("batch_size", 20).clamp(1, 50) as usize;
    let mut terms: Vec<String> = lookups.iter().filter_map(Lookup::term).collect();
    terms.sort();
    terms.dedup();
    let mut out: Vec<Vec<String>> = terms.chunks(size).map(|c| c.to_vec()).collect();
    out.extend(cfg.option_strings("queries").into_iter().map(|q| vec![q]));
    match cfg.option_u64("max_batches", 0) {
        0 => out,
        n => out.into_iter().take(n as usize).collect(),
    }
}

fn clause(terms: &[String]) -> String {
    terms.join(" OR ")
}

/// A file name for a batch, from its terms.
fn batch_file(terms: &[String]) -> String {
    let digest = hex::encode(&<sha2::Sha256 as sha2::Digest>::digest(clause(terms).as_bytes())[..8]);
    format!("batch_{digest}.json")
}

fn token(cfg: &SourceConfig) -> Result<String> {
    let Some(var) = cfg.authentication.strip_prefix("env:") else {
        bail!("{} needs an API token: set authentication = \"env:<VAR>\"", cfg.source_id);
    };
    std::env::var(var).ok().filter(|t| !t.trim().is_empty()).with_context(|| format!("{var} is not set (a GFW API token)"))
}

fn fresh(path: &Path, max_age: Duration) -> bool {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .is_ok_and(|t| SystemTime::now().duration_since(t).unwrap_or_default() < max_age)
}

/// GFW's limits are per user across all tokens and runs: 50,000 requests a
/// day and 1,500,000 a month, with a 24-hour block (30 days for the monthly
/// limit) once exceeded. Every request this binary sends, retries included,
/// is counted in a ledger that outlives the scratch directory, and a run
/// sends nothing once a cap is reached or while GFW has blocked the token.
#[derive(Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Usage {
    /// UTC day (`yyyy-mm-dd`) and the requests sent on it.
    pub day: String,
    pub day_requests: u64,
    /// UTC month (`yyyy-mm`) and the requests sent in it.
    pub month: String,
    pub month_requests: u64,
    /// Set after a 429: nothing is sent before this.
    pub blocked_until: Option<DateTime<Utc>>,
}

pub struct Caps {
    pub daily: u64,
    pub monthly: u64,
}

impl Usage {
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(b) => serde_json::from_slice(&b).with_context(|| format!("reading the GFW usage ledger {}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Usage::default()),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("part");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(&tmp, path).with_context(|| format!("writing {}", path.display()))
    }

    fn roll(&mut self, now: DateTime<Utc>) {
        let (day, month) = (now.format("%Y-%m-%d").to_string(), now.format("%Y-%m").to_string());
        if self.day != day {
            self.day = day;
            self.day_requests = 0;
        }
        if self.month != month {
            self.month = month;
            self.month_requests = 0;
        }
    }

    /// Why no request may be sent now, if one may not.
    pub fn refusal(&mut self, now: DateTime<Utc>, caps: &Caps) -> Option<String> {
        self.roll(now);
        if let Some(t) = self.blocked_until.filter(|t| *t > now) {
            return Some(format!("GFW refused with 429; waiting until {}", t.format("%Y-%m-%d %H:%M UTC")));
        }
        if self.day_requests >= caps.daily {
            return Some(format!("daily cap reached ({} of {} requests on {})", self.day_requests, caps.daily, self.day));
        }
        if self.month_requests >= caps.monthly {
            return Some(format!("monthly cap reached ({} of {} requests in {})", self.month_requests, caps.monthly, self.month));
        }
        None
    }

    /// GFW's own count, from its `x-ratelimit-*-current-usage` headers, when
    /// it is higher than ours (other use of the account). A cached response
    /// carries an old count, so the higher of the two is kept.
    pub fn sync(&mut self, headers: &reqwest::header::HeaderMap) {
        let num = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).and_then(|v| v.trim().parse::<u64>().ok());
        if let Some(n) = num("x-ratelimit-daily-current-usage") {
            self.day_requests = self.day_requests.max(n);
        }
        if let Some(n) = num("x-ratelimit-monthly-current-usage") {
            self.month_requests = self.month_requests.max(n);
        }
    }

    pub fn record(&mut self, now: DateTime<Utc>) {
        self.roll(now);
        self.day_requests += 1;
        self.month_requests += 1;
    }

    /// GFW blocks for 24 hours after the daily limit and 30 days after the
    /// monthly one; which it was isn't said, so the monthly count decides.
    pub fn blocked(&mut self, now: DateTime<Utc>, caps: &Caps) {
        let days = if self.month_requests >= caps.monthly { 30 } else { 1 };
        self.blocked_until = Some(now + chrono::Duration::days(days));
    }
}

/// Where the ledger lives: `usage_file`, or a state directory under `$HOME`
/// (the scratch directory may be a temporary one), or else the scratch.
fn ledger_path(cfg: &SourceConfig, scratch: &Path) -> std::path::PathBuf {
    if let Some(p) = cfg.options.get("usage_file").and_then(|v| v.as_str()) {
        return p.into();
    }
    match std::env::var_os("HOME") {
        Some(home) => Path::new(&home).join(".local/state/vessel-identity").join(format!("{}_usage.json", cfg.source_id)),
        None => scratch.join(&cfg.source_id).join("usage.json"),
    }
}

enum Stop {
    /// A usage cap or budget is reached, or GFW refused with 429: nothing
    /// more is sent this run.
    Budget(String),
    Failed(String),
}

/// Sends searches within the usage limits, counting each request.
struct Client<'a> {
    ctx: &'a FetchCtx<'a>,
    url: String,
    token: String,
    usage: Usage,
    ledger: std::path::PathBuf,
    caps: Caps,
    budget: u64,
    delay: Duration,
    attempts: u32,
    requests: u64,
}

impl Client<'_> {
    /// One page (up to 50 vessels) for `where`: the entries and GFW's total.
    async fn search(&mut self, clause: &str) -> Result<(Vec<Value>, u64), Stop> {
        if self.budget == 0 {
            return Err(Stop::Budget("this run's request budget (max_requests) is spent".into()));
        }
        self.budget -= 1;
        let query: Vec<(&str, String)> = vec![
            ("datasets[0]", DATASET.into()),
            ("where", clause.to_string()),
            ("limit", PAGE_SIZE.to_string()),
            ("includes[0]", "OWNERSHIP".into()),
            ("includes[1]", "AUTHORIZATIONS".into()),
        ];
        let mut last = String::new();
        for attempt in 1..=self.attempts {
            if self.requests > 0 {
                tokio::time::sleep(self.delay).await;
            }
            if attempt > 1 {
                tokio::time::sleep(Duration::from_secs((2u64 << attempt.min(6)).min(120))).await;
            }
            if let Some(why) = self.usage.refusal(Utc::now(), &self.caps) {
                return Err(Stop::Budget(why));
            }
            // Counted before it is sent: a request that fails still counts.
            self.usage.record(Utc::now());
            self.usage.save(&self.ledger).map_err(|e| Stop::Failed(format!("{e:#}")))?;
            self.requests += 1;
            let resp = self.ctx.http.client().get(&self.url).bearer_auth(&self.token).query(&query).send().await;
            match resp {
                Ok(r) if r.status().as_u16() == 429 => {
                    self.usage.blocked(Utc::now(), &self.caps);
                    self.usage.save(&self.ledger).map_err(|e| Stop::Failed(format!("{e:#}")))?;
                    let until = self.usage.blocked_until.map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string()).unwrap_or_default();
                    return Err(Stop::Budget(format!("HTTP 429: GFW's rate limit is reached; nothing more is sent before {until}")));
                }
                Ok(r) if r.status().is_success() => {
                    self.usage.sync(r.headers());
                    self.usage.save(&self.ledger).map_err(|e| Stop::Failed(format!("{e:#}")))?;
                    match r.json::<Value>().await {
                        Ok(body) => {
                            let Some(entries) = body.get("entries").and_then(Value::as_array) else {
                                return Err(Stop::Failed("the response has no `entries`; has the API changed?".into()));
                            };
                            let total = body.get("total").and_then(Value::as_u64).unwrap_or(entries.len() as u64);
                            return Ok((entries.clone(), total));
                        }
                        Err(e) => last = format!("reading the response: {e}"),
                    }
                }
                Ok(r) if r.status().is_client_error() => {
                    let status = r.status();
                    let text: String = r.text().await.unwrap_or_default().chars().take(300).collect();
                    return Err(Stop::Failed(format!("HTTP {status}: {text}")));
                }
                Ok(r) => last = format!("HTTP {}", r.status()),
                Err(e) => last = e.to_string(),
            }
        }
        Err(Stop::Failed(last))
    }

    /// Every vessel a batch matches. GFW's search pages can't be followed
    /// reliably (its cursor repeats and repeated requests come back cached),
    /// so a batch whose matches don't fit on one page is split in half until
    /// they do; a single term that still doesn't fit keeps its first page.
    async fn batch(&mut self, terms: &[String], warnings: &mut Vec<String>) -> Result<Vec<Value>, Stop> {
        let mut out = Vec::new();
        let mut queue: Vec<Vec<String>> = vec![terms.to_vec()];
        while let Some(part) = queue.pop() {
            let (entries, total) = self.search(&clause(&part)).await?;
            if total as usize <= entries.len() {
                out.extend(entries);
            } else if part.len() > 1 {
                let (a, b) = part.split_at(part.len() / 2);
                queue.push(b.to_vec());
                queue.push(a.to_vec());
            } else {
                warnings.push(format!("{}: {total} matches, kept the first {}", part[0], entries.len()));
                out.extend(entries);
            }
        }
        Ok(out)
    }
}

pub async fn fetch(cfg: &SourceConfig, ctx: &FetchCtx<'_>) -> Result<Fetched> {
    if let Some(dir) = ctx.local_dir {
        let objects: Vec<RawObject> = files_in(dir, &["zip", "json"])?.iter().map(|f| read_local(f)).collect::<Result<_>>()?;
        anyhow::ensure!(!objects.is_empty(), "no .zip or .json in {}", dir.display());
        return Ok(Fetched { objects, source_version: None, source_published_at: None, complete: true, warnings: Vec::new() });
    }

    let all = batches(cfg, ctx.lookups);
    anyhow::ensure!(
        !all.is_empty(),
        "{}: nothing to look up; the registry has no IMO numbers or MMSIs from other sources yet, and there are no `queries`",
        cfg.source_id
    );
    let dir = ctx.scratch.join(&cfg.source_id).join("batches");
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let max_age = Duration::from_secs(3600 * cfg.option_u64("batch_max_age_hours", 72));
    let ledger = ledger_path(cfg, ctx.scratch);
    let mut client = Client {
        ctx,
        url: format!("{}/vessels/search", cfg.source_url.trim_end_matches('/')),
        token: token(cfg)?,
        usage: Usage::load(&ledger)?,
        ledger,
        // Caps never above GFW's published limits.
        caps: Caps {
            daily: cfg.option_u64("daily_request_cap", 45_000).min(GFW_DAILY_LIMIT),
            monthly: cfg.option_u64("monthly_request_cap", 1_350_000).min(GFW_MONTHLY_LIMIT),
        },
        budget: cfg.option_u64("max_requests", 40_000),
        delay: Duration::from_millis(cfg.option_u64("request_delay_ms", 250)),
        attempts: cfg.option_u64("max_attempts", 5) as u32,
        requests: 0,
    };
    if let Some(why) = client.usage.refusal(Utc::now(), &client.caps) {
        bail!("not calling GFW: {why} (ledger {})", client.ledger.display());
    }
    eprintln!("  gfw: {} identifiers to look up in {} batches", ctx.lookups.len(), all.len());

    let mut warnings = Vec::new();
    let (mut done, mut reused, mut failed) = (0usize, 0usize, 0usize);
    let mut stopped: Option<String> = None;
    for (i, terms) in all.iter().enumerate() {
        let path = dir.join(batch_file(terms));
        if fresh(&path, max_age) {
            reused += 1;
            continue;
        }
        if stopped.is_some() {
            failed += 1;
            continue;
        }
        match client.batch(terms, &mut warnings).await {
            Ok(entries) => {
                let doc = serde_json::json!({ "where": clause(terms), "dataset": DATASET, "entries": entries });
                let tmp = path.with_extension("part");
                std::fs::write(&tmp, serde_json::to_vec(&doc)?)?;
                std::fs::rename(&tmp, &path)?;
                done += 1;
                if (i + 1) % 100 == 0 {
                    eprintln!("  gfw: {}/{} batches, {} requests", i + 1, all.len(), client.requests);
                }
            }
            Err(Stop::Budget(why)) => {
                eprintln!("  gfw: stopping: {why}");
                stopped = Some(why);
                failed += 1;
            }
            Err(Stop::Failed(why)) => {
                eprintln!("  gfw: batch {} failed: {why}", i + 1);
                failed += 1;
            }
        }
    }
    let u = &client.usage;
    eprintln!(
        "  gfw: {done} batches fetched, {reused} reused, {failed} not fetched; {} requests this run, {} today, {} this month (caps {} and {}; ledger {})",
        client.requests,
        u.day_requests,
        u.month_requests,
        client.caps.daily,
        client.caps.monthly,
        client.ledger.display()
    );
    if done + reused == 0 {
        bail!("no GFW batch fetched ({})", stopped.as_deref().unwrap_or("see above"));
    }

    // One zip, entries in a fixed order with a fixed timestamp, so the same
    // responses always give the same bytes.
    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let opts = zip::write::SimpleFileOptions::default().last_modified_time(zip::DateTime::default());
        for terms in &all {
            let name = batch_file(terms);
            if let Ok(bytes) = std::fs::read(dir.join(&name)) {
                zip.start_file(name, opts)?;
                zip.write_all(&bytes)?;
            }
        }
        zip.finish()?;
    }
    if failed > 0 {
        warnings.push(format!("incomplete: {failed} of {} batches not fetched; the next run resumes", all.len()));
    }
    Ok(Fetched {
        objects: vec![RawObject::new(format!("gfw_vessels_{}.zip", Utc::now().format("%Y%m%d")), Bytes::from(buf))],
        source_version: Some(format!("{DATASET}, {} of {} batches", done + reused, all.len())),
        source_published_at: None,
        complete: failed == 0,
        warnings,
    })
}

/// A JSON scalar as text; arrays of scalars joined with `,`.
fn scalar(v: &Value) -> Option<String> {
    let s = match v {
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Array(a) => {
            let parts: Vec<String> = a.iter().filter_map(|x| match x {
                Value::String(_) | Value::Number(_) | Value::Bool(_) => scalar(x),
                Value::Object(o) => o.get("name").and_then(scalar),
                _ => None,
            }).collect();
            parts.join(",")
        }
        _ => return None,
    };
    (!s.is_empty()).then_some(s)
}

fn sort_key(o: &Value, keys: &[&str]) -> Vec<String> {
    keys.iter().map(|k| o.get(*k).and_then(scalar).unwrap_or_default()).collect()
}

/// An entry as flat fields, `<section>.<i>.<field>`, with each section's
/// items in a stable order so an unchanged vessel gives unchanged fields.
pub fn flatten(entry: &Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(d) = entry.get("dataset").and_then(scalar) {
        out.insert("dataset".into(), d);
    }
    let sections: [(&str, &[&str]); 5] = [
        (REGISTRY, &["transmissionDateFrom", "vesselInfoReference", "ssvid"]),
        (SELF_REPORTED, &["transmissionDateFrom", "id", "ssvid"]),
        (OWNERS, &["dateFrom", "name", "ssvid"]),
        (AUTHORIZATIONS, &["dateFrom", "sourceCode", "ssvid"]),
        (COMBINED, &["vesselId"]),
    ];
    for (section, keys) in sections {
        let Some(items) = entry.get(section).and_then(Value::as_array) else { continue };
        let mut items: Vec<&Value> = items.iter().filter(|v| v.is_object()).collect();
        items.sort_by_cached_key(|o| sort_key(o, keys));
        for (i, item) in items.iter().enumerate() {
            let Some(obj) = item.as_object() else { continue };
            for (k, v) in obj {
                if k == "extraFields" {
                    // Registry extras: the first item's scalars.
                    if let Some(x) = v.as_array().and_then(|a| a.first()).and_then(Value::as_object) {
                        for (ek, ev) in x {
                            if let Some(s) = scalar(ev) {
                                out.insert(format!("{section}.{i}.{ek}"), s);
                            }
                        }
                    }
                } else if let Some(s) = scalar(v) {
                    out.insert(format!("{section}.{i}.{k}"), s);
                }
            }
        }
    }
    out
}

/// The subject: GFW's registry vessel id, or else its AIS vessel id.
fn subject_of(fields: &BTreeMap<String, String>) -> Option<String> {
    let ids = |section: &str| {
        fields
            .iter()
            .filter(|(k, _)| k.starts_with(&format!("{section}.")) && k.ends_with(".id"))
            .map(|(_, v)| v)
            .min()
    };
    ids(REGISTRY).map(|id| format!("registry:{id}")).or_else(|| ids(SELF_REPORTED).map(|id| format!("ais:{id}")))
}

pub fn parse(objects: &[RawObject]) -> Result<Parsed> {
    let mut parsed = Parsed::default();
    let mut seen = HashSet::new();
    let mut repeats = 0usize;
    for (oi, o) in objects.iter().enumerate() {
        let docs: Vec<(String, Vec<u8>)> = if o.name.to_ascii_lowercase().ends_with(".zip") {
            unzip(&o.bytes, ".json")?
        } else {
            vec![(o.name.clone(), o.bytes.to_vec())]
        };
        for (name, bytes) in docs {
            let doc: Value = serde_json::from_slice(&bytes).with_context(|| format!("parsing {name}"))?;
            let entries = doc
                .get("entries")
                .and_then(Value::as_array)
                .with_context(|| format!("{name}: no `entries`; has the layout changed?"))?;
            for (i, entry) in entries.iter().enumerate() {
                let fields = flatten(entry);
                let Some(subject) = subject_of(&fields) else {
                    parsed.rejected.push(Rejected {
                        location: format!("{name} entry {}", i + 1),
                        raw: serde_json::to_string(entry).ok(),
                        reasons: vec!["no registry or AIS vessel id".into()],
                        object: oi,
                    });
                    continue;
                };
                // A vessel matching two searches (two flags) is kept once.
                if !seen.insert(subject.clone()) {
                    repeats += 1;
                    continue;
                }
                parsed.records.push(Record { source_record_id: subject.clone(), subject_id: subject, fields, object: oi, published_at: None });
            }
        }
    }
    if repeats > 0 {
        parsed.warnings.push(format!("{repeats} vessels appeared in more than one search"));
    }
    Ok(parsed)
}

fn timestamp(s: Option<&str>) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s?.trim()).ok().map(|t| t.with_timezone(&Utc))
}

pub fn map(fields: &BTreeMap<String, String>) -> Vec<Draft> {
    let mut m = Mapper::new(fields);
    let count = |section: &str| (0..).take_while(|i| fields.keys().any(|k| k.starts_with(&format!("{section}.{i}.")))).count();

    // Registry records: what the registries GFW compiled say.
    let registry = count(REGISTRY);
    for i in 0..registry {
        let key = |f: &str| format!("{REGISTRY}.{i}.{f}");
        let from = timestamp(m.get(&key("transmissionDateFrom")));
        let latest = m.get(&key("latestVesselInfo")) == Some("true");
        let to = if latest { None } else { timestamp(m.get(&key("transmissionDateTo"))) };
        let q = format!("registry {}", m.get(&key("sourceCode")).unwrap_or("-"));
        let start = m.out.len();
        m.imo(&key("imo"));
        m.callsign(&key("callsign"));
        m.name(attr::NAME, &key("shipname"));
        m.flag(&key("flag"));
        mmsi(&mut m, &key("ssvid"));
        m.text(attr::VESSEL_TYPE, &key("geartypes"));
        m.measure(attr::LENGTH_M, &key("lengthM"), "m", 1.0);
        m.measure(attr::GROSS_TONNAGE, &key("tonnageGt"), "GT", 1.0);
        m.measure(attr::DEPTH_M, &key("depthM"), "m", 1.0);
        m.year(attr::YEAR_BUILT, &key("builtYear"));
        for d in &mut m.out[start..] {
            d.qualifier = Some(match d.qualifier.take() {
                Some(unit) => format!("{q}; {unit}"),
                None => q.clone(),
            });
            d.valid_from = from;
            d.valid_to = to;
        }
    }

    // AIS identities: what the vessel broadcast, and when. The newest is
    // open-ended; the IMO a ship broadcasts asserts nothing.
    let ais = count(SELF_REPORTED);
    let newest = (0..ais).filter_map(|i| timestamp(m.get(&format!("{SELF_REPORTED}.{i}.transmissionDateTo")))).max();
    for i in 0..ais {
        let key = |f: &str| format!("{SELF_REPORTED}.{i}.{f}");
        let from = timestamp(m.get(&key("transmissionDateFrom")));
        let to = timestamp(m.get(&key("transmissionDateTo"))).filter(|t| Some(*t) != newest);
        let start = m.out.len();
        mmsi(&mut m, &key("ssvid"));
        m.callsign(&key("callsign"));
        m.name(attr::NAME, &key("shipname"));
        m.flag(&key("flag"));
        for d in &mut m.out[start..] {
            d.qualifier = Some("AIS self-reported".into());
            d.valid_from = from;
            d.valid_to = to;
        }
    }
    m.out
}

fn mmsi(m: &mut Mapper, key: &str) {
    if let Some(v) = m.get(key) {
        if let Some(n) = normalize::mmsi(v) {
            m.push(Draft::new(attr::MMSI, "identifier", v, Some(n)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GFW's own client test fixture for one vessel (trimmed).
    pub const ENTRY: &str = r#"{
      "registryInfoTotalRecords": 2,
      "registryInfo": [
        {"id": "a8d00ce54b37add7f85a35fcce8e7a1b", "sourceCode": ["GFW-REVIEW", "IMO", "NEAFC", "TMT_OTHER"], "ssvid": "273379740", "flag": "RUS",
         "shipname": "FRIO FORWIN", "nShipname": "FRIOFORWIN", "callsign": "UCRZ", "imo": "9076260", "latestVesselInfo": true,
         "transmissionDateFrom": "2015-02-27T10:59:43Z", "transmissionDateTo": "2025-02-25T08:26:32Z", "geartypes": ["SPECIALIZED_REEFER"],
         "lengthM": 126.8, "tonnageGt": 6963.6, "vesselInfoReference": "418225ffa5438506023be0c1d0f4716a",
         "extraFields": [{"registrySource": "TMT", "iuuStatus": null, "images": [], "builtYear": null, "depthM": null}]},
        {"id": "a8d00ce54b37add7f85a35fcce8e7a1b", "sourceCode": ["GFW-REVIEW", "ICCAT", "IMO", "RUS"], "ssvid": "210631000", "flag": "CYP",
         "shipname": "FRIO FORWIN", "callsign": "5BWC3", "imo": "9076260",
         "transmissionDateFrom": "2013-05-15T20:19:43Z", "transmissionDateTo": "2024-07-04T14:19:36Z", "geartypes": ["SPECIALIZED_REEFER"],
         "lengthM": 126.8, "tonnageGt": 6963.6, "vesselInfoReference": "e99af472b926d251e41457f6f1f5e9cc"}
      ],
      "registryOwners": [{"name": "ZHOUSHAN SHUNHANG OCEAN FISHERIES", "flag": "CHN", "ssvid": "412331038", "sourceCode": ["RESEARCH-PAPER"], "dateFrom": "2013-11-15T00:28:32Z", "dateTo": "2024-10-31T16:25:53Z"}],
      "registryPublicAuthorizations": [{"dateFrom": "2023-01-01T00:00:00Z", "dateTo": "2024-12-31T00:00:00Z", "ssvid": "210631000", "sourceCode": ["ICCAT"]}],
      "combinedSourcesInfo": [{"vesselId": "da1cd7e1b-b8d0-539c-6581-2b3df8d0a6af", "geartypes": [{"name": "CARRIER", "source": "GFW_VESSEL_LIST", "yearFrom": 2015, "yearTo": 2025}], "shiptypes": [{"name": "CARRIER", "source": "GFW_VESSEL_LIST", "yearFrom": 2015, "yearTo": 2025}]}],
      "selfReportedInfo": [
        {"id": "da1cd7e1b-b8d0-539c-6581-2b3df8d0a6af", "ssvid": "273379740", "shipname": "FRIO FORWIN", "flag": "RUS", "callsign": "UCRZ", "imo": "9076260",
         "messagesCounter": 263107, "sourceCode": ["AIS"], "transmissionDateFrom": "2015-02-27T10:59:21Z", "transmissionDateTo": "2025-03-02T23:51:32Z"},
        {"id": "0cb77880e-ee49-2ce4-a849-c0b413b6ec89", "ssvid": "354666000", "shipname": "FRIO AEGEAN", "flag": "PAN", "callsign": "3FGY4", "imo": "9076261",
         "messagesCounter": 120929, "sourceCode": ["AIS"], "transmissionDateFrom": "2012-01-01T00:32:48Z", "transmissionDateTo": "2013-05-15T20:25:05Z"}
      ],
      "dataset": "public-global-vessel-identity:v3.0"
    }"#;

    fn slice(entries: &[&str]) -> RawObject {
        let doc = format!(r#"{{"where": "flag = 'RUS'", "entries": [{}]}}"#, entries.join(","));
        RawObject::new("flag_RUS.json", Bytes::from(doc))
    }

    #[test]
    fn flattens_in_a_stable_order() {
        let v: Value = serde_json::from_str(ENTRY).unwrap();
        let f = flatten(&v);
        // Registry rows sorted by first transmission: the Cyprus period first.
        assert_eq!(f.get("registryInfo.0.flag").unwrap(), "CYP");
        assert_eq!(f.get("registryInfo.1.flag").unwrap(), "RUS");
        assert_eq!(f.get("registryInfo.1.sourceCode").unwrap(), "GFW-REVIEW,IMO,NEAFC,TMT_OTHER");
        assert_eq!(f.get("registryInfo.1.registrySource").unwrap(), "TMT");
        assert_eq!(f.get("selfReportedInfo.0.shipname").unwrap(), "FRIO AEGEAN");
        assert_eq!(f.get("combinedSourcesInfo.0.shiptypes").unwrap(), "CARRIER");
        assert_eq!(f.get("registryOwners.0.name").unwrap(), "ZHOUSHAN SHUNHANG OCEAN FISHERIES");
        assert!(!f.contains_key("registryInfo.1.builtYear"), "nulls are dropped");
    }

    #[test]
    fn one_record_per_vessel() {
        let ais_only = r#"{"selfReportedInfo": [{"id": "zz-1", "ssvid": "316001234", "shipname": "SMALL BOAT", "flag": "CAN"}]}"#;
        let p = parse(&[slice(&[ENTRY, ais_only, ENTRY, "{}"])]).unwrap();
        let ids: Vec<&str> = p.records.iter().map(|r| r.subject_id.as_str()).collect();
        assert_eq!(ids, ["registry:a8d00ce54b37add7f85a35fcce8e7a1b", "ais:zz-1"]);
        assert_eq!(p.rejected.len(), 1);
        assert_eq!(p.warnings, ["1 vessels appeared in more than one search"]);
    }

    fn drafts() -> Vec<Draft> {
        let v: Value = serde_json::from_str(ENTRY).unwrap();
        map(&flatten(&v))
    }

    #[test]
    fn only_registry_records_claim_an_imo() {
        let imos: Vec<_> = drafts().into_iter().filter(|d| d.attribute == attr::IMO).collect();
        assert!(!imos.is_empty());
        assert!(imos.iter().all(|d| d.value_normalized.as_deref() == Some("9076260")), "the broadcast 9076261 asserts nothing");
        assert!(imos.iter().all(|d| d.qualifier.as_deref().unwrap().starts_with("registry ")));
    }

    #[test]
    fn mmsis_carry_when_they_were_used() {
        let d = drafts();
        let ais: Vec<&Draft> = d.iter().filter(|d| d.attribute == attr::MMSI && d.qualifier.as_deref() == Some("AIS self-reported")).collect();
        let old = ais.iter().find(|d| d.value_normalized.as_deref() == Some("354666000")).unwrap();
        assert_eq!(old.valid_from, timestamp(Some("2012-01-01T00:32:48Z")));
        assert_eq!(old.valid_to, timestamp(Some("2013-05-15T20:25:05Z")));
        let current = ais.iter().find(|d| d.value_normalized.as_deref() == Some("273379740")).unwrap();
        assert_eq!(current.valid_to, None, "the newest MMSI is open-ended");

        // The registry row marked latest is open-ended too; the older one closes.
        let flags: Vec<(&str, bool)> = d
            .iter()
            .filter(|d| d.attribute == attr::FLAG && d.qualifier.as_deref().unwrap().starts_with("registry"))
            .map(|d| (d.value_normalized.as_deref().unwrap(), d.valid_to.is_none()))
            .collect();
        assert!(flags.contains(&("RUS", true)) && flags.contains(&("CYP", false)), "{flags:?}");

        let length = d.iter().find(|d| d.attribute == attr::LENGTH_M).unwrap();
        assert_eq!(length.value_normalized.as_deref(), Some("126.8"));
        assert!(length.qualifier.as_deref().unwrap().ends_with("; m"));
    }

    #[test]
    fn the_ledger_enforces_daily_and_monthly_caps() {
        let caps = Caps { daily: 3, monthly: 5 };
        let day = |d: u32, h: u32| chrono::NaiveDate::from_ymd_opt(2026, 10, d).unwrap().and_hms_opt(h, 0, 0).unwrap().and_utc();
        let mut u = Usage::default();
        for _ in 0..3 {
            assert_eq!(u.refusal(day(18, 9), &caps), None);
            u.record(day(18, 9));
        }
        assert!(u.refusal(day(18, 23), &caps).unwrap().starts_with("daily cap"));
        // A new day resets the daily count but not the monthly one.
        assert_eq!(u.refusal(day(19, 0), &caps), None);
        u.record(day(19, 0));
        u.record(day(19, 0));
        assert!(u.refusal(day(20, 0), &caps).unwrap().starts_with("monthly cap"));
        // A 429 blocks for a day, or 30 once the monthly cap is reached.
        u.blocked(day(20, 0), &caps);
        assert_eq!(u.blocked_until, Some(day(20, 0) + chrono::Duration::days(30)));
        let mut fresh = Usage::default();
        fresh.record(day(18, 9));
        fresh.blocked(day(18, 9), &caps);
        assert!(fresh.refusal(day(19, 8), &caps).unwrap().contains("429"));
        assert_eq!(fresh.refusal(day(19, 10), &caps), None);

        // GFW's own count wins when higher, never when lower.
        let mut h = reqwest::header::HeaderMap::new();
        h.insert("x-ratelimit-daily-current-usage", "9".parse().unwrap());
        h.insert("x-ratelimit-monthly-current-usage", "1".parse().unwrap());
        u.sync(&h);
        assert_eq!((u.day_requests, u.month_requests), (9, 5));

        // It survives a restart.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state/usage.json");
        assert_eq!(Usage::load(&path).unwrap(), Usage::default());
        u.save(&path).unwrap();
        assert_eq!(Usage::load(&path).unwrap(), u);
    }

    #[test]
    fn batches_known_identifiers() {
        let base = include_str!("../../config/sources/gfw_vessel_identity.toml");
        let mut cfg = SourceConfig::parse(base, "gfw").unwrap();
        cfg.options.insert("batch_size".into(), toml::Value::Integer(2));
        let l = |k: &str, v: &str| Lookup { kind: k.into(), value: v.into() };
        let lookups = [l("IMO", "9235983"), l("MMSI", "316001325"), l("IMO", "9194957"), l("IMO", "9194957"), l("CALLSIGN", "VCXY"), l("MMSI", "1' OR '1")];
        let b = batches(&cfg, &lookups);
        assert_eq!(b, [vec!["imo = '9194957'".to_string(), "imo = '9235983'".into()], vec!["ssvid = '316001325'".to_string()]], "sorted, deduplicated, nothing unsafe quoted in");
        assert_eq!(clause(&b[0]), "imo = '9194957' OR imo = '9235983'");
        assert_ne!(batch_file(&b[0]), batch_file(&b[1]));
        cfg.options.insert("max_batches".into(), toml::Value::Integer(1));
        assert_eq!(batches(&cfg, &lookups).len(), 1);
    }
}
