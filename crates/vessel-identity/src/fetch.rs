//! Fetching source material: HTTP with retries and backoff, or files an
//! operator saved by hand. Every object is hashed; the hashes identify
//! content already ingested.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use sha2::{Digest, Sha256};

pub const USER_AGENT: &str = concat!("vessel-identity/", env!("CARGO_PKG_VERSION"));

/// One file as fetched, unmodified.
#[derive(Clone, Debug)]
pub struct RawObject {
    /// A file name, unique within the fetch (`large-vessel-registry_dataset_en.xlsx`).
    pub name: String,
    pub bytes: Bytes,
    pub sha256: String,
    /// The HTTP `Last-Modified` or `ETag`, when served.
    pub last_modified: Option<String>,
    pub etag: Option<String>,
}

impl RawObject {
    pub fn new(name: impl Into<String>, bytes: Bytes) -> Self {
        let sha256 = sha256_hex(&bytes);
        RawObject { name: name.into(), bytes, sha256, last_modified: None, etag: None }
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// The hash of a fetch as a whole: of its objects' names and hashes.
pub fn content_hash(objects: &[RawObject]) -> String {
    let mut parts: Vec<String> = objects.iter().map(|o| format!("{}={}", o.name, o.sha256)).collect();
    parts.sort();
    sha256_hex(parts.join("\n").as_bytes())
}

pub struct Http {
    client: reqwest::Client,
    pub max_attempts: u32,
}

impl Http {
    pub fn new(user_agent: Option<&str>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(user_agent.unwrap_or(USER_AGENT))
            .timeout(Duration::from_secs(600))
            .build()?;
        Ok(Http { client, max_attempts: 4 })
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// GETs `url` as `name`, retrying server errors and timeouts.
    pub async fn get(&self, url: &str, name: &str) -> Result<RawObject> {
        let req = || self.client.get(url);
        self.send(req, url, name).await
    }

    /// Sends what `req` builds until it succeeds, retrying 429, 5xx and
    /// transport errors with exponential backoff (2s, 4s, 8s, ...).
    pub async fn send(&self, req: impl Fn() -> reqwest::RequestBuilder, url: &str, name: &str) -> Result<RawObject> {
        let mut last_err = None;
        for attempt in 1..=self.max_attempts {
            if attempt > 1 {
                let wait = Duration::from_secs(1 << (attempt - 1).min(6));
                eprintln!("  {name}: retrying in {}s ({attempt}/{})", wait.as_secs(), self.max_attempts);
                tokio::time::sleep(wait).await;
            }
            match req().send().await {
                Ok(resp) if resp.status().is_success() => {
                    let header = |h: &str| resp.headers().get(h).and_then(|v| v.to_str().ok()).map(String::from);
                    let last_modified = header("last-modified");
                    let etag = header("etag");
                    let bytes = resp.bytes().await.with_context(|| format!("downloading {url}"))?;
                    let mut o = RawObject::new(name, bytes);
                    o.last_modified = last_modified;
                    o.etag = etag;
                    return Ok(o);
                }
                Ok(resp) => {
                    let status = resp.status();
                    if !(status.is_server_error() || status.as_u16() == 429) {
                        bail!("{url}: HTTP {status}");
                    }
                    last_err = Some(anyhow::anyhow!("{url}: HTTP {status}"));
                }
                Err(e) => last_err = Some(anyhow::Error::new(e).context(format!("requesting {url}"))),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("{url}: no attempt made")))
    }
}

/// Files in `dir` whose extension is one of `exts` (case-insensitive),
/// sorted by name.
pub fn files_in(dir: &Path, exts: &[&str]) -> Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("listing {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_file()
                && p.extension()
                    .and_then(|e| e.to_str())
                    .is_some_and(|e| exts.iter().any(|x| e.eq_ignore_ascii_case(x)))
        })
        .collect();
    out.sort();
    Ok(out)
}

pub fn read_local(path: &Path) -> Result<RawObject> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file").to_string();
    Ok(RawObject::new(name, Bytes::from(bytes)))
}

/// Text that is UTF-8, or else Latin-1 (every byte a code point).
pub fn decode_text(bytes: &[u8]) -> String {
    let bytes = bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes);
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => bytes.iter().map(|&b| b as char).collect(),
    }
}

/// The entries of a zip archive whose names end in `suffix`.
pub fn unzip(bytes: &[u8], suffix: &str) -> Result<Vec<(String, Vec<u8>)>> {
    use std::io::Read;
    let mut zip = zip::ZipArchive::new(std::io::Cursor::new(bytes)).context("reading zip")?;
    let mut out = Vec::new();
    for i in 0..zip.len() {
        let mut f = zip.by_index(i)?;
        let name = f.name().to_string();
        if name.to_ascii_lowercase().ends_with(suffix) {
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;
            out.push((name, buf));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_are_stable_and_order_free() {
        let a = RawObject::new("a", Bytes::from_static(b"1"));
        let b = RawObject::new("b", Bytes::from_static(b"2"));
        assert_eq!(content_hash(&[a.clone(), b.clone()]), content_hash(&[b, a]));
    }

    #[test]
    fn decodes_latin1() {
        assert_eq!(decode_text(b"caf\xe9"), "café");
        assert_eq!(decode_text("café".as_bytes()), "café");
        assert_eq!(decode_text(b"\xEF\xBB\xBFx"), "x");
    }
}
