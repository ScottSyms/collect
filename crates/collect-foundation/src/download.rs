//! Fetching source files: downloaded into a scratch directory, or read from a
//! local directory holding the same file names.

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use collect_core::availability::SourceUnavailable;

pub const USER_AGENT: &str = concat!("collect-foundation/", env!("CARGO_PKG_VERSION"));

pub struct Source {
    /// Base URL of the dump directory, ending in `/`.
    pub base_url: String,
    /// Read files from here instead of downloading them.
    pub local_dir: Option<PathBuf>,
    /// Where downloads go.
    pub scratch: PathBuf,
    http: reqwest::Client,
}

impl Source {
    pub fn new(base_url: &str, local_dir: Option<PathBuf>, scratch: PathBuf) -> Result<Self> {
        Self::with_user_agent(base_url, local_dir, scratch, None)
    }

    /// Like [`Source::new`], sending `user_agent` instead of
    /// `collect-foundation/<version>` (some publishers' firewalls refuse
    /// anything that doesn't look like a browser).
    pub fn with_user_agent(
        base_url: &str,
        local_dir: Option<PathBuf>,
        scratch: PathBuf,
        user_agent: Option<&str>,
    ) -> Result<Self> {
        let mut base_url = base_url.to_string();
        if !base_url.is_empty() && !base_url.ends_with('/') {
            base_url.push('/');
        }
        let http = reqwest::Client::builder()
            .user_agent(user_agent.unwrap_or(USER_AGENT))
            .build()?;
        Ok(Source { base_url, local_dir, scratch, http })
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// The local path of `name` (relative to the dump directory, e.g.
    /// `allCountries.zip` or `alternatenames/NO.zip`), downloading it first
    /// unless reading from a local directory.
    pub async fn fetch(&self, name: &str) -> Result<PathBuf> {
        let url = format!("{}{name}", self.base_url);
        self.fetch_url(&url, name).await
    }

    /// The local path of `name`, downloading it from `url` first unless
    /// reading from a local directory (where it is looked up as `name`).
    pub async fn fetch_url(&self, url: &str, name: &str) -> Result<PathBuf> {
        if let Some(dir) = &self.local_dir {
            let path = dir.join(name);
            anyhow::ensure!(path.is_file(), "{} not found", path.display());
            return Ok(path);
        }
        let path = self.scratch.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let started = Instant::now();
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .with_context(|| format!("requesting {url}"))?;
        save(resp, url, name, &path, started).await?;
        Ok(path)
    }
}

/// Streams a successful response to `path` via a temporary name, so an
/// interrupted download never looks like a complete file.
pub async fn save(
    mut resp: reqwest::Response,
    url: &str,
    name: &str,
    path: &Path,
    started: Instant,
) -> Result<()> {
    if !resp.status().is_success() {
        // Marked so a failed run is recorded as the source being down.
        return Err(SourceUnavailable(format!("{url}: HTTP {}", resp.status())).into());
    }
    let partial = path.with_extension("part");
    let mut out = File::create(&partial).with_context(|| format!("creating {}", partial.display()))?;
    let mut bytes: u64 = 0;
    while let Some(chunk) = resp.chunk().await.with_context(|| format!("downloading {url}"))? {
        out.write_all(&chunk)?;
        bytes += chunk.len() as u64;
    }
    out.flush()?;
    drop(out);
    std::fs::rename(&partial, path)?;
    println!(
        "  downloaded {name}: {:.1} MB in {:.1}s",
        bytes as f64 / 1e6,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Runs `f` over the text of `path`: the file itself, or for a `.zip` the
/// entry named like the zip (`NO.zip` -> `NO.txt`).
pub fn with_text<T>(path: &Path, f: impl FnOnce(&mut dyn BufRead) -> Result<T>) -> Result<T> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    if path.extension().and_then(|e| e.to_str()) != Some("zip") {
        return f(&mut BufReader::with_capacity(1 << 20, file));
    }
    let entry = format!(
        "{}.txt",
        path.file_stem().and_then(|s| s.to_str()).unwrap_or_default()
    );
    let mut zip = zip::ZipArchive::new(file).with_context(|| format!("reading {}", path.display()))?;
    let inner = zip
        .by_name(&entry)
        .with_context(|| format!("{} has no {entry}", path.display()))?;
    // Bound to a local so the entry reader is dropped before the archive it
    // borrows from.
    #[allow(clippy::let_and_return)]
    let out = f(&mut BufReader::with_capacity(1 << 20, inner));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_matching_zip_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("NO.zip");
        let mut zip = zip::ZipWriter::new(File::create(&path).unwrap());
        let opts = zip::write::SimpleFileOptions::default();
        zip.start_file("readme.txt", opts).unwrap();
        zip.write_all(b"not this one").unwrap();
        zip.start_file("NO.txt", opts).unwrap();
        zip.write_all(b"1\tOslo\n").unwrap();
        zip.finish().unwrap();

        let text = with_text(&path, |r| {
            let mut s = String::new();
            r.read_to_string(&mut s)?;
            Ok(s)
        })
        .unwrap();
        assert_eq!(text, "1\tOslo\n");
    }
}
