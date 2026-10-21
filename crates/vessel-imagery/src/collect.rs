//! Getting photos in: from Wikimedia Commons by IMO category, or from a
//! manifest of files already held (licensed or restricted sets).

use std::collections::HashSet;
use std::path::Path;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use iceberg::Catalog;
use serde::Deserialize;

use crate::commons::Commons;
use crate::identity::{Target, Targets};
use crate::images;
use crate::tables::{ImageRow, Store, IMAGES, IMAGE_COLUMNS};

/// Rows are appended every this many images, so an interrupted run keeps
/// most of its work.
const FLUSH_EVERY: usize = 50;

pub struct FetchOptions {
    pub apply: bool,
    /// At most this many vessels with an IMO number.
    pub limit: Option<usize>,
    pub per_vessel: usize,
    pub depth: usize,
    /// Revisit vessels that already have Commons photos.
    pub refresh: bool,
    pub archive_root: Option<String>,
}

/// Fetches Commons photos for `targets` (already narrowed by the caller).
pub async fn fetch_commons<C: Catalog>(
    store: Option<&Store<C>>,
    commons: &mut Commons,
    targets: &[&Target],
    opts: &FetchOptions,
) -> Result<()> {
    let existing = match store {
        Some(s) => images::rows(&s.read(IMAGES).await?)?,
        None => Vec::new(),
    };
    let have_vessel: HashSet<&str> = existing.iter().filter(|r| r.source == "wikimedia_commons").map(|r| r.vessel_id.as_str()).collect();
    let mut have_url: HashSet<(String, String)> =
        existing.iter().filter_map(|r| r.original_url.clone().map(|u| (r.vessel_id.clone(), u))).collect();
    let mut have_image: HashSet<(String, String)> = existing.iter().map(|r| (r.vessel_id.clone(), r.image_id.clone())).collect();

    let todo: Vec<&&Target> = targets
        .iter()
        .filter(|t| t.imo().is_some())
        .filter(|t| opts.refresh || !have_vessel.contains(t.vessel_id.as_str()))
        .take(opts.limit.unwrap_or(usize::MAX))
        .collect();
    println!("{} vessel(s) with an IMO number to look up on Commons", todo.len());

    let table = match (store, opts.apply) {
        (Some(s), true) => Some(s.table(IMAGES).await?),
        _ => None,
    };
    let root = match (&table, store) {
        (Some(t), Some(s)) => Some(images::root_for(t, &s.config.namespace, opts.archive_root.as_deref())),
        _ => None,
    };
    let (mut pending, mut total, mut vessels_with) = (Vec::new(), 0usize, 0usize);
    for t in todo {
        let mut files = Vec::new();
        for imo in &t.imos {
            match commons.files_for_imo(imo, opts.depth, opts.per_vessel.saturating_sub(files.len())).await {
                Ok(f) => files.extend(f),
                Err(e) if e.to_string().contains("request cap") => {
                    eprintln!("{e}");
                    break;
                }
                Err(e) => eprintln!("  IMO {imo}: {e:#}"),
            }
            if files.len() >= opts.per_vessel {
                break;
            }
        }
        if files.is_empty() {
            continue;
        }
        vessels_with += 1;
        println!("  {} (IMO {}): {} photo(s)", t.name.as_deref().unwrap_or("?"), t.imo().unwrap_or("?"), files.len());
        for f in files {
            if !have_url.insert((t.vessel_id.clone(), f.original_url.clone())) {
                continue;
            }
            println!("    {} [{}] {}", f.title, f.licence_short.as_deref().unwrap_or("no licence"), f.author.as_deref().unwrap_or(""));
            let (Some(table), Some(root)) = (&table, &root) else { continue };
            let bytes = match commons.download(&f.thumb_url).await {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("    {e:#}");
                    continue;
                }
            };
            let sniffed = match images::sniff(&bytes) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("    {}: {e:#}", f.title);
                    continue;
                }
            };
            let (sha, uri) = images::store(table, root, false, bytes, sniffed.ext).await?;
            if !have_image.insert((t.vessel_id.clone(), sha.clone())) {
                continue;
            }
            pending.push(ImageRow {
                image_id: sha,
                vessel_id: t.vessel_id.clone(),
                imo_number: t.imo().map(String::from),
                mmsi: t.mmsis.first().cloned(),
                match_basis: "commons_imo_category".into(),
                source: "wikimedia_commons".into(),
                source_page_url: Some(f.page_url.clone()),
                original_url: Some(f.original_url.clone()),
                object_uri: uri,
                mime: sniffed.mime.into(),
                width: Some(sniffed.width as i32),
                height: Some(sniffed.height as i32),
                licence_short: f.licence_short.clone(),
                licence_url: f.licence_url.clone(),
                licence_class: f.licence_class().into(),
                restricted: false,
                attribution_required: f.attribution_required,
                author: f.author.clone(),
                credit: f.credit.clone(),
                captured_at: f.captured_at,
                retrieved_at: Utc::now(),
            });
            if pending.len() >= FLUSH_EVERY {
                total += flush(store, &mut pending).await?;
            }
        }
        if commons.requests >= commons.max_requests {
            println!("request cap reached; stopping");
            break;
        }
    }
    total += flush(store, &mut pending).await?;
    println!("{vessels_with} vessel(s) had photos; {total} new image(s) recorded; {} request(s)", commons.requests);
    Ok(())
}

async fn flush<C: Catalog>(store: Option<&Store<C>>, rows: &mut Vec<ImageRow>) -> Result<usize> {
    let n = match store {
        Some(s) if !rows.is_empty() => s.append(IMAGES, IMAGE_COLUMNS, rows, None).await?,
        _ => 0,
    };
    rows.clear();
    Ok(n)
}

/// One line of an import manifest. Exactly one of `imo`, `mmsi`,
/// `vessel_id` ties the file to a vessel. `restricted` defaults to true:
/// a held photo is restricted unless the manifest says otherwise.
#[derive(Debug, Deserialize)]
pub struct ManifestLine {
    pub path: String,
    #[serde(default)]
    pub imo: Option<String>,
    #[serde(default)]
    pub mmsi: Option<String>,
    #[serde(default)]
    pub vessel_id: Option<String>,
    pub source: String,
    pub licence: String,
    #[serde(default)]
    pub licence_url: Option<String>,
    #[serde(default)]
    pub attribution: Option<String>,
    #[serde(default)]
    pub author: Option<String>,
    #[serde(default)]
    pub source_url: Option<String>,
    /// `YYYY-MM-DD` or RFC 3339.
    #[serde(default)]
    pub captured_at: Option<String>,
    #[serde(default)]
    pub restricted: Option<bool>,
}

fn empty(s: &Option<String>) -> Option<&str> {
    s.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

/// The vessel a manifest line names, and how.
pub fn resolve<'a>(line: &ManifestLine, targets: &'a Targets) -> Result<(&'a Target, &'static str)> {
    match (empty(&line.vessel_id), empty(&line.imo), empty(&line.mmsi)) {
        (Some(id), None, None) => targets.by_id(id).map(|t| (t, "manifest_vessel_id")).with_context(|| format!("no vessel {id}")),
        (None, Some(imo), None) => targets.by_imo(imo).map(|t| (t, "manifest_imo")).with_context(|| format!("no vessel with IMO {imo}")),
        (None, None, Some(m)) => targets
            .by_mmsi(m)
            .map(|t| (t, "manifest_mmsi"))
            .with_context(|| format!("MMSI {m} matches no vessel, or more than one")),
        _ => bail!("give exactly one of vessel_id, imo, mmsi"),
    }
}

fn parse_when(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|t| t.with_timezone(&Utc))
        .ok()
        .or_else(|| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok().map(|d| d.and_hms_opt(0, 0, 0).expect("midnight").and_utc()))
}

/// Imports the files a CSV manifest lists (paths relative to the manifest).
pub async fn import<C: Catalog>(store: Option<&Store<C>>, targets: &Targets, manifest: &Path, apply: bool, archive_root: Option<&str>) -> Result<()> {
    let base = manifest.parent().unwrap_or(Path::new("."));
    let mut rdr = csv::ReaderBuilder::new().trim(csv::Trim::All).from_path(manifest).with_context(|| format!("reading {}", manifest.display()))?;
    let existing: HashSet<(String, String)> = match store {
        Some(s) => images::rows(&s.read(IMAGES).await?)?.into_iter().map(|r| (r.vessel_id, r.image_id)).collect(),
        None => HashSet::new(),
    };
    let table = match (store, apply) {
        (Some(s), true) => Some(s.table(IMAGES).await?),
        _ => None,
    };
    let root = match (&table, store) {
        (Some(t), Some(s)) => Some(images::root_for(t, &s.config.namespace, archive_root)),
        _ => None,
    };
    let (mut rows, mut refused, mut skipped) = (Vec::new(), 0usize, 0usize);
    for (n, line) in rdr.deserialize::<ManifestLine>().enumerate() {
        let line_no = n + 2;
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("  line {line_no}: {e}");
                refused += 1;
                continue;
            }
        };
        let (target, basis) = match resolve(&line, targets) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("  line {line_no} ({}): {e:#}", line.path);
                refused += 1;
                continue;
            }
        };
        let path = base.join(&line.path);
        let bytes = match std::fs::read(&path) {
            Ok(b) => Bytes::from(b),
            Err(e) => {
                eprintln!("  line {line_no}: {}: {e}", path.display());
                refused += 1;
                continue;
            }
        };
        let sniffed = match images::sniff(&bytes) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("  line {line_no}: {}: {e:#}", path.display());
                refused += 1;
                continue;
            }
        };
        let restricted = line.restricted.unwrap_or(true);
        let sha = vessel_identity::fetch::sha256_hex(&bytes);
        if existing.contains(&(target.vessel_id.clone(), sha.clone())) {
            skipped += 1;
            continue;
        }
        let uri = match (&table, &root) {
            (Some(t), Some(r)) => images::store(t, r, restricted, bytes, sniffed.ext).await?.1,
            _ => images::object_uri("<archive>", restricted, &sha, sniffed.ext),
        };
        let class = if restricted { "restricted".to_string() } else { crate::commons::licence_class(Some(&line.licence), None).to_string() };
        rows.push(ImageRow {
            image_id: sha,
            vessel_id: target.vessel_id.clone(),
            imo_number: target.imo().map(String::from),
            mmsi: empty(&line.mmsi).map(String::from).or_else(|| target.mmsis.first().cloned()),
            match_basis: basis.into(),
            source: line.source.clone(),
            source_page_url: line.source_url.clone(),
            original_url: None,
            object_uri: uri,
            mime: sniffed.mime.into(),
            width: Some(sniffed.width as i32),
            height: Some(sniffed.height as i32),
            licence_short: Some(line.licence.clone()),
            licence_url: line.licence_url.clone(),
            licence_class: class,
            restricted,
            attribution_required: line.attribution.is_some() || restricted,
            author: line.author.clone(),
            credit: line.attribution.clone(),
            captured_at: line.captured_at.as_deref().and_then(parse_when),
            retrieved_at: Utc::now(),
        });
    }
    println!("{} image(s) to record, {skipped} already recorded, {refused} refused", rows.len());
    if let (Some(s), true) = (store, apply) {
        s.append(IMAGES, IMAGE_COLUMNS, &rows, None).await?;
        println!("recorded {} image(s)", rows.len());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn line(imo: Option<&str>, mmsi: Option<&str>, id: Option<&str>) -> ManifestLine {
        ManifestLine {
            path: "a.jpg".into(),
            imo: imo.map(Into::into),
            mmsi: mmsi.map(Into::into),
            vessel_id: id.map(Into::into),
            source: "held".into(),
            licence: "licensed".into(),
            licence_url: None,
            attribution: None,
            author: None,
            source_url: None,
            captured_at: None,
            restricted: None,
        }
    }

    #[test]
    fn resolves_one_identifier_only() {
        let t = Targets::new(
            vec![Target { vessel_id: "v".into(), imos: vec!["9074729".into()], mmsis: vec!["235000001".into()], ..Default::default() }],
            HashMap::new(),
        );
        assert_eq!(resolve(&line(Some("9074729"), None, None), &t).unwrap().1, "manifest_imo");
        assert_eq!(resolve(&line(None, Some("235000001"), None), &t).unwrap().1, "manifest_mmsi");
        assert!(resolve(&line(Some("9074729"), Some("235000001"), None), &t).is_err());
        assert!(resolve(&line(None, None, None), &t).is_err());
        assert!(resolve(&line(Some("1234567"), None, None), &t).is_err());
    }
}
