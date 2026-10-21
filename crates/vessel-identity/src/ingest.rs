//! One ingestion cycle for one source:
//!
//! 1. check the source is authorized for automated collection
//! 2. fetch (or read the operator's files), hashing every object
//! 3. stop if exactly this content was already ingested
//! 4. parse and validate the layout
//! 5. compare with bronze: new, modified, unchanged, removed
//! 6. quarantine if too much would disappear at once
//! 7. archive the fetched objects (when the licence allows keeping them)
//! 8. append the changed records to `source_records`
//! 9. rebuild the source's assertions from its whole bronze history
//! 10. record the run and refresh `source_catalog`
//!
//! The run is recorded as succeeded only after the bronze and assertion
//! commits. If it stops in between, the next run finds no successful run
//! for the content, adds nothing to bronze (it is already there), and
//! rebuilds the assertions: the interrupted run is completed, not repeated.

use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use iceberg::Catalog;

use crate::assertions;
use crate::config::{Catalogue, SourceConfig};
use crate::fetch::{content_hash, Http, RawObject};
use crate::load;
use crate::records::{self, Delta, RunInfo};
use crate::sources::{Adapter, FetchCtx};
use crate::tables::{
    self, CatalogRow, RunRow, Store, ASSERTIONS, ASSERTION_COLUMNS, CATALOG_COLUMNS, INGESTION_RUNS, REJECTED_COLUMNS,
    REJECTED_RECORDS, RUN_COLUMNS, SOURCE_CATALOG, SOURCE_RECORDS, SOURCE_RECORD_COLUMNS,
};

pub struct Options {
    pub apply: bool,
    /// Ingest even when the content matches the last successful run.
    pub force: bool,
    /// Apply a run that would remove more than `max_removal_fraction`.
    pub accept_removals: bool,
    pub source_dir: Option<PathBuf>,
    pub scratch: PathBuf,
    pub user_agent: Option<String>,
    /// Where fetched objects are archived. Default: `raw_archive/<namespace>`
    /// at the root of the warehouse bucket.
    pub raw_archive: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum Outcome {
    Succeeded,
    Unchanged,
    Quarantined,
    Refused,
    DryRun,
}

#[cfg(test)]
mod tests {
    use super::archive_root;

    #[test]
    fn archives_outside_the_table_prefixes() {
        assert_eq!(archive_root("s3://maritimedata/tables/5d07177b", "identity"), "s3://maritimedata/raw_archive/identity");
        assert_eq!(archive_root("file:///tmp/w/identity/source_records", "identity"), "file:///tmp/w/identity/raw_archive");
    }
}

pub fn run_id(source_id: &str, at: DateTime<Utc>) -> String {
    format!("{source_id}-{}-{}", at.format("%Y%m%dT%H%M%SZ"), &uuid::Uuid::new_v4().simple().to_string()[..6])
}

/// The last run of `source` that ended with its content in the registry.
fn last_good<'a>(runs: &'a [RunRow], source: &str) -> Option<&'a RunRow> {
    runs.iter()
        .filter(|r| r.source_id == source && (r.status == "succeeded" || r.status == "unchanged"))
        .max_by_key(|r| r.started_at)
}

pub async fn ingest<C: Catalog>(store: Option<&Store<C>>, cat: &Catalogue, source_id: &str, opts: &Options) -> Result<Outcome> {
    let (cfg, config_hash) = cat.get(source_id)?;
    let adapter = Adapter::from_name(&cfg.adapter)?;
    let started = Utc::now();
    let mut run = RunRow {
        run_id: run_id(source_id, started),
        source_id: source_id.to_string(),
        started_at: started,
        parser_version: adapter.parser_version().to_string(),
        config_hash: config_hash.clone(),
        ..Default::default()
    };
    println!("{source_id}: {}", cfg.name);

    let refusal = if opts.source_dir.is_some() { cfg.refusal_for_local_files() } else { cfg.refusal() };
    if let Some(why) = refusal {
        println!("  refused: {why}");
        run.status = "refused".into();
        run.message = Some(why);
        record(store, opts, cat, run).await?;
        return Ok(Outcome::Refused);
    }

    match ingest_inner(store, cfg, adapter, opts, &mut run).await {
        Ok(outcome) => {
            if outcome != Outcome::DryRun {
                record(store, opts, cat, run).await?;
            }
            Ok(outcome)
        }
        Err(e) => {
            run.status = "failed".into();
            run.message = Some(format!("{e:#}"));
            if let Err(e2) = record(store, opts, cat, run).await {
                eprintln!("  could not record the failed run: {e2:#}");
            }
            Err(e)
        }
    }
}

async fn ingest_inner<C: Catalog>(
    store: Option<&Store<C>>,
    cfg: &SourceConfig,
    adapter: Adapter,
    opts: &Options,
    run: &mut RunRow,
) -> Result<Outcome> {
    let source_id = cfg.source_id.as_str();
    let ua = opts.user_agent.clone().or_else(|| cfg.options.get("user_agent").and_then(|v| v.as_str()).map(String::from));
    let http = Http::new(ua.as_deref())?;
    let lookups = match (adapter, store) {
        (Adapter::Gfw, Some(s)) if opts.source_dir.is_none() => crate::registry::known_identifiers(s, source_id).await?,
        _ => Vec::new(),
    };
    let ctx = FetchCtx { http: &http, local_dir: opts.source_dir.as_deref(), scratch: &opts.scratch, lookups: &lookups };
    let fetched = adapter.fetch(cfg, &ctx).await.context("fetching")?;
    let retrieved_at = Utc::now();
    let hash = content_hash(&fetched.objects);
    let bytes: usize = fetched.objects.iter().map(|o| o.bytes.len()).sum();
    println!(
        "  fetched {} object(s), {:.1} MB, content {}{}",
        fetched.objects.len(),
        bytes as f64 / 1e6,
        &hash[..12],
        fetched.source_version.as_deref().map(|v| format!(", version {v}")).unwrap_or_default()
    );
    run.content_hash = Some(hash.clone());
    run.source_version = fetched.source_version.clone();
    run.source_published_at = fetched.source_published_at;
    run.complete = Some(fetched.complete);
    run.warnings.extend(fetched.warnings.iter().cloned());

    let runs = match store {
        Some(s) => load::runs(&s.read(INGESTION_RUNS, None).await?)?,
        None => Vec::new(),
    };
    if !opts.force {
        if let Some(prev) = last_good(&runs, source_id).filter(|r| r.content_hash.as_deref() == Some(hash.as_str())) {
            println!("  unchanged: same content as run {} ({})", prev.run_id, prev.started_at.format("%Y-%m-%d %H:%M"));
            run.status = "unchanged".into();
            run.finished_at = Some(Utc::now());
            return Ok(if opts.apply { Outcome::Unchanged } else { Outcome::DryRun });
        }
    }

    let parsed = adapter.parse(cfg, &fetched).context("parsing")?;
    anyhow::ensure!(!parsed.records.is_empty(), "the source yielded no records");
    run.warnings.extend(parsed.warnings.iter().cloned());
    for w in &run.warnings {
        println!("  note: {w}");
    }

    let stored = match store {
        Some(s) => records::read_versions(&s.read(SOURCE_RECORDS, Some(source_id)).await?)?,
        None => Vec::new(),
    };

    let ingested_at = Utc::now();
    // Archive first, so bronze rows can point at the objects.
    let uris: Vec<Option<String>> = match (store, opts.apply && cfg.retain_raw) {
        (Some(s), true) => archive(s, cfg, &fetched.objects, ingested_at, opts.raw_archive.as_deref()).await?,
        _ => vec![None; fetched.objects.len()],
    };
    run.source_object_uris = uris.iter().flatten().cloned().collect();
    let info = RunInfo {
        run_id: &run.run_id,
        source_version: fetched.source_version.as_deref(),
        source_published_at: fetched.source_published_at,
        retrieved_at,
        ingested_at,
        object_uris: &uris,
        complete: fetched.complete,
    };
    let delta = records::diff(cfg, adapter, &info, &parsed, &stored);
    report(&delta);
    run.records_read = Some(delta.read as i64);
    run.records_new = Some(delta.new as i64);
    run.records_modified = Some(delta.modified as i64);
    run.records_unchanged = Some(delta.unchanged as i64);
    run.records_removed = Some(delta.removed as i64);
    run.records_rejected = Some(delta.rejected.len() as i64);

    if let Some(why) = delta.quarantine_reason(cfg).filter(|_| !opts.accept_removals) {
        println!("  quarantined: {why}; nothing written (rerun with --accept-removals if the removals are real)");
        run.status = "quarantined".into();
        run.message = Some(why);
        run.finished_at = Some(Utc::now());
        return Ok(if opts.apply { Outcome::Quarantined } else { Outcome::DryRun });
    }

    let mut all = stored;
    all.extend(delta.versions());
    records::sort_versions(&mut all);
    let derived = assertions::derive(cfg, adapter, &all);
    let active = derived.iter().filter(|a| a.assertion_status == "active").count();
    println!("  assertions: {} ({} active)", derived.len(), active);
    run.assertions_total = Some(derived.len() as i64);
    run.assertions_active = Some(active as i64);

    let Some(s) = store.filter(|_| opts.apply) else {
        return Ok(Outcome::DryRun);
    };
    s.append(
        SOURCE_RECORDS,
        SOURCE_RECORD_COLUMNS,
        &delta.rows,
        Some(tables::source_month_partition(source_id, ingested_at)),
    )
    .await?;
    s.append(REJECTED_RECORDS, REJECTED_COLUMNS, &delta.rejected, Some(tables::source_partition(source_id))).await?;
    if derived.is_empty() {
        run.warnings.push("no assertions derived; identity_assertions left as it was".into());
    } else {
        s.replace_source(ASSERTIONS, ASSERTION_COLUMNS, source_id, &derived).await?;
    }
    run.source_records_snapshot_id = s.snapshot_id(SOURCE_RECORDS).await?;
    run.assertions_snapshot_id = s.snapshot_id(ASSERTIONS).await?;
    run.status = "succeeded".into();
    run.finished_at = Some(Utc::now());
    println!("  written: {} source record row(s), {} rejected", delta.rows.len(), delta.rejected.len());
    Ok(Outcome::Succeeded)
}

fn report(d: &Delta) {
    println!(
        "  records: {} read, {} new, {} modified, {} unchanged, {} removed, {} rejected, {} with validation findings",
        d.read,
        d.new,
        d.modified,
        d.unchanged,
        d.removed,
        d.rejected.len(),
        d.partial
    );
}

/// The archive root: `s3://<bucket>/raw_archive/<namespace>` for a table in
/// `s3://<bucket>/...`, else `raw_archive` beside the table's directory.
/// Never inside a table's or the catalog's own prefix, where table
/// maintenance would take the objects for orphans.
pub fn archive_root(table_location: &str, namespace: &str) -> String {
    let location = table_location.trim_end_matches('/');
    if let Some(rest) = location.strip_prefix("s3://").or_else(|| location.strip_prefix("s3a://")) {
        let bucket = rest.split('/').next().unwrap_or(rest);
        return format!("s3://{bucket}/raw_archive/{namespace}");
    }
    let parent = location.rsplit_once('/').map_or(location, |(p, _)| p);
    format!("{parent}/raw_archive")
}

/// Writes each object to `<root>/<source>/<date>/<sha256 prefix>-<name>`,
/// skipping objects already there. Returns each object's URI.
async fn archive<C: Catalog>(
    store: &Store<C>,
    cfg: &SourceConfig,
    objects: &[RawObject],
    at: DateTime<Utc>,
    root: Option<&str>,
) -> Result<Vec<Option<String>>> {
    let table = store.table(SOURCE_RECORDS).await?;
    let base = match root {
        Some(r) => r.trim_end_matches('/').to_string(),
        None => archive_root(table.metadata().location(), &store.config.namespace),
    };
    let io = table.file_io();
    let mut uris = Vec::new();
    for o in objects {
        let uri = format!("{base}/{}/{}/{}-{}", cfg.source_id, at.format("%Y-%m-%d"), &o.sha256[..16], o.name);
        if !io.exists(&uri).await.unwrap_or(false) {
            io.new_output(&uri)?.write(o.bytes.clone()).await.with_context(|| format!("archiving {uri}"))?;
        }
        uris.push(Some(uri));
    }
    println!("  archived {} object(s) under {base}/{}", objects.len(), cfg.source_id);
    Ok(uris)
}

/// Appends the run to `ingestion_runs` and refreshes `source_catalog`.
async fn record<C: Catalog>(store: Option<&Store<C>>, opts: &Options, cat: &Catalogue, mut run: RunRow) -> Result<()> {
    let Some(s) = store.filter(|_| opts.apply) else {
        return Ok(());
    };
    run.finished_at.get_or_insert_with(Utc::now);
    s.append(INGESTION_RUNS, RUN_COLUMNS, std::slice::from_ref(&run), None).await?;
    refresh_catalog(s, cat).await
}

pub async fn refresh_catalog<C: Catalog>(s: &Store<C>, cat: &Catalogue) -> Result<()> {
    let runs = load::runs(&s.read(INGESTION_RUNS, None).await?)?;
    let now = Utc::now();
    let rows: Vec<CatalogRow> = cat
        .sources
        .values()
        .map(|(c, hash)| {
            let mine: Vec<&RunRow> = runs.iter().filter(|r| r.source_id == c.source_id).collect();
            let good = last_good(&runs, &c.source_id);
            let ok = mine.iter().filter(|r| r.status == "succeeded").max_by_key(|r| r.started_at);
            let last = mine.iter().max_by_key(|r| r.started_at);
            CatalogRow {
                source_id: c.source_id.clone(),
                name: c.name.clone(),
                publisher: c.publisher.clone(),
                adapter: c.adapter.clone(),
                source_url: c.source_url.clone(),
                dataset_url: c.dataset_url.clone(),
                access_method: c.access_method.clone(),
                data_format: c.data_format.clone(),
                refresh_policy: c.refresh_policy.clone(),
                license: c.license.clone(),
                permitted_uses: c.permitted_uses.clone(),
                access_status: c.access_status.clone(),
                authorization_basis: c.authorization_basis.clone(),
                authentication: c.authentication.clone(),
                enabled: Some(c.enabled),
                priority: Some(c.priority),
                trust: c.trust.clone(),
                jurisdiction: c.jurisdiction.clone(),
                removal_semantics: c.removal_semantics.clone(),
                config_hash: hash.clone(),
                parser_version: Adapter::from_name(&c.adapter).map(|a| a.parser_version()).unwrap_or("?").to_string(),
                last_successful_fetch: good.map(|r| r.started_at),
                last_successful_ingestion: ok.and_then(|r| r.finished_at),
                last_source_publication: good.and_then(|r| r.source_published_at),
                last_content_hash: good.and_then(|r| r.content_hash.clone()),
                last_run_status: last.map(|r| r.status.clone()),
                updated_at: now,
            }
        })
        .collect();
    s.replace(SOURCE_CATALOG, CATALOG_COLUMNS, &rows, false).await
}

/// Sources due for collection now: approved, enabled, and not fetched
/// within their refresh policy.
pub async fn due<C: Catalog>(store: &Store<C>, cat: &Catalogue) -> Result<Vec<String>> {
    let runs = load::runs(&store.read(INGESTION_RUNS, None).await?)?;
    let now = Utc::now();
    Ok(cat
        .sources
        .values()
        .filter(|(c, _)| c.refusal().is_none())
        .filter(|(c, _)| c.is_due(last_good(&runs, &c.source_id).map(|r| r.started_at), now))
        .map(|(c, _)| c.source_id.clone())
        .collect())
}

/// One line per source: authorization, last run, freshness.
pub async fn status<C: Catalog>(store: Option<&Store<C>>, cat: &Catalogue) -> Result<()> {
    let runs = match store {
        Some(s) => load::runs(&s.read(INGESTION_RUNS, None).await?)?,
        None => Vec::new(),
    };
    let now = Utc::now();
    for (c, _) in cat.sources.values() {
        let good = last_good(&runs, &c.source_id);
        let last = runs.iter().filter(|r| r.source_id == c.source_id).max_by_key(|r| r.started_at);
        let auth = match c.refusal() {
            None => "collectable".to_string(),
            Some(why) => format!("not collected: {why}"),
        };
        println!("{} ({}, {})", c.source_id, c.refresh_policy, auth);
        match last {
            Some(r) => println!(
                "  last run {} {} ({}){}",
                r.started_at.format("%Y-%m-%d %H:%M"),
                r.status,
                r.run_id,
                r.message.as_deref().map(|m| format!(": {m}")).unwrap_or_default()
            ),
            None => println!("  never run"),
        }
        if let Some(g) = good {
            println!(
                "  last good fetch {}{}{}",
                g.started_at.format("%Y-%m-%d %H:%M"),
                g.source_version.as_deref().map(|v| format!(", version {v}")).unwrap_or_default(),
                g.source_published_at.map(|p| format!(", published {}", p.format("%Y-%m-%d"))).unwrap_or_default()
            );
        }
        if c.refusal().is_none() && c.is_due(good.map(|r| r.started_at), now) {
            println!("  due now");
        }
    }
    Ok(())
}
