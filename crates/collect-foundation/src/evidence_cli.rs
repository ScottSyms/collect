//! `collect-foundation evidence <source>`: fetch, parse and (with `--apply`)
//! load one evidence source. Each load replaces that source's partition of
//! `vessel_events` and `vessel_event_findings` with everything the source
//! holds, so the whole history is loaded every time.

use std::path::PathBuf;
use std::time::Duration;

use ais_tracks::output::{replace_string_partition, replace_table};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use collect_core::exitcode;
use collect_core::iceberg::IcebergCliArgs;
use iceberg::Catalog;

use crate::download::Source;
use crate::evidence::{
    self, iuu, paris, schema, to_batch, tokyo, tsb, uscg, Column, Evidence, RawTable, EVENTS_TABLE, EVENT_BLOOM,
    EVENT_COLUMNS, FINDINGS_TABLE, FINDING_BLOOM, FINDING_COLUMNS, PARTITION_COLUMN, SOURCES, SOURCES_TABLE,
    SOURCE_COLUMNS,
};
use crate::{connect, print_dry_run, Writer};

#[derive(Args, Debug)]
pub struct EvidenceArgs {
    #[command(subcommand)]
    command: EvidenceCommand,
}

#[derive(Subcommand, Debug)]
enum EvidenceCommand {
    /// Paris MoU PSC inspections from bulk XML files (registered users).
    ParisMou(ParisArgs),
    /// USCG inspection deficiencies (MISLE Deficiency Data Report, 2018 on).
    Uscg(UscgArgs),
    /// TSB Canada marine occurrences (MARSIS, 1975 on).
    Tsb(TsbArgs),
    /// Tokyo MoU detentions (monthly detention lists, 2000 on).
    TokyoMou(TokyoArgs),
    /// IUU fishing vessel listings by RFMOs (TMT Combined IUU Vessel List).
    IuuList(IuuArgs),
    /// Only (re)write `evidence_sources`, the table describing each source.
    Sources(Common),
}

#[derive(Args, Debug, Clone)]
struct Common {
    /// Where downloads go.
    #[arg(long, default_value_os_t = std::env::temp_dir().join("collect-foundation"))]
    scratch: PathBuf,

    /// Keep the downloaded files afterwards.
    #[arg(long)]
    keep_scratch: bool,

    /// Write the tables. Without it, fetch, parse and report only.
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct ParisArgs {
    /// Directory of bulk files from the Paris MoU data-sharing page (`.xml`,
    /// or `.zip` holding `.xml`). Every file is loaded, so keep the whole
    /// history here.
    #[arg(long)]
    source_dir: PathBuf,

    #[command(flatten)]
    common: Common,
}

#[derive(Args, Debug)]
struct UscgArgs {
    /// Read every `.xlsx` in this directory instead of downloading. The
    /// Coast Guard site refuses non-browser downloads, so save the
    /// workbooks from the report page into a directory and point here.
    #[arg(long)]
    source_dir: Option<PathBuf>,

    /// Workbook names to download from the report's file directory
    /// (URL-encoded). Default: every year listed on the page in October 2026.
    #[arg(long, value_delimiter = ',')]
    file: Vec<String>,

    /// User-Agent header for downloads.
    #[arg(long, env = "COLLECT_USER_AGENT")]
    user_agent: Option<String>,

    #[command(flatten)]
    common: Common,
}

#[derive(Args, Debug)]
struct TsbArgs {
    /// Directory the six MARSIS CSV files are published in.
    #[arg(long, env = "TSB_MARSIS_URL", default_value = tsb::DEFAULT_URL)]
    source_url: String,

    /// Read the six CSV files (same names as published) from this directory.
    #[arg(long)]
    source_dir: Option<PathBuf>,

    /// User-Agent header for downloads. The TSB's firewall refuses clients
    /// that don't identify as a browser.
    #[arg(long, env = "COLLECT_USER_AGENT")]
    user_agent: Option<String>,

    /// Skip the `marsis_*` tables holding the files as published.
    #[arg(long)]
    no_raw: bool,

    #[command(flatten)]
    common: Common,
}

#[derive(Args, Debug)]
struct TokyoArgs {
    /// The APCIS public interface.
    #[arg(long, env = "TOKYO_MOU_URL", default_value = tokyo::DEFAULT_URL)]
    source_url: String,

    /// Read monthly lists from this directory (`tokyo_mou/detlist-YYYY-MM.xls`,
    /// as `--keep-scratch` leaves them) instead of downloading.
    #[arg(long)]
    source_dir: Option<PathBuf>,

    /// First month (YYYY-MM). The load replaces every Tokyo MoU row, so a
    /// later start drops the earlier months from the table.
    #[arg(long, default_value = "2000-01", value_parser = parse_month)]
    from: (i32, u32),

    /// Last month (YYYY-MM). Default: this month.
    #[arg(long, value_parser = parse_month)]
    to: Option<(i32, u32)>,

    /// Pause between monthly requests, in milliseconds.
    #[arg(long, default_value_t = 250)]
    delay_ms: u64,

    #[command(flatten)]
    common: Common,
}

#[derive(Args, Debug)]
struct IuuArgs {
    /// Directory holding the list's Excel file(s), saved from
    /// https://iuu-vessels.org/Home/Download (the site asks who wants the
    /// file and why, so it isn't downloaded automatically). With several
    /// snapshots, every listing in any of them is kept, newest file's
    /// details first (files are taken newest name first).
    #[arg(long)]
    source_dir: PathBuf,

    #[command(flatten)]
    common: Common,
}

fn parse_month(s: &str) -> Result<(i32, u32), String> {
    let (y, m) = s.split_once('-').ok_or("expected YYYY-MM")?;
    let y: i32 = y.parse().map_err(|_| "bad year")?;
    let m: u32 = m.parse().map_err(|_| "bad month")?;
    if !(1..=12).contains(&m) || !(1900..=2200).contains(&y) {
        return Err("expected YYYY-MM".into());
    }
    Ok((y, m))
}

impl<C: Catalog> Writer<C> {
    async fn write_rows<T>(&self, table: &str, cols: &[Column<T>], rows: &[T], loaded_at: DateTime<Utc>, bloom: &[&str]) -> Result<()> {
        let batch = to_batch(cols, rows, loaded_at)?;
        let r = replace_table(&self.catalog, &self.rest, &self.config, table, schema(cols), &[batch], bloom)
            .await
            .with_context(|| format!("writing {table}"))?;
        self.report(table, r);
        Ok(())
    }

    async fn write_raw(&self, raw: &RawTable, loaded_at: DateTime<Utc>) -> Result<()> {
        let batch = raw.to_batch(loaded_at)?;
        let r = replace_table(&self.catalog, &self.rest, &self.config, &raw.table, raw.schema(), &[batch], &[])
            .await
            .with_context(|| format!("writing {}", raw.table))?;
        self.report(&raw.table, r);
        Ok(())
    }

    /// Replaces `source`'s partition of the shared tables.
    async fn write_evidence(&self, source: &str, ev: &Evidence, loaded_at: DateTime<Utc>) -> Result<()> {
        let parts: [(&str, Option<arrow::record_batch::RecordBatch>, iceberg::spec::Schema, &[&str]); 2] = [
            (EVENTS_TABLE, Some(to_batch(EVENT_COLUMNS, &ev.events, loaded_at)?), schema(EVENT_COLUMNS), EVENT_BLOOM),
            (
                FINDINGS_TABLE,
                (!ev.findings.is_empty()).then(|| to_batch(FINDING_COLUMNS, &ev.findings, loaded_at)).transpose()?,
                schema(FINDING_COLUMNS),
                FINDING_BLOOM,
            ),
        ];
        for (table, batch, schema, bloom) in parts {
            // A source with no findings at all (TSB) leaves its findings
            // partition alone rather than failing on an empty write.
            let Some(batch) = batch else { continue };
            let r = replace_string_partition(
                &self.catalog,
                &self.rest,
                &self.config,
                table,
                schema,
                PARTITION_COLUMN,
                source,
                &[batch],
                bloom,
            )
            .await
            .with_context(|| format!("writing {table} ({source})"))?;
            self.report(&format!("{table}[{source}]"), r);
        }
        self.write_rows(SOURCES_TABLE, SOURCE_COLUMNS, SOURCES, loaded_at, &[]).await
    }

    fn report(&self, table: &str, r: ais_tracks::output::WriteReport) {
        println!(
            "  {}.{table}: {} {} rows ({} files written, {} replaced)",
            self.config.namespace,
            if r.created { "created with" } else { "replaced with" },
            r.rows,
            r.files_added,
            r.files_removed
        );
    }
}

pub async fn run(iceberg: &IcebergCliArgs, a: &EvidenceArgs) -> Result<i32> {
    let common = match &a.command {
        EvidenceCommand::ParisMou(x) => &x.common,
        EvidenceCommand::Uscg(x) => &x.common,
        EvidenceCommand::Tsb(x) => &x.common,
        EvidenceCommand::TokyoMou(x) => &x.common,
        EvidenceCommand::IuuList(x) => &x.common,
        EvidenceCommand::Sources(x) => x,
    };
    let writer = connect(iceberg, common.apply).await?;
    let loaded_at = Utc::now();
    let downloads = match &a.command {
        EvidenceCommand::Uscg(x) => x.source_dir.is_none(),
        EvidenceCommand::Tsb(x) => x.source_dir.is_none(),
        EvidenceCommand::TokyoMou(x) => x.source_dir.is_none(),
        _ => false,
    };

    let result = match &a.command {
        EvidenceCommand::ParisMou(x) => load_paris(writer.as_ref(), x, loaded_at).await,
        EvidenceCommand::Uscg(x) => load_uscg(writer.as_ref(), x, loaded_at).await,
        EvidenceCommand::Tsb(x) => load_tsb(writer.as_ref(), x, loaded_at).await,
        EvidenceCommand::TokyoMou(x) => load_tokyo(writer.as_ref(), x, loaded_at).await,
        EvidenceCommand::IuuList(x) => load_iuu(writer.as_ref(), x, loaded_at).await,
        EvidenceCommand::Sources(_) => match &writer {
            Some(w) => w.write_rows(SOURCES_TABLE, SOURCE_COLUMNS, SOURCES, loaded_at, &[]).await,
            None => {
                for s in SOURCES {
                    println!("{}: {} ({})", s.source, s.name, s.url);
                }
                Ok(())
            }
        },
    };
    if downloads && !common.keep_scratch {
        let _ = std::fs::remove_dir_all(&common.scratch);
    }
    result?;
    if writer.is_none() {
        print_dry_run(iceberg);
    }
    Ok(exitcode::SUCCESS)
}

async fn load_paris<C: Catalog>(writer: Option<&Writer<C>>, a: &ParisArgs, loaded_at: DateTime<Utc>) -> Result<()> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(&a.source_dir)
        .with_context(|| format!("listing {}", a.source_dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| e.eq_ignore_ascii_case("xml") || e.eq_ignore_ascii_case("zip"))
        })
        .collect();
    files.sort();
    anyhow::ensure!(!files.is_empty(), "no .xml or .zip files in {}", a.source_dir.display());

    let mut all = paris::Parsed::default();
    for path in files {
        let label = path.file_name().unwrap().to_string_lossy().into_owned();
        let p = {
            let label = label.clone();
            tokio::task::spawn_blocking(move || paris::read(&path, &label)).await??
        };
        println!("  {label}: {} inspections", p.evidence.events.len());
        all.extend(p);
    }
    evidence::summarize(paris::SOURCE, &all.evidence);
    println!(
        "  {} certificates, {} charterers{}",
        all.certificates.len(),
        all.charterers.len(),
        if all.rejected > 0 { format!(", {} inspections without an id skipped", all.rejected) } else { String::new() }
    );
    if let Some(w) = writer {
        w.write_evidence(paris::SOURCE, &all.evidence, loaded_at).await?;
        if !all.certificates.is_empty() {
            w.write_rows(paris::CERTIFICATES_TABLE, paris::CERTIFICATE_COLUMNS, &all.certificates, loaded_at, &["event_id"]).await?;
        }
        if !all.charterers.is_empty() {
            w.write_rows(paris::CHARTERERS_TABLE, paris::CHARTERER_COLUMNS, &all.charterers, loaded_at, &["event_id"]).await?;
        }
    }
    Ok(())
}

async fn load_uscg<C: Catalog>(writer: Option<&Writer<C>>, a: &UscgArgs, loaded_at: DateTime<Utc>) -> Result<()> {
    let paths: Vec<(PathBuf, String)> = match &a.source_dir {
        Some(dir) => {
            let files = uscg::files_in(dir)?;
            anyhow::ensure!(!files.is_empty(), "no .xlsx files in {}", dir.display());
            files
                .into_iter()
                .map(|p| {
                    let label = p.file_name().unwrap().to_string_lossy().into_owned();
                    (p, label)
                })
                .collect()
        }
        None => {
            let source = Source::with_user_agent("", None, a.common.scratch.clone(), a.user_agent.as_deref())?;
            let names: Vec<String> = if a.file.is_empty() {
                uscg::DEFAULT_FILES.iter().map(|s| s.to_string()).collect()
            } else {
                a.file.clone()
            };
            let mut out = Vec::new();
            for name in names {
                let local = format!("uscg/{}", uscg::local_name(&name));
                let path = source.fetch_url(&uscg::url(&name), &local).await.with_context(|| {
                    format!(
                        "downloading {name}; if the site refuses, save the workbooks from {} into a directory and pass --source-dir",
                        uscg::PAGE
                    )
                })?;
                out.push((path, uscg::local_name(&name)));
            }
            out
        }
    };

    let sheets = tokio::task::spawn_blocking(move || -> Result<Vec<uscg::Sheet>> {
        paths.iter().map(|(p, label)| uscg::read(p, label)).collect()
    })
    .await??;
    let p = uscg::events(&sheets);
    evidence::summarize(uscg::SOURCE, &p.evidence);
    if p.rejected > 0 || p.duplicates > 0 {
        println!("  {} rows without an activity id skipped, {} repeated deficiencies skipped", p.rejected, p.duplicates);
    }
    if let Some(w) = writer {
        w.write_evidence(uscg::SOURCE, &p.evidence, loaded_at).await?;
    }
    Ok(())
}

async fn load_tsb<C: Catalog>(writer: Option<&Writer<C>>, a: &TsbArgs, loaded_at: DateTime<Utc>) -> Result<()> {
    let source = Source::with_user_agent(&a.source_url, a.source_dir.clone(), a.common.scratch.clone(), a.user_agent.as_deref())?;
    let mut raws: Vec<RawTable> = Vec::new();
    for (file, table) in tsb::FILES {
        let path = source.fetch(file).await.with_context(|| {
            format!("fetching {file}; if the TSB refuses the download, pass a browser --user-agent or download the files into --source-dir")
        })?;
        let raw = tokio::task::spawn_blocking(move || tsb::read_raw(&path, table, file)).await??;
        println!("  {file}: {} rows, {} columns", raw.rows.len(), raw.columns.len());
        raws.push(raw);
    }
    let ev = tsb::events(&raws[0], &raws[1])?;
    evidence::summarize(tsb::SOURCE, &ev);
    if let Some(w) = writer {
        w.write_evidence(tsb::SOURCE, &ev, loaded_at).await?;
        if !a.no_raw {
            for raw in &raws {
                w.write_raw(raw, loaded_at).await?;
            }
        }
    }
    Ok(())
}

async fn load_tokyo<C: Catalog>(writer: Option<&Writer<C>>, a: &TokyoArgs, loaded_at: DateTime<Utc>) -> Result<()> {
    let to = a.to.unwrap_or_else(|| tokyo::this_month(Utc::now().date_naive()));
    let months = tokyo::months(a.from, to);
    anyhow::ensure!(!months.is_empty(), "--from is after --to");
    if a.from > tokyo::FIRST_MONTH {
        println!(
            "note: starting at {}-{:02}; the load replaces every Tokyo MoU row, so earlier months will be gone",
            a.from.0, a.from.1
        );
    }
    let source = Source::new("", a.source_dir.clone(), a.common.scratch.clone())?;
    let mut all = Evidence::default();
    let mut seen = tokyo::Seen::new();
    let mut empty_months = Vec::new();
    for (i, &(y, m)) in months.iter().enumerate() {
        if i > 0 && a.source_dir.is_none() {
            tokio::time::sleep(Duration::from_millis(a.delay_ms)).await;
        }
        let path = tokyo::fetch_month(&source, &a.source_url, y, m).await?;
        let label = tokyo::file_name(y, m);
        let ev = tokyo::read(&path, &label, &mut seen)?;
        if ev.events.is_empty() {
            empty_months.push(format!("{y}-{m:02}"));
        }
        all.events.extend(ev.events);
        all.findings.extend(ev.findings);
        if (i + 1) % 24 == 0 {
            println!("  read {} of {} months ({} detentions so far)", i + 1, months.len(), all.events.len());
        }
    }
    evidence::summarize(tokyo::SOURCE, &all);
    if !empty_months.is_empty() {
        println!("  months with no detentions listed: {}", empty_months.join(", "));
    }
    let open = all.events.iter().filter(|e| e.event_end_date.is_none()).count();
    if open > 0 {
        println!("  {open} detentions without a release date");
    }
    if let Some(w) = writer {
        w.write_evidence(tokyo::SOURCE, &all, loaded_at).await?;
    }
    Ok(())
}

async fn load_iuu<C: Catalog>(writer: Option<&Writer<C>>, a: &IuuArgs, loaded_at: DateTime<Utc>) -> Result<()> {
    let mut files = iuu::files_in(&a.source_dir)?;
    anyhow::ensure!(
        !files.is_empty(),
        "no .xlsx or .xls files in {}; save the list from {} there",
        a.source_dir.display(),
        iuu::PAGE
    );
    files.reverse();
    let vessels = tokio::task::spawn_blocking(move || -> Result<Vec<iuu::Vessel>> {
        let mut all = Vec::new();
        for p in &files {
            let label = p.file_name().unwrap().to_string_lossy().into_owned();
            let v = iuu::read(p, &label)?;
            println!("  {label}: {} vessels", v.len());
            all.extend(v);
        }
        Ok(all)
    })
    .await??;
    let p = iuu::events(&vessels);
    evidence::summarize(iuu::SOURCE, &p.evidence);
    let mut by_rfmo: std::collections::BTreeMap<&str, (usize, usize)> = Default::default();
    for e in &p.evidence.events {
        let n = by_rfmo.entry(e.regime.as_str()).or_default();
        if e.event_type == "iuu_listing" { n.0 += 1 } else { n.1 += 1 }
    }
    println!("  {} listed vessels; by RFMO (listings, delistings):", p.vessels);
    for (r, (l, d)) in by_rfmo {
        println!("    {r}: {l}, {d}");
    }
    println!(
        "  {} rows without a listing skipped, {} repeated listings skipped, {} dates given only as a year or month, {} listings without a date",
        p.skipped, p.duplicates, p.partial_dates, p.undated
    );
    if p.status_mismatches > 0 {
        println!("  {} vessels whose CurrentlyListed flag disagrees with their listings", p.status_mismatches);
    }
    if let Some(w) = writer {
        w.write_evidence(iuu::SOURCE, &p.evidence, loaded_at).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, FromArgMatches, Parser};

    #[derive(Parser, Debug)]
    struct T {
        #[command(subcommand)]
        command: EvidenceCommand,
    }

    fn parse(args: &[&str]) -> EvidenceCommand {
        let mut argv = vec!["t"];
        argv.extend_from_slice(args);
        T::from_arg_matches(&T::command().try_get_matches_from(argv).unwrap()).unwrap().command
    }

    #[test]
    fn parses_source_commands() {
        let EvidenceCommand::TokyoMou(t) = parse(&["tokyo-mou", "--from", "2019-03", "--to", "2020-01"]) else {
            panic!()
        };
        assert_eq!((t.from, t.to), ((2019, 3), Some((2020, 1))));
        assert!(!t.common.apply);
        let EvidenceCommand::ParisMou(p) = parse(&["paris-mou", "--source-dir", "/x", "--apply"]) else { panic!() };
        assert!(p.common.apply);
        let EvidenceCommand::Uscg(u) = parse(&["uscg", "--file", "a.xlsx,b.xlsx"]) else { panic!() };
        assert_eq!(u.file, ["a.xlsx", "b.xlsx"]);
        let EvidenceCommand::IuuList(i) = parse(&["iuu-list", "--source-dir", "/x"]) else { panic!() };
        assert_eq!(i.source_dir, PathBuf::from("/x"));
        assert!(parse_month("2020-13").is_err());
    }
}
