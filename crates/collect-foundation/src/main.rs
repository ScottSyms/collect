//! `collect-foundation`: loads foundation (reference) datasets into Iceberg,
//! by default namespace `foundation` in warehouse `maritimedata`: the GeoNames
//! gazetteer, and vessel evidence (port state control inspections and
//! detentions, marine occurrences) from several publishers. Every command is
//! a dry run unless `--apply`.

mod download;
mod evidence;
mod evidence_cli;
mod geonames;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use ais_tracks::output::replace_table;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use collect_core::exitcode;
use collect_core::iceberg::{open_catalog, IcebergCliArgs, IcebergConfig};
use collect_maint::commit::RestClient;
use iceberg::Catalog;

use download::{with_text, Source};
use geonames::{Dataset, Filter, Parsed};

const DEFAULT_WAREHOUSE: &str = "maritimedata";
const DEFAULT_NAMESPACE: &str = "foundation";

#[derive(Parser, Debug)]
#[command(
    name = "collect-foundation",
    version,
    about = "Load foundation reference data (GeoNames placenames, vessel evidence) into Iceberg (dry run unless --apply)"
)]
struct Cli {
    /// Defaults here: warehouse `maritimedata`, namespace `foundation`.
    #[command(flatten)]
    iceberg: IcebergCliArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// The GeoNames gazetteer (geonames.org, CC-BY 4.0).
    Placenames(PlacenamesArgs),
    /// Vessel evidence: PSC inspections and detentions, marine occurrences
    /// and enforcement actions, into `vessel_events` and
    /// `vessel_event_findings` (one partition per source).
    #[command(visible_alias = "interdictions")]
    Evidence(evidence_cli::EvidenceArgs),
    /// Print shell completions to stdout.
    Completions { shell: clap_complete::Shell },
}

#[derive(Args, Debug)]
struct PlacenamesArgs {
    #[command(subcommand)]
    command: PlacenamesCommand,
}

#[derive(Subcommand, Debug)]
enum PlacenamesCommand {
    /// Download the GeoNames dump and replace the `placenames` table and its
    /// lookup tables (feature codes, countries, admin1/admin2 codes).
    Load(LoadArgs),
}

#[derive(Args, Debug)]
struct LoadArgs {
    /// GeoNames dump directory.
    #[arg(long, env = "GEONAMES_URL", default_value = "https://download.geonames.org/export/dump/")]
    source_url: String,

    /// Read the dump files from this directory (same names and layout as the
    /// server) instead of downloading them.
    #[arg(long)]
    source_dir: Option<PathBuf>,

    /// Load only these countries (ISO 3166 alpha-2, comma-separated), from
    /// the per-country files. Default: the whole world (`allCountries.zip`).
    #[arg(long, value_delimiter = ',')]
    country: Vec<String>,

    /// Keep only these feature classes (comma-separated: A admin, H water,
    /// L area, P populated place, R road/rail, S spot, T terrain, U undersea,
    /// V vegetation).
    #[arg(long, value_delimiter = ',')]
    feature_class: Vec<String>,

    /// Also load `placename_alternate_names` (names in other languages,
    /// historic and colloquial names), for the places loaded.
    #[arg(long)]
    alternate_names: bool,

    /// Where downloads go.
    #[arg(long, default_value_os_t = std::env::temp_dir().join("collect-foundation"))]
    scratch: PathBuf,

    /// Keep the downloaded files afterwards.
    #[arg(long)]
    keep_scratch: bool,

    /// Write the tables. Without it, download, parse and report only.
    #[arg(long)]
    apply: bool,
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::exit(exitcode::UNCLASSIFIED_ERROR);
        }
    }
}

/// The CLI with this binary's warehouse and namespace defaults in place of
/// the shared `IcebergCliArgs` ones.
fn command() -> clap::Command {
    Cli::command()
        .mut_arg("iceberg_warehouse", |a| a.default_value(DEFAULT_WAREHOUSE))
        .mut_arg("iceberg_namespace", |a| a.default_value(DEFAULT_NAMESPACE))
}

async fn run() -> Result<i32> {
    let cli = Cli::from_arg_matches(&command().get_matches())?;
    match &cli.command {
        Command::Completions { shell } => {
            clap_complete::generate(*shell, &mut command(), "collect-foundation", &mut std::io::stdout());
            Ok(exitcode::SUCCESS)
        }
        Command::Placenames(PlacenamesArgs { command: PlacenamesCommand::Load(a) }) => {
            load_placenames(&cli.iceberg, a).await
        }
        Command::Evidence(a) => evidence_cli::run(&cli.iceberg, a).await,
    }
}

/// Where and how tables are written, present only with `--apply`.
pub(crate) struct Writer<C> {
    pub(crate) catalog: C,
    pub(crate) rest: RestClient,
    pub(crate) config: IcebergConfig,
}

/// Connects to the catalog when `apply`; fails fast on a bad catalog.
pub(crate) async fn connect(
    iceberg: &IcebergCliArgs,
    apply: bool,
) -> Result<Option<Writer<impl Catalog>>> {
    if !apply {
        return Ok(None);
    }
    iceberg.validate()?;
    anyhow::ensure!(iceberg.is_iceberg_mode(), "--iceberg-catalog-uri is required with --apply");
    let config = IcebergConfig::from(iceberg);
    let catalog = open_catalog(&config).await?;
    let rest = RestClient::connect(&config).await?;
    Ok(Some(Writer { catalog, rest, config }))
}

pub(crate) fn print_dry_run(iceberg: &IcebergCliArgs) {
    println!(
        "dry run; pass --apply to write to {}.{}",
        iceberg.iceberg_warehouse.as_deref().unwrap_or(DEFAULT_WAREHOUSE),
        iceberg.iceberg_namespace
    );
}

impl<C: Catalog> Writer<C> {
    async fn write(&self, ds: &Dataset, parsed: &Parsed, bloom: &[&str]) -> Result<()> {
        let r = replace_table(
            &self.catalog,
            &self.rest,
            &self.config,
            ds.table,
            ds.schema(),
            &parsed.batches,
            bloom,
        )
        .await
        .with_context(|| format!("writing {}", ds.table))?;
        println!(
            "  {}.{}: {} {} rows ({} files written, {} replaced)",
            self.config.namespace,
            ds.table,
            if r.created { "created with" } else { "replaced with" },
            r.rows,
            r.files_added,
            r.files_removed
        );
        Ok(())
    }
}

fn normalize(values: &[String], what: &str, valid: impl Fn(&str) -> bool) -> Result<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for v in values.iter().map(|v| v.trim().to_ascii_uppercase()).filter(|v| !v.is_empty()) {
        anyhow::ensure!(valid(&v), "invalid {what} '{v}'");
        if !out.contains(&v) {
            out.push(v);
        }
    }
    Ok(out)
}

async fn load_placenames(iceberg: &IcebergCliArgs, a: &LoadArgs) -> Result<i32> {
    let countries = normalize(&a.country, "country code", |c| {
        c.len() == 2 && c.bytes().all(|b| b.is_ascii_uppercase())
    })?;
    let classes = normalize(&a.feature_class, "feature class", |c| {
        c.len() == 1 && "AHLPRSTUV".contains(c)
    })?;

    // Connect before downloading anything so a bad catalog fails fast.
    let writer = connect(iceberg, a.apply).await?;

    let source = Source::new(&a.source_url, a.source_dir.clone(), a.scratch.clone())?;
    let loaded_at = Utc::now();
    let result = load_all(&source, writer.as_ref(), a, &countries, &classes, loaded_at).await;
    if a.source_dir.is_none() && !a.keep_scratch {
        let _ = std::fs::remove_dir_all(&a.scratch);
    }
    result?;

    if writer.is_none() {
        print_dry_run(iceberg);
    }
    Ok(exitcode::SUCCESS)
}

async fn load_all<C: Catalog>(
    source: &Source,
    writer: Option<&Writer<C>>,
    a: &LoadArgs,
    countries: &[String],
    classes: &[String],
    loaded_at: DateTime<Utc>,
) -> Result<()> {
    let no_filter = Arc::new(Filter::default());

    for ds in geonames::LOOKUPS {
        let parsed = fetch_and_parse(source, ds, &[ds.file.to_string()], loaded_at, no_filter.clone()).await?;
        if let Some(w) = writer {
            w.write(ds, &parsed, &[]).await?;
        }
    }

    let files: Vec<String> = if countries.is_empty() {
        vec!["allCountries.zip".to_string()]
    } else {
        countries.iter().map(|c| format!("{c}.zip")).collect()
    };
    let filter = Arc::new(Filter {
        feature_classes: (!classes.is_empty()).then(|| classes.iter().cloned().collect()),
        geonameids: None,
    });
    let ds = &geonames::PLACENAMES;
    let parsed = fetch_and_parse(source, ds, &files, loaded_at, filter).await?;
    if let Some(w) = writer {
        w.write(ds, &parsed, &["geonameid", "country_code"]).await?;
    }
    if !a.alternate_names {
        return Ok(());
    }

    // Keep alternate names to the places loaded, so a filtered load doesn't
    // carry names for places that aren't there. A full load takes them all.
    let narrowed = !countries.is_empty() || !classes.is_empty();
    let ids: Option<HashSet<i64>> = narrowed.then(|| geonames::geonameids(&parsed.batches));
    drop(parsed);
    let files: Vec<String> = if countries.is_empty() {
        vec!["alternateNamesV2.zip".to_string()]
    } else {
        countries.iter().map(|c| format!("alternatenames/{c}.zip")).collect()
    };
    let filter = Arc::new(Filter { feature_classes: None, geonameids: ids });
    let ds = &geonames::ALTERNATE_NAMES;
    let parsed = fetch_and_parse(source, ds, &files, loaded_at, filter).await?;
    if let Some(w) = writer {
        w.write(ds, &parsed, &["geonameid"]).await?;
    }
    Ok(())
}

/// Fetches each of `files` and parses it as `ds`, printing what was read.
async fn fetch_and_parse(
    source: &Source,
    ds: &'static Dataset,
    files: &[String],
    loaded_at: DateTime<Utc>,
    filter: Arc<Filter>,
) -> Result<Parsed> {
    let mut all = Parsed::default();
    for name in files {
        let path = source.fetch(name).await?;
        let label = name.clone();
        let filter = filter.clone();
        let parsed = tokio::task::spawn_blocking(move || {
            with_text(&path, |r| geonames::parse(ds, r, &label, loaded_at, &filter))
        })
        .await??;
        all.extend(parsed);
    }
    let mut notes = Vec::new();
    if all.filtered > 0 {
        notes.push(format!("{} filtered out", all.filtered));
    }
    if all.rejected > 0 {
        notes.push(format!("{} lines rejected", all.rejected));
    }
    if all.bad_values > 0 {
        notes.push(format!("{} unparseable values stored as null", all.bad_values));
    }
    let notes = if notes.is_empty() { String::new() } else { format!(" ({})", notes.join(", ")) };
    println!("{}: {} rows from {}{notes}", ds.table, all.rows, files.join(", "));
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        let mut argv = vec!["collect-foundation"];
        argv.extend_from_slice(args);
        Cli::from_arg_matches(&command().try_get_matches_from(argv).unwrap()).unwrap()
    }

    #[test]
    fn defaults_to_maritimedata_foundation() {
        let cli = parse(&["placenames", "load"]);
        assert_eq!(cli.iceberg.iceberg_warehouse.as_deref(), Some("maritimedata"));
        assert_eq!(cli.iceberg.iceberg_namespace, "foundation");
    }

    #[test]
    fn warehouse_and_namespace_are_configurable() {
        let cli = parse(&[
            "--iceberg-warehouse", "other", "--iceberg-namespace", "ref", "placenames", "load",
            "--country", "no,ca", "--feature-class", "H",
        ]);
        assert_eq!(cli.iceberg.iceberg_warehouse.as_deref(), Some("other"));
        assert_eq!(cli.iceberg.iceberg_namespace, "ref");
        let Command::Placenames(PlacenamesArgs { command: PlacenamesCommand::Load(a) }) = cli.command else {
            panic!("expected placenames load");
        };
        assert_eq!(a.country, ["no", "ca"]);
    }

    #[test]
    fn normalizes_and_validates_codes() {
        let ok = |c: &str| c.len() == 2;
        assert_eq!(normalize(&["no".into(), " NO".into(), "ca".into()], "x", ok).unwrap(), ["NO", "CA"]);
        assert!(normalize(&["NOR".into()], "x", ok).is_err());
    }
}
