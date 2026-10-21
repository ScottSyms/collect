//! `vessel-identity`: the vessel identity registry's collector and
//! reconciler. Writes to namespace `identity` in warehouse `maritimedata`
//! by default. Every command that writes is a dry run unless `--apply`.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use collect_core::exitcode;
use collect_core::iceberg::{open_catalog, IcebergCliArgs, IcebergConfig};
use collect_maint::commit::RestClient;
use iceberg::Catalog;

use vessel_identity::config::Catalogue;
use vessel_identity::ingest::{self, Options, Outcome};
use vessel_identity::registry::{self, Query};
use vessel_identity::tables::{Store, TABLES};

const DEFAULT_WAREHOUSE: &str = "maritimedata";
const DEFAULT_NAMESPACE: &str = "identity";

#[derive(Parser, Debug)]
#[command(
    name = "vessel-identity",
    version,
    about = "Collect public vessel identity data into Iceberg and reconcile it into a registry of physical vessels"
)]
struct Cli {
    /// Defaults here: warehouse `maritimedata`, namespace `identity`.
    #[command(flatten)]
    iceberg: IcebergCliArgs,

    /// Read source configurations from this directory of `*.toml` files
    /// instead of the built-in ones.
    #[arg(long, global = true, env = "VESSEL_IDENTITY_CONFIG_DIR")]
    config_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Create the namespace and every registry table.
    Init(Apply),
    /// The configured sources.
    Sources {
        #[command(subcommand)]
        command: SourcesCommand,
    },
    /// Collect one or more sources into source_records and identity_assertions.
    Ingest(IngestArgs),
    /// Rebuild vessels, identifiers, history, conflicts and proposals from
    /// the assertions, keeping vessel ids stable.
    Reconcile(Apply),
    /// Record a manual identity decision (takes effect at the next reconcile).
    Decide(DecideArgs),
    /// Source freshness and the last run of each source.
    Status,
    /// Find a vessel by identifier or name.
    Lookup(LookupArgs),
    /// Print shell completions to stdout.
    Completions { shell: clap_complete::Shell },
}

#[derive(Subcommand, Debug)]
enum SourcesCommand {
    /// List sources with their licence and whether they may be collected.
    List,
}

#[derive(Args, Debug)]
struct Apply {
    /// Write. Without it, report what would be done.
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct IngestArgs {
    /// Source ids to ingest (see `sources list`).
    #[arg(long = "source", value_delimiter = ',')]
    sources: Vec<String>,

    /// Every collectable source.
    #[arg(long, conflicts_with_all = ["sources", "due"])]
    all: bool,

    /// Every collectable source whose refresh policy says it's due.
    #[arg(long, conflicts_with = "sources")]
    due: bool,

    /// Read the source's files from this directory instead of fetching
    /// them (one source at a time).
    #[arg(long)]
    source_dir: Option<PathBuf>,

    /// Working space kept between runs (PSIX keeps fetched slices here, so
    /// an interrupted fetch resumes).
    #[arg(long, default_value_os_t = std::env::temp_dir().join("vessel-identity"))]
    scratch: PathBuf,

    /// Ingest even if the content matches the last successful run.
    #[arg(long)]
    force: bool,

    /// Apply a run that removes more of the source than its
    /// max_removal_fraction allows.
    #[arg(long)]
    accept_removals: bool,

    /// User-Agent to send (default: vessel-identity/<version>).
    #[arg(long, env = "VESSEL_IDENTITY_USER_AGENT")]
    user_agent: Option<String>,

    /// Archive fetched objects under this URI (default:
    /// s3://<bucket>/raw_archive/<namespace>).
    #[arg(long, env = "VESSEL_IDENTITY_RAW_ARCHIVE")]
    raw_archive: Option<String>,

    /// Then run reconcile.
    #[arg(long)]
    reconcile: bool,

    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct DecideArgs {
    #[command(subcommand)]
    kind: DecideKind,
}

#[derive(Subcommand, Debug)]
enum DecideKind {
    /// Two subjects are different vessels: never link them.
    KeepApart(Pair),
    /// Two subjects are the same vessel.
    Link(Pair),
    /// Withdraw an earlier decision.
    Revoke {
        decision_id: String,
        #[command(flatten)]
        common: DecisionCommon,
    },
}

#[derive(Args, Debug)]
struct Pair {
    /// `<source_id>/<subject_id>`, as in vessel_subjects.
    a: String,
    b: String,
    #[command(flatten)]
    common: DecisionCommon,
}

#[derive(Args, Debug)]
struct DecisionCommon {
    /// Why (kept in the decision log).
    #[arg(long)]
    reason: String,
    /// Who decided (default: $USER).
    #[arg(long, env = "USER")]
    by: String,
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
#[group(required = true, multiple = false)]
struct LookupArgs {
    #[arg(long)]
    imo: Option<String>,
    #[arg(long)]
    mmsi: Option<String>,
    #[arg(long)]
    callsign: Option<String>,
    /// National registration (official number).
    #[arg(long)]
    registration: Option<String>,
    /// Current or former name.
    #[arg(long)]
    name: Option<String>,
    /// A vessel id, including a retired one.
    #[arg(long)]
    id: Option<String>,
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

fn command() -> clap::Command {
    Cli::command()
        .mut_arg("iceberg_warehouse", |a| a.default_value(DEFAULT_WAREHOUSE))
        .mut_arg("iceberg_namespace", |a| a.default_value(DEFAULT_NAMESPACE))
}

/// The store, when a catalog is configured. `required` makes a missing
/// catalog an error.
async fn connect(iceberg: &IcebergCliArgs, required: bool) -> Result<Option<Store<impl Catalog>>> {
    if !iceberg.is_iceberg_mode() {
        anyhow::ensure!(!required, "--iceberg-catalog-uri (or ICEBERG_CATALOG_URI) is required");
        return Ok(None);
    }
    iceberg.validate()?;
    let config = IcebergConfig::from(iceberg);
    let catalog = open_catalog(&config).await?;
    let rest = RestClient::connect(&config).await?;
    Ok(Some(Store { catalog, rest, config }))
}

fn dry_run_note(iceberg: &IcebergCliArgs) {
    println!(
        "dry run; pass --apply to write to {}.{}",
        iceberg.iceberg_warehouse.as_deref().unwrap_or(DEFAULT_WAREHOUSE),
        iceberg.iceberg_namespace
    );
}

async fn run() -> Result<i32> {
    let cli = Cli::from_arg_matches(&command().get_matches())?;
    let cat = Catalogue::load(cli.config_dir.as_deref())?;
    match &cli.command {
        Command::Completions { shell } => {
            clap_complete::generate(*shell, &mut command(), "vessel-identity", &mut std::io::stdout());
        }
        Command::Sources { command: SourcesCommand::List } => {
            for (c, hash) in cat.sources.values() {
                println!("{}  [{}] {}", c.source_id, c.adapter, c.name);
                println!("    {} | {} | refresh {} | priority {}", c.license, c.access_status, c.refresh_policy, c.priority);
                println!("    permitted: {}", c.permitted_uses.join(", "));
                match c.refusal() {
                    None => println!("    collectable (config {hash})"),
                    Some(why) => println!("    not collected: {why}"),
                }
            }
        }
        Command::Init(a) => {
            if !a.apply {
                for t in TABLES {
                    println!("{}", t.name);
                }
                dry_run_note(&cli.iceberg);
                return Ok(exitcode::SUCCESS);
            }
            let store = connect(&cli.iceberg, true).await?.expect("required");
            let created = store.init().await?;
            println!("created {} table(s){}", created.len(), if created.is_empty() { "" } else { ":" });
            for t in created {
                println!("  {}.{t}", store.config.namespace);
            }
            ingest::refresh_catalog(&store, &cat).await?;
        }
        Command::Ingest(a) => return ingest_cmd(&cli, &cat, a).await,
        Command::Reconcile(a) => {
            let store = connect(&cli.iceberg, true).await?.expect("required");
            registry::reconcile(&store, &cat, a.apply).await?;
            if !a.apply {
                dry_run_note(&cli.iceberg);
            }
        }
        Command::Decide(d) => {
            let store = connect(&cli.iceberg, true).await?.expect("required");
            let (kind, subjects, revokes, common) = match &d.kind {
                DecideKind::KeepApart(p) => ("KEEP_APART", vec![p.a.clone(), p.b.clone()], None, &p.common),
                DecideKind::Link(p) => ("LINK", vec![p.a.clone(), p.b.clone()], None, &p.common),
                DecideKind::Revoke { decision_id, common } => ("REVOKE", Vec::new(), Some(decision_id.as_str()), common),
            };
            let target = common.apply.then_some(&store);
            let d = registry::decide(target, kind, &subjects, revokes, &common.by, &common.reason).await?;
            println!("{} {} {}", d.decision_type, d.decision_id, d.subjects.join(" "));
            if !common.apply {
                dry_run_note(&cli.iceberg);
            }
        }
        Command::Status => {
            let store = connect(&cli.iceberg, false).await?;
            ingest::status(store.as_ref(), &cat).await?;
        }
        Command::Lookup(l) => {
            let store = connect(&cli.iceberg, true).await?.expect("required");
            let q = if let Some(v) = &l.imo {
                Query::Imo(v.clone())
            } else if let Some(v) = &l.mmsi {
                Query::Mmsi(v.clone())
            } else if let Some(v) = &l.callsign {
                Query::Callsign(v.clone())
            } else if let Some(v) = &l.registration {
                Query::Registration(v.clone())
            } else if let Some(v) = &l.name {
                Query::Name(v.clone())
            } else {
                Query::VesselId(l.id.clone().unwrap_or_default())
            };
            if registry::lookup(&store, &q).await? == 0 {
                return Ok(exitcode::UNCLASSIFIED_ERROR);
            }
        }
    }
    Ok(exitcode::SUCCESS)
}

async fn ingest_cmd(cli: &Cli, cat: &Catalogue, a: &IngestArgs) -> Result<i32> {
    // Dry runs still read the store when one is configured, to report the
    // real delta; --apply needs it.
    let store = connect(&cli.iceberg, a.apply).await?;
    let ids: Vec<String> = if a.all {
        cat.sources.values().filter(|(c, _)| c.refusal().is_none()).map(|(c, _)| c.source_id.clone()).collect()
    } else if a.due {
        match &store {
            Some(s) => ingest::due(s, cat).await?,
            None => anyhow::bail!("--due needs the catalog to know when sources last ran"),
        }
    } else {
        anyhow::ensure!(!a.sources.is_empty(), "name sources with --source, or pass --all or --due");
        a.sources.clone()
    };
    anyhow::ensure!(a.source_dir.is_none() || ids.len() == 1, "--source-dir reads one source's files; name one source");
    if ids.is_empty() {
        println!("nothing to ingest");
        return Ok(exitcode::SUCCESS);
    }
    let opts = Options {
        apply: a.apply,
        force: a.force,
        accept_removals: a.accept_removals,
        source_dir: a.source_dir.clone(),
        scratch: a.scratch.clone(),
        user_agent: a.user_agent.clone(),
        raw_archive: a.raw_archive.clone(),
    };
    // One source failing doesn't stop the others.
    let mut failed = Vec::new();
    let mut quarantined = Vec::new();
    for id in &ids {
        match ingest::ingest(store.as_ref(), cat, id, &opts).await {
            Ok(Outcome::Quarantined) => quarantined.push(id.clone()),
            Ok(_) => {}
            Err(e) => {
                eprintln!("{id}: failed: {e:#}");
                failed.push(id.clone());
            }
        }
    }
    if a.reconcile && a.apply {
        if let Some(s) = &store {
            registry::reconcile(s, cat, true).await?;
        }
    }
    if !a.apply {
        dry_run_note(&cli.iceberg);
    }
    if !failed.is_empty() || !quarantined.is_empty() {
        eprintln!("failed: [{}]  quarantined: [{}]", failed.join(", "), quarantined.join(", "));
        return Ok(exitcode::UNCLASSIFIED_ERROR);
    }
    Ok(exitcode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        let mut argv = vec!["vessel-identity"];
        argv.extend_from_slice(args);
        Cli::from_arg_matches(&command().try_get_matches_from(argv).unwrap()).unwrap()
    }

    #[test]
    fn defaults_to_maritimedata_identity() {
        let cli = parse(&["status"]);
        assert_eq!(cli.iceberg.iceberg_warehouse.as_deref(), Some("maritimedata"));
        assert_eq!(cli.iceberg.iceberg_namespace, "identity");
    }

    #[test]
    fn parses_ingest_and_decide() {
        let cli = parse(&["ingest", "--source", "tc_large_vessels,tc_small_vessels", "--apply", "--reconcile"]);
        let Command::Ingest(a) = cli.command else { panic!() };
        assert_eq!(a.sources, ["tc_large_vessels", "tc_small_vessels"]);
        assert!(a.apply && a.reconcile);
        let cli = parse(&["decide", "keep-apart", "tc_large_vessels/1", "uscg_psix/2", "--reason", "different hulls", "--by", "me"]);
        assert!(matches!(cli.command, Command::Decide(_)));
        assert!(command().try_get_matches_from(["vessel-identity", "ingest", "--all", "--source", "x"]).is_err());
        assert!(command().try_get_matches_from(["vessel-identity", "lookup"]).is_err());
    }
}
