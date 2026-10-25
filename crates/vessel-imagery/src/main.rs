//! `vessel-imagery`: ship photos for the identity registry's vessels, and
//! the visual attributes a System 1 model reads from them. Writes to
//! namespace `imagery` in warehouse `maritimedata` by default and reads the
//! registry from namespace `identity`. Every command that writes is a dry
//! run unless `--apply`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use collect_core::exitcode;
use collect_core::iceberg::{open_catalog, IcebergCliArgs, IcebergConfig};
use collect_maint::commit::RestClient;
use iceberg::Catalog;

use vessel_imagery::collect::{self, FetchOptions};
use vessel_imagery::commons::Commons;
use vessel_imagery::describe::{self, DescribeOptions, ProfileOptions};
use vessel_imagery::identity::{Target, Targets};
use vessel_imagery::images;
use vessel_imagery::questions::QuestionSet;
use vessel_imagery::systemone::Client;
use vessel_imagery::tables::{Store, IMAGES, OBSERVATIONS, PROFILE, PROFILE_COLUMNS, TABLES};

const DEFAULT_WAREHOUSE: &str = "maritimedata";
const DEFAULT_NAMESPACE: &str = "imagery";

#[derive(Parser, Debug)]
#[command(
    name = "vessel-imagery",
    version,
    about = "Collect ship photos for registered vessels and read their visual attributes with a System 1 model"
)]
struct Cli {
    /// Defaults here: warehouse `maritimedata`, namespace `imagery`.
    #[command(flatten)]
    iceberg: IcebergCliArgs,

    /// The identity registry's namespace, read for the vessels.
    #[arg(long, global = true, env = "VESSEL_IMAGERY_IDENTITY_NAMESPACE", default_value = "identity")]
    identity_namespace: String,

    /// Read question sets from `<dir>/questions/<version>.toml` instead of
    /// the built-in ones.
    #[arg(long, global = true, env = "VESSEL_IMAGERY_CONFIG_DIR")]
    config_dir: Option<PathBuf>,

    /// Store image bytes under this URI instead of
    /// `s3://<bucket>/raw_archive/<namespace>`.
    #[arg(long, global = true, env = "VESSEL_IMAGERY_ARCHIVE_ROOT")]
    archive_root: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Create the namespace and the imagery tables.
    Init(Apply),
    /// Find and download freely licensed photos.
    Fetch {
        #[command(subcommand)]
        source: FetchSource,
    },
    /// Record photos already held (licensed or restricted) from a CSV manifest.
    Import(ImportArgs),
    /// Ask the System 1 model each question about each photo not yet asked.
    Describe(DescribeArgs),
    /// Rebuild vessel_visual_profile from the observations.
    Profile(ProfileArgs),
    /// Score the model's answers against hand labels, per question.
    Eval(EvalArgs),
    /// Images, observations and profiles held, by source and licence.
    Status,
    /// One vessel's photos, licences and visual profile.
    Lookup(LookupArgs),
    /// Print shell completions to stdout.
    Completions { shell: clap_complete::Shell },
}

#[derive(Subcommand, Debug)]
enum FetchSource {
    /// Wikimedia Commons, through its `Category:IMO <number>` categories.
    Commons(FetchArgs),
}

#[derive(Args, Debug)]
struct Apply {
    /// Write. Without it, report what would be done.
    #[arg(long)]
    apply: bool,
}

#[derive(Args, Debug)]
struct Select {
    /// Only this IMO number.
    #[arg(long)]
    imo: Option<String>,
    /// Only this registry vessel id.
    #[arg(long, conflicts_with = "imo")]
    vessel_id: Option<String>,
}

#[derive(Args, Debug)]
struct FetchArgs {
    #[command(flatten)]
    select: Select,
    /// At most this many vessels.
    #[arg(long)]
    limit: Option<usize>,
    /// At most this many photos per vessel.
    #[arg(long, default_value_t = 10)]
    per_vessel: usize,
    /// Subcategory levels to walk below `Category:IMO <n>`.
    #[arg(long, default_value_t = 2)]
    depth: usize,
    /// Look again at vessels that already have Commons photos.
    #[arg(long)]
    refresh: bool,
    /// Contact details for the User-Agent Wikimedia requires (an email
    /// address or URL).
    #[arg(long, env = "VESSEL_IMAGERY_CONTACT")]
    contact: String,
    /// Pause between requests.
    #[arg(long, env = "VESSEL_IMAGERY_COMMONS_DELAY_MS", default_value_t = 1000)]
    delay_ms: u64,
    /// Thumbnail width to download (the model takes at most 1568 px).
    #[arg(long, default_value_t = 1280)]
    thumb_width: u32,
    /// Stop after this many requests to Commons.
    #[arg(long, env = "VESSEL_IMAGERY_COMMONS_MAX_REQUESTS", default_value_t = 2000)]
    max_requests: usize,
    #[arg(long, hide = true)]
    commons_api: Option<String>,
    #[command(flatten)]
    apply: Apply,
}

#[derive(Args, Debug)]
struct ImportArgs {
    /// CSV with columns path, imo|mmsi|vessel_id, source, licence and
    /// optionally licence_url, attribution, author, source_url,
    /// captured_at, restricted (default true).
    #[arg(long)]
    manifest: PathBuf,
    #[command(flatten)]
    apply: Apply,
}

#[derive(Args, Debug)]
struct ModelArgs {
    /// Question set version.
    #[arg(long, default_value = "v1")]
    question_set: String,
    /// The model the System 1 server is asked to use; recorded on every
    /// observation.
    #[arg(long, env = "VESSEL_IMAGERY_MODEL", default_value = "laya-vision")]
    model: String,
}

#[derive(Args, Debug)]
struct DescribeArgs {
    #[command(flatten)]
    model: ModelArgs,
    /// The Jev-style server's root URL (`POST <url>/v1/systemone`).
    #[arg(long, env = "VESSEL_IMAGERY_SYSTEMONE_URL", default_value = "http://localhost:8765")]
    systemone_url: String,
    /// Bearer token for the server, if it wants one.
    #[arg(long, env = "VESSEL_IMAGERY_SYSTEMONE_TOKEN", hide_env_values = true)]
    systemone_token: Option<String>,
    /// At most this many images.
    #[arg(long)]
    limit: Option<usize>,
    /// Don't send restricted images (for a hosted model their licence
    /// doesn't cover).
    #[arg(long)]
    skip_restricted: bool,
    #[command(flatten)]
    apply: Apply,
}

#[derive(Args, Debug)]
struct ProfileArgs {
    #[command(flatten)]
    model: ModelArgs,
    /// Also count photos whose licence couldn't be classified.
    #[arg(long)]
    include_unknown_licence: bool,
    #[command(flatten)]
    apply: Apply,
}

#[derive(Args, Debug)]
struct EvalArgs {
    #[command(flatten)]
    model: ModelArgs,
    /// CSV with columns image_id, question, expected.
    #[arg(long)]
    labels: PathBuf,
    /// Flag questions whose accuracy is below this.
    #[arg(long, default_value_t = 0.8)]
    threshold: f64,
}

#[derive(Args, Debug)]
struct LookupArgs {
    #[command(flatten)]
    select: Select,
    /// Also print every photo's raw model answers.
    #[arg(long)]
    answers: bool,
}

fn command() -> clap::Command {
    Cli::command()
        .mut_arg("iceberg_warehouse", |a| a.default_value(DEFAULT_WAREHOUSE))
        .mut_arg("iceberg_namespace", |a| a.default_value(DEFAULT_NAMESPACE))
}

struct Stores<C> {
    imagery: Store<C>,
    identity: vessel_identity::tables::Store<C>,
}

async fn connect(cli: &Cli) -> Result<Stores<impl Catalog>> {
    anyhow::ensure!(cli.iceberg.is_iceberg_mode(), "--iceberg-catalog-uri (or ICEBERG_CATALOG_URI) is required");
    cli.iceberg.validate()?;
    let config = IcebergConfig::from(&cli.iceberg);
    let mut id_config = IcebergConfig::from(&cli.iceberg);
    id_config.namespace = cli.identity_namespace.clone();
    let imagery = Store { catalog: open_catalog(&config).await?, rest: RestClient::connect(&config).await?, config };
    let identity = vessel_identity::tables::Store {
        catalog: open_catalog(&id_config).await?,
        rest: RestClient::connect(&id_config).await?,
        config: id_config,
    };
    Ok(Stores { imagery, identity })
}

fn dry_run_note(cli: &Cli) {
    println!(
        "dry run; pass --apply to write to {}.{}",
        cli.iceberg.iceberg_warehouse.as_deref().unwrap_or(DEFAULT_WAREHOUSE),
        cli.iceberg.iceberg_namespace
    );
}

fn select<'a>(targets: &'a Targets, s: &Select) -> Result<Vec<&'a Target>> {
    Ok(match (&s.imo, &s.vessel_id) {
        (Some(imo), _) => vec![targets.by_imo(imo).with_context(|| format!("no vessel with IMO {imo} in the registry"))?],
        (_, Some(id)) => vec![targets.by_id(id).with_context(|| format!("no vessel {id} in the registry"))?],
        _ => targets.vessels.iter().collect(),
    })
}

async fn run() -> Result<i32> {
    let cli = Cli::from_arg_matches(&command().get_matches())?;
    match &cli.command {
        Command::Completions { shell } => {
            clap_complete::generate(*shell, &mut command(), "vessel-imagery", &mut std::io::stdout());
        }
        Command::Init(a) => {
            if !a.apply {
                TABLES.iter().for_each(|t| println!("{}", t.name));
                dry_run_note(&cli);
                return Ok(exitcode::SUCCESS);
            }
            let s = connect(&cli).await?;
            let created = s.imagery.init().await?;
            println!("created {} table(s){}", created.len(), if created.is_empty() { "" } else { ":" });
            for t in created {
                println!("  {}.{t}", s.imagery.config.namespace);
            }
        }
        Command::Fetch { source: FetchSource::Commons(a) } => {
            let s = connect(&cli).await?;
            let targets = Targets::load(&s.identity).await?;
            println!("{} vessel(s) in {}", targets.vessels.len(), s.identity.config.namespace);
            let chosen = select(&targets, &a.select)?;
            let ua = format!("vessel-imagery/{} ({}) reqwest", env!("CARGO_PKG_VERSION"), a.contact);
            let mut commons = Commons::new(&ua, a.commons_api.as_deref(), Duration::from_millis(a.delay_ms), a.thumb_width, a.max_requests)?;
            let opts = FetchOptions {
                apply: a.apply.apply,
                limit: a.limit,
                per_vessel: a.per_vessel,
                depth: a.depth,
                refresh: a.refresh || a.select.imo.is_some() || a.select.vessel_id.is_some(),
                archive_root: cli.archive_root.clone(),
            };
            collect::fetch_commons(Some(&s.imagery), &mut commons, &chosen, &opts).await?;
            if !a.apply.apply {
                dry_run_note(&cli);
            }
        }
        Command::Import(a) => {
            let s = connect(&cli).await?;
            let targets = Targets::load(&s.identity).await?;
            collect::import(Some(&s.imagery), &targets, &a.manifest, a.apply.apply, cli.archive_root.as_deref()).await?;
            if !a.apply.apply {
                dry_run_note(&cli);
            }
        }
        Command::Describe(a) => {
            let qs = QuestionSet::load(&a.model.question_set, cli.config_dir.as_deref())?;
            let s = connect(&cli).await?;
            let client = Client::new(&a.systemone_url, a.systemone_token.clone())?;
            let opts = DescribeOptions { apply: a.apply.apply, limit: a.limit, model: &a.model.model, skip_restricted: a.skip_restricted };
            describe::describe(&s.imagery, &client, &qs, &opts).await?;
            if !a.apply.apply {
                dry_run_note(&cli);
            }
        }
        Command::Profile(a) => {
            let qs = QuestionSet::load(&a.model.question_set, cli.config_dir.as_deref())?;
            let s = connect(&cli).await?;
            let imgs = images::rows(&s.imagery.read(IMAGES).await?)?;
            let obs = describe::observation_rows(&s.imagery.read(OBSERVATIONS).await?)?;
            let rows = describe::profile(&qs, &a.model.model, &imgs, &obs, &ProfileOptions { include_unknown_licence: a.include_unknown_licence });
            let vessels: std::collections::HashSet<&str> = rows.iter().map(|r| r.vessel_id.as_str()).collect();
            println!("{} attribute(s) for {} vessel(s); {} conflicting", rows.len(), vessels.len(), rows.iter().filter(|r| r.conflicting).count());
            if a.apply.apply {
                s.imagery.replace(PROFILE, PROFILE_COLUMNS, &rows).await?;
                println!("replaced {PROFILE}");
            } else {
                dry_run_note(&cli);
            }
        }
        Command::Eval(a) => {
            let s = connect(&cli).await?;
            let labels = describe::read_labels(&a.labels)?;
            let obs = describe::observation_rows(&s.imagery.read(OBSERVATIONS).await?)?;
            let scores = describe::score(&labels, &obs, &a.model.model, &a.model.question_set);
            println!("{:<26} {:>8} {:>8} {:>8} {:>9}", "question", "labelled", "answered", "correct", "accuracy");
            let mut weak = Vec::new();
            for (q, (n, answered, correct)) in &scores {
                let acc = if *answered > 0 { *correct as f64 / *answered as f64 } else { 0.0 };
                println!("{q:<26} {n:>8} {answered:>8} {correct:>8} {:>8.0}%", acc * 100.0);
                if acc < a.threshold {
                    weak.push(q.as_str());
                }
            }
            if !weak.is_empty() {
                println!("\nbelow {:.0}%: set `enabled = false` for {} in the question set", a.threshold * 100.0, weak.join(", "));
            }
        }
        Command::Status => {
            let s = connect(&cli).await?;
            let imgs = images::rows(&s.imagery.read(IMAGES).await?)?;
            let obs = describe::observation_rows(&s.imagery.read(OBSERVATIONS).await?)?;
            let mut by: BTreeMap<(String, String), usize> = BTreeMap::new();
            for i in &imgs {
                *by.entry((i.source.clone(), i.licence_class.clone())).or_default() += 1;
            }
            let vessels: std::collections::HashSet<&str> = imgs.iter().map(|i| i.vessel_id.as_str()).collect();
            println!("{} image(s) of {} vessel(s)", imgs.len(), vessels.len());
            for ((src, class), n) in by {
                println!("  {src:<20} {class:<14} {n}");
            }
            let mut asked: BTreeMap<(String, String), std::collections::HashSet<&str>> = BTreeMap::new();
            for o in &obs {
                asked.entry((o.model_id.clone(), o.question_set_version.clone())).or_default().insert(o.image_id.as_str());
            }
            for ((m, v), set) in asked {
                println!("described by {m} ({v}): {} image(s)", set.len());
            }
            let profile = s.imagery.read(PROFILE).await?;
            println!("{PROFILE}: {} row(s)", profile.iter().map(|b| b.num_rows()).sum::<usize>());
        }
        Command::Lookup(a) => {
            anyhow::ensure!(a.select.imo.is_some() || a.select.vessel_id.is_some(), "give --imo or --vessel-id");
            let s = connect(&cli).await?;
            let targets = Targets::load(&s.identity).await?;
            let t = select(&targets, &a.select)?[0];
            println!("{} {} (IMO {})", t.vessel_id, t.name.as_deref().unwrap_or("?"), t.imos.join(", "));
            let imgs: Vec<_> = images::rows(&s.imagery.read(IMAGES).await?)?.into_iter().filter(|i| i.vessel_id == t.vessel_id).collect();
            println!("{} photo(s):", imgs.len());
            for i in &imgs {
                println!(
                    "  {} {}  {} [{}{}]  {}",
                    &i.image_id[..12],
                    i.source,
                    i.licence_short.as_deref().unwrap_or("?"),
                    i.licence_class,
                    if i.restricted { ", restricted" } else { "" },
                    i.author.as_deref().or(i.credit.as_deref()).unwrap_or("")
                );
                if let Some(u) = i.source_page_url.as_deref() {
                    println!("      {u}");
                }
            }
            let rows: Vec<_> = describe::observation_rows(&s.imagery.read(OBSERVATIONS).await?)?;
            if a.answers {
                for i in &imgs {
                    println!("answers for {}:", &i.image_id[..12]);
                    for o in rows.iter().filter(|o| o.image_id == i.image_id && o.vessel_id == t.vessel_id) {
                        let p = o.probability.map_or(String::new(), |p| format!("{p:.2}"));
                        let ru = match (&o.runner_up, o.runner_up_probability) {
                            (Some(r), Some(rp)) => format!("  (then {r} {rp:.2})"),
                            _ => String::new(),
                        };
                        println!("  {:<26} {:<40} {p}{ru}", o.question, o.answer);
                    }
                }
            }
            let profile = s.imagery.read(PROFILE).await?;
            let mut shown = false;
            for b in &profile {
                let r = vessel_identity::columns::Reader::new(b);
                let (vid, attr, val) = (r.str("vessel_id")?, r.str("attribute")?, r.str("value")?);
                let (sup, tot) = (r.int("support_images")?, r.int("total_images")?);
                for i in 0..r.rows() {
                    if vid(i).as_deref() != Some(t.vessel_id.as_str()) {
                        continue;
                    }
                    if !shown {
                        println!("visual profile:");
                        shown = true;
                    }
                    println!("  {:<26} {:<36} {}/{}", attr(i).unwrap_or_default(), val(i).unwrap_or_default(), sup(i).unwrap_or(0), tot(i).unwrap_or(0));
                }
            }
            if !shown {
                let n = rows.iter().filter(|o| o.vessel_id == t.vessel_id).count();
                println!("no visual profile yet ({n} observation(s); run describe, then profile)");
            }
        }
    }
    Ok(exitcode::SUCCESS)
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
