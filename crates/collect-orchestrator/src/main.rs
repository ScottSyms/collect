use anyhow::{Context, Result};
use clap::Parser;
use collect_core::{apply_config_file, S3Storage};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;

mod config;
mod db;
mod decode;
mod dispatcher;
mod http;
mod worker;

use collect_core::iceberg::IcebergConfig;

#[derive(Parser, Debug)]
#[command(name="collect-orchestrator", version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_COMMIT_HASH"), ")"),
 about="Event-driven per-file AIS parser orchestrator: RustFS webhook → Postgres queue → Iceberg")]
struct Args {
    #[arg(long, env="DATABASE_URL")]
    database_url: Option<String>,

    #[arg(long, env="LISTEN_ADDR", default_value="0.0.0.0:8080")]
    listen_addr: String,

    #[arg(long, env="MAX_INFLIGHT", default_value_t=4)]
    max_inflight: usize,

    #[arg(long, env="BATCH_SIZE", default_value_t=8192)]
    batch_size: usize,

    #[arg(long, env="COMPRESSION_LEVEL", default_value_t=5)]
    compression_level: i32,

    #[arg(long, env="SCRATCH_DIR")]
    scratch_dir: Option<PathBuf>,

    #[arg(long, env="INGEST_TOKEN")]
    ingest_token: Option<String>,

    #[arg(long, env="SOURCE_MAP")]
    source_map: Option<String>,

    // S3 raw input bucket (bronze)
    #[arg(long, env="INPUT_S3_BUCKET")]
    input_s3_bucket: Option<String>,
    #[arg(long, env="INPUT_S3_PREFIX", default_value="")]
    input_s3_prefix: String,
    #[arg(long, env="S3_ENDPOINT")]
    s3_endpoint: Option<String>,
    #[arg(long, env="S3_REGION", default_value="us-east-1")]
    s3_region: String,
    #[arg(long, env="S3_ACCESS_KEY")]
    s3_access_key: Option<String>,
    #[arg(long, env="S3_SECRET_KEY")]
    s3_secret_key: Option<String>,
    #[arg(long, env="S3_DISABLE_TLS", value_parser=clap::builder::FalseyValueParser::new())]
    s3_disable_tls: bool,

    #[command(flatten)]
    iceberg: collect_core::iceberg::IcebergCliArgs,

    #[arg(long, env="CALLBACK_TOKEN")]
    callback_token: Option<String>,

    // Nomad dispatch mode
    #[arg(long, env="ENABLE_DISPATCH", value_parser=clap::builder::FalseyValueParser::new())]
    enable_dispatch: bool,

    #[arg(long, env="NOMAD_ADDR", default_value="http://nomad.service.consul:4646")]
    nomad_addr: String,

    #[arg(long, env="NOMAD_TOKEN")]
    nomad_token: Option<String>,

    #[arg(long, env="NOMAD_JOB", default_value="parse-file")]
    nomad_job: String,

    #[arg(long, env="DISPATCH_CONCURRENCY", default_value_t=32)]
    dispatch_concurrency: usize,

    #[arg(long, env="DISPATCH_RECLAIM_SECS", default_value_t=1800)]
    dispatch_reclaim_secs: i64,

    #[arg(long)]
    backfill: bool,

    #[arg(long, env="CONFIG_FILE")]
    config: Option<PathBuf>,

    #[arg(long, exclusive=true)]
    completions: Option<clap_complete::Shell>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    if let Some(shell) = args.completions {
        collect_core::print_completions::<Args>(shell, "collect-orchestrator");
        return Ok(());
    }
    if let Some(cfg) = &args.config {
        apply_config_file(cfg)?;
        args = Args::parse();
    }
    args.iceberg.validate()?;

    let database_url = args.database_url.clone().or_else(|| std::env::var("DATABASE_URL").ok()).context("DATABASE_URL required")?;
    let pool = db::init_pool(&database_url).await?;
    db::run_migrations(&pool).await?;
    eprintln!("✅ Postgres connected, migrations applied");

    let s3_bucket = args.input_s3_bucket.clone().unwrap_or_else(|| "collections".to_string());
    let s3_prefix = args.input_s3_prefix.clone();
    let storage = S3Storage::new(
        s3_bucket.clone(),
        s3_prefix.clone(),
        args.s3_region.clone(),
        args.s3_endpoint.clone(),
        args.s3_access_key.clone(),
        args.s3_secret_key.clone(),
        false,
        args.s3_disable_tls,
    ).await.context("connecting to S3")?;
    let storages = Arc::new(vec![storage]);

    if args.backfill {
        return run_backfill(pool, storages, s3_bucket, s3_prefix, args).await;
    }

    // Source map
    let source_map: HashMap<String,String> = config::load_source_map(args.source_map.as_deref()).unwrap_or_default();
    if !source_map.is_empty() {
        eprintln!("source_map: {source_map:?}");
    }

    // Iceberg config
    let iceberg_config: IcebergConfig = (&args.iceberg).into();

    let app_state = http::AppState {
        pool: pool.clone(),
        source_map: Arc::new(source_map),
        ingest_token: args.ingest_token.clone(),
        callback_token: args.callback_token.clone().or_else(|| args.ingest_token.clone()),
    };

    let app = axum::Router::new()
        .route("/ingest", axum::routing::post(http::ingest_handler))
        .route("/complete", axum::routing::post(http::complete_handler))
        .route("/fail", axum::routing::post(http::fail_handler))
        .route("/healthz", axum::routing::get(http::healthz))
        .route("/metrics", axum::routing::get(http::metrics_handler))
        .route("/queue", axum::routing::get(http::queue_handler))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(&args.listen_addr).await?;
    eprintln!("🚀 collect-orchestrator listening on {}", args.listen_addr);
    eprintln!("   POST /ingest  (RustFS webhook)");
    eprintln!("   POST /complete /fail (Nomad worker callback)");
    eprintln!("   GET  /healthz /metrics /queue");

    if args.enable_dispatch {
        eprintln!("📤 Nomad dispatch mode enabled: job={} addr={} concurrency={}", args.nomad_job, args.nomad_addr, args.dispatch_concurrency);
        let cfg = dispatcher::DispatcherConfig {
            nomad_addr: args.nomad_addr.clone(),
            nomad_token: args.nomad_token.clone(),
            nomad_job: args.nomad_job.clone(),
            dispatch_concurrency: args.dispatch_concurrency,
            poll_interval_ms: 500,
            reclaim_secs: args.dispatch_reclaim_secs,
        };
        let pool2 = pool.clone();
        tokio::spawn(async move {
            dispatcher::run_dispatcher_loop(pool2, cfg).await;
        });
    } else {
        // Worker pool (inline mode)
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "orchestrator".to_string());
        let ctx = Arc::new(worker::WorkerContext {
            pool: pool.clone(),
            s3_storages: storages,
            s3_bucket,
            s3_prefix,
            iceberg_config,
            scratch_dir: args.scratch_dir,
            batch_size: args.batch_size,
            compression_level: args.compression_level,
            hostname,
        });
        let sem = Arc::new(Semaphore::new(args.max_inflight.max(1)));
        let worker_ctx = ctx.clone();
        let worker_sem = sem.clone();
        tokio::spawn(async move {
            worker::run_worker_loop(worker_ctx, worker_sem).await;
        });
    }

    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_backfill(
    pool: sqlx::PgPool,
    storages: Arc<Vec<S3Storage>>,
    bucket: String,
    prefix: String,
    args: Args,
) -> Result<()> {
    eprintln!("backfill listing s3://{bucket}/{prefix} ...");
    let storage = &storages[0];
    let objs = storage.list_keys_with_prefix(&prefix).await?;
    let mut inserted = 0usize;
    let source_map: HashMap<String,String> = config::load_source_map(args.source_map.as_deref()).unwrap_or_default();
    for obj in objs {
        if !obj.key.ends_with(".parquet") { continue; }
        // Compute rel key without prefix for queue uniqueness (but store full key)
        let rel = if prefix.is_empty() { obj.key.clone() } else {
            obj.key.strip_prefix(&format!("{}/", prefix.trim_matches('/'))).unwrap_or(&obj.key).to_string()
        };
        let source = config::extract_source_from_key(&rel).unwrap_or_else(|| "unknown".to_string());
        let parser = config::parser_for_source(&source, &source_map);
        if db::enqueue(&pool, &bucket, &rel, &source, parser).await? {
            inserted += 1;
        }
    }
    eprintln!("backfill done: {inserted} new rows enqueued");
    Ok(())
}
