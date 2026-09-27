use anyhow::{Context, Result};
use clap::Parser;
use collect_core::backoff::{Backoff, ReconnectCliArgs};
use collect_core::{
    apply_config_file, health_file_path, line_reader_from_async_read, log, print_completions,
    run_ingest, CommonCliArgs, IngestOptions, LineReader, LineSource, LineTransformer,
    ReaderTransition, S3CliArgs,
    log::LoggingCliArgs,
};
use collect_core::iceberg::{init_raw_handle, IcebergCliArgs};
use collect_core::silver::ParserCliArgs;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::RwLock;

const TOKEN_URL: &str = "https://id.barentswatch.no/connect/token";
const DEFAULT_ENDPOINT: &str = "https://live.ais.barentswatch.no/v1/combined";
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);
const TOKEN_REFRESH_BUFFER: Duration = Duration::from_secs(300);

#[derive(Parser, Debug)]
#[command(
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_COMMIT_HASH"), ")"),
    about = "Consume AIS data from BarentsWatch Live API into Hive-partitioned Parquet with Zstd compression"
)]
struct Args {
    /// BarentsWatch API client ID
    #[arg(long, env = "BARENTSWATCH_CLIENT_ID", hide_env_values = true)]
    client_id: Option<String>,

    /// BarentsWatch API client secret
    #[arg(long, env = "BARENTSWATCH_CLIENT_SECRET", hide_env_values = true)]
    client_secret: Option<String>,

    /// Stream endpoint URL
    #[arg(long, env = "BARENTSWATCH_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
    endpoint: String,

    /// Model type: Standard or Full
    #[arg(long, env = "MODEL_TYPE", default_value = "Full")]
    model_type: String,

    /// Model format: Json (only supported)
    #[arg(long, env = "MODEL_FORMAT", default_value = "Json")]
    model_format: String,

    /// Logical source label; defaults to "barentswatch"
    #[arg(short, long, env = "SOURCE")]
    source: Option<String>,

    /// Suppress informational progress lines; warnings and errors still print
    #[arg(short, long, env = "QUIET")]
    quiet: bool,

    #[command(flatten)]
    common: CommonCliArgs,

    #[command(flatten)]
    logging: LoggingCliArgs,

    #[command(flatten)]
    reconnect: ReconnectCliArgs,

    #[command(flatten)]
    s3: S3CliArgs,

    #[command(flatten)]
    iceberg: IcebergCliArgs,

    #[command(flatten)]
    parser: ParserCliArgs,

    /// Print shell completions for the given shell to stdout and exit
    #[arg(long, exclusive = true)]
    completions: Option<clap_complete::Shell>,

    /// Load flag defaults from a flat TOML config file (KEY = value, using
    /// the same env var names shown in --help); explicit flags and
    /// already-set env vars still take precedence over the file
    #[arg(long, env = "CONFIG_FILE")]
    config: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
    token_type: String,
    scope: String,
}

#[derive(Debug, Clone)]
struct Token {
    access_token: String,
    expires_at: Instant,
}

struct TokenManager {
    client: reqwest::Client,
    client_id: String,
    client_secret: String,
    token: Arc<RwLock<Option<Token>>>,
}

impl TokenManager {
    fn new(client_id: String, client_secret: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("reqwest client");
        Self {
            client,
            client_id,
            client_secret,
            token: Arc::new(RwLock::new(None)),
        }
    }

    async fn fetch_token(&self) -> Result<Token> {
        let params = [
            ("grant_type", "client_credentials"),
            ("scope", "ais"),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
        ];

        let resp = self.client
            .post(TOKEN_URL)
            .form(&params)
            .send()
            .await
            .context("Token request failed")?;

        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            anyhow::bail!("Token request failed: {} - {}", status, text);
        }

        let token_resp: TokenResponse = serde_json::from_str(&text).context("Parse token response")?;
        Ok(Token {
            access_token: token_resp.access_token,
            expires_at: Instant::now() + Duration::from_secs(token_resp.expires_in),
        })
    }

    async fn get_token(&self) -> Result<String> {
        let mut token_guard = self.token.write().await;

        if let Some(token) = token_guard.as_ref() {
            if token.expires_at > Instant::now() + TOKEN_REFRESH_BUFFER {
                return Ok(token.access_token.clone());
            }
        }

        let new_token = self.fetch_token().await?;
        let access_token = new_token.access_token.clone();
        *token_guard = Some(new_token);
        Ok(access_token)
    }

    async fn force_refresh(&self) -> Result<String> {
        let new_token = self.fetch_token().await?;
        let access_token = new_token.access_token.clone();
        *self.token.write().await = Some(new_token);
        Ok(access_token)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct BarentsWatchMessage {
    mmsi: u32,
    msgtime: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
    speed_over_ground: Option<f64>,
    course_over_ground: Option<f64>,
    true_heading: Option<u16>,
    rate_of_turn: Option<i16>,
    ship_type: u8,
    name: Option<String>,
    navigational_status: Option<u8>,
    call_sign: Option<String>,
    destination: Option<String>,
    eta: Option<String>,
    imo_number: Option<u32>,
    dimension_a: Option<u16>,
    dimension_b: Option<u16>,
    dimension_c: Option<u16>,
    dimension_d: Option<u16>,
    draught: Option<f64>,
    ship_length: Option<u16>,
    ship_width: Option<u16>,
    position_fixing_device_type: Option<u8>,
    report_class: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AisStreamEnvelope {
    message_type: String,
    meta_data: AisStreamMetaData,
    message: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AisStreamMetaData {
    mmsi: u32,
    ship_name: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    time_utc: Option<String>,
}

struct BarentsWatchTransformer;

impl BarentsWatchTransformer {
    fn ship_type_to_ais_msg_type(&self, ship_type: u8) -> u8 {
        match ship_type {
            30..=39 => 18, // Fishing -> Class B position report (type 18)
            60..=69 => 1,  // Passenger -> Class A position report (type 1)
            70..=79 => 1,  // Cargo -> Class A position report (type 1)
            80..=89 => 1,  // Tanker -> Class A position report (type 1)
            90..=99 => 1,  // Other -> Class A position report (type 1)
            20..=29 => 18, // Wing in ground -> Class B
            40..=49 => 18, // High-speed craft -> Class B
            50 => 1,       // Pilot -> Class A
            51 => 1,       // SAR -> Class A
            52 => 1,       // Tug -> Class A
            53 => 1,       // Port tender -> Class A
            54 => 1,       // Anti-pollution -> Class A
            55 => 1,       // Law enforcement -> Class A
            58 => 1,       // Medical transport -> Class A
            59 => 1,       // Noncombatant -> Class A
            _ => 1,        // Default to Class A position report
        }
    }

    fn transform_line(&self, line: &str) -> Option<String> {
        let bw: BarentsWatchMessage = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => return None,
        };

        let ais_msg_type = self.ship_type_to_ais_msg_type(bw.ship_type);

        let meta = AisStreamMetaData {
            mmsi: bw.mmsi,
            ship_name: bw.name.clone(),
            latitude: bw.latitude,
            longitude: bw.longitude,
            time_utc: Some(bw.msgtime.clone()),
        };

        let mut msg = serde_json::Map::new();
        msg.insert("MessageID".to_string(), serde_json::json!(ais_msg_type));
        msg.insert("UserID".to_string(), serde_json::json!(bw.mmsi));

        if let Some(v) = bw.latitude { msg.insert("Latitude".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.longitude { msg.insert("Longitude".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.speed_over_ground { msg.insert("Sog".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.course_over_ground { msg.insert("Cog".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.true_heading {
            if v != 511 { msg.insert("TrueHeading".to_string(), serde_json::json!(v)); }
        }
        if let Some(v) = bw.rate_of_turn {
            if v != -128 { msg.insert("RateOfTurn".to_string(), serde_json::json!(v)); }
        }
        if let Some(v) = bw.navigational_status { msg.insert("NavigationalStatus".to_string(), serde_json::json!(v)); }
        msg.insert("PositionAccuracy".to_string(), serde_json::json!(true));
        msg.insert("Raim".to_string(), serde_json::json!(false));
        msg.insert("SpecialManoeuvreIndicator".to_string(), serde_json::json!(0));

        if let Some(v) = bw.call_sign { msg.insert("CallSign".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.destination { msg.insert("Destination".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.eta { msg.insert("Eta".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.imo_number { msg.insert("ImoNumber".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.dimension_a { msg.insert("A".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.dimension_b { msg.insert("B".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.dimension_c { msg.insert("C".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.dimension_d { msg.insert("D".to_string(), serde_json::json!(v)); }
        if let Some(v) = bw.draught { msg.insert("MaximumStaticDraught".to_string(), serde_json::json!(v)); }

        let envelope = AisStreamEnvelope {
            message_type: "PositionReport".to_string(),
            meta_data: meta,
            message: serde_json::Value::Object(msg),
        };

        serde_json::to_string(&envelope).ok()
    }
}

impl LineTransformer for BarentsWatchTransformer {
    fn transform(&mut self, line: &str, arrival_ts_ms: i64) -> Vec<(i64, String)> {
        if let Some(transformed) = self.transform_line(line) {
            vec![(arrival_ts_ms, transformed)]
        } else {
            vec![]
        }
    }
}

struct HttpStreamAdapter {
    stream: futures_util::stream::BoxStream<'static, Result<bytes::Bytes, reqwest::Error>>,
    buffer: Vec<u8>,
    pos: usize,
}

impl HttpStreamAdapter {
    fn new(stream: impl futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send + 'static) -> Self {
        Self {
            stream: Box::pin(stream),
            buffer: Vec::new(),
            pos: 0,
        }
    }
}

impl AsyncRead for HttpStreamAdapter {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.pos < self.buffer.len() {
            let avail = self.buffer.len() - self.pos;
            let n = std::cmp::min(buf.remaining(), avail);
            buf.put_slice(&self.buffer[self.pos..self.pos + n]);
            self.pos += n;
            if self.pos >= self.buffer.len() {
                self.buffer.clear();
                self.pos = 0;
            }
            return std::task::Poll::Ready(Ok(()));
        }

        loop {
            match futures_util::ready!(self.stream.as_mut().poll_next(cx)) {
                Some(Ok(chunk)) => {
                    self.buffer.extend_from_slice(chunk.as_ref());
                    self.buffer.push(b'\n');
                    self.pos = 0;
                    let avail = self.buffer.len();
                    let n = std::cmp::min(buf.remaining(), avail);
                    buf.put_slice(&self.buffer[..n]);
                    self.pos = n;
                    return std::task::Poll::Ready(Ok(()));
                }
                Some(Err(e)) => {
                    return std::task::Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
                }
                None => {
                    return std::task::Poll::Ready(Ok(()));
                }
            }
        }
    }
}

struct BarentsWatchSource {
    token_manager: Arc<TokenManager>,
    endpoint: String,
    source: String,
    max_reconnect_seconds: u64,
    client: reqwest::Client,
}

impl BarentsWatchSource {
    fn new(
        token_manager: Arc<TokenManager>,
        endpoint: String,
        model_type: String,
        model_format: String,
        source: String,
        max_reconnect_seconds: u64,
    ) -> Self {
        let mut url = endpoint;
        if !url.contains('?') {
            url.push('?');
        } else {
            url.push('&');
        }
        url.push_str(&format!("modelType={}&modelFormat={}", model_type, model_format));

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .expect("reqwest client");

        Self {
            token_manager,
            endpoint: url,
            source,
            max_reconnect_seconds,
            client,
        }
    }

    async fn connect(&self, max_line_length: usize) -> Result<LineReader> {
        let token = self.token_manager.get_token().await
            .context("Failed to get access token")?;

        let resp = self.client
            .get(&self.endpoint)
            .bearer_auth(token)
            .header("Accept", "application/x-ndjson")
            .send()
            .await
            .context("HTTP request to BarentsWatch failed")?;

        let status = resp.status();
        if status == 401 {
            let new_token = self.token_manager.force_refresh().await
                .context("Token refresh failed")?;
            let resp = self.client
                .get(&self.endpoint)
                .bearer_auth(new_token)
                .header("Accept", "application/x-ndjson")
                .send()
                .await
                .context("HTTP request after token refresh failed")?;
            let status = resp.status();
            if !status.is_success() {
                anyhow::bail!("BarentsWatch API error: {}", status);
            }
            let stream = resp.bytes_stream();
            let adapter = HttpStreamAdapter::new(stream);
            return Ok(line_reader_from_async_read(adapter, max_line_length));
        }

        if !status.is_success() {
            anyhow::bail!("BarentsWatch API error: {}", status);
        }

        let stream = resp.bytes_stream();
        let adapter = HttpStreamAdapter::new(stream);
        Ok(line_reader_from_async_read(adapter, max_line_length))
    }

    async fn reconnect(
        &self,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        let upstream = "BarentsWatch";
        let mut backoff = Backoff::new(
            RECONNECT_INITIAL_DELAY,
            RECONNECT_MAX_DELAY,
            self.max_reconnect_seconds,
        );

        loop {
            if shutdown.load(Ordering::SeqCst) {
                return Ok(ReaderTransition::Stop);
            }

            log::warn(
                "reconnect_attempt",
                &format!("{upstream} disconnected, attempting to reconnect"),
                &[("upstream", upstream)],
            );

            match self.connect(max_line_length).await {
                Ok(reader) => {
                    log::info(
                        "reconnected",
                        &format!("reconnected to {upstream}"),
                        &[("upstream", upstream)],
                    );
                    return Ok(ReaderTransition::Continue(reader));
                }
                Err(error) => {
                    log::warn(
                        "reconnect_failed",
                        &format!("reconnect to {upstream} failed: {error}"),
                        &[("upstream", upstream)],
                    );
                }
            }

            if !backoff.wait(shutdown).await {
                if shutdown.load(Ordering::SeqCst) {
                    return Ok(ReaderTransition::Stop);
                }
                collect_core::backoff::give_up(upstream, self.max_reconnect_seconds);
            }
        }
    }
}

#[collect_core::async_trait]
impl LineSource for BarentsWatchSource {
    fn source_name(&self) -> &str {
        &self.source
    }

    async fn open(&mut self, max_line_length: usize) -> Result<LineReader> {
        self.connect(max_line_length).await
    }

    async fn on_stream_end(
        &mut self,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        self.reconnect(shutdown, max_line_length).await
    }

    async fn on_stream_error(
        &mut self,
        _error: &collect_core::LinesCodecError,
        shutdown: &AtomicBool,
        max_line_length: usize,
    ) -> Result<ReaderTransition> {
        self.reconnect(shutdown, max_line_length).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();

    if let Some(shell) = args.completions {
        print_completions::<Args>(shell, "collect-barentswatch");
        return Ok(());
    }

    if let Some(config_path) = &args.config {
        apply_config_file(config_path)?;
        args = Args::parse();
    }

    args.logging.init("collect-barentswatch");

    let client_id = args
        .client_id
        .context("missing client ID; set --client-id or BARENTSWATCH_CLIENT_ID")?;
    let client_secret = args
        .client_secret
        .context("missing client secret; set --client-secret or BARENTSWATCH_CLIENT_SECRET")?;
    let source_name = args.source.unwrap_or_else(|| "barentswatch".to_string());
    let endpoint = args.endpoint;
    let model_type = args.model_type;
    let model_format = args.model_format;
    let max_reconnect_seconds = args.reconnect.max_reconnect_seconds;

    let health_file = health_file_path("collect-barentswatch");

    let token_manager = Arc::new(TokenManager::new(client_id, client_secret));
    token_manager.get_token().await.context("Initial token fetch failed")?;

    let common_options = args.common.to_options();
    let iceberg = if common_options.health_check {
        None
    } else {
        init_raw_handle(
            &args.iceberg,
            common_options.partition.as_str(),
            common_options.compression_level,
        )
        .await?
    };

    let silver = if common_options.health_check {
        None
    } else {
        collect_silver::init_silver(
            &args.parser,
            &args.iceberg,
            &common_options.out_dir,
            common_options.partition,
            common_options.compression_level,
        )
        .await?
    };

    let transformer = if args.parser.parser == collect_core::silver::ParserKind::Aisstream {
        Some(Box::new(BarentsWatchTransformer) as Box<dyn LineTransformer>)
    } else {
        None
    };

    run_ingest(
        &mut BarentsWatchSource::new(
            token_manager,
            endpoint,
            model_type,
            model_format,
            source_name,
            max_reconnect_seconds,
        ),
        IngestOptions {
            common: common_options,
            s3: args.s3.to_options(),
            s3_storage: None,
            iceberg,
            health_file,
            manage_health: true,
            report_progress: !args.quiet,
            log_writes: !args.quiet,
            shutdown: None,
            write_workers: None,
            sweep_orphans: true,
            line_transformer: transformer,
            silver,
        },
    )
    .await
}