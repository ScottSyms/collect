use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Tabs, Wrap},
    Frame, Terminal,
};
use serde::{Deserialize, Serialize};
use std::io;
use std::fs;

const TABS: [&str; 7] = ["Input", "Output", "S3", "WebSocket", "Kafka", "Config", "Run"];

#[derive(Clone, Serialize, Deserialize)]
pub struct TuiConfig {
    // Input tab
    pub input_file: String,
    pub tcp_host: String,
    pub tcp_port: String,
    pub source: String,
    
    // Output tab
    pub out_dir: String,
    pub max_rows: String,
    pub keep_local: bool,
    
    // S3 tab
    pub s3_bucket: String,
    pub s3_endpoint: String,
    pub s3_region: String,
    pub s3_key_prefix: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_disable_tls: bool,
    
    // WebSocket tab
    pub ws_url: String,
    pub ws_api_key: String,
    pub ws_bbox: String,
    pub ws_mmsi_filter: String,
    pub ws_message_type_filter: String,
    pub ws_debug: bool,
    
    // Kafka tab
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub kafka_group_id: String,
    pub kafka_schema_registry: String,
    pub kafka_debug: bool,
}

impl Default for TuiConfig {
    fn default() -> Self {
        Self {
            input_file: String::new(),
            tcp_host: String::new(),
            tcp_port: String::new(),
            source: String::new(),
            out_dir: "data".to_string(),
            max_rows: String::new(),
            keep_local: false,
            s3_bucket: String::new(),
            s3_endpoint: String::new(),
            s3_region: "us-east-1".to_string(),
            s3_key_prefix: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_disable_tls: false,
            ws_url: String::new(),
            ws_api_key: String::new(),
            ws_bbox: String::new(),
            ws_mmsi_filter: String::new(),
            ws_message_type_filter: String::new(),
            ws_debug: false,
            kafka_brokers: String::new(),
            kafka_topic: String::new(),
            kafka_group_id: String::new(),
            kafka_schema_registry: String::new(),
            kafka_debug: false,
        }
    }
}

impl TuiConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        // Load from environment variables
        if let Ok(val) = std::env::var("SOURCE") {
            config.source = val;
        }
        if let Ok(val) = std::env::var("OUT_DIR") {
            config.out_dir = val;
        }
        if let Ok(val) = std::env::var("MAX_ROWS") {
            config.max_rows = val;
        }
        if let Ok(val) = std::env::var("S3_BUCKET") {
            config.s3_bucket = val;
        }
        if let Ok(val) = std::env::var("S3_ENDPOINT") {
            config.s3_endpoint = val;
        }
        if let Ok(val) = std::env::var("S3_REGION") {
            config.s3_region = val;
        }
        if let Ok(val) = std::env::var("S3_KEY_PREFIX") {
            config.s3_key_prefix = val;
        }
        if let Ok(val) = std::env::var("AWS_ACCESS_KEY_ID") {
            config.s3_access_key = val;
        }
        if let Ok(val) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            config.s3_secret_key = val;
        }
        if let Ok(val) = std::env::var("KEEP_LOCAL") {
            config.keep_local = val.parse().unwrap_or(false);
        }
        if let Ok(val) = std::env::var("S3_DISABLE_TLS") {
            config.s3_disable_tls = val.parse().unwrap_or(false);
        }
        if let Ok(val) = std::env::var("WS_URL") {
            config.ws_url = val;
        }
        if let Ok(val) = std::env::var("WS_API_KEY") {
            config.ws_api_key = val;
        }
        if let Ok(val) = std::env::var("WS_DEBUG") {
            config.ws_debug = val.parse().unwrap_or(false);
        }
        if let Ok(val) = std::env::var("KAFKA_BROKERS") {
            config.kafka_brokers = val;
        }
        if let Ok(val) = std::env::var("KAFKA_TOPIC") {
            config.kafka_topic = val;
        }
        if let Ok(val) = std::env::var("KAFKA_GROUP_ID") {
            config.kafka_group_id = val;
        }
        if let Ok(val) = std::env::var("KAFKA_SCHEMA_REGISTRY") {
            config.kafka_schema_registry = val;
        }
        if let Ok(val) = std::env::var("KAFKA_DEBUG") {
            config.kafka_debug = val.parse().unwrap_or(false);
        }
        
        config
    }
    
    pub fn save_to_file(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        fs::write(path, json)?;
        Ok(())
    }
    
    pub fn load_from_file(path: &str) -> Result<Self> {
        let json = fs::read_to_string(path)?;
        let config: TuiConfig = serde_json::from_str(&json)?;
        Ok(config)
    }
    
    pub fn to_cli_args(&self) -> Vec<String> {
        let mut args = vec![];
        
        // Input
        if !self.input_file.is_empty() {
            args.push("--input".to_string());
            args.push(self.input_file.clone());
        }
        if !self.tcp_host.is_empty() {
            args.push("--tcp-host".to_string());
            args.push(self.tcp_host.clone());
        }
        if !self.tcp_port.is_empty() {
            args.push("--tcp-port".to_string());
            args.push(self.tcp_port.clone());
        }
        if !self.source.is_empty() {
            args.push("--source".to_string());
            args.push(self.source.clone());
        }
        
        // Output
        args.push("--out-dir".to_string());
        args.push(self.out_dir.clone());
        if !self.max_rows.is_empty() {
            args.push("--max-rows".to_string());
            args.push(self.max_rows.clone());
        }
        if self.keep_local {
            args.push("--keep-local".to_string());
        }
        
        // S3
        if !self.s3_bucket.is_empty() {
            args.push("--s3-bucket".to_string());
            args.push(self.s3_bucket.clone());
        }
        if !self.s3_endpoint.is_empty() {
            args.push("--s3-endpoint".to_string());
            args.push(self.s3_endpoint.clone());
        }
        args.push("--s3-region".to_string());
        args.push(self.s3_region.clone());
        if !self.s3_key_prefix.is_empty() {
            args.push("--s3-key-prefix".to_string());
            args.push(self.s3_key_prefix.clone());
        }
        if !self.s3_access_key.is_empty() {
            args.push("--s3-access-key".to_string());
            args.push(self.s3_access_key.clone());
        }
        if !self.s3_secret_key.is_empty() {
            args.push("--s3-secret-key".to_string());
            args.push(self.s3_secret_key.clone());
        }
        if self.s3_disable_tls {
            args.push("--s3-disable-tls".to_string());
        }
        
        // WebSocket
        if !self.ws_url.is_empty() {
            args.push("--ws-url".to_string());
            args.push(self.ws_url.clone());
        }
        if !self.ws_api_key.is_empty() {
            args.push("--ws-api-key".to_string());
            args.push(self.ws_api_key.clone());
        }
        if !self.ws_bbox.is_empty() {
            for bbox in self.ws_bbox.split(',') {
                if !bbox.trim().is_empty() {
                    args.push("--ws-bbox".to_string());
                    args.push(bbox.trim().to_string());
                }
            }
        }
        if !self.ws_mmsi_filter.is_empty() {
            for mmsi in self.ws_mmsi_filter.split(',') {
                if !mmsi.trim().is_empty() {
                    args.push("--ws-mmsi-filter".to_string());
                    args.push(mmsi.trim().to_string());
                }
            }
        }
        if !self.ws_message_type_filter.is_empty() {
            for msg_type in self.ws_message_type_filter.split(',') {
                if !msg_type.trim().is_empty() {
                    args.push("--ws-message-type-filter".to_string());
                    args.push(msg_type.trim().to_string());
                }
            }
        }
        if self.ws_debug {
            args.push("--ws-debug".to_string());
        }
        
        // Kafka
        if !self.kafka_brokers.is_empty() {
            args.push("--kafka-brokers".to_string());
            args.push(self.kafka_brokers.clone());
        }
        if !self.kafka_topic.is_empty() {
            args.push("--kafka-topic".to_string());
            args.push(self.kafka_topic.clone());
        }
        if !self.kafka_group_id.is_empty() {
            args.push("--kafka-group-id".to_string());
            args.push(self.kafka_group_id.clone());
        }
        if !self.kafka_schema_registry.is_empty() {
            args.push("--kafka-schema-registry".to_string());
            args.push(self.kafka_schema_registry.clone());
        }
        if self.kafka_debug {
            args.push("--kafka-debug".to_string());
        }
        
        args
    }
}

struct App {
    config: TuiConfig,
    selected_tab: usize,
    current_field: usize,
    editing: bool,
    input_buffer: String,
    quit: bool,
    run: bool,
    validation_errors: Vec<String>,
    show_help: bool,
    status_message: Option<String>,
    config_file_path: String,
}

impl App {
    fn new(config: TuiConfig) -> Self {
        Self {
            config,
            selected_tab: 0,
            current_field: 0,
            editing: false,
            input_buffer: String::new(),
            quit: false,
            run: false,
            validation_errors: Vec::new(),
            show_help: false,
            status_message: None,
            config_file_path: "capture-config.json".to_string(),
        }
    }
    
    fn fields_for_tab(&self, tab: usize) -> Vec<(&str, String, bool)> {
        match tab {
            0 => vec![
                ("Input File", self.config.input_file.clone(), false),
                ("TCP Host", self.config.tcp_host.clone(), false),
                ("TCP Port", self.config.tcp_port.clone(), false),
                ("Source Label", self.config.source.clone(), false),
            ],
            1 => vec![
                ("Output Directory", self.config.out_dir.clone(), false),
                ("Max Rows per File", self.config.max_rows.clone(), false),
                ("Keep Local Files", format!("{}", self.config.keep_local), true),
            ],
            2 => vec![
                ("S3 Bucket", self.config.s3_bucket.clone(), false),
                ("S3 Endpoint", self.config.s3_endpoint.clone(), false),
                ("S3 Region", self.config.s3_region.clone(), false),
                ("S3 Key Prefix", self.config.s3_key_prefix.clone(), false),
                ("S3 Access Key", self.config.s3_access_key.clone(), false),
                ("S3 Secret Key", "***".to_string(), false),
                ("Disable TLS", format!("{}", self.config.s3_disable_tls), true),
            ],
            3 => vec![
                ("WebSocket URL", self.config.ws_url.clone(), false),
                ("API Key", if self.config.ws_api_key.is_empty() { String::new() } else { "***".to_string() }, false),
                ("Bounding Box (csv)", self.config.ws_bbox.clone(), false),
                ("MMSI Filter (csv)", self.config.ws_mmsi_filter.clone(), false),
                ("Message Type Filter (csv)", self.config.ws_message_type_filter.clone(), false),
                ("Debug Mode", format!("{}", self.config.ws_debug), true),
            ],
            4 => vec![
                ("Kafka Brokers", self.config.kafka_brokers.clone(), false),
                ("Kafka Topic", self.config.kafka_topic.clone(), false),
                ("Consumer Group ID", self.config.kafka_group_id.clone(), false),
                ("Schema Registry URL", self.config.kafka_schema_registry.clone(), false),
                ("Debug Mode", format!("{}", self.config.kafka_debug), true),
            ],
            5 => vec![
                ("Config File Path", self.config_file_path.clone(), false),
                ("Save Config", "Press Enter".to_string(), false),
                ("Load Config", "Press Enter".to_string(), false),
            ],
            6 => vec![], // Run tab
            _ => vec![],
        }
    }
    
    fn get_field_hint(&self, tab: usize, field: usize) -> Option<&str> {
        match (tab, field) {
            // Input tab
            (0, 0) => Some("e.g., /path/to/data.txt (one record per line)"),
            (0, 1) => Some("e.g., 153.44.253.27"),
            (0, 2) => Some("e.g., 5631"),
            (0, 3) => Some("e.g., norway, test-data (optional label)"),
            
            // Output tab
            (1, 0) => Some("e.g., data, /mnt/storage/parquet"),
            (1, 1) => Some("e.g., 10000 (optional, flushes on minute boundary by default)"),
            
            // S3 tab
            (2, 0) => Some("e.g., my-bucket-name"),
            (2, 1) => Some("e.g., https://s3.example.com (for MinIO, R2, etc.)"),
            (2, 2) => Some("e.g., us-east-1, us-west-2, eu-central-1"),
            (2, 3) => Some("e.g., ais-data/, production/ (optional prefix)"),
            (2, 4) => Some("AWS Access Key ID"),
            (2, 5) => Some("AWS Secret Access Key (hidden)"),
            
            // WebSocket tab
            (3, 0) => Some("e.g., wss://stream.aisstream.io/v0/stream"),
            (3, 1) => Some("API key for authentication"),
            (3, 2) => Some("e.g., [[lat1,lon1],[lat2,lon2]] or multiple boxes"),
            (3, 3) => Some("e.g., 123456789,987654321 (comma-separated, max 50)"),
            (3, 4) => Some("e.g., PositionReport,ShipStaticData"),
            
            // Kafka tab
            (4, 0) => Some("e.g., localhost:9092 or broker1:9092,broker2:9092"),
            (4, 1) => Some("e.g., ais-messages"),
            (4, 2) => Some("e.g., capture-consumer-group"),
            (4, 3) => Some("e.g., http://localhost:8081 (optional for Avro)"),
            
            // Config tab
            (5, 0) => Some("Path to save/load JSON config file"),
            (5, 1) => Some("Save current configuration to file"),
            (5, 2) => Some("Load configuration from file"),
            
            _ => None,
        }
    }
    
    fn validate_config(&mut self) -> bool {
        self.validation_errors.clear();
        
        // Check for at least one input source
        let has_input = !self.config.input_file.is_empty()
            || !self.config.tcp_host.is_empty()
            || !self.config.ws_url.is_empty()
            || !self.config.kafka_brokers.is_empty();
        
        if !has_input {
            self.validation_errors.push("⚠ No input source configured. Choose one: File, TCP, WebSocket, or Kafka".to_string());
        }
        
        // Check for multiple input sources
        let input_count = [
            !self.config.input_file.is_empty(),
            !self.config.tcp_host.is_empty(),
            !self.config.ws_url.is_empty(),
            !self.config.kafka_brokers.is_empty(),
        ].iter().filter(|&&x| x).count();
        
        if input_count > 1 {
            self.validation_errors.push("⚠ Multiple input sources configured. Only one can be active at a time".to_string());
        }
        
        // TCP validation
        if !self.config.tcp_host.is_empty() && self.config.tcp_port.is_empty() {
            self.validation_errors.push("⚠ TCP Port required when TCP Host is specified".to_string());
        }
        
        if !self.config.tcp_port.is_empty() {
            if let Err(_) = self.config.tcp_port.parse::<u16>() {
                self.validation_errors.push("⚠ TCP Port must be a valid number (1-65535)".to_string());
            }
        }
        
        // WebSocket validation
        if !self.config.ws_url.is_empty() {
            if !self.config.ws_url.starts_with("ws://") && !self.config.ws_url.starts_with("wss://") {
                self.validation_errors.push("⚠ WebSocket URL must start with ws:// or wss://".to_string());
            }
        }
        
        // Kafka validation
        if !self.config.kafka_brokers.is_empty() {
            if self.config.kafka_topic.is_empty() {
                self.validation_errors.push("⚠ Kafka Topic required when Kafka Brokers is specified".to_string());
            }
            if self.config.kafka_group_id.is_empty() {
                self.validation_errors.push("⚠ Kafka Consumer Group ID required when Kafka Brokers is specified".to_string());
            }
        }
        
        // Max rows validation
        if !self.config.max_rows.is_empty() {
            if let Err(_) = self.config.max_rows.parse::<usize>() {
                self.validation_errors.push("⚠ Max Rows must be a valid positive number".to_string());
            }
        }
        
        // S3 validation
        if !self.config.s3_bucket.is_empty() {
            if self.config.s3_access_key.is_empty() || self.config.s3_secret_key.is_empty() {
                // Check environment variables as fallback
                if std::env::var("AWS_ACCESS_KEY_ID").is_err() || std::env::var("AWS_SECRET_ACCESS_KEY").is_err() {
                    self.validation_errors.push("⚠ S3 credentials required (access key + secret key or env vars)".to_string());
                }
            }
        }
        
        self.validation_errors.is_empty()
    }
    
    fn save_config(&mut self) {
        match self.config.save_to_file(&self.config_file_path) {
            Ok(_) => {
                self.status_message = Some(format!("✓ Config saved to {}", self.config_file_path));
            }
            Err(e) => {
                self.status_message = Some(format!("✗ Failed to save config: {}", e));
            }
        }
    }
    
    fn load_config(&mut self) {
        match TuiConfig::load_from_file(&self.config_file_path) {
            Ok(config) => {
                self.config = config;
                self.status_message = Some(format!("✓ Config loaded from {}", self.config_file_path));
            }
            Err(e) => {
                self.status_message = Some(format!("✗ Failed to load config: {}", e));
            }
        }
    }
    
    fn start_editing(&mut self) {
        let fields = self.fields_for_tab(self.selected_tab);
        if self.current_field < fields.len() {
            let (_, value, is_bool) = fields[self.current_field].clone();
            self.editing = true;
            if !is_bool {
                self.input_buffer = value.clone();
                // Don't show masked passwords
                if value == "***" {
                    self.input_buffer.clear();
                }
            }
        }
    }
    
    fn finish_editing(&mut self) {
        let field_idx = self.current_field;
        let value = self.input_buffer.clone();
        
        match self.selected_tab {
            0 => match field_idx {
                0 => self.config.input_file = value,
                1 => self.config.tcp_host = value,
                2 => self.config.tcp_port = value,
                3 => self.config.source = value,
                _ => {}
            },
            1 => match field_idx {
                0 => self.config.out_dir = value,
                1 => self.config.max_rows = value,
                2 => self.config.keep_local = !self.config.keep_local,
                _ => {}
            },
            2 => match field_idx {
                0 => self.config.s3_bucket = value,
                1 => self.config.s3_endpoint = value,
                2 => self.config.s3_region = value,
                3 => self.config.s3_key_prefix = value,
                4 => self.config.s3_access_key = value,
                5 => self.config.s3_secret_key = value,
                6 => self.config.s3_disable_tls = !self.config.s3_disable_tls,
                _ => {}
            },
            3 => match field_idx {
                0 => self.config.ws_url = value,
                1 => self.config.ws_api_key = value,
                2 => self.config.ws_bbox = value,
                3 => self.config.ws_mmsi_filter = value,
                4 => self.config.ws_message_type_filter = value,
                5 => self.config.ws_debug = !self.config.ws_debug,
                _ => {}
            },
            4 => match field_idx {
                0 => self.config.kafka_brokers = value,
                1 => self.config.kafka_topic = value,
                2 => self.config.kafka_group_id = value,
                3 => self.config.kafka_schema_registry = value,
                4 => self.config.kafka_debug = !self.config.kafka_debug,
                _ => {}
            },
            5 => match field_idx {
                0 => self.config_file_path = value,
                1 => self.save_config(),
                2 => self.load_config(),
                _ => {}
            },
            _ => {}
        }
        
        self.editing = false;
        self.input_buffer.clear();
    }
    
    fn toggle_bool(&mut self) {
        let fields = self.fields_for_tab(self.selected_tab);
        if self.current_field < fields.len() {
            let (_, _, is_bool) = &fields[self.current_field];
            if *is_bool {
                match self.selected_tab {
                    1 => if self.current_field == 2 { self.config.keep_local = !self.config.keep_local; },
                    2 => if self.current_field == 6 { self.config.s3_disable_tls = !self.config.s3_disable_tls; },
                    3 => if self.current_field == 5 { self.config.ws_debug = !self.config.ws_debug; },
                    4 => if self.current_field == 4 { self.config.kafka_debug = !self.config.kafka_debug; },
                    _ => {}
                }
            }
        }
    }
}

pub fn run_tui(initial_config: TuiConfig) -> Result<Option<TuiConfig>> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(initial_config);
    let res = run_app(&mut terminal, &mut app);

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("Error: {:?}", err);
        return Err(err);
    }

    if app.run {
        Ok(Some(app.config))
    } else {
        Ok(None)
    }
}

fn run_app<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> Result<()> {
    loop {
        terminal.draw(|f| ui(f, app))?;

        if let Event::Key(key) = event::read()? {
            if key.kind != KeyEventKind::Press {
                continue;
            }

            if app.editing {
                match key.code {
                    KeyCode::Enter => app.finish_editing(),
                    KeyCode::Esc => {
                        app.editing = false;
                        app.input_buffer.clear();
                    }
                    KeyCode::Char(c) => {
                        app.input_buffer.push(c);
                    }
                    KeyCode::Backspace => {
                        app.input_buffer.pop();
                    }
                    _ => {}
                }
            } else {
                match key.code {
                    KeyCode::Char('q') => {
                        app.quit = true;
                        return Ok(());
                    }
                    KeyCode::Tab => {
                        app.selected_tab = (app.selected_tab + 1) % TABS.len();
                        app.current_field = 0;
                    }
                    KeyCode::BackTab => {
                        app.selected_tab = if app.selected_tab == 0 {
                            TABS.len() - 1
                        } else {
                            app.selected_tab - 1
                        };
                        app.current_field = 0;
                    }
                    KeyCode::Up => {
                        let num_fields = app.fields_for_tab(app.selected_tab).len();
                        if num_fields > 0 {
                            app.current_field = if app.current_field == 0 {
                                num_fields - 1
                            } else {
                                app.current_field - 1
                            };
                        }
                    }
                    KeyCode::Down => {
                        let num_fields = app.fields_for_tab(app.selected_tab).len();
                        if num_fields > 0 {
                            app.current_field = (app.current_field + 1) % num_fields;
                        }
                    }
                    KeyCode::Enter => {
                        if app.selected_tab == 6 {
                            // Run tab - validate first
                            if app.validate_config() {
                                app.run = true;
                                return Ok(());
                            }
                        } else if app.selected_tab == 5 {
                            // Config tab - special handling
                            let field_idx = app.current_field;
                            match field_idx {
                                0 => app.start_editing(), // Edit file path
                                1 => app.save_config(),    // Save config
                                2 => app.load_config(),    // Load config
                                _ => {}
                            }
                        } else {
                            let fields = app.fields_for_tab(app.selected_tab);
                            if app.current_field < fields.len() {
                                let (_, _, is_bool) = &fields[app.current_field];
                                if *is_bool {
                                    app.toggle_bool();
                                } else {
                                    app.start_editing();
                                }
                            }
                        }
                    }
                    KeyCode::Char(' ') => {
                        app.toggle_bool();
                    }
                    KeyCode::Char('?') | KeyCode::F(1) => {
                        app.show_help = !app.show_help;
                    }
                    KeyCode::Char('v') => {
                        // Manual validation check
                        app.validate_config();
                    }
                    _ => {}
                }
            }
        }

        if app.quit {
            break;
        }
    }
    Ok(())
}

fn ui(f: &mut Frame, app: &App) {
    let mut constraints = vec![
        Constraint::Length(3), // Title
        Constraint::Min(10),   // Content
        Constraint::Length(3), // Footer
    ];
    
    // Add space for status message if present
    if app.status_message.is_some() {
        constraints.insert(2, Constraint::Length(3));
    }
    
    // Add space for validation errors if present
    if !app.validation_errors.is_empty() {
        let error_lines = app.validation_errors.len().min(5) as u16 + 2;
        constraints.insert(2, Constraint::Length(error_lines));
    }
    
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(constraints)
        .split(f.area());
    
    let mut chunk_idx = 0;

    // Title
    let title = Paragraph::new("Capture Configuration - Press '?' for help, 'v' to validate, 'q' to quit")
        .style(Style::default().fg(Color::Cyan))
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(title, chunks[chunk_idx]);
    chunk_idx += 1;

    // Tabs
    let tab_titles: Vec<Line> = TABS
        .iter()
        .map(|t| Line::from(Span::styled(*t, Style::default())))
        .collect();
    let tabs = Tabs::new(tab_titles)
        .block(Block::default().borders(Borders::ALL).title("Tabs"))
        .select(app.selected_tab)
        .style(Style::default().fg(Color::White))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        );

    let inner_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(7)])
        .split(chunks[chunk_idx]);
    chunk_idx += 1;

    f.render_widget(tabs, inner_chunks[0]);

    // Content
    if app.selected_tab == 6 {
        render_run_tab(f, inner_chunks[1], app);
    } else {
        render_fields_tab(f, inner_chunks[1], app);
    }
    
    // Validation errors
    if !app.validation_errors.is_empty() {
        let error_text: Vec<Line> = app.validation_errors.iter()
            .take(5)
            .map(|e| Line::from(Span::styled(e.as_str(), Style::default().fg(Color::Red))))
            .collect();
        
        let errors = Paragraph::new(error_text)
            .style(Style::default().fg(Color::Red))
            .block(Block::default().borders(Borders::ALL).title("Validation Errors"));
        f.render_widget(errors, chunks[chunk_idx]);
        chunk_idx += 1;
    }
    
    // Status message
    if let Some(ref msg) = app.status_message {
        let color = if msg.starts_with('✓') {
            Color::Green
        } else if msg.starts_with('✗') {
            Color::Red
        } else {
            Color::Yellow
        };
        
        let status = Paragraph::new(msg.as_str())
            .style(Style::default().fg(color))
            .block(Block::default().borders(Borders::ALL).title("Status"));
        f.render_widget(status, chunks[chunk_idx]);
        chunk_idx += 1;
    }

    // Footer
    let help_text = if app.editing {
        "Enter: Save | Esc: Cancel | Type to edit"
    } else if app.show_help {
        "?: Hide help | ↑↓: Navigate | Enter: Edit/Toggle | Space: Toggle | Tab: Next | v: Validate"
    } else {
        "?: Help | ↑↓: Navigate | Enter: Edit/Toggle/Run | Space: Toggle | Tab: Next Tab | v: Validate"
    };
    let footer = Paragraph::new(help_text)
        .style(Style::default().fg(Color::Gray))
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, chunks[chunk_idx]);
}

fn render_fields_tab(f: &mut Frame, area: Rect, app: &App) {
    let fields = app.fields_for_tab(app.selected_tab);
    
    // Split area to show hint at bottom if available
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(if app.show_help {
            vec![Constraint::Min(5), Constraint::Length(3)]
        } else {
            vec![Constraint::Min(5)]
        })
        .split(area);
    
    let list_area = chunks[0];
    
    let items: Vec<ListItem> = fields
        .iter()
        .enumerate()
        .map(|(i, (name, value, is_bool))| {
            let display_value = if app.editing && i == app.current_field {
                format!("{}: {}_", name, app.input_buffer)
            } else if *is_bool {
                format!("{}: [{}]", name, if value == "true" { "X" } else { " " })
            } else {
                format!("{}: {}", name, value)
            };
            
            let style = if i == app.current_field {
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };
            
            ListItem::new(Line::from(display_value)).style(style)
        })
        .collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(TABS[app.selected_tab]))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        );

    f.render_widget(list, list_area);
    
    // Show hint for current field if help is enabled
    if app.show_help && chunks.len() > 1 {
        if let Some(hint) = app.get_field_hint(app.selected_tab, app.current_field) {
            let hint_widget = Paragraph::new(hint)
                .style(Style::default().fg(Color::Cyan))
                .block(Block::default().borders(Borders::ALL).title("Hint"))
                .wrap(Wrap { trim: false });
            f.render_widget(hint_widget, chunks[1]);
        }
    }
}

fn render_run_tab(f: &mut Frame, area: Rect, app: &App) {
    let args = app.config.to_cli_args();
    let cmd = format!("./capture {}", args.join(" "));
    
    let text = vec![
        Line::from(""),
        Line::from(Span::styled("Ready to run with the following configuration:", Style::default().fg(Color::Green))),
        Line::from(""),
        Line::from(Span::styled("Command:", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))),
        Line::from(""),
    ];
    
    let mut all_lines = text;
    
    // Wrap command for display
    for line in wrap_text(&cmd, area.width.saturating_sub(4) as usize) {
        all_lines.push(Line::from(line));
    }
    
    all_lines.push(Line::from(""));
    all_lines.push(Line::from(""));
    all_lines.push(Line::from(Span::styled("Press Enter to run, or 'q' to quit", Style::default().fg(Color::Yellow))));
    
    let paragraph = Paragraph::new(all_lines)
        .block(Block::default().borders(Borders::ALL).title("Run"))
        .wrap(Wrap { trim: false });
    
    f.render_widget(paragraph, area);
}

fn wrap_text(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current_line = String::new();
    
    for word in text.split_whitespace() {
        if current_line.len() + word.len() + 1 > width {
            if !current_line.is_empty() {
                lines.push(current_line.clone());
                current_line.clear();
            }
            if word.len() > width {
                // Word too long, break it
                lines.push(word[..width].to_string());
                current_line = word[width..].to_string();
            } else {
                current_line = word.to_string();
            }
        } else {
            if !current_line.is_empty() {
                current_line.push(' ');
            }
            current_line.push_str(word);
        }
    }
    
    if !current_line.is_empty() {
        lines.push(current_line);
    }
    
    lines
}
