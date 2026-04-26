use collect_core::PartitionGranularity;
use collect_tui::{FieldKind, FieldState, TuiModel};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct TuiConfig {
    pub input_path: String,
    pub source: String,
    pub ais: bool,
    pub out_dir: String,
    pub partition: String,
    pub max_rows: String,
    pub max_batch_bytes: String,
    pub keep_local: bool,
    pub s3_bucket: String,
    pub s3_endpoint: String,
    pub s3_region: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_disable_tls: bool,
}

impl Default for TuiConfig {
    fn default() -> Self {
        Self {
            input_path: String::new(),
            source: String::new(),
            ais: false,
            out_dir: "data".to_string(),
            partition: "minute".to_string(),
            max_rows: String::new(),
            max_batch_bytes: String::new(),
            keep_local: false,
            s3_bucket: String::new(),
            s3_endpoint: String::new(),
            s3_region: "us-east-1".to_string(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_disable_tls: false,
        }
    }
}

impl TuiConfig {
    pub fn load_from_env() -> Self {
        let mut config = Self::default();

        if let Some(value) = env_value(&["INPUT_PATH", "INPUT_FILE"]) {
            config.input_path = value;
        }
        if let Some(value) = env_value(&["SOURCE"]) {
            config.source = value;
        }
        if let Some(value) = bool_env(&["AIS"]) {
            config.ais = value;
        }
        if let Some(value) = env_value(&["OUT_DIR"]) {
            config.out_dir = value;
        }
        if let Some(value) = env_value(&["PARTITION"]) {
            config.partition = value;
        }
        if let Some(value) = env_value(&["MAX_ROWS"]) {
            config.max_rows = value;
        }
        if let Some(value) = env_value(&["MAX_BATCH_BYTES"]) {
            config.max_batch_bytes = value;
        }
        if let Some(value) = env_value(&["S3_BUCKET"]) {
            config.s3_bucket = value;
        }
        if let Some(value) = env_value(&["S3_ENDPOINT"]) {
            config.s3_endpoint = value;
        }
        if let Some(value) = env_value(&["S3_REGION"]) {
            config.s3_region = value;
        }
        if let Some(value) = env_value(&["S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"]) {
            config.s3_access_key = value;
        }
        if let Some(value) = env_value(&["S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"]) {
            config.s3_secret_key = value;
        }
        if let Some(value) = bool_env(&["KEEP_LOCAL"]) {
            config.keep_local = value;
        }
        if let Some(value) = bool_env(&["S3_DISABLE_TLS"]) {
            config.s3_disable_tls = value;
        }

        config
    }
}

impl TuiModel for TuiConfig {
    fn app_title() -> &'static str {
        "collect-file"
    }

    fn run_command_prefix() -> &'static str {
        "cargo run -p collect-file --"
    }

    fn default_config_file_path() -> &'static str {
        "collect-file-config.json"
    }

    fn from_env() -> Self {
        Self::load_from_env()
    }

    fn fields_for_tab(&self, tab: usize) -> Vec<FieldState> {
        match tab {
            0 => vec![
                FieldState {
                    label: "Input Path",
                    value: self.input_path.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "Source Label",
                    value: self.source.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "AIS Timestamping",
                    value: self.ais.to_string(),
                    kind: FieldKind::Bool,
                },
            ],
            1 => vec![
                FieldState {
                    label: "Output Directory",
                    value: self.out_dir.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "Partition Granularity",
                    value: self.partition.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "Max Rows per File",
                    value: self.max_rows.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "Max Batch Bytes",
                    value: self.max_batch_bytes.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "Keep Local Files",
                    value: self.keep_local.to_string(),
                    kind: FieldKind::Bool,
                },
            ],
            2 => vec![
                FieldState {
                    label: "S3 Bucket",
                    value: self.s3_bucket.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "S3 Endpoint",
                    value: self.s3_endpoint.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "S3 Region",
                    value: self.s3_region.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "S3 Access Key",
                    value: self.s3_access_key.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "S3 Secret Key",
                    value: if self.s3_secret_key.is_empty() {
                        String::new()
                    } else {
                        "***".to_string()
                    },
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "Disable TLS",
                    value: self.s3_disable_tls.to_string(),
                    kind: FieldKind::Bool,
                },
            ],
            _ => vec![],
        }
    }

    fn field_hint(tab: usize, field: usize) -> Option<&'static str> {
        match (tab, field) {
            (0, 0) => Some("e.g., /path/to/data or /path/to/directory"),
            (0, 1) => Some("e.g., test-data, norway, or leave blank to auto-detect"),
            (0, 2) => {
                Some("Use NMEA c:<epoch> tag blocks or $PGHP capture timestamps when present")
            }
            (1, 0) => Some("e.g., data, /mnt/storage/parquet"),
            (1, 1) => Some("minute, hour, day, month, or year"),
            (1, 2) => Some("e.g., 10000 (optional, flushes on the selected boundary)"),
            (1, 3) => Some("e.g., 67108864 (optional, defaults to 64 MiB)"),
            (2, 0) => Some("e.g., my-bucket-name"),
            (2, 1) => Some("e.g., https://s3.example.com (for MinIO, R2, etc.)"),
            (2, 2) => Some("e.g., us-east-1, us-west-2, eu-central-1"),
            (2, 3) => Some("S3 or AWS access key ID"),
            (2, 4) => Some("S3 or AWS secret access key (hidden)"),
            _ => None,
        }
    }

    fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if self.input_path.is_empty() {
            errors.push("⚠ No input path configured. Provide a file or directory".to_string());
        }

        if !self.max_rows.is_empty() && self.max_rows.parse::<usize>().is_err() {
            errors.push("⚠ Max Rows must be a valid positive number".to_string());
        }

        if self.partition.parse::<PartitionGranularity>().is_err() {
            errors.push("⚠ Partition must be minute, hour, day, month, or year".to_string());
        }

        if !self.max_batch_bytes.is_empty() && self.max_batch_bytes.parse::<usize>().is_err() {
            errors.push("⚠ Max Batch Bytes must be a valid positive number".to_string());
        }

        if !self.s3_bucket.is_empty() {
            let access_key = !self.s3_access_key.is_empty()
                || env_present(&["S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"]);
            let secret_key = !self.s3_secret_key.is_empty()
                || env_present(&["S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"]);

            if !access_key || !secret_key {
                errors.push(
                    "⚠ S3 credentials required (access key + secret key or env vars)".to_string(),
                );
            }
        }

        errors
    }

    fn set_field_value(&mut self, tab: usize, field: usize, value: String) {
        match tab {
            0 => match field {
                0 => self.input_path = value,
                1 => self.source = value,
                2 => self.ais = matches!(value.to_ascii_lowercase().as_str(), "true" | "1"),
                _ => {}
            },
            1 => match field {
                0 => self.out_dir = value,
                1 => self.partition = value,
                2 => self.max_rows = value,
                3 => self.max_batch_bytes = value,
                4 => self.keep_local = !self.keep_local,
                _ => {}
            },
            2 => match field {
                0 => self.s3_bucket = value,
                1 => self.s3_endpoint = value,
                2 => self.s3_region = value,
                3 => self.s3_access_key = value,
                4 => self.s3_secret_key = value,
                5 => self.s3_disable_tls = !self.s3_disable_tls,
                _ => {}
            },
            _ => {}
        }
    }

    fn toggle_field(&mut self, tab: usize, field: usize) {
        match tab {
            0 if field == 2 => self.ais = !self.ais,
            1 if field == 4 => self.keep_local = !self.keep_local,
            2 if field == 5 => self.s3_disable_tls = !self.s3_disable_tls,
            _ => {}
        }
    }

    fn to_cli_args(&self) -> Vec<String> {
        let mut args = vec![];

        if !self.input_path.is_empty() {
            args.push("--input".to_string());
            args.push(self.input_path.clone());
        }
        if !self.source.is_empty() {
            args.push("--source".to_string());
            args.push(self.source.clone());
        }

        if self.ais {
            args.push("--ais".to_string());
        }

        args.push("--out-dir".to_string());
        args.push(self.out_dir.clone());
        if !self.partition.is_empty() {
            args.push("--partition".to_string());
            args.push(self.partition.clone());
        }
        if !self.max_rows.is_empty() {
            args.push("--max-rows".to_string());
            args.push(self.max_rows.clone());
        }
        if !self.max_batch_bytes.is_empty() {
            args.push("--max-batch-bytes".to_string());
            args.push(self.max_batch_bytes.clone());
        }
        if self.keep_local {
            args.push("--keep-local".to_string());
        }

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

        args
    }
}

fn env_value(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| std::env::var(key).ok())
}

fn env_present(keys: &[&str]) -> bool {
    keys.iter().any(|key| std::env::var(key).is_ok())
}

fn bool_env(keys: &[&str]) -> Option<bool> {
    env_value(keys).map(|value| matches!(value.to_ascii_lowercase().as_str(), "true" | "1"))
}
