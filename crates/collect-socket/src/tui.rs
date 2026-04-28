use collect_core::PartitionGranularity;
use collect_tui::{FieldKind, FieldState, TuiModel};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct TuiConfig {
    pub tcp_host: String,
    pub tcp_port: String,
    pub source: String,
    pub out_dir: String,
    pub partition: String,
    pub max_rows: String,
    pub max_batch_bytes: String,
    pub compression_level: String,
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
            tcp_host: String::new(),
            tcp_port: String::new(),
            source: String::new(),
            out_dir: "data".to_string(),
            partition: "minute".to_string(),
            max_rows: String::new(),
            max_batch_bytes: String::new(),
            compression_level: String::new(),
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

        if let Some(value) = env_value(&["TCP_HOST"]) {
            config.tcp_host = value;
        }
        if let Some(value) = env_value(&["TCP_PORT"]) {
            config.tcp_port = value;
        }
        if let Some(value) = env_value(&["SOURCE"]) {
            config.source = value;
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
        if let Some(value) = env_value(&["COMPRESSION_LEVEL"]) {
            config.compression_level = value;
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
        "collect-socket"
    }

    fn run_command_prefix() -> &'static str {
        "cargo run -p collect-socket --"
    }

    fn default_config_file_path() -> &'static str {
        "collect-socket-config.json"
    }

    fn from_env() -> Self {
        Self::load_from_env()
    }

    fn fields_for_tab(&self, tab: usize) -> Vec<FieldState> {
        match tab {
            0 => vec![
                FieldState {
                    label: "TCP Host",
                    value: self.tcp_host.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "TCP Port",
                    value: self.tcp_port.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "Source Label",
                    value: self.source.clone(),
                    kind: FieldKind::Text,
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
                    label: "Compression Level",
                    value: self.compression_level.clone(),
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
            (0, 0) => Some("e.g., 153.44.253.27"),
            (0, 1) => Some("e.g., 5631"),
            (0, 2) => Some("e.g., norway-tcp, telemetry, or leave blank"),
            (1, 0) => Some("e.g., data, /mnt/storage/parquet"),
            (1, 1) => Some("minute, hour, day, month, or year"),
            (1, 2) => Some("e.g., 10000 (optional, flushes on the selected boundary)"),
            (1, 3) => Some("e.g., 67108864 (optional, defaults to 64 MiB)"),
            (1, 4) => Some("e.g., 1-22 (optional, defaults to 5; lower is faster)"),
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

        if self.tcp_host.is_empty() {
            errors.push("⚠ TCP Host is required".to_string());
        }

        if self.tcp_port.is_empty() {
            errors.push("⚠ TCP Port is required".to_string());
        } else if self.tcp_port.parse::<u16>().is_err() {
            errors.push("⚠ TCP Port must be a valid number (1-65535)".to_string());
        }

        if self.partition.parse::<PartitionGranularity>().is_err() {
            errors.push("⚠ Partition must be minute, hour, day, month, or year".to_string());
        }

        if !self.max_rows.is_empty() && self.max_rows.parse::<usize>().is_err() {
            errors.push("⚠ Max Rows must be a valid positive number".to_string());
        }

        if !self.max_batch_bytes.is_empty() && self.max_batch_bytes.parse::<usize>().is_err() {
            errors.push("⚠ Max Batch Bytes must be a valid positive number".to_string());
        }

        if !self.compression_level.is_empty() && self.compression_level.parse::<i32>().is_err() {
            errors.push("⚠ Compression Level must be a valid integer".to_string());
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
                0 => self.tcp_host = value,
                1 => self.tcp_port = value,
                2 => self.source = value,
                _ => {}
            },
            1 => match field {
                0 => self.out_dir = value,
                1 => self.partition = value,
                2 => self.max_rows = value,
                3 => self.max_batch_bytes = value,
                4 => self.compression_level = value,
                5 => self.keep_local = !self.keep_local,
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
            1 if field == 5 => self.keep_local = !self.keep_local,
            2 if field == 5 => self.s3_disable_tls = !self.s3_disable_tls,
            _ => {}
        }
    }

    fn to_cli_args(&self) -> Vec<String> {
        let mut args = vec![];

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
        if !self.compression_level.is_empty() {
            args.push("--compression-level".to_string());
            args.push(self.compression_level.clone());
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
