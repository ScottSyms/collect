//! Structured logging for operationally-significant events — startup and
//! shutdown, reconnect/backoff attempts, health-state transitions, retry
//! exhaustion, partition/file failures, and exit-code decisions.
//!
//! This is deliberately narrow: routine per-row/per-file progress output
//! (`emit_ingest_progress` and friends) stays as plain `eprintln!` text.
//! What lives here is the subset of output an operator or a telemetry
//! pipeline actually needs to notice and act on, so converting it costs one
//! call per site instead of a rewrite of every print statement.
//!
//! Two formats, chosen once per process and never mixed:
//! - `text` — a human-readable line, close to the existing emoji-prefixed
//!   style, for an interactive terminal.
//! - `json` — one self-contained JSON object per line to stderr, for a
//!   container/Nomad log driver or any telemetry pipeline that tails logs.
//!
//! The default auto-detects: `json` when stderr is not a TTY, `text` when it
//! is. `--log-format`/`LOG_FORMAT` overrides either way.
//!
//! `--quiet` is unrelated to this module — it only suppresses the routine
//! progress lines. These events are fault/diagnostic signals, not chatter,
//! so they are never suppressed by `--quiet`. `--verbose`/`VERBOSE` gates
//! [`Level::Debug`] events specifically.

use clap::{Args, ValueEnum};
use serde::Serialize;
use std::io::IsTerminal;
use std::sync::OnceLock;

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
pub enum LogFormat {
    /// Human-readable, emoji-prefixed lines (the default on a TTY).
    Text,
    /// One JSON object per line to stderr (the default off a TTY).
    Json,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }

    fn icon(self) -> &'static str {
        match self {
            Level::Debug => "🐛",
            Level::Info => "ℹ️",
            Level::Warn => "⚠️",
            Level::Error => "❌",
        }
    }
}

struct LoggerState {
    binary: &'static str,
    format: LogFormat,
    verbose: bool,
}

static STATE: OnceLock<LoggerState> = OnceLock::new();

/// CLI flags every binary flattens to control this module — separate from
/// [`crate::CommonCliArgs`] since the batch parsers (`ais-parse`,
/// `aisstream-parse`) don't flatten that struct but still want logging
/// control.
#[derive(Clone, Debug, Args)]
pub struct LoggingCliArgs {
    /// Log format for operationally-significant events (default: json off a
    /// TTY, text on one)
    #[arg(long = "log-format", env = "LOG_FORMAT", value_enum)]
    pub log_format: Option<LogFormat>,

    /// Emit debug-level events in addition to info/warn/error
    #[arg(short = 'v', long = "verbose", env = "VERBOSE", value_parser = clap::builder::FalseyValueParser::new())]
    pub verbose: bool,
}

impl LoggingCliArgs {
    /// Call once, near the top of `main()`, before anything else logs.
    pub fn init(&self, binary: &'static str) {
        init(binary, self.log_format, self.verbose);
    }
}

/// Initialize the logger. Only the first call takes effect (matches the
/// usual `env_logger`/`tracing_subscriber::init`-style idiom) — later calls,
/// e.g. from a test harness that doesn't go through [`LoggingCliArgs`], are
/// silently ignored rather than panicking.
pub fn init(binary: &'static str, format: Option<LogFormat>, verbose: bool) {
    let format = format.unwrap_or_else(|| {
        if std::io::stderr().is_terminal() {
            LogFormat::Text
        } else {
            LogFormat::Json
        }
    });
    let _ = STATE.set(LoggerState {
        binary,
        format,
        verbose,
    });
}

fn state() -> &'static LoggerState {
    STATE.get_or_init(|| LoggerState {
        binary: "unknown",
        format: LogFormat::Text,
        verbose: false,
    })
}

#[derive(Serialize)]
struct JsonEvent<'a> {
    ts: String,
    level: &'static str,
    binary: &'static str,
    event: &'a str,
    message: &'a str,
    #[serde(flatten)]
    fields: std::collections::BTreeMap<&'a str, &'a str>,
}

/// Emit one event. `fields` are simple key/value context (already
/// formatted by the caller); kept as `&str` pairs rather than `dyn Display`
/// to keep call sites and this function trivial to reason about, since the
/// scope here is a few dozen sites, not universal logging.
pub fn emit(level: Level, event: &str, message: &str, fields: &[(&str, &str)]) {
    let st = state();
    if level == Level::Debug && !st.verbose {
        return;
    }
    match st.format {
        LogFormat::Json => {
            let event = JsonEvent {
                ts: chrono::Utc::now().to_rfc3339(),
                level: level.as_str(),
                binary: st.binary,
                event,
                message,
                fields: fields.iter().copied().collect(),
            };
            if let Ok(line) = serde_json::to_string(&event) {
                eprintln!("{line}");
            }
        }
        LogFormat::Text => {
            if fields.is_empty() {
                eprintln!("{} {message}", level.icon());
            } else {
                let ctx: Vec<String> = fields.iter().map(|(k, v)| format!("{k}={v}")).collect();
                eprintln!("{} {message} ({})", level.icon(), ctx.join(", "));
            }
        }
    }
}

pub fn debug(event: &str, message: &str, fields: &[(&str, &str)]) {
    emit(Level::Debug, event, message, fields);
}

pub fn info(event: &str, message: &str, fields: &[(&str, &str)]) {
    emit(Level::Info, event, message, fields);
}

pub fn warn(event: &str, message: &str, fields: &[(&str, &str)]) {
    emit(Level::Warn, event, message, fields);
}

pub fn error(event: &str, message: &str, fields: &[(&str, &str)]) {
    emit(Level::Error, event, message, fields);
}
