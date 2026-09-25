//! Inline silver-layer parsing for collectors (`--parser`).
//!
//! When a collector runs with `--parser ais` or `--parser aisstream`, every
//! sealed bronze batch (`[ts, payload]`) is additionally decoded into the six
//! typed silver tables (`positions`, `statics`, `meteo`, `binary`, `atons`,
//! `other`) — either committed to Iceberg (when `--iceberg-catalog-uri` is
//! set) or written as Hive-partitioned Parquet under the output root.
//!
//! This module holds only the flag types, the per-batch statistics, and the
//! [`SilverCommit`] trait. Concrete implementations live in the
//! `collect-silver` crate: `collect-core` cannot depend on the parse
//! libraries (`ais-parse` / `aisstream-parse` already depend on
//! `collect-core` for S3 and Iceberg helpers, so the reverse edge would be a
//! dependency cycle).
//!
//! Design notes (see ORCHESTRATOR.md for the queue-based counterpart):
//!
//! - Decoding runs in the write worker, **after** the bronze batch is durable
//!   on local disk — so Kafka offset commits (driven by `on_batch_durable`)
//!   and the shutdown flush keep their existing bronze-only semantics.
//! - A silver failure never fails the bronze batch: it is logged, counted,
//!   and the batch's bronze upload proceeds. The bronze dataset stays the
//!   authoritative backstop; `collect-orchestrator --backfill` repairs gaps.
//! - Dedup is per-batch (a bounded `HashSet`), not global: a crash-replay can
//!   still double-append silver rows. Accepted residual risk, same class as
//!   the batch parsers' narrow six-commit window.

use arrow::record_batch::RecordBatch;
use clap::{Args, ValueEnum};

/// Which inline parser to run on ingested lines. Disabled by default.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
pub enum ParserKind {
    /// No inline parsing (current behavior; bronze only).
    #[default]
    None,
    /// Decode NMEA/AIVDM sentences via the `ais-parse` library.
    Ais,
    /// Decode aisstream.io JSON payloads via the `aisstream-parse` library.
    Aisstream,
}

impl ParserKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Ais => "ais",
            Self::Aisstream => "aisstream",
        }
    }

    pub const fn is_enabled(self) -> bool {
        !matches!(self, Self::None)
    }
}

impl std::fmt::Display for ParserKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ParserKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "none" | "off" | "disabled" => Ok(Self::None),
            "ais" | "ais-parse" | "nmea" => Ok(Self::Ais),
            "aisstream" | "aisstream-parse" | "json" => Ok(Self::Aisstream),
            _ => Err(format!(
                "invalid parser: {value} (expected none, ais, or aisstream)"
            )),
        }
    }
}

/// Shared `--parser` flag, flattened into every collector's CLI.
#[derive(Clone, Debug, Args)]
pub struct ParserCliArgs {
    /// Decode each ingested line into typed silver tables inline.
    /// `ais` decodes NMEA/AIVDM sentences; `aisstream` decodes aisstream.io
    /// JSON payloads. With `--iceberg-catalog-uri` set, silver rows are
    /// committed to the six Iceberg tables; otherwise they are written as
    /// Hive-partitioned Parquet siblings (`positions/`, `statics`, …) under
    /// `--output-dir`. `none` (default) keeps bronze-only behavior.
    #[arg(long, env = "PARSER", default_value = "none")]
    pub parser: ParserKind,

    /// Delete each local bronze Parquet file once its silver rows have been
    /// committed to Iceberg. Requires `--parser` and `--iceberg-catalog-uri`,
    /// and cannot be combined with S3 upload (which already deletes local
    /// files). The raw bronze payloads are then not retained anywhere: rows
    /// that fail to decode are counted in the log but lost. A failed Iceberg
    /// commit keeps the file.
    #[arg(long, env = "DELETE_AFTER_ICEBERG", value_parser = clap::builder::FalseyValueParser::new())]
    pub delete_after_iceberg: bool,
}

impl ParserCliArgs {
    /// Reject flag combinations that cannot work (`iceberg_mode` is whether
    /// `--iceberg-catalog-uri` is set).
    pub fn validate(&self, iceberg_mode: bool) -> anyhow::Result<()> {
        if self.delete_after_iceberg {
            anyhow::ensure!(
                self.parser.is_enabled(),
                "--delete-after-iceberg requires --parser (ais or aisstream)"
            );
            anyhow::ensure!(
                iceberg_mode,
                "--delete-after-iceberg requires --iceberg-catalog-uri"
            );
        }
        Ok(())
    }

    /// Mark `silver` so the write worker deletes the local bronze file after
    /// each successful commit, when `--delete-after-iceberg` is set.
    pub fn wrap(&self, silver: Option<std::sync::Arc<dyn SilverCommit>>) -> Option<std::sync::Arc<dyn SilverCommit>> {
        match silver {
            Some(inner) if self.delete_after_iceberg => {
                Some(std::sync::Arc::new(DeleteAfterCommit(inner)))
            }
            other => other,
        }
    }
}

/// Wrapper that asks the write worker to delete the local bronze file after a
/// successful silver commit (`--delete-after-iceberg`).
struct DeleteAfterCommit(std::sync::Arc<dyn SilverCommit>);

#[async_trait::async_trait]
impl SilverCommit for DeleteAfterCommit {
    async fn commit_bronze_batch(
        &self,
        batch: &RecordBatch,
        source: &str,
    ) -> anyhow::Result<SilverStats> {
        self.0.commit_bronze_batch(batch, source).await
    }

    fn describe(&self) -> String {
        format!("{} (deleting local bronze after commit)", self.0.describe())
    }

    fn delete_local_after_commit(&self) -> bool {
        true
    }
}

/// Per-batch decode statistics, merged into [`crate::IngestMetrics`].
#[derive(Default, Debug, Clone)]
pub struct SilverStats {
    pub rows_in: u64,
    pub positions: u64,
    pub statics: u64,
    pub meteo: u64,
    pub binary: u64,
    pub atons: u64,
    pub other: u64,
    pub incomplete: u64,
    pub failed: u64,
    pub deduped: u64,
}

impl SilverStats {
    pub fn add(&mut self, other: &SilverStats) {
        self.rows_in += other.rows_in;
        self.positions += other.positions;
        self.statics += other.statics;
        self.meteo += other.meteo;
        self.binary += other.binary;
        self.atons += other.atons;
        self.other += other.other;
        self.incomplete += other.incomplete;
        self.failed += other.failed;
        self.deduped += other.deduped;
    }

    pub fn decoded_total(&self) -> u64 {
        self.positions + self.statics + self.meteo + self.binary + self.atons + self.other
    }
}

/// Decodes one sealed bronze `[ts, payload]` batch into silver and makes it
/// durable (Iceberg commit or Hive-Parquet files).
///
/// Implementations must be stateless across batches (or internally
/// synchronized): one instance is shared by all write workers via `Arc`.
/// Returning `Err` signals a commit/IO failure (counted, bronze unaffected);
/// per-row decode failures are counted in [`SilverStats`] instead.
#[async_trait::async_trait]
pub trait SilverCommit: Send + Sync {
    async fn commit_bronze_batch(
        &self,
        batch: &RecordBatch,
        source: &str,
    ) -> anyhow::Result<SilverStats>;

    /// One-line description for the startup log (parser + target).
    fn describe(&self) -> String;

    /// Whether the write worker should delete the local bronze file after a
    /// successful [`commit_bronze_batch`](Self::commit_bronze_batch).
    fn delete_local_after_commit(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parser_kind_parses_cli_names_and_aliases() {
        assert_eq!("none".parse::<ParserKind>().unwrap(), ParserKind::None);
        assert_eq!("ais".parse::<ParserKind>().unwrap(), ParserKind::Ais);
        assert_eq!(
            "aisstream".parse::<ParserKind>().unwrap(),
            ParserKind::Aisstream
        );
        assert_eq!("ais-parse".parse::<ParserKind>().unwrap(), ParserKind::Ais);
        assert_eq!(
            "AISSTREAM-PARSE".parse::<ParserKind>().unwrap(),
            ParserKind::Aisstream
        );
        assert!("bogus".parse::<ParserKind>().is_err());
    }

    #[test]
    fn parser_kind_enabled_only_when_not_none() {
        assert!(!ParserKind::None.is_enabled());
        assert!(ParserKind::Ais.is_enabled());
        assert!(ParserKind::Aisstream.is_enabled());
        assert_eq!(ParserKind::default(), ParserKind::None);
    }

    #[test]
    fn silver_stats_add_merges_counters() {
        let mut total = SilverStats {
            rows_in: 2,
            positions: 1,
            failed: 1,
            ..Default::default()
        };
        total.add(&SilverStats {
            rows_in: 3,
            statics: 2,
            deduped: 1,
            ..Default::default()
        });
        assert_eq!(total.rows_in, 5);
        assert_eq!(total.positions, 1);
        assert_eq!(total.statics, 2);
        assert_eq!(total.failed, 1);
        assert_eq!(total.deduped, 1);
        assert_eq!(total.decoded_total(), 3);
    }
}
