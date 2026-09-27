//! Shared process exit codes.
//!
//! Every binary in this workspace runs either ad hoc or under Nomad, which
//! primarily learns whether a process is healthy from its exit code (its
//! restart policy, `nomad job status`/`alloc status`, and any alerting on
//! exit code all key off this). Before this module each binary either
//! defined its own local `EXIT_NOTHING_TO_DO` constant (duplicated three
//! times) or fell back on `anyhow`'s implicit default of exit 1 for any
//! `Err`, so nothing distinguished *why* a process gave up. All six binaries
//! should use these constants instead of a local one or a bare
//! `std::process::exit`.
//!
//! The HTTP `/healthz` endpoint and the health-status file
//! (`--health-check`) are secondary signals for a process that is still
//! running — see [`crate::metrics::HealthState`]. The exit code is
//! authoritative for "this process gave up and stopped."

/// Success.
pub const SUCCESS: i32 = 0;

/// Unclassified error, e.g. bad configuration — `anyhow`'s implicit default
/// for any `Err` returned from `main`. Not a named constant on purpose:
/// every other code here is deliberately chosen at a specific decision
/// point, this one is everything else.
pub const UNCLASSIFIED_ERROR: i32 = 1;

/// Nothing matched the given filters/inputs — a successful run that found
/// no work to do (e.g. no partitions matched, no files under the input
/// path). Distinct from success so a caller can tell "ran and did nothing"
/// from "ran and did something."
pub const NOTHING_TO_DO: i32 = 2;

/// A collector's upstream connection (TCP, Kafka broker, WebSocket) could
/// not be (re-)established within the configured bounded-retry window.
pub const UPSTREAM_UNAVAILABLE: i32 = 3;

/// A collector received no rows for longer than the configured data-drought
/// window, even though its upstream connection appears fine.
pub const DATA_DROUGHT: i32 = 4;

/// A batch parser (`ais-parse`/`aisstream-parse`) skipped more
/// partitions/files than `--max-partition-failures` allows.
pub const PARTIAL_FAILURE_THRESHOLD: i32 = 5;
