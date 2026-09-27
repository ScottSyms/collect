//! Bounded exponential backoff for reconnecting to an upstream (TCP, Kafka
//! broker, WebSocket).
//!
//! `collect-socket` and `collect-aisstream` used to hand-roll this same
//! 1s-to-5s exponential backoff independently, and `collect-kafka` had none
//! at all — a broker outage meant an immediate, undelayed reconnect attempt
//! with no bound, so a persistent outage retried forever with no signal to
//! the operator. This module gives all three the same behavior: keep
//! retrying at a capped delay, but give up and exit
//! [`crate::exitcode::UPSTREAM_UNAVAILABLE`] once `--max-reconnect-seconds`
//! of *total* retrying has passed, so Nomad's restart policy (or an
//! operator) can take over instead of the process sitting in a silent,
//! permanent retry loop.

use crate::{exitcode, log};
use clap::Args;
use std::cmp::min;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// A collector that legitimately expects to run unattended for a long time
/// against a flaky link may want the old "retry forever" behavior back —
/// `0` opts out of the bound entirely.
const DEFAULT_MAX_RECONNECT_SECONDS: u64 = 300;

/// CLI flag for the give-up bound below. Separate from [`crate::CommonCliArgs`]
/// since it only makes sense for a source with a live upstream connection to
/// retry (`collect-socket`, `collect-kafka`, `collect-aisstream`) — not
/// `collect-file`, which has no such connection.
#[derive(Clone, Debug, Args)]
pub struct ReconnectCliArgs {
    /// Give up on the upstream connection after this many seconds of
    /// bounded retry and exit (0 = retry forever, the old behavior)
    #[arg(
        long = "max-reconnect-seconds",
        env = "MAX_RECONNECT_SECONDS",
        default_value_t = DEFAULT_MAX_RECONNECT_SECONDS
    )]
    pub max_reconnect_seconds: u64,
}

/// Exponential backoff with a capped per-attempt delay and an optional
/// overall deadline.
pub struct Backoff {
    delay: Duration,
    max_delay: Duration,
    deadline: Option<Instant>,
}

impl Backoff {
    /// `max_total_secs == 0` means no deadline — retry forever at
    /// `max_delay`, matching the pre-existing (and still supported)
    /// behavior for a deployment that wants it.
    pub fn new(initial: Duration, max_delay: Duration, max_total_secs: u64) -> Self {
        let deadline = if max_total_secs == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_secs(max_total_secs))
        };
        Backoff {
            delay: initial,
            max_delay,
            deadline,
        }
    }

    /// Sleep for the current delay (unless `shutdown` is already set or the
    /// deadline has passed), then double the delay up to `max_delay`.
    /// Returns `false` when the caller should stop retrying: either the
    /// deadline is exhausted, or shutdown fired while waiting.
    pub async fn wait(&mut self, shutdown: &AtomicBool) -> bool {
        if shutdown.load(Ordering::SeqCst) {
            return false;
        }
        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                return false;
            }
        }
        tokio::time::sleep(self.delay).await;
        self.delay = min(self.delay.saturating_mul(2), self.max_delay);
        !shutdown.load(Ordering::SeqCst)
    }
}

/// Log the exhaustion of a reconnect deadline and exit
/// [`exitcode::UPSTREAM_UNAVAILABLE`]. Called at the point a caller's own
/// retry loop gives up; never returns.
pub fn give_up(upstream: &str, max_reconnect_seconds: u64) -> ! {
    log::error(
        "upstream_unavailable",
        &format!(
            "giving up on {upstream} after {max_reconnect_seconds}s of bounded retry"
        ),
        &[("upstream", upstream)],
    );
    std::process::exit(exitcode::UPSTREAM_UNAVAILABLE);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delay_doubles_up_to_the_cap() {
        let mut backoff = Backoff::new(
            Duration::from_millis(10),
            Duration::from_millis(30),
            0, // unbounded
        );
        let shutdown = AtomicBool::new(false);
        assert_eq!(backoff.delay, Duration::from_millis(10));
        assert!(backoff.wait(&shutdown).await);
        assert_eq!(backoff.delay, Duration::from_millis(20));
        assert!(backoff.wait(&shutdown).await);
        assert_eq!(backoff.delay, Duration::from_millis(30)); // capped
        assert!(backoff.wait(&shutdown).await);
        assert_eq!(backoff.delay, Duration::from_millis(30)); // stays capped
    }

    #[tokio::test]
    async fn shutdown_stops_waiting_immediately() {
        let mut backoff = Backoff::new(Duration::from_secs(5), Duration::from_secs(5), 0);
        let shutdown = AtomicBool::new(true);
        assert!(!backoff.wait(&shutdown).await);
    }

    #[tokio::test]
    async fn deadline_is_exhausted_after_max_total_secs() {
        // A 0-delay backoff with a 0-second bound (rounds to "immediately
        // exhausted" since Instant::now() + Duration::from_secs(0) is now).
        let mut backoff = Backoff::new(Duration::from_millis(1), Duration::from_millis(1), 0);
        // Force an already-past deadline directly, since a real deadline is
        // seconds-granular and a unit test shouldn't sleep for real seconds.
        backoff.deadline = Some(Instant::now() - Duration::from_secs(1));
        let shutdown = AtomicBool::new(false);
        assert!(!backoff.wait(&shutdown).await);
    }

    #[test]
    fn zero_max_total_means_no_deadline() {
        let backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(5), 0);
        assert!(backoff.deadline.is_none());
    }

    #[test]
    fn nonzero_max_total_sets_a_future_deadline() {
        let backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(5), 60);
        let deadline = backoff.deadline.expect("deadline set");
        assert!(deadline > Instant::now());
    }
}
