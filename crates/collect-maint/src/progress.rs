//! Progress reporting for long compaction runs: phase timings per partition,
//! running throughput with an ETA, and a heartbeat while one phase runs long.
//!
//! Plain lines on stdout rather than a progress bar, so the output reads the
//! same in a terminal, a log file, or a Nomad/cron job's captured output.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How often a phase that is still running says so.
const HEARTBEAT_EVERY: Duration = Duration::from_secs(30);

/// `1.5s`, `3m 12s`, `2h 14m`.
pub fn fmt_duration(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s < 60.0 {
        format!("{s:.1}s")
    } else if s < 3600.0 {
        format!("{}m {:02}s", (s / 60.0) as u64, (s % 60.0) as u64)
    } else {
        format!("{}h {:02}m", (s / 3600.0) as u64, ((s % 3600.0) / 60.0) as u64)
    }
}

/// MiB below 1 GiB, GiB above.
pub fn fmt_bytes(bytes: u64) -> String {
    let mib = bytes as f64 / (1024.0 * 1024.0);
    if mib < 1024.0 {
        format!("{mib:.1} MiB")
    } else {
        format!("{:.2} GiB", mib / 1024.0)
    }
}

/// Prints `label: <phase> for <elapsed>` every [`HEARTBEAT_EVERY`] while
/// alive, so a partition that takes minutes to read, sort or write doesn't
/// look hung. Stops when dropped.
pub struct Heartbeat {
    state: Arc<Mutex<(&'static str, Instant)>>,
    task: tokio::task::JoinHandle<()>,
}

impl Heartbeat {
    pub fn start(label: &str) -> Self {
        let state = Arc::new(Mutex::new(("starting", Instant::now())));
        let shared = state.clone();
        let label = label.to_string();
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(HEARTBEAT_EVERY);
            tick.tick().await; // the first tick fires immediately
            loop {
                tick.tick().await;
                let (phase, since) = *shared.lock().unwrap();
                println!("    {label}: still {phase} ({})", fmt_duration(since.elapsed()));
            }
        });
        Self { state, task }
    }

    /// Marks the start of `phase`.
    pub fn set(&self, phase: &'static str) {
        *self.state.lock().unwrap() = (phase, Instant::now());
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Bytes of compressed input processed so far against the planned total.
pub struct Throughput {
    start: Instant,
    total: u64,
    done: u64,
}

impl Throughput {
    pub fn new(total: u64) -> Self {
        Self { start: Instant::now(), total, done: 0 }
    }

    pub fn add(&mut self, bytes: u64) {
        self.done += bytes;
    }

    /// `progress: 4.10 GiB of 41.20 GiB (10.0%) · 18.3 MiB/s · elapsed 3m 45s · ETA 33m 10s`
    pub fn line(&self) -> String {
        let elapsed = self.start.elapsed();
        let pct = if self.total == 0 {
            100.0
        } else {
            self.done as f64 * 100.0 / self.total as f64
        };
        let rate = self.done as f64 / elapsed.as_secs_f64().max(1e-3);
        let eta = if self.done == 0 || rate <= 0.0 {
            "unknown".to_string()
        } else {
            let left = self.total.saturating_sub(self.done) as f64 / rate;
            fmt_duration(Duration::from_secs_f64(left))
        };
        format!(
            "progress: {} of {} ({pct:.1}%) · {:.1} MiB/s · elapsed {} · ETA {eta}",
            fmt_bytes(self.done),
            fmt_bytes(self.total),
            rate / (1024.0 * 1024.0),
            fmt_duration(elapsed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(fmt_duration(Duration::from_millis(1500)), "1.5s");
        assert_eq!(fmt_duration(Duration::from_secs(192)), "3m 12s");
        assert_eq!(fmt_duration(Duration::from_secs(2 * 3600 + 14 * 60 + 5)), "2h 14m");
    }

    #[test]
    fn bytes() {
        assert_eq!(fmt_bytes(512 << 20), "512.0 MiB");
        assert_eq!(fmt_bytes(3 << 30), "3.00 GiB");
    }

    #[test]
    fn throughput_without_progress_has_no_eta() {
        let t = Throughput::new(1 << 30);
        assert!(t.line().contains("ETA unknown"), "{}", t.line());
    }
}
