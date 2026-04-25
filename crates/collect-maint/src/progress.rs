use std::fmt::Display;

pub const SCAN_REPORT_INTERVAL: usize = 100;
pub const LIST_REPORT_INTERVAL: usize = 1000;

pub fn report(stage: &str, message: impl Display) {
    eprintln!("[{}] {}", stage, message);
}

pub fn report_step(stage: &str, current: usize, total: usize, detail: impl Display) {
    if should_report(current, total, SCAN_REPORT_INTERVAL) {
        report(stage, format!("{}/{} {}", current, total, detail));
    }
}

pub fn should_report(current: usize, total: usize, interval: usize) -> bool {
    current == 1 || current == total || current % interval == 0
}
