use std::sync::atomic::{AtomicBool, Ordering};

static CANCEL: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusMode {
    Plain,
    Tui,
}

impl StatusMode {
    pub fn from_tty(is_tty: bool) -> Self {
        if is_tty {
            Self::Tui
        } else {
            Self::Plain
        }
    }

    pub fn is_plain(self) -> bool {
        self == Self::Plain
    }
}

pub fn request_cancel() {
    CANCEL.store(true, Ordering::SeqCst);
}

pub fn is_cancelled() -> bool {
    CANCEL.load(Ordering::SeqCst)
}

/// Print a plain progress update every N partitions.
pub fn should_emit_plain_update(processed: usize, every: usize) -> bool {
    every > 0 && processed % every == 0
}

pub fn print_plain_update(processed: usize, total: usize) {
    let pct = if total > 0 {
        processed * 100 / total
    } else {
        100
    };
    eprintln!("  [{:>3}%] partitions: {}/{}", pct, processed, total);
}
