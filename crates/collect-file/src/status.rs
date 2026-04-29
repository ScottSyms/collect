use anyhow::Result;
use chrono::Utc;
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Terminal,
};
use std::io;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

const REPORT_EVERY: usize = 10;
static CANCELLED: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatusMode {
    Tui,
    Plain,
}

impl StatusMode {
    pub(crate) fn from_tty(use_tui: bool) -> Self {
        if use_tui {
            Self::Tui
        } else {
            Self::Plain
        }
    }

    pub(crate) fn is_tui(self) -> bool {
        matches!(self, Self::Tui)
    }

    pub(crate) fn is_plain(self) -> bool {
        matches!(self, Self::Plain)
    }
}

pub(crate) fn should_emit_plain_update(completed: usize, total: usize) -> bool {
    completed == total || (completed > 0 && completed % REPORT_EVERY == 0)
}

pub(crate) fn request_cancel() {
    CANCELLED.store(1, Ordering::SeqCst);
}

pub(crate) fn is_cancelled() -> bool {
    CANCELLED.load(Ordering::SeqCst) == 1
}

pub(crate) fn print_plain_update(completed: usize, total: usize, started_at: Instant) {
    eprintln!("📊 {}", format_plain_update(completed, total, started_at));
}

pub(crate) fn spawn_status_tui(
    total_files: usize,
    completed_files: Arc<AtomicUsize>,
    running: Arc<AtomicBool>,
    started_at: Instant,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        if let Err(error) = run_status_tui(total_files, completed_files, running, started_at) {
            eprintln!("⚠️  Status UI unavailable: {}", error);
        }
    })
}

fn run_status_tui(
    total_files: usize,
    completed_files: Arc<AtomicUsize>,
    running: Arc<AtomicBool>,
    started_at: Instant,
) -> Result<()> {
    CANCELLED.store(0, Ordering::SeqCst);
    enable_raw_mode()?;
    let _cleanup = TerminalCleanup;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    run_status_loop(
        &mut terminal,
        total_files,
        completed_files,
        running,
        started_at,
    )?;
    terminal.show_cursor()?;
    Ok(())
}

struct TerminalCleanup;

impl Drop for TerminalCleanup {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(stdout, LeaveAlternateScreen);
    }
}

fn run_status_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    total_files: usize,
    completed_files: Arc<AtomicUsize>,
    running: Arc<AtomicBool>,
    started_at: Instant,
) -> Result<()> {
    loop {
        terminal.draw(|frame| {
            render_status(
                frame,
                total_files,
                completed_files.load(Ordering::SeqCst),
                started_at,
            )
        })?;

        if !running.load(Ordering::SeqCst) {
            break;
        }

        if event::poll(Duration::from_millis(250))? {
            match event::read()? {
                Event::Key(key)
                    if key.kind == crossterm::event::KeyEventKind::Press
                        && (matches!(key.code, KeyCode::Char('c'))
                            && key.modifiers.contains(KeyModifiers::CONTROL)
                            || matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)) =>
                {
                    request_cancel();
                    break;
                }
                _ => {}
            }
        }

        thread::sleep(Duration::from_secs(1));
    }

    Ok(())
}

fn render_status(
    frame: &mut ratatui::Frame,
    total_files: usize,
    completed: usize,
    started_at: Instant,
) {
    let remaining = total_files.saturating_sub(completed);
    let elapsed = started_at.elapsed();
    let rate = if elapsed.as_secs_f64() > 0.0 {
        completed as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(7),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let header = Paragraph::new(format!(
        "processed {} files | remaining {} files",
        completed, remaining
    ))
    .style(Style::default().fg(Color::Cyan))
    .block(Block::default().borders(Borders::ALL).title("collect-file"));
    frame.render_widget(header, chunks[0]);

    let body = Paragraph::new(vec![
        Line::from("stage ingest"),
        Line::from(format!("{}/{} files", completed, total_files)),
        Line::from(format!("updates every {} files", REPORT_EVERY)),
        Line::from(format!("throughput {:.2} files/s", rate)),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title("Aggregate Stats"),
    );
    frame.render_widget(body, chunks[1]);

    let footer = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[2]);

    let clock = Paragraph::new(Line::from(Span::styled(
        format!("current {}", Utc::now().format("%Y-%m-%d %H:%M:%S UTC")),
        Style::default(),
    )))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(clock, footer[0]);

    let elapsed = Paragraph::new(Line::from(Span::styled(
        format!("elapsed {}", format_duration(elapsed)),
        Style::default(),
    )))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(elapsed, footer[1]);
}

fn format_plain_update(completed: usize, total: usize, started_at: Instant) -> String {
    let remaining = total.saturating_sub(completed);
    let elapsed = format_duration(started_at.elapsed());

    format!(
        "completed {}/{} files | remaining {} | elapsed {}",
        completed, total, remaining, elapsed
    )
}

fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_updates_happen_every_ten_files() {
        assert!(!should_emit_plain_update(9, 25));
        assert!(should_emit_plain_update(10, 25));
        assert!(!should_emit_plain_update(11, 25));
        assert!(should_emit_plain_update(250, 250));
    }

    #[test]
    fn plain_update_uses_completed_file_counts() {
        assert!(format_plain_update(7, 87, Instant::now()).contains("completed 7/87 files"));
        assert!(format_plain_update(37, 87, Instant::now()).contains("completed 37/87 files"));
        assert!(format_plain_update(143, 250, Instant::now()).contains("completed 143/250 files"));
        assert!(format_plain_update(250, 250, Instant::now()).contains("completed 250/250 files"));
    }

    #[test]
    fn plain_update_includes_elapsed_and_remaining() {
        let text = format_plain_update(100, 250, Instant::now() - Duration::from_secs(65));
        assert!(text.contains("completed 100/250 files"));
        assert!(text.contains("remaining 150"));
        assert!(text.contains("elapsed 00:01:05"));
    }
}
