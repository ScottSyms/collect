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
    atomic::{AtomicBool, AtomicU8, Ordering},
    Arc, Mutex, OnceLock,
};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatusMode {
    Tui,
    Plain,
}

static MODE: AtomicU8 = AtomicU8::new(0);
static CANCELLED: AtomicU8 = AtomicU8::new(0);
static SESSION: OnceLock<Arc<Mutex<StatusState>>> = OnceLock::new();

#[derive(Debug)]
struct StatusState {
    title: String,
    stage: String,
    detail: String,
    current: usize,
    total: usize,
    started_at: Instant,
    finished: bool,
}

pub(crate) struct StatusSession {
    handle: Option<thread::JoinHandle<()>>,
    running: Arc<AtomicBool>,
}

impl Drop for StatusSession {
    fn drop(&mut self) {
        if let Some(state) = SESSION.get() {
            if let Ok(mut state) = state.lock() {
                state.finished = true;
            }
        }

        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub(crate) fn set_mode(mode: StatusMode) {
    MODE.store(
        match mode {
            StatusMode::Tui => 1,
            StatusMode::Plain => 0,
        },
        Ordering::SeqCst,
    );
}

pub(crate) fn request_cancel() {
    CANCELLED.store(1, Ordering::SeqCst);
}

pub(crate) fn is_cancelled() -> bool {
    CANCELLED.load(Ordering::SeqCst) == 1
}

pub(crate) fn mode() -> StatusMode {
    if MODE.load(Ordering::SeqCst) == 1 {
        StatusMode::Tui
    } else {
        StatusMode::Plain
    }
}

pub(crate) fn start(title: impl Into<String>) -> StatusSession {
    CANCELLED.store(0, Ordering::SeqCst);
    let state = Arc::new(Mutex::new(StatusState {
        title: title.into(),
        stage: String::new(),
        detail: String::new(),
        current: 0,
        total: 0,
        started_at: Instant::now(),
        finished: false,
    }));
    let _ = SESSION.set(state.clone());

    if mode() != StatusMode::Tui {
        return StatusSession {
            handle: None,
            running: Arc::new(AtomicBool::new(false)),
        };
    }

    let running = Arc::new(AtomicBool::new(true));
    let running_for_thread = running.clone();
    let handle = thread::spawn(move || {
        if let Err(error) = run_tui(state, running_for_thread) {
            eprintln!("⚠️  Status UI unavailable: {}", error);
        }
    });

    StatusSession {
        handle: Some(handle),
        running,
    }
}

pub(crate) fn update_message(stage: &str, message: impl Into<String>) {
    if mode() == StatusMode::Plain {
        return;
    }

    if let Some(state) = SESSION.get() {
        if let Ok(mut state) = state.lock() {
            state.stage = stage.to_string();
            state.detail = message.into();
        }
    }
}

pub(crate) fn update_step(stage: &str, current: usize, total: usize, detail: impl Into<String>) {
    let detail = detail.into();

    match mode() {
        StatusMode::Plain => {
            if current == total || current % 100 == 0 {
                eprintln!("[{}] {}/{} {}", stage, current, total, detail);
            }
        }
        StatusMode::Tui => {
            if let Some(state) = SESSION.get() {
                if let Ok(mut state) = state.lock() {
                    state.stage = stage.to_string();
                    state.detail = detail;
                    state.current = current;
                    state.total = total;
                }
            }
        }
    }
}

pub(crate) fn set_progress(current: usize, total: usize) {
    if let Some(state) = SESSION.get() {
        if let Ok(mut state) = state.lock() {
            state.current = current;
            state.total = total;
        }
    }
}

fn run_tui(state: Arc<Mutex<StatusState>>, running: Arc<AtomicBool>) -> Result<()> {
    enable_raw_mode()?;
    let _cleanup = TerminalCleanup;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    loop {
        terminal.draw(|frame| render(frame, &state))?;

        let finished = state.lock().map(|state| state.finished).unwrap_or(true);
        if finished || !running.load(Ordering::SeqCst) {
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
                    if let Some(state) = SESSION.get() {
                        if let Ok(mut state) = state.lock() {
                            state.finished = true;
                        }
                    }
                    break;
                }
                _ => {}
            }
        }
    }

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

fn render(frame: &mut ratatui::Frame, state: &Arc<Mutex<StatusState>>) {
    let snapshot = state.lock().map(|state| StatusSnapshot::from(&*state));
    let snapshot = match snapshot {
        Ok(snapshot) => snapshot,
        Err(_) => return,
    };

    let remaining = snapshot.total.saturating_sub(snapshot.current);
    let elapsed = snapshot.started_at.elapsed();
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        snapshot.current as f64 / elapsed.as_secs_f64()
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
        "processed {} | remaining {}",
        snapshot.current, remaining
    ))
    .style(Style::default().fg(Color::Cyan))
    .block(Block::default().borders(Borders::ALL).title(snapshot.title));
    frame.render_widget(header, chunks[0]);

    let body = Paragraph::new(vec![
        Line::from(format!("stage {}", snapshot.stage)),
        Line::from(format!("{}", snapshot.detail)),
        Line::from(format!("updates every 10 items")),
        Line::from(format!("throughput {:.2} items/s", throughput)),
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

struct StatusSnapshot {
    title: String,
    stage: String,
    detail: String,
    current: usize,
    total: usize,
    started_at: Instant,
}

impl From<&StatusState> for StatusSnapshot {
    fn from(state: &StatusState) -> Self {
        Self {
            title: state.title.clone(),
            stage: state.stage.clone(),
            detail: state.detail.clone(),
            current: state.current,
            total: state.total,
            started_at: state.started_at,
        }
    }
}

fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}
