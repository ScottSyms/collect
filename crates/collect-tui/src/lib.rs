use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Tabs, Wrap},
    Frame, Terminal,
};
use serde::{de::DeserializeOwned, Serialize};
use std::fs;
use std::io;

const DEFAULT_TABS: [&str; 5] = ["Input", "Output", "S3", "Config", "Run"];
const CONFIG_TAB_INDEX: usize = 3;
const RUN_TAB_INDEX: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldKind {
    Text,
    Bool,
    Action,
}

#[derive(Clone, Debug)]
pub struct FieldState {
    pub label: &'static str,
    pub value: String,
    pub kind: FieldKind,
}

pub trait TuiModel: Serialize + DeserializeOwned + Clone {
    fn app_title() -> &'static str;

    fn run_command_prefix() -> &'static str;

    fn default_config_file_path() -> &'static str;

    fn from_env() -> Self;

    fn fields_for_tab(&self, tab: usize) -> Vec<FieldState>;

    fn field_hint(tab: usize, field: usize) -> Option<&'static str>;

    fn validate(&self) -> Vec<String>;

    fn set_field_value(&mut self, tab: usize, field: usize, value: String);

    fn toggle_field(&mut self, tab: usize, field: usize);

    fn to_cli_args(&self) -> Vec<String>;

    fn tabs() -> &'static [&'static str] {
        &DEFAULT_TABS
    }
}

pub fn run_tui<C: TuiModel>(initial_config: C) -> Result<Option<C>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(initial_config);
    let res = run_app(&mut terminal, &mut app);

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("Error: {:?}", err);
        return Err(err);
    }

    if app.run {
        Ok(Some(app.config))
    } else {
        Ok(None)
    }
}

fn save_config_to_file<C: Serialize>(config: &C, path: &str) -> Result<()> {
    let json = serde_json::to_string_pretty(config)?;
    fs::write(path, json)?;
    Ok(())
}

fn load_config_from_file<C: DeserializeOwned>(path: &str) -> Result<C> {
    let json = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&json)?)
}

struct App<C: TuiModel> {
    config: C,
    selected_tab: usize,
    current_field: usize,
    editing: bool,
    input_buffer: String,
    quit: bool,
    run: bool,
    validation_errors: Vec<String>,
    show_help: bool,
    status_message: Option<String>,
    config_file_path: String,
}

impl<C: TuiModel> App<C> {
    fn new(config: C) -> Self {
        Self {
            config,
            selected_tab: 0,
            current_field: 0,
            editing: false,
            input_buffer: String::new(),
            quit: false,
            run: false,
            validation_errors: Vec::new(),
            show_help: false,
            status_message: None,
            config_file_path: C::default_config_file_path().to_string(),
        }
    }

    fn fields_for_tab(&self, tab: usize) -> Vec<FieldState> {
        match tab {
            0..=2 => self.config.fields_for_tab(tab),
            CONFIG_TAB_INDEX => vec![
                FieldState {
                    label: "Config File Path",
                    value: self.config_file_path.clone(),
                    kind: FieldKind::Text,
                },
                FieldState {
                    label: "Save Config",
                    value: "Press Enter".to_string(),
                    kind: FieldKind::Action,
                },
                FieldState {
                    label: "Load Config",
                    value: "Press Enter".to_string(),
                    kind: FieldKind::Action,
                },
            ],
            RUN_TAB_INDEX => vec![],
            _ => vec![],
        }
    }

    fn get_field_hint(&self, tab: usize, field: usize) -> Option<&'static str> {
        match tab {
            0..=2 => C::field_hint(tab, field),
            CONFIG_TAB_INDEX => match field {
                0 => Some("Path to save/load JSON config file"),
                1 => Some("Save current configuration to file"),
                2 => Some("Load configuration from file"),
                _ => None,
            },
            _ => None,
        }
    }

    fn validate_config(&mut self) -> bool {
        self.validation_errors = self.config.validate();
        self.validation_errors.is_empty()
    }

    fn save_config(&mut self) {
        match save_config_to_file(&self.config, &self.config_file_path) {
            Ok(_) => {
                self.status_message = Some(format!("✓ Config saved to {}", self.config_file_path));
            }
            Err(e) => {
                self.status_message = Some(format!("✗ Failed to save config: {}", e));
            }
        }
    }

    fn load_config(&mut self) {
        match load_config_from_file::<C>(&self.config_file_path) {
            Ok(config) => {
                self.config = config;
                self.status_message =
                    Some(format!("✓ Config loaded from {}", self.config_file_path));
            }
            Err(e) => {
                self.status_message = Some(format!("✗ Failed to load config: {}", e));
            }
        }
    }

    fn start_editing(&mut self) {
        let fields = self.fields_for_tab(self.selected_tab);
        if self.current_field < fields.len() {
            let field = &fields[self.current_field];
            self.editing = true;
            if matches!(field.kind, FieldKind::Text) {
                self.input_buffer = field.value.clone();
                if field.value == "***" {
                    self.input_buffer.clear();
                }
            }
        }
    }

    fn finish_editing(&mut self) {
        let field_idx = self.current_field;
        let value = self.input_buffer.clone();

        match self.selected_tab {
            0..=2 => self
                .config
                .set_field_value(self.selected_tab, field_idx, value),
            CONFIG_TAB_INDEX => match field_idx {
                0 => self.config_file_path = value,
                1 => self.save_config(),
                2 => self.load_config(),
                _ => {}
            },
            _ => {}
        }

        self.editing = false;
        self.input_buffer.clear();
    }

    fn toggle_bool(&mut self) {
        let fields = self.fields_for_tab(self.selected_tab);
        if self.current_field < fields.len() {
            let field = &fields[self.current_field];
            if matches!(field.kind, FieldKind::Bool) {
                if self.selected_tab <= 2 {
                    self.config
                        .toggle_field(self.selected_tab, self.current_field);
                }
            }
        }
    }
}

fn run_app<C: TuiModel, B: Backend>(terminal: &mut Terminal<B>, app: &mut App<C>) -> Result<()> {
    loop {
        terminal.draw(|f| ui(f, app))?;

        if let Event::Key(key) = event::read()? {
            if key.kind != KeyEventKind::Press {
                continue;
            }

            if app.editing {
                match key.code {
                    KeyCode::Enter => app.finish_editing(),
                    KeyCode::Esc => {
                        app.editing = false;
                        app.input_buffer.clear();
                    }
                    KeyCode::Char(c) => {
                        app.input_buffer.push(c);
                    }
                    KeyCode::Backspace => {
                        app.input_buffer.pop();
                    }
                    _ => {}
                }
            } else {
                match key.code {
                    KeyCode::Char('q') => {
                        app.quit = true;
                        return Ok(());
                    }
                    KeyCode::Tab => {
                        app.selected_tab = (app.selected_tab + 1) % C::tabs().len();
                        app.current_field = 0;
                    }
                    KeyCode::BackTab => {
                        app.selected_tab = if app.selected_tab == 0 {
                            C::tabs().len() - 1
                        } else {
                            app.selected_tab - 1
                        };
                        app.current_field = 0;
                    }
                    KeyCode::Up => {
                        let num_fields = app.fields_for_tab(app.selected_tab).len();
                        if num_fields > 0 {
                            app.current_field = if app.current_field == 0 {
                                num_fields - 1
                            } else {
                                app.current_field - 1
                            };
                        }
                    }
                    KeyCode::Down => {
                        let num_fields = app.fields_for_tab(app.selected_tab).len();
                        if num_fields > 0 {
                            app.current_field = (app.current_field + 1) % num_fields;
                        }
                    }
                    KeyCode::Enter => {
                        if app.selected_tab == RUN_TAB_INDEX {
                            if app.validate_config() {
                                app.run = true;
                                return Ok(());
                            }
                        } else if app.selected_tab == CONFIG_TAB_INDEX {
                            match app.current_field {
                                0 => app.start_editing(),
                                1 => app.save_config(),
                                2 => app.load_config(),
                                _ => {}
                            }
                        } else {
                            let fields = app.fields_for_tab(app.selected_tab);
                            if app.current_field < fields.len() {
                                match fields[app.current_field].kind {
                                    FieldKind::Bool => app.toggle_bool(),
                                    FieldKind::Text => app.start_editing(),
                                    FieldKind::Action => {}
                                }
                            }
                        }
                    }
                    KeyCode::Char(' ') => {
                        app.toggle_bool();
                    }
                    KeyCode::Char('?') | KeyCode::F(1) => {
                        app.show_help = !app.show_help;
                    }
                    KeyCode::Char('v') => {
                        app.validate_config();
                    }
                    _ => {}
                }
            }
        }

        if app.quit {
            break;
        }
    }

    Ok(())
}

fn ui<C: TuiModel>(f: &mut Frame, app: &App<C>) {
    let mut constraints = vec![
        Constraint::Length(3),
        Constraint::Min(10),
        Constraint::Length(3),
    ];

    if app.status_message.is_some() {
        constraints.insert(2, Constraint::Length(3));
    }

    if !app.validation_errors.is_empty() {
        let error_lines = app.validation_errors.len().min(5) as u16 + 2;
        constraints.insert(2, Constraint::Length(error_lines));
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(constraints)
        .split(f.area());

    let mut chunk_idx = 0;

    let title = Paragraph::new(format!(
        "{} Configuration - Press '?' for help, 'v' to validate, 'q' to quit",
        C::app_title()
    ))
    .style(Style::default().fg(Color::Cyan))
    .block(Block::default().borders(Borders::ALL));
    f.render_widget(title, chunks[chunk_idx]);
    chunk_idx += 1;

    let tab_titles: Vec<Line> = C::tabs()
        .iter()
        .map(|t| Line::from(Span::styled(*t, Style::default())))
        .collect();
    let tabs = Tabs::new(tab_titles)
        .block(Block::default().borders(Borders::ALL).title("Tabs"))
        .select(app.selected_tab)
        .style(Style::default().fg(Color::White))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );

    let inner_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(7)])
        .split(chunks[chunk_idx]);
    chunk_idx += 1;

    f.render_widget(tabs, inner_chunks[0]);

    if app.selected_tab == RUN_TAB_INDEX {
        render_run_tab(f, inner_chunks[1], app);
    } else {
        render_fields_tab(f, inner_chunks[1], app);
    }

    if !app.validation_errors.is_empty() {
        let error_text: Vec<Line> = app
            .validation_errors
            .iter()
            .take(5)
            .map(|e| Line::from(Span::styled(e.as_str(), Style::default().fg(Color::Red))))
            .collect();

        let errors = Paragraph::new(error_text)
            .style(Style::default().fg(Color::Red))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Validation Errors"),
            );
        f.render_widget(errors, chunks[chunk_idx]);
        chunk_idx += 1;
    }

    if let Some(ref msg) = app.status_message {
        let color = if msg.starts_with('✓') {
            Color::Green
        } else if msg.starts_with('✗') {
            Color::Red
        } else {
            Color::Yellow
        };

        let status = Paragraph::new(msg.as_str())
            .style(Style::default().fg(color))
            .block(Block::default().borders(Borders::ALL).title("Status"));
        f.render_widget(status, chunks[chunk_idx]);
        chunk_idx += 1;
    }

    let help_text = if app.editing {
        "Enter: Save | Esc: Cancel | Type to edit"
    } else if app.show_help {
        "?: Hide help | ↑↓: Navigate | Enter: Edit/Toggle | Space: Toggle | Tab: Next | v: Validate"
    } else {
        "?: Help | ↑↓: Navigate | Enter: Edit/Toggle | Space: Toggle | Tab: Next Tab | v: Validate"
    };
    let footer = Paragraph::new(help_text)
        .style(Style::default().fg(Color::Gray))
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, chunks[chunk_idx]);
}

fn render_fields_tab<C: TuiModel>(f: &mut Frame, area: Rect, app: &App<C>) {
    let fields = app.fields_for_tab(app.selected_tab);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(if app.show_help {
            vec![Constraint::Min(5), Constraint::Length(3)]
        } else {
            vec![Constraint::Min(5)]
        })
        .split(area);

    let list_area = chunks[0];

    let items: Vec<ListItem> = fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let display_value = if app.editing && i == app.current_field {
                format!("{}: {}_", field.label, app.input_buffer)
            } else if matches!(field.kind, FieldKind::Bool) {
                format!(
                    "{}: [{}]",
                    field.label,
                    if field.value == "true" { "X" } else { " " }
                )
            } else {
                format!("{}: {}", field.label, field.value)
            };

            let style = if i == app.current_field {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };

            ListItem::new(Line::from(display_value)).style(style)
        })
        .collect();

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(C::tabs()[app.selected_tab]),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );

    f.render_widget(list, list_area);

    if app.show_help && chunks.len() > 1 {
        if let Some(hint) = app.get_field_hint(app.selected_tab, app.current_field) {
            let hint_widget = Paragraph::new(hint)
                .style(Style::default().fg(Color::Cyan))
                .block(Block::default().borders(Borders::ALL).title("Hint"))
                .wrap(Wrap { trim: false });
            f.render_widget(hint_widget, chunks[1]);
        }
    }
}

fn render_run_tab<C: TuiModel>(f: &mut Frame, area: Rect, app: &App<C>) {
    let args = app.config.to_cli_args();
    let cmd = if args.is_empty() {
        C::run_command_prefix().to_string()
    } else {
        format!("{} {}", C::run_command_prefix(), args.join(" "))
    };

    let text = vec![
        Line::from(""),
        Line::from(Span::styled(
            "Ready to run with the following configuration:",
            Style::default().fg(Color::Green),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "Command:",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
    ];

    let mut all_lines = text;

    for line in wrap_text(&cmd, area.width.saturating_sub(4) as usize) {
        all_lines.push(Line::from(line));
    }

    all_lines.push(Line::from(""));
    all_lines.push(Line::from(""));
    all_lines.push(Line::from(Span::styled(
        "Press Enter to run, or 'q' to quit",
        Style::default().fg(Color::Yellow),
    )));

    let paragraph = Paragraph::new(all_lines)
        .block(Block::default().borders(Borders::ALL).title("Run"))
        .wrap(Wrap { trim: false });

    f.render_widget(paragraph, area);
}

fn wrap_text(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current_line = String::new();

    for word in text.split_whitespace() {
        if current_line.len() + word.len() + 1 > width {
            if !current_line.is_empty() {
                lines.push(current_line.clone());
                current_line.clear();
            }
            if word.len() > width {
                lines.push(word[..width].to_string());
                current_line = word[width..].to_string();
            } else {
                current_line = word.to_string();
            }
        } else {
            if !current_line.is_empty() {
                current_line.push(' ');
            }
            current_line.push_str(word);
        }
    }

    if !current_line.is_empty() {
        lines.push(current_line);
    }

    lines
}
