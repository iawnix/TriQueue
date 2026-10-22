use crate::client;
use crate::model::{NodeInfo, Task, TaskId, TaskState};
use crate::protocol::{Request, Response};
use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table, TableState};
use std::io::stdout;
use std::time::Duration;

const CYAN: Color = Color::Rgb(93, 205, 220);
const GREEN: Color = Color::Rgb(151, 208, 109);
const YELLOW: Color = Color::Rgb(235, 196, 106);
const RED: Color = Color::Rgb(235, 112, 112);
const MUTED: Color = Color::Rgb(136, 145, 161);
const PANEL: Color = Color::Rgb(43, 49, 62);

struct App {
    tasks: Vec<Task>,
    nodes: Vec<NodeInfo>,
    selected: usize,
    message: String,
}

impl App {
    async fn refresh(&mut self) {
        if let Ok(Response::Tasks(mut tasks)) = client::local(&Request::List { state: None }).await
        {
            tasks.sort_by(|left, right| {
                state_rank(&left.state)
                    .cmp(&state_rank(&right.state))
                    .then_with(|| right.id.cmp(&left.id))
            });
            self.tasks = tasks;
        }
        if let Ok(Response::Nodes(nodes)) = client::local(&Request::Nodes).await {
            self.nodes = nodes;
            self.nodes.sort_by(|left, right| left.name.cmp(&right.name));
        }
        if self.selected >= self.tasks.len() {
            self.selected = self.tasks.len().saturating_sub(1);
        }
    }

    fn selected_id(&self) -> Option<TaskId> {
        self.tasks.get(self.selected).map(|task| task.id)
    }
}

pub async fn run() -> Result<()> {
    enable_raw_mode()?;
    let mut out = stdout();
    execute!(out, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(out);
    let mut terminal = Terminal::new(backend)?;
    let mut app = App {
        tasks: Vec::new(),
        nodes: Vec::new(),
        selected: 0,
        message: "connecting".into(),
    };
    app.refresh().await;
    let result = loop {
        terminal.draw(|frame| draw(frame, &app))?;
        if event::poll(Duration::from_millis(800))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break Ok(()),
                    KeyCode::Char('r') => app.refresh().await,
                    KeyCode::Down | KeyCode::Char('j') => {
                        if !app.tasks.is_empty() {
                            app.selected = (app.selected + 1).min(app.tasks.len() - 1);
                        }
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        app.selected = app.selected.saturating_sub(1)
                    }
                    KeyCode::Char('c') => {
                        if let Some(id) = app.selected_id() {
                            app.message = match client::local(&Request::Cancel { id }).await {
                                Ok(Response::Task(task)) => {
                                    format!("cancel requested for {} ({})", task.id, task.state)
                                }
                                Ok(Response::Error(message)) => message,
                                Ok(_) => "unexpected response".into(),
                                Err(error) => error.to_string(),
                            };
                            app.refresh().await;
                        }
                    }
                    _ => {}
                }
            }
        } else {
            app.refresh().await;
        }
    };
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(9),
            Constraint::Min(7),
            Constraint::Length(4),
        ])
        .split(frame.area());
    draw_header(frame, app, chunks[0]);
    draw_nodes(frame, app, chunks[1]);
    draw_tasks(frame, app, chunks[2]);
    draw_footer(frame, app, chunks[3]);
}

fn draw_header(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let cpu_used: u32 = app.nodes.iter().map(|node| node.cpu_used).sum();
    let cpu_total: u32 = app.nodes.iter().map(|node| node.cpu_total).sum();
    let mem_used: u64 = app.nodes.iter().map(|node| node.memory_used).sum();
    let mem_total: u64 = app.nodes.iter().map(|node| node.memory_bytes).sum();
    let gpu_used: u32 = app.nodes.iter().map(|node| node.gpu_used).sum();
    let gpu_total: u32 = app.nodes.iter().map(|node| node.gpu_total).sum();
    let online = app.nodes.iter().filter(|node| node.online).count();
    let running = app
        .tasks
        .iter()
        .filter(|task| task.state == TaskState::Run)
        .count();
    let waiting = app
        .tasks
        .iter()
        .filter(|task| task.state == TaskState::Wait)
        .count();
    let failed = app
        .tasks
        .iter()
        .filter(|task| task.state == TaskState::Fail)
        .count();
    let line = Line::from(vec![
        Span::styled(
            " TRI ",
            Style::default()
                .fg(Color::Black)
                .bg(CYAN)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "  QUEUE",
            Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(
                "   {} nodes · {}/{} CPU · {}/{} GPU · {}/{} MEM",
                online,
                cpu_used,
                cpu_total,
                gpu_used,
                gpu_total,
                format_bytes(mem_used),
                format_bytes(mem_total)
            ),
            Style::default().fg(MUTED),
        ),
        Span::styled(
            format!(
                "   {} running  {} waiting  {} failed  {} total",
                running,
                waiting,
                failed,
                app.tasks.len()
            ),
            Style::default().fg(Color::White),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(line).block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(MUTED)),
        ),
        area,
    );
}

fn draw_nodes(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let block = Block::default()
        .borders(Borders::TOP)
        .border_style(Style::default().fg(MUTED))
        .title(Span::styled(
            " NODES ",
            Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
        ));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if app.nodes.is_empty() {
        frame.render_widget(
            Paragraph::new("waiting for node registration…").style(Style::default().fg(MUTED)),
            inner,
        );
        return;
    }
    let constraints = vec![Constraint::Ratio(1, app.nodes.len() as u32); app.nodes.len()];
    let cards = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(constraints)
        .split(inner);
    for (node, card) in app.nodes.iter().zip(cards.iter()) {
        draw_node_card(frame, node, *card);
    }
}

fn draw_node_card(frame: &mut ratatui::Frame<'_>, node: &NodeInfo, area: Rect) {
    let border = if node.quarantined {
        YELLOW
    } else if node.online {
        GREEN
    } else {
        RED
    };
    let state = if node.quarantined {
        "● quarantined"
    } else if node.online {
        "● online"
    } else {
        "● offline"
    };
    let title = Line::from(vec![
        Span::styled(
            format!(" {} ", node.name),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(state, Style::default().fg(border)),
    ]);
    let block = Block::default()
        .borders(Borders::LEFT | Borders::RIGHT)
        .border_style(Style::default().fg(PANEL))
        .title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(1),
        ])
        .split(inner);
    let cpu_ratio = ratio(node.cpu_used as u64, node.cpu_total as u64);
    let mem_ratio = ratio(node.memory_used, node.memory_bytes);
    frame.render_widget(
        Gauge::default()
            .ratio(cpu_ratio)
            .label(format!("CPU  {}/{}", node.cpu_used, node.cpu_total))
            .gauge_style(Style::default().fg(CYAN).bg(PANEL)),
        rows[0],
    );
    frame.render_widget(
        Gauge::default()
            .ratio(mem_ratio)
            .label(format!(
                "MEM  {}/{}",
                format_bytes(node.memory_used),
                format_bytes(node.memory_bytes)
            ))
            .gauge_style(Style::default().fg(YELLOW).bg(PANEL)),
        rows[1],
    );
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                format!(
                    "  {} task{}",
                    node.running_tasks.len(),
                    if node.running_tasks.len() == 1 {
                        ""
                    } else {
                        "s"
                    }
                ),
                Style::default().fg(Color::White),
            ),
            Span::styled(format!("   {}", node.address), Style::default().fg(MUTED)),
        ])),
        rows[2],
    );
    if node.gpu_total > 0 {
        frame.render_widget(
            Paragraph::new(format!("GPU  {}/{}", node.gpu_used, node.gpu_total))
                .style(Style::default().fg(GREEN)),
            rows[3],
        );
    }
}

fn draw_tasks(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app.tasks.iter().map(|task| {
        Row::new(vec![
            Cell::from(task.id.to_string()).style(Style::default().fg(MUTED)),
            Cell::from(state_label(&task.state)).style(task_style(&task.state)),
            Cell::from(task.node.clone().unwrap_or_else(|| "--".into())),
            Cell::from(task.spec.requested_cpu.to_string()),
            Cell::from(task.spec.requested_gpus.to_string()),
            Cell::from(format_bytes(task.spec.requested_mem_bytes)),
            Cell::from(crate::format_elapsed(
                task.started_at.as_deref(),
                task.finished_at.as_deref(),
            )),
            Cell::from(task.spec.name.clone()),
        ])
    });
    let mut state = TableState::default();
    state.select((!app.tasks.is_empty()).then_some(app.selected));
    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(9),
            Constraint::Length(8),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(9),
            Constraint::Length(11),
            Constraint::Min(14),
        ],
    )
    .header(
        Row::new(vec![
            "ID", "STATE", "NODE", "CPU", "GPU", "MEM", "TIME", "JOB",
        ])
        .style(Style::default().fg(MUTED).add_modifier(Modifier::BOLD)),
    )
    .row_highlight_style(
        Style::default()
            .bg(Color::Rgb(55, 63, 78))
            .add_modifier(Modifier::BOLD),
    )
    .highlight_symbol("› ")
    .column_spacing(1)
    .block(
        Block::default()
            .borders(Borders::TOP)
            .border_style(Style::default().fg(MUTED))
            .title(Span::styled(
                " QUEUE ",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            )),
    );
    frame.render_stateful_widget(table, area, &mut state);
}

fn draw_footer(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let selected = app.tasks.get(app.selected);
    let detail = selected.map_or_else(
        || "No jobs in queue".into(),
        |task| {
            format!(
                "Selected  #{}  {}  ·  {}  ·  {}",
                task.id,
                task.spec.name,
                state_label(&task.state),
                task.stdout_path
            )
        },
    );
    let lines = vec![
        Line::from(Span::styled(detail, Style::default().fg(Color::White))),
        Line::from(vec![
            Span::styled(
                " ↑↓/jk ",
                Style::default().fg(YELLOW).add_modifier(Modifier::BOLD),
            ),
            Span::styled("select", Style::default().fg(MUTED)),
            Span::styled(
                "   c ",
                Style::default().fg(YELLOW).add_modifier(Modifier::BOLD),
            ),
            Span::styled("cancel", Style::default().fg(MUTED)),
            Span::styled(
                "   r ",
                Style::default().fg(YELLOW).add_modifier(Modifier::BOLD),
            ),
            Span::styled("refresh", Style::default().fg(MUTED)),
            Span::styled(
                "   q ",
                Style::default().fg(YELLOW).add_modifier(Modifier::BOLD),
            ),
            Span::styled("quit", Style::default().fg(MUTED)),
            Span::styled(format!("     {}", app.message), Style::default().fg(MUTED)),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(Style::default().fg(MUTED)),
        ),
        area,
    );
}

fn state_rank(state: &TaskState) -> u8 {
    match state {
        TaskState::Unknown => 0,
        TaskState::Run => 0,
        TaskState::Wait => 1,
        TaskState::Fail => 2,
        TaskState::Lost => 3,
        TaskState::Cancel => 4,
        TaskState::Done => 5,
        TaskState::Blocked => 6,
        TaskState::TimedOut => 7,
    }
}
fn state_label(state: &TaskState) -> &'static str {
    match state {
        TaskState::Unknown => "? unknown",
        TaskState::Wait => "· wait",
        TaskState::Run => "▶ run",
        TaskState::Done => "✓ done",
        TaskState::Fail => "! fail",
        TaskState::Cancel => "× cancel",
        TaskState::Lost => "? lost",
        TaskState::Blocked => "! blocked",
        TaskState::TimedOut => "! timed out",
    }
}
fn task_style(state: &TaskState) -> Style {
    Style::default()
        .fg(match state {
            TaskState::Unknown => Color::Magenta,
            TaskState::Wait => YELLOW,
            TaskState::Run => CYAN,
            TaskState::Done => GREEN,
            TaskState::Fail => RED,
            TaskState::Cancel => MUTED,
            TaskState::Lost => Color::Magenta,
            TaskState::Blocked => YELLOW,
            TaskState::TimedOut => RED,
        })
        .add_modifier(Modifier::BOLD)
}
fn ratio(used: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (used as f64 / total as f64).clamp(0.0, 1.0)
    }
}
fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1}G", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1}M", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1}K", bytes as f64 / 1024.0)
    } else {
        format!("{bytes}B")
    }
}
