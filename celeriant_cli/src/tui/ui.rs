use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{
        Block, Borders, List, ListItem, Paragraph, Scrollbar,
        ScrollbarOrientation, ScrollbarState, Wrap,
    },
    Frame,
};

use super::app::{App, InputMode, PendingAction, Screen};
use crate::utils::format_u128_uuid;

const HEADER_COLOR: Color = Color::Cyan;
const SELECTED_COLOR: Color = Color::Yellow;
const ERROR_COLOR: Color = Color::Red;
const SUCCESS_COLOR: Color = Color::Green;
const DIM_COLOR: Color = Color::DarkGray;
const EDITING_COLOR: Color = Color::Magenta;

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(3),
        ])
        .split(f.area());

    draw_header(f, app, chunks[0]);

    match app.screen {
        Screen::Home => draw_home(f, app, chunks[1]),
        Screen::Connect => draw_connect(f, app, chunks[1]),
        Screen::Settings => draw_settings(f, app, chunks[1]),
        Screen::AggregateContext => draw_aggregate_context(f, app, chunks[1]),
        Screen::EnterAggregate => draw_enter_aggregate(f, app, chunks[1]),
        Screen::ReadEvents => draw_read_events(f, app, chunks[1]),
        Screen::WriteEvent => draw_write_event(f, app, chunks[1]),
        Screen::TrimStart => draw_trim_start(f, app, chunks[1]),
        Screen::Watch => draw_watch_screen(f, app, chunks[1], false),
        Screen::OrgWatch => draw_watch_screen(f, app, chunks[1], true),
        Screen::List => draw_list(f, app, chunks[1]),
        Screen::RegisterSchema => draw_register_schema(f, app, chunks[1]),
        Screen::Help => draw_help(f, app, chunks[1]),
    }

    draw_status_bar(f, app, chunks[2]);
}

/// Render a single input field with editing highlight, disabled dim, and placeholder.
fn render_input_field(
    f: &mut Frame,
    area: Rect,
    label: &str,
    value: &str,
    placeholder: &str,
    is_active: bool,
    is_disabled: bool,
) {
    let (display, style) = if is_active {
        (value, Style::default().fg(EDITING_COLOR))
    } else if is_disabled {
        (value, Style::default().fg(DIM_COLOR))
    } else if value.is_empty() && !placeholder.is_empty() {
        (placeholder, Style::default().fg(DIM_COLOR))
    } else {
        (value, Style::default())
    };

    let widget = Paragraph::new(display)
        .style(style)
        .block(Block::default().borders(Borders::ALL).title(format!(" {} ", label)));
    f.render_widget(widget, area);

    if is_active {
        f.set_cursor_position((area.x + value.len() as u16 + 1, area.y + 1));
    }
}

fn draw_enter_aggregate(f: &mut Frame, app: &App, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" Enter Aggregate Details ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(2)
        .constraints([
            Constraint::Length(3),  // Org ID
            Constraint::Length(3),  // Type ID
            Constraint::Length(3),  // Aggregate ID
            Constraint::Min(1),     // Help text
        ])
        .split(inner);

    for (i, field) in app.input_fields.iter().enumerate() {
        let is_active = app.input_mode == InputMode::Editing && app.input_field_index == i;
        render_input_field(f, chunks[i], &field.label, &field.value, &field.placeholder, is_active, false);
    }

    let help = Paragraph::new(vec![
        Line::from(""),
        Line::from("Press 'e' or 'i' to start editing"),
        Line::from("Use Tab/Shift+Tab to switch between fields"),
        Line::from("Press Enter to navigate to the aggregate"),
        Line::from("Press Esc or 'q' to go back"),
    ])
    .style(Style::default().fg(DIM_COLOR));

    f.render_widget(help, chunks[3]);
}

fn draw_header(f: &mut Frame, app: &App, area: Rect) {
    let connection_status = if app.is_connected() {
        Span::styled(" ● Connected ", Style::default().fg(SUCCESS_COLOR).bold())
    } else {
        Span::styled(" ○ Disconnected ", Style::default().fg(ERROR_COLOR).bold())
    };

    let title = Line::from(vec![
        Span::styled(" ⚡ Celeriant ", Style::default().fg(HEADER_COLOR).bold()),
        Span::raw("│"),
        connection_status,
        Span::raw("│"),
        Span::styled(format!(" {} ", app.server_address), Style::default().fg(DIM_COLOR)),
    ]);

    let breadcrumb = match &app.screen {
        Screen::Home => "Home".to_string(),
        Screen::Connect => "Connect".to_string(),
        Screen::Settings => "Settings".to_string(),
        Screen::AggregateContext => {
            if let Some(ctx) = &app.aggregate_context {
                format!(
                    "Org {} › Type {} › Agg {}",
                    format_u128_uuid(ctx.org_id),
                    format_u128_uuid(ctx.aggregate_type_id),
                    format_u128_uuid(ctx.aggregate_id)
                )
            } else {
                "Aggregate".to_string()
            }
        }
        Screen::EnterAggregate => "Enter Aggregate".to_string(),
        Screen::ReadEvents => "Read Events".to_string(),
        Screen::WriteEvent => "Write Event".to_string(),
        Screen::TrimStart => "Trim Start".to_string(),
        Screen::Watch => "Watch Events".to_string(),
        Screen::OrgWatch => "Organisation Watch".to_string(),
        Screen::List => "List".to_string(),
        Screen::RegisterSchema => "Register Schema".to_string(),
        Screen::Help => "Help".to_string(),
    };

    let header = Paragraph::new(vec![
        title,
        Line::from(Span::styled(format!(" 📍 {} ", breadcrumb), Style::default().fg(DIM_COLOR))),
    ])
    .block(Block::default().borders(Borders::BOTTOM).border_style(Style::default().fg(DIM_COLOR)));

    f.render_widget(header, area);
}

fn draw_status_bar(f: &mut Frame, app: &App, area: Rect) {
    let mode_indicator = match app.input_mode {
        InputMode::Normal => Span::styled(" NORMAL ", Style::default().bg(Color::Blue).fg(Color::White).bold()),
        InputMode::Editing => Span::styled(" EDITING ", Style::default().bg(EDITING_COLOR).fg(Color::White).bold()),
    };

    let status_style = if app.last_error.is_some() {
        Style::default().fg(ERROR_COLOR)
    } else {
        Style::default().fg(DIM_COLOR)
    };

    let hints = get_screen_hints(&app.screen, &app.input_mode);

    let status = Line::from(vec![
        mode_indicator,
        Span::raw(" "),
        Span::styled(&app.status_message, status_style),
        Span::raw(" │ "),
        Span::styled(hints, Style::default().fg(DIM_COLOR)),
    ]);

    let status_bar = Paragraph::new(status)
        .block(Block::default().borders(Borders::TOP).border_style(Style::default().fg(DIM_COLOR)));

    f.render_widget(status_bar, area);
}


fn get_screen_hints(screen: &Screen, input_mode: &InputMode) -> &'static str {
    match input_mode {
        InputMode::Editing => "Tab: next │ Enter: confirm │ Esc: cancel",
        InputMode::Normal => match screen {
            Screen::Home => "↑↓/jk: navigate │ Enter: select │ s: settings │ q: quit │ ?: help",
            Screen::Settings => "↑↓/jk: navigate │ Enter/Space: toggle or edit │ S: save │ q: back",
            Screen::AggregateContext => "↑↓/jk: navigate │ Enter: select │ r: refresh │ q: back",
            Screen::TrimStart => "e/i: edit │ x: trim │ q: back",
            Screen::ReadEvents | Screen::WriteEvent => "e/i: edit │ x: execute │ ↑↓: scroll │ q: back",
            Screen::Watch => "e/i: edit │ x: start │ s: stop │ c: clear │ ↑↓: scroll │ q: back",
            Screen::OrgWatch => "e/i: edit │ x: start │ s: stop │ c: clear │ ↑↓: scroll │ q: back",
            Screen::List => "e/i: edit │ x: execute │ ↑↓: scroll │ Enter: navigate │ q: back",
            Screen::RegisterSchema => "e/i: edit │ x: register │ q: back",
            Screen::Connect => "e/i: edit address │ x: connect │ s: settings │ q: back",
            Screen::Help => "↑↓/jk/PgUp/PgDn: scroll │ Esc/Enter: back",
            _ => "q: back │ ?: help",
        },
    }
}

fn draw_list(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),   // Input section
            Constraint::Min(10),     // Results display
        ])
        .split(area);

    // Input section
    let input_block = Block::default()
        .borders(Borders::ALL)
        .title(" List - Leave fields empty to list at that level ");
    let input_inner = input_block.inner(chunks[0]);
    f.render_widget(input_block, chunks[0]);

    let input_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Org ID
            Constraint::Length(3),  // Aggregate Type
        ])
        .split(input_inner);

    for (i, field) in app.input_fields.iter().enumerate() {
        let is_active = app.input_mode == InputMode::Editing && app.input_field_index == i;
        render_input_field(f, input_chunks[i], &field.label, &field.value, &field.placeholder, is_active, false);
    }

    // Results section
    let visible_height = chunks[1].height.saturating_sub(2) as usize;
    app.visible_height.set(visible_height);
    let total_lines = app.list_results.len();
    let scroll_offset = app.list_scroll.min(total_lines.saturating_sub(visible_height));

    let result_lines: Vec<Line> = app
        .list_results
        .iter()
        .enumerate()
        .skip(scroll_offset)
        .take(visible_height)
        .map(|(abs_idx, s)| {
            let is_cursor = abs_idx == app.list_scroll;
            let is_selectable = app.list_selectable.get(abs_idx).copied().flatten().is_some();
            let style = if is_cursor && is_selectable {
                Style::default().fg(SELECTED_COLOR).add_modifier(ratatui::style::Modifier::BOLD)
            } else if s.starts_with('━') {
                Style::default().fg(HEADER_COLOR).bold()
            } else if s.contains("[DELETED]") {
                Style::default().fg(ERROR_COLOR)
            } else if s.starts_with("  Org:") || s.starts_with("  Type:") || s.starts_with("  Aggregate:") {
                Style::default().fg(SUCCESS_COLOR)
            } else if s.starts_with("Total:") {
                Style::default().fg(DIM_COLOR).italic()
            } else if s.starts_with("  Error:") {
                Style::default().fg(ERROR_COLOR)
            } else {
                Style::default()
            };
            let prefix = if is_cursor && is_selectable { "▶" } else { " " };
            Line::from(vec![
                Span::styled(prefix, style),
                Span::styled(s.as_str(), style),
            ])
        })
        .collect();

    // Determine what will be listed based on current input
    let list_hint = if app.list_org_id.trim().is_empty() {
        "Will list: Organisations"
    } else if app.list_aggregate_type.trim().is_empty() {
        "Will list: Aggregate Types"
    } else {
        "Will list: Aggregates"
    };

    let results_title = Line::from(vec![
        Span::raw(" Results "),
        Span::styled(
            format!("({}/{}) ", scroll_offset + 1, total_lines.max(1)),
            Style::default().fg(DIM_COLOR),
        ),
        Span::styled(format!("│ {} ", list_hint), Style::default().fg(DIM_COLOR)),
        Span::styled("│ Press 'x' to execute ", Style::default().fg(DIM_COLOR)),
    ]);

    let results = Paragraph::new(result_lines)
        .block(Block::default()
            .borders(Borders::ALL)
            .title(results_title))
        .wrap(Wrap { trim: false });

    f.render_widget(results, chunks[1]);

    if total_lines > visible_height {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        let mut scrollbar_state = ScrollbarState::new(total_lines).position(scroll_offset);
        f.render_stateful_widget(scrollbar, chunks[1], &mut scrollbar_state);
    }
}

/// Draws the Watch or OrgWatch screen. `is_org` selects layout/title differences.
fn draw_watch_screen(f: &mut Frame, app: &App, area: Rect, is_org: bool) {
    let input_height: u16 = if is_org { 14 } else { 11 };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(input_height),
            Constraint::Min(10),
        ])
        .split(area);

    let config_title = match (is_org, app.watch_active) {
        (true, true)  => " Organisation Watch Configuration (Active) ",
        (true, false) => " Organisation Watch Configuration ",
        (false, true) => " Watch Configuration (Active) ",
        (false, false) => " Watch Configuration ",
    };

    let input_block = Block::default().borders(Borders::ALL).title(config_title);
    let input_inner = input_block.inner(chunks[0]);
    f.render_widget(input_block, chunks[0]);

    let field_count = app.input_fields.len();
    let field_constraints: Vec<Constraint> = (0..field_count)
        .map(|_| Constraint::Length(3))
        .collect();
    let input_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(field_constraints)
        .split(input_inner);

    for (i, field) in app.input_fields.iter().enumerate() {
        let is_active = app.input_mode == InputMode::Editing
            && app.input_field_index == i
            && !app.watch_active;
        let is_disabled = app.watch_active;
        render_input_field(f, input_chunks[i], &field.label, &field.value, &field.placeholder, is_active, is_disabled);
    }

    // Events display section
    let visible_height = chunks[1].height.saturating_sub(2) as usize;
    app.visible_height.set(visible_height);
    let total_lines = app.watch_events.len();
    let scroll_offset = app.watch_scroll.min(total_lines.saturating_sub(visible_height));

    let event_lines: Vec<Line> = app
        .watch_events
        .iter()
        .skip(scroll_offset)
        .take(visible_height)
        .map(|s| {
            let style = if s.starts_with('━') {
                Style::default().fg(HEADER_COLOR).bold()
            } else if s.starts_with("⚠") {
                Style::default().fg(ERROR_COLOR)
            } else if s.starts_with('♥') {
                Style::default().fg(DIM_COLOR)
            } else if s.starts_with("Event:") || s.contains("Event:") {
                Style::default().fg(SUCCESS_COLOR)
            } else {
                Style::default()
            };
            Line::from(Span::styled(s.as_str(), style))
        })
        .collect();

    let status_indicator = if app.watch_active {
        Span::styled(" ● LIVE ", Style::default().fg(SUCCESS_COLOR).bold())
    } else {
        Span::styled(" ○ STOPPED ", Style::default().fg(DIM_COLOR))
    };

    let events_title = Line::from(vec![
        Span::raw(" Events "),
        status_indicator,
        Span::styled(
            format!("({}/{}) ", scroll_offset + 1, total_lines.max(1)),
            Style::default().fg(DIM_COLOR),
        ),
        if app.watch_active {
            Span::styled("Press 's' to stop ", Style::default().fg(DIM_COLOR))
        } else {
            Span::styled("Press 'x' to start ", Style::default().fg(DIM_COLOR))
        },
    ]);

    let events = Paragraph::new(event_lines)
        .block(Block::default()
            .borders(Borders::ALL)
            .title(events_title))
        .wrap(Wrap { trim: false });

    f.render_widget(events, chunks[1]);

    if total_lines > visible_height {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        let mut scrollbar_state = ScrollbarState::new(total_lines).position(scroll_offset);
        f.render_stateful_widget(scrollbar, chunks[1], &mut scrollbar_state);
    }
}

fn draw_home(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let menu_items = app.get_home_menu_items();
    let items: Vec<ListItem> = menu_items
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            let style = if i == app.menu_index {
                Style::default().fg(SELECTED_COLOR).add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            let prefix = if i == app.menu_index { "▶ " } else { "  " };
            ListItem::new(Line::from(vec![
                Span::styled(prefix, style),
                Span::styled(*name, style),
            ]))
        })
        .collect();

    let menu = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(" Menu "));

    f.render_widget(menu, chunks[0]);

    let description = if app.menu_index < menu_items.len() {
        menu_items[app.menu_index].1
    } else {
        ""
    };

    let mut info_lines = vec![
        Line::from(""),
        Line::from(Span::styled(description, Style::default().fg(DIM_COLOR))),
        Line::from(""),
    ];

    if app.is_connected() {
        info_lines.push(Line::from(Span::styled(
            format!("Server: {}", app.server_address),
            Style::default().fg(SUCCESS_COLOR),
        )));
    }

    let info = Paragraph::new(info_lines)
        .block(Block::default().borders(Borders::ALL).title(" Info "))
        .wrap(Wrap { trim: true });

    f.render_widget(info, chunks[1]);
}

fn draw_connect(f: &mut Frame, app: &App, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" Connect to Server ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(2)
        .constraints([
            Constraint::Length(3),   // Server address field
            Constraint::Length(10),  // Settings summary panel
            Constraint::Min(1),      // Help text
        ])
        .split(inner);

    // Use input_fields if populated, otherwise fall back to server_address
    let (value, placeholder) = if let Some(field) = app.input_fields.first() {
        (field.value.as_str(), field.placeholder.as_str())
    } else {
        (app.server_address.as_str(), "")
    };
    let is_active = app.input_mode == InputMode::Editing;
    render_input_field(f, chunks[0], "Server Address", value, placeholder, is_active, false);

    // Settings summary panel
    let s = &app.settings;
    let tls_line = if s.tls.enabled {
        let certs = if !s.tls.ca_cert.is_empty() { " (CA configured)" } else { " (no CA)" };
        format!("on{}", certs)
    } else {
        "off".to_string()
    };
    let identity_line = s.identity.mode.label();
    let api_key_line = if s.auth.api_key.is_empty() { "not configured" } else { "configured" };
    let seeds_line = if s.connection.seed_addresses.is_empty() {
        "none".to_string()
    } else {
        s.connection.seed_addresses.join(", ")
    };
    let pool_line = format!(
        "max {} conns, connect {}ms, request {}ms",
        s.pool.max_connections_per_node,
        s.pool.connection_timeout_ms,
        s.pool.request_timeout_ms
    );
    let routing_line = if s.routing.route_reads_to_followers { "reads to followers" } else { "reads to leader" };
    let compression_line = if s.compression.enabled { "on" } else { "off" };

    let dim = Style::default().fg(DIM_COLOR);
    let label_style = Style::default().fg(DIM_COLOR).add_modifier(Modifier::DIM);
    let summary_lines = vec![
        Line::from(vec![Span::styled("  TLS:          ", label_style), Span::styled(tls_line, dim)]),
        Line::from(vec![Span::styled("  Identity:     ", label_style), Span::styled(identity_line, dim)]),
        Line::from(vec![Span::styled("  API Key:      ", label_style), Span::styled(api_key_line, dim)]),
        Line::from(vec![Span::styled("  Seeds:        ", label_style), Span::styled(seeds_line, dim)]),
        Line::from(vec![Span::styled("  Pool:         ", label_style), Span::styled(pool_line, dim)]),
        Line::from(vec![Span::styled("  Routing:      ", label_style), Span::styled(routing_line, dim)]),
        Line::from(vec![Span::styled("  Compression:  ", label_style), Span::styled(compression_line, dim)]),
    ];
    let summary = Paragraph::new(summary_lines)
        .block(Block::default()
            .borders(Borders::ALL)
            .border_style(dim)
            .title(Span::styled(" Active Settings — press 's' to edit ", dim)));
    f.render_widget(summary, chunks[1]);

    let help = Paragraph::new(vec![
        Line::from(""),
        Line::from("Press 'e' or 'i' to edit the server address"),
        Line::from("Press Enter or 'x' to connect"),
        Line::from("Press 's' to open Settings"),
        Line::from("Press Esc or 'q' to go back"),
    ])
    .style(dim);

    f.render_widget(help, chunks[2]);
}

fn draw_aggregate_context(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    // Left panel - menu
    let menu_items = app.get_aggregate_menu_items();
    let items: Vec<ListItem> = menu_items
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            let style = if i == app.menu_index {
                Style::default().fg(SELECTED_COLOR).add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            let prefix = if i == app.menu_index { "▶ " } else { "  " };
            ListItem::new(Line::from(vec![
                Span::styled(prefix, style),
                Span::styled(*name, style),
            ]))
        })
        .collect();

    let menu = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(" Operations "));

    f.render_widget(menu, chunks[0]);

    // Right panel - aggregate info
    let mut info_lines = vec![];

    if let Some(ctx) = &app.aggregate_context {
        info_lines.push(Line::from(vec![
            Span::styled("Organisation: ", Style::default().fg(DIM_COLOR)),
            Span::styled(format_u128_uuid(ctx.org_id), Style::default().bold()),
        ]));
        info_lines.push(Line::from(vec![
            Span::styled("Type: ", Style::default().fg(DIM_COLOR)),
            Span::styled(format_u128_uuid(ctx.aggregate_type_id), Style::default().bold()),
        ]));
        info_lines.push(Line::from(vec![
            Span::styled("Aggregate ID: ", Style::default().fg(DIM_COLOR)),
            Span::styled(format_u128_uuid(ctx.aggregate_id), Style::default().bold()),
        ]));
        info_lines.push(Line::from(""));

        if let Some(info) = &ctx.info {
            info_lines.push(Line::from(Span::styled(
                "━━━ Aggregate Info ━━━",
                Style::default().fg(HEADER_COLOR),
            )));
            info_lines.push(Line::from(vec![
                Span::styled("Batch range: ", Style::default().fg(DIM_COLOR)),
                Span::raw(format!("{} - {}", info.min_batch, info.max_batch)),
            ]));
            info_lines.push(Line::from(vec![
                Span::styled("Max event index: ", Style::default().fg(DIM_COLOR)),
                Span::raw(info.max_event_seq.to_string()),
            ]));
            if info.is_deleted {
                info_lines.push(Line::from(Span::styled(
                    "DELETED",
                    Style::default().fg(ERROR_COLOR).bold(),
                )));
            }
        } else {
            info_lines.push(Line::from(Span::styled(
                "Press 'r' to load aggregate info",
                Style::default().fg(DIM_COLOR).italic(),
            )));
        }
    }

    // Description or confirmation prompt
    if app.pending_action == Some(PendingAction::Delete) {
        info_lines.push(Line::from(""));
        info_lines.push(Line::from(Span::styled(
            "⚠ CONFIRM DELETE",
            Style::default().fg(ERROR_COLOR).bold(),
        )));
        info_lines.push(Line::from(""));
        info_lines.push(Line::from("This will permanently delete the aggregate."));
        info_lines.push(Line::from(""));
        info_lines.push(Line::from(vec![
            Span::styled("Type YES to confirm: ", Style::default().fg(ERROR_COLOR)),
            Span::styled(&app.confirm_input, Style::default().fg(EDITING_COLOR).bold()),
        ]));
        info_lines.push(Line::from(Span::styled(
            "Press Enter to confirm, Esc to cancel",
            Style::default().fg(DIM_COLOR).italic(),
        )));
    } else if app.menu_index < menu_items.len() {
        info_lines.push(Line::from(""));
        info_lines.push(Line::from(Span::styled(
            menu_items[app.menu_index].1,
            Style::default().fg(DIM_COLOR).italic(),
        )));
    }

    let info = Paragraph::new(info_lines)
        .block(Block::default().borders(Borders::ALL).title(" Aggregate "))
        .wrap(Wrap { trim: true });

    f.render_widget(info, chunks[1]);
}

fn draw_read_events(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Min(10)])
        .split(area);

    // Input section — two fields side by side
    let input_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[0]);

    for (i, field) in app.input_fields.iter().enumerate() {
        let is_active = app.input_mode == InputMode::Editing && app.input_field_index == i;
        render_input_field(f, input_chunks[i], &field.label, &field.value, &field.placeholder, is_active, false);
    }

    // Results section
    let visible_height = chunks[1].height.saturating_sub(2) as usize;
    app.visible_height.set(visible_height);
    let total_lines = app.result_output.len();
    let scroll_offset = app.result_scroll.min(total_lines.saturating_sub(visible_height));

    let result_lines: Vec<Line> = app
        .result_output
        .iter()
        .skip(scroll_offset)
        .take(visible_height)
        .map(|s| Line::from(s.as_str()))
        .collect();

    let results = Paragraph::new(result_lines)
        .block(Block::default().borders(Borders::ALL).title(format!(
            " Results ({}/{}) - Press 'x' to read ",
            scroll_offset + 1,
            total_lines.max(1)
        )))
        .wrap(Wrap { trim: false });

    f.render_widget(results, chunks[1]);

    if total_lines > visible_height {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        let mut scrollbar_state = ScrollbarState::new(total_lines).position(scroll_offset);
        f.render_stateful_widget(scrollbar, chunks[1], &mut scrollbar_state);
    }
}

fn draw_trim_start(f: &mut Frame, app: &App, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" Trim Start ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(2)
        .constraints([
            Constraint::Length(5),  // Info
            Constraint::Length(3),  // Input
            Constraint::Min(1),     // Help text
        ])
        .split(inner);

    // Info section
    let mut info_lines = vec![];
    let mut has_info = false;

    if let Some(ctx) = &app.aggregate_context {
        if let Some(info) = &ctx.info {
            has_info = true;
            info_lines.push(Line::from(vec![
                Span::styled("Current batch range: ", Style::default().fg(DIM_COLOR)),
                Span::styled(
                    format!("from batch {}", info.min_batch),
                    Style::default().bold()
                ),
            ]));
            info_lines.push(Line::from(""));
            info_lines.push(Line::from(Span::styled(
                "Events before the specified batch will be permanently deleted.",
                Style::default().fg(ERROR_COLOR),
            )));
        } else {
            info_lines.push(Line::from(Span::styled(
                "⚠ Aggregate info not loaded",
                Style::default().fg(Color::Yellow).bold()
            )));
            info_lines.push(Line::from(""));
            info_lines.push(Line::from(Span::styled(
                "Press 'q' to go back and refresh info first (press 'r')",
                Style::default().fg(DIM_COLOR).italic()
            )));
        }
    }

    let info = Paragraph::new(info_lines)
        .block(Block::default().borders(Borders::ALL).title(" Current State "));
    f.render_widget(info, chunks[0]);

    // Input field — use input_fields if available
    let (value, placeholder) = if let Some(field) = app.input_fields.first() {
        (field.value.as_str(), field.placeholder.as_str())
    } else {
        (app.trim_keep_from.as_str(), "")
    };
    let is_active = app.input_mode == InputMode::Editing;
    let is_disabled = !has_info;
    render_input_field(f, chunks[1], "Keep From version", value, placeholder, is_active, is_disabled);

    // Help text or confirmation prompt
    let help_lines = if app.pending_action == Some(PendingAction::Trim) {
        vec![
            Line::from(""),
            Line::from(Span::styled(
                "⚠ CONFIRM TRIM",
                Style::default().fg(ERROR_COLOR).bold(),
            )),
            Line::from(""),
            Line::from("Events before the specified batch will be permanently deleted."),
            Line::from(""),
            Line::from(vec![
                Span::styled("Type YES to confirm: ", Style::default().fg(ERROR_COLOR)),
                Span::styled(&app.confirm_input, Style::default().fg(EDITING_COLOR).bold()),
            ]),
            Line::from(Span::styled(
                "Press Enter to confirm, Esc to cancel",
                Style::default().fg(DIM_COLOR).italic(),
            )),
        ]
    } else {
        vec![
            Line::from(""),
            Line::from("Press 'e' or 'i' to edit the aggregate version"),
            Line::from("Press 'x' to execute the trim"),
            Line::from("Press Esc or 'q' to go back"),
            Line::from(""),
            Line::from(Span::styled(
                "⚠ Warning: This operation cannot be undone!",
                Style::default().fg(ERROR_COLOR).bold(),
            )),
        ]
    };

    let help = Paragraph::new(help_lines).style(Style::default().fg(DIM_COLOR));
    f.render_widget(help, chunks[2]);
}

fn draw_write_event(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Client ID (read-only)
            Constraint::Length(3),  // Event Type
            Constraint::Length(8),  // Event Data
            Constraint::Min(5),     // Results
        ])
        .split(area);

    // Client ID (read-only display)
    let client_id_display = Paragraph::new(format_u128_uuid(app.client_id))
        .style(Style::default().fg(DIM_COLOR))
        .block(Block::default()
            .borders(Borders::ALL)
            .title(" Client ID (derived from keypair) ")
            .border_style(Style::default().fg(DIM_COLOR)));
    f.render_widget(client_id_display, chunks[0]);

    // Event Type and Event Data from input_fields
    if let Some(type_field) = app.input_fields.first() {
        let is_active = app.input_mode == InputMode::Editing && app.input_field_index == 0;
        render_input_field(f, chunks[1], &type_field.label, &type_field.value, &type_field.placeholder, is_active, false);
    }

    if let Some(data_field) = app.input_fields.get(1) {
        let is_active = app.input_mode == InputMode::Editing && app.input_field_index == 1;
        let style = if is_active {
            Style::default().fg(EDITING_COLOR)
        } else {
            Style::default()
        };
        let display = if data_field.value.is_empty() && !data_field.placeholder.is_empty() && !is_active {
            data_field.placeholder.as_str()
        } else {
            data_field.value.as_str()
        };
        let data_input = Paragraph::new(display)
            .style(style)
            .block(Block::default().borders(Borders::ALL).title(format!(" {} ", data_field.label)))
            .wrap(Wrap { trim: false });
        f.render_widget(data_input, chunks[2]);

        if is_active {
            let lines: Vec<&str> = data_field.value.lines().collect();
            let last_line_len = lines.last().map(|s| s.len()).unwrap_or(data_field.value.len());
            f.set_cursor_position((
                chunks[2].x + last_line_len as u16 + 1,
                chunks[2].y + lines.len().max(1) as u16,
            ));
        }
    }

    // Results
    let visible_height = chunks[3].height.saturating_sub(2) as usize;
    app.visible_height.set(visible_height);
    let total_lines = app.result_output.len();
    let scroll_offset = app.result_scroll.min(total_lines.saturating_sub(visible_height));

    let result_lines: Vec<Line> = app
        .result_output
        .iter()
        .skip(scroll_offset)
        .take(visible_height)
        .map(|s| Line::from(s.as_str()))
        .collect();

    let results = Paragraph::new(result_lines)
        .block(Block::default().borders(Borders::ALL).title(format!(
            " Result ({}/{}) - Press 'x' to write ",
            scroll_offset + 1,
            total_lines.max(1)
        )))
        .wrap(Wrap { trim: false });

    f.render_widget(results, chunks[3]);
}

fn draw_register_schema(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),  // Context info
            Constraint::Length(14), // Input fields (4 x 3 + margins)
            Constraint::Min(5),     // Result
        ])
        .split(area);

    // Context info
    let mut ctx_lines = vec![];
    if let Some(ctx) = &app.aggregate_context {
        ctx_lines.push(Line::from(vec![
            Span::styled("Organisation: ", Style::default().fg(DIM_COLOR)),
            Span::styled(format_u128_uuid(ctx.org_id), Style::default().bold()),
        ]));
        ctx_lines.push(Line::from(vec![
            Span::styled("Aggregate Type: ", Style::default().fg(DIM_COLOR)),
            Span::styled(format_u128_uuid(ctx.aggregate_type_id), Style::default().bold()),
        ]));
    } else {
        ctx_lines.push(Line::from(Span::styled("No aggregate context", Style::default().fg(ERROR_COLOR))));
    }
    ctx_lines.push(Line::from(Span::styled(
        "org_id and aggregate_type_id come from the current context",
        Style::default().fg(DIM_COLOR).italic(),
    )));
    let ctx_block = Paragraph::new(ctx_lines)
        .block(Block::default().borders(Borders::ALL).title(" Context "));
    f.render_widget(ctx_block, chunks[0]);

    // Input fields
    let input_block = Block::default().borders(Borders::ALL).title(" Register Schema ");
    let input_inner = input_block.inner(chunks[1]);
    f.render_widget(input_block, chunks[1]);

    let field_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(3),
        ])
        .split(input_inner);

    for (i, field) in app.input_fields.iter().enumerate().take(4) {
        let is_active = app.input_mode == InputMode::Editing && app.input_field_index == i;
        render_input_field(f, field_chunks[i], &field.label, &field.value, &field.placeholder, is_active, false);
    }

    // Result
    let result_lines: Vec<Line> = app
        .result_output
        .iter()
        .map(|s| {
            let style = if s.contains("successfully") {
                Style::default().fg(SUCCESS_COLOR)
            } else if s.contains("Error") || s.contains("failed") {
                Style::default().fg(ERROR_COLOR)
            } else {
                Style::default()
            };
            Line::from(Span::styled(s.as_str(), style))
        })
        .collect();

    let results = Paragraph::new(result_lines)
        .block(Block::default().borders(Borders::ALL).title(" Result — Press 'x' to register "))
        .wrap(Wrap { trim: false });
    f.render_widget(results, chunks[2]);
}

fn draw_settings(f: &mut Frame, app: &App, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" Settings ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let total = app.input_fields.len();
    let field_height: u16 = 3;
    let visible_count = (inner.height / field_height) as usize;
    let scroll = app.settings_scroll.min(total.saturating_sub(visible_count));

    let field_constraints: Vec<Constraint> = (0..visible_count.min(total))
        .map(|_| Constraint::Length(field_height))
        .collect();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(field_constraints)
        .split(inner);

    for (slot, abs_idx) in (scroll..total).take(visible_count).enumerate() {
        if slot >= chunks.len() {
            break;
        }
        let field = &app.input_fields[abs_idx];
        let is_selected = app.input_field_index == abs_idx;
        let is_editing = is_selected && app.input_mode == InputMode::Editing;

        if is_selected && !is_editing {
            // Selected but not editing: yellow highlight (not handled by render_input_field)
            let widget = Paragraph::new(field.value.as_str())
                .style(Style::default().fg(SELECTED_COLOR))
                .block(Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(SELECTED_COLOR))
                    .title(format!(" {} ", field.label)));
            f.render_widget(widget, chunks[slot]);
        } else {
            render_input_field(f, chunks[slot], &field.label, &field.value, &field.placeholder, is_editing, false);
        }
    }

    if total > visible_count {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        let mut scrollbar_state = ScrollbarState::new(total).position(scroll);
        f.render_stateful_widget(scrollbar, inner, &mut scrollbar_state);
    }
}

fn draw_help(f: &mut Frame, app: &App, area: Rect) {
    let help_text = vec![
        Line::from(Span::styled("Celeriant CLI Help", Style::default().fg(HEADER_COLOR).bold())),
        Line::from(""),
        Line::from(Span::styled("Navigation", Style::default().bold())),
        Line::from("  ↑/↓ or j/k    Move up/down in lists"),
        Line::from("  Enter         Select item / Confirm"),
        Line::from("  Esc or q      Go back / Quit"),
        Line::from("  g/G           Jump to start/end of list"),
        Line::from("  ?/F1          Show this help"),
        Line::from(""),
        Line::from(Span::styled("Input Mode", Style::default().bold())),
        Line::from("  e/i           Enter edit mode"),
        Line::from("  Tab           Next input field"),
        Line::from("  Shift+Tab     Previous input field"),
        Line::from("  Enter         Confirm input"),
        Line::from("  Esc           Cancel editing"),
        Line::from(""),
        Line::from(Span::styled("Watch Mode", Style::default().bold())),
        Line::from("  e/i           Edit watch parameters"),
        Line::from("  x             Start watching"),
        Line::from("  s             Stop watching"),
        Line::from("  c             Clear watch log"),
        Line::from("  ↑↓/jk         Scroll through events"),
        Line::from("  g/G           Jump to start/end"),
        Line::from(""),
        Line::from(Span::styled("Event Types:", Style::default().fg(DIM_COLOR))),
        Line::from("  0=DELETE  1=WRITE  2=READ"),
        Line::from("  3=TRIM_START  4=DETAILS  5=CREATE"),
        Line::from(""),
        Line::from(Span::styled("Actions", Style::default().bold())),
        Line::from("  r             Refresh current list"),
        Line::from("  x             Execute operation (read/write)"),
        Line::from("  Space         Toggle options"),
        Line::from("  Ctrl+C        Force quit"),
        Line::from(""),
        Line::from(Span::styled("CLI Mode", Style::default().bold())),
        Line::from("  Run with --help for CLI usage"),
        Line::from(""),
        Line::from(Span::styled("Examples:", Style::default().fg(DIM_COLOR))),
        Line::from("  celeriant list-orgs"),
        Line::from("  celeriant exists --org 1 --type 1 --id 1"),
        Line::from("  celeriant read --org 1 --type 1 --id 1 --from 1"),
        Line::from("  celeriant write --org 1 --type 1 --id 1 \\"),
        Line::from("      --client-id 1 --event-type 1 --data '{\"foo\":1}'"),
        Line::from(""),
        Line::from(Span::styled("Press q or Esc to close", Style::default().fg(DIM_COLOR).italic())),
    ];

    let help = Paragraph::new(help_text)
        .block(Block::default().borders(Borders::ALL).title(" Help "))
        .wrap(Wrap { trim: true })
        .scroll((app.help_scroll as u16, 0));

    f.render_widget(help, area);
}
