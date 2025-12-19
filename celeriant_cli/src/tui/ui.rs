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

use super::app::{App, InputMode, Screen};

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
        Screen::AggregateContext => draw_aggregate_context(f, app, chunks[1]),
        Screen::EnterAggregate => draw_enter_aggregate(f, app, chunks[1]),
        Screen::ReadEvents => draw_read_events(f, app, chunks[1]),
        Screen::WriteEvent => draw_write_event(f, app, chunks[1]),
        Screen::TrimStart => draw_trim_start(f, app, chunks[1]),
        Screen::Watch => draw_watch(f, app, chunks[1]),  // Add this
        Screen::Help => draw_help(f, app, chunks[1]),
    }
    
    draw_status_bar(f, app, chunks[2]);
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

    // Render input fields
    for (i, field) in app.input_fields.iter().enumerate() {
        let is_editing = app.input_mode == InputMode::Editing && app.input_field_index == i;
        let style = if is_editing {
            Style::default().fg(EDITING_COLOR)
        } else {
            Style::default()
        };

        let input = Paragraph::new(field.value.as_str())
            .style(style)
            .block(Block::default()
                .borders(Borders::ALL)
                .title(format!(" {} ", field.label)));

        f.render_widget(input, chunks[i]);

        // Set cursor position when editing this field
        if is_editing {
            f.set_cursor_position((
                chunks[i].x + field.value.len() as u16 + 1,
                chunks[i].y + 1,
            ));
        }
    }

    // Help text
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
        Screen::AggregateContext => {
            if let Some(ctx) = &app.aggregate_context {
                format!("Org {} › Type {} › Agg {}", ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id)
            } else {
                "Aggregate".to_string()
            }
        }
        Screen::EnterAggregate => "Enter Aggregate".to_string(),
        Screen::ReadEvents => "Read Events".to_string(),
        Screen::WriteEvent => "Write Event".to_string(),
        Screen::TrimStart => "Trim Start".to_string(),
        Screen::Watch => "Watch Events".to_string(),
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
            Screen::Home => "↑↓/jk: navigate │ Enter: select │ q: quit │ ?: help",
            Screen::AggregateContext => "↑↓/jk: navigate │ Enter: select │ r: refresh │ q: back",
            Screen::ReadEvents | Screen::WriteEvent => "e/i: edit │ x: execute │ ↑↓: scroll │ q: back",
            Screen::Watch => "e/i: edit │ x: start │ s: stop │ ↑↓: scroll │ q: back",  // Add this
            Screen::Help => "q/Esc: back",
            _ => "q: back │ ?: help",
        },
    }
}

fn draw_watch(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(11),   // Input section
            Constraint::Min(10),     // Events display
        ])
        .split(area);

    // Input section
    let input_block = Block::default()
        .borders(Borders::ALL)
        .title(if app.watch_active {
            " Watch Configuration (Active) "
        } else {
            " Watch Configuration "
        });
    let input_inner = input_block.inner(chunks[0]);
    f.render_widget(input_block, chunks[0]);

    let input_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Event types
            Constraint::Length(3),  // Latency
            Constraint::Length(3),  // Throughput
        ])
        .split(input_inner);

    // Event types input
    let et_style = if app.input_mode == InputMode::Editing && app.input_field_index == 0 {
        Style::default().fg(EDITING_COLOR)
    } else if app.watch_active {
        Style::default().fg(DIM_COLOR)
    } else {
        Style::default()
    };
    let et_input = Paragraph::new(app.watch_event_types.as_str())
        .style(et_style)
        .block(Block::default()
            .borders(Borders::ALL)
            .title(" Event Types (0=DEL,1=WRITE,2=READ,3=TRIM,4=EXISTS,5=PREPEND) "));
    f.render_widget(et_input, input_chunks[0]);

    // Latency input
    let lat_style = if app.input_mode == InputMode::Editing && app.input_field_index == 1 {
        Style::default().fg(EDITING_COLOR)
    } else if app.watch_active {
        Style::default().fg(DIM_COLOR)
    } else {
        Style::default()
    };
    let lat_input = Paragraph::new(app.watch_latency_ms.as_str())
        .style(lat_style)
        .block(Block::default().borders(Borders::ALL).title(" Latency (ms) "));
    f.render_widget(lat_input, input_chunks[1]);

    // Set cursor position when editing
    if app.input_mode == InputMode::Editing && !app.watch_active {
        let (x, y) = match app.input_field_index {
            0 => (input_chunks[0].x + app.watch_event_types.len() as u16 + 1, input_chunks[0].y + 1),
            1 => (input_chunks[1].x + app.watch_latency_ms.len() as u16 + 1, input_chunks[1].y + 1),
            _ => (0, 0),
        };
        f.set_cursor_position((x, y));
    }

    // Events display section
    let visible_height = chunks[1].height.saturating_sub(2) as usize;
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
            } else if s.starts_with("Event:") {
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

    let input_style = match app.input_mode {
        InputMode::Editing => Style::default().fg(EDITING_COLOR),
        InputMode::Normal => Style::default(),
    };

    let input = Paragraph::new(app.server_address.as_str())
        .style(input_style)
        .block(Block::default().borders(Borders::ALL).title(" Server Address "));

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(2)
        .constraints([Constraint::Length(3), Constraint::Min(1)])
        .split(inner);

    f.render_widget(input, chunks[0]);

    if app.input_mode == InputMode::Editing {
        f.set_cursor_position((
            chunks[0].x + app.server_address.len() as u16 + 1,
            chunks[0].y + 1,
        ));
    }

    let help = Paragraph::new(vec![
        Line::from(""),
        Line::from("Press 'e' or 'i' to edit the server address"),
        Line::from("Press Enter to connect"),
        Line::from("Press Esc or 'q' to go back"),
    ])
    .style(Style::default().fg(DIM_COLOR));

    f.render_widget(help, chunks[1]);
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
            Span::styled(ctx.org_id.to_string(), Style::default().bold()),
        ]));
        info_lines.push(Line::from(vec![
            Span::styled("Type: ", Style::default().fg(DIM_COLOR)),
            Span::styled(ctx.aggregate_type_id.to_string(), Style::default().bold()),
        ]));
        info_lines.push(Line::from(vec![
            Span::styled("Aggregate ID: ", Style::default().fg(DIM_COLOR)),
            Span::styled(ctx.aggregate_id.to_string(), Style::default().bold()),
        ]));
        info_lines.push(Line::from(""));
        
        if let Some(info) = &ctx.info {
            info_lines.push(Line::from(Span::styled(
                "━━━ Aggregate Info ━━━",
                Style::default().fg(HEADER_COLOR),
            )));
            info_lines.push(Line::from(vec![
                Span::styled("Min batch: ", Style::default().fg(DIM_COLOR)),
                Span::raw(info.min_batch.to_string()),
            ]));
        } else {
            info_lines.push(Line::from(Span::styled(
                "Press 'r' to load aggregate info",
                Style::default().fg(DIM_COLOR).italic(),
            )));
        }
    }

    // Description
    if app.menu_index < menu_items.len() {
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

    // Input section
    let input_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[0]);

    let from_style = if app.input_mode == InputMode::Editing && app.input_field_index == 0 {
        Style::default().fg(EDITING_COLOR)
    } else {
        Style::default()
    };
    let from_input = Paragraph::new(app.read_from_index.as_str())
        .style(from_style)
        .block(Block::default().borders(Borders::ALL).title(" From Batch Index "));
    f.render_widget(from_input, input_chunks[0]);

    let to_style = if app.input_mode == InputMode::Editing && app.input_field_index == 1 {
        Style::default().fg(EDITING_COLOR)
    } else {
        Style::default()
    };
    let to_input = Paragraph::new(app.read_to_index.as_str())
        .style(to_style)
        .block(Block::default().borders(Borders::ALL).title(" To Batch Index (optional) "));
    f.render_widget(to_input, input_chunks[1]);

    // Set cursor position when editing
    if app.input_mode == InputMode::Editing {
        let (x, y) = match app.input_field_index {
            0 => (input_chunks[0].x + app.read_from_index.len() as u16 + 1, input_chunks[0].y + 1),
            1 => (input_chunks[1].x + app.read_to_index.len() as u16 + 1, input_chunks[1].y + 1),
            _ => (0, 0),
        };
        f.set_cursor_position((x, y));
    }

    // Results section
    let visible_height = chunks[1].height.saturating_sub(2) as usize;
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

    // Input field
    let input_style = if app.input_mode == InputMode::Editing {
        Style::default().fg(EDITING_COLOR)
    } else if !has_info {
        Style::default().fg(DIM_COLOR)
    } else {
        Style::default()
    };

    let input = Paragraph::new(app.trim_keep_from.as_str())
        .style(input_style)
        .block(Block::default().borders(Borders::ALL).title(" Keep From Batch Index "));
    f.render_widget(input, chunks[1]);

    if app.input_mode == InputMode::Editing {
        f.set_cursor_position((
            chunks[1].x + app.trim_keep_from.len() as u16 + 1,
            chunks[1].y + 1,
        ));
    }

    // Help text
    let help = Paragraph::new(vec![
        Line::from(""),
        Line::from("Press 'e' or 'i' to edit the batch index"),
        Line::from("Press Enter or 'x' to execute the trim"),
        Line::from("Press Esc or 'q' to go back"),
        Line::from(""),
        Line::from(Span::styled(
            "⚠ Warning: This operation cannot be undone!",
            Style::default().fg(ERROR_COLOR).bold(),
        )),
    ])
    .style(Style::default().fg(DIM_COLOR));

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
    let client_id_display = Paragraph::new(format!("{}", app.client_id))
        .style(Style::default().fg(DIM_COLOR))
        .block(Block::default()
            .borders(Borders::ALL)
            .title(" Client ID (derived from keypair) ")
            .border_style(Style::default().fg(DIM_COLOR)));
    f.render_widget(client_id_display, chunks[0]);

    // Event Type
    let type_style = if app.input_mode == InputMode::Editing && app.input_field_index == 0 {
        Style::default().fg(EDITING_COLOR)
    } else {
        Style::default()
    };
    let type_input = Paragraph::new(app.write_event_type.as_str())
        .style(type_style)
        .block(Block::default().borders(Borders::ALL).title(" Event Type "));
    f.render_widget(type_input, chunks[1]);

    // Event Data
    let data_style = if app.input_mode == InputMode::Editing && app.input_field_index == 1 {
        Style::default().fg(EDITING_COLOR)
    } else {
        Style::default()
    };
    let data_input = Paragraph::new(app.write_data.as_str())
        .style(data_style)
        .block(Block::default().borders(Borders::ALL).title(" Event Data (JSON/text or file path) "))
        .wrap(Wrap { trim: false });
    f.render_widget(data_input, chunks[2]);

    // Set cursor position when editing
    if app.input_mode == InputMode::Editing {
        let (x, y) = match app.input_field_index {
            0 => (chunks[1].x + app.write_event_type.len() as u16 + 1, chunks[1].y + 1),
            1 => {
                let lines: Vec<&str> = app.write_data.lines().collect();
                let last_line_len = lines.last().map(|s| s.len()).unwrap_or(app.write_data.len());
                (chunks[2].x + last_line_len as u16 + 1, chunks[2].y + lines.len().max(1) as u16)
            }
            _ => (0, 0),
        };
        f.set_cursor_position((x, y));
    }

    // Results
    let result_lines: Vec<Line> = app
        .result_output
        .iter()
        .map(|s| Line::from(s.as_str()))
        .collect();

    let results = Paragraph::new(result_lines)
        .block(Block::default().borders(Borders::ALL).title(" Result - Press 'x' to write "))
        .wrap(Wrap { trim: false });

    f.render_widget(results, chunks[3]);
}

fn draw_help(f: &mut Frame, _app: &App, area: Rect) {
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
        Line::from("  ↑↓/jk         Scroll through events"),
        Line::from("  g/G           Jump to start/end"),
        Line::from(""),
        Line::from(Span::styled("Event Types:", Style::default().fg(DIM_COLOR))),
        Line::from("  0=DELETE  1=WRITE  2=READ"),
        Line::from("  3=TRIM_START  4=EXISTS  5=PREPEND_BATCHES"),
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
        .wrap(Wrap { trim: true });

    f.render_widget(help, area);
}