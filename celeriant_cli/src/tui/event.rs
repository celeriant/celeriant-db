use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

use super::app::{App, InputMode, Screen};

pub async fn handle_events(app: &mut App) -> anyhow::Result<bool> {
    if event::poll(Duration::from_millis(100))? {
        if let Event::Key(key) = event::read()? {
            // Global quit
            if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
                app.should_quit = true;
                return Ok(true);
            }
            
            match app.input_mode {
                InputMode::Normal => handle_normal_mode(app, key).await?,
                InputMode::Editing => handle_editing_mode(app, key).await?,
            }
        }
    }
    
    Ok(app.should_quit)
}

async fn handle_normal_mode(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    match key.code {
        KeyCode::Char('q') => {
            if app.screen == Screen::Home {
                app.should_quit = true;
            } else {
                app.go_back();
            }
        }
        KeyCode::Esc => {
            if app.screen != Screen::Home {
                app.go_back();
            }
        }
        KeyCode::Char('?') | KeyCode::F(1) => {
            app.go_to_screen(Screen::Help);
        }
        _ => {
            match app.screen {
                Screen::Home => handle_home_keys(app, key).await?,
                Screen::Connect => handle_connect_keys(app, key).await?,
                Screen::AggregateContext => handle_aggregate_context_keys(app, key).await?,
                Screen::EnterAggregate => handle_enter_aggregate_keys(app, key).await?,
                Screen::ReadEvents => handle_read_events_keys(app, key).await?,
                Screen::WriteEvent => handle_write_event_keys(app, key).await?,
                Screen::TrimStart => handle_trim_start_keys(app, key).await?,
                Screen::Watch => handle_watch_keys(app, key).await?,  // Add this
                Screen::Help => handle_help_keys(app, key),
            }
        }
    }
    Ok(())
}

async fn handle_trim_start_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    match key.code {
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Char('x') => {
            // Execute trim
            if let Err(e) = app.trim_aggregate().await {
                app.set_error(&e);
            } else {
                // Refresh info and go back
                let _ = app.check_aggregate_exists().await;
                app.go_back();
            }
        }
        _ => {}
    }
    Ok(())
}

async fn handle_enter_aggregate_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    match key.code {
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            app.input_mode = InputMode::Editing;
        }
        _ => {}
    }
    Ok(())
}

async fn handle_editing_mode(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    match key.code {
        KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
        }
        KeyCode::Enter => {
            app.input_mode = InputMode::Normal;
            match app.screen {
                Screen::Connect => {
                    if let Err(e) = app.connect().await {
                        app.set_error(&e);
                    } else {
                        app.go_to_screen(Screen::Home);
                    }
                }
                Screen::EnterAggregate => {  
                    if let Err(e) = app.navigate_to_aggregate_from_input().await {
                        app.set_error(&e);
                    }
                }
                Screen::ReadEvents => {
                    if let Err(e) = app.read_events().await {
                        app.set_error(&e);
                    }
                }
                Screen::WriteEvent => {
                    // Don't execute on Enter - user must press 'x' to write
                }
                Screen::TrimStart => {
                    if let Err(e) = app.trim_aggregate().await {
                        app.set_error(&e);
                    } else {
                        // Refresh info and go back
                        let _ = app.check_aggregate_exists().await;
                        app.go_back();
                    }
                }
                _ => {}
            }
        }
        KeyCode::Tab => {
            if !app.input_fields.is_empty() {
                app.input_field_index = (app.input_field_index + 1) % app.input_fields.len();
            }
        }
        KeyCode::BackTab => {
            if !app.input_fields.is_empty() {
                app.input_field_index = app.input_field_index.checked_sub(1)
                    .unwrap_or(app.input_fields.len() - 1);
            }
        }
        KeyCode::Char(c) => {
            match app.screen {
                Screen::Connect => {
                    app.server_address.push(c);
                }
                Screen::EnterAggregate => {  
                    if app.input_field_index < app.input_fields.len() {
                        app.input_fields[app.input_field_index].value.push(c);
                    }
                }
                Screen::ReadEvents => {
                    match app.input_field_index {
                        0 => app.read_from_index.push(c),
                        1 => app.read_to_index.push(c),
                        _ => {}
                    }
                }
                Screen::WriteEvent => {
                    match app.input_field_index {
                        0 => app.write_event_type.push(c),
                        1 => app.write_data.push(c),
                        _ => {}
                    }
                }
                Screen::TrimStart => {
                    app.trim_keep_from.push(c);
                }
                Screen::Watch => {
                    match app.input_field_index {
                        0 => app.watch_event_types.push(c),
                        1 => app.watch_latency_ms.push(c),
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        KeyCode::Backspace => {
            match app.screen {
                Screen::Connect => {
                    app.server_address.pop();
                }
                Screen::EnterAggregate => {  
                    if app.input_field_index < app.input_fields.len() {
                        app.input_fields[app.input_field_index].value.pop();
                    }
                }
                Screen::ReadEvents => {
                    match app.input_field_index {
                        0 => { app.read_from_index.pop(); }
                        1 => { app.read_to_index.pop(); }
                        _ => {}
                    }
                }
                Screen::WriteEvent => {
                    match app.input_field_index {
                        0 => { app.write_event_type.pop(); }
                        1 => { app.write_data.pop(); }
                        _ => {}
                    }
                }
                Screen::TrimStart => {
                    app.trim_keep_from.pop();
                }
                Screen::Watch => {
                    match app.input_field_index {
                        0 => { app.watch_event_types.pop(); }
                        1 => { app.watch_latency_ms.pop(); }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
    Ok(())
}

async fn handle_home_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    let menu_items = app.get_home_menu_items();
    
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            if app.menu_index > 0 {
                app.menu_index -= 1;
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.menu_index < menu_items.len() - 1 {
                app.menu_index += 1;
            }
        }
        KeyCode::Enter => {
            if app.is_connected() {
                match app.menu_index {
                    0 => {
                        // Fixed: Navigate to EnterAggregate screen
                        app.setup_enter_aggregate_fields();
                        app.go_to_screen(Screen::EnterAggregate);
                    }
                    1 => {
                        app.disconnect().await;
                    }
                    2 => {
                        app.go_to_screen(Screen::Help);
                    }
                    3 => {
                        app.should_quit = true;
                    }
                    _ => {}
                }
            } else {
                match app.menu_index {
                    0 => {
                        if let Err(e) = app.connect().await {
                            app.set_error(&e);
                        }
                    }
                    1 => {
                        app.input_mode = InputMode::Editing;
                        app.go_to_screen(Screen::Connect);
                    }
                    2 => {
                        app.go_to_screen(Screen::Help);
                    }
                    3 => {
                        app.should_quit = true;
                    }
                    _ => {}
                }
            }
        }
        KeyCode::Char('c') if !app.is_connected() => {
            if let Err(e) = app.connect().await {
                app.set_error(&e);
            }
        }
        _ => {}
    }
    Ok(())
}


async fn handle_connect_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    match key.code {
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            app.input_mode = InputMode::Editing;
        }
        _ => {}
    }
    Ok(())
}

async fn handle_aggregate_context_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    let menu_items = app.get_aggregate_menu_items();
    
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            if app.menu_index > 0 {
                app.menu_index -= 1;
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.menu_index < menu_items.len() - 1 {
                app.menu_index += 1;
            }
        }
        KeyCode::Enter => {
            match app.menu_index {
                0 => {
                    // Refresh Info
                    if let Err(e) = app.check_aggregate_exists().await {
                        app.set_error(&e);
                    }
                }
                1 => {
                    // Read Events
                    app.input_fields = vec![
                        InputField::with_value("From batch index", &app.read_from_index),
                        InputField::new("To batch index (optional)", ""),
                    ];
                    app.input_field_index = 0;
                    app.result_output.clear();
                    app.go_to_screen(Screen::ReadEvents);
                }
                2 => {
                    // Write Event
                    app.input_fields = vec![
                        InputField::with_value("Event Type", &app.write_event_type),
                        InputField::new("Event Data (JSON/text)", ""),
                    ];
                    app.input_field_index = 0;
                    app.result_output.clear();
                    app.go_to_screen(Screen::WriteEvent);
                }
                3 => {
                    // Watch
                    app.setup_watch_fields();
                    app.go_to_screen(Screen::Watch);
                }
                4 => {
                    // Trim Start
                    if let Some(ctx) = &app.aggregate_context {
                        if let Some(info) = &ctx.info {
                            app.trim_keep_from = (info.min_batch + 1).to_string();
                        } else {
                            app.trim_keep_from = "1".to_string();
                        }
                        app.go_to_screen(Screen::TrimStart);
                    }
                }
                5 => {
                    // Delete
                    if let Err(e) = app.delete_aggregate().await {
                        app.set_error(&e);
                    } else {
                        app.go_back();
                    }
                }
                6 => {
                    // Back
                    app.go_back();
                }
                _ => {}
            }
        }
        KeyCode::Char('r') => {
            if let Err(e) = app.check_aggregate_exists().await {
                app.set_error(&e);
            }
        }
        _ => {}
    }
    Ok(())
}

async fn handle_watch_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    // Poll for new watch events
    app.poll_watch_events();
    
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            if app.watch_scroll > 0 {
                app.watch_scroll -= 1;
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.watch_scroll < app.watch_events.len().saturating_sub(10) {
                app.watch_scroll += 1;
            }
        }
        KeyCode::PageUp => {
            app.watch_scroll = app.watch_scroll.saturating_sub(10);
        }
        KeyCode::PageDown => {
            app.watch_scroll = (app.watch_scroll + 10).min(app.watch_events.len().saturating_sub(10));
        }
        KeyCode::Home | KeyCode::Char('g') => {
            app.watch_scroll = 0;
        }
        KeyCode::End | KeyCode::Char('G') => {
            app.watch_scroll = app.watch_events.len().saturating_sub(10);
        }
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            if !app.watch_active {
                app.input_mode = InputMode::Editing;
            }
        }
        KeyCode::Char('x') => {
            if !app.watch_active {
                if let Err(e) = app.start_watch().await {
                    app.set_error(&e);
                }
            }
        }
        KeyCode::Char('s') => {
            if app.watch_active {
                app.stop_watch();
            }
        }
        KeyCode::Esc | KeyCode::Char('q') => {
            // Stop watch when leaving
            app.stop_watch();
            app.go_back();
        }
        _ => {}
    }
    Ok(())
}

async fn handle_read_events_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            if app.result_scroll > 0 {
                app.result_scroll -= 1;
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.result_scroll < app.result_output.len().saturating_sub(10) {
                app.result_scroll += 1;
            }
        }
        KeyCode::PageUp => {
            app.result_scroll = app.result_scroll.saturating_sub(10);
        }
        KeyCode::PageDown => {
            app.result_scroll = (app.result_scroll + 10).min(app.result_output.len().saturating_sub(10));
        }
        KeyCode::Home | KeyCode::Char('g') => {
            app.result_scroll = 0;
        }
        KeyCode::End | KeyCode::Char('G') => {
            app.result_scroll = app.result_output.len().saturating_sub(10);
        }
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Char('x') => {
            // Execute read
            if let Err(e) = app.read_events().await {
                app.set_error(&e);
            }
        }
        _ => {}
    }
    Ok(())
}

async fn handle_write_event_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            if app.result_scroll > 0 {
                app.result_scroll -= 1;
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.result_scroll < app.result_output.len().saturating_sub(10) {
                app.result_scroll += 1;
            }
        }
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Char('x') => {
            // Execute write
            if let Err(e) = app.write_event().await {
                app.set_error(&e);
            }
        }
        _ => {}
    }
    Ok(())
}

fn handle_help_keys(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => {
            app.go_back();
        }
        _ => {}
    }
}

use super::app::InputField;