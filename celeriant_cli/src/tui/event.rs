use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

use super::app::{App, InputMode, PendingAction, Screen};

pub async fn handle_events(app: &mut App) -> anyhow::Result<bool> {
    if event::poll(Duration::from_millis(100))?
        && let Event::Key(key) = event::read()? {
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

    Ok(app.should_quit)
}

/// Returns true if the key was a scroll key and the scroll was updated.
fn handle_scroll(key: KeyCode, scroll: &mut usize, total: usize, page_size: usize) -> bool {
    match key {
        KeyCode::Up | KeyCode::Char('k') => { *scroll = scroll.saturating_sub(1); true }
        KeyCode::Down | KeyCode::Char('j') => { *scroll = (*scroll + 1).min(total.saturating_sub(page_size)); true }
        KeyCode::PageUp => { *scroll = scroll.saturating_sub(page_size); true }
        KeyCode::PageDown => { *scroll = (*scroll + page_size).min(total.saturating_sub(page_size)); true }
        KeyCode::Home | KeyCode::Char('g') => { *scroll = 0; true }
        KeyCode::End | KeyCode::Char('G') => { *scroll = total.saturating_sub(page_size); true }
        _ => false,
    }
}

async fn handle_normal_mode(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    match key.code {
        KeyCode::Char('q') => {
            if app.screen == Screen::Home {
                app.should_quit = true;
            } else {
                if app.screen == Screen::Settings {
                    app.sync_settings_from_fields();
                }
                app.go_back();
            }
        }
        KeyCode::Esc => {
            if app.screen != Screen::Home {
                if app.screen == Screen::Settings {
                    app.sync_settings_from_fields();
                }
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
                Screen::Settings => handle_settings_keys(app, key).await?,
                Screen::AggregateContext => handle_aggregate_context_keys(app, key).await?,
                Screen::EnterAggregate => handle_enter_aggregate_keys(app, key).await?,
                Screen::ReadEvents => handle_read_events_keys(app, key).await?,
                Screen::WriteEvent => handle_write_event_keys(app, key).await?,
                Screen::TrimStart => handle_trim_start_keys(app, key).await?,
                Screen::Watch => handle_watch_keys(app, key, false).await?,
                Screen::OrgWatch => handle_watch_keys(app, key, true).await?,
                Screen::List => handle_list_keys(app, key).await?,
                Screen::RegisterSchema => handle_register_schema_keys(app, key).await?,
                Screen::Help => handle_help_keys(app, key),
            }
        }
    }
    Ok(())
}

async fn handle_list_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    let total = app.list_results.len();
    let page_size = app.visible_height.get();
    if handle_scroll(key.code, &mut app.list_scroll, total, page_size) {
        return Ok(());
    }
    match key.code {
        KeyCode::Enter => {
            // If the current scroll position maps to a selectable aggregate, navigate to it.
            // Otherwise fall through to enter editing mode.
            if !app.list_selectable.is_empty()
                && app.navigate_to_aggregate_from_list().await {
                    return Ok(());
                }
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Char('e') | KeyCode::Char('i') => {
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Char('x') => {
            app.sync_fields_to_state();
            if let Err(e) = app.execute_list().await {
                app.set_error(&e);
            }
        }
        _ => {}
    }
    Ok(())
}

async fn handle_trim_start_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    match key.code {
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Char('x') => {
            app.sync_fields_to_state();
            // Require confirmation before executing
            app.pending_action = Some(PendingAction::Trim);
            app.confirm_input.clear();
            app.input_mode = InputMode::Editing;
            app.set_status("Type YES to confirm trim, or Esc to cancel");
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
    // If a confirmation prompt is active, route keystrokes to it
    if app.pending_action.is_some() {
        match key.code {
            KeyCode::Esc => {
                app.pending_action = None;
                app.confirm_input.clear();
                app.set_status("Cancelled");
                app.input_mode = InputMode::Normal;
            }
            KeyCode::Enter => {
                if app.confirm_input == "YES" {
                    let action = app.pending_action.take().unwrap();
                    app.confirm_input.clear();
                    app.input_mode = InputMode::Normal;
                    match action {
                        PendingAction::Delete => {
                            if let Err(e) = app.delete_aggregate().await {
                                app.set_error(&e);
                            } else {
                                app.go_back();
                            }
                        }
                        PendingAction::Trim => {
                            app.sync_fields_to_state();
                            if let Err(e) = app.trim_aggregate().await {
                                app.set_error(&e);
                            } else {
                                let _ = app.check_aggregate_exists().await;
                                app.go_back();
                            }
                        }
                    }
                } else {
                    app.set_error("Type YES to confirm, or Esc to cancel");
                }
            }
            KeyCode::Char(c) => {
                app.confirm_input.push(c);
            }
            KeyCode::Backspace => {
                app.confirm_input.pop();
            }
            _ => {}
        }
        return Ok(());
    }

    match key.code {
        KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
        }
        KeyCode::Enter => {
            app.input_mode = InputMode::Normal;
            match app.screen {
                Screen::Connect => {
                    app.sync_fields_to_state();
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
                    app.sync_fields_to_state();
                    if let Err(e) = app.read_events().await {
                        app.set_error(&e);
                    }
                }
                Screen::WriteEvent => {
                    // Don't execute on Enter - user must press 'x' to write
                }
                Screen::TrimStart => {
                    // Don't execute on Enter - user must press 'x' to trigger confirmation
                }
                _ => {}
            }
        }
        KeyCode::Tab => {
            if !app.input_fields.is_empty() {
                app.input_field_index = (app.input_field_index + 1) % app.input_fields.len();
                if app.screen == Screen::Settings {
                    let page_size = app.visible_height.get();
                    if app.input_field_index >= app.settings_scroll + page_size {
                        app.settings_scroll = app.input_field_index + 1 - page_size;
                    }
                    if app.input_field_index < app.settings_scroll {
                        app.settings_scroll = app.input_field_index;
                    }
                }
            }
        }
        KeyCode::BackTab => {
            if !app.input_fields.is_empty() {
                app.input_field_index = app.input_field_index.checked_sub(1)
                    .unwrap_or(app.input_fields.len() - 1);
                if app.screen == Screen::Settings {
                    let page_size = app.visible_height.get();
                    if app.input_field_index < app.settings_scroll {
                        app.settings_scroll = app.input_field_index;
                    }
                    if app.input_field_index >= app.settings_scroll + page_size {
                        app.settings_scroll = app.input_field_index + 1 - page_size;
                    }
                }
            }
        }
        KeyCode::Char(c) => {
            if app.input_field_index < app.input_fields.len() {
                app.input_fields[app.input_field_index].value.push(c);
            }
        }
        KeyCode::Backspace => {
            if app.input_field_index < app.input_fields.len() {
                app.input_fields[app.input_field_index].value.pop();
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
                        // Enter Aggregate
                        app.setup_enter_aggregate_fields();
                        app.go_to_screen(Screen::EnterAggregate);
                    }
                    1 => {
                        // List
                        app.setup_list_fields();
                        app.go_to_screen(Screen::List);
                    }
                    2 => {
                        // Organisation Watch
                        app.setup_org_watch_fields();
                        app.go_to_screen(Screen::OrgWatch);
                    }
                    3 => {
                        app.disconnect().await;
                    }
                    4 => {
                        app.go_to_screen(Screen::Help);
                    }
                    5 => {
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
                        app.setup_connect_fields();
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
        KeyCode::Char('s') => {
            app.setup_settings_fields();
            app.go_to_screen(Screen::Settings);
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
        KeyCode::Char('x') => {
            app.sync_fields_to_state();
            if let Err(e) = app.connect().await {
                app.set_error(&e);
            } else {
                app.go_to_screen(Screen::Home);
            }
        }
        KeyCode::Char('s') => {
            app.setup_settings_fields();
            app.go_to_screen(Screen::Settings);
        }
        _ => {}
    }
    Ok(())
}

/// Toggle a boolean or enum-cycle field in-place. Returns true if the field was toggled.
fn toggle_settings_field(app: &mut App) -> bool {
    let field = &app.input_fields[app.input_field_index];
    if field.label.contains("(true/false)") {
        let toggled = if field.value.trim().eq_ignore_ascii_case("true") { "false" } else { "true" };
        app.input_fields[app.input_field_index].value = toggled.to_string();
        true
    } else if field.label.contains("(auto/custom/none)") {
        let next = match field.value.trim() {
            v if v.eq_ignore_ascii_case("auto") => "custom",
            v if v.eq_ignore_ascii_case("custom") => "none",
            _ => "auto",
        };
        app.input_fields[app.input_field_index].value = next.to_string();
        true
    } else {
        false
    }
}

async fn handle_settings_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    let total = app.input_fields.len();
    let page_size = app.visible_height.get();
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            app.input_field_index = app.input_field_index.saturating_sub(1);
            app.settings_scroll = app.settings_scroll.min(app.input_field_index);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if total > 0 {
                app.input_field_index = (app.input_field_index + 1).min(total - 1);
                if app.input_field_index >= app.settings_scroll + page_size {
                    app.settings_scroll = app.input_field_index + 1 - page_size;
                }
            }
        }
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            if app.input_field_index < total {
                if !toggle_settings_field(app) {
                    app.input_mode = InputMode::Editing;
                }
            }
        }
        KeyCode::Char(' ') => {
            if app.input_field_index < total {
                toggle_settings_field(app);
            }
        }
        KeyCode::Char('S') => {
            app.sync_settings_from_fields();
            match app.settings.save() {
                Ok(()) => app.set_status("Settings saved to ~/.celeriant/settings.toml"),
                Err(e) => app.set_error(&format!("Failed to save settings: {e}")),
            }
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
                    app.setup_read_events_fields();
                    app.go_to_screen(Screen::ReadEvents);
                }
                2 => {
                    // Write Event
                    app.setup_write_event_fields();
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
                    }
                    app.setup_trim_start_fields();
                    app.go_to_screen(Screen::TrimStart);
                }
                5 => {
                    // Register Schema
                    app.setup_register_schema_fields();
                    app.go_to_screen(Screen::RegisterSchema);
                }
                6 => {
                    // Delete — require confirmation
                    app.pending_action = Some(PendingAction::Delete);
                    app.confirm_input.clear();
                    app.input_mode = InputMode::Editing;
                    app.set_status("Type YES to confirm delete, or Esc to cancel");
                }
                7 => {
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

/// Handles both Watch and OrgWatch screens. `is_org` selects which start method to call.
async fn handle_watch_keys(app: &mut App, key: KeyEvent, is_org: bool) -> anyhow::Result<()> {
    let total = app.watch_events.len();
    let page_size = app.visible_height.get();
    if handle_scroll(key.code, &mut app.watch_scroll, total, page_size) {
        return Ok(());
    }

    match key.code {
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            if !app.watch_active {
                app.input_mode = InputMode::Editing;
            }
        }
        KeyCode::Char('x') => {
            if !app.watch_active {
                app.sync_fields_to_state();
                let result = if is_org {
                    app.start_org_watch().await
                } else {
                    app.start_watch().await
                };
                if let Err(e) = result {
                    app.set_error(&e);
                }
            }
        }
        KeyCode::Char('s') => {
            if app.watch_active {
                app.stop_watch();
            }
        }
        KeyCode::Char('c') => {
            app.watch_events.clear();
            app.watch_scroll = 0;
            app.set_status("Watch log cleared");
        }
        // q is handled by handle_normal_mode which calls go_back() -> stop_watch()
        _ => {}
    }
    Ok(())
}

async fn handle_read_events_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    let total = app.result_output.len();
    let page_size = app.visible_height.get();
    if handle_scroll(key.code, &mut app.result_scroll, total, page_size) {
        return Ok(());
    }
    match key.code {
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Char('x') => {
            app.sync_fields_to_state();
            if let Err(e) = app.read_events().await {
                app.set_error(&e);
            }
        }
        _ => {}
    }
    Ok(())
}

async fn handle_write_event_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    let total = app.result_output.len();
    let page_size = app.visible_height.get();
    if handle_scroll(key.code, &mut app.result_scroll, total, page_size) {
        return Ok(());
    }
    match key.code {
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Char('x') => {
            app.sync_fields_to_state();
            if let Err(e) = app.write_event().await {
                app.set_error(&e);
            }
        }
        _ => {}
    }
    Ok(())
}

async fn handle_register_schema_keys(app: &mut App, key: KeyEvent) -> anyhow::Result<()> {
    let total = app.result_output.len();
    let page_size = app.visible_height.get();
    if handle_scroll(key.code, &mut app.result_scroll, total, page_size) {
        return Ok(());
    }
    match key.code {
        KeyCode::Enter | KeyCode::Char('e') | KeyCode::Char('i') => {
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Char('x') => {
            app.sync_fields_to_state();
            if let Err(e) = app.register_schema().await {
                app.set_error(&e);
            }
        }
        _ => {}
    }
    Ok(())
}

fn handle_help_keys(app: &mut App, key: KeyEvent) {
    // Help text line count — keep in sync with draw_help
    const HELP_TOTAL_LINES: usize = 46;
    let page_size = app.visible_height.get();
    if handle_scroll(key.code, &mut app.help_scroll, HELP_TOTAL_LINES, page_size) {
        return;
    }
    match key.code {
        KeyCode::Esc | KeyCode::Enter => {
            app.go_back();
        }
        _ => {}
    }
}
