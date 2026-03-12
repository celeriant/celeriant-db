mod app;
mod event;
pub mod settings;
mod ui;

use anyhow::Result;
use crossterm::{
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::prelude::*;
use std::io;

pub use app::App;

use crate::cli::Cli;

pub async fn run(cli: &Cli) -> Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app and run
    let mut app = App::new(cli)?;
    let result = run_app(&mut terminal, &mut app).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    if let Err(err) = result {
        eprintln!("Error: {:?}", err);
    }

    Ok(())
}

async fn run_app<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> Result<()> {
    loop {
        app.poll_watch_events();

        // Update visible_height from terminal size before drawing.
        // The main content area occupies the terminal height minus 6 rows (3 header + 3 status).
        let term_height = terminal.size()?.height as usize;
        app.visible_height.set(term_height.saturating_sub(8).max(1));

        terminal.draw(|f| ui::draw(f, app))?;

        if event::handle_events(app).await? {
            return Ok(());
        }
    }
}