mod cli;
mod operations;
mod tui;
mod utils;

use anyhow::Result;
use clap::Parser;
use cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(cmd) => {
            // CLI mode - execute command and exit
            operations::execute_command(&cli.server, cli.api_key.as_deref(), cmd).await?;
        }
        None => {
            // Interactive TUI mode
            tui::run(&cli.server).await?;
        }
    }

    Ok(())
}