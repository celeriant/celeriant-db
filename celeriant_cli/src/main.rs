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

    match cli.command.clone() {
        Some(cmd) => {
            operations::execute_command(&cli, cmd).await?;
        }
        None => {
            tui::run(&cli).await?;
        }
    }

    Ok(())
}