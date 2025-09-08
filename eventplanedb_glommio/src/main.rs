use clap::Parser;
use std::{net::SocketAddr, path::PathBuf};

use eventplanedb_storage_glommio::{GlommioServer, GlommioServerConfig};

#[derive(Parser)]
#[command(name = "eventplane-glommio-server")]
#[command(about = "EventPlane DB Glommio Server")]
struct Args {
    #[arg(short, long, default_value = "./data")]
    base_path: PathBuf,

    #[arg(short, long, default_value = "127.0.0.1:8080")]
    bind_addr: SocketAddr,

    #[arg(short, long)]
    shards: Option<usize>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let config = GlommioServerConfig::default()
        .with_base_path(args.base_path)
        .with_bind_addr(args.bind_addr);

    let config = if let Some(shard_count) = args.shards {
        config.with_shard_count(shard_count)
    } else {
        config
    };

    println!("Starting EventPlane Glommio server...");
    println!("Base path: {:?}", config.base_path);
    println!("Bind address: {}", config.bind_addr);
    println!("Shards: {:?}", config.shard_count);

    let server = GlommioServer::new(config);
    server.run()
}
