use std::{net::SocketAddr, path::PathBuf};
use eventplanedb_storage_glommio::{GlommioServer, GlommioServerConfig};

#[derive(clap::Parser)]
#[command(name = "eventplane-glommio-server")]
#[command(about = "EventPlane DB Glommio Server")]
struct Args {
    #[arg(short, long, default_value = "./data")]
    base_path: PathBuf,

    #[arg(short, long, default_value = "127.0.0.1:8080")]
    bind_addr: SocketAddr,

    #[arg(short, long)]
    cores: Option<usize>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = <Args as clap::Parser>::parse();

    let config = GlommioServerConfig::default()
        .with_base_path(args.base_path)
        .with_bind_addr(args.bind_addr)
        .with_core_count(args.cores.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        }));

    println!("Starting EventPlane Glommio server...");
    println!("Base path: {:?}", config.base_path);
    println!("Bind address: {}", config.bind_addr);
    println!("Cores: {:?}", config.core_count);

    let server = GlommioServer::new(config);
    
    // Run the server using Glommio's built-in executor pool
    glommio::LocalExecutor::default().run(async move {
        server.run().await
    })
}