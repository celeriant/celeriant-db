use celeriant_crypto::Crypto;
use celeriant_runtimes::run_executors_and_sidecar;
use clap::Parser;
use dotenvy::dotenv;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::server_config::ServerConfig;

mod server_config;
mod dio_check;

pub fn startup(args: Vec<String>) -> Result<(), std::io::Error> {
    install_crash_handler();

    load_dotenv();

    let server_config = ServerConfig::parse_from(args);

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&server_config.log_level)),
        )
        .init();

    server_config.log_non_defaults();

    // Verify Direct I/O is actually working
    if let Err(e) = dio_check::verify_direct_io(&server_config.data_root) {
        error!("Direct I/O verification failed: {}", e);
        std::process::exit(1);
    }
    info!("Direct I/O verification passed");

    // Load or generate a persistent node ID
    let node_id = match Crypto::load_or_generate_node_id(&server_config.data_root) {
        Ok(id) => id,
        Err(e) => {
            error!("Failed to initialize node ID: {}", e);
            std::process::exit(1);
        }
    };

    info!("node_id={}, data_root={:?}, listen_address={}, client_port={}, replication_port={}", node_id, server_config.data_root, server_config.listen_address, server_config.client_port, server_config.replication_port);
    
    let nbr_shards = server_config.num_shards.unwrap_or_else(num_cpus::get) as u32;
    let shard_config = server_config.to_shard_config(node_id, nbr_shards);
    let sidecar_config = server_config.to_sidecar_config(nbr_shards);
    let sidecar_store_config = server_config.to_sidecar_store_config();

    let sidecar_store = match celeriant_sidecar::store::SidecarStore::new(sidecar_store_config) {
        Ok(sidecar_store) => sidecar_store,
        Err(e) => {
            error!("Failed to initialize SidecarStore: {}", e);
            std::process::exit(1);
        }
    };

    run_executors_and_sidecar(shard_config, sidecar_config, server_config.mesh_channel_size, node_id, sidecar_store);

    Ok(())
}

fn load_dotenv() {
    match dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            eprintln!("FATAL Error loading config from: {e}");
            eprintln!("Aborting");
            std::process::exit(1);
        }
    };
}

fn install_crash_handler() {
    unsafe {
        set_signal_handler(libc::SIGBUS, signal_handler);
        set_signal_handler(libc::SIGSEGV, signal_handler);
        set_signal_handler(libc::SIGILL, signal_handler);
    }
}

unsafe extern "C" fn signal_handler(_sig: i32) {
    std::process::abort();
}

unsafe fn set_signal_handler(signal: libc::c_int, handler: unsafe extern "C" fn(libc::c_int)) {
    use libc::{sigaction, sigfillset, sighandler_t};
    let mut sigset = unsafe { std::mem::zeroed() };
    if unsafe { sigfillset(&mut sigset) } != -1 {
        let mut action: sigaction = unsafe { std::mem::zeroed() };
        action.sa_mask = sigset;
        action.sa_sigaction = handler as sighandler_t;

        unsafe {
            sigaction(signal, &action, std::ptr::null_mut());
        }
    }
}
