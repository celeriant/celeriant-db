use celeriant_crypto::Crypto;
use celeriant_ktls::verify_ktls_support;
use celeriant_runtimes::run_executors_and_sidecar;
use clap::Parser;
use dotenvy::dotenv;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::server_config::ServerConfig;

pub mod server_config;
pub mod cert_cmd;
pub mod keys_cmd;
pub mod api_keys;
mod dio_check;
mod fs_check;

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

    // Route panics through tracing so they appear in stdout (captured by test harness).
    // The default panic hook writes to stderr, which integration tests don't read.
    std::panic::set_hook(Box::new(|info| {
        error!("PANIC: {}", info);
    }));

    server_config.log_non_defaults();

    // Verify Direct I/O is actually working
    if let Err(e) = dio_check::verify_direct_io(&server_config.data_root) {
        error!("Direct I/O verification failed: {}", e);
        std::process::exit(1);
    }
    info!("Direct I/O verification passed");

    if let Some(ref temp_dir) = server_config.compaction_temp_dir {
        if let Err(e) = fs_check::verify_same_filesystem(&server_config.data_root, temp_dir) {
            error!("Compaction temp directory check failed: {}", e);
            std::process::exit(1);
        }
        info!("Compaction temp directory filesystem check passed");
    }

    // Load or generate a persistent node ID
    let node_id = match Crypto::load_or_generate_node_id(&server_config.data_root) {
        Ok(id) => id,
        Err(e) => {
            error!("Failed to initialize node ID: {}", e);
            std::process::exit(1);
        }
    };

    info!("node_id={}, data_root={:?}, listen_address={}, client_port={}, replication_port={}", node_id, server_config.data_root, server_config.listen_address, server_config.client_port, server_config.replication_port);

    // Check ports aren't already in use (Glommio uses SO_REUSEPORT, so bind won't fail).
    // Connect to 127.0.0.1 regardless of listen_address since 0.0.0.0 isn't connectable.
    for (label, port) in [("client", server_config.client_port), ("replication", server_config.replication_port)] {
        let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        if std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(100)).is_ok() {
            error!("Port {} ({}) is already in use — another server may be running", port, label);
            std::process::exit(1);
        }
    }

    let nbr_shards = server_config.num_shards.unwrap_or_else(num_cpus::get) as u32;

    // Build TLS config if enabled, and verify kernel kTLS support.
    let tls_config = match server_config.build_tls_config() {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("TLS configuration error: {}", e);
            std::process::exit(1);
        }
    };

    if tls_config.is_some() {
        if let Err(e) = verify_ktls_support() {
            error!("Kernel kTLS support check failed: {:?}. Ensure CONFIG_TLS=y and the tls kernel module is loaded.", e);
            std::process::exit(1);
        }
        info!("Kernel kTLS support verified");
    }

    // Load API keys if configured
    let api_keys = match api_keys::load_api_keys(&server_config.data_root) {
        Ok(keys) => keys,
        Err(e) => {
            error!("Failed to load API keys: {}", e);
            std::process::exit(1);
        }
    };

    // Validate TLS requirement for API key auth
    if api_keys.is_some() && tls_config.is_none() && !server_config.insecure_allow_plaintext_auth {
        error!(
            "API key authentication is configured (api_keys.toml exists) but TLS is not enabled.\n\
             API keys are transmitted during connection handshake and MUST be encrypted in transit.\n\
             \n\
             To fix, enable TLS:\n\
               --tls-ca-cert /path/to/ca.crt --tls-node-cert /path/to/node.crt --tls-node-key /path/to/node.key\n\
             \n\
             For development/testing only, you can bypass this check:\n\
               --insecure-allow-plaintext-auth\n\
             \n\
             WARNING: --insecure-allow-plaintext-auth transmits API keys in cleartext.\n\
             Never use this flag in production."
        );
        std::process::exit(1);
    }

    if api_keys.is_some() && tls_config.is_none() && server_config.insecure_allow_plaintext_auth {
        tracing::warn!("API key authentication running WITHOUT TLS - keys transmitted in plaintext");
    }

    if api_keys.is_some() {
        info!("API key authentication enabled");
    }

    let shard_config = server_config.to_shard_config(node_id, nbr_shards, tls_config, api_keys);
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
