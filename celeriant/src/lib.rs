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
pub mod memory_budget;
mod dio_check;
mod fs_check;
mod fs_warmup;
mod ntp_check;
mod server_meta;

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

    // Warm filesystem metadata (XFS extent trees, inodes) into page cache.
    // With O_DIRECT, data bypasses page cache but metadata doesn't — cold metadata
    // after restart causes severe throughput degradation until naturally warmed.
    if let Err(e) = fs_warmup::warm_fs_metadata(&server_config.data_root) {
        tracing::warn!("Filesystem metadata warmup failed (non-fatal): {}", e);
    }

    // Warn if system clock is not NTP-synchronized (cluster requires synced clocks)
    match ntp_check::check_clock_synchronized() {
        Ok(()) => info!("Clock synchronization verified (kernel NTP-disciplined)"),
        Err(e) => tracing::warn!("{}", e),
    }

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

    // Validate immutable config hasn't changed since initial setup
    let current_meta = server_meta::ServerMeta {
        num_shards: nbr_shards,
        timestamp_precision: match server_config.timestamp_precision {
            server_config::ConfigTimestampPrecision::Milliseconds => "milliseconds",
            server_config::ConfigTimestampPrecision::Microseconds => "microseconds",
            server_config::ConfigTimestampPrecision::Nanoseconds => "nanoseconds",
        }.to_string(),
        timestamp_epoch_offset_secs: server_config.timestamp_epoch_offset_secs,
        routing_rule: server_config.routing_rule.to_string(),
        reserve_coordinator_shard: server_config.reserve_coordinator_shard,
        compression: server_meta::CompressionMeta::default(),
    };
    let desired_compression = server_config.to_compression_meta();
    if let Err(e) = server_meta::validate_or_create(
        &server_config.data_root,
        &current_meta,
        &desired_compression,
        Some(celeriant_wal::resolve_builtin_dict),
    ) {
        error!("{}", e);
        std::process::exit(1);
    } else {
        info!("No breaking configuration changes detected");
    }

    // Load dict bytes for shard executors. Happens before any port listens (Inv 4).
    let (dict_bytes, dict_sha256): (std::sync::Arc<[u8]>, std::sync::Arc<str>) = {
        let dict_path = server_config.data_root.join("dictionary.zstd_dict");
        match std::fs::read(&dict_path) {
            Ok(bytes) => {
                let name = desired_compression.dictionary_name.as_deref().unwrap_or("(unnamed)");
                let full_sha = {
                    use hex::ToHex;
                    use sha2::{Digest, Sha256};
                    let digest = Sha256::digest(&bytes);
                    let s: String = digest.encode_hex();
                    s
                };
                info!("loaded dict '{}' (sha {}...)", name, &full_sha[..12]);
                let arc_bytes = std::sync::Arc::from(bytes.into_boxed_slice());
                let arc_sha: std::sync::Arc<str> = full_sha.into();
                (arc_bytes, arc_sha)
            }
            Err(e) => {
                error!("Failed to read dictionary.zstd_dict: {}", e);
                std::process::exit(1);
            }
        }
    };

    // Detect available memory and compute budget
    let (detected_memory, cgroup_limit, total_budget, memory_budget) = {
        let detected = memory_budget::detect_available_memory();

        // Warn if detected memory is suspiciously low
        if let Ok(mem) = detected {
            const MIN_RECOMMENDED_MEMORY: u64 = 512 * 1024 * 1024; // 512 MB
            if mem < MIN_RECOMMENDED_MEMORY {
                tracing::warn!(
                    "Detected system memory is only {} MB (< 512 MB) - this may be too low for production use",
                    mem / (1024 * 1024)
                );
            }
        }

        let budget_result = server_config.compute_memory_budget(nbr_shards);
        let budget = match budget_result {
            Ok(b) => b,
            Err(e) => {
                error!("Memory budget calculation failed: {}", e);
                std::process::exit(1);
            }
        };

        // Get detection details for logging
        let physical_ram = std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|content| {
                content.lines()
                    .find(|line| line.starts_with("MemTotal:"))
                    .and_then(|line| {
                        line.strip_prefix("MemTotal:")
                            .and_then(|rest| rest.trim().split_whitespace().next())
                            .and_then(|kb| kb.parse::<u64>().ok())
                            .map(|kb| kb * 1024)
                    })
            });

        let cgroup = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
            .ok()
            .and_then(|content| {
                let trimmed = content.trim();
                if trimmed == "max" { None } else { trimmed.parse().ok() }
            });

        let detected_mem = detected.unwrap_or(0);
        let total = if let Some(explicit) = server_config.memory_budget_bytes {
            explicit
        } else {
            (detected_mem as f64 * (server_config.memory_consumption_percent as f64 / 100.0)) as u64
        };

        (physical_ram, cgroup, total, budget)
    };

    // Log memory plan
    if server_config.memory_budget_bytes.is_some() {
        info!("Using explicit memory budget (override)");
    } else if let Some(physical) = detected_memory {
        if let Some(cgroup) = cgroup_limit {
            let used = std::cmp::min(physical, cgroup);
            info!(
                "Detected memory: {:.1} GB (physical: {:.1} GB, cgroup limit: {:.1} GB, using: {:.1} GB)",
                physical as f64 / (1024.0 * 1024.0 * 1024.0),
                physical as f64 / (1024.0 * 1024.0 * 1024.0),
                cgroup as f64 / (1024.0 * 1024.0 * 1024.0),
                used as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        } else {
            info!(
                "Detected memory: {:.1} GB (physical RAM)",
                physical as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        }
    }

    let per_shard_budget = total_budget / nbr_shards as u64;
    if server_config.memory_budget_bytes.is_some() {
        info!(
            "Memory budget: {:.1} GB (explicit), {} shards -> {:.1} GB/shard",
            total_budget as f64 / (1024.0 * 1024.0 * 1024.0),
            nbr_shards,
            per_shard_budget as f64 / (1024.0 * 1024.0 * 1024.0)
        );
    } else {
        info!(
            "Memory budget: {:.1} GB ({}% of detected), {} shards -> {:.1} GB/shard",
            total_budget as f64 / (1024.0 * 1024.0 * 1024.0),
            server_config.memory_consumption_percent,
            nbr_shards,
            per_shard_budget as f64 / (1024.0 * 1024.0 * 1024.0)
        );
    }

    let pct = |bytes: u64| -> f64 {
        if per_shard_budget == 0 { 0.0 } else { bytes as f64 / per_shard_budget as f64 * 100.0 }
    };
    info!("Per-shard allocation:");
    info!("  recent_write_cache:          {:4} MB ({:.1}%)", memory_budget.recent_write_cache_bytes / (1024 * 1024), pct(memory_budget.recent_write_cache_bytes));
    info!("  aggregate_snapshots:         {:4} MB  ({:.1}%)", memory_budget.aggregate_snapshots_cache_bytes / (1024 * 1024), pct(memory_budget.aggregate_snapshots_cache_bytes));
    info!("  client_idempotency_snapshots:{:4} MB  ({:.1}%)", memory_budget.aggregate_client_snapshots_cache_bytes / (1024 * 1024), pct(memory_budget.aggregate_client_snapshots_cache_bytes));
    info!("  schema_cache:                {:4} MB  ({:.1}%)", memory_budget.schema_cache_bytes / (1024 * 1024), pct(memory_budget.schema_cache_bytes));

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

    // Validate TLS requirement for client auth (API keys and client identity)
    let requires_tls = api_keys.is_some() || server_config.require_client_identity;
    if requires_tls && tls_config.is_none() && !server_config.insecure_allow_plaintext_auth {
        error!(
            "Client authentication is configured but TLS is not enabled.\n\
             API keys are transmitted in cleartext without TLS. Signed identity nonces are \
             vulnerable to replay attacks within the 2-minute acceptance window.\n\
             \n\
             To fix, enable TLS:\n\
               --tls-ca-cert /path/to/ca.crt --tls-node-cert /path/to/node.crt --tls-node-key /path/to/node.key\n\
             \n\
             For development/testing only, you can bypass this check:\n\
               --insecure-allow-plaintext-auth\n\
             \n\
             WARNING: --insecure-allow-plaintext-auth disables transport security for auth.\n\
             Never use this flag in production."
        );
        std::process::exit(1);
    }

    if requires_tls && tls_config.is_none() && server_config.insecure_allow_plaintext_auth {
        tracing::warn!("Client authentication running WITHOUT TLS - vulnerable to replay attacks");
    }

    if api_keys.is_some() {
        info!("API key authentication enabled");
    }

    let shard_config = server_config.to_shard_config(node_id, nbr_shards, tls_config, api_keys, memory_budget, dict_bytes, dict_sha256);
    let sidecar_config = server_config.to_sidecar_config(nbr_shards, node_id);
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
