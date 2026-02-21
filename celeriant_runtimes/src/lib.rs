use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::Duration;

use celeriant_distributed::lease_manager::LeaseManager;
use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_shard::{internal_shard_config::InternalShardConfig, replication_client::FollowerConnection, shard_wal::ShardWal};
use celeriant_sidecar::store::SidecarStoreTrait;
use glommio::{
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement,
    channels::channel_mesh::{Full, MeshBuilder},
    enclose, net::TcpListener,
};
use tracing::{error, info};

use crate::{sharded::{intrashard_messages::IntrashardMessages, shard::Shard}, sidecar::{sidecar_channels::{SidecarSenders, create_sidecar_channel}, sidecar_lease_storage::SidecarLeaseStorage, sidecar_runtime::SidecarRuntime, sidecar_s3_downloader::SidecarS3Downloader, sidecar_s3_uploader::SidecarS3Uploader}};

mod sharded;
mod sidecar;

pub use {sharded::shard_config::ShardConfig, sidecar::sidecar_config::SidecarConfig, sharded::routing_rule::RoutingRule, celeriant_wal::compression_type::CompressionType};

const MAX_SHARD_RESTARTS: u32 = 3;
const SHARD_RESTART_DELAY: Duration = Duration::from_secs(5);
const RESTART_BUDGET_RESET: Duration = Duration::from_secs(600);

pub fn run_executors_and_sidecar<S: SidecarStoreTrait>(shard_config: ShardConfig, sidecar_config: SidecarConfig, mesh_channel_size: usize, node_id: u128, sidecar_store: S) {
    info!("Starting {} shard executors on node {}", shard_config.num_shards, node_id);

    let (sidecar_senders, _sidecar_runtime) = match new_sidecar(sidecar_config, sidecar_store) {
        Ok(sidecar_handle) => sidecar_handle,
        Err(e) => {
            error!("Failed to initialise sidecar: {}", e);
            panic!("Cannot start server without sidecar");
        },
    };

    let num_shards = shard_config.num_shards as usize;
    let mut restart_count = 0u32;
    let shard_failed = Arc::new(AtomicBool::new(false));

    let hook_flag = shard_failed.clone();
    std::panic::set_hook(Box::new(move |info| {
        error!("PANIC: {}", info);
        hook_flag.store(true, Ordering::SeqCst);
    }));

    loop {
        shard_failed.store(false, Ordering::SeqCst);
        let started_at = std::time::Instant::now();
        let mesh = MeshBuilder::<IntrashardMessages, Full>::full(num_shards, mesh_channel_size);

        let results = LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(num_shards, CpuSet::online().ok()))
            .on_all_shards(enclose!((mesh, shard_config, sidecar_senders, shard_failed) move || async move {

                let (sender, receivers) = mesh.join().await
                    .expect("Failed to join mesh - cannot initialize shard");

                let current_shard_id = sender.peer_id();
                info!(shard_id = current_shard_id, "Shard executor started, binding listeners");

                let client_bind_address = format!("{}:{}", shard_config.listen_address, shard_config.client_port);
                let client_tcp_listener = TcpListener::bind(&client_bind_address)
                    .expect(&format!("Failed to bind client TCP listener to {} - cannot initialize shard", client_bind_address));

                let replication_bind_address = format!("{}:{}", shard_config.listen_address, shard_config.replication_port);
                let replication_tcp_listener = TcpListener::bind(&replication_bind_address)
                    .expect(&format!("Failed to bind replication TCP listener to {} - cannot initialize shard", replication_bind_address));

                let shard_dir = shard_config.data_root.join(format!("shard_{current_shard_id}"));
                let internal_shard_config = InternalShardConfig {
                    shard_log_preallocate_bytes: shard_config.shard_log_preallocate_bytes,
                    node_id,
                    shard_id: current_shard_id as u32,
                    s3_download_max_rounds: shard_config.s3_download_max_rounds,
                    fsync_delay: shard_config.fsync_delay,
                    replication_delay: shard_config.replication_delay,
                    shard_dir,
                    max_open_files: shard_config.max_open_files,
                    recent_write_cache_bytes: shard_config.recent_write_cache_bytes,
                    max_response_size: shard_config.max_response_size,
                    max_request_size: shard_config.max_request_size,
                    aggregate_client_snapshots_cache_bytes: shard_config.aggregate_client_snapshots_cache_bytes,
                    aggregate_snapshots_cache_bytes: shard_config.aggregate_snapshots_cache_bytes,
                    read_max_chunk_size: shard_config.read_max_chunk_size,
                    timestamp_config: shard_config.timestamp_config,
                    list_max_duration: shard_config.list_max_duration,
                    list_page_size: shard_config.list_page_size,
                    list_wal_index_cache_bytes: shard_config.list_wal_index_cache_bytes,
                    pending_replication_high_water_bytes: shard_config.pending_replication_high_water_bytes,
                    max_cluster_time_drift_ms: shard_config.max_cluster_time_drift_ms,
                    max_catchup_gap_bytes: shard_config.max_catchup_gap_bytes,
                    max_s3_fallback_batch_bytes: shard_config.max_s3_fallback_batch_bytes,
                };
                let s3_uploader = SidecarS3Uploader::new(sidecar_senders.clone());
                let replication_client = FollowerConnection::new(
                    None,
                    shard_config.internode_connection_timeout,
                    Some(shard_config.internode_request_timeout),
                    shard_config.max_request_size,
                    shard_config.max_response_size,
                    current_shard_id as u64,
                    Some(s3_uploader),
                );
                let s3_downloader = SidecarS3Downloader::new(sidecar_senders.clone());

                let validated_node_status = if shard_config.replication_config.is_some() {
                    ValidatedNodeStatus::boot_catchup()
                } else {
                    ValidatedNodeStatus::standalone()
                };
                info!(shard_id = current_shard_id, "Opening WAL");
                let filesystem = match ShardWal::open(internal_shard_config, validated_node_status, replication_client, s3_downloader).await {
                    Ok(wal) => {
                        info!(shard_id = current_shard_id, "WAL opened successfully");
                        wal
                    }
                    Err(e) => {
                        error!(shard_id = current_shard_id, error = ?e, "Failed to open WAL");
                        panic!("Failed to initialize filesystem at {:?}: {:?}", shard_config.data_root, e);
                    }
                };

                let lease_manager = if shard_config.replication_config.is_some() && current_shard_id == 0 {
                    let lease_storage = SidecarLeaseStorage::new(sidecar_senders.clone());
                    let replication_config = shard_config.replication_config.clone().unwrap();
                    Some(LeaseManager::new(lease_storage, replication_config))
                } else {
                    None
                };

                Shard::new(shard_config, current_shard_id, sender, receivers, client_tcp_listener, replication_tcp_listener, filesystem, lease_manager, shard_failed).run().await;

            }))
            .expect("Failed to spawn shard executor threads")
            .join_all();

        let thread_panicked = results.iter().any(Result::is_err);
        let task_panicked = shard_failed.load(Ordering::SeqCst);

        if !thread_panicked && !task_panicked {
            break;
        }

        for (i, result) in results.iter().enumerate() {
            if let Err(e) = result {
                error!(shard_id = i, error = %e, "Shard executor thread panicked");
            }
        }

        if started_at.elapsed() > RESTART_BUDGET_RESET {
            restart_count = 0;
        }
        restart_count += 1;
        if restart_count > MAX_SHARD_RESTARTS {
            error!("Shard executors panicked {} times, exceeded max restarts ({}). Shutting down.", restart_count, MAX_SHARD_RESTARTS);
            std::process::exit(1);
        }

        error!("Shard executor panic detected. Restarting all shards (attempt {}/{})", restart_count, MAX_SHARD_RESTARTS);
        std::thread::sleep(SHARD_RESTART_DELAY);
    }

    info!("Finished shutdown of {} shard executors on node {}", shard_config.num_shards, node_id);
}

pub fn new_sidecar<S: SidecarStoreTrait>(sidecar_config: SidecarConfig, sidecar_store: S) -> Result<(SidecarSenders, SidecarRuntime), String> {
    let (sidecar_senders, sidecar_receivers) = create_sidecar_channel(&sidecar_config);

    let sidecar_runtime = SidecarRuntime::with_store(sidecar_config, sidecar_receivers, sidecar_store)
        .map_err(|e| format!("Failed to sidecar runtime: {}", e))?;

    Ok((sidecar_senders, sidecar_runtime))
}