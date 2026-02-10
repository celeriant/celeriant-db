use std::time::Instant;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_shard::{internal_shard_config::InternalShardConfig, replication_client::GlommioReplicationClient, shard_wal::ShardWal};
use celeriant_sidecar::store::SidecarStoreTrait;
use glommio::{
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement,
    channels::channel_mesh::{Full, MeshBuilder},
    enclose, net::TcpListener,
};
use tracing::{error, info};

use crate::{sharded::{intrashard_messages::IntrashardMessages, shard::Shard}, sidecar::{sidecar_channels::{SidecarSenders, create_sidecar_channel}, sidecar_runtime::SidecarRuntime, sidecar_s3_uploader::SidecarS3Uploader}};

mod sharded;
mod sidecar;

pub use {sharded::shard_config::ShardConfig, sidecar::sidecar_config::SidecarConfig, sharded::routing_rule::RoutingRule, celeriant_wal::compression_type::CompressionType};

pub fn run_executors_and_sidecar<S: SidecarStoreTrait>(shard_config: ShardConfig, sidecar_config: SidecarConfig, mesh_channel_size: usize, node_id: u128, sidecar_store: S) {
    info!("Starting {} shard executors on node {}", shard_config.num_shards, node_id);

    let mesh =
        MeshBuilder::<IntrashardMessages, Full>::full(shard_config.num_shards as usize, mesh_channel_size);

    let (sidecar_senders, _sidecar_runtime) = match new_sidecar(sidecar_config, sidecar_store) {
        Ok(sidecar_handle) => sidecar_handle,
        Err(e) => {
            error!("Failed to initialise sidecar: {}", e);
            panic!("Cannot start server without sidecar");
        },
    };

    //TODO: Need a way to handle panics of shards & their restart. It's like a heartbeat

    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(shard_config.num_shards as usize, CpuSet::online().ok()))
        .on_all_shards(enclose!((mesh, shard_config, sidecar_senders) move || async move {
            
            let (sender, receivers) = mesh.join().await
                .expect("Failed to join mesh - cannot initialize shard");
            
            let client_bind_address = format!("{}:{}", shard_config.listen_address, shard_config.client_port);
            let client_tcp_listener = TcpListener::bind(&client_bind_address)
                .expect(&format!("Failed to bind client TCP listener to {} - cannot initialize shard", client_bind_address));

            let replication_bind_address = format!("{}:{}", shard_config.listen_address, shard_config.replication_port);
            let replication_tcp_listener = TcpListener::bind(&replication_bind_address)
                .expect(&format!("Failed to bind replication TCP listener to {} - cannot initialize shard", replication_bind_address));
            
            let current_shard_id = sender.peer_id();

            let shard_dir = shard_config.data_root.join(format!("shard_{current_shard_id}"));
            let internal_shard_config = InternalShardConfig { 
                shard_log_preallocate_bytes: shard_config.shard_log_preallocate_bytes, 
                node_id, 
                fsync_delay: shard_config.fsync_delay,
                replication_delay: shard_config.replication_delay,
                non_durable_writes: shard_config.non_durable_writes, 
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
            };
            let s3_uploader = SidecarS3Uploader::new(sidecar_senders.clone());
            let replication_client = GlommioReplicationClient::new(
                String::new(),
                shard_config.internode_connection_timeout,
                shard_config.max_request_size,
                shard_config.max_response_size,
                current_shard_id as u64,
                Some(s3_uploader),
            );

            let validated_node_status = ValidatedNodeStatus::new(shard_config.node_status, Instant::now());
            let filesystem = ShardWal::open(internal_shard_config, validated_node_status, replication_client).await
                .expect(&format!("Failed to initialize filesystem at {:?} - cannot initialize shard", shard_config.data_root));

            Shard::new(shard_config, current_shard_id, sender, receivers, sidecar_senders, client_tcp_listener, replication_tcp_listener, filesystem).run().await;

        }))
        .unwrap()
        .join_all();

    info!("Finished shutdown of {} shard executors on node {}", shard_config.num_shards, node_id);
}

pub fn new_sidecar<S: SidecarStoreTrait>(sidecar_config: SidecarConfig, sidecar_store: S) -> Result<(SidecarSenders, SidecarRuntime), String> {
    let (sidecar_senders, sidecar_receivers) = create_sidecar_channel(&sidecar_config);

    let sidecar_runtime = SidecarRuntime::with_store(sidecar_config, sidecar_receivers, sidecar_store)
        .map_err(|e| format!("Failed to sidecar runtime: {}", e))?;

    Ok((sidecar_senders, sidecar_runtime))
}