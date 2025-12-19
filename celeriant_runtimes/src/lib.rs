use celeriant_filesystem::shard_write_ahead_log::ShardWriteAheadLog;
use celeriant_sidecar::store::SidecarStoreTrait;
use glommio::{
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement,
    channels::channel_mesh::{Full, MeshBuilder},
    enclose, net::TcpListener,
};
use tracing::{error, info};

use crate::{sharded::{intrashard_messages::IntrashardMessages, shard::Shard}, sidecar::{sidecar_channels::{SidecarSenders, create_sidecar_channel}, sidecar_runtime::SidecarRuntime}};

mod sharded;
mod sidecar;

pub use {sharded::shard_config::ShardConfig, sidecar::sidecar_config::SidecarConfig, sharded::routing_rule::RoutingRule};

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

    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(shard_config.num_shards as usize, CpuSet::online().ok()))
        .on_all_shards(enclose!((mesh, shard_config, sidecar_senders) move || async move {
            
            let (sender, receivers) = mesh.join().await
                .expect("Failed to join mesh - cannot initialize shard");
            
            let tcp_listener = TcpListener::bind(&shard_config.listen_address)
                .expect(&format!("Failed to bind TCP listener to {} - cannot initialize shard", shard_config.listen_address));
            
            let current_shard_id = sender.peer_id();

            let shard_dir = shard_config.data_root.join(format!("shard_{current_shard_id}"));
            let internal_shard_config = celeriant_filesystem::internal_shard_config::InternalShardConfig { 
                shard_log_preallocate_bytes: shard_config.shard_log_preallocate_bytes, 
                node_id, 
                fsync_delay: shard_config.fsync_delay, 
                non_durable_writes: shard_config.non_durable_writes, 
                shard_dir,
                max_open_files: shard_config.max_open_files,
                recent_write_cache_bytes: shard_config.recent_write_cache_bytes,
            };
            let filesystem = ShardWriteAheadLog::new(internal_shard_config).await
                .expect(&format!("Failed to initialize filesystem at {:?} - cannot initialize shard", shard_config.data_root));

            Shard::new(shard_config, current_shard_id, sender, receivers, sidecar_senders, tcp_listener, filesystem).run().await;

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