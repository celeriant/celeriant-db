use glommio::{
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement,
    channels::channel_mesh::{Full, MeshBuilder},
    enclose,
};
use tracing::{error, info};

use crate::{sharded::{intrashard_messages::IntrashardMessages, shard::Shard}, sidecar::sidecar_handle::SidecarHandle};

mod sharded;
mod sidecar;

pub use {sharded::shard_config::ShardConfig, sidecar::sidecar_config::SidecarConfig, sidecar::s3_config::S3Config, sidecar::object_store_retry_config::ObjectStoreRetryConfig};

pub fn run_executors_and_sidecar(shard_config: ShardConfig, sidecar_config: SidecarConfig, mesh_channel_size: usize, node_id: u128) {
    info!("Starting {} shard executors on node {}", shard_config.num_shards, node_id);

    let mesh =
        MeshBuilder::<IntrashardMessages, Full>::full(shard_config.num_shards, mesh_channel_size);

    let sidecar_handle: Option<SidecarHandle> = match SidecarHandle::new(sidecar_config) {
        Ok(sidecar_handle) => sidecar_handle,
        Err(e) => {
            error!("Failed to initialise sidecar: {}", e);
            return;
        },
    };

    let sidecar_senders = sidecar_handle.as_ref().map(|h| h.sidecar_senders.clone());

    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(shard_config.num_shards, CpuSet::online().ok()))
        .on_all_shards(enclose!((mesh, shard_config, sidecar_senders) move || async move {
            let (sender, receivers) = mesh.join().await.unwrap();            
            Shard::new(shard_config, sender.peer_id(), sender, receivers, sidecar_senders).run().await;
        }))
        .unwrap()
        .join_all();

    info!("Finished shutdown of {} shard executors on node {}", shard_config.num_shards, node_id);
}
