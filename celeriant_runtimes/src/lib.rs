use glommio::{
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement,
    channels::channel_mesh::{Full, MeshBuilder},
    enclose,
};
use tracing::info;

use crate::{intrashard_messages::IntrashardMessages, shard::Shard};

mod intrashard_messages;
mod shard;
mod shard_config;

pub use shard_config::ShardConfig;

pub fn run_executors_and_sidecar(shard_config: ShardConfig, mesh_channel_size: usize, node_id: u128) {
    info!("Starting {} shard executors on node {}", shard_config.num_shards, node_id);

    let mesh =
        MeshBuilder::<IntrashardMessages, Full>::full(shard_config.num_shards, mesh_channel_size);

    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(shard_config.num_shards, CpuSet::online().ok()))
        .on_all_shards(enclose!((mesh, shard_config) move || async move {
            let (sender, receivers) = mesh.join().await.unwrap();            
            Shard::new(shard_config, sender.peer_id(), sender, receivers).run().await;
        }))
        .unwrap()
        .join_all();

    info!("Finished shutdown of {} shard executors on node {}", shard_config.num_shards, node_id);
}
