use glommio::channels::channel_mesh::{Receivers, Senders};
use tracing::info;

use crate::{intrashard_messages::IntrashardMessages, shard_config::ShardConfig};


pub struct Shard {
    pub config: ShardConfig,
    pub shard_id: usize,
    pub sender: Senders<IntrashardMessages>,
    pub receivers: Receivers<IntrashardMessages>,
}

impl Shard {
    pub fn new(
        config: ShardConfig,
        shard_id: usize,
        sender: Senders<IntrashardMessages>,
        receivers: Receivers<IntrashardMessages>,
    ) -> Self {
        info!("Initializing shard {shard_id}");
        Self {
            config,
            shard_id,
            sender,
            receivers,
        }
    }

    pub async fn run(self) {
        info!("Shard {} starting main loop", self.shard_id);
        
        // TODO: Spawn tasks for:
        // - TCP listener (maybe only on shard 0?)
        // - Processing mesh messages from other shards
        // - Periodic flush timer
        // - etc.

        info!("Shard {} shutdown complete", self.shard_id);
    }
}
