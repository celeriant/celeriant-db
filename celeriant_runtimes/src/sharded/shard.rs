use std::time::{Duration, Instant};

use glommio::channels::channel_mesh::{Receivers, Senders};
use tracing::{error, info};

use crate::{sharded::{intrashard_messages::IntrashardMessages, shard_config::ShardConfig}, sidecar::{sidecar_messages::{SidecarOperation, SidecarTarget}, sidecar_senders::SidecarSenders}};


pub struct Shard {
    config: ShardConfig,
    shard_id: usize,
    intrashard_sender: Senders<IntrashardMessages>,
    intrashard_receivers: Receivers<IntrashardMessages>,
    sidecar_sender: Option<SidecarSenders>
}

impl Shard {
    pub fn new(
        config: ShardConfig,
        shard_id: usize,
        sender: Senders<IntrashardMessages>,
        receivers: Receivers<IntrashardMessages>,
        sidecar_shard_handle: Option<SidecarSenders>
    ) -> Self {
        info!("Initializing shard {shard_id}");
        Self {
            config,
            shard_id,
            intrashard_sender: sender,
            intrashard_receivers: receivers,
            sidecar_sender: sidecar_shard_handle
        }
    }

    pub async fn run(self) {
        info!("Shard {} starting main loop", self.shard_id);
        
        // TODO: Spawn tasks for:
        // - TCP listener (maybe only on shard 0?)
        // - Processing mesh messages from other shards
        // - Periodic flush timer
        // - etc.

        if let Some(sidecar_sender) = self.sidecar_sender {
            match sidecar_sender.send_request(
                SidecarTarget::ControlPlaneLease, 
                SidecarOperation::ObjectGet { path: "foo".to_string() }, 
                Instant::now() + Duration::from_millis(100)
            ).await {
                Ok(sidecar_response) => info!("Got response from sidecar: {:?}", sidecar_response),
                Err(sidecar_error) => error!("Got error from sidecar: {:?}", sidecar_error),
            }
        }

        info!("Shard {} shutdown complete", self.shard_id);
    }
}
