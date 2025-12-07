use std::{
    cell::Cell,
    rc::Rc,
    time::Duration,
};

use celeriant_sidecar::request::Request;
use glommio::channels::channel_mesh::{Receivers, Senders};
use tracing::{error, info};

use crate::{
    sharded::{
        intrashard_messages::IntrashardMessages, shard_config::ShardConfig,
        signal_handler::SignalHandler,
    },
    sidecar::{
        sidecar_messages::{SidecarTarget},
        sidecar_channels::SidecarSenders,
    },
};

pub struct Shard {
    _config: ShardConfig,
    shard_id: usize,
    intrashard_sender: Rc<Senders<IntrashardMessages>>,
    intrashard_receivers: Receivers<IntrashardMessages>,
    sidecar_senders: SidecarSenders,
    shutdown_requested: Rc<Cell<bool>>,
}

impl Shard {
    pub fn new(
        config: ShardConfig,
        shard_id: usize,
        sender: Senders<IntrashardMessages>,
        receivers: Receivers<IntrashardMessages>,
        sidecar_senders: SidecarSenders,
    ) -> Self {
        info!("Initializing shard {shard_id}");
        Self {
            _config: config,
            shard_id,
            intrashard_sender: Rc::new(sender),
            intrashard_receivers: receivers,
            sidecar_senders,
            shutdown_requested: Rc::new(Cell::new(false)),
        }
    }

    fn spawn_shard_zero_background_handler(&mut self) {
        if self.shard_id != 0 {
            return;
        }

        let mut signal_handler =
            SignalHandler::new().expect("Failed to initialize signal handler");

        let sender_clone = self.intrashard_sender.clone();
        let shutdown_requested = self.shutdown_requested.clone();
        let shard_id = self.shard_id;

        glommio::spawn_local(async move {
            loop {
                match signal_handler.poll_signal() {
                    Ok(Some(sig)) => {
                        info!(
                            "Received shutdown signal ({:?}). Initiating graceful shutdown...",
                            sig
                        );
                        shutdown_requested.set(true);

                        // Broadcast shutdown to all other shards
                        for peer in 0..sender_clone.as_ref().nr_consumers() {
                            if peer != shard_id {
                                let shutdown_msg = IntrashardMessages::Shutdown;
                                if let Err(e) =
                                    sender_clone.as_ref().try_send_to(peer, shutdown_msg)
                                {
                                    error!(
                                        "Failed to send shutdown signal to shard {peer}: {e:?}"
                                    );
                                }
                            }
                        }

                        break;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!("Error polling for signals: {e}");
                        break;
                    }
                }

                glommio::timer::sleep(Duration::from_secs(1)).await;
            }
        })
        .detach();
    }

    fn spawn_intrashard_message_handler(&mut self) {
        // Set up receivers for messages from other shards
        for (_src_shard, stream) in self.intrashard_receivers.streams() {
            let shutdown_flag = self.shutdown_requested.clone();
            glommio::spawn_local(async move {
                while let Some(msg) = stream.recv().await {
                    match msg {
                        IntrashardMessages::Shutdown => {
                            shutdown_flag.set(true);
                        }
                    }
                }
            })
            .detach();
        }
    }

    async fn enter_main_loop_until_shutdown(&self) {
        loop {
            if self.shutdown_requested.get() {
                info!("Shard {} stopping acceptance of new connections due to shutdown", self.shard_id);
                break;
            }
            glommio::timer::sleep(Duration::from_secs(3)).await;
        }
    }

    pub async fn run(&mut self) {
        info!("Shard {} starting main loop", self.shard_id);

        self.spawn_shard_zero_background_handler();
        self.spawn_intrashard_message_handler();

        // Test sidecar request
        match self.sidecar_senders
            .send_async(
                SidecarTarget::ControlPlaneLease,
                Request::ObjectGet {
                    path: "foo".to_string(),
                },
            )
            .await
        {
            Ok(sidecar_response) => info!("Got response from sidecar: {:?}", sidecar_response),
            Err(sidecar_error) => error!("Got error from sidecar: {:?}", sidecar_error),
        }

        self.enter_main_loop_until_shutdown().await;

        info!("Shard {} shutdown complete", self.shard_id);
    }
}
