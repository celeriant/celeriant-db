use std::{cell::Cell, rc::Rc, time::Duration};

use celeriant_shard::{replication_client::ReplicationClient, shard_wal::ShardWal};
use glommio::{
    channels::{
        channel_mesh::{Receivers, Senders},
        shared_channel::ConnectedReceiver,
    },
    net::TcpListener,
};
use tracing::{error, info};

use crate::{
    sharded::{
        connection_handler::{handle_new_connection, handle_redirected_connection, ConnectionContext, PortType},
        intrashard_messages::IntrashardMessages,
        shard_config::ShardConfig,
        signal_handler::SignalHandler,
    },
    sidecar::sidecar_channels::SidecarSenders,
};

pub struct Shard<R: ReplicationClient + 'static> {
    intrashard_receivers: Receivers<IntrashardMessages>,
    client_tcp_listener: Rc<TcpListener>,
    replication_tcp_listener: Rc<TcpListener>,
    ctx: ConnectionContext<R>,
    shutdown_requested: Rc<Cell<bool>>,
    shard_wal: Rc<ShardWal<R>>,
}

impl<R: ReplicationClient + 'static> Shard<R> {
    pub fn new(
        config: ShardConfig,
        current_shard_id: usize,
        sender: Senders<IntrashardMessages>,
        receivers: Receivers<IntrashardMessages>,
        _sidecar_senders: SidecarSenders,
        client_tcp_listener: TcpListener,
        replication_tcp_listener: TcpListener,
        shard_wal: ShardWal<R>,
    ) -> Self {
        info!("Initializing shard {current_shard_id}");

        let shutdown_requested = Rc::new(Cell::new(false));
        let shard_wal = Rc::new(shard_wal);

        let ctx = ConnectionContext {
            config: Rc::new(config),
            current_shard_id,
            intrashard_sender: Rc::new(sender),
            shutdown_requested: shutdown_requested.clone(),
            shard_wal: shard_wal.clone(),
        };

        Self {
            intrashard_receivers: receivers,
            client_tcp_listener: Rc::new(client_tcp_listener),
            replication_tcp_listener: Rc::new(replication_tcp_listener),
            ctx,
            shutdown_requested,
            shard_wal,
        }
    }

    pub async fn run(&mut self) {
        spawn_shard_zero_shutdown_handler(self.ctx.clone());

        for (_src_shard, stream) in self.intrashard_receivers.streams() {
            spawn_intrashard_message_handler(stream, self.ctx.clone());
        }

        self.enter_main_loop_until_shutdown().await;

        info!("Shard {} shutdown complete", self.ctx.current_shard_id);
    }

    async fn enter_main_loop_until_shutdown(&self) {
        let client_listener = self.client_tcp_listener.clone();
        let client_ctx = self.ctx.clone();
        glommio::spawn_local(async move {
            loop {
                if client_ctx.shutdown_requested.get() {
                    break;
                }
                match glommio::timer::timeout(Duration::from_secs(1), client_listener.shared_accept()).await {
                    Ok(stream) => handle_new_connection(stream.bind_to_executor(), client_ctx.clone(), PortType::Client),
                    Err(_) => {}
                }
            }
        }).detach();

        let repl_listener = self.replication_tcp_listener.clone();
        let repl_ctx = self.ctx.clone();
        glommio::spawn_local(async move {
            loop {
                if repl_ctx.shutdown_requested.get() {
                    break;
                }
                match glommio::timer::timeout(Duration::from_secs(1), repl_listener.shared_accept()).await {
                    Ok(stream) => handle_new_connection(stream.bind_to_executor(), repl_ctx.clone(), PortType::Replication),
                    Err(_) => {}
                }
            }
        }).detach();

        loop {
            if self.shutdown_requested.get() {
                let _ = self.shard_wal.close().await;
                break;
            }
            glommio::timer::sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn broadcast_message_to_other_shards(
    current_shard_id: usize,
    message: IntrashardMessages,
    senders: Rc<Senders<IntrashardMessages>>,
) {
    for peer in 0..senders.as_ref().nr_consumers() {
        if peer == current_shard_id {
            continue;
        }
        if let Err(e) = senders.as_ref().send_to(peer, message.clone()).await {
            error!("Failed to send shutdown signal to shard {peer}: {e:?}");
        }
    }
}

fn spawn_shard_zero_shutdown_handler<R: ReplicationClient + 'static>(ctx: ConnectionContext<R>) {
    if ctx.current_shard_id != 0 {
        return;
    }

    let mut signal_handler = SignalHandler::new().expect("Failed to initialize signal handler");

    glommio::spawn_local(async move {
        loop {
            match signal_handler.poll_signal() {
                Ok(Some(sig)) => {
                    info!("Received shutdown signal ({:?}). Initiating graceful shutdown...", sig);
                    ctx.shutdown_requested.set(true);
                    broadcast_message_to_other_shards(
                        ctx.current_shard_id,
                        IntrashardMessages::Shutdown,
                        ctx.intrashard_sender.clone(),
                    ).await;
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
    }).detach();
}

fn spawn_intrashard_message_handler<R: ReplicationClient + 'static>(stream: ConnectedReceiver<IntrashardMessages>, ctx: ConnectionContext<R>) {
    glommio::spawn_local(async move {
        while let Some(msg) = stream.recv().await {
            handle_intrashard_message(msg, &ctx);
        }
    }).detach();
}

fn handle_intrashard_message<R: ReplicationClient + 'static>(msg: IntrashardMessages, ctx: &ConnectionContext<R>) {
    match msg {
        IntrashardMessages::Shutdown => {
            ctx.shutdown_requested.set(true);
        }
        IntrashardMessages::ConnectionRedirect { accepted_tcp_stream, request, message_version, port_type } => {
            handle_redirected_connection(
                accepted_tcp_stream.bind_to_executor(),
                request,
                ctx.config.max_request_size,
                ctx.config.max_response_size,
                ctx.config.server_compression_algorithm,
                message_version,
                ctx.clone(),
                port_type,
            );
        }
    }
}
