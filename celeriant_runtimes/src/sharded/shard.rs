use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_shard::{replication_client::ReplicationClient, s3_downloader::S3Downloader, shard_wal::ShardWal};
use glommio::{
    channels::{
        channel_mesh::{Receivers, Senders},
        local_channel::LocalReceiver,
        shared_channel::ConnectedReceiver,
    },
    net::TcpListener,
};
use tracing::{error, info, warn};

use crate::{
    sharded::{
        connection_handler::{
            CatchupCompletionMsg, ConnectionContext, PortType, handle_enter_s3_catchup, handle_new_connection, handle_redirected_connection,
        },
        intrashard_messages::IntrashardMessages,
        shard_config::ShardConfig,
        signal_handler::SignalHandler,
    },
    sidecar::sidecar_channels::SidecarSenders,
};

pub struct Shard<R: ReplicationClient + 'static, D: S3Downloader + 'static> {
    intrashard_receivers: Receivers<IntrashardMessages>,
    client_tcp_listener: Rc<TcpListener>,
    replication_tcp_listener: Rc<TcpListener>,
    ctx: ConnectionContext<R, D>,
    shutdown_requested: Rc<Cell<bool>>,
    shard_wal: Rc<ShardWal<R, D>>,
}

impl<R: ReplicationClient + 'static, D: S3Downloader + 'static> Shard<R, D> {
    pub fn new(
        config: ShardConfig,
        current_shard_id: usize,
        sender: Senders<IntrashardMessages>,
        receivers: Receivers<IntrashardMessages>,
        _sidecar_senders: SidecarSenders,
        client_tcp_listener: TcpListener,
        replication_tcp_listener: TcpListener,
        shard_wal: ShardWal<R, D>,
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
            catchup_completion_tx: None,
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

        let shard_0_distributed_mode = self.ctx.current_shard_id == 0 && !self.shard_wal.node_status.get().raw().is_standalone();

        let rx = if shard_0_distributed_mode {
            let (tx, rx) = glommio::channels::local_channel::new_unbounded();
            self.ctx.catchup_completion_tx = Some(Rc::new(tx));
            Some(rx)
        } else {
            None
        };

        for (_src_shard, stream) in self.intrashard_receivers.streams() {
            spawn_intrashard_message_handler(stream, self.ctx.clone());
        }

        if let Some(rx) = rx {
            spawn_boot_orchestrator(self.ctx.clone(), rx);
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
        })
        .detach();

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
        })
        .detach();

        loop {
            if self.shutdown_requested.get() {
                let _ = self.shard_wal.close().await;
                break;
            }
            glommio::timer::sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn broadcast_message_to_other_shards(current_shard_id: usize, message: IntrashardMessages, senders: Rc<Senders<IntrashardMessages>>) {
    for peer in 0..senders.as_ref().nr_consumers() {
        if peer == current_shard_id {
            continue;
        }
        if let Err(e) = senders.as_ref().send_to(peer, message.clone()).await {
            error!("Failed to send shutdown signal to shard {peer}: {e:?}");
        }
    }
}

fn spawn_shard_zero_shutdown_handler<R: ReplicationClient + 'static, D: S3Downloader + 'static>(ctx: ConnectionContext<R, D>) {
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
                    broadcast_message_to_other_shards(ctx.current_shard_id, IntrashardMessages::Shutdown, ctx.intrashard_sender.clone()).await;
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

fn spawn_boot_orchestrator<R: ReplicationClient + 'static, D: S3Downloader + 'static>(
    ctx: ConnectionContext<R, D>,
    rx: LocalReceiver<CatchupCompletionMsg>,
) {
    glommio::spawn_local(async move {
        let shard_count = ctx.config.num_shards as usize;

        loop {
            for peer in 1..shard_count {
                let _ = ctx.intrashard_sender.send_to(peer, IntrashardMessages::EnterS3Catchup).await;
            }

            let mut results = vec![];

            // Shard0 is NOT part of ctx.intrashard_sender so kick it off explicitly
            let shard0_result = ctx.shard_wal.enter_s3_catchup().await;
            results.push(CatchupCompletionMsg { shard_id: 0, result: shard0_result });

            let mut remaining = shard_count - 1;
            while remaining > 0 {
                match rx.recv().await {
                    Some(msg) => {
                        results.push(msg);
                        remaining -= 1;
                    }
                    None => break,
                }
            }

            let mut has_retriable = false;
            let mut has_fatal = false;

            for msg in &results {
                match &msg.result {
                    Ok(_) => {}
                    Err(e) if e.is_retriable() => {
                        warn!(shard_id = msg.shard_id, error = ?e, "S3 catchup retriable error, will retry");
                        has_retriable = true;
                    }
                    Err(e) => {
                        error!(shard_id = msg.shard_id, error = ?e, "S3 catchup fatal error, shutting down");
                        has_fatal = true;
                    }
                }
            }

            if has_fatal {
                ctx.shutdown_requested.set(true);
                broadcast_message_to_other_shards(ctx.current_shard_id, IntrashardMessages::Shutdown, ctx.intrashard_sender.clone()).await;
                return;
            }

            if !has_retriable {
                break;
            }

            glommio::timer::sleep(Duration::from_secs(5)).await;
        }

        info!("All shards caught up, ready for S3 election");

        ctx.shard_wal.node_status.set(ValidatedNodeStatus::new(
            celeriant_distributed::node_status::NodeStatus::Leader { lease_index: 1 }, 
            Instant::now() + Duration::from_hours(1)));

        for peer in 1..shard_count {
            let _ = ctx
                .intrashard_sender
                .send_to(
                    peer,
                    IntrashardMessages::StatusUpdate {
                        status: celeriant_distributed::node_status::NodeStatus::Leader { lease_index: 1 },
                        valid_until: Instant::now() + Duration::from_hours(1),
                    },
                )
                .await;
        }
    })
    .detach();
}

fn spawn_intrashard_message_handler<R: ReplicationClient + 'static, D: S3Downloader + 'static>(
    stream: ConnectedReceiver<IntrashardMessages>,
    ctx: ConnectionContext<R, D>,
) {
    glommio::spawn_local(async move {
        while let Some(msg) = stream.recv().await {
            handle_intrashard_message(msg, &ctx);
        }
    })
    .detach();
}

fn handle_intrashard_message<R: ReplicationClient + 'static, D: S3Downloader + 'static>(msg: IntrashardMessages, ctx: &ConnectionContext<R, D>) {
    match msg {
        IntrashardMessages::Shutdown => {
            ctx.shutdown_requested.set(true);
        }
        IntrashardMessages::ConnectionRedirect {
            accepted_tcp_stream,
            request,
            message_version,
            port_type,
        } => {
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
        IntrashardMessages::EnterS3Catchup => handle_enter_s3_catchup(ctx.clone()),
        IntrashardMessages::S3CatchupComplete { shard_id, result } => {
            if let Some(tx) = &ctx.catchup_completion_tx {
                let _ = tx.try_send(CatchupCompletionMsg { result, shard_id });
            }
        }
        IntrashardMessages::StatusUpdate { status, valid_until } => {
            ctx.shard_wal.node_status.set(ValidatedNodeStatus::new(status, valid_until));
        }
    }
}
