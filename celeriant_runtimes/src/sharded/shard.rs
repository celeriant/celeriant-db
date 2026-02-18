use std::{
    cell::Cell,
    rc::Rc,
    time::Duration,
};

use celeriant_disk::files::rwlock_timeout::write_with_timeout;
use celeriant_distributed::{heartbeat::now_ms, lease_manager::{ElectionOutcome, LeaseManager}, lease_store::LeaseStore, node_status::NodeStatus, validated_node_status::ValidatedNodeStatus};
use celeriant_msg::response::responses::HeartbeatResult;
use celeriant_shard::{replication_client::ReplicationClient, s3_downloader::S3Downloader, shard_wal::ShardWal};
use glommio::{
    channels::{
        channel_mesh::{Receivers, Senders},
        local_channel::LocalReceiver,
        shared_channel::ConnectedReceiver,
    },
    net::TcpListener,
};
use tracing::{debug, error, info, warn};

use crate::sharded::{
    connection_handler::{
        CatchupCompletionMsg, ConnectionContext, PortType, handle_enter_s3_catchup, handle_new_connection, handle_redirected_connection,
    },
    intrashard_messages::IntrashardMessages,
    shard_config::ShardConfig,
    signal_handler::SignalHandler,
};

pub struct Shard<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static> {
    intrashard_receivers: Receivers<IntrashardMessages>,
    client_tcp_listener: Rc<TcpListener>,
    replication_tcp_listener: Rc<TcpListener>,
    ctx: ConnectionContext<R, D, S>,
    shutdown_requested: Rc<Cell<bool>>,
    shard_wal: Rc<ShardWal<R, D>>,
}

impl<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static> Shard<R, D, S> {
    pub fn new(
        config: ShardConfig,
        current_shard_id: usize,
        sender: Senders<IntrashardMessages>,
        receivers: Receivers<IntrashardMessages>,
        client_tcp_listener: TcpListener,
        replication_tcp_listener: TcpListener,
        shard_wal: ShardWal<R, D>,
        lease_manager: Option<LeaseManager<S>>,
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
            lease_manager: lease_manager.map(Rc::new),
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

        let rx = if self.ctx.lease_manager.is_some() {
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

fn spawn_shard_zero_shutdown_handler<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(ctx: ConnectionContext<R, D, S>) {
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

/// Returns true if all shards caught up successfully, false if shutdown was triggered.
async fn run_s3_catchup<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: &ConnectionContext<R, D, S>,
    rx: &LocalReceiver<CatchupCompletionMsg>,
) -> bool {
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
            return false;
        }

        if !has_retriable {
            return true;
        }

        glommio::timer::sleep(Duration::from_secs(5)).await;
    }
}

async fn update_follower_address<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: &ConnectionContext<R, D, S>,
    address: Option<String>,
) {
    if let Ok(mut guard) = write_with_timeout(&ctx.shard_wal.replication_client, "set_follower_address").await {
        guard.set_follower_address(address.clone());
    } else {
        warn!("Failed to acquire replication client lock for follower address update");
    }
    broadcast_message_to_other_shards(ctx.current_shard_id, IntrashardMessages::UpdateFollower { replication_address: address }, ctx.intrashard_sender.clone()).await;
}

/// Renew leadership via S3 CAS when heartbeat path is unavailable.
/// CAS-promotes the existing lease for a fresh TTL, updates node_status,
/// and broadcasts to all local shards.
async fn renew_s3_lease_and_broadcast<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    lease_manager: &LeaseManager<S>,
    ctx: &ConnectionContext<R, D, S>,
) -> Result<ElectionOutcome, celeriant_distributed::lease_store::LeaseStoreError> {
    let outcome = lease_manager.run_election().await?;
    ctx.shard_wal.node_status.set(outcome.status);
    broadcast_message_to_other_shards(
        ctx.current_shard_id,
        IntrashardMessages::StatusUpdate { status: outcome.status },
        ctx.intrashard_sender.clone(),
    ).await;
    Ok(outcome)
}

/// Check if the follower in membership differs from the current peer.
/// Returns Some(new_peer) if changed, None if same or lookup failed.
async fn check_follower_changed<S: LeaseStore>(
    lease_manager: &LeaseManager<S>,
    current_peer: &celeriant_wal::s3::membership::NodeInfo,
) -> Option<celeriant_wal::s3::membership::NodeInfo> {
    match lease_manager.discover_peer().await {
        Ok(Some(peer)) if peer != *current_peer => {
            info!(old = ?current_peer, new = ?peer, "Follower address changed in membership");
            Some(peer)
        }
        _ => None,
    }
}

fn spawn_boot_orchestrator<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: ConnectionContext<R, D, S>,
    rx: LocalReceiver<CatchupCompletionMsg>,
) {
    let lease_manager = ctx.lease_manager.clone().unwrap();
    glommio::spawn_local(async move {

        info!("Starting pre-election S3 catchup");
        if !run_s3_catchup(&ctx, &rx).await {
            return;
        }

        info!("All shards caught up, running election");

        if let Err(e) = lease_manager.register_self().await {
            error!(error = ?e, "Failed to register node in membership, shutting down");
            ctx.shutdown_requested.set(true);
            broadcast_message_to_other_shards(ctx.current_shard_id, IntrashardMessages::Shutdown, ctx.intrashard_sender.clone()).await;
            return;
        }

        let outcome = match lease_manager.run_election().await {
            Ok(outcome) => outcome,
            Err(e) => {
                error!(error = ?e, "Election failed, shutting down");
                ctx.shutdown_requested.set(true);
                broadcast_message_to_other_shards(ctx.current_shard_id, IntrashardMessages::Shutdown, ctx.intrashard_sender.clone()).await;
                return;
            }
        };

        info!(status = ?outcome.status, peer = ?outcome.peer_info, "Election complete. Starting post-election S3 catchup");

        if !run_s3_catchup(&ctx, &rx).await {
            return;
        }

        ctx.shard_wal.node_status.set(outcome.status);
        broadcast_message_to_other_shards(ctx.current_shard_id, IntrashardMessages::StatusUpdate { status: outcome.status }, ctx.intrashard_sender.clone()).await;

        let mut initial_peer = outcome.peer_info;

        loop {
            let status = ctx.shard_wal.node_status.get().raw();
            if status.is_leader() {
                run_leader_loop(&lease_manager, &ctx, initial_peer.take()).await;
                update_follower_address(&ctx, None).await;
            } else if matches!(status, NodeStatus::FollowerCatchingUp { .. }) {
                run_kick_catchup(&ctx, &rx).await;
            } else {
                run_follower_watchdog(&lease_manager, &ctx).await;
            }
            if ctx.shutdown_requested.get() {
                break;
            }
        }
    })
    .detach();
}

/// Leader steady-state: discover follower, heartbeat, renew S3 lease on failure.
/// Returns when leadership is lost (status already set to Follower by renew_s3_lease_and_broadcast).
async fn run_leader_loop<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    lease_manager: &Rc<LeaseManager<S>>,
    ctx: &ConnectionContext<R, D, S>,
    initial_peer: Option<celeriant_wal::s3::membership::NodeInfo>,
) {
    let rc = ctx.config.replication_config.as_ref().unwrap();
    let heartbeat_interval = rc.heartbeat_interval;
    let status_ttl_ms = rc.status_ttl_ms();

    let mut known_peer = initial_peer;

    loop {
        // Phase 1: discover a follower
        // Each iteration renews the S3 lease (prevents self-fencing during
        // extended discovery) and discovers the peer in one shot —
        // run_election() calls discover_peer() internally.
        // Backoff is capped at half the S3 lease TTL so renewal always
        // lands with headroom before the lease expires.
        let peer_info = match known_peer.take() {
            Some(info) => info,
            None => {
                let max_backoff = rc.initial_lease_duration / 2;
                let mut backoff = Duration::from_secs(1).min(max_backoff);
                loop {
                    glommio::timer::sleep(backoff).await;
                    match renew_s3_lease_and_broadcast(lease_manager, ctx).await {
                        Ok(outcome) if !outcome.status.raw().is_leader() => {
                            warn!("Lost leadership during follower discovery");
                            return;
                        }
                        Ok(outcome) => {
                            if let Some(info) = outcome.peer_info {
                                info!(peer = ?info, "Follower discovered");
                                break info;
                            }
                            debug!("Follower not yet registered, retrying");
                            update_follower_address(ctx, None).await;
                            backoff = (backoff * 2).min(max_backoff);
                        }
                        Err(e) => {
                            warn!(error = ?e, "S3 lease renewal failed during discovery, retrying");
                            backoff = (backoff * 2).min(max_backoff);
                        }
                    }
                }
            }
        };

        update_follower_address(ctx, Some(peer_info.replication_address.clone())).await;

        // Phase 2: heartbeat loop
        // On failure: renew lease via S3, check if follower changed in membership.
        // If changed, set known_peer and break to re-discover. Otherwise keep
        // trying — transient failures don't mean the follower is gone.
        loop {
            glommio::timer::sleep(heartbeat_interval).await;

            let result = match write_with_timeout(&ctx.shard_wal.replication_client, "send_heartbeat").await {
                Ok(mut guard) => guard.send_heartbeat().await,
                Err(_) => {
                    warn!("Replication client lock contention, skipping heartbeat");
                    continue;
                }
            };

            if let Ok(HeartbeatResult::Ack { .. }) = result {
                let leader_ms = now_ms();
                let current_status = ctx.shard_wal.node_status.get().raw();
                let refreshed = ValidatedNodeStatus::new(current_status, leader_ms + status_ttl_ms);
                ctx.shard_wal.node_status.set(refreshed);
                broadcast_message_to_other_shards(ctx.current_shard_id, IntrashardMessages::StatusUpdate { status: refreshed }, ctx.intrashard_sender.clone()).await;
                continue;
            }

            warn!(result = ?result, "Heartbeat unsuccessful, renewing lease via S3");
            match renew_s3_lease_and_broadcast(lease_manager, ctx).await {
                Ok(renewal) if !renewal.status.raw().is_leader() => {
                    info!("Lost leadership after S3 CAS race");
                    return;
                }
                Ok(_) => {
                    if let Some(new_peer) = check_follower_changed(lease_manager, &peer_info).await {
                        known_peer = Some(new_peer);
                        break;
                    }
                }
                Err(e) => {
                    // Both heartbeat and S3 unavailable. Writes already rejected
                    // (replication has no target). TTL self-fencing is the safety bound.
                    error!(error = ?e, "S3 lease renewal failed, relying on TTL");
                }
            }
        }
    }
}

/// Monitor heartbeat liveness and race to S3 when leader is presumed dead.
/// Returns when:
/// - This node wins the CAS race (status set to Leader by renew_s3_lease_and_broadcast)
/// - A kick transitions status to FollowerCatchingUp (role-flip loop handles catchup)
async fn run_follower_watchdog<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    lease_manager: &Rc<LeaseManager<S>>,
    ctx: &ConnectionContext<R, D, S>,
) {
    info!("Entering follower watchdog");

    loop {
        // Sleep until TTL expiry or kick detection. If a heartbeat refreshes
        // the TTL during our sleep, we wake at the old deadline, re-check,
        // and sleep the delta to the new deadline.
        // For TTL-exempt catchup states, poll every 500ms to detect kicks.
        loop {
            let status = ctx.shard_wal.node_status.get();
            if !status.is_any_follower_state() {
                break;
            }
            // Kick received — return to role-flip loop for catchup orchestration
            if status.raw().is_catching_up() {
                info!("Kick detected, returning to role-flip loop for catchup");
                return;
            }
            let sleep_ms = status.expires_at_ms().saturating_sub(now_ms()).min(500);
            glommio::timer::sleep(Duration::from_millis(sleep_ms)).await;
        }

        // TTL expired — leader presumed dead.
        // Self-fencing via TTL already rejects writes/replication.
        // Explicit broadcast updates raw() for observability.
        info!("Leader heartbeat expired, racing to S3");
        let fenced = ValidatedNodeStatus::fenced();
        ctx.shard_wal.node_status.set(fenced);
        broadcast_message_to_other_shards(
            ctx.current_shard_id,
            IntrashardMessages::StatusUpdate { status: fenced },
            ctx.intrashard_sender.clone(),
        ).await;

        match renew_s3_lease_and_broadcast(lease_manager, ctx).await {
            Ok(outcome) if outcome.status.raw().is_leader() => {
                info!("Won S3 CAS race, becoming leader");
                return;
            }
            Ok(_) => {
                info!("Lost S3 CAS race, resuming watchdog");
                // Status already set to Follower with fresh TTL by
                // renew_s3_lease_and_broadcast. Inner loop will sleep
                // until the new leader's heartbeats stop.
            }
            Err(e) => {
                // Already fenced. Retry after backoff.
                error!(error = ?e, "S3 race failed, retrying");
                glommio::timer::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Coordinated S3 catchup after being kicked by the leader.
/// Reuses the same catchup logic as boot. On success, transitions directly
/// to Follower — the leader's next TCP replication attempt succeeds naturally.
async fn run_kick_catchup<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: &ConnectionContext<R, D, S>,
    rx: &LocalReceiver<CatchupCompletionMsg>,
) {
    info!("Starting kick catchup (S3)");
    if !run_s3_catchup(ctx, rx).await {
        return; // fatal error, shutdown already triggered
    }

    let current = ctx.shard_wal.node_status.get().raw();
    match current {
        NodeStatus::FollowerCatchingUp { leader_lease_index } => {
            let rc = ctx.config.replication_config.as_ref();
            let status_ttl_ms = rc.map(|r| r.status_ttl_ms()).unwrap_or(5000);
            let follower = ValidatedNodeStatus::new(
                NodeStatus::Follower { leader_lease_index },
                now_ms() + status_ttl_ms,
            );
            ctx.shard_wal.node_status.set(follower);
            broadcast_message_to_other_shards(
                ctx.current_shard_id,
                IntrashardMessages::StatusUpdate { status: follower },
                ctx.intrashard_sender.clone(),
            ).await;
            info!("Kick catchup complete — resuming as Follower");
        }
        NodeStatus::Fenced => {
            info!("Leader died during kick catchup — fenced, will race");
        }
        other => {
            warn!(status = ?other, "Unexpected status after kick catchup");
        }
    }
}

fn spawn_intrashard_message_handler<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    stream: ConnectedReceiver<IntrashardMessages>,
    ctx: ConnectionContext<R, D, S>,
) {
    let shard_id = ctx.current_shard_id;
    glommio::spawn_local(async move {
        while let Some(msg) = stream.recv().await {
            handle_intrashard_message(msg, &ctx).await;
        }
        error!(shard_id, "Intrashard message handler exited — channel closed");
    })
    .detach();
}

async fn handle_intrashard_message<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(msg: IntrashardMessages, ctx: &ConnectionContext<R, D, S>) {
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
        IntrashardMessages::StatusUpdate { status } => {
            ctx.shard_wal.node_status.set(status);
        }
        IntrashardMessages::UpdateFollower { replication_address } => {
            if let Ok(mut guard) = write_with_timeout(&ctx.shard_wal.replication_client, "set_follower_address").await {
                guard.set_follower_address(replication_address);
            } else {
                warn!("Failed to acquire replication client lock for follower address update");
            }
        }
    }
}
