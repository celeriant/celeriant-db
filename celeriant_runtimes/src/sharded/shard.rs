use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    time::Duration,
};

use celeriant_distributed::{lease_store::LeaseStore, s3_lease_manager::{ElectionOutcome, S3LeaseManager}, validated_node_status::{self, ValidatedNodeStatus}};
use celeriant_msg::response::responses::HeartbeatResult;
use celeriant_shard::{error::send_heartbeat_error::SendHeartbeatError, replication_client::ReplicationClient, s3_downloader::S3Downloader, shard_wal::ShardWal};
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
    api_key_reloader::ApiKeyReloader,
    connection_handler::{
        CatchupCompletionMsg, ConnectionContext, PortType, handle_enter_s3_catchup, handle_new_connection, handle_redirected_client_connection, handle_redirected_cluster_connection,
    },
    intrashard_messages::IntrashardMessages,
    shard_config::ShardConfig,
    signal_handler::SignalHandler,
    tls_config::{TlsConfig, TlsMode},
    tls_reloader::TlsReloader,
};

const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Enable TCP keepalive on a socket to detect dead peers at the OS level.
/// Uses a 10-second idle time and 3-second probe interval — if a peer is
/// unreachable, the kernel will close the connection after ~30 seconds
/// (10s idle + 3 × 3s probes with default retry count).
fn set_tcp_keepalive(stream: &glommio::net::TcpStream) {
    use std::os::unix::io::AsRawFd;
    let fd = stream.as_raw_fd();
    let enabled: libc::c_int = 1;
    let idle_secs: libc::c_int = 10;
    let interval_secs: libc::c_int = 3;
    unsafe {
        libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, &enabled as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t);
        libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, &idle_secs as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t);
        libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, &interval_secs as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t);
    }
}

pub struct Shard<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static> {
    intrashard_receivers: Receivers<IntrashardMessages>,
    client_tcp_listener: Rc<TcpListener>,
    replication_tcp_listener: Rc<TcpListener>,
    ctx: ConnectionContext<R, D, S>,
    shutdown_requested: Rc<Cell<bool>>,
    shard_wal: Rc<ShardWal<R, D>>,
    shard_failed: Arc<AtomicBool>,
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
        lease_manager: Option<S3LeaseManager<S>>,
        shard_failed: Arc<AtomicBool>,
    ) -> Self {
        debug!("Initializing shard {current_shard_id}");

        let shutdown_requested = Rc::new(Cell::new(false));
        let shard_wal = Rc::new(shard_wal);

        let ctx = ConnectionContext {
            config: Rc::new(config),
            current_shard_id,
            intrashard_sender: Rc::new(sender),
            shutdown_requested: shutdown_requested.clone(),
            shard_wal: shard_wal.clone(),
            catchup_completion_tx: None,
            schema_registration_pending: None,
            lease_manager: lease_manager.map(Rc::new),
        };

        Self {
            intrashard_receivers: receivers,
            client_tcp_listener: Rc::new(client_tcp_listener),
            replication_tcp_listener: Rc::new(replication_tcp_listener),
            ctx,
            shutdown_requested,
            shard_wal,
            shard_failed,
        }
    }

    pub async fn run(&mut self) {
        debug!("Shard {} entering run loop", self.ctx.current_shard_id);
        spawn_shard_zero_shutdown_handler(self.ctx.clone());

        let rx = if self.ctx.lease_manager.is_some() {
            let (tx, rx) = glommio::channels::local_channel::new_unbounded();
            self.ctx.catchup_completion_tx = Some(Rc::new(tx));
            Some(rx)
        } else {
            None
        };

        if self.ctx.current_shard_id == 0 && self.ctx.config.num_shards > 1 {
            self.ctx.schema_registration_pending = Some(Rc::new(std::cell::RefCell::new(std::collections::HashMap::new())));
        }

        for (_src_shard, stream) in self.intrashard_receivers.streams() {
            spawn_intrashard_message_handler(stream, self.ctx.clone());
        }

        if let Some(rx) = rx {
            spawn_boot_orchestrator(self.ctx.clone(), rx);
        } else {
            // Standalone mode — no election, always leader
            metrics::gauge!("celeriant_node_role").set(1.0);
        }

        self.enter_main_loop_until_shutdown().await;

        info!("Shard {} shutdown complete", self.ctx.current_shard_id);
    }

    fn should_shutdown(&self) -> bool {
        self.shutdown_requested.get() || self.shard_failed.load(Ordering::Relaxed)
    }

    async fn enter_main_loop_until_shutdown(&self) {
        debug!("Shard {} entering main loop (shutdown_requested={})", self.ctx.current_shard_id, self.shutdown_requested.get());

        let tls_cell: Rc<RefCell<Option<Arc<TlsConfig>>>> =
            Rc::new(RefCell::new(self.ctx.config.tls_config.clone()));

        let client_listener = self.client_tcp_listener.clone();
        let client_ctx = self.ctx.clone();
        let client_shard_failed = self.shard_failed.clone();
        let client_tls = tls_cell.clone();
        glommio::spawn_local(async move {
            loop {
                if client_ctx.shutdown_requested.get() || client_shard_failed.load(Ordering::Relaxed) {
                    break;
                }
                // Read current TLS config each iteration so hot-reloads take effect
                // for new connections without requiring a restart.
                let tls_snapshot = client_tls.borrow().as_ref()
                    .map(|t| (t.tls_mode, t.client_server_config.clone()));
                match glommio::timer::timeout(Duration::from_secs(1), client_listener.shared_accept()).await {
                    Ok(stream) => {
                        let tcp_stream = stream.bind_to_executor();
                        if let Err(e) = tcp_stream.set_nodelay(true) {
                            warn!("set_nodelay failed on client connection: {e}");
                        }
                        match maybe_ktls_accept(tcp_stream, &tls_snapshot).await {
                            Ok((tcp_stream, trailing)) => handle_new_connection(tcp_stream, trailing, client_ctx.clone(), PortType::Client),
                            Err(e) => warn!("TLS handshake failed on client port: {:?}", e),
                        }
                    }
                    Err(_) => {}
                }
            }
        })
        .detach();

        let repl_listener = self.replication_tcp_listener.clone();
        let repl_ctx = self.ctx.clone();
        let repl_shard_failed = self.shard_failed.clone();
        let repl_tls = tls_cell.clone();
        glommio::spawn_local(async move {
            loop {
                if repl_ctx.shutdown_requested.get() || repl_shard_failed.load(Ordering::Relaxed) {
                    break;
                }
                let tls_snapshot = repl_tls.borrow().as_ref()
                    .map(|t| (t.tls_mode, t.replication_server_config.clone()));
                match glommio::timer::timeout(Duration::from_secs(1), repl_listener.shared_accept()).await {
                    Ok(stream) => {
                        let tcp_stream = stream.bind_to_executor();
                        if let Err(e) = tcp_stream.set_nodelay(true) {
                            warn!("set_nodelay failed on replication connection: {e}");
                        }
                        set_tcp_keepalive(&tcp_stream);
                        match maybe_ktls_accept(tcp_stream, &tls_snapshot).await {
                            Ok((tcp_stream, trailing)) => handle_new_connection(tcp_stream, trailing, repl_ctx.clone(), PortType::Replication),
                            Err(e) => warn!("TLS handshake failed on replication port: {:?}", e),
                        }
                    }
                    Err(_) => {}
                }
            }
        })
        .detach();

        // Spawn the cert hot-reload timer if reload is enabled and cert paths are configured.
        let reload_interval = self.ctx.config.tls_cert_reload_interval;
        if !reload_interval.is_zero() {
            if let Some(paths) = &self.ctx.config.tls_cert_paths {
                let tls_mode = self.ctx.config.tls_config
                    .as_ref()
                    .map(|t| t.tls_mode)
                    .unwrap_or(TlsMode::Disabled);
                let client_auth = self.ctx.config.tls_client_auth;
                let reloader = TlsReloader::new(
                    paths.ca_cert.clone(),
                    paths.intracluster_ca_cert.clone(),
                    paths.node_cert.clone(),
                    paths.node_key.clone(),
                    paths.client_cert.clone(),
                    paths.client_key.clone(),
                    client_auth,
                    tls_mode,
                );
                let reload_tls = tls_cell.clone();
                let shard_id = self.ctx.current_shard_id;
                let shutdown = self.ctx.shutdown_requested.clone();
                glommio::spawn_local(async move {
                    loop {
                        glommio::timer::sleep(reload_interval).await;
                        if shutdown.get() { break; }
                        if let Some(new_cfg) = reloader.check_and_reload() {
                            info!(shard_id, "TLS config hot-reloaded, new connections will use new certs");
                            *reload_tls.borrow_mut() = Some(new_cfg);
                        }
                    }
                })
                .detach();
            }
        }

        // Spawn the API key hot-reload timer if API keys are configured and reload is enabled.
        if !reload_interval.is_zero() {
            if self.ctx.config.api_key_hashes.borrow().is_some() {
                let reloader = ApiKeyReloader::new(&self.ctx.config.data_root);
                let api_key_hashes_cell = self.ctx.config.api_key_hashes.clone();
                let shard_id = self.ctx.current_shard_id;
                let shutdown = self.ctx.shutdown_requested.clone();
                glommio::spawn_local(async move {
                    loop {
                        glommio::timer::sleep(reload_interval).await;
                        if shutdown.get() { break; }
                        if let Some(new_hashes) = reloader.check_and_reload() {
                            info!(shard_id, "API key config hot-reloaded, new connections will use new keys");
                            *api_key_hashes_cell.borrow_mut() = Some(new_hashes);
                        }
                    }
                })
                .detach();
            }
        }

        // Spawn background compaction timer.
        {
            let compaction_interval = self.ctx.config.compaction_check_interval;
            let shard_id = self.ctx.current_shard_id;
            let shutdown = self.ctx.shutdown_requested.clone();
            let shard_wal = self.shard_wal.clone();
            glommio::spawn_local(async move {
                loop {
                    glommio::timer::sleep(compaction_interval).await;
                    if shutdown.get() { break; }
                    let started_at = std::time::Instant::now();
                    match shard_wal.compact_oldest_eligible_segment().await {
                        Ok(Some(result)) => {
                            info!(
                                shard_id,
                                log_id = result.log_id,
                                original_size = result.original_size,
                                compacted_size = result.compacted_size,
                                bytes_reclaimed = result.original_size.saturating_sub(result.compacted_size),
                                duration_ms = started_at.elapsed().as_millis(),
                                "Compaction complete"
                            );
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!(shard_id, error = ?e, "Compaction failed");
                        }
                    }
                }
            })
            .detach();
        }

        loop {
            if self.should_shutdown() {
                self.shutdown_requested.set(true);
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
        if let Err(e) = try_send_with_retry(senders.as_ref(), peer, message.clone(), 10).await {
            error!("Failed to send message to shard {peer}: {e:?}");
        }
    }
}

pub(crate) async fn try_send_with_retry<T: Send>(senders: &Senders<T>, peer: usize, mut msg: T, max_retries: usize) -> Result<(), glommio::GlommioError<T>> {
    for _ in 0..max_retries {
        match senders.try_send_to(peer, msg) {
            Ok(()) => return Ok(()),
            Err(e) => match e {
                glommio::GlommioError::WouldBlock(glommio::ResourceType::Channel(returned)) => {
                    msg = returned;
                    glommio::yield_if_needed().await;
                }
                other => return Err(other),
            },
        }
    }
    senders.try_send_to(peer, msg)
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
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        for peer in 1..shard_count {
            if let Err(e) = try_send_with_retry(ctx.intrashard_sender.as_ref(), peer, IntrashardMessages::EnterS3Catchup, 10).await {
                panic!("Failed to send S3 catchup to shard {peer} after retries: {e:?}");
            }
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
                Ok(r) => {
                    info!(
                        shard_id = msg.shard_id,
                        batches_applied = r.batches_applied,
                        bytes_downloaded = r.bytes_downloaded,
                        rounds = r.rounds,
                        fully_caught_up = r.fully_caught_up,
                        "S3 catchup complete for shard"
                    );
                }
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

        warn!(attempt, "S3 catchup has retriable errors, retrying in 5s");
        glommio::timer::sleep(Duration::from_secs(5)).await;
    }
}

/// Retry an async S3 operation with exponential backoff (1s, 2s, 4s, …) up to
/// `max_duration`. If `max_duration` is None, retries indefinitely.
/// Only retries `LeaseStoreError::Unavailable`; other errors propagate immediately.
async fn retry_s3_operation<F, Fut, T>(
    max_duration: Option<Duration>,
    op_name: &str,
    mut op: F,
) -> Result<T, celeriant_distributed::lease_store::LeaseStoreError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, celeriant_distributed::lease_store::LeaseStoreError>>,
{
    use celeriant_distributed::lease_store::LeaseStoreError;

    let started_at = std::time::Instant::now();
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(LeaseStoreError::Unavailable { ref message }) => {
                let elapsed = started_at.elapsed();
                if let Some(max) = max_duration {
                    if elapsed + backoff > max {
                        warn!(op = op_name, elapsed_ms = elapsed.as_millis() as u64, error = %message, "S3 retry budget exhausted");
                        return Err(LeaseStoreError::Unavailable { message: message.clone() });
                    }
                }
                warn!(op = op_name, elapsed_ms = elapsed.as_millis() as u64, next_backoff_ms = backoff.as_millis() as u64, error = %message, "S3 unavailable, retrying");
                glommio::timer::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Reach out to S3 immediately to determine if this node is leader or follower
/// Ensure we are up-to-date with any replicated S3 entries if we become leader
/// Ensure all shards are updated with our new status
async fn set_node_role_via_s3<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    lease_manager: &S3LeaseManager<S>,
    ctx: &ConnectionContext<R, D, S>,
    rx: &LocalReceiver<CatchupCompletionMsg>,
) -> Result<ElectionOutcome, celeriant_distributed::lease_store::LeaseStoreError> {

    let is_currently_leader = ctx.shard_wal.node_status.get().raw().is_leader();

    let outcome = retry_s3_operation(ctx.config.s3_retry_max_duration, "renew_s3_lease", || lease_manager.run_election_to_acquire_s3_lease()).await?;

    metrics::counter!("celeriant_leader_elections_total").increment(1);
    metrics::gauge!("celeriant_node_role").set(if outcome.status.raw().is_leader() { 1.0 } else { 0.0 });

    // We took over the leadership. Catch up from S3 as a sanity check
    // Unlikely to have previous leader race condition here, but possible
    if !is_currently_leader && outcome.status.is_leader() {
        info!("Starting post-election S3 catchup");
        if !run_s3_catchup(ctx, &rx).await {
            return Err(celeriant_distributed::lease_store::LeaseStoreError::Unavailable { message: "Could not catch up WAL via S3".to_string() });
        }
    }

    // Do we have a peer? It tells us where to replicate or where to direct clients if this node isn't the leader
    let (leader_client_address, follower_replication_address) = if let Some(peer_info) = outcome.peer_info.as_ref() {
        if outcome.status.is_leader() {
            (None, Some(peer_info.replication_address.clone()))
        } else {
            (Some(peer_info.client_address.clone()), None)
        }
    } else {
        (None, None)
    };
    
    *ctx.shard_wal.leader_client_address.borrow_mut() = leader_client_address.clone();
    broadcast_message_to_other_shards(
        ctx.current_shard_id,
        IntrashardMessages::UpdateLeaderClientAddress { client_address: leader_client_address },
        ctx.intrashard_sender.clone(),
    ).await;
    
    ctx.shard_wal.replication_client.set_follower_address(follower_replication_address.clone());
    broadcast_message_to_other_shards(
        ctx.current_shard_id, 
        IntrashardMessages::UpdateFollower { replication_address: follower_replication_address }, 
        ctx.intrashard_sender.clone()
    ).await;

    // Finally open up writes if leader or accept replication if follower
    let previous = ctx.shard_wal.node_status.get();
    if !previous.raw().same_role(&outcome.status.raw()) {
        warn!(
            shard_id = ctx.current_shard_id,
            previous = ?previous.raw(),
            new = ?outcome.status.raw(),
            expires_at_ms = outcome.status.lease_expires_at_ms(),
            "Node status transition"
        );
    }
    ctx.shard_wal.node_status.set(outcome.status);
    broadcast_message_to_other_shards(
        ctx.current_shard_id,
        IntrashardMessages::StatusUpdate { status: outcome.status },
        ctx.intrashard_sender.clone(),
    ).await;


    Ok(outcome)
}

fn spawn_boot_orchestrator<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: ConnectionContext<R, D, S>,
    rx: LocalReceiver<CatchupCompletionMsg>,
) {
    // Sanity check for standalone
    if ctx.shard_wal.node_status.get().raw().is_standalone() || ctx.config.replication_config.is_none() || ctx.lease_manager.is_none() {
        warn!("Running boot orchestrator but in standalone mode. Skipping.");
        return;
    }

    let lease_manager = ctx.lease_manager.clone().unwrap();
    glommio::spawn_local(async move {

        // Node registration only needs to be done once on boot
        if let Err(e) = lease_manager.register_self_on_membership_s3_object().await {
            panic!("Failed to register node in membership (IAM or network issue): {e}");
        }

        let half_s3_lease = ctx.config.replication_config.as_ref().unwrap().s3_lease_duration / 2;
        let mut has_peer = false;
        let mut peer_discovery_backoff = Duration::from_secs(1);
        let mut last_peer_discovery_attempt = std::time::Instant::now();

        loop {
            if ctx.shutdown_requested.get() {
                break;
            }

            if ctx.shard_wal.node_status.get().is_leader() {
                glommio::timer::sleep(ctx.config.heartbeat_interval_duration).await;

                let unix_epoch_now_ms = validated_node_status::unix_epoch_now_ms();
                let result = ctx.shard_wal.replication_client.send_heartbeat(unix_epoch_now_ms).await;

                if let Err(SendHeartbeatError::LockTimeout) = &result {
                    warn!("Heartbeat lock contention, skipping heartbeat");
                    continue;
                }

                if let Ok(HeartbeatResult::Ack { .. }) = result {
                    has_peer = true;
                    peer_discovery_backoff = Duration::from_secs(1);
                    let refreshed = ValidatedNodeStatus::create_custom_status(
                        ctx.shard_wal.node_status.get().raw(), ctx.config.max_clock_drift_ms, unix_epoch_now_ms + ctx.config.heartbeat_lease_duration.as_millis() as u64);
                    ctx.shard_wal.node_status.set(refreshed);
                    broadcast_message_to_other_shards(ctx.current_shard_id, IntrashardMessages::StatusUpdate { status: refreshed }, ctx.intrashard_sender.clone()).await;
                    continue;
                }

                metrics::counter!("celeriant_heartbeat_failures_total").increment(1);

                // Decide whether to check S3: either for peer discovery (no known peer)
                // or for lease renewal (known peer but unreachable)
                let should_check_s3 = if !has_peer {
                    // No peer known — eagerly discover with backoff (1s, 2s, 4s, … capped at half S3 lease)
                    last_peer_discovery_attempt.elapsed() >= peer_discovery_backoff
                } else {
                    // Peer known but unreachable — only check S3 when lease needs renewal
                    let unix_epoch_now_ms = validated_node_status::unix_epoch_now_ms();
                    let half_s3_lease_ms = half_s3_lease.as_millis() as u64;
                    let proactive = unix_epoch_now_ms > ctx.shard_wal.node_status.get().lease_expires_at_ms().saturating_sub(half_s3_lease_ms);
                    let expired = ctx.shard_wal.node_status.get().is_lease_expired();
                    proactive || expired
                };

                if !should_check_s3 {
                    continue;
                }

                warn!(has_peer, "Heartbeat failure, attempting S3 lease extension and membership discovery");

                match set_node_role_via_s3(&lease_manager, &ctx, &rx).await {
                    Ok(outcome) => {
                        if outcome.peer_info.is_some() {
                            has_peer = true;
                            peer_discovery_backoff = Duration::from_secs(1);
                        } else {
                            has_peer = false;
                            last_peer_discovery_attempt = std::time::Instant::now();
                            peer_discovery_backoff = (peer_discovery_backoff * 2).min(half_s3_lease);
                        }
                    }
                    Err(e) => {
                        panic!("Election failed after retries: {e}");
                    }
                }

                continue;
            }

            if ctx.shard_wal.node_status.get().is_follower() || ctx.shard_wal.node_status.get().is_fenced() {

                // If we are a happy follower of just fenced, we can challenge the lease at the expiry time
                let unix_epoch_now_ms = validated_node_status::unix_epoch_now_ms();
                let remaining_time_until_lease_expires_ms = ctx.shard_wal.node_status.get().lease_expires_at_ms().saturating_sub(unix_epoch_now_ms);
                if remaining_time_until_lease_expires_ms > 0 {
                    //Only sleep for max of 500ms so we can still respond to shutdown commands
                    glommio::timer::sleep(Duration::from_millis(remaining_time_until_lease_expires_ms.min(500))).await;
                }

                // Could have been updated by a re-connecting heartbeat on another task
                if !ctx.shard_wal.node_status.get().is_lease_expired() {
                    continue;
                }

                let still_right_status = ctx.shard_wal.node_status.get().is_follower() || ctx.shard_wal.node_status.get().is_fenced();
                if !still_right_status {
                    continue;
                }

                info!(node_status = ?ctx.shard_wal.node_status.get(), "Follower or fenced node detected expired lease, challenging for leadership");

                if let Err(e) = set_node_role_via_s3(&lease_manager, &ctx, &rx).await {
                    panic!("Election failed after retries: {e}");
                }

                continue;
            }

            if ctx.shard_wal.node_status.get().raw().is_catching_up() {

                info!("Node was follower but got kicked, or we are in boot catchup phase, asking shards to catch up via s3");

                if !run_s3_catchup(&ctx, &rx).await {
                    panic!("S3 catchup failed with fatal error");
                }

                // WAL is caught up — now determine our role via S3 election
                if let Err(e) = set_node_role_via_s3(&lease_manager, &ctx, &rx).await {
                    panic!("Post-catchup election failed after retries: {e}");
                }

                continue;
            }
        }
    })
    .detach();
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
        IntrashardMessages::ClientConnectionRedirect {
            accepted_tcp_stream,
            request,
            message_version,
            verified_client_id,
            access_level,
        } => {
            handle_redirected_client_connection(
                accepted_tcp_stream.bind_to_executor(),
                request,
                ctx.config.max_response_size,
                ctx.config.server_compression_algorithm,
                message_version,
                ctx.clone(),
                verified_client_id,
                access_level,
            );
        }
        IntrashardMessages::ClusterConnectionRedirect {
            accepted_tcp_stream,
            request,
            message_version,
        } => {
            handle_redirected_cluster_connection(
                accepted_tcp_stream.bind_to_executor(),
                request,
                ctx.config.max_response_size,
                ctx.config.server_compression_algorithm,
                message_version,
                ctx.clone(),
            );
        }
        IntrashardMessages::EnterS3Catchup => handle_enter_s3_catchup(ctx.clone()),
        IntrashardMessages::S3CatchupComplete { shard_id, result } => {
            if let Some(tx) = &ctx.catchup_completion_tx {
                let _ = tx.try_send(CatchupCompletionMsg { result, shard_id });
            }
        }
        IntrashardMessages::StatusUpdate { status } => {
            let previous = ctx.shard_wal.node_status.get();
            if !previous.raw().same_role(&status.raw()) {
                warn!(
                    shard_id = ctx.current_shard_id,
                    previous = ?previous.raw(),
                    new = ?status.raw(),
                    expires_at_ms = status.lease_expires_at_ms(),
                    "Node status transition"
                );
            }
            ctx.shard_wal.node_status.set(status);
        }
        IntrashardMessages::UpdateFollower { replication_address } => {
            ctx.shard_wal.replication_client.set_follower_address(replication_address);
        }
        IntrashardMessages::UpdateLeaderClientAddress { client_address } => {
            *ctx.shard_wal.leader_client_address.borrow_mut() = client_address;
        }
        IntrashardMessages::SchemaRegistration { request, request_id } => {
            let result = ctx.shard_wal.register_schema(request).await
                .map(|_| ())
                .map_err(|e| e);

            let completion_msg = IntrashardMessages::SchemaRegistrationComplete {
                request_id,
                result,
            };
            let _ = try_send_with_retry(ctx.intrashard_sender.as_ref(), 0, completion_msg, 10).await;
        }
        IntrashardMessages::SchemaRegistrationComplete { request_id, result } => {
            if let Some(pending_map) = &ctx.schema_registration_pending {
                if let Some(tx) = pending_map.borrow().get(&request_id) {
                    let _ = tx.try_send(crate::sharded::connection_handler::SchemaRegistrationCompletionMsg {
                        result,
                    });
                }
            }
        }
    }
}

/// Apply kTLS upgrade to a freshly accepted stream based on `tls_config`.
///
/// Returns the stream unchanged when no TLS config is present (disabled mode).
/// Performs a kTLS handshake in strict mode; errors are returned to the caller
/// which logs and continues the accept loop.
async fn maybe_ktls_accept(
    stream: glommio::net::TcpStream,
    tls_config: &Option<(TlsMode, Arc<rustls::ServerConfig>)>,
) -> Result<(glommio::net::TcpStream, Vec<u8>), celeriant_ktls::KtlsError> {
    let (mode, server_config) = match tls_config {
        Some(t) => t,
        None => return Ok((stream, Vec::new())),
    };

    match mode {
        TlsMode::Disabled => {
            warn!("TlsConfig present with TlsMode::Disabled; passing stream through unencrypted");
            Ok((stream, Vec::new()))
        }
        TlsMode::Strict => {
            match glommio::timer::timeout(TLS_HANDSHAKE_TIMEOUT, async {
                Ok::<_, glommio::GlommioError<()>>(celeriant_ktls::ktls_accept(stream, server_config.clone()).await)
            })
            .await
            {
                Ok(result) => result,
                Err(_) => Err(celeriant_ktls::KtlsError::Io(
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "TLS handshake timed out"),
                )),
            }
        }
    }
}
