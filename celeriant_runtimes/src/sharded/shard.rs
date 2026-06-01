use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    time::Duration,
};

use celeriant_distributed::{lease_store::LeaseStore, node_status::NodeStatus, node_status_logic::{compute_new_ttl, decide_post_catchup_action, PostCatchupAction}, s3_lease_manager::{ElectionOutcome, S3LeaseManager}, validated_node_status::{self, ValidatedNodeStatus, set_node_status_and_metric}};
use celeriant_msg::response::responses::HeartbeatResult;
use celeriant_shard::{error::send_heartbeat_error::SendHeartbeatError, replication_client::ReplicationClient, s3_downloader::S3Downloader, shard_wal::{LeaseRenewalRequester, ShardWal}, shard_wal_s3_catchup::CatchupCompletion};
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

        let dict_codec = shard_wal.dict_codec.clone();
        let ctx = ConnectionContext {
            config: Rc::new(config),
            current_shard_id,
            intrashard_sender: Rc::new(sender),
            shutdown_requested: shutdown_requested.clone(),
            shard_wal: shard_wal.clone(),
            catchup_completion_tx: None,
            schema_registration_pending: None,
            lease_manager: lease_manager.map(Rc::new),
            dict_codec,
        };

        // Wire the out-of-band lease-renewal hook: lets the replication path nudge shard 0
        // to re-CAS the S3 lease when an S3 fallback would otherwise be gated by a stale
        // CAS confirmation, instead of relying on the (possibly kernel-stalled) heartbeat loop.
        ctx.shard_wal.set_lease_renewal_requester(Rc::new(IntrashardLeaseRenewalRequester {
            sender: ctx.intrashard_sender.clone(),
            shard_id: current_shard_id,
        }));

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

    /// Read-only access to the per-shard WAL. Used by external runtime
    /// extensions (e.g. celeriant-queue) to wire additional listeners on
    /// the same executor as the storage engine, sharing the local
    /// ShardWal so writes route through one fsync-before-ack path.
    pub fn shard_wal_rc(&self) -> Rc<ShardWal<R, D>> {
        self.shard_wal.clone()
    }

    /// Per-executor shutdown flag. Extension tasks should observe this so
    /// they tear down cleanly when the shard initiates shutdown.
    pub fn shutdown_flag(&self) -> Rc<Cell<bool>> {
        self.shutdown_requested.clone()
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
        } else if self.ctx.config.replication_config.is_none() {
            // Standalone mode — no election, always leader.
            // In distributed mode only shard 0 owns a lease_manager
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
                    metrics::gauge!("celeriant_node_role").set(0.0);
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

enum CatchupRunOutcome {
    Caught,
    Shutdown,
    S3Unreachable,
}

async fn run_s3_catchup<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: &ConnectionContext<R, D, S>,
    rx: &LocalReceiver<CatchupCompletionMsg>,
) -> CatchupRunOutcome {
    let shard_count = ctx.config.num_shards as usize;
    let mut attempt = 0u32;
    // 4 rounds x 5s ~= 20s of confirmed S3 outage before bailing so a follower can resume via TCP.
    const MAX_S3_UNREACHABLE_ROUNDS: u32 = 4;
    let mut consecutive_s3_unreachable = 0u32;

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

        // Split so a pure-S3-error round (no contact) counts toward the unreachable bound, while
        // any S3 contact (undrained) resets it.
        let mut has_undrained = false;
        let mut has_s3_error = false;
        let mut has_fatal = false;

        for msg in &results {
            match &msg.result {
                Ok(r) => match r.completion {
                    CatchupCompletion::Caught => {
                        info!(
                            shard_id = msg.shard_id,
                            batches_applied = r.batches_applied,
                            bytes_downloaded = r.bytes_downloaded,
                            rounds = r.rounds,
                            "S3 catchup caught up for shard"
                        );
                    }
                    CatchupCompletion::Retry => {
                        warn!(
                            shard_id = msg.shard_id,
                            batches_applied = r.batches_applied,
                            bytes_downloaded = r.bytes_downloaded,
                            rounds = r.rounds,
                            "S3 catchup did not drain for shard, will retry"
                        );
                        has_undrained = true;
                    }
                },
                Err(e) if e.is_retriable() => {
                    warn!(shard_id = msg.shard_id, error = ?e, "S3 catchup retriable error, will retry");
                    has_s3_error = true;
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
            return CatchupRunOutcome::Shutdown;
        }

        if !has_undrained && !has_s3_error {
            return CatchupRunOutcome::Caught;
        }

        if has_s3_error && !has_undrained {
            consecutive_s3_unreachable += 1;
            if consecutive_s3_unreachable >= MAX_S3_UNREACHABLE_ROUNDS {
                warn!(attempt, consecutive_s3_unreachable, "S3 unreachable across consecutive catchup rounds; bailing so a heartbeated follower can resume via TCP");
                return CatchupRunOutcome::S3Unreachable;
            }
        } else {
            consecutive_s3_unreachable = 0;
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
/// Bridges the layering gap for the lease-renewal hook: `celeriant_shard` defines the
/// `LeaseRenewalRequester` trait but has no access to the intra-shard mesh, which lives
/// here. A data shard's replication path calls `request_renewal()`; this sends a
/// `RenewS3LeaseNow` to shard 0.
struct IntrashardLeaseRenewalRequester {
    sender: Rc<Senders<IntrashardMessages>>,
    shard_id: usize,
}

impl LeaseRenewalRequester for IntrashardLeaseRenewalRequester {
    fn request_renewal(&self) {
        // Fire-and-forget, best-effort: coalesced on shard 0, and the replication spin loop
        // re-requests on its next iteration if the queue was momentarily full.
        let sent = self.sender.try_send_to(0, IntrashardMessages::RenewS3LeaseNow { requesting_shard: self.shard_id });
        metrics::counter!("celeriant_s3_lease_renewal_requested_total",
            &[("shard_id", self.shard_id.to_string()),
              ("result", if sent.is_ok() { "sent" } else { "dropped" }.to_string())]).increment(1);
    }
}

/// Out-of-band S3 lease renewal, handled on shard 0 in response to a data shard's
/// `RenewS3LeaseNow`. The data shard sends this when it must S3-fallback (a durability
/// ack) but its CAS-confirmed lease has gone stale — rather than wait for the heartbeat
/// loop (which can stall in the kernel under load) to renew, it pokes shard 0 to re-CAS
/// `lease.json` here and now, then spin-waits for the broadcast green light.
///
/// Single-flight without a lock: the intra-shard handler is strictly sequential, and a
/// debounce on `s3_cas_confirmed_at_ms` makes a burst from all shards trigger at most one
/// CAS — the first renews and refreshes everyone; the rest fall through the debounce.
///
/// `run_election_to_acquire_s3_lease` renews a self-held lease in place (no epoch bump);
/// only a peer-held/expired lease promotes. A peer that has superseded us returns a
/// Follower outcome → we fence immediately to stop acking divergent data (the dual-ack).
async fn renew_s3_lease_on_demand<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: &ConnectionContext<R, D, S>,
    requesting_shard: usize,
) {
    let Some(lease_manager) = ctx.lease_manager.clone() else { return; };
    // Only renew while we still believe we hold leadership; acquiring from a follower/boot
    // state is the election path's job, not this hook.
    if !ctx.shard_wal.node_status.get().raw().is_leader() {
        metrics::counter!("celeriant_s3_lease_renewal_handled_total", &[("result", "not_leader".to_string())]).increment(1);
        return;
    }
    let lease_duration_ms = match ctx.config.replication_config.as_ref() {
        Some(rc) => rc.s3_lease_duration.as_millis() as u64,
        None => return, // no replication config (standalone) — nothing to renew
    };
    if lease_duration_ms == 0 {
        return; // gate disabled (tests / standalone)
    }
    // Debounce: a CAS within the last half-lease already refreshed every shard's
    // confirmation, so coalesce the rest of the burst.
    let now_ms = validated_node_status::unix_epoch_now_ms();
    let cas_age_ms = now_ms.saturating_sub(ctx.shard_wal.s3_cas_confirmed_at_ms.get());
    if cas_age_ms < lease_duration_ms / 2 {
        metrics::counter!("celeriant_s3_lease_renewal_handled_total", &[("result", "debounced".to_string())]).increment(1);
        return;
    }

    let prior_lease_epoch = ctx.shard_wal.node_status.get().raw().lease_epoch_for_logging();
    metrics::counter!("celeriant_s3_lease_renewal_handled_total", &[("result", "attempted".to_string())]).increment(1);
    let cas_start = std::time::Instant::now();
    let cas_outcome = retry_s3_operation(ctx.config.s3_retry_max_duration, "on_demand_lease_renewal",
        || lease_manager.run_election_to_acquire_s3_lease()).await;
    metrics::histogram!("celeriant_s3_lease_cas_duration_seconds", &[("reason", "on_demand".to_string())])
        .record(cas_start.elapsed().as_secs_f64());
    match cas_outcome
    {
        Ok(outcome) if outcome.status.raw().is_leader() => {
            // Renewed. Refresh our own CAS signal + TTL, then broadcast the fresh
            // confirmation so every data shard's fallback gate sees the green light.
            let confirmed_at = validated_node_status::unix_epoch_now_ms();
            ctx.shard_wal.s3_cas_confirmed_at_ms.set(confirmed_at);
            set_node_status_and_metric(&ctx.shard_wal.node_status, outcome.status, ctx.current_shard_id as u32);
            broadcast_message_to_other_shards(
                ctx.current_shard_id,
                IntrashardMessages::StatusUpdate { status: outcome.status, cas_confirmed_at_ms: Some(confirmed_at), leader_changed_hands: false },
                ctx.intrashard_sender.clone(),
            ).await;
            metrics::counter!("celeriant_s3_lease_on_demand_renewal_total", &[("result", "renewed".to_string())]).increment(1);
            info!(requesting_shard, "On-demand S3 lease renewal: re-CAS confirmed; broadcast green light to all shards");
        }
        Ok(outcome) => {
            // Superseded — a peer holds a higher epoch. Stop acking NOW: adopt the follower
            // status and broadcast it to fence every shard. The heavier demotion recovery
            // (speculative-tail cull + catchup) is handled by the orchestrator loop when it
            // observes the role change.
            set_node_status_and_metric(&ctx.shard_wal.node_status, outcome.status, ctx.current_shard_id as u32);
            broadcast_message_to_other_shards(
                ctx.current_shard_id,
                IntrashardMessages::StatusUpdate { status: outcome.status, cas_confirmed_at_ms: None, leader_changed_hands: false },
                ctx.intrashard_sender.clone(),
            ).await;
            metrics::counter!("celeriant_s3_lease_on_demand_renewal_total", &[("result", "superseded".to_string())]).increment(1);
            // Probe: distinguish a legit handoff (a peer really took the lease, epoch bumped)
            // from a false self-fence (our own lease lapsed under the fallback storm and the
            // election declined to reclaim with no peer actually holding it).
            let peer_present = outcome.peer_info.is_some();
            metrics::counter!("celeriant_s3_lease_superseded_total", &[("peer_present", peer_present.to_string())]).increment(1);
            warn!(
                requesting_shard,
                our_node_id = ctx.config.node_id,
                prior_lease_epoch,
                new_status = ?outcome.status.raw(),
                new_lease_epoch = outcome.status.raw().lease_epoch_for_logging(),
                peer_present,
                peer_node_id = ?outcome.peer_info.as_ref().map(|p| p.node_id),
                reacquired_own_lease = outcome.reacquired_own_lease,
                cas_age_ms,
                "On-demand S3 lease renewal: superseded by peer — fencing self, refusing further fallback acks");
        }
        Err(e) => {
            warn!(requesting_shard, error = %e, "On-demand S3 lease renewal CAS failed (transient); requesting shard keeps spin-waiting");
        }
    }
}

async fn set_node_role_via_s3<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    lease_manager: &S3LeaseManager<S>,
    ctx: &ConnectionContext<R, D, S>,
    rx: &LocalReceiver<CatchupCompletionMsg>,
    reason: &'static str,
) -> Result<ElectionOutcome, celeriant_distributed::lease_store::LeaseStoreError> {

    let previous_status = ctx.shard_wal.node_status.get();
    let is_currently_leader = previous_status.raw().is_leader();
    let previous_lease_epoch = previous_status.raw().lease_epoch().unwrap_or(0);

    let cas_start = std::time::Instant::now();
    let outcome = retry_s3_operation(ctx.config.s3_retry_max_duration, "renew_s3_lease", || lease_manager.run_election_to_acquire_s3_lease()).await?;
    metrics::histogram!("celeriant_s3_lease_cas_duration_seconds", &[("reason", reason.to_string())])
        .record(cas_start.elapsed().as_secs_f64());

    metrics::counter!("celeriant_leader_elections_total").increment(1);
    metrics::counter!(
        "celeriant_s3_lease_writes_total",
        &[("shard_id", ctx.current_shard_id.to_string()), ("reason", reason.to_string())],
    ).increment(1);

    // Set peer_node_id early so S3 catchup can filter stale batches from old cluster generations.
    // Full UpdateFollower broadcast (with replication address) happens after catchup.
    let peer_node_id = outcome.peer_info.as_ref().map(|p| p.node_id);
    ctx.shard_wal.peer_node_id.set(peer_node_id);
    broadcast_message_to_other_shards(
        ctx.current_shard_id,
        IntrashardMessages::UpdatePeerNodeId { peer_node_id },
        ctx.intrashard_sender.clone()
    ).await;

    let new_lease_epoch = outcome.status.raw().lease_epoch().unwrap_or(0);
    // Same-leader renewals don't bump lease_epoch, so any increase means another
    // node held the lease in between and may have uploaded S3 fallback batches.
    let lease_changed_hands = new_lease_epoch > previous_lease_epoch;
    let became_leader = !is_currently_leader && outcome.status.is_leader();
    let became_follower_from_leader_or_fenced = is_currently_leader && !outcome.status.is_leader();

    // Use the restart-proof signal from S3 to decide whether leadership changed hands.
    // outcome.reacquired_own_lease is derived from the durable S3 lease holder (not from
    // the in-memory epoch which is 0 after a restart), so it correctly identifies a
    // SIGKILL+restart self-reclaim where previous_lease_epoch==0 but no peer held the lease.
    //
    // Cull the speculative tail only when leadership genuinely changed hands (peer took over
    // and may have authored a divergent tail) or on demotion.  Self-reclaim: keep the tail.
    // Also skip upload_s3_promotion_batch on self-reclaim: no incoming replication means
    // nothing to bridge, and its idempotent-cull prefix would re-cull the tail we kept.
    let (needs_cull, leader_changed_hands, needs_catchup, rewind_to_ack_barrier) = promotion_cull_flags(
        became_leader,
        became_follower_from_leader_or_fenced,
        outcome.reacquired_own_lease,
        lease_changed_hands,
        outcome.status.raw().is_any_follower_state(),
    );

    if needs_cull || needs_catchup {
        if outcome.status.is_leader() && leader_changed_hands {
            info!(previous_lease_epoch, new_lease_epoch, "Lease changed hands during partition — running S3 catchup");
        } else if outcome.status.is_leader() {
            info!("Self-reclaim: re-acquired own lease, keeping speculative tail, running S3 catchup to confirm no peer batches");
        } else if became_follower_from_leader_or_fenced && lease_changed_hands {
            info!(previous_lease_epoch, new_lease_epoch, "Lost leadership / fenced — running S3 catchup before becoming follower");
        } else if became_follower_from_leader_or_fenced {
            info!("Self-fenced; culling speculative tail without catchup (lease has not changed hands yet)");
        } else {
            info!(previous_lease_epoch, new_lease_epoch, "Lease epoch advanced while non-leader — running S3 catchup");
        }

        // Queues are FIFO; broadcasting cull first means any following EnterS3Catchup
        // arrives after the cull lands.
        if needs_cull {
            let shard_count = ctx.config.num_shards as usize;
            for peer in 1..shard_count {
                if let Err(e) = try_send_with_retry(ctx.intrashard_sender.as_ref(), peer, IntrashardMessages::CullSpeculativeTail { rewind_to_ack_barrier }, 10).await {
                    panic!("Failed to send CullSpeculativeTail to shard {peer} after retries: {e:?}");
                }
            }
            if let Err(e) = ctx.shard_wal.cull_speculative_tail_for_promotion(rewind_to_ack_barrier).await {
                return Err(celeriant_distributed::lease_store::LeaseStoreError::Unavailable {
                    message: format!("pre-catchup speculative tail cull failed: {e:?}"),
                });
            }
        }

        if needs_catchup {
            // A promoting leader must finish S3 catchup before serving. Fatal or S3-outage fails
            // the election so it retries; never serve writes on an unverified WAL.
            match run_s3_catchup(ctx, &rx).await {
                CatchupRunOutcome::Caught => {}
                CatchupRunOutcome::Shutdown | CatchupRunOutcome::S3Unreachable => {
                    return Err(celeriant_distributed::lease_store::LeaseStoreError::Unavailable { message: "Could not catch up WAL via S3".to_string() });
                }
            }

            // Close any gap in S3 left by the old leader rolling back a batch we kept.
            // Skipped on self-reclaim (leader_changed_hands=false): no incoming replication
            // means last_received_replication_wal_seq=0 and nothing to bridge, but more
            // importantly upload_s3_promotion_batch's idempotent-cull prefix would re-cull
            // the speculative tail we deliberately kept.
            if leader_changed_hands {
                if let Err(e) = ctx.shard_wal.upload_s3_promotion_batch().await {
                    tracing::warn!(error = ?e, "Failed to upload promotion batch to S3; old leader may not be able to catch up via S3");
                }
            }
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
    
    let peer_node_id = outcome.peer_info.as_ref().map(|p| p.node_id);
    ctx.shard_wal.replication_client.set_follower_address(follower_replication_address.clone());
    ctx.shard_wal.peer_node_id.set(peer_node_id);
    broadcast_message_to_other_shards(
        ctx.current_shard_id,
        IntrashardMessages::UpdateFollower { replication_address: follower_replication_address, peer_node_id },
        ctx.intrashard_sender.clone()
    ).await;

    // Finally open up writes if leader or accept replication if follower
    let previous = ctx.shard_wal.node_status.get();
    let role_changed = !previous.raw().same_role(&outcome.status.raw());
    if role_changed {
        warn!(
            shard_id = ctx.current_shard_id,
            reason,
            previous = ?previous.raw(),
            new = ?outcome.status.raw(),
            expires_at_ms = outcome.status.lease_expires_at_ms(),
            "Node status transition"
        );
        // Clear any stale in-flight heartbeat timestamp left from a previous
        // Leader stint, so back-pressure doesn't fire spuriously based on a
        // phantom in-flight heartbeat carried across the role transition.
        if outcome.status.raw().is_leader() {
            ctx.shard_wal.replication_client.reset_heartbeat_state();
        }
    }
    let was_leader = previous.raw().is_leader();
    let now_leader = outcome.status.raw().is_leader();
    set_node_status_and_metric(&ctx.shard_wal.node_status, outcome.status, ctx.current_shard_id as u32);
    if role_changed && was_leader && !now_leader {
        ctx.shard_wal.drain_pending_replication_on_role_change().await;
    }
    // CAS path: record the confirmation timestamp on this shard and broadcast to all
    // other shards. Data shards unblock their S3 fallback uploads on receipt.
    // The heartbeat-ack path (cas_confirmed_at_ms: None) must NOT update this cell.
    let cas_confirmed_at_ms = validated_node_status::unix_epoch_now_ms();
    ctx.shard_wal.s3_cas_confirmed_at_ms.set(cas_confirmed_at_ms);
    broadcast_message_to_other_shards(
        ctx.current_shard_id,
        IntrashardMessages::StatusUpdate { status: outcome.status, cas_confirmed_at_ms: Some(cas_confirmed_at_ms), leader_changed_hands },
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

        // Node registration only needs to be done once on boot. Retry with 2s
        // backoff until S3 is reachable; transient MinIO unavailability (e.g.,
        // partition healing, restart in progress) shouldn't crash-loop systemd.
        // Termination is shutdown-driven, not a bounded retry count.
        loop {
            if ctx.shutdown_requested.get() {
                return;
            }
            match lease_manager.register_self_on_membership_s3_object().await {
                Ok(()) => break,
                Err(e) => {
                    warn!(error = ?e, "Failed to register node in membership; retrying in 2s");
                    glommio::timer::sleep(Duration::from_secs(2)).await;
                }
            }
        }

        let half_s3_lease = ctx.config.replication_config.as_ref().unwrap().s3_lease_duration / 2;
        let mut has_peer = false;
        let mut peer_discovery_backoff = Duration::from_secs(1);
        let mut last_peer_discovery_attempt = std::time::Instant::now();
        let mut last_auto_fence_warn: Option<std::time::Instant> = None;
        let mut last_s3_lease_write_at_ms: Option<u64> = None;
        let mut last_probe_at_ms: u64 = 0;

        loop {
            if ctx.shutdown_requested.get() {
                break;
            }

            if ctx.shard_wal.node_status.get().is_leader() {
                glommio::timer::sleep(ctx.config.heartbeat_interval_duration).await;

                let unix_epoch_now_ms = validated_node_status::unix_epoch_now_ms();
                let lease_epoch = ctx.shard_wal.node_status.get().raw().lease_epoch().unwrap_or(0);
                let hb_start = std::time::Instant::now();
                let shard_label = ctx.current_shard_id.to_string();
                metrics::counter!("celeriant_heartbeat_attempts_total", &[("shard_id", shard_label.clone())]).increment(1);
                if let Some(at_ms) = last_s3_lease_write_at_ms {
                    let age_s = unix_epoch_now_ms.saturating_sub(at_ms) as f64 / 1000.0;
                    metrics::gauge!("celeriant_s3_lease_age_seconds", &[("shard_id", shard_label.clone())]).set(age_s);
                }
                let lease_expires_at_ms = ctx.shard_wal.node_status.get().lease_expires_at_ms();
                let lease_remaining_ms = lease_expires_at_ms.saturating_sub(unix_epoch_now_ms) as f64;
                metrics::gauge!("celeriant_lease_remaining_ms", &[("shard_id", shard_label.clone()), ("role", "leader".to_string())]).set(lease_remaining_ms);
                // Detect leader self-fence (must_fence fired but raw still says Leader)
                let raw_status = ctx.shard_wal.node_status.get().raw();
                let effective_status = ctx.shard_wal.node_status.get().effective_node_status();
                if raw_status.is_leader() && !effective_status.is_leader() {
                    metrics::counter!("celeriant_leader_self_fence_total", &[("shard_id", shard_label.clone())]).increment(1);
                    warn!(shard_id = ctx.current_shard_id, ?raw_status, ?effective_status, lease_remaining_ms, "Leader self-fenced (must_fence fired) — TTL exhausted before next renewal");
                }

                // Mark heartbeat in-flight locally and broadcast to peers so any
                // shard's write path can back-pressure when the heartbeat hangs.
                // The mirror lives in each shard's local Cell — no cross-core
                // atomic on the hot read path.
                ctx.shard_wal.replication_client.set_heartbeat_in_flight(Some(unix_epoch_now_ms));
                broadcast_message_to_other_shards(
                    ctx.current_shard_id,
                    IntrashardMessages::HeartbeatInFlightStarted { unix_ms: unix_epoch_now_ms },
                    ctx.intrashard_sender.clone(),
                ).await;

                // Hard timeout: the internal heartbeat_timeout (500ms) relies on the
                // network stack cooperating, but under NIC saturation a kTLS send can
                // block in the kernel for 20+ seconds (TCP retransmit timeout). This
                // outer timeout ensures shard 0 isn't stuck and can proceed to lease
                // renewal promptly. We use 4x the heartbeat timeout as the hard cap.
                let hb_hard_timeout = ctx.config.heartbeat_timeout * ctx.config.heartbeat_hard_timeout_multiplier;
                let hb_hard_timed_out;
                let result = match glommio::timer::timeout(hb_hard_timeout, async {
                    Ok::<_, glommio::GlommioError<()>>(
                        ctx.shard_wal.replication_client.send_heartbeat(unix_epoch_now_ms, lease_epoch).await
                    )
                }).await {
                    Ok(inner) => { hb_hard_timed_out = false; inner },
                    Err(_) => {
                        warn!(shard_id = ctx.current_shard_id, elapsed_ms = hb_start.elapsed().as_millis() as u64, timeout_ms = hb_hard_timeout.as_millis() as u64, "Heartbeat hard timeout — kernel TCP send blocked");
                        metrics::counter!("celeriant_heartbeat_kernel_blocked_total", &[("shard_id", ctx.current_shard_id.to_string())]).increment(1);
                        hb_hard_timed_out = true;
                        Err(SendHeartbeatError::UnexpectedResponse)
                    }
                };
                let hb_elapsed_ms = hb_start.elapsed().as_millis() as u64;

                // Heartbeat path concluded (Ack/Reject/timeout/Err) — clear the
                // in-flight signal locally and broadcast.
                ctx.shard_wal.replication_client.set_heartbeat_in_flight(None);
                broadcast_message_to_other_shards(
                    ctx.current_shard_id,
                    IntrashardMessages::HeartbeatInFlightCleared,
                    ctx.intrashard_sender.clone(),
                ).await;

                if hb_hard_timed_out {
                    metrics::counter!("celeriant_heartbeat_outcomes_total", &[("shard_id", shard_label.clone()), ("outcome", "hard_timeout".to_string())]).increment(1);
                } else if let Err(SendHeartbeatError::LockTimeout) = &result {
                    metrics::counter!("celeriant_heartbeat_outcomes_total", &[("shard_id", shard_label.clone()), ("outcome", "lock_timeout".to_string())]).increment(1);
                    warn!("Heartbeat lock contention, skipping heartbeat");
                    continue;
                }

                let outcome_label = match &result {
                    Ok(HeartbeatResult::Ack { .. }) => "ack",
                    Ok(HeartbeatResult::Rejected(celeriant_msg::response::responses::HeartbeatRejection::NotAFollower)) => "rejected_not_follower",
                    Ok(HeartbeatResult::Rejected(celeriant_msg::response::responses::HeartbeatRejection::ClockDriftTooHigh { .. })) => "rejected_clock_drift",
                    Err(SendHeartbeatError::LockTimeout) => "lock_timeout",
                    Err(_) => "network_error",
                };
                if !hb_hard_timed_out {
                    metrics::counter!("celeriant_heartbeat_outcomes_total", &[("shard_id", shard_label.clone()), ("outcome", outcome_label.to_string())]).increment(1);
                }

                if let Err(ref e) = result {
                    warn!(shard_id = ctx.current_shard_id, elapsed_ms = hb_elapsed_ms, error = ?e, "Heartbeat send returned error");
                }
                if let Ok(ref r) = result {
                    if !matches!(r, HeartbeatResult::Ack { .. }) {
                        warn!(shard_id = ctx.current_shard_id, elapsed_ms = hb_elapsed_ms, result = ?r, "Heartbeat non-ack response");
                    }
                }

                if let Ok(HeartbeatResult::Ack { follower_can_accept_tcp_replication, .. }) = result {
                    metrics::counter!("celeriant_heartbeat_acks_total", &[("shard_id", shard_label.clone())]).increment(1);
                    has_peer = true;
                    peer_discovery_backoff = Duration::from_secs(1);
                    // Node is there on network but still hasn't joined the cluster as follower
                    let reachable = follower_can_accept_tcp_replication;
                    let was_reachable = ctx.shard_wal.replication_client.is_follower_reachable();
                    ctx.shard_wal.replication_client.set_follower_reachable(reachable);

                    let leader_for_ms = unix_epoch_now_ms.saturating_sub(ctx.shard_wal.node_status.get().leader_since_ms());
                    let transition_probe = should_fire_reachability_probe(was_reachable, reachable, ctx.shard_wal.node_status.get().is_leader(), leader_for_ms, MIN_PROBE_AFTER_LEADER_MS);
                    let periodic_probe = reachable
                        && ctx.shard_wal.node_status.get().is_leader()
                        && leader_for_ms >= MIN_PROBE_AFTER_LEADER_MS
                        && unix_epoch_now_ms.saturating_sub(last_probe_at_ms) >= PERIODIC_PROBE_INTERVAL_MS;
                    if transition_probe || periodic_probe {
                        debug!(shard_id = ctx.current_shard_id, transition_probe, periodic_probe, "Firing reconciliation probe");
                        last_probe_at_ms = unix_epoch_now_ms;
                        let shard_wal = ctx.shard_wal.clone();
                        glommio::spawn_local(async move {
                            if let Err(e) = shard_wal.probe_replicate().await {
                                debug!(error = ?e, "Reconciliation probe replication errored");
                            }
                        }).detach();
                    }
                    broadcast_message_to_other_shards(ctx.current_shard_id, IntrashardMessages::FollowerReachable { reachable, was_reachable }, ctx.intrashard_sender.clone()).await;
                    
                    let prev_expires_at_ms = ctx.shard_wal.node_status.get().lease_expires_at_ms();
                    let new_expires_at_ms = compute_new_ttl(
                        prev_expires_at_ms,
                        unix_epoch_now_ms,
                        ctx.config.heartbeat_lease_duration.as_millis() as u64,
                    );
                    debug!(
                        shard_id = ctx.current_shard_id,
                        prev_expires_at_ms,
                        new_expires_at_ms,
                        delta_ms = new_expires_at_ms as i64 - prev_expires_at_ms as i64,
                        now_ms = unix_epoch_now_ms,
                        "HeartbeatTtlRefresh: max-merging local lease via heartbeat ack (S3 lease NOT touched)",
                    );
                    let refreshed = ValidatedNodeStatus::create_custom_status(
                        ctx.shard_wal.node_status.get().raw(), ctx.config.max_clock_drift_ms, new_expires_at_ms);
                    set_node_status_and_metric(&ctx.shard_wal.node_status, refreshed, ctx.current_shard_id as u32);
                    // Heartbeat-ack path: cas_confirmed_at_ms=None — does NOT update the CAS signal.
                    // leader_changed_hands=false: a leader refreshing its own lease is not a promotion.
                    broadcast_message_to_other_shards(ctx.current_shard_id, IntrashardMessages::StatusUpdate { status: refreshed, cas_confirmed_at_ms: None, leader_changed_hands: false }, ctx.intrashard_sender.clone()).await;
                    continue;
                }

                let was_reachable = ctx.shard_wal.replication_client.is_follower_reachable();
                ctx.shard_wal.replication_client.set_follower_reachable(false);
                broadcast_message_to_other_shards(ctx.current_shard_id, IntrashardMessages::FollowerReachable { reachable: false, was_reachable }, ctx.intrashard_sender.clone()).await;
                metrics::counter!("celeriant_heartbeat_failures_total").increment(1);

                // Preemptive lease renewal: when follower just went unreachable, renew
                // the lease immediately to get a fresh TTL before the S3 replication
                // fallback storm from other shards saturates MinIO.
                if was_reachable && has_peer {
                    warn!("Follower just became unreachable — preemptive S3 lease renewal");
                    match set_node_role_via_s3(&lease_manager, &ctx, &rx, "preemptive").await {
                        Ok(outcome) => {
                            last_s3_lease_write_at_ms = Some(validated_node_status::unix_epoch_now_ms());
                            if let Some(ref peer) = outcome.peer_info {
                                info!(peer_replication_address = %peer.replication_address, "Preemptive renewal: peer confirmed via S3");
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Preemptive S3 lease renewal failed");
                        }
                    }
                }

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

                match set_node_role_via_s3(&lease_manager, &ctx, &rx, if has_peer { "proactive" } else { "discovery" }).await {
                    Ok(outcome) => {
                        last_s3_lease_write_at_ms = Some(validated_node_status::unix_epoch_now_ms());
                        if let Some(ref peer) = outcome.peer_info {
                            info!(peer_replication_address = %peer.replication_address, "Peer discovered via S3");
                            has_peer = true;
                            peer_discovery_backoff = Duration::from_secs(1);
                        } else {
                            warn!(next_backoff_ms = peer_discovery_backoff.as_millis() as u64, "No peer found in S3 membership");
                            has_peer = false;
                            last_peer_discovery_attempt = std::time::Instant::now();
                            peer_discovery_backoff = (peer_discovery_backoff * 2).min(half_s3_lease);
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "S3 lease renewal/election failed, panic");
                        panic!("Election failed after retries: {e}");
                    }
                }

                continue;
            }

            if ctx.shard_wal.node_status.get().is_follower() || ctx.shard_wal.node_status.get().is_fenced() {
                let effective = ctx.shard_wal.node_status.get().effective_node_status();
                let raw = ctx.shard_wal.node_status.get().raw();
                let now_ms_for_gauge = validated_node_status::unix_epoch_now_ms();
                let lease_remaining_ms = ctx.shard_wal.node_status.get().lease_expires_at_ms().saturating_sub(now_ms_for_gauge) as f64;
                metrics::gauge!("celeriant_lease_remaining_ms", &[("shard_id", ctx.current_shard_id.to_string()), ("role", "follower".to_string())]).set(lease_remaining_ms);
                if effective != raw {
                    metrics::counter!("celeriant_follower_auto_fence_total", &[("shard_id", ctx.current_shard_id.to_string())]).increment(1);
                    let should_warn = last_auto_fence_warn.is_none_or(|t| t.elapsed() >= Duration::from_secs(1));
                    if should_warn {
                        warn!(shard_id = ctx.current_shard_id, ?raw, ?effective, "Heartbeat TTL expired — auto-fenced");
                        last_auto_fence_warn = Some(std::time::Instant::now());
                    }
                } else {
                    last_auto_fence_warn = None;
                }

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

                if let Err(e) = set_node_role_via_s3(&lease_manager, &ctx, &rx, "challenge").await {
                    panic!("Election failed after retries: {e}");
                }
                last_s3_lease_write_at_ms = Some(validated_node_status::unix_epoch_now_ms());

                continue;
            }

            if ctx.shard_wal.node_status.get().raw().is_catching_up() {

                info!("Node was follower but got kicked, or we are in boot catchup phase, asking shards to catch up via s3");

                let catchup_outcome = run_s3_catchup(&ctx, &rx).await;
                if matches!(catchup_outcome, CatchupRunOutcome::Shutdown) {
                    panic!("S3 catchup failed with fatal error");
                }

                // Wait briefly only when another node's expired lease.bin is in S3, so
                // we can defer to its heartbeat if it's actually alive. 5s allows ~10 HB
                // attempts at the 500ms interval.
                const BOOT_GRACE_MAX_MS: u64 = 5_000;
                let status_now = ctx.shard_wal.node_status.get();
                let now_ms = validated_node_status::unix_epoch_now_ms();
                let self_node_id = lease_manager.node_id();
                let peeked_lease = lease_manager.peek_lease().await.ok().flatten();
                let needs_boot_grace = peeked_lease.as_ref().map_or(false, |lease| {
                    lease.leader_node_id != self_node_id && lease.is_expired(now_ms)
                });
                let boot_grace_ms = if needs_boot_grace {
                    (ctx.config.heartbeat_lease_duration.as_millis() as u64).min(BOOT_GRACE_MAX_MS)
                } else {
                    0
                };
                let lease_expires_at_ms = status_now.lease_expires_at_ms();
                let action = decide_post_catchup_action(
                    status_now.raw(),
                    lease_expires_at_ms,
                    now_ms,
                    boot_grace_ms,
                );
                match action {
                    PostCatchupAction::StayFollower { leader_lease_epoch, lease_expires_at_ms } => {
                        if matches!(catchup_outcome, CatchupRunOutcome::S3Unreachable) {
                            // Live leader heartbeating but S3 down: the normal resume (post-catchup
                            // election) reads S3, so without this we stay pinned in FollowerCatchingUp
                            // (can-accept-TCP=false) while the leader routes its tail to a dead S3
                            // fallback. Resume to Follower locally so TCP drains the tail; the
                            // receive-path tip-hash check rejects a genuine divergence (re-kicks us).
                            let follower = ValidatedNodeStatus::create_custom_status(
                                NodeStatus::Follower { leader_lease_epoch },
                                ctx.config.max_clock_drift_ms,
                                lease_expires_at_ms,
                            );
                            set_node_status_and_metric(&ctx.shard_wal.node_status, follower, ctx.current_shard_id as u32);
                            broadcast_message_to_other_shards(
                                ctx.current_shard_id,
                                IntrashardMessages::StatusUpdate { status: follower, cas_confirmed_at_ms: None, leader_changed_hands: false },
                                ctx.intrashard_sender.clone(),
                            ).await;
                            warn!(leader_lease_epoch, "S3 unreachable during catchup but live leader heartbeating; resumed as Follower for TCP-driven recovery");
                        } else {
                            info!("Post-catchup: lease alive (heartbeat received during catchup); not challenging");
                        }
                    }
                    PostCatchupAction::BootWaitThenReevaluate { wait_ms } => {
                        info!(wait_ms, "Post-catchup boot-grace wait before challenging");
                        // 100ms slices so we exit early if a heartbeat flips us to follower.
                        let start = std::time::Instant::now();
                        let total = Duration::from_millis(wait_ms);
                        loop {
                            let remaining = total.saturating_sub(start.elapsed());
                            if remaining.is_zero() { break; }
                            glommio::timer::sleep(Duration::from_millis(100).min(remaining)).await;
                            if !ctx.shard_wal.node_status.get().raw().is_catching_up() {
                                break;
                            }
                        }
                        if ctx.shard_wal.node_status.get().raw().is_catching_up() {
                            info!("Boot-grace wait elapsed without heartbeat; challenging via CAS");
                            if let Err(e) = set_node_role_via_s3(&lease_manager, &ctx, &rx, "post_catchup").await {
                                panic!("Post-catchup election failed after retries: {e}");
                            }
                            last_s3_lease_write_at_ms = Some(validated_node_status::unix_epoch_now_ms());
                        } else {
                            info!("Heartbeat arrived during boot-grace wait; following established leader");
                        }
                    }
                    PostCatchupAction::ChallengeViaCAS => {
                        if let Err(e) = set_node_role_via_s3(&lease_manager, &ctx, &rx, "post_catchup").await {
                            panic!("Post-catchup election failed after retries: {e}");
                        }
                        last_s3_lease_write_at_ms = Some(validated_node_status::unix_epoch_now_ms());
                    }
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
                message_version,
                ctx.clone(),
            );
        }
        IntrashardMessages::CullSpeculativeTail { rewind_to_ack_barrier } => {
            if let Err(e) = ctx.shard_wal.cull_speculative_tail_for_promotion(rewind_to_ack_barrier).await {
                tracing::warn!(shard_id = ctx.current_shard_id, error = ?e, "CullSpeculativeTail fsync failed — catchup may miss peer S3 batches");
            }
        }
        IntrashardMessages::RenewS3LeaseNow { requesting_shard } => {
            renew_s3_lease_on_demand(ctx, requesting_shard).await;
        }
        IntrashardMessages::EnterS3Catchup => handle_enter_s3_catchup(ctx.clone()),
        IntrashardMessages::S3CatchupComplete { shard_id, result } => {
            if let Some(tx) = &ctx.catchup_completion_tx {
                let _ = tx.try_send(CatchupCompletionMsg { result, shard_id });
            }
        }
        IntrashardMessages::StatusUpdate { status, cas_confirmed_at_ms, leader_changed_hands } => {
            let previous = ctx.shard_wal.node_status.get();
            let role_changed = !previous.raw().same_role(&status.raw());
            if role_changed {
                warn!(
                    shard_id = ctx.current_shard_id,
                    previous = ?previous.raw(),
                    new = ?status.raw(),
                    expires_at_ms = status.lease_expires_at_ms(),
                    "Node status transition"
                );
            }
            let was_leader = previous.raw().is_leader();
            let now_leader = status.raw().is_leader();
            set_node_status_and_metric(&ctx.shard_wal.node_status, status, ctx.current_shard_id as u32);
            if let Some(confirmed_at) = cas_confirmed_at_ms {
                ctx.shard_wal.s3_cas_confirmed_at_ms.set(confirmed_at);
            }
            if role_changed && was_leader && !now_leader {
                ctx.shard_wal.drain_pending_replication_on_role_change().await;
            }
            // Mirror the lease-handler's promotion-batch upload (shard.rs:579) for
            // shards 1..N which inherit lease via this broadcast. Without this,
            // entries received over TCP from the previous leader (and never
            // uploaded to S3) are stranded on this node's local disk: the old
            // leader rolled them back on resume, S3 has no record, and catchup
            // wedges with `Chain mismatch with no common ancestor`.
            //
            // Gated on leader_changed_hands to match shard 0's gate: on self-reclaim
            // the node kept its speculative tail, and upload_s3_promotion_batch's
            // idempotent re-cull (shard_wal.rs:2644) would rewind write→read and
            // discard that tail, re-creating the same-seq fork the peer holds.
            if should_upload_promotion_batch_on_status(role_changed, was_leader, now_leader, leader_changed_hands) {
                if let Err(e) = ctx.shard_wal.upload_s3_promotion_batch().await {
                    tracing::warn!(shard_id = ctx.current_shard_id, error = ?e, "Failed to upload promotion batch to S3 — old leader may not be able to catch up via S3");
                }
            } else if role_changed && !was_leader && now_leader {
                // Self-reclaim promotion: the gate above skipped the upload. Its re-cull
                // would have discarded the speculative tail we kept. This line is the
                // behavioral signal that the data-shard gate fired (anti-stale grep target).
                info!(shard_id = ctx.current_shard_id, "self-reclaim: keeping speculative tail, skipping data-shard promotion-batch upload");
            }
        }
        IntrashardMessages::UpdatePeerNodeId { peer_node_id } => {
            ctx.shard_wal.peer_node_id.set(peer_node_id);
        }
        IntrashardMessages::UpdateFollower { replication_address, peer_node_id } => {
            ctx.shard_wal.replication_client.set_follower_address(replication_address);
            ctx.shard_wal.peer_node_id.set(peer_node_id);
        }
        IntrashardMessages::FollowerReachable { reachable, was_reachable } => {
            // Use the leader's pre-transition view (was_reachable) rather than
            // this shard's local is_follower_reachable(), which may be out of
            // sync due to per-shard replication client state updates. This
            // ensures all shards fire the probe on the same transition edge.
            // leader_since_ms comes from node_status (propagated to every shard
            // via the StatusUpdate broadcast that fires on every heartbeat-ack).
            ctx.shard_wal.replication_client.set_follower_reachable(reachable);
            let leader_for_ms = validated_node_status::unix_epoch_now_ms()
                .saturating_sub(ctx.shard_wal.node_status.get().leader_since_ms());
            let is_leader = ctx.shard_wal.node_status.get().is_leader();
            let transition_probe = should_fire_reachability_probe(was_reachable, reachable, is_leader, leader_for_ms, MIN_PROBE_AFTER_LEADER_MS);
            if transition_probe {
                let shard_wal = ctx.shard_wal.clone();
                glommio::spawn_local(async move {
                    if let Err(e) = shard_wal.probe_replicate().await {
                        debug!(error = ?e, "Reconciliation probe replication errored");
                    }
                }).detach();
            }
        }
        IntrashardMessages::HeartbeatInFlightStarted { unix_ms } => {
            ctx.shard_wal.replication_client.set_heartbeat_in_flight(Some(unix_ms));
        }
        IntrashardMessages::HeartbeatInFlightCleared => {
            ctx.shard_wal.replication_client.set_heartbeat_in_flight(None);
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

const MIN_PROBE_AFTER_LEADER_MS: u64 = 2000;
/// Periodic-probe interval. After this many ms since the last probe, the
/// leader fires a `probe_replicate` on the next heartbeat ack as a
/// convergence safety net — closes the post-quiescence follower-tail gap
/// the transition-only probe misses (writers stop, follower is reachable,
/// no transition triggers the probe, follower stays behind forever).
const PERIODIC_PROBE_INTERVAL_MS: u64 = 5_000;

/// Compute cull and catchup flags for a role transition.
///
/// became_leader: transition from non-leader to leader.
/// became_follower_from_leader_or_fenced: transition from leader to non-leader.
/// reacquired_own_lease: from ElectionOutcome, a restart-proof S3 signal.
/// lease_changed_hands: epoch increased (a peer held the lease in between).
/// now_following_peer: new status is Follower/FollowerCatchingUp (a confirmed leader
/// exists), not Fenced.
///
/// Returns (needs_cull, leader_changed_hands, needs_catchup, rewind_to_ack_barrier).
fn promotion_cull_flags(
    became_leader: bool,
    became_follower_from_leader_or_fenced: bool,
    reacquired_own_lease: bool,
    lease_changed_hands: bool,
    now_following_peer: bool,
) -> (bool, bool, bool, bool) {
    let leader_changed_hands = became_leader && !reacquired_own_lease;
    // Cull whenever we follow a peer while non-leader (peer took a higher epoch, OR we were
    // stopped-while-leader and rejoin as follower via boot). NOT gated on lease_changed_hands: a
    // Follower outcome carries no lease_epoch (reads 0), so gating left the stale un-acked tail
    // unculled. A bare self-fence (following no one) stays excluded so a self-reclaim keeps its tail.
    let demoted_to_peer = now_following_peer && !reacquired_own_lease;
    let needs_cull = leader_changed_hands || became_follower_from_leader_or_fenced || demoted_to_peer;
    let needs_catchup = became_leader || lease_changed_hands;
    // Rewind read to last_self_acked only when demoting from leadership actually held; there it is
    // a real durable floor. A node that never led has last_self_acked=0, where rewinding wipes its
    // S3-caught-up chain. needs_cull still fires for it but only via the safe write->read arm.
    let rewind_to_ack_barrier = became_follower_from_leader_or_fenced;
    (needs_cull, leader_changed_hands, needs_catchup, rewind_to_ack_barrier)
}

/// Whether a data shard, on receiving a leader-promotion `StatusUpdate`, should
/// upload its promotion batch to S3. Mirrors shard 0's own gate (the orchestrator
/// only calls `upload_s3_promotion_batch` when `leader_changed_hands`).
///
/// Fires only on a genuine peer-takeover promotion (`!was_leader → now_leader`
/// with leadership changing hands). On self-reclaim (`leader_changed_hands=false`)
/// the node deliberately kept its speculative tail; the upload's idempotent
/// re-cull would rewind `write→read` and discard it, re-creating the same-seq
/// fork the peer still holds.
fn should_upload_promotion_batch_on_status(
    role_changed: bool,
    was_leader: bool,
    now_leader: bool,
    leader_changed_hands: bool,
) -> bool {
    role_changed && !was_leader && now_leader && leader_changed_hands
}

/// Returns true when a reconciliation probe should fire after a reachability update.
fn should_fire_reachability_probe(
    was_reachable: bool,
    reachable: bool,
    is_leader: bool,
    leader_for_ms: u64,
    min_leader_warmup_ms: u64,
) -> bool {
    !was_reachable && reachable && is_leader && leader_for_ms >= min_leader_warmup_ms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_fire_reachability_probe_truth_table() {
        // Only the false→true transition on a warm leader must fire.
        // (was_reachable, reachable, is_leader, leader_for_ms, min_warmup_ms, expected)
        let cases: &[(bool, bool, bool, u64, u64, bool)] = &[
            // Core transition cases (warm leader, 5s > 2s threshold)
            (false, true,  true,  5000, 2000, true),   // false→true, warm leader: FIRE
            (false, true,  false, 5000, 2000, false),  // false→true, follower: no fire
            (false, false, true,  5000, 2000, false),  // false→false, leader: no fire
            (false, false, false, 5000, 2000, false),  // false→false, follower: no fire
            (true,  true,  true,  5000, 2000, false),  // true→true (already reachable): no fire
            (true,  true,  false, 5000, 2000, false),  // true→true, follower: no fire
            (true,  false, true,  5000, 2000, false),  // true→false (going unreachable): no fire
            (true,  false, false, 5000, 2000, false),  // true→false, follower: no fire
            // Warmup gate cases
            (false, true,  true,     0, 2000, false),  // just-promoted (0ms): skip
            (false, true,  true,  1999, 2000, false),  // just under threshold: skip
            (false, true,  true,  2000, 2000, true),   // at threshold: FIRE
            (false, true,  true,  5000, 2000, true),   // warm: FIRE
        ];
        for &(was, now, leader, leader_for_ms, min_warmup_ms, expected) in cases {
            assert_eq!(
                should_fire_reachability_probe(was, now, leader, leader_for_ms, min_warmup_ms),
                expected,
                "was_reachable={was} reachable={now} is_leader={leader} leader_for_ms={leader_for_ms}",
            );
        }
    }

    /// Cull flags truth table for the promotion_cull_flags helper.
    ///
    /// Columns: became_leader, became_follower, reacquired_own_lease, lease_changed_hands,
    ///          now_following_peer
    /// Expected: (needs_cull, leader_changed_hands, needs_catchup, rewind_to_ack_barrier).
    /// rewind_to_ack_barrier == the became_follower column: rewind read to last_self_acked only
    /// when demoting from held leadership (§7.5).
    #[test]
    fn promotion_cull_flags_truth_table() {
        // (became_leader, became_follower, reacquired_own_lease, lease_changed_hands,
        //  now_following_peer, exp_needs_cull, exp_leader_changed_hands, exp_needs_catchup,
        //  exp_rewind_to_ack_barrier)
        let cases: &[(bool, bool, bool, bool, bool, bool, bool, bool, bool)] = &[
            // RESTART self-reclaim: previous_lease_epoch=0 (BootCatchup), S3 lease is ours
            // (epoch 1). reacquired_own_lease=true, so no cull (keep the tail).
            (true, false, true, true, false, false, false, true, false),

            // Warm self-reclaim (no restart): epoch stable, reacquired.
            (true, false, true, false, false, false, false, true, false),

            // Promote-over-peer: took over an expired/fenced peer lease.
            (true, false, false, true, false, true, true, true, false),

            // Promote-over-peer where epoch didn't change (shouldn't happen; verify).
            (true, false, false, false, false, true, true, true, false),

            // Demotion: leader lost lease to peer (Leader to Follower edge observed).
            (false, true, false, true, true, true, false, true, true),

            // Demotion: self-fenced (Leader to Fenced, lease not yet taken). Culls; no catchup.
            (false, true, false, false, false, true, false, false, true),

            // No role change (already leader, renewals): no cull, no catchup.
            (false, false, true, false, false, false, false, false, false),

            // Already non-leader, a higher-epoch peer took the lease and we now follow it
            // (self-fenced before challenging, so no Leader to Follower edge).
            // !reacquired_own_lease confirms a peer, so cull the stranded tail.
            (false, false, false, true, true, true, false, true, false),

            // Stop-while-leader rejoin as follower: cull the stale un-acked tail (write>read arm),
            // no catchup, no rewind (lease_changed_hands false; last_self_acked not a valid floor).
            (false, false, false, false, true, true, false, false, false),

            // guard: boot->follower after a clean S3 catchup (read==write, last_self_acked=0).
            // Same inputs; rewind MUST stay false or we wipe the caught-up chain (fork-from-genesis).
            (false, false, false, false, true, true, false, false, false),

            // Already non-leader, epoch advanced but not following anyone (Fenced): may
            // self-reclaim, so no cull. Catchup only.
            (false, false, false, true, false, false, false, true, false),

            // Non-leader, epoch advanced, but reacquired our own lease: never cull even if
            // momentarily following.
            (false, false, true, true, true, false, false, true, false),
        ];

        for &(became_leader, became_follower, reacquired, lease_changed, following, exp_cull, exp_lch, exp_catchup, exp_rewind) in cases {
            let (cull, lch, catchup, rewind) = promotion_cull_flags(became_leader, became_follower, reacquired, lease_changed, following);
            assert_eq!(
                (cull, lch, catchup, rewind),
                (exp_cull, exp_lch, exp_catchup, exp_rewind),
                "became_leader={became_leader} became_follower={became_follower} reacquired={reacquired} lease_changed={lease_changed} following={following}",
            );
        }
    }

    /// A data shard runs its promotion-batch upload (which re-culls the speculative
    /// tail) ONLY on a genuine peer-takeover promotion — never on self-reclaim,
    /// where the kept tail would be wrongly discarded into a same-seq fork.
    ///
    /// Columns: role_changed, was_leader, now_leader, leader_changed_hands -> expected
    #[test]
    fn should_upload_promotion_batch_on_status_truth_table() {
        let cases: &[(bool, bool, bool, bool, bool)] = &[
            // Self-reclaim: BootCatchup→Leader, leadership did NOT change hands. SKIP (the bug).
            (true, false, true, false, false),
            // Promote-over-peer: Follower→Leader, leadership changed hands. UPLOAD.
            (true, false, true, true, true),
            // Heartbeat refresh on an existing leader: no role change. SKIP.
            (false, true, true, true, false),
            // Demotion: Leader→Follower. SKIP (now_leader=false).
            (true, true, false, false, false),
            // Follower staying follower (heartbeat-ack). SKIP.
            (false, false, false, false, false),
        ];
        for &(role_changed, was_leader, now_leader, lch, expected) in cases {
            assert_eq!(
                should_upload_promotion_batch_on_status(role_changed, was_leader, now_leader, lch),
                expected,
                "role_changed={role_changed} was_leader={was_leader} now_leader={now_leader} leader_changed_hands={lch}",
            );
        }
    }
}