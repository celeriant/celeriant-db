use std::{cell::Cell, fmt, rc::Rc, time::Duration};

use celeriant_distributed::{heartbeat::now_ms, lease_manager::LeaseManager, lease_store::LeaseStore, node_status::NodeStatus, validated_node_status::ValidatedNodeStatus};
use celeriant_msg::{
    process_requests::Request,
    process_responses::Response,
    request::requests::{HeartbeatRequest, KickFollowerRequest, WatchRequest},
    response::responses::{ErrorResponse, HeartbeatRejection, HeartbeatResponse, HeartbeatResult, KickFollowerResponse, WatchResponse},
};
use celeriant_shard::{
    error::{s3_catchup_error::S3CatchupError, watch_session_error::WatchSessionError}, replication_client::ReplicationClient, s3_downloader::S3Downloader, shard_wal::ShardWal, shard_wal_s3_catchup::S3CatchupResult
};
use celeriant_wal::compression_type::CompressionType;
use celeriant_watch::{watch_output_type::WatchOutputType, watch_session::WatchSession};
use celeriant_msg::read_wire_data_error::ReadWireDataError;
use celeriant_wire::network::wire_error::WireError;
use glommio::{channels::{channel_mesh::Senders, local_channel::LocalSender}, net::TcpStream};
use tracing::{info, warn};

use super::{
    intrashard_messages::IntrashardMessages,
    shard_config::ShardConfig,
    shard_error_response::{shard_error_to_response, watch_read_error_to_response, watch_session_error_to_response, IDENTIFY_INVALID_NONCE, IDENTIFY_INVALID_SIGNATURE, IDENTIFY_MISMATCH, IDENTIFY_REQUIRED},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortType {
    Client,
    Replication,
}

pub struct CatchupCompletionMsg {
    pub shard_id: usize,
    pub result: Result<S3CatchupResult, S3CatchupError>,
}

pub struct ConnectionContext<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static> {
    pub config: Rc<ShardConfig>,
    pub current_shard_id: usize,
    pub intrashard_sender: Rc<Senders<IntrashardMessages>>,
    pub shutdown_requested: Rc<Cell<bool>>,
    pub shard_wal: Rc<ShardWal<R, D>>,
    pub catchup_completion_tx: Option<Rc<LocalSender<CatchupCompletionMsg>>>,
    pub lease_manager: Option<Rc<LeaseManager<S>>>,
}

/// Connection-level state for identity verification
struct ConnectionState {
    verified_client_id: Option<u128>,
}

impl<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static> Clone for ConnectionContext<R, D, S> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            current_shard_id: self.current_shard_id,
            intrashard_sender: self.intrashard_sender.clone(),
            shutdown_requested: self.shutdown_requested.clone(),
            shard_wal: self.shard_wal.clone(),
            catchup_completion_tx: self.catchup_completion_tx.clone(),
            lease_manager: self.lease_manager.clone(),
        }
    }
}

#[derive(Debug)]
pub enum ShardRoutingError {
    NoRoutingKeyProvided,
    MultipleShardRoutes,
    IncompatibleFilters(String),
}

impl fmt::Display for ShardRoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardRoutingError::NoRoutingKeyProvided => write!(f, "no routing key provided"),
            ShardRoutingError::MultipleShardRoutes => write!(f, "request routes to multiple shards"),
            ShardRoutingError::IncompatibleFilters(details) => write!(f, "incompatible filters: {}", details),
        }
    }
}

enum RedirectResult {
    ProcessLocally(Request, TcpStream),
    Redirected,
    ErrorSentContinue(TcpStream),
}

pub fn handle_new_connection<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(mut tcp_stream: TcpStream, ctx: ConnectionContext<R, D, S>, port_type: PortType) {
    let _ = tcp_stream.set_nodelay(true);

    glommio::spawn_local(async move {
        if ctx.shutdown_requested.get() {
            return;
        }

        // Initialize connection state (no identity verified yet)
        let mut conn_state = ConnectionState {
            verified_client_id: None,
        };

        let (request, message_version) = match read_request(&mut tcp_stream, &ctx).await {
            Some(r) => r,
            None => return,
        };

        // Reject non-identity first messages when identity is required
        if ctx.config.require_client_identity && !matches!(request, Request::Identify(_)) {
            let response = Response::GenericError(ErrorResponse {
                correlation_id: request.correlation_id(),
                error_code: IDENTIFY_REQUIRED,
                error_message: "Server requires client identity verification".to_string(),
            });
            let _ = write_response(&mut tcp_stream, &response, ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
            return;
        }

        // Handle IdentifyRequest at connection level (before redirect/processing)
        if let Request::Identify(ref identify_req) = request {
            if let Err(response) = handle_identify(identify_req, &mut conn_state) {
                let _ = write_response(&mut tcp_stream, &response, ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
                return; // Close connection on identity failure
            }
            let response = Response::Identify(celeriant_msg::response::responses::IdentifyResponse {
                correlation_id: identify_req.correlation_id,
                client_id: conn_state.verified_client_id.unwrap(),
            });
            if write_response(&mut tcp_stream, &response, ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await.is_err() {
                return;
            }
            // Read next message after successful identity verification
            let (next_request, next_version) = match read_request(&mut tcp_stream, &ctx).await {
                Some(r) => r,
                None => return,
            };
            match check_redirect(tcp_stream, next_request, ctx.config.max_response_size, ctx.config.server_compression_algorithm, next_version, &ctx, port_type, &conn_state).await {
                RedirectResult::ProcessLocally(request, tcp_stream) => {
                    handle_pipelining(tcp_stream, request, ctx.config.max_request_size, ctx.config.max_response_size, ctx.config.server_compression_algorithm, next_version, ctx, port_type, conn_state).await;
                }
                RedirectResult::Redirected => {}
                RedirectResult::ErrorSentContinue(_) => {}
            }
            return;
        }

        match check_redirect(tcp_stream, request, ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, &ctx, port_type, &conn_state).await {
            RedirectResult::ProcessLocally(request, tcp_stream) => {
                handle_pipelining(tcp_stream, request, ctx.config.max_request_size, ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, ctx, port_type, conn_state).await;
            }
            RedirectResult::Redirected => {}
            RedirectResult::ErrorSentContinue(_) => {}
        }
    })
    .detach();
}

pub fn handle_redirected_connection<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: TcpStream,
    request: Request,
    max_request_size: u64,
    max_response_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
    ctx: ConnectionContext<R, D, S>,
    port_type: PortType,
    verified_client_id: Option<u128>,
) {
    let _ = tcp_stream.set_nodelay(true);

    glommio::spawn_local(async move {
        let conn_state = ConnectionState {
            verified_client_id,
        };
        handle_pipelining(tcp_stream, request, max_request_size, max_response_size, server_compression_algorithm, message_version, ctx, port_type, conn_state).await;
    })
    .detach();
}

pub fn handle_enter_s3_catchup<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: ConnectionContext<R, D, S>,
) {
    glommio::spawn_local(async move {        
        let result = ctx.shard_wal.enter_s3_catchup().await;

        let _ = ctx.intrashard_sender.send_to(
            0, IntrashardMessages::S3CatchupComplete { shard_id: ctx.current_shard_id, result }
        ).await;
    })
    .detach();
}

async fn handle_pipelining<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    mut tcp_stream: TcpStream,
    request: Request,
    max_request_size: u64,
    max_response_size: u64,
    server_compression_algorithm: CompressionType,
    mut message_version: u32,
    ctx: ConnectionContext<R, D, S>,
    port_type: PortType,
    conn_state: ConnectionState,
) {
    let mut optional_request = Some(request);

    loop {
        if ctx.shutdown_requested.get() {
            break;
        }

        if let Some(request) = optional_request.take() {
            if !is_valid_for_port(&request, port_type) {
                let response = Response::GenericError(ErrorResponse {
                    correlation_id: request.correlation_id(),
                    error_code: 400,
                    error_message: format!(
                        "Request type {:?} is not allowed on the {} port",
                        request.request_type(),
                        match port_type {
                            PortType::Client => "client",
                            PortType::Replication => "replication",
                        }
                    ),
                });
                let _ = write_response(&mut tcp_stream, &response, max_response_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
                continue;
            }

            // Validate client_id for mutating requests
            if let Err(response) = validate_client_id(&request, conn_state.verified_client_id) {
                let _ = write_response(&mut tcp_stream, &response, max_response_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
                continue;
            }

            if port_type == PortType::Client {
                if let Request::Watch(watch_request) = request {
                    handle_watch(tcp_stream, watch_request, max_response_size, server_compression_algorithm, message_version, &ctx).await;
                    return;
                }
            }

            process_request(&mut tcp_stream, &ctx, request, max_request_size, server_compression_algorithm, message_version).await;
        }

        if ctx.shutdown_requested.get() {
            break;
        }

        match read_request(&mut tcp_stream, &ctx).await {
            Some((next_request, next_version)) => {
                optional_request = Some(next_request);
                message_version = next_version;
            }
            None => return,
        }

        if ctx.shutdown_requested.get() {
            break;
        }

        if let Some(request) = optional_request.take() {
            match check_redirect(tcp_stream, request, max_response_size, server_compression_algorithm, message_version, &ctx, port_type, &conn_state).await {
                RedirectResult::ProcessLocally(req, stream) => {
                    optional_request = Some(req);
                    tcp_stream = stream;
                }
                RedirectResult::Redirected => return,
                RedirectResult::ErrorSentContinue(stream) => {
                    tcp_stream = stream;
                    continue;
                }
            }
        }
    }
}

fn is_valid_for_port(request: &Request, port_type: PortType) -> bool {
    match port_type {
        PortType::Client => request.is_client_port_request(),
        PortType::Replication => request.is_replication_port_request(),
    }
}

/// Handle IdentifyRequest during connection handshake.
/// Validates nonce and signature, derives client_id, stores in connection state.
fn handle_identify(req: &celeriant_msg::request::requests::IdentifyRequest, conn_state: &mut ConnectionState) -> Result<(), Response> {
    use celeriant_crypto::Crypto;

    // Validate nonce and signature, derive client_id
    let client_id = match Crypto::validate_with_public_key(&req.public_key, &req.nonce, &req.signature) {
        Ok(id) => id,
        Err(celeriant_crypto::CryptoError::InvalidNonce) => {
            return Err(Response::GenericError(ErrorResponse {
                correlation_id: req.correlation_id,
                error_code: IDENTIFY_INVALID_NONCE,
                error_message: "Nonce expired or too far in the future".to_string(),
            }));
        }
        Err(_) => {
            return Err(Response::GenericError(ErrorResponse {
                correlation_id: req.correlation_id,
                error_code: IDENTIFY_INVALID_SIGNATURE,
                error_message: "Invalid signature".to_string(),
            }));
        }
    };

    // Store verified client_id in connection state
    conn_state.verified_client_id = Some(client_id);
    Ok(())
}

/// Validate that a request's client_id matches the connection's verified identity.
/// Returns error response if verification failed.
fn validate_client_id(request: &Request, verified_client_id: Option<u128>) -> Result<(), Response> {
    let Some(verified) = verified_client_id else {
        return Ok(()); // No identity verification on this connection
    };

    let request_client_id = match request {
        Request::Write(req) => Some(req.client_id),
        Request::TrimStart(req) => Some(req.client_id),
        Request::Delete(req) => Some(req.client_id),
        _ => None, // Read operations don't carry client_id
    };

    if let Some(claimed) = request_client_id {
        if claimed != verified {
            return Err(Response::GenericError(ErrorResponse {
                correlation_id: request.correlation_id(),
                error_code: IDENTIFY_MISMATCH,
                error_message: format!(
                    "client_id mismatch: request has {}, connection verified as {}",
                    claimed, verified
                ),
            }));
        }
    }

    Ok(())
}

async fn check_redirect<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    mut tcp_stream: TcpStream,
    request: Request,
    max_message_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
    ctx: &ConnectionContext<R, D, S>,
    port_type: PortType,
    conn_state: &ConnectionState,
) -> RedirectResult {
    let target_shard = match determine_shard(&request, &ctx.config, port_type) {
        Ok(idx) => idx,
        Err(e) => {
            let response = Response::GenericError(ErrorResponse {
                correlation_id: request.correlation_id(),
                error_code: 400,
                error_message: format!("Shard routing error: {}", e),
            });
            let _ = write_response(&mut tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
            return RedirectResult::ErrorSentContinue(tcp_stream);
        }
    };

    if target_shard != ctx.current_shard_id {
        let msg = IntrashardMessages::ConnectionRedirect {
            accepted_tcp_stream: tcp_stream.into_accepted(),
            request,
            message_version,
            port_type,
            verified_client_id: conn_state.verified_client_id,
        };
        if let Err(e) = ctx.intrashard_sender.send_to(target_shard, msg).await {
            warn!("Failed to redirect connection to shard {target_shard}: {e:?}");
        }
        return RedirectResult::Redirected;
    }

    RedirectResult::ProcessLocally(request, tcp_stream)
}

pub fn determine_shard(
    request: &Request,
    config: &ShardConfig,
    port_type: PortType,
) -> Result<usize, ShardRoutingError> {
    let num_shards = config.num_shards as u128;

    match request {
        Request::ReplicationBatch(req) => validate_shard_id(req.shard_id, num_shards),
        Request::CatchUp(req) => validate_shard_id(req.shard_id, num_shards),
        Request::KickFollower(_) => Ok(0),
        Request::Watch(req) if port_type == PortType::Client => {
            determine_shard_watch(req, config)
        }
        Request::Write(req) => determine_shard_write(req, config),
        Request::Delete(req) => determine_shard_delete(req, config),
        Request::ListOrgs(req) => validate_shard_id(req.shard_id, num_shards),
        Request::ListAggregateTypes(req) => validate_shard_id(req.shard_id, num_shards),
        Request::ListAggregates(req) => validate_shard_id(req.shard_id, num_shards),
        other => {
            let routing_id = config.routing_rule.routing_id_for_request(other);
            Ok((routing_id % num_shards) as usize)
        }
    }
}

fn validate_shard_id(shard_id: u64, num_shards: u128) -> Result<usize, ShardRoutingError> {
    if (shard_id as u128) >= num_shards {
        return Err(ShardRoutingError::IncompatibleFilters(format!(
            "Invalid shard_id {}. Must be less than {} (total number of shards).",
            shard_id, num_shards
        )));
    }
    Ok(shard_id as usize)
}

fn determine_shard_write(
    req: &celeriant_msg::request::requests::WriteRequest,
    config: &ShardConfig,
) -> Result<usize, ShardRoutingError> {
    let num_shards = config.num_shards as u128;

    if req.writes.is_empty() {
        return Err(ShardRoutingError::IncompatibleFilters(
            "Write request must contain at least one write operation.".into(),
        ));
    }

    let mut shard_ids: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for aggregate_key in req.writes.keys() {
        let routing_id = config.routing_rule.routing_id_for_rule(aggregate_key);
        shard_ids.insert((routing_id % num_shards) as usize);
    }

    if shard_ids.len() > 1 {
        return Err(ShardRoutingError::IncompatibleFilters(format!(
            "Write request spans multiple shards. All writes must route to the same shard when using {} routing.",
            config.routing_rule
        )));
    }

    Ok(shard_ids.into_iter().next().unwrap())
}

fn determine_shard_delete(
    req: &celeriant_msg::request::requests::DeleteRequest,
    config: &ShardConfig,
) -> Result<usize, ShardRoutingError> {
    let num_shards = config.num_shards as u128;

    if req.deletes.is_empty() {
        return Err(ShardRoutingError::IncompatibleFilters(
            "Delete request must contain at least one delete operation.".into(),
        ));
    }

    let mut shard_ids: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for aggregate_key in req.deletes.keys() {
        let routing_id = config.routing_rule.routing_id_for_rule(aggregate_key);
        shard_ids.insert((routing_id % num_shards) as usize);
    }

    if shard_ids.len() > 1 {
        return Err(ShardRoutingError::IncompatibleFilters(format!(
            "Delete request spans multiple shards. All delete must route to the same shard when using {} routing.",
            config.routing_rule
        )));
    }

    Ok(shard_ids.into_iter().next().unwrap())
}

fn determine_shard_watch(
    req: &WatchRequest,
    config: &ShardConfig,
) -> Result<usize, ShardRoutingError> {
    let num_shards = config.num_shards as u128;

    match config.routing_rule {
        crate::RoutingRule::OrgId => {
            if req.orgs.is_none() || req.orgs.as_ref().unwrap().is_empty() {
                return Err(ShardRoutingError::IncompatibleFilters(
                    "Must specify at least one organisation. Server is setup to shard by organisation.".into()
                ));
            }
            collect_unique_shard_id(num_shards, &req.orgs)
        }
        crate::RoutingRule::AggregateTypeId => {
            if req.aggregate_types.is_none() || req.aggregate_types.as_ref().unwrap().is_empty() {
                return Err(ShardRoutingError::IncompatibleFilters(
                    "Must specify at least one aggregate type. Server is setup to shard by aggregate type.".into()
                ));
            }
            collect_unique_shard_id(num_shards, &req.aggregate_types)
        }
        crate::RoutingRule::AggregateId => {
            if req.aggregates.is_none() || req.aggregates.as_ref().unwrap().is_empty() {
                return Err(ShardRoutingError::IncompatibleFilters(
                    "Must specify at least one aggregate. Server is setup to shard by aggregate.".into()
                ));
            }
            collect_unique_shard_id(num_shards, &req.aggregates)
        }
    }
}

fn collect_unique_shard_id(
    num_shards: u128,
    sources: &Option<std::collections::HashSet<u128>>,
) -> Result<usize, ShardRoutingError> {
    let set = sources.as_ref().ok_or(ShardRoutingError::NoRoutingKeyProvided)?;
    let mut shard_id: Option<usize> = None;

    for &routing_key in set.iter() {
        let computed_shard = (routing_key % num_shards) as usize;
        match shard_id {
            None => shard_id = Some(computed_shard),
            Some(existing) if existing != computed_shard => {
                return Err(ShardRoutingError::MultipleShardRoutes);
            }
            Some(_) => {}
        }
    }

    shard_id.ok_or(ShardRoutingError::NoRoutingKeyProvided)
}

async fn read_request<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: &mut TcpStream,
    ctx: &ConnectionContext<R, D, S>,
) -> Option<(Request, u32)> {
    match glommio::timer::timeout(ctx.config.slow_client_timeout, async {
        let result = Request::read_request(tcp_stream, ctx.config.max_request_size).await;
        Ok::<_, glommio::GlommioError<()>>(result)
    })
    .await
    {
        Ok(Ok(result)) => Some(result),
        Ok(Err(ReadWireDataError::ReadHeaderFailure(WireError::NetworkError(ref e))))
            if e.kind() == std::io::ErrorKind::UnexpectedEof => None,
        Ok(Err(e)) => {
            warn!(shard = ctx.current_shard_id, "Failed to read request: {e:?}");
            None
        }
        Err(_) => {
            warn!("Client timed out reading request");
            None
        }
    }
}

async fn write_response(
    tcp_stream: &mut TcpStream,
    response: &Response,
    max_message_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
    timeout_duration: Duration,
) -> Result<(), WireError> {
    let compression = Response::determine_compression_type(response, server_compression_algorithm);
    match glommio::timer::timeout(timeout_duration, async {
        let result = Response::write_response(tcp_stream, response, compression, max_message_size, message_version).await;
        Ok::<_, glommio::GlommioError<()>>(result)
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(WireError::NetworkError(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out"))),
    }
}

async fn process_request<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: &mut TcpStream,
    ctx: &ConnectionContext<R, D, S>,
    request: Request,
    max_message_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
) {
    // Heartbeat and KickFollower are intercepted here rather than in ShardWal because
    // they need intrashard broadcast access (TTL refresh / kick coordination).
    if let Request::Heartbeat(ref heartbeat_req) = request {
        let response = handle_heartbeat(heartbeat_req, ctx).await;
        let _ = write_response(tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
        return;
    }
    if let Request::KickFollower(ref kick_req) = request {
        let response = handle_kick_follower(kick_req, ctx).await;
        let _ = write_response(tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
        return;
    }

    let correlation_id = request.correlation_id();
    let response = match ctx.shard_wal.process_request(request).await {
        Ok(result) => result,
        Err(error) => shard_error_to_response(correlation_id, error),
    };
    let _ = write_response(tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
}

/// Follower shard 0 heartbeat handler.
///
/// Validates role and clock drift, then refreshes the TTL on all local shards
/// so they don't self-fence via ValidatedNodeStatus::effective().
async fn handle_heartbeat<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    req: &HeartbeatRequest,
    ctx: &ConnectionContext<R, D, S>,
) -> Response {
    let follower_ms = now_ms();

    if !ctx.shard_wal.node_status.get().is_any_follower_state() {
        return Response::Heartbeat(HeartbeatResponse {
            correlation_id: req.correlation_id,
            result: HeartbeatResult::Rejected(HeartbeatRejection::NotAFollower),
        });
    }

    // Clock drift too high — nodes' clocks are dangerously skewed. Fence all local
    // shards immediately rather than waiting for TTL expiry, because we can't trust
    // any time-based decisions with skewed clocks.
    let drift = follower_ms.abs_diff(req.leader_timestamp_ms);
    if drift > ctx.config.max_cluster_time_drift_ms {
        let fenced = ValidatedNodeStatus::fenced();
        ctx.shard_wal.node_status.set(fenced);
        broadcast_status(ctx, fenced).await;

        return Response::Heartbeat(HeartbeatResponse {
            correlation_id: req.correlation_id,
            result: HeartbeatResult::Rejected(HeartbeatRejection::ClockDriftTooHigh {
                leader_ms: req.leader_timestamp_ms,
                follower_ms,
                max_allowed_ms: ctx.config.max_cluster_time_drift_ms,
            }),
        });
    }

    let status_ttl_ms = match &ctx.config.replication_config {
        Some(rc) => rc.status_ttl_ms(),
        None => 5000,
    };
    let new_expires_at = follower_ms + status_ttl_ms;
    let current_status = ctx.shard_wal.node_status.get().raw();
    let refreshed = ValidatedNodeStatus::new(current_status, new_expires_at);
    ctx.shard_wal.node_status.set(refreshed);
    broadcast_status(ctx, refreshed).await;

    Response::Heartbeat(HeartbeatResponse {
        correlation_id: req.correlation_id,
        result: HeartbeatResult::Ack { follower_timestamp_ms: follower_ms },
    })
}

/// Handle KickFollower from the leader. Always routed to shard 0.
/// Transitions to FollowerCatchingUp and broadcasts to all shards.
async fn handle_kick_follower<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    req: &KickFollowerRequest,
    ctx: &ConnectionContext<R, D, S>,
) -> Response {
    let status = ctx.shard_wal.node_status.get();
    if !status.is_any_follower_state() {
        return Response::KickFollower(KickFollowerResponse {
            correlation_id: req.correlation_id,
            acknowledged: false,
        });
    }

    // Only transition from Follower (already catching up → ignore duplicate kicks)
    if status.raw().is_follower() {
        let leader_lease_index = status.raw().lease_index_for_logging();
        let catching_up = ValidatedNodeStatus::new(
            NodeStatus::FollowerCatchingUp { leader_lease_index }, 0,
        );
        ctx.shard_wal.node_status.set(catching_up);
        broadcast_status(ctx, catching_up).await;
        info!("Kicked by leader — transitioning to FollowerCatchingUp");
    }

    Response::KickFollower(KickFollowerResponse {
        correlation_id: req.correlation_id,
        acknowledged: true,
    })
}

/// Broadcast a status update to all other local shards via intrashard mesh.
async fn broadcast_status<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: &ConnectionContext<R, D, S>,
    status: ValidatedNodeStatus,
) {
    let shard_count = ctx.intrashard_sender.nr_consumers();
    for peer in 0..shard_count {
        if peer == ctx.current_shard_id {
            continue;
        }
        let _ = ctx.intrashard_sender.send_to(
            peer, IntrashardMessages::StatusUpdate { status }
        ).await;
    }
}

async fn handle_watch<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    mut tcp_stream: TcpStream,
    watch_request: WatchRequest,
    max_message_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
    ctx: &ConnectionContext<R, D, S>,
) {
    let correlation_id = watch_request.correlation_id;

    let mut watch_session = match create_watch_session(&ctx.shard_wal, watch_request, ctx.config.max_requested_latency) {
        Ok(session) => session,
        Err(error) => {
            let response = watch_session_error_to_response(correlation_id, error);
            let _ = write_response(&mut tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
            return;
        }
    };

    loop {
        match watch_session.next().await {
            Ok(WatchOutputType::Continue) => continue,
            Ok(WatchOutputType::Response(watch_response)) => {
                let response = Response::Watch(watch_response);
                if write_response(&mut tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await.is_err() {
                    break;
                }
            }
            Ok(WatchOutputType::Heartbeat) => {
                let response = Response::Watch(WatchResponse { events: None });
                if write_response(&mut tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await.is_err() {
                    break;
                }
            }
            Ok(WatchOutputType::Done) => break,
            Err(error) => {
                let response = watch_read_error_to_response(correlation_id, error);
                let _ = write_response(&mut tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
                break;
            }
        }
    }
}

fn create_watch_session<R: ReplicationClient + 'static, D: S3Downloader + 'static>(
    shard_wal: &Rc<ShardWal<R, D>>,
    request: WatchRequest,
    max_requested_latency: Duration,
) -> Result<WatchSession<ShardWal<R, D>>, WatchSessionError> {
    if let Some(latency_ms) = request.requested_latency_ms
        && Duration::from_millis(latency_ms) > max_requested_latency
    {
        return Err(WatchSessionError::WatchLatencyTooHigh {
            latency_ms,
            max_latency_ms: max_requested_latency.as_millis() as u64,
        });
    }

    let (watcher_id, subscribed_client) = shard_wal.watched_aggregates.add_subscriber(request);
    Ok(WatchSession::new(watcher_id, subscribed_client, shard_wal.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_msg::request::{
        read_filters::ReadFilters,
        requests::{
            CatchUpRequest, DeleteRequest, AggregateDetailsRequest, ListAggregateTypesRequest,
            ListAggregatesRequest, ListOrgsRequest, ReadRequest, ReplicationBatchRequest,
            SingleAggregateDelete, SingleAggregateWrite, TrimStartRequest, WriteRequest,
        },
    };
    use celeriant_shard::timestamp_config::TimestampPrecision;
    use celeriant_wal::{aggregate_key::AggregateKey, compression_type::CompressionType};
    use std::collections::HashMap;

    fn test_config(num_shards: u32, routing_rule: crate::RoutingRule) -> ShardConfig {
        ShardConfig {
            node_id: 1,
            num_shards,
            s3_download_max_rounds: 3,
            replication_config: None,
            advertised_replication_address: None,
            data_root: "/tmp".into(),
            listen_address: "127.0.0.1".into(),
            client_port: 8080,
            replication_port: 8081,
            max_open_files: 100,
            read_max_chunk_size: 1024,
            write_max_chunk_size: 1024,
            max_request_size: 1024,
            max_response_size: 1024,
            server_compression_algorithm: CompressionType::Snappy,
            slow_client_timeout: Duration::from_secs(30),
            max_requested_latency: Duration::from_millis(100),
            shard_log_preallocate_bytes: 1024,
            fsync_delay: Duration::from_millis(10),
            recent_write_cache_bytes: 1024,
            routing_rule,
            aggregate_client_snapshots_cache_bytes: 1024,
            aggregate_snapshots_cache_bytes: 1024,
            timestamp_config: celeriant_shard::timestamp_config::TimestampConfig {
                precision: TimestampPrecision::Milliseconds,
                epoch_offset_secs: 0,
            },
            list_max_duration: Duration::from_secs(10),
            list_page_size: 100,
            list_wal_index_cache_bytes: 1024,
            pending_replication_high_water_bytes: 67_108_864, // 64MB
            replication_delay: Duration::from_millis(20),
            max_cluster_time_drift_ms: 5000,
            max_catchup_gap_bytes: 104_857_600,
            internode_connection_timeout: None,
            internode_request_timeout: Duration::from_secs(10),
            max_s3_fallback_batch_bytes: 1024 * 1024 * 100,
            tls_config: None,
            tls_cert_paths: None,
            tls_client_auth: celeriant_crypto::pki::ClientAuthMode::None,
            tls_cert_reload_interval: Duration::ZERO,
            require_client_identity: false,
        }
    }

    #[test]
    fn port_validation_client_requests() {
        let exists = Request::AggregateDetails(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(1, 1, 1),
        });
        assert!(is_valid_for_port(&exists, PortType::Client));
        assert!(!is_valid_for_port(&exists, PortType::Replication));

        let read = Request::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(1, 1, 1),
            filters: ReadFilters::default(),
        });
        assert!(is_valid_for_port(&read, PortType::Client));
        assert!(!is_valid_for_port(&read, PortType::Replication));
    }

    #[test]
    fn port_validation_replication_requests() {
        let repl = Request::ReplicationBatch(ReplicationBatchRequest {
            correlation_id: None,
            shard_id: 0,
            batches: vec![],
            leader_timestamp_ms: 0,
        });
        assert!(!is_valid_for_port(&repl, PortType::Client));
        assert!(is_valid_for_port(&repl, PortType::Replication));
    }

    #[test]
    fn routing_exists_request_by_aggregate_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::AggregateDetails(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(100, 200, 7),
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 3); // 7 % 4 = 3
    }

    #[test]
    fn routing_exists_request_by_org_id() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let request = Request::AggregateDetails(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(5, 200, 7),
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 1); // 5 % 4 = 1
    }

    #[test]
    fn routing_read_request_by_aggregate_type() {
        let config = test_config(3, crate::RoutingRule::AggregateTypeId);
        let request = Request::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(1, 8, 1),
            filters: ReadFilters::default(),
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 2); // 8 % 3 = 2
    }

    #[test]
    fn routing_replication_uses_explicit_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::ReplicationBatch(ReplicationBatchRequest {
            correlation_id: None,
            shard_id: 2,
            leader_timestamp_ms: 0,
            batches: vec![],
        });
        let shard = determine_shard(&request, &config, PortType::Replication).unwrap();
        assert_eq!(shard, 2);
    }

    #[test]
    fn routing_replication_invalid_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::ReplicationBatch(ReplicationBatchRequest {
            correlation_id: None,
            shard_id: 10,
            leader_timestamp_ms: 0,
            batches: vec![],
        });
        let result = determine_shard(&request, &config, PortType::Replication);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters(_))));
    }

    #[test]
    fn routing_catchup_uses_explicit_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::CatchUp(CatchUpRequest {
            correlation_id: None,
            shard_id: 2,
            last_follower_metablock: None,
            follower_tip_hash: None,
        });
        let shard = determine_shard(&request, &config, PortType::Replication).unwrap();
        assert_eq!(shard, 2);
    }

    #[test]
    fn routing_catchup_invalid_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::CatchUp(CatchUpRequest {
            correlation_id: None,
            shard_id: 10,
            last_follower_metablock: None,
            follower_tip_hash: None,
        });
        let result = determine_shard(&request, &config, PortType::Replication);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters(_))));
    }

    #[test]
    fn routing_list_orgs_uses_explicit_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::ListOrgs(ListOrgsRequest {
            correlation_id: None,
            shard_id: 3,
            cursor: None,
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 3);
    }

    #[test]
    fn routing_list_aggregate_types_uses_explicit_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::ListAggregateTypes(ListAggregateTypesRequest {
            correlation_id: None,
            shard_id: 1,
            org_id: Some(100),
            cursor: None,
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 1);
    }

    #[test]
    fn routing_list_aggregates_uses_explicit_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::ListAggregates(ListAggregatesRequest {
            correlation_id: None,
            shard_id: 0,
            org_id: Some(100),
            aggregate_type_id: Some(200),
            cursor: None,
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 0);
    }

    #[test]
    fn routing_write_single_aggregate() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let key = AggregateKey::new(1, 2, 11);
        let mut writes = HashMap::new();
        writes.insert(key, SingleAggregateWrite {
            expected_event_batch_index: None,
            allow_create: true,
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
            events: vec![],
        });
        let request = Request::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes,
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 3); // 11 % 4 = 3
    }

    #[test]
    fn routing_write_multiple_aggregates_same_shard() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let mut writes = HashMap::new();
        writes.insert(
            AggregateKey::new(1, 2, 4),
            SingleAggregateWrite {
                expected_event_batch_index: None,
                allow_create: true,
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
                events: vec![],
            },
        );
        writes.insert(
            AggregateKey::new(1, 2, 8),
            SingleAggregateWrite {
                expected_event_batch_index: None,
                allow_create: true,
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
                events: vec![],
            },
        );
        let request = Request::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes,
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 0); // both 4 % 4 and 8 % 4 = 0
    }

    #[test]
    fn routing_write_multiple_aggregates_different_shards() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let mut writes = HashMap::new();
        writes.insert(
            AggregateKey::new(1, 2, 4),
            SingleAggregateWrite {
                expected_event_batch_index: None,
                allow_create: true,
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
                events: vec![],
            },
        );
        writes.insert(
            AggregateKey::new(1, 2, 5),
            SingleAggregateWrite {
                expected_event_batch_index: None,
                allow_create: true,
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
                events: vec![],
            },
        );
        let request = Request::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes,
        });
        let result = determine_shard(&request, &config, PortType::Client);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters(_))));
    }

    #[test]
    fn routing_write_empty_writes() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes: HashMap::new(),
        });
        let result = determine_shard(&request, &config, PortType::Client);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters(_))));
    }

    #[test]
    fn routing_delete_single_aggregate() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let key = AggregateKey::new(1, 2, 9);
        let mut deletes = HashMap::new();
        deletes.insert(key, SingleAggregateDelete {
            allow_recreate: false,
            allow_index_continuation: false,
            expected_event_batch_index: None,
        });
        let request = Request::Delete(DeleteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            deletes,
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 1); // 9 % 4 = 1
    }

    #[test]
    fn routing_delete_empty_deletes() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::Delete(DeleteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            deletes: HashMap::new(),
        });
        let result = determine_shard(&request, &config, PortType::Client);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters(_))));
    }

    #[test]
    fn routing_trim_start() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::TrimStart(TrimStartRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(1, 2, 6),
            keep_from_event_batch_index: 10,
            client_id: 1,
            user_id: None,
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 2); // 6 % 4 = 2
    }

    #[test]
    fn routing_watch_by_org_id() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let mut orgs = std::collections::HashSet::new();
        orgs.insert(5u128);
        let request = Request::Watch(WatchRequest {
            correlation_id: None,
            orgs: Some(orgs),
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
            requested_latency_ms: None,
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 1); // 5 % 4 = 1
    }

    #[test]
    fn routing_watch_by_org_id_missing_org() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let request = Request::Watch(WatchRequest {
            correlation_id: None,
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
            requested_latency_ms: None,
        });
        let result = determine_shard(&request, &config, PortType::Client);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters(_))));
    }

    #[test]
    fn routing_watch_by_aggregate_type_id() {
        let config = test_config(3, crate::RoutingRule::AggregateTypeId);
        let mut agg_types = std::collections::HashSet::new();
        agg_types.insert(7u128);
        let request = Request::Watch(WatchRequest {
            correlation_id: None,
            orgs: None,
            aggregate_types: Some(agg_types),
            aggregates: None,
            operation_types: None,
            requested_latency_ms: None,
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 1); // 7 % 3 = 1
    }

    #[test]
    fn routing_watch_by_aggregate_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let mut aggregates = std::collections::HashSet::new();
        aggregates.insert(10u128);
        let request = Request::Watch(WatchRequest {
            correlation_id: None,
            orgs: None,
            aggregate_types: None,
            aggregates: Some(aggregates),
            operation_types: None,
            requested_latency_ms: None,
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 2); // 10 % 4 = 2
    }

    #[test]
    fn routing_watch_multiple_orgs_same_shard() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let mut orgs = std::collections::HashSet::new();
        orgs.insert(4u128);
        orgs.insert(8u128);
        let request = Request::Watch(WatchRequest {
            correlation_id: None,
            orgs: Some(orgs),
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
            requested_latency_ms: None,
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 0); // both 4 % 4 and 8 % 4 = 0
    }

    #[test]
    fn routing_watch_multiple_orgs_different_shards() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let mut orgs = std::collections::HashSet::new();
        orgs.insert(4u128);
        orgs.insert(5u128);
        let request = Request::Watch(WatchRequest {
            correlation_id: None,
            orgs: Some(orgs),
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
            requested_latency_ms: None,
        });
        let result = determine_shard(&request, &config, PortType::Client);
        assert!(matches!(result, Err(ShardRoutingError::MultipleShardRoutes)));
    }

    #[test]
    fn shard_id_validation_boundary() {
        assert!(validate_shard_id(0, 4).is_ok());
        assert!(validate_shard_id(3, 4).is_ok());
        assert!(validate_shard_id(4, 4).is_err());
        assert!(validate_shard_id(100, 4).is_err());
    }

    #[test]
    fn validate_client_id_no_verification() {
        // When verified_client_id is None, all requests pass
        let key = AggregateKey::new(1, 2, 3);
        let mut writes = HashMap::new();
        writes.insert(key, SingleAggregateWrite {
            expected_event_batch_index: None,
            allow_create: true,
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
            events: vec![],
        });
        let req = Request::Write(WriteRequest {
            correlation_id: Some(123),
            client_id: 999,
            user_id: None,
            writes,
        });
        assert!(super::validate_client_id(&req, None).is_ok());
    }

    #[test]
    fn validate_client_id_match() {
        // When verified_client_id matches request, passes
        let key = AggregateKey::new(1, 2, 3);
        let mut writes = HashMap::new();
        writes.insert(key, SingleAggregateWrite {
            expected_event_batch_index: None,
            allow_create: true,
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
            events: vec![],
        });
        let req = Request::Write(WriteRequest {
            correlation_id: Some(123),
            client_id: 999,
            user_id: None,
            writes,
        });
        assert!(super::validate_client_id(&req, Some(999)).is_ok());
    }

    #[test]
    fn validate_client_id_mismatch() {
        // When verified_client_id doesn't match request, fails
        let key = AggregateKey::new(1, 2, 3);
        let mut writes = HashMap::new();
        writes.insert(key, SingleAggregateWrite {
            expected_event_batch_index: None,
            allow_create: true,
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
            events: vec![],
        });
        let req = Request::Write(WriteRequest {
            correlation_id: Some(123),
            client_id: 999,
            user_id: None,
            writes,
        });
        let result = super::validate_client_id(&req, Some(888));
        assert!(result.is_err());
        if let Err(Response::GenericError(err)) = result {
            assert_eq!(err.error_code, IDENTIFY_MISMATCH);
            assert!(err.error_message.contains("999"));
            assert!(err.error_message.contains("888"));
        } else {
            panic!("Expected GenericError with IDENTITY_MISMATCH");
        }
    }

    #[test]
    fn validate_client_id_read_always_passes() {
        // Read requests don't carry client_id, so they always pass
        let req = Request::Read(ReadRequest {
            correlation_id: Some(123),
            aggregate_key: AggregateKey::new(1, 2, 3),
            filters: ReadFilters::default(),
        });
        assert!(super::validate_client_id(&req, Some(999)).is_ok());
    }

    #[test]
    fn validate_client_id_trim_start() {
        let req = Request::TrimStart(TrimStartRequest {
            correlation_id: Some(123),
            aggregate_key: AggregateKey::new(1, 2, 3),
            keep_from_event_batch_index: 10,
            client_id: 777,
            user_id: None,
        });
        assert!(super::validate_client_id(&req, Some(777)).is_ok());
        assert!(super::validate_client_id(&req, Some(666)).is_err());
    }

    #[test]
    fn validate_client_id_delete() {
        let key = AggregateKey::new(1, 2, 3);
        let mut deletes = HashMap::new();
        deletes.insert(key, SingleAggregateDelete {
            allow_recreate: false,
            allow_index_continuation: false,
            expected_event_batch_index: None,
        });
        let req = Request::Delete(DeleteRequest {
            correlation_id: Some(123),
            client_id: 555,
            user_id: None,
            deletes,
        });
        assert!(super::validate_client_id(&req, Some(555)).is_ok());
        assert!(super::validate_client_id(&req, Some(444)).is_err());
    }
}
