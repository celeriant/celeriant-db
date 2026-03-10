use std::{cell::{Cell, RefCell}, collections::HashMap, fmt, future::Future, rc::Rc, time::Duration};

use base64::Engine;
use celeriant_distributed::{heartbeat::now_ms, lease_manager::LeaseManager, lease_store::LeaseStore, node_status::NodeStatus, validated_node_status::ValidatedNodeStatus};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    process_cluster_requests::ClusterRequest,
    process_cluster_responses::ClusterResponse,
    process_identify::{IDENTIFY_REQUEST_TYPE_ID, read_identify_request, write_identify_response},
    request::requests::{HeartbeatRequest, IdentifyRequest, KickFollowerRequest, WatchRequest},
    response::responses::{AccessLevel, ErrorResponse, HeartbeatRejection, HeartbeatResponse, HeartbeatResult, IdentifyResponse, KickFollowerResponse, WatchResponse},
};
use celeriant_shard::{
    error::{s3_catchup_error::S3CatchupError, watch_session_error::WatchSessionError}, replication_client::ReplicationClient, s3_downloader::S3Downloader, shard_wal::ShardWal, shard_wal_s3_catchup::S3CatchupResult
};
use celeriant_wal::compression_type::CompressionType;
use celeriant_watch::{watch_output_type::WatchOutputType, watch_session::WatchSession};
use celeriant_msg::read_wire_data_error::ReadWireDataError;
use celeriant_wire::network::wire_error::WireError;
use celeriant_wire::network::wire_header::WireHeader;
use glommio::{channels::{channel_mesh::Senders, local_channel::LocalSender}, net::TcpStream};
use tracing::{info, warn};

use super::{
    intrashard_messages::IntrashardMessages,
    shard_config::ShardConfig,
    shard_error_response::{shard_error_to_client_response, shard_error_to_cluster_response, shard_routing_error_to_code, watch_read_error_to_client_response, watch_session_error_to_client_response, IDENTIFY_INVALID_NONCE, IDENTIFY_INVALID_SIGNATURE, IDENTIFY_MISMATCH, IDENTIFY_REQUIRED},
};

struct ConnectionGuard<'a>(&'a [(&'static str, String); 1]);

impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        metrics::gauge!("celeriant_client_connections_active", self.0).decrement(1.0);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortType {
    Client,
    Replication,
}

pub struct CatchupCompletionMsg {
    pub shard_id: usize,
    pub result: Result<S3CatchupResult, S3CatchupError>,
}

pub struct SchemaRegistrationCompletionMsg {
    pub result: Result<(), celeriant_shard::error::shard_schema_error::ShardSchemaError>,
}

pub struct ConnectionContext<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static> {
    pub config: Rc<ShardConfig>,
    pub current_shard_id: usize,
    pub intrashard_sender: Rc<Senders<IntrashardMessages>>,
    pub shutdown_requested: Rc<Cell<bool>>,
    pub shard_wal: Rc<ShardWal<R, D>>,
    pub catchup_completion_tx: Option<Rc<LocalSender<CatchupCompletionMsg>>>,
    pub schema_registration_pending: Option<Rc<RefCell<HashMap<u64, LocalSender<SchemaRegistrationCompletionMsg>>>>>,
    pub lease_manager: Option<Rc<LeaseManager<S>>>,
}

/// Connection-level state for identity verification and access control
struct ConnectionState {
    verified_client_id: Option<u128>,
    access_level: Option<celeriant_msg::response::responses::AccessLevel>,
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
            schema_registration_pending: self.schema_registration_pending.clone(),
            lease_manager: self.lease_manager.clone(),
        }
    }
}

#[derive(Debug)]
pub enum ShardRoutingError {
    NoRoutingKeyProvided,
    MultipleShardRoutes { num_shards: u64 },
    IncompatibleFilters { detail: String, num_shards: u64 },
}

impl fmt::Display for ShardRoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardRoutingError::NoRoutingKeyProvided => write!(f, "no routing key provided"),
            ShardRoutingError::MultipleShardRoutes { num_shards } => write!(f, "request routes to multiple shards (num_shards={})", num_shards),
            ShardRoutingError::IncompatibleFilters { detail, num_shards } => write!(f, "incompatible filters: {} (num_shards={})", detail, num_shards),
        }
    }
}

enum ClientRedirectResult {
    ProcessLocally(ClientRequest, TcpStream),
    Redirected,
    ErrorSentContinue(TcpStream),
}

enum ClusterRedirectResult {
    ProcessLocally(ClusterRequest, TcpStream),
    Redirected,
    ErrorSentContinue(TcpStream),
}

pub fn handle_new_connection<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(mut tcp_stream: TcpStream, ctx: ConnectionContext<R, D, S>, port_type: PortType) {
    let _ = tcp_stream.set_nodelay(true);

    glommio::spawn_local(async move {
        if ctx.shutdown_requested.get() {
            return;
        }

        let shard_label = [("shard_id", ctx.current_shard_id.to_string())];
        metrics::gauge!("celeriant_client_connections_active", &shard_label).increment(1.0);
        let _guard = ConnectionGuard(&shard_label);

        match port_type {
            PortType::Client => {
                let mut conn_state = ConnectionState {
                    verified_client_id: None,
                    access_level: None,
                };

                let first_message = match read_first_message(&mut tcp_stream, &ctx).await {
                    Some(m) => m,
                    None => return,
                };

                let (request, message_version) = match first_message {
                    FirstMessage::Identify(identify_req, version) => {
                        if let Err(response) = handle_identify(&identify_req, &mut conn_state, &ctx.config) {
                            let _ = write_client_response_with_timeout(&mut tcp_stream, &response, ctx.config.max_response_size, ctx.config.server_compression_algorithm, version, ctx.config.slow_client_timeout).await;
                            return;
                        }
                        let res = IdentifyResponse {
                            correlation_id: identify_req.correlation_id,
                            client_id: conn_state.verified_client_id,
                            access_level: conn_state.access_level,
                        };
                        if write_identify_response(&mut tcp_stream, &res, version).await.is_err() {
                            return;
                        }
                        match read_client_request(&mut tcp_stream, &ctx).await {
                            Some(r) => r,
                            None => return,
                        }
                    }
                    FirstMessage::ClientRequest(request, version) => {
                        if ctx.config.require_client_identity {
                            let response = ClientResponse::GenericError(ErrorResponse {
                                correlation_id: request.correlation_id(),
                                error_code: IDENTIFY_REQUIRED,
                                error_message: "Server requires client identity verification".to_string(),
                            });
                            let _ = write_client_response_with_timeout(&mut tcp_stream, &response, ctx.config.max_response_size, ctx.config.server_compression_algorithm, version, ctx.config.slow_client_timeout).await;
                            return;
                        }
                        (request, version)
                    }
                };

                match check_client_redirect(tcp_stream, request, ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, &ctx, &conn_state).await {
                    ClientRedirectResult::ProcessLocally(request, tcp_stream) => {
                        handle_client_pipelining(tcp_stream, Some(request), ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, ctx, conn_state).await;
                    }
                    ClientRedirectResult::Redirected => {}
                    ClientRedirectResult::ErrorSentContinue(tcp_stream) => {
                        handle_client_pipelining(tcp_stream, None, ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, ctx, conn_state).await;
                    }
                }
            }
            PortType::Replication => {
                let (request, message_version) = match read_cluster_request(&mut tcp_stream, &ctx).await {
                    Some(r) => r,
                    None => return,
                };

                match check_cluster_redirect(tcp_stream, request, ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, &ctx).await {
                    ClusterRedirectResult::ProcessLocally(request, tcp_stream) => {
                        handle_cluster_pipelining(tcp_stream, Some(request), ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, ctx).await;
                    }
                    ClusterRedirectResult::Redirected => {}
                    ClusterRedirectResult::ErrorSentContinue(tcp_stream) => {
                        handle_cluster_pipelining(tcp_stream, None, ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, ctx).await;
                    }
                }
            }
        }
    })
    .detach();
}

pub fn handle_redirected_client_connection<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: TcpStream,
    request: ClientRequest,
    max_response_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
    ctx: ConnectionContext<R, D, S>,
    verified_client_id: Option<u128>,
    access_level: Option<AccessLevel>,
) {
    let _ = tcp_stream.set_nodelay(true);

    glommio::spawn_local(async move {
        let conn_state = ConnectionState { verified_client_id, access_level };
        handle_client_pipelining(tcp_stream, Some(request), max_response_size, server_compression_algorithm, message_version, ctx, conn_state).await;
    })
    .detach();
}

pub fn handle_redirected_cluster_connection<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: TcpStream,
    request: ClusterRequest,
    max_response_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
    ctx: ConnectionContext<R, D, S>,
) {
    let _ = tcp_stream.set_nodelay(true);

    glommio::spawn_local(async move {
        handle_cluster_pipelining(tcp_stream, Some(request), max_response_size, server_compression_algorithm, message_version, ctx).await;
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

async fn handle_client_pipelining<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    mut tcp_stream: TcpStream,
    request: Option<ClientRequest>,
    max_response_size: u64,
    server_compression_algorithm: CompressionType,
    mut message_version: u32,
    ctx: ConnectionContext<R, D, S>,
    conn_state: ConnectionState,
) {
    let mut optional_request = request;

    loop {
        if ctx.shutdown_requested.get() {
            break;
        }

        if let Some(request) = optional_request.take() {
            if let Err(response) = validate_client_id(&request, conn_state.verified_client_id) {
                let _ = write_client_response_with_timeout(&mut tcp_stream, &response, max_response_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
                continue;
            }

            if !is_valid_for_access_level(&request, conn_state.access_level) {
                let response = ClientResponse::GenericError(ErrorResponse {
                    correlation_id: request.correlation_id(),
                    error_code: ErrorResponse::AUTH_INSUFFICIENT_PERMISSIONS,
                    error_message: "Insufficient permissions for this operation".to_string(),
                });
                let _ = write_client_response_with_timeout(&mut tcp_stream, &response, max_response_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
                continue;
            }

            if let ClientRequest::Watch(watch_request) = request {
                handle_watch(tcp_stream, watch_request, max_response_size, server_compression_algorithm, message_version, &ctx).await;
                return;
            }

            process_client_request(&mut tcp_stream, &ctx, request, max_response_size, server_compression_algorithm, message_version).await;
        }

        if ctx.shutdown_requested.get() {
            break;
        }

        match read_client_request(&mut tcp_stream, &ctx).await {
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
            match check_client_redirect(tcp_stream, request, max_response_size, server_compression_algorithm, message_version, &ctx, &conn_state).await {
                ClientRedirectResult::ProcessLocally(req, stream) => {
                    optional_request = Some(req);
                    tcp_stream = stream;
                }
                ClientRedirectResult::Redirected => return,
                ClientRedirectResult::ErrorSentContinue(stream) => {
                    tcp_stream = stream;
                    continue;
                }
            }
        }
    }
}

async fn handle_cluster_pipelining<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    mut tcp_stream: TcpStream,
    request: Option<ClusterRequest>,
    max_response_size: u64,
    server_compression_algorithm: CompressionType,
    mut message_version: u32,
    ctx: ConnectionContext<R, D, S>,
) {
    let mut optional_request = request;

    loop {
        if ctx.shutdown_requested.get() {
            break;
        }

        if let Some(request) = optional_request.take() {
            process_cluster_request(&mut tcp_stream, &ctx, request, max_response_size, server_compression_algorithm, message_version).await;
        }

        if ctx.shutdown_requested.get() {
            break;
        }

        match read_cluster_request(&mut tcp_stream, &ctx).await {
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
            match check_cluster_redirect(tcp_stream, request, max_response_size, server_compression_algorithm, message_version, &ctx).await {
                ClusterRedirectResult::ProcessLocally(req, stream) => {
                    optional_request = Some(req);
                    tcp_stream = stream;
                }
                ClusterRedirectResult::Redirected => return,
                ClusterRedirectResult::ErrorSentContinue(stream) => {
                    tcp_stream = stream;
                    continue;
                }
            }
        }
    }
}



/// Handle IdentifyRequest during connection handshake.
/// Validates nonce and signature, derives client_id, and validates API key if configured.
fn handle_identify(req: &celeriant_msg::request::requests::IdentifyRequest, conn_state: &mut ConnectionState, config: &ShardConfig) -> Result<(), ClientResponse> {
    use celeriant_crypto::Crypto;

    if let (Some(public_key), Some(nonce), Some(signature)) = (&req.public_key, &req.nonce, &req.signature) {
        let client_id = match Crypto::validate_with_public_key(public_key, nonce, signature) {
            Ok(id) => id,
            Err(celeriant_crypto::CryptoError::InvalidNonce) => {
                return Err(ClientResponse::GenericError(ErrorResponse {
                    correlation_id: req.correlation_id,
                    error_code: IDENTIFY_INVALID_NONCE,
                    error_message: "Nonce expired or too far in the future".to_string(),
                }));
            }
            Err(_) => {
                return Err(ClientResponse::GenericError(ErrorResponse {
                    correlation_id: req.correlation_id,
                    error_code: IDENTIFY_INVALID_SIGNATURE,
                    error_message: "Invalid signature".to_string(),
                }));
            }
        };
        conn_state.verified_client_id = Some(client_id);
    }

    if let Some(ref api_key_hashes) = *config.api_key_hashes.borrow() {
        let api_key_str = req.api_key.as_ref().ok_or_else(|| {
            ClientResponse::GenericError(ErrorResponse {
                correlation_id: req.correlation_id,
                error_code: ErrorResponse::AUTH_REQUIRED,
                error_message: "API key required but not provided".to_string(),
            })
        })?;

        let api_key_bytes = base64::engine::general_purpose::STANDARD
            .decode(api_key_str)
            .map_err(|_| {
                ClientResponse::GenericError(ErrorResponse {
                    correlation_id: req.correlation_id,
                    error_code: ErrorResponse::AUTH_INVALID_KEY,
                    error_message: "Invalid API key format".to_string(),
                })
            })?;

        if api_key_bytes.len() != 32 {
            return Err(ClientResponse::GenericError(ErrorResponse {
                correlation_id: req.correlation_id,
                error_code: ErrorResponse::AUTH_INVALID_KEY,
                error_message: "Invalid API key length".to_string(),
            }));
        }

        let mut key_array = [0u8; 32];
        key_array.copy_from_slice(&api_key_bytes);

        let key_hash = celeriant_crypto::hash_api_key(&key_array);

        let access_level = api_key_hashes.validate(&key_hash).ok_or_else(|| {
            ClientResponse::GenericError(ErrorResponse {
                correlation_id: req.correlation_id,
                error_code: ErrorResponse::AUTH_INVALID_KEY,
                error_message: "Invalid API key".to_string(),
            })
        })?;

        conn_state.access_level = Some(access_level);
    } else {
        conn_state.access_level = None;
    }

    Ok(())
}

fn validate_client_id(request: &ClientRequest, verified_client_id: Option<u128>) -> Result<(), ClientResponse> {
    let Some(verified) = verified_client_id else {
        return Ok(());
    };

    let request_client_id = match request {
        ClientRequest::Write(req) => Some(req.client_id),
        ClientRequest::TrimStart(req) => Some(req.client_id),
        ClientRequest::Delete(req) => Some(req.client_id),
        ClientRequest::RegisterSchema(req) => Some(req.client_id),
        _ => None,
    };

    if let Some(claimed) = request_client_id {
        if claimed != verified {
            return Err(ClientResponse::GenericError(ErrorResponse {
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

fn is_valid_for_access_level(request: &ClientRequest, access_level: Option<AccessLevel>) -> bool {
    match access_level {
        None | Some(AccessLevel::ReadWrite) => true,
        Some(AccessLevel::ReadOnly) => {
            !matches!(request,
                ClientRequest::Write(_) |
                ClientRequest::TrimStart(_) |
                ClientRequest::Delete(_) | 
                ClientRequest::RegisterSchema(_)
            )
        }
    }
}

async fn check_client_redirect<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    mut tcp_stream: TcpStream,
    request: ClientRequest,
    max_message_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
    ctx: &ConnectionContext<R, D, S>,
    conn_state: &ConnectionState,
) -> ClientRedirectResult {
    let target_shard = match determine_client_shard(&request, &ctx.config) {
        Ok(idx) => idx,
        Err(e) => {
            let (error_code, error_message) = shard_routing_error_to_code(e);
            let response = ClientResponse::GenericError(ErrorResponse {
                correlation_id: request.correlation_id(),
                error_code,
                error_message,
            });
            let _ = write_client_response_with_timeout(&mut tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
            return ClientRedirectResult::ErrorSentContinue(tcp_stream);
        }
    };

    if target_shard != ctx.current_shard_id {
        metrics::counter!("celeriant_connection_redirects_total").increment(1);
        let msg = IntrashardMessages::ClientConnectionRedirect {
            accepted_tcp_stream: tcp_stream.into_accepted(),
            request,
            message_version,
            verified_client_id: conn_state.verified_client_id,
            access_level: conn_state.access_level,
        };
        if let Err(e) = ctx.intrashard_sender.send_to(target_shard, msg).await {
            warn!("Failed to redirect client connection to shard {target_shard}: {e:?}");
        }
        return ClientRedirectResult::Redirected;
    }

    ClientRedirectResult::ProcessLocally(request, tcp_stream)
}

async fn check_cluster_redirect<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    mut tcp_stream: TcpStream,
    request: ClusterRequest,
    max_message_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
    ctx: &ConnectionContext<R, D, S>,
) -> ClusterRedirectResult {
    let target_shard = match determine_cluster_shard(&request, &ctx.config) {
        Ok(idx) => idx,
        Err(e) => {
            let (error_code, error_message) = shard_routing_error_to_code(e);
            let response = ClusterResponse::GenericError(ErrorResponse {
                correlation_id: request.correlation_id(),
                error_code,
                error_message,
            });
            let _ = write_cluster_response_with_timeout(&mut tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
            return ClusterRedirectResult::ErrorSentContinue(tcp_stream);
        }
    };

    if target_shard != ctx.current_shard_id {
        let msg = IntrashardMessages::ClusterConnectionRedirect {
            accepted_tcp_stream: tcp_stream.into_accepted(),
            request,
            message_version,
        };
        if let Err(e) = ctx.intrashard_sender.send_to(target_shard, msg).await {
            warn!("Failed to redirect cluster connection to shard {target_shard}: {e:?}");
        }
        return ClusterRedirectResult::Redirected;
    }

    ClusterRedirectResult::ProcessLocally(request, tcp_stream)
}

pub fn determine_client_shard(
    request: &ClientRequest,
    config: &ShardConfig,
) -> Result<usize, ShardRoutingError> {
    let num_shards = config.num_shards as u128;

    match request {
        ClientRequest::RegisterSchema(_) => Ok(0),
        ClientRequest::Watch(req) => determine_shard_watch(req, config),
        ClientRequest::Write(req) => determine_shard_write(req, config),
        ClientRequest::Delete(req) => determine_shard_delete(req, config),
        ClientRequest::ListOrgs(req) => validate_shard_id(req.shard_id, num_shards),
        ClientRequest::ListAggregateTypes(req) => validate_shard_id(req.shard_id, num_shards),
        ClientRequest::ListAggregates(req) => validate_shard_id(req.shard_id, num_shards),
        other => {
            let routing_id = config.routing_rule.routing_id_for_client_request(other);
            Ok((routing_id % num_shards) as usize)
        }
    }
}

pub fn determine_cluster_shard(
    request: &ClusterRequest,
    config: &ShardConfig,
) -> Result<usize, ShardRoutingError> {
    let num_shards = config.num_shards as u128;

    match request {
        ClusterRequest::ReplicationBatch(req) => validate_shard_id(req.shard_id, num_shards),
        ClusterRequest::KickFollower(_) | ClusterRequest::Heartbeat(_) => Ok(0),
    }
}

fn validate_shard_id(shard_id: u64, num_shards: u128) -> Result<usize, ShardRoutingError> {
    if (shard_id as u128) >= num_shards {
        return Err(ShardRoutingError::IncompatibleFilters {
            detail: format!(
                "Invalid shard_id {}. Must be less than {} (total number of shards).",
                shard_id, num_shards
            ),
            num_shards: num_shards as u64,
        });
    }
    Ok(shard_id as usize)
}

fn determine_shard_write(
    req: &celeriant_msg::request::requests::WriteRequest,
    config: &ShardConfig,
) -> Result<usize, ShardRoutingError> {
    let num_shards = config.num_shards as u128;

    if req.writes.is_empty() {
        return Err(ShardRoutingError::IncompatibleFilters {
            detail: "Write request must contain at least one write operation.".into(),
            num_shards: num_shards as u64,
        });
    }

    let mut shard_id: Option<usize> = None;
    for aggregate_key in req.writes.keys() {
        let routing_id = config.routing_rule.routing_id_for_rule(aggregate_key);
        let id = (routing_id % num_shards) as usize;
        match shard_id {
            None => shard_id = Some(id),
            Some(first) if first != id => {
                return Err(ShardRoutingError::IncompatibleFilters {
                    detail: format!(
                        "Write request spans multiple shards. All writes must route to the same shard when using {} routing.",
                        config.routing_rule
                    ),
                    num_shards: num_shards as u64,
                });
            }
            _ => {}
        }
    }

    Ok(shard_id.unwrap())
}

fn determine_shard_delete(
    req: &celeriant_msg::request::requests::DeleteRequest,
    config: &ShardConfig,
) -> Result<usize, ShardRoutingError> {
    let num_shards = config.num_shards as u128;

    if req.deletes.is_empty() {
        return Err(ShardRoutingError::IncompatibleFilters {
            detail: "Delete request must contain at least one delete operation.".into(),
            num_shards: num_shards as u64,
        });
    }

    let mut shard_id: Option<usize> = None;
    for aggregate_key in req.deletes.keys() {
        let routing_id = config.routing_rule.routing_id_for_rule(aggregate_key);
        let id = (routing_id % num_shards) as usize;
        match shard_id {
            None => shard_id = Some(id),
            Some(first) if first != id => {
                return Err(ShardRoutingError::IncompatibleFilters {
                    detail: format!(
                        "Delete request spans multiple shards. All delete must route to the same shard when using {} routing.",
                        config.routing_rule
                    ),
                    num_shards: num_shards as u64,
                });
            }
            _ => {}
        }
    }

    Ok(shard_id.unwrap())
}

fn determine_shard_watch(
    req: &WatchRequest,
    config: &ShardConfig,
) -> Result<usize, ShardRoutingError> {
    if let Some(shard_id) = req.shard_id {
        return validate_shard_id(shard_id, config.num_shards as u128);
    }

    let num_shards = config.num_shards as u128;

    match config.routing_rule {
        crate::RoutingRule::OrgId => {
            if req.orgs.is_none() || req.orgs.as_ref().unwrap().is_empty() {
                return Err(ShardRoutingError::IncompatibleFilters {
                    detail: "Must specify at least one organisation. Server is setup to shard by organisation.".into(),
                    num_shards: num_shards as u64,
                });
            }
            collect_unique_shard_id(num_shards, &req.orgs)
        }
        crate::RoutingRule::AggregateTypeId => {
            if req.aggregate_types.is_none() || req.aggregate_types.as_ref().unwrap().is_empty() {
                return Err(ShardRoutingError::IncompatibleFilters {
                    detail: "Must specify at least one aggregate type. Server is setup to shard by aggregate type.".into(),
                    num_shards: num_shards as u64,
                });
            }
            collect_unique_shard_id(num_shards, &req.aggregate_types)
        }
        crate::RoutingRule::AggregateId => {
            if req.aggregates.is_none() || req.aggregates.as_ref().unwrap().is_empty() {
                return Err(ShardRoutingError::IncompatibleFilters {
                    detail: "Must specify at least one aggregate. Server is setup to shard by aggregate.".into(),
                    num_shards: num_shards as u64,
                });
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
                return Err(ShardRoutingError::MultipleShardRoutes { num_shards: num_shards as u64 });
            }
            Some(_) => {}
        }
    }

    shard_id.ok_or(ShardRoutingError::NoRoutingKeyProvided)
}

enum FirstMessage {
    Identify(IdentifyRequest, u32),
    ClientRequest(ClientRequest, u32),
}

/// Read the first message on a new client connection.
/// Peeks at the wire header to distinguish Identify (type 14) from client requests.
async fn read_with_timeout<T>(
    timeout: Duration,
    shard_id: usize,
    label: &str,
    fut: impl Future<Output = Result<T, ReadWireDataError>>,
) -> Option<T> {
    let read_result = match glommio::timer::timeout(timeout, async {
        Ok::<_, glommio::GlommioError<()>>(fut.await)
    }).await {
        Ok(result) => result,
        Err(_) => {
            warn!("Client timed out reading {label}");
            return None;
        }
    };
    match read_result {
        Ok(result) => Some(result),
        Err(ReadWireDataError::ReadHeaderFailure(WireError::NetworkError(ref e)))
            if e.kind() == std::io::ErrorKind::UnexpectedEof => None,
        Err(e) => {
            warn!(shard = shard_id, "Failed to read {label}: {e:?}");
            None
        }
    }
}

async fn read_first_message<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: &mut TcpStream,
    ctx: &ConnectionContext<R, D, S>,
) -> Option<FirstMessage> {
    read_with_timeout(ctx.config.slow_client_timeout, ctx.current_shard_id, "first message", async {
        let header = WireHeader::from_reader(tcp_stream, ctx.config.max_request_size).await
            .map_err(ReadWireDataError::ReadHeaderFailure)?;
        let version = header.version;
        if header.message_type == IDENTIFY_REQUEST_TYPE_ID {
            let req = read_identify_request(header, tcp_stream).await?;
            Ok(FirstMessage::Identify(req, version))
        } else {
            let req = ClientRequest::read_from_header(header, tcp_stream).await?;
            Ok(FirstMessage::ClientRequest(req, version))
        }
    }).await
}

async fn read_client_request<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: &mut TcpStream,
    ctx: &ConnectionContext<R, D, S>,
) -> Option<(ClientRequest, u32)> {
    read_with_timeout(ctx.config.slow_client_timeout, ctx.current_shard_id, "client request", async {
        let header = WireHeader::from_reader(tcp_stream, ctx.config.max_request_size).await
            .map_err(ReadWireDataError::ReadHeaderFailure)?;
        let version = header.version;
        let req = ClientRequest::read_from_header(header, tcp_stream).await?;
        Ok((req, version))
    }).await
}

async fn read_cluster_request<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: &mut TcpStream,
    ctx: &ConnectionContext<R, D, S>,
) -> Option<(ClusterRequest, u32)> {
    // No application-level timeout for cluster (replication) connections.
    // Liveness is handled by the heartbeat TTL mechanism and TCP keepalive
    // detects dead peers at the OS level. An idle replication connection is
    // normal between write bursts and should not be closed.
    let result: Result<(ClusterRequest, u32), ReadWireDataError> = async {
        let header = WireHeader::from_reader(tcp_stream, ctx.config.max_request_size).await
            .map_err(ReadWireDataError::ReadHeaderFailure)?;
        let version = header.version;
        let req = ClusterRequest::read_from_header(header, tcp_stream).await?;
        Ok((req, version))
    }.await;
    match result {
        Ok(r) => Some(r),
        Err(ReadWireDataError::ReadHeaderFailure(WireError::NetworkError(ref e)))
            if e.kind() == std::io::ErrorKind::UnexpectedEof => None,
        Err(e) => {
            warn!(shard = ctx.current_shard_id, "Failed to read cluster request: {e:?}");
            None
        }
    }
}

macro_rules! write_response_with_timeout_fn {
    ($name:ident, $response_type:ty) => {
        async fn $name(
            tcp_stream: &mut TcpStream,
            response: &$response_type,
            max_message_size: u64,
            server_compression_algorithm: CompressionType,
            message_version: u32,
            timeout_duration: Duration,
        ) -> Result<(), WireError> {
            let compression = <$response_type>::determine_compression_type(response, server_compression_algorithm);
            match glommio::timer::timeout(timeout_duration, async {
                let result = <$response_type>::write_response(tcp_stream, response, compression, max_message_size, message_version).await;
                Ok::<_, glommio::GlommioError<()>>(result)
            })
            .await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(WireError::NetworkError(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out"))),
            }
        }
    };
}

write_response_with_timeout_fn!(write_client_response_with_timeout, ClientResponse);
write_response_with_timeout_fn!(write_cluster_response_with_timeout, ClusterResponse);

async fn handle_schema_registration_coordination<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: &ConnectionContext<R, D, S>,
    request: celeriant_msg::request::requests::RegisterSchemaRequest,
) -> Result<celeriant_msg::response::responses::SuccessResponse, celeriant_shard::error::shard_schema_error::ShardSchemaError> {
    use celeriant_shard::error::shard_schema_error::ShardSchemaError;
    use glommio::channels::local_channel;

    let shard_0_result = ctx.shard_wal.register_schema(request.clone()).await?;

    let num_shards = ctx.config.num_shards as usize;
    if num_shards == 1 {
        return Ok(shard_0_result);
    }

    let request_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let (result_tx, result_rx) = local_channel::new_unbounded();

    let pending_map = ctx.schema_registration_pending.as_ref()
        .ok_or_else(|| ShardSchemaError::SchemaCoordinationFailed {
            failed_shard_count: num_shards - 1,
            total_shards: num_shards,
        })?;

    pending_map.borrow_mut().insert(request_id, result_tx);

    for shard_id in 1..num_shards {
        let msg = IntrashardMessages::SchemaRegistration {
            request: request.clone(),
            request_id,
        };

        if ctx.intrashard_sender.send_to(shard_id, msg).await.is_err() {
            return Err(ShardSchemaError::SchemaCoordinationFailed {
                failed_shard_count: num_shards - shard_id,
                total_shards: num_shards,
            });
        }
    }

    let mut failed_count = 0;
    let mut received_count = 0;
    let expected_count = num_shards - 1;
    let coordination_timeout = Duration::from_secs(10);

    while received_count < expected_count {
        let recv_result = glommio::timer::timeout(coordination_timeout, async {
            Ok::<_, glommio::GlommioError<()>>(result_rx.recv().await)
        }).await;

        match recv_result {
            Ok(Some(completion_msg)) => {
                received_count += 1;
                if completion_msg.result.is_err() {
                    failed_count += 1;
                }
            }
            Ok(None) => {
                failed_count += expected_count - received_count;
                break;
            }
            Err(_) => {
                return Err(ShardSchemaError::SchemaCoordinationFailed {
                    failed_shard_count: expected_count - received_count,
                    total_shards: num_shards,
                });
            }
        }
    }

    pending_map.borrow_mut().remove(&request_id);

    if failed_count > 0 {
        return Err(ShardSchemaError::SchemaCoordinationFailed {
            failed_shard_count: failed_count,
            total_shards: num_shards,
        });
    }

    Ok(shard_0_result)
}

async fn process_client_request<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: &mut TcpStream,
    ctx: &ConnectionContext<R, D, S>,
    request: ClientRequest,
    max_message_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
) {
    let correlation_id = request.correlation_id();

    let response = if let ClientRequest::RegisterSchema(ref schema_request) = request {
        match handle_schema_registration_coordination(ctx, schema_request.clone()).await {
            Ok(success) => ClientResponse::RegisterSchema(success),
            Err(error) => shard_error_to_client_response(correlation_id, celeriant_shard::error::shard_error::ShardError::RegisterSchema(error)),
        }
    } else {
        match ctx.shard_wal.process_client_request(request).await {
            Ok(result) => result,
            Err(error) => shard_error_to_client_response(correlation_id, error),
        }
    };

    let _ = write_client_response_with_timeout(tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
}

async fn process_cluster_request<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: &mut TcpStream,
    ctx: &ConnectionContext<R, D, S>,
    request: ClusterRequest,
    max_message_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
) {
    let response = match request {
        ClusterRequest::Heartbeat(ref req) => handle_heartbeat(req, ctx).await,
        ClusterRequest::KickFollower(ref req) => handle_kick_follower(req, ctx).await,
        ClusterRequest::ReplicationBatch(req) => {
            let correlation_id = req.correlation_id;
            match ctx.shard_wal.handle_replication_batch(req).await {
                Ok(result) => ClusterResponse::ReplicationBatch(result),
                Err(error) => shard_error_to_cluster_response(correlation_id, celeriant_shard::error::shard_error::ShardError::ReplicationBatch(error)),
            }
        }
    };
    let _ = write_cluster_response_with_timeout(tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
}

/// Follower shard 0 heartbeat handler.
///
/// Validates role and clock drift, then refreshes the TTL on all local shards
/// so they don't self-fence via ValidatedNodeStatus::effective().
async fn handle_heartbeat<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    req: &HeartbeatRequest,
    ctx: &ConnectionContext<R, D, S>,
) -> ClusterResponse {
    let follower_ms = now_ms();

    if !ctx.shard_wal.node_status.get().is_any_follower_state() {
        return ClusterResponse::Heartbeat(HeartbeatResponse {
            correlation_id: req.correlation_id,
            result: HeartbeatResult::Rejected(HeartbeatRejection::NotAFollower),
        });
    }

    // Clock drift too high — nodes' clocks are dangerously skewed. Fence all local
    // shards immediately rather than waiting for TTL expiry, because we can't trust
    // any time-based decisions with skewed clocks.
    let drift = follower_ms.abs_diff(req.leader_timestamp_ms);
    metrics::gauge!("celeriant_clock_drift_ms").set(drift as f64);
    if drift > ctx.config.max_cluster_time_drift_ms {
        let fenced = ValidatedNodeStatus::fenced();
        ctx.shard_wal.node_status.set(fenced);
        broadcast_status(ctx, fenced).await;

        return ClusterResponse::Heartbeat(HeartbeatResponse {
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

    ClusterResponse::Heartbeat(HeartbeatResponse {
        correlation_id: req.correlation_id,
        result: HeartbeatResult::Ack { follower_timestamp_ms: follower_ms },
    })
}

/// Handle KickFollower from the leader. Always routed to shard 0.
/// Transitions to FollowerCatchingUp and broadcasts to all shards.
async fn handle_kick_follower<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    req: &KickFollowerRequest,
    ctx: &ConnectionContext<R, D, S>,
) -> ClusterResponse {
    let status = ctx.shard_wal.node_status.get();
    if !status.is_any_follower_state() {
        return ClusterResponse::KickFollower(KickFollowerResponse {
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

    ClusterResponse::KickFollower(KickFollowerResponse {
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
            let response = watch_session_error_to_client_response(correlation_id, error);
            let _ = write_client_response_with_timeout(&mut tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
            return;
        }
    };

    metrics::gauge!("celeriant_watch_subscribers_active").increment(1.0);

    loop {
        match watch_session.next().await {
            Ok(WatchOutputType::Continue) => continue,
            Ok(WatchOutputType::Response(watch_response)) => {
                let response = ClientResponse::Watch(watch_response);
                if write_client_response_with_timeout(&mut tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await.is_err() {
                    break;
                }
            }
            Ok(WatchOutputType::Heartbeat) => {
                let response = ClientResponse::Watch(WatchResponse::default());
                if write_client_response_with_timeout(&mut tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await.is_err() {
                    break;
                }
            }
            Ok(WatchOutputType::Done) => break,
            Err(error) => {
                let response = watch_read_error_to_client_response(correlation_id, error);
                let _ = write_client_response_with_timeout(&mut tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
                break;
            }
        }
    }

    metrics::gauge!("celeriant_watch_subscribers_active").decrement(1.0);
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
            DeleteRequest, AggregateDetailsRequest, ListAggregateTypesRequest,
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
            list_max_concurrent: 16,
            list_wal_index_cache_bytes: 1024,
            schema_cache_bytes: 4_194_304, // 4MB
            max_schema_size_bytes: 16384,
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
            api_key_hashes: std::cell::RefCell::new(None),
            compaction_check_interval: Duration::from_secs(600),
            compaction_min_reclaimable_ratio: 0.20,
            compaction_temp_dir: None,
        }
    }

    // --- Client shard routing ---

    #[test]
    fn routing_exists_request_by_aggregate_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(100, 200, 7),
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 3); // 7 % 4 = 3
    }

    #[test]
    fn routing_exists_request_by_org_id() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(5, 200, 7),
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 1); // 5 % 4 = 1
    }

    #[test]
    fn routing_read_request_by_aggregate_type() {
        let config = test_config(3, crate::RoutingRule::AggregateTypeId);
        let request = ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(1, 8, 1),
            filters: ReadFilters::default(),
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 2); // 8 % 3 = 2
    }

    // --- Cluster shard routing ---

    #[test]
    fn routing_replication_uses_explicit_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClusterRequest::ReplicationBatch(ReplicationBatchRequest {
            correlation_id: None,
            shard_id: 2,
            leader_timestamp_ms: 0,
            batches: vec![],
        });
        let shard = determine_cluster_shard(&request, &config).unwrap();
        assert_eq!(shard, 2);
    }

    #[test]
    fn routing_replication_invalid_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClusterRequest::ReplicationBatch(ReplicationBatchRequest {
            correlation_id: None,
            shard_id: 10,
            leader_timestamp_ms: 0,
            batches: vec![],
        });
        let result = determine_cluster_shard(&request, &config);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters { .. })));
    }

    // --- Client shard routing (list, write, delete, watch) ---

    #[test]
    fn routing_list_orgs_uses_explicit_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClientRequest::ListOrgs(ListOrgsRequest {
            correlation_id: None,
            shard_id: 3,
            cursor: None,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 3);
    }

    #[test]
    fn routing_list_aggregate_types_uses_explicit_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClientRequest::ListAggregateTypes(ListAggregateTypesRequest {
            correlation_id: None,
            shard_id: 1,
            org_id: Some(100),
            cursor: None,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 1);
    }

    #[test]
    fn routing_list_aggregates_uses_explicit_shard_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClientRequest::ListAggregates(ListAggregatesRequest {
            correlation_id: None,
            shard_id: 0,
            org_id: Some(100),
            aggregate_type_id: Some(200),
            cursor: None,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
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
            compression_type_id: 0,
            compression_level: None,
            events: vec![],
        });
        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
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
                compression_type_id: 0,
            compression_level: None,
                events: vec![],
            },
        );
        writes.insert(
            AggregateKey::new(1, 2, 8),
            SingleAggregateWrite {
                expected_event_batch_index: None,
                allow_create: true,
                enforce_client_idempotency: false,
                compression_type_id: 0,
            compression_level: None,
                events: vec![],
            },
        );
        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
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
                compression_type_id: 0,
            compression_level: None,
                events: vec![],
            },
        );
        writes.insert(
            AggregateKey::new(1, 2, 5),
            SingleAggregateWrite {
                expected_event_batch_index: None,
                allow_create: true,
                enforce_client_idempotency: false,
                compression_type_id: 0,
            compression_level: None,
                events: vec![],
            },
        );
        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes,
        });
        let result = determine_client_shard(&request, &config);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters { .. })));
    }

    #[test]
    fn routing_write_empty_writes() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes: HashMap::new(),
        });
        let result = determine_client_shard(&request, &config);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters { .. })));
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
        let request = ClientRequest::Delete(DeleteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            deletes,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 1); // 9 % 4 = 1
    }

    #[test]
    fn routing_delete_empty_deletes() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClientRequest::Delete(DeleteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            deletes: HashMap::new(),
        });
        let result = determine_client_shard(&request, &config);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters { .. })));
    }

    #[test]
    fn routing_trim_start() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClientRequest::TrimStart(TrimStartRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(1, 2, 6),
            keep_from_event_batch_index: 10,
            client_id: 1,
            user_id: None,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 2); // 6 % 4 = 2
    }

    // --- Watch routing ---

    #[test]
    fn routing_watch_by_org_id() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let mut orgs = std::collections::HashSet::new();
        orgs.insert(5u128);
        let request = ClientRequest::Watch(WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: Some(orgs),
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 1); // 5 % 4 = 1
    }

    #[test]
    fn routing_watch_by_org_id_missing_org() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let request = ClientRequest::Watch(WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        });
        let result = determine_client_shard(&request, &config);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters { .. })));
    }

    #[test]
    fn routing_watch_by_aggregate_type_id() {
        let config = test_config(3, crate::RoutingRule::AggregateTypeId);
        let mut agg_types = std::collections::HashSet::new();
        agg_types.insert(7u128);
        let request = ClientRequest::Watch(WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: None,
            aggregate_types: Some(agg_types),
            aggregates: None,
            operation_types: None,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 1); // 7 % 3 = 1
    }

    #[test]
    fn routing_watch_by_aggregate_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let mut aggregates = std::collections::HashSet::new();
        aggregates.insert(10u128);
        let request = ClientRequest::Watch(WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: None,
            aggregate_types: None,
            aggregates: Some(aggregates),
            operation_types: None,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 2); // 10 % 4 = 2
    }

    #[test]
    fn routing_watch_multiple_orgs_same_shard() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let mut orgs = std::collections::HashSet::new();
        orgs.insert(4u128);
        orgs.insert(8u128);
        let request = ClientRequest::Watch(WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: Some(orgs),
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 0); // both 4 % 4 and 8 % 4 = 0
    }

    #[test]
    fn routing_watch_multiple_orgs_different_shards() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let mut orgs = std::collections::HashSet::new();
        orgs.insert(4u128);
        orgs.insert(5u128);
        let request = ClientRequest::Watch(WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: Some(orgs),
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        });
        let result = determine_client_shard(&request, &config);
        assert!(matches!(result, Err(ShardRoutingError::MultipleShardRoutes { num_shards: 4 })));
    }

    #[test]
    fn routing_watch_with_explicit_shard_id() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let request = ClientRequest::Watch(WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: Some(2),
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 2);
    }

    #[test]
    fn routing_watch_shard_id_overrides_filters() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let mut orgs = std::collections::HashSet::new();
        orgs.insert(5u128);
        let request = ClientRequest::Watch(WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: Some(3),
            orgs: Some(orgs),
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 3);
    }

    #[test]
    fn routing_watch_invalid_shard_id() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let request = ClientRequest::Watch(WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: Some(4),
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        });
        let result = determine_client_shard(&request, &config);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters { .. })));
    }

    // --- Shard ID validation ---

    #[test]
    fn shard_id_validation_boundary() {
        assert!(validate_shard_id(0, 4).is_ok());
        assert!(validate_shard_id(3, 4).is_ok());
        assert!(validate_shard_id(4, 4).is_err());
        assert!(validate_shard_id(100, 4).is_err());
    }

    // --- Client identity validation ---

    #[test]
    fn validate_client_id_no_verification() {
        let key = AggregateKey::new(1, 2, 3);
        let mut writes = HashMap::new();
        writes.insert(key, SingleAggregateWrite {
            expected_event_batch_index: None,
            allow_create: true,
            enforce_client_idempotency: false,
            compression_type_id: 0,
            compression_level: None,
            events: vec![],
        });
        let req = ClientRequest::Write(WriteRequest {
            correlation_id: Some(123),
            client_id: 999,
            user_id: None,
            writes,
        });
        assert!(super::validate_client_id(&req, None).is_ok());
    }

    #[test]
    fn validate_client_id_match() {
        let key = AggregateKey::new(1, 2, 3);
        let mut writes = HashMap::new();
        writes.insert(key, SingleAggregateWrite {
            expected_event_batch_index: None,
            allow_create: true,
            enforce_client_idempotency: false,
            compression_type_id: 0,
            compression_level: None,
            events: vec![],
        });
        let req = ClientRequest::Write(WriteRequest {
            correlation_id: Some(123),
            client_id: 999,
            user_id: None,
            writes,
        });
        assert!(super::validate_client_id(&req, Some(999)).is_ok());
    }

    #[test]
    fn validate_client_id_mismatch() {
        let key = AggregateKey::new(1, 2, 3);
        let mut writes = HashMap::new();
        writes.insert(key, SingleAggregateWrite {
            expected_event_batch_index: None,
            allow_create: true,
            enforce_client_idempotency: false,
            compression_type_id: 0,
            compression_level: None,
            events: vec![],
        });
        let req = ClientRequest::Write(WriteRequest {
            correlation_id: Some(123),
            client_id: 999,
            user_id: None,
            writes,
        });
        let result = super::validate_client_id(&req, Some(888));
        assert!(result.is_err());
        if let Err(ClientResponse::GenericError(err)) = result {
            assert_eq!(err.error_code, IDENTIFY_MISMATCH);
            assert!(err.error_message.contains("999"));
            assert!(err.error_message.contains("888"));
        } else {
            panic!("Expected GenericError with IDENTITY_MISMATCH");
        }
    }

    #[test]
    fn validate_client_id_read_always_passes() {
        let req = ClientRequest::Read(ReadRequest {
            correlation_id: Some(123),
            aggregate_key: AggregateKey::new(1, 2, 3),
            filters: ReadFilters::default(),
        });
        assert!(super::validate_client_id(&req, Some(999)).is_ok());
    }

    #[test]
    fn validate_client_id_trim_start() {
        let req = ClientRequest::TrimStart(TrimStartRequest {
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
        let req = ClientRequest::Delete(DeleteRequest {
            correlation_id: Some(123),
            client_id: 555,
            user_id: None,
            deletes,
        });
        assert!(super::validate_client_id(&req, Some(555)).is_ok());
        assert!(super::validate_client_id(&req, Some(444)).is_err());
    }
}
