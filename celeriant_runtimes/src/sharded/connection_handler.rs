use std::{cell::{Cell, RefCell}, collections::HashMap, fmt, future::Future, io, pin::Pin, rc::Rc, task::{Context, Poll}, time::Duration};

use futures_lite::AsyncRead;

use base64::Engine;
use celeriant_distributed::{lease_store::LeaseStore, node_status::NodeStatus, node_status_logic::compute_new_ttl, s3_lease_manager::S3LeaseManager, validated_node_status::{self, ValidatedNodeStatus, set_node_status_and_metric}};
use celeriant_msg::{
    error_codes,
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
use celeriant_wire::codec::compression::DictCodec;
use celeriant_watch::{aggregate_reader::WatchReadError, watch_output_type::WatchOutputType, watch_session::WatchSession};
use celeriant_msg::read_wire_data_error::ReadWireDataError;
use celeriant_wire::network::wire_error::WireError;
use celeriant_wire::network::wire_header::WireHeader;
use glommio::{channels::{channel_mesh::Senders, local_channel::LocalSender}, net::TcpStream};
use tracing::{debug, info, warn};

use super::{
    intrashard_messages::{IntrashardMessages, RedirectedConnection},
    shard::try_send_with_retry,
    shard_config::ShardConfig,
    shard_error_response::{shard_error_to_client_response, shard_error_to_cluster_response, shard_routing_error_to_code, watch_read_error_to_client_response, watch_session_error_to_client_response},
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
    pub lease_manager: Option<Rc<S3LeaseManager<S>>>,
    pub dict_codec: Rc<DictCodec>,
    pub extension_redirect_sink: Option<Rc<LocalSender<RedirectedConnection>>>,
}

/// Connection-level state for identity verification and access control
struct ConnectionState {
    verified_client_id: Option<u128>,
    access_level: Option<celeriant_msg::response::responses::AccessLevel>,
    client_has_dict: bool,
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
            dict_codec: self.dict_codec.clone(),
            extension_redirect_sink: self.extension_redirect_sink.clone(),
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

pub fn handle_new_connection<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(mut tcp_stream: TcpStream, trailing: Vec<u8>, ctx: ConnectionContext<R, D, S>, port_type: PortType) {
    let _ = tcp_stream.set_nodelay(true);

    let peer_addr = tcp_stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();

    glommio::spawn_local(async move {
        if ctx.shutdown_requested.get() {
            return;
        }

        debug!(shard_id = ctx.current_shard_id, peer = %peer_addr, ?port_type, trailing_bytes = trailing.len(), "Connection accepted");

        let shard_label = [("shard_id", ctx.current_shard_id.to_string())];
        metrics::gauge!("celeriant_client_connections_active", &shard_label).increment(1.0);
        let _guard = ConnectionGuard(&shard_label);

        match port_type {
            PortType::Client => {
                let mut conn_state = ConnectionState {
                    verified_client_id: None,
                    access_level: None,
                    client_has_dict: false,
                };

                let first_message = if trailing.is_empty() {
                    match read_first_message(&mut tcp_stream, &ctx).await {
                        Some(m) => m,
                        None => return,
                    }
                } else {
                    let mut reader = PrefixedReader::new(trailing, &mut tcp_stream);
                    match read_first_message_from(&mut reader, &ctx).await {
                        Some(m) => m,
                        None => return,
                    }
                };

                let (request, message_version) = match first_message {
                    FirstMessage::Identify(identify_req, version) => {
                        if let Err(response) = handle_identify(&identify_req, &mut conn_state, &ctx.config) {
                            let _ = write_client_response_with_timeout(&mut tcp_stream, &response, ctx.config.max_response_size, conn_state.client_has_dict, &ctx.dict_codec, version, ctx.config.slow_client_timeout).await;
                            return;
                        }
                        let (compression_dict_sha256, compression_dict_bytes) =
                            build_identify_dict_fields(&identify_req.known_dict_sha256, &ctx.config);
                        let res = IdentifyResponse {
                            correlation_id: identify_req.correlation_id,
                            client_id: conn_state.verified_client_id,
                            access_level: conn_state.access_level,
                            compression_dict_sha256,
                            compression_dict_bytes,
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
                                error_code: error_codes::IDENTIFY_REQUIRED,
                                error_message: "Server requires client identity verification".to_string(),
                            });
                            let _ = write_client_response_with_timeout(&mut tcp_stream, &response, ctx.config.max_response_size, conn_state.client_has_dict, &ctx.dict_codec, version, ctx.config.slow_client_timeout).await;
                            return;
                        }
                        (request, version)
                    }
                };

                match check_client_redirect(tcp_stream, request, ctx.config.max_response_size, message_version, &ctx, &conn_state).await {
                    ClientRedirectResult::ProcessLocally(request, tcp_stream) => {
                        handle_client_pipelining(tcp_stream, Some(request), ctx.config.max_response_size, message_version, ctx, conn_state).await;
                    }
                    ClientRedirectResult::Redirected => {}
                    ClientRedirectResult::ErrorSentContinue(tcp_stream) => {
                        handle_client_pipelining(tcp_stream, None, ctx.config.max_response_size, message_version, ctx, conn_state).await;
                    }
                }
            }
            PortType::Replication => {
                let (request, message_version) = if trailing.is_empty() {
                    match read_cluster_request(&mut tcp_stream, &ctx).await {
                        Some(r) => r,
                        None => return,
                    }
                } else {
                    let mut reader = PrefixedReader::new(trailing, &mut tcp_stream);
                    match read_cluster_request_from(&mut reader, &ctx).await {
                        Some(r) => r,
                        None => return,
                    }
                };

                match check_cluster_redirect(tcp_stream, request, ctx.config.max_response_size, message_version, &ctx).await {
                    ClusterRedirectResult::ProcessLocally(request, tcp_stream) => {
                        handle_cluster_pipelining(tcp_stream, Some(request), ctx.config.max_response_size, message_version, ctx).await;
                    }
                    ClusterRedirectResult::Redirected => {}
                    ClusterRedirectResult::ErrorSentContinue(tcp_stream) => {
                        handle_cluster_pipelining(tcp_stream, None, ctx.config.max_response_size, message_version, ctx).await;
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
    message_version: u32,
    ctx: ConnectionContext<R, D, S>,
    verified_client_id: Option<u128>,
    access_level: Option<AccessLevel>,
) {
    let _ = tcp_stream.set_nodelay(true);

    debug!(
        shard_id = ctx.current_shard_id,
        request_type = ?request.request_type(),
        "Client connection redirected to shard",
    );

    glommio::spawn_local(async move {
        let conn_state = ConnectionState { verified_client_id, access_level, client_has_dict: false };
        handle_client_pipelining(tcp_stream, Some(request), max_response_size, message_version, ctx, conn_state).await;
    })
    .detach();
}

pub fn handle_redirected_cluster_connection<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: TcpStream,
    request: ClusterRequest,
    max_response_size: u64,
    message_version: u32,
    ctx: ConnectionContext<R, D, S>,
) {
    let _ = tcp_stream.set_nodelay(true);

    glommio::spawn_local(async move {
        handle_cluster_pipelining(tcp_stream, Some(request), max_response_size, message_version, ctx).await;
    })
    .detach();
}

pub fn handle_enter_s3_catchup<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: ConnectionContext<R, D, S>,
) {
    glommio::spawn_local(async move {
        let result = ctx.shard_wal.enter_s3_catchup().await;

        if let Err(e) = try_send_with_retry(
            ctx.intrashard_sender.as_ref(), 0,
            IntrashardMessages::S3CatchupComplete { shard_id: ctx.current_shard_id, result }, 10
        ).await {
            panic!("Shard {} failed to send S3CatchupComplete to shard 0 after retries: {e:?}", ctx.current_shard_id);
        }
    })
    .detach();
}

async fn handle_client_pipelining<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    mut tcp_stream: TcpStream,
    request: Option<ClientRequest>,
    max_response_size: u64,
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
                let _ = write_client_response_with_timeout(&mut tcp_stream, &response, max_response_size, conn_state.client_has_dict, &ctx.dict_codec, message_version, ctx.config.slow_client_timeout).await;
                continue;
            }

            if !is_valid_for_access_level(&request, conn_state.access_level) {
                let response = ClientResponse::GenericError(ErrorResponse {
                    correlation_id: request.correlation_id(),
                    error_code: error_codes::AUTH_INSUFFICIENT_PERMISSIONS,
                    error_message: "Insufficient permissions for this operation".to_string(),
                });
                let _ = write_client_response_with_timeout(&mut tcp_stream, &response, max_response_size, conn_state.client_has_dict, &ctx.dict_codec, message_version, ctx.config.slow_client_timeout).await;
                continue;
            }

            if let ClientRequest::Watch(watch_request) = request {
                handle_watch(tcp_stream, watch_request, max_response_size, conn_state.client_has_dict, &ctx.dict_codec, message_version, &ctx).await;
                return;
            }

            process_client_request(&mut tcp_stream, &ctx, request, max_response_size, conn_state.client_has_dict, message_version).await;
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
            match check_client_redirect(tcp_stream, request, max_response_size, message_version, &ctx, &conn_state).await {
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
    mut message_version: u32,
    ctx: ConnectionContext<R, D, S>,
) {
    let mut optional_request = request;

    loop {
        if ctx.shutdown_requested.get() {
            break;
        }

        if let Some(request) = optional_request.take() {
            process_cluster_request(&mut tcp_stream, &ctx, request, max_response_size, message_version).await;
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
            match check_cluster_redirect(tcp_stream, request, max_response_size, message_version, &ctx).await {
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
                debug!("Identity rejected: nonce expired or clock skew");
                return Err(ClientResponse::GenericError(ErrorResponse {
                    correlation_id: req.correlation_id,
                    error_code: error_codes::IDENTIFY_INVALID_NONCE,
                    error_message: "Nonce expired or too far in the future".to_string(),
                }));
            }
            Err(_) => {
                debug!("Identity rejected: invalid signature");
                return Err(ClientResponse::GenericError(ErrorResponse {
                    correlation_id: req.correlation_id,
                    error_code: error_codes::IDENTIFY_INVALID_SIGNATURE,
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
                error_code: error_codes::AUTH_REQUIRED,
                error_message: "API key required but not provided".to_string(),
            })
        })?;

        let api_key_bytes = base64::engine::general_purpose::STANDARD
            .decode(api_key_str)
            .map_err(|_| {
                ClientResponse::GenericError(ErrorResponse {
                    correlation_id: req.correlation_id,
                    error_code: error_codes::AUTH_INVALID_KEY,
                    error_message: "Invalid API key format".to_string(),
                })
            })?;

        if api_key_bytes.len() != 32 {
            return Err(ClientResponse::GenericError(ErrorResponse {
                correlation_id: req.correlation_id,
                error_code: error_codes::AUTH_INVALID_KEY,
                error_message: "Invalid API key length".to_string(),
            }));
        }

        let mut key_array = [0u8; 32];
        key_array.copy_from_slice(&api_key_bytes);

        let key_hash = celeriant_crypto::hash_api_key(&key_array);

        let access_level = api_key_hashes.validate(&key_hash).ok_or_else(|| {
            debug!("Identity rejected: invalid API key");
            ClientResponse::GenericError(ErrorResponse {
                correlation_id: req.correlation_id,
                error_code: error_codes::AUTH_INVALID_KEY,
                error_message: "Invalid API key".to_string(),
            })
        })?;

        conn_state.access_level = Some(access_level);
    } else {
        conn_state.access_level = None;
    }

    conn_state.client_has_dict = true;

    Ok(())
}

/// Returns (compression_dict_sha256, compression_dict_bytes) for the IdentifyResponse.
///
/// Always includes the sha. Includes bytes only when the client doesn't already have
/// this exact dict version.
fn build_identify_dict_fields(
    known_dict_sha256: &Option<String>,
    config: &ShardConfig,
) -> (Option<String>, Option<Vec<u8>>) {
    let cluster_sha = &config.dict_sha256;
    let client_already_has_it = known_dict_sha256.as_deref() == Some(cluster_sha.as_ref());
    let bytes = if client_already_has_it {
        None
    } else {
        Some(config.dict_bytes.to_vec())
    };
    (Some(cluster_sha.to_string()), bytes)
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
                error_code: error_codes::IDENTIFY_MISMATCH,
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
            let _ = write_client_response_with_timeout(&mut tcp_stream, &response, max_message_size, conn_state.client_has_dict, &ctx.dict_codec, message_version, ctx.config.slow_client_timeout).await;
            return ClientRedirectResult::ErrorSentContinue(tcp_stream);
        }
    };

    if target_shard != ctx.current_shard_id {
        debug!(
            from_shard = ctx.current_shard_id,
            to_shard = target_shard,
            "Client connection redirected to another shard"
        );
        metrics::counter!("celeriant_connection_redirects_total").increment(1);
        let correlation_id = request.correlation_id();
        let msg = IntrashardMessages::ClientConnectionRedirect {
            accepted_tcp_stream: tcp_stream.into_accepted(),
            request,
            message_version,
            verified_client_id: conn_state.verified_client_id,
            access_level: conn_state.access_level,
        };
        if let Err(e) = ctx.intrashard_sender.try_send_to(target_shard, msg) {
            metrics::counter!("celeriant_mesh_channel_full_total", "message_type" => "client_redirect").increment(1);
            warn!("Mesh channel full, rejecting client redirect to shard {target_shard}: {e:?}");
            if let Some(inner) = e.into_inner() {
                if let IntrashardMessages::ClientConnectionRedirect { accepted_tcp_stream, .. } = inner {
                    let mut stream = accepted_tcp_stream.bind_to_executor();
                    let response = ClientResponse::GenericError(ErrorResponse {
                        correlation_id,
                        error_code: error_codes::SERVER_BUSY,
                        error_message: "{}".into(),
                    });
                    let _ = write_client_response_with_timeout(&mut stream, &response, max_message_size, conn_state.client_has_dict, &ctx.dict_codec, message_version, ctx.config.slow_client_timeout).await;
                    return ClientRedirectResult::ErrorSentContinue(stream);
                }
            }
        }
        return ClientRedirectResult::Redirected;
    }

    ClientRedirectResult::ProcessLocally(request, tcp_stream)
}

async fn check_cluster_redirect<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    mut tcp_stream: TcpStream,
    request: ClusterRequest,
    max_message_size: u64,
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
            let _ = write_cluster_response_with_timeout(&mut tcp_stream, &response, max_message_size, message_version, ctx.config.slow_client_timeout).await;
            return ClusterRedirectResult::ErrorSentContinue(tcp_stream);
        }
    };

    if target_shard != ctx.current_shard_id {
        let correlation_id = request.correlation_id();
        let msg = IntrashardMessages::ClusterConnectionRedirect {
            accepted_tcp_stream: tcp_stream.into_accepted(),
            request,
            message_version,
        };
        if let Err(e) = ctx.intrashard_sender.try_send_to(target_shard, msg) {
            metrics::counter!("celeriant_mesh_channel_full_total", "message_type" => "cluster_redirect").increment(1);
            warn!("Mesh channel full, rejecting cluster redirect to shard {target_shard}: {e:?}");
            if let Some(inner) = e.into_inner() {
                if let IntrashardMessages::ClusterConnectionRedirect { accepted_tcp_stream, .. } = inner {
                    let mut stream = accepted_tcp_stream.bind_to_executor();
                    let response = ClusterResponse::GenericError(ErrorResponse {
                        correlation_id,
                        error_code: error_codes::SERVER_BUSY,
                        error_message: "{}".into(),
                    });
                    let _ = write_cluster_response_with_timeout(&mut stream, &response, max_message_size, message_version, ctx.config.slow_client_timeout).await;
                    return ClusterRedirectResult::ErrorSentContinue(stream);
                }
            }
        }
        return ClusterRedirectResult::Redirected;
    }

    ClusterRedirectResult::ProcessLocally(request, tcp_stream)
}

/// Route a routing_id to a data shard. When `reserve_coordinator_shard` is true,
/// shard 0 is reserved for coordination and data routes to shards 1..num_shards-1.
fn data_shard(routing_id: u128, config: &ShardConfig) -> usize {
    data_shard_for_routing_id(routing_id, config.num_shards, config.reserve_coordinator_shard)
}

pub fn data_shard_for_routing_id(routing_id: u128, num_shards: u32, reserve_coordinator_shard: bool) -> usize {
    let num_shards = num_shards as u128;

    if num_shards <= 1 {
        return 0;
    }
    if reserve_coordinator_shard {
        let data_shards = num_shards - 1;
        (routing_id % data_shards + 1) as usize
    } else {
        (routing_id % num_shards) as usize
    }
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
            Ok(data_shard(routing_id, config))
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
        let id = data_shard(routing_id, config);
        match shard_id {
            None => shard_id = Some(id),
            Some(first) if first != id => {
                return Err(ShardRoutingError::MultipleShardRoutes {
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
        let id = data_shard(routing_id, config);
        match shard_id {
            None => shard_id = Some(id),
            Some(first) if first != id => {
                return Err(ShardRoutingError::MultipleShardRoutes {
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
            collect_unique_shard_id(config, &req.orgs)
        }
        crate::RoutingRule::AggregateTypeId => {
            if req.aggregate_types.is_none() || req.aggregate_types.as_ref().unwrap().is_empty() {
                return Err(ShardRoutingError::IncompatibleFilters {
                    detail: "Must specify at least one aggregate type. Server is setup to shard by aggregate type.".into(),
                    num_shards: num_shards as u64,
                });
            }
            collect_unique_shard_id(config, &req.aggregate_types)
        }
        crate::RoutingRule::AggregateId => {
            if req.aggregates.is_none() || req.aggregates.as_ref().unwrap().is_empty() {
                return Err(ShardRoutingError::IncompatibleFilters {
                    detail: "Must specify at least one aggregate. Server is setup to shard by aggregate.".into(),
                    num_shards: num_shards as u64,
                });
            }
            collect_unique_shard_id(config, &req.aggregates)
        }
    }
}

fn collect_unique_shard_id(
    config: &ShardConfig,
    sources: &Option<std::collections::HashSet<u128>>,
) -> Result<usize, ShardRoutingError> {
    let set = sources.as_ref().ok_or(ShardRoutingError::NoRoutingKeyProvided)?;
    let mut shard_id: Option<usize> = None;

    for &routing_key in set.iter() {
        let computed_shard = data_shard(routing_key, config);
        match shard_id {
            None => shard_id = Some(computed_shard),
            Some(existing) if existing != computed_shard => {
                return Err(ShardRoutingError::MultipleShardRoutes { num_shards: config.num_shards as u64 });
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
            let req = ClientRequest::read_from_header(header, tcp_stream, &ctx.dict_codec).await?;
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
        let req = ClientRequest::read_from_header(header, tcp_stream, &ctx.dict_codec).await?;
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
        let header = WireHeader::from_reader(tcp_stream, ctx.config.internode_max_request_size).await
            .map_err(ReadWireDataError::ReadHeaderFailure)?;
        let version = header.version;
        let req = ClusterRequest::read_from_header(header, tcp_stream, &ctx.dict_codec).await?;
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

/// Wraps a prefix buffer and an inner reader, serving the prefix first.
/// Used to deliver application data that was buffered during the kTLS handshake.
struct PrefixedReader<'a> {
    prefix: Vec<u8>,
    prefix_pos: usize,
    inner: &'a mut TcpStream,
}

impl<'a> PrefixedReader<'a> {
    fn new(prefix: Vec<u8>, inner: &'a mut TcpStream) -> Self {
        Self { prefix, prefix_pos: 0, inner }
    }
}

impl AsyncRead for PrefixedReader<'_> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let remaining = this.prefix.len() - this.prefix_pos;
        if remaining > 0 {
            let n = remaining.min(buf.len());
            buf[..n].copy_from_slice(&this.prefix[this.prefix_pos..this.prefix_pos + n]);
            this.prefix_pos += n;
            Poll::Ready(Ok(n))
        } else {
            Pin::new(&mut *this.inner).poll_read(cx, buf)
        }
    }
}

async fn read_first_message_from<Rd: futures_lite::AsyncReadExt + Unpin, R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    reader: &mut Rd,
    ctx: &ConnectionContext<R, D, S>,
) -> Option<FirstMessage> {
    read_with_timeout(ctx.config.slow_client_timeout, ctx.current_shard_id, "first message", async {
        let header = WireHeader::from_reader(reader, ctx.config.max_request_size).await
            .map_err(ReadWireDataError::ReadHeaderFailure)?;
        let version = header.version;
        if header.message_type == IDENTIFY_REQUEST_TYPE_ID {
            let req = read_identify_request(header, reader).await?;
            Ok(FirstMessage::Identify(req, version))
        } else {
            let req = ClientRequest::read_from_header(header, reader, &ctx.dict_codec).await?;
            Ok(FirstMessage::ClientRequest(req, version))
        }
    }).await
}

async fn read_cluster_request_from<Rd: futures_lite::AsyncReadExt + Unpin, R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    reader: &mut Rd,
    ctx: &ConnectionContext<R, D, S>,
) -> Option<(ClusterRequest, u32)> {
    let result: Result<(ClusterRequest, u32), ReadWireDataError> = async {
        let header = WireHeader::from_reader(reader, ctx.config.internode_max_request_size).await
            .map_err(ReadWireDataError::ReadHeaderFailure)?;
        let version = header.version;
        let req = ClusterRequest::read_from_header(header, reader, &ctx.dict_codec).await?;
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

async fn write_client_response_with_timeout(
    tcp_stream: &mut TcpStream,
    response: &ClientResponse,
    max_message_size: u64,
    client_has_dict: bool,
    dict_codec: &DictCodec,
    message_version: u32,
    timeout_duration: Duration,
) -> Result<(), WireError> {
    match glommio::timer::timeout(timeout_duration, async {
        let result = ClientResponse::write_response(
            tcp_stream, response,
            client_has_dict, dict_codec,
            max_message_size, message_version,
        ).await;
        Ok::<_, glommio::GlommioError<()>>(result)
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(WireError::NetworkError(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out"))),
    }
}

async fn write_cluster_response_with_timeout(
    tcp_stream: &mut TcpStream,
    response: &ClusterResponse,
    max_message_size: u64,
    message_version: u32,
    timeout_duration: Duration,
) -> Result<(), WireError> {
    // Cluster (replication) responses use None — Inv 7.
    match glommio::timer::timeout(timeout_duration, async {
        let result = ClusterResponse::write_response(
            tcp_stream, response, max_message_size, message_version,
        ).await;
        Ok::<_, glommio::GlommioError<()>>(result)
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(WireError::NetworkError(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out"))),
    }
}

async fn handle_schema_registration_coordination<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    ctx: &ConnectionContext<R, D, S>,
    request: celeriant_msg::request::requests::RegisterSchemaRequest,
) -> Result<celeriant_msg::response::responses::RegisterSchemaResponse, celeriant_shard::error::shard_schema_error::ShardSchemaError> {
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

        if try_send_with_retry(ctx.intrashard_sender.as_ref(), shard_id, msg, 10).await.is_err() {
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
    client_has_dict: bool,
    message_version: u32,
) {
    let correlation_id = request.correlation_id();

    if matches!(request, ClientRequest::Write(_) | ClientRequest::Delete(_) | ClientRequest::TrimStart(_) | ClientRequest::RegisterSchema(_)) {
        debug!(
            shard_id = ctx.current_shard_id,
            request_type = ?request.request_type(),
            "Processing mutating client request",
        );
    }

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

    let _ = write_client_response_with_timeout(tcp_stream, &response, max_message_size, client_has_dict, &ctx.dict_codec, message_version, ctx.config.slow_client_timeout).await;
}

async fn process_cluster_request<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    tcp_stream: &mut TcpStream,
    ctx: &ConnectionContext<R, D, S>,
    request: ClusterRequest,
    max_message_size: u64,
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
    let _ = write_cluster_response_with_timeout(tcp_stream, &response, max_message_size, message_version, ctx.config.slow_client_timeout).await;
}

/// Follower shard 0 heartbeat handler.
///
/// Validates role and clock drift, then refreshes the TTL on all local shards
/// so they don't self-fence via ValidatedNodeStatus::effective().
async fn handle_heartbeat<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    req: &HeartbeatRequest,
    ctx: &ConnectionContext<R, D, S>,
) -> ClusterResponse {
    let follower_ms = validated_node_status::unix_epoch_now_ms();
    let shard_label = ctx.current_shard_id.to_string();
    metrics::counter!("celeriant_heartbeat_received_total", &[("shard_id", shard_label.clone())]).increment(1);

    let current_status = ctx.shard_wal.node_status.get();
    let local_lease_epoch = current_status.raw().lease_epoch().unwrap_or(0);

    // Determine whether to accept the heartbeat and which raw_status to use.
    let raw_status = if current_status.is_any_follower_state() {
        // Normal path: already a follower, accept heartbeat.
        current_status.raw()
    } else if let NodeStatus::Promoting { lease_epoch } = current_status.raw() {
        // Raw, not effective: even a decayed (expired) promotion must refuse a
        // zombie's adoption — a heartbeat at epoch <= our won epoch is from a
        // deposed leader and must not re-open the replication gate mid-window.
        match promoting_heartbeat_adoption(lease_epoch, req.lease_epoch) {
            Some(adopted) => {
                tracing::warn!(
                    shard_id = req.shard_id,
                    promoting_lease_epoch = lease_epoch,
                    remote_lease_epoch = req.lease_epoch,
                    "Promotion lost the race to a higher-epoch leader, stepping down to follower"
                );
                adopted
            }
            None => {
                metrics::counter!("celeriant_heartbeat_received_outcomes_total", &[("shard_id", shard_label.clone()), ("outcome", "rejected_promoting".to_string())]).increment(1);
                return ClusterResponse::Heartbeat(HeartbeatResponse {
                    correlation_id: req.correlation_id,
                    result: HeartbeatResult::Rejected(HeartbeatRejection::NotAFollower),
                });
            }
        }
    } else if current_status.is_fenced() {
        // Fenced node (e.g. old leader whose TTL expired): adopt follower role
        // with the remote leader's lease_epoch.
        tracing::warn!(
            shard_id = req.shard_id,
            local_lease_epoch,
            remote_lease_epoch = req.lease_epoch,
            "Fenced node accepting heartbeat from leader, transitioning to follower"
        );
        NodeStatus::Follower { leader_lease_epoch: req.lease_epoch }
    } else if current_status.is_leader() && req.lease_epoch > local_lease_epoch {
        // This node thinks it's leader, but the heartbeat sender won a more
        // recent S3 election. Step down immediately instead of waiting for
        // the slow S3 discovery path.
        tracing::warn!(
            shard_id = req.shard_id,
            local_lease_epoch,
            remote_lease_epoch = req.lease_epoch,
            "Received heartbeat from leader with higher lease_epoch, stepping down"
        );
        NodeStatus::Follower { leader_lease_epoch: req.lease_epoch }
    } else {
        // Stale heartbeat (lower/equal lease_epoch from old leader), or
        // BootCatchup/Standalone — reject.
        metrics::counter!("celeriant_heartbeat_received_outcomes_total", &[("shard_id", shard_label.clone()), ("outcome", "rejected_not_follower".to_string())]).increment(1);
        return ClusterResponse::Heartbeat(HeartbeatResponse {
            correlation_id: req.correlation_id,
            result: HeartbeatResult::Rejected(HeartbeatRejection::NotAFollower),
        });
    };

    // Clock drift too high — nodes' clocks are dangerously skewed. Fence all local
    // shards immediately rather than waiting for TTL expiry, because we can't trust
    // any time-based decisions with skewed clocks.
    let drift = follower_ms.abs_diff(req.leader_timestamp_ms);
    metrics::gauge!("celeriant_clock_drift_ms").set(drift as f64);
    if drift > ctx.config.max_clock_drift_ms {
        tracing::error!(
            leader_ms = req.leader_timestamp_ms,
            follower_ms,
            drift_ms = drift,
            max_allowed_ms = ctx.config.max_clock_drift_ms,
            "Clock drift too high, fencing all shards"
        );
        // Remember whether leadership was held at fence time: a later demotion may
        // rewind an ex-leader's tail to its ack barrier, but a fenced ex-follower's
        // tail is peer data that must survive.
        let fenced = ValidatedNodeStatus::create_fenced(ctx.shard_wal.node_status.get().held_leadership());
        set_node_status_and_metric(&ctx.shard_wal.node_status, fenced, ctx.current_shard_id as u32);
        broadcast_status(ctx, fenced).await;
        metrics::counter!("celeriant_heartbeat_received_outcomes_total", &[("shard_id", shard_label.clone()), ("outcome", "rejected_clock_drift".to_string())]).increment(1);

        return ClusterResponse::Heartbeat(HeartbeatResponse {
            correlation_id: req.correlation_id,
            result: HeartbeatResult::Rejected(HeartbeatRejection::ClockDriftTooHigh {
                leader_ms: req.leader_timestamp_ms,
                follower_ms,
                max_allowed_ms: ctx.config.max_clock_drift_ms,
            }),
        });
    }

    // A heartbeat from a higher-epoch peer (req.lease_epoch > local) demotes a non-follower
    // (Fenced/Leader) to follower. Reconcile the durable tail first, mirroring the
    // election-path CullSpeculativeTail, or an own-speculation tail strands (write>read)
    // and S3 catchup wedges on no-common-ancestor. A self-reclaim never accepts a peer
    // heartbeat, so it is unaffected. Reconcile first so a following EnterS3Catchup lands
    // after. The mode follows the same rule as promotion_cull_flags: rewinding to the ack
    // barrier is only sound when leadership was actually held (last_self_acked is a real
    // floor and the tail is own speculation); a fenced ex-follower or a booting node may
    // hold a peer tail, which provenance-checked reconciliation keeps parked.
    let demoting_to_higher_epoch_peer = !current_status.is_any_follower_state()
        && raw_status.is_follower()
        && req.lease_epoch > local_lease_epoch;
    if demoting_to_higher_epoch_peer {
        let mode = crate::sharded::shard::demotion_mode(current_status.held_leadership());
        let shard_count = ctx.intrashard_sender.nr_consumers();
        for peer in 0..shard_count {
            if peer == ctx.current_shard_id {
                continue;
            }
            let _ = try_send_with_retry(
                ctx.intrashard_sender.as_ref(), peer,
                IntrashardMessages::CullSpeculativeTail { mode }, 10,
            ).await;
        }
        if let Err(e) = ctx.shard_wal.reconcile_durable_tail(mode).await {
            tracing::warn!(shard_id = ctx.current_shard_id, error = ?e,
                "heartbeat-demotion reconcile failed, catchup may wedge on no-common-ancestor");
        }
    }

    // Extend the lease due to the live heartbeat
    let current_lease_expiry_ms = ctx.shard_wal.node_status.get().lease_expires_at_ms();
    let new_lease_expiry_ms = compute_new_ttl(
        current_lease_expiry_ms,
        req.leader_timestamp_ms,
        ctx.config.heartbeat_lease_duration.as_millis() as u64,
    );
    let outcome_label = if new_lease_expiry_ms > current_lease_expiry_ms {
        "accepted_extended"
    } else {
        "accepted_no_extension"
    };
    metrics::counter!("celeriant_heartbeat_received_outcomes_total", &[("shard_id", shard_label.clone()), ("outcome", outcome_label.to_string())]).increment(1);

    let refreshed = ValidatedNodeStatus::create_custom_status(
        raw_status,
        ctx.config.max_clock_drift_ms,
        new_lease_expiry_ms);
    set_node_status_and_metric(&ctx.shard_wal.node_status, refreshed, ctx.current_shard_id as u32);
    broadcast_status(ctx, refreshed).await;

    ClusterResponse::Heartbeat(HeartbeatResponse {
        correlation_id: req.correlation_id,
        result: HeartbeatResult::Ack {
            follower_timestamp_ms: follower_ms,
            follower_can_accept_tcp_replication: raw_status.is_follower(),
        },
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
        let leader_lease_epoch = status.raw().lease_epoch_for_logging();
        // A kick is a liveness signal from the leader: set TTL as if this were a heartbeat so
        // that decide_post_catchup_action returns StayFollower instead of ChallengeViaCAS.
        let now_ms = validated_node_status::unix_epoch_now_ms();
        let current_expiry_ms = status.lease_expires_at_ms();
        let new_expiry_ms = compute_new_ttl(
            current_expiry_ms,
            now_ms,
            ctx.config.heartbeat_lease_duration.as_millis() as u64,
        );
        let catching_up = ValidatedNodeStatus::create_custom_status(
            NodeStatus::FollowerCatchingUp { leader_lease_epoch },
            ctx.config.max_clock_drift_ms,
            new_expiry_ms,
        );
        set_node_status_and_metric(&ctx.shard_wal.node_status, catching_up, ctx.current_shard_id as u32);
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
        let _ = try_send_with_retry(
            ctx.intrashard_sender.as_ref(), peer,
            // These status updates originate from follower-side events (kick, clock-drift fence,
            // heartbeat receipt). They are not CAS-confirmed leases, so cas_confirmed_at_ms=None.
            // leader_changed_hands=false: none of these are a peer-takeover promotion.
            IntrashardMessages::StatusUpdate { status, cas_confirmed_at_ms: None, leader_changed_hands: false }, 10
        ).await;
    }
}

async fn handle_watch<R: ReplicationClient + 'static, D: S3Downloader + 'static, S: LeaseStore + 'static>(
    mut tcp_stream: TcpStream,
    watch_request: WatchRequest,
    max_message_size: u64,
    client_has_dict: bool,
    dict_codec: &DictCodec,
    message_version: u32,
    ctx: &ConnectionContext<R, D, S>,
) {
    let correlation_id = watch_request.correlation_id;

    let mut watch_session = match create_watch_session(&ctx.shard_wal, watch_request, ctx.config.max_requested_latency, ctx.config.max_watch_subscribers) {
        Ok(session) => session,
        Err(error) => {
            let response = watch_session_error_to_client_response(correlation_id, error);
            let _ = write_client_response_with_timeout(&mut tcp_stream, &response, max_message_size, client_has_dict, dict_codec, message_version, ctx.config.slow_client_timeout).await;
            return;
        }
    };

    metrics::gauge!("celeriant_watch_subscribers_active").increment(1.0);

    // Ack the subscription immediately so the client confirms the watch without
    // blocking until the first idle heartbeat (~5s). Empty events make this a
    // heartbeat-shaped frame the client already treats as a no-op.
    let ack = ClientResponse::Watch(WatchResponse::default());
    if write_client_response_with_timeout(&mut tcp_stream, &ack, max_message_size, client_has_dict, dict_codec, message_version, ctx.config.slow_client_timeout).await.is_err() {
        metrics::gauge!("celeriant_watch_subscribers_active").decrement(1.0);
        return;
    }

    // A watch is connection-terminal. Client only reads from here on. Race
    // each outgoing frame against a readable/EOF poll so a client disconnect (FIN)
    // is detected promptly instead of lagging a full heartbeat; that lag is what
    // left sockets stuck in CLOSE-WAIT. peek() borrows the stream shared and the
    // race future drops before the &mut write below, so there's no borrow conflict.
    enum WatchStep {
        Frame(Result<WatchOutputType, WatchReadError>),
        PeerGone,
    }

    loop {
        let mut peek_buf = [0u8; 1];
        let step = futures_lite::future::or(
            async { WatchStep::Frame(watch_session.next().await) },
            async {
                // Ok(0) = FIN, Ok(n) = unexpected client data, Err = socket error.
                // Any of these means the watcher is gone; stop.
                let _ = tcp_stream.peek(&mut peek_buf).await;
                WatchStep::PeerGone
            },
        ).await;

        match step {
            WatchStep::PeerGone => break,
            WatchStep::Frame(Ok(WatchOutputType::Continue)) => continue,
            WatchStep::Frame(Ok(WatchOutputType::Response(watch_response))) => {
                let response = ClientResponse::Watch(watch_response);
                if write_client_response_with_timeout(&mut tcp_stream, &response, max_message_size, client_has_dict, dict_codec, message_version, ctx.config.slow_client_timeout).await.is_err() {
                    break;
                }
            }
            WatchStep::Frame(Ok(WatchOutputType::Heartbeat)) => {
                let response = ClientResponse::Watch(WatchResponse::default());
                if write_client_response_with_timeout(&mut tcp_stream, &response, max_message_size, client_has_dict, dict_codec, message_version, ctx.config.slow_client_timeout).await.is_err() {
                    break;
                }
            }
            WatchStep::Frame(Ok(WatchOutputType::Done)) => break,
            WatchStep::Frame(Err(error)) => {
                let response = watch_read_error_to_client_response(correlation_id, error);
                let _ = write_client_response_with_timeout(&mut tcp_stream, &response, max_message_size, client_has_dict, dict_codec, message_version, ctx.config.slow_client_timeout).await;
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
    max_watch_subscribers: usize,
) -> Result<WatchSession<ShardWal<R, D>>, WatchSessionError> {
    if let Some(latency_ms) = request.requested_latency_ms
        && Duration::from_millis(latency_ms) > max_requested_latency
    {
        return Err(WatchSessionError::WatchLatencyTooHigh {
            latency_ms,
            max_latency_ms: max_requested_latency.as_millis() as u64,
        });
    }

    // Bound resident memory: refuse new subscriptions past the per-shard cap.
    let (watcher_id, subscribed_client) = match shard_wal
        .watched_aggregates
        .add_subscriber_capped(request, max_watch_subscribers)
    {
        Some(pair) => pair,
        None => {
            return Err(WatchSessionError::TooManySubscribers {
                active: shard_wal.watched_aggregates.subscriber_count(),
                max: max_watch_subscribers,
            });
        }
    };
    Ok(WatchSession::new(watcher_id, subscribed_client, shard_wal.clone()))
}

/// A promoting node's reaction to a heartbeat: refuse adoption at epoch <= its
/// own (only a deposed leader heartbeats at those epochs — adopting would
/// re-open the replication gate mid-promotion); a higher epoch is a lost race
/// and steps down to follow the winner.
fn promoting_heartbeat_adoption(promoting_lease_epoch: u64, req_lease_epoch: u64) -> Option<NodeStatus> {
    (req_lease_epoch > promoting_lease_epoch).then_some(NodeStatus::Follower { leader_lease_epoch: req_lease_epoch })
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_shard::shard_wal::TailReconciliation;
    use celeriant_msg::request::{
        read_filters::ReadFilters,
        requests::{
            DeleteRequest, AggregateDetailsRequest, ListAggregateTypesRequest,
            ListAggregatesRequest, ListOrgsRequest, ReadRequest, ReplicationBatchRequest,
            SingleAggregateDelete, SingleAggregateWrite, TrimStartRequest, WriteRequest,
        },
    };
    use celeriant_shard::timestamp_config::TimestampPrecision;
    use celeriant_wal::aggregate_key::AggregateKey;
    use std::collections::HashMap;

    /// A promoting node refuses heartbeat adoption at epoch <= its own (zombie
    /// leader must not re-open the replication gate mid-window) and steps down
    /// only for a genuinely newer election win.
    #[test]
    fn promoting_heartbeat_adoption_by_epoch() {
        // (promoting epoch, heartbeat epoch, expected adoption)
        let cases: &[(u64, u64, Option<NodeStatus>)] = &[
            (5, 4, None),
            (5, 5, None),
            (5, 6, Some(NodeStatus::Follower { leader_lease_epoch: 6 })),
        ];
        for &(own, req, expected) in cases {
            assert_eq!(promoting_heartbeat_adoption(own, req), expected, "own={own} req={req}");
        }
    }

    /// A heartbeat demotion may rewind to the ack barrier ONLY when the demoted
    /// node actually held leadership — the same rule promotion_cull_flags encodes
    /// via the shared derivation. Every other pre-demotion state maps to
    /// provenance-checked reconciliation (a peer tail must survive).
    #[test]
    fn heartbeat_demotion_mode_by_pre_demotion_state() {
        use celeriant_distributed::validated_node_status::{unix_epoch_now_ms, ValidatedNodeStatus};
        let expired = unix_epoch_now_ms().saturating_sub(10_000);
        // (name, pre-demotion status, expected mode)
        let cases: &[(&str, ValidatedNodeStatus, TailReconciliation)] = &[
            (
                "ttl_expired_leader",
                ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, expired),
                TailReconciliation::RewindToAckBarrier,
            ),
            (
                "explicitly_fenced_ex_leader",
                ValidatedNodeStatus::create_fenced(true),
                TailReconciliation::RewindToAckBarrier,
            ),
            (
                "explicitly_fenced_ex_follower",
                ValidatedNodeStatus::create_fenced(false),
                TailReconciliation::ReconcileAsFollower,
            ),
            (
                "ttl_expired_follower",
                ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 1 }, 500, expired),
                TailReconciliation::ReconcileAsFollower,
            ),
            (
                "booting_node",
                ValidatedNodeStatus::create_boot_catchup(),
                TailReconciliation::ReconcileAsFollower,
            ),
        ];
        for (name, status, expected) in cases {
            assert_eq!(
                crate::sharded::shard::demotion_mode(status.held_leadership()),
                *expected,
                "{name}",
            );
        }
    }

    fn test_config(num_shards: u32, routing_rule: crate::RoutingRule) -> ShardConfig {
        ShardConfig {
            node_id: 1,
            num_shards,
            replication_config: None,
            heartbeat_lease_duration: Duration::from_millis(1500),
            heartbeat_interval_duration: Duration::from_millis(250),
            heartbeat_timeout: Duration::from_millis(500),
            heartbeat_hard_timeout_multiplier: 4,
            s3_max_concurrent_fallback_uploads: 2,
            s3_retry_max_duration: None,
            advertised_replication_address: None,
            data_root: "/tmp".into(),
            listen_address: "127.0.0.1".into(),
            client_port: 8080,
            replication_port: 8081,
            max_open_files: 100,
            read_max_chunk_size: 1024,
            chain_read_window_bytes: 1024,
            write_max_chunk_size: 1024,
            max_request_size: 1024,
            internode_max_request_size: 64 * 1024 * 1024,
            max_response_size: 1024,
            slow_client_timeout: Duration::from_secs(30),
            max_requested_latency: Duration::from_millis(100),
            max_watch_subscribers: 10_000,
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
            read_max_concurrent: 64,
            schema_cache_bytes: 4_194_304, // 4MB
            max_schema_size_bytes: 16384,
            replication_delay: Duration::from_millis(20),
            max_clock_drift_ms: 5000,
            max_catchup_gap_bytes: Some(104_857_600),
            max_promotion_batch_bytes: None,
            internode_connection_timeout: None,
            internode_request_timeout: Duration::from_secs(10),
            tls_config: None,
            tls_cert_paths: None,
            tls_client_auth: celeriant_crypto::pki::ClientAuthMode::None,
            tls_cert_reload_interval: Duration::ZERO,
            require_client_identity: false,
            api_key_hashes: std::cell::RefCell::new(None),
            compaction_check_interval: Duration::from_secs(600),
            compaction_min_reclaimable_ratio: 0.20,
            compaction_temp_dir: None,
            cache_warmup_max_duration: None,
            reserve_coordinator_shard: false,
            s3_replication_delay: Duration::from_millis(500),
            replication_rollback_cooldown: Duration::from_millis(500),
            heartbeat_starve_threshold: Duration::ZERO,
            dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
            dict_sha256: std::sync::Arc::from("test-sha256"),
            wal_compression_level: 3,
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
            leader_confirmed_wal_seq: 0,
            sender_lease_epoch: 0,
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
            leader_confirmed_wal_seq: 0,
            sender_lease_epoch: 0,
            batches: vec![],
        });
        let result = determine_cluster_shard(&request, &config);
        assert!(matches!(result, Err(ShardRoutingError::IncompatibleFilters { .. })));
    }

    /// Heartbeat and KickFollower always route to shard 0
    #[test]
    fn routing_schema_registration_always_shard_zero() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClientRequest::RegisterSchema(celeriant_msg::request::requests::RegisterSchemaRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            schema_key: celeriant_wal::schema_key::SchemaKey::new(1, 1, 100, 0),
            schema_type: 0,
            schema: String::new(),
        });
        let shard = determine_client_shard(&request, &config).unwrap();
        assert_eq!(shard, 0, "Schema registration must always route to shard 0");
    }

    #[test]
    fn routing_heartbeat_always_shard_zero() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClusterRequest::Heartbeat(HeartbeatRequest {
            correlation_id: None,
            shard_id: 0,
            leader_timestamp_ms: 1000,
            lease_epoch: 1,
        });
        let shard = determine_cluster_shard(&request, &config).unwrap();
        assert_eq!(shard, 0, "Heartbeat must always route to shard 0");
    }

    // Regression: a kick must NOT zero the TTL. Before the fix, handle_kick_follower called
    // create_custom_status(..., 0, 0), leaving lease_expires_at_ms = 0. decide_post_catchup_action
    // then returned ChallengeViaCAS immediately even though the leader was live, causing split-brain.
    #[test]
    fn kick_must_set_alive_ttl_to_prevent_split_brain() {
        let now_ms = validated_node_status::unix_epoch_now_ms();
        let heartbeat_lease_duration_ms = 1_500u64;
        let max_clock_drift_ms = 500u64;

        // Simulate the FIXED kick handler: extend TTL from current expiry (or 0 if none) by
        // heartbeat_lease_duration.
        let current_expiry_ms = 0u64; // follower had no prior TTL
        let new_expiry_ms = compute_new_ttl(current_expiry_ms, now_ms, heartbeat_lease_duration_ms);
        let catching_up = ValidatedNodeStatus::create_custom_status(
            NodeStatus::FollowerCatchingUp { leader_lease_epoch: 7 },
            max_clock_drift_ms,
            new_expiry_ms,
        );

        assert!(
            catching_up.lease_expires_at_ms() > now_ms,
            "kick must produce an alive TTL (got {}; now {})", catching_up.lease_expires_at_ms(), now_ms
        );

        // With the alive TTL, decide_post_catchup_action must defer (StayFollower), not challenge.
        let action = celeriant_distributed::node_status_logic::decide_post_catchup_action(
            catching_up.raw(),
            catching_up.lease_expires_at_ms(),
            now_ms,
            heartbeat_lease_duration_ms,
        );
        assert!(
            matches!(action, celeriant_distributed::node_status_logic::PostCatchupAction::StayFollower { .. }),
            "kicked node with alive TTL must stay follower, got {action:?}"
        );
    }

    #[test]
    fn routing_kick_follower_always_shard_zero() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = ClusterRequest::KickFollower(KickFollowerRequest {
            correlation_id: None,
        });
        let shard = determine_cluster_shard(&request, &config).unwrap();
        assert_eq!(shard, 0, "KickFollower must always route to shard 0");
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
            expected_version: None,
            allow_create: true,
            enforce_client_idempotency: false,
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
                expected_version: None,
                allow_create: true,
                enforce_client_idempotency: false,
                events: vec![],
            },
        );
        writes.insert(
            AggregateKey::new(1, 2, 8),
            SingleAggregateWrite {
                expected_version: None,
                allow_create: true,
                enforce_client_idempotency: false,
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
                expected_version: None,
                allow_create: true,
                enforce_client_idempotency: false,
                events: vec![],
            },
        );
        writes.insert(
            AggregateKey::new(1, 2, 5),
            SingleAggregateWrite {
                expected_version: None,
                allow_create: true,
                enforce_client_idempotency: false,
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
        assert!(matches!(result, Err(ShardRoutingError::MultipleShardRoutes { .. })));
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
            allow_sequence_continuation: false,
            expected_version: None,
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
            keep_from_aggregate_version: 10,
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
            expected_version: None,
            allow_create: true,
            enforce_client_idempotency: false,
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
            expected_version: None,
            allow_create: true,
            enforce_client_idempotency: false,
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
            expected_version: None,
            allow_create: true,
            enforce_client_idempotency: false,
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
            assert_eq!(err.error_code, error_codes::IDENTIFY_MISMATCH);
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
            keep_from_aggregate_version: 10,
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
            allow_sequence_continuation: false,
            expected_version: None,
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

    // --- build_identify_dict_fields ---

    fn base_identify_request() -> celeriant_msg::request::requests::IdentifyRequest {
        celeriant_msg::request::requests::IdentifyRequest {
            correlation_id: None,
            public_key: None,
            nonce: None,
            signature: None,
            api_key: None,
            known_dict_sha256: None,
        }
    }

    fn config_with_dict(sha: &str, bytes: &[u8]) -> ShardConfig {
        let mut cfg = test_config(1, crate::RoutingRule::AggregateId);
        cfg.dict_sha256 = std::sync::Arc::from(sha);
        cfg.dict_bytes = std::sync::Arc::from(bytes);
        cfg
    }

    #[test]
    fn identify_dict_fields_ships_bytes_when_client_has_no_dict() {
        let sha = "aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111";
        let bytes = vec![0xFFu8; 100];
        let cfg = config_with_dict(sha, &bytes);
        let req = base_identify_request(); // known_dict_sha256 = None
        let (out_sha, out_bytes) = super::build_identify_dict_fields(&req.known_dict_sha256, &cfg);
        assert_eq!(out_sha.as_deref(), Some(sha));
        assert_eq!(out_bytes.as_deref(), Some(bytes.as_slice()));
    }

    #[test]
    fn identify_dict_fields_omits_bytes_when_client_sha_matches() {
        let sha = "bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222";
        let bytes = vec![0xEEu8; 100];
        let cfg = config_with_dict(sha, &bytes);
        let mut req = base_identify_request();
        req.known_dict_sha256 = Some(sha.to_string());
        let (out_sha, out_bytes) = super::build_identify_dict_fields(&req.known_dict_sha256, &cfg);
        assert_eq!(out_sha.as_deref(), Some(sha));
        assert_eq!(out_bytes, None); // bytes skipped: client already has this dict
    }

    #[test]
    fn identify_dict_fields_ships_new_bytes_when_client_sha_mismatches() {
        let sha = "cccc3333cccc3333cccc3333cccc3333cccc3333cccc3333cccc3333cccc3333";
        let bytes = vec![0xDDu8; 100];
        let cfg = config_with_dict(sha, &bytes);
        let mut req = base_identify_request();
        req.known_dict_sha256 = Some("stale0000stale0000stale0000stale0000stale0000stale0000stale00000".to_string());
        let (out_sha, out_bytes) = super::build_identify_dict_fields(&req.known_dict_sha256, &cfg);
        assert_eq!(out_sha.as_deref(), Some(sha));
        assert_eq!(out_bytes.as_deref(), Some(bytes.as_slice())); // bytes shipped: sha mismatch
    }

    #[test]
    fn handle_identify_sets_client_has_dict_true_when_cluster_has_dict() {
        let sha = "dddd4444dddd4444dddd4444dddd4444dddd4444dddd4444dddd4444dddd4444";
        let cfg = config_with_dict(sha, &[1, 2, 3]);
        let req = base_identify_request();
        let mut conn_state = ConnectionState {
            verified_client_id: None,
            access_level: None,
            client_has_dict: false,
        };
        let result = super::handle_identify(&req, &mut conn_state, &cfg);
        assert!(result.is_ok());
        assert!(conn_state.client_has_dict);
    }

}
