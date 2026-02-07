use std::{cell::Cell, fmt, rc::Rc, time::Duration};

use celeriant_msg::{
    process_requests::Request,
    process_responses::Response,
    request::requests::WatchRequest,
    response::responses::{ErrorResponse, WatchResponse},
};
use celeriant_shard::{
    error::watch_session_error::WatchSessionError,
    replication_client::ReplicationClient,
    shard_wal::ShardWal,
};
use celeriant_wal::compression_type::CompressionType;
use celeriant_watch::{watch_output_type::WatchOutputType, watch_session::WatchSession};
use celeriant_msg::read_wire_data_error::ReadWireDataError;
use celeriant_wire::network::wire_error::WireError;
use glommio::{channels::channel_mesh::Senders, net::TcpStream};
use tracing::warn;

use super::{
    intrashard_messages::IntrashardMessages,
    shard_config::ShardConfig,
    shard_error_response::{shard_error_to_response, watch_read_error_to_response, watch_session_error_to_response},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortType {
    Client,
    Replication,
}

pub struct ConnectionContext<R: ReplicationClient + 'static> {
    pub config: Rc<ShardConfig>,
    pub current_shard_id: usize,
    pub intrashard_sender: Rc<Senders<IntrashardMessages>>,
    pub shutdown_requested: Rc<Cell<bool>>,
    pub shard_wal: Rc<ShardWal<R>>,
}

impl<R: ReplicationClient + 'static> Clone for ConnectionContext<R> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            current_shard_id: self.current_shard_id,
            intrashard_sender: self.intrashard_sender.clone(),
            shutdown_requested: self.shutdown_requested.clone(),
            shard_wal: self.shard_wal.clone(),
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

pub fn handle_new_connection<R: ReplicationClient + 'static>(mut tcp_stream: TcpStream, ctx: ConnectionContext<R>, port_type: PortType) {
    let _ = tcp_stream.set_nodelay(true);

    glommio::spawn_local(async move {
        if ctx.shutdown_requested.get() {
            return;
        }

        let (request, message_version) = match read_request(&mut tcp_stream, &ctx).await {
            Some(r) => r,
            None => return,
        };

        match check_redirect(tcp_stream, request, ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, &ctx, port_type).await {
            RedirectResult::ProcessLocally(request, tcp_stream) => {
                handle_pipelining(tcp_stream, request, ctx.config.max_request_size, ctx.config.max_response_size, ctx.config.server_compression_algorithm, message_version, ctx, port_type).await;
            }
            RedirectResult::Redirected => {}
            RedirectResult::ErrorSentContinue(_) => {}
        }
    })
    .detach();
}

pub fn handle_redirected_connection<R: ReplicationClient + 'static>(
    tcp_stream: TcpStream,
    request: Request,
    max_request_size: u64,
    max_response_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
    ctx: ConnectionContext<R>,
    port_type: PortType,
) {
    let _ = tcp_stream.set_nodelay(true);

    glommio::spawn_local(async move {
        handle_pipelining(tcp_stream, request, max_request_size, max_response_size, server_compression_algorithm, message_version, ctx, port_type).await;
    })
    .detach();
}

async fn handle_pipelining<R: ReplicationClient + 'static>(
    mut tcp_stream: TcpStream,
    request: Request,
    max_request_size: u64,
    max_response_size: u64,
    server_compression_algorithm: CompressionType,
    mut message_version: u32,
    ctx: ConnectionContext<R>,
    port_type: PortType,
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
            match check_redirect(tcp_stream, request, max_response_size, server_compression_algorithm, message_version, &ctx, port_type).await {
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

async fn check_redirect<R: ReplicationClient + 'static>(
    mut tcp_stream: TcpStream,
    request: Request,
    max_message_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
    ctx: &ConnectionContext<R>,
    port_type: PortType,
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

async fn read_request<R: ReplicationClient + 'static>(
    tcp_stream: &mut TcpStream,
    ctx: &ConnectionContext<R>,
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
            warn!("Failed to read request: {e:?}");
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

async fn process_request<R: ReplicationClient + 'static>(
    tcp_stream: &mut TcpStream,
    ctx: &ConnectionContext<R>,
    request: Request,
    max_message_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
) {
    let correlation_id = request.correlation_id();
    let response = match ctx.shard_wal.process_request(Some(0), request).await {
        Ok(result) => result,
        Err(error) => shard_error_to_response(correlation_id, error),
    };
    let _ = write_response(tcp_stream, &response, max_message_size, server_compression_algorithm, message_version, ctx.config.slow_client_timeout).await;
}

async fn handle_watch<R: ReplicationClient + 'static>(
    mut tcp_stream: TcpStream,
    watch_request: WatchRequest,
    max_message_size: u64,
    server_compression_algorithm: CompressionType,
    message_version: u32,
    ctx: &ConnectionContext<R>,
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

fn create_watch_session<R: ReplicationClient + 'static>(
    shard_wal: &Rc<ShardWal<R>>,
    request: WatchRequest,
    max_requested_latency: Duration,
) -> Result<WatchSession<ShardWal<R>>, WatchSessionError> {
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
            CatchUpRequest, DeleteRequest, ExistsRequest, ListAggregateTypesRequest,
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
            node_status: celeriant_distributed::node_status::NodeStatus::Standalone,
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
            non_durable_writes: false,
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
            heartbeat_interval_ms: 500,
            heartbeat_lease_duration_ms: 1500,
            max_clock_drift_ms: 500,
            bootstrap_as_leader: false,
        }
    }

    #[test]
    fn port_validation_client_requests() {
        let exists = Request::Exists(ExistsRequest {
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
            follower_too_far_behind: false,
        });
        assert!(!is_valid_for_port(&repl, PortType::Client));
        assert!(is_valid_for_port(&repl, PortType::Replication));
    }

    #[test]
    fn routing_exists_request_by_aggregate_id() {
        let config = test_config(4, crate::RoutingRule::AggregateId);
        let request = Request::Exists(ExistsRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(100, 200, 7),
        });
        let shard = determine_shard(&request, &config, PortType::Client).unwrap();
        assert_eq!(shard, 3); // 7 % 4 = 3
    }

    #[test]
    fn routing_exists_request_by_org_id() {
        let config = test_config(4, crate::RoutingRule::OrgId);
        let request = Request::Exists(ExistsRequest {
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
            follower_too_far_behind: false,
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
            follower_too_far_behind: false,
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
}
