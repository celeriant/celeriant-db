use std::{cell::Cell, rc::Rc, time::Duration};

use celeriant_watch::{watch_output_type::WatchOutputType, watch_session::WatchSession};
use celeriant_filesystem::{read_write_error::ReadWriteError, shard_log_read_error::ShardLogReadError, shard_log_write_error::ShardLogWriteError, shard_write_ahead_log::ShardWriteAheadLog};
use celeriant_msg::{
    request::{requests::WatchRequest},
    response::responses::{ErrorResponse, WatchResponse},
};
use celeriant_wire::wire_error::WireError;
use glommio::{
    channels::{
        channel_mesh::{Receivers, Senders},
        shared_channel::ConnectedReceiver,
    },
    net::{TcpListener, TcpStream},
};
use tracing::{error, info, warn};

use crate::{
    sharded::{
        intrashard_messages::IntrashardMessages, shard_config::ShardConfig,
        signal_handler::SignalHandler,
    },
    sidecar::sidecar_channels::SidecarSenders,
};

pub struct Shard {
    intrashard_receivers: Receivers<IntrashardMessages>,
    tcp_listener: Rc<TcpListener>,
    shard_data: ShardData,
}

impl Shard {
    pub fn new(
        config: ShardConfig,
        current_shard_id: usize,
        sender: Senders<IntrashardMessages>,
        receivers: Receivers<IntrashardMessages>,
        sidecar_senders: SidecarSenders,
        tcp_listener: TcpListener,
        filesystem: ShardWriteAheadLog,
    ) -> Self {
        info!("Initializing shard {current_shard_id}");

        let shard_data = ShardData {
            config: Rc::new(config),
            current_shard_id,
            intrashard_sender: Rc::new(sender),
            _sidecar_senders: Rc::new(sidecar_senders),
            shutdown_requested: Rc::new(Cell::new(false)),
            filesystem: Rc::new(filesystem),
        };

        Self {
            intrashard_receivers: receivers,
            tcp_listener: Rc::new(tcp_listener),
            shard_data,
        }
    }

    pub async fn run(&mut self) {
        // On initial startup of our shard, we need background tasks to handle shutdown polling (shard 0 only) and intrashard messaging
        spawn_shard_zero_shutdown_handler(self.shard_data.clone());

        for (_src_shard, stream) in self.intrashard_receivers.streams() {
            spawn_intrashard_message_handler(stream, self.shard_data.clone());
        }

        self.enter_main_loop_until_shutdown().await;

        info!(
            "Shard {} shutdown complete",
            self.shard_data.current_shard_id
        );
    }

    async fn enter_main_loop_until_shutdown(&self) {
        loop {
            if self.shard_data.shutdown_requested.get() {
                let _ = self.shard_data.filesystem.close().await;
                break;
            }

            // Don't hang the main loop forever otherwise we won't be able to shutdown gracefully
            match glommio::timer::timeout(Duration::from_secs(1), self.tcp_listener.shared_accept())
                .await
            {
                Ok(accepted_tcp_stream) => {
                    handle_new_client_connection(
                        accepted_tcp_stream.bind_to_executor(),
                        self.shard_data.clone(),
                    );
                }
                Err(_) => {}
            }
        }
    }
}

#[derive(Clone)]
struct ShardData {
    config: Rc<ShardConfig>,
    current_shard_id: usize,
    intrashard_sender: Rc<Senders<IntrashardMessages>>,
    _sidecar_senders: Rc<SidecarSenders>,
    shutdown_requested: Rc<Cell<bool>>,
    filesystem: Rc<ShardWriteAheadLog>,
}


async fn check_for_shard_redirect(
    mut tcp_stream: TcpStream,
    request: celeriant_msg::process_requests::Request,
    message_version: u32,
    shard_data: &ShardData,
) -> Option<(celeriant_msg::process_requests::Request, TcpStream)> {

    let idx = match determine_shard_route(&request, Rc::clone(&shard_data.config)) {
        Ok(shard_idx) => shard_idx,
        Err(e) => {
            let response = celeriant_msg::process_responses::Response::GenericError(
                celeriant_msg::response::responses::ErrorResponse {
                    correlation_id: request.correlation_id(),
                    error_code: 400,
                    error_message: format!("Shard routing error: {:?}", e),
                }
            );
            let _ = write_response_with_timeout(
                &mut tcp_stream,
                &response,
                celeriant_wal::compression_type::CompressionType::None,
                message_version,
                shard_data.config.slow_client_timeout,
            ).await;
            return None;
        }
    };

    if idx != shard_data.current_shard_id {
        let msg = IntrashardMessages::ConnectionRedirect {
            accepted_tcp_stream: tcp_stream.into_accepted(),
            request,
            message_version,
        };
        if let Err(e) = shard_data.intrashard_sender.send_to(idx, msg).await {
            warn!("Failed to redirect connection to shard {idx}: {e:?}");
        }
        return None;
    }

    Some((request, tcp_stream))
}

#[derive(Debug)]
pub enum ShardRoutingError {
    NoRoutingKeyProvided,
    MultipleShardRoutes,
    IncompatibleFilters(String),
}

fn determine_shard_route(
    request: &celeriant_msg::process_requests::Request,
    config: Rc<ShardConfig>,
) -> Result<usize, ShardRoutingError> {
    let num_shards = config.num_shards as u128;
    
    match request {
        celeriant_msg::process_requests::Request::Watch(watch_request) => {
            determine_shard_route_watch_request(watch_request, config)
        },
        the_rest => {
            let shard_id = match config.routing_rule {
                crate::RoutingRule::OrgId => the_rest.org_id() % num_shards,
                crate::RoutingRule::AggregateTypeId => the_rest.aggregate_type_id() % num_shards,
                crate::RoutingRule::AggregateId => the_rest.aggregate_id() % num_shards,
            };
            Ok(shard_id as usize)
        },
    }
}

fn determine_shard_route_watch_request(
    watch_request: &WatchRequest,
    config: Rc<ShardConfig>,
) -> Result<usize, ShardRoutingError> {
    let num_shards = config.num_shards as u128;
    
    match config.routing_rule {
        crate::RoutingRule::OrgId => {
            collect_unique_shard_id(num_shards, [
                watch_request.orgs.as_ref().map(|orgs| 
                    orgs.iter().map(|id| *id).collect::<Vec<_>>()
                ),
                watch_request.aggregate_types.as_ref().map(|types| 
                    types.iter().map(|k| k.org_id).collect()
                ),
                watch_request.aggregates.as_ref().map(|aggs| 
                    aggs.iter().map(|k| k.org_id).collect()
                ),
            ])
        },
        crate::RoutingRule::AggregateTypeId => {
            // Cannot route by aggregate_type_id if only orgs are specified
            if watch_request.orgs.is_some() 
                && watch_request.aggregate_types.is_none() 
                && watch_request.aggregates.is_none() 
            {
                return Err(ShardRoutingError::IncompatibleFilters(
                    "Cannot route by aggregate_type_id when only orgs are specified".into()
                ));
            }
            
            collect_unique_shard_id(num_shards, [
                None, // orgs don't have aggregate_type_id for routing
                watch_request.aggregate_types.as_ref().map(|types| 
                    types.iter().map(|k| k.aggregate_type_id).collect()
                ),
                watch_request.aggregates.as_ref().map(|aggs| 
                    aggs.iter().map(|k| k.aggregate_type_id).collect()
                ),
            ])
        },
        crate::RoutingRule::AggregateId => {
            // Cannot route by aggregate_id if aggregates are not specified
            if (watch_request.orgs.is_some() || watch_request.aggregate_types.is_some())
                && watch_request.aggregates.is_none()
            {
                return Err(ShardRoutingError::IncompatibleFilters(
                    "Cannot route by aggregate_id when aggregates are not specified".into()
                ));
            }
            
            collect_unique_shard_id(num_shards, [
                None,
                None,
                watch_request.aggregates.as_ref().map(|aggs| 
                    aggs.iter().map(|k| k.aggregate_id).collect()
                ),
            ])
        },
    }
}

fn collect_unique_shard_id(
    num_shards: u128,
    sources: [Option<Vec<u128>>; 3],
) -> Result<usize, ShardRoutingError> {
    let mut shard_id: Option<usize> = None;
    
    for source in sources.into_iter().flatten() {
        for routing_key in source {
            let computed_shard = (routing_key % num_shards) as usize;
            match shard_id {
                None => shard_id = Some(computed_shard),
                Some(existing) if existing != computed_shard => {
                    return Err(ShardRoutingError::MultipleShardRoutes);
                }
                Some(_) => {}
            }
        }
    }
    
    shard_id.ok_or(ShardRoutingError::NoRoutingKeyProvided)
}

fn read_write_error_to_response(
    correlation_id: Option<u128>,
    error: ReadWriteError,
) -> celeriant_msg::process_responses::Response {
    let (error_code, error_message) = match error {
        ReadWriteError::Read(read_error) => match read_error {
            ShardLogReadError::WatchLatencyTooHigh { latency_ms, max_latency_ms: max_latency_mx } => (
                400,
                format!(
                    "Requested watch latency {}ms exceeds server max of {}ms",
                    latency_ms, max_latency_mx
                ),
            ),
            ShardLogReadError::NotExists => (404, "Aggregate does not exist".to_string()),
            ShardLogReadError::IoError(msg) => (500, format!("IO error: {}", msg)),
            ShardLogReadError::CannotCreateFolders(msg) => (500, format!("Cannot create folders: {}", msg)),
            ShardLogReadError::MaxBytesTooSmall {
                current_max_bytes,
                required_max_bytes,
            } => (
                400,
                format!(
                    "Max bytes too small: current={}, required={}",
                    current_max_bytes, required_max_bytes
                ),
            ),
            ShardLogReadError::SerializationError(wire_error) => {
                (500, format!("Serialization error: {:?}", wire_error))
            }
            ShardLogReadError::UnavailableBatchIndex {
                minimum_available_event_batch_index,
                requested_event_batch_index,
            } => (
                410,
                format!(
                    "Batch index unavailable: requested={}, minimum available={}",
                    requested_event_batch_index, minimum_available_event_batch_index
                ),
            ),
            ShardLogReadError::CorruptMetadata { file_pos_metadata } => (
                500,
                format!("Corrupt metadata at position {}", file_pos_metadata),
            ),
            ShardLogReadError::CorruptEventBatch {
                expected_crc,
                actual_crc,
                event_batch_index,
                file_pos_event_batch,
            } => (
                500,
                format!(
                    "Corrupt event batch: index={}, expected_crc={}, actual_crc={}, batch_pos={}",
                    event_batch_index,
                    expected_crc,
                    actual_crc,
                    file_pos_event_batch
                ),
            ),
        },
        ReadWriteError::Write(write_error) => match write_error {
            ShardLogWriteError::DmaFileNotInitialized => (500, "DMA file not initialized".to_string()),
            ShardLogWriteError::IoError(msg) => (500, format!("IO error: {}", msg)),
            ShardLogWriteError::SerializationError(wire_error) => {
                (500, format!("Serialization error: {:?}", wire_error))
            }
            ShardLogWriteError::OptimisticConcurrencyViolation {
                expected_event_batch_index,
                current_event_batch_index,
            } => (
                409,
                format!(
                    "Optimistic concurrency violation: expected={}, current={}",
                    expected_event_batch_index, current_event_batch_index
                ),
            ),
            ShardLogWriteError::ClientIdempotencyViolation {
                client_id,
                last_client_event_index,
                attempted_client_event_index,
            } => (
                409,
                format!(
                    "Client idempotency violation: client_id={}, last={}, attempted={}",
                    client_id, last_client_event_index, attempted_client_event_index
                ),
            ),
            ShardLogWriteError::EmptyEventsList => (400, "Empty events list".to_string()),
            ShardLogWriteError::NoEventsToAppend {
                client_id,
                existing_event_index,
            } => (
                400,
                format!(
                    "No events to append: client_id={}, existing_index={}",
                    client_id, existing_event_index
                ),
            ),
            ShardLogWriteError::ZeroEventType { client_event_index } => (
                400,
                format!("Zero event type at index {}", client_event_index),
            ),
            ShardLogWriteError::CacheMiss {
                missing_from_event_batch_index,
                missing_to_event_batch_index,
            } => (
                503,
                format!(
                    "Cache miss: from={}, to={:?}",
                    missing_from_event_batch_index, missing_to_event_batch_index
                ),
            ),
            ShardLogWriteError::PrependCreatesEventBatchIndexGap {
                provided_last_batch_index,
                current_first_event_batch_index,
            } => (
                400,
                format!(
                    "Prepend creates gap: provided_last={}, current_first={}",
                    provided_last_batch_index, current_first_event_batch_index
                ),
            ),
            ShardLogWriteError::PrependNonContiguousBatches {
                from_event_batch_index,
                to_event_batch_index,
            } => (
                400,
                format!(
                    "Prepend non-contiguous batches: from={}, to={}",
                    from_event_batch_index, to_event_batch_index
                ),
            ),
            ShardLogWriteError::FileRenameFailure { from, to } => (
                500,
                format!("File rename failure: from='{}', to='{}'", from, to),
            ),
            ShardLogWriteError::MaxBytesTooSmall {
                current_max_bytes,
                required_max_bytes,
            } => (
                400,
                format!(
                    "Max bytes too small: current={}, required={}",
                    current_max_bytes, required_max_bytes
                ),
            ),
            ShardLogWriteError::InvalidLeaseIndex => (400, "Invalid lease index".to_string()),
            ShardLogWriteError::DatablocksCarryOverBufferNotPresent => (500, "Datablocks carry over buffer not present".to_string()),
        },
    };

    celeriant_msg::process_responses::Response::GenericError(ErrorResponse {
        correlation_id,
        error_code,
        error_message,
    })
}

async fn process_client_request(
    tcp_stream: &mut TcpStream,
    shard_write_ahead_log: Rc<ShardWriteAheadLog>,
    request: celeriant_msg::process_requests::Request,
    message_version: u32,
    slow_client_timeout: Duration,
) {
    let correlation_id = request.correlation_id();

    let response = match shard_write_ahead_log
        .as_ref()
        .process_request(Some(0), request)
        .await
    {
        Ok(result) => result,
        Err(error) => read_write_error_to_response(correlation_id, error),
    };

    let compression_type =
        celeriant_msg::process_responses::Response::determine_compression_type(&response);

    let _ = write_response_with_timeout(
        tcp_stream,
        &response,
        compression_type,
        message_version,
        slow_client_timeout,
    )
    .await;
}

/// Try to read a request from the client.
/// Read can fail due to disconnect, wrong protocol version, timeout, or other errors
/// read_request may write a message back, otherwise we just close the connection
async fn read_client_request(
    tcp_stream: &mut TcpStream,
    shard_data: &ShardData,
    timeout_duration: Duration,
) -> Option<(celeriant_msg::process_requests::Request, u32)> {
    match glommio::timer::timeout(timeout_duration, async {
        let result = celeriant_msg::process_requests::Request::read_request(
            tcp_stream,
            shard_data.config.max_request_size,
        )
        .await;
        Ok::<_, glommio::GlommioError<()>>(result)
    })
    .await
    {
        Ok(Ok(read_result)) => Some(read_result),
        Ok(Err(WireError::NetworkError(ref e)))
            if e.kind() == std::io::ErrorKind::UnexpectedEof =>
        {
            None
        }
        Ok(Err(WireError::UnsupportedProtocol(_version))) => None,
        Ok(Err(_e)) => None,
        Err(_) => None, // Timeout
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

fn spawn_shard_zero_shutdown_handler(shard_data: ShardData) {
    if shard_data.current_shard_id != 0 {
        return;
    }

    let mut signal_handler = SignalHandler::new().expect("Failed to initialize signal handler");

    glommio::spawn_local(async move {
        loop {
            match signal_handler.poll_signal() {
                Ok(Some(sig)) => {
                    info!(
                        "Received shutdown signal ({:?}). Initiating graceful shutdown...",
                        sig
                    );
                    shard_data.shutdown_requested.set(true);
                    broadcast_message_to_other_shards(
                        shard_data.current_shard_id,
                        IntrashardMessages::Shutdown,
                        shard_data.intrashard_sender,
                    )
                    .await;
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

fn spawn_intrashard_message_handler(
    stream: ConnectedReceiver<IntrashardMessages>,
    shard_data: ShardData,
) {
    glommio::spawn_local(async move {
        while let Some(msg) = stream.recv().await {
            handle_intrashard_message(msg, &shard_data);
        }
    })
    .detach();
}

fn handle_intrashard_message(msg: IntrashardMessages, shard_data: &ShardData) {
    match msg {
        IntrashardMessages::Shutdown => {
            // Simple flag for the shard which is read in the main connection loop and all active connection processors
            shard_data.shutdown_requested.set(true);
        }
        IntrashardMessages::ConnectionRedirect {
            accepted_tcp_stream,
            request,
            message_version,
        } => {
            // Another shard is passing a client connection
            // Spawn another task to handle it to avoid blocking future messages
            spawn_intrashard_message_connection_redirect_task(
                accepted_tcp_stream.bind_to_executor(),
                request,
                message_version,
                shard_data.clone(),
            );
        }
    }
}

fn spawn_intrashard_message_connection_redirect_task(
    tcp_stream: TcpStream,
    request: celeriant_msg::process_requests::Request,
    message_version: u32,
    shard_data: ShardData,
) {
    // Disable Nagle's algorithm if possible
    let _ = tcp_stream.set_nodelay(true);
    
    glommio::spawn_local(async move {
        handle_request_and_further_pipelining(tcp_stream, request, message_version, shard_data)
            .await;
    })
    .detach();
}

async fn create_watch_session(
    shard_write_ahead_log: Rc<ShardWriteAheadLog>,
    request: WatchRequest,
    max_requested_latency: Duration,
) -> Result<WatchSession<ShardWriteAheadLog>, ReadWriteError> {

    // Don't allow setup of a watch with a latency too high
    // as this creates too much load on the server
    // Clients should long-poll instead
    if let Some(latency_ms) = request.requested_latency_ms
        && Duration::from_millis(latency_ms) > max_requested_latency
    {
        return Err(ShardLogReadError::WatchLatencyTooHigh {
            latency_ms,
            max_latency_ms: max_requested_latency.as_millis() as u64,
        })?;
    }

    let (watcher_id, subscribed_client) = shard_write_ahead_log
        .watched_aggregates
        .add_subscriber(request);

    // Watcher needs a ref to local_aggregates so it can retrieve batches
    Ok(WatchSession::new(
        watcher_id,
        subscribed_client,
        shard_write_ahead_log.clone(),
    ))
}

async fn handle_watch_request(
    mut tcp_stream: TcpStream,
    watch_request: WatchRequest,
    message_version: u32,
    shard_write_ahead_log: Rc<ShardWriteAheadLog>,
    max_requested_latency: Duration,
    slow_client_timeout: Duration,
) {
    let correlation_id = watch_request.correlation_id;

    let mut watch_session = match create_watch_session(
        shard_write_ahead_log.clone(),
        watch_request,
        max_requested_latency,
    )
    .await
    {
        Ok(session) => session,
        Err(error) => {
            let response = read_write_error_to_response(correlation_id, error);
            let compression_type =
                celeriant_msg::process_responses::Response::determine_compression_type(&response);
            let _ = write_response_with_timeout(
                &mut tcp_stream,
                &response,
                compression_type,
                message_version,
                slow_client_timeout,
            )
            .await;
            return;
        }
    };

    loop {
        match watch_session.next().await {
            Ok(WatchOutputType::Continue) => continue,
            Ok(WatchOutputType::Response(watch_response)) => {
                let response = celeriant_msg::process_responses::Response::Watch(watch_response);
                let compression_type =
                    celeriant_msg::process_responses::Response::determine_compression_type(
                        &response,
                    );
                if write_response_with_timeout(
                    &mut tcp_stream,
                    &response,
                    compression_type,
                    message_version,
                    slow_client_timeout,
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            Ok(WatchOutputType::Heartbeat) => {
                let response = celeriant_msg::process_responses::Response::Watch(WatchResponse {
                    events: None,
                });
                if write_response_with_timeout(
                    &mut tcp_stream,
                    &response,
                    celeriant_wal::compression_type::CompressionType::None,
                    message_version,
                    slow_client_timeout,
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            Ok(WatchOutputType::Done) => break,
            Err(error) => {
                let write_error: ShardLogWriteError = error.into();
                let response = read_write_error_to_response(correlation_id, write_error.into());
                let compression_type =
                    celeriant_msg::process_responses::Response::determine_compression_type(
                        &response,
                    );
                let _ = write_response_with_timeout(
                    &mut tcp_stream,
                    &response,
                    compression_type,
                    message_version,
                    slow_client_timeout,
                )
                .await;
                break;
            }
        }
    }
}

async fn handle_request_and_further_pipelining(
    mut tcp_stream: TcpStream,
    mut request: celeriant_msg::process_requests::Request,
    mut message_version: u32,
    shard_data: ShardData,
) {
    loop {
        if shard_data.shutdown_requested.get() {
            break;
        }

        // if request is Watch type process seprately
        if let celeriant_msg::process_requests::Request::Watch(watch_request) = request {
            handle_watch_request(
                tcp_stream,
                watch_request,
                message_version,
                shard_data.filesystem.clone(),
                shard_data.config.max_requested_latency,
                shard_data.config.slow_client_timeout,
            )
            .await;
            return;
        } else {
            process_client_request(
                &mut tcp_stream,
                shard_data.filesystem.clone(),
                request,
                message_version,
                shard_data.config.slow_client_timeout,
            )
            .await;
        }

        if shard_data.shutdown_requested.get() {
            break;
        }

        // Support pipelining of another request on the same connection
        match read_client_request(&mut tcp_stream, &shard_data, shard_data.config.slow_client_timeout).await {
            Some((next_request, next_message_version)) => {
                request = next_request;
                message_version = next_message_version;
            }
            None => return,
        }

        if shard_data.shutdown_requested.get() {
            break;
        }

        // The next request might need to be forwarded to another shard
        match check_for_shard_redirect(tcp_stream, request, message_version, &shard_data).await {
            Some((returned_request, returned_tcp_stream)) => {
                request = returned_request;
                tcp_stream = returned_tcp_stream;
            }
            None => return,
        }
    }
}

fn handle_new_client_connection(mut tcp_stream: TcpStream, shard_data: ShardData) {
    
    // Disable Nagle's algorithm if possible
    let _ = tcp_stream.set_nodelay(true);

    glommio::spawn_local(async move {
        if shard_data.shutdown_requested.get() {
            return;
        }

        let (mut request, message_version) =
            match read_client_request(&mut tcp_stream, &shard_data, shard_data.config.slow_client_timeout).await {
                Some(r) => r,
                None => return,
            };

        // The request might need to be forwarded to another shard
        match check_for_shard_redirect(tcp_stream, request, message_version, &shard_data).await {
            Some((returned_request, returned_tcp_stream)) => {
                request = returned_request;
                tcp_stream = returned_tcp_stream;
            }
            None => return,
        }

        handle_request_and_further_pipelining(tcp_stream, request, message_version, shard_data)
            .await;
    })
    .detach();
}

async fn write_response_with_timeout(
    tcp_stream: &mut TcpStream,
    response: &celeriant_msg::process_responses::Response,
    compression_type: celeriant_wal::compression_type::CompressionType,
    message_version: u32,
    timeout_duration: Duration,
) -> Result<(), WireError> {
    match glommio::timer::timeout(timeout_duration, async {
        let result = celeriant_msg::process_responses::Response::write_response(
            tcp_stream,
            response,
            compression_type,
            message_version,
        ).await;
        Ok::<_, glommio::GlommioError<()>>(result)
    }).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(wire_error)) => Err(wire_error),
        Err(_) => Err(WireError::NetworkError(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "Write timed out - slow client",
        ))),
    }
}