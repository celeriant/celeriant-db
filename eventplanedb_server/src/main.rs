use clap::Parser;
use std::{cell::{Cell, RefCell}, os::fd::{FromRawFd, IntoRawFd}, rc::Rc, time::Duration};

use eventplanedb_core::{node_config::NodeConfig, process_request::ProcessRequest, read_operations::read_structures::AggregateReadConfig, write_operations::write_structures::AggregateWriteConfig};
use eventplanedb_structures::{request::{Request, read_request}, response::{ProtocolErrorResponse, Response, write_response}, wire_error::WireError};
use glommio::{CpuSet, LocalExecutorPoolBuilder, PoolPlacement, channels::channel_mesh::{Full, MeshBuilder, Senders}, enclose, net::TcpListener, spawn_local};
use futures_lite::AsyncWriteExt;
use log::{debug, error, info};

mod config;
mod signal_handler;

use signal_handler::SignalHandler;

use mimalloc::MiMalloc;

use crate::{config::EventPlaneDBConfig};

//TODO: Compare with default allocator in benchmarks
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

struct Msg {
    fd: i32,
    value: Option<Request>,
    message_version: u32,
    require_shutdown: bool,
}

fn main() {
    // Parse CLI arguments
    let config = EventPlaneDBConfig::parse();
    
    // Initialize logger with configured level
    env_logger::Builder::from_default_env()
        .filter_level(config.default_log_level.parse().unwrap_or(log::LevelFilter::Info))
        .init();
    
    info!("Starting EventPlaneDB Server...");
    info!("Configuration: data_root={:?}, listen_address={}", config.data_root, config.listen_address);

    // Determine number of shards
    let nbr_shards = config.num_shards.unwrap_or_else(num_cpus::get);
    let online_cpus = CpuSet::online().ok();
    info!("Number of shards: {nbr_shards}, Online CPUs: {online_cpus:?}");

    let aggregate_read_config = AggregateReadConfig {
        max_chunk_size: config.aggregate_read_max_chunk_size,
    };

    let aggregate_write_config = AggregateWriteConfig {
        max_data_cache_size_bytes: config.aggregate_write_max_data_cache_size_bytes,
        cache_trim_factor: config.cache_trim_factor,
        max_chunk_size: config.aggregate_write_max_chunk_size,
    };

    // A full mesh channel is required to allow any shard to communicate with any other shard without locking primitives
    let mesh = MeshBuilder::<Msg, Full>::full(nbr_shards, config.mesh_channel_size);
    
    let node_config = NodeConfig {
        data_root_folder: config.data_root.to_string_lossy().to_string(),
        node_id: todo!(),
        margin_ms: todo!(),
        lease_expiry_ms: todo!(),
        async_flush_ms: config.async_flush_ms,
        max_open_aggregates: config.max_open_aggregates,
        max_request_size: config.max_request_size,
        listen_address: config.listen_address.clone(),
        max_event_batches_response_size: config.max_event_batches_response_size.map(|v| v as usize),
        s3_enabled: config.s3_enabled,
    };

    // Create a pool of executors, one per shard
    // Each executor will run threads pinned to a single core
    // The mesh BUILDER is copied in to each shard so they can all join the full mesh
    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(
        nbr_shards,
        online_cpus,
    ))
    .on_all_shards(enclose!((mesh, node_config) move || async move {

        // Join the full mesh to get this shard's sender and all receivers
        // The receivers are for receiving messages from other shards (synonymous with executors)
        let (sender, mut receivers) = mesh.join().await.unwrap();
        let shard_id = sender.peer_id();

        // Should be equivalent - just executor id is 1 based while peer id is 0 based
        let executor_id = glommio::executor().id();
        info!("Starting executor {executor_id} with shard id {shard_id}");

        // Local shutdown flag to avoid cross-thread contention
        let shutdown_flag = Rc::new(Cell::new(false));

        // The same sender is used in multiple threads so we need to wrap it in a RefCell and Rc
        // This is safe because we are in a single threaded executor
        let sender = Rc::new(RefCell::new(sender));

        // Our stateful request processor, pinned per shard
        // Each shard has its own instance of the engine
        let process_request = Rc::new(ProcessRequest::new(
            aggregate_read_config.clone(),
            aggregate_write_config.clone(),
            node_config.clone()
        ));

        // Shard 0 sets up signal handler with polling loop
        if shard_id == 0 {
            let mut signal_handler = SignalHandler::new()
                .expect("Failed to initialize signal handler");
            
            let sender_clone = sender.clone();
            let shutdown_flag_clone = shutdown_flag.clone();
            
            spawn_local(async move {
                loop {
                    // Check for signal FIRST
                    match signal_handler.poll_signal() {
                        Ok(Some(sig)) => {
                            info!("Received shutdown signal ({:?}). Initiating graceful shutdown...", sig);
                            shutdown_flag_clone.set(true);
                            
                            // Broadcast shutdown to all other shards
                            for peer in 0..sender_clone.borrow().nr_consumers() {
                                if peer != shard_id {
                                    let shutdown_msg = Msg {
                                        fd: -1,
                                        value: None,
                                        message_version: 0,
                                        require_shutdown: true,
                                    };
                                    if let Err(e) = sender_clone.borrow().try_send_to(peer, shutdown_msg) {
                                        error!("Failed to send shutdown signal to shard {peer}: {e:?}");
                                    }
                                }
                            }
                            break;
                        }
                        Ok(None) => {
                            // No signal yet, sleep and check again
                        }
                        Err(e) => {
                            error!("Error polling for signals: {e}");
                            break;
                        }
                    }
                    
                    // Sleep at the END of the loop
                    glommio::timer::sleep(Duration::from_secs(1)).await;
                }
            }).detach();
        }

        // There is a receiver for each other shard that we must listen to
        // So we can spin up a thread for each of these, to allow concurrent processing from other shards
        // We still need the sender for THIS shard though as we may have to forward requests from clients to other shards
        // This is because we allow clients to have a persistent TCP connection to the server (pipelining)
        for (_src_shard, stream) in receivers.streams() {
            let sender_clone = sender.clone();
            let process_request = process_request.clone();
            let shutdown_flag = shutdown_flag.clone();

            spawn_local(async move {
                while let Some(msg) = stream.recv().await {
                    // Check if this is a shutdown signal
                    if msg.require_shutdown {
                        info!("Shard {shard_id} received shutdown signal");
                        shutdown_flag.set(true);
                        let cache_clear = process_request.handle_shutdown().await;
                        if let Err(e) = cache_clear {
                            error!("Error during shard {shard_id} shutdown: {:?}", e);
                        }
                        continue;
                    }

                    // Check local shutdown flag
                    if shutdown_flag.get() {
                        info!("Shard {shard_id} rejecting forwarded message due to shutdown");
                        if msg.fd >= 0 {
                            let mut tcp_stream = unsafe { glommio::net::TcpStream::from_raw_fd(msg.fd) };
                            let error_msg = "Server is shutting down. Please reconnect.\n";
                            let _ = tcp_stream.write_all(error_msg.as_bytes()).await;
                        }
                        continue;
                    }

                    debug!("Shard {shard_id} received forwarded message");
                    
                    // If fd is -1, this is a broadcast message (e.g., UpdateCacheLimits)
                    // Process it locally without responding to a client
                    if msg.fd == -1 {
                        debug!("Shard {shard_id} processing broadcast message");
                        let _ = process_request.process(msg.value.unwrap()).await;
                        continue;
                    }
                    
                    // Reconstruct TcpStream from raw fd
                    // Ensure to use a glommio TcpStream
                    // We can do this because the sender has forgotten the fd keeping it open
                    let tcp_stream = unsafe { glommio::net::TcpStream::from_raw_fd(msg.fd) };
                    let mut tcp_stream = tcp_stream.buffered();

                    // Other shard has accepted and already read the request data.
                    // So we can process it immediately and write the response back
                    debug!("Shard {shard_id} processing forwarded request for client");
                    let response = process_request.process(msg.value.unwrap()).await;
                    write_to_tcp_stream(response, &mut tcp_stream, msg.message_version).await;

                    if !shutdown_flag.get() {
                        debug!("Shard {shard_id} keeping client alive for pipelining");
                        process_tcp_stream(shard_id, nbr_shards, tcp_stream, sender_clone.clone(), process_request.clone(), node_config.max_request_size, node_config.max_event_batches_response_size, shutdown_flag.clone()).await;
                    } else {
                        debug!("Shard {shard_id} closing connection due to shutdown");
                    }
                }
            }).detach();
        }

        // Now that we have setup our listeners for other shards, we can start accepting TCP connections from clients
        let listener = match TcpListener::bind(&node_config.listen_address) {
            Ok(l) => l,
            Err(e) => {
                error!("Shard {shard_id} failed to bind to address {}: {}", node_config.listen_address, e);
                return;
            }
        };
        let local_addr = listener.local_addr().map(|a| a.to_string()).unwrap_or_else(|_| "unknown".to_string());
        info!("Shard {shard_id} listening on {}", local_addr);

        // An infinite loop to accept incoming TCP connections
        // Essentially a fire + forget model where each connection is handled in its own task
        loop {
            if shutdown_flag.get() {
                info!("Shard {shard_id} stopping acceptance of new connections");
                break;
            }

            match glommio::timer::timeout(Duration::from_millis(100), listener.accept()).await {
                Ok(tcp_stream) => {
                    if shutdown_flag.get() {
                        info!("Shard {shard_id} rejecting connection due to shutdown");
                        let mut stream = tcp_stream;
                        let error_msg = "Server is shutting down. Please reconnect.\n";
                        let _ = stream.write_all(error_msg.as_bytes()).await;
                        continue;
                    }

                    match tcp_stream.peer_addr() {
                        Ok(addr) => debug!("Shard {shard_id} accepted connection from {addr}"),
                        Err(_) => error!("Shard {shard_id} accepted connection from unknown address"),
                    }
                    process_tcp_stream(shard_id, nbr_shards, tcp_stream.buffered(), sender.clone(), process_request.clone(), node_config.max_request_size, node_config.max_event_batches_response_size, shutdown_flag.clone()).await;
                }
                Err(_) => {
                    debug!("Shard {shard_id} timed out waiting for connection. Could also be error due to tcp connection limit on OS");
                    glommio::timer::sleep(Duration::from_millis(10)).await;
                },
            }
        }

        info!("Shard {shard_id} shutdown complete");
    }))
    .unwrap()
    .join_all();

    info!("EventPlaneDB Server shutdown complete");
}

async fn process_and_maybe_broadcast_request(
    request: Request,
    shard_id: usize,
    nbr_shards: usize,
    process_request: Rc<ProcessRequest>,
    sender: Rc<RefCell<Senders<Msg>>>,
    max_event_batches_response_size: Option<usize>,
) -> Response {
    // Check if this is an UpdateCacheLimits request that needs broadcasting
    if matches!(request, Request::UpdateCacheLimits(_)) {
        info!("Shard {shard_id} broadcasting UpdateCacheLimits to all shards");
        
        // Broadcast to all other shards (they process without responding to client)
        for peer in 0..nbr_shards {
            if peer != shard_id {
                let broadcast_msg = Msg {
                    fd: -1,  // -1 indicates broadcast message, no client response needed
                    value: Some(request.clone()),
                    message_version: 0,
                    require_shutdown: false,
                };
                if let Err(e) = sender.borrow().try_send_to(peer, broadcast_msg) {
                    error!("Failed to broadcast UpdateCacheLimits to shard {peer}: {e:?}");
                }
            }
        }
    }
    
    // Process locally and return response
    process_request.process(request).await
}

async fn process_tcp_stream(
    shard_id: usize, 
    nbr_shards: usize, 
    mut tcp_stream: glommio::net::TcpStream<glommio::net::Preallocated>, 
    sender: Rc<RefCell<Senders<Msg>>>,
    process_request: Rc<ProcessRequest>,
    max_request_size: Option<u32>, 
    max_event_batches_response_size: Option<usize>,
    shutdown_flag: Rc<Cell<bool>>,
) {
    spawn_local(async move {
        loop {
            // Check shutdown before reading next request (prevents pipelining during shutdown)
            if shutdown_flag.get() {
                debug!("Shard {shard_id} closing connection due to shutdown");
                break;
            }

            let (request, message_version) = match read_from_tcp_stream(shard_id, &mut tcp_stream, max_request_size).await {
                Some((num, message_version)) => (num, message_version),
                None => break,
            };

            let aggregate_id = request.routing_id();
            let idx = (aggregate_id % nbr_shards as u128) as usize;

            if idx != shard_id {
                debug!("Shard {shard_id} forwarding request for aggregate {aggregate_id} to shard {idx}");
                let fd = tcp_stream.into_raw_fd();
                let msg = Msg { 
                    value: Some(request), 
                    fd, 
                    message_version,
                    require_shutdown: false,
                };
                
                match sender.borrow().try_send_to(idx, msg) {
                    Ok(()) => {
                        // Successfully forwarded - this connection is now owned by another shard
                        break;
                    }
                    Err(_) => {
                        error!("Shard {shard_id} failed to forward message to shard {idx}: channel full or unavailable. Closing connection.");
                        
                        let mut tcp_stream = unsafe { glommio::net::TcpStream::from_raw_fd(fd) };
                        let error_response = "Server is overwhelmed. Please try again later.\n";
                        if let Err(e) = tcp_stream.write_all(error_response.as_bytes()).await {
                            error!("Failed to write error response: {e}");
                        }
                        break;
                    }
                }
            } else {
                debug!("Shard {shard_id} processing request for aggregate {aggregate_id} locally");
                let response = process_and_maybe_broadcast_request(
                    request,
                    shard_id,
                    nbr_shards,
                    process_request.clone(),
                    sender.clone(),
                    max_event_batches_response_size,
                ).await;
                write_to_tcp_stream(response, &mut tcp_stream, message_version).await;
            }
        }
    }).detach();
}

async fn write_to_tcp_stream(response: Response, tcp_stream: &mut glommio::net::TcpStream<glommio::net::Preallocated>, version: u32) {
    debug!("Writing response to client: {:?}", response.response_type());
    if let Err(_e) = write_response(tcp_stream, &response, eventplanedb_structures::compression_type::CompressionType::None, version).await {
        error!("Failed to write response to TCP stream");
        // Connection will be dropped when tcp_stream goes out of scope
        //TODO: We don't really expect a failure to serialize here, but its an edge case to handle
    }
}

async fn read_from_tcp_stream(
    shard_id: usize, 
    tcp_stream: &mut glommio::net::TcpStream<glommio::net::Preallocated>,
    max_request_size: Option<u32>,
) -> Option<(Request, u32)> {
    match read_request(tcp_stream, max_request_size).await {
        Ok((request, version)) => {
            debug!("Shard {shard_id} successfully parsed request with version {version}");
            Some((request, version))
        }
        Err(WireError::NetworkError(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            debug!("Client on shard {shard_id} disconnected");
            None
        }
        Err(WireError::UnsupportedProtocol(version)) => {
            let error_response = Response::ProtocolError(ProtocolErrorResponse {
            });
            
            if let Err(_e) = write_response(tcp_stream, &error_response, eventplanedb_structures::compression_type::CompressionType::None, version).await {
                error!("Shard {shard_id} failed to send error response");
            }
            
            None
        }
        Err(_e) => {
            // Fallback to v1 for unknown errors
            error!("Shard {shard_id} failed to read request");
            None
        }
    }
}