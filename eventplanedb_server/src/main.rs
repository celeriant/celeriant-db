use base64::{Engine, engine::general_purpose};
use clap::Parser;
use eventplanedb_crypto::Crypto;
use std::{cell::{Cell, RefCell}, fs, io::{Read, Write}, os::fd::{FromRawFd, IntoRawFd}, rc::Rc, time::Duration};

use eventplanedb_core::{
    cache::lease_error::LeaseError, msg::Msg, node_config::NodeConfig, object_store::{
        ObjectStoreGateway, ObjectStoreRetryConfig, ObjectStoreRuntime, ObjectStoreRuntimeConfig, S3Config
    }, process_request::{LeasingChannelTrait, ProcessRequest}, read_operations::read_structures::AggregateReadConfig, replication::node_lease::NodeLease, write_operations::write_structures::AggregateWriteConfig
};
use eventplanedb_structures::{
    lease_info::LeaseInfo, request::{Request, read_request}, response::{ProtocolErrorResponse, Response, write_response}, wire_error::WireError
};
use glommio::{
    channels::channel_mesh::{Full, MeshBuilder, Senders},
    enclose,
    net::TcpListener,
    spawn_local,
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement,
};
use futures_lite::AsyncWriteExt;
use log::{debug, error, info};

mod config;
mod signal_handler;

use config::EventPlaneDBConfig;
use signal_handler::SignalHandler;

use mimalloc::MiMalloc;

use eventplanedb_core::leasing_channel::{LeasingChannel, LeaseRequestMsg, LeaseResponseMsg, LEADER_SHARD_ID};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;


/// Holds the object store sidecar runtime (lives on the main thread, outside Glommio).
struct SidecarHandle {
    runtime: Option<ObjectStoreRuntime>,
    gateway: ObjectStoreGateway,
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

    // Load or generate a persistent node ID
    let node_id = match load_or_generate_node_id(&config.data_root) {
        Ok(id) => id,
        Err(e) => {
            error!("Failed to initialize node ID: {}", e);
            std::process::exit(1);
        }
    };
    info!("Node ID: {}", node_id);

    let aggregate_read_config = AggregateReadConfig {
        max_chunk_size: config.aggregate_read_max_chunk_size,
    };

    let aggregate_write_config = AggregateWriteConfig {
        max_data_cache_size_bytes: config.aggregate_write_max_data_cache_size_bytes,
        cache_trim_factor: config.cache_trim_factor,
        max_chunk_size: config.aggregate_write_max_chunk_size,
    };

    // Initialize object store sidecar if S3 is enabled
    let sidecar_handle: Option<SidecarHandle> = if config.s3_enabled {
        match initialize_object_store_sidecar(&config, nbr_shards) {
            Ok(handle) => {
                info!("Object store sidecar initialized successfully");
                Some(handle)
            }
            Err(e) => {
                error!("Failed to initialize object store sidecar: {}. S3 features will be disabled.", e);
                None
            }
        }
    } else {
        info!("S3 integration is disabled");
        None
    };

    // Extract gateway for sharing with shards (if available)
    let gateway = sidecar_handle.as_ref().map(|h| h.gateway.clone());
    
    let s3_subfolder = config
        .s3_subfolder
        .as_ref()
        .filter(|s| !s.is_empty())
        .cloned();

    // A full mesh channel is required to allow any shard to communicate with any other shard
    let mesh = MeshBuilder::<Msg, Full>::full(nbr_shards, config.mesh_channel_size);

    let node_config = NodeConfig {
        data_root_folder: config.data_root.to_string_lossy().to_string(),
        node_id,
        margin_ms: 1500, // 1.5 second safety margin before lease expiry
        lease_expiry_ms: 10000, // 10 second lease duration
        async_flush_ms: config.async_flush_ms,
        max_open_aggregates: config.max_open_aggregates,
        max_request_size: config.max_request_size,
        listen_address: config.listen_address.clone(),
        max_event_batches_response_size: config.max_event_batches_response_size.map(|v| v as usize),
        s3_enabled: config.s3_enabled && sidecar_handle.is_some(),
    };

    // Create a pool of executors, one per shard
    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(nbr_shards, online_cpus))
        .on_all_shards(enclose!((mesh, node_config, gateway, s3_subfolder) move || async move {
            // Join the full mesh to get this shard's sender and all receivers
            let (sender, mut receivers) = mesh.join().await.unwrap();
            let shard_id = sender.peer_id();

            let executor_id = glommio::executor().id();
            info!("Starting executor {executor_id} with shard id {shard_id}");

            // Local shutdown flag
            let shutdown_flag = Rc::new(Cell::new(false));

            let sender = Rc::new(RefCell::new(sender));

            // Create the leasing channel
            let leasing_channel = Rc::new(LeasingChannel::new());

            //Initialize NodeLease only on shard 0
            if shard_id == LEADER_SHARD_ID {
                if let Some(ref gw) = gateway {
                    let node_lease = Rc::new(NodeLease::new(
                        node_config.clone(),
                        gw.clone(),
                        s3_subfolder.clone(),
                    ));
                    leasing_channel.initialize_leader(shard_id, sender.clone(), node_lease).await;
                    info!("Shard {} initialized as lease leader", shard_id);
                } else {
                    // No S3, initialize as follower (leasing will be no-op)
                    leasing_channel.initialize_follower(shard_id, sender.clone()).await;
                }
            } else {
                leasing_channel.initialize_follower(shard_id, sender.clone()).await;
            }

            // Create the request processor
            let process_request = Rc::new(ProcessRequest::new(
                aggregate_read_config.clone(),
                aggregate_write_config.clone(),
                node_config.clone(),
                leasing_channel.clone(),
            ));

            // Shard 0 sets up signal handler with polling loop
            if shard_id == 0 {
                let mut signal_handler = SignalHandler::new()
                    .expect("Failed to initialize signal handler");

                let sender_clone = sender.clone();
                let shutdown_flag_clone = shutdown_flag.clone();

                spawn_local(async move {
                    loop {
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
                                            lease_request: None,
                                            lease_response: None,
                                        };
                                        if let Err(e) = sender_clone.borrow().try_send_to(peer, shutdown_msg) {
                                            error!("Failed to send shutdown signal to shard {peer}: {e:?}");
                                        }
                                    }
                                }
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
                }).detach();
            }

            // Set up receivers for messages from other shards
            for (_src_shard, stream) in receivers.streams() {
                let sender_clone = sender.clone();
                let process_request = process_request.clone();
                let shutdown_flag = shutdown_flag.clone();
                let leasing_channel = leasing_channel.clone();

                spawn_local(async move {
                    while let Some(msg) = stream.recv().await {
                        // Handle shutdown messages
                        if msg.require_shutdown {
                            info!("Shard {shard_id} received shutdown signal");
                            shutdown_flag.set(true);
                            let cache_clear = process_request.handle_shutdown().await;
                            if let Err(e) = cache_clear {
                                error!("Error during shard {shard_id} shutdown: {:?}", e);
                            }
                            continue;
                        }

                        // Handle lease requests (only on leader shard)
                        if let Some(lease_request) = msg.lease_request {
                            if shard_id == LEADER_SHARD_ID {
                                debug!("Shard {} handling lease request from shard {}", shard_id, lease_request.from_shard);
                                let from_shard = lease_request.from_shard;
                                let response = leasing_channel.handle_lease_request(lease_request).await;
                                
                                // Send response back to requesting shard
                                let response_msg = Msg {
                                    fd: -1,
                                    value: None,
                                    message_version: 0,
                                    require_shutdown: false,
                                    lease_request: None,
                                    lease_response: Some(response),
                                };
                                if let Err(e) = sender_clone.borrow().try_send_to(from_shard, response_msg) {
                                    error!("Failed to send lease response to shard {}: {:?}", from_shard, e);
                                }
                            } else {
                                error!("Shard {} received lease request but is not leader", shard_id);
                            }
                            continue;
                        }

                        // Handle lease responses (on follower shards)
                        if let Some(lease_response) = msg.lease_response {
                            debug!("Shard {} received lease response for request {}", shard_id, lease_response.request_id);
                            leasing_channel.deliver_response(lease_response).await;
                            continue;
                        }

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

                        if msg.fd == -1 {
                            debug!("Shard {shard_id} processing broadcast message");
                            if let Some(request) = msg.value {
                                let _ = process_request.process(request).await;
                            }
                            continue;
                        }

                        let tcp_stream = unsafe { glommio::net::TcpStream::from_raw_fd(msg.fd) };
                        let mut tcp_stream = tcp_stream.buffered();

                        debug!("Shard {shard_id} processing forwarded request for client");
                        let response = process_request.process(msg.value.unwrap()).await;
                        write_to_tcp_stream(response, &mut tcp_stream, msg.message_version).await;

                        if !shutdown_flag.get() {
                            debug!("Shard {shard_id} keeping client alive for pipelining");
                            process_tcp_stream(
                                shard_id,
                                nbr_shards,
                                tcp_stream,
                                sender_clone.clone(),
                                process_request.clone(),
                                node_config.max_request_size,
                                node_config.max_event_batches_response_size,
                                shutdown_flag.clone(),
                            ).await;
                        } else {
                            debug!("Shard {shard_id} closing connection due to shutdown");
                        }
                    }
                }).detach();
            }

            // Accept TCP connections
            let listener = match TcpListener::bind(&node_config.listen_address) {
                Ok(l) => l,
                Err(e) => {
                    error!("Shard {shard_id} failed to bind to address {}: {}", node_config.listen_address, e);
                    return;
                }
            };
            let local_addr = listener.local_addr().map(|a| a.to_string()).unwrap_or_else(|_| "unknown".to_string());
            info!("Shard {shard_id} listening on {}", local_addr);

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
                        process_tcp_stream(
                            shard_id,
                            nbr_shards,
                            tcp_stream.buffered(),
                            sender.clone(),
                            process_request.clone(),
                            node_config.max_request_size,
                            node_config.max_event_batches_response_size,
                            shutdown_flag.clone(),
                        ).await;
                    }
                    Err(_) => {
                        glommio::timer::sleep(Duration::from_millis(10)).await;
                    }
                }
            }

            info!("Shard {shard_id} shutdown complete");
        }))
        .unwrap()
        .join_all();

    // Shutdown the sidecar after all Glommio executors have stopped
    if let Some(handle) = sidecar_handle {
        if let Some(runtime) = handle.runtime {
            info!("Shutting down object store sidecar...");
            runtime.shutdown();
            info!("Object store sidecar shutdown complete");
        }
    }

    info!("EventPlaneDB Server shutdown complete");
}

/// Initialize the object store sidecar runtime.
fn initialize_object_store_sidecar(
    config: &EventPlaneDBConfig,
    num_shards: usize,
) -> Result<SidecarHandle, String> {
    let s3_bucket = config.s3_bucket.as_ref()
        .ok_or("S3 bucket name is required when s3_enabled is true")?;

    let s3_config = S3Config {
        bucket: s3_bucket.clone(),
        region: config.s3_region.clone(),
        access_key_id: config.s3_access_key_id.clone(),
        secret_access_key: config.s3_secret_access_key.clone(),
        subfolder: config.s3_subfolder.clone(),
    };

    let runtime_config = ObjectStoreRuntimeConfig::with_num_shards(num_shards);
    let retry_config = ObjectStoreRetryConfig::default();

    let (gateway, receivers) = ObjectStoreGateway::new(&runtime_config);

    let runtime = ObjectStoreRuntime::spawn(runtime_config, retry_config, s3_config, receivers)
        .map_err(|e| format!("Failed to spawn object store runtime: {}", e))?;

    Ok(SidecarHandle {
        runtime: Some(runtime),
        gateway,
    })
}

/// Load existing keys from disk or generate and persist a new keypair.
/// Returns the node ID derived from the public key.
fn load_or_generate_node_id(data_root: &std::path::Path) -> Result<u128, String> {
    let private_key_path = data_root.join("private_key");
    let public_key_path = data_root.join("public_key");

    // Try to load existing keys
    if private_key_path.exists() && public_key_path.exists() {
        let mut public_key_file = fs::File::open(&public_key_path)
            .map_err(|e| format!("Failed to open public_key file: {}", e))?;
        
        let mut public_key_base64 = String::new();
        public_key_file.read_to_string(&mut public_key_base64)
            .map_err(|e| format!("Failed to read public_key file: {}", e))?;
        
        let public_key_base64 = public_key_base64.trim();
        
        // Decode public key from base64 to get raw bytes for identity generation
        let public_key_bytes = general_purpose::STANDARD
            .decode(public_key_base64)
            .map_err(|e| format!("Failed to decode public key: {}", e))?;
        
        let node_id = Crypto::generate_short_client_identity(&public_key_bytes);
        
        info!("Loaded existing keypair from {:?}", data_root);
        return Ok(node_id);
    }

    // Generate new keypair
    let keypair = Crypto::generate_keypair(None)
        .map_err(|e| format!("Failed to generate keypair: {}", e))?;

    // Ensure data_root exists
    fs::create_dir_all(data_root)
        .map_err(|e| format!("Failed to create data root directory: {}", e))?;

    // Save private key
    let mut private_key_file = fs::File::create(&private_key_path)
        .map_err(|e| format!("Failed to create private_key file: {}", e))?;
    private_key_file.write_all(keypair.private_key_base64.as_bytes())
        .map_err(|e| format!("Failed to write private_key file: {}", e))?;
    private_key_file.sync_all()
        .map_err(|e| format!("Failed to sync private_key file: {}", e))?;

    // Save public key
    let mut public_key_file = fs::File::create(&public_key_path)
        .map_err(|e| format!("Failed to create public_key file: {}", e))?;
    public_key_file.write_all(keypair.public_key_base64.as_bytes())
        .map_err(|e| format!("Failed to write public_key file: {}", e))?;
    public_key_file.sync_all()
        .map_err(|e| format!("Failed to sync public_key file: {}", e))?;

    // Decode public key from base64 to get raw bytes for identity generation
    let public_key_bytes = general_purpose::STANDARD
        .decode(&keypair.public_key_base64)
        .map_err(|e| format!("Failed to decode public key: {}", e))?;

    let node_id = Crypto::generate_short_client_identity(&public_key_bytes);

    info!("Generated and saved new keypair to {:?}", data_root);
    Ok(node_id)
}

async fn process_and_maybe_broadcast_request(
    request: Request,
    shard_id: usize,
    nbr_shards: usize,
    process_request: Rc<ProcessRequest<LeasingChannel>>,
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
                    lease_request: None,
                    lease_response: None,
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
    process_request: Rc<ProcessRequest<LeasingChannel>>,
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
                    lease_request: None,
                    lease_response: None,
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