use std::{cell::RefCell, os::fd::{FromRawFd, IntoRawFd}, rc::Rc};

use eventplanedb_core::files::{read_operations::AggregateReadConfig, write_operations::AggregateWriteConfig};
use eventplanedb_structures::{eventplanedb_error::EventPlaneDBError, request::{Request, read_request}, response::{Response, write_response}, wire_format::{PROTOCOL_VERSION_V2, WireError}};
use glommio::{CpuSet, LocalExecutorPoolBuilder, PoolPlacement, channels::channel_mesh::{Full, MeshBuilder, Senders}, enclose, net::TcpListener, spawn_local};
use futures_lite::AsyncWriteExt;
use log::{debug, error, info};

mod process_request;

use mimalloc::MiMalloc;

use crate::process_request::ProcessRequest;

//TODO: Compare with default allocator in benchmarks
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

struct Msg {
    fd: i32,
    value: Request,
    message_version: u32,
}

//TODO: Graceful shutdown handling with signal handling (SIGINT, SIGTERM)
fn main() {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info) // Set default level
        .init();
    
    info!("Starting EventPlaneDB Server...");
    
    // Take advantage of all available CPUs
    //TODO: Server configuration via cli parameters or config file
    let nbr_shards = num_cpus::get();
    let online_cpus = CpuSet::online().ok();
    info!("Number of CPUs: {nbr_shards}, Online CPUs: {online_cpus:?}");

    let aggregate_read_config = AggregateReadConfig {
        max_chunk_size: 1 << 20,
        max_data_cache_size_bytes: 1 << 20,
    };

    let aggregate_write_config = AggregateWriteConfig {
        max_data_cache_size_bytes: 1 << 25,
        max_chunk_size: 1 << 20,
    };

    // A full mesh channel is required to allow any shard to communicate with any other shard without locking primitives
    let mesh_channel_size = 1024;
    let mesh = MeshBuilder::<Msg, Full>::full(nbr_shards, mesh_channel_size);

    // Create a pool of executors, one per shard
    // Each executor will run threads pinned to a single core
    // The mesh BUILDER is copied in to each shard so they can all join the full mesh
    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(
        nbr_shards,
        online_cpus,
    ))
    .on_all_shards(enclose!((mesh) move || async move {

        // Join the full mesh to get this shard's sender and all receivers
        // The receivers are for receiving messages from other shards (synonymous with executors)
        let (sender, mut receivers) = mesh.join().await.unwrap();
        let shard_id = sender.peer_id();

        // Should be equivalent - just executor id is 1 based while peer id is 0 based
        let executor_id = glommio::executor().id();
        info!("Starting executor {executor_id} with shard id {shard_id}");

        // The same sender is used in multiple threads so we need to wrap it in a RefCell and Rc
        // This is safe because we are in a single threaded executor
        let sender = Rc::new(RefCell::new(sender));

        // Our stateful request processor, pinned per shard
        // Each shard has its own instance of the engine
        let process_request = Rc::new(ProcessRequest::new(
            aggregate_read_config.clone(),
            aggregate_write_config.clone(),
        ));

        // There is a receiver for each other shard that we must listen to
        // So we can spin up a thread for each of these, to allow concurrent processing from other shards
        // We still need the sender for THIS shard though as we may have to forward requests from clients to other shards
        // This is because we allow clients to have a persistent TCP connection to the server (pipelining)
        for (_src_shard, stream) in receivers.streams() {
            let sender_clone = sender.clone();
            let process_request = process_request.clone();

            spawn_local(async move {
                while let Some(msg) = stream.recv().await {
                    // Reconstruct TcpStream from raw fd
                    // Ensure to use a glommio TcpStream
                    // We can do this because the sender has forgotten the fd keeping it open
                    let tcp_stream = unsafe { glommio::net::TcpStream::from_raw_fd(msg.fd) };
                    let mut tcp_stream = tcp_stream.buffered();

                    // Other shard has accepted and already read the request data.
                    // So we can process it immediately and write the response back
                    let response = process_request.process(msg.value).await;
                    write_to_tcp_stream(response, &mut tcp_stream, msg.message_version).await;

                    // Continue on the tcp connection to read more data from the client
                    // Note that we might still have to forward it on to the right shard again
                    // We need to clone sender again here as inside is a spawn local
                    process_tcp_stream(shard_id, nbr_shards, tcp_stream, sender_clone.clone(), process_request.clone()).await;
                }
            }).detach();
        }

        // Now that we have setup our listeners for other shards, we can start accepting TCP connections from clients
        // We will accept connections on port 10000 on localhost
        //TODO: Include in server configuration
        let listener = TcpListener::bind("0.0.0.0:10000").unwrap();
        info!("Shard {shard_id} listening on {}", listener.local_addr().unwrap());

        // An infinite loop to accept incoming TCP connections
        // Essentially a fire + forget model where each connection is handled in its own task
        loop {
            match listener.accept().await {
                Ok(tcp_stream) => {
                    match tcp_stream.peer_addr() {
                        Ok(addr) => debug!("Shard {shard_id} accepted connection from {addr}"),
                        Err(_) => error!("Shard {shard_id} accepted connection from unknown address"),
                    }
                    process_tcp_stream(shard_id, nbr_shards, tcp_stream.buffered(), sender.clone(), process_request.clone()).await;
                }
                Err(e) => {
                    error!("Shard {shard_id} failed to accept connection: {e}");
                    // Continue listening for other connections instead of crashing
                    continue;
                }
            }
        }

    }))
    .unwrap()
    .join_all();
}

/// Hash function for aggregate_id to determine shard assignment
///TODO: Compare with murmur3 as dbeel uses that
// fn hash_aggregate_id(aggregate_id: &u128) -> u64 {
//     let mut hasher = AHasher::default();
//     aggregate_id.hash(&mut hasher);
//     hasher.finish()
// }

async fn process_tcp_stream(
    shard_id: usize, 
    nbr_shards: usize, 
    mut tcp_stream: glommio::net::TcpStream<glommio::net::Preallocated>, 
    sender: Rc<RefCell<Senders<Msg>>>,
    process_request: Rc<ProcessRequest>,
) {
    // It's critical to spawn local here as this allows accepting of new connections
    // It also allows processing messages sent from other shards 
    // Both of these can happen with spawn local while still listening to an open TCP connection    
    spawn_local(async move {
        loop {
            let (request, message_version) = match read_from_tcp_stream(shard_id, &mut tcp_stream).await {
                Some((num, message_version)) => (num, message_version),
                None => return,
            };

            // Determine which shard to route this request to
            // If we are already on the correct shard, we can process it directly without an additional spawn local
            let aggregate_id = request.routing_id();
            // let hash = hash_aggregate_id(aggregate_id);
            // let idx = (hash as usize) % nbr_shards;
            let idx = (aggregate_id % nbr_shards as u128) as usize;

            if idx != shard_id {
                // Leave the TCP connection from the client open
                // This effectively transfers the connection to another shard
                let fd = tcp_stream.into_raw_fd();
                let msg = Msg { value: request, fd, message_version };
                
                // Try to send the message to the target shard
                match sender.borrow().try_send_to(idx, msg) {
                    Ok(()) => {
                        // Successfully sent to other shard
                        break;
                    }
                    Err(_) => {
                        // Channel is full or other error occurred
                        error!("Shard {shard_id} failed to forward message to shard {idx}: channel full or unavailable. Closing connection.");
                        
                        // Reconstruct the stream to properly close it
                        let mut tcp_stream = unsafe { glommio::net::TcpStream::from_raw_fd(fd) };
                        
                        // Write an error response to the client before closing
                        let error_response = "Server is overwhelmed. Please try again later.\n";
                        if let Err(e) = tcp_stream.write_all(error_response.as_bytes()).await {
                            error!("Failed to write error response: {e}");
                        }
                        
                        // The tcp_stream will be properly dropped here, closing the connection
                        return;
                    }
                }
            } else {
                let response = process_request.process(request).await;
                write_to_tcp_stream(response, &mut tcp_stream, message_version).await;
            }
        }
    }).detach();
}

async fn write_to_tcp_stream(response: Response, tcp_stream: &mut glommio::net::TcpStream<glommio::net::Preallocated>, version: u32) {
    if let Err(e) = write_response(tcp_stream, &response, eventplanedb_structures::compression_type::CompressionType::None).await {
        error!("Failed to write response to TCP stream: {e}");
        // Connection will be dropped when tcp_stream goes out of scope
        //TODO: We don't really expect a failure to serialize here, but its an edge case to handle
    }
}

async fn read_from_tcp_stream(
    shard_id: usize, 
    tcp_stream: &mut glommio::net::TcpStream<glommio::net::Preallocated>
) -> Option<(Request, u32)> {
    match read_request(tcp_stream).await {
        Ok((request, version)) => Some((request, version)),
        Err(WireError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            debug!("Shard {shard_id} client disconnected");
            None
        }
        Err(WireError::InvalidFormatWithVersion(version)) => {
            let error_response = Response::ProtocolError {
                correlation_id: None,
                error: EventPlaneDBError::invalid_request(),
            };
            
            if let Err(e) = write_response(tcp_stream, &error_response, eventplanedb_structures::compression_type::CompressionType::None).await {
                error!("Shard {shard_id} failed to send error response: {e}");
            }
            
            None
        }
        Err(e) => {
            // Fallback to v1 for unknown errors
            error!("Shard {shard_id} failed to read request: {e}");
            None
        }
    }
}