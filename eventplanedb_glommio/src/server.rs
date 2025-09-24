use crate::{
    hash_aggregate_id, protocol::{read_message, write_message, Request, Response},
    GlommioResult, GlommioServerConfig,
};
use bincode::enc::write;
use eventplanedb_storage_stateful::stateful_engine::{StatefulDestructive, StatefulEngine, StatefulReader, StatefulWriter};

use core::fmt;
use std::{cell::RefCell, num::NonZeroUsize, os::fd::{AsRawFd, FromRawFd}, rc::Rc, vec};

use glommio::{channels::channel_mesh::{Full, MeshBuilder, Senders}, enclose, net::TcpListener, spawn_local, CpuSet, LocalExecutorPoolBuilder, PoolPlacement};
use futures_lite::AsyncReadExt;
use futures_lite::AsyncWriteExt;
use log::{debug, error, info};

enum WorkItem {
    Connection(i32, Request), // fd and first request
    Shutdown,
}

struct Msg {
    fd: i32,
    value: Request,
}

pub struct GlommioServer {
    config: GlommioServerConfig,
}

impl GlommioServer {
    pub fn new(config: GlommioServerConfig) -> Self {
        Self { config }
    }

    pub fn run(self) -> GlommioResult<()> {
        // Take advantage of all available CPUs if not specified
        let nbr_shards = self.config.shard_count.unwrap_or_else(|| {
            num_cpus::get()
        });
        let online_cpus = CpuSet::online().ok();
        info!("Number of CPUs: {nbr_shards}, Online CPUs: {online_cpus:?}");

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

            // Our stateful storage engine, pinned per shard
            // Each shard has its own instance of the engine
            let stateful_engine = Rc::new(RefCell::new(StatefulEngine::new(self.config.stateful_config)));

            // There is a receiver for each other shard that we must listen to
            // So we can spin up a thread for each of these, to allow concurrent processing from other shards
            // We still need the sender for THIS shard though as we may have to forward requests from clients to other shards
            // This is because we allow clients to have a persistent TCP connection to the server (pipelining)
            for (src_shard, stream) in receivers.streams() {
                let sender_clone = sender.clone();
                let stateful_engine_clone = stateful_engine.clone();
                spawn_local(async move {
                    while let Some(msg) = stream.recv().await {
                        debug!("Shard {shard_id} received message from {src_shard}");

                        // Reconstruct TcpStream from raw fd
                        // Ensure to use a glommio TcpStream
                        // We can do this because the sender has forgotten the fd keeping it open
                        let mut tcp_stream = unsafe { glommio::net::TcpStream::from_raw_fd(msg.fd) };

                        // Other shard has accepted and already read the request data.
                        // So we can process it immediately and write the response back
                        let response = process_synchronously_on_shard(msg.value, &stateful_engine_clone);
                        write_to_tcp_stream(&response, &mut tcp_stream).await;

                        // Continue on the tcp connection to read more data from the client
                        // Note that we might still have to forward it on to the right shard again
                        // We need to clone sender again here as inside is a spawn local
                        process_tcp_stream(shard_id, nbr_shards, tcp_stream, sender_clone.clone(), stateful_engine_clone.clone()).await;
                    }
                }).detach();
            }

            // Now that we have setup our listeners for other shards, we can start accepting TCP connections from clients
            // We will accept connections on port 10000 on localhost
            let listener = TcpListener::bind(self.config.bind_addr).unwrap();
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
                        process_tcp_stream(shard_id, nbr_shards, tcp_stream, sender.clone(), stateful_engine.clone()).await;
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
        Ok(())
    }
}


async fn process_tcp_stream(
    shard_id: usize, 
    nbr_shards: usize, 
    mut tcp_stream: glommio::net::TcpStream, 
    sender: Rc<RefCell<Senders<Msg>>>,
    stateful_engine: Rc<RefCell<StatefulEngine>>) {
    // It's critical to spawn local here as this allows accepting of new connections
    // It also allows processing messages sent from other shards 
    // Both of these can happen with spawn local while still listening to an open TCP connection
    spawn_local(async move {
        loop {
            let request = match read_from_tcp_stream(shard_id, &mut tcp_stream).await {
                Some(request) => request,
                None => return,
            };

            // Determine which shard to route this request to
            // If we are already on the correct shard, we can process it directly without an additional spawn local
            let aggregate_id = request.aggregate_id();
            let hash = hash_aggregate_id(aggregate_id);
            let idx = (hash as usize) % nbr_shards;

            if idx != shard_id {
                // Leave the TCP connection from the client open
                // This effectively transfers the connection to another shard
                let msg = Msg { value: request, fd: tcp_stream.as_raw_fd()};
                
                // Try to send the message to the target shard
                match sender.borrow().try_send_to(idx, msg) {
                    Ok(()) => {
                        // Successfully sent, forget the TCP stream so it transfers to the other shard
                        std::mem::forget(tcp_stream);
                        break;
                    }
                    Err(_) => {
                        // Channel is full or other error occurred
                        error!("Shard {shard_id} failed to forward message to shard {idx}: channel full or unavailable. Closing connection.");
                        
                        // Write an error response to the client before closing
                        let error_response = "Server is overwhelmed. Please try again later.\n";
                        if let Err(e) = tcp_stream.write_all(error_response.as_bytes()).await {
                            error!("Failed to write error response: {e}");
                        }
                        
                        // Close the connection by returning from the loop
                        return;
                    }
                }
            } else {
                let response = process_synchronously_on_shard(request, &stateful_engine);
                write_to_tcp_stream(&response, &mut tcp_stream).await;
            }
        }
    }).detach();
}

/// This is a synchronous function that blocks the current thread
/// It is called on the shard that is responsible for the given value
fn process_synchronously_on_shard(
    request: Request,
    stateful_engine: &Rc<RefCell<StatefulEngine>>) -> Response {

    let mut engine = stateful_engine.borrow_mut();

    match request {
        Request::AppendEvents { org_id, aggregate_type_id, aggregate_id, client_id, user_id, events, expected_event_batch_index } => {
            let result = engine.append_events(
                org_id, aggregate_type_id, aggregate_id, client_id, user_id, events, expected_event_batch_index,
            );
            Response::AppendEventsResult(result.map_err(|e| format!("{:?}", e)))
        }
        Request::ReadFiltered { org_id, aggregate_type_id, aggregate_id, filters } => {
            let result = engine.read_filtered(org_id, aggregate_type_id, aggregate_id, &filters);
            Response::ReadFilteredResult(result.map_err(|e| format!("{:?}", e)))
        }
        Request::Exists { org_id, aggregate_type_id, aggregate_id } => {
            let result = engine.exists(org_id, aggregate_type_id, aggregate_id);
            Response::ExistsResult(result.map_err(|e| format!("{:?}", e)))
        }
        Request::TrimStart { org_id, aggregate_type_id, aggregate_id, keep_from_event_batch_index } => {
            let result = engine.trim_start(org_id, aggregate_type_id, aggregate_id, keep_from_event_batch_index);
            Response::TrimStartResult(result.map_err(|e| format!("{:?}", e)))
        }
        Request::Delete { org_id, aggregate_type_id, aggregate_id } => {
            let result = engine.delete(org_id, aggregate_type_id, aggregate_id);
            Response::DeleteResult(result.map_err(|e| format!("{:?}", e)))
        }
    }
}

/// Write the response back to the TCP stream
/// Note we can be async here as we have already completed our syncronous 'work'
async fn write_to_tcp_stream(response: &Response, tcp_stream: &mut glommio::net::TcpStream) {
    if let Err(e) = write_message(tcp_stream, response).await {
        error!("Failed to write response to TCP stream: {e}");
        // Connection will be dropped when tcp_stream goes out of scope
    }
}

/// Read all available bytes on the TCP stream
/// Return None if the connection is closed or no data is read
/// Otherwise return the parsed u64 value
/// Note we can be async here as we are just reading from the TCP stream and other 
/// tasks can proceed while we wait for data
async fn read_from_tcp_stream(shard_id: usize, tcp_stream: &mut glommio::net::TcpStream) -> Option<Request> {
    
    // return Some(Request::Exists { org_id: 43, aggregate_type_id: 23, aggregate_id: 12 });
    
    //TODO: THis is SUPER slow
    match read_message(tcp_stream).await {
        Ok(request) => Some(request),
        Err(e) => {
            match e {
                crate::protocol::ProtocolError::ConnectionClosed => {
                    debug!("Shard {shard_id} client connection closed gracefully");
                }
                _ => {
                    error!("Shard {shard_id} failed to read from TCP stream: {e}");
                }
            }
            None
        }
    }
}
