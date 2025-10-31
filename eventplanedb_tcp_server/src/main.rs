use core::fmt;
use std::{cell::RefCell, collections::HashMap, fmt::format, os::fd::{FromRawFd, IntoRawFd}, path, rc::Rc, time::Duration};

use ahash::AHasher;
use eventplanedb_core::{files::write_operations::{AggregateWriteConfig, AppendError, AppendOptions, WriteOperations, WriteOperationsDataRequirements}, local_event::LocalEvent};
use eventplanedb_structures::{aggregate_key::AggregateKey, compression_type::CompressionType};
use eventplanedb_sync_stateful::stateful_engine::{StatefulDestructive, StatefulEngine, StatefulEngineConfig, StatefulReader};
use std::hash::{Hash, Hasher};
use glommio::{channels::channel_mesh::{Full, MeshBuilder, Senders}, enclose, net::TcpListener, spawn_local, sync::Semaphore, timer::sleep, CpuSet, LocalExecutorPoolBuilder, PoolPlacement};
use futures_lite::AsyncWriteExt;
use log::{debug, error, info};

mod protocol;
use protocol::{Request, Response};

mod wire_format;
use wire_format::WireError;
use wire_format::read_message;
use wire_format::write_message;

use mimalloc::MiMalloc;

//TODO: Compare with default allocator in benchmarks
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

struct Msg {
    fd: i32,
    value: Request,
    is_bincode: bool, //TODO: Change to 'message_version' instead
}

impl fmt::Display for Msg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Msg(fd: {})", self.fd)
    }
}

//TODO: Graceful shutdown handling with signal handling (SIGINT, SIGTERM)
fn main() {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info) // Set default level
        .init();
    
    info!("Starting EventPlaneDb TCP Server...");
    
    // Take advantage of all available CPUs
    //TODO: Server configuration via cli parameters or config file
    let nbr_shards = num_cpus::get();
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
        //TODO: Included in server configuration
        let write_operations = Rc::new(RefCell::new(HashMap::<AggregateKey, Rc<RefCell<WriteOperations>>>::new()));
        let wal_sync_events = Rc::new(RefCell::new(HashMap::<AggregateKey, Rc<RefCell<Option<Rc<LocalEvent>>>>>::new()));
        let semaphores = Rc::new(RefCell::new(HashMap::<AggregateKey, Rc<Semaphore>>::new()));
        let stateful_engine = Rc::new(RefCell::new(StatefulEngine::new(StatefulEngineConfig::default())));

        // There is a receiver for each other shard that we must listen to
        // So we can spin up a thread for each of these, to allow concurrent processing from other shards
        // We still need the sender for THIS shard though as we may have to forward requests from clients to other shards
        // This is because we allow clients to have a persistent TCP connection to the server (pipelining)
        for (src_shard, stream) in receivers.streams() {
            let sender_clone = sender.clone();
            let write_operations_clone = write_operations.clone();
            let wal_sync_events = wal_sync_events.clone();
            let semaphores = semaphores.clone();
            let stateful_engine = stateful_engine.clone();
            spawn_local(async move {
                while let Some(msg) = stream.recv().await {
                    debug!("Shard {shard_id} received message from {src_shard} with value: {msg}");

                    // Reconstruct TcpStream from raw fd
                    // Ensure to use a glommio TcpStream
                    // We can do this because the sender has forgotten the fd keeping it open
                    let tcp_stream = unsafe { glommio::net::TcpStream::from_raw_fd(msg.fd) };
                    let mut tcp_stream = tcp_stream.buffered();

                    // Other shard has accepted and already read the request data.
                    // So we can process it immediately and write the response back
                    let response = process_on_shard_async(msg.value, &stateful_engine, &write_operations_clone, &wal_sync_events, &semaphores).await;
                    write_to_tcp_stream(response, &mut tcp_stream, msg.is_bincode).await;

                    // Continue on the tcp connection to read more data from the client
                    // Note that we might still have to forward it on to the right shard again
                    // We need to clone sender again here as inside is a spawn local
                    process_tcp_stream(shard_id, nbr_shards, tcp_stream, sender_clone.clone(), write_operations_clone.clone(), wal_sync_events.clone(), semaphores.clone(), stateful_engine.clone()).await;
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
                    process_tcp_stream(shard_id, nbr_shards, tcp_stream.buffered(), sender.clone(), write_operations.clone(), wal_sync_events.clone(), semaphores.clone(), stateful_engine.clone()).await;
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
fn hash_aggregate_id(aggregate_id: &u128) -> u64 {
    let mut hasher = AHasher::default();
    aggregate_id.hash(&mut hasher);
    hasher.finish()
}

async fn process_tcp_stream(
    shard_id: usize, 
    nbr_shards: usize, 
    mut tcp_stream: glommio::net::TcpStream<glommio::net::Preallocated>, 
    sender: Rc<RefCell<Senders<Msg>>>,
    write_operations: Rc<RefCell<HashMap<AggregateKey, Rc<RefCell<WriteOperations>>>>>,
    wal_sync_events: Rc<RefCell<HashMap<AggregateKey, Rc<RefCell<Option<Rc<LocalEvent>>>>>>>,
    semaphores: Rc<RefCell<HashMap<AggregateKey, Rc<Semaphore>>>>,
    stateful_engine: Rc<RefCell<StatefulEngine>>,
) {
    // It's critical to spawn local here as this allows accepting of new connections
    // It also allows processing messages sent from other shards 
    // Both of these can happen with spawn local while still listening to an open TCP connection    
    spawn_local(async move {
        loop {
            let (request, is_bincode) = match read_from_tcp_stream(shard_id, &mut tcp_stream).await {
                Some((num, is_bincode)) => (num, is_bincode),
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
                let fd = tcp_stream.into_raw_fd();
                let msg = Msg { value: request, fd, is_bincode };
                
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
                let response = process_on_shard_async(request, &stateful_engine, &write_operations, &wal_sync_events, &semaphores).await;
                write_to_tcp_stream(response, &mut tcp_stream, is_bincode).await;
            }
        }
    }).detach();
}

//TODO: Remove write config hardcoded
fn write_config() -> AggregateWriteConfig {
    AggregateWriteConfig {
        max_data_cache_size_bytes: 10 * 1024 * 1024,
    }
}


async fn sync_with_delay(write_operations: &Rc<RefCell<WriteOperations>>, wal_sync_event: Rc<RefCell<Option<Rc<LocalEvent>>>>, wal_sync_delay: Duration, sem_entry_rc: Rc<Semaphore>) -> Result<(), Response> {
    // Check if there's already a sync in progress
    let maybe_event = wal_sync_event.borrow().as_ref().cloned();
    
    if let Some(event) = maybe_event {
        // A sync is already scheduled, wait for it
        //TODO: Not propogating errors from the sync here
        event.listen().await;
    } else {
        // No sync scheduled, create new event and schedule one
        let event = Rc::new(LocalEvent::new());
        wal_sync_event.replace(Some(event.clone()));
        
        // Sleep for the delay period
        sleep(wal_sync_delay).await;
        
        // Clear the event before sync
        wal_sync_event.replace(None);
        
        // Do the actual sync
        {
            let try_permit = sem_entry_rc.acquire_permit(1).await;
            if try_permit.is_err() {
                return Err(Response::AppendEventsResult(Err("Failed to acquire write lock for sync".to_string())));
            }

            let mut write_operations = write_operations.borrow_mut();
            let try_sync = write_operations.sync_with_rollback().await;
            if try_sync.is_err() {
                let err = try_sync.err().unwrap();
                return Err(Response::AppendEventsResult(Err(format!("Failed to sync events to disk: {:?}", err))));
            }
        }
        
        // Notify waiters
        event.notify();
    }
    
    Ok(())
}

/// This is a synchronous function that blocks the current thread
/// It is called on the shard that is responsible for the given value
async fn process_on_shard_async(
    request: Request,
    engine: &Rc<RefCell<StatefulEngine>>,
    write_operations: &Rc<RefCell<HashMap<AggregateKey, Rc<RefCell<WriteOperations>>>>>,
    wal_sync_events: &Rc<RefCell<HashMap<AggregateKey, Rc<RefCell<Option<Rc<LocalEvent>>>>>>>,
    semaphores: &Rc<RefCell<HashMap<AggregateKey, Rc<Semaphore>>>>,
) -> Response {
    

    match request {
        Request::AppendEvents { sync_delay_us, org_id, aggregate_type_id, aggregate_id, client_id, user_id, events, expected_event_batch_index, filter_duplicate_client_events } => {

            let base_folder = format!("data/{org_id}/{aggregate_type_id}/{aggregate_id}");
            if let Err(e) = std::fs::create_dir_all(&base_folder) {
                return Response::AppendEventsResult(Err(format!("Failed to create directory structure: {}", e)));
            }

            // Create metadata and event batch files in the aggregate folder
            let path_metadata = format!("{}/metadata.bin", base_folder);
            let path_event_batches = format!("{}/event_batches.bin", base_folder);

            // Create files if they don't exist
            for path in [&path_metadata, &path_event_batches] {
                if !std::path::Path::new(path).exists() {
                    if let Err(e) = std::fs::File::create(path) {
                        return Response::AppendEventsResult(Err(format!("Failed to create file {}: {}", path, e)));
                    }
                }
            }

            let aggregate_key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);

            // try read-only access to get an Rc to the entry (no mutable borrow of the whole map)
            let entry_rc = {
                let map_ref = write_operations.borrow();
                map_ref.get(&aggregate_key).cloned()
            };

            let entry_rc = match entry_rc {
                Some(rc) => rc,
                None => {
                    let data_requirements = WriteOperationsDataRequirements {
                        data_cache: vec![],
                        next_event_index: 1,
                        next_event_batch_index: 1,
                        client_event_indexes: HashMap::new(),
                    };
                    // create outside of any borrow (open is async)
                    let new_wo = WriteOperations::open(
                        &path_metadata,
                        &path_event_batches,
                        data_requirements,
                        write_config(),
                    ).await.unwrap();
                    let rc = Rc::new(RefCell::new(new_wo));
                    // insert with a short-lived mutable borrow of the map
                    write_operations.borrow_mut().insert(aggregate_key.clone(), rc.clone());
                    rc
                }
            };

            let sem_entry_rc = {
                let map_ref = semaphores.borrow();
                map_ref.get(&aggregate_key).cloned()
            };

            let sem_entry_rc = match sem_entry_rc {
                Some(rc) => rc,
                None => {
                    let rc = Rc::new(Semaphore::new(1));
                    // insert with a short-lived mutable borrow of the map
                    semaphores.borrow_mut().insert(aggregate_key.clone(), rc.clone());
                    rc
                }
            };

            let result: Response;
            {
                let try_permit = sem_entry_rc.acquire_permit(1).await;
                if try_permit.is_err() {
                    Response::AppendEventsResult(Err("Failed to acquire write lock for sync".to_string()));
                }

                let server_timestamp_millis = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                let mut wo = entry_rc.borrow_mut();
                let append_options = AppendOptions {
                    client_id,
                    user_id,
                    expected_event_batch_index,
                    enforce_client_idempotency: filter_duplicate_client_events,
                    server_timestamp_millis,
                    compression_type: CompressionType::Snappy
                };
                result = match wo.queue_events_in_memory(events, &append_options) {
                    Ok(result) => Response::AppendEventsResult(Ok(result)),
                    Err(e) => Response::AppendEventsResult(Err(format!("Failed to append events: {:?}", e))),
                };
            }

            let wal_sync_event = {
                let map_ref = wal_sync_events.borrow();
                map_ref.get(&aggregate_key).cloned()
            };

            let wal_sync_event = match wal_sync_event {
                Some(rc) => rc,
                None => {
                    let rc: Rc<RefCell<Option<Rc<LocalEvent>>>> = Rc::new(RefCell::new(None));
                    // insert with a short-lived mutable borrow of the map
                    wal_sync_events.borrow_mut().insert(aggregate_key.clone(), rc.clone());
                    rc
                }
            };

            // // Have dropped mutable borrow, so now wait for write to disk
            //TODO: Need better metrics around wal sync time
            match sync_with_delay(&entry_rc, wal_sync_event, Duration::from_micros(sync_delay_us), sem_entry_rc).await {
                Ok(_) => result,
                Err(e) => e,
            }
        }
        Request::ReadFiltered { org_id, aggregate_type_id, aggregate_id, filters } => {
            let result = engine.borrow_mut().read_filtered(org_id, aggregate_type_id, aggregate_id, &filters);
            Response::ReadFilteredResult(result.map_err(|e| format!("{:?}", e)))
        }
        Request::Exists { org_id, aggregate_type_id, aggregate_id } => {
            let result = engine.borrow_mut().exists(org_id, aggregate_type_id, aggregate_id);
            Response::ExistsResult(result.map_err(|e| format!("{:?}", e)))
        }
        Request::TrimStart { org_id, aggregate_type_id, aggregate_id, keep_from_event_batch_index } => {
            let result = engine.borrow_mut().trim_start(org_id, aggregate_type_id, aggregate_id, keep_from_event_batch_index);
            Response::TrimStartResult(result.map_err(|e| format!("{:?}", e)))
        }
        Request::Delete { org_id, aggregate_type_id, aggregate_id } => {
            let result = engine.borrow_mut().delete(org_id, aggregate_type_id, aggregate_id);
            Response::DeleteResult(result.map_err(|e| format!("{:?}", e)))
        }
    }
}

/// Write the response back to the TCP stream
async fn write_to_tcp_stream(response: Response, tcp_stream: &mut glommio::net::TcpStream<glommio::net::Preallocated>, use_v2: bool) {
    if let Err(e) = write_message(tcp_stream, &response, use_v2).await {
        error!("Failed to write response to TCP stream: {e}");
        // Connection will be dropped when tcp_stream goes out of scope
    }
}

/// Read a request from the TCP stream
async fn read_from_tcp_stream(shard_id: usize, tcp_stream: &mut glommio::net::TcpStream<glommio::net::Preallocated>) -> Option<(Request, bool)> {
    match read_message(tcp_stream).await {
        Ok((request, is_bincode)) => Some((request, is_bincode)),
        Err(WireError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            debug!("Shard {shard_id} client disconnected");
            None
        }
        Err(e) => {
            error!("Shard {shard_id} failed to read request: {e}");
            None
        }
    }
}