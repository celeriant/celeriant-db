use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use futures_lite::StreamExt;
use glommio::{
    channels::{shared_channel, ChannelError},
    net::{TcpListener, TcpStream},
    spawn_local_into, LocalExecutor, LocalExecutorBuilder, Placement,
    ExecutorJoinHandle, PoolPlacement, LocalExecutorPoolBuilder,
    executor, task::TaskQueueHandle, 
};

use crate::{
    protocol::{read_message, write_message, Request, Response},
    hash_aggregate_id, GlommioError, GlommioResult,
};

use eventplanedb_storage_stateful::stateful_engine::{
    StatefulEngine, StatefulEngineConfig,
};

/// Configuration for the Glommio server
#[derive(Debug, Clone)]
pub struct GlommioServerConfig {
    /// Base path for storage
    pub base_path: PathBuf,
    /// Number of cores to use (None = all available)
    pub core_count: Option<usize>,
    /// Bind address
    pub bind_addr: SocketAddr,
    /// StatefulEngine configuration
    pub stateful_config: StatefulEngineConfig,
}

impl Default for GlommioServerConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("./data"),
            core_count: None,
            bind_addr: "127.0.0.1:8080".parse().unwrap(),
            stateful_config: StatefulEngineConfig::default(),
        }
    }
}

impl GlommioServerConfig {
    pub fn with_base_path(mut self, base_path: PathBuf) -> Self {
        self.base_path = base_path;
        self.stateful_config.base_path = base_path.clone();
        self
    }

    pub fn with_core_count(mut self, count: usize) -> Self {
        self.core_count = Some(count);
        self
    }

    pub fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }
}

/// Message to send requests to specific cores
#[derive(Debug)]
struct CoreRequest {
    request: Request,
    response_sender: glommio::channels::shared_channel::SharedSender<Response>,
}

/// Glommio-based server
pub struct GlommioServer {
    config: GlommioServerConfig,
    core_count: usize,
}

impl GlommioServer {
    pub fn new(config: GlommioServerConfig) -> Self {
        let core_count = config.core_count.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        });

        Self { config, core_count }
    }

    pub async fn run(&self) -> GlommioResult<()> {
        // Create shared channels for communication between cores
        let (request_senders, request_receivers): (Vec<_>, Vec<_>) = (0..self.core_count)
            .map(|_| shared_channel::new_bounded(1024))
            .unzip();

        // Start core executors
        let mut pool_builder = LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(
            self.core_count, 
            None
        ));

        let mut handles = Vec::new();

        // Start worker cores (all except core 0)
        for core_id in 1..self.core_count {
            let config = self.config.clone();
            let request_receiver = request_receivers[core_id].clone();

            let handle = pool_builder
                .on_placement(core_id, move || async move {
                    let mut engine = StatefulEngine::new(config.stateful_config);
                    
                    println!("Worker core {} started", core_id);

                    // Process requests on this core
                    while let Ok(core_request) = request_receiver.recv().await {
                        let response = Self::process_request(&mut engine, core_request.request).await;
                        let _ = core_request.response_sender.send(response).await;
                    }

                    println!("Worker core {} shutting down", core_id);
                })
                .expect("Failed to spawn worker core");

            handles.push(handle);
        }

        // Start the listener on core 0
        let config = self.config.clone();
        let request_senders = Arc::new(request_senders);
        let core_count = self.core_count;

        let listener_handle = pool_builder
            .on_placement(0, move || async move {
                let mut engine = StatefulEngine::new(config.stateful_config);
                
                println!("Listener core 0 started, binding to {}", config.bind_addr);
                
                let listener = TcpListener::bind(config.bind_addr)
                    .expect("Failed to bind listener");

                println!("Server listening on {}", listener.local_addr().unwrap());

                let mut incoming = listener.incoming();
                while let Some(stream) = incoming.next().await {
                    match stream {
                        Ok(stream) => {
                            let request_senders = request_senders.clone();
                            spawn_local_into(
                                Self::handle_connection(stream, request_senders, core_count, &mut engine),
                                executor().create_task_queue(
                                    glommio::Shares::Static(1),
                                    glommio::Latency::NotImportant,
                                    "connection-handler",
                                ),
                            ).detach();
                        }
                        Err(e) => {
                            eprintln!("Failed to accept connection: {:?}", e);
                        }
                    }
                }

                println!("Listener core 0 shutting down");
            })
            .expect("Failed to spawn listener core");

        handles.push(listener_handle);

        // Wait for all cores to complete
        for handle in handles {
            handle.join().await;
        }

        Ok(())
    }

    async fn handle_connection(
        mut stream: TcpStream,
        request_senders: Arc<Vec<glommio::channels::shared_channel::SharedSender<CoreRequest>>>,
        core_count: usize,
        engine: &mut StatefulEngine, // Core 0's engine for handling requests assigned to it
    ) -> GlommioResult<()> {
        println!("New connection from {:?}", stream.peer_addr());

        loop {
            // Read request
            let request: Request = match read_message(&mut stream).await {
                Ok(req) => req,
                Err(crate::protocol::ProtocolError::ConnectionClosed) => {
                    println!("Connection closed");
                    break;
                }
                Err(e) => {
                    eprintln!("Protocol error: {:?}", e);
                    break;
                }
            };

            // Determine which core should handle this request
            let core_id = if core_count == 1 {
                0
            } else {
                let hash = hash_aggregate_id(request.aggregate_id());
                (hash as usize) % core_count
            };

            let response = if core_id == 0 {
                // Handle on current core (core 0)
                Self::process_request(engine, request).await
            } else {
                // Send to appropriate worker core
                let (response_sender, response_receiver) = shared_channel::new_bounded(1);
                
                let core_request = CoreRequest {
                    request,
                    response_sender,
                };

                if let Err(_) = request_senders[core_id].send(core_request).await {
                    eprintln!("Failed to send request to core {}", core_id);
                    Response::AppendEventsResult(Err("Internal server error".to_string()))
                } else {
                    match response_receiver.recv().await {
                        Ok(response) => response,
                        Err(_) => {
                            eprintln!("Failed to receive response from core {}", core_id);
                            Response::AppendEventsResult(Err("Internal server error".to_string()))
                        }
                    }
                }
            };

            // Send response
            if let Err(e) = write_message(&mut stream, &response).await {
                eprintln!("Failed to send response: {:?}", e);
                break;
            }
        }

        Ok(())
    }

    async fn process_request(engine: &mut StatefulEngine, request: Request) -> Response {
        match request {
            Request::AppendEvents {
                aggregate_id,
                client_id,
                user_id,
                events,
                expected_event_batch_index,
            } => {
                let result = engine.append_events(
                    &aggregate_id,
                    client_id,
                    user_id,
                    events,
                    expected_event_batch_index,
                );
                Response::AppendEventsResult(result.map_err(|e| format!("{:?}", e)))
            }
            Request::ReadFiltered { aggregate_id, filters } => {
                let result = engine.read_filtered(&aggregate_id, &filters);
                Response::ReadFilteredResult(result.map_err(|e| format!("{:?}", e)))
            }
            Request::Exists { aggregate_id } => {
                let result = engine.exists(&aggregate_id);
                Response::ExistsResult(result.map_err(|e| format!("{:?}", e)))
            }
            Request::TrimStart {
                aggregate_id,
                keep_from_event_batch_index,
            } => {
                let result = engine.trim_start(&aggregate_id, keep_from_event_batch_index);
                Response::TrimStartResult(result.map_err(|e| format!("{:?}", e)))
            }
            Request::Delete { aggregate_id } => {
                let result = engine.delete(&aggregate_id);
                Response::DeleteResult(result.map_err(|e| format!("{:?}", e)))
            }
        }
    }
}