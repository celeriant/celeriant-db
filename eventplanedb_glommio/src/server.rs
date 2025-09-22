use futures_lite::StreamExt;
use glommio::{
    channels::sharding::{Handler, HandlerResult, Sharded},
    net::{TcpListener, TcpStream},
    prelude::*,
    CpuSet, LocalExecutorPoolBuilder, PoolPlacement,
};

use crate::{
    hash_aggregate_id, protocol::{read_message, write_message, Request, Response},
    GlommioResult, GlommioServerConfig,
};

use eventplanedb_storage_stateful::stateful_engine::{StatefulEngine, StatefulWriter};

/// Message handler for processing requests on shards
#[derive(Clone)]
pub struct RequestHandler {
    engine: StatefulEngine,
}

impl RequestHandler {
    fn new(config: &GlommioServerConfig) -> Self {
        let engine = StatefulEngine::new(config.stateful_config.clone());
        Self { engine }
    }

    fn process_request(&mut self, request: Request) -> Response {
        match request {
            Request::AppendEvents {
                aggregate_id,
                client_id,
                user_id,
                events,
                expected_event_batch_index,
            } => {
                let result = self.engine.append_events(
                    &aggregate_id,
                    client_id,
                    user_id,
                    events,
                    expected_event_batch_index,
                );
                Response::AppendEventsResult(result.map_err(|e| format!("{:?}", e)))
            }
            Request::ReadFiltered { aggregate_id, filters } => {
                let result = self.engine.read_filtered(&aggregate_id, &filters);
                Response::ReadFilteredResult(result.map_err(|e| format!("{:?}", e)))
            }
            Request::Exists { aggregate_id } => {
                let result = self.engine.exists(&aggregate_id);
                Response::ExistsResult(result.map_err(|e| format!("{:?}", e)))
            }
            Request::TrimStart {
                aggregate_id,
                keep_from_event_batch_index,
            } => {
                let result = self.engine.trim_start(&aggregate_id, keep_from_event_batch_index);
                Response::TrimStartResult(result.map_err(|e| format!("{:?}", e)))
            }
            Request::Delete { aggregate_id } => {
                let result = self.engine.delete(&aggregate_id);
                Response::DeleteResult(result.map_err(|e| format!("{:?}", e)))
            }
        }
    }
}

impl Handler<(Request, TcpStream)> for RequestHandler {
    fn handle(
        &mut self,
        (request, mut stream): (Request, TcpStream),
        _src_shard: usize,
        _cur_shard: usize,
    ) -> HandlerResult {
        async move {
            let response = self.process_request(request);
            if let Err(e) = write_message(&mut stream, &response).await {
                eprintln!("Failed to write response: {:?}", e);
            }
            let _ = stream.close().await;
        }.boxed_local()
    }
}

/// The main Glommio server
pub struct GlommioServer {
    config: GlommioServerConfig,
}

impl GlommioServer {
    pub fn new(config: GlommioServerConfig) -> Self {
        Self { config }
    }

    pub fn run(self) -> GlommioResult<()> {
        let shard_count = self.config.shard_count.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        });

        let bind_addr = self.config.bind_addr;
        let config = self.config;

        // Determine shard assignment for requests
        fn get_shard_for(req: &(Request, TcpStream), nr_shards: usize) -> usize {
            let hash = hash_aggregate_id(req.0.aggregate_id());
            (hash as usize) % nr_shards
        }

        println!("Starting EventPlane Glommio server on {} with {} shards", bind_addr, shard_count);

        // Use the proper LocalExecutorPoolBuilder pattern from examples
        LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(shard_count, CpuSet::online().ok()))
            .on_all_shards(move || {
                let config = config.clone();
                let bind_addr = bind_addr;
                
                async move {
                    let shard_id = glommio::executor().id();
                    println!("Starting shard {}", shard_id);

                    // Create the mesh for sharding
                    let mesh = glommio::channels::channel_mesh::MeshBuilder::full(shard_count, 1024);
                    
                    // Create the request handler for this shard
                    let handler = RequestHandler::new(&config);
                    
                    // Create the sharded system
                    let mut sharded = Sharded::new(mesh, get_shard_for, handler)
                        .await
                        .expect("Failed to create sharded system");

                    // Only shard 0 listens for connections
                    if shard_id == 0 {
                        let listener = TcpListener::bind(bind_addr)
                            .expect("Failed to bind listener");
                        
                        println!("Listening on {}", listener.local_addr().unwrap());

                        let mut incoming = listener.incoming();
                        while let Some(stream) = incoming.next().await {
                            match stream {
                                Ok(stream) => {
                                    // Spawn a task to handle this connection
                                    glommio::spawn_local(handle_connection(stream, &mut sharded)).detach();
                                }
                                Err(e) => {
                                    eprintln!("Failed to accept connection: {:?}", e);
                                }
                            }
                        }
                    } else {
                        // Worker shards just process their queue
                        sharded.handle(futures_lite::stream::pending::<(Request, TcpStream)>()).unwrap();
                    }

                    sharded.close().await;
                }
            })
            .unwrap()
            .join_all();

        Ok(())
    }
}

async fn handle_connection(
    mut stream: TcpStream, 
    sharded: &mut Sharded<(Request, TcpStream), RequestHandler>
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

        // Send the request and stream to the appropriate shard
        // The sharded system will handle routing based on aggregate_id
        if let Err(e) = sharded.send((request, stream.clone())).await {
            eprintln!("Failed to send request to shard: {:?}", e);
            break;
        }
    }

    Ok(())
}