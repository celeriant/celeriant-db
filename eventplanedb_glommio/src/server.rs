use glommio::{
    channels::shared_channel,
    net::TcpListener,
    LocalExecutorBuilder, Placement, spawn_local,
};
use crate::{
    hash_aggregate_id, protocol::{read_message, write_message, Request, Response},
    GlommioResult, GlommioServerConfig,
};
use eventplanedb_storage_stateful::stateful_engine::{StatefulDestructive, StatefulEngine, StatefulReader, StatefulWriter};
use std::{cell::RefCell, os::fd::{AsRawFd, FromRawFd}, rc::Rc};

enum WorkItem {
    Connection(i32, Request), // fd and first request
    Shutdown,
}

pub struct GlommioServer {
    config: GlommioServerConfig,
}

impl GlommioServer {
    pub fn new(config: GlommioServerConfig) -> Self {
        Self { config }
    }

    pub fn run(self) -> GlommioResult<()> {
        let shard_count = self.config.shard_count.unwrap_or_else(|| {
            std::thread::available_parallelism().map(|p| p.get()).unwrap_or(1)
        });

        let mut worker_senders = Vec::new();
        let mut worker_receivers = Vec::new();
        for _ in 0..shard_count-1 {
            let (tx, rx) = shared_channel::new_bounded::<WorkItem>(128);
            worker_senders.push(tx);
            worker_receivers.push(rx);
        }

        // Spawn worker threads
        let config = self.config.clone();
        let handles: Vec<_> = worker_receivers
            .into_iter()
            .enumerate()
            .map(|(worker_id, rx)| {
                let config = config.clone();
                LocalExecutorBuilder::new(Placement::Fixed(worker_id + 1))
                    .name(&format!("worker-{}", worker_id))
                    .spawn(move || worker_main(worker_id, rx, config))
                    .expect("Failed to spawn worker executor")
            })
            .collect();

        // Main acceptor thread
        let acceptor_handle = {
            let config = self.config.clone();
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .name("acceptor")
                .spawn(move || accept_main(worker_senders, config))
                .expect("Failed to spawn acceptor executor")
        };

        acceptor_handle.join().unwrap();
        for handle in handles {
            let _ = handle.join();
        }
        Ok(())
    }
}

async fn accept_main(
    worker_senders: Vec<shared_channel::SharedSender<WorkItem>>,
    config: GlommioServerConfig,
) {
    let mut connected_senders = Vec::new();
    for sender in worker_senders {
        connected_senders.push(sender.connect().await);
    }

    let listener = TcpListener::bind(config.bind_addr).unwrap();
    println!("Listening on {}", config.bind_addr);

    loop {
        match listener.accept().await {
            Ok(mut stream) => {
                // Read the first request to get aggregate_id
                let request: Request = match read_message(&mut stream).await {
                    Ok(req) => req,
                    Err(e) => {
                        eprintln!("Failed to read request: {:?}", e);
                        continue;
                    }
                };
                let agg_id = request.aggregate_id();
                let hash = hash_aggregate_id(agg_id);
                let worker_idx = (hash as usize) % connected_senders.len();

                let fd = stream.as_raw_fd();
                std::mem::forget(stream); // Don't close fd

                let work_item = WorkItem::Connection(fd, request);
                if let Err(e) = connected_senders[worker_idx].try_send(work_item) {
                    eprintln!("Failed to send to worker {}: {}", worker_idx, e);
                    unsafe { libc::close(fd); }
                }
            }
            Err(e) => {
                eprintln!("Accept failed: {}", e);
                break;
            }
        }
    }

    // Shutdown all workers
    for sender in connected_senders {
        let _ = sender.try_send(WorkItem::Shutdown);
    }
}

async fn worker_main(
    worker_id: usize,
    work_rx: shared_channel::SharedReceiver<WorkItem>,
    config: GlommioServerConfig,
) {
    let connected_receiver = work_rx.connect().await;
    let mut work_stream = connected_receiver;
    let engine = Rc::new(RefCell::new(StatefulEngine::new(config.stateful_config)));

    while let Some(work) = work_stream.recv().await {
        match work {
            WorkItem::Connection(fd, first_request) => {
                let stream = unsafe { glommio::net::TcpStream::from_raw_fd(fd) };
                let engine = engine.clone();
                spawn_local(async move {
                    handle_connection(stream, first_request, engine).await;
                }).detach();
            }
            WorkItem::Shutdown => {
                println!("Worker {} received shutdown", worker_id);
                break;
            }
        }
    }
    println!("Worker {} exiting", worker_id);
}

async fn handle_connection(
    mut stream: glommio::net::TcpStream,
    mut request: Request,
    engine: Rc<RefCell<StatefulEngine>>,
) {
    loop {
        let response = process_request(&mut engine.borrow_mut(), request);
        if let Err(e) = write_message(&mut stream, &response).await {
            eprintln!("Failed to write response: {:?}", e);
            break;
        }
        // Try to read next request (if client supports pipelining)
        match read_message(&mut stream).await {
            Ok(req) => request = req,
            Err(crate::protocol::ProtocolError::ConnectionClosed) => break,
            Err(e) => {
                eprintln!("Protocol error: {:?}", e);
                break;
            }
        }
    }
}

fn process_request(engine: &mut StatefulEngine, request: Request) -> Response {
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