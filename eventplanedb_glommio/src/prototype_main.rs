use glommio::{
    net::TcpListener,
    LocalExecutorBuilder, Placement,
    spawn_local,
    channels::shared_channel,
};
use crate::echo_fib::{echo, read_fibonacci_input};
use std::os::fd::{AsRawFd, FromRawFd};

mod echo_fib;

#[derive(Debug)]
enum WorkItem {
    Connection(i32, std::net::SocketAddr, u32), // Raw fd instead of TcpStream
    Shutdown,
}

struct WorkerPool {
    // Change from UnboundedSender to SharedSender
    senders: Vec<shared_channel::SharedSender<WorkItem>>,
}

impl WorkerPool {
    fn new(senders: Vec<shared_channel::SharedSender<WorkItem>>) -> Self {
        Self { senders }
    }
    
    // Update to use SharedSender's try_send method
    fn send_work(&self, work: WorkItem, input_value: u32) -> Result<(), WorkItem> {
        let worker_idx = (input_value % 3) as usize;
        // Note: SharedSender needs to be connected first, so we'll handle this differently
        // For now, return the work item on error
        match self.senders.get(worker_idx) {
            Some(_) => {
                // We'll need to handle this in the async context where senders are connected
                Ok(())
            }
            None => Err(work)
        }
    }
}

fn main() {
    let worker_threads = 3;
    
    // Create shared channels for each worker
    let mut worker_senders = Vec::new();
    let mut worker_receivers = Vec::new();
    
    for _ in 0..worker_threads {
        let (tx, rx) = shared_channel::new_bounded::<WorkItem>(100); // Set reasonable buffer size
        worker_senders.push(tx);
        worker_receivers.push(rx);
    }
    
    // Spawn worker threads
    let handles: Vec<_> = worker_receivers
        .into_iter()
        .enumerate()
        .map(|(worker_id, rx)| {
            LocalExecutorBuilder::new(Placement::Fixed(worker_id + 1))
                .name(&format!("worker-{}", worker_id))
                .spawn(move || worker_main(worker_id, rx))
                .expect("Failed to spawn worker executor")
        })
        .collect();
    
    // Main thread handles accepting connections
    let acceptor_handle = LocalExecutorBuilder::new(Placement::Fixed(0))
        .name("acceptor")
        .spawn(move || accept_main(worker_senders))
        .expect("Failed to spawn acceptor executor");
    
    println!("Starting acceptor and workers");
    
    // Wait for acceptor to complete
    acceptor_handle.join().unwrap();
    
    // Wait for all worker threads to complete
    println!("Waiting for all workers to finish");
    for handle in handles {
        let _ = handle.join();
    }
    
    println!("All threads completed");
}

async fn accept_main(worker_senders: Vec<shared_channel::SharedSender<WorkItem>>) {
    // Connect all shared senders
    let mut connected_senders = Vec::new();
    for sender in worker_senders {
        connected_senders.push(sender.connect().await);
    }
    
    let listener = TcpListener::bind("127.0.0.1:50002").unwrap();
    println!("Acceptor listening on 127.0.0.1:50002");
    
    loop {
        match listener.accept().await {
            Ok(mut stream) => {
                let addr = stream.peer_addr().unwrap();
                println!("Acceptor accepted connection from {}", addr);

                let input = read_fibonacci_input(&mut stream).await.expect("able to read input");
                if input.is_none() {
                    continue;
                }
                let input_v = input.unwrap();
                
                // Extract raw fd and send to worker
                let fd = stream.as_raw_fd();
                
                // Prevent the stream from closing the fd when dropped
                std::mem::forget(stream);
                
                // Send work to appropriate worker based on input value
                let worker_idx = (input_v % 3) as usize;
                let work_item = WorkItem::Connection(fd, addr, input_v);
                
                match connected_senders[worker_idx].try_send(work_item) {
                    Ok(()) => {
                        println!("Connection sent to worker {}", worker_idx);
                    }
                    Err(glommio::GlommioError::WouldBlock(glommio::ResourceType::Channel(work_item))) => {
                        println!("Worker {} channel full, trying async send", worker_idx);
                        // Try async send for backpressure handling
                        match connected_senders[worker_idx].send(work_item).await {
                            Ok(()) => {
                                println!("Connection sent to worker {} (async)", worker_idx);
                            }
                            Err(send_error) => {
                                println!("Failed to send work to worker {}: {:?}", worker_idx, send_error);
                                // Extract the work item from the error if possible
                                if let glommio::GlommioError::Closed(glommio::ResourceType::Channel(returned_work)) = send_error {
                                    if let WorkItem::Connection(fd, _, _) = returned_work {
                                        unsafe { libc::close(fd); }
                                    }
                                }
                                break;
                            }

                        }
                    }
                    Err(glommio::GlommioError::Closed(glommio::ResourceType::Channel(work_item))) => {
                        println!("Worker {} channel closed", worker_idx);
                        if let WorkItem::Connection(fd, _, _) = work_item {
                            unsafe { libc::close(fd); }
                        }
                        break;
                    }
                    Err(e) => {
                        println!("Unexpected error sending to worker {}: {}", worker_idx, e);
                        unsafe { libc::close(fd); }
                        break;
                    }
                }
            }
            Err(e) => {
                println!("Accept failed: {}", e);
                break;
            }
        }
    }
    
    // Shutdown all workers when acceptor exits
    println!("Acceptor shutting down, signaling workers...");
    for (i, sender) in connected_senders.iter().enumerate() {
        if let Err(e) = sender.try_send(WorkItem::Shutdown) {
            println!("Failed to send shutdown to worker {}: {}", i, e);
        }
    }
}

async fn worker_main(worker_id: usize, work_rx: shared_channel::SharedReceiver<WorkItem>) {
    // Connect the shared receiver
    let connected_receiver = work_rx.connect().await;
        
    // Use the receiver as a stream
    let mut work_stream = connected_receiver;
    
    while let Some(work) = work_stream.recv().await {
        match work {
            WorkItem::Connection(fd, addr, input) => {
                
                println!(
                    "Worker handling connection {} from {} (fd: {})", 
                    worker_id, addr, fd
                );
                
                // Convert fd directly to glommio::net::TcpStream using FromRawFd
                let stream = unsafe { glommio::net::TcpStream::from_raw_fd(fd) };
                
                let worker_id_clone = worker_id;
                spawn_local(async move {
                    let result = echo(stream, input).await;
                    match result {
                        Ok(()) => {
                            println!("Worker {} completed connection", worker_id_clone);
                        }
                        Err(e) => {
                            println!("Worker {} error: {}", worker_id_clone, e);
                        }
                    }
                }).detach();
            }
            WorkItem::Shutdown => {
                println!("Worker {} received shutdown signal", worker_id);
                break;
            }
        }
    }
    
    println!("Worker {} exiting", worker_id);
}