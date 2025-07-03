use std::io;

use std::hash::{Hash, Hasher};
use ahash::AHasher;
use event_storage::event_storage_cache::EventStorageCache;
use event_storage::{catchup_result::CatchupResult, event_batch_item::EventBatchItem};
use tokio::sync::oneshot;
use core_affinity;
use crossbeam::channel::{unbounded, Receiver, Sender};

fn hash_string_to_index(id: &str, num_threads: usize) -> usize {
    let mut hasher = AHasher::default();
    id.hash(&mut hasher);
    (hasher.finish() as usize) % num_threads
}

pub fn create_thread_pool(n: usize) -> Vec<Sender<Job>> {
    let cores = core_affinity::get_core_ids().unwrap();
    let num_available_cores = cores.len(); // Get the total number of cores
    let num_threads_to_use = std::cmp::min(n, num_available_cores); // Use min to not exceed available cores

    let mut senders = Vec::new();

    for i in 0..num_threads_to_use {
        let (tx, rx): (Sender<Job>, Receiver<Job>) = unbounded();
        let core_id = cores[i];

        // Spawn pinned thread
        std::thread::spawn(move || {
            core_affinity::set_for_current(core_id);

            let mut event_storage_cache = EventStorageCache::new(30, 1000000, 10000);

            for job in rx.iter() {
                match job {
                    Job::Write { file_path, allow_create, event_batch_item, responder } => {
                        let result = event_storage_cache.write(&file_path, allow_create, event_batch_item);
                        let _ = responder.send(result);
                    }
                    Job::Read { file_path, from_si, max_bytes, responder } => {
                        let result = event_storage_cache.read(&file_path, from_si, max_bytes);
                        let _ = responder.send(result);
                    }
                    Job::Shutdown { responder } => {
                        let _ = responder.send(());
                        break; // Exit the worker loop
                    }
                }
            }
        });

        senders.push(tx);
    }

    senders
}

pub enum Job {
    Write {
        file_path: String,
        allow_create: bool,
        event_batch_item: EventBatchItem,
        responder: oneshot::Sender<io::Result<u64>>,
    },
    Read {
        file_path: String,
        from_si: u64,
        max_bytes: usize,
        responder: oneshot::Sender<io::Result<CatchupResult>>,
    },
    Shutdown {
        responder: oneshot::Sender<()>,
    },
}

pub async fn write_async(
    workers: &[Sender<Job>],
    file_path: String,
    allow_create: bool,
    event_batch_item: EventBatchItem,
) -> io::Result<u64> {
    let index = hash_string_to_index(&file_path, workers.len());
    let (tx, rx) = oneshot::channel(); // Channel to receive the result

    let job = Job::Write {
        file_path,
        allow_create,
        event_batch_item,
        responder: tx, // Attach the sender
    };

    // Send the job to the appropriate worker
    workers[index].send(job).map_err(|_| io::Error::new(io::ErrorKind::Other, "Worker thread channel closed"))?;

    // Await the result from the worker
    rx.await.map_err(|_| io::Error::new(io::ErrorKind::Other, "Worker thread dropped responder"))?
}

pub async fn read_async(
    workers: &[Sender<Job>],
    file_path: String,
    from_si: u64,
    max_bytes: usize,
) -> io::Result<CatchupResult> {
    let index = hash_string_to_index(&file_path, workers.len());
    let (tx, rx) = oneshot::channel(); // Channel to receive the result

    let job = Job::Read {
        file_path,
        from_si,
        max_bytes,
        responder: tx, // Attach the sender
    };

    // Send the job to the appropriate worker
    workers[index].send(job).map_err(|_| io::Error::new(io::ErrorKind::Other, "Worker thread channel closed"))?;

    // Await the result from the worker
    rx.await.map_err(|_| io::Error::new(io::ErrorKind::Other, "Worker thread dropped responder"))?
}

pub async fn shutdown_workers(workers: Vec<Sender<Job>>) -> Result<(), Box<dyn std::error::Error>> {
    let mut shutdown_futures = Vec::new();
    
    for worker in workers {
        let (tx, rx) = oneshot::channel();
        let job = Job::Shutdown { responder: tx };
        
        if worker.send(job).is_ok() {
            shutdown_futures.push(rx);
        }
    }
    
    // Wait for all workers to confirm shutdown
    for future in shutdown_futures {
        let _ = future.await;
    }
    
    Ok(())
}