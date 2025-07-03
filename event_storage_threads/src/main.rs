use std::collections::HashMap;
use std::sync::{Arc};
use std::thread;
use core_affinity;
use crossbeam::channel::{Sender, Receiver, unbounded};
use tokio::sync::oneshot;
use ahash::AHasher;
use std::hash::{Hash, Hasher};

// A job that optionally returns a result
enum Job {
    NoResult(String),
    WithResult(String, oneshot::Sender<String>),
}

// Create a thread pool where each thread is pinned to a core
fn create_thread_pool(n: usize) -> (Vec<Sender<Job>>, Vec<thread::JoinHandle<()>>) {
    let cores = core_affinity::get_core_ids().unwrap();
    let mut senders = Vec::new();
    let mut handles = Vec::new();

    for i in 0..n {
        let (tx, rx): (Sender<Job>, Receiver<Job>) = unbounded();
        let core_id = cores[i];
        let handle = thread::spawn(move || {
            // Pin thread to CPU core
            core_affinity::set_for_current(core_id);

            // Worker loop
            for job in rx.iter() {
                match job {
                    Job::NoResult(data) => {
                        println!("[Core {}] Processed (fire-and-forget): {}", core_id.id, data);
                    }
                    Job::WithResult(data, tx) => {
                        let result = format!("Processed: {}", data);
                        let _ = tx.send(result); // ignore if receiver dropped
                    }
                }
            }
        });

        senders.push(tx);
        handles.push(handle);
    }

    (senders, handles)
}

// Consistent hashing to select worker
fn hash_string_to_index(id: &str, num_threads: usize) -> usize {
    let mut hasher = AHasher::default();
    id.hash(&mut hasher);
    (hasher.finish() as usize) % num_threads
}

// Submit a job and optionally wait for a result
fn submit_job(
    workers: &[Sender<Job>],
    id: &str,
    wait_for_result: bool,
) -> Option<String> {
    let index = hash_string_to_index(id, workers.len());
    if wait_for_result {
        let (tx, rx) = oneshot::channel();
        workers[index].send(Job::WithResult(id.to_string(), tx)).unwrap();
        Some(rx.blocking_recv().unwrap())
    } else {
        workers[index].send(Job::NoResult(id.to_string())).unwrap();
        None
    }
}

fn main() {
    let num_threads = 4;
    let (workers, handles) = create_thread_pool(num_threads);

    // Submit jobs
    submit_job(&workers, "user_123", false);
    submit_job(&workers, "user_456", false);

    let result = submit_job(&workers, "user_789", true);
    println!("Got result: {:?}", result);

    // Let threads run a bit (in real app, you'd handle shutdown properly)
    std::thread::sleep(std::time::Duration::from_secs(1));
}
