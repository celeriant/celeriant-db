use std::hash::{Hash, Hasher};
use std::thread;

use ahash::AHasher;
use core_affinity;
use crossbeam::channel::{unbounded, Receiver, Sender};
use nanoid::nanoid;
use tokio::sync::oneshot;

enum Job {
    NoResult(String),
    WithResult(String, oneshot::Sender<String>),
}

fn create_thread_pool(n: usize) -> Vec<Sender<Job>> {
    let cores = core_affinity::get_core_ids().unwrap();
    let num_available_cores = cores.len(); // Get the total number of cores
    let num_threads_to_use = std::cmp::min(n, num_available_cores); // Use min to not exceed available cores

    let mut senders = Vec::new();

    for i in 0..num_threads_to_use {
        let (tx, rx): (Sender<Job>, Receiver<Job>) = unbounded();
        let core_id = cores[i];

        // Spawn pinned thread
        thread::spawn(move || {
            core_affinity::set_for_current(core_id);

            for job in rx.iter() {
                match job {
                    Job::NoResult(data) => {
                        println!("[Core {}] Fire-and-forget: {}", core_id.id, data);
                    }
                    Job::WithResult(data, tx) => {
                        let result = format!("Processed: {}", data);
                        let _ = tx.send(result);
                    }
                }
            }
        });

        senders.push(tx);
    }

    senders
}

fn hash_string_to_index(id: &str, num_threads: usize) -> usize {
    let mut hasher = AHasher::default();
    id.hash(&mut hasher);
    (hasher.finish() as usize) % num_threads
}

async fn submit_job(
    workers: &[Sender<Job>],
    id: &str,
    wait_for_result: bool,
) -> Option<String> {
    let index = hash_string_to_index(id, workers.len());

    if wait_for_result {
        let (tx, rx) = oneshot::channel();
        workers[index]
            .send(Job::WithResult(id.to_string(), tx))
            .unwrap();
        Some(rx.await.expect("Worker dropped response"))
    } else {
        workers[index]
            .send(Job::NoResult(id.to_string()))
            .unwrap();
        None
    }
}

#[tokio::main]
async fn main() {
    let cores = core_affinity::get_core_ids().unwrap();
    let workers = create_thread_pool(cores.len());

    // Fire-and-forget job
    for _ in 0..1000 {
        submit_job(&workers, nanoid!().as_str(), false).await;
        if let Some(result) = submit_job(&workers, nanoid!().as_str(), true).await {
            println!("Got result: {}", result);
        }
    }

    // Let threads finish processing
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
}
