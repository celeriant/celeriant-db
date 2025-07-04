use crossbeam::channel::Sender;
use event_storage::{catchup_result::CatchupResult, event_batch_item::EventBatchItem};
use tokio::sync::oneshot;

use crate::{job::Job, job_error::JobError, thread_assigner::hash_string_to_index};

pub async fn write_async(
    workers: &[Sender<Job>],
    file_path: String,
    allow_create: bool,
    share_key: Option<String>,
    event_batch_item: EventBatchItem,
) -> Result<u64, JobError> {
    let index = hash_string_to_index(&file_path, workers.len());
    let (tx, rx) = oneshot::channel();

    let job = Job::Write {
        file_path,
        allow_create,
        share_key,
        event_batch_item,
        responder: tx,
    };

    workers[index]
        .send(job)
        .map_err(|_| JobError::Other("Worker thread channel closed".to_string()))?;

    rx.await
        .map_err(|_| JobError::Other("Worker thread dropped responder".to_string()))?
}

pub async fn read_async(
    workers: &[Sender<Job>],
    file_path: String,
    cb: String,
    share_key: Option<String>,
    from_si: u64,
    max_bytes: usize,
) -> Result<CatchupResult, JobError> {
    let index = hash_string_to_index(&file_path, workers.len());
    let (tx, rx) = oneshot::channel();

    let job = Job::Read {
        file_path,
        from_si,
        cb,
        share_key,
        max_bytes,
        responder: tx,
    };

    workers[index]
        .send(job)
        .map_err(|_| JobError::Other("Worker thread channel closed".to_string()))?;

    rx.await
        .map_err(|_| JobError::Other("Worker thread dropped responder".to_string()))?
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
