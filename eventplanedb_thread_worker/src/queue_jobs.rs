use crossbeam::channel::Sender;
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError};
use eventplanedb_storage::{catchup_result::CatchupResult, event_batch_item::EventBatchItem, event_item::EventItem};
use tokio::sync::oneshot;

use crate::{job::Job, process_write::WriteResult, thread_assigner::hash_string_to_index};

async fn send_job<T>(workers: &[Sender<Job>], file_path: String, job_creator: impl FnOnce(oneshot::Sender<T>) -> Job) -> Result<T, JobError> {
    let index = hash_string_to_index(&file_path, workers.len());
    let (tx, rx) = oneshot::channel();

    let job = job_creator(tx);

    workers[index]
        .send(job)
        .map_err(|_| JobError::Other("Worker thread channel closed".to_string()))?;

    rx.await.map_err(|_| JobError::Other("Worker thread dropped responder".to_string()))
}

pub async fn write_async(workers: &[Sender<Job>], file_path: String, current_user_hash: String, server_time: u64, allow_create: bool, events: Vec<EventItem>) -> Result<WriteResult, JobError> {
    send_job(workers, file_path.clone(), |responder| Job::Write {
        file_path,
        current_user_hash,
        server_time,
        allow_create,
        events,
        responder,
    })
    .await?
}

pub async fn delete_async(
    workers: &[Sender<Job>],
    file_path: String,
    current_user_hash: String,
    server_time: u64,
) -> Result<(), JobError> {
    send_job(workers, file_path.clone(), |responder| Job::Delete {
        file_path,
        current_user_hash,
        server_time,
        responder,
    })
    .await?
}

pub async fn disable_share_async(
    workers: &[Sender<Job>],
    file_path: String,
    current_user_hash: String,
    server_time: u64,
    share_hash: String,
) -> Result<WriteResult, JobError> {
    send_job(workers, file_path.clone(), |responder| Job::DisableShare {
        file_path,
        current_user_hash,
        server_time,
        share_hash,
        responder,
    })
    .await?
}

pub async fn disable_user_async(workers: &[Sender<Job>], file_path: String, current_user_hash: String, server_time: u64, user_hash: String) -> Result<WriteResult, JobError> {
    send_job(workers, file_path.clone(), |responder| Job::DisableUser {
        file_path,
        current_user_hash,
        server_time,
        user_hash,
        responder,
    })
    .await?
}

pub async fn share_async(
    workers: &[Sender<Job>],
    file_path: String,
    current_user_hash: String,
    server_time: u64,
    share_hash: String,
    access_level: AccessLevel,
    is_single_use: bool,
    iv: Option<Vec<u8>>,
    description: Option<String>,
    expires_on: u64,
) -> Result<EventBatchItem, JobError> {
    send_job(workers, file_path.clone(), |responder| Job::Share {
        file_path,
        current_user_hash,
        server_time,
        share_hash,
        access_level,
        is_single_use,
        iv,
        description,
        expires_on,
        responder,
    })
    .await?
}

pub async fn access_check_async(
    workers: &[Sender<Job>],
    file_path: String,
    current_user_hash: String,
    server_time: u64,
    required_access_level: AccessLevel,
) -> Result<(), JobError> {
    send_job(workers, file_path.clone(), |responder| Job::AccessCheck {
        file_path,
        current_user_hash,
        server_time,
        required_access_level,
        responder,
    })
    .await?
}

pub async fn read_async(
    workers: &[Sender<Job>],
    file_path: String,
    current_user_hash: String,
    server_time: u64,
    share_key: Option<String>,
    from_si: u64,
    max_bytes: usize,
    own_events: bool,
) -> Result<CatchupResult, JobError> {
    send_job(workers, file_path.clone(), |responder| Job::Read {
        file_path,
        from_si,
        current_user_hash,
        server_time,
        share_key,
        max_bytes,
        own_events,
        responder,
    })
    .await?
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
