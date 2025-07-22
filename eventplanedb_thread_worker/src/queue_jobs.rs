use crossbeam::channel::Sender;
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError};
use eventplanedb_storage::{catchup_result::CatchupResult, event_batch_item::EventBatchItem, event_item::EventItem};
use tokio::sync::oneshot;

use crate::{job::Job, job_context::JobContext, process_write::WriteResult, thread_assigner::hash_string_to_index};

async fn send_job<T>(workers: &[Sender<Job>], file_path: String, job_creator: impl FnOnce(oneshot::Sender<T>) -> Job) -> Result<T, JobError> {
    let index = hash_string_to_index(&file_path, workers.len());
    let (tx, rx) = oneshot::channel();

    let job = job_creator(tx);

    workers[index]
        .send(job)
        .map_err(|_| JobError::Other("Worker thread channel closed".to_string()))?;

    rx.await.map_err(|_| JobError::Other("Worker thread dropped responder".to_string()))
}

pub async fn write_async(
    workers: &[Sender<Job>], 
    context: JobContext,
    allow_create: bool, 
    events: Vec<EventItem>
) -> Result<WriteResult, JobError> {
    send_job(workers, context.file_path.clone(), |responder| Job::Write {
        context,
        allow_create,
        events,
        responder,
    })
    .await?
}

pub async fn delete_async(
    workers: &[Sender<Job>],
    context: JobContext,
) -> Result<(), JobError> {
    send_job(workers, context.file_path.clone(), |responder| Job::Delete {
        context,
        responder,
    })
    .await?
}

pub async fn disable_share_async(
    workers: &[Sender<Job>],
    context: JobContext,
    share_id: u128,
) -> Result<WriteResult, JobError> {
    send_job(workers, context.file_path.clone(), |responder| Job::DisableShare {
        context,
        share_id,
        responder,
    })
    .await?
}

pub async fn disable_user_async(
    workers: &[Sender<Job>], 
    context: JobContext,
    for_client_id: Option<u128>,
    for_user_id: Option<String>
) -> Result<WriteResult, JobError> {
    send_job(workers, context.file_path.clone(), |responder| Job::DisableUser {
        context,
        for_client_id,
        for_user_id,
        responder,
    })
    .await?
}

pub async fn share_async(
    workers: &[Sender<Job>],
    context: JobContext,
    share_id: u128,
    access_level: AccessLevel,
    is_single_use: bool,
    iv: Option<[u8; 12]>,
    description: Option<String>,
    expires_on: u64,
) -> Result<EventBatchItem, JobError> {
    send_job(workers, context.file_path.clone(), |responder| Job::Share {
        context,
        share_id,
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
    context: JobContext,
    required_access_level: AccessLevel,
) -> Result<(), JobError> {
    send_job(workers, context.file_path.clone(), |responder| Job::AccessCheck {
        context,
        required_access_level,
        responder,
    })
    .await?
}

pub async fn read_async(
    workers: &[Sender<Job>],
    context: JobContext,
    share_id: Option<u128>,
    from_server_id: u64,
    max_bytes: usize,
    include_own_events: bool,
) -> Result<CatchupResult, JobError> {
    send_job(workers, context.file_path.clone(), |responder| Job::Read {
        context,
        from_server_id,
        share_id,
        max_bytes,
        include_own_events,
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
