use event_storage::{catchup_result::CatchupResult, event_batch_item::EventBatchItem, event_item::EventItem};
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError};
use tokio::sync::oneshot;

pub enum Job {
    Share {
        file_path: String,
        cb: String,
        share_hash: String,
        access_level: AccessLevel,
        is_single_use: bool,
        iv: Option<String>,
        description: Option<String>,
        expires_on: Option<i64>,
        responder: oneshot::Sender<Result<EventItem, JobError>>,
    },
    Write {
        file_path: String,
        allow_create: bool,
        share_key: Option<String>,
        event_batch_item: EventBatchItem,
        responder: oneshot::Sender<Result<u64, JobError>>,
    },
    Read {
        file_path: String,
        from_si: u64,
        cb: String,
        share_key: Option<String>,
        max_bytes: usize,
        responder: oneshot::Sender<Result<CatchupResult, JobError>>,
    },
    Shutdown {
        responder: oneshot::Sender<()>,
    },
}
