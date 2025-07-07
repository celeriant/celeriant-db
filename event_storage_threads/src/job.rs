use event_storage::{catchup_result::CatchupResult, event_batch_item::EventBatchItem};
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError};
use tokio::sync::oneshot;

use crate::process_write::WriteResult;

pub enum Job {
    Share {
        file_path: String,
        cb: String,
        share_hash: String,
        access_level: AccessLevel,
        is_single_use: bool,
        iv: Option<Vec<u8>>,
        description: Option<String>,
        expires_on: u64,
        responder: oneshot::Sender<Result<EventBatchItem, JobError>>,
    },
    Write {
        file_path: String,
        allow_create: bool,
        event_batch_item: EventBatchItem,
        responder: oneshot::Sender<Result<WriteResult, JobError>>,
    },
    Read {
        file_path: String,
        from_si: u64,
        cb: String,
        share_key: Option<String>,
        max_bytes: usize,
        own_events: bool,
        responder: oneshot::Sender<Result<CatchupResult, JobError>>,
    },
    Shutdown {
        responder: oneshot::Sender<()>,
    },
}
