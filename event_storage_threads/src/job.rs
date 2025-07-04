use crate::job_error::JobError;
use event_storage::{catchup_result::CatchupResult, event_batch_item::EventBatchItem};
use tokio::sync::oneshot;

pub enum Job {
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
