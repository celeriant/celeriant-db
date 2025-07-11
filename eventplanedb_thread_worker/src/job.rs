use eventplanedb_storage::{catchup_result::CatchupResult, event_batch_item::EventBatchItem, event_item::EventItem};
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError};
use tokio::sync::oneshot;

use crate::process_write::WriteResult;

pub enum Job {
    Share {
        file_path: String,
        current_user_hash: String,
        server_time: u64,
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
        current_user_hash: String, 
        server_time: u64, 
        allow_create: bool,
        events: Vec<EventItem>,
        responder: oneshot::Sender<Result<WriteResult, JobError>>,
    },
    AccessCheck {
        file_path: String,
        current_user_hash: String,
        server_time: u64,
        required_access_level: AccessLevel,
        responder: oneshot::Sender<Result<(), JobError>>,
    },
    Read {
        file_path: String,
        from_si: u64,
        current_user_hash: String,
        server_time: u64,
        share_key: Option<String>,
        max_bytes: usize,
        own_events: bool,
        responder: oneshot::Sender<Result<CatchupResult, JobError>>,
    },
    DisableUser {
        file_path: String,
        current_user_hash: String,
        server_time: u64,
        user_hash: String,
        responder: oneshot::Sender<Result<WriteResult, JobError>>,
    },
    DisableShare {
        file_path: String,
        current_user_hash: String,
        server_time: u64,
        share_hash: String,
        responder: oneshot::Sender<Result<WriteResult, JobError>>,
    },
    Delete {
        file_path: String,
        current_user_hash: String,
        server_time: u64,
        responder: oneshot::Sender<Result<(), JobError>>,
    },
    Shutdown {
        responder: oneshot::Sender<()>,
    },
}
