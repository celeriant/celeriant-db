use eventplanedb_access::{access_level::AccessLevel, job_error::JobError};
use eventplanedb_storage::{catchup_result::CatchupResult, event_item::EventItem};
use tokio::sync::oneshot;

use crate::{
    job_context::JobContext, process_access_check::AccessCheckResult, process_delete::DeleteResult, process_disable_share::DisableShareResult,
    process_disable_user::DisableResult, process_read::ReadResult, process_share::ShareResult, process_write::WriteResult,
};

pub enum Job {
    Share {
        context: JobContext,
        share_id: u128,
        access_level: AccessLevel,
        is_single_use: bool,
        iv: Option<[u8; 12]>,
        description: Option<String>,
        expires_on: u64,
        responder: oneshot::Sender<Result<ShareResult, JobError>>,
    },
    Write {
        context: JobContext,
        allow_create: bool,
        client_last_server_id: Option<u64>,
        events: Vec<EventItem>,
        responder: oneshot::Sender<Result<WriteResult, JobError>>,
    },
    AccessCheck {
        context: JobContext,
        required_access_level: AccessLevel,
        responder: oneshot::Sender<Result<AccessCheckResult, JobError>>,
    },
    Read {
        context: JobContext,
        from_server_id: u64,
        share_id: Option<u128>,
        max_bytes: usize,
        include_own_events: bool,
        responder: oneshot::Sender<Result<ReadResult, JobError>>,
    },
    DisableUser {
        context: JobContext,
        for_client_id: Option<u128>,
        for_user_id: Option<String>,
        responder: oneshot::Sender<Result<DisableResult, JobError>>,
    },
    DisableShare {
        context: JobContext,
        share_id: u128,
        responder: oneshot::Sender<Result<DisableShareResult, JobError>>,
    },
    Delete {
        context: JobContext,
        responder: oneshot::Sender<Result<DeleteResult, JobError>>,
    },
    Shutdown {
        responder: oneshot::Sender<()>,
    },
}
