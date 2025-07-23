use eventplanedb_access::{
    access_level::AccessLevel, job_error::JobError, require_permission::require_permission, share_links_cache::ShareLinksCache,
    user_access_cache::UserAccessCache,
};
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};

use crate::{event_notifications::EventNotifier, job_context::JobContext, process_write::WriteResult};

pub fn handle_disable_user_job(
    context: JobContext,
    for_client_id: Option<u128>,
    for_user_id: Option<String>,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<WriteResult, JobError> {
    require_permission(
        event_storage_cache,
        share_links_cache,
        user_access_cache,
        &context.file_path,
        &context.current_client_id,
        context.current_user_id.as_deref(),
        context.server_time,
        AccessLevel::Owner,
        None,
    )?;

    if for_client_id.is_none() && for_user_id.is_none() {
        return Err(JobError::NotFound(format!(
            "Unable to disable user access. At least a client id or a user id must be specified"
        )));
    }

    let event_batch = user_access_cache.update_access_for_user(
        event_storage_cache,
        &context.file_path,
        &context.current_client_id,
        context.current_user_id.as_deref(),
        for_client_id.as_ref(), //Edge case where user wants to disable user instead of client
        for_user_id.as_deref(),
        AccessLevel::None,
        true,
        None,
        context.server_time,
    )?;

    if event_batch.is_none() {
        return Err(JobError::NotFound(format!("Unable to disable user access")));
    }

    let server_id = event_batch.as_ref().unwrap().server_id;

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&context.file_path, &context.current_client_id);
    }

    let events: Vec<EventBatchItem> = vec![event_batch.unwrap()];

    Ok(WriteResult { server_id, events })
}
