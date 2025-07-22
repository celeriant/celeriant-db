use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError, require_permission::require_permission, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache};

use crate::{event_notifications::EventNotifier, job_context::JobContext, process_write::WriteResult};

pub fn handle_disable_share_job(
    context: JobContext,
    share_id: u128,
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

    let event_batch = share_links_cache.disable_share_link(
        event_storage_cache, 
        &context.file_path, 
        &context.current_client_id,
        context.current_user_id.as_deref(),
        share_id,
        context.server_time)?;

    let server_id = event_batch.server_id;

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&context.file_path, &context.current_client_id);
    }

    let events: Vec<EventBatchItem> = vec![event_batch];

    Ok(WriteResult { server_id, events })
    
}
