use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError, require_permission::require_permission, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache};

use crate::{event_notifications::EventNotifier, job_context::JobContext};

pub fn handle_share_job(
    context: JobContext,
    share_id: u128,
    access_level: AccessLevel,
    is_single_use: bool,
    iv: Option<[u8; 12]>,
    description: Option<String>,
    expires_on: u64,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<EventBatchItem, JobError> {

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

    let create_share_link_result = share_links_cache.create_share_link(
        event_storage_cache,
        &context.file_path,
        &context.current_client_id,
        context.current_user_id.as_deref(),
        share_id,
        access_level,
        is_single_use,
        iv,
        description,
        expires_on,
        context.server_time,
    )?;

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&context.file_path, &context.current_client_id);
    }

    Ok(create_share_link_result)
    
}
