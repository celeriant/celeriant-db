use eventplanedb_storage::{event_storage_cache::EventStorageCache};
use eventplanedb_access::{
    access_level::AccessLevel, job_error::JobError, require_permission::require_permission, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache
};

use crate::{event_notifications::EventNotifier, job_context::JobContext};

pub fn handle_delete_job(
    context: JobContext,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<(), JobError> {

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

    event_storage_cache.delete(&context.file_path)?;

    //TODO: Now that the file has been deleted, we should clear any in-memory caches

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&context.file_path, &context.current_client_id);
    }

    Ok(())
    
}
