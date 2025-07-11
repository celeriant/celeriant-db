use eventplanedb_storage::{event_storage_cache::EventStorageCache};
use eventplanedb_access::{
    access_level::AccessLevel, job_error::JobError, share_links_cache::ShareLinksCache,
    user_access_cache::UserAccessCache,
};

use crate::{event_notifications::EventNotifier};

pub fn handle_delete_job(
    file_path: String,
    current_user_hash: String,
    server_time: u64,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<(), JobError> {
    AccessLevel::require_permission(
        event_storage_cache,
        share_links_cache,
        user_access_cache,
        &file_path,
        &current_user_hash,
        server_time,
        AccessLevel::Owner,
        None,
    )?;

    event_storage_cache.delete(&file_path)?;

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&file_path, &current_user_hash);
    }

    Ok(())
}
