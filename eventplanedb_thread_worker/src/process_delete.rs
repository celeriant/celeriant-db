use eventplanedb_storage::{event_storage_cache::EventStorageCache};
use eventplanedb_access::{
    access_level::AccessLevel, claims::Claims, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache
};

use crate::{event_notifications::EventNotifier};

pub fn handle_delete_job(
    file_path: String,
    current_user_hash: Option<String>,
    current_user_claims: Option<Claims>,
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
        current_user_hash.as_deref(),
        current_user_claims.as_ref().map(|c| c.sub.as_str()),
        server_time,
        AccessLevel::Owner,
        None,
    )?;

    event_storage_cache.delete(&file_path)?;

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        let user_id = current_user_claims.as_ref().map(|c| c.sub.clone()).unwrap_or(current_user_hash.unwrap());
        notifier.notify(&file_path, user_id.as_str());
    }

    Ok(())
}
