use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};
use eventplanedb_access::{access_level::AccessLevel, claims::Claims, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache};

use crate::event_notifications::EventNotifier;

pub fn handle_share_job(
    file_path: String,
    current_user_hash: String,
    current_user_claims: Option<Claims>,
    server_time: u64,
    share_hash: String,
    access_level: AccessLevel,
    is_single_use: bool,
    iv: Option<Vec<u8>>,
    description: Option<String>,
    expires_on: u64,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<EventBatchItem, JobError> {
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

    let create_share_link_result = share_links_cache.create_share_link(
        event_storage_cache,
        file_path.clone(),
        current_user_hash.clone(),
        share_hash.clone(),
        access_level,
        is_single_use,
        iv,
        description,
        expires_on,
    )?;

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&file_path, &current_user_hash);
    }

    Ok(create_share_link_result)
}
