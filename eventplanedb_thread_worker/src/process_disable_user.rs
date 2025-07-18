use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};
use eventplanedb_access::{access_level::AccessLevel, claims::Claims, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache};

use crate::{event_notifications::EventNotifier, process_write::WriteResult};

pub fn handle_disable_user_job(
    file_path: String,
    current_user_hash: String,
    current_user_claims: Option<Claims>,
    server_time: u64,
    for_user_hash: String,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<WriteResult, JobError> {
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

    let event_batch = user_access_cache.update_access_for_user(
        event_storage_cache,
        &file_path,
        &current_user_hash,
        &for_user_hash,
        AccessLevel::None,
        true,
        None,
        Some(server_time),
    )?;

    if event_batch.is_none() {
        return Err(JobError::NotFound(format!("Unable to disable user access for {}", for_user_hash)));
    }

    let si = event_batch.as_ref().unwrap().si;

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&file_path, &current_user_hash);
    }

    let events: Vec<EventBatchItem> = vec![event_batch.unwrap()];

    Ok(WriteResult { si, events })
}
