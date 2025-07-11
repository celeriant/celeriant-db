use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache};

use crate::{event_notifications::EventNotifier, process_write::WriteResult};

pub fn handle_disable_share_job(
    file_path: String,
    current_user_hash: String,
    server_time: u64,
    share_hash: String,
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
        AccessLevel::Owner,
        None,
    )?;

    let event_batch = share_links_cache.disable_share_link(event_storage_cache, &file_path, current_user_hash.clone(), share_hash, server_time)?;

    let si = event_batch.si;

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&file_path, &current_user_hash);
    }

    let events: Vec<EventBatchItem> = vec![event_batch];

    Ok(WriteResult { si, events })
}
