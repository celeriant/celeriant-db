use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem, event_storage_cache::EventStorageCache};
use eventplanedb_access::{
    access_level::AccessLevel, job_error::JobError, project_event_type::TopicEventType, share_links_cache::ShareLinksCache,
    user_access_cache::UserAccessCache,
};

use crate::{event_notifications::EventNotifier, process_write::WriteResult};

pub fn handle_restore_job(
    file_path: String,
    current_user_hash: String,
    server_time: u64,
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

    let mut event_item = EventItem::new();
    event_item.ed = server_time;
    event_item.tp = TopicEventType::TopicRestored as u64;

    let mut event_batch_item = EventBatchItem::new();
    event_batch_item.events = vec![event_item.clone()];
    event_batch_item.cb = Some(current_user_hash.clone());
    event_batch_item.sd = server_time;

    let si: u64 = event_storage_cache.write(&file_path, false, event_batch_item.clone())?;

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&file_path, &current_user_hash);
    }

    Ok(WriteResult {
        si,
        events: vec![event_batch_item],
    })
}
