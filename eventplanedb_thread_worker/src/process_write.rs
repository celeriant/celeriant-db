use eventplanedb_access::{
    access_level::AccessLevel, job_error::JobError, require_permission::require_permission, share_links_cache::ShareLinksCache,
    user_access_cache::UserAccessCache,
};
use eventplanedb_storage::event_storage_cache::EventStorageCache;
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem};

use crate::{event_notifications::EventNotifier, job_context::JobContext};

pub struct WriteResult {
    pub server_id: u64,
    pub events: Vec<EventBatchItem>,
}

pub fn handle_write_job(
    context: JobContext,
    allow_create: bool,
    events: Vec<EventItem>,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<WriteResult, JobError> {
    let file_exists = event_storage_cache.exists(&context.file_path);

    if !file_exists && !allow_create {
        return Err(JobError::NotFound("Aggregate does not exist".to_string()));
    }

    if file_exists {
        require_permission(
            event_storage_cache,
            share_links_cache,
            user_access_cache,
            &context.file_path,
            &context.current_client_id,
            context.current_user_id.as_deref(),
            context.current_org_id.as_deref(),
            context.server_time,
            AccessLevel::Contributor,
            None,
        )?;
    }

    let mut event_batch_item = EventBatchItem::new();
    event_batch_item.events = events;
    event_batch_item.client_id = context.current_client_id;
    event_batch_item.user_id = context.current_user_id.clone();
    event_batch_item.server_date = context.server_time;

    let server_id: u64 = event_storage_cache.write(&context.file_path, allow_create, event_batch_item)?;

    let mut events: Vec<EventBatchItem> = vec![];

    if !file_exists {
        // Give owner access
        events.extend(user_access_cache.update_access_for_user(
            event_storage_cache,
            &context.file_path,
            &context.current_client_id,
            context.current_user_id.as_deref(),
            Some(&context.current_client_id),
            context.current_user_id.as_deref(),
            context.current_org_id.as_deref(),
            AccessLevel::Owner,
            false,
            None,
            context.server_time,
        )?);
    }

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&context.file_path, &context.current_client_id);
    }

    Ok(WriteResult { server_id, events })
}
