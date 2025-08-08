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
    client_last_server_id: Option<u64>,
    events: Vec<EventItem>,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<WriteResult, JobError> {
    let file_exists = event_storage_cache.exists(&context.file_path);
    let current_server_id = event_storage_cache.get_last_si(&context.file_path)?;

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

        if let Some(client_last_server_id) = client_last_server_id {
            if current_server_id.is_some() && current_server_id.unwrap() != client_last_server_id {
                return Err(JobError::Conflict("Optimistic concurrency violation.".to_string()));
            }
        }
    }

    let mut event_batch_item = EventBatchItem::new();
    event_batch_item.events = events;
    event_batch_item.client_id = context.current_client_id;
    event_batch_item.user_id = context.current_user_id.clone();
    event_batch_item.server_date = context.server_time;

    let mut server_id: u64 = event_storage_cache.write(&context.file_path, allow_create, false, event_batch_item)?;

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

        // Keep client up to date with the server ID
        server_id += 1;
    }

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&context.file_path, &context.current_client_id);
    }

    Ok(WriteResult { server_id, events })
}
