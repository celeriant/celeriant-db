use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem, event_storage_cache::EventStorageCache};
use eventplanedb_access::{access_level::AccessLevel, claims::Claims, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::{UserAccessCache, UserIdType}};

use crate::event_notifications::EventNotifier;

pub struct WriteResult {
    pub si: u64,
    pub events: Vec<EventBatchItem>,
}

pub fn handle_write_job(
    file_path: String, 
    current_user_hash: Option<String>, 
    current_user_claims: Option<Claims>,
    server_time: u64, 
    allow_create: bool,
    events: Vec<EventItem>,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<WriteResult, JobError> {
    let file_exists = event_storage_cache.exists(&file_path);

    if !file_exists && !allow_create {
        return Err(JobError::NotFound("Aggregate does not exist".to_string()));
    }

    if file_exists {
        AccessLevel::require_permission(
            event_storage_cache,
            share_links_cache,
            user_access_cache,
            &file_path,
            current_user_hash.as_deref(),
            current_user_claims.as_ref().map(|c| c.sub.as_str()),
            server_time,
            AccessLevel::Contributor,
            None,
        )?;
    }

    let user_id = current_user_claims.as_ref().map(|c| c.sub.clone()).unwrap_or(current_user_hash.unwrap());
    let mut user_id_type = UserIdType::ZeroTrust;
    if current_user_claims.is_some() {
        user_id_type = UserIdType::OAuth2;
    }

    let mut event_batch_item = EventBatchItem::new();
    event_batch_item.events = events;
    event_batch_item.sd = server_time;
    event_batch_item.cb = Some(user_id.clone());

    let si: u64 = event_storage_cache.write(&file_path, allow_create, event_batch_item)?;

    let mut events: Vec<EventBatchItem> = vec![];

    if !file_exists {
        // Give owner access
        events.extend(user_access_cache.update_access_for_user(
            event_storage_cache,
            &file_path,
            &user_id,
            &user_id,
            AccessLevel::Owner,
            false,
            None,
            server_time,
            user_id_type,
        )?);
    }

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&file_path, &user_id);
    }

    Ok(WriteResult { si, events })
}
