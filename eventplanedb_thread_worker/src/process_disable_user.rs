use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};
use eventplanedb_access::{access_level::AccessLevel, claims::Claims, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::{UserAccessCache, UserIdType}};

use crate::{event_notifications::EventNotifier, process_write::WriteResult};

pub fn handle_disable_user_job(
    file_path: String,
    current_user_hash: Option<String>,
    current_user_claims: Option<Claims>,
    server_time: u64,
    for_user_id: String,
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
        current_user_hash.as_deref(),
        current_user_claims.as_ref().map(|c| c.sub.as_str()),
        server_time,
        AccessLevel::Owner,
        None,
    )?;

    //Critical that we preference the machine public key here as the same user could be logged in on multiple devices
    let mut user_id_type = UserIdType::OAuth2; //TODO: Not technically correct as is based on current user, not user we are disabling
    if current_user_hash.is_some() {
        user_id_type = UserIdType::ZeroTrust;
    }
    let user_id = current_user_hash.unwrap_or(current_user_claims.unwrap().sub);

    let event_batch = user_access_cache.update_access_for_user(
        event_storage_cache,
        &file_path,
        &user_id,
        &for_user_id,
        AccessLevel::None,
        true,
        None,
        server_time,
        user_id_type
    )?;

    if event_batch.is_none() {
        return Err(JobError::NotFound(format!("Unable to disable user access for {}", user_id)));
    }

    let si = event_batch.as_ref().unwrap().si;

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&file_path, &user_id);
    }

    let events: Vec<EventBatchItem> = vec![event_batch.unwrap()];

    Ok(WriteResult { si, events })
}
