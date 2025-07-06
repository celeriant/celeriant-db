use event_storage::{event_item::EventItem, event_storage_cache::EventStorageCache};
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache};

pub fn handle_share_job(
    file_path: String,
    cb: String,
    share_hash: String,
    access_level: AccessLevel,
    is_single_use: bool,
    iv: Option<Vec<u8>>,
    description: Option<String>,
    expires_on: u64,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
) -> Result<EventItem, JobError> {

    AccessLevel::require_permission(
        event_storage_cache,
        share_links_cache,
        user_access_cache,
        &file_path,
        &cb,
        AccessLevel::Owner,
        None,
    )?;

    let create_share_link_result = share_links_cache.create_share_link(
        event_storage_cache,
        file_path.clone(),
        cb.clone(),
        share_hash.clone(),
        access_level,
        is_single_use,
        iv,
        description,
        expires_on,
    )?;

    Ok(create_share_link_result)
}