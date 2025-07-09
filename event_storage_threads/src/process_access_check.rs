use event_storage::event_storage_cache::EventStorageCache;
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache};

pub fn handle_access_check(
    file_path: String,
    current_user_hash: String,
    required_access_level: AccessLevel,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
) -> Result<(), JobError> {
    AccessLevel::require_permission(
        event_storage_cache,
        share_links_cache,
        user_access_cache,
        &file_path,
        &current_user_hash,
        required_access_level,
        None,
    )?;

    Ok(())
}
