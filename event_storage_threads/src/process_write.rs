use event_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache};

pub fn handle_write_job(
    file_path: String,
    allow_create: bool,
    event_batch_item: EventBatchItem,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
) -> Result<u64, JobError> {

    AccessLevel::require_permission(
        event_storage_cache,
        share_links_cache,
        user_access_cache,
        &file_path,
        &event_batch_item.cb.as_ref().unwrap(),
        AccessLevel::Contributor,
        None,
    )?;

    let si: u64 = event_storage_cache.write(&file_path, allow_create, event_batch_item)?;

    Ok(si)
}