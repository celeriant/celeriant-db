use event_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache};

pub struct WriteResult {
    pub si: u64,
    pub events: Vec<EventBatchItem>,
}

pub fn handle_write_job(
    file_path: String,
    allow_create: bool,
    event_batch_item: EventBatchItem,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
) -> Result<WriteResult, JobError> {

    let file_exists = event_storage_cache.exists(&file_path);

    if !file_exists && !allow_create {
        return Err(JobError::NotFound("Project does not exist".to_string()));
    }
    
    let current_user_hash = event_batch_item.cb.as_ref().unwrap().clone();

    if file_exists {
        AccessLevel::require_permission(
            event_storage_cache,
            share_links_cache,
            user_access_cache,
            &file_path,
            &current_user_hash,
            AccessLevel::Contributor,
            None,
        )?;
    }
    let ed_override = Some(event_batch_item.sd);
    let si: u64 = event_storage_cache.write(&file_path, allow_create, event_batch_item)?;

    let mut events: Vec<EventBatchItem> = vec![];

    if !file_exists {
        // Give owner access
        events.extend(user_access_cache.update_access_for_user(
            event_storage_cache, 
            &file_path, 
            &current_user_hash,
            &current_user_hash, 
            AccessLevel::Owner,
            false,
            None,
            ed_override)?);
    }

    Ok(WriteResult {
        si,
        events,
    })
}