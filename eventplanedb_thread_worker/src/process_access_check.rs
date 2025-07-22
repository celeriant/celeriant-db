use eventplanedb_storage::event_storage_cache::EventStorageCache;
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError, require_permission::require_permission, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache};

use crate::job_context::JobContext;

pub fn handle_access_check(
    context: JobContext,
    required_access_level: AccessLevel,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
) -> Result<(), JobError> {

    require_permission(
        event_storage_cache,
        share_links_cache,
        user_access_cache,
        &context.file_path,
        &context.current_client_id,
        context.current_user_id.as_deref(),
        context.server_time,
        required_access_level,
        None,
    )?;

    Ok(())
}
