use eventplanedb_access::{
    access_level::AccessLevel, job_error::JobError, require_permission::require_permission, share_links_cache::ShareLinksCache,
    special_aggregates::SpecialAggregates, user_access_cache::UserAccessCache,
};
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};

use crate::job_context::JobContext;

pub struct AccessCheckResult {
    pub access_events: Vec<EventBatchItem>,
    pub special_aggregates: SpecialAggregates,
}

pub fn handle_access_check(
    context: JobContext,
    required_access_level: AccessLevel,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
) -> Result<AccessCheckResult, JobError> {
    let mut special_aggregates = SpecialAggregates::new(Some(context.current_client_id), context.current_user_id.clone(), context.current_org_id.clone());
    let access_events = require_permission(
        event_storage_cache,
        share_links_cache,
        user_access_cache,
        &context.aggregate_id,
        &context.file_path,
        &context.current_client_id,
        context.current_user_id.as_deref(),
        context.current_org_id.as_deref(),
        context.server_time,
        required_access_level,
        None,
        &mut special_aggregates,
    )?;

    Ok(AccessCheckResult {
        access_events,
        special_aggregates,
    })
}
