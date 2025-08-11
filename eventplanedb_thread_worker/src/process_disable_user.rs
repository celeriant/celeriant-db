use eventplanedb_access::{
    access_level::AccessLevel, job_error::JobError, require_permission::require_permission, share_links_cache::ShareLinksCache,
    special_aggregates::SpecialAggregates, user_access_cache::UserAccessCache,
};
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};

use crate::{event_notifications::EventNotifier, job_context::JobContext};

pub struct DisableResult {
    pub server_id: u64,
    pub events: Vec<EventBatchItem>,
    pub special_aggregates: SpecialAggregates,
    pub for_user_special_aggregates: SpecialAggregates,
}

pub fn handle_disable_user_job(
    context: JobContext,
    for_client_id: Option<u128>,
    for_user_id: Option<String>,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<DisableResult, JobError> {
    if for_client_id.is_none() && for_user_id.is_none() {
        return Err(JobError::NotFound(format!(
            "Unable to disable user access. At least a client id or a user id must be specified"
        )));
    }

    let mut special_aggregates = SpecialAggregates::new(Some(context.current_client_id), context.current_user_id.clone(), context.current_org_id.clone());
    let mut events = require_permission(
        event_storage_cache,
        share_links_cache,
        user_access_cache,
        &context.aggregate_id,
        &context.file_path,
        &context.current_client_id,
        context.current_user_id.as_deref(),
        context.current_org_id.as_deref(),
        context.server_time,
        AccessLevel::Owner,
        None,
        &mut special_aggregates,
    )?;

    let event_batch = user_access_cache.update_access_for_user(
        event_storage_cache,
        &context.file_path,
        &context.current_client_id,
        context.current_user_id.as_deref(),
        for_client_id.as_ref(),
        for_user_id.as_deref(),
        None,
        AccessLevel::None,
        true,
        None,
        context.server_time,
    )?;

    if event_batch.is_none() {
        return Err(JobError::NotFound(format!("User not found or is already disabled")));
    }

    // Access has now been removed for the specified client or user.
    let mut for_user_special_aggregates = SpecialAggregates::new(for_client_id, for_user_id, None);
    for_user_special_aggregates.permission_updated_on_aggregate(&context.aggregate_id, AccessLevel::None, None, context.server_time);

    let server_id = event_batch.as_ref().unwrap().server_id;

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&context.aggregate_id, &context.current_client_id);
    }

    events.push(event_batch.unwrap());

    Ok(DisableResult {
        server_id,
        events,
        special_aggregates,
        for_user_special_aggregates,
    })
}
