use eventplanedb_access::{
    access_level::AccessLevel, job_error::JobError, require_permission::require_permission, share_links_cache::ShareLinksCache,
    special_aggregates::SpecialAggregates, user_access_cache::UserAccessCache,
};
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};

use crate::{event_notifications::EventNotifier, job_context::JobContext};

pub struct DisableShareResult {
    pub server_id: u64,
    pub events: Vec<EventBatchItem>,
    pub special_aggregates: SpecialAggregates,
}

pub fn handle_disable_share_job(
    context: JobContext,
    share_id: u128,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<DisableShareResult, JobError> {
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

    let disable_share_link_result = share_links_cache.disable_share_link(
        event_storage_cache,
        &context.file_path,
        &context.current_client_id,
        context.current_user_id.as_deref(),
        share_id,
        context.server_time,
    )?;

    let server_id = disable_share_link_result.server_id;
    events.push(disable_share_link_result);

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&context.aggregate_id, &context.current_client_id);
    }

    Ok(DisableShareResult {
        server_id,
        events,
        special_aggregates,
    })
}
