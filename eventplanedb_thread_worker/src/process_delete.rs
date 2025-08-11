use eventplanedb_access::{
    access_level::AccessLevel, job_error::JobError, require_permission::require_permission, share_links_cache::ShareLinksCache,
    special_aggregates::SpecialAggregates, user_access_cache::UserAccessCache,
};
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};

use crate::{event_notifications::EventNotifier, job_context::JobContext};

pub struct DeleteResult {
    pub events: Vec<EventBatchItem>,
    pub special_aggregates: Vec<SpecialAggregates>,
}
pub fn handle_delete_job(
    context: JobContext,
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    event_notifier: Option<&EventNotifier>,
) -> Result<DeleteResult, JobError> {
    let mut special_aggregates = SpecialAggregates::new(Some(context.current_client_id), context.current_user_id.clone(), context.current_org_id.clone());

    let events = require_permission(
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

    let mut special_aggregates: Vec<SpecialAggregates> = Vec::new();
    let (client_ids, user_ids) = user_access_cache.get_complete_access_list(&context.file_path);
    for client_id in client_ids {
        let mut special_aggregate = SpecialAggregates::new(Some(client_id), None, None);
        special_aggregate.client_removed_from_aggregate(&context.aggregate_id, context.server_time);
        special_aggregates.push(special_aggregate);
    }
    for user_id in user_ids {
        let mut special_aggregate = SpecialAggregates::new(None, Some(user_id), None);
        special_aggregate.user_removed_from_aggregate(&context.aggregate_id, context.server_time);
        special_aggregates.push(special_aggregate);
    }

    event_storage_cache.delete(&context.file_path)?;

    user_access_cache.clear_for_file_path(&context.file_path);
    share_links_cache.clear_for_file_path(&context.file_path);

    // Notify subscribers that there are new events for this file path
    if let Some(notifier) = event_notifier {
        notifier.notify(&context.aggregate_id, &context.current_client_id);
    }

    Ok(DeleteResult { events, special_aggregates })
}
