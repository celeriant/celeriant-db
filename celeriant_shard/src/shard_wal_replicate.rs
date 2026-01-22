use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use celeriant_memcache::cache_path::CachePath;
use celeriant_memcache::pending_commit_data::PendingCommitData;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use celeriant_rotating_log::log_segment_file::write_dual_shard_log_header;
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wal::constants::{FIRST_EVENT_BATCH_INDEX, HEADER_BLOCK_SIZE_BYTES};
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_watch::aggregate_watchers::AggregateWatchers;

use crate::amortisation::coordinator::{CaptureResult, Coordinator};
use crate::error::replication_error::ReplicationError;
use crate::error::rollback_error::RollbackError;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::replication_client::ReplicationClient;
use crate::watch_event_collector::WatchEventCollector;

/// Captured data from the replication snapshot phase.
pub(crate) struct ReplicationCapturedData {
    pub use_s3: bool,
    pub replication_snapshot: Vec<PendingCommitData>,
}

/// Capture phase: take replication snapshot from memcache.
/// Must be called while the coordinator still holds the orchestrator event.
pub(crate) fn capture_replication_snapshot(shard_mem_cache: &Rc<RefCell<ShardMemCache>>) -> CaptureResult<ReplicationCapturedData, ReplicationError> {
    let mut cache = shard_mem_cache.borrow_mut();

    let use_s3 = cache.is_replication_queue_pressured();
    let replication_snapshot = cache.take_pending_replication();

    if cache.take_replication_rollback_flag() && replication_snapshot.is_empty() {
        return CaptureResult::Failed(ReplicationError::RollbackInProgress);
    }

    if replication_snapshot.is_empty() {
        return CaptureResult::NoCaptureRaceButOk;
    }

    CaptureResult::Captured(ReplicationCapturedData {
        use_s3,
        replication_snapshot,
    })
}

/// Commit phase: replicate captured data and update caches.
pub(crate) async fn commit_replication_with_rollback<R: ReplicationClient>(
    replication_client: Rc<R>,
    fsync_coordinator: Rc<Coordinator<ShardFsyncError>>,
    log_segments_cache: Rc<LogSegmentsCache>,
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
    captured: ReplicationCapturedData,
) -> Result<(), ReplicationError> {
    match replicate(&*replication_client, &captured.replication_snapshot, captured.use_s3).await {
        Ok(()) => {
            commit_replication(log_segments_cache, shard_mem_cache, watched_aggregates, captured.replication_snapshot);
            Ok(())
        }
        Err(replication_err) => {
            match rollback_replicate(&fsync_coordinator, &log_segments_cache, &shard_mem_cache, captured.replication_snapshot).await {
                Ok(()) => Err(replication_err),
                Err(rollback_err) => Err(ReplicationError::RollbackFailed(rollback_err)),
            }
        }
    }
}

/// Commits successful replication by updating read path, recent write cache, and broadcasting events.
fn commit_replication(
    log_segments_cache: Rc<LogSegmentsCache>,
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
    replication_snapshot: Vec<PendingCommitData>,
) {
    let mut event_collector = WatchEventCollector::new();

    for commit_data in replication_snapshot {
        // Advance read position (visible) if log segment is cached
        if let Some(log_segment) = log_segments_cache.get_if_cached(commit_data.log_id()) {
            let mut metadata = log_segment.metadata.borrow_mut();
            metadata.read = Some(commit_data.log_metadata.write.clone());
        }

        let mut shard_mem_cache = shard_mem_cache.borrow_mut();

        let log_id = commit_data.log_id();
        for item in commit_data.pending_queue {
            match &item.metablock.wal_metablock_type {
                MetablockKind::EventBatchMetadata(event_batch) => {

                    shard_mem_cache.commit_read_position_snapshot(&event_batch, log_id, item.metablock_absolute_pos);

                    event_collector.add_write_event(event_batch);

                    if event_batch.event_batch_index == FIRST_EVENT_BATCH_INDEX {
                        event_collector.add_create_event(event_batch.aggregate_key.clone());
                    }

                    let size_bytes = item.size_bytes();
                    shard_mem_cache.cache_recent_write(
                        event_batch.aggregate_key.clone(),
                        event_batch.event_batch_index,
                        item.metablock,
                        item.datablock,
                        size_bytes,
                    );
                }
                MetablockKind::SoftTrim(soft_trim) => {
                    shard_mem_cache.update_aggregate_min_event_batch_index(&soft_trim.aggregate_key, soft_trim.keep_from_event_batch_index, CachePath::Read);
                    shard_mem_cache.update_aggregate_min_event_batch_index(
                        &soft_trim.aggregate_key,
                        soft_trim.keep_from_event_batch_index,
                        CachePath::Read,
                    );
                    event_collector.add_trim_event(
                        soft_trim.aggregate_key.clone(),
                        soft_trim.keep_from_event_batch_index,
                    );
                }
                MetablockKind::SoftDelete(soft_delete) => {
                    shard_mem_cache.put_aggregate_into_cache_as_deleted(
                        soft_delete.aggregate_key.clone(),
                        soft_delete.event_index,
                        soft_delete.event_batch_index,
                        soft_delete.allow_recreate,
                        soft_delete.allow_index_continuation,
                        CachePath::Read,
                    );
                    event_collector.add_delete_event(soft_delete.aggregate_key.clone());
                }
                _ => {}
            }
        }
    }

    event_collector.broadcast_all(&watched_aggregates);
}

async fn rollback_replicate(
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<ShardMemCache>>,
    replication_snapshot: Vec<PendingCommitData>,
) -> Result<(), RollbackError> {
    let _fsync_gate = fsync_coordinator
        .acquire_rollback_lock()
        .await
        .ok_or_else(|| RollbackError::HeaderFsyncFailed("failed to acquire fsync gate".to_string()))?;

    let log_ids: HashSet<u64> = replication_snapshot.iter().map(|c| c.log_id()).collect();

    for log_id in log_ids {
        if let Some(log_segment) = log_segments_cache.get_if_cached(log_id) {
            let mut metadata = log_segment.metadata.borrow_mut();
            if let Some(read) = &metadata.read {
                metadata.write = read.clone();

                let dma_file_writer = log_segment
                    .lock_writer("rollback_replicate")
                    .await
                    .map_err(|e| RollbackError::HeaderFsyncFailed(format!("lock failed: {e:?}")))?;
                let dma_file_writer = dma_file_writer
                    .as_ref()
                    .ok_or_else(|| RollbackError::HeaderFsyncFailed("no file handle".to_string()))?;

                let header_end_start_pos = metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
                let header = metadata.to_shard_log_header();
                write_dual_shard_log_header(dma_file_writer, header_end_start_pos, &header)
                    .await
                    .map_err(|e| RollbackError::HeaderFsyncFailed(format!("header write failed: {e:?}")))?;
                dma_file_writer
                    .fdatasync()
                    .await
                    .map_err(|e| RollbackError::HeaderFsyncFailed(format!("fsync failed: {e:?}")))?;
            }
        }
    }

    shard_mem_cache.borrow_mut().execute_replication_rollback();
    Ok(())
}

async fn replicate<R: ReplicationClient>(
    client: &R,
    batches: &[PendingCommitData],
    use_s3: bool,
) -> Result<(), ReplicationError> {
    if use_s3 {
        return client.replicate_to_s3(batches).await;
    }

    // Try follower first, fall back to S3 on failure
    match client.replicate_to_follower(batches).await {
        Ok(()) => Ok(()),
        Err(_) => client.replicate_to_s3(batches).await,
    }
}
