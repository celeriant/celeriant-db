use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use tracing::{debug, error};

use celeriant_msg::request::requests::ReplicationBatchItem;
use celeriant_rotating_log::log_segment_file::aggregate_key_bloom::AggregateKeyBloom;
use celeriant_rotating_log::log_segment_file::log_segment_file::{read_datablocks_carry_over_bytes, write_dual_shard_log_header};
use celeriant_rotating_log::reverse_metablock_scanner::ReverseMetablockScanner;
use celeriant_wal::shard_log_header::ShardLogHeader;
use celeriant_wire::disk::disk_format_error::DiskFormatError;
use celeriant_wire::disk::metablock_bytes;
use celeriant_wire::disk::versioned_block::deserialise_metablock;
use celeriant_memcache::cache_path::CachePath;
use celeriant_memcache::pending_commit_data::PendingCommitData;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use crate::schema_validator::CompiledValidator;

type MemCache = ShardMemCache<CompiledValidator>;
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wal::constants::{FIRST_EVENT_BATCH_INDEX, FIXED_BLOCK_SIZE_BYTES, GENESIS_HASH, HEADER_BLOCK_SIZE_BYTES};
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_watch::aggregate_watchers::AggregateWatchers;

use crate::amortisation::coordinator::{CaptureResult, Coordinator};
use crate::collect_from_disk::{EventBatchFromLogSegmentFile, fetch_datablocks_for_metablocks};
use crate::error::fetch_catchup_entries_error::FetchCatchupEntriesError;
use crate::error::replication_error::ReplicationError;
use crate::error::replication_rollback_failure::ReplicationRollbackFailure;
use crate::error::replication_to_follower_error::ReplicateToFollowerError;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::replication_client::ReplicationClient;
use crate::watch_event_collector::WatchEventCollector;

/// Returns the number of items from the start that fit within `max_size_bytes`.
/// Always returns at least 1 to ensure progress even with oversized items.
fn batch_end_index(items: &[ReplicationBatchItem], max_size_bytes: u64) -> usize {
    let mut cumulative = 0u64;
    for (i, item) in items.iter().enumerate() {
        let size = item.size_bytes();
        if cumulative + size > max_size_bytes && i > 0 {
            return i;
        }
        cumulative += size;
    }
    items.len()
}

/// Captured data from the replication snapshot phase.
pub(crate) struct ReplicationCapturedData {
    pub follower_falling_behind_or_offline: bool,
    pub replication_snapshot: Vec<PendingCommitData>,
}

/// Capture phase: take replication snapshot from memcache.
/// Must be called while the coordinator still holds the orchestrator event.
pub(crate) fn capture_replication_snapshot(shard_mem_cache: &Rc<RefCell<MemCache>>) -> CaptureResult<ReplicationCapturedData, ReplicationError> {
    let mut cache = shard_mem_cache.borrow_mut();

    metrics::gauge!("celeriant_replication_queue_bytes").set(cache.pending_replication_bytes() as f64);
    let follower_falling_behind = cache.is_replication_queue_pressured();
    metrics::gauge!("celeriant_replication_follower_pressured").set(if follower_falling_behind { 1.0 } else { 0.0 });
    let replication_snapshot = cache.take_pending_replication();

    if cache.take_replication_rollback_flag() {
        return CaptureResult::Failed(ReplicationError::RollbackInProgress);
    }

    if replication_snapshot.is_empty() {
        return CaptureResult::NoCaptureRaceButOk;
    }

    CaptureResult::Captured(ReplicationCapturedData {
        follower_falling_behind_or_offline: follower_falling_behind,
        replication_snapshot,
    })
}

pub enum ReplicationDetails {
    ReplicatedToFollower,
    RepliatedToS3(ReplicateToFollowerError),
}

/// Commit phase: replicate captured data and update caches.
pub(crate) async fn commit_replication_with_rollback<R: ReplicationClient>(
    replication_client: Rc<R>,
    fsync_coordinator: Rc<Coordinator<ShardFsyncError>>,
    log_segments_cache: Rc<LogSegmentsCache>,
    shard_mem_cache: Rc<RefCell<MemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
    replication_captured_data: ReplicationCapturedData,
    max_catchup_gap_bytes: u64,
    max_request_size: u64,
    max_s3_fallback_batch_bytes: u64,
    read_max_chunk_size: u64,
    shard_id: u32,
) -> Result<(), ReplicationError> {
    let start = std::time::Instant::now();
    let shard_label = [("shard_id", shard_id.to_string())];

    // Because we are paginating data to the follower, we have a loop
    // At any point we might have to fallback to s3. Also the follower
    // can push back and require additional entries to be sent
    let mut follower_falling_behind_or_offline = replication_captured_data.follower_falling_behind_or_offline;
    let mut added_additional_entries = false;
    let mut kick_sent = false;

    // Final deets we send back to waiting callers
    let mut _replication_details = ReplicationDetails::ReplicatedToFollower;

    // Move replication batches into a flat list of items for replication
    let mut batches: Vec<ReplicationBatchItem> = replication_captured_data
        .replication_snapshot
        .iter()
        .flat_map(|batch| batch.pending_queue.iter())
        .map(|item| ReplicationBatchItem {
            metablock: item.metablock.clone(), //Don't consume captured data as its needed for commit
            datablock: item.datablock.clone(), //Maybe we could optimise this further later, serialisation technincally only needs a ref
        })
        .collect();
    let initial_batch_count = batches.len();

    // Loop until we run out of batches to replicate, or an unrecoverable error occurs.
    // We must split batches into batches that are less than max_request_size (bytes)
    // If batches at any point grows > max_catchup_gap_bytes (follower behind too far), we must fallback so S3

    let mut workset_size_bytes = batches.iter().map(|c| c.size_bytes()).sum::<u64>();
    while !batches.is_empty() {
        // If at any point the amount of data that we have to replicate to the
        // follower becomes too large,  we skip the follower and send the data to S3 instead.
        if workset_size_bytes > max_catchup_gap_bytes {
            follower_falling_behind_or_offline = true;
        }

        // Either the follower is too slow to keep up, or it has been offline for a while
        // and needs to catch up itself first, or the follower is completely offline
        if follower_falling_behind_or_offline {
            metrics::counter!("celeriant_replication_s3_fallbacks_total", &shard_label).increment(1);
            let end_idx = batch_end_index(&batches, max_s3_fallback_batch_bytes);
            match replication_client.replicate_to_s3(batches[..end_idx].to_vec()).await {
                Ok(()) => {
                    batches.drain(..end_idx);
                    // Kick after S3 write — follower's catchup will find data
                    if !kick_sent && batches.is_empty() {
                        let _ = replication_client.send_kick().await;
                        kick_sent = true;
                    }
                    continue;
                }
                Err(replication_err) => {
                    error!(shard_id, error = ?replication_err, "S3 fallback upload failed — triggering replication rollback");
                    return match rollback_replicate(
                        &fsync_coordinator,
                        &log_segments_cache,
                        &shard_mem_cache,
                        replication_captured_data.replication_snapshot,
                        shard_id,
                    )
                    .await
                    {
                        Ok(()) => Err(ReplicationError::ReplicateToS3Error(replication_err)),
                        Err(rollback_err) => {
                            error!(shard_id, error = ?rollback_err, "Replication rollback itself failed — node is in inconsistent state");
                            Err(ReplicationError::RollbackFailed(rollback_err))
                        }
                    };
                }
            }
        }

        // The happy low latency path - batched replication to follower
        let end_idx = batch_end_index(&batches, max_request_size);

        match replication_client.replicate_to_follower(batches[..end_idx].to_vec()).await {
            Ok(()) => {
                // Success means we can drain out the replicated entries and go again
                // No leader commit yet, wait until full replication of the written batches is done
                // This saves the complexity of doing a partial rollback, we just take read pos -> write pos which is easier
                batches.drain(..end_idx);
            }
            Err(replication_err) => {
                // Already got a rejection once, this is second or more. nothing leader can do here, just fallback to s3
                if added_additional_entries {
                    _replication_details = ReplicationDetails::RepliatedToS3(replication_err);
                    follower_falling_behind_or_offline = true;
                    continue;
                }

                match replication_err {
                    ReplicateToFollowerError::FollowerRejected(ref follower_rejection) => {
                        match follower_rejection {
                            celeriant_msg::response::responses::FollowerRejection::WalIndexMismatch { max_follower_wal_index } => {
                                //We need to provide older wal entries to follower as they are behind
                                //If we are unable to provide older wal entries, we need to fallback to S3
                                let fetch_catchup_entries_result = fetch_catchup_entries(
                                    &log_segments_cache,
                                    *max_follower_wal_index,
                                    batches[0].metablock.wal_index,
                                    max_catchup_gap_bytes,
                                    read_max_chunk_size,
                                )
                                .await;

                                let additional_entries_for_follower = match fetch_catchup_entries_result {
                                    Ok(entries) => entries,
                                    Err(e) => {
                                        match e {
                                            FetchCatchupEntriesError::FollowerTooFarBehind => {
                                                // We tried to get the event batches the follower needs
                                                // but they are waaay too far behind which would kill our
                                                // client write latency. So we fallback to S3 instead.
                                                _replication_details =
                                                    ReplicationDetails::RepliatedToS3(ReplicateToFollowerError::FollowerTooFarBehind);
                                                follower_falling_behind_or_offline = true;
                                                vec![]
                                            }
                                            _ => {
                                                // The rare scenario where our local disk has failed to read the entries
                                                // In this case we rollback the writes and notify all clients to stop using this node
                                                return match rollback_replicate(
                                                    &fsync_coordinator,
                                                    &log_segments_cache,
                                                    &shard_mem_cache,
                                                    replication_captured_data.replication_snapshot,
                                                    shard_id,
                                                )
                                                .await
                                                {
                                                    Ok(()) => Err(ReplicationError::ExtendedCatchupFailure(e)),
                                                    Err(rollback_err) => {
                                                        error!(shard_id, error = ?rollback_err, "Replication rollback itself failed — node is in inconsistent state");
                                                        Err(ReplicationError::RollbackFailed(rollback_err))
                                                    }
                                                };
                                            }
                                        }
                                    }
                                };

                                let additional_batch_items: Vec<ReplicationBatchItem> = additional_entries_for_follower
                                    .into_iter()
                                    .map(|f| ReplicationBatchItem {
                                        datablock: f.datablock,
                                        metablock: f.metablock,
                                    })
                                    .collect();
                                workset_size_bytes += additional_batch_items.iter().map(|c| c.size_bytes()).sum::<u64>();
                                batches.splice(0..0, additional_batch_items);

                                added_additional_entries = true; //Stops infinite requests for more batches from follower
                            }
                            _ => {
                                _replication_details = ReplicationDetails::RepliatedToS3(replication_err);
                                follower_falling_behind_or_offline = true;
                            }
                        }
                    }
                    _ => {
                        _replication_details = ReplicationDetails::RepliatedToS3(replication_err);
                        follower_falling_behind_or_offline = true;
                    }
                }
            }
        }
    }

    let path = match &_replication_details {
        ReplicationDetails::ReplicatedToFollower => "tcp",
        ReplicationDetails::RepliatedToS3(_) => "s3",
    };
    debug!(
        shard_id,
        batch_count = initial_batch_count,
        path,
        duration_ms = start.elapsed().as_millis() as u64,
        "Replication batch committed"
    );

    commit_replication(
        &log_segments_cache,
        &shard_mem_cache,
        &watched_aggregates,
        replication_captured_data.replication_snapshot,
    );

    metrics::histogram!("celeriant_replication_duration_seconds", &shard_label).record(start.elapsed().as_secs_f64());
    metrics::histogram!("celeriant_replication_batch_size", &shard_label).record(initial_batch_count as f64);

    Ok(())
}

/// Commits successful replication by updating read path, recent write cache, and broadcasting events.
/// No failures here, all in-memory operations
fn commit_replication(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    watched_aggregates: &Rc<AggregateWatchers>,
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
                    shard_mem_cache.commit_position_snapshot(&event_batch, log_id, item.metablock_absolute_pos, CachePath::Read);

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
                    shard_mem_cache.update_aggregate_min_event_batch_index(
                        &soft_trim.aggregate_key,
                        soft_trim.keep_from_event_batch_index,
                        CachePath::Write,
                    );
                    shard_mem_cache.update_aggregate_min_event_batch_index(
                        &soft_trim.aggregate_key,
                        soft_trim.keep_from_event_batch_index,
                        CachePath::Read,
                    );
                    event_collector.add_trim_event(soft_trim.aggregate_key.clone(), soft_trim.keep_from_event_batch_index);
                }
                MetablockKind::SoftDelete(soft_delete) => {
                    shard_mem_cache.put_aggregate_into_cache_as_deleted(
                        soft_delete.aggregate_key.clone(),
                        log_id, item.metablock_absolute_pos,
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
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    replication_snapshot: Vec<PendingCommitData>,
    shard_id: u32,
) -> Result<(), ReplicationRollbackFailure> {
    let batches_to_rollback = replication_snapshot.len();
    error!(shard_id, batches_to_rollback, "Replication failed to both follower and S3, rolling back");
    metrics::counter!("celeriant_replication_rollbacks_total").increment(1);
    // In replication rollback, we modify the write positions in the file header
    // So we must drain and block any more writes to disk until rollback completes
    let _fsync_gate = fsync_coordinator
        .acquire_rollback_lock()
        .await
        .ok_or_else(|| ReplicationRollbackFailure::FsyncAmortisedBatchLockTimeout)?;

    // We nuke all the in-memory caches at this step at let everything rebuild naturally
    // Key learning here is this failure mode is rare - follower and S3 must both be down
    shard_mem_cache.borrow_mut().execute_replication_rollback();

    let log_ids: HashSet<u64> = replication_snapshot.iter().map(|c| c.log_id()).collect();

    let mut trailing_shard_log_header: Option<ShardLogHeader> = None;

    // Rollback each log segment file. Normally only one file, but could be two files if a rotation occurred
    // Failure to rollback of a file stops entire process
    for log_id in log_ids {
        if let Some(log_segment) = log_segments_cache.get_if_cached(log_id) {
            let (header, header_end_start_pos) = {
                let mut metadata = log_segment.metadata.borrow_mut();
                let shard_log_header_end_pos = metadata.file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);

                // This is actually a valid scenario - we have *just* rotated a log
                // while still having uncommitted data in the previous log segment file
                // *or* this is the first log file ever for this shard and we have to rollback the first batch
                if metadata.read.is_none() {
                    metadata.write.datablocks_position = shard_log_header_end_pos;
                    metadata.write.metablocks_position = HEADER_BLOCK_SIZE_BYTES as u64;
                    metadata.write.aggregate_key_bloom = AggregateKeyBloom::new();

                    if let Some(trailing_shard_log_header) = trailing_shard_log_header {
                        metadata.write.wal_index = trailing_shard_log_header.wal_index;
                        metadata.write.tip_hash = trailing_shard_log_header.tip_hash;
                    } else {
                        metadata.write.wal_index = 0;
                        metadata.write.tip_hash = GENESIS_HASH;
                    }
                }

                // The in-memory position needs updating
                metadata.write = metadata.read.as_ref().unwrap().clone();

                (metadata.to_shard_log_header(), shard_log_header_end_pos)
            };

            let dma_file_writer = log_segment
                .lock_writer("rollback_replicate")
                .await
                .map_err(|_| ReplicationRollbackFailure::WriteLockTimeout { log_id })?;
            let dma_file_writer = dma_file_writer
                .as_ref()
                .ok_or_else(|| ReplicationRollbackFailure::LogSegmentFileUnavailable { log_id })?;

            write_dual_shard_log_header(dma_file_writer, header_end_start_pos, &header)
                .await
                .map_err(|source| ReplicationRollbackFailure::WriteDualHeaderError { log_id, source })?;

            dma_file_writer
                .fdatasync()
                .await
                .map_err(|_| ReplicationRollbackFailure::HeaderFsyncFailed { log_id })?;

            // Now that the write position has changed for datablocks, we need to prepare a new datablocks carry over bytes for the next write
            let mut metadata = log_segment.metadata.borrow_mut();
            metadata.datablocks_carry_over = read_datablocks_carry_over_bytes(&dma_file_writer, metadata.write.datablocks_position)
                .await
                .map_err(|e| ReplicationRollbackFailure::UnableToReadDatablocksCarryOver {
                    log_id,
                    source: e.to_string(),
                })?;

            // Critical we keep this to handle rollback of now-empty-shard log segment files
            trailing_shard_log_header = Some(header);
        }
    }

    Ok(())
}

async fn fetch_catchup_entries(
    log_segments_cache: &Rc<LogSegmentsCache>,
    follower_wal_index: u64,
    leader_wal_index: u64,
    max_size_bytes: u64,
    read_max_chunk_size: u64,
) -> Result<Vec<EventBatchFromLogSegmentFile>, FetchCatchupEntriesError> {
    let current_log_id = log_segments_cache.active_log_id();
    let mut scanner = ReverseMetablockScanner::new(log_segments_cache, current_log_id, None, read_max_chunk_size);

    let mut replication_items: Vec<EventBatchFromLogSegmentFile> = vec![];
    let mut accumulated_size = 0u64;

    let _scan_result = scanner
        .scan(|log_id, _pos, bytes| {
            let wal_index = metablock_bytes::read_wal_index(bytes);

            // Stop if we've gone too far back
            if wal_index <= follower_wal_index {
                return Ok(Some(()));
            }

            // Include if in range
            if wal_index < leader_wal_index {
                let metablock = deserialise_metablock(bytes)?;

                // Estimate size (metablock + potential datablock)
                let size_estimate = metablock.uncompressed_size.saturating_add(FIXED_BLOCK_SIZE_BYTES as u64);
                accumulated_size += size_estimate;

                replication_items.push(EventBatchFromLogSegmentFile {
                    log_id,
                    metablock,
                    datablock: None,
                });

                // Stop if size limit exceeded
                if accumulated_size > max_size_bytes {
                    return Ok(Some(()));
                }
            }

            Ok::<Option<()>, DiskFormatError>(None)
        })
        .await
        .map_err(FetchCatchupEntriesError::MetablockDiscoveryError)?;

    // If nothing collected, return empty
    // This is still valid because the leader no longer has the entries
    // So the follower doesn't need them either (hard deleted)
    if replication_items.is_empty() {
        return Ok(vec![]);
    }

    if accumulated_size > max_size_bytes {
        return Err(FetchCatchupEntriesError::FollowerTooFarBehind);
    }

    // Reverse to get chronological order
    replication_items.reverse();

    fetch_datablocks_for_metablocks(&mut replication_items, read_max_chunk_size, log_segments_cache)
        .await
        .map_err(FetchCatchupEntriesError::FetchDatablockError)?;

    Ok(replication_items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use glommio::{LocalExecutorBuilder, Placement};

    use celeriant_memcache::pending_cache_item::PendingCacheItem;
    use celeriant_msg::request::requests::ReplicationBatchItem;
    use celeriant_rotating_log::log_segment_file::log_segment_cursor::LogSegmentCursor;
    use celeriant_rotating_log::log_segment_file::log_segment_file_metadata::LogSegmentFileMetadata;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::metablocks::metablock::Metablock;

    use crate::error::replication_to_s3_error::ReplicateToS3Error;

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    fn item() -> ReplicationBatchItem {
        ReplicationBatchItem {
            metablock: Metablock::default_inline_event_batch_metadata(AggregateKey::default()),
            datablock: None,
        }
    }

    #[test]
    fn batch_end_index_scenarios() {
        let sz = item().size_bytes();
        let items: Vec<_> = (0..5).map(|_| item()).collect();

        let cases: Vec<(&[ReplicationBatchItem], u64, usize)> = vec![
            (&items[..1], sz * 2, 1),         // single item, generous budget
            (&items[..5], sz * 10, 5),         // all fit
            (&items[..3], 1, 1),               // oversized first item, progress guarantee
            (&items[..4], sz * 2, 2),           // exact budget for 2
            (&items[..5], sz * 3, 3),           // budget for 3 of 5
            (&items[..1], sz, 1),               // single item, exact budget
            (&items[..5], sz * 5, 5),           // all fit exactly
        ];

        for (i, (slice, max_bytes, expected)) in cases.iter().enumerate() {
            assert_eq!(
                batch_end_index(slice, *max_bytes),
                *expected,
                "case {i}: items={}, max_bytes={max_bytes}",
                slice.len()
            );
        }
    }

    // ── S3 fallback chunking ──

    struct RecordingReplicationClient {
        s3_item_counts: Rc<RefCell<Vec<usize>>>,
    }

    impl RecordingReplicationClient {
        fn new() -> (Self, Rc<RefCell<Vec<usize>>>) {
            let counts = Rc::new(RefCell::new(Vec::new()));
            (Self { s3_item_counts: counts.clone() }, counts)
        }
    }

    impl ReplicationClient for RecordingReplicationClient {
        async fn replicate_to_follower(&self, _: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToFollowerError> {
            unreachable!("s3 path should not call replicate_to_follower")
        }

        async fn replicate_to_s3(&self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
            self.s3_item_counts.borrow_mut().push(batches.len());
            Ok(())
        }

        fn set_follower_address(&self, _address: Option<String>) {}

        async fn send_heartbeat(&self) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
            unreachable!("s3 replication test should not call send_heartbeat")
        }

        async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> {
            Ok(true)
        }
    }

    fn make_captured_data(count: usize) -> ReplicationCapturedData {
        let items: Vec<PendingCacheItem> = (0..count)
            .map(|_| PendingCacheItem {
                metablock: Metablock::default_inline_event_batch_metadata(AggregateKey::default()),
                datablock: None,
                metablock_absolute_pos: 0,
            })
            .collect();

        ReplicationCapturedData {
            follower_falling_behind_or_offline: true,
            replication_snapshot: vec![PendingCommitData {
                log_metadata: LogSegmentFileMetadata {
                    log_id: 999999,
                    file_len: 0,
                    write: LogSegmentCursor::default(),
                    read: None,
                    datablocks_carry_over: None,
                },
                pending_queue: items,
            }],
        }
    }

    fn test_dir() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("shard");
        (tmp, dir)
    }

    #[test]
    fn s3_fallback_splits_by_max_batch_bytes() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let lsc = Rc::new(
                LogSegmentsCache::ready_up(dir, 4 * 1024 * 1024, 4, 0).await.unwrap()
            );
            let smc = Rc::new(RefCell::new(
                MemCache::new(64 * 1024 * 1024, 64 * 1024 * 1024, 32 * 1024 * 1024, 1024 * 1024, 4 * 1024 * 1024, 64 * 1024 * 1024)
            ));

            let (client, s3_counts) = RecordingReplicationClient::new();
            let client = Rc::new(client);

            let sz = item().size_bytes();

            commit_replication_with_rollback(
                client,
                Rc::new(Coordinator::new()),
                lsc.clone(),
                smc,
                Rc::new(AggregateWatchers::new()),
                make_captured_data(5),
                u64::MAX,   // max_catchup_gap_bytes (irrelevant, already flagged)
                u64::MAX,   // max_request_size (irrelevant, S3 path)
                sz * 2,     // max_s3_fallback_batch_bytes: 2 items per chunk
                64 * 1024,  // read_max_chunk_size
                0,          // shard_id
            ).await.unwrap();

            // 5 items, 2 per chunk: [2, 2, 1]
            assert_eq!(*s3_counts.borrow(), vec![2, 2, 1]);

            lsc.close().await;
        });
    }
}
