use std::cell::{RefCell};
use std::rc::Rc;

use celeriant_distributed::node_status::NodeStatus;
use celeriant_distributed::paths::fallback_shard_prefix;
use celeriant_msg::request::requests::ReplicationBatchItem;
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::constants::GENESIS_HASH;
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wire::disk::serialised_datablock::SerialisedDatablock;
use celeriant_wire::disk::versioned_block::deserialise_fallback_batch;

use celeriant_memcache::shard_log_queue_item::ShardLogQueueItem;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use celeriant_watch::aggregate_watchers::AggregateWatchers;

use crate::amortisation::coordinator::Coordinator;
use crate::error::apply_batch_error::ApplyBatchError;
use crate::error::s3_catchup_error::S3CatchupError;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::s3_downloader::S3Downloader;
use crate::shard_wal_sync::{capture_fsync_snapshot, commit_fsync_with_rollback};

#[derive(Debug)]
pub struct S3CatchupResult {
    pub batches_applied: u64,
    pub bytes_downloaded: u64,
    pub rounds: u32,
    pub fully_caught_up: bool,
}

struct FallbackBatchRef {
    path: String,
    start_wal_index: u64,
    end_wal_index: u64,
}

pub(crate) async fn catchup_from_s3<D: S3Downloader>(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<ShardMemCache>>,
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    watched_aggregates: &Rc<AggregateWatchers>,
    downloader: &D,
    shard_id: u32,
    max_rounds: u32,
) -> Result<S3CatchupResult, S3CatchupError> {
    let prefix = fallback_shard_prefix(shard_id);
    let mut result = S3CatchupResult {
        batches_applied: 0,
        bytes_downloaded: 0,
        rounds: 0,
        fully_caught_up: false,
    };

    for _ in 0..max_rounds {
        result.rounds += 1;

        let round = catchup_round(
            log_segments_cache, shard_mem_cache, fsync_coordinator,
            watched_aggregates, downloader, &prefix,
        ).await?;

        if round.batches == 0 {
            result.fully_caught_up = true;
            break;
        }

        result.batches_applied += round.batches;
        result.bytes_downloaded += round.bytes;
    }

    Ok(result)
}

struct RoundApplied {
    batches: u64,
    bytes: u64,
}

async fn catchup_round<D: S3Downloader>(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<ShardMemCache>>,
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    watched_aggregates: &Rc<AggregateWatchers>,
    downloader: &D,
    prefix: &str,
) -> Result<RoundApplied, S3CatchupError> {
    let objects = downloader.list_objects(prefix).await?;

    let current_wal_index = {
        let active = log_segments_cache.active();
        active.metadata.borrow().write.wal_index
    };

    let mut batches: Vec<FallbackBatchRef> = objects
        .into_iter()
        .filter_map(|obj| {
            let (_sid, start, end) = parse_fallback_path(&obj.path)?;
            Some(FallbackBatchRef { path: obj.path, start_wal_index: start, end_wal_index: end })
        })
        .filter(|b| b.end_wal_index > current_wal_index)
        .collect();

    batches.sort_by_key(|b| b.start_wal_index);

    if batches.is_empty() {
        return Ok(RoundApplied { batches: 0, bytes: 0 });
    }

    for window in batches.windows(2) {
        let expected = window[0].end_wal_index + 1;
        let got = window[1].start_wal_index;
        if expected != got {
            return Err(S3CatchupError::WalIndexGap { expected, got });
        }
    }

    let mut round = RoundApplied { batches: 0, bytes: 0 };

    for batch_ref in &batches {
        let data = downloader.download(&batch_ref.path).await?;
        round.bytes += data.len() as u64;

        let fallback_batch = deserialise_fallback_batch(&data)
            .map_err(|e| S3CatchupError::DeserializationFailed {
                path: batch_ref.path.clone(),
                source: e,
            })?;

        let all_items: Vec<ReplicationBatchItem> = fallback_batch
            .items
            .into_iter()
            .map(|fi| ReplicationBatchItem { metablock: fi.metablock, datablock: fi.datablock })
            .collect();

        // Skip already-applied entries within partially-overlapping batches
        let current_wal = log_segments_cache.active().metadata.borrow().write.wal_index;
        let skip = all_items.iter()
            .position(|item| item.metablock.wal_index > current_wal)
            .unwrap_or(all_items.len());
        let items = &all_items[skip..];

        if items.is_empty() {
            downloader.delete(&batch_ref.path).await?;
            continue;
        }

        apply_external_batch(log_segments_cache, shard_mem_cache, items)
            .map_err(S3CatchupError::ApplyFailed)?;

        sync_applied_batch(
            log_segments_cache, shard_mem_cache, fsync_coordinator,
            watched_aggregates,
        ).await.map_err(S3CatchupError::FsyncFailed)?;

        downloader.delete(&batch_ref.path).await?;
        round.batches += 1;
    }

    Ok(round)
}

/// Validate WAL continuity and queue entries. Does not fsync.
pub(crate) fn apply_external_batch(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<ShardMemCache>>,
    items: &[ReplicationBatchItem],
) -> Result<(), ApplyBatchError> {
    let (current_tip_hash, current_wal_index) = {
        let active = log_segments_cache.active();
        let metadata = active.metadata.borrow();
        (metadata.write.tip_hash, metadata.write.wal_index)
    };
    let (batch_tip_hash, batch_wal_index) = items
        .first()
        .map(|b| (b.metablock.previous_tip_hash, b.metablock.wal_index))
        .unwrap_or((GENESIS_HASH, 0));

    if current_wal_index.saturating_add(1) != batch_wal_index {
        return Err(ApplyBatchError::WalIndexMismatch {
            current: current_wal_index,
            batch_first: batch_wal_index,
        });
    }
    if current_tip_hash != batch_tip_hash {
        return Err(ApplyBatchError::TipHashMismatch {
            current: current_tip_hash,
            batch: batch_tip_hash,
        });
    }

    queue_replicated_entries(shard_mem_cache, items)
}

fn queue_replicated_entries(
    shard_mem_cache: &Rc<RefCell<ShardMemCache>>,
    items: &[ReplicationBatchItem],
) -> Result<(), ApplyBatchError> {
    let mut prepared = Vec::with_capacity(items.len());

    for item in items {
        let (datablock_bytes, datablock) = match &item.metablock.datablock {
            DatablockStorageKind::None | DatablockStorageKind::Inline(_) => (None, None),
            DatablockStorageKind::Block(_) => {
                if let Some(datablock) = &item.datablock {
                    let compression_type = CompressionType::from_tuple(item.metablock.datablock_compression_type, None);
                    let serialized = SerialisedDatablock::new(datablock, compression_type)
                        .map_err(ApplyBatchError::SerialiseDatablocks)?;
                    (serialized.external_data, Some(datablock.clone()))
                } else {
                    return Err(ApplyBatchError::MissingDatablock);
                }
            }
        };
        prepared.push(ShardLogQueueItem::new(datablock, datablock_bytes, item.metablock.clone()));
    }

    shard_mem_cache.borrow_mut().add_to_pending_queue(prepared);
    Ok(())
}

/// Fsync via the coordinator (immediate, no amortisation delay).
async fn sync_applied_batch(
    log_segments_cache: &Rc<LogSegmentsCache>,
    shard_mem_cache: &Rc<RefCell<ShardMemCache>>,
    fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
    watched_aggregates: &Rc<AggregateWatchers>,
) -> Result<(), ShardFsyncError> {
    let lsc = log_segments_cache.clone();
    let smc = shard_mem_cache.clone();
    let wa = watched_aggregates.clone();
    let mc_capture = smc.clone();

    // We hardcode node status to standalone as we are in offline-catchup mode
    // and can advance the read position immediately (no follower replication)
    fsync_coordinator
        .request_sync_two_phase(
            None,
            move || async move { capture_fsync_snapshot(&mc_capture) },
            move |captured| commit_fsync_with_rollback(NodeStatus::Standalone, lsc, smc, wa, captured),
        )
        .await
}

/// Parse a fallback batch path to extract shard_id, start_index, and end_index.
/// Returns None if the path doesn't match the expected format.
pub fn parse_fallback_path(path: &str) -> Option<(u32, u64, u64)> {
    // Expected format: cluster/fallback/shard_XXX/batch_XXXXXXXXX_XXXXXXXXX.bin
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 3 {
        return None;
    }

    let shard_part = parts.iter().find(|p| p.starts_with("shard_"))?;
    let batch_part = parts.iter().find(|p| p.starts_with("batch_"))?;

    let shard_id: u32 = shard_part.strip_prefix("shard_")?.parse().ok()?;
    let batch_name = batch_part.strip_prefix("batch_")?.strip_suffix(".bin")?;

    let indices: Vec<&str> = batch_name.split('_').collect();
    if indices.len() != 2 {
        return None;
    }

    let start_index: u64 = indices[0].parse().ok()?;
    let end_index: u64 = indices[1].parse().ok()?;

    Some((shard_id, start_index, end_index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::path::PathBuf;

    use bytes::Bytes;
    use celeriant_wal::datablocks::datablock::Datablock;
    use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
    use celeriant_wal::datablocks::datablock_kind::DatablockKind;
    use celeriant_wal::metablocks::metablock_event_batch::MetablockEventBatch;
    use celeriant_wal::metablocks::metablock_kind::MetablockKind;
    use glommio::{LocalExecutorBuilder, Placement};

    use celeriant_distributed::paths::fallback_batch_path;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::constants::WIRE_VERSION_S3_FALLBACK_BATCH;
    use celeriant_wal::metablocks::metablock::Metablock;
    use celeriant_wal::s3::fallback_batch::{FallbackBatch, FallbackItem};
    use celeriant_wire::disk::versioned_block::serialize_versioned_message_heap;

    use crate::s3_downloader::S3ObjectRef;

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    const PREALLOCATE: u64 = 4 * 1024 * 1024;

    fn test_dir() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("shard");
        (tmp, dir)
    }

    fn test_metablock(wal_index: u64, previous_tip_hash: [u8; 32]) -> Metablock {
        let mut mb = Metablock::default_inline_event_batch_metadata(AggregateKey::new(1, 1, 1));
        mb.wal_index = wal_index;
        mb.previous_tip_hash = previous_tip_hash;
        mb
    }

    fn serialize_fallback_batch(batch: &FallbackBatch) -> Bytes {
        let data = serialize_versioned_message_heap(batch, WIRE_VERSION_S3_FALLBACK_BATCH).unwrap();
        Bytes::from(data)
    }

    fn make_fallback_batch(shard_id: u32, start: u64, end: u64, tip_hash: [u8; 32]) -> (String, Bytes) {
        let mut batch = FallbackBatch::new(start, end, shard_id);
        for wal_index in start..=end {
            batch.push_item(FallbackItem {
                metablock: test_metablock(wal_index, tip_hash),
                datablock: None,
            });
        }
        let path = fallback_batch_path(shard_id, start, end);
        (path, serialize_fallback_batch(&batch))
    }

    // ── Mock S3Downloader ──

    struct MockDownloader {
        objects: RefCell<HashMap<String, Bytes>>,
        delete_log: RefCell<Vec<String>>,
        list_call_count: Cell<u32>,
        on_list_hooks: RefCell<HashMap<u32, Vec<Box<dyn Fn(&MockDownloader)>>>>,
    }

    impl MockDownloader {
        fn new() -> Self {
            Self {
                objects: RefCell::new(HashMap::new()),
                delete_log: RefCell::new(Vec::new()),
                list_call_count: Cell::new(0),
                on_list_hooks: RefCell::new(HashMap::new()),
            }
        }

        fn insert(&self, path: String, data: Bytes) {
            self.objects.borrow_mut().insert(path, data);
        }

        fn deleted_paths(&self) -> Vec<String> {
            self.delete_log.borrow().clone()
        }

        /// Register a hook that fires when `list_objects` is called for the Nth time (0-indexed).
        /// The hook receives `&MockDownloader` so it can call `insert()`.
        fn on_list(&self, call_index: u32, hook: impl Fn(&Self) + 'static) {
            self.on_list_hooks.borrow_mut()
                .entry(call_index)
                .or_default()
                .push(Box::new(hook));
        }
    }

    impl S3Downloader for MockDownloader {
        async fn list_objects(&self, prefix: &str) -> Result<Vec<S3ObjectRef>, S3CatchupError> {
            let call = self.list_call_count.get();
            self.list_call_count.set(call + 1);
            if let Some(hooks) = self.on_list_hooks.borrow_mut().remove(&call) {
                for hook in hooks {
                    hook(self);
                }
            }
            Ok(self.objects.borrow().iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| S3ObjectRef { path: k.clone(), size: v.len() as u64 })
                .collect())
        }

        async fn download(&self, path: &str) -> Result<Bytes, S3CatchupError> {
            self.objects.borrow().get(path).cloned().ok_or_else(|| S3CatchupError::S3GetFailed {
                path: path.to_string(),
                message: "not found".to_string(),
            })
        }

        async fn delete(&self, path: &str) -> Result<(), S3CatchupError> {
            self.objects.borrow_mut().remove(path);
            self.delete_log.borrow_mut().push(path.to_string());
            Ok(())
        }
    }

    // ── Component setup ──

    struct TestComponents {
        log_segments_cache: Rc<LogSegmentsCache>,
        shard_mem_cache: Rc<RefCell<ShardMemCache>>,
        fsync_coordinator: Rc<Coordinator<ShardFsyncError>>,
        watched_aggregates: Rc<AggregateWatchers>,
    }

    impl TestComponents {
        async fn new(dir: &std::path::Path) -> Self {
            let log_segments_cache = LogSegmentsCache::ready_up(dir.to_path_buf(), PREALLOCATE, 4)
                .await
                .unwrap();
            Self {
                log_segments_cache: Rc::new(log_segments_cache),
                shard_mem_cache: Rc::new(RefCell::new(ShardMemCache::new(64 * 1024 * 1024, 64 * 1024 * 1024, 32 * 1024 * 1024, 1024 * 1024, 64 * 1024 * 1024))),
                fsync_coordinator: Rc::new(Coordinator::new()),
                watched_aggregates: Rc::new(AggregateWatchers::new()),
            }
        }

        fn wal_index(&self) -> u64 {
            self.log_segments_cache.active().metadata.borrow().write.wal_index
        }

        fn tip_hash(&self) -> [u8; 32] {
            self.log_segments_cache.active().metadata.borrow().write.tip_hash
        }

        async fn catchup(&self, downloader: &MockDownloader, shard_id: u32, max_rounds: u32) -> Result<S3CatchupResult, S3CatchupError> {
            catchup_from_s3(
                &self.log_segments_cache, &self.shard_mem_cache, &self.fsync_coordinator,
                &self.watched_aggregates, 
                downloader, shard_id, max_rounds,
            ).await
        }

        async fn close(&self) {
            self.log_segments_cache.close().await;
        }
    }

    // ── apply_external_batch tests ──

    #[test]
    fn apply_rejects_wal_index_mismatch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            let item = ReplicationBatchItem {
                metablock: test_metablock(99, GENESIS_HASH),
                datablock: None,
            };
            let err = apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item]).unwrap_err();
            assert!(matches!(err, ApplyBatchError::WalIndexMismatch { current: 0, batch_first: 99 }));

            tc.close().await;
        });
    }

    #[test]
    fn apply_rejects_tip_hash_mismatch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            let item = ReplicationBatchItem {
                metablock: test_metablock(1, [0xAB; 32]),
                datablock: None,
            };
            let err = apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item]).unwrap_err();
            assert!(matches!(err, ApplyBatchError::TipHashMismatch { .. }));

            tc.close().await;
        });
    }

    #[test]
    fn apply_queues_valid_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;

            let item = ReplicationBatchItem {
                metablock: test_metablock(1, GENESIS_HASH),
                datablock: None,
            };
            apply_external_batch(&tc.log_segments_cache, &tc.shard_mem_cache, &[item]).unwrap();
            assert!(!tc.shard_mem_cache.borrow().pending_append_queue_is_empty());

            tc.close().await;
        });
    }

    // ── catchup_from_s3 tests ──

    #[test]
    fn catchup_empty_listing_returns_zero() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = MockDownloader::new();

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 0);
            assert!(result.fully_caught_up);
            assert_eq!(result.rounds, 1);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_applies_single_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = MockDownloader::new();

            let (path, data) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert!(result.fully_caught_up);
            assert_eq!(result.rounds, 2);
            assert_eq!(tc.wal_index(), 1);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_applies_batch_with_multiple_entries() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = MockDownloader::new();

            let (path, data) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert_eq!(tc.wal_index(), 5);
            assert!(result.fully_caught_up);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_detects_wal_index_gap() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = MockDownloader::new();

            let (path, data) = make_fallback_batch(0, 1, 2, GENESIS_HASH);
            dl.insert(path, data);
            // Gap: missing batch 3-4
            let (path, data) = make_fallback_batch(0, 5, 6, GENESIS_HASH);
            dl.insert(path, data);

            let err = tc.catchup(&dl, 0, 10).await.unwrap_err();
            assert!(matches!(err, S3CatchupError::WalIndexGap { expected: 3, got: 5 }));

            tc.close().await;
        });
    }

    #[test]
    fn catchup_deletes_applied_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = MockDownloader::new();

            let (path, data) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            let expected_path = path.clone();
            dl.insert(path, data);

            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(dl.deleted_paths(), vec![expected_path]);
            assert!(dl.objects.borrow().is_empty());

            tc.close().await;
        });
    }

    #[test]
    fn catchup_respects_max_rounds() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = MockDownloader::new();

            // Simulate leader writing faster than we catch up:
            // After each round's deletes, inject a new batch for the next round
            let (path, data) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl.insert(path, data);

            // max_rounds=1: apply batch 1, then stop without re-listing
            let result = tc.catchup(&dl, 0, 1).await.unwrap();
            assert_eq!(result.rounds, 1);
            assert_eq!(result.batches_applied, 1);
            assert!(!result.fully_caught_up);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_skips_already_applied_batches() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = MockDownloader::new();

            // Apply batch 1 first
            let (path, data) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 1);

            // Add batch 2, but also re-add batch 1 (already applied)
            let tip = tc.tip_hash();
            let (path1, data1) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl.insert(path1, data1);
            let (path2, data2) = make_fallback_batch(0, 2, 2, tip);
            dl.insert(path2, data2);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert_eq!(tc.wal_index(), 2);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_filters_by_shard_id() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = MockDownloader::new();

            // Shard 0 batch
            let (path, data) = make_fallback_batch(0, 1, 1, GENESIS_HASH);
            dl.insert(path, data);
            // Shard 1 batch (should be ignored when catching up shard 0)
            let (path, data) = make_fallback_batch(1, 1, 1, GENESIS_HASH);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert_eq!(tc.wal_index(), 1);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_handles_partial_overlap() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = MockDownloader::new();

            // Apply batch 1-3 first
            let (path, data) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            dl.insert(path, data);
            tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(tc.wal_index(), 3);

            // Add overlapping batch 2-6: entries 2-3 already applied, 4-6 are new.
            // All items get the same tip_hash; only item 4 (first after slicing) is checked.
            let tip = tc.tip_hash();
            let (path, data) = make_fallback_batch(0, 2, 6, tip);
            dl.insert(path, data);

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 1);
            assert_eq!(tc.wal_index(), 6);
            assert!(result.fully_caught_up);

            tc.close().await;
        });
    }

    #[test]
    fn catchup_multi_round_picks_up_new_batches() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let tc = TestComponents::new(&dir).await;
            let dl = MockDownloader::new();

            // Round 1: batch 1-3 available immediately
            let (path, data) = make_fallback_batch(0, 1, 3, GENESIS_HASH);
            dl.insert(path, data);

            // Round 2: inject batch 4-6 when list_objects is called the second time
            // (simulates leader uploading while we applied round 1)
            let lsc = tc.log_segments_cache.clone();
            dl.on_list(1, move |dl| {
                let tip = lsc.active().metadata.borrow().write.tip_hash;
                let (path, data) = make_fallback_batch(0, 4, 6, tip);
                dl.insert(path, data);
            });

            let result = tc.catchup(&dl, 0, 10).await.unwrap();
            assert_eq!(result.batches_applied, 2);
            assert_eq!(result.rounds, 3); // round 1: apply 1-3, round 2: apply 4-6, round 3: empty
            assert_eq!(tc.wal_index(), 6);
            assert!(result.fully_caught_up);

            tc.close().await;
        });
    }
    #[test]
    fn test_fallback_batch_s3_path() {
        let batch = FallbackBatch::new(5, 10, 2);
        assert_eq!(fallback_batch_path(batch.shard_id, batch.fallback_index, batch.end_wal_index), "cluster/fallback/shard_002/batch_000000005_000000010.bin");
    }

    #[test]
    fn test_parse_fallback_path() {
        assert_eq!(
            parse_fallback_path("cluster/fallback/shard_002/batch_000000005_000000010.bin"),
            Some((2, 5, 10))
        );
        assert_eq!(
            parse_fallback_path("cluster/fallback/shard_015/batch_123456789_123456799.bin"),
            Some((15, 123456789, 123456799))
        );
        assert_eq!(parse_fallback_path("cluster/lease.bin"), None);
        assert_eq!(parse_fallback_path("invalid"), None);
        assert_eq!(parse_fallback_path("cluster/fallback/shard_002/batch_000000005.bin"), None);
    }

    #[test]
    fn test_fallback_batch_bincode_roundtrip() {
        let aggregate_key = AggregateKey::new(1, 2, 3);

        let metablock1 = Metablock {
            wal_index: 42,
            server_timestamp: 1000,
            lease_index: 5,
            node_id: 999,
            uncompressed_size: 1024,
            compressed_size: 512,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [1u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                event_batch_index: 10,
                min_event_batch_index: 1,
                min_client_event_index: 1,
                max_client_event_index: 5,
                min_event_timestamp: 100,
                max_event_timestamp: 500,
                min_event_index: 1,
                max_event_index: 5,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::Block(celeriant_wal::metablocks::datablock_block_ref::DatablockBlockRef {
                crc32c: 0,
                datablock_position: 1000,
            }),
        };

        let metablock2 = Metablock {
            wal_index: 43,
            server_timestamp: 2000,
            lease_index: 5,
            node_id: 999,
            uncompressed_size: 2048,
            compressed_size: 1024,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [2u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                event_batch_index: 11,
                min_event_batch_index: 1,
                min_client_event_index: 6,
                max_client_event_index: 10,
                min_event_timestamp: 600,
                max_event_timestamp: 1000,
                min_event_index: 6,
                max_event_index: 10,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::None,
        };

        let datablock1 = Some(Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                event_batch_index: 10,
                events: vec![],
            }),
        });

        let original_batch = FallbackBatch {
            fallback_index: 42,
            end_wal_index: 43,
            shard_id: 7,
            items: vec![
                FallbackItem {
                    metablock: metablock1,
                    datablock: datablock1,
                },
                FallbackItem {
                    metablock: metablock2,
                    datablock: None,
                },
            ],
        };

        let serialized = celeriant_wire::disk::versioned_block::serialize_versioned_message_heap(
            &original_batch,
            celeriant_wal::constants::WIRE_VERSION_S3_FALLBACK_BATCH,
        ).expect("serialization should succeed");

        let deserialized = celeriant_wire::disk::versioned_block::deserialise_fallback_batch(&serialized)
            .expect("deserialization should succeed");

        assert_eq!(deserialized.fallback_index, 42);
        assert_eq!(deserialized.end_wal_index, 43);
        assert_eq!(deserialized.shard_id, 7);
        assert_eq!(deserialized.items.len(), 2);

        assert_eq!(deserialized.items[0].metablock.wal_index, 42);
        assert_eq!(deserialized.items[0].metablock.server_timestamp, 1000);
        assert!(deserialized.items[0].datablock.is_some());

        assert_eq!(deserialized.items[1].metablock.wal_index, 43);
        assert_eq!(deserialized.items[1].metablock.server_timestamp, 2000);
        assert!(deserialized.items[1].datablock.is_none());
    }

    #[test]
    fn test_fallback_index_is_first_wal_index() {
        let aggregate_key = AggregateKey::new(1, 2, 3);

        let metablock_first = Metablock {
            wal_index: 100,
            server_timestamp: 1000,
            lease_index: 5,
            node_id: 999,
            uncompressed_size: 1024,
            compressed_size: 512,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [1u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                event_batch_index: 10,
                min_event_batch_index: 1,
                min_client_event_index: 1,
                max_client_event_index: 5,
                min_event_timestamp: 100,
                max_event_timestamp: 500,
                min_event_index: 1,
                max_event_index: 5,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::None,
        };

        let metablock_second = Metablock {
            wal_index: 101,
            ..metablock_first.clone()
        };

        let batch = FallbackBatch {
            fallback_index: 100,
            end_wal_index: 101,
            shard_id: 5,
            items: vec![
                FallbackItem {
                    metablock: metablock_first,
                    datablock: None,
                },
                FallbackItem {
                    metablock: metablock_second,
                    datablock: None,
                },
            ],
        };

        assert_eq!(batch.fallback_index, batch.items[0].metablock.wal_index);
        assert_eq!(batch.end_wal_index, batch.items[batch.items.len() - 1].metablock.wal_index);
    }

    #[test]
    fn test_shard_id_narrowing() {
        let batch_0 = FallbackBatch::new(1, 5, 0);
        assert_eq!(
            fallback_batch_path(batch_0.shard_id, batch_0.fallback_index, batch_0.end_wal_index),
            "cluster/fallback/shard_000/batch_000000001_000000005.bin"
        );

        let batch_999 = FallbackBatch::new(1, 10, 999);
        assert_eq!(
            fallback_batch_path(batch_999.shard_id, batch_999.fallback_index, batch_999.end_wal_index),
            "cluster/fallback/shard_999/batch_000000001_000000010.bin"
        );

        assert!(u32::MAX > 999);
    }
}