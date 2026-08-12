//! Shared fixtures for S3-catchup tests: an in-memory S3 downloader, shard
//! components wired for `catchup_from_s3`, and fallback-batch builders.
//! Lives outside the implementation module so contract tests can be written
//! against the catchup surface alone.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;

use bytes::Bytes;

use celeriant_distributed::paths::fallback_batch_path;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, GENESIS_HASH, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_S3_FALLBACK_BATCH};
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::s3::fallback_batch::{FallbackBatch, FallbackItem};
use celeriant_watch::aggregate_watchers::AggregateWatchers;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::disk::versioned_block::serialize_versioned_message_heap;

use crate::amortisation::coordinator::Coordinator;
use crate::error::s3_catchup_error::S3CatchupError;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::s3_downloader::{S3Downloader, S3ObjectRef};
use crate::schema_validator::CompiledValidator;
use crate::shard_wal_s3_catchup::{catchup_from_s3, CatchupRole, S3CatchupResult};

pub(crate) type MemCache = ShardMemCache<CompiledValidator>;

pub(crate) const PREALLOCATE: u64 = 4 * 1024 * 1024;

pub(crate) fn test_dir() -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("shard");
    (tmp, dir)
}

pub(crate) fn test_codec() -> Rc<DictCodec> {
    Rc::new(DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile"))
}

pub(crate) fn test_metablock(wal_seq: u64, previous_tip_hash: [u8; 32]) -> Metablock {
    let mut mb = Metablock::default_inline_event_batch_metadata(AggregateKey::new(1, 1, 1));
    mb.wal_seq = wal_seq;
    mb.previous_tip_hash = previous_tip_hash;
    mb
}

pub(crate) fn test_metablock_for_agg(wal_seq: u64, prev: [u8; 32], agg: AggregateKey) -> Metablock {
    let mut mb = Metablock::default_inline_event_batch_metadata(agg);
    mb.wal_seq = wal_seq;
    mb.previous_tip_hash = prev;
    mb
}

pub(crate) fn pos_at(wal_seq: u64) -> u64 {
    HEADER_BLOCK_SIZE_BYTES as u64 + (wal_seq - 1) * FIXED_BLOCK_SIZE_BYTES as u64
}

pub(crate) fn serialize_fallback_batch(batch: &FallbackBatch) -> Bytes {
    let data = serialize_versioned_message_heap(batch, WIRE_VERSION_S3_FALLBACK_BATCH).unwrap();
    Bytes::from(data)
}

pub(crate) fn make_fallback_batch(shard_id: u32, start: u64, end: u64, tip_hash: [u8; 32]) -> (String, Bytes) {
    make_fallback_batch_with_node(shard_id, start, end, tip_hash, 0)
}

pub(crate) fn make_fallback_batch_with_node(shard_id: u32, start: u64, end: u64, tip_hash: [u8; 32], node_id: u128) -> (String, Bytes) {
    make_fallback_batch_with_seq(shard_id, start, end, tip_hash, node_id, 0)
}

pub(crate) fn make_fallback_batch_with_seq(shard_id: u32, start: u64, end: u64, tip_hash: [u8; 32], node_id: u128, upload_sequence: u64) -> (String, Bytes) {
    make_fallback_batch_with_lease_seq(shard_id, start, end, tip_hash, node_id, upload_sequence, 0)
}

pub(crate) fn make_fallback_batch_with_lease_seq(shard_id: u32, start: u64, end: u64, tip_hash: [u8; 32], node_id: u128, upload_sequence: u64, lease_epoch: u64) -> (String, Bytes) {
    let mut batch = FallbackBatch::new(start, end, shard_id, node_id, upload_sequence, lease_epoch);
    for wal_seq in start..=end {
        let mut mb = test_metablock(wal_seq, tip_hash);
        mb.lease_epoch = lease_epoch;
        batch.push_item(FallbackItem {
            metablock: mb,
            datablock: None,
        });
    }
    let path = fallback_batch_path(shard_id, start, end, node_id);
    (path, serialize_fallback_batch(&batch))
}

pub(crate) fn build_batch(start: u64, prevs: &[[u8; 32]], lease_epoch: u64) -> FallbackBatch {
    let mut b = FallbackBatch::new(start, start + prevs.len() as u64 - 1, 0, 0, 0, lease_epoch);
    for (i, &prev) in prevs.iter().enumerate() {
        let mut mb = test_metablock(start + i as u64, prev);
        mb.lease_epoch = lease_epoch;
        b.push_item(FallbackItem { metablock: mb, datablock: None });
    }
    b
}

// ── Mock S3Downloader ──

pub(crate) struct MockDownloader {
    pub(crate) objects: RefCell<HashMap<String, Bytes>>,
    pub(crate) download_log: RefCell<Vec<String>>,
    delete_log: RefCell<Vec<String>>,
    list_call_count: Cell<u32>,
    on_list_hooks: RefCell<HashMap<u32, Vec<Box<dyn Fn(&MockDownloader)>>>>,
    fail_paths: RefCell<HashSet<String>>,
}

impl MockDownloader {
    pub(crate) fn new() -> Self {
        Self {
            objects: RefCell::new(HashMap::new()),
            download_log: RefCell::new(Vec::new()),
            delete_log: RefCell::new(Vec::new()),
            list_call_count: Cell::new(0),
            on_list_hooks: RefCell::new(HashMap::new()),
            fail_paths: RefCell::new(HashSet::new()),
        }
    }

    pub(crate) fn insert(&self, path: String, data: Bytes) {
        self.objects.borrow_mut().insert(path, data);
    }

    pub(crate) fn fail_download(&self, path: String) {
        self.fail_paths.borrow_mut().insert(path);
    }

    pub(crate) fn downloaded_paths(&self) -> Vec<String> {
        self.download_log.borrow().clone()
    }

    pub(crate) fn deleted_paths(&self) -> Vec<String> {
        self.delete_log.borrow().clone()
    }

    /// Run `hook` just before returning the results of list call number
    /// `call_index` (0-based). Lets a test land "late" uploads mid-catchup.
    pub(crate) fn on_list(&self, call_index: u32, hook: impl Fn(&Self) + 'static) {
        self.on_list_hooks.borrow_mut().entry(call_index).or_default().push(Box::new(hook));
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
        Ok(self
            .objects
            .borrow()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| S3ObjectRef {
                path: k.clone(),
                size: v.len() as u64,
            })
            .collect())
    }

    async fn download(&self, path: &str) -> Result<Bytes, S3CatchupError> {
        self.download_log.borrow_mut().push(path.to_string());
        if self.fail_paths.borrow().contains(path) {
            return Err(S3CatchupError::S3GetFailed {
                path: path.to_string(),
                message: "injected failure".to_string(),
            });
        }
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

pub(crate) struct TestComponents {
    pub(crate) log_segments_cache: Rc<LogSegmentsCache>,
    pub(crate) shard_mem_cache: Rc<RefCell<MemCache>>,
    pub(crate) fsync_coordinator: Rc<Coordinator<ShardFsyncError>>,
    pub(crate) watched_aggregates: Rc<AggregateWatchers>,
    pub(crate) summary_cache: RefCell<crate::shard_wal::SummaryCache>,
}

impl TestComponents {
    pub(crate) async fn new(dir: &std::path::Path) -> Self {
        Self::with_preallocate(dir, PREALLOCATE).await
    }

    pub(crate) async fn with_preallocate(dir: &std::path::Path, preallocate: u64) -> Self {
        let log_segments_cache = LogSegmentsCache::ready_up(dir.to_path_buf(), preallocate, 4, 0).await.unwrap();
        Self {
            log_segments_cache: Rc::new(log_segments_cache),
            shard_mem_cache: Rc::new(RefCell::new(MemCache::new(
                64 * 1024 * 1024,
                64 * 1024 * 1024,
                32 * 1024 * 1024,
                4 * 1024 * 1024,
                2 * 1024 * 1024,
                64 * 1024 * 1024,
            ))),
            fsync_coordinator: Rc::new(Coordinator::new()),
            watched_aggregates: Rc::new(AggregateWatchers::new()),
            summary_cache: RefCell::new(lru::LruCache::new(std::num::NonZeroUsize::new(16).unwrap())),
        }
    }

    pub(crate) fn wal_seq(&self) -> u64 {
        self.log_segments_cache.active().metadata.borrow().write.wal_seq
    }

    pub(crate) fn tip_hash(&self) -> [u8; 32] {
        self.log_segments_cache.active().metadata.borrow().write.tip_hash
    }

    /// Default test entry: Promoting role (consume everything visible), the
    /// closest semantics to "apply these S3 files to the local WAL" that most
    /// loop-mechanics tests and the seed helpers rely on. Follower-role exit
    /// behavior is exercised explicitly via catchup_with_gap/catchup_with_target.
    pub(crate) async fn catchup(&self, downloader: &Rc<MockDownloader>, shard_id: u32, _max_rounds: u32) -> Result<S3CatchupResult, S3CatchupError> {
        self.catchup_with_peer(downloader, shard_id, None).await
    }

    pub(crate) async fn catchup_with_peer(
        &self,
        downloader: &Rc<MockDownloader>,
        shard_id: u32,
        peer_node_id: Option<u128>,
    ) -> Result<S3CatchupResult, S3CatchupError> {
        self.catchup_full(downloader, shard_id, peer_node_id, Some(100), CatchupRole::Promoting, 0).await
    }

    /// Full-control entry: `max_catchup_gap_bytes = None` matches the deployed
    /// rpi configuration (no CELERIANT_MAX_CATCHUP_GAP_BYTES set). Defaults to
    /// the Following role with no observed leader target (a fresh boot's view).
    pub(crate) async fn catchup_with_gap(
        &self,
        downloader: &Rc<MockDownloader>,
        shard_id: u32,
        peer_node_id: Option<u128>,
        max_catchup_gap_bytes: Option<u64>,
    ) -> Result<S3CatchupResult, S3CatchupError> {
        self.catchup_full(downloader, shard_id, peer_node_id, max_catchup_gap_bytes, CatchupRole::Following, 0).await
    }

    /// Following-role catchup with a recorded leader tip (what a kicked
    /// follower knows from its last rejected replication batch).
    pub(crate) async fn catchup_with_target(
        &self,
        downloader: &Rc<MockDownloader>,
        shard_id: u32,
        catchup_target_wal_seq: u64,
    ) -> Result<S3CatchupResult, S3CatchupError> {
        self.catchup_full(downloader, shard_id, None, None, CatchupRole::Following, catchup_target_wal_seq).await
    }

    /// Promoting-role catchup (leader-elect: must consume everything, settle
    /// window before Caught).
    pub(crate) async fn catchup_as_promoting(
        &self,
        downloader: &Rc<MockDownloader>,
        shard_id: u32,
    ) -> Result<S3CatchupResult, S3CatchupError> {
        self.catchup_full(downloader, shard_id, None, None, CatchupRole::Promoting, 0).await
    }

    pub(crate) async fn catchup_full(
        &self,
        downloader: &Rc<MockDownloader>,
        shard_id: u32,
        peer_node_id: Option<u128>,
        max_catchup_gap_bytes: Option<u64>,
        role: CatchupRole,
        catchup_target_wal_seq: u64,
    ) -> Result<S3CatchupResult, S3CatchupError> {
        // Fresh latch per call: single-invocation tests see first-kick semantics.
        let latch = std::cell::Cell::new(0u64);
        self.catchup_full_with_latch(downloader, shard_id, peer_node_id, max_catchup_gap_bytes, role, catchup_target_wal_seq, &latch).await
    }

    /// Boot-role catchup (first catchup after process start).
    pub(crate) async fn catchup_as_boot(
        &self,
        downloader: &Rc<MockDownloader>,
        shard_id: u32,
    ) -> Result<S3CatchupResult, S3CatchupError> {
        self.catchup_full(downloader, shard_id, None, None, CatchupRole::Boot, 0).await
    }

    /// Full-control entry with a caller-owned live-tail yield latch, for
    /// contracts spanning several catchup invocations (kick cycles).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn catchup_full_with_latch(
        &self,
        downloader: &Rc<MockDownloader>,
        shard_id: u32,
        peer_node_id: Option<u128>,
        max_catchup_gap_bytes: Option<u64>,
        role: CatchupRole,
        catchup_target_wal_seq: u64,
        live_tail_yielded_wal_seq: &std::cell::Cell<u64>,
    ) -> Result<S3CatchupResult, S3CatchupError> {
        catchup_from_s3(
            &self.log_segments_cache,
            &self.shard_mem_cache,
            &self.fsync_coordinator,
            &self.watched_aggregates,
            &self.summary_cache,
            downloader,
            shard_id,
            99,
            peer_node_id,
            max_catchup_gap_bytes,
            test_codec(),
            role,
            catchup_target_wal_seq,
            live_tail_yielded_wal_seq,
        )
        .await
    }

    pub(crate) async fn close(&self) {
        self.log_segments_cache.close().await;
    }

    /// Simulate a fresh boot's deferred read cursor: the follower's read cursor
    /// is initialised only by the first commit-notify after startup, so a node
    /// that just restarted has `metadata.read = None` while its write cursor
    /// (and durable chain) carry the pre-restart state.
    pub(crate) fn clear_read_cursor(&self) {
        self.log_segments_cache.active().metadata.borrow_mut().read = None;
    }

    /// Set the persisted self-ack floor (survives restarts in the real header):
    /// `truncate_wal` refuses to drop wal_seqs at or below this mark.
    pub(crate) fn set_last_self_acked(&self, wal_seq: u64) {
        self.log_segments_cache.active().metadata.borrow_mut().last_self_acked_wal_seq = wal_seq;
    }

    /// Apply 1..=end. Returns the tip captured at wal=end-1, which equals
    /// local @ wal=end's previous_tip_hash.
    pub(crate) async fn seed_chain(&self, end: u64) -> [u8; 32] {
        assert!(end >= 2);
        let dl = Rc::new(MockDownloader::new());
        let (p, d) = make_fallback_batch(0, 1, end - 1, GENESIS_HASH);
        dl.insert(p, d);
        self.catchup(&dl, 0, 10).await.unwrap();
        let prev = self.tip_hash();
        let (p, d) = make_fallback_batch(0, end, end, prev);
        dl.insert(p, d);
        self.catchup(&dl, 0, 10).await.unwrap();
        prev
    }
}

/// Apply 1..=end one wal_seq at a time, returning tips[i] = tip after wal_seq=i+1.
pub(crate) async fn seed_capturing_tips(tc: &TestComponents, end: u64) -> Vec<[u8; 32]> {
    let dl = Rc::new(MockDownloader::new());
    let mut tips = Vec::with_capacity(end as usize);
    for wal_seq in 1..=end {
        let prev = *tips.last().unwrap_or(&GENESIS_HASH);
        let (p, d) = make_fallback_batch(0, wal_seq, wal_seq, prev);
        dl.insert(p, d);
        tc.catchup(&dl, 0, 10).await.unwrap();
        tips.push(tc.tip_hash());
    }
    tips
}

/// Local at wal_seq=6; S3 holds a 6..=8 chain anchored at tip_after_5, which
/// trips TipHashMismatch on local's wal_seq=7 expectation and drives
/// truncate_wal at divergent_wal_seq=6. Returns the prepped downloader.
pub(crate) async fn divergence_at_6(tc: &TestComponents) -> Rc<MockDownloader> {
    let dl = Rc::new(MockDownloader::new());
    let (p, d) = make_fallback_batch(0, 1, 5, GENESIS_HASH);
    dl.insert(p, d);
    tc.catchup(&dl, 0, 10).await.unwrap();
    let tip_after_5 = tc.tip_hash();
    let (p, d) = make_fallback_batch(0, 6, 6, tip_after_5);
    dl.insert(p, d);
    tc.catchup(&dl, 0, 10).await.unwrap();
    dl.objects.borrow_mut().clear();
    let (p, d) = make_fallback_batch(0, 6, 8, tip_after_5);
    dl.insert(p, d);
    dl
}

/// After find_divergence_via_s3 truncates, drop the stale trigger and plant a fresh
/// one anchored at the live local tip so catchup can converge.
pub(crate) fn resume_after_truncate(
    dl: &Rc<MockDownloader>,
    lsc: Rc<LogSegmentsCache>,
    bad_trigger_path: String,
    resume_start: u64,
    resume_end: u64,
) {
    dl.on_list(2, move |dl| {
        dl.objects.borrow_mut().remove(&bad_trigger_path);
    });
    dl.on_list(3, move |dl| {
        let tip = lsc.active().metadata.borrow().write.tip_hash;
        let (p, d) = make_fallback_batch(0, resume_start, resume_end, tip);
        dl.insert(p, d);
    });
}
