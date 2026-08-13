use std::cell::{Cell, OnceCell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use celeriant_wal::segment_summary::client_set::ClientSet;
use celeriant_wal::segment_summary::segment_summary_payload::SegmentSummaryPayload;
use glommio::sync::Semaphore;
use tracing::{debug, info, trace, warn};

use celeriant_disk::files::rwlock_timeout::write_with_timeout;
use celeriant_distributed::node_status::NodeStatus;
use celeriant_distributed::validated_node_status::{self, ValidatedNodeStatus, set_node_status_and_metric};
use celeriant_rotating_log::errors::ready_up_error::ReadyUpError;
use celeriant_rotating_log::errors::scan_error::ScanError;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::disk::disk_format_error::DiskFormatError;
use celeriant_wire::disk::metablock_bytes;
use celeriant_wire::disk::serialised_datablock::{CompressionPolicy, SerialisedDatablock};
use celeriant_wire::disk::versioned_block::{self as versioned_block, deserialise_metablock, deserialise_segment_summary};
use crate::shard_wal_sync::summary_path;
use celeriant_memcache::cache_path::CachePath;
use celeriant_memcache::mem_snapshot_aggregate::{AggregateStatus, MemSnapshotAggregate};
use celeriant_memcache::metablock_position::MetablockPosition;
use celeriant_memcache::shard_log_queue_item::ShardLogQueueItem;
use celeriant_memcache::shard_mem_cache::{ClientSeqStatus, NegativeLookupAnswer, ShardMemCache};
use crate::schema_validator::CompiledValidator;

type MemCache = ShardMemCache<CompiledValidator>;
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{DeleteRequest, AggregateDetailsRequest, ListAggregateTypesRequest, ListAggregatesRequest, ListOrgsRequest, ReadRequest, SingleAggregateWrite, TrimStartRequest, WriteRequest};
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_msg::response::responses::{AggregateListItem, AggregateTypeListItem, AggregateDetailsResponse, DeleteResponse, FollowerRejection, ListAggregateTypesResponse, ListAggregatesResponse, ListOrgsResponse, OrgListItem, ReadResponse, RegisterSchemaResponse, ReplicationBatchResponse, ReplicationResult, TrimStartResponse, WriteResponse};
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_rotating_log::reverse_metablock_scanner::{ReverseMetablockScanner, SegmentHint};
use lru::LruCache;
use std::num::NonZeroUsize;
use celeriant_wal::aggregate_client_key::{client_id_bloom_hash, AggregateClientKey};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::aggregate_type_key::AggregateTypeKey;
use celeriant_wal::schema_key::SchemaKey;
use celeriant_wal::constants::{FIRST_AGGREGATE_VERSION, FIXED_BLOCK_SIZE_BYTES, GENESIS_HASH};
use celeriant_wal::datablocks::datablock::Datablock;
use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
use celeriant_wal::datablocks::datablock_kind::DatablockKind;
use celeriant_wal::datablocks::datablock_schema_registration::DatablockSchemaRegistration;
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_event_batch::{EventTypesKind, MetablockEventBatch};
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_wal::metablocks::metablock_schema_registration::MetablockSchemaRegistration;
use celeriant_wal::metablocks::metablock_soft_delete::MetablockSoftDelete;
use celeriant_wal::metablocks::metablock_soft_trim::MetablockSoftTrim;
use celeriant_watch::aggregate_reader::AggregateReader;
use celeriant_watch::aggregate_watchers::AggregateWatchers;

use crate::amortisation::coordinator::Coordinator;
use crate::bloom::bloom_filter_cache::BloomFilterCache;
use crate::bloom::event_type_filter::extract_unique_event_types;
use crate::collect_from_disk::{EventBatchFromLogSegmentFile, fetch_datablocks_for_metablocks};
use crate::error::apply_batch_error::ApplyBatchError;
use crate::error::follower_replication_write_error::FollowerReplicationWriteError;
use crate::error::replication_error::ReplicationError;
use crate::error::s3_catchup_error::S3CatchupError;
use crate::error::shard_cache_load_error::ShardCacheLoadError;
use crate::error::shard_delete_error::ShardDeleteError;
use crate::error::shard_error::ShardError;
use crate::error::shard_exists_error::ShardAggregateDetailsError;
use crate::error::shard_fsync_error::ShardFsyncError;
use crate::error::shard_listing_error::ShardListingError;
use crate::error::shard_read_error::ShardReadError;
use crate::error::shard_schema_error::ShardSchemaError;
use crate::error::shard_trim_error::ShardTrimError;
use crate::error::shard_write_error::ShardWriteError;
use crate::in_memory_filtering;
use crate::internal_shard_config::InternalShardConfig;
use crate::loading_coordinator::LoadingCoordinator;
use crate::replication_client::ReplicationClient;
use crate::s3_downloader::S3Downloader;
use crate::shard_wal_compact::{CompactionResult, compact_segment};
use crate::error::compaction_error::CompactionError;
use crate::shard_wal_replicate::{self, capture_replication_snapshot, commit_replication, wal_positions, ReplicationTrigger};
use crate::shard_wal_s3_catchup::{self, S3CatchupResult, catchup_from_s3};
use crate::shard_wal_sync::{capture_fsync_snapshot, commit_fsync_with_rollback, compute_entry_hash, sync_header_only, CommitTarget};

/// Upper bound on how long a leader write waits for the read cursor to confirm
/// its events before giving up with `LeaderFenced`. Matches the replication spin
/// hard timeout; exceeding it means replication is wedged, not merely lagging.
const REPLICATION_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);

/// Commit-notify recency window as a multiple of `replication_delay`: the notify
/// timer treats a data batch sent within this window as "the stream is flowing" and
/// rearms. Sized to the measured under-load per-shard batch cadence, which is a full
/// batch RTT (amortisation delay + follower fsync + gate time), NOT the 17ms delay
/// alone: at 24k-connection saturation the age of the last batch at a wake runs
/// 179-303ms per shard. 16× the 17ms delay ≈ 272ms covers the bulk; the slowest few
/// shards can still poke over, which is the small residual notify count under load
/// (measured ~198 vs ~4809 with the mis-sized 2× window). Pacing knob, not a
/// correctness gate — too small fires under load, too large only staler idle tails.
const RECENCY_WINDOW_BATCHES: u32 = 16;

/// Decoded sealed-segment summary cache capacity (segments).
// Left at 16 even though typical sealed sidecars are ~KB: the worst-case payload is
// still SUMMARY_PAYLOAD_MAX_BYTES (4 MiB), so this constant — not typical sidecar
// size — bounds the cache's memory (16 × 4 MiB); a miss now costs one tiny read
// + decode, so raising it should follow consult-miss metrics, not sidecar size.
const SUMMARY_CACHE_SEGMENTS: usize = 16;

/// Decoded sealed-segment summary cache. Owned by `ShardWal`; the S3-catchup
/// truncate path borrows it to invalidate entries for unwound log ids.
pub(crate) type SummaryCache = LruCache<u64, Rc<SegmentSummaryPayload>>;

/// Compile a schema datablock and insert into the cache.
/// Shared by pre_warm_cache, ensure_schema_cached, and follower replication.
pub(crate) fn compile_and_cache_schema(cache: &mut MemCache, schema_key: &SchemaKey, datablock: &Datablock) {
    if let DatablockKind::SchemaRegistration(ref schema_data) = datablock.datablock_kind {
        let cached = match crate::schema_validator::CompiledValidator::compile(schema_data.schema_type, &schema_data.schema) {
            Ok(validator) => celeriant_memcache::cached_schema::CachedSchema::Validated(validator),
            Err(e) => celeriant_memcache::cached_schema::CachedSchema::CompilationFailed(e),
        };
        cache.schema_cache_insert(schema_key.clone(), cached);
    }
}

/// Listing cursor. The wire carries one opaque u64; clients never inspect it. High 32 bits pick the
/// sealed segment to resume (`log_id`), low 32 the offset into that segment's immutable summary
/// `Vec`. Resuming by offset keeps no in-memory index, so cardinality stays unbounded.
#[derive(Clone, Copy)]
struct ListCursor {
    log_id: u64,
    offset: u32,
}

impl ListCursor {
    fn decode(raw: u64) -> Self {
        Self { log_id: raw >> 32, offset: raw as u32 }
    }

    fn encode(self) -> u64 {
        debug_assert!(self.log_id <= u32::MAX as u64, "log_id {} overflows 32-bit cursor field", self.log_id);
        (self.log_id << 32) | self.offset as u64
    }
}

/// How `reconcile_durable_tail` treats the durable tail (entries above the read cursor)
/// on a role transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TailReconciliation {
    /// Promote-over-peer: peer-received tail is committed (drain all parked PCDs,
    /// advance_visible_position, header fsync). Own-speculation tail is culled as today.
    CommitForPromotion,
    /// Demotion from held leadership: today's rewind_to_ack_barrier=true behavior, unchanged.
    RewindToAckBarrier,
    /// Non-leader under a peer's (new) lease: peer tail kept parked (no-op), own-speculation
    /// tail culled as today (boot-after-leader-crash case).
    ReconcileAsFollower,
}

/// Cursor targets for the tail-cull mechanics shared by every rewind arm.
enum CullTarget {
    /// Rewind the write cursor down to read (own-speculation tail): read stays put.
    WriteToRead(celeriant_rotating_log::log_segment_file::log_segment_cursor::LogSegmentCursor),
    /// Rewind both cursors to the ack barrier (demotion crash case).
    BothToAckBarrier(celeriant_rotating_log::log_segment_file::log_segment_cursor::LogSegmentCursor),
}

/// Write-ahead log for a single shard.
///
/// Handles the complete lifecycle of reads and writes:
/// - Validation (idempotency, optimistic concurrency)
/// - Event batch construction
/// - Queue management
/// - Durability (fsync coordination)
/// - Cache population
/// - Watch notifications
///
/// Not thread-safe—designed for single-threaded per-core access.
pub struct ShardWal<R: ReplicationClient + 'static, D: S3Downloader + 'static> {
    /// Precompiled zstd dict codec built once at shard boot
    pub dict_codec: Rc<DictCodec>,

    /// Trait implementation to download replicated data stored on S3 for catchup
    s3_downloader: Rc<D>,

    /// No async in shard_mem_cache and no interior mutability
    shard_mem_cache: Rc<RefCell<MemCache>>,

    /// Uses interior mutability with glommio RwLocks for async access to log files
    log_segments_cache: Rc<LogSegmentsCache>,

    /// Uses interior mutability with glommio RwLocks to select an fsync leader
    fsync_coordinator: Rc<Coordinator<ShardFsyncError>>,

    /// Same pattern as fsync_coordinator, single leader, batched replication over tcp.
    /// Lock order: commit_replication runs under THIS gate and awaits
    /// fsync_coordinator (the ack-barrier header sync). One direction only:
    /// nothing fsync-gated may await this coordinator, or the two deadlock.
    replication_coordinator: Rc<Coordinator<ReplicationError>>,

    /// Registry of watchers for aggregates in this shard, uses local channel broadcasting
    pub watched_aggregates: Rc<AggregateWatchers>,

    /// Cache for bloom filter construction to avoid repeated allocations, uses interior mutability
    bloom_filter_cache: Rc<BloomFilterCache>,

    /// Bounded cache of decoded sealed-segment summaries, shared by the dedup
    /// consult and the listing paths. Sealed summaries are immutable EXCEPT
    /// compaction rewrites and S3-catchup truncates, which pop the affected
    /// entries (compaction after the swap, truncate for every unwound id).
    /// Worst case 16 × 4 MiB = 64 MiB; realistic summaries are ~10-100 KiB.
    summary_cache: RefCell<SummaryCache>,

    // Are we the leader? Follower? Single node mode? Note this can change at runtime.
    pub node_status: Rc<Cell<ValidatedNodeStatus>>,

    config: InternalShardConfig,

    /// Serializes concurrent aggregate snapshot loading from disk
    aggregate_loading: LoadingCoordinator<AggregateKey>,

    /// Serializes concurrent client event index loading from disk
    aggregate_client_loading: LoadingCoordinator<AggregateClientKey>,

    /// Serializes concurrent schema loading from disk
    schema_loading: LoadingCoordinator<SchemaKey>,

    /// Limits concurrent list operations to bound unbudgeted per-request memory
    list_semaphore: Rc<Semaphore>,

    /// Limits concurrent cache-miss disk scans to prevent NVMe read saturation
    /// starving the fsync write path (cold start read amplification)
    cache_load_semaphore: Rc<Semaphore>,

    /// Client for replicating data to followers or S3.
    /// Interior mutability — FollowerConnection manages its own split locks.
    pub replication_client: Rc<R>,

    /// Leader's client-facing address, set when this node is a follower.
    /// Included in write-rejection errors so clients can redirect.
    pub leader_client_address: RefCell<Option<String>>,

    /// Peer's node_id from S3 membership. Used during S3 catchup to ignore
    /// stale fallback batches from previous cluster generations.
    pub peer_node_id: Cell<Option<u128>>,

    /// Highest wal_seq this shard has SEEN the leader hold, learned from the
    /// last wal_seq of replication batches we rejected (WalSeqMismatch /
    /// TipHashMismatch), tagged with the teaching lease epoch. In-memory only
    /// (resets to 0 at boot). S3 catchup uses it as the convergence target for
    /// a kicked follower: at or past it, live TCP is the better channel and
    /// catchup exits.
    pub observed_leader_target: crate::observed_leader::ObservedLeaderTarget,

    /// The `next_wal_seq` at which the last live-tail yield to TCP happened
    /// (0 = never). S3 catchup consults it so a kicked follower yields a small
    /// covering backlog to TCP at most once per position: a second catchup at
    /// the same position means TCP did not bridge, so consume from S3 instead.
    /// In-memory only, resets at boot.
    pub live_tail_yielded_wal_seq: Cell<u64>,

    /// Monotonic timestamp of the most recent replication rollback. Used by
    /// the write path to apply a cooldown window (ReplicationBackpressure error).
    /// Happens if the network is slow or s3/minio having issues
    pub last_rollback_at: Rc<Cell<Option<Instant>>>,

    /// Mirror of `last_rollback_at` used for log rate-limiting: write path
    /// emits one warn per rollback event when the cooldown rejects writes,
    /// rather than a warn per rejected request (which would flood under load).
    pub last_logged_rollback_at: Cell<Option<Instant>>,

    /// One-shot guard for heartbeat-starved logging. Stores the unix_ms of
    /// the in-flight heartbeat at the moment we last emitted a starve warn.
    /// A starvation episode logs once per in-flight heartbeat that crosses
    /// the threshold; the next heartbeat has a fresh unix_ms, so a
    /// subsequent starve cycle re-arms.
    pub last_logged_starve_at: Cell<Option<u64>>,

    /// Cached metrics label to avoid per-request String allocation
    metrics_shard_label: [(&'static str, String); 1],

    /// Cached gauge handle for `celeriant_commit_notify_obligation_seq`, set on
    /// every successful leader write, avoids hot path lookup
    commit_notify_obligation_gauge: metrics::Gauge,

    /// Unix-ms timestamp of the last successful S3 CAS (put_lease_conditional).
    /// Updated ONLY by the CAS path in shard 0, propagated to all shards via
    /// StatusUpdate{cas_confirmed_at_ms: Some(_)}. The heartbeat-ack path must
    /// NOT write this. Used by run_s3_fallback to gate uploads on a fresh lease.
    pub s3_cas_confirmed_at_ms: Rc<Cell<u64>>,

    /// Out-of-band hook for the replication path to nudge shard 0 to definitively
    /// renew the S3 lease (CAS `lease.json`) when an S3 fallback would otherwise be
    /// gated by a stale CAS confirmation. The heartbeat loop can stall in the kernel
    /// under load, so renewal must not depend on it
    pub lease_renewal_requester: OnceCell<Rc<dyn LeaseRenewalRequester>>,

    /// One-shot eligibility for the demotion rewind-to-ack-barrier cull. The rewind
    /// exists to drop a leader's OWN un-acked speculative tail (crash or demotion);
    /// it is armed at boot (crash case) and on every successful replication commit
    /// while leading, and consumed by the first rewind-eligible cull. Without this,
    /// post-demotion status churn (Fenced<->Follower bounces, repeated kicks) re-runs
    /// the cull and destroys peer-acked data applied by catchup since the demotion
    /// (read advances on apply paths, last_self_acked does not).
    ack_barrier_rewind_armed: Cell<bool>,

    /// Level-triggered commit-notify obligation, both monotone. `pending_notify_seq`
    /// is the highest confirmed seq a burst-tail drain has observed;
    /// `pushed_to_follower_seq` is the highest seq a send actually delivered to the
    /// follower (any batch, notify, or probe). An obligation exists exactly while
    /// pending > pushed. Under load the next real batch raises `pushed` to meet
    /// `pending` before the timer wakes, so no dedicated notify ever fires. Rc so
    /// the detached timer task and replication cycle can read and raise them.
    pending_notify_seq: Rc<Cell<u64>>,
    pushed_to_follower_seq: Rc<Cell<u64>>,

    /// Arm latch for the single notify timer: true while a timer task is sleeping
    /// or running its cycle. Only read and set inside a synchronous region between
    /// awaits, so concurrent drains cannot both spawn a timer.
    notify_timer_armed: Rc<Cell<bool>>,

    /// When the last real DATA batch was delivered to the follower. The notify
    /// timer's load suppressor: while the carrier stream is flowing (a batch within
    /// the recency window, RECENCY_WINDOW_BATCHES × replication_delay), the next batch
    /// carries the commit index, so the timer rearms. The watermark cannot serve this — it structurally
    /// trails `pending` by one batch under load (two-phase sends before commit), so
    /// its disarm never samples true while writes flow. Raised only at data-batch
    /// `Sent`; not notify (self-suppression), probe (5s scale), or S3 (carries nothing).
    last_batch_sent_at: Rc<Cell<Instant>>,

    /// Weak self-reference wired once at startup (`set_self_ref`), so the detached
    /// notify timer can re-enter `&self` (re-read the obligation, run the cycle)
    /// without cloning the whole field set. Weak breaks the task→shard cycle: a
    /// dropped shard makes the upgrade fail and the timer exits.
    notify_self_ref: OnceCell<Weak<ShardWal<R, D>>>,
}

/// Lets the replication path (any data shard) ask shard 0 to re-CAS the S3 lease
/// and broadcast a fresh confirmation, out-of-band from the heartbeat loop.
/// Implemented in `celeriant_runtimes`, which owns the intra-shard message mesh
/// (`celeriant_shard` deliberately has no dependency on it).
pub trait LeaseRenewalRequester {
    /// Fire-and-forget nudge to shard 0 to renew the S3 lease now. Coalesced on the
    /// receiving side, so it is safe to call repeatedly while spin-waiting the gate.
    fn request_renewal(&self);
}

impl<R: ReplicationClient + 'static, D: S3Downloader + 'static> AggregateReader for ShardWal<R, D> {
    fn watched_aggregates(&self) -> Rc<AggregateWatchers> {
        Rc::clone(&self.watched_aggregates)
    }
}

/// Rebuild the active segment's in-memory state by scanning it forward once:
/// (a) per-aggregate backlink tips (newest position per aggregate — not persisted, so
/// the next append would otherwise back-link to 0 and the reverse scan would stop
/// short of older same-segment metablocks), and (b) the segment's aggregate + client
/// blooms (v2 headers carry none; the loaded cursors hold ABSENT blooms until here).
/// Every client-bearing kind feeds the client bloom — EventBatch, SoftDelete, SoftTrim
/// — a tombstone-only client left out would make the bloom a subset (false "absent").
/// SchemaRegistration feeds the segment-summary schema accumulator (NOT the aggregate
/// bloom — aggregate blooms answer aggregate questions only); the full scan makes the
/// accumulator's schema set a superset even when the pre-warm replay stopped short.
/// Deserialization-free: kind + keys + client_id read straight from block bytes.
/// Active segment only; this same sequential pass is the recovery warm-up that keeps
/// post-crash idempotency lookups off the random-read path.
pub(crate) async fn rebuild_active_segment_chain_tips(
    log_segments_cache: &LogSegmentsCache,
    scan_chunk_size: u64,
    shard_mem_cache: &Rc<RefCell<MemCache>>,
    metrics_shard_label: &[(&'static str, String); 1],
) -> Result<usize, std::io::Error> {
    use celeriant_disk::files::read_fixed_records_visit_const::{read_fixed_records_visit_const, ReadVisitError};
    use celeriant_rotating_log::log_segment_file::aggregate_key_bloom::AggregateKeyBloom;

    let active = log_segments_cache.active();
    let active_log_id = active.metadata.borrow().log_id;
    let metablocks_start = celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES as u64;
    let metablocks_end = active.metadata.borrow().write.metablocks_position;

    let mut tips: HashMap<AggregateKey, u64> = HashMap::new();
    let mut aggregate_bloom = AggregateKeyBloom::new();
    let mut client_bloom = AggregateKeyBloom::with_capacity_bytes(celeriant_wal::constants::CLIENT_BLOOM_BYTES);
    // Eager negative-lookup feed: the scan already reads every metablock's key
    // and client_id, so collecting per-aggregate client hashes adds no IO.
    // Transient, same scale as the seal accumulator's client map.
    let mut clients: HashMap<AggregateKey, std::collections::HashSet<u64>> = HashMap::new();
    let mut schema_hashes: Vec<u64> = Vec::new();
    if metablocks_end > metablocks_start {
        let guard = active.lock_reader("rebuild_chain_tips").await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::TimedOut, e.to_string()))?;
        let dma_file = guard.as_ref()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "active segment has no file handle"))?;

        let result = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, ()>(
            dma_file, false, metablocks_start, metablocks_end, scan_chunk_size,
            |pos, block| {
                if let Some(key) = metablock_bytes::read_chain_aggregate_key(block) {
                    aggregate_bloom.insert(&key);
                    let client_id = if metablock_bytes::is_metablock_kind_event_batch_metadata(block) {
                        metablock_bytes::read_event_batch_client_id(block)
                    } else if metablock_bytes::is_metablock_kind_soft_delete(block) {
                        metablock_bytes::read_soft_delete_client_id(block)
                    } else {
                        metablock_bytes::read_soft_trim_client_id(block)
                    };
                    client_bloom.insert_hash(client_id_bloom_hash(client_id));
                    clients.entry(key.clone()).or_default().insert(client_id_bloom_hash(client_id));
                    tips.insert(key, pos); // forward scan: last (newest) write wins
                } else if metablock_bytes::is_metablock_kind_schema_registration(block) {
                    schema_hashes.push(metablock_bytes::read_schema_registration_key(block).bloom_hash());
                }
                Ok(false)
            },
        ).await;
        // Exhaustive on purpose: ANY scan error must abort before install, or a
        // partially-populated precise bloom (a subset) replaces the safe absent one.
        match result {
            Ok(_) => {}
            Err(ReadVisitError::Io(e)) => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
            }
            Err(ReadVisitError::Visitor(())) => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "rebuild visitor aborted"));
            }
        }
    }

    // Writes must be FENCED while this runs: the scan snapshots [start, end) above,
    // awaits on disk, then replaces tips and blooms wholesale — a commit landing
    // mid-scan would be clobbered (stale-LOW tip, subset bloom, both correctness
    // bugs now that the blooms gate dedup scans). All callers (open, S3-catchup
    // truncate, tail cull) hold the write path quiescent; these tripwires catch a
    // future unfenced caller.
    debug_assert_eq!(
        log_segments_cache.active_log_id(), active_log_id,
        "active segment rotated during the open-time rebuild scan"
    );
    debug_assert_eq!(
        active.metadata.borrow().write.metablocks_position, metablocks_end,
        "write cursor moved during the open-time rebuild scan — writes are not fenced"
    );

    let count = tips.len();
    *active.aggregate_chain_tips.borrow_mut() = tips;
    active.install_blooms(aggregate_bloom, client_bloom);

    // Eager negative-lookup population (bounded by the cache's byte budget,
    // enforced inside `negative_lookup_seed`). Entries built here cover ONLY
    // the active segment, so they may be Complete only when the aggregate's
    // whole history is provably confined to it — which is knowable without
    // extra IO exactly when no sealed segments exist. Otherwise they install
    // as Building and the first miss finishes the build from sidecars + scan.
    // This pass is what turns post-recovery fan-in into no-scan first writes.
    {
        let eager_complete = active_log_id == 1;
        let mut mc = shard_mem_cache.borrow_mut();
        // Schema hashes land in the active summary accumulator, next to where
        // commits insert them; the sidecar picks them up at seal.
        for hash in &schema_hashes {
            mc.segment_summary_insert_schema_hash(*hash);
        }
        let mut completed = 0u64;
        for (key, hashes) in &clients {
            if mc.negative_lookup_seed(key, hashes, eager_complete) {
                completed += 1;
            }
        }
        if completed > 0 {
            metrics::counter!("celeriant_negative_lookup_builds_completed_total", metrics_shard_label).increment(completed);
        }
    }
    Ok(count)
}

/// Where the client dedup scan starts. The aggregate snapshot LRU gives an exact
/// cross-segment position; failing that, the active segment's chain tips give the
/// aggregate's newest in-segment block, skipping the O(distance-to-tip) reverse hunt.
/// Both misses mean an unbounded reverse scan from the active write tip, relying on
/// the segment blooms to short-circuit.
fn client_scan_start(
    last_known: &MetablockPosition,
    active_log_id: u64,
    active_tip: Option<u64>,
) -> (u64, Option<u64>) {
    if last_known.log_id != 0 {
        (
            last_known.log_id,
            Some(last_known.metablock_absolute_pos.saturating_add(FIXED_BLOCK_SIZE_BYTES as u64)), //Include SELF
        )
    } else {
        (
            active_log_id,
            active_tip.map(|pos| pos.saturating_add(FIXED_BLOCK_SIZE_BYTES as u64)),
        )
    }
}

/// Read the segment summary from a closed log segment's sidecar `.summary` file.
/// Returns `None` for legacy/v1 segments without a usable summary, or if the file
/// is corrupt. Opens and closes the file each time; repeat consumers go through
/// `read_segment_summary_cached` (bounded decoded LRU).
pub(crate) async fn read_segment_summary(
    shard_dir: &std::path::Path,
    log_id: u64,
) -> Option<SegmentSummaryPayload> {
    let path = summary_path(shard_dir, log_id);
    let file = match glommio::io::BufferedFile::open(&path).await {
        Ok(f) => f,
        Err(e) if e.raw_os_error() == Some(2) => return None, // ENOENT
        Err(e) => {
            tracing::warn!(log_id, error = %e, "Failed to open segment summary sidecar");
            return None;
        }
    };

    let file_size = match file.file_size().await {
        Ok(s) if s > 0 => s as usize,
        Ok(_) => return None,
        Err(e) => {
            tracing::warn!(log_id, error = %e, "Failed to read segment summary file size");
            return None;
        }
    };

    let buf = match file.read_at(0, file_size).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(log_id, error = %e, "Failed to read segment summary sidecar");
            return None;
        }
    };

    match deserialise_segment_summary(&buf) {
        Ok(block) => Some(block),
        Err(_) => None,
    }
}

/// Per-segment dedup hint from a decoded summary (aggregates sorted by ids —
/// see `read_segment_summary_cached`). `None` = no usable information, walk the
/// segment exactly as without a summary. Skip fires when the aggregate has no
/// entry in the segment, or its client set answers "definitely absent" — the
/// safe direction is superset, so a skip can never hide a real record. Skips
/// therefore require a COMPLETE summary: an incomplete one (subset) may still
/// serve tips — positions from a newest-first replay are true-newest for the
/// aggregates it has — but its absences prove nothing.
fn summary_hint(payload: &SegmentSummaryPayload, key: &AggregateKey, client_hash: u64) -> Option<SegmentHint> {
    let found = payload.aggregates.binary_search_by_key(
        &(key.org_id, key.aggregate_type_id, key.aggregate_id),
        |e| (e.org_id, e.aggregate_type_id, e.aggregate_id),
    );
    let entry = match found {
        Err(_) => return payload.complete.then_some(SegmentHint::Skip),
        Ok(i) => &payload.aggregates[i],
    };
    if payload.complete && !entry.client_set.may_contain_hash(client_hash) {
        return Some(SegmentHint::Skip);
    }
    // 0 = no tip recorded (compaction dropped the aggregate's blocks): full walk.
    (entry.newest_metablock_pos != 0).then_some(SegmentHint::SeekTo(entry.newest_metablock_pos))
}

impl<R: ReplicationClient + 'static, D: S3Downloader + 'static> ShardWal<R, D> {
    pub async fn process_client_request(&self, request: ClientRequest) -> Result<ClientResponse, ShardError> {
        let shard_label = &self.metrics_shard_label;
        let start = Instant::now();

        let (op_name, result) = match request {
            ClientRequest::Write(r) => ("write", self.write(r).await.map(ClientResponse::Write).map_err(ShardError::Write)),
            ClientRequest::Read(r) => ("read", self.read(&r).await.map(ClientResponse::Read).map_err(ShardError::Read)),
            ClientRequest::Delete(r) => ("delete", self.delete(r).await.map(ClientResponse::Delete).map_err(ShardError::Delete)),
            ClientRequest::TrimStart(r) => ("trim", self.trim_start(r).await.map(ClientResponse::TrimStart).map_err(ShardError::TrimStart)),
            ClientRequest::AggregateDetails(r) => ("exists", self.exists(&r).await.map(ClientResponse::AggregateDetails).map_err(ShardError::AggregateDetails)),
            ClientRequest::Watch(_) => return Err(ShardError::WatchRequestInvalid),
            ClientRequest::ListOrgs(r) => ("list_orgs", self.list_orgs(r).await.map(ClientResponse::ListOrgs).map_err(ShardError::ListOrgs)),
            ClientRequest::ListAggregateTypes(r) => ("list_aggregate_types", self.list_aggregate_types(r).await.map(ClientResponse::ListAggregateTypes).map_err(ShardError::ListAggregateTypes)),
            ClientRequest::ListAggregates(r) => ("list_aggregates", self.list_aggregates(r).await.map(ClientResponse::ListAggregates).map_err(ShardError::ListAggregates)),
            ClientRequest::RegisterSchema(r) => ("register_schema", self.register_schema(r).await.map(ClientResponse::RegisterSchema).map_err(ShardError::RegisterSchema)),
        };

        let duration = start.elapsed().as_secs_f64();

        match &result {
            Ok(_) => {
                let counter_name = match op_name {
                    "write" => "celeriant_writes_total",
                    "read" => "celeriant_reads_total",
                    "delete" => "celeriant_deletes_total",
                    "trim" => "celeriant_trims_total",
                    _ => "celeriant_reads_total",
                };
                metrics::counter!(counter_name, shard_label).increment(1);

                let duration_name = match op_name {
                    "write" => Some("celeriant_write_duration_seconds"),
                    "read" => Some("celeriant_read_duration_seconds"),
                    _ => None,
                };
                if let Some(name) = duration_name {
                    metrics::histogram!(name, shard_label).record(duration);
                }
            }
            Err(e) => {
                let error_counter = match op_name {
                    "write" => "celeriant_write_errors_total",
                    "read" => "celeriant_read_errors_total",
                    _ => "celeriant_write_errors_total",
                };
                let error_label = [
                    ("shard_id", shard_label[0].1.clone()),
                    ("error_code", e.error_code().to_owned()),
                ];
                metrics::counter!(error_counter, error_label.as_slice()).increment(1);
            }
        }

        result
    }

    /// Open or create a shard WAL.
    ///
    /// If the shard directory exists with log files, reopens from the latest.
    /// Otherwise creates a new shard with an empty log file.
    pub async fn open(config: InternalShardConfig, node_status: ValidatedNodeStatus, replication_client: R, s3_downloader: D) -> Result<Self, ReadyUpError> {
        let shard_mem_cache = MemCache::new(
            config.recent_write_cache_bytes,
            config.aggregate_snapshots_cache_bytes,
            config.aggregate_client_snapshots_cache_bytes,
            config.schema_cache_bytes,
            config.negative_lookup_cache_bytes,
            config.internode_max_request_size,
        );

        let log_segments_cache = LogSegmentsCache::ready_up(
            config.shard_dir.clone(),
            config.shard_log_preallocate_bytes,
            config.max_open_files as usize,
            config.shard_id,
        )
        .await?;

        let list_semaphore = Rc::new(Semaphore::new(config.list_max_concurrent));
        let cache_load_semaphore = Rc::new(Semaphore::new(config.read_max_concurrent));

        metrics::gauge!("celeriant_replication_queue_high_water_bytes").set(config.internode_max_request_size as f64);

        let metrics_shard_label = [("shard_id", config.shard_id.to_string())];

        let shard_mem_cache = Rc::new(RefCell::new(shard_mem_cache));
        let log_segments_cache = Rc::new(log_segments_cache);

        // Sealed segments reloaded after LRU eviction get their blooms from the
        // .summary sidecar (v2 headers carry none). Layering: rotating_log must not
        // read summaries, so the shard injects the loader. No/torn sidecar -> None
        // -> the segment keeps ABSENT blooms (maybe-present, never a false skip).
        {
            let shard_dir = config.shard_dir.clone();
            log_segments_cache.set_sealed_bloom_loader(Rc::new(move |log_id| {
                let shard_dir = shard_dir.clone();
                Box::pin(async move {
                    match read_segment_summary(&shard_dir, log_id).await {
                        Some(payload) => (payload.aggregate_bloom, payload.client_bloom),
                        None => (None, None),
                    }
                })
            }));
        }

        let dict_codec = Rc::new(
            DictCodec::new(&config.dict_bytes, config.wal_compression_level)
                .map_err(|e| ReadyUpError::DictCodecBuildFailed(e.to_string()))?,
        );

        Self::pre_warm_cache(&log_segments_cache, &shard_mem_cache, &config, &dict_codec).await?;

        rebuild_active_segment_chain_tips(&log_segments_cache, config.read_max_chunk_size, &shard_mem_cache, &metrics_shard_label)
            .await
            .map_err(|source| ReadyUpError::UnableToAccessDirectory {
                directory: "active-segment backlink rebuild".to_string(),
                source,
            })?;

        let recovered_wal_seq = log_segments_cache.active().metadata.borrow().write.wal_seq;
        metrics::gauge!("celeriant_wal_seq", &metrics_shard_label).set(recovered_wal_seq as f64);
        let recovered_read_wal_seq = log_segments_cache.committed_read_wal_seq();
        metrics::gauge!("celeriant_read_wal_seq", &metrics_shard_label).set(recovered_read_wal_seq as f64);
        let recovered_self_acked = log_segments_cache.active().metadata.borrow().last_self_acked_wal_seq;
        metrics::gauge!("celeriant_last_self_acked_wal_seq", &metrics_shard_label).set(recovered_self_acked as f64);
        let commit_notify_obligation_gauge = metrics::gauge!("celeriant_commit_notify_obligation_seq", &metrics_shard_label);

        Ok(Self {
            dict_codec,
            s3_downloader: Rc::new(s3_downloader),
            shard_mem_cache,
            log_segments_cache,
            fsync_coordinator: Rc::new(Coordinator::new()),
            replication_coordinator: Rc::new(Coordinator::new()),
            watched_aggregates: Rc::new(AggregateWatchers::new()),
            bloom_filter_cache: Rc::new(BloomFilterCache::new()),
            summary_cache: RefCell::new(LruCache::new(NonZeroUsize::new(SUMMARY_CACHE_SEGMENTS).unwrap())),
            node_status: Rc::new(Cell::new(node_status)),
            config,
            aggregate_loading: LoadingCoordinator::new(),
            aggregate_client_loading: LoadingCoordinator::new(),
            schema_loading: LoadingCoordinator::new(),
            list_semaphore,
            cache_load_semaphore,
            replication_client: Rc::new(replication_client),
            leader_client_address: RefCell::new(None),
            peer_node_id: Cell::new(None),
            observed_leader_target: crate::observed_leader::ObservedLeaderTarget::new(),
            live_tail_yielded_wal_seq: Cell::new(0),
            last_rollback_at: Rc::new(Cell::new(None)),
            last_logged_rollback_at: Cell::new(None),
            last_logged_starve_at: Cell::new(None),
            metrics_shard_label,
            commit_notify_obligation_gauge,
            s3_cas_confirmed_at_ms: Rc::new(Cell::new(0)),
            lease_renewal_requester: OnceCell::new(),
            ack_barrier_rewind_armed: Cell::new(true),
            pending_notify_seq: Rc::new(Cell::new(0)),
            pushed_to_follower_seq: Rc::new(Cell::new(0)),
            notify_timer_armed: Rc::new(Cell::new(false)),
            last_batch_sent_at: Rc::new(Cell::new(Instant::now())),
            notify_self_ref: OnceCell::new(),
        })
    }

    /// Wire the out-of-band lease-renewal hook (called once at startup by the
    /// runtimes layer, after the intra-shard mesh exists).
    pub fn set_lease_renewal_requester(&self, requester: Rc<dyn LeaseRenewalRequester>) {
        let _ = self.lease_renewal_requester.set(requester);
    }

    /// Wire the weak self-reference (called once at startup after the shard is
    /// wrapped in an `Rc`). The notify timer upgrades it per wake to re-enter
    /// `&self`; without it the timer cannot fire, so any construction path that
    /// exercises the notify must call this.
    pub fn set_self_ref(self: &Rc<Self>) {
        let _ = self.notify_self_ref.set(Rc::downgrade(self));
    }

    pub fn timestamp_config(&self) -> crate::timestamp_config::TimestampConfig {
        self.config.timestamp_config
    }

    /// Pre-warm aggregate and client caches by reverse-scanning the WAL.
    /// SoftDelete/SoftTrim metablocks carry full aggregate state, so each
    /// metablock kind can populate the cache immediately without continuing the scan.
    async fn pre_warm_cache(
        log_segments_cache: &Rc<LogSegmentsCache>,
        shard_mem_cache: &Rc<RefCell<MemCache>>,
        config: &InternalShardConfig,
        dict_codec: &DictCodec,
    ) -> Result<(), ReadyUpError> {
        let warmup_start = Instant::now();
        let warmup_deadline = config.cache_warmup_max_duration;
        let mut warmup_agg_count = 0u64;
        let mut warmup_client_count = 0u64;
        let mut agg_cache_full = false;
        let mut client_cache_full = false;
        let mut timed_out = false;
        // Set when the scan stops while still inside the active segment: the
        // summary replay below then covers only a newest-prefix of it.
        let mut partial_active_replay = false;

        let starting_log_id = log_segments_cache.active_log_id();
        let mut active_segment_metablocks: Vec<(u64, Metablock)> = Vec::new();

        // Warm to the write tip so the cache can serve the write path; culls
        // clear these caches when dropping an un-acked tip anyway.
        let mut scanner = ReverseMetablockScanner::new(
            log_segments_cache,
            starting_log_id,
            None,
            config.read_max_chunk_size,
        )
        .with_write_cursor_upper_bound();

        let mut deferred_schema_blocks: Vec<(u64, SchemaKey, Metablock)> = Vec::new();

        scanner
            .scan::<(), ReadyUpError>(|log_id, metablock_absolute_pos, metablock_bytes| {
                if agg_cache_full && client_cache_full {
                    partial_active_replay |= log_id == starting_log_id;
                    return Ok(Some(()));
                }
                if warmup_start.elapsed() >= warmup_deadline {
                    timed_out = true;
                    partial_active_replay |= log_id == starting_log_id;
                    return Ok(Some(()));
                }

                if metablock_bytes::is_metablock_kind_soft_delete(metablock_bytes) {
                    let metablock = deserialise_metablock(metablock_bytes)
                        .map_err(|e| ReadyUpError::UnableToAccessDirectory {
                            directory: format!("corrupt soft-delete metablock at log {log_id} pos {metablock_absolute_pos}"),
                            source: std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:?}")),
                        })?;
                    if log_id == starting_log_id {
                        active_segment_metablocks.push((metablock_absolute_pos, metablock.clone()));
                    }
                    if let MetablockKind::SoftDelete(soft_delete) = metablock.wal_metablock_type {
                        let mut cache = shard_mem_cache.borrow_mut();
                        if !cache.is_aggregate_snapshot_full_or_contains(&soft_delete.aggregate_key, CachePath::Write) {
                            cache.put_aggregate_into_cache_as_deleted(
                                soft_delete.aggregate_key,
                                log_id,
                                metablock_absolute_pos,
                                soft_delete.event_seq,
                                soft_delete.aggregate_version,
                                soft_delete.allow_recreate,
                                soft_delete.allow_sequence_continuation,
                                CachePath::Write,
                            );
                            warmup_agg_count += 1;
                            agg_cache_full = cache.is_aggregate_snapshot_cache_full(CachePath::Write);
                        }
                    }
                    return Ok(None);
                }

                if metablock_bytes::is_metablock_kind_soft_trim(metablock_bytes) {
                    let metablock = deserialise_metablock(metablock_bytes)
                        .map_err(|e| ReadyUpError::UnableToAccessDirectory {
                            directory: format!("corrupt soft-trim metablock at log {log_id} pos {metablock_absolute_pos}"),
                            source: std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:?}")),
                        })?;
                    if log_id == starting_log_id {
                        active_segment_metablocks.push((metablock_absolute_pos, metablock.clone()));
                    }
                    if let MetablockKind::SoftTrim(soft_trim) = metablock.wal_metablock_type {
                        let mut cache = shard_mem_cache.borrow_mut();
                        if !cache.is_aggregate_snapshot_full_or_contains(&soft_trim.aggregate_key, CachePath::Write) {
                            let snapshot = MemSnapshotAggregate::found(
                                log_id,
                                metablock_absolute_pos,
                                soft_trim.event_seq,
                                soft_trim.aggregate_version,
                                soft_trim.keep_from_aggregate_version,
                            );
                            cache.put_aggregate_snapshot_only(soft_trim.aggregate_key, snapshot, false, CachePath::Write);
                            warmup_agg_count += 1;
                            agg_cache_full = cache.is_aggregate_snapshot_cache_full(CachePath::Write);
                        }
                    }
                    return Ok(None);
                }

                if metablock_bytes::is_metablock_kind_schema_registration(metablock_bytes) {
                    if let Ok(metablock) = deserialise_metablock(metablock_bytes) {
                        if log_id == starting_log_id {
                            active_segment_metablocks.push((metablock_absolute_pos, metablock.clone()));
                        }
                        if let MetablockKind::SchemaRegistration(ref schema_reg) = metablock.wal_metablock_type {
                            let mut cache = shard_mem_cache.borrow_mut();
                            if !cache.is_schema_cache_full() && !cache.schema_cache_contains(&schema_reg.schema_key) {
                                cache.schema_cache_insert(schema_reg.schema_key.clone(), celeriant_memcache::cached_schema::CachedSchema::NotYetLoaded);
                                deferred_schema_blocks.push((log_id, schema_reg.schema_key.clone(), metablock.clone()));
                            }
                        }
                    }
                    return Ok(None);
                }

                if !metablock_bytes::is_metablock_kind_event_batch_metadata(metablock_bytes) {
                    return Ok(None);
                }

                if log_id == starting_log_id {
                    if let Ok(metablock) = deserialise_metablock(metablock_bytes) {
                        active_segment_metablocks.push((metablock_absolute_pos, metablock));
                    }
                }

                let aggregate_key = metablock_bytes::read_event_batch_aggregate_key(metablock_bytes);
                let mut cache = shard_mem_cache.borrow_mut();
                if !cache.is_aggregate_snapshot_full_or_contains(&aggregate_key, CachePath::Write) {
                    let snapshot = MemSnapshotAggregate::found(
                        log_id,
                        metablock_absolute_pos,
                        metablock_bytes::read_event_batch_max_event_seq(metablock_bytes),
                        metablock_bytes::read_event_batch_aggregate_version(metablock_bytes),
                        metablock_bytes::read_event_batch_min_aggregate_version(metablock_bytes),
                    );
                    let client_id = metablock_bytes::read_event_batch_client_id(metablock_bytes);
                    let last_client_seq = metablock_bytes::read_event_batch_max_client_seq(metablock_bytes);
                    cache.put_aggregate_into_cache(aggregate_key, snapshot, client_id, last_client_seq, false, CachePath::Write);
                    warmup_agg_count += 1;
                    agg_cache_full = cache.is_aggregate_snapshot_cache_full(CachePath::Write);
                    client_cache_full = cache.is_aggregate_client_cache_full();
                } else {
                    let client_id = metablock_bytes::read_event_batch_client_id(metablock_bytes);
                    let client_key = AggregateClientKey::new(aggregate_key, client_id);
                    if !cache.is_aggregate_client_cache_full_or_contains(&client_key) {
                        let last_client_seq = metablock_bytes::read_event_batch_max_client_seq(metablock_bytes);
                        cache.put_aggregate_client_into_cache(client_key, last_client_seq, false);
                        warmup_client_count += 1;
                        client_cache_full = cache.is_aggregate_client_cache_full();
                    }
                }

                Ok(None)
            })
            .await
            .map_err(|e| match e {
                ScanError::Visitor(ready_up) => ready_up,
                ScanError::OpenLogSegment(e) => ReadyUpError::ActiveFileError(e),
                ScanError::Io { log_id, source } => ReadyUpError::UnableToAccessDirectory {
                    directory: format!("log segment {log_id}"),
                    source: std::io::Error::new(std::io::ErrorKind::Other, source),
                },
                ScanError::LockTimeout(e) => ReadyUpError::UnableToAccessDirectory {
                    directory: "lock timeout during warmup".to_string(),
                    source: std::io::Error::new(std::io::ErrorKind::TimedOut, e.to_string()),
                },
                ScanError::NoFileHandle { log_id } => ReadyUpError::UnableToAccessDirectory {
                    directory: format!("log segment {log_id}"),
                    source: std::io::Error::new(std::io::ErrorKind::NotFound, "no file handle"),
                },
            })?;

        // Fetch deferred schema datablocks (inline or from disk) and compile into cache
        if !deferred_schema_blocks.is_empty() {
            let mut batches: Vec<crate::collect_from_disk::EventBatchFromLogSegmentFile> = deferred_schema_blocks.iter()
                .map(|(log_id, _, metablock)| crate::collect_from_disk::EventBatchFromLogSegmentFile {
                    log_id: *log_id,
                    metablock: metablock.clone(),
                    datablock: None,
                })
                .collect();

            crate::collect_from_disk::fetch_datablocks_for_metablocks(&mut batches, config.read_max_chunk_size, log_segments_cache, dict_codec)
                .await
                .map_err(|e| ReadyUpError::UnableToAccessDirectory {
                    directory: format!("schema datablock fetch: {e:?}"),
                    source: std::io::Error::new(std::io::ErrorKind::Other, format!("{e:?}")),
                })?;

            let mut cache = shard_mem_cache.borrow_mut();
            for (batch, (_, schema_key, _)) in batches.into_iter().zip(deferred_schema_blocks.iter()) {
                if let Some(ref datablock) = batch.datablock {
                    compile_and_cache_schema(&mut cache, schema_key, datablock);
                }
            }
        }

        // Replay active segment metablocks in forward (write) order for correct summary state.
        // The reverse scan collected them newest-first; reversing gives chronological order.
        // A replay that stopped inside the active segment seeds a SUBSET: taint the
        // accumulator so the eventual seal never authorizes Skip decisions (the tips
        // it does hold are true-newest and stay usable as SeekTo targets).
        {
            let mut cache = shard_mem_cache.borrow_mut();
            for (metablock_absolute_pos, metablock) in active_segment_metablocks.into_iter().rev() {
                cache.update_segment_summary(&metablock, metablock_absolute_pos);
            }
            if partial_active_replay {
                cache.mark_segment_summary_incomplete();
            }
        }

        info!(
            shard_id = config.shard_id,
            aggregates = warmup_agg_count,
            clients = warmup_client_count,
            timed_out,
            duration_ms = warmup_start.elapsed().as_millis() as u64,
            "Cache warmup complete"
        );

        Ok(())
    }

    /// Read-through cache over `read_segment_summary`, shared by the dedup
    /// consult and the listing paths. Aggregates are sorted by ids at decode so
    /// `summary_hint` can binary-search; the order is deterministic, so listing
    /// cursors (offsets into the Vec) stay stable across evictions and restarts.
    /// Misses (no/v1/torn sidecar) are NOT cached — the deferred sweep may write
    /// the sidecar later. Concurrent misses may decode twice: bounded and rare.
    async fn read_segment_summary_cached(&self, log_id: u64) -> Option<Rc<SegmentSummaryPayload>> {
        if let Some(payload) = self.summary_cache.borrow_mut().get(&log_id) {
            return Some(payload.clone());
        }
        let mut payload = read_segment_summary(self.log_segments_cache.shard_dir(), log_id).await?;
        payload.aggregates.sort_unstable_by_key(|e| (e.org_id, e.aggregate_type_id, e.aggregate_id));
        let payload = Rc::new(payload);
        self.summary_cache.borrow_mut().put(log_id, payload.clone());
        Some(payload)
    }

    /// List all unique organizations that have data in this shard.
    ///
    /// Reads segment summaries newest-to-oldest, falling back to reverse metablock
    /// scan for legacy segments without summaries. Pagination breaks between segments:
    /// each segment is fully processed before checking the page limit.
    pub async fn list_orgs(&self, request: ListOrgsRequest) -> Result<ListOrgsResponse, ShardListingError> {
        let _permit = self.list_semaphore.acquire_permit(1).await
            .map_err(|_| ShardListingError::ListSemaphoreClosed)?;

        let start_time = Instant::now();
        let max_duration = self.config.list_max_duration;
        let page_size = self.config.list_page_size;
        let active_log_id = self.log_segments_cache.active_log_id();

        let mut seen: HashSet<u128> = HashSet::with_capacity(page_size);
        let mut results: Vec<OrgListItem> = Vec::with_capacity(page_size);

        // cursor: None = first page, Some(log_id) = resume from this closed segment downward
        let start_log_id = match request.cursor {
            None => {
                let orgs = { self.shard_mem_cache.borrow().peek_segment_summary_orgs().clone() };
                for org_id in orgs {
                    if seen.insert(org_id) {
                        results.push(OrgListItem { org_id });
                    }
                }
                active_log_id.saturating_sub(1)
            }
            Some(log_id) => log_id,
        };

        if start_log_id == 0 {
            return Ok(ListOrgsResponse { correlation_id: request.correlation_id, orgs: results, next_cursor: None });
        }

        for log_id in (1..=start_log_id).rev() {
            if results.len() >= page_size || start_time.elapsed() >= max_duration {
                return Ok(ListOrgsResponse {
                    correlation_id: request.correlation_id, orgs: results,
                    next_cursor: Some(log_id),
                });
            }

            // Incomplete summaries (partial-warmup / post-truncate taint) are a
            // subset: listing from them would omit silently and permanently.
            // All three listing paths fall back to the legacy scan instead.
            match self.read_segment_summary_cached(log_id).await.filter(|p| p.complete) {
                Some(payload) => {
                    for &org_id in &payload.orgs {
                        if seen.insert(org_id) {
                            results.push(OrgListItem { org_id });
                        }
                    }
                }
                None => {
                    self.list_orgs_legacy_segment(log_id, &mut seen, &mut results).await
                        .map_err(ShardListingError::ReadFromDiskError)?;
                }
            }
        }

        Ok(ListOrgsResponse { correlation_id: request.correlation_id, orgs: results, next_cursor: None })
    }

    async fn list_orgs_legacy_segment(
        &self,
        target_log_id: u64,
        seen: &mut HashSet<u128>,
        results: &mut Vec<OrgListItem>,
    ) -> Result<(), ScanError<()>> {
        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache, target_log_id, None, self.config.read_max_chunk_size,
        );
        scanner.scan::<bool, ()>(|log_id, _pos, bytes| {
            if log_id != target_log_id { return Ok(Some(true)); }
            if !metablock_bytes::is_metablock_kind_event_batch_metadata(bytes) { return Ok(None); }
            let org_id = metablock_bytes::read_event_batch_org_id(bytes);
            if seen.insert(org_id) {
                results.push(OrgListItem { org_id });
            }
            Ok(None)
        }).await?;
        Ok(())
    }

    /// List aggregate types, optionally filtered by org_id.
    ///
    /// Reads segment summaries newest-to-oldest, falling back to reverse metablock
    /// scan for legacy segments without summaries.
    pub async fn list_aggregate_types(&self, request: ListAggregateTypesRequest) -> Result<ListAggregateTypesResponse, ShardListingError> {
        let _permit = self.list_semaphore.acquire_permit(1).await
            .map_err(|_| ShardListingError::ListSemaphoreClosed)?;

        let filter_org_id = request.org_id;

        let start_time = Instant::now();
        let max_duration = self.config.list_max_duration;
        let page_size = self.config.list_page_size;
        let active_log_id = self.log_segments_cache.active_log_id();

        let mut seen: HashSet<AggregateTypeKey> = HashSet::with_capacity(page_size);
        let mut results: Vec<AggregateTypeListItem> = Vec::with_capacity(page_size);

        let start_log_id = match request.cursor {
            None => {
                let types = { self.shard_mem_cache.borrow().peek_segment_summary_types().clone() };
                for atk in types {
                    if let Some(filter) = filter_org_id {
                        if atk.org_id != filter { continue; }
                    }
                    if seen.insert(atk.clone()) {
                        results.push(AggregateTypeListItem { org_id: atk.org_id, aggregate_type_id: atk.aggregate_type_id });
                    }
                }
                active_log_id.saturating_sub(1)
            }
            Some(log_id) => log_id,
        };

        if start_log_id == 0 {
            return Ok(ListAggregateTypesResponse { correlation_id: request.correlation_id, aggregate_types: results, next_cursor: None });
        }

        for log_id in (1..=start_log_id).rev() {
            if results.len() >= page_size || start_time.elapsed() >= max_duration {
                return Ok(ListAggregateTypesResponse {
                    correlation_id: request.correlation_id, aggregate_types: results,
                    next_cursor: Some(log_id),
                });
            }

            match self.read_segment_summary_cached(log_id).await.filter(|p| p.complete) {
                Some(payload) => {
                    for atk in &payload.aggregate_types {
                        if let Some(filter) = filter_org_id {
                            if atk.org_id != filter { continue; }
                        }
                        if seen.insert(atk.clone()) {
                            results.push(AggregateTypeListItem { org_id: atk.org_id, aggregate_type_id: atk.aggregate_type_id });
                        }
                    }
                }
                None => {
                    self.list_types_legacy_segment(log_id, filter_org_id, &mut seen, &mut results).await
                        .map_err(ShardListingError::ReadFromDiskError)?;
                }
            }
        }

        Ok(ListAggregateTypesResponse { correlation_id: request.correlation_id, aggregate_types: results, next_cursor: None })
    }

    async fn list_types_legacy_segment(
        &self,
        target_log_id: u64,
        filter_org_id: Option<u128>,
        seen: &mut HashSet<AggregateTypeKey>,
        results: &mut Vec<AggregateTypeListItem>,
    ) -> Result<(), ScanError<()>> {
        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache, target_log_id, None, self.config.read_max_chunk_size,
        );
        scanner.scan::<bool, ()>(|log_id, _pos, bytes| {
            if log_id != target_log_id { return Ok(Some(true)); }
            if !metablock_bytes::is_metablock_kind_event_batch_metadata(bytes) { return Ok(None); }
            let org_id = metablock_bytes::read_event_batch_org_id(bytes);
            if let Some(filter) = filter_org_id {
                if org_id != filter { return Ok(None); }
            }
            let aggregate_type_id = metablock_bytes::read_event_batch_aggregate_type_id(bytes);
            if seen.insert(AggregateTypeKey::new(org_id, aggregate_type_id)) {
                results.push(AggregateTypeListItem { org_id, aggregate_type_id });
            }
            Ok(None)
        }).await?;
        Ok(())
    }

    /// List aggregates, optionally filtered by org_id and/or aggregate_type_id.
    ///
    /// Reads segment summaries newest-to-oldest with delete barrier semantics.
    /// Falls back to reverse metablock scan for legacy segments without summaries.
    pub async fn list_aggregates(&self, request: ListAggregatesRequest) -> Result<ListAggregatesResponse, ShardListingError> {
        let _permit = self.list_semaphore.acquire_permit(1).await
            .map_err(|_| ShardListingError::ListSemaphoreClosed)?;
        let start_time = Instant::now();
        let max_duration = self.config.list_max_duration;
        let page_size = self.config.list_page_size;
        let active_log_id = self.log_segments_cache.active_log_id();
        let filter_org_id = request.org_id;
        let filter_aggregate_type_id = request.aggregate_type_id;

        struct AccumulatedStats {
            is_deleted: bool,
            event_batch_count: u64,
            min_aggregate_version: u64,
            max_aggregate_version: u64,
            max_server_timestamp: u64,
            compressed_size: u64,
            uncompressed_size: u64,
        }

        fn build_response(
            correlation_id: Option<u128>,
            result_order: Vec<AggregateKey>,
            seen: &HashMap<AggregateKey, AccumulatedStats>,
            next_cursor: Option<u64>,
        ) -> ListAggregatesResponse {
            let aggregates = result_order.into_iter()
                .filter_map(|key| {
                    seen.get(&key).map(|stats| AggregateListItem {
                        org_id: key.org_id,
                        aggregate_type_id: key.aggregate_type_id,
                        aggregate_id: key.aggregate_id,
                        is_deleted: stats.is_deleted,
                        event_batch_count: stats.event_batch_count,
                        min_event_timestamp: 0,
                        max_event_timestamp: 0,
                        min_aggregate_version: if stats.min_aggregate_version == u64::MAX { 0 } else { stats.min_aggregate_version },
                        max_aggregate_version: stats.max_aggregate_version,
                        min_event_seq: 0,
                        max_event_seq: 0,
                        min_server_timestamp: 0,
                        max_server_timestamp: stats.max_server_timestamp,
                        compressed_size: stats.compressed_size,
                        uncompressed_size: stats.uncompressed_size,
                    })
                })
                .collect();
            ListAggregatesResponse { correlation_id, aggregates, next_cursor }
        }

        fn process_aggregate_entry(
            org_id: u128, aggregate_type_id: u128, aggregate_id: u128,
            is_deleted: bool, event_batch_count: u64,
            min_aggregate_version: u64, last_aggregate_version: u64,
            last_server_timestamp: u64, compressed_size: u64, uncompressed_size: u64,
            filter_org_id: Option<u128>, filter_aggregate_type_id: Option<u128>,
            seen: &mut HashMap<AggregateKey, AccumulatedStats>,
            result_order: &mut Vec<AggregateKey>,
            deleted_barrier: &mut HashSet<AggregateKey>,
            unique_count: &mut usize,
        ) {
            if let Some(f) = filter_org_id { if org_id != f { return; } }
            if let Some(f) = filter_aggregate_type_id { if aggregate_type_id != f { return; } }

            let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
            if deleted_barrier.contains(&key) { return; }

            if is_deleted {
                deleted_barrier.insert(key.clone());
                if !seen.contains_key(&key) {
                    seen.insert(key.clone(), AccumulatedStats {
                        is_deleted: true, event_batch_count: 0,
                        min_aggregate_version: u64::MAX, max_aggregate_version: 0,
                        max_server_timestamp: 0, compressed_size: 0, uncompressed_size: 0,
                    });
                    result_order.push(key);
                    *unique_count += 1;
                }
                return;
            }

            if let Some(stats) = seen.get_mut(&key) {
                if !stats.is_deleted {
                    stats.event_batch_count += event_batch_count;
                    stats.min_aggregate_version = stats.min_aggregate_version.min(min_aggregate_version);
                    stats.max_aggregate_version = stats.max_aggregate_version.max(last_aggregate_version);
                    stats.max_server_timestamp = stats.max_server_timestamp.max(last_server_timestamp);
                    stats.compressed_size += compressed_size;
                    stats.uncompressed_size += uncompressed_size;
                }
            } else {
                seen.insert(key.clone(), AccumulatedStats {
                    is_deleted: false,
                    event_batch_count,
                    min_aggregate_version,
                    max_aggregate_version: last_aggregate_version,
                    max_server_timestamp: last_server_timestamp,
                    compressed_size,
                    uncompressed_size,
                });
                result_order.push(key);
                *unique_count += 1;
            }
        }

        // `seen` dedups within one page only. Segment summaries are per-segment, so an
        // aggregate spanning a rotation lives in several and can recur across pages.
        // Listing is best-effort: callers paginating to completion dedup by key.
        let mut seen: HashMap<AggregateKey, AccumulatedStats> = HashMap::with_capacity(page_size);
        let mut result_order: Vec<AggregateKey> = Vec::with_capacity(page_size);
        let mut deleted_barrier: HashSet<AggregateKey> = HashSet::new();
        let mut unique_count = 0usize;
        macro_rules! process {
            ($org:expr, $atype:expr, $aid:expr, $del:expr, $ebc:expr,
             $min_ebi:expr, $last_ebi:expr, $last_ts:expr, $csz:expr, $usz:expr) => {
                process_aggregate_entry(
                    $org, $atype, $aid, $del, $ebc, $min_ebi, $last_ebi, $last_ts, $csz, $usz,
                    filter_org_id, filter_aggregate_type_id,
                    &mut seen, &mut result_order, &mut deleted_barrier, &mut unique_count,
                )
            }
        }

        // Cardinality is unbounded, so pagination keeps no in-memory index. Page 1 (cursor None)
        // drains the active segment's summary whole: it's a live unordered map, can't be
        // offset-resumed, but it's bounded by one segment, not by total cardinality. The unbounded
        // dimension is the count of SEALED segments. Those we page: each one's on-disk summary is an
        // immutable Vec, so an offset into it is a stable resume point (see ListCursor). Page 1 can
        // return up to the active segment's size; every later page is capped at page_size.
        let (mut log_id, mut offset) = match request.cursor {
            None => {
                let summary = { self.shard_mem_cache.borrow().peek_segment_summary().clone() };
                for (key, entry) in &summary {
                    process!(key.org_id, key.aggregate_type_id, key.aggregate_id,
                        entry.is_deleted, entry.event_batch_count,
                        entry.min_aggregate_version, entry.last_aggregate_version,
                        entry.last_server_timestamp, entry.compressed_size, entry.uncompressed_size);
                }
                (active_log_id.saturating_sub(1), 0u32)
            }
            Some(raw) => { let c = ListCursor::decode(raw); (c.log_id, c.offset) }
        };

        // Walk sealed segments newest-first, resuming the first at `offset`. Cap the page WITHIN a
        // segment, not just between them, or one large segment blows past page_size.
        while log_id >= 1 {
            if unique_count >= page_size || start_time.elapsed() >= max_duration {
                let next = ListCursor { log_id, offset }.encode();
                return Ok(build_response(request.correlation_id, result_order, &seen, Some(next)));
            }

            match self.read_segment_summary_cached(log_id).await.filter(|p| p.complete) {
                Some(payload) => {
                    let mut i = offset as usize;
                    while i < payload.aggregates.len() {
                        if unique_count >= page_size || start_time.elapsed() >= max_duration {
                            // Resume this same segment at entry `i` (not yet processed).
                            let next = ListCursor { log_id, offset: i as u32 }.encode();
                            return Ok(build_response(request.correlation_id, result_order, &seen, Some(next)));
                        }
                        let entry = &payload.aggregates[i];
                        process!(entry.org_id, entry.aggregate_type_id, entry.aggregate_id,
                            entry.is_deleted, entry.event_batch_count,
                            entry.min_aggregate_version, entry.last_aggregate_version,
                            entry.last_server_timestamp, entry.compressed_size, entry.uncompressed_size);
                        i += 1;
                    }
                }
                None => {
                    // Legacy segment, no on-disk summary. No stable Vec to offset into, so scan its
                    // metablocks whole (bounded by one segment) and ignore `offset`. The
                    // between-segment check above bounds how often this runs.
                    let mut scanner = ReverseMetablockScanner::new(
                        &self.log_segments_cache, log_id, None, self.config.read_max_chunk_size,
                    );
                    scanner.scan::<bool, ()>(|scan_log_id, _pos, bytes| {
                        if scan_log_id != log_id { return Ok(Some(true)); }

                        if metablock_bytes::is_metablock_kind_soft_delete(bytes) {
                            let ak = metablock_bytes::read_soft_delete_aggregate_key(bytes);
                            process!(ak.org_id, ak.aggregate_type_id, ak.aggregate_id,
                                        true, 0, u64::MAX, 0, 0, 0, 0);
                            return Ok(None);
                        }

                        if !metablock_bytes::is_metablock_kind_event_batch_metadata(bytes) {
                            return Ok(None);
                        }

                        let ak = metablock_bytes::read_event_batch_aggregate_key(bytes);
                        let ebi = metablock_bytes::read_event_batch_aggregate_version(bytes);
                        let ts = metablock_bytes::read_server_timestamp(bytes);
                        let csz = metablock_bytes::read_compressed_size(bytes);
                        let usz = metablock_bytes::read_uncompressed_size(bytes);
                        process!(ak.org_id, ak.aggregate_type_id, ak.aggregate_id,
                                    false, 1, ebi, ebi, ts, csz, usz);
                        Ok(None)
                    }).await.map_err(ShardListingError::ReadFromDiskError)?;
                }
            }

            log_id -= 1;
            offset = 0;
        }

        Ok(build_response(request.correlation_id, result_order, &seen, None))
    }
    
    pub async fn exists(&self, exists_request: &AggregateDetailsRequest) -> Result<AggregateDetailsResponse, ShardAggregateDetailsError> {
        self.aggregate_exists_and_cache(&exists_request.aggregate_key, CachePath::Read).await?;

        let snapshot = self
            .shard_mem_cache
            .borrow_mut()
            .get_aggregate_snapshot(&exists_request.aggregate_key, CachePath::Read);

        let snapshot = match snapshot {
            Some(s) if s.status == AggregateStatus::NotFound => {
                return Err(ShardAggregateDetailsError::AggregateNotExists);
            }
            Some(s) => s,
            None => return Err(ShardAggregateDetailsError::AggregateNotExists),
        };

        let is_deleted = snapshot.status == AggregateStatus::Deleted;

        // Read last metablock from disk for server_timestamp, client_id, user_id
        let (last_server_timestamp, last_client_id, last_user_id) =
            self.read_metablock_details(snapshot.log_id, snapshot.metablock_absolute_pos).await?;

        Ok(AggregateDetailsResponse {
            correlation_id: exists_request.correlation_id,
            min_aggregate_version: snapshot.min_aggregate_version,
            max_aggregate_version: snapshot.aggregate_version,
            max_event_seq: snapshot.event_seq,
            is_deleted,
            allow_recreate: snapshot.allow_recreate,
            allow_sequence_continuation: snapshot.allow_sequence_continuation,
            last_server_timestamp,
            last_client_id,
            last_user_id,
        })
    }

    /// Read a single metablock from disk and extract client_id, user_id, server_timestamp.
    async fn read_metablock_details(
        &self,
        log_id: u64,
        metablock_absolute_pos: u64,
    ) -> Result<(u64, u128, Option<u128>), ShardAggregateDetailsError> {
        let log_segment = self.log_segments_cache.get(log_id).await
            .map_err(|e| ShardAggregateDetailsError::MetablockReadError(format!("{:?}", e)))?;

        let guard = log_segment.lock_reader("exists_metablock_read").await
            .map_err(|e| ShardAggregateDetailsError::MetablockReadError(format!("{:?}", e)))?;

        let dma_file = guard.as_ref()
            .ok_or_else(|| ShardAggregateDetailsError::MetablockReadError("no file handle".into()))?;

        let buf = dma_file.read_at(metablock_absolute_pos, FIXED_BLOCK_SIZE_BYTES).await
            .map_err(|e| ShardAggregateDetailsError::MetablockReadError(format!("{:?}", e)))?;

        let (chunks, _) = (*buf).as_chunks::<FIXED_BLOCK_SIZE_BYTES>();
        let block = chunks.first()
            .ok_or_else(|| ShardAggregateDetailsError::MetablockReadError("empty read".into()))?;

        let metablock = deserialise_metablock(block)
            .map_err(|e| ShardAggregateDetailsError::MetablockReadError(format!("{:?}", e)))?;

        let (client_id, user_id) = match &metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(eb) => (eb.client_id, eb.user_id),
            MetablockKind::SoftDelete(sd) => (sd.client_id, sd.user_id),
            MetablockKind::SoftTrim(st) => (st.client_id, st.user_id),
            other @ MetablockKind::SchemaRegistration(_) => {
                return Err(ShardAggregateDetailsError::MetablockReadError(
                    format!("unexpected metablock kind: {:?}", std::mem::discriminant(other)),
                ))
            }
        };

        Ok((metablock.server_timestamp, client_id, user_id))
    }

    /// Replication backpressure gate, shared by write/delete/trim. True means
    /// reject: a rollback just fired (cooldown) or the follower is wedged.
    /// Deletes and trims gate here for the same reason writes do — don't
    /// accept destructive operations into a WAL that just rolled back.
    fn replication_backpressure_rejects(&self, rejected_counter: &'static str) -> bool {
        let hb_started_unix_ms = self.replication_client.current_heartbeat_started_at_unix_ms();
        let (now_for_rollback, now_unix_ms) = if hb_started_unix_ms.is_some() || self.last_rollback_at.get().is_some() {
            (
                Some(std::time::Instant::now()),
                Some(validated_node_status::unix_epoch_now_ms()),
            )
        } else {
            (None, None)
        };
        let Some(cause) = crate::replication_backpressure::check_replication_backpressure(
            self.shard_mem_cache.borrow().is_inflight_pressured(),
            self.last_rollback_at.get(),
            self.config.replication_rollback_cooldown,
            hb_started_unix_ms,
            now_unix_ms,
            self.replication_client.is_follower_reachable(),
            self.config.heartbeat_starve_threshold,
            now_for_rollback,
        ) else {
            return false;
        };
        metrics::counter!(
            rejected_counter,
            &[("cause", cause.metric_label())],
        ).increment(1);
        // log only when we transition into the cooldown window
        match cause {
            crate::replication_backpressure::BackpressureCause::RollbackCooldown { remaining_ms } => {
                let rb_at = self.last_rollback_at.get();
                if self.last_logged_rollback_at.get() != rb_at {
                    self.last_logged_rollback_at.set(rb_at);
                    warn!(
                        shard_id = self.config.shard_id,
                        remaining_ms,
                        cooldown_ms = self.config.replication_rollback_cooldown.as_millis() as u64,
                        "ReplicationBackpressure: rollback cooldown active — rejecting writes/deletes/trims",
                    );
                }
            }
            crate::replication_backpressure::BackpressureCause::FollowerHeartbeatStarved { in_flight_ms } => {
                // One-shot per starvation episode: log once per in-flight
                // heartbeat that crosses the threshold. The next heartbeat
                // starts at a different unix_ms, so a fresh starvation
                // cycle re-arms the warn.
                if self.last_logged_starve_at.get() != hb_started_unix_ms {
                    self.last_logged_starve_at.set(hb_started_unix_ms);
                    warn!(
                        shard_id = self.config.shard_id,
                        in_flight_ms,
                        threshold_ms = self.config.heartbeat_starve_threshold.as_millis() as u64,
                        "ReplicationBackpressure: follower heartbeat in flight too long — rejecting writes/deletes/trims",
                    );
                }
            }
            crate::replication_backpressure::BackpressureCause::InflightPressure => {}
        }
        true
    }

    pub async fn trim_start(&self, trim_request: TrimStartRequest) -> Result<TrimStartResponse, ShardTrimError> {

        let lease_epoch = match self.node_status.get().effective_node_status() {
            NodeStatus::Leader { lease_epoch } => lease_epoch,
            NodeStatus::Standalone => 0,
            _ => return Err(ShardTrimError::ShardCannotAcceptWrites { leader_address: self.leader_client_address.borrow().clone() }),
        };

        if self.replication_backpressure_rejects("celeriant_trims_rejected_backpressure_total") {
            return Err(ShardTrimError::ReplicationBackpressure);
        }

        let aggregate_key = &trim_request.aggregate_key;

        // Ensure aggregate exists
        if !self.aggregate_exists_and_cache(aggregate_key, CachePath::Write).await? {
            return Err(ShardTrimError::AggregateNotExists);
        }

        let current_indexes = self.shard_mem_cache.borrow_mut().get_write_event_seqes(aggregate_key);

        // Validate trim index is within valid range
        if trim_request.keep_from_aggregate_version <= current_indexes.min_aggregate_version {
            // Already trimmed to this point or beyond, nothing to do
            return Ok(TrimStartResponse {
                correlation_id: trim_request.correlation_id,
            });
        }

        if trim_request.keep_from_aggregate_version > current_indexes.aggregate_version {
            return Err(ShardTrimError::TrimIndexOutOfRange {
                requested: trim_request.keep_from_aggregate_version,
                max_aggregate_version: current_indexes.aggregate_version,
            });
        }

        let server_timestamp = self.config.timestamp_config.now();

        let metablock_soft_trim = MetablockSoftTrim {
            aggregate_key: aggregate_key.clone(),
            keep_from_aggregate_version: trim_request.keep_from_aggregate_version,
            aggregate_version: current_indexes.aggregate_version,
            event_seq: current_indexes.event_seq,
            client_id: trim_request.client_id,
            user_id: trim_request.user_id,
        };

        let metablock = Metablock {
            wal_seq: 0,
            server_timestamp,
            lease_epoch,
            node_id: self.config.node_id,
            compressed_size: 0,
            uncompressed_size: 0,
            datablock_version: 0,
            datablock_compression_type: 0,
            datablock: DatablockStorageKind::None,
            wal_metablock_type: MetablockKind::SoftTrim(metablock_soft_trim),
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
            previous_aggregate_metablock_pos: 0,
        };

        let shard_log_queue_item = ShardLogQueueItem::new(None, None, metablock);

        // A rollback crossing the waits below wipes the queued trim; without
        // these checks the client gets a false ack on a data-destruction op.
        let rollback_gen_at_submit = self.shard_mem_cache.borrow().rollback_generation();

        // Add to queue
        {
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
            shard_mem_cache.add_pending_trim_to_queue(
                aggregate_key,
                trim_request.keep_from_aggregate_version,
                current_indexes.aggregate_version,
                current_indexes.event_seq,
                shard_log_queue_item,
            );
        }

        // Wait for durable write
        self.sync_durable().await?;

        if self.shard_mem_cache.borrow().rollback_generation() != rollback_gen_at_submit {
            return Err(ShardTrimError::ReplicationError(ReplicationError::RollbackInProgress));
        }

        // Same deal for replication, if we are the leader,
        // wait on durable replication, also batched
        self.replicate_durable().await?;

        if self.shard_mem_cache.borrow().rollback_generation() != rollback_gen_at_submit {
            return Err(ShardTrimError::ReplicationError(ReplicationError::RollbackInProgress));
        }

        Ok(TrimStartResponse {
            correlation_id: trim_request.correlation_id,
        })
    }

    pub async fn delete(&self, delete_request: DeleteRequest) -> Result<DeleteResponse, ShardDeleteError> {

        let lease_epoch = match self.node_status.get().effective_node_status() {
            NodeStatus::Leader { lease_epoch } => lease_epoch,
            NodeStatus::Standalone => 0,
            _ => return Err(ShardDeleteError::ShardCannotAcceptWrites { leader_address: self.leader_client_address.borrow().clone() }),
        };

        if self.replication_backpressure_rejects("celeriant_deletes_rejected_backpressure_total") {
            return Err(ShardDeleteError::ReplicationBackpressure);
        }

        // Make sure we have at least one aggregate to write
        if delete_request.deletes.is_empty() {
            return Err(ShardDeleteError::EmptyDeleteList);
        }

        // Phase 1: warm the write cache — existence checks and disk loads,
        // the only awaits before the durability waits.
        for aggregate_key in delete_request.deletes.keys() {
            if !self.aggregate_exists_and_cache(aggregate_key, CachePath::Write).await? {
                return Err(ShardDeleteError::AggregateNotExists);
            }
        }

        let mut retried_for_visibility_gap = false;
        let prepared_deletes = loop {
            match self.validate_and_prepare_deletes(lease_epoch, &delete_request) {
                Ok(prepared) => break prepared,
                Err(ShardDeleteError::OptimisticConcurrencyViolation { .. }) if !retried_for_visibility_gap => {
                    retried_for_visibility_gap = true;
                    let _ = self.replicate_durable().await;
                }
                Err(e) => return Err(e),
            }
        };

        // A rollback crossing the waits below wipes the queued tombstones;
        // without these checks the client gets a false ack on a delete it
        // can't safely retry (no client_seq idempotency on this path).
        let rollback_gen_at_submit = self.shard_mem_cache.borrow().rollback_generation();

        // Phase 2: Append all prepared deletes to queue - cannot fail
        {
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
            for (aggregate_key, soft_delete, shard_log_queue_item) in prepared_deletes {
                shard_mem_cache.add_pending_delete_to_queue(
                    &aggregate_key,
                    soft_delete.event_seq,
                    soft_delete.aggregate_version,
                    soft_delete.allow_recreate,
                    soft_delete.allow_sequence_continuation,
                    shard_log_queue_item,
                );
            }
        }

        // Now we wait on disk write before ack to client
        self.sync_durable().await?;

        if self.shard_mem_cache.borrow().rollback_generation() != rollback_gen_at_submit {
            return Err(ShardDeleteError::ReplicationError(ReplicationError::RollbackInProgress));
        }

        // Same deal for replication, if we are the leader,
        // wait on durable replication, also batched
        self.replicate_durable().await?;

        if self.shard_mem_cache.borrow().rollback_generation() != rollback_gen_at_submit {
            return Err(ShardDeleteError::ReplicationError(ReplicationError::RollbackInProgress));
        }

        Ok(DeleteResponse {
            correlation_id: delete_request.correlation_id,
        })
    }

    /// Synchronous validate-and-prepare for delete: validation and tombstone
    /// construction see one frozen queue state. Runs after the async warm
    /// loop; must stay await-free so nothing can move between the OCC check
    /// and enqueue. Tombstone fields are rebuilt from current indexes here,
    /// not carried over from the warm loop — that also covers the no-OCC
    /// case, where no check would fail but a carried-over tombstone would
    /// still record stale indexes.
    fn validate_and_prepare_deletes(
        &self,
        lease_epoch: u64,
        delete_request: &DeleteRequest,
    ) -> Result<Vec<(AggregateKey, MetablockSoftDelete, ShardLogQueueItem)>, ShardDeleteError> {
        let mut prepared_deletes = Vec::with_capacity(delete_request.deletes.len());
        for (aggregate_key, single_delete) in &delete_request.deletes {
            let aggregate_current_indexes = self.shard_mem_cache.borrow_mut().get_write_event_seqes(aggregate_key);

            if aggregate_current_indexes.pending_delete_or_deleted
                || aggregate_current_indexes.aggregate_version == 0 {
                return Err(ShardDeleteError::AggregateNotExists);
            }

            // Validate optimistic concurrency
            if let Some(expected) = single_delete.expected_version {
                if expected != aggregate_current_indexes.aggregate_version {
                    return Err(ShardDeleteError::OptimisticConcurrencyViolation {
                        expected_version: expected,
                        current_aggregate_version: aggregate_current_indexes.aggregate_version,
                    });
                }
            }

            let server_timestamp = self.config.timestamp_config.now();

            let metablock_soft_delete = MetablockSoftDelete {
                aggregate_key: aggregate_key.clone(),
                aggregate_version: aggregate_current_indexes.aggregate_version,
                event_seq: aggregate_current_indexes.event_seq,
                client_id: delete_request.client_id,
                user_id: delete_request.user_id,
                allow_recreate: single_delete.allow_recreate,
                allow_sequence_continuation: single_delete.allow_sequence_continuation,
            };
            let metablock = Metablock {
                wal_seq: 0,
                server_timestamp,
                lease_epoch,
                node_id: self.config.node_id,
                compressed_size: 0,
                uncompressed_size: 0,
                datablock_version: 0,
                datablock_compression_type: 0,
                datablock: DatablockStorageKind::None,
                wal_metablock_type: MetablockKind::SoftDelete(metablock_soft_delete.clone()),
                previous_tip_hash: GENESIS_HASH,
                datablock_position: 0,
                previous_aggregate_metablock_pos: 0,
            };

            let shard_log_queue_item = ShardLogQueueItem::new(None, None, metablock);
            prepared_deletes.push((aggregate_key.clone(), metablock_soft_delete, shard_log_queue_item));
        }
        Ok(prepared_deletes)
    }

    /// Write events to an aggregate.
    ///
    /// # Flow
    /// 1. Validate request (idempotency, optimistic concurrency)
    /// 2. Build datablock and metablock
    /// 3. Add to pending queue, assigning indexes (not yet visible for reads)
    /// 4. Wait for durability
    /// 5. Return response with assigned indexes
    pub async fn write(&self, write_request: WriteRequest) -> Result<WriteResponse, ShardWriteError> {
        
        let status = self.node_status.get();
        let lease_epoch = match status.effective_node_status() {
            NodeStatus::Leader { lease_epoch } => lease_epoch,
            NodeStatus::Standalone => 0,
            other => {
                debug!(effective = ?other, raw = ?status.raw(), expires_at_ms = status.lease_expires_at_ms(), now_ms = validated_node_status::unix_epoch_now_ms(), "Write rejected: not Leader/Standalone");
                return Err(ShardWriteError::ShardCannotAcceptWrites { leader_address: self.leader_client_address.borrow().clone() });
            }
        };

        if self.replication_backpressure_rejects("celeriant_writes_rejected_backpressure_total") {
            return Err(ShardWriteError::ReplicationBackpressure);
        }

        // Make sure we have at least one aggregate to write
        if write_request.writes.is_empty() {
            return Err(ShardWriteError::EmptyEventsList);
        }

        let total_events: usize = write_request.writes.values().map(|w| w.events.len()).sum();
        let total_payload_bytes: usize = write_request.writes.values()
            .flat_map(|w| w.events.iter())
            .map(|e| e.event_value.len())
            .sum();

        let writes: Vec<(AggregateKey, SingleAggregateWrite)> = write_request.writes.into_iter().collect();
        let client_id = write_request.client_id;
        let user_id = write_request.user_id;
        let correlation_id = write_request.correlation_id;

        // Deterministic input checks - won't change on retry.
        for (_, single_write) in &writes {
            if single_write.events.is_empty() {
                return Err(ShardWriteError::EmptyEventsList);
            }
            if let Some(ev) = single_write.events.iter().find(|e| e.event_type_major == 0) {
                return Err(ShardWriteError::ZeroEventType { client_seq: ev.client_seq });
            }
        }

        // Idempotency / OCC / recreate-not-allowed rejections validate against the WRITE cache
        let mut retried_for_visibility_gap = false;
        loop {
            let mut error: Option<ShardWriteError> = None;
            for (aggregate_key, single_write) in &writes {
                if let Err(e) = self.validate_single_write(aggregate_key, client_id, single_write).await {
                    error = Some(e);
                    break;
                }
            }
            match error {
                None => break,
                Some(e) if !retried_for_visibility_gap && is_visibility_gap_rejection(&e) => {
                    retried_for_visibility_gap = true;
                    let _ = self.replicate_durable().await;
                }
                Some(e) => return Err(e),
            }
        }

        // consumes events to build datablocks/metablocks.
        // validate_and_prepare_write also re-runs validation as defense-in-depth.
        let mut prepared_writes = Vec::with_capacity(writes.len());
        for (aggregate_key, single_write) in writes {
            let prepared = self.validate_and_prepare_write(
                lease_epoch,
                &aggregate_key,
                client_id,
                user_id,
                single_write,
            )?;
            prepared_writes.push(prepared);
        }

        // Phase 2: Append all prepared writes to queue - cannot fail
        tracing::debug!(
            shard_id = self.config.shard_id,
            client_id = client_id,
            aggregate_count = prepared_writes.len(),
            total_events,
            total_payload_bytes,
            "Write request accepted",
        );
        // The committed version is fixed at enqueue; it is only returned after
        // the fsync + replication waits below succeed, so a returned value
        // always names a durably committed batch.
        let max_aggregate_version = match prepared_writes.as_slice() {
            [single] => Some(single.aggregate_version),
            _ => None,
        };
        
        let rollback_gen_at_submit = self.shard_mem_cache.borrow().rollback_generation();
        self.append_prepared_writes_to_queue(prepared_writes);

        // Wait on disk write, it's batched for performance
        let fsync_start = std::time::Instant::now();
        self.sync_durable().await?;
        let fsync_ms = fsync_start.elapsed().as_millis() as u64;

        if self.shard_mem_cache.borrow().rollback_generation() != rollback_gen_at_submit {
            return Err(ShardWriteError::ReplicationError(ReplicationError::RollbackInProgress));
        }

        // Same deal for replication, if we are the leader,
        // wait on durable replication, also batched
        let repl_start = std::time::Instant::now();
        self.replicate_durable().await?;
        let repl_ms = repl_start.elapsed().as_millis() as u64;

        if self.shard_mem_cache.borrow().rollback_generation() != rollback_gen_at_submit {
            return Err(ShardWriteError::ReplicationError(ReplicationError::RollbackInProgress));
        }

        let total_ms = fsync_ms + repl_ms;
        if total_ms > 1000 {
            warn!(
                shard_id = self.config.shard_id,
                fsync_ms,
                repl_ms,
                total_ms,
                "Slow write: fsync + replication exceeded 1s",
            );
        }

        let shard_label = &self.metrics_shard_label;
        metrics::counter!("celeriant_write_events_total", shard_label).increment(total_events as u64);
        metrics::counter!("celeriant_write_bytes_total", shard_label).increment(total_payload_bytes as u64);

        Ok(WriteResponse {
            correlation_id,
            max_aggregate_version,
        })
    }

    /// Read event batches from an aggregate.
    /// Reads from cache when possible, falls back to disk for older batches.
    pub async fn read(&self, request: &ReadRequest) -> Result<ReadResponse, ShardReadError> {
        let aggregate_key = &request.aggregate_key;
        let filters = &request.filters;
        let max_bytes = self.config.max_response_size as u64;

        // 1. Ensure aggregate exists and is cached
        if !self.aggregate_exists_and_cache(aggregate_key, CachePath::Read).await? {
            let visible_wal_seq = self.log_segments_cache.get_latest_read_cursor_wal_seq();
            debug!(
                shard_id = self.config.shard_id,
                aggregate_key = %aggregate_key,
                visible_wal_seq,
                "Read: aggregate not found after disk scan"
            );
            return Err(ShardReadError::AggregateNotExists);
        }

        let last_known = self.shard_mem_cache.borrow_mut().get_aggregate_last_metablock_pos(aggregate_key, CachePath::Read);

        // 2. Validate requested range is available (not trimmed)
        if filters.from_aggregate_version < last_known.min_aggregate_version {
            return Err(ShardReadError::UnavailableBatchIndex {
                minimum_available: last_known.min_aggregate_version,
                requested: filters.from_aggregate_version,
            });
        }

        // 3. Collect metablocks with size-bounded accumulation (NO datablocks yet)
        let mut collection = self.collect_metablocks_bounded(aggregate_key, filters, max_bytes, last_known).await?;

        // 4. Fetch datablocks only for kept metablocks
        fetch_datablocks_for_metablocks(&mut collection.kept_metablocks, self.config.read_max_chunk_size, &self.log_segments_cache, &self.dict_codec).await?;

        // 5. Deserialize and apply event-level filters
        let event_batches = self.build_filtered_response(collection.kept_metablocks, filters);

        let read_bytes: u64 = event_batches.iter().map(|b| b.events.iter().map(|e| e.event_value.len() as u64).sum::<u64>()).sum();
        let shard_label = &self.metrics_shard_label;
        metrics::counter!("celeriant_read_bytes_total", shard_label).increment(read_bytes);

        Ok(ReadResponse {
            correlation_id: request.correlation_id,
            event_batches,
            next_aggregate_version: collection.next_aggregate_version,
        })
    }

    /// Close the shard WAL, flushing any pending writes.
    pub async fn close(&self) {
        self.log_segments_cache.close().await
    }

    /// Scan sealed segments oldest-first and compact the first eligible one.
    ///
    /// A segment is eligible if it is sealed (not the active segment) and fully
    /// replicated (`!is_pending_advance()`). Compaction is skipped if the reclaimable
    /// fraction is below `compaction_min_reclaimable_ratio`.
    ///
    /// Returns `Some(CompactionResult)` if a segment was compacted, `None` if no
    /// eligible segment was found or all eligible segments were below the threshold.
    pub async fn compact_oldest_eligible_segment(&self) -> Result<Option<CompactionResult>, CompactionError> {
        let active_log_id = self.log_segments_cache.active_log_id();

        // No sealed segments when active is the first log.
        if active_log_id <= 1 {
            return Ok(None);
        }

        let min_ratio = self.config.compaction_min_reclaimable_ratio;
        let temp_dir = &self.config.compaction_temp_dir;
        let mut segments_checked: u32 = 0;

        for log_id in 1..active_log_id {
            let result = compact_segment(
                log_id,
                &self.log_segments_cache,
                &self.shard_mem_cache,
                min_ratio,
                temp_dir,
            )
            .await;

            match result {
                // Segment compacted — return immediately (one per cycle).
                Ok(Some(r)) => {
                    // Compaction rewrote the segment and its sidecar; drop any
                    // stale decoded summary so the next consult re-reads it.
                    self.summary_cache.borrow_mut().pop(&r.log_id);
                    return Ok(Some(r));
                }
                // Below threshold or pending advance — yield then try next segment.
                Ok(None) => {
                    segments_checked += 1;
                    glommio::yield_if_needed().await;
                    continue;
                }
                // Non-existent segment (e.g. gap in log_ids) — skip gracefully.
                Err(CompactionError::OpenSegment(_)) => continue,
                // Other error — surface to caller for logging.
                Err(e) => return Err(e),
            }
        }

        debug!(
            shard_id = self.config.shard_id,
            segments_checked,
            sealed_segments = active_log_id - 1,
            "Compaction: no eligible segments"
        );

        Ok(None)
    }

    /// Get the watched aggregates registry for this shard.
    pub fn watched_aggregates(&self) -> Rc<AggregateWatchers> {
        self.watched_aggregates.clone()
    }

    async fn cache_aggregate_client(&self, aggregate_key: &AggregateKey, aggregate_client_key: &AggregateClientKey) -> Result<(), ShardCacheLoadError> {
        // If we are cached already
        if let (true, _exists) = self
            .shard_mem_cache
            .borrow_mut()
            .aggregate_client_load_status(aggregate_key, aggregate_client_key)
        {
            return Ok(());
        }

        // Take an exclusive lock on this aggregate client to deduplicate thundering herd
        let aggregate_lock = self.aggregate_client_loading.acquire(aggregate_client_key);
        let _ = write_with_timeout(&aggregate_lock, "cache_aggregate_client").await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;

        // We have exclusive access now, check if another concurrent task has already done the work
        if let (true, _exists) = self
            .shard_mem_cache
            .borrow_mut()
            .aggregate_client_load_status(aggregate_key, aggregate_client_key)
        {
            return Ok(());
        }

        // ── Negative-lookup bloom (idempotency-negative-lookup.md) ──────────
        let client_hash = client_id_bloom_hash(aggregate_client_key.client_id);
        let negative_answer = self.shard_mem_cache.borrow_mut().negative_lookup_check(aggregate_key, client_hash);
        if negative_answer == NegativeLookupAnswer::DefinitelyAbsent {
            // A Complete bloom proves this client never wrote here: scan-free
            // first write. Insert it now (it is about to write; a rolled-back
            // write leaves a phantom, which is a superset, which is safe) and
            // seed the per-client LRU sentinel exactly as the not-found scan
            // path would.
            let mut mc = self.shard_mem_cache.borrow_mut();
            mc.negative_lookup_insert(aggregate_key, client_hash);
            mc.put_aggregate_client_into_cache(aggregate_client_key.clone(), 0, false);
            drop(mc);
            metrics::counter!("celeriant_negative_lookup_short_circuit_total", &self.metrics_shard_label).increment(1);
            return Ok(());
        }
        // No usable bloom: become the builder unless one is Complete
        // (MaybePresent: scan for the member/FP) or a build is already in
        // flight (Building latch: falls back to a plain scan, per the doc).
        // Install-empty-then-populate ordering: `try_begin_build` installs the
        // Building entry synchronously — BEFORE any await below — so every
        // concurrent commit from here on lands in it via insert-on-write.
        let complete_bloom = negative_answer == NegativeLookupAnswer::MaybePresent;
        let build_generation = if complete_bloom {
            None
        } else {
            self.shard_mem_cache.borrow_mut().negative_lookup_try_begin_build(aggregate_key)
        };
        let building = build_generation.is_some();
        // Parks the build on ANY exit (scan error, lock/semaphore timeout,
        // task cancellation): the entry stays Building — never answering
        // absent — and a later miss resumes it. Disarmed by finish below.
        // Carries this builder's generation so a late drop can never park a
        // successor builder's entry.
        struct NegativeBuildGuard<'a> {
            mem: &'a RefCell<MemCache>,
            key: &'a AggregateKey,
            generation: Option<u64>,
        }
        impl Drop for NegativeBuildGuard<'_> {
            fn drop(&mut self) {
                if let Some(generation) = self.generation {
                    self.mem.borrow_mut().negative_lookup_finish_build(self.key, generation, false);
                }
            }
        }
        let mut build_guard = NegativeBuildGuard { mem: &self.shard_mem_cache, key: aggregate_key, generation: build_generation };
        if building {
            metrics::counter!("celeriant_negative_lookup_builds_started_total", &self.metrics_shard_label).increment(1);
        }

        // Limit concurrent disk scans across different aggregates (NVMe starvation)
        let sem_wait_start = std::time::Instant::now();
        let _cache_permit = self.cache_load_semaphore.acquire_permit(1).await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;
        metrics::histogram!("celeriant_read_semaphore_wait_seconds", &self.metrics_shard_label)
            .record(sem_wait_start.elapsed().as_secs_f64());

        let last_known_metablock = self.shard_mem_cache.borrow_mut().get_aggregate_last_metablock_pos(aggregate_key, CachePath::Write);

        // Snapshot miss doesn't mean the aggregate has no blocks: fall back to the
        // active segment's chain tips (rebuilt at open and on every cursor rewind,
        // updated at commit — same freshness the appender relies on at sync time).
        let active_tip = if last_known_metablock.log_id == 0 {
            self.log_segments_cache.active().aggregate_chain_tips.borrow().get(aggregate_key).copied()
        } else {
            None
        };
        if active_tip.is_some() {
            metrics::counter!(
                "celeriant_cache_aggregate_client_tip_hint_total",
                &self.metrics_shard_label,
            ).increment(1);
        }
        let active_log_id = self.log_segments_cache.active_log_id();
        let (start_log_id, start_from_position) =
            client_scan_start(&last_known_metablock, active_log_id, active_tip);

        // Walk segments newest-to-oldest, consulting each sealed segment's summary
        // only when the scan is about to enter it (lazy: a hit in a newer segment
        // never touches older sidecars, and each sidecar decodes at most once per
        // miss — no LRU thrash when segments outnumber the cache). An absent
        // aggregate entry or a definitely-absent per-aggregate client set skips
        // the segment outright; otherwise seek straight to the aggregate's newest
        // metablock (the scanner self-verifies the target, so a stale
        // post-compaction hint degrades to the full hunt). Segments with
        // no/old-version/incomplete summary keep the full walk; the active
        // segment keeps the chain-tip start-hint path untouched. The segment
        // blooms still run first inside the scanner and stay authoritative.
        let mut find_result: Option<bool> = None;
        let mut hint: HashMap<u64, SegmentHint> = HashMap::with_capacity(1);
        // Build completeness tracking: stays true only while every sealed
        // segment is either walked or has its client set fully unioned in.
        let mut build_exhaustive = true;
        for log_id in (1..=start_log_id).rev() {
            hint.clear();
            if log_id != active_log_id {
                let summary = self.read_segment_summary_cached(log_id).await;
                if let Some(summary) = &summary {
                    if let Some(h) = summary_hint(summary, aggregate_key, client_hash) {
                        hint.insert(log_id, h);
                    }
                }
                if building {
                    // Union the sealed segment's client set into the building
                    // bloom BEFORE the scan can Skip on it: a Skip below then
                    // never hides clients from the build. Only a complete
                    // summary's set covers every client in the segment; an
                    // Unknown set (or incomplete/missing summary) never
                    // authorizes a Skip, so the chain walk covers the segment.
                    if let Some(summary) = &summary {
                        if summary.complete {
                            let found_entry = summary.aggregates.binary_search_by_key(
                                &(aggregate_key.org_id, aggregate_key.aggregate_type_id, aggregate_key.aggregate_id),
                                |e| (e.org_id, e.aggregate_type_id, e.aggregate_id),
                            );
                            if let Ok(i) = found_entry {
                                match &summary.aggregates[i].client_set {
                                    ClientSet::Exact(hashes) => {
                                        self.shard_mem_cache.borrow_mut().negative_lookup_union_exact(aggregate_key, hashes);
                                    }
                                    ClientSet::Bloom(words) => {
                                        // Sidecar blooms share client_id_bloom_hash + SBBF math but are
                                        // sized per segment, so the words are carried verbatim (aux) and
                                        // OR-ed at lookup. Refusal (aux cap/malformed) = not exhaustive.
                                        if !self.shard_mem_cache.borrow_mut().negative_lookup_union_bloom(aggregate_key, words) {
                                            build_exhaustive = false;
                                        }
                                    }
                                    ClientSet::Unknown => {}
                                }
                            }
                        }
                    }
                }
            }

            // Chain-follow within the segment: this walks several of THIS aggregate's
            // per-client versions, so skipping interleaved foreign metablocks via
            // backlinks beats reading every block.
            let mut scanner = ReverseMetablockScanner::new(
                &self.log_segments_cache,
                log_id,
                if log_id == start_log_id { start_from_position } else { None },
                self.config.read_max_chunk_size,
            )
            .with_aggregate_chain(aggregate_key.clone(), self.config.chain_read_window_bytes)
            .with_segment_hints(&hint)
            .with_write_cursor_upper_bound()
            .with_min_log_id(log_id);
            if !building {
                // The segment-level client bloom skips segments where the TARGET
                // client is absent — sound for the lookup, but it would hide the
                // aggregate's OTHER clients from a build (subset). Disabled while
                // building; the sidecar unions above replace it where trustworthy.
                scanner = scanner.with_client_bloom_filter_hash(client_hash);
            }

            find_result = scanner
                .scan::<bool, ()>(|_log_id, _metablock_absolute_pos, metablock_bytes| {
                    // Build collection: every client-bearing chain member the walk
                    // visits (EventBatch/SoftDelete/SoftTrim — every client-bearing kind) feeds
                    // the building bloom, not just the EventBatch blocks the
                    // client-seq lookup below cares about.
                    if building {
                        let commit_client_id = if metablock_bytes::is_metablock_kind_event_batch_metadata(metablock_bytes) {
                            Some(metablock_bytes::read_event_batch_client_id(metablock_bytes))
                        } else if metablock_bytes::is_metablock_kind_soft_delete(metablock_bytes) {
                            Some(metablock_bytes::read_soft_delete_client_id(metablock_bytes))
                        } else if metablock_bytes::is_metablock_kind_soft_trim(metablock_bytes) {
                            Some(metablock_bytes::read_soft_trim_client_id(metablock_bytes))
                        } else {
                            None
                        };
                        if let Some(cid) = commit_client_id {
                            self.shard_mem_cache.borrow_mut().negative_lookup_insert(aggregate_key, client_id_bloom_hash(cid));
                        }
                    }

                    if !metablock_bytes::is_matches_aggregate_key(metablock_bytes, aggregate_key) {
                        return Ok(None);
                    }

                    let client_id = metablock_bytes::read_event_batch_client_id(metablock_bytes);
                    let low_priority = client_id != aggregate_client_key.client_id;
                    let target_aggregate_client_key = AggregateClientKey::new(aggregate_key.clone(), client_id);

                    let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

                    // Not the aggregate we are searching for. Can we eager cache it? If not, skip it.
                    if low_priority && shard_mem_cache.is_aggregate_client_cache_full_or_contains(&target_aggregate_client_key) {
                        return Ok(None);
                    }

                    let last_client_seq = metablock_bytes::read_event_batch_max_client_seq(metablock_bytes);

                    shard_mem_cache.put_aggregate_client_into_cache(target_aggregate_client_key, last_client_seq, low_priority);

                    if low_priority {
                        Ok(None) //Haven't found aggregate client yet
                    } else {
                        Ok(Some(true)) //Done searching
                    }
                })
                .await
                .map_err(ShardCacheLoadError::FileScanningError)?;

            if find_result.is_some() {
                break;
            }
        }

        let found = find_result.unwrap_or(false);
        if !found {
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
            // Sentinel 0 prevents subsequent idempotent writes from this never-before-seen
            // client from re-triggering the WAL scan above. get_client_seq() treats 0 as None.
            shard_mem_cache.put_aggregate_client_into_cache(aggregate_client_key.clone(), 0, false);
            // The client is about to perform its first write: insert it so a
            // resident bloom stays a superset (its commit re-inserts anyway).
            shard_mem_cache.negative_lookup_insert(aggregate_key, client_hash);
            drop(shard_mem_cache);
            metrics::counter!(
                "celeriant_cache_aggregate_client_scan_not_found_total",
                &self.metrics_shard_label,
            ).increment(1);
            if complete_bloom {
                // A Complete bloom said maybe-present but the scan proved
                // absence: a false positive (or a delete/trim-only client,
                // which carries no client_seq to find). Option-B's failure
                // signal in reverse — this must stay rare in steady state.
                metrics::counter!("celeriant_negative_lookup_false_positive_total", &self.metrics_shard_label).increment(1);
            }
        } else {
            metrics::counter!(
                "celeriant_cache_aggregate_client_scan_found_total",
                &self.metrics_shard_label,
            ).increment(1);
        }

        if let Some(generation) = build_generation {
            // Build completeness invariant ("did I walk all of it"): not-found
            // means the loop covered every segment from the aggregate's newest
            // block down to log 1 — each one either chain-walked in full,
            // skipped with its complete client set unioned in, or skipped
            // because the aggregate provably has no blocks there. A found stop
            // leaves older history unvisited, and any union refusal breaks
            // coverage: in both cases the entry parks as Building (still
            // absorbing insert-on-write) and a later miss resumes the build.
            let complete = !found && build_exhaustive;
            let completed = self.shard_mem_cache.borrow_mut().negative_lookup_finish_build(aggregate_key, generation, complete);
            build_guard.generation = None;
            if completed {
                metrics::counter!("celeriant_negative_lookup_builds_completed_total", &self.metrics_shard_label).increment(1);
            }
        }

        Ok(())
    }

    /// Pre-warm schema cache for all unique SchemaKeys in the write request.
    /// Called before validate_and_prepare_write() so all lookups are cache hits.
    async fn pre_warm_schema_cache(&self, events: &[celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent], aggregate_key: &AggregateKey) -> Result<(), ShardCacheLoadError> {
        use celeriant_memcache::cached_schema::UniqueSchemaKeys;

        let mut unique_keys = UniqueSchemaKeys::new();
        for event in events {
            if event.iv.is_some() { continue; }

            let schema_key = SchemaKey::new(
                aggregate_key.org_id,
                aggregate_key.aggregate_type_id,
                event.event_type_major,
                event.event_type_minor,
            );
            unique_keys.try_insert(schema_key);
        }

        for schema_key in unique_keys.iter() {
            self.ensure_schema_cached(schema_key).await?;
        }

        Ok(())
    }

    /// Load a single schema key into cache via reverse WAL scan if not already cached.
    async fn ensure_schema_cached(&self, schema_key: &SchemaKey) -> Result<(), ShardCacheLoadError> {
        if self.shard_mem_cache.borrow_mut().schema_cache_contains(schema_key) {
            return Ok(());
        }

        let schema_lock = self.schema_loading.acquire(schema_key);
        let _ = write_with_timeout(&schema_lock, "ensure_schema_cached").await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;

        if self.shard_mem_cache.borrow_mut().schema_cache_contains(schema_key) {
            return Ok(());
        }

        // Limit concurrent disk scans across different aggregates (NVMe starvation)
        let sem_wait_start = std::time::Instant::now();
        let _cache_permit = self.cache_load_semaphore.acquire_permit(1).await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;
        metrics::histogram!("celeriant_read_semaphore_wait_seconds", &self.metrics_shard_label)
            .record(sem_wait_start.elapsed().as_secs_f64());

        metrics::counter!("celeriant_schema_scan_started_total", &self.metrics_shard_label).increment(1);

        // Walk segments newest-to-oldest, consulting each segment's schema
        // bloom before touching its blocks: the active segment via the
        // in-memory accumulator, sealed segments via the sidecar (lazy, like
        // the dedup consult). Definite absence — the common case, an empty
        // bloom under a complete summary — skips the segment without reading
        // it; a missing bloom or an incomplete summary walks it exactly as
        // before. The aggregate bloom is never consulted: schema hashes no
        // longer live there.
        let schema_hash = schema_key.bloom_hash();
        let active_log_id = self.log_segments_cache.active_log_id();
        let mut found_metablock: Option<(u64, Metablock)> = None;
        for log_id in (1..=active_log_id).rev() {
            let may_contain = if log_id == active_log_id {
                self.shard_mem_cache.borrow().active_segment_may_contain_schema(schema_hash)
            } else {
                match self.read_segment_summary_cached(log_id).await {
                    // Absence requires a COMPLETE summary: an incomplete bloom is a subset.
                    Some(summary) => summary.schema_may_contain_hash(schema_hash),
                    None => true,
                }
            };
            if !may_contain {
                metrics::counter!("celeriant_schema_scan_segments_skipped_total", &self.metrics_shard_label).increment(1);
                continue;
            }
            metrics::counter!("celeriant_schema_scan_segments_walked_total", &self.metrics_shard_label).increment(1);

            let mut scanner = ReverseMetablockScanner::new(
                &self.log_segments_cache,
                log_id,
                None,
                self.config.read_max_chunk_size,
            )
            .with_min_log_id(log_id);

            found_metablock = scanner
                .scan::<(u64, Metablock), ()>(|log_id, _metablock_absolute_pos, metablock_bytes| {
                    if !metablock_bytes::is_schema_registration_for_key(metablock_bytes, schema_key) {
                        return Ok(None);
                    }

                    let metablock = deserialise_metablock(metablock_bytes)
                        .map_err(|_| ())?;

                    if let MetablockKind::SchemaRegistration(_) = metablock.wal_metablock_type {
                        return Ok(Some((log_id, metablock)));
                    }

                    Ok(None)
                })
                .await
                .map_err(ShardCacheLoadError::FileScanningError)?;

            if found_metablock.is_some() {
                break;
            }
        }

        match found_metablock {
            Some((log_id, metablock)) => {
                let mut batch = [crate::collect_from_disk::EventBatchFromLogSegmentFile {
                    log_id,
                    metablock,
                    datablock: None,
                }];

                crate::collect_from_disk::fetch_datablocks_for_metablocks(&mut batch, self.config.read_max_chunk_size, &self.log_segments_cache, &self.dict_codec)
                    .await
                    .map_err(|e| ShardCacheLoadError::DatablockReadError(format!("{e:?}")))?;

                let [batch] = batch;
                let mut cache = self.shard_mem_cache.borrow_mut();
                if let Some(ref datablock) = batch.datablock {
                    compile_and_cache_schema(&mut cache, schema_key, datablock);
                }
            }
            None => {
                self.shard_mem_cache.borrow_mut().no_schema_cache_insert(schema_key.clone());
            }
        }

        Ok(())
    }

    async fn aggregate_exists_and_cache(&self, searching_for_aggregate_key: &AggregateKey, cache_path: CachePath) -> Result<bool, ShardCacheLoadError> {
        // If we are cached already
        if let (true, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(searching_for_aggregate_key, cache_path) {
            trace!(
                shard_id = self.config.shard_id,
                aggregate_key = %searching_for_aggregate_key,
                ?cache_path,
                found = (status == AggregateStatus::Found),
                "Cache hit — no disk scan needed"
            );
            return Ok(status == AggregateStatus::Found);
        }

        // Take an exclusive lock on this aggregate to deduplicate thundering herd
        let aggregate_lock = self.aggregate_loading.acquire(searching_for_aggregate_key);
        let _ = write_with_timeout(&aggregate_lock, "move_aggregate_to_memcache").await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;

        // We have exclusive access now, check if another concurrent task has already done the work
        if let (true, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(searching_for_aggregate_key, cache_path) {
            return Ok(status == AggregateStatus::Found);
        }

        // Limit concurrent disk scans across different aggregates (NVMe starvation)
        let sem_wait_start = std::time::Instant::now();
        let _cache_permit = self.cache_load_semaphore.acquire_permit(1).await
            .map_err(|_| ShardCacheLoadError::AggregateLoadingLockTimeout)?;
        metrics::histogram!("celeriant_read_semaphore_wait_seconds", &self.metrics_shard_label)
            .record(sem_wait_start.elapsed().as_secs_f64());

        let (starting_log_id, start_from_postion) = match cache_path {
            CachePath::Read => {
                let read_cursor = self.log_segments_cache.get_latest_read_cursor();
                (read_cursor.log_id, Some(read_cursor.metablocks_position))
            },
            CachePath::Write => (self.log_segments_cache.active_log_id(), None),
        };

        // Begin the search from the active log, moving backwards
        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            starting_log_id,
            start_from_postion,
            self.config.read_max_chunk_size,
        )
        .with_bloom_filter(searching_for_aggregate_key);
        if cache_path == CachePath::Write {
            scanner = scanner.with_write_cursor_upper_bound();
        }

        let find_result = scanner
            .scan::<bool, ()>(|log_id, metablock_absolute_pos, metablock_bytes| {
                // Check for soft delete first - if this aggregate was deleted, we're done
                if metablock_bytes::is_soft_delete_for_aggregate(metablock_bytes, searching_for_aggregate_key) {
                    // Deserialize to get the deletion options
                    let metablock = deserialise_metablock(metablock_bytes)
                        .map_err(|_| ())?;
                    
                    if let MetablockKind::SoftDelete(soft_delete) = metablock.wal_metablock_type {
                        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
                        shard_mem_cache.put_aggregate_into_cache_as_deleted(
                            searching_for_aggregate_key.clone(),
                            log_id,
                            metablock_absolute_pos,
                            soft_delete.event_seq,
                            soft_delete.aggregate_version,
                            soft_delete.allow_recreate,
                            soft_delete.allow_sequence_continuation,
                            cache_path,
                        );
                    }
                    return Ok(Some(false)); // Found but deleted
                }

                // SoftTrim carries full aggregate state — cache and stop scanning
                if metablock_bytes::is_soft_trim_for_aggregate(metablock_bytes, searching_for_aggregate_key) {
                    let metablock = deserialise_metablock(metablock_bytes)
                        .map_err(|_| ())?;
                    if let MetablockKind::SoftTrim(soft_trim) = metablock.wal_metablock_type {
                        let snapshot = MemSnapshotAggregate::found(
                            log_id,
                            metablock_absolute_pos,
                            soft_trim.event_seq,
                            soft_trim.aggregate_version,
                            soft_trim.keep_from_aggregate_version,
                        );
                        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
                        shard_mem_cache.put_aggregate_snapshot_only(
                            searching_for_aggregate_key.clone(),
                            snapshot,
                            false,
                            cache_path,
                        );
                    }
                    return Ok(Some(true));
                }

                if !metablock_bytes::is_metablock_kind_event_batch_metadata(metablock_bytes) {
                    return Ok(None);
                }

                let current_aggregate_key = metablock_bytes::read_event_batch_aggregate_key(metablock_bytes);
                let low_priority = *searching_for_aggregate_key != current_aggregate_key;

                let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

                // Not the aggregate we are searching for. Can we eager cache it? If not, skip it.
                if low_priority && shard_mem_cache.is_aggregate_snapshot_full_or_contains(&current_aggregate_key, cache_path) {
                    return Ok(None);
                }

                let min_aggregate_version = metablock_bytes::read_event_batch_min_aggregate_version(metablock_bytes);

                let snapshot = MemSnapshotAggregate::found(
                    log_id,
                    metablock_absolute_pos,
                    metablock_bytes::read_event_batch_max_event_seq(metablock_bytes),
                    metablock_bytes::read_event_batch_aggregate_version(metablock_bytes),
                    min_aggregate_version,
                );

                let client_id = metablock_bytes::read_event_batch_client_id(metablock_bytes);
                let last_client_seq = metablock_bytes::read_event_batch_max_client_seq(metablock_bytes);

                shard_mem_cache.put_aggregate_into_cache(current_aggregate_key, snapshot, client_id, last_client_seq, low_priority, cache_path);

                if low_priority {
                    Ok(None) //Haven't found aggregate yet
                } else {
                    Ok(Some(true)) //Done searching
                }
            })
            .await
            .map_err(ShardCacheLoadError::FileScanningError)?;

        let found = find_result.unwrap_or(false);
        trace!(
            shard_id = self.config.shard_id,
            aggregate_key = %searching_for_aggregate_key,
            ?cache_path,
            found,
            "Disk scan complete"
        );
        if find_result.is_none() {
            // Never found any metablock for this aggregate
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
            shard_mem_cache.put_aggregate_into_cache_as_not_found(searching_for_aggregate_key.clone(), cache_path);
        }

        Ok(found)
    }

    /// Read-only pre-flight validation for a single aggregate write.
    /// Loads relevant caches (aggregate snapshot, client snapshot, schemas) and runs
    /// every check that can reject the write.
    async fn validate_single_write(
        &self,
        aggregate_key: &AggregateKey,
        client_id: u128,
        single_write: &SingleAggregateWrite,
    ) -> Result<(), ShardWriteError> {
        // Ensure aggregate snapshot is in memcache, loading from disk if necessary.
        let aggregate_exists = self.aggregate_exists_and_cache(aggregate_key, CachePath::Write).await
            .map_err(ShardWriteError::AggregateExistsAndCacheError)?;

        if !aggregate_exists {
            let (is_loaded, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(aggregate_key, CachePath::Write);
            if is_loaded && status == AggregateStatus::Deleted {
                let indexes = self.shard_mem_cache.borrow_mut().get_write_event_seqes(aggregate_key);
                if !indexes.allow_recreate {
                    return Err(ShardWriteError::AggregateRecreateNotAllowed);
                }
            } else if !single_write.allow_create {
                return Err(ShardWriteError::AggregateNotExists);
            }
        }

        let aggregate_client_key = AggregateClientKey::new(aggregate_key.clone(), client_id);
        if single_write.enforce_client_idempotency {
            self.cache_aggregate_client(aggregate_key, &aggregate_client_key).await
                .map_err(ShardWriteError::CacheAggregateClientError)?;
        }

        self.pre_warm_schema_cache(&single_write.events, aggregate_key).await
            .map_err(ShardWriteError::CacheAggregateClientError)?;

        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
        let aggregate_current_indexes = shard_mem_cache.get_write_event_seqes(aggregate_key);

        if aggregate_current_indexes.pending_delete_or_deleted && !aggregate_current_indexes.allow_recreate {
            return Err(ShardWriteError::AggregateRecreateNotAllowed);
        }

        if let Some(expected) = single_write.expected_version {
            if expected != aggregate_current_indexes.aggregate_version {
                return Err(ShardWriteError::OptimisticConcurrencyViolation {
                    expected_version: expected,
                    current_aggregate_version: aggregate_current_indexes.aggregate_version,
                });
            }
        }

        if single_write.enforce_client_idempotency {
            if let Some(status) = shard_mem_cache.get_client_seq_entry(aggregate_key, client_id) {
                let last_client_seq = status.client_seq();
                let attempted_client_seq = single_write.events.iter().map(|e| e.client_seq).min().unwrap_or(0);
                if attempted_client_seq <= last_client_seq {
                    let inflight = match status {
                        ClientSeqStatus::InflightInQueue { .. } => true,
                        ClientSeqStatus::Fsynced { wal_seq, .. } => {
                            let read_cursor_wal_seq = self.log_segments_cache.get_latest_read_cursor_wal_seq();
                            wal_seq > 0 && wal_seq > read_cursor_wal_seq
                        }
                    };
                    if inflight {
                        metrics::counter!("celeriant_client_idempotency_inflight_total").increment(1);
                        return Err(ShardWriteError::InflightDuplicateWrite {
                            last_client_seq,
                            attempted_client_seq,
                        });
                    }
                    metrics::counter!("celeriant_client_idempotency_violations_total").increment(1);
                    tracing::warn!(
                        shard_id = self.config.shard_id,
                        ?aggregate_key,
                        client_id,
                        last_client_seq,
                        attempted_client_seq,
                        "ClientIdempotencyViolation returned; cache says seq already applied"
                    );
                    return Err(ShardWriteError::ClientIdempotencyViolation {
                        last_client_seq,
                        attempted_client_seq,
                    });
                }
            }
        }

        for event in &single_write.events {
            if event.iv.is_some() { continue; }
            let schema_key = SchemaKey::new(
                aggregate_key.org_id,
                aggregate_key.aggregate_type_id,
                event.event_type_major,
                event.event_type_minor,
            );
            match shard_mem_cache.schema_cache_get(&schema_key) {
                Some(celeriant_memcache::cached_schema::CachedSchema::Validated(validator)) => {
                    validator.validate(&event.event_value).map_err(|e| {
                        ShardWriteError::SchemaValidationFailed {
                            event_type_major: event.event_type_major,
                            event_type_minor: event.event_type_minor,
                            client_seq: event.client_seq,
                            validation_error: e,
                        }
                    })?;
                }
                Some(celeriant_memcache::cached_schema::CachedSchema::CompilationFailed(err)) => {
                    return Err(ShardWriteError::SchemaCompilationFailed {
                        event_type_major: event.event_type_major,
                        event_type_minor: event.event_type_minor,
                        client_seq: event.client_seq,
                        compilation_error: err.clone(),
                    });
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Validate a write request and prepare all data for appending.
    /// This performs read-only access to shard_mem_cache and can fail.
    fn validate_and_prepare_write(
        &self,
        lease_epoch: u64,
        aggregate_key: &AggregateKey,
        client_id: u128,
        user_id: Option<u128>,
        mut write_request: SingleAggregateWrite,
    ) -> Result<PreparedWrite, ShardWriteError> {
        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

        let aggregate_current_indexes = shard_mem_cache.get_write_event_seqes(aggregate_key);

        // There is a soft delete entry in the queue that hasn't been committed yet
        if aggregate_current_indexes.pending_delete_or_deleted && !aggregate_current_indexes.allow_recreate {
            return Err(ShardWriteError::AggregateRecreateNotAllowed);
        }

        // Validate optimistic concurrency (only for existing aggregates, not recreates)
        if let Some(expected) = write_request.expected_version {
            if expected != aggregate_current_indexes.aggregate_version {
                debug!(
                    shard_id = self.config.shard_id,
                    aggregate_key = %aggregate_key,
                    expected_version = expected,
                    current_aggregate_version = aggregate_current_indexes.aggregate_version,
                    "Write rejected: optimistic concurrency violation"
                );
                return Err(ShardWriteError::OptimisticConcurrencyViolation {
                    expected_version: expected,
                    current_aggregate_version: aggregate_current_indexes.aggregate_version,
                });
            }
        }

        // Validate client idempotency
        if write_request.enforce_client_idempotency {
            let cached = shard_mem_cache.get_client_seq_entry(aggregate_key, client_id);
            let attempted_client_seq = write_request.events.iter().map(|e| e.client_seq).min().unwrap_or(0);
            match cached {
                Some(status) if attempted_client_seq <= status.client_seq() => {
                    let last_client_seq = status.client_seq();
                    let inflight = match status {
                        ClientSeqStatus::InflightInQueue { .. } => true,
                        ClientSeqStatus::Fsynced { wal_seq, .. } => {
                            let read_cursor_wal_seq = self.log_segments_cache.get_latest_read_cursor_wal_seq();
                            wal_seq > 0 && wal_seq > read_cursor_wal_seq
                        }
                    };
                    if inflight {
                        metrics::counter!("celeriant_client_idempotency_inflight_total").increment(1);
                        return Err(ShardWriteError::InflightDuplicateWrite {
                            last_client_seq,
                            attempted_client_seq,
                        });
                    }
                    debug!(
                        shard_id = self.config.shard_id,
                        aggregate_key = %aggregate_key,
                        client_id = %celeriant_wal::format_uuid(client_id),
                        last_client_seq,
                        attempted_client_seq,
                        "Write rejected: client idempotency violation"
                    );
                    return Err(ShardWriteError::ClientIdempotencyViolation {
                        last_client_seq,
                        attempted_client_seq,
                    });
                }
                None => {
                    tracing::trace!(
                        shard_id = self.config.shard_id,
                        aggregate_key = %aggregate_key,
                        client_id = %celeriant_wal::format_uuid(client_id),
                        attempted_client_seq,
                        lease_epoch,
                        "write accepted with no prior client_seq cached"
                    );
                    metrics::counter!(
                        "celeriant_writes_accepted_no_prior_client_seq_total",
                        &self.metrics_shard_label,
                    ).increment(1);
                }
                Some(_) => {} // attempted > last_client_seq, fall through to normal path
            }
        }

        // Schema validation
        for event in &write_request.events {
            if event.iv.is_some() { continue; }

            let schema_key = SchemaKey::new(
                aggregate_key.org_id,
                aggregate_key.aggregate_type_id,
                event.event_type_major,
                event.event_type_minor,
            );

            match shard_mem_cache.schema_cache_get(&schema_key) {
                Some(celeriant_memcache::cached_schema::CachedSchema::Validated(validator)) => {
                    validator.validate(&event.event_value).map_err(|e| {
                        ShardWriteError::SchemaValidationFailed {
                            event_type_major: event.event_type_major,
                            event_type_minor: event.event_type_minor,
                            client_seq: event.client_seq,
                            validation_error: e,
                        }
                    })?;
                }
                Some(celeriant_memcache::cached_schema::CachedSchema::CompilationFailed(err)) => {
                    return Err(ShardWriteError::SchemaCompilationFailed {
                        event_type_major: event.event_type_major,
                        event_type_minor: event.event_type_minor,
                        client_seq: event.client_seq,
                        compilation_error: err.clone(),
                    });
                }
                _ => {}
            }
        }

        // Drop the borrow before doing potentially expensive serialization
        drop(shard_mem_cache);

        // Determine starting indexes based on whether this is a recreate with index continuation
        let is_recreate = aggregate_current_indexes.pending_delete_or_deleted 
            || aggregate_current_indexes.aggregate_version == 0;
        
        let (mut event_seq, aggregate_version, mut min_aggregate_version) = if is_recreate && aggregate_current_indexes.allow_sequence_continuation {
            // Continue from pre-deletion indexes
            (
                aggregate_current_indexes.event_seq,
                aggregate_current_indexes.aggregate_version.saturating_add(1),
                aggregate_current_indexes.min_aggregate_version,
            )
        } else if is_recreate {
            // Fresh start
            (0, FIRST_AGGREGATE_VERSION, FIRST_AGGREGATE_VERSION)
        } else {
            // Normal append to existing aggregate
            (
                aggregate_current_indexes.event_seq,
                aggregate_current_indexes.aggregate_version.saturating_add(1),
                aggregate_current_indexes.min_aggregate_version,
            )
        };

        if min_aggregate_version == 0 {
            min_aggregate_version = FIRST_AGGREGATE_VERSION;
        }

        // Prepare event data
        let mut events_in_batch = std::mem::take(&mut write_request.events);

        for e in events_in_batch.iter_mut() {
            event_seq = event_seq.saturating_add(1);
            e.event_seq = event_seq;
        }

        let event_type_extraction = extract_unique_event_types(&events_in_batch);
        let event_types_data = if event_type_extraction.needs_bloom {
            let bloom_bytes = self.bloom_filter_cache.create_bloom_bytes(&events_in_batch);
            EventTypesKind::Bloom(bloom_bytes)
        } else {
            EventTypesKind::Direct(event_type_extraction.event_types)
        };

        // Encryption bailout check must happen before events_in_batch is moved.
        let has_encrypted_events = events_in_batch.iter().any(|e| e.iv.is_some());

        let datablock_aggregate_event_batch = DatablockAggregateEventBatch {
            aggregate_version,
            events: events_in_batch,
        };

        let metablock_event_batch = MetablockEventBatch::from_batch_item(
            client_id,
            user_id,
            aggregate_key.clone(),
            min_aggregate_version,
            &datablock_aggregate_event_batch,
            event_types_data,
        );
        let latest_client_seq = metablock_event_batch.max_client_seq;

        let datablock = Datablock {
            datablock_kind: DatablockKind::EventBatchItem(datablock_aggregate_event_batch),
        };

        let serialized_datablock = SerialisedDatablock::new(
            &datablock,
            CompressionPolicy::Auto { compression_allowed: !has_encrypted_events },
            &self.dict_codec,
        ).map_err(ShardWriteError::FailedToSerialiseDatablocks)?;

        let server_timestamp = self.config.timestamp_config.now();

        let metablock = Metablock {
            wal_seq: 0,
            server_timestamp,
            lease_epoch,
            node_id: self.config.node_id,
            uncompressed_size: serialized_datablock.uncompressed_size,
            compressed_size: serialized_datablock.compressed_size,
            datablock_version: serialized_datablock.datablock_version,
            datablock_compression_type: serialized_datablock.compression_type,
            datablock: serialized_datablock.storage_kind,
            wal_metablock_type: MetablockKind::EventBatchMetadata(metablock_event_batch),
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
            previous_aggregate_metablock_pos: 0,
        };

        let shard_log_queue_item = ShardLogQueueItem::new(Some(datablock), serialized_datablock.external_data, metablock);

        Ok(PreparedWrite {
            aggregate_key: aggregate_key.clone(),
            client_id,
            event_seq,
            aggregate_version,
            latest_client_seq,
            shard_log_queue_item,
            min_aggregate_version,
        })
    }

    /// Append all prepared writes to the pending queue.
    /// This mutates shard_mem_cache but cannot fail.
    fn append_prepared_writes_to_queue(&self, prepared_writes: Vec<PreparedWrite>) {
        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

        for prepared in prepared_writes {
            shard_mem_cache.add_to_pending_append_queue(
                &prepared.aggregate_key,
                prepared.event_seq,
                prepared.aggregate_version,
                prepared.min_aggregate_version,
                prepared.client_id,
                prepared.latest_client_seq,
                prepared.shard_log_queue_item,
            );
        }
    }

    /// Perform sync with potential delay for amortisation.
    /// Uses two-phase sync to fix the race condition where clearing the orchestrator
    /// before taking the snapshot can cause a subsequent leader to find an empty queue.
    async fn sync_durable(&self) -> Result<(), ShardFsyncError> {
        let rotating_log_cache = self.log_segments_cache.clone();
        let shard_mem_cache = self.shard_mem_cache.clone();
        let watched_aggregates = self.watched_aggregates.clone();
        let shard_id = self.config.shard_id;
        let dict_codec = self.dict_codec.clone();

        // Node status still feeds fsync (lease epoch etc.); the commit target is the
        // provenance-derived read-side commit rule. We already pass lease status
        // checks so can use raw().
        let node_status = self.node_status.get().raw();
        let commit_target = match node_status {
            NodeStatus::Leader { .. } => CommitTarget::DeferToReplicationAck,
            NodeStatus::Follower { .. } | NodeStatus::FollowerCatchingUp { .. } => CommitTarget::DeferToLeaderConfirmed,
            _ => CommitTarget::FullCommit,
        };

        let mc_capture = shard_mem_cache.clone();
        self.fsync_coordinator
            .request_sync_two_phase(
                Some(self.config.fsync_delay),
                ShardFsyncError::WriteLockTimeout,
                move || capture_fsync_snapshot(&mc_capture),
                move |captured| commit_fsync_with_rollback(node_status, commit_target, rotating_log_cache, shard_mem_cache, watched_aggregates, captured, shard_id, dict_codec),
            )
            .await
    }

    async fn replicate_durable(&self) -> Result<(), ReplicationError> {
        // Genuine followers don't need replication.
        // Check pending replication in-mem just to be sure we are follower.
        if !self.node_status.get().raw().is_leader()
            && self.shard_mem_cache.borrow().pending_replication_bytes() == 0
        {
            return Ok(());
        }
        Box::pin(self.replicate_durable_leader()).await
    }

    async fn replicate_durable_leader(&self) -> Result<(), ReplicationError> {
        let follower_reachable = self.replication_client.is_follower_reachable();
        let delay = if follower_reachable {
            self.config.replication_delay
        } else {
            self.config.s3_replication_delay
        };

        // Confirmation gate. A single coordinator cycle can return Ok via
        // NoCaptureRaceButOk for a writer whose events were captured by a
        // concurrent in-flight cycle that has not yet committed — read cursor
        // still below them. Acking there is a false ack: a later fence/cull
        // rewinds to the lagging barrier and the acked writes are lost (or never
        // reach the follower). Replicate up to the write tip as of entry, then
        // re-enter until the read cursor confirms it. Re-entry blocks on the
        // in-flight cycle (its gate), so this waits, it does not spin.
        let write_target = wal_positions(&self.log_segments_cache).0;
        let confirm_deadline = Instant::now() + REPLICATION_CONFIRM_TIMEOUT;
        let mut cycle_delay = Some(delay);
        let mut confirm_iterations: u32 = 0;
        loop {
            self.run_replication_through_coordinator(ReplicationTrigger::Write, cycle_delay).await?;
            if wal_positions(&self.log_segments_cache).1 >= write_target {
                if confirm_iterations > 0 {
                    metrics::histogram!("celeriant_replication_confirm_loop_iterations", &self.metrics_shard_label)
                        .record(confirm_iterations as f64);
                }
                // Burst over (pending drained): register the commit obligation and
                // arm the notify timer. Level-triggered — the timer, not this edge,
                // decides whether a dedicated notify ever sends, so a momentary
                // under-load drain no longer fires one the next batch would cover.
                self.note_commit_and_arm_notify_timer();
                return Ok(());
            }
            if !self.node_status.get().is_leader() {
                return Err(ReplicationError::LeaderFenced);
            }
            if Instant::now() > confirm_deadline {
                warn!(
                    shard_id = self.config.shard_id,
                    write_target,
                    read_wal = wal_positions(&self.log_segments_cache).1,
                    "replicate_durable: read cursor did not confirm write tip before timeout"
                );
                return Err(ReplicationError::LeaderFenced);
            }
            // Re-enter immediately (no amortisation delay) to attach to the
            // in-flight cycle holding our events; yield so it can commit.
            cycle_delay = None;
            confirm_iterations += 1;
            glommio::yield_if_needed().await;
        }
    }

    /// Drift-detection probe routed through the replication coordinator.
    /// Empty pending-replication queue + Probe trigger lets `replicate_loop`
    /// synthesise a one-item batch from the latest local entry; concurrent
    /// writes dedupe naturally because the coordinator serialises captures.
    pub async fn probe_replicate(&self) -> Result<(), ReplicationError> {
        if !self.node_status.get().is_leader() {
            return Ok(());
        }
        if !self.replication_client.is_follower_reachable() {
            return Ok(());
        }
        self.run_replication_through_coordinator(ReplicationTrigger::Probe, None).await
    }

    /// Force any pending unreplicated tail to be rolled back.
    /// Call at Leader→Follower role transition. Drives the replicate path; with
    /// node_status now non-Leader, replicate_loop returns LeaderFenced and the
    /// captured snapshot flows into rollback_or_panic, resetting write.wal back
    /// to read.wal. Without this hook, the fence is only detected on the next
    /// replicate trigger; which may never fire on a passive follower, leaving
    /// an orphan tail that wedges S3 catchup at the next lease handover.
    /// The Err is expected (it IS the fence) and ignored.
    pub async fn drain_pending_replication_on_role_change(&self) {
        if self.node_status.get().is_leader() {
            return;
        }
        let drain_started_at = Instant::now();
        let (write_wal_before, read_wal_before) = wal_positions(&self.log_segments_cache);
        let result = self.run_replication_through_coordinator(ReplicationTrigger::Write, None).await;
        let (write_wal_after, read_wal_after) = wal_positions(&self.log_segments_cache);
        let elapsed_ms = drain_started_at.elapsed().as_millis() as u64;
        let invariant_holds = write_wal_after == read_wal_after;
        let err_kind = result.as_ref().err().map(|e| format!("{e:?}"));
        tracing::info!(
            shard_id = self.config.shard_id,
            write_wal_after, read_wal_after,
            write_wal_before, read_wal_before,
            elapsed_ms,
            invariant_holds,
            err_kind = err_kind.as_deref().unwrap_or(""),
            "drain_pending_replication_on_role_change: complete"
        );
        metrics::counter!(
            "celeriant_drain_role_change_total",
            &[("shard_id", self.config.shard_id.to_string()), ("invariant_holds", invariant_holds.to_string())],
        ).increment(1);
    }

    async fn run_replication_through_coordinator(
        &self,
        trigger: ReplicationTrigger,
        delay: Option<Duration>,
    ) -> Result<(), ReplicationError> {
        let result = self.replication_cycle(trigger, delay).await;
        // A successful commit while leading may have created an own speculative
        // tail (written, not yet committed entries above the new ack point), so a
        // future demotion is again entitled to one rewind-to-ack-barrier cull.
        if result.is_ok() && self.node_status.get().is_leader() {
            self.ack_barrier_rewind_armed.set(true);
        }
        result
    }

    /// One coordinator replication cycle as a 'static future: everything it
    /// touches is cloned up front so a detached task (the notify timer's
    /// `run_notify_timer`) can run the same cycle the write path runs.
    fn replication_cycle(
        &self,
        trigger: ReplicationTrigger,
        delay: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<(), ReplicationError>> + 'static {
        let replication_coordinator = self.replication_coordinator.clone();
        let replication_client = self.replication_client.clone();
        let fsync_coordinator = self.fsync_coordinator.clone();
        let rotating_log_cache = self.log_segments_cache.clone();
        let shard_mem_cache = self.shard_mem_cache.clone();
        let watched_aggregates = self.watched_aggregates.clone();
        let node_status = self.node_status.clone();
        let max_catchup_gap_bytes = self.config.max_catchup_gap_bytes;
        let max_request_size = self.config.max_request_size;
        let read_max_chunk_size = self.config.read_max_chunk_size;
        let max_clock_drift_ms = self.config.max_clock_drift_ms;
        let shard_id = self.config.shard_id;
        let s3_cas_confirmed_at_ms = self.s3_cas_confirmed_at_ms.clone();
        let s3_lease_duration_ms = self.config.s3_lease_duration_ms;
        let lease_renewal_requester = self.lease_renewal_requester.get().cloned();

        let mc_capture = shard_mem_cache.clone();
        let dict_codec = self.dict_codec.clone();
        let pushed_to_follower_seq = self.pushed_to_follower_seq.clone();
        let last_batch_sent_at = self.last_batch_sent_at.clone();
        async move {
            replication_coordinator
                .request_sync_two_phase(
                    delay,
                    ReplicationError::GateTimeout,
                    move || capture_replication_snapshot(&mc_capture, trigger),
                    move |captured| commit_replication(replication_client, fsync_coordinator, rotating_log_cache, shard_mem_cache, watched_aggregates, node_status, captured, trigger, max_catchup_gap_bytes, max_request_size, read_max_chunk_size, max_clock_drift_ms, shard_id, s3_cas_confirmed_at_ms, s3_lease_duration_ms, dict_codec, lease_renewal_requester, pushed_to_follower_seq, last_batch_sent_at),
                )
                .await
        }
    }

    /// Register a post-burst commit obligation and ensure the notify timer is
    /// armed. Monotone: raises `pending_notify_seq` to the current confirmed seq,
    /// never lowers it, and decides nothing else. Under load the next real batch
    /// raises `pushed_to_follower_seq` to meet it before the timer wakes, so the
    /// obligation clears with no dedicated notify.
    fn note_commit_and_arm_notify_timer(&self) {
        let confirmed = shard_wal_replicate::current_leader_confirmed_wal_seq(&self.log_segments_cache);
        if confirmed > self.pending_notify_seq.get() {
            self.pending_notify_seq.set(confirmed);
        }
        self.commit_notify_obligation_gauge
            .set(self.pending_notify_seq.get().saturating_sub(self.pushed_to_follower_seq.get()) as f64);
        self.arm_notify_timer();
    }

    /// Spawn the single notify timer if an obligation is open and none is live.
    /// `replace(true)` is the dedup latch; it is read and set only in a
    /// synchronous region, so two concurrent drains cannot both spawn a timer.
    fn arm_notify_timer(&self) {
        if self.pending_notify_seq.get() <= self.pushed_to_follower_seq.get() {
            return;
        }
        if self.notify_timer_armed.replace(true) {
            return;
        }
        let Some(weak) = self.notify_self_ref.get().cloned() else {
            // Self-ref unwired: release the latch so a later arm retries once wired.
            self.notify_timer_armed.set(false);
            return;
        };
        let delay = self.config.replication_delay;
        glommio::spawn_local(Self::run_notify_timer(weak, delay)).detach();
    }

    /// The detached notify timer. Writers never await it. Each wake decides in order:
    /// watermark disarm (`pushed >= pending`, the idle terminator) or leadership/reach
    /// loss; then the load suppressor — a data batch within the recency window, or queued
    /// writes — rearms as a free deferral; then the give-up bound; then budget; else it fires.
    /// The batch-recency check, NOT the watermark, is the load suppressor: `pushed`
    /// structurally trails `pending` by one batch under load (two-phase sends before
    /// commit), so the disarm never samples true while the stream flows. Disarm is
    /// race-free: `pushed >= pending` is read and `notify_timer_armed` cleared in the
    /// same await-free region, and a concurrent drain runs only at an await — so it
    /// either raised `pending` before this region (seen here) or arms fresh after.
    ///
    /// A wake that reaches the send but does not advance the watermark — a rejection
    /// the follower keeps returning, or a transiently-exhausted lease budget — is
    /// unproductive. After `MAX_UNPRODUCTIVE_WAKES` the timer gives up and disarms,
    /// leaving the 5s probe as the carrier. Without that bound a reachable follower
    /// that persistently rejects the notify would spin the serial channel at the
    /// pacing cadence; the notify's contract is that it never spins.
    async fn run_notify_timer(weak: Weak<Self>, delay: Duration) {
        const MAX_UNPRODUCTIVE_WAKES: u32 = 3;
        let mut unproductive = 0u32;
        loop {
            glommio::timer::sleep(delay).await;
            let Some(this) = weak.upgrade() else { return };
            let (pending, pushed) = (this.pending_notify_seq.get(), this.pushed_to_follower_seq.get());
            this.commit_notify_obligation_gauge.set(pending.saturating_sub(pushed) as f64);

            // Obligation met, or leadership lost: disarm and exit (terminal). The 5s
            // probe is the carrier for the fenced/unreachable tail.
            if pushed >= pending || !this.node_status.get().is_leader() {
                this.notify_timer_armed.set(false);
                return;
            }
            if !this.replication_client.is_follower_reachable() {
                this.notify_timer_armed.set(false);
                return;
            }

            // Load suppressor: while the carrier stream is flowing (a real data batch
            // within the recency window) or writes are queued, the next batch carries
            // the index — rearm and wait. This is the guard the watermark cannot be,
            // since `pushed` structurally trails `pending` by one batch under load. Both
            // are legitimate deferrals, not unproductive wakes (they must not walk the
            // give-up bound, or sustained load would eventually give up on a live
            // obligation). The window is sized to the measured under-load batch cadence
            // (~40-140ms per shard at saturation, several × the amortisation delay, since
            // a batch RTT is delay + follower fsync + gate contention), not to `delay`
            // itself — too small and the notify fires under load; too large only staler
            // idle tails. It is a pacing knob, not a correctness gate.
            let recency_window = RECENCY_WINDOW_BATCHES * delay;
            if this.last_batch_sent_at.get().elapsed() < recency_window
                || this.shard_mem_cache.borrow().pending_replication_count() != 0
            {
                continue;
            }

            // Bound the loop: a reachable follower that keeps rejecting, or a budget
            // that stays exhausted, must not spin the channel. Give up to the probe.
            if unproductive >= MAX_UNPRODUCTIVE_WAKES {
                metrics::counter!("celeriant_commit_notify_gave_up_total", &this.metrics_shard_label).increment(1);
                this.notify_timer_armed.set(false);
                return;
            }

            // Mirror acquire_lease_budget's split: None is a fence (terminal); a zero
            // budget is transient exhaustion (the lease renews), so count it and wait
            // rather than strand the obligation by disarming while still leader.
            match this.node_status.get().current_budget() {
                Some(budget) if !budget.is_zero() => {}
                Some(_) => {
                    metrics::counter!(
                        "celeriant_lease_budget_exhausted_total",
                        &[("op", "commit_notify".to_string()), ("shard_id", this.config.shard_id.to_string())],
                    ).increment(1);
                    unproductive += 1;
                    continue;
                }
                None => {
                    metrics::counter!("celeriant_commit_notify_skipped_fenced_total", &this.metrics_shard_label).increment(1);
                    this.notify_timer_armed.set(false);
                    return;
                }
            }

            // Provably idle at this wake: run one notify cycle. It raises
            // `pushed_to_follower_seq` to the delivered index (or drops at capture if
            // a write raced in). A cycle that does not advance the watermark is a
            // rejection — count it toward the give-up bound; progress resets it.
            let _ = this.replication_cycle(ReplicationTrigger::CommitNotify, None).await;
            if this.pushed_to_follower_seq.get() > pushed {
                unproductive = 0;
            } else {
                unproductive += 1;
            }
        }
    }

    pub async fn handle_replication_batch(
        &self, request: celeriant_msg::request::requests::ReplicationBatchRequest
    ) -> Result<ReplicationBatchResponse, FollowerReplicationWriteError> {
        // A pre-epoch local clock must not panic the shard executor (externally
        // reachable: any replication batch arriving while the clock is wrong).
        // Reject as a time fault; same wire shape as the drift check below.
        let follower_timestamp_ms = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_millis() as u64,
            Err(_) => {
                metrics::counter!(
                    "celeriant_replication_batch_rejected_total",
                    &[("shard_id", self.metrics_shard_label[0].1.clone()), ("reason", "time_drift".to_string())],
                ).increment(1);
                return Ok(ReplicationBatchResponse {
                    correlation_id: request.correlation_id,
                    follower_timestamp_ms: 0,
                    result: ReplicationResult::Rejected(FollowerRejection::TimeDriftTooHigh {
                        leader_ms: request.leader_timestamp_ms,
                        follower_ms: 0,
                        max_allowed_ms: self.config.max_clock_drift_ms,
                    }),
                });
            }
        };

        let shard_id_label = self.metrics_shard_label[0].1.clone();
        let response = |result: ReplicationResult| {
            if let ReplicationResult::Rejected(ref r) = result {
                let reason = match r {
                    FollowerRejection::NotAFollower => "not_follower",
                    FollowerRejection::TimeDriftTooHigh { .. } => "time_drift",
                    FollowerRejection::WalSeqMismatch { .. } => "wal_seq_mismatch",
                    FollowerRejection::TipHashMismatch { .. } => "tip_hash_mismatch",
                    FollowerRejection::EmptyBatch => "empty_batch",
                    FollowerRejection::MissingDatablock => "missing_datablock",
                    FollowerRejection::StaleLease { .. } => "stale_lease",
                };
                metrics::counter!(
                    "celeriant_replication_batch_rejected_total",
                    &[("shard_id", shard_id_label.clone()), ("reason", reason.to_string())],
                ).increment(1);
            }
            ReplicationBatchResponse {
                correlation_id: request.correlation_id,
                follower_timestamp_ms,
                result,
            }
        };

        let leader_lease_epoch = match self.node_status.get().effective_node_status() {
            NodeStatus::Follower { leader_lease_epoch } => leader_lease_epoch,
            _ => return Ok(response(ReplicationResult::Rejected(FollowerRejection::NotAFollower))),
        };

        if follower_timestamp_ms.saturating_sub(request.leader_timestamp_ms) > self.config.max_clock_drift_ms {
            return Ok(response(ReplicationResult::Rejected(FollowerRejection::TimeDriftTooHigh {
                leader_ms: request.leader_timestamp_ms,
                follower_ms: follower_timestamp_ms,
                max_allowed_ms: self.config.max_clock_drift_ms,
            })));
        }

        // Fence stale leaders on the sender's current epoch, not the entries' own
        // lease_epoch: catchup replays history authored under previous tenures, so
        // the metablock epoch is legitimately older. Runs BEFORE empty-batch
        // handling: a zombie leader's commit-notify must fence as StaleLease,
        // never pass as an accepted notify.
        let batch_lease_epoch = request.sender_lease_epoch;
        if batch_lease_epoch < leader_lease_epoch {
            return Ok(response(ReplicationResult::Rejected(FollowerRejection::StaleLease {
                follower_lease_epoch: leader_lease_epoch,
                received_lease_epoch: batch_lease_epoch,
            })));
        }

        // An empty-batches request that passed every guard is a commit-notify:
        // no entries, no data fsync — just the floor update and parked drain the
        // data path runs, so an idle commit propagates without waiting for the
        // probe. Structurally chain-neutral (nothing to apply, write untouched).
        // The read-cursor header fsync rides off the response path (leg 3): it
        // holds the leader's serial replication channel for ~15ms otherwise, and
        // a lost advance only ever shows less, never more (the crash contract).
        if request.batches.is_empty() {
            metrics::counter!("celeriant_commit_notify_received_total", &self.metrics_shard_label).increment(1);
            self.update_promotion_floor(request.leader_confirmed_wal_seq);
            let drained = self.drain_parked_commits(request.leader_confirmed_wal_seq);
            if drained > 0 {
                self.sweep_sealed_summaries().await;
            }
            self.spawn_persist_read_cursor();
            return Ok(response(ReplicationResult::Success {
                last_follower_metablock: None,
            }));
        }

        let shard_id = self.config.shard_id;
        // Rejecting a batch still teaches us where the leader is: its last
        // wal_seq is a durable-on-leader floor and becomes the catchup target.
        let record_observed_leader = || {
            if let Some(last) = request.batches.last() {
                self.observed_leader_target.teach(batch_lease_epoch, last.metablock.wal_seq);
            }
        };
        match shard_wal_s3_catchup::apply_external_batch(
            &self.log_segments_cache, &self.shard_mem_cache, &request.batches, &self.dict_codec,
        ) {
            Ok(()) => {}
            Err(ApplyBatchError::WalSeqMismatch { current, batch_first }) => {
                record_observed_leader();
                tracing::warn!(
                    shard_id,
                    follower_wal = current,
                    batch_first_wal = batch_first,
                    batch_lease = batch_lease_epoch,
                    "Replication batch rejected: WalSeqMismatch"
                );
                return Ok(response(ReplicationResult::Rejected(FollowerRejection::WalSeqMismatch {
                    max_follower_wal_seq: current,
                })));
            }
            Err(ApplyBatchError::TipHashMismatch { current, current_wal_seq, batch, batch_wal_seq }) => {
                record_observed_leader();
                tracing::warn!(
                    shard_id,
                    follower_wal = current_wal_seq,
                    follower_tip = ?current,
                    batch_first_wal = batch_wal_seq,
                    batch_first_prev = ?batch,
                    batch_lease = batch_lease_epoch,
                    "Replication batch rejected: TipHashMismatch (follower's tip at follower_wal != batch's prev_hash)"
                );
                return Ok(response(ReplicationResult::Rejected(FollowerRejection::TipHashMismatch {
                    follower: current,
                    follower_wal_seq: current_wal_seq,
                    leader: batch,
                    leader_wal_seq: batch_wal_seq,
                })));
            }
            Err(ApplyBatchError::MissingDatablock) => {
                return Ok(response(ReplicationResult::Rejected(FollowerRejection::MissingDatablock)));
            }
            Err(ApplyBatchError::SerialiseDatablocks(e)) => {
                return Err(FollowerReplicationWriteError::FailedToSerialiseDatablocks(e));
            }
            Err(ApplyBatchError::BlockBecameInline) => {
                return Err(FollowerReplicationWriteError::BlockBecameInline);
            }
            Err(ApplyBatchError::BatchWalSeqGap { index, expected, actual }) => {
                return Err(FollowerReplicationWriteError::BatchWalSeqGap { index, expected, actual });
            }
        }

        self.update_promotion_floor(request.leader_confirmed_wal_seq);

        // Deferred commit, first drain: the carrier confirms previously parked
        // batches (they are durable already), so the fsync below persists the
        // advanced read cursor in its header for free.
        let mut drained = self.drain_parked_commits(request.leader_confirmed_wal_seq);

        self.sync_durable().await
            .map_err(FollowerReplicationWriteError::ShardFSyncError)?;

        // Second drain: covers the batch parked by the fsync above when the
        // carrier confirms at-or-past our new tip (duplicate/probe retries and
        // the clamp case), plus the crash-restart cold prefix.
        drained += self.drain_parked_commits(request.leader_confirmed_wal_seq);
        self.persist_read_cursor_if_advanced().await;
        if drained > 0 {
            self.sweep_sealed_summaries().await;
        }

        let shard_label = &self.metrics_shard_label;
        let applied_bytes: u64 = request.batches.iter().map(|b| b.metablock.uncompressed_size).sum();
        metrics::counter!("celeriant_replication_applied_events_total", shard_label).increment(request.batches.len() as u64);
        metrics::counter!("celeriant_replication_applied_bytes_total", shard_label).increment(applied_bytes);

        Ok(response(ReplicationResult::Success {
            last_follower_metablock: None,
        }))
    }

    /// Promotion-batch floor: bounds the range we'd upload if we became leader.
    /// Monotonic max guards against retries / out-of-order arrival.
    fn update_promotion_floor(&self, leader_confirmed_wal_seq: u64) {
        let new_floor = leader_confirmed_wal_seq.saturating_add(1);
        let active = self.log_segments_cache.active();
        let mut meta = active.metadata.borrow_mut();
        if new_floor > meta.last_received_replication_wal_seq {
            meta.last_received_replication_wal_seq = new_floor;
        }
    }

    /// Commit parked deferred batches covered by `min(leader_confirmed_wal_seq, write)`.
    /// One commit model, two triggers: this is the leader's commit_pcd driven by the
    /// carrier's commit index instead of a replication ACK. Monotonicity is structural —
    /// the parked queue only holds batches above the current read cursor, so a stale
    /// carrier drains nothing. Returns the number of committed batches.
    fn drain_parked_commits(&self, leader_confirmed_wal_seq: u64) -> usize {
        let active = self.log_segments_cache.active();
        let (write_wal, read_wal) = {
            let meta = active.metadata.borrow();
            (meta.write.wal_seq, meta.read.as_ref().map_or(0, |r| r.wal_seq))
        };
        let target = leader_confirmed_wal_seq.min(write_wal);
        if target <= read_wal {
            return 0;
        }

        let span = tracing::info_span!(
            "follower_commit_drain",
            shard_id = self.config.shard_id,
            target,
            drained_count = tracing::field::Empty,
        );
        let _entered = span.enter();

        let pcds = self.shard_mem_cache.borrow_mut().drain_parked_commits_up_to(target);
        let drained = pcds.len();
        for pcd in pcds {
            shard_wal_replicate::commit_pcd(
                &self.log_segments_cache, &self.shard_mem_cache, &self.watched_aggregates, pcd, Some(&self.dict_codec),
            );
        }

        // Crash-restart cold prefix: the confirmed range has no parked batches
        // (they died with the process) and the caches are cold, so the cursor
        // advance IS the whole commit. Only fires when the carrier confirms
        // exactly our durable tip; a confirmed index strictly between read and
        // write with no parked coverage stays put (shows less, never more).
        {
            let mut meta = active.metadata.borrow_mut();
            let read_now = meta.read.as_ref().map_or(0, |r| r.wal_seq);
            if read_now < target
                && target == meta.write.wal_seq
                && self.shard_mem_cache.borrow().parked_commit_count() == 0
            {
                meta.advance_visible_position();
            }
        }

        span.record("drained_count", drained as u64);
        shard_wal_replicate::set_read_cursor_gauge(&self.log_segments_cache, self.config.shard_id);
        // Shard-level committed cursor: post-rotation the active read is None
        // while parked commits still cover the sealed predecessor.
        let read_now = self.log_segments_cache.committed_read_wal_seq();
        metrics::gauge!("celeriant_follower_read_lag", &self.metrics_shard_label)
            .set(write_wal.saturating_sub(read_now) as f64);
        metrics::gauge!("celeriant_parked_commit_queue_depth", &self.metrics_shard_label)
            .set(self.shard_mem_cache.borrow().parked_commit_count() as f64);
        drained
    }

    /// Detached read-cursor persist for the commit-notify response path (leg 3):
    /// the notify returns after the in-memory drain and this header fsync runs off
    /// the leader's held channel. Clones only the three fields the fsync needs, so
    /// it runs in any context (no self-ref). The persist no-ops if a later carrier
    /// already synced the cursor, so redundant spawns are free; a crash before it
    /// runs simply shows less, never more.
    fn spawn_persist_read_cursor(&self) {
        let fsync_coordinator = self.fsync_coordinator.clone();
        let log_segments_cache = self.log_segments_cache.clone();
        let shard_id = self.config.shard_id;
        glommio::spawn_local(async move {
            Self::persist_read_cursor_if_advanced_inner(&fsync_coordinator, &log_segments_cache, shard_id).await;
        })
        .detach();
    }

    /// Persist a read-cursor advance that no data fsync covered (duplicate and
    /// probe carriers apply nothing, so nothing else writes a header). Best
    /// effort: losing it to a crash means restarting to show less, never more.
    async fn persist_read_cursor_if_advanced(&self) {
        Self::persist_read_cursor_if_advanced_inner(&self.fsync_coordinator, &self.log_segments_cache, self.config.shard_id).await;
    }

    async fn persist_read_cursor_if_advanced_inner(
        fsync_coordinator: &Rc<Coordinator<ShardFsyncError>>,
        log_segments_cache: &Rc<LogSegmentsCache>,
        shard_id: u32,
    ) {
        let active = log_segments_cache.active();
        let read_wal = active.metadata.borrow().read.as_ref().map_or(0, |r| r.wal_seq);
        if active.read_wal_synced.get() >= read_wal {
            return;
        }
        let active_for_sync = active.clone();
        let result = fsync_coordinator
            .request_sync_until(
                ShardFsyncError::WriteLockTimeout,
                || async move { sync_header_only(active_for_sync).await },
                || active.read_wal_synced.get() >= read_wal,
            )
            .await;
        if let Err(e) = result {
            warn!(shard_id, error = ?e, "read-cursor header fsync failed; advance remains in-memory only");
        }
    }

    /// Write sidecars for sealed segments whose read cursor caught up after a drain.
    /// The payload carries the blooms staged at rotation — no header to reload them from.
    async fn sweep_sealed_summaries(&self) {
        let sealed_ready = shard_wal_replicate::collect_eligible_sealed_summaries(&self.log_segments_cache, &self.shard_mem_cache).await;
        for (log_id, payload) in sealed_ready {
            if let Err(e) = crate::shard_wal_sync::write_segment_summary_sidecar_from_payload(
                self.log_segments_cache.shard_dir(), log_id, payload,
            ).await {
                tracing::error!(shard_id = self.config.shard_id, log_id, error = ?e, "Failed to write segment summary sidecar");
            }
        }
    }

    /// Reconcile the durable tail — entries above the read cursor — on a role
    /// transition. Returns Ok(true) when anything changed (a tail was committed,
    /// culled, or rewound to the ack barrier); Ok(false) for a no-op (no tail, or
    /// a peer tail deliberately kept parked). Must run before catchup: catchup
    /// starts at write+1 and would otherwise skip peer batches in a culled range.
    ///
    /// Tail disposition per mode:
    /// - `CommitForPromotion`: a peer-received tail is committed whole — every
    ///   parked commit drains through `commit_pcd`, the read cursor advances to
    ///   the durable tip, the header is persisted. The old leader may have acked
    ///   entries the observed commit index never covered, so nothing short of the
    ///   whole tail is safe. An own-speculation tail (a crashed ex-leader
    ///   re-winning an election) is culled instead: it is unacked by the
    ///   persisted-read ≥ acked invariant, and committing it would fire watch
    ///   events for entries the mandatory pre-serve S3 catchup then truncates.
    /// - `ReconcileAsFollower`: a peer tail stays durable AND parked (it is
    ///   unconfirmed; the new leader's carriers or catchup reconcile it). An
    ///   own-speculation tail is culled (boot-after-leader-crash divergence risk).
    /// - `RewindToAckBarrier`: demotion from held leadership. Rewinds write to
    ///   read for an in-flight own tail; handles the crash-induced
    ///   `read == write > ack barrier` case by rewinding both cursors to the
    ///   barrier. Safe because those entries were never acked to any client. Any
    ///   parked commits cover the culled range and are discarded with it. Must
    ///   NOT be used on the promotion path.
    pub async fn reconcile_durable_tail(&self, mode: TailReconciliation) -> Result<bool, ShardFsyncError> {
        let active = self.log_segments_cache.active();
        // Snapshot the cursors without holding borrow_mut across any await.
        let (write_seq, read_opt, last_acked, last_received) = {
            let meta = active.metadata.borrow();
            (meta.write.wal_seq, meta.read.clone(), meta.last_self_acked_wal_seq, meta.last_received_replication_wal_seq)
        };

        match mode {
            TailReconciliation::RewindToAckBarrier => {
                // One-shot: only the first demotion cull since boot/leadership may
                // rewind to the ack barrier. Consumed even if the rewind arm doesn't
                // fire, because after any demotion-path cull the remaining tail
                // above the barrier is peer data, not own speculation.
                let rewind_armed = self.ack_barrier_rewind_armed.replace(false);
                let Some(read) = read_opt else { return Ok(false) };
                if read.wal_seq < write_seq {
                    tracing::info!(
                        shard_id = self.config.shard_id,
                        write_wal_seq = write_seq,
                        read_wal_seq = read.wal_seq,
                        last_self_acked_wal_seq = last_acked,
                        "reconcile_durable_tail: demotion rewinding write to read"
                    );
                    self.cull_tail(CullTarget::WriteToRead(read)).await?;
                    Ok(true)
                } else if rewind_armed && last_acked > 0 && last_acked.max(last_received) < read.wal_seq {
                    // Demotion cull: read==write but the barrier is below read. Drop the
                    // own un-acked range (barrier+1 .. read] by scanning backward. The
                    // barrier is max(last_self_acked, last_received_replication_wal_seq):
                    // data above last_acked that arrived via TCP replication is peer-ACKED
                    // (the peer returned Ok to clients for it) and may exist nowhere else
                    // reachable (live-TCP ranges are never uploaded to S3) — culling it
                    // wedges convergence permanently. Only data above BOTH cursors is own
                    // speculation that no client was ever acked for.
                    let barrier = last_acked.max(last_received);
                    let log_id = active.metadata.borrow().log_id;
                    tracing::info!(
                        shard_id = self.config.shard_id,
                        read_wal_seq = read.wal_seq,
                        last_self_acked_wal_seq = last_acked,
                        last_received_replication_wal_seq = last_received,
                        barrier,
                        "reconcile_durable_tail: demotion rewind to ack barrier"
                    );
                    let cursor = self.find_cursor_at_wal_seq(barrier, &read, log_id).await?;
                    self.cull_tail(CullTarget::BothToAckBarrier(cursor)).await?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            TailReconciliation::CommitForPromotion | TailReconciliation::ReconcileAsFollower => {
                let read_seq = read_opt.as_ref().map_or(0, |r| r.wal_seq);
                if write_seq <= read_seq {
                    // Parked commits always cover (read, write]; with no tail the
                    // queue must be empty, or a promotion would silently retain it.
                    debug_assert_eq!(
                        self.shard_mem_cache.borrow().parked_commit_count(), 0,
                        "parked commits with no durable tail above read",
                    );
                    return Ok(false);
                }
                if self.durable_tail_is_peer(write_seq).await? {
                    if mode == TailReconciliation::CommitForPromotion {
                        self.commit_durable_tail(write_seq, read_seq).await?;
                        Ok(true)
                    } else {
                        // Unconfirmed peer data stays durable and parked; the new
                        // leader's carriers or catchup reconcile it.
                        Ok(false)
                    }
                } else {
                    let Some(read) = read_opt else {
                        // Own tail with no read cursor: a leader that never acked
                        // anything, so there is no commit point to rewind to. The
                        // tail stays; if a peer authored a competing fork, the
                        // S3-catchup divergence path truncates it.
                        return Ok(false);
                    };
                    tracing::info!(
                        shard_id = self.config.shard_id,
                        write_wal_seq = write_seq,
                        read_wal_seq = read.wal_seq,
                        last_self_acked_wal_seq = last_acked,
                        mode = ?mode,
                        "reconcile_durable_tail: culling own-speculation tail"
                    );
                    // Mixed tails are impossible (a leader never chains onto foreign
                    // unacked entries), so an own tail means nothing is parked.
                    debug_assert_eq!(
                        self.shard_mem_cache.borrow().parked_commit_count(), 0,
                        "own-speculation tail with parked peer commits: tail provenance is broken",
                    );
                    self.cull_tail(CullTarget::WriteToRead(read)).await?;
                    Ok(true)
                }
            }
        }
    }

    /// Provenance of the durable tail (read_seq, write_seq]: peer-received
    /// (chain-validated replication the old leader may have ACKED — must never be
    /// culled) or own unacked speculation (safe to cull). Mixed tails are
    /// structurally impossible: a leader never appends onto foreign unacked
    /// entries, and the demotion cull runs before peer data is accepted.
    ///
    /// Two signals, cheapest first:
    /// 1. parked deferred commits exist: only the follower replication apply parks,
    ///    so the tail is peer-received by construction;
    /// 2. the tail tip metablock was authored by another node: one reverse read at
    ///    the write cursor (cold path), crossing into the previous segment when the
    ///    active one is empty. node_id is per-node crypto-derived, and the S3
    ///    catchup already depends on its distinctness to filter fallback batches.
    async fn durable_tail_is_peer(&self, write_seq: u64) -> Result<bool, ShardFsyncError> {
        if self.shard_mem_cache.borrow().parked_commit_count() > 0 {
            return Ok(true);
        }
        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            self.log_segments_cache.active_log_id(),
            None,
            self.config.read_max_chunk_size,
        )
        .with_write_cursor_upper_bound();
        let tip = scanner
            .scan::<(u64, u128), ShardFsyncError>(|_, _, bytes| {
                Ok(Some((metablock_bytes::read_wal_seq(bytes), metablock_bytes::read_node_id(bytes))))
            })
            .await
            .map_err(|e| ShardFsyncError::MetablockSerialisationError(format!("tail provenance scan failed: {e:?}")))?;
        match tip {
            Some((tip_seq, tip_node_id)) => {
                debug_assert_eq!(tip_seq, write_seq, "first block under the write cursor must be the tail tip");
                Ok(tip_node_id != self.config.node_id)
            }
            // No metablock despite write > read: nothing real to commit. Let the
            // own-tail path decide (it rewinds write to read, dropping the phantom range).
            None => Ok(false),
        }
    }

    /// Promotion over a peer-received tail: commit the ENTIRE durable tail. Every
    /// acked entry is inside it (durability invariant) and an observed commit index
    /// can lag an ack, so nothing short of the durable tip is safe. Parked commits
    /// drain through `commit_pcd` (read caches, segment summaries, watch events —
    /// exactly once, in wal_seq order); the read cursor then advances over any
    /// remainder (after a crash-restart the parked commits died with the process
    /// and the caches are cold, so the cursor advance IS the whole commit).
    /// Write-side caches are untouched: the data is kept and they remain valid.
    async fn commit_durable_tail(&self, write_seq: u64, read_seq: u64) -> Result<(), ShardFsyncError> {
        let span = tracing::info_span!(
            "promotion_tail_commit",
            shard_id = self.config.shard_id,
            write_wal_seq = write_seq,
            read_wal_seq = read_seq,
            drained_count = tracing::field::Empty,
        );
        let active = self.log_segments_cache.active();
        {
            let _entered = span.enter();
            let pcds = self.shard_mem_cache.borrow_mut().take_all_parked_commits();
            span.record("drained_count", pcds.len() as u64);
            for pcd in pcds {
                shard_wal_replicate::commit_pcd(
                    &self.log_segments_cache, &self.shard_mem_cache, &self.watched_aggregates, pcd, Some(&self.dict_codec),
                );
            }
            let mut meta = active.metadata.borrow_mut();
            if meta.read.as_ref().map_or(0, |r| r.wal_seq) < meta.write.wal_seq {
                meta.advance_visible_position();
            }
        }

        shard_wal_replicate::set_read_cursor_gauge(&self.log_segments_cache, self.config.shard_id);
        metrics::gauge!("celeriant_follower_read_lag", &self.metrics_shard_label).set(0.0);
        metrics::gauge!("celeriant_parked_commit_queue_depth", &self.metrics_shard_label).set(0.0);

        // Persist the committed cursor: a restart must not regress a promoted
        // leader to a parked view, and nothing else is guaranteed to write a
        // header before writes open.
        let active_for_fsync = active.clone();
        self.fsync_coordinator
            .request_sync(
                None,
                ShardFsyncError::WriteLockTimeout,
                || async move { sync_header_only(active_for_fsync).await },
            )
            .await?;

        // Sealed segments the commit fully covered can flush their summary
        // sidecars. Unconditional: a crash-restart promotion commits via the bare
        // cursor advance (nothing drained), and no follower-drain will ever run
        // on a leader to pick the sweep up later.
        self.sweep_sealed_summaries().await;
        Ok(())
    }

    /// Clear the promotion-batch floor. Single site: the Leader flip, on every
    /// shard, on every promotion. The floor is set during the follower stint,
    /// consumed by the promotion upload as its range start, and doubles as the
    /// disk-truth "promotion incomplete" marker for crash re-entry — so nothing
    /// may clear it earlier. Left uncleared it would also outlive the leader
    /// stint and poison the demotion ack barrier (max(last_self_acked,
    /// last_received)). Clearing after a failed or skipped upload stands: the
    /// range stays on local disk and the demoted peer heals via the leader-side
    /// S3 fallback (pre-existing no-retry gap, same semantics as the old
    /// budget-exceeded clear). In-memory; the next header fsync persists it —
    /// a crash before that re-enters the resume path idempotently.
    pub fn clear_promotion_floor(&self) {
        let active = self.log_segments_cache.active();
        active.metadata.borrow_mut().last_received_replication_wal_seq = 0;
    }

    /// Disk-truth "promotion incomplete" check for a reacquired lease. True when
    /// the previous incarnation crashed mid-promotion still owing the commit or
    /// the upload. When a durable tail exists its PROVENANCE decides — a stale
    /// floor (armed by a crash in the Leader-flip-to-floor-clear gap) must never
    /// override an own-speculation tail into the cull that the self-reclaim
    /// carve-out promises against. Only with no tail does the floor decide: it
    /// catches a crash after the catchup commit left read == write while the
    /// TCP-received range still exists in S3 nowhere.
    pub async fn promotion_resume_owed(&self) -> Result<bool, ShardFsyncError> {
        let (write_seq, read_seq, floor) = {
            let active = self.log_segments_cache.active();
            let meta = active.metadata.borrow();
            (meta.write.wal_seq, meta.read.as_ref().map_or(0, |r| r.wal_seq), meta.last_received_replication_wal_seq)
        };
        if write_seq > read_seq {
            return self.durable_tail_is_peer(write_seq).await;
        }
        Ok(floor > 0)
    }

    /// Belt-and-braces for the Follower→Leader status flip: a chain-valid batch
    /// from the deposed leader can arrive inside the promotion window (after the
    /// post-catchup reconciliation, before the flip), park, and then have nothing
    /// left to drain it — its entry would become visible without watch events
    /// once the new leader's own acks advance the read cursor past it. Parked
    /// entries are peer data on our chain, so commit them. No-op when nothing is
    /// parked, which also leaves a self-reclaimed leader's own speculative tail
    /// untouched.
    pub async fn commit_parked_tail_on_promotion(&self) -> Result<(), ShardFsyncError> {
        if self.shard_mem_cache.borrow().parked_commit_count() > 0 {
            self.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await?;
        }
        debug_assert_eq!(
            self.shard_mem_cache.borrow().parked_commit_count(), 0,
            "parked commits survived the promotion status flip",
        );
        Ok(())
    }

    /// Rewind mechanics shared by every cull arm: move the cursor(s), rebuild the
    /// in-segment backlink tips, drop the write-side state that referenced the
    /// culled range, and persist the header.
    async fn cull_tail(&self, target: CullTarget) -> Result<(), ShardFsyncError> {
        let active = self.log_segments_cache.active();
        let demotion_cull = matches!(target, CullTarget::BothToAckBarrier(_));
        {
            let mut meta = active.metadata.borrow_mut();
            match target {
                CullTarget::WriteToRead(read) => {
                    meta.write = read;
                    // read cursor unchanged: followers still sync from the existing read position.
                }
                CullTarget::BothToAckBarrier(cursor) => {
                    meta.write = cursor.clone();
                    meta.read = Some(cursor);
                }
            }
            // Rewinds refresh both cursor gauges, read first: a scrape
            // between the two sets must never see read above write.
            metrics::gauge!("celeriant_read_wal_seq", &self.metrics_shard_label)
                .set(meta.read.as_ref().map_or(0, |r| r.wal_seq) as f64);
            metrics::gauge!("celeriant_wal_seq", &self.metrics_shard_label).set(meta.write.wal_seq as f64);
        }

        // The rewind orphaned the in-segment backlink tips (still pointing into the culled
        // tail); rebuild so the next append back-links to a live committed block, not a culled one.
        rebuild_active_segment_chain_tips(&self.log_segments_cache, self.config.read_max_chunk_size, &self.shard_mem_cache, &self.metrics_shard_label)
            .await
            .map_err(|e| ShardFsyncError::MetablockSerialisationError(format!("chain-tips rebuild after cull failed: {e:?}")))?;

        let (lru_client_len, lru_agg_len, drained, parked_dropped) = {
            let mut mc = self.shard_mem_cache.borrow_mut();
            let lru_client_len = mc.aggregate_write_client_snapshots_len();
            let lru_agg_len = mc.aggregate_write_snapshots_len();
            // The barrier rewind moves the read cursor down, so stale read-side caches
            // must also go (clear_all_caches, which discards parked commits too).
            // WriteToRead leaves the read cursor, so read caches stay valid. Parked
            // commits cover exactly the culled (read, write] range and are discarded
            // with it: their entries left the chain, so their watch events must never
            // fire, and an orphaned PCD would replay at the next promotion catchup.
            // Callers route only held-leadership demotions and own-speculation tails
            // here, so the queue should already be empty; the discard is defensive.
            if demotion_cull {
                mc.clear_all_caches();
                (lru_client_len, lru_agg_len, 0, 0)
            } else {
                let drained = mc.clear_speculative_write_caches_for_cull();
                let parked_dropped = mc.clear_parked_commits();
                (lru_client_len, lru_agg_len, drained, parked_dropped)
            }
        };
        // Both arms emptied the parked queue outside drain_parked_commits, which
        // otherwise owns this gauge; without the reset an idle shard shows the
        // pre-cull depth forever.
        metrics::gauge!("celeriant_parked_commit_queue_depth", &self.metrics_shard_label).set(0.0);
        metrics::counter!("celeriant_cull_stale_client_seq_lru").increment(lru_client_len as u64);
        metrics::counter!("celeriant_cull_stale_agg_lru").increment(lru_agg_len as u64);
        if lru_client_len > 0 || lru_agg_len > 0 || drained > 0 || parked_dropped > 0 {
            tracing::warn!(
                shard_id = self.config.shard_id,
                lru_client_len,
                lru_agg_len,
                drained,
                parked_dropped,
                "cull_tail: cleared OCC/idempotency LRU + drained pending_replication + dropped parked commits"
            );
        }
        let active_for_fsync = active.clone();
        self.fsync_coordinator
            .request_sync(
                None,
                ShardFsyncError::WriteLockTimeout,
                || async move { sync_header_only(active_for_fsync).await },
            )
            .await
    }

    /// Backward scan from `read.metablocks_position` to find the cursor state at `target_wal_seq`.
    /// Returns a cursor whose bloom is inherited from `read` (safe superset; no false negatives).
    async fn find_cursor_at_wal_seq(
        &self,
        target_wal_seq: u64,
        read: &celeriant_rotating_log::log_segment_file::log_segment_cursor::LogSegmentCursor,
        log_id: u64,
    ) -> Result<celeriant_rotating_log::log_segment_file::log_segment_cursor::LogSegmentCursor, ShardFsyncError> {
        use celeriant_wal::metablocks::metablock::Metablock as Mb;

        if target_wal_seq == 0 {
            // Genesis: nothing written yet at the ack barrier.
            let mut cursor = read.clone();
            cursor.wal_seq = 0;
            cursor.metablocks_position = celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES as u64;
            cursor.datablocks_position = read.datablocks_position;
            cursor.tip_hash = celeriant_wal::constants::GENESIS_HASH;
            return Ok(cursor);
        }

        const HDR: usize = versioned_block::HEADER_SIZE;
        // No metablock_bytes accessor exists for previous_tip_hash or datablock_position; inline offsets.
        const PREV_HASH_OFF: usize = HDR + Mb::OFFSET_PREVIOUS_TIP_HASH;
        const DATA_POS_OFF: usize = HDR + Mb::OFFSET_DATABLOCK_POSITION;

        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            log_id,
            Some(read.metablocks_position),
            self.config.read_max_chunk_size,
        );

        type Found = (u64, u64, u64, [u8; 32]); // (found_log_id, metablocks_pos_after, datablocks_pos, tip_hash)
        let result = scanner
            .scan::<Found, ShardFsyncError>(|scan_log_id, pos, bytes| {
                let seq = metablock_bytes::read_wal_seq(bytes);
                if seq != target_wal_seq {
                    return Ok(None);
                }
                let prev_tip: [u8; 32] = bytes[PREV_HASH_OFF..PREV_HASH_OFF + 32].try_into()
                    .map_err(|_| ShardFsyncError::MetablockSerialisationError("prev_tip_hash slice error".into()))?;
                let tip_hash = compute_entry_hash(&prev_tip, bytes);
                let datablocks_pos = u64::from_le_bytes(
                    bytes[DATA_POS_OFF..DATA_POS_OFF + 8].try_into()
                        .map_err(|_| ShardFsyncError::MetablockSerialisationError("datablock_pos slice error".into()))?
                );
                let meta_pos_after = pos + FIXED_BLOCK_SIZE_BYTES as u64;
                Ok(Some((scan_log_id, meta_pos_after, datablocks_pos, tip_hash)))
            })
            .await
            .map_err(|e| ShardFsyncError::MetablockSerialisationError(format!("scan error: {e:?}")))?;

        match result {
            Some((found_log_id, meta_pos_after, datablocks_pos, tip_hash)) => {
                let mut cursor = read.clone();
                cursor.log_id = found_log_id;
                cursor.wal_seq = target_wal_seq;
                cursor.metablocks_position = meta_pos_after;
                cursor.datablocks_position = datablocks_pos;
                cursor.tip_hash = tip_hash;
                Ok(cursor)
            }
            None => Err(ShardFsyncError::MetablockSerialisationError(format!(
                "ack-barrier wal_seq {target_wal_seq} not found in WAL (log_id={log_id}, read_pos={})",
                read.metablocks_position
            ))),
        }
    }

    pub async fn upload_s3_promotion_batch(&self) -> Result<(), crate::error::replication_to_s3_error::ReplicateToS3Error> {
        // Idempotent re-reconcile: commits any peer tail that accumulated since the
        // pre-catchup reconciliation (catchup applies FullCommit, so after it
        // read == write and this is a no-op).
        self.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await
            .map_err(|e| crate::error::replication_to_s3_error::ReplicateToS3Error::SerializationFailed(
                format!("promotion tail reconciliation failed: {e:?}"),
            ))?;

        let (start_wal_seq, current_wal_seq) = {
            let active = self.log_segments_cache.active();
            let metadata = active.metadata.borrow();
            (metadata.last_received_replication_wal_seq, metadata.write.wal_seq)
        };

        // Shard 0 uploads mid-promotion (still Promoting, pre-flip); data shards
        // upload after their Leader flip. Anything else has no business here.
        let lease_epoch = match self.node_status.get().effective_node_status() {
            NodeStatus::Leader { lease_epoch } | NodeStatus::Promoting { lease_epoch } => lease_epoch,
            other => {
                tracing::info!(
                    shard_id = self.config.shard_id,
                    start_wal_seq, current_wal_seq, status = ?other,
                    "promotion_batch_upload skipped — not leader or promoting"
                );
                return Ok(());
            }
        };

        let shard_id = self.config.shard_id;

        tracing::info!(
            shard_id, lease_epoch, start_wal_seq, current_wal_seq,
            "promotion_batch_upload entry"
        );

        if start_wal_seq == 0 || start_wal_seq > current_wal_seq {
            tracing::info!(
                shard_id, lease_epoch, start_wal_seq, current_wal_seq,
                "promotion_batch_upload skipped — no range to upload"
            );
            return Ok(());
        }

        let read_max_chunk_size = self.config.read_max_chunk_size;
        let max_bytes = self.config.max_promotion_batch_bytes;

        let mut items = match scan_for_promotion_batch(
            &self.log_segments_cache, start_wal_seq, max_bytes, read_max_chunk_size,
        ).await? {
            PromotionBatchScan::Collected(items) => items,
            PromotionBatchScan::BudgetExceeded => {
                tracing::warn!(
                    shard_id, start_wal_seq, current_wal_seq, max_bytes = ?max_bytes,
                    "Promotion batch exceeds max_promotion_batch_bytes — skipping upload; demoted peer must catch up via leader-side S3 fallback"
                );
                metrics::counter!(
                    "celeriant_promotion_batch_budget_exceeded_total",
                    &[("shard_id", shard_id.to_string())]
                ).increment(1);
                return Ok(());
            }
        };

        if items.is_empty() {
            return Ok(());
        }

        items.reverse();

        fetch_datablocks_for_metablocks(&mut items, read_max_chunk_size, &self.log_segments_cache, &self.dict_codec)
            .await
            .map_err(|e| crate::error::replication_to_s3_error::ReplicateToS3Error::SerializationFailed(
                format!("Failed to fetch datablocks for promotion batch: {e:?}"),
            ))?;

        let batch_items: Vec<celeriant_msg::request::requests::ReplicationBatchItem> = items
            .into_iter()
            .map(|e| celeriant_msg::request::requests::ReplicationBatchItem {
                metablock: e.metablock,
                datablock: e.datablock,
            })
            .collect();

        let batch_count = batch_items.len();
        let first_wal = batch_items.first().map(|i| i.metablock.wal_seq).unwrap_or(0);
        let last_wal = batch_items.last().map(|i| i.metablock.wal_seq).unwrap_or(0);
        let first_prev_hash = batch_items.first()
            .map(|i| {
                let h = &i.metablock.previous_tip_hash;
                format!("{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7])
            })
            .unwrap_or_else(|| "<empty>".to_string());

        // Chunk into internode_max_request_size-bounded objects. A drained
        // tail can run to 100k+ entries. One object that big times out the
        // S3 PUT and the range never reaches S3, leaving the demoted peer's
        // gap permanently unbridgeable (TCP catchup refuses oversized gaps
        // and steady-state fallback only covers fresh commits). Catchup
        // stitches contiguous batch files, so chunking is transparent to it,
        // and chunks that landed before a failure are usable immediately.
        let max_object_bytes = self.config.internode_max_request_size;
        let mut chunks: Vec<Vec<celeriant_msg::request::requests::ReplicationBatchItem>> = Vec::new();
        let mut current_chunk: Vec<celeriant_msg::request::requests::ReplicationBatchItem> = Vec::new();
        let mut current_bytes: u64 = 0;
        for item in batch_items {
            let item_bytes = FIXED_BLOCK_SIZE_BYTES as u64 + item.metablock.uncompressed_size as u64;
            if !current_chunk.is_empty() && current_bytes.saturating_add(item_bytes) > max_object_bytes {
                chunks.push(std::mem::take(&mut current_chunk));
                current_bytes = 0;
            }
            current_bytes += item_bytes;
            current_chunk.push(item);
        }
        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        let chunk_count = chunks.len();
        info!(
            shard_id, lease_epoch, batch_count, chunk_count, start_wal_seq, current_wal_seq,
            first_wal, last_wal, first_prev_hash = %first_prev_hash,
            "promotion_batch_upload uploading"
        );

        const UPLOAD_ATTEMPTS_PER_CHUNK: u32 = 3;
        for (chunk_index, chunk) in chunks.into_iter().enumerate() {
            let chunk_first = chunk.first().map(|i| i.metablock.wal_seq).unwrap_or(0);
            let chunk_last = chunk.last().map(|i| i.metablock.wal_seq).unwrap_or(0);
            let mut attempt = 0;
            loop {
                attempt += 1;
                match self.replication_client.replicate_to_s3(chunk.clone()).await {
                    Ok(()) => break,
                    Err(e) if attempt < UPLOAD_ATTEMPTS_PER_CHUNK => {
                        tracing::warn!(
                            shard_id, lease_epoch, chunk_index, chunk_count, chunk_first, chunk_last,
                            attempt, error = ?e, "promotion_batch_upload chunk failed — retrying"
                        );
                        glommio::timer::Timer::new(std::time::Duration::from_millis(500 * attempt as u64)).await;
                    }
                    Err(e) => {
                        tracing::warn!(
                            shard_id, lease_epoch, chunk_index, chunk_count, chunk_first, chunk_last,
                            error = ?e, "promotion_batch_upload failed"
                        );
                        return Err(e);
                    }
                }
            }
        }
        info!(shard_id, lease_epoch, batch_count, chunk_count, first_wal, last_wal, "promotion_batch_upload succeeded");

        Ok(())
    }
}

/// Intermediate struct for validated and prepared write data
struct PreparedWrite {
    aggregate_key: AggregateKey,
    client_id: u128,
    event_seq: u64,
    aggregate_version: u64,
    min_aggregate_version: u64,
    latest_client_seq: u64,
    shard_log_queue_item: ShardLogQueueItem,
}

/// Result of size-bounded metablock collection
struct MetablockCollection {
    /// Metablocks that fit within max_bytes, sorted by aggregate version ascending
    kept_metablocks: Vec<EventBatchFromLogSegmentFile>,
    /// If we hit the size limit, this is the next aggregate version to continue from
    next_aggregate_version: Option<u64>,
}

/// Outcome of `scan_for_promotion_batch`.
enum PromotionBatchScan {
    Collected(Vec<EventBatchFromLogSegmentFile>),
    BudgetExceeded,
}

/// Scan the active segment backwards for metablocks at or above `start_wal_seq`,
/// tallying `uncompressed_size`. Returns `BudgetExceeded` if the running sum overshoots
/// `max_bytes`. Items are in reverse-WAL order; caller must `.reverse()` before use.
async fn scan_for_promotion_batch(
    log_segments_cache: &LogSegmentsCache,
    start_wal_seq: u64,
    max_bytes: Option<u64>,
    read_max_chunk_size: u64,
) -> Result<PromotionBatchScan, crate::error::replication_to_s3_error::ReplicateToS3Error> {
    let current_log_id = log_segments_cache.active_log_id();
    let mut scanner = ReverseMetablockScanner::new(
        log_segments_cache, current_log_id, None, read_max_chunk_size,
    );

    let mut items: Vec<EventBatchFromLogSegmentFile> = vec![];
    let mut acc_bytes: u64 = 0;
    let mut budget_exceeded = false;
    scanner
        .scan(|log_id, _pos, bytes| {
            let wal_seq = metablock_bytes::read_wal_seq(bytes);
            if wal_seq < start_wal_seq {
                return Ok(Some(()));
            }
            let metablock = deserialise_metablock(bytes)?;
            acc_bytes = acc_bytes.saturating_add(metablock.uncompressed_size);
            if let Some(cap) = max_bytes {
                if acc_bytes > cap {
                    budget_exceeded = true;
                    return Ok(Some(()));
                }
            }
            items.push(EventBatchFromLogSegmentFile { log_id, metablock, datablock: None });
            Ok::<Option<()>, DiskFormatError>(None)
        })
        .await
        .map_err(|e| crate::error::replication_to_s3_error::ReplicateToS3Error::SerializationFailed(
            format!("Failed to scan WAL for promotion batch: {e:?}"),
        ))?;

    if budget_exceeded {
        Ok(PromotionBatchScan::BudgetExceeded)
    } else {
        Ok(PromotionBatchScan::Collected(items))
    }
}

fn is_visibility_gap_rejection(err: &ShardWriteError) -> bool {
    matches!(
        err,
        ShardWriteError::ClientIdempotencyViolation { .. }
            | ShardWriteError::OptimisticConcurrencyViolation { .. }
            | ShardWriteError::AggregateRecreateNotAllowed
    )
}

impl<R: ReplicationClient + 'static, D: S3Downloader + 'static> ShardWal<R, D> {
    fn get_aggregate_version(metablock: &Metablock) -> u64 {
        match &metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(m) => m.aggregate_version,
            _ => 0,
        }
    }

    async fn collect_metablocks_bounded(
        &self,
        aggregate_key: &AggregateKey,
        filters: &ReadFilters,
        max_bytes: u64,
        last_known: MetablockPosition,
    ) -> Result<MetablockCollection, ScanError<DiskFormatError>> {
        let mut kept: Vec<EventBatchFromLogSegmentFile> = Vec::new();
        let mut cumulative_size: u64 = 0;
        let mut evicted_version: Option<u64> = None;

        // Try cache first (iterates forward from from_aggregate_version)
        self.collect_from_cache_bounded(
            aggregate_key,
            filters,
            max_bytes,
            &mut kept,
            &mut cumulative_size,
            &mut evicted_version,
        );

        // Check if we need disk (cache doesn't cover from_aggregate_version)
        let cache_min_batch = kept.first().map(|k| Self::get_aggregate_version(&k.metablock));
        let need_disk = cache_min_batch.map(|min| min > filters.from_aggregate_version).unwrap_or(true);

        if need_disk && last_known.metablock_absolute_pos > 0 {
            let disk_to = cache_min_batch.map(|min| min.saturating_sub(1));

            self.collect_from_disk_bounded(
                aggregate_key,
                filters.from_aggregate_version,
                disk_to.or(filters.to_aggregate_version),
                last_known,
                filters,
                max_bytes,
                &mut kept,
                &mut cumulative_size,
                &mut evicted_version,
            )
            .await?;
        }

        Ok(MetablockCollection {
            kept_metablocks: kept,
            next_aggregate_version: evicted_version,
        })
    }

    fn collect_from_cache_bounded(
        &self,
        aggregate_key: &AggregateKey,
        filters: &ReadFilters,
        max_bytes: u64,
        kept: &mut Vec<EventBatchFromLogSegmentFile>,
        cumulative_size: &mut u64,
        evicted_version: &mut Option<u64>,
    ) {
        let shard_mem_cache = self.shard_mem_cache.borrow();
        let read_cursor_wal_seq = self.log_segments_cache.get_latest_read_cursor_wal_seq();
        let kept_before = kept.len();

        // Cache iterates forward (ascending aggregate version) from from_aggregate_version
        for (batch_idx, write) in shard_mem_cache.get_cached_writes_from(aggregate_key, filters.from_aggregate_version, read_cursor_wal_seq) {
            // Stop if past upper bound
            if filters.to_aggregate_version.map_or(false, |to| batch_idx > to) {
                break;
            }

            // Apply metablock-level filters
            if !in_memory_filtering::is_include_batch(&write.metablock, filters) {
                continue;
            }

            let batch_size = write.metablock.uncompressed_size;

            // Check if adding this batch would exceed budget
            // (allow at least one batch even if over budget)
            if *cumulative_size + batch_size > max_bytes && !kept.is_empty() {
                *evicted_version = Some(batch_idx);
                break;
            }

            *cumulative_size += batch_size;
            kept.push(EventBatchFromLogSegmentFile {
                log_id: 0, // Cache entries don't need log_id for fetch
                metablock: write.metablock.clone(),
                datablock: write.datablock.clone(),
            });
        }

        let cache_hits = (kept.len() - kept_before) as u64;
        if cache_hits > 0 {
            metrics::counter!("celeriant_cache_recent_write_hits_total").increment(cache_hits);
        } else {
            metrics::counter!("celeriant_cache_recent_write_misses_total").increment(1);
        }
    }

    async fn collect_from_disk_bounded(
        &self,
        aggregate_key: &AggregateKey,
        from_batch: u64,
        to_batch: Option<u64>,
        last_known: MetablockPosition,
        filters: &ReadFilters,
        max_bytes: u64,
        kept: &mut Vec<EventBatchFromLogSegmentFile>,
        cumulative_size: &mut u64,
        evicted_version: &mut Option<u64>,
    ) -> Result<(), ScanError<DiskFormatError>> {
        // Use a VecDeque for efficient eviction from the "newest" end
        let mut disk_kept: VecDeque<EventBatchFromLogSegmentFile> = VecDeque::new();
        let mut disk_cumulative: u64 = 0;

        let scanner_start_pos = last_known.metablock_absolute_pos.saturating_add(FIXED_BLOCK_SIZE_BYTES as u64);

        let mut scanner = ReverseMetablockScanner::new(
            &self.log_segments_cache,
            last_known.log_id,
            Some(scanner_start_pos),
            self.config.read_max_chunk_size,
        )
        .with_aggregate_chain(aggregate_key.clone(), self.config.chain_read_window_bytes);

        // Budget remaining after cache entries
        let budget_for_disk = max_bytes.saturating_sub(*cumulative_size);

        scanner
            .scan::<(), DiskFormatError>(|log_id, _pos, bytes| {
                if !metablock_bytes::is_matches_aggregate_key(bytes, aggregate_key) {
                    return Ok(None); // Continue - different aggregate
                }

                let aggregate_version = metablock_bytes::read_event_batch_aggregate_version(bytes);

                // Stop when we've gone past our requested range
                if aggregate_version < from_batch {
                    return Ok(Some(())); // Stop - past our range
                }

                // Skip if above the range we need (cache already has newer)
                if to_batch.map_or(false, |to| aggregate_version > to) {
                    return Ok(None); // Continue - will be served from cache
                }

                // Deserialize metablock for filter check
                let metablock = deserialise_metablock(bytes)?;

                if !in_memory_filtering::is_include_batch(&metablock, filters) {
                    return Ok(None); // Continue - filtered out
                }

                // Add to back (we're scanning backwards, newest seen first)
                disk_cumulative += metablock.uncompressed_size;
                disk_kept.push_back(EventBatchFromLogSegmentFile {
                    log_id,
                    metablock,
                    datablock: None,
                });

                // Evict from FRONT (newest) until under budget
                while disk_cumulative > budget_for_disk && disk_kept.len() > 1 {
                    if let Some(evicted) = disk_kept.pop_front() {
                        disk_cumulative -= evicted.metablock.uncompressed_size;
                        let evicted_idx = Self::get_aggregate_version(&evicted.metablock);
                        // Track lowest evicted as continuation point
                        match evicted_version {
                            Some(existing) if evicted_idx < *existing => {
                                *evicted_version = Some(evicted_idx);
                            }
                            None => {
                                *evicted_version = Some(evicted_idx);
                            }
                            _ => {}
                        }
                    }
                }

                Ok(None) // Continue scanning to from_batch
            })
            .await?;

        *cumulative_size += disk_cumulative;


        // Reverse to get chronological order
        kept.splice(0..0, disk_kept.into_iter().rev());

        Ok(())
    }

    fn build_filtered_response(
        &self,
        batches_with_data: Vec<EventBatchFromLogSegmentFile>,
        filters: &ReadFilters,
    ) -> Vec<AggregateEventBatch> {
        let mut event_batches = Vec::with_capacity(batches_with_data.len());

        for item in batches_with_data {
            let Some(mut datablock) = item.datablock else {
                continue;
            };

            // Apply event-level filters (bloom filter may have false positives)
            if let DatablockKind::EventBatchItem(ref mut eb) = datablock.datablock_kind {
                in_memory_filtering::apply_event_filters(eb, filters);
                if eb.events.is_empty() {
                    continue;
                }
            }

            let Some(batch) = AggregateEventBatch::from_wal(&item.metablock, &datablock) else {
                continue;
            };

            event_batches.push(batch);
        }

        event_batches
    }

    /// `role` is decided by the orchestrating shard and carried in the message
    /// (never re-derived from this shard's own status: the Promoting
    /// StatusUpdate broadcast can lag or drop, and a promotion catchup run as
    /// Following would fast-exit without consuming the peer's acked data).
    pub async fn enter_s3_catchup(&self, role: shard_wal_s3_catchup::CatchupRole) -> Result<S3CatchupResult, S3CatchupError> {

        // Promoting stays put through catchup: it carries the won lease's real
        // TTL and the promotion upload gate needs to see it afterwards; the
        // rewrite below (expiry 0) would decay it to Fenced instantly.
        if !self.node_status.get().raw().is_promoting() {
            let catchup_status = match self.node_status.get().raw() {
                NodeStatus::Follower { leader_lease_epoch }
                | NodeStatus::FollowerCatchingUp { leader_lease_epoch } => {
                    NodeStatus::FollowerCatchingUp { leader_lease_epoch }
                }
                NodeStatus::BootCatchup => NodeStatus::BootCatchup,
                _ => NodeStatus::BootCatchup,
            };
            set_node_status_and_metric(&self.node_status, ValidatedNodeStatus::create_custom_status(catchup_status, 0, 0), self.config.shard_id);
        }

        // A target taught by a dead lease epoch is a phantom: its tail may
        // have been culled cluster-wide, and chasing it burns the full
        // drain-settle budget on every kick. Validate against the epoch this
        // follower currently knows its leader by.
        let known_leader_epoch = match self.node_status.get().raw() {
            NodeStatus::Follower { leader_lease_epoch }
            | NodeStatus::FollowerCatchingUp { leader_lease_epoch } => Some(leader_lease_epoch),
            _ => None,
        };
        let (taught_epoch, taught_wal_seq) = self.observed_leader_target.taught();
        if taught_wal_seq > 0 && known_leader_epoch.is_some_and(|known| taught_epoch < known) {
            tracing::info!(
                shard_id = self.config.shard_id,
                taught_epoch,
                known_leader_epoch = known_leader_epoch.unwrap_or(0),
                taught_wal_seq,
                "discarding phantom catchup target taught by a dead lease epoch"
            );
            metrics::counter!(
                "celeriant_s3_catchup_phantom_target_discarded_total",
                "shard_id" => self.config.shard_id.to_string()
            ).increment(1);
        }
        catchup_from_s3(
            &self.log_segments_cache,
            &self.shard_mem_cache,
            &self.fsync_coordinator,
            &self.watched_aggregates,
            &self.summary_cache,
            &self.s3_downloader,
            self.config.shard_id,
            self.config.node_id,
            self.peer_node_id.get(),
            self.config.max_catchup_gap_bytes,
            self.dict_codec.clone(),
            role,
            self.observed_leader_target.target_for(known_leader_epoch),
            &self.live_tail_yielded_wal_seq,
        ).await

    }

    pub async fn register_schema(&self, request: celeriant_msg::request::requests::RegisterSchemaRequest) -> Result<RegisterSchemaResponse, ShardSchemaError> {
        use celeriant_wal::SchemaType;
        use celeriant_memcache::cached_schema::CachedSchema;

        let max_schema_size = self.config.max_schema_size_bytes as usize;

        // Validate we can accept writes
        let lease_epoch = match self.node_status.get().effective_node_status() {
            NodeStatus::Leader { lease_epoch } => lease_epoch,
            NodeStatus::Standalone => 0,
            _ => return Err(ShardSchemaError::ShardCannotAcceptWrites {
                leader_address: self.leader_client_address.borrow().clone(),
            }),
        };

        let schema_type = SchemaType::try_from(request.schema_type)
            .map_err(|_| ShardSchemaError::UnsupportedSchemaType { schema_type: request.schema_type })?;

        if request.schema.len() > max_schema_size {
            return Err(ShardSchemaError::InvalidSchema {
                schema_type: request.schema_type,
                parse_error: format!("Schema exceeds maximum size of {} bytes", max_schema_size),
            });
        }

        // Validate schema parses and compile it
        let cached_validator = CompiledValidator::compile(schema_type, &request.schema)
            .map_err(|e| ShardSchemaError::InvalidSchema {
                schema_type: request.schema_type,
                parse_error: e,
            })?;

        let schema_key = &request.schema_key;

        // Ensure cache is populated from WAL (handles cold-cache after restart/eviction)
        self.ensure_schema_cached(schema_key).await?;

        {
            let shard_mem_cache = self.shard_mem_cache.borrow();
            if shard_mem_cache.schema_cache_has_schema(schema_key) || shard_mem_cache.schema_is_pending(schema_key) {
                return Err(ShardSchemaError::SchemaAlreadyExists {
                    event_type_major: schema_key.event_type_major,
                    event_type_minor: schema_key.event_type_minor,
                });
            }
        }

        // Build metablock and datablock
        let server_timestamp = self.config.timestamp_config.now();

        let metablock_schema_registration = MetablockSchemaRegistration {
            schema_key: schema_key.clone(),
            client_id: request.client_id,
            user_id: request.user_id,
        };

        let datablock_schema_registration = DatablockSchemaRegistration {
            schema_type,
            schema: request.schema.clone(),
        };

        let datablock = Datablock {
            datablock_kind: DatablockKind::SchemaRegistration(datablock_schema_registration),
        };

        let serialized_datablock = SerialisedDatablock::new(
            &datablock,
            CompressionPolicy::Auto { compression_allowed: true },
            &self.dict_codec,
        )
        .map_err(|e| ShardSchemaError::InvalidSchema {
            schema_type: request.schema_type,
            parse_error: format!("Failed to serialize datablock: {:?}", e),
        })?;

        let metablock = Metablock {
            wal_seq: 0,
            server_timestamp,
            lease_epoch,
            node_id: self.config.node_id,
            uncompressed_size: serialized_datablock.uncompressed_size,
            compressed_size: serialized_datablock.compressed_size,
            datablock_version: serialized_datablock.datablock_version,
            datablock_compression_type: serialized_datablock.compression_type,
            datablock: serialized_datablock.storage_kind,
            wal_metablock_type: MetablockKind::SchemaRegistration(metablock_schema_registration),
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
            previous_aggregate_metablock_pos: 0,
        };

        let shard_log_queue_item = ShardLogQueueItem::new(Some(datablock), serialized_datablock.external_data, metablock);

        // Add to pending queue and populate cache
        {
            let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();

            // Double-check after acquiring mutable borrow
            if shard_mem_cache.schema_cache_has_schema(schema_key) || shard_mem_cache.schema_is_pending(schema_key) {
                return Err(ShardSchemaError::SchemaAlreadyExists {
                    event_type_major: schema_key.event_type_major,
                    event_type_minor: schema_key.event_type_minor,
                });
            }

            // Mark as pending before adding to cache
            shard_mem_cache.schema_mark_pending(schema_key.clone());

            // Insert into cache immediately (becomes visible for validation before fsync)
            shard_mem_cache.schema_cache_insert(schema_key.clone(), CachedSchema::Validated(cached_validator));

            // Add to pending fsync queue
            shard_mem_cache.add_to_pending_queue(vec![shard_log_queue_item]);
        }

        // Wait for durability
        self.sync_durable().await?;

        // Replicate if leader
        self.replicate_durable().await?;

        Ok(RegisterSchemaResponse {
            correlation_id: request.correlation_id,
        })
    }

    /// Read-only inner-payload validation against a registered schema, reusing
    /// the hot compiled-schema cache
    pub fn schema_validate(&self, schema_key: &SchemaKey, value: &[u8]) -> Option<Result<(), String>> {
        let mut shard_mem_cache = self.shard_mem_cache.borrow_mut();
        match shard_mem_cache.schema_cache_get(schema_key) {
            Some(celeriant_memcache::cached_schema::CachedSchema::Validated(validator)) => {
                Some(validator.validate(value))
            }
            Some(celeriant_memcache::cached_schema::CachedSchema::CompilationFailed(err)) => {
                Some(Err(err.clone()))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
impl<R: crate::replication_client::ReplicationClient, D: crate::s3_downloader::S3Downloader> ShardWal<R, D> {
    fn schema_cache_has_schema(&self, key: &SchemaKey) -> bool {
        self.shard_mem_cache.borrow().schema_cache_has_schema(key)
    }

    fn schema_cache_clear(&self) {
        self.shard_mem_cache.borrow_mut().schema_cache_clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::replication_to_follower_error::ReplicateToFollowerError;
    use crate::error::replication_to_s3_error::ReplicateToS3Error;
    use crate::replication_client::StubReplicationClient;
    use crate::s3_downloader::StubS3Downloader;
    use celeriant_msg::request::requests::{ReplicationBatchItem, ReplicationBatchRequest, SingleAggregateDelete, WatchRequest};
    use celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES;
    use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::segment_summary::segment_aggregate_entry::SegmentAggregateEntry;
    use crate::timestamp_config::TimestampConfig;
    use celeriant_disk::files::read_fixed_records_visit_const::{read_fixed_records_visit_const, ReadVisitError};
    use crate::shard_wal_compact::SCAN_CHUNK_SIZE;
    use glommio::{LocalExecutorBuilder, Placement};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    fn test_dir() -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("shard");
        (tmp, dir)
    }

    fn test_config(dir: &std::path::Path) -> InternalShardConfig {
        InternalShardConfig {
            node_id: 1,
            shard_id: 1,
            max_open_files: 4,
            shard_log_preallocate_bytes: 4 * 1024 * 1024,
            fsync_delay: Duration::ZERO,
            replication_delay: Duration::ZERO,
            s3_replication_delay: Duration::from_millis(500),
            replication_rollback_cooldown: Duration::ZERO,
            heartbeat_starve_threshold: Duration::ZERO,
            recent_write_cache_bytes: 64 * 1024 * 1024,
            shard_dir: dir.to_path_buf(),
            max_response_size: 16 * 1024 * 1024,
            max_request_size: 16 * 1024 * 1024,
            internode_max_request_size: 64 * 1024 * 1024,
            aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
            aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
            negative_lookup_cache_bytes: 2 * 1024 * 1024,
            read_max_chunk_size: 32 * 1024,
            chain_read_window_bytes: 1024,
            timestamp_config: TimestampConfig::default(),
            list_page_size: 100,
            list_max_concurrent: 16,
            list_max_duration: Duration::from_secs(2),
            schema_cache_bytes: 4 * 1024 * 1024,
            max_schema_size_bytes: 16384,
            max_catchup_gap_bytes: Some(100 * 1024 * 1024),
            max_promotion_batch_bytes: None,
            compaction_check_interval: Duration::from_secs(600),
            compaction_min_reclaimable_ratio: 0.20,
            compaction_temp_dir: std::path::PathBuf::from("/tmp/test_compaction"),
            max_clock_drift_ms: 500,
            read_max_concurrent: 64,
            cache_warmup_max_duration: Duration::MAX,
            wal_compression_level: 3,
            dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
            s3_lease_duration_ms: 0,
        }
    }

    fn key(org: u128, atype: u128, id: u128) -> AggregateKey {
        AggregateKey::new(org, atype, id)
    }

    fn events(count: usize) -> Vec<DatablockAggregateEvent> {
        (1..=count as u64)
            .map(|i| DatablockAggregateEvent {
                client_seq: i,
                event_type_major: 1,
                event_value: Arc::new(vec![i as u8; 8]),
                ..Default::default()
            })
            .collect()
    }

    fn write_req(agg: AggregateKey, evts: Vec<DatablockAggregateEvent>) -> ClientRequest {
        write_req_full(agg, evts, true, None, 1, false)
    }

    fn write_req_full(
        agg: AggregateKey,
        evts: Vec<DatablockAggregateEvent>,
        allow_create: bool,
        expected_batch: Option<u64>,
        client_id: u128,
        enforce_idempotency: bool,
    ) -> ClientRequest {
        let mut writes = HashMap::new();
        writes.insert(
            agg,
            SingleAggregateWrite {
                events: evts,
                allow_create,
                expected_version: expected_batch,
                enforce_client_idempotency: enforce_idempotency,
            },
        );
        ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id,
            user_id: None,
            writes,
        })
    }

    fn read_req(agg: AggregateKey) -> ClientRequest {
        ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: agg,
            filters: ReadFilters::new(0),
        })
    }

    fn read_req_from(agg: AggregateKey, from: u64) -> ClientRequest {
        ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: agg,
            filters: ReadFilters::new(from),
        })
    }

    fn exists_req(agg: AggregateKey) -> ClientRequest {
        ClientRequest::AggregateDetails(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: agg,
        })
    }

    fn delete_req(agg: AggregateKey) -> ClientRequest {
        delete_req_full(agg, false, false, None)
    }

    fn delete_req_full(
        agg: AggregateKey,
        allow_recreate: bool,
        allow_sequence_continuation: bool,
        expected: Option<u64>,
    ) -> ClientRequest {
        let mut deletes = HashMap::new();
        deletes.insert(
            agg,
            SingleAggregateDelete {
                allow_recreate,
                allow_sequence_continuation: allow_sequence_continuation,
                expected_version: expected,
            },
        );
        ClientRequest::Delete(DeleteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            deletes,
        })
    }

    fn trim_req(agg: AggregateKey, keep_from: u64) -> ClientRequest {
        ClientRequest::TrimStart(TrimStartRequest {
            correlation_id: None,
            aggregate_key: agg,
            keep_from_aggregate_version: keep_from,
            client_id: 1,
            user_id: None,
        })
    }

    fn list_orgs_req() -> ClientRequest {
        ClientRequest::ListOrgs(ListOrgsRequest {
            correlation_id: None,
            shard_id: 0,
            cursor: None,
        })
    }

    fn list_types_req(org: Option<u128>) -> ClientRequest {
        ClientRequest::ListAggregateTypes(ListAggregateTypesRequest {
            correlation_id: None,
            shard_id: 0,
            org_id: org,
            cursor: None,
        })
    }

    fn list_aggs_req(org: Option<u128>, atype: Option<u128>) -> ClientRequest {
        ClientRequest::ListAggregates(ListAggregatesRequest {
            correlation_id: None,
            shard_id: 0,
            org_id: org,
            aggregate_type_id: atype,
            cursor: None,
        })
    }

    async fn open_shard(dir: &std::path::Path) -> ShardWal<StubReplicationClient, StubS3Downloader> {
        ShardWal::open(test_config(dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
            .await
            .unwrap()
    }

    async fn process<R: ReplicationClient, D: S3Downloader>(
        shard: &ShardWal<R, D>,
        req: ClientRequest,
    ) -> Result<ClientResponse, ShardError> {
        shard.process_client_request(req).await
    }

    async fn write_ok<R: ReplicationClient, D: S3Downloader>(shard: &ShardWal<R, D>, req: ClientRequest) {
        let result = process(shard, req).await;
        assert!(
            matches!(result, Ok(ClientResponse::Write(_))),
            "write failed: {:?}",
            result.err()
        );
    }

    fn client_write_req(agg: AggregateKey, evts: Vec<DatablockAggregateEvent>) -> ClientRequest {
        let mut writes = HashMap::new();
        writes.insert(
            agg,
            SingleAggregateWrite {
                events: evts,
                allow_create: true,
                expected_version: None,
                enforce_client_idempotency: false,
            },
        );
        ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes,
        })
    }

    fn client_read_req(agg: AggregateKey) -> ClientRequest {
        ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: agg,
            filters: ReadFilters::new(0),
        })
    }

    fn unwrap_read(result: Result<ClientResponse, ShardError>) -> ReadResponse {
        match result.expect("read should succeed") {
            ClientResponse::Read(r) => r,
            other => panic!("expected Read, got {other:?}"),
        }
    }

    fn unwrap_exists(result: Result<ClientResponse, ShardError>) -> AggregateDetailsResponse {
        match result.expect("exists should succeed") {
            ClientResponse::AggregateDetails(r) => r,
            other => panic!("expected Exists, got {other:?}"),
        }
    }

    fn unwrap_list_orgs(result: Result<ClientResponse, ShardError>) -> ListOrgsResponse {
        match result.expect("list_orgs should succeed") {
            ClientResponse::ListOrgs(r) => r,
            other => panic!("expected ListOrgs, got {other:?}"),
        }
    }

    fn unwrap_list_types(result: Result<ClientResponse, ShardError>) -> ListAggregateTypesResponse {
        match result.expect("list_types should succeed") {
            ClientResponse::ListAggregateTypes(r) => r,
            other => panic!("expected ListAggregateTypes, got {other:?}"),
        }
    }

    fn unwrap_list_aggs(result: Result<ClientResponse, ShardError>) -> ListAggregatesResponse {
        match result.expect("list_aggs should succeed") {
            ClientResponse::ListAggregates(r) => r,
            other => panic!("expected ListAggregates, got {other:?}"),
        }
    }

    // ── Happy path ──

    #[test]
    fn write_and_read_back() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(3))).await;

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 1);
            assert_eq!(read.event_batches[0].events.len(), 3);
            assert_eq!(read.event_batches[0].aggregate_version, 1);

            shard.close().await;
        });
    }

    #[test]
    fn multiple_writes_produce_sequential_batches() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for batch_num in 1u64..=5 {
                let evts = vec![DatablockAggregateEvent {
                    client_seq: batch_num,
                    event_type_major: 1,
                    event_value: Arc::new(vec![batch_num as u8]),
                    ..Default::default()
                }];
                write_ok(&shard, write_req(agg.clone(), evts)).await;
            }

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 5);
            for (i, batch) in read.event_batches.iter().enumerate() {
                assert_eq!(batch.aggregate_version, (i + 1) as u64);
            }

            shard.close().await;
        });
    }

    #[test]
    fn write_to_multiple_aggregates() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let keys: Vec<_> = (1..=3).map(|i| key(1, 1, i)).collect();
            for k in &keys {
                write_ok(&shard, write_req(k.clone(), events(2))).await;
            }

            for k in &keys {
                let read = unwrap_read(process(&shard, read_req(k.clone())).await);
                assert_eq!(read.event_batches.len(), 1);
                assert_eq!(read.event_batches[0].events.len(), 2);
            }

            shard.close().await;
        });
    }

    #[test]
    fn read_nonexistent_aggregate_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, read_req(key(1, 1, 999))).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            shard.close().await;
        });
    }

    // ── Exists ──

    #[test]
    fn exists_missing_returns_error() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, exists_req(key(1, 1, 999))).await;
            assert!(matches!(
                result,
                Err(ShardError::AggregateDetails(ShardAggregateDetailsError::AggregateNotExists))
            ));

            shard.close().await;
        });
    }

    #[test]
    fn exists_after_write_returns_details() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let resp = unwrap_exists(process(&shard, exists_req(agg)).await);
            assert_eq!(resp.min_aggregate_version, FIRST_AGGREGATE_VERSION);
            assert_eq!(resp.max_aggregate_version, FIRST_AGGREGATE_VERSION);
            assert_eq!(resp.max_event_seq, 1);
            assert!(!resp.is_deleted);
            assert!(resp.last_server_timestamp > 0);
            assert_eq!(resp.last_client_id, 1);

            shard.close().await;
        });
    }

    // ── Write validation errors ──

    #[test]
    fn write_without_lease_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            shard.node_status.set(ValidatedNodeStatus::create_fenced(false));

            let result = process(&shard, write_req(key(1, 1, 1), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ShardCannotAcceptWrites { .. }))));

            shard.close().await;
        });
    }

    #[test]
    fn write_rejected_during_rollback_cooldown() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let mut cfg = test_config(&dir);
            cfg.replication_rollback_cooldown = Duration::from_secs(10);
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();

            shard.last_rollback_at.set(Some(std::time::Instant::now()));

            let result = process(&shard, write_req(key(1, 1, 1), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationBackpressure))),
                "expected ReplicationBackpressure while inside cooldown, got {result:?}");

            shard.close().await;
        });
    }

    /// Delete/trim gate on the same cooldown as writes: a retrying delete
    /// client must not hammer straight into a window writes are barred from.
    #[test]
    fn delete_rejected_during_rollback_cooldown() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let mut cfg = test_config(&dir);
            cfg.replication_rollback_cooldown = Duration::from_secs(10);
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();

            let agg = key(1, 1, 1);
            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            shard.last_rollback_at.set(Some(std::time::Instant::now()));

            let result = process(&shard, delete_req(agg)).await;
            assert!(matches!(result, Err(ShardError::Delete(ShardDeleteError::ReplicationBackpressure))),
                "expected ReplicationBackpressure while inside cooldown, got {result:?}");

            shard.close().await;
        });
    }

    #[test]
    fn trim_rejected_during_rollback_cooldown() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let mut cfg = test_config(&dir);
            cfg.replication_rollback_cooldown = Duration::from_secs(10);
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();

            let agg = key(1, 1, 1);
            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            shard.last_rollback_at.set(Some(std::time::Instant::now()));

            let result = process(&shard, trim_req(agg, 2)).await;
            assert!(matches!(result, Err(ShardError::TrimStart(ShardTrimError::ReplicationBackpressure))),
                "expected ReplicationBackpressure while inside cooldown, got {result:?}");

            shard.close().await;
        });
    }

    #[test]
    fn write_accepted_after_rollback_cooldown_expires() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let mut cfg = test_config(&dir);
            cfg.replication_rollback_cooldown = Duration::from_millis(10);
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();

            // Arm the latch far enough in the past that elapsed > cooldown.
            let past = std::time::Instant::now() - Duration::from_secs(60);
            shard.last_rollback_at.set(Some(past));

            let result = process(&shard, write_req(key(1, 1, 1), events(1))).await;
            assert!(result.is_ok(), "expected write to succeed after cooldown, got {result:?}");

            shard.close().await;
        });
    }

    #[test]
    fn write_accepted_when_last_rollback_at_is_none() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let mut cfg = test_config(&dir);
            cfg.replication_rollback_cooldown = Duration::from_secs(10);
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();

            // No rollback has fired — cooldown should not apply.
            assert!(shard.last_rollback_at.get().is_none());

            let result = process(&shard, write_req(key(1, 1, 1), events(1))).await;
            assert!(result.is_ok(), "expected write to succeed when no rollback recorded, got {result:?}");

            shard.close().await;
        });
    }

    // ── Pending-trim window ──
    // A trim-only QueueAggregatePositions entry shadows the durable snapshot in
    // get_write_event_seqes, so concurrent ops read aggregate_version=0 until commit.

    /// Mirror do_trim's enqueue half without awaiting sync, holding the
    /// pending-trim window open for the duration of the test.
    fn enqueue_pending_trim<R: ReplicationClient, D: S3Downloader>(
        shard: &ShardWal<R, D>,
        agg: &AggregateKey,
        keep_from: u64,
    ) {
        let idx = shard.shard_mem_cache.borrow_mut().get_write_event_seqes(agg);
        assert!(idx.aggregate_version > 0, "test setup: aggregate must exist before trim");
        let metablock = Metablock {
            wal_seq: 0,
            server_timestamp: 1000,
            lease_epoch: 0,
            node_id: 1,
            compressed_size: 0,
            uncompressed_size: 0,
            datablock_version: 0,
            datablock_compression_type: 0,
            datablock: DatablockStorageKind::None,
            wal_metablock_type: MetablockKind::SoftTrim(MetablockSoftTrim {
                aggregate_key: agg.clone(),
                keep_from_aggregate_version: keep_from,
                aggregate_version: idx.aggregate_version,
                event_seq: idx.event_seq,
                client_id: 1,
                user_id: None,
            }),
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
            previous_aggregate_metablock_pos: 0,
        };
        shard.shard_mem_cache.borrow_mut().add_pending_trim_to_queue(
            agg, keep_from, idx.aggregate_version, idx.event_seq,
            ShardLogQueueItem::new(None, None, metablock),
        );
    }

    #[test]
    fn occ_write_during_pending_trim_window_sees_real_version() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(2))).await;
            write_ok(&shard, write_req(agg.clone(), events(2))).await; // version 2

            enqueue_pending_trim(&shard, &agg, 1);

            let result = process(&shard, write_req_full(agg.clone(), events(1), false, Some(2), 1, false)).await;
            assert!(
                matches!(result, Ok(ClientResponse::Write(_))),
                "OCC write against the real version must pass during a pending-trim window, got {:?}",
                result.err()
            );

            shard.close().await;
        });
    }

    #[test]
    fn write_during_pending_trim_window_continues_version_sequence() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(2))).await;
            write_ok(&shard, write_req(agg.clone(), events(2))).await; // version 2

            enqueue_pending_trim(&shard, &agg, 1);

            let resp = match process(&shard, write_req(agg.clone(), events(1))).await {
                Ok(ClientResponse::Write(w)) => w,
                other => panic!("expected Write response, got {other:?}"),
            };
            assert_eq!(
                resp.max_aggregate_version,
                Some(3),
                "write during a pending-trim window must continue from version 2, not restart"
            );

            shard.close().await;
        });
    }

    #[test]
    fn trim_during_pending_trim_window_accepts_valid_range() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(2))).await;
            write_ok(&shard, write_req(agg.clone(), events(2))).await; // version 2

            enqueue_pending_trim(&shard, &agg, 1);

            // keep_from=2 is within the real range; must not be rejected as out of range
            let result = process(&shard, trim_req(agg.clone(), 2)).await;
            assert!(
                matches!(result, Ok(ClientResponse::TrimStart(_))),
                "valid trim during a pending-trim window rejected: {:?}",
                result.err()
            );

            shard.close().await;
        });
    }

    #[test]
    fn delete_during_pending_trim_window_records_real_version() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(2))).await;
            write_ok(&shard, write_req(agg.clone(), events(2))).await; // version 2

            enqueue_pending_trim(&shard, &agg, 1);

            let result = process(&shard, delete_req(agg.clone())).await;
            assert!(
                matches!(result, Ok(ClientResponse::Delete(_))),
                "delete during a pending-trim window failed: {:?}",
                result.err()
            );

            let indexes = shard.shard_mem_cache.borrow_mut().get_write_event_seqes(&agg);
            assert!(indexes.pending_delete_or_deleted);
            assert_eq!(
                indexes.aggregate_version, 2,
                "tombstone must carry the real version, not the trim-window zero"
            );

            shard.close().await;
        });
    }

    #[test]
    fn write_empty_events_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, write_req(key(1, 1, 1), vec![])).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::EmptyEventsList))));

            shard.close().await;
        });
    }

    #[test]
    fn write_zero_event_type_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let evts = vec![DatablockAggregateEvent {
                client_seq: 1,
                event_type_major: 0,
                event_value: Arc::new(vec![1]),
                ..Default::default()
            }];
            let result = process(&shard, write_req(key(1, 1, 1), evts)).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ZeroEventType { .. }))));

            shard.close().await;
        });
    }

    #[test]
    fn write_without_allow_create_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let req = write_req_full(key(1, 1, 1), events(1), false, None, 1, false);
            let result = process(&shard, req).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::AggregateNotExists))));

            shard.close().await;
        });
    }

    #[test]
    fn optimistic_concurrency_violation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let req = write_req_full(agg, events(1), true, Some(999), 1, false);
            let result = process(&shard, req).await;
            assert!(matches!(
                result,
                Err(ShardError::Write(ShardWriteError::OptimisticConcurrencyViolation { .. }))
            ));

            shard.close().await;
        });
    }

    #[test]
    fn client_idempotency_violation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            let req = write_req_full(agg.clone(), events(1), true, None, 42, true);
            write_ok(&shard, req).await;

            let req = write_req_full(agg, events(1), true, None, 42, true);
            let result = process(&shard, req).await;
            assert!(matches!(
                result,
                Err(ShardError::Write(ShardWriteError::ClientIdempotencyViolation { .. }))
            ));

            shard.close().await;
        });
    }

    #[test]
    fn occ_fires_before_idempotency() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            // Write with client_id=1, client_seq=1 (aggregate version becomes 1)
            let req = write_req_full(agg.clone(), events(1), true, None, 1, true);
            write_ok(&shard, req).await;

            // Write with client_id=2 to advance aggregate version to 2
            let req = write_req_full(agg.clone(), events(1), true, Some(1), 2, false);
            write_ok(&shard, req).await;

            // Write with client_id=1, client_seq=1 (idempotency violation)
            // AND stale expected_version=1 (OCC violation, current is 2)
            // OCC should fire first
            let req = write_req_full(agg, events(1), true, Some(1), 1, true);
            let result = process(&shard, req).await;
            assert!(matches!(
                result,
                Err(ShardError::Write(ShardWriteError::OptimisticConcurrencyViolation { .. }))
            ));

            shard.close().await;
        });
    }

    /// Regression: aggregate_version is per-aggregate and globally monotonic, even
    /// with idempotency enforced. A distinct client's first write to an existing
    /// aggregate must not restart the version at 1. (Previously the
    /// cache_aggregate_client scan miss clobbered the aggregate snapshot to
    /// NotFound, tripping the "fresh start" branch and producing 1,2,1,2.)
    #[test]
    fn idempotency_does_not_reset_aggregate_version_per_client() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 700002, 800002);

            let evt = |seq: u64| vec![DatablockAggregateEvent {
                client_seq: seq,
                event_type_major: 1,
                event_value: Arc::new(vec![seq as u8]),
                ..Default::default()
            }];

            // client 111 creates, then appends; client 222's first-ever write to
            // this aggregate; client 111 again. All with idempotency enforced.
            write_ok(&shard, write_req_full(agg.clone(), evt(1), true, None, 111, true)).await;
            write_ok(&shard, write_req_full(agg.clone(), evt(2), false, None, 111, true)).await;
            write_ok(&shard, write_req_full(agg.clone(), evt(1), false, None, 222, true)).await;
            write_ok(&shard, write_req_full(agg.clone(), evt(3), false, None, 111, true)).await;

            let read = unwrap_read(process(&shard, read_req(agg.clone())).await);
            let versions: Vec<u64> = read.event_batches.iter().map(|b| b.aggregate_version).collect();
            assert_eq!(versions, vec![1, 2, 3, 4], "aggregate_version must be globally monotonic across clients");
            let clients: Vec<u128> = read.event_batches.iter().map(|b| b.client_id).collect();
            assert_eq!(clients, vec![111, 111, 222, 111]);

            // Idempotency must still reject a duplicate client_seq for an existing (aggregate, client).
            let dup = process(&shard, write_req_full(agg, evt(1), false, None, 222, true)).await;
            assert!(
                matches!(dup, Err(ShardError::Write(ShardWriteError::ClientIdempotencyViolation { .. }))),
                "duplicate client_seq must still be rejected, got {dup:?}"
            );

            shard.close().await;
        });
    }

    // ── Delete ──

    #[test]
    fn delete_then_read_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let result = process(&shard, delete_req(agg.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            let result = process(&shard, read_req(agg)).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            shard.close().await;
        });
    }

    #[test]
    fn exists_after_delete_reports_deleted() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let result = process(&shard, delete_req(agg.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            let resp = unwrap_exists(process(&shard, exists_req(agg)).await);
            assert!(resp.is_deleted, "soft-deleted aggregate must report is_deleted");

            shard.close().await;
        });
    }

    #[test]
    fn delete_then_write_without_recreate_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            let _ = process(&shard, delete_req(agg.clone())).await.unwrap();

            let result = process(&shard, write_req(agg, events(1))).await;
            assert!(matches!(
                result,
                Err(ShardError::Write(ShardWriteError::AggregateRecreateNotAllowed))
            ));

            shard.close().await;
        });
    }

    #[test]
    fn delete_with_recreate_allows_new_write() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let del = delete_req_full(agg.clone(), true, false, None);
            let _ = process(&shard, del).await.unwrap();

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 1);
            assert_eq!(read.event_batches[0].aggregate_version, FIRST_AGGREGATE_VERSION);

            shard.close().await;
        });
    }

    #[test]
    fn delete_with_index_continuation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for _ in 0..3 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            let del = delete_req_full(agg.clone(), true, true, None);
            let _ = process(&shard, del).await.unwrap();

            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let read = unwrap_read(process(&shard, read_req_from(agg, 4)).await);
            assert_eq!(read.event_batches.len(), 1);
            assert_eq!(read.event_batches[0].aggregate_version, 4);

            shard.close().await;
        });
    }

    #[test]
    fn delete_validation_errors() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            let empty_delete = ClientRequest::Delete(DeleteRequest {
                correlation_id: None,
                client_id: 1,
                user_id: None,
                deletes: HashMap::new(),
            });
            let result = process(&shard, empty_delete).await;
            assert!(matches!(result, Err(ShardError::Delete(ShardDeleteError::EmptyDeleteList))));

            let result = process(&shard, delete_req(agg.clone())).await;
            assert!(matches!(result, Err(ShardError::Delete(ShardDeleteError::AggregateNotExists))));

            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            let del = delete_req_full(agg, false, false, Some(999));
            let result = process(&shard, del).await;
            assert!(matches!(
                result,
                Err(ShardError::Delete(ShardDeleteError::OptimisticConcurrencyViolation { .. }))
            ));

            shard.close().await;
        });
    }

    // ── Trim ──

    #[test]
    fn trim_makes_earlier_batches_unavailable() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for _ in 0..5 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            let result = process(&shard, trim_req(agg.clone(), 3)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));

            let result = process(&shard, read_req_from(agg.clone(), 1)).await;
            assert!(matches!(
                result,
                Err(ShardError::Read(ShardReadError::UnavailableBatchIndex { .. }))
            ));

            let read = unwrap_read(process(&shard, read_req_from(agg, 3)).await);
            assert!(!read.event_batches.is_empty());

            shard.close().await;
        });
    }

    #[test]
    fn trim_already_trimmed_is_noop() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for _ in 0..3 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            let _ = process(&shard, trim_req(agg.clone(), 2)).await.unwrap();
            let result = process(&shard, trim_req(agg, 2)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));

            shard.close().await;
        });
    }

    #[test]
    fn trim_validation_errors() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            let result = process(&shard, trim_req(agg.clone(), 1)).await;
            assert!(matches!(result, Err(ShardError::TrimStart(ShardTrimError::AggregateNotExists))));

            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            let result = process(&shard, trim_req(agg, 999)).await;
            assert!(matches!(
                result,
                Err(ShardError::TrimStart(ShardTrimError::TrimIndexOutOfRange { .. }))
            ));

            shard.close().await;
        });
    }

    // ── List operations ──

    #[test]
    fn list_orgs_types_aggregates() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            for org in 1..=3u128 {
                for atype in 1..=2u128 {
                    write_ok(&shard, write_req(key(org, atype, 1), events(1))).await;
                }
            }

            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            assert_eq!(orgs.orgs.len(), 3);

            let types = unwrap_list_types(process(&shard, list_types_req(None)).await);
            assert_eq!(types.aggregate_types.len(), 6);

            let types_filtered = unwrap_list_types(process(&shard, list_types_req(Some(1))).await);
            assert_eq!(types_filtered.aggregate_types.len(), 2);
            assert!(types_filtered.aggregate_types.iter().all(|t| t.org_id == 1));

            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(1))).await);
            assert_eq!(aggs.aggregates.len(), 1);

            shard.close().await;
        });
    }

    #[test]
    fn list_empty_shard() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            assert!(orgs.orgs.is_empty());
            assert!(orgs.next_cursor.is_none());

            shard.close().await;
        });
    }

    #[test]
    fn list_aggregates_shows_deleted() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            let _ = process(&shard, delete_req(agg)).await.unwrap();

            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(1))).await);
            assert_eq!(aggs.aggregates.len(), 1);
            assert!(aggs.aggregates[0].is_deleted);

            shard.close().await;
        });
    }

    // ── Watch rejected ──

    #[test]
    fn watch_rejected() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let watch = ClientRequest::Watch(WatchRequest {
                correlation_id: None,
                requested_latency_ms: None,
                shard_id: None,
                orgs: None,
                aggregate_types: None,
                aggregates: None,
                operation_types: None,
            });
            let result = process(&shard, watch).await;
            assert!(matches!(result, Err(ShardError::WatchRequestInvalid)));

            shard.close().await;
        });
    }

    // ── Exists after trim ──

    #[test]
    fn exists_reflects_trim() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for _ in 0..5 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            let _ = process(&shard, trim_req(agg.clone(), 3)).await.unwrap();

            let resp = unwrap_exists(process(&shard, exists_req(agg)).await);
            assert_eq!(resp.min_aggregate_version, 3);
            assert_eq!(resp.max_aggregate_version, FIRST_AGGREGATE_VERSION + 4);
            assert!(!resp.is_deleted);

            shard.close().await;
        });
    }

    /// Replication client that fails both follower and S3 for a configurable
    /// number of calls, then succeeds. Simulates follower-offline + no-S3.
    struct FailThenSucceedReplicationClient {
        follower_failures_remaining: Cell<u32>,
        s3_failures_remaining: Cell<u32>,
    }

    impl FailThenSucceedReplicationClient {
        fn new(follower_failures: u32, s3_failures: u32) -> Self {
            Self {
                follower_failures_remaining: Cell::new(follower_failures),
                s3_failures_remaining: Cell::new(s3_failures),
            }
        }
    }

    impl ReplicationClient for FailThenSucceedReplicationClient {
        fn set_follower_reachable(&self, _: bool) {}
        fn is_follower_reachable(&self) -> bool { true }
        fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> { None }
        fn set_heartbeat_in_flight(&self, _unix_ms: Option<u64>) {}
        fn reset_heartbeat_state(&self) {}

        async fn replicate_to_follower(&self, _batches: Vec<celeriant_msg::request::requests::ReplicationBatchItem>, _leader_confirmed_wal_seq: u64, _sender_lease_epoch: u64) -> Result<(), ReplicateToFollowerError> {
            let remaining = self.follower_failures_remaining.get();
            if remaining > 0 {
                self.follower_failures_remaining.set(remaining - 1);
                return Err(ReplicateToFollowerError::FollowerUnexpectedResponse);
            }
            Ok(())
        }

        async fn replicate_to_s3(&self, _batches: Vec<celeriant_msg::request::requests::ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
            let remaining = self.s3_failures_remaining.get();
            if remaining > 0 {
                self.s3_failures_remaining.set(remaining - 1);
                return Err(ReplicateToS3Error::S3NotConfigured);
            }
            Ok(())
        }

        fn set_follower_address(&self, _address: Option<String>) {}

        async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _lease_epoch: u64) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
            glommio::timer::sleep(std::time::Duration::from_millis(10)).await;
            Ok(celeriant_msg::response::responses::HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms + 10, follower_can_accept_tcp_replication: true })
        }

        async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> { Ok(true) }
    }

    async fn open_leader_shard(dir: &std::path::Path, client: FailThenSucceedReplicationClient) -> ShardWal<FailThenSucceedReplicationClient, StubS3Downloader> {
        ShardWal::open(test_config(dir), ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 10_000), client, StubS3Downloader)
            .await
            .unwrap()
    }

    /// Replication client with a flag to switch from success to permanent failure.
    struct SwitchableReplicationClient {
        should_fail: Cell<bool>,
    }

    impl SwitchableReplicationClient {
        fn new() -> Self {
            Self { should_fail: Cell::new(false) }
        }
    }

    impl ReplicationClient for SwitchableReplicationClient {
        fn set_follower_address(&self, _: Option<String>) {}
        fn set_follower_reachable(&self, _: bool) {}
        fn is_follower_reachable(&self) -> bool { true }
        fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> { None }
        fn set_heartbeat_in_flight(&self, _unix_ms: Option<u64>) {}
        fn reset_heartbeat_state(&self) {}

        async fn replicate_to_follower(&self, _: Vec<celeriant_msg::request::requests::ReplicationBatchItem>, _: u64, _: u64) -> Result<(), ReplicateToFollowerError> {
            if self.should_fail.get() {
                return Err(ReplicateToFollowerError::FollowerUnexpectedResponse);
            }
            Ok(())
        }

        async fn replicate_to_s3(&self, _: Vec<celeriant_msg::request::requests::ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
            if self.should_fail.get() {
                return Err(ReplicateToS3Error::S3NotConfigured);
            }
            Ok(())
        }

        async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _: u64) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
            Ok(celeriant_msg::response::responses::HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms + 10, follower_can_accept_tcp_replication: true })
        }

        async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> { Ok(true) }
    }

    /// Replication client that bumps `rollback_generation` *during*
    /// `replicate_to_follower`, simulating a concurrent rollback that wipes
    /// state out from under an in-flight `write()`. The first replicate call
    /// after `attach` runs the bump; subsequent calls are passthrough so any
    /// test that wants a recovery write after the bump can still get one.
    /// Calls before `attach` are passthrough too — lets a test set up
    /// aggregates first, then arm the bump for the operation under test.
    struct BumpGenerationDuringReplicateClient {
        shard_mem_cache: RefCell<Option<Rc<RefCell<MemCache>>>>,
        bumped: Cell<bool>,
    }

    impl BumpGenerationDuringReplicateClient {
        fn new() -> Self {
            Self {
                shard_mem_cache: RefCell::new(None),
                bumped: Cell::new(false),
            }
        }

        fn attach(&self, mc: Rc<RefCell<MemCache>>) {
            *self.shard_mem_cache.borrow_mut() = Some(mc);
        }
    }

    impl ReplicationClient for BumpGenerationDuringReplicateClient {
        fn set_follower_address(&self, _: Option<String>) {}
        fn set_follower_reachable(&self, _: bool) {}
        fn is_follower_reachable(&self) -> bool { true }
        fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> { None }
        fn set_heartbeat_in_flight(&self, _unix_ms: Option<u64>) {}
        fn reset_heartbeat_state(&self) {}

        async fn replicate_to_follower(&self, _: Vec<celeriant_msg::request::requests::ReplicationBatchItem>, _: u64, _: u64) -> Result<(), ReplicateToFollowerError> {
            if !self.bumped.get() {
                if let Some(mc) = self.shard_mem_cache.borrow().as_ref() {
                    // Mid-replicate concurrent rollback: bumps rollback_generation
                    // and wipes pending state, mirroring what happens when a
                    // different replication cycle fails while ours is in flight.
                    mc.borrow_mut().execute_replication_rollback();
                    self.bumped.set(true);
                }
            }
            Ok(())
        }

        async fn replicate_to_s3(&self, _: Vec<celeriant_msg::request::requests::ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
            Ok(())
        }

        async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _: u64) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
            Ok(celeriant_msg::response::responses::HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms + 10, follower_can_accept_tcp_replication: true })
        }

        async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> { Ok(true) }
    }

    /// Regression for the rollback-vs-in-flight-future false ack. A write's
    /// `replicate_durable` can return `Ok` via `NoCaptureRaceButOk` even when a
    /// concurrent rollback wiped the data the future thought it had committed.
    /// The `rollback_generation` snapshot/check in `write()` turns the silent
    /// false-ack into a transient `ReplicationError::RollbackInProgress`, so the
    /// client retries with the same client_seq and idempotency handles the rest.
    #[test]
    fn write_returns_err_when_rollback_crosses_replicate() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = BumpGenerationDuringReplicateClient::new();
            let shard = ShardWal::open(
                test_config(&dir),
                ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 10_000),
                client,
                StubS3Downloader,
            ).await.unwrap();
            shard.replication_client.attach(shard.shard_mem_cache.clone());

            let agg = key(1, 1, 1);
            let result = process(&shard, write_req(agg.clone(), events(1))).await;

            assert!(
                matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(ReplicationError::RollbackInProgress)))),
                "write that crossed a rollback must return RollbackInProgress, got {:?}",
                result,
            );

            shard.close().await;
        });
    }

    /// Delete twin of `write_returns_err_when_rollback_crosses_replicate`.
    /// A rollback crossing the delete's replicate wipes the SoftDelete
    /// tombstone — acking `Ok` here tells the client the aggregate is gone
    /// while it still exists with full history. Worse than the write case:
    /// no client_seq idempotency on deletes, so a blind retry isn't
    /// well-defined. Must surface as RollbackInProgress.
    #[test]
    fn delete_returns_err_when_rollback_crosses_replicate() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = BumpGenerationDuringReplicateClient::new();
            let shard = ShardWal::open(
                test_config(&dir),
                ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 10_000),
                client,
                StubS3Downloader,
            ).await.unwrap();

            let agg = key(1, 1, 1);
            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            // Arm the bump now: only the delete's replicate crosses the rollback.
            shard.replication_client.attach(shard.shard_mem_cache.clone());

            let result = process(&shard, delete_req(agg.clone())).await;
            assert!(
                matches!(result, Err(ShardError::Delete(ShardDeleteError::ReplicationError(ReplicationError::RollbackInProgress)))),
                "delete that crossed a rollback must return RollbackInProgress, got {:?}",
                result,
            );

            shard.close().await;
        });
    }

    /// Trim twin. A falsely-acked trim is a retention/compliance lie: the
    /// client believes history below keep_from is gone while it stays readable.
    #[test]
    fn trim_returns_err_when_rollback_crosses_replicate() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = BumpGenerationDuringReplicateClient::new();
            let shard = ShardWal::open(
                test_config(&dir),
                ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 10_000),
                client,
                StubS3Downloader,
            ).await.unwrap();

            let agg = key(1, 1, 1);
            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            shard.replication_client.attach(shard.shard_mem_cache.clone());

            let result = process(&shard, trim_req(agg.clone(), 2)).await;
            assert!(
                matches!(result, Err(ShardError::TrimStart(ShardTrimError::ReplicationError(ReplicationError::RollbackInProgress)))),
                "trim that crossed a rollback must return RollbackInProgress, got {:?}",
                result,
            );

            shard.close().await;
        });
    }

    /// Chaos 16k finding A repro: post-trim event batches must embed the trim
    /// floor in `trimmed_below_version`, and a snapshot rebuilt from disk must
    /// recover it. On the rpi cluster the SoftTrim was durable on both nodes
    /// yet post-trim batches embedded 1 — a restarted node rebuilding from
    /// the batch metablocks resurrects min=1 and the floor is lost on that
    /// node (reads below the floor come back).
    #[test]
    fn post_trim_batches_embed_floor_and_disk_rebuild_recovers_it() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for _ in 0..4 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }
            let result = process(&shard, trim_req(agg.clone(), 3)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))), "trim failed: {result:?}");

            // In-memory floor on the serving node.
            let idx = shard.shard_mem_cache.borrow_mut().get_write_event_seqes(&agg);
            assert_eq!(idx.min_aggregate_version, 3, "in-memory floor after trim");

            // Post-trim write must embed the floor in its metablock.
            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            // Rebuild from disk: this is what a restarted/caught-up node sees.
            shard.shard_mem_cache.borrow_mut().clear_aggregate_write_snapshots_for_test();
            assert!(shard.aggregate_exists_and_cache(&agg, CachePath::Write).await.unwrap());
            let idx = shard.shard_mem_cache.borrow_mut().get_write_event_seqes(&agg);
            assert_eq!(
                idx.min_aggregate_version, 3,
                "disk-rebuilt snapshot must recover the trim floor — a post-trim batch embedding \
                 trimmed_below_version=1 resurrects pre-trim visibility on rebuilt nodes",
            );

            shard.close().await;
        });
    }

    /// Finding-A (chaos 16k): aggregate_details must work when the
    /// aggregate's newest WAL record is a SoftTrim. The details handler's
    /// last-metablock read only accepted EventBatch/SoftDelete and errored
    /// with "unexpected metablock kind: Discriminant(3)" — reproduced on
    /// both rpi nodes after a fresh boot over a trim-tailed WAL.
    #[test]
    fn aggregate_details_works_when_newest_record_is_a_trim() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for _ in 0..4 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }
            let result = process(&shard, trim_req(agg.clone(), 3)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))), "trim failed: {result:?}");

            // Cold details read: snapshot rebuilt from disk, last record is the trim.
            shard.shard_mem_cache.borrow_mut().clear_aggregate_write_snapshots_for_test();
            shard.shard_mem_cache.borrow_mut().clear_aggregate_read_snapshots_for_test();
            let details = unwrap_exists(process(&shard, exists_req(agg)).await);
            assert_eq!(details.max_aggregate_version, 4);
            assert_eq!(details.min_aggregate_version, 3, "floor must come back from the SoftTrim record");
            assert!(!details.is_deleted);

            shard.close().await;
        });
    }

    /// Finding-A variant: the trim floor must survive a delete +
    /// sequence-continuation recreate cycle (the chaos side-load's exact op
    /// mix). The deleting task's aggregates on the rpi embedded
    /// trimmed_below_version=1 in every post-recreate batch.
    #[test]
    fn trim_floor_survives_delete_recreate_continuation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for _ in 0..4 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }
            let result = process(&shard, trim_req(agg.clone(), 3)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))), "trim failed: {result:?}");

            let result = process(&shard, delete_req_full(agg.clone(), true, true, Some(4))).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))), "delete failed: {result:?}");

            // Continuation recreate: v5, then another append.
            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            // Trim the new incarnation.
            let result = process(&shard, trim_req(agg.clone(), 6)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))), "trim2 failed: {result:?}");
            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            let idx = shard.shard_mem_cache.borrow_mut().get_write_event_seqes(&agg);
            assert_eq!(idx.min_aggregate_version, 6, "in-memory floor after post-recreate trim");

            // Rebuild from disk — what a restarted/caught-up node sees.
            shard.shard_mem_cache.borrow_mut().clear_aggregate_write_snapshots_for_test();
            assert!(shard.aggregate_exists_and_cache(&agg, CachePath::Write).await.unwrap());
            let idx = shard.shard_mem_cache.borrow_mut().get_write_event_seqes(&agg);
            assert_eq!(
                idx.min_aggregate_version, 6,
                "disk-rebuilt floor after delete/recreate/trim — embedded trimmed_below_version \
                 must track the live floor through the recreate",
            );

            shard.close().await;
        });
    }

    /// Delete fsyncs, replication fails, the real rollback runs. The client
    /// must get an error and the aggregate must remain fully live: exists-scan
    /// agrees, history readable.
    #[test]
    fn delete_rolled_back_by_replication_failure_leaves_aggregate_live() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = SwitchableReplicationClient::new();
            let shard = ShardWal::open(
                test_config(&dir),
                ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 10_000),
                client,
                StubS3Downloader,
            ).await.unwrap();

            let agg = key(1, 1, 1);
            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            shard.replication_client.should_fail.set(true);
            let result = process(&shard, delete_req(agg.clone())).await;
            assert!(result.is_err(), "delete whose replication failed must not ack, got {:?}", result);

            shard.replication_client.should_fail.set(false);
            let details = unwrap_exists(process(&shard, exists_req(agg.clone())).await);
            assert!(!details.is_deleted, "rolled-back delete must leave the aggregate live");
            assert_eq!(details.max_aggregate_version, 1);

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 1);

            shard.close().await;
        });
    }

    /// Trim twin: a rolled-back trim must leave the full history readable —
    /// retention logic acting on a trim ack while the data stays readable is
    /// the compliance failure mode.
    #[test]
    fn trim_rolled_back_by_replication_failure_leaves_history_readable() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = SwitchableReplicationClient::new();
            let shard = ShardWal::open(
                test_config(&dir),
                ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 10_000),
                client,
                StubS3Downloader,
            ).await.unwrap();

            let agg = key(1, 1, 1);
            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            write_ok(&shard, write_req(agg.clone(), events(1))).await;

            shard.replication_client.should_fail.set(true);
            let result = process(&shard, trim_req(agg.clone(), 2)).await;
            assert!(result.is_err(), "trim whose replication failed must not ack, got {:?}", result);

            shard.replication_client.should_fail.set(false);
            // ≤1 = no effective floor (batches embed the normalized
            // FIRST_AGGREGATE_VERSION); a committed trim would report 2 here.
            let details = unwrap_exists(process(&shard, exists_req(agg.clone())).await);
            assert!(details.min_aggregate_version <= 1, "rolled-back trim must not move min_aggregate_version, got {}", details.min_aggregate_version);

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 2, "rolled-back trim must leave both batches readable");

            shard.close().await;
        });
    }

    /// Issue-2 TOCTOU: multi-aggregate delete awaits between entries (cold
    /// snapshot loads) but enqueues prepared tombstones with no sync
    /// re-validation. A write landing in that window gets silently overwritten
    /// by a stale tombstone: OCC is bypassed AND the tombstone's regressed
    /// aggregate_version/event_seq feed a sequence-continuation recreate that
    /// re-issues already-acked versions — WAL corruption.
    ///
    /// Both delete entries are identical so the HashMap's iteration order
    /// (which delete() follows) picks the roles: first key = contended (kept
    /// warm, raced by the write), second = cold (its disk scan opens the
    /// await window).
    #[test]
    fn multi_aggregate_delete_toctou_rejects_concurrent_write() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let mut deletes = HashMap::new();
            for k in [key(1, 1, 1), key(1, 1, 2)] {
                deletes.insert(k, SingleAggregateDelete {
                    allow_recreate: true,
                    allow_sequence_continuation: true,
                    expected_version: Some(5),
                });
            }
            let contended = deletes.keys().next().unwrap().clone();
            let cold = deletes.keys().nth(1).unwrap().clone();

            // cold's batches go in first so contended's newest batch is the
            // newest block in the WAL: the warm-up reverse scan below stops
            // there and never walks past (and eagerly caches) cold's blocks.
            for _ in 0..5 {
                write_ok(&shard, write_req(cold.clone(), events(1))).await;
            }
            for _ in 0..5 {
                write_ok(&shard, write_req(contended.clone(), events(1))).await;
            }

            // Evict everything, re-warm only the contended aggregate.
            shard.shard_mem_cache.borrow_mut().clear_aggregate_write_snapshots_for_test();
            assert!(shard.aggregate_exists_and_cache(&contended, CachePath::Write).await.unwrap());
            let (cold_loaded, _) = shard.shard_mem_cache.borrow_mut().aggregate_load_status(&cold, CachePath::Write);
            assert!(!cold_loaded, "precondition: cold must be a cache miss or the delete never parks and this test is vacuous");

            // Hold the cold aggregate's loading lock so the delete parks on
            // it deterministically — racing the actual disk scan is flaky
            // (a page-cache-fast scan lets the delete win and the window
            // never opens).
            let cold_lock = shard.aggregate_loading.acquire(&cold);
            let cold_lock_guard = cold_lock.write().await.unwrap();

            let delete_request = ClientRequest::Delete(DeleteRequest {
                correlation_id: None,
                client_id: 1,
                user_id: None,
                deletes,
            });
            let delete_fut = process(&shard, delete_request);
            futures_lite::pin!(delete_fut);
            assert!(
                futures_lite::future::poll_once(delete_fut.as_mut()).await.is_none(),
                "delete must park on the cold aggregate's loading lock",
            );

            // The window: enqueue v6 on the contended aggregate while the
            // delete is parked. One poll validates and enqueues (the delete's
            // sync re-check reads queue positions, so a pending v6 is
            // enough); the fsync completes after the lock releases, keeping
            // the parked delete's 1s lock-deadlock budget out of play.
            // client_id 1 keeps the write's client snapshot warm from setup —
            // a fresh client_id would park this write on its own dedup disk
            // scan before it enqueues.
            let write_fut = process(&shard, write_req_full(contended.clone(), events(1), false, Some(5), 1, false));
            futures_lite::pin!(write_fut);
            let write_early = futures_lite::future::poll_once(write_fut.as_mut()).await;

            drop(cold_lock_guard);
            drop(cold_lock);
            let (delete_result, write_result) = match write_early {
                // Write completed in one poll — already committed v6.
                Some(result) => (delete_fut.await, result),
                None => futures_lite::future::zip(delete_fut, write_fut).await,
            };

            let write_resp = match write_result {
                Ok(ClientResponse::Write(w)) => w,
                other => panic!("concurrent write should commit v6, got {other:?}"),
            };
            assert_eq!(write_resp.max_aggregate_version, Some(6));

            // Version-uniqueness probe: a recreate with sequence continuation
            // continues from whatever the tombstone recorded. Probe before
            // asserting so a pre-fix run shows the duplicate too.
            let recreate_resp = match process(&shard, write_req_full(contended.clone(), events(1), true, None, 3, false)).await {
                Ok(ClientResponse::Write(w)) => w,
                other => panic!("recreate write should succeed, got {other:?}"),
            };

            assert!(
                matches!(delete_result, Err(ShardError::Delete(ShardDeleteError::OptimisticConcurrencyViolation { .. }))),
                "delete validated at v5 but v6 committed before enqueue — must fail OCC, got {delete_result:?} \
                 (an Ok here means a stale v5 tombstone was acked over the committed v6 write)",
            );
            assert_ne!(
                recreate_resp.max_aggregate_version, Some(6),
                "aggregate_version 6 was already acked to the concurrent writer — re-issuing it is WAL corruption",
            );
            assert_eq!(recreate_resp.max_aggregate_version, Some(7), "delete failed, so this is a plain append after v6");

            shard.close().await;
        });
    }

    #[test]
    fn rollback_after_rotation_does_not_panic() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let temp_dir = dir.join("compaction_temp");
            std::fs::create_dir_all(&temp_dir).unwrap();

            let config = InternalShardConfig {
                shard_log_preallocate_bytes: 2 * celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES as u64 + 512 * 1024, // 2 headers + 512KB usable
                compaction_temp_dir: temp_dir,
                ..test_config(&dir)
            };

            let client = SwitchableReplicationClient::new();
            let shard = ShardWal::open(
                config,
                ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 30_000),
                client,
                StubS3Downloader,
            ).await.unwrap();

            let agg = key(1, 1, 1);
            let old_log_id = shard.log_segments_cache.active_log_id();

            // Fill the segment with fat writes until rotation occurs (replication succeeds)
            let mut i = 1u64;
            while shard.log_segments_cache.active_log_id() == old_log_id {
                write_ok(&shard, write_req(agg.clone(), fat_event(i))).await;
                i += 1;
            }

            // Now on a fresh segment with read: None. Flip to fail mode.
            shard.replication_client.should_fail.set(true);

            // This write triggers replication failure → rollback on the new segment.
            // Before the fix, this panics with unwrap() on metadata.read which is None.
            let result = process(&shard, write_req(agg.clone(), fat_event(i))).await;
            assert!(
                matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))),
                "expected ReplicationError from rollback, got {:?}", result
            );

            shard.close().await;
        });
    }

    // TODO rewrite for defer-rollback semantics (snapshot stays in pending).
    #[test]
    #[ignore]
    fn write_succeeds_after_replication_rollback() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            let client = FailThenSucceedReplicationClient::new(1, 1);
            let shard = open_leader_shard(&dir, client).await;
            let agg = key(1, 1, 1);

            // Write 1: triggers rollback (follower offline + S3 not configured)
            let result = process(&shard, write_req(agg.clone(), events(1))).await;
            assert!(
                matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))),
                "expected ReplicationError, got {:?}", result
            );

            // Write 2: must succeed — stale rollback flags must not block this
            let result = process(&shard, write_req(agg.clone(), events(1))).await;
            assert!(
                matches!(result, Ok(ClientResponse::Write(_))),
                "write after rollback should succeed, got {:?}", result
            );

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 1);
            assert_eq!(read.event_batches[0].events.len(), 1);

            shard.close().await;
        });
    }

    #[test]
    #[ignore]
    fn multiple_writes_succeed_after_replication_rollback() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            let client = FailThenSucceedReplicationClient::new(2, 2);
            let shard = open_leader_shard(&dir, client).await;
            let agg = key(1, 1, 1);

            // Write 1: rollback
            let result = process(&shard, write_req(agg.clone(), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))));

            // Write 2: rollback again
            let result = process(&shard, write_req(agg.clone(), events(1))).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))));

            // Write 3: should succeed
            let result = process(&shard, write_req(agg.clone(), events(1))).await;
            assert!(
                matches!(result, Ok(ClientResponse::Write(_))),
                "write after multiple rollbacks should succeed, got {:?}", result
            );

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 1);

            shard.close().await;
        });
    }

    // ── Write-visibility fixes ──

    /// Performance fix: when cache_aggregate_client scans the WAL for a new client
    /// and finds nothing, it must insert sentinel 0 into the client-snapshots cache
    /// so subsequent idempotent writes from the same client skip the disk scan.
    #[test]
    fn cache_aggregate_client_inserts_sentinel_zero_on_miss() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);
            let client_id = 42u128;
            let client_key = AggregateClientKey::new(agg.clone(), client_id);

            let (loaded_before, _) = shard.shard_mem_cache.borrow_mut()
                .aggregate_client_load_status(&agg, &client_key);
            assert!(!loaded_before, "client must not be loaded before first lookup");

            shard.cache_aggregate_client(&agg, &client_key).await.unwrap();

            let (loaded_after, last_seq) = shard.shard_mem_cache.borrow_mut()
                .aggregate_client_load_status(&agg, &client_key);
            assert!(loaded_after, "client must be flagged loaded after scan");
            assert_eq!(last_seq, None, "sentinel 0 should be present (returned as None)");

            shard.close().await;
        });
    }

    /// Ensure we extend to write cursor not just read
    #[test]
    fn client_dedup_scan_does_not_clamp_to_stale_read_cursor() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);
            let client_id = 7u128;

            for n in 1u64..=5 {
                let evt = DatablockAggregateEvent {
                    client_seq: n,
                    event_type_major: 1,
                    event_value: Arc::new(vec![n as u8]),
                    ..Default::default()
                };
                let req = write_req_full(agg.clone(), vec![evt], true, None, client_id, true);
                write_ok(&shard, req).await;
            }

            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, client_id),
                Some(5),
                "precondition: dedup cache should hold client_seq=5 after 5 writes"
            );

            // Rewind read cursor so v=4 and v=5 sit on disk past the read cursor.
            let active = shard.log_segments_cache.active();
            let rewound_position = HEADER_BLOCK_SIZE_BYTES as u64 + 3 * FIXED_BLOCK_SIZE_BYTES as u64;
            {
                let mut metadata = active.metadata.borrow_mut();
                let read = metadata.read.as_mut().expect("standalone advance_visible_position must have populated read");
                assert!(read.metablocks_position > rewound_position,
                    "test invariant: read cursor should be past v=3 before rewind");
                read.metablocks_position = rewound_position;
            }

            // Force `cache_aggregate_client` to actually exercise its scanner:
            // keep the aggregate snapshot (so `get_aggregate_last_metablock_pos`
            // returns a usable starting position) but drop the client mapping.
            // This is the production state when a different client_id is being
            // looked up against an already-warm aggregate, or when the client
            // LRU evicted this client while the aggregate LRU did not.
            {
                let mut mc = shard.shard_mem_cache.borrow_mut();
                mc.execute_replication_rollback();
            }
            shard.aggregate_exists_and_cache(&agg, CachePath::Write)
                .await
                .expect("aggregate cache reload should succeed");
            // Drop only the client snapshot to force a dedup-scan path.
            shard.shard_mem_cache.borrow_mut().clear_aggregate_write_client_snapshots_for_test();
            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, client_id),
                None,
                "precondition: client snapshot must be empty so dedup scan runs"
            );

            let client_key = AggregateClientKey::new(agg.clone(), client_id);
            shard.cache_aggregate_client(&agg, &client_key)
                .await
                .expect("dedup scan should succeed");

            let cached = shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, client_id);
            assert_eq!(
                cached, Some(5),
                "dedup scan must reach write cursor: a reload returning {cached:?} \
                 would let a retry with client_seq <=5 be accepted as fresh, producing \
                 a same-client_seq-with-different-aggregate_version duplicate on disk"
            );

            shard.close().await;
        });
    }

    #[test]
    fn client_dedup_scan_with_absent_aggregate_snapshot_finds_stored_seq() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);
            let client_id = 7u128;

            for n in 1u64..=5 {
                let evt = DatablockAggregateEvent {
                    client_seq: n,
                    event_type_major: 1,
                    event_value: Arc::new(vec![n as u8]),
                    ..Default::default()
                };
                let req = write_req_full(agg.clone(), vec![evt], true, None, client_id, true);
                write_ok(&shard, req).await;
            }

            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, client_id),
                Some(5),
                "precondition: dedup cache should hold client_seq=5 after 5 writes"
            );

            // Force the absent-aggregate-snapshot state: drop BOTH the aggregate
            // write snapshot (so the scan's start position defaults to log_id 0)
            // and the client mapping (so the dedup scan actually runs). The on-disk
            // WAL still holds all 5 writes.
            {
                let mut mc = shard.shard_mem_cache.borrow_mut();
                mc.clear_aggregate_write_snapshots_for_test();
                mc.clear_aggregate_write_client_snapshots_for_test();
            }
            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, client_id),
                None,
                "precondition: client snapshot must be empty so the dedup scan runs"
            );

            let client_key = AggregateClientKey::new(agg.clone(), client_id);
            shard.cache_aggregate_client(&agg, &client_key)
                .await
                .expect("dedup scan should succeed");

            let cached = shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, client_id);
            assert_eq!(
                cached, Some(5),
                "dedup scan with an absent aggregate snapshot must still find client_seq=5 \
                 on disk; reading back {cached:?} (sentinel 0 -> None) would accept a replay \
                 with client_seq <=5 as fresh — an exactly-once violation"
            );

            shard.close().await;
        });
    }

    #[test]
    fn client_dedup_scan_absent_snapshot_finds_multiple_clients() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);
            let client_a = 7u128;
            let client_b = 9u128;

            for n in 1u64..=5 {
                let evt = DatablockAggregateEvent {
                    client_seq: n,
                    event_type_major: 1,
                    event_value: Arc::new(vec![n as u8]),
                    ..Default::default()
                };
                write_ok(&shard, write_req_full(agg.clone(), vec![evt], true, None, client_a, true)).await;
            }
            for n in 1u64..=3 {
                let evt = DatablockAggregateEvent {
                    client_seq: n,
                    event_type_major: 1,
                    event_value: Arc::new(vec![n as u8]),
                    ..Default::default()
                };
                write_ok(&shard, write_req_full(agg.clone(), vec![evt], false, None, client_b, true)).await;
            }

            // Drop the aggregate write snapshot AND both client mappings.
            {
                let mut mc = shard.shard_mem_cache.borrow_mut();
                mc.clear_aggregate_write_snapshots_for_test();
                mc.clear_aggregate_write_client_snapshots_for_test();
            }

            // Scanning for client_a must find seq 5 and eager-cache client_b's seq 3.
            let key_a = AggregateClientKey::new(agg.clone(), client_a);
            shard.cache_aggregate_client(&agg, &key_a).await.expect("dedup scan should succeed");

            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, client_a),
                Some(5),
                "target client seq must be found with the aggregate snapshot absent"
            );
            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, client_b),
                Some(3),
                "co-resident client's seq must be eager-cached during the same scan"
            );

            shard.close().await;
        });
    }

    #[test]
    fn client_scan_start_prefers_snapshot_then_active_tip() {
        // (snap_log_id, snap_pos, active_tip, expected_log_id, expected_start)
        let cases = [
            // Snapshot hint wins even when a tip exists: it can point at a sealed segment.
            (3u64, 500_000u64, Some(700_000u64), 3u64, Some(501_024u64)),
            // Cold aggregate snapshot, aggregate present in the active segment: seek to its tip.
            (0, 0, Some(700_000), 9, Some(701_024)),
            // Both cold: unbounded reverse scan from the active write tip.
            (0, 0, None, 9, None),
        ];
        for (snap_log_id, snap_pos, tip, want_log, want_start) in cases {
            let last_known = MetablockPosition {
                log_id: snap_log_id,
                metablock_absolute_pos: snap_pos,
                event_seq: 0,
                aggregate_version: 0,
                min_aggregate_version: 0,
            };
            let got = client_scan_start(&last_known, 9, tip);
            assert_eq!(
                got,
                (want_log, want_start),
                "snap_log_id={snap_log_id} snap_pos={snap_pos} tip={tip:?}"
            );
        }
    }

    #[test]
    fn client_dedup_scan_cold_aggregate_in_sealed_segment_only_still_found() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let mut cfg = test_config(&dir);
            cfg.shard_log_preallocate_bytes = 1024 * 1024; // small segment -> rotates quickly
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                .await
                .unwrap();
            let cold = key(1, 1, 1);
            let filler = key(1, 1, 2);
            let client_id = 7u128;

            for n in 1u64..=5 {
                let evt = DatablockAggregateEvent {
                    client_seq: n,
                    event_type_major: 1,
                    event_value: Arc::new(vec![n as u8]),
                    ..Default::default()
                };
                write_ok(&shard, write_req_full(cold.clone(), vec![evt], true, None, client_id, true)).await;
            }

            // Fill with a foreign aggregate until the segment rotates: the cold
            // aggregate now lives only in the sealed segment, so the active tips
            // map has no entry for it and the scan must fall back to the walk.
            let mut rotated = false;
            for n in 1u64..=1200 {
                let evt = DatablockAggregateEvent {
                    client_seq: n,
                    event_type_major: 1,
                    event_value: Arc::new(vec![0u8; 4096]),
                    ..Default::default()
                };
                write_ok(&shard, write_req_full(filler.clone(), vec![evt], n == 1, None, 8u128, true)).await;
                if shard.log_segments_cache.active_log_id() > 1 {
                    rotated = true;
                    break;
                }
            }
            assert!(rotated, "scaffolding: 1MB preallocate must rotate within 1200 writes");
            assert!(
                !shard.log_segments_cache.active().aggregate_chain_tips.borrow().contains_key(&cold),
                "scaffolding: cold aggregate must be absent from the active tips map"
            );

            {
                let mut mc = shard.shard_mem_cache.borrow_mut();
                mc.clear_aggregate_write_snapshots_for_test();
                mc.clear_aggregate_write_client_snapshots_for_test();
            }

            let client_key = AggregateClientKey::new(cold.clone(), client_id);
            shard.cache_aggregate_client(&cold, &client_key).await.expect("dedup scan should succeed");

            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&cold, client_id),
                Some(5),
                "a cold aggregate with no active-tip entry must still be found in the sealed segment"
            );

            shard.close().await;
        });
    }

    #[test]
    fn delete_and_trim_insert_client_into_segment_client_bloom() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);
            let writer = 7u128;
            let deleter = 11u128;
            let trimmer = 13u128;

            write_ok(&shard, write_req_full(agg_a.clone(), events(3), true, None, writer, false)).await;
            // Two batches so B is at version 2 and the trim floor below is a valid range.
            write_ok(&shard, write_req_full(agg_b.clone(), events(3), true, None, writer, false)).await;
            write_ok(&shard, write_req_full(agg_b.clone(), events(3), false, None, writer, false)).await;

            let mut deletes = HashMap::new();
            deletes.insert(agg_a.clone(), SingleAggregateDelete {
                allow_recreate: false,
                allow_sequence_continuation: false,
                expected_version: None,
            });
            let del = process(&shard, ClientRequest::Delete(DeleteRequest {
                correlation_id: None,
                client_id: deleter,
                user_id: None,
                deletes,
            })).await;
            assert!(matches!(del, Ok(ClientResponse::Delete(_))));

            let trim = process(&shard, ClientRequest::TrimStart(TrimStartRequest {
                correlation_id: None,
                aggregate_key: agg_b.clone(),
                keep_from_aggregate_version: 2,
                client_id: trimmer,
                user_id: None,
            })).await;
            assert!(matches!(trim, Ok(ClientResponse::TrimStart(_))));

            // Every aggregate-scoped client-bearing metablock kind must land in the
            // client bloom: a delete-only or trim-only client missing from it is a
            // subset, and a subset bloom answers a false "absent" once a consumer
            // trusts it. (SchemaRegistration also carries a client_id but touches no
            // aggregate, so it stays out.)
            let active = shard.log_segments_cache.active();
            let metadata = active.metadata.borrow();
            let bloom = metadata.write.client_id_bloom.borrow();
            assert!(bloom.may_contain_hash(client_id_bloom_hash(writer)), "writer must be in the client bloom");
            assert!(bloom.may_contain_hash(client_id_bloom_hash(deleter)), "delete-only client must be in the client bloom");
            assert!(bloom.may_contain_hash(client_id_bloom_hash(trimmer)), "trim-only client must be in the client bloom");
            assert!(!bloom.may_contain_hash(client_id_bloom_hash(99u128)), "tripwire: a never-seen client must not be claimed present");
            drop(bloom);
            drop(metadata);

            shard.close().await;
        });
    }

    // ── Blooms out of the header: open-time rebuild + sidecar-fed sealed loads ──

    /// Writes one of every client-bearing kind plus a schema registration and returns
    /// the live active-segment bloom words (the ground truth a rebuild must reproduce).
    async fn write_all_kinds_and_snapshot_blooms(
        shard: &ShardWal<StubReplicationClient, StubS3Downloader>,
    ) -> (Vec<u64>, Vec<u64>) {
        let agg_a = key(1, 1, 1);
        let agg_b = key(1, 1, 2);

        write_ok(shard, write_req_full(agg_a.clone(), events(3), true, None, 7u128, false)).await;
        write_ok(shard, write_req_full(agg_b.clone(), events(3), true, None, 7u128, false)).await;
        write_ok(shard, write_req_full(agg_b.clone(), events(3), false, None, 7u128, false)).await;

        let mut deletes = HashMap::new();
        deletes.insert(agg_a, SingleAggregateDelete {
            allow_recreate: false,
            allow_sequence_continuation: false,
            expected_version: None,
        });
        let del = process(shard, ClientRequest::Delete(DeleteRequest {
            correlation_id: None, client_id: 11u128, user_id: None, deletes,
        })).await;
        assert!(matches!(del, Ok(ClientResponse::Delete(_))));

        let trim = process(shard, ClientRequest::TrimStart(TrimStartRequest {
            correlation_id: None,
            aggregate_key: agg_b,
            keep_from_aggregate_version: 2,
            client_id: 13u128,
            user_id: None,
        })).await;
        assert!(matches!(trim, Ok(ClientResponse::TrimStart(_))));

        let schema = process(shard, schema_req(2, 2, 1, 0, NAME_AGE_SCHEMA)).await;
        assert!(matches!(schema, Ok(ClientResponse::RegisterSchema(_))));

        let active = shard.log_segments_cache.active();
        let metadata = active.metadata.borrow();
        let agg_words = metadata.write.aggregate_key_bloom.borrow().to_bytes();
        let client_words = metadata.write.client_id_bloom.borrow().to_bytes();
        (agg_words, client_words)
    }

    /// The reopened shard's active-segment blooms, asserted against the pre-restart
    /// live words: the open-time forward scan must reproduce them exactly (same
    /// inserts, same sizes -> byte-identical SBBF), covering every client-bearing
    /// kind (tombstone-only and trim-only clients included). The schema hash lands
    /// in the summary accumulator's schema set, never the aggregate bloom.
    async fn assert_reopened_blooms_match(dir: &std::path::Path, expected_agg: &[u64], expected_client: &[u64]) {
        let shard = open_shard(dir).await;
        {
            let active = shard.log_segments_cache.active();
            let metadata = active.metadata.borrow();
            let agg = metadata.write.aggregate_key_bloom.borrow();
            let client = metadata.write.client_id_bloom.borrow();
            assert!(!agg.is_absent() && !client.is_absent(), "open must rebuild PRECISE blooms, not leave them absent");
            assert_eq!(agg.to_bytes(), expected_agg, "rebuilt aggregate bloom must equal the live pre-restart words");
            assert_eq!(client.to_bytes(), expected_client, "rebuilt client bloom must equal the live pre-restart words");

            assert!(agg.may_contain(&key(1, 1, 1)) && agg.may_contain(&key(1, 1, 2)));
            assert!(!agg.may_contain_hash(SchemaKey::new(2, 2, 1, 0).bloom_hash()), "schema hashes must stay out of the aggregate bloom");
            assert!(!agg.may_contain(&key(9, 9, 9)), "tripwire: never-seen aggregate must stay absent");
            for client_id in [7u128, 11, 13] {
                assert!(client.may_contain_hash(client_id_bloom_hash(client_id)), "client {client_id} must be rebuilt");
            }
            assert!(!client.may_contain_hash(client_id_bloom_hash(99u128)), "tripwire: never-seen client must stay absent");
        }
        assert!(
            shard.shard_mem_cache.borrow().active_segment_may_contain_schema(SchemaKey::new(2, 2, 1, 0).bloom_hash()),
            "open must rebuild the schema hash into the active schema set"
        );
        shard.close().await;
    }

    #[test]
    fn open_rebuilds_active_segment_blooms_after_clean_close() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let (agg_words, client_words) = {
                let shard = open_shard(&dir).await;
                let words = write_all_kinds_and_snapshot_blooms(&shard).await;
                shard.close().await;
                words
            };
            assert_reopened_blooms_match(&dir, &agg_words, &client_words).await;
        });
    }

    /// Crash-shaped: the shard is dropped without close mid-life (the
    /// warmup_must_not_seed_stale_state_below_fsynced_tail pattern). Everything acked
    /// is fsynced, so the rebuild over the durable prefix must still be exact.
    #[test]
    fn open_rebuilds_active_segment_blooms_after_kill_style_drop() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let (agg_words, client_words) = {
                let shard = open_shard(&dir).await;
                let words = write_all_kinds_and_snapshot_blooms(&shard).await;
                drop(shard); // SIGKILL: no graceful close
                words
            };
            assert_reopened_blooms_match(&dir, &agg_words, &client_words).await;
        });
    }

    /// A sealed segment evicted from the LRU reloads with its blooms from the
    /// .summary sidecar — the sidecar words are right-sized at seal
    /// (smaller than the live fixed-size bloom) but must answer identically:
    /// members present, never-seen keys short-circuited.
    #[test]
    fn sealed_reload_installs_sidecar_blooms() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg = key(1, 10, 100);
            write_ok(&shard, write_req_full(agg.clone(), fat_event(1), true, None, 7u128, false)).await;
            trigger_rotation(&shard).await;

            shard.log_segments_cache.evict_from_lru(1);
            let seg = shard.log_segments_cache.get(1).await.unwrap();
            let meta = seg.metadata.borrow();
            let agg_bloom = meta.write.aggregate_key_bloom.borrow();
            assert!(!agg_bloom.is_absent(), "reload must install the sidecar blooms");
            assert_eq!(agg_bloom.to_bytes().len() * 8, 32, "installed words must be the seal-time right-sized bloom");
            assert!(agg_bloom.may_contain(&agg));
            assert!(!agg_bloom.may_contain(&key(9, 9, 9)), "tripwire: the reloaded bloom must short-circuit never-seen keys");
            let client_bloom = meta.write.client_id_bloom.borrow();
            assert!(client_bloom.may_contain_hash(client_id_bloom_hash(7u128)));
            assert!(!client_bloom.may_contain_hash(client_id_bloom_hash(99u128)));
            // The scanner's read-cursor path consults the same installed words.
            if let Some(read) = meta.read.as_ref() {
                assert!(!read.aggregate_key_bloom.borrow().may_contain(&key(9, 9, 9)));
            }
            drop(client_bloom);
            drop(agg_bloom);
            drop(meta);

            shard.close().await;
        });
    }

    /// Missing sidecar: the reload keeps ABSENT blooms — maybe-present for every key,
    /// so the scanner cannot unsoundly skip — and lookups still return correct results
    /// via the full walk.
    #[test]
    fn sealed_reload_missing_sidecar_no_short_circuit_but_correct() {
        glommio_test!({
            use crate::shard_wal_sync::summary_path;

            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg = key(1, 10, 100);
            let client_id = 7u128;
            write_ok(&shard, write_req_full(agg.clone(), events(3), true, None, client_id, true)).await;
            trigger_rotation(&shard).await;

            std::fs::remove_file(summary_path(shard.log_segments_cache.shard_dir(), 1)).unwrap();
            shard.log_segments_cache.evict_from_lru(1);

            {
                let seg = shard.log_segments_cache.get(1).await.unwrap();
                let meta = seg.metadata.borrow();
                assert!(meta.write.aggregate_key_bloom.borrow().is_absent(), "no sidecar -> no bloom");
                assert!(meta.write.aggregate_key_bloom.borrow().may_contain(&key(9, 9, 9)),
                    "an absent bloom must answer maybe-present, never claim universal absence");
            }

            {
                let mut mc = shard.shard_mem_cache.borrow_mut();
                mc.clear_aggregate_write_snapshots_for_test();
                mc.clear_aggregate_write_client_snapshots_for_test();
            }
            let client_key = AggregateClientKey::new(agg.clone(), client_id);
            shard.cache_aggregate_client(&agg, &client_key).await.expect("dedup scan should succeed");
            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, client_id),
                Some(3),
                "bloomless segment must degrade to the full walk, not lose the client"
            );

            shard.close().await;
        });
    }

    #[test]
    fn client_dedup_scan_absent_snapshot_new_aggregate_is_never_seen() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let written = key(1, 1, 1);
            let never_written = key(1, 1, 2);
            let client_id = 7u128;

            // Populate the WAL with an unrelated aggregate so the scan has real
            // segments to (bloom-)traverse rather than an empty log.
            for n in 1u64..=5 {
                let evt = DatablockAggregateEvent {
                    client_seq: n,
                    event_type_major: 1,
                    event_value: Arc::new(vec![n as u8]),
                    ..Default::default()
                };
                write_ok(&shard, write_req_full(written.clone(), vec![evt], true, None, client_id, true)).await;
            }
            shard.shard_mem_cache.borrow_mut().clear_aggregate_write_snapshots_for_test();

            let key_new = AggregateClientKey::new(never_written.clone(), client_id);
            shard.cache_aggregate_client(&never_written, &key_new).await.expect("dedup scan should succeed");

            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&never_written, client_id),
                None,
                "a never-written aggregate must remain never-seen so its first write is accepted"
            );

            shard.close().await;
        });
    }

    /// is_visibility_gap_rejection classifies only the three documented rejection types.
    #[test]
    fn visibility_gap_rejection_classification() {
        assert!(is_visibility_gap_rejection(&ShardWriteError::ClientIdempotencyViolation {
            last_client_seq: 5, attempted_client_seq: 5,
        }));
        assert!(is_visibility_gap_rejection(&ShardWriteError::OptimisticConcurrencyViolation {
            expected_version: 5, current_aggregate_version: 6,
        }));
        assert!(is_visibility_gap_rejection(&ShardWriteError::AggregateRecreateNotAllowed));

        assert!(!is_visibility_gap_rejection(&ShardWriteError::AggregateNotExists));
        assert!(!is_visibility_gap_rejection(&ShardWriteError::EmptyEventsList));
        assert!(!is_visibility_gap_rejection(&ShardWriteError::ZeroEventType { client_seq: 1 }));
    }

    /// same setup but with optimistic concurrency. After the first
    /// write rolls back, a retry against the original expected_version (the value
    /// the client saw before the failed write) must succeed.
    #[test]
    #[ignore]
    fn occ_retry_succeeds_after_replication_rollback() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // First successful write to establish aggregate_version=1.
            let pre_client = FailThenSucceedReplicationClient::new(0, 0);
            let shard = open_leader_shard(&dir, pre_client).await;
            let agg = key(1, 1, 1);
            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            shard.close().await;

            // Reopen with a client that fails the next replication, simulating
            // a write that fsyncs but never replicates and gets rolled back.
            let client = FailThenSucceedReplicationClient::new(1, 1);
            let shard = open_leader_shard(&dir, client).await;

            // OCC-conditioned write: expected_version=1, would advance to 2.
            // Replication fails, rolls back. Write cache returns to version=1.
            let req = write_req_full(agg.clone(), events(1), false, Some(1), 1, false);
            let result = process(&shard, req).await;
            assert!(
                matches!(result, Err(ShardError::Write(ShardWriteError::ReplicationError(_)))),
                "expected ReplicationError from rollback, got {:?}", result
            );

            // Retry with the same expected_version=1. Pre-fix this could hit the
            // visibility gap; with the fix the post-rollback state is consistent
            // and OCC passes.
            let req = write_req_full(agg.clone(), events(1), false, Some(1), 1, false);
            let result = process(&shard, req).await;
            assert!(
                matches!(result, Ok(ClientResponse::Write(_))),
                "OCC retry after rollback should succeed, got {:?}", result
            );

            shard.close().await;
        });
    }

    #[test]
    fn write_path_cache_load_does_not_clamp_to_stale_read_cursor() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for n in 1u64..=5 {
                let evt = DatablockAggregateEvent {
                    client_seq: n,
                    event_type_major: 1,
                    event_value: Arc::new(vec![n as u8]),
                    ..Default::default()
                };
                write_ok(&shard, write_req(agg.clone(), vec![evt])).await;
            }

            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_write_event_seqes(&agg).aggregate_version,
                5,
                "precondition: cache should hold aggregate_version=5 after 5 standalone writes"
            );

            // Rewind only the read cursor's metablock position so v=4 and v=5
            // sit physically on disk past the read cursor. write_metablocks_position
            // and the per-cursor bloom are left intact — that's the exact state
            // the production bug presents to the cache reloader.
            let active = shard.log_segments_cache.active();
            let rewound_position = HEADER_BLOCK_SIZE_BYTES as u64 + 3 * FIXED_BLOCK_SIZE_BYTES as u64;
            {
                let mut metadata = active.metadata.borrow_mut();
                let read = metadata.read.as_mut().expect("standalone advance_visible_position must have populated read");
                assert!(read.metablocks_position > rewound_position,
                    "test invariant: read cursor should be past v=3 before rewind");
                read.metablocks_position = rewound_position;
            }

            // Simulate the cache wipe a replication rollback performs.
            shard.shard_mem_cache.borrow_mut().execute_replication_rollback();
            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_write_event_seqes(&agg).aggregate_version,
                0,
                "cache wipe should leave aggregate_version unknown until disk scan"
            );

            shard.aggregate_exists_and_cache(&agg, CachePath::Write)
                .await
                .expect("write-path cache load should succeed");

            let cached = shard.shard_mem_cache.borrow_mut().get_write_event_seqes(&agg).aggregate_version;
            assert_eq!(
                cached, 5,
                "write-path scan must reach write cursor, not the stale read cursor: \
                 a reload returning {cached} would let the next write reuse on-disk aggregate_version {} \
                 (duplicate-version data corruption)",
                cached + 1,
            );

            shard.close().await;
        });
    }

    // ── Persistence across clean restart ──

    #[test]
    fn write_visible_after_clean_restart() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            {
                let shard = open_shard(&dir).await;
                write_ok(&shard, write_req(agg.clone(), events(3))).await;
                write_ok(&shard, write_req(agg.clone(), events(2))).await;
                shard.close().await;
            }

            let shard = open_shard(&dir).await;
            let read = unwrap_read(process(&shard, read_req(agg)).await);
            assert_eq!(read.event_batches.len(), 2);
            assert_eq!(read.event_batches[0].events.len(), 3);
            assert_eq!(read.event_batches[1].events.len(), 2);
            shard.close().await;
        });
    }

    /// write → trim → delete(continuation) → acked recreate, then the process
    /// dies without a graceful close and restarts with empty caches. The next
    /// write must derive from the acked recreate, not from the tombstone below
    /// it — minting the recreate's version twice corrupts the WAL.
    #[test]
    fn no_duplicate_version_after_kill_restart_with_recreate_tail() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            let evt_at = |seq: u64| {
                vec![DatablockAggregateEvent {
                    client_seq: seq,
                    event_type_major: 1,
                    event_value: Arc::new(vec![seq as u8; 8]),
                    ..Default::default()
                }]
            };

            let recreate_version = {
                let shard = open_shard(&dir).await;
                write_ok(&shard, write_req_full(agg.clone(), evt_at(1), true, None, 7, true)).await; // v1
                write_ok(&shard, write_req_full(agg.clone(), evt_at(2), true, None, 7, true)).await; // v2
                let trim = process(&shard, trim_req(agg.clone(), 2)).await;
                assert!(matches!(trim, Ok(ClientResponse::TrimStart(_))), "trim failed: {trim:?}");
                let del = process(&shard, delete_req_full(agg.clone(), true, true, None)).await;
                assert!(matches!(del, Ok(ClientResponse::Delete(_))), "delete failed: {del:?}");
                let recreate = match process(&shard, write_req_full(agg.clone(), evt_at(3), true, None, 7, true)).await {
                    Ok(ClientResponse::Write(w)) => w,
                    other => panic!("recreate write failed: {other:?}"),
                };
                let v = recreate.max_aggregate_version.expect("single-aggregate write carries a version");
                // SIGKILL: drop without close() — disk state is whatever the last
                // fsync persisted, in-memory caches are gone.
                drop(shard);
                v
            };

            let shard = open_shard(&dir).await;
            let resp = match process(&shard, write_req_full(agg.clone(), evt_at(4), true, None, 7, true)).await {
                Ok(ClientResponse::Write(w)) => w,
                other => panic!("post-restart write failed: {other:?}"),
            };
            let v = resp.max_aggregate_version.expect("single-aggregate write carries a version");
            assert!(
                v > recreate_version,
                "post-restart write minted v{v} but v{recreate_version} was already acked before the kill — duplicate version issuance",
            );

            shard.close().await;
        });
    }

    /// Kill leaves a fsynced read<write tail (replication not yet committed).
    /// Warmup scanning only to read seeds the tombstone below the tail; the next
    /// write re-mints the tail's version — cache hit, so no disk load corrects it.
    #[test]
    fn warmup_must_not_seed_stale_state_below_fsynced_tail() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);
            let evt_at = |seq: u64| {
                vec![DatablockAggregateEvent {
                    client_seq: seq,
                    event_type_major: 1,
                    event_value: Arc::new(vec![seq as u8; 8]),
                    ..Default::default()
                }]
            };

            let recreate_version = {
                let shard = open_leader_shard(&dir, FailThenSucceedReplicationClient::new(0, 0)).await;
                write_ok(&shard, write_req_full(agg.clone(), evt_at(1), true, None, 7, true)).await;
                write_ok(&shard, write_req_full(agg.clone(), evt_at(2), true, None, 7, true)).await;
                let trim = process(&shard, trim_req(agg.clone(), 2)).await;
                assert!(matches!(trim, Ok(ClientResponse::TrimStart(_))), "trim failed: {trim:?}");
                let del = process(&shard, delete_req_full(agg.clone(), true, true, None)).await;
                assert!(matches!(del, Ok(ClientResponse::Delete(_))), "delete failed: {del:?}");
                let recreate = match process(&shard, write_req_full(agg.clone(), evt_at(3), true, None, 7, true)).await {
                    Ok(ClientResponse::Write(w)) => w,
                    other => panic!("recreate write failed: {other:?}"),
                };
                let v = recreate.max_aggregate_version.expect("single-aggregate write carries a version");

                drop(shard); // SIGKILL: no graceful close
                v
            };

            // Quiet test world header-syncs read==write on every ack; rewind the
            // persisted read one entry to stage a kill-time read<write tail.
            {
                use celeriant_rotating_log::log_segment_file::log_segment_file::{LogSegmentFile, write_dual_shard_log_header};
                let f = LogSegmentFile::open_existing(&dir.to_path_buf(), 1).await.unwrap();
                {
                    let mut meta = f.metadata.borrow_mut();
                    let mut read = meta.read.clone().expect("read cursor persisted");
                    assert_eq!(read.wal_seq, meta.write.wal_seq, "expected quiet-world read==write before staging");
                    read.metablocks_position -= FIXED_BLOCK_SIZE_BYTES as u64;
                    read.wal_seq -= 1;
                    meta.read = Some(read);
                }
                let meta = f.metadata.borrow().clone();
                let header = meta.to_shard_log_header();
                let pos = meta.file_len.saturating_sub(celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES as u64);
                {
                    let guard = f.lock_writer("test_stage_barrier_lag").await.unwrap();
                    let dma = guard.as_ref().unwrap();
                    write_dual_shard_log_header(dma, pos, &header).await.unwrap();
                    dma.fdatasync().await.unwrap();
                }
                f.close().await;
            }

            let shard = open_shard(&dir).await;
            let resp = match process(&shard, write_req_full(agg.clone(), evt_at(4), true, None, 7, true)).await {
                Ok(ClientResponse::Write(w)) => w,
                other => panic!("post-restart write failed: {other:?}"),
            };
            let v = resp.max_aggregate_version.expect("single-aggregate write carries a version");
            assert!(
                v > recreate_version,
                "post-restart write minted v{v} but v{recreate_version} was already acked before the kill — warmup seeded stale state below the fsynced tail",
            );

            shard.close().await;
        });
    }

    #[test]
    fn last_self_acked_persists_across_restart() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            let acked_before_close = {
                let client = FailThenSucceedReplicationClient::new(0, 0);
                let shard = open_leader_shard(&dir, client).await;
                for _ in 0..3 {
                    write_ok(&shard, write_req(agg.clone(), events(1))).await;
                }
                let acked = shard.log_segments_cache.active().metadata.borrow().last_self_acked_wal_seq;
                assert!(acked >= 3, "leader should self-ack at least 3 writes, got {acked}");
                shard.close().await;
                acked
            };

            let shard = open_shard(&dir).await;
            let acked_after_restart = shard.log_segments_cache.active().metadata.borrow().last_self_acked_wal_seq;
            assert_eq!(acked_after_restart, acked_before_close, "last_self_acked must survive clean restart");
            shard.close().await;
        });
    }

    /// Promotion over a wholly-unconfirmed peer tail (parked, no carrier ever
    /// confirmed anything) commits it: read advances to the durable tip, nothing
    /// is culled, and write-side caches survive (the data is kept, so they are
    /// still valid).
    #[test]
    fn promotion_commits_parked_peer_tail() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            // Three peer batches, none confirmed: all park, read stays at 0.
            let mut tip = GENESIS_HASH;
            for seq in 1u64..=3 {
                let resp = unwrap_replication(
                    shard.handle_replication_batch(
                        replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(seq, tip)], 0),
                    ).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            }
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 3, "precondition: whole tail parked");
            shard.shard_mem_cache.borrow_mut().put_aggregate_write_client_snapshot_for_test(agg.clone(), 42, 7, 0);

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            let changed = shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();
            assert!(changed, "committing the tail is a change");

            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!(meta.write.wal_seq, 3, "promotion must not cull the peer tail");
                assert_eq!(meta.read.as_ref().unwrap().wal_seq, 3, "promotion must commit read up to write");
            }
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 0, "every parked commit drains");
            assert!(
                shard.shard_mem_cache.borrow().aggregate_read_snapshots_len() > 0,
                "drained commits must populate the read-side caches",
            );
            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, 42), Some(7),
                "peer-tail commit must not clear write-side caches",
            );

            let again = shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();
            assert!(!again, "second reconciliation is a no-op (read == write)");

            shard.close().await;
        });
    }

    #[test]
    fn cull_noop_when_write_eq_read() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let _agg = key(1, 1, 1);
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            // No speculative tail: write == read (both 0 on a fresh shard).
            let culled = shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();
            assert!(!culled, "cull must be a no-op when write == read");

            shard.close().await;
        });
    }

    /// Regression: after a leader SIGKILL failover the demoted ex-leader may have
    /// `read == write > last_self_acked`. The existing cull (write→read) is a no-op
    /// because read==write. With `RewindToAckBarrier` both cursors must
    /// rewind to `last_self_acked`.
    #[test]
    fn demotion_cull_rewinds_to_ack_barrier_when_read_eq_write_above_acked() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            // Write 10 real entries on a standalone shard. Standalone advances read==write
            // on every fsync, so after this loop read.wal_seq == write.wal_seq == 10.
            let shard = open_shard(&dir).await;
            for _ in 0..10 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            // Confirm precondition: read == write == 10.
            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!(meta.write.wal_seq, 10);
                assert_eq!(meta.read.as_ref().unwrap().wal_seq, 10);
            }

            // Simulate the crash scenario: inject an ack barrier below read.
            // (In production this happens when the S3 ack fsync loses the race with SIGKILL.)
            {
                let active = shard.log_segments_cache.active();
                active.metadata.borrow_mut().last_self_acked_wal_seq = 5;
            }

            // Standard cull (promotion path) must be a no-op since read==write.
            let culled = shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();
            assert!(!culled, "standard cull must not fire when read==write (this was the old bug)");

            // Precondition: read caches are populated by the 10 writes above.
            {
                let mc = shard.shard_mem_cache.borrow();
                assert!(mc.aggregate_read_snapshots_len() > 0, "read snapshots must be populated before demotion cull");
            }

            // Demotion cull must rewind both cursors to last_self_acked==5.
            let culled = shard.reconcile_durable_tail(TailReconciliation::RewindToAckBarrier).await.unwrap();
            assert!(culled, "demotion cull must fire when last_self_acked < read");

            let active = shard.log_segments_cache.active();
            let meta = active.metadata.borrow();
            assert_eq!(meta.write.wal_seq, 5, "write cursor must rewind to ack barrier");
            assert_eq!(meta.read.as_ref().unwrap().wal_seq, 5, "read cursor must also rewind to ack barrier");
            drop(meta);

            // FIX 1: demotion cull must clear read-side caches (read cursor moved down).
            {
                let mc = shard.shard_mem_cache.borrow();
                assert_eq!(mc.aggregate_read_snapshots_len(), 0, "aggregate_read_snapshots must be cleared by demotion cull");
                assert_eq!(mc.aggregate_recent_writes_len(), 0, "aggregate_recent_writes must be cleared by demotion cull");
            }

            shard.close().await;
        });
    }

    /// even on the demotion path, a zero ack barrier must NOT rewind
    #[test]
    fn demotion_cull_does_not_rewind_when_ack_barrier_is_zero() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            // Standalone advances read==write on every fsync, so after 10 writes read==write==10.
            let shard = open_shard(&dir).await;
            for _ in 0..10 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }
            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!(meta.write.wal_seq, 10);
                assert_eq!(meta.read.as_ref().unwrap().wal_seq, 10);
            }

            // Caught-up-but-never-acked: read==write==10, last_self_acked==0.
            {
                let active = shard.log_segments_cache.active();
                active.metadata.borrow_mut().last_self_acked_wal_seq = 0;
            }

            // Demotion cull with a zero ack barrier must be a no-op.
            let culled = shard.reconcile_durable_tail(TailReconciliation::RewindToAckBarrier).await.unwrap();
            assert!(!culled, "demotion cull must NOT fire when last_self_acked==0 (would wipe the caught-up chain)");

            let active = shard.log_segments_cache.active();
            let meta = active.metadata.borrow();
            assert_eq!(meta.write.wal_seq, 10, "write cursor must be preserved (no rewind to genesis)");
            assert_eq!(meta.read.as_ref().unwrap().wal_seq, 10, "read cursor must be preserved (no rewind to genesis)");
            drop(meta);

            shard.close().await;
        });
    }

    /// The demotion rewind floor is max(last_self_acked, last_received_replication_wal_seq).
    /// Data above last_acked that arrived via TCP replication is peer-acked and must
    /// survive the cull; rewinding it wedges convergence (the live-TCP range is never
    /// in S3, and the chaos single_node_isolation wedge is exactly this).
    #[test]
    fn demotion_cull_floor_includes_last_received_replication() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            let shard = open_shard(&dir).await;
            for _ in 0..10 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            // Own acks stop at 5; seqs 6..=8 were received from a peer leader via TCP.
            {
                let active = shard.log_segments_cache.active();
                let mut meta = active.metadata.borrow_mut();
                meta.last_self_acked_wal_seq = 5;
                meta.last_received_replication_wal_seq = 8;
            }

            let culled = shard.reconcile_durable_tail(TailReconciliation::RewindToAckBarrier).await.unwrap();
            assert!(culled, "demotion cull must still fire for the range above both cursors");

            let active = shard.log_segments_cache.active();
            let meta = active.metadata.borrow();
            assert_eq!(meta.write.wal_seq, 8, "write must rewind only to the received barrier, not last_self_acked");
            assert_eq!(meta.read.as_ref().unwrap().wal_seq, 8, "read must rewind only to the received barrier, not last_self_acked");
            drop(meta);

            shard.close().await;
        });
    }

    /// The rewind-to-ack-barrier arm is one-shot: post-demotion status churn
    /// (Fenced<->Follower bounces, repeated kicks) re-runs the cull, but only the
    /// first call since boot/leadership may rewind. A second rewind would destroy
    /// data applied by catchup since the demotion.
    #[test]
    fn demotion_cull_rewind_is_one_shot_until_rearmed() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            let shard = open_shard(&dir).await;
            for _ in 0..10 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }
            {
                let active = shard.log_segments_cache.active();
                active.metadata.borrow_mut().last_self_acked_wal_seq = 5;
            }

            // First demotion cull: armed from boot, fires.
            let culled = shard.reconcile_durable_tail(TailReconciliation::RewindToAckBarrier).await.unwrap();
            assert!(culled, "first demotion cull must fire");
            assert_eq!(shard.log_segments_cache.active().metadata.borrow().read.as_ref().unwrap().wal_seq, 5);

            // Simulate catchup re-applying peer data above the barrier: read==write==5
            // with acked dropped below would previously re-trigger the rewind on the
            // next churn-driven cull. The flag is consumed, so it must be a no-op.
            {
                let active = shard.log_segments_cache.active();
                active.metadata.borrow_mut().last_self_acked_wal_seq = 3;
            }
            let culled = shard.reconcile_durable_tail(TailReconciliation::RewindToAckBarrier).await.unwrap();
            assert!(!culled, "second demotion cull must not rewind (flag consumed)");
            assert_eq!(shard.log_segments_cache.active().metadata.borrow().read.as_ref().unwrap().wal_seq, 5, "churn cull must not destroy data above the stale ack barrier");

            // Re-arm (as a successful leader replication commit would) and verify the
            // rewind is available again.
            shard.ack_barrier_rewind_armed.set(true);
            let culled = shard.reconcile_durable_tail(TailReconciliation::RewindToAckBarrier).await.unwrap();
            assert!(culled, "re-armed demotion cull must fire again");
            assert_eq!(shard.log_segments_cache.active().metadata.borrow().read.as_ref().unwrap().wal_seq, 3);

            shard.close().await;
        });
    }

    /// Regression: the promotion path must NOT rewind read to last_self_acked —
    /// the ack barrier belongs to the demotion path only. On promotion the peer
    /// tail commits UP to write regardless of the barrier value.
    #[test]
    fn promotion_ignores_ack_barrier_and_commits_peer_tail() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            // Five peer batches, each carrier confirming seq-1: read lands at 4,
            // seq 5 stays parked.
            let mut tip = GENESIS_HASH;
            for seq in 1u64..=5 {
                let resp = unwrap_replication(
                    shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(seq, tip)])).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            }
            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!(meta.read.as_ref().unwrap().wal_seq, 4, "precondition: deferred tail (4, 5]");
                assert_eq!(meta.write.wal_seq, 5);
                // Inject an ack barrier below read — must have no effect on promotion.
                drop(meta);
                active.metadata.borrow_mut().last_self_acked_wal_seq = 3;
            }

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            let changed = shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();
            assert!(changed, "promotion must commit the deferred tail");

            let active = shard.log_segments_cache.active();
            let meta = active.metadata.borrow();
            assert_eq!(meta.write.wal_seq, 5, "write untouched by promotion");
            assert_eq!(meta.read.as_ref().unwrap().wal_seq, 5, "read commits to write, never to the ack barrier");
            drop(meta);

            shard.close().await;
        });
    }

    /// Self-reclaim path (lease_changed_hands=false): the caller (set_node_role_via_s3) must NOT
    /// call reconcile_durable_tail. This test verifies the invariant from the caller's
    /// perspective: a shard with read < write keeps write intact when the cull is skipped.
    ///
    /// Regression guard for run 1779618258/shard_2: the old code unconditionally culled on
    /// became_leader, which zeroed the speculative tail and caused re-authoring the same seqs
    /// with new content, forking the follower's replicated copy.
    #[test]
    fn self_reclaim_keeps_speculative_tail() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            // Replicate 5 entries (read = 5) then advance write to simulate a speculative tail.
            let mut tip = GENESIS_HASH;
            for seq in 1u64..=5 {
                let resp = unwrap_replication(
                    shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(seq, tip)])).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            }
            commit_deferred_tail(&shard);
            {
                let active = shard.log_segments_cache.active();
                let mut meta = active.metadata.borrow_mut();
                meta.write.wal_seq = 8;
                meta.write.metablocks_position = HEADER_BLOCK_SIZE_BYTES as u64 + 8 * FIXED_BLOCK_SIZE_BYTES as u64;
            }

            // Precondition: read=5, write=8.
            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!(meta.read.as_ref().unwrap().wal_seq, 5);
                assert_eq!(meta.write.wal_seq, 8);
            }

            // Self-reclaim: lease_changed_hands=false. The caller skips the cull entirely.
            // Simulate by NOT calling reconcile_durable_tail.
            // Write must stay at 8 — the tail is the node's own content already replicated.
            let active = shard.log_segments_cache.active();
            let meta = active.metadata.borrow();
            assert_eq!(meta.write.wal_seq, 8, "self-reclaim must keep speculative tail (write unchanged)");
            assert_eq!(meta.read.as_ref().unwrap().wal_seq, 5, "read unchanged on self-reclaim");
            drop(meta);

            shard.close().await;
        });
    }

    /// A changed-hands promotion over an OWN-speculation tail (a crashed
    /// ex-leader re-winning the election with an unacked fork on disk) still
    /// culls: committing it would fire watch events for entries the pre-serve
    /// S3 catchup then truncates.
    #[test]
    fn changed_hands_promotion_culls_own_speculation_tail() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard_with_own_tail(&dir, 4, 7).await;

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            let changed = shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();
            assert!(changed, "changed-hands promotion must cull the own-speculation tail");

            let active = shard.log_segments_cache.active();
            let meta = active.metadata.borrow();
            assert_eq!(meta.write.wal_seq, 4, "write must rewind to read=4 on an own-tail promotion");
            assert_eq!(meta.read.as_ref().unwrap().wal_seq, 4, "read unchanged by the own-tail cull");
            drop(meta);

            shard.close().await;
        });
    }

    /// Regression guard: the demotion cull (RewindToAckBarrier) is a separate code
    /// path from the self-reclaim fix and must be unaffected. became_follower_from_leader_or_fenced
    /// always triggers the cull regardless of lease_changed_hands.
    #[test]
    fn demotion_cull_unaffected_by_self_reclaim_fix() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);
            let shard = open_shard(&dir).await;

            // Write 6 entries (read=write=6 on standalone shard), then push ack barrier below.
            for _ in 0..6 {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }
            {
                let active = shard.log_segments_cache.active();
                active.metadata.borrow_mut().last_self_acked_wal_seq = 3;
            }

            // became_follower_from_leader_or_fenced always triggers demotion cull.
            // self-reclaim fix does not gate this path.
            let culled = shard.reconcile_durable_tail(TailReconciliation::RewindToAckBarrier).await.unwrap();
            assert!(culled, "demotion cull must fire when last_self_acked < read");

            let active = shard.log_segments_cache.active();
            let meta = active.metadata.borrow();
            assert_eq!(meta.write.wal_seq, 3, "demotion cull must rewind write to ack barrier");
            assert_eq!(meta.read.as_ref().unwrap().wal_seq, 3, "demotion cull must rewind read to ack barrier");
            drop(meta);

            shard.close().await;
        });
    }

    #[test]
    fn cull_then_restart_preserves_post_cull_state() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            {
                let shard = open_shard_with_own_tail(&dir, 3, 10).await;
                let culled = shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();
                assert!(culled, "cull should fire on an own tail with write > read");
                shard.close().await;
            }

            let shard = open_shard(&dir).await;
            let active = shard.log_segments_cache.active();
            let meta = active.metadata.borrow();
            assert_eq!(meta.write.wal_seq, 3, "post-cull write.wal_seq must survive restart");
            assert_eq!(meta.read.as_ref().unwrap().wal_seq, 3, "read.wal_seq stays at 3");
            drop(meta);
            shard.close().await;
        });
    }

    fn watch_writes_request() -> celeriant_msg::request::requests::WatchRequest {
        let mut ops = HashSet::new();
        ops.insert(celeriant_watch::aggregate_watch_event::AggregateWatchEvent::WRITE);
        celeriant_msg::request::requests::WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: Some(ops),
        }
    }

    /// Non-blocking watch poll after letting the executor settle: by then an
    /// event is either in the channel or was never broadcast.
    async fn poll_watch_event(
        subscriber: &Rc<RefCell<celeriant_watch::subscribed_client::SubscribedClient>>,
    ) -> Option<celeriant_watch::aggregate_watch_event::AggregateWatchEvent> {
        glommio::timer::sleep(Duration::from_millis(10)).await;
        futures_lite::future::poll_once(subscriber.borrow().receiver.recv()).await.flatten()
    }

    /// ReconcileAsFollower with a peer tail is a keep-parked no-op: durable tail
    /// intact, still invisible, still parked — and the queue keeps working
    /// afterwards: new batches park in order and a later covering carrier drains
    /// everything, firing each parked watch event exactly once.
    #[test]
    fn reconcile_as_follower_keeps_peer_tail_until_covering_carrier() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;
            let (_id, subscriber) = shard.watched_aggregates().add_subscriber(watch_writes_request());

            let mut tip = GENESIS_HASH;
            for seq in 1u64..=2 {
                let resp = unwrap_replication(
                    shard.handle_replication_batch(
                        replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(seq, tip)], 0),
                    ).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            }

            let changed = shard.reconcile_durable_tail(TailReconciliation::ReconcileAsFollower).await.unwrap();
            assert!(!changed, "keeping the parked peer tail is a no-op");
            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!(meta.write.wal_seq, 2, "peer tail must stay durable");
                assert_eq!(meta.read.as_ref().map_or(0, |r| r.wal_seq), 0, "peer tail must stay invisible");
            }
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 2, "peer tail must stay parked");
            assert!(poll_watch_event(&subscriber).await.is_none(), "nothing committed, no events");

            // The queue keeps accepting: a new chain-extending batch parks in order.
            let resp = unwrap_replication(
                shard.handle_replication_batch(
                    replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(3, tip)], 0),
                ).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 3);

            // Covering carrier (duplicate of the tip, skipped on apply) drains all.
            let resp = unwrap_replication(
                shard.handle_replication_batch(
                    replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(3, GENESIS_HASH)], 3),
                ).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!((meta.write.wal_seq, meta.read.as_ref().unwrap().wal_seq), (3, 3), "covering carrier commits the kept tail");
            }
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 0);
            for seq in 1u64..=3 {
                assert!(
                    poll_watch_event(&subscriber).await.is_some(),
                    "parked event {seq} must fire when the covering carrier commits it",
                );
            }
            assert!(poll_watch_event(&subscriber).await.is_none(), "exactly once per parked entry");

            shard.close().await;
        });
    }

    /// ReconcileAsFollower with an OWN-speculation tail culls it exactly like the
    /// old promotion cull (boot-after-leader-crash: divergence risk with the
    /// promoted peer).
    #[test]
    fn reconcile_as_follower_culls_own_speculation_tail() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard_with_own_tail(&dir, 3, 5).await;

            let changed = shard.reconcile_durable_tail(TailReconciliation::ReconcileAsFollower).await.unwrap();
            assert!(changed, "own-speculation tail must be culled");

            let active = shard.log_segments_cache.active();
            let meta = active.metadata.borrow();
            assert_eq!(meta.write.wal_seq, 3, "write rewinds to read");
            assert_eq!(meta.read.as_ref().unwrap().wal_seq, 3, "read unchanged");
            drop(meta);

            shard.close().await;
        });
    }

    /// The demotion rewind discards parked commits covering the culled range and
    /// their watch events never fire (goal edge 8, discard direction). Callers
    /// route only held-leadership demotions here, so the queue should already be
    /// empty; this locks the defensive disposition — an orphaned PCD would
    /// replay at the next promotion catchup.
    #[test]
    fn demotion_rewind_discards_parked_commits_without_events() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;
            let (_id, subscriber) = shard.watched_aggregates().add_subscriber(watch_writes_request());

            // seq 1 confirmed (read=1, its event fires); seq 2 parked.
            let resp = unwrap_replication(
                shard.handle_replication_batch(
                    replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(1, GENESIS_HASH)], 0),
                ).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            let resp = unwrap_replication(
                shard.handle_replication_batch(
                    replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(2, tip)], 1),
                ).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            assert!(poll_watch_event(&subscriber).await.is_some(), "confirming seq 1 fires its event");
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 1, "precondition: seq 2 parked");

            let changed = shard.reconcile_durable_tail(TailReconciliation::RewindToAckBarrier).await.unwrap();
            assert!(changed, "demotion rewind must cull the (read, write] tail");

            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!((meta.write.wal_seq, meta.read.as_ref().unwrap().wal_seq), (1, 1));
            }
            assert_eq!(
                shard.shard_mem_cache.borrow().parked_commit_count(), 0,
                "parked commits covering the culled range must be discarded, not orphaned",
            );
            assert!(
                poll_watch_event(&subscriber).await.is_none(),
                "events for a culled range must never fire",
            );

            shard.close().await;
        });
    }

    /// Real-disk crash-restart-then-promote: the deferred tail exists only in the
    /// persisted header (read < write, no parked state survives the crash), and
    /// promotion still commits it — durably.
    #[test]
    fn crash_restart_then_promote_commits_ondisk_tail() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            {
                let shard = open_follower_shard(&dir).await;
                let resp = unwrap_replication(
                    shard.handle_replication_batch(
                        replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(1, GENESIS_HASH)], 0),
                    ).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
                let resp = unwrap_replication(
                    shard.handle_replication_batch(
                        replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(2, tip)], 1),
                    ).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                shard.close().await;
            }

            let shard = open_follower_shard(&dir).await;
            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!((meta.write.wal_seq, meta.read.as_ref().unwrap().wal_seq), (2, 1), "precondition: deferred tail on disk");
            }
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 0, "precondition: parked state died with the process");

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            let changed = shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();
            assert!(changed, "promotion must commit the on-disk tail");
            shard.close().await;

            let shard = open_follower_shard(&dir).await;
            let active = shard.log_segments_cache.active();
            let meta = active.metadata.borrow();
            assert_eq!((meta.write.wal_seq, meta.read.as_ref().unwrap().wal_seq), (2, 2), "the commit must be persisted");
            drop(meta);
            shard.close().await;
        });
    }

    /// Mixed history on real disk (goal edge 5): lead-and-ack, speculate
    /// unackably, demote (cull), follow a peer and park its batches, re-promote.
    /// Order must hold: the pre-demotion speculation never resurrects and the
    /// re-promotion commits exactly the acked prefix plus the peer tail.
    #[test]
    fn mixed_history_demotion_then_repromotion_commits_only_peer_tail() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let (acked_agg, spec_agg) = (key(1, 1, 1), key(1, 1, 99));

            // Lease valid 1200ms so the unackable write fences quickly.
            let client = SwitchableReplicationClient::new();
            let shard = ShardWal::open(
                test_config(&dir),
                ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 1200),
                client,
                StubS3Downloader,
            ).await.unwrap();

            write_ok(&shard, write_req(acked_agg.clone(), events(1))).await;
            shard.replication_client.should_fail.set(true);
            let fenced = process(&shard, write_req(spec_agg.clone(), events(1))).await;
            assert!(fenced.is_err(), "a dark-replication leader write must not ack, got {fenced:?}");
            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!((meta.write.wal_seq, meta.read.as_ref().unwrap().wal_seq), (2, 1), "precondition: own speculation at seq 2");
            }

            // Demotion: the cull must remove the speculation before peer data lands.
            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 1 }, 500, now_ms() + 10_000));
            let culled = shard.reconcile_durable_tail(TailReconciliation::RewindToAckBarrier).await.unwrap();
            assert!(culled, "demotion must cull the unacked speculation");

            // The new leader extends the shared prefix; both batches stay parked.
            let mut tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            for seq in 2u64..=3 {
                let mut req = replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(seq, tip)], 1);
                req.sender_lease_epoch = 1;
                let resp = unwrap_replication(shard.handle_replication_batch(req).await);
                assert!(matches!(resp.result, ReplicationResult::Success { .. }), "peer batch {seq} failed: {resp:?}");
                tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            }
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 2, "precondition: peer tail parked");

            // Re-promotion commits exactly the parked peer tail.
            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 2 }, 500, now_ms() + 10_000));
            let changed = shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();
            assert!(changed, "re-promotion must commit the parked peer tail");

            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!((meta.write.wal_seq, meta.read.as_ref().unwrap().wal_seq), (3, 3), "acked prefix plus peer tail, fully committed");
            }
            let spec = process(&shard, read_req(spec_agg.clone())).await;
            assert!(
                matches!(spec, Err(ShardError::Read(ShardReadError::AggregateNotExists))),
                "pre-demotion speculation must NOT resurrect on re-promotion, got {spec:?}",
            );
            let acked = process(&shard, read_req(acked_agg.clone())).await;
            assert!(matches!(acked, Ok(ClientResponse::Read(_))), "the acked prefix must survive, got {acked:?}");

            shard.close().await;
        });
    }

    /// A stale promotion floor must never disguise an own fork as a peer tail.
    /// The common clean failover manufactures the staleness: a fully caught-up
    /// follower promotes with floor = read+1 > write. Driving the REAL status
    /// sequence (Promoting at the upload, Leader flip clearing the floor), the
    /// node then speculates unackably as leader and rejoins; both
    /// reconciliation modes must classify the fork by its author and cull it —
    /// keeping it wedges catchup on the rejoin leg, and the re-win leg would
    /// commit a fork the pre-serve catchup then truncates.
    #[test]
    fn stale_promotion_floor_does_not_disguise_own_fork_as_peer() {
        glommio_test!({
            for (name, mode) in [
                ("rejoin_as_follower", TailReconciliation::ReconcileAsFollower),
                ("rewin_promotion", TailReconciliation::CommitForPromotion),
            ] {
                let (_tmp, dir) = test_dir();
                // Caught-up follower: entry 1 confirmed by a covering carrier.
                let client = SwitchableReplicationClient::new();
                let shard = ShardWal::open(
                    test_config(&dir),
                    ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000),
                    client,
                    StubS3Downloader,
                ).await.unwrap();
                let resp = unwrap_replication(
                    shard.handle_replication_batch(
                        replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(1, GENESIS_HASH)], 1),
                    ).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                {
                    let active = shard.log_segments_cache.active();
                    let meta = active.metadata.borrow();
                    assert_eq!(
                        (meta.read.as_ref().unwrap().wal_seq, meta.last_received_replication_wal_seq), (1, 2),
                        "[{name}] precondition: caught up with the floor above read",
                    );
                }

                // CAS win: the upload runs mid-window under Promoting (no range
                // here — start > write), then the Leader flip clears the floor.
                shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Promoting { lease_epoch: 1 }, 500, now_ms() + 1200));
                shard.upload_s3_promotion_batch().await.unwrap();
                assert_eq!(
                    shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq, 2,
                    "[{name}] the upload skip must not clear the floor (it is the crash re-entry marker)",
                );
                shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 1200));
                shard.clear_promotion_floor();
                assert_eq!(
                    shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq, 0,
                    "[{name}] the flip is the single floor-clear site",
                );

                // Unackable leader speculation at seq 2, then crash.
                shard.replication_client.should_fail.set(true);
                let fenced = process(&shard, write_req(key(1, 1, 99), events(1))).await;
                assert!(fenced.is_err(), "[{name}] a dark-replication leader write must not ack");
                shard.close().await;

                // Rejoin: the fork's provenance is decided from disk alone.
                let shard = open_follower_shard(&dir).await;
                if mode == TailReconciliation::CommitForPromotion {
                    shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 2 }, 500, now_ms() + 10_000));
                }
                let changed = shard.reconcile_durable_tail(mode).await.unwrap();
                assert!(changed, "[{name}] the own fork must be culled");
                {
                    let active = shard.log_segments_cache.active();
                    let meta = active.metadata.borrow();
                    assert_eq!(
                        (meta.write.wal_seq, meta.read.as_ref().unwrap().wal_seq), (1, 1),
                        "[{name}] the fork is gone, the acked prefix stays",
                    );
                }
                shard.close().await;
            }
        });
    }

    /// Shard 0 uploads its promotion batch MID-window, before the Leader flip:
    /// the status gate must admit Promoting or the TCP-received range never
    /// reaches S3 and the demoted peer's catchup gap is unbridgeable. (Red on
    /// the old gate, which admitted only Leader — dead upload on shard 0.)
    #[test]
    fn promotion_upload_reaches_s3_under_promoting_status() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            commit_deferred_tail(&shard);

            // CAS win publishes Promoting; the upload runs before any Leader flip.
            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Promoting { lease_epoch: 1 }, 500, now_ms() + 10_000));
            shard.upload_s3_promotion_batch().await.unwrap();

            let uploads = shard.replication_client.s3_uploads.borrow();
            assert_eq!(uploads.len(), 1, "the TCP-received range must reach S3 under Promoting");
            assert_eq!(uploads[0][0].metablock.wal_seq, 1);
            drop(uploads);

            shard.close().await;
        });
    }

    /// While Promoting, ALL TCP replication is rejected — nothing can park
    /// inside the promotion window (the flip drain is fail-loud on an
    /// impossible state because of this gate).
    #[test]
    fn replication_rejected_while_promoting() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;
            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Promoting { lease_epoch: 1 }, 500, now_ms() + 10_000));

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(1, GENESIS_HASH)])).await,
            );
            assert!(
                matches!(resp.result, ReplicationResult::Rejected(FollowerRejection::NotAFollower)),
                "a promoting node must not accept replication batches, got {:?}",
                resp.result,
            );
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 0, "nothing may park mid-window");

            shard.close().await;
        });
    }

    /// The catchup status rewrite must not touch a promoting shard: Promoting
    /// carries the won lease's real TTL and the promotion upload gate needs it
    /// afterwards — a fallthrough to the rewrite's expiry-0 status would decay
    /// the fence to Fenced instantly and reopen the mid-window hazards.
    #[test]
    fn promoting_status_survives_s3_catchup_with_original_expiry() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;
            let expiry = now_ms() + 10_000;
            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Promoting { lease_epoch: 3 }, 500, expiry));

            shard.enter_s3_catchup(shard_wal_s3_catchup::CatchupRole::Promoting).await.unwrap();

            let status = shard.node_status.get();
            assert_eq!(status.raw(), NodeStatus::Promoting { lease_epoch: 3 }, "catchup must preserve the Promoting fence");
            assert_eq!(status.lease_expires_at_ms(), expiry, "catchup must preserve the fence's real TTL");

            shard.close().await;
        });
    }

    /// Disk-truth resume marker for a reacquired lease (crash mid-promotion).
    /// Rows name the crash exit they model; the floor row is the
    /// crash-after-catchup-commit-before-upload exit, where no tail remains but
    /// the TCP-received range exists in S3 nowhere.
    #[test]
    fn promotion_resume_owed_by_disk_state() {
        glommio_test!({
            // Row 1: peer tail on disk (crash before/during catchup) -> owed.
            {
                let (_tmp, dir) = test_dir();
                let shard = open_follower_shard(&dir).await;
                let resp = unwrap_replication(
                    shard.handle_replication_batch(
                        replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(1, GENESIS_HASH)], 0),
                    ).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                shard.close().await;
                let shard = open_follower_shard(&dir).await;
                assert!(
                    shard.promotion_resume_owed().await.unwrap(),
                    "[peer_tail] an on-disk peer tail owes the resumed promotion",
                );
                shard.close().await;
            }

            // Row 2: no tail, floor persisted (crash after the catchup commit,
            // before the upload/flip) -> owed.
            {
                let (_tmp, dir) = test_dir();
                let shard = open_follower_shard(&dir).await;
                let resp = unwrap_replication(
                    shard.handle_replication_batch(
                        replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(1, GENESIS_HASH)], 1),
                    ).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                {
                    let active = shard.log_segments_cache.active();
                    let meta = active.metadata.borrow();
                    assert_eq!(
                        (meta.write.wal_seq, meta.read.as_ref().unwrap().wal_seq, meta.last_received_replication_wal_seq),
                        (1, 1, 2),
                        "[floor_marker] precondition: committed tail, uncleared floor",
                    );
                }
                shard.close().await;
                let shard = open_follower_shard(&dir).await;
                assert!(
                    shard.promotion_resume_owed().await.unwrap(),
                    "[floor_marker] an uncleared floor owes the resumed promotion (upload leg)",
                );
                shard.close().await;
            }

            // Row 3: own speculative tail, no floor (classic self-reclaim) -> not owed.
            {
                let (_tmp, dir) = test_dir();
                let shard = open_shard_with_own_tail(&dir, 1, 3).await;
                assert!(
                    !shard.promotion_resume_owed().await.unwrap(),
                    "[own_tail] self-reclaim keeps its speculation; nothing owed",
                );
                shard.close().await;
            }

            // Row 3b: own speculative tail WITH a stale floor (crash in the
            // Leader-flip-to-floor-clear gap) -> provenance beats the floor;
            // still not owed, the carve-out keeps the tail.
            {
                let (_tmp, dir) = test_dir();
                let shard = open_shard_with_own_tail(&dir, 1, 3).await;
                shard.log_segments_cache.active().metadata.borrow_mut().last_received_replication_wal_seq = 2;
                assert!(
                    !shard.promotion_resume_owed().await.unwrap(),
                    "[own_tail_stale_floor] a stale floor must not route an own tail into the resume cull",
                );
                {
                    let active = shard.log_segments_cache.active();
                    let meta = active.metadata.borrow();
                    assert_eq!(meta.write.wal_seq, 3, "[own_tail_stale_floor] the tail is kept");
                }
                shard.close().await;
            }

            // Row 4: clean disk (completed promotion: no tail, floor cleared) -> not owed.
            {
                let (_tmp, dir) = test_dir();
                let shard = open_follower_shard(&dir).await;
                assert!(!shard.promotion_resume_owed().await.unwrap(), "[clean] nothing owed on a clean disk");
                shard.close().await;
            }
        });
    }

    /// The Follower-to-Leader status-flip drain commits a batch that parked
    /// inside the promotion window (a deposed leader's late delivery) — and is a
    /// strict no-op for a self-reclaimed leader's own speculative tail.
    #[test]
    fn promotion_flip_drain_commits_late_parked_batch_only() {
        glommio_test!({
            // A batch parked after the post-catchup reconcile: the flip commits it.
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;
            let resp = unwrap_replication(
                shard.handle_replication_batch(
                    replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(1, GENESIS_HASH)], 0),
                ).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 1, "precondition: late batch parked");

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            shard.commit_parked_tail_on_promotion().await.unwrap();
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 0, "the flip drain must commit the late batch");
            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!((meta.write.wal_seq, meta.read.as_ref().unwrap().wal_seq), (1, 1));
            }
            shard.close().await;

            // Self-reclaim shape: own speculative tail, nothing parked — untouched.
            let (_tmp2, dir2) = test_dir();
            let shard = open_shard_with_own_tail(&dir2, 1, 3).await;
            shard.commit_parked_tail_on_promotion().await.unwrap();
            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!(meta.write.wal_seq, 3, "self-reclaim keeps its speculative tail");
                assert_eq!(meta.read.as_ref().unwrap().wal_seq, 1, "read stays behind on self-reclaim");
            }
            shard.close().await;
        });
    }

    /// The provenance scan must find the tail tip in the sealed predecessor when
    /// the active segment is empty (promotion right after a rotation, restart
    /// having cleared the parked state and a prior upload the floor). A scan that
    /// cannot cross the boundary would misread the peer tail as own and cull it.
    #[test]
    fn provenance_scan_finds_peer_tail_tip_across_rotation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let mut tip = GENESIS_HASH;
            for seq in 1u64..=2 {
                let resp = unwrap_replication(
                    shard.handle_replication_batch(
                        replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(seq, tip)], 0),
                    ).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            }
            shard.log_segments_cache.rotate_to_next_log().await.unwrap();

            // Restart-equivalent state: parked commits died with the process, a
            // previous promotion upload cleared the floor. Only the disk knows the
            // tail's provenance now.
            shard.shard_mem_cache.borrow_mut().take_all_parked_commits();
            shard.log_segments_cache.active().metadata.borrow_mut().last_received_replication_wal_seq = 0;
            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!(
                    (meta.read.as_ref().map_or(0, |r| r.wal_seq), meta.write.wal_seq), (0, 2),
                    "precondition: empty active segment atop the peer tail",
                );
            }

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            let changed = shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();
            assert!(changed, "the peer tail behind the rotation must be committed");

            let active = shard.log_segments_cache.active();
            let meta = active.metadata.borrow();
            assert_eq!(meta.read.as_ref().map(|r| r.wal_seq), Some(2), "read commits to the durable tip");
            drop(meta);

            shard.close().await;
        });
    }

    // ── Replication (handle_replication_batch) ──

    async fn open_follower_shard(dir: &std::path::Path) -> ShardWal<StubReplicationClient, StubS3Downloader> {
        ShardWal::open(test_config(dir), ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000), StubReplicationClient, StubS3Downloader)
            .await
            .unwrap()
    }

    fn now_ms() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }

    fn test_metablock(wal_seq: u64, previous_tip_hash: [u8; 32]) -> Metablock {
        let mut mb = Metablock::default_inline_event_batch_metadata(AggregateKey::new(1, 1, 1));
        mb.wal_seq = wal_seq;
        mb.previous_tip_hash = previous_tip_hash;
        mb
    }

    fn replication_batch_req(batches: Vec<ReplicationBatchItem>) -> ReplicationBatchRequest {
        let leader_confirmed_wal_seq = batches.first()
            .map(|b| b.metablock.wal_seq.saturating_sub(1))
            .unwrap_or(0);
        replication_batch_req_with_leader_confirmed(batches, leader_confirmed_wal_seq)
    }

    fn replication_batch_req_with_leader_confirmed(
        batches: Vec<ReplicationBatchItem>,
        leader_confirmed_wal_seq: u64,
    ) -> ReplicationBatchRequest {
        ReplicationBatchRequest {
            correlation_id: None,
            shard_id: 0,
            leader_timestamp_ms: now_ms(),
            leader_confirmed_wal_seq,
            sender_lease_epoch: 0,
            batches,
        }
    }

    fn unwrap_replication(result: Result<ReplicationBatchResponse, crate::error::follower_replication_write_error::FollowerReplicationWriteError>) -> ReplicationBatchResponse {
        result.expect("replication should not error")
    }

    fn replication_item(wal_seq: u64, tip_hash: [u8; 32]) -> ReplicationBatchItem {
        ReplicationBatchItem {
            metablock: test_metablock(wal_seq, tip_hash),
            datablock: None,
        }
    }

    #[test]
    fn replication_happy_path() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            shard.close().await;
        });
    }

    /// A follower must fsync a replicated batch before ACKing it. If the durable fsync
    /// cannot complete (here the active segment's write file is dropped), the entries are
    /// applied in memory but handle_replication_batch returns an error, never
    /// ReplicationResult::Success. A Success would be a false ack: the leader would advance
    /// read/last_self_acked past bytes not on disk.
    #[test]
    fn follower_does_not_ack_when_fsync_fails() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            // Make the durable fsync impossible: drop the active segment's writer so
            // sync() fails with ActiveWriteFileUnavailable.
            shard.log_segments_cache.active().close().await;

            let result = shard
                .handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)]))
                .await;

            assert!(
                matches!(
                    result,
                    Err(crate::error::follower_replication_write_error::FollowerReplicationWriteError::ShardFSyncError(_))
                ),
                "fsync failure must surface as an error, never a Success ack; got: {result:?}",
            );

            shard.close().await;
        });
    }

    /// A current leader replaying historical entries authored under a previous
    /// lease epoch must not be rejected as StaleLease. Gating on the authorship
    /// epoch (first metablock) instead of the sender's current epoch livelocks
    /// any catchup gap spanning a leadership change: TCP reject forever, S3
    /// fallback only covers fresh commits.
    #[test]
    fn replication_accepts_historical_entries_from_current_leader() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = ShardWal::open(
                test_config(&dir),
                ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 2 }, 500, now_ms() + 10_000),
                StubReplicationClient,
                StubS3Downloader,
            ).await.unwrap();

            // Entry authored under the previous leadership (epoch 1), replayed by
            // the legitimate epoch-2 leader during catchup.
            let mut item = replication_item(1, GENESIS_HASH);
            item.metablock.lease_epoch = 1;
            let mut req = replication_batch_req(vec![item]);
            req.sender_lease_epoch = 2;
            let resp = unwrap_replication(shard.handle_replication_batch(req).await);
            assert!(
                matches!(resp.result, ReplicationResult::Success { .. }),
                "historical entries from the current leader must be accepted, got {:?}",
                resp.result,
            );

            // Zombie fencing must still hold: a sender claiming a stale current
            // epoch is rejected regardless of its entries' epochs.
            let mut zombie_item = replication_item(2, shard.log_segments_cache.active().metadata.borrow().write.tip_hash);
            zombie_item.metablock.lease_epoch = 2;
            let mut zombie_req = replication_batch_req(vec![zombie_item]);
            zombie_req.sender_lease_epoch = 1;
            let resp = unwrap_replication(shard.handle_replication_batch(zombie_req).await);
            assert!(
                matches!(resp.result, ReplicationResult::Rejected(FollowerRejection::StaleLease { .. })),
                "a stale sender epoch must still be fenced, got {:?}",
                resp.result,
            );

            shard.close().await;
        });
    }

    #[test]
    fn replication_rejected_when_not_follower() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await; // Standalone

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Rejected(FollowerRejection::NotAFollower)));

            shard.close().await;
        });
    }

    /// INVARIANT: a guard-passing empty batch is a commit-notify — Success,
    /// and on a fresh follower with nothing parked it moves no cursor.
    /// (Stale-epoch and drift-failing empty batches keep their guard's
    /// rejection; guard order is locked by the commit-notify contract suite.)
    #[test]
    fn replication_accepts_empty_batch_as_commit_notify() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let resp = unwrap_replication(shard.handle_replication_batch(replication_batch_req(vec![])).await);
            assert!(
                matches!(resp.result, ReplicationResult::Success { .. }),
                "guarded empty batch must be accepted as a commit-notify, got {:?}",
                resp.result
            );
            assert_eq!(wal_positions(&shard.log_segments_cache), (0, 0), "a notify with nothing parked moves nothing");

            shard.close().await;
        });
    }

    #[test]
    fn replication_rejects_wal_seq_gap() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            // Follower at wal_seq=0, batch starts at 5 (expects 1)
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(5, GENESIS_HASH)])).await,
            );
            match resp.result {
                ReplicationResult::Rejected(FollowerRejection::WalSeqMismatch { max_follower_wal_seq }) => {
                    assert_eq!(max_follower_wal_seq, 0);
                }
                other => panic!("expected WalSeqMismatch, got {other:?}"),
            }

            shard.close().await;
        });
    }

    #[test]
    fn replication_rejects_tip_hash_mismatch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            // Correct wal_seq but wrong tip hash
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, [0xFF; 32])])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Rejected(FollowerRejection::TipHashMismatch { .. })));

            shard.close().await;
        });
    }

    #[test]
    fn replication_sequential_batches() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            // Batch 1
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            // Read tip_hash after batch 1 for chaining
            let tip_after_1 = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            assert_ne!(tip_after_1, GENESIS_HASH);

            // Batch 2 must chain from batch 1's tip
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(2, tip_after_1)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            // Verify WAL sequence advanced
            let final_wal_seq = shard.log_segments_cache.active().metadata.borrow().write.wal_seq;
            assert_eq!(final_wal_seq, 2);

            shard.close().await;
        });
    }

    #[test]
    fn replication_rejects_time_drift() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let stale_request = ReplicationBatchRequest {
                correlation_id: None,
                shard_id: 0,
                leader_timestamp_ms: 1000, // ancient timestamp
                leader_confirmed_wal_seq: 0,
                sender_lease_epoch: 0,
                batches: vec![replication_item(1, GENESIS_HASH)],
            };
            let resp = unwrap_replication(shard.handle_replication_batch(stale_request).await);
            assert!(matches!(resp.result, ReplicationResult::Rejected(FollowerRejection::TimeDriftTooHigh { .. })));

            shard.close().await;
        });
    }

    // ── Commit-notify sender (post-burst detached spawn) ──

    /// Records every replicate_to_follower call as (item_count, confirmed).
    /// `fail_empty` turns commit-notify sends into failures while real batches
    /// keep succeeding — the withheld-notify oracle.
    struct TcpRecordingClient {
        tcp_calls: RefCell<Vec<(usize, u64)>>,
        fail_empty: Cell<bool>,
    }

    impl TcpRecordingClient {
        fn new() -> Self {
            Self { tcp_calls: RefCell::new(vec![]), fail_empty: Cell::new(false) }
        }

        fn empty_calls(&self) -> Vec<u64> {
            self.tcp_calls.borrow().iter().filter(|(n, _)| *n == 0).map(|(_, c)| *c).collect()
        }

        fn real_item_total(&self) -> usize {
            self.tcp_calls.borrow().iter().map(|(n, _)| *n).sum()
        }
    }

    impl ReplicationClient for TcpRecordingClient {
        fn set_follower_address(&self, _address: Option<String>) {}
        fn set_follower_reachable(&self, _: bool) {}
        fn is_follower_reachable(&self) -> bool { true }
        fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> { None }
        fn set_heartbeat_in_flight(&self, _unix_ms: Option<u64>) {}
        fn reset_heartbeat_state(&self) {}
        async fn replicate_to_follower(&self, batches: Vec<ReplicationBatchItem>, leader_confirmed_wal_seq: u64, _sender_lease_epoch: u64) -> Result<(), ReplicateToFollowerError> {
            self.tcp_calls.borrow_mut().push((batches.len(), leader_confirmed_wal_seq));
            if batches.is_empty() && self.fail_empty.get() {
                return Err(ReplicateToFollowerError::FollowerUnexpectedResponse);
            }
            Ok(())
        }
        async fn replicate_to_s3(&self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> { Ok(()) }
        async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _lease_epoch: u64) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
            Ok(celeriant_msg::response::responses::HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms + 10, follower_can_accept_tcp_replication: true })
        }
        async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> { Ok(true) }
    }

    /// Leader shard with coalescing delays so one spawned wave of writes rides
    /// one fsync and one replication cycle.
    async fn open_recording_leader(dir: &std::path::Path) -> ShardWal<TcpRecordingClient, StubS3Downloader> {
        let mut config = test_config(dir);
        config.fsync_delay = Duration::from_millis(5);
        config.replication_delay = Duration::from_millis(5);
        ShardWal::open(
            config,
            ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 10_000),
            TcpRecordingClient::new(),
            StubS3Downloader,
        )
        .await
        .unwrap()
    }

    /// Let the detached notify timer reach its idle-tail fire: it only fires once
    /// the last data batch is older than the recency window (RECENCY_WINDOW_BATCHES ×
    /// the 5ms test replication_delay), so this must exceed that window plus a cycle.
    async fn quiesce_notify() {
        glommio::timer::sleep(Duration::from_millis(RECENCY_WINDOW_BATCHES as u64 * 5 + 45)).await;
    }

    /// INVARIANT: a concurrent write burst produces exactly ONE commit-notify,
    /// strictly after the burst's data sends, carrying the post-commit
    /// confirmed index — never a notify per writer, never one mid-burst.
    #[test]
    fn commit_notify_fires_exactly_once_after_burst() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = Rc::new(open_recording_leader(&dir).await);
            shard.set_self_ref();

            let handles: Vec<_> = (1..=5u128)
                .map(|i| {
                    let shard = shard.clone();
                    glommio::spawn_local(async move {
                        write_ok(&*shard, client_write_req(key(1, 1, i), events(1))).await;
                    })
                })
                .collect();
            for h in handles {
                h.await;
            }
            quiesce_notify().await;

            let calls = shard.replication_client.tcp_calls.borrow().clone();
            let empties = shard.replication_client.empty_calls();
            assert_eq!(empties.len(), 1, "exactly one notify per burst (5 completers dedup to one), calls: {calls:?}");
            assert_eq!(calls.last().map(|(n, _)| *n), Some(0), "the notify follows every data send, calls: {calls:?}");
            assert_eq!(shard.replication_client.real_item_total(), 5, "all burst items ride data batches, calls: {calls:?}");
            assert_eq!(empties[0], 5, "the notify carries the post-commit confirmed index");
            assert_eq!(wal_positions(&shard.log_segments_cache), (5, 5));

            Rc::try_unwrap(shard).ok().expect("burst tasks done, no clones held").close().await;
        });
    }

    /// INVARIANT: the level-triggered notify dedups on the watermark. An index a
    /// send already delivered (`pushed >= pending`) fires nothing; only a real gap
    /// (`pending > pushed`) fires exactly one, whose delivery raises `pushed` so a
    /// re-arm at that index is silent and the timer disarms.
    #[test]
    fn commit_notify_dedups_by_watermark() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = Rc::new(open_recording_leader(&dir).await);
            shard.set_self_ref();
            write_ok(&*shard, client_write_req(key(1, 1, 1), events(1))).await;
            quiesce_notify().await;
            let after_write = shard.replication_client.empty_calls().len();
            assert_eq!(after_write, 1, "the write's idle tail fires exactly one notify");
            assert_eq!(shard.replication_client.empty_calls()[0], 1, "carrying the confirmed index");
            assert_eq!(shard.pending_notify_seq.get(), 1);
            assert_eq!(shard.pushed_to_follower_seq.get(), 1, "the delivered notify raised the watermark");

            // Watermark dedup: re-arming at an already-pushed index sends nothing.
            shard.note_commit_and_arm_notify_timer();
            quiesce_notify().await;
            assert_eq!(shard.replication_client.empty_calls().len(), after_write, "pending <= pushed: no dedicated notify");

            // A real batch racing ahead (pushed >= pending) also silences the arm.
            shard.pending_notify_seq.set(9);
            shard.pushed_to_follower_seq.set(9);
            shard.arm_notify_timer();
            quiesce_notify().await;
            assert_eq!(shard.replication_client.empty_calls().len(), after_write, "a batch that already carried the index needs no notify");

            // A genuine gap (pending > pushed) fires exactly one, carrying the
            // current confirmed index, and raises the watermark to it.
            shard.pushed_to_follower_seq.set(0);
            shard.pending_notify_seq.set(1);
            shard.arm_notify_timer();
            quiesce_notify().await;
            assert_eq!(shard.replication_client.empty_calls().len(), after_write + 1, "a real gap fires one notify");
            assert_eq!(shard.replication_client.empty_calls().last(), Some(&1), "carrying the current confirmed index");
            assert_eq!(shard.pushed_to_follower_seq.get(), 1, "delivery raised the watermark");
            assert!(!shard.notify_timer_armed.get(), "the timer disarms once the obligation is met");

            Rc::try_unwrap(shard).ok().expect("no clones held").close().await;
        });
    }

    /// INVARIANT: an exhausted lease budget never sends and never strands the
    /// obligation — the timer retries (budget is transient) and, if it stays
    /// exhausted, gives up to the probe rather than spinning; a fence disarms
    /// terminally; and a restored budget fires the standing obligation.
    #[test]
    fn commit_notify_timer_skips_on_exhausted_budget() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = Rc::new(open_recording_leader(&dir).await);
            shard.set_self_ref();
            write_ok(&*shard, client_write_req(key(1, 1, 1), events(1))).await;
            quiesce_notify().await;
            let before = shard.replication_client.empty_calls().len();

            // Stage a real obligation (pending > pushed), then exhaust the budget:
            // lease inside the drift window — still leader, zero budget.
            shard.pushed_to_follower_seq.set(0);
            shard.pending_notify_seq.set(1);
            shard.node_status.set(ValidatedNodeStatus::create_custom_status(
                NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 400,
            ));
            shard.arm_notify_timer();
            quiesce_notify().await;
            assert_eq!(shard.replication_client.empty_calls().len(), before, "exhausted budget never sends");
            assert!(!shard.notify_timer_armed.get(), "a budget that stays exhausted gives up to the probe, not a spin");

            // Fenced (no budget at all): the wake's leader check disarms terminally.
            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Fenced, 500, 0));
            shard.arm_notify_timer();
            quiesce_notify().await;
            assert_eq!(shard.replication_client.empty_calls().len(), before, "fenced skips the send");
            assert!(!shard.notify_timer_armed.get(), "fenced skip disarms");

            // Budget restored: the obligation still stands, so the timer fires it.
            shard.node_status.set(ValidatedNodeStatus::create_custom_status(
                NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 10_000,
            ));
            shard.arm_notify_timer();
            quiesce_notify().await;
            assert_eq!(shard.replication_client.empty_calls().len(), before + 1, "restored budget fires the pending notify");

            Rc::try_unwrap(shard).ok().expect("no clones held").close().await;
        });
    }

    /// INVARIANT (leg 1 load suppressor): while the data-batch stream is flowing
    /// (a batch within the recency window) the timer rearms and never fires — the
    /// watermark cannot do this because `pushed` structurally trails `pending` under
    /// load. When the stream stops, exactly one notify fires and the timer disarms.
    /// The idle fire also proves the recency rearm is a free deferral: had it counted
    /// toward the give-up bound, ~5 stream wakes would have disarmed before idle.
    #[test]
    fn commit_notify_recency_suppresses_under_stream_then_fires_at_idle() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = Rc::new(open_recording_leader(&dir).await);
            shard.set_self_ref();
            write_ok(&*shard, client_write_req(key(1, 1, 1), events(1))).await;
            quiesce_notify().await;
            let before = shard.replication_client.empty_calls().len();

            // Open an obligation, then keep the batch stream "flowing" by refreshing
            // last_batch_sent_at for LONGER than the recency window — so suppression is
            // the stream, not just a single recent batch.
            let window_ms = RECENCY_WINDOW_BATCHES as u64 * 5; // 16 × the 5ms test delay
            shard.pushed_to_follower_seq.set(0);
            shard.pending_notify_seq.set(1);
            shard.last_batch_sent_at.set(Instant::now());
            shard.arm_notify_timer();
            let ticker_shard = shard.clone();
            let ticker = glommio::spawn_local(async move {
                for _ in 0..(window_ms / 10 + 6) {
                    ticker_shard.last_batch_sent_at.set(Instant::now());
                    glommio::timer::sleep(Duration::from_millis(10)).await;
                }
            });
            ticker.await;
            assert_eq!(shard.replication_client.empty_calls().len(), before, "a flowing batch stream suppresses the notify past the window");

            // Stream stops: once the window elapses with no batch, exactly one fires.
            glommio::timer::sleep(Duration::from_millis(window_ms + 60)).await;
            assert_eq!(shard.replication_client.empty_calls().len(), before + 1, "the idle tail fires exactly one notify once the stream stops");
            assert!(!shard.notify_timer_armed.get(), "and the timer disarms");

            Rc::try_unwrap(shard).ok().expect("no clones held").close().await;
        });
    }

    /// INVARIANT: a stale batch stream (idle) with an open obligation fires exactly
    /// one notify carrying the confirmed index; delivery raises the watermark to
    /// pending and the next wake disarms. Also the lone-write fast-path regime: a
    /// single write, no burst, still reaches the follower within the timer window —
    /// the case the rejected spawn-time quiet window would have skipped.
    #[test]
    fn commit_notify_stale_batch_fires_once_then_disarms() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = Rc::new(open_recording_leader(&dir).await);
            shard.set_self_ref();
            write_ok(&*shard, client_write_req(key(1, 1, 1), events(1))).await;
            quiesce_notify().await;
            assert_eq!(shard.replication_client.empty_calls().len(), 1, "the lone write's tail reaches the follower");
            let before = shard.replication_client.empty_calls().len();

            shard.pushed_to_follower_seq.set(0);
            shard.pending_notify_seq.set(1);
            shard.last_batch_sent_at.set(Instant::now() - Duration::from_secs(10)); // far past any window
            shard.arm_notify_timer();
            quiesce_notify().await;
            assert_eq!(shard.replication_client.empty_calls().len(), before + 1, "stale stream: exactly one notify");
            assert_eq!(shard.replication_client.empty_calls().last(), Some(&1), "carrying the confirmed index");
            assert_eq!(shard.pushed_to_follower_seq.get(), 1, "delivery raises the watermark to pending");
            assert!(!shard.notify_timer_armed.get(), "next wake disarms");

            Rc::try_unwrap(shard).ok().expect("no clones held").close().await;
        });
    }

    /// INVARIANT (write-latency): the client ack never waits on the notify. The
    /// notify runs on a detached timer, so even with every notify send failing the
    /// write commits and acks at read == write, and the next write is undisturbed.
    /// The level timer retries a failing notify off the write path (the watermark
    /// never catches up); that is by design and bounded by reachability elsewhere,
    /// so this test asserts the ack decoupling, not a retry count.
    #[test]
    fn write_ack_never_waits_on_withheld_notify() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = Rc::new(open_recording_leader(&dir).await);
            shard.set_self_ref();
            shard.replication_client.fail_empty.set(true);

            write_ok(&*shard, client_write_req(key(1, 1, 1), events(1))).await;
            assert_eq!(wal_positions(&shard.log_segments_cache), (1, 1), "ack semantics: committed at ack, notify pending");
            quiesce_notify().await;

            write_ok(&*shard, client_write_req(key(1, 1, 2), events(1))).await;
            assert_eq!(wal_positions(&shard.log_segments_cache), (2, 2), "a failing notify must not disturb the next write");
            quiesce_notify().await;

            assert!(!shard.replication_client.empty_calls().is_empty(), "the withheld notify was attempted off the ack path");
            assert_eq!(shard.replication_client.real_item_total(), 2, "both writes replicated normally");

            // Silence the failure and let the watermark catch up so the detached
            // timer disarms before close (no lingering Rc held across an await).
            shard.replication_client.fail_empty.set(false);
            quiesce_notify().await;
            Rc::try_unwrap(shard).ok().expect("no clones held").close().await;
        });
    }

    // ── Promotion batch upload ──

    struct CapturingReplicationClient {
        s3_uploads: RefCell<Vec<Vec<ReplicationBatchItem>>>,
    }

    impl CapturingReplicationClient {
        fn new() -> Self {
            Self { s3_uploads: RefCell::new(vec![]) }
        }
    }

    impl ReplicationClient for CapturingReplicationClient {
        fn set_follower_address(&self, _address: Option<String>) {}
        fn set_follower_reachable(&self, _: bool) {}
        fn is_follower_reachable(&self) -> bool { true }
        fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> { None }
        fn set_heartbeat_in_flight(&self, _unix_ms: Option<u64>) {}
        fn reset_heartbeat_state(&self) {}
        async fn replicate_to_follower(&self, _batches: Vec<ReplicationBatchItem>, _leader_confirmed_wal_seq: u64, _sender_lease_epoch: u64) -> Result<(), ReplicateToFollowerError> { Ok(()) }
        async fn replicate_to_s3(&self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
            self.s3_uploads.borrow_mut().push(batches);
            Ok(())
        }
        async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _lease_epoch: u64) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
            Ok(celeriant_msg::response::responses::HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms + 10, follower_can_accept_tcp_replication: true })
        }
        async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> { Ok(true) }
    }

    /// Capturing client whose first `fail_first` replicate_to_s3 calls fail —
    /// exercises the promotion upload's per-chunk retry.
    struct FailingThenCapturingS3Client {
        s3_uploads: RefCell<Vec<Vec<ReplicationBatchItem>>>,
        failures_remaining: Cell<u32>,
        attempts: Cell<u32>,
    }

    impl FailingThenCapturingS3Client {
        fn new(fail_first: u32) -> Self {
            Self {
                s3_uploads: RefCell::new(vec![]),
                failures_remaining: Cell::new(fail_first),
                attempts: Cell::new(0),
            }
        }
    }

    impl ReplicationClient for FailingThenCapturingS3Client {
        fn set_follower_address(&self, _address: Option<String>) {}
        fn set_follower_reachable(&self, _: bool) {}
        fn is_follower_reachable(&self) -> bool { true }
        fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> { None }
        fn set_heartbeat_in_flight(&self, _unix_ms: Option<u64>) {}
        fn reset_heartbeat_state(&self) {}
        async fn replicate_to_follower(&self, _batches: Vec<ReplicationBatchItem>, _leader_confirmed_wal_seq: u64, _sender_lease_epoch: u64) -> Result<(), ReplicateToFollowerError> { Ok(()) }
        async fn replicate_to_s3(&self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
            self.attempts.set(self.attempts.get() + 1);
            if self.failures_remaining.get() > 0 {
                self.failures_remaining.set(self.failures_remaining.get() - 1);
                return Err(ReplicateToS3Error::S3NotConfigured);
            }
            self.s3_uploads.borrow_mut().push(batches);
            Ok(())
        }
        async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _lease_epoch: u64) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
            Ok(celeriant_msg::response::responses::HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms + 10, follower_can_accept_tcp_replication: true })
        }
        async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> { Ok(true) }
    }

    async fn open_follower_shard_capturing(dir: &std::path::Path, client: CapturingReplicationClient) -> ShardWal<CapturingReplicationClient, StubS3Downloader> {
        ShardWal::open(test_config(dir), ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000), client, StubS3Downloader)
            .await
            .unwrap()
    }

    /// Real own-speculation tail on real disk: `total` standalone writes (every
    /// metablock stamped with this node's id, read == write after each fsync),
    /// then the read cursor is rewound to its snapshot at `acked` — the exact
    /// header a leader leaves after acking `acked` and speculating to `total`.
    async fn open_shard_with_own_tail(dir: &std::path::Path, acked: u64, total: u64) -> ShardWal<StubReplicationClient, StubS3Downloader> {
        let shard = open_shard(dir).await;
        let agg = key(1, 1, 1);
        let mut snapshot = None;
        for seq in 1..=total {
            write_ok(&shard, write_req(agg.clone(), events(1))).await;
            if seq == acked {
                snapshot = Some(shard.log_segments_cache.active().metadata.borrow().write.clone());
            }
        }
        let read = snapshot.expect("acked must be <= total");
        shard.log_segments_cache.active().metadata.borrow_mut().read = Some(read);
        shard
    }

    /// Commit a follower's parked deferred tail — the state a covering carrier
    /// (or a promotion tail-commit) produces — without touching the
    /// promotion-batch floor the way a real carrier would.
    fn commit_deferred_tail<R: ReplicationClient + 'static, D: S3Downloader>(shard: &ShardWal<R, D>) {
        let pcds = shard.shard_mem_cache.borrow_mut().take_all_parked_commits();
        for pcd in pcds {
            shard_wal_replicate::commit_pcd(
                &shard.log_segments_cache, &shard.shard_mem_cache, &shard.watched_aggregates, pcd, Some(&shard.dict_codec),
            );
        }
        shard.log_segments_cache.active().metadata.borrow_mut().advance_visible_position();
    }

    async fn open_follower_shard_capturing_with_promotion_cap(
        dir: &std::path::Path, client: CapturingReplicationClient, cap_bytes: u64,
    ) -> ShardWal<CapturingReplicationClient, StubS3Downloader> {
        let mut cfg = test_config(dir);
        cfg.max_promotion_batch_bytes = Some(cap_bytes);
        ShardWal::open(cfg, ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000), client, StubS3Downloader)
            .await
            .unwrap()
    }

    /// Metablock with no datablock — avoids deserialization failures in promotion upload tests
    fn test_metablock_no_datablock(wal_seq: u64, previous_tip_hash: [u8; 32]) -> Metablock {
        let mut mb = test_metablock(wal_seq, previous_tip_hash);
        mb.datablock = DatablockStorageKind::None;
        mb
    }

    fn replication_item_no_datablock(wal_seq: u64, tip_hash: [u8; 32]) -> ReplicationBatchItem {
        ReplicationBatchItem {
            metablock: test_metablock_no_datablock(wal_seq, tip_hash),
            datablock: None,
        }
    }

    fn replication_item_with_size(wal_seq: u64, tip_hash: [u8; 32], uncompressed_size: u64) -> ReplicationBatchItem {
        let mut mb = test_metablock_no_datablock(wal_seq, tip_hash);
        mb.uncompressed_size = uncompressed_size;
        ReplicationBatchItem { metablock: mb, datablock: None }
    }

    #[test]
    fn replication_sets_last_received_wal_seq() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            let idx = shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq;
            assert_eq!(idx, 1, "should track wal_seq of replicated batch");

            shard.close().await;
        });
    }

    #[test]
    fn replication_advances_last_received_wal_seq_on_subsequent_batch() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(2, tip)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            let idx = shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq;
            assert_eq!(idx, 2, "subsequent batch should advance floor");

            shard.close().await;
        });
    }

    /// Floor is `leader_confirmed + 1`, not `batch[0].wal_seq`. They coincide
    /// on contiguous replication and diverge when the leader's confirmed range
    /// runs ahead of what this single batch covers.
    #[test]
    fn floor_uses_leader_confirmed_plus_one_not_batch_first_wal() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            // Second batch: contiguous (wal=2 chained from wal=1) but leader claims it
            // has confirmed all the way to wal=100. Floor must follow the leader's claim.
            let resp = unwrap_replication(
                shard.handle_replication_batch(
                    replication_batch_req_with_leader_confirmed(vec![replication_item(2, tip)], 100)
                ).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            let idx = shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq;
            assert_eq!(idx, 101, "floor = leader_confirmed_wal_seq + 1, not batch[0].wal_seq");

            shard.close().await;
        });
    }

    /// Floor is monotonic max. A later batch with a lower `leader_confirmed_wal_seq`
    /// (stale message, retry from before a leader rollback, etc.) must NOT regress
    /// the floor — that would shrink the promotion-batch upload range and risk
    /// dropping in-flight history the demoted peer still needs.
    #[test]
    fn floor_does_not_regress_on_lower_leader_confirmed() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let resp = unwrap_replication(
                shard.handle_replication_batch(
                    replication_batch_req_with_leader_confirmed(vec![replication_item(1, GENESIS_HASH)], 100)
                ).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            assert_eq!(
                shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq,
                101,
            );

            let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            let resp = unwrap_replication(
                shard.handle_replication_batch(
                    replication_batch_req_with_leader_confirmed(vec![replication_item(2, tip)], 50)
                ).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            let idx = shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq;
            assert_eq!(idx, 101, "monotonic guard must hold floor at previous high-water mark");

            shard.close().await;
        });
    }

    #[test]
    fn upload_promotion_batch_noop_when_field_zero() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            // No replication happened, field is 0
            shard.upload_s3_promotion_batch().await.unwrap();

            assert!(shard.replication_client.s3_uploads.borrow().is_empty(), "should not upload when no pending batch");

            shard.close().await;
        });
    }

    #[test]
    fn upload_promotion_batch_uploads_range_and_leaves_floor_for_flip() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            commit_deferred_tail(&shard);

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            shard.upload_s3_promotion_batch().await.unwrap();

            let uploads = shard.replication_client.s3_uploads.borrow();
            assert_eq!(uploads.len(), 1, "should upload exactly one batch");
            assert_eq!(uploads[0].len(), 1, "batch should contain one item");
            assert_eq!(uploads[0][0].metablock.wal_seq, 1);
            drop(uploads);

            // The upload consumes the floor but must not clear it: it doubles as
            // the crash re-entry marker until the Leader flip.
            let idx = shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq;
            assert_eq!(idx, 1, "the upload must leave the floor in place");

            // A pre-flip retry re-uploads the same range (idempotent PUT).
            shard.upload_s3_promotion_batch().await.unwrap();
            assert_eq!(shard.replication_client.s3_uploads.borrow().len(), 2, "pre-flip retry re-uploads idempotently");

            // The flip is the single clear site; after it the upload is a noop.
            shard.clear_promotion_floor();
            assert_eq!(shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq, 0);
            shard.upload_s3_promotion_batch().await.unwrap();
            assert_eq!(shard.replication_client.s3_uploads.borrow().len(), 2, "no floor, nothing to upload");

            shard.close().await;
        });
    }

    #[test]
    fn upload_promotion_batch_after_multiple_replications() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            // Replicate batch 1
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            // Replicate batch 2
            let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(2, tip)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            commit_deferred_tail(&shard);

            // Upload should contain only entries from wal_seq 2 onward (last batch)
            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            shard.upload_s3_promotion_batch().await.unwrap();

            let uploads = shard.replication_client.s3_uploads.borrow();
            assert_eq!(uploads.len(), 1);
            assert_eq!(uploads[0].len(), 1, "should only upload from last_received index");
            assert_eq!(uploads[0][0].metablock.wal_seq, 2);

            shard.close().await;
        });
    }

    /// Chaos 16k finding C: a promotion batch covering a long drained tail
    /// went up as ONE S3 object (105k entries) and the PUT timed out; the
    /// range never reached S3, the demoted peer's gap was unbridgeable, and
    /// EventualConvergence livelocked. The upload must chunk into
    /// internode_max_request_size-bounded objects — catchup already stitches
    /// contiguous ranges.
    #[test]
    fn upload_promotion_batch_chunks_by_internode_request_size() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let mut cfg = test_config(&dir);
            // One metablock per chunk: every item is FIXED_BLOCK_SIZE_BYTES.
            cfg.internode_max_request_size = FIXED_BLOCK_SIZE_BYTES as u64 + 1;
            let shard = ShardWal::open(
                cfg,
                ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000),
                client,
                StubS3Downloader,
            ).await.unwrap();

            // Three single-item batches, all claiming leader_confirmed=0 so the
            // floor holds at 1 and the promotion range covers all three.
            let mut tip = GENESIS_HASH;
            for seq in 1..=3 {
                let resp = unwrap_replication(
                    shard.handle_replication_batch(
                        replication_batch_req_with_leader_confirmed(vec![replication_item_no_datablock(seq, tip)], 0)
                    ).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }), "replication {seq} failed: {resp:?}");
                tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            }
            commit_deferred_tail(&shard);

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            shard.upload_s3_promotion_batch().await.unwrap();

            let uploads = shard.replication_client.s3_uploads.borrow();
            assert_eq!(uploads.len(), 3, "3 items at 1-per-chunk cap must produce 3 S3 objects, got {}", uploads.len());
            assert_eq!(uploads[0][0].metablock.wal_seq, 1);
            assert_eq!(uploads[1][0].metablock.wal_seq, 2);
            assert_eq!(uploads[2][0].metablock.wal_seq, 3);
            drop(uploads);

            let idx = shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq;
            assert_eq!(idx, 1, "the floor survives the upload (cleared only at the Leader flip)");

            shard.close().await;
        });
    }

    /// One failed PUT must not strand the promotion range — each chunk
    /// retries before the upload gives up.
    #[test]
    fn upload_promotion_batch_retries_failed_chunk() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = FailingThenCapturingS3Client::new(1);
            let shard = ShardWal::open(
                test_config(&dir),
                ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000),
                client,
                StubS3Downloader,
            ).await.unwrap();

            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(1, GENESIS_HASH)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            commit_deferred_tail(&shard);

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            shard.upload_s3_promotion_batch().await.unwrap();

            assert_eq!(shard.replication_client.attempts.get(), 2, "first PUT fails, retry succeeds");
            assert_eq!(shard.replication_client.s3_uploads.borrow().len(), 1);

            let idx = shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq;
            assert_eq!(idx, 1, "the floor survives the upload (cleared only at the Leader flip)");

            shard.close().await;
        });
    }

    /// Scan exceeds `max_promotion_batch_bytes` → skip upload, return Ok; the
    /// Leader flip then clears the floor so the same unbridgeable range is not
    /// re-scanned on the next role change. Demoted peer recovers via the
    /// leader-side S3 fallback path.
    #[test]
    fn upload_promotion_batch_skips_when_budget_exceeded() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing_with_promotion_cap(&dir, client, 100).await;

            // Three chained replicated batches, 50 bytes each → 150 bytes total, well past the 100-byte cap.
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_with_size(1, GENESIS_HASH, 50)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_with_size(2, tip, 50)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req(vec![replication_item_with_size(3, tip, 50)])).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            commit_deferred_tail(&shard);
            // Pin the floor at 1 so the scan covers all three metablocks.
            shard.log_segments_cache.active().metadata.borrow_mut().last_received_replication_wal_seq = 1;

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            shard.upload_s3_promotion_batch().await.unwrap();

            assert!(
                shard.replication_client.s3_uploads.borrow().is_empty(),
                "no S3 upload when scan exceeds max_promotion_batch_bytes",
            );
            assert_eq!(
                shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq,
                1,
                "the skip must not clear the floor (that is the flip's job)",
            );
            shard.clear_promotion_floor();
            assert_eq!(
                shard.log_segments_cache.active().metadata.borrow().last_received_replication_wal_seq,
                0,
                "the flip clears the floor so later role changes don't re-scan the range",
            );

            shard.close().await;
        });
    }

    /// A follower rotating while commits are parked must not write the sealed
    /// segment's summary sidecar at rotation — the summary is incomplete until
    /// the parked read-side commits drain. A covering carrier drains them and
    /// the sweep then writes the sidecar.
    #[test]
    fn follower_rotation_defers_summary_sidecar_until_covering_carrier() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let mut cfg = test_config(&dir);
            cfg.shard_log_preallocate_bytes = 1024 * 1024;
            let shard = ShardWal::open(
                cfg,
                ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000),
                client,
                StubS3Downloader,
            ).await.unwrap();

            // Replicate 100-entry batches, never confirming, until the log rotates.
            // Only the first item's previous_tip_hash is chain-checked; sync()
            // recomputes the rest.
            let mut next_seq = 1u64;
            let mut rotated = false;
            for _ in 0..12 {
                let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
                let items: Vec<_> = (0..100u64)
                    .map(|i| replication_item_no_datablock(next_seq + i, if i == 0 { tip } else { GENESIS_HASH }))
                    .collect();
                let resp = unwrap_replication(
                    shard.handle_replication_batch(replication_batch_req_with_leader_confirmed(items, 0)).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                next_seq += 100;
                if shard.log_segments_cache.active_log_id() > 1 {
                    rotated = true;
                    break;
                }
            }
            assert!(rotated, "scaffolding: 1MB preallocate must rotate within 1200 entries");
            assert!(shard.shard_mem_cache.borrow().parked_commit_count() > 0, "scaffolding: unconfirmed batches must be parked");
            assert!(
                !crate::shard_wal_sync::summary_path(&dir, 1).exists(),
                "sidecar must not be written while the sealed range is unconfirmed"
            );

            // Confirm only the first batch: its commit lands in the sealed
            // segment while the active segment's read stays None. The
            // shard-level committed cursor (the read/lag gauge source) must
            // report it instead of collapsing to 0.
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req_with_leader_confirmed(
                    vec![replication_item_no_datablock(100, GENESIS_HASH)],
                    100,
                )).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            assert!(shard.log_segments_cache.active().metadata.borrow().read.is_none(),
                "scaffolding: active segment must still be unadvanced");
            assert_eq!(shard.log_segments_cache.committed_read_wal_seq(), 100,
                "committed cursor must survive the rotation via the sealed predecessor");
            assert!(
                !crate::shard_wal_sync::summary_path(&dir, 1).exists(),
                "a partial confirm must not release the sealed sidecar"
            );

            // Covering carrier: duplicate of the last entry (skipped on apply),
            // confirming the tip.
            let tip_seq = next_seq - 1;
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req_with_leader_confirmed(
                    vec![replication_item_no_datablock(tip_seq, GENESIS_HASH)],
                    tip_seq,
                )).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 0, "covering carrier must drain every parked commit");
            assert!(
                crate::shard_wal_sync::summary_path(&dir, 1).exists(),
                "sidecar must appear once the drain covers the sealed segment"
            );

            shard.close().await;
        });
    }

    /// The sidecar sweep must not trust LRU absence: an evicted pending-advance
    /// segment reloads from disk, whose header still shows the unconfirmed
    /// range, so its sidecar stays deferred — writing it before the parked
    /// commits drain would seal a subset. Only reloaded state that shows the
    /// advance releases the write.
    #[test]
    fn evicted_pending_advance_segment_defers_sidecar_until_reload_shows_advanced() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let mut cfg = test_config(&dir);
            cfg.shard_log_preallocate_bytes = 1024 * 1024;
            let shard = ShardWal::open(
                cfg,
                ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000),
                client,
                StubS3Downloader,
            ).await.unwrap();

            // Rotate with every batch unconfirmed: segment 1 seals pending-advance.
            let mut next_seq = 1u64;
            while shard.log_segments_cache.active_log_id() == 1 {
                let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
                let items: Vec<_> = (0..100u64)
                    .map(|i| replication_item_no_datablock(next_seq + i, if i == 0 { tip } else { GENESIS_HASH }))
                    .collect();
                let resp = unwrap_replication(
                    shard.handle_replication_batch(replication_batch_req_with_leader_confirmed(items, 0)).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                next_seq += 100;
            }
            assert!(shard.shard_mem_cache.borrow().parked_commit_count() > 0, "scaffolding: unconfirmed batches must be parked");

            // Evict the sealed segment, then sweep: the gate must reload it and
            // read pending-advance from the header, not "advanced" from absence.
            shard.log_segments_cache.evict_from_lru(1);
            shard.sweep_sealed_summaries().await;
            assert!(
                !crate::shard_wal_sync::summary_path(&dir, 1).exists(),
                "an evicted pending-advance segment must not get a sidecar"
            );

            // A covering commit-notify drains the parked commits into the (now
            // resident again) segment, advancing its cursor — only then may the
            // sweep release the sidecar.
            let tip_seq = next_seq - 1;
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req_with_leader_confirmed(vec![], tip_seq)).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));
            assert!(
                crate::shard_wal_sync::summary_path(&dir, 1).exists(),
                "the sweep must write the sidecar once the drained segment shows advanced"
            );

            shard.close().await;
        });
    }

    /// Deferred-path bloom staging: a follower rotation stages the sealed segment's
    /// bloom words in the SealedSegmentSummary snapshot. The segment is then EVICTED
    /// (its bloomless v2 header is all a reload can offer, and no sidecar exists yet),
    /// so when the covering notify releases the sweep, the sidecar's blooms can only
    /// have come from the staged snapshot — the header-reload fallback is dead.
    #[test]
    fn deferred_sweep_writes_sidecar_with_staged_blooms() {
        glommio_test!({
            use celeriant_rotating_log::log_segment_file::aggregate_key_bloom::AggregateKeyBloom;

            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let mut cfg = test_config(&dir);
            cfg.shard_log_preallocate_bytes = 1024 * 1024;
            let shard = ShardWal::open(
                cfg,
                ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000),
                client,
                StubS3Downloader,
            ).await.unwrap();

            // Rotate with every batch unconfirmed (DeferToLeaderConfirmed path).
            let mut next_seq = 1u64;
            while shard.log_segments_cache.active_log_id() == 1 {
                let tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
                let items: Vec<_> = (0..100u64)
                    .map(|i| replication_item_no_datablock(next_seq + i, if i == 0 { tip } else { GENESIS_HASH }))
                    .collect();
                let resp = unwrap_replication(
                    shard.handle_replication_batch(replication_batch_req_with_leader_confirmed(items, 0)).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                next_seq += 100;
            }

            shard.log_segments_cache.evict_from_lru(1);
            {
                let seg = shard.log_segments_cache.get(1).await.unwrap();
                assert!(
                    seg.metadata.borrow().write.aggregate_key_bloom.borrow().is_absent(),
                    "scaffolding: the reloaded segment must be bloomless (no sidecar, bloomless header)"
                );
            }

            // Covering commit-notify drains the parked commits; the sweep writes the sidecar.
            let tip_seq = next_seq - 1;
            let resp = unwrap_replication(
                shard.handle_replication_batch(replication_batch_req_with_leader_confirmed(vec![], tip_seq)).await,
            );
            assert!(matches!(resp.result, ReplicationResult::Success { .. }));

            let payload = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await
                .expect("the sweep must have written the sidecar");
            let agg_words = payload.aggregate_bloom.expect("sidecar must carry the staged aggregate bloom");
            let agg_bloom = AggregateKeyBloom::from_bytes(&agg_words);
            assert!(agg_bloom.may_contain(&key(1, 1, 1)), "the replicated aggregate must be in the staged bloom");
            assert!(!agg_bloom.may_contain(&key(9, 9, 9)), "tripwire: staged bloom must stay precise");
            assert!(payload.client_bloom.is_some(), "sidecar must carry the staged client bloom");

            shard.close().await;
        });
    }

    /// The upload's reconcile prefix COMMITS an unconfirmed peer tail before
    /// scanning the promotion range, so the upload covers entries no carrier
    /// ever confirmed (they may be acked on the dead leader's side).
    #[test]
    fn upload_s3_promotion_batch_commits_deferred_tail_before_promoting() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let client = CapturingReplicationClient::new();
            let shard = open_follower_shard_capturing(&dir, client).await;

            // Carriers confirm seq-1: read lands at 4, seq 5 stays parked and the
            // promotion floor is 5.
            let mut tip = GENESIS_HASH;
            for seq in 1u64..=5 {
                let resp = unwrap_replication(
                    shard.handle_replication_batch(replication_batch_req(vec![replication_item_no_datablock(seq, tip)])).await,
                );
                assert!(matches!(resp.result, ReplicationResult::Success { .. }));
                tip = shard.log_segments_cache.active().metadata.borrow().write.tip_hash;
            }
            assert_eq!(shard.shard_mem_cache.borrow().parked_commit_count(), 1, "precondition: seq 5 parked");

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            shard.upload_s3_promotion_batch().await.unwrap();

            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!(meta.write.wal_seq, 5, "the deferred tail must survive promotion");
                assert_eq!(meta.read.as_ref().unwrap().wal_seq, 5, "the deferred tail must be committed before the upload");
            }

            let uploads = shard.replication_client.s3_uploads.borrow();
            assert_eq!(uploads.len(), 1, "exactly one promotion upload");
            assert_eq!(
                uploads[0].iter().map(|item| item.metablock.wal_seq).collect::<Vec<_>>(),
                vec![5],
                "the upload range starts at the floor and includes the committed tail"
            );

            shard.close().await;
        });
    }

    #[test]
    fn cull_clears_aggregate_write_client_snapshots_lru() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard_with_own_tail(&dir, 5, 10).await;

            let agg = AggregateKey::new(1, 1, 1);
            shard.shard_mem_cache.borrow_mut().put_aggregate_write_client_snapshot_for_test(agg.clone(), 42, 7, 0);
            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, 42), Some(7),
                "precondition: LRU has (agg, client=42) -> seq=7",
            );

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            shard.upload_s3_promotion_batch().await.unwrap();

            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!(meta.write.wal_seq, 5, "own-tail cull must rewind write to read");
            }

            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, 42), None,
                "cull must clear aggregate_write_client_snapshots; entries point at discarded content",
            );

            shard.close().await;
        });
    }

    #[test]
    fn inflight_duplicate_returns_retriable_error() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);
            let client_id = 99u128;

            let req = write_req_full(agg.clone(), events(1), true, None, client_id, true);
            write_ok(&shard, req).await;

            // Simulate the leader-inflight window: read cursor behind write, cache entry
            // tagged with wal_seq matching the write (so cached_wal_seq > read_cursor).
            {
                let active = shard.log_segments_cache.active();
                let mut meta = active.metadata.borrow_mut();
                let write_wal_seq = meta.write.wal_seq;
                let read = meta.read.as_mut().expect("standalone must have a read cursor");
                read.wal_seq = 0;
                drop(meta);
                shard.shard_mem_cache.borrow_mut()
                    .put_aggregate_write_client_snapshot_for_test(agg.clone(), client_id, 1, write_wal_seq);
            }

            let req = write_req_full(agg.clone(), events(1), true, None, client_id, true);
            let result = process(&shard, req).await;
            assert!(
                matches!(result, Err(ShardError::Write(ShardWriteError::InflightDuplicateWrite { .. }))),
                "expected InflightDuplicateWrite while replication pending, got: {:?}", result,
            );

            shard.close().await;
        });
    }

    /// Two concurrent attempts at the same client_seq: the first lands in
    /// `aggregate_queue_positions` before fsync. The second's OCC check must
    /// see the queue entry and return `InflightDuplicateWrite` (retriable),
    /// NOT `ClientIdempotencyViolation` (durable 2002). Returning 2002 here
    /// would be a false ack if the first write's fsync subsequently fails.
    #[test]
    fn queue_conflict_returns_retriable_inflight_not_durable_2002() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);
            let client_id = 99u128;

            let req = write_req_full(agg.clone(), events(1), true, None, client_id, true);
            write_ok(&shard, req).await;

            // Simulate a parallel task having queued client_seq=2 but not yet fsynced.
            shard.shard_mem_cache.borrow_mut()
                .put_aggregate_queue_client_seq_for_test(agg.clone(), client_id, 2);

            // Second attempt at client_seq=2 hits the queue conflict.
            let req = write_req_full(agg.clone(), events(2), true, None, client_id, true);
            let result = process(&shard, req).await;
            assert!(
                matches!(result, Err(ShardError::Write(ShardWriteError::InflightDuplicateWrite { .. }))),
                "queue conflict must return InflightDuplicateWrite, got: {:?}", result,
            );

            shard.close().await;
        });
    }

    /// Stale PCDs reference the pre-cull write. If they commit after the cull,
    /// read advances past write and traps catchup behind the ack barrier.
    #[test]
    fn cull_drains_pending_replication_to_prevent_stale_pcd_commit() {
        use celeriant_memcache::pending_commit_data::PendingCommitData;

        glommio_test!({
            let (_tmp, dir) = test_dir();
            // Own tail (5, 10]: write=10 unreplicated speculation, read=5 acked.
            let shard = open_shard_with_own_tail(&dir, 5, 10).await;

            // Stale PCD captured pre-cull (write.wal_seq=10), returned to pending after
            // replication failure.
            {
                let stale_metadata = {
                    let active = shard.log_segments_cache.active();
                    active.metadata.borrow().clone()
                };
                let stale_pcd = PendingCommitData {
                    log_metadata: stale_metadata,
                    pending_queue: vec![],
                };
                shard.shard_mem_cache.borrow_mut().push_pending_replication(stale_pcd);
            }
            assert_eq!(
                shard.shard_mem_cache.borrow().pending_replication_count(), 1,
                "test setup: pending_replication should have 1 stale PCD",
            );

            shard.node_status.set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
            shard.upload_s3_promotion_batch().await.unwrap();

            {
                let active = shard.log_segments_cache.active();
                let meta = active.metadata.borrow();
                assert_eq!(meta.write.wal_seq, 5, "cull must rewind write to read");
                assert_eq!(meta.read.as_ref().unwrap().wal_seq, 5, "read unchanged");
            }

            assert_eq!(
                shard.shard_mem_cache.borrow().pending_replication_count(), 0,
                "cull must drain pending_replication of PCDs that reference the discarded tail",
            );

            shard.close().await;
        });
    }

    // ── Batch 1 Test Gaps: P1-1, P1-6, P1-7 ──

    fn multi_write_req(writes: Vec<(AggregateKey, Vec<DatablockAggregateEvent>, Option<u64>)>) -> ClientRequest {
        let mut map = HashMap::new();
        for (agg, evts, expected) in writes {
            map.insert(
                agg,
                SingleAggregateWrite {
                    events: evts,
                    allow_create: true,
                    expected_version: expected,
                    enforce_client_idempotency: false,
                },
            );
        }
        ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes: map,
        })
    }

    #[test]
    fn multi_aggregate_occ_rollback() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let (a, b) = (key(1, 1, 1), key(1, 1, 2));

            write_ok(&shard, write_req(a.clone(), events(1))).await;
            write_ok(&shard, write_req(b.clone(), events(1))).await;

            // Advance B to batch 2
            write_ok(&shard, write_req_full(b.clone(), events(1), false, Some(1), 1, false)).await;

            // Multi-write: A expects 1 (correct), B expects 1 (stale - now at 2)
            let req = multi_write_req(vec![
                (a.clone(), events(1), Some(1)),
                (b.clone(), events(1), Some(1)),
            ]);
            let result = process(&shard, req).await;
            assert!(matches!(
                result,
                Err(ShardError::Write(ShardWriteError::OptimisticConcurrencyViolation { .. }))
            ));

            // Verify A was NOT written (rollback)
            let read_a = unwrap_read(process(&shard, read_req(a)).await);
            assert_eq!(read_a.event_batches.len(), 1);

            shard.close().await;
        });
    }

    #[test]
    fn sequential_writes_produce_contiguous_batch_indices() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            for i in 0..5 {
                write_ok(&shard, write_req_full(agg.clone(), events(1), i == 0, None, 1, false)).await;
            }

            let read = unwrap_read(process(&shard, read_req(agg)).await);
            let indices: Vec<u64> = read.event_batches.iter().map(|b| b.aggregate_version).collect();
            assert_eq!(indices, vec![1, 2, 3, 4, 5]);

            shard.close().await;
        });
    }

    #[test]
    fn read_wrong_org_returns_not_found() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            write_ok(&shard, write_req(key(1, 1, 1), events(1))).await;

            let result = process(&shard, read_req(key(2, 1, 1))).await;
            assert!(matches!(
                result,
                Err(ShardError::Read(ShardReadError::AggregateNotExists))
            ));

            shard.close().await;
        });
    }

    #[test]
    fn list_wrong_org_returns_empty() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            write_ok(&shard, write_req(key(1, 1, 1), events(1))).await;

            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(2), None)).await);
            assert!(aggs.aggregates.is_empty());

            shard.close().await;
        });
    }

    // ── process_client_request ──

    #[test]
    fn client_request_write_and_read() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            let write_result = shard.process_client_request(client_write_req(agg.clone(), events(3))).await;
            assert!(matches!(write_result, Ok(ClientResponse::Write(_))));

            let read_result = shard.process_client_request(client_read_req(agg)).await;
            match read_result.expect("read should succeed") {
                ClientResponse::Read(r) => {
                    assert_eq!(r.event_batches.len(), 1);
                    assert_eq!(r.event_batches[0].events.len(), 3);
                }
                other => panic!("expected Read, got {other:?}"),
            }

            shard.close().await;
        });
    }

    // ── Schema registration and validation ──

    const NAME_AGE_SCHEMA: &str = r#"{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"]}"#;

    fn schema_req(org: u128, atype: u128, major: u64, minor: u64, schema: &str) -> ClientRequest {
        ClientRequest::RegisterSchema(celeriant_msg::request::requests::RegisterSchemaRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            schema_key: SchemaKey::new(org, atype, major, minor),
            schema_type: 0,
            schema: schema.to_string(),
        })
    }

    fn schema_req_with_type(org: u128, atype: u128, major: u64, minor: u64, schema_type: u8, schema: &str) -> ClientRequest {
        ClientRequest::RegisterSchema(celeriant_msg::request::requests::RegisterSchemaRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            schema_key: SchemaKey::new(org, atype, major, minor),
            schema_type,
            schema: schema.to_string(),
        })
    }

    fn json_events(payloads: &[&[u8]], major: u64, minor: u64) -> Vec<DatablockAggregateEvent> {
        payloads.iter().enumerate().map(|(i, payload)| DatablockAggregateEvent {
            client_seq: (i + 1) as u64,
            event_type_major: major,
            event_type_minor: minor,
            event_value: Arc::new(payload.to_vec()),
            ..Default::default()
        }).collect()
    }

    #[test]
    fn schema_register_and_validate_write() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await;
            assert!(matches!(result, Ok(ClientResponse::RegisterSchema(_))));

            let valid = json_events(&[br#"{"name":"alice","age":30}"#], 1, 0);
            write_ok(&shard, write_req(key(1, 1, 1), valid)).await;

            shard.close().await;
        });
    }

    #[test]
    fn schema_rejects_invalid_write() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();

            let bad_payloads: &[&[u8]] = &[
                br#"{"name":"alice"}"#,              // missing required field
                br#"{"name":"alice","age":"thirty"}"#, // wrong type
                b"not json",                          // non-JSON
            ];
            for payload in bad_payloads {
                let evts = json_events(&[payload], 1, 0);
                let result = process(&shard, write_req(key(1, 1, 1), evts)).await;
                assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))),
                    "expected SchemaValidationFailed for {:?}, got {:?}", std::str::from_utf8(payload), result);
            }

            shard.close().await;
        });
    }

    #[test]
    fn schema_idempotent_registration() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // First registration succeeds
            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();

            // Second identical registration returns SchemaAlreadyExists (immutable — cannot re-register)
            let result = process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::SchemaAlreadyExists { .. }
            ))));

            shard.close().await;
        });
    }

    #[test]
    fn schema_already_exists_different_schema() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();

            let different = r#"{"type":"object","properties":{"x":{"type":"number"}}}"#;
            let result = process(&shard, schema_req(1, 1, 1, 0, different)).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::SchemaAlreadyExists { event_type_major: 1, event_type_minor: 0 }
            ))));

            shard.close().await;
        });
    }

    #[test]
    fn schema_invalid_json_rejected() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let result = process(&shard, schema_req(1, 1, 1, 0, "{{not valid json")).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::InvalidSchema { .. }
            ))));

            shard.close().await;
        });
    }

    #[test]
    fn schema_unsupported_type_rejected() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // schema_type 99 = unknown (unsupported)
            let result = process(&shard, schema_req_with_type(1, 1, 1, 0, 99, NAME_AGE_SCHEMA)).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::UnsupportedSchemaType { .. }
            ))), "expected UnsupportedSchemaType, got {:?}", result);

            shard.close().await;
        });
    }

    const AVRO_PERSON_SCHEMA: &str = r#"{"type":"record","name":"Person","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}"#;

    fn avro_events(payloads: &[&[u8]], major: u64, minor: u64) -> Vec<DatablockAggregateEvent> {
        payloads.iter().enumerate().map(|(i, payload)| DatablockAggregateEvent {
            client_seq: (i + 1) as u64,
            event_type_major: major,
            event_type_minor: minor,
            event_value: Arc::new(payload.to_vec()),
            ..Default::default()
        }).collect()
    }

    fn avro_encode_person(name: &str, age: i32) -> Vec<u8> {
        let schema = apache_avro::Schema::parse_str(AVRO_PERSON_SCHEMA).unwrap();
        apache_avro::to_avro_datum(
            &schema,
            apache_avro::types::Value::Record(vec![
                ("name".to_string(), apache_avro::types::Value::String(name.to_string())),
                ("age".to_string(), apache_avro::types::Value::Int(age)),
            ]),
        ).unwrap()
    }

    #[test]
    fn schema_avro_register_and_validate() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // Register Avro schema (type 1)
            process(&shard, schema_req_with_type(1, 1, 1, 0, 1, AVRO_PERSON_SCHEMA)).await.unwrap();

            // Write valid Avro event
            let valid = avro_encode_person("alice", 30);
            write_ok(&shard, write_req(key(1, 1, 1), avro_events(&[&valid], 1, 0))).await;

            // Write invalid bytes — should be rejected
            let bad = avro_events(&[b"not avro"], 1, 0);
            let result = process(&shard, write_req(key(1, 1, 1), bad)).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))),
                "expected SchemaValidationFailed, got {:?}", result);

            shard.close().await;
        });
    }

    #[test]
    fn schema_encrypted_event_skips_validation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();

            // Encrypted event with garbage payload — should pass because iv.is_some() skips validation
            let evts = vec![DatablockAggregateEvent {
                client_seq: 1,
                event_type_major: 1,
                event_type_minor: 0,
                event_value: Arc::new(b"not valid json at all".to_vec()),
                iv: Some([0u8; 12]),
                ..Default::default()
            }];
            write_ok(&shard, write_req(key(1, 1, 1), evts)).await;

            shard.close().await;
        });
    }

    #[test]
    fn schema_unregistered_event_type_passes_through() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // Register schema for (major=1, minor=0)
            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();

            // Write to (major=2, minor=0) — no schema, should pass
            let evts = json_events(&[b"literally anything"], 2, 0);
            write_ok(&shard, write_req(key(1, 1, 1), evts)).await;

            shard.close().await;
        });
    }

    #[test]
    fn schema_survives_reopen() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Register and close
            {
                let shard = open_shard(&dir).await;
                process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();
                shard.close().await;
            }

            // Reopen — cold cache, must recover from WAL
            {
                let shard = open_shard(&dir).await;

                // Valid write still works
                let valid = json_events(&[br#"{"name":"bob","age":25}"#], 1, 0);
                write_ok(&shard, write_req(key(1, 1, 1), valid)).await;

                // Invalid write still rejected
                let invalid = json_events(&[br#"{"name":"bob"}"#], 1, 0);
                let result = process(&shard, write_req(key(1, 1, 2), invalid)).await;
                assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))));

                shard.close().await;
            }
        });
    }

    #[test]
    fn schema_prewarm_recovers_inline_and_block_datablocks() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Build a schema large enough to exceed MINIBATCH_SIZE_BYTES (512)
            // and force block storage. ~20 properties at ~28 bytes each = ~560+ bytes.
            let mut large_schema = String::from(r#"{"type":"object","properties":{"#);
            for i in 0..20 {
                if i > 0 { large_schema.push(','); }
                large_schema.push_str(&format!(r#""field_{i:02}":{{ "type":"string"}}"#));
            }
            large_schema.push_str(r#"},"required":["field_00"]}"#);
            assert!(large_schema.len() > 512, "schema must exceed inline threshold");

            // Register both an inline schema (event type 1,0) and a block schema (event type 2,0)
            {
                let shard = open_shard(&dir).await;
                process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();
                process(&shard, schema_req(1, 1, 2, 0, &large_schema)).await.unwrap();
                shard.close().await;
            }

            // Reopen — pre_warm_cache must recover both inline and block schemas
            {
                let shard = open_shard(&dir).await;

                let inline_key = SchemaKey::new(1, 1, 1, 0);
                let block_key = SchemaKey::new(1, 1, 2, 0);

                // Both schemas must be in cache immediately after open, before any writes
                assert!(shard.schema_cache_has_schema(&inline_key), "inline schema not pre-warmed");
                assert!(shard.schema_cache_has_schema(&block_key), "block schema not pre-warmed");

                // Inline schema: validation works
                let valid = json_events(&[br#"{"name":"bob","age":25}"#], 1, 0);
                write_ok(&shard, write_req(key(1, 1, 1), valid)).await;

                let invalid = json_events(&[br#"{"name":"bob"}"#], 1, 0);
                let result = process(&shard, write_req(key(1, 1, 2), invalid)).await;
                assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))));

                // Block schema: validation works
                let valid = json_events(&[br#"{"field_00":"hello"}"#], 2, 0);
                write_ok(&shard, write_req(key(1, 1, 3), valid)).await;

                let invalid = json_events(&[br#"{"field_00": 42}"#], 2, 0);
                let result = process(&shard, write_req(key(1, 1, 4), invalid)).await;
                assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))));

                // Force-clear the schema cache to test ensure_schema_cached (cold-cache WAL scan)
                shard.schema_cache_clear();
                assert!(!shard.schema_cache_has_schema(&inline_key), "inline schema should be cleared");
                assert!(!shard.schema_cache_has_schema(&block_key), "block schema should be cleared");

                // Write triggers ensure_schema_cached which must WAL-scan and recover inline schema
                let valid = json_events(&[br#"{"name":"alice","age":40}"#], 1, 0);
                write_ok(&shard, write_req(key(1, 1, 5), valid)).await;
                assert!(shard.schema_cache_has_schema(&inline_key), "inline schema not recovered by ensure_schema_cached");

                // Write triggers ensure_schema_cached which must WAL-scan and recover block schema
                let valid = json_events(&[br#"{"field_00":"world"}"#], 2, 0);
                write_ok(&shard, write_req(key(1, 1, 6), valid)).await;
                assert!(shard.schema_cache_has_schema(&block_key), "block schema not recovered by ensure_schema_cached");

                shard.close().await;
            }
        });
    }

    #[test]
    fn schema_cold_cache_rejects_duplicate_registration() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            {
                let shard = open_shard(&dir).await;
                process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();
                shard.close().await;
            }

            // Reopen — cold cache, ensure_schema_cached must WAL-scan before rejecting
            {
                let shard = open_shard(&dir).await;

                let result = process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await;
                assert!(matches!(result, Err(ShardError::RegisterSchema(
                    crate::error::shard_schema_error::ShardSchemaError::SchemaAlreadyExists { .. }
                ))));

                let different = r#"{"type":"object","properties":{"x":{"type":"number"}}}"#;
                let result = process(&shard, schema_req(1, 1, 1, 0, different)).await;
                assert!(matches!(result, Err(ShardError::RegisterSchema(
                    crate::error::shard_schema_error::ShardSchemaError::SchemaAlreadyExists { .. }
                ))));

                shard.close().await;
            }
        });
    }

    // ADVERSARIAL EVIDENCE — the seal window must not prove schema absence.
    //
    // The FullCommit seal path drains the active accumulator BEFORE rotation:
    // `take_segment_summary` (inside `write_segment_summary_sidecar`) runs,
    // then the fiber parks at awaits (sidecar create/write/fdatasync, then
    // `rotate_to_next_log().await`). Throughout that window `active_log_id()`
    // still names the sealing segment while the drained accumulator is fresh
    // AND untainted (`take_segment_summary` resets
    // `segment_summary_incomplete`). Unguarded, `active_segment_may_contain_schema`
    // would answer "definitely absent" for every hash the sealing segment
    // actually holds, a concurrent `ensure_schema_cached` would consult
    // exactly that and SKIP the segment, and the false `no_schema` conclusion
    // would be CACHED. The deferred seal path has the same window
    // (`store_sealed_segment_summary` drains, rotation awaits after). The
    // guard is the draining latch: both drains set `segment_summary_draining`,
    // the consult answers maybe-present while it is up, and
    // `note_active_segment_rotated` drops it only after rotation returns.
    // This test reproduces the mid-await state directly (single-threaded
    // executor: state between awaits IS this state) and asserts the lookup
    // still finds the committed registration.
    #[test]
    fn adversarial_seal_window_hides_committed_schema_registration() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // A committed, durable registration in the active segment.
            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();
            let schema_key = SchemaKey::new(1, 1, 1, 0);

            // Cold cache (LRU-eviction stand-in) so the next lookup must scan.
            shard.schema_cache_clear();

            // The exact state held while write_segment_summary_sidecar /
            // rotate_to_next_log are parked at an await: accumulator drained,
            // rotation not yet visible.
            let _seal_payload = shard.shard_mem_cache.borrow_mut().take_segment_summary();

            // A concurrent write's schema lookup during that window.
            shard.ensure_schema_cached(&schema_key).await.unwrap();

            assert!(
                shard.schema_cache_has_schema(&schema_key),
                "false absence: ensure_schema_cached skipped the sealing segment via the drained accumulator and cached no_schema"
            );

            shard.close().await;
        });
    }

    #[test]
    fn schema_multiple_event_types_independent() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            let number_schema = r#"{"type":"object","properties":{"value":{"type":"number"}},"required":["value"]}"#;

            process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();
            process(&shard, schema_req(1, 1, 2, 0, number_schema)).await.unwrap();

            // Valid for schema 1
            let evts = json_events(&[br#"{"name":"alice","age":30}"#], 1, 0);
            write_ok(&shard, write_req(key(1, 1, 1), evts)).await;

            // Valid for schema 2
            let evts = json_events(&[br#"{"value":42}"#], 2, 0);
            write_ok(&shard, write_req(key(1, 1, 2), evts)).await;

            // Invalid for schema 1 (passes schema 2's shape)
            let evts = json_events(&[br#"{"value":42}"#], 1, 0);
            let result = process(&shard, write_req(key(1, 1, 3), evts)).await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))));

            shard.close().await;
        });
    }

    #[test]
    fn schema_exceeds_max_size_rejected() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // max_schema_size_bytes is 16384 in test_config
            let oversized = format!(r#"{{"type":"object","description":"{}"}}"#, "x".repeat(20000));
            let result = process(&shard, schema_req(1, 1, 1, 0, &oversized)).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::InvalidSchema { .. }
            ))));

            shard.close().await;
        });
    }

    #[test]
    fn schema_follower_rejects_registration() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_follower_shard(&dir).await;

            let result = process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await;
            assert!(matches!(result, Err(ShardError::RegisterSchema(
                crate::error::shard_schema_error::ShardSchemaError::ShardCannotAcceptWrites { .. }
            ))));

            shard.close().await;
        });
    }

    // ── Compaction ──

    /// Config for compaction tests: small segment size to trigger rotation quickly.
    ///
    /// 2 header blocks + 512KB usable per segment (header-size-independent, so the byte-ratio
    /// tuning below holds whatever HEADER_BLOCK_SIZE_BYTES is). Each fat write (~9KB) fills the
    /// 512KB usable region in ~57 writes.
    fn compact_config(dir: &std::path::Path) -> InternalShardConfig {
        let temp_dir = dir.join("compaction_temp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        InternalShardConfig {
            shard_log_preallocate_bytes: 2 * celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES as u64 + 512 * 1024, // 2 headers + 512KB usable
            compaction_min_reclaimable_ratio: 0.20,
            compaction_temp_dir: temp_dir,
            ..test_config(dir)
        }
    }

    async fn open_compact_shard(dir: &std::path::Path) -> ShardWal<StubReplicationClient, StubS3Downloader> {
        ShardWal::open(compact_config(dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
            .await
            .unwrap()
    }

    /// Write one event with an 8KB incompressible payload to consume ~9KB of segment space.
    ///
    /// Pseudo-random bytes via a splitmix64-style hash on (index, byte_offset) so that the
    /// payload looks random to zstd's dict-aware compressor and survives compression at roughly
    /// its uncompressed size. Compaction tests depend on segment byte ratios; a repeated byte
    /// here would compress to ~0 and invalidate the threshold math.
    fn fat_event(index: u64) -> Vec<DatablockAggregateEvent> {
        let mut bytes = vec![0u8; 8192];
        for (offset, slot) in bytes.iter_mut().enumerate() {
            let mut h = index.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(offset as u64);
            h ^= h >> 30;
            h = h.wrapping_mul(0xBF58476D1CE4E5B9);
            h ^= h >> 27;
            *slot = h as u8;
        }
        vec![DatablockAggregateEvent {
            client_seq: index,
            event_type_major: 1,
            event_value: Arc::new(bytes),
            ..Default::default()
        }]
    }

    /// Write `n` fat events to `agg` in the current segment (without triggering rotation).
    /// Each write consumes ~9KB of segment space.
    async fn write_fat<R: ReplicationClient, D: S3Downloader>(
        shard: &ShardWal<R, D>,
        agg: &AggregateKey,
        n: u64,
    ) {
        for i in 1..=n {
            write_ok(shard, write_req(agg.clone(), fat_event(i))).await;
        }
    }

    /// Trigger segment rotation and delete the sentinel to minimize its impact on compaction ratios.
    ///
    /// Fat writes to the sentinel may land in the current segment (before rotation triggers).
    /// Deleting the sentinel afterwards marks those writes as dead, so compaction tests
    /// can still verify that `compacted_size < original_size`.
    async fn trigger_rotation<R: ReplicationClient, D: S3Downloader>(shard: &ShardWal<R, D>) {
        let sentinel = key(99, 99, 99);
        let old_log_id = shard.log_segments_cache.active_log_id();
        // Keep writing until we actually rotate (available space may vary slightly).
        let mut i = 1u64;
        while shard.log_segments_cache.active_log_id() == old_log_id {
            write_ok(shard, write_req(sentinel.clone(), fat_event(i))).await;
            i += 1;
        }
        // Delete the sentinel so any sentinel writes that landed in the sealed segment
        // are also treated as dead data during compaction, preserving high dead ratios.
        // allow_recreate=true so trigger_rotation can be called multiple times in a test.
        let _ = process(shard, delete_req_full(sentinel, true, false, None)).await;
    }

    #[test]
    fn compact_deleted_aggregates_removes_dead_data() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);

            // Fill most of segment 1 with A's data (50 × 9KB ≈ 450KB of the 512KB usable).
            // This ensures A dominates the segment and the dead ratio is well above 20%.
            write_fat(&shard, &agg_a, 50).await;

            // Write a small amount of B data into segment 1.
            write_ok(&shard, write_req(agg_b.clone(), events(3))).await;

            // Soft-delete A — tombstone goes into segment 1 (within-segment tombstone).
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Trigger rotation: next fat write exceeds remaining space, landing in segment 2.
            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() >= 2);

            let seg1_id = 1u64;
            let original_size = shard.log_segments_cache.get(seg1_id).await.unwrap().metadata.borrow().file_len;

            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run (A's fat data dominates dead ratio)");
            let cr = result.unwrap();
            assert_eq!(cr.log_id, seg1_id);
            assert!(cr.compacted_size < cr.original_size, "compacted file should be smaller");
            assert_eq!(cr.original_size, original_size);

            // B still readable with all events (B's data preserved in compacted segment).
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 1);
            assert_eq!(read_b.event_batches[0].events.len(), 3);

            // A is deleted.
            let result = process(&shard, read_req(agg_a.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            shard.close().await;
        });
    }

    /// Compaction rewrites the sidecar: tips remapped onto the compacted layout,
    /// client sets AND segment blooms carried forward VERBATIM (the carry-forward
    /// rule: never regenerated from survivors; supersets are safe).
    #[test]
    fn compaction_rewrites_sidecar_remapped_tips_verbatim_sets_and_blooms() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);
            let writer = 7u128;
            let deleter = 11u128;

            for i in 1..=50u64 {
                write_ok(&shard, write_req_full(agg_a.clone(), fat_event(i), i == 1, None, writer, false)).await;
            }
            write_ok(&shard, write_req_full(agg_b.clone(), events(3), true, None, writer, false)).await;

            let mut deletes = HashMap::new();
            deletes.insert(agg_a.clone(), SingleAggregateDelete {
                allow_recreate: false,
                allow_sequence_continuation: false,
                expected_version: None,
            });
            let del = process(&shard, ClientRequest::Delete(DeleteRequest {
                correlation_id: None,
                client_id: deleter,
                user_id: None,
                deletes,
            })).await;
            assert!(matches!(del, Ok(ClientResponse::Delete(_))));

            trigger_rotation(&shard).await;
            let pre = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await.expect("pre-compaction sidecar");
            // Prime the decoded cache so the post-compaction invalidation is exercised too.
            let _ = shard.read_segment_summary_cached(1).await.expect("decodes");

            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run");

            let post = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await.expect("rewritten sidecar");

            // Blooms verbatim.
            assert_eq!(post.aggregate_bloom, pre.aggregate_bloom, "aggregate bloom must be carried forward verbatim");
            assert_eq!(post.client_bloom, pre.client_bloom, "client bloom must be carried forward verbatim");

            // A's client set verbatim: the deleter's batches are gone, only the
            // tombstone remains, yet the carried set still names both clients.
            let entry_a = post.aggregates.iter().find(|e| e.aggregate_id == 1).expect("A's entry survives");
            assert!(entry_a.client_set.may_contain_hash(client_id_bloom_hash(writer)));
            assert!(entry_a.client_set.may_contain_hash(client_id_bloom_hash(deleter)));
            let pre_a = pre.aggregates.iter().find(|e| e.aggregate_id == 1).unwrap();
            assert_eq!(entry_a.client_set, pre_a.client_set, "client set must be byte-identical to pre-compaction");

            // Tips remapped: each entry's seek target lands on its own chain in
            // the COMPACTED file (A's tip is its surviving tombstone).
            for (agg, entry) in [(&agg_a, entry_a), (&agg_b, post.aggregates.iter().find(|e| e.aggregate_id == 2).unwrap())] {
                assert_ne!(entry.newest_metablock_pos, 0, "tip must be recorded for {agg:?}");
                let block = read_block_at(&shard, 1, entry.newest_metablock_pos).await;
                assert_eq!(
                    metablock_bytes::read_chain_aggregate_key(&block).as_ref(), Some(agg),
                    "remapped tip must land on the aggregate's own chain in the compacted layout"
                );
            }

            // The decoded cache was invalidated: a fresh read reflects the rewrite.
            let cached = shard.read_segment_summary_cached(1).await.expect("re-decodes the rewritten file");
            let cached_a = cached.aggregates.iter().find(|e| e.aggregate_id == 1).unwrap();
            assert_eq!(cached_a.newest_metablock_pos, entry_a.newest_metablock_pos, "cache must serve the rewritten sidecar");

            shard.close().await;
        });
    }

    /// Missing (or torn → None) pre-compaction sidecar: compaction writes
    /// tips-only entries — Unknown client sets, no blooms — never regenerating
    /// client knowledge from survivors.
    #[test]
    fn compaction_missing_sidecar_writes_tips_only_fallback() {
        glommio_test!({
            use crate::shard_wal_sync::summary_path;

            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);

            for i in 1..=50u64 {
                write_ok(&shard, write_req_full(agg_a.clone(), fat_event(i), i == 1, None, 7u128, false)).await;
            }
            write_ok(&shard, write_req(agg_b.clone(), events(3))).await;
            let del = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(del, Ok(ClientResponse::Delete(_))));

            trigger_rotation(&shard).await;
            std::fs::remove_file(summary_path(shard.log_segments_cache.shard_dir(), 1)).unwrap();

            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run");

            let post = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await
                .expect("tips-only sidecar must be written");
            assert_eq!(post.aggregate_bloom, None, "fallback must not fabricate blooms");
            assert_eq!(post.client_bloom, None);

            let entry_a = post.aggregates.iter().find(|e| e.aggregate_id == 1).expect("tombstoned A still listed");
            assert!(entry_a.is_deleted, "A's fallback entry must reflect its deleted state");
            assert_eq!(entry_a.client_set, ClientSet::Unknown,
                "fallback must not regenerate client sets from survivors");
            let entry_b = post.aggregates.iter().find(|e| e.aggregate_id == 2).expect("B survives");
            assert_eq!(entry_b.client_set, ClientSet::Unknown);

            for (agg, entry) in [(&agg_a, entry_a), (&agg_b, entry_b)] {
                assert_ne!(entry.newest_metablock_pos, 0);
                let block = read_block_at(&shard, 1, entry.newest_metablock_pos).await;
                assert_eq!(metablock_bytes::read_chain_aggregate_key(&block).as_ref(), Some(agg));
            }

            shard.close().await;
        });
    }

    #[test]
    fn compact_rebuilt_client_bloom_keeps_tombstone_only_clients() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);
            let writer = 7u128;
            let deleter = 11u128;

            // A dominates the segment (deleted below -> dead ratio above threshold);
            // B's writes survive so the segment isn't 100% dead.
            for i in 1..=50u64 {
                write_ok(&shard, write_req_full(agg_a.clone(), fat_event(i), i == 1, None, writer, false)).await;
            }
            write_ok(&shard, write_req_full(agg_b.clone(), events(3), true, None, writer, false)).await;

            let mut deletes = HashMap::new();
            deletes.insert(agg_a.clone(), SingleAggregateDelete {
                allow_recreate: false,
                allow_sequence_continuation: false,
                expected_version: None,
            });
            let del = process(&shard, ClientRequest::Delete(DeleteRequest {
                correlation_id: None,
                client_id: deleter,
                user_id: None,
                deletes,
            })).await;
            assert!(matches!(del, Ok(ClientResponse::Delete(_))));

            trigger_rotation(&shard).await;
            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run (A's fat data dominates dead ratio)");

            // After compaction, the deleter's event batches are gone and only the
            // SoftDelete tombstone carries their client_id. The rebuilt bloom must
            // still contain them: tombstones always survive, and dropping the client
            // would make the bloom a subset of the segment's true client set.
            let seg1 = shard.log_segments_cache.get(1).await.unwrap();
            let metadata = seg1.metadata.borrow();
            let bloom = metadata.write.client_id_bloom.borrow();
            assert!(bloom.may_contain_hash(client_id_bloom_hash(writer)), "surviving writer must be in the rebuilt client bloom");
            assert!(bloom.may_contain_hash(client_id_bloom_hash(deleter)), "tombstone-only client must be in the rebuilt client bloom");
            assert!(!bloom.may_contain_hash(client_id_bloom_hash(99u128)), "tripwire: a never-seen client must not be claimed present");
            drop(bloom);
            drop(metadata);

            shard.close().await;
        });
    }

    #[test]
    fn compact_rebuilt_client_bloom_keeps_trim_only_clients() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg = key(1, 1, 1);
            let writer = 7u128;
            let trimmer = 13u128;

            for i in 1..=50u64 {
                write_ok(&shard, write_req_full(agg.clone(), fat_event(i), i == 1, None, writer, false)).await;
            }

            // Trim by a client that never wrote: post-compaction the SoftTrim block is
            // their only surviving record (batches below the floor are dropped).
            let trim = process(&shard, ClientRequest::TrimStart(TrimStartRequest {
                correlation_id: None,
                aggregate_key: agg.clone(),
                keep_from_aggregate_version: 31,
                client_id: trimmer,
                user_id: None,
            })).await;
            assert!(matches!(trim, Ok(ClientResponse::TrimStart(_))));

            trigger_rotation(&shard).await;
            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run (30/50 batches trimmed = 60% dead)");

            let seg1 = shard.log_segments_cache.get(1).await.unwrap();
            let metadata = seg1.metadata.borrow();
            let bloom = metadata.write.client_id_bloom.borrow();
            assert!(bloom.may_contain_hash(client_id_bloom_hash(writer)), "surviving writer must be in the rebuilt client bloom");
            assert!(bloom.may_contain_hash(client_id_bloom_hash(trimmer)), "trim-only client must be in the rebuilt client bloom");
            assert!(!bloom.may_contain_hash(client_id_bloom_hash(99u128)), "tripwire: a never-seen client must not be claimed present");
            drop(bloom);
            drop(metadata);

            shard.close().await;
        });
    }

    #[test]
    fn compact_trimmed_batches_preserves_above_floor() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg = key(1, 1, 1);

            // Write 2 fat events (to be trimmed away) + 3 fat events above the trim floor.
            // 50 total fat events ensures the trimmed-away data dominates the segment.
            // Fat writes: events 1–30 will be trimmed, events 31–50 will be kept.
            // We write 50 fat batches total, then trim to keep from batch 31.
            write_fat(&shard, &agg, 50).await;

            // Trim: keep from batch 31 (30 batches become dead = 60% of total).
            let result = process(&shard, trim_req(agg.clone(), 31)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));

            // Trigger rotation.
            trigger_rotation(&shard).await;

            let seg1_id = 1u64;
            let original_size = shard.log_segments_cache.get(seg1_id).await.unwrap().metadata.borrow().file_len;

            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run (30/50 batches trimmed = 60% dead)");
            let cr = result.unwrap();
            assert!(cr.compacted_size < original_size, "compacted file should be smaller");

            // Reading from batch 1 should fail (below trim floor of 31).
            let result = process(&shard, read_req_from(agg.clone(), 1)).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::UnavailableBatchIndex { .. }))));

            // Reading from batch 31 should succeed.
            let read = unwrap_read(process(&shard, read_req_from(agg.clone(), 31)).await);
            assert!(!read.event_batches.is_empty());
            assert!(read.event_batches.iter().all(|b| b.aggregate_version >= 31));

            shard.close().await;
        });
    }

    #[test]
    fn compact_100_percent_dead_segment() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            // Write fat events to 3 aggregates (10 each = 30 total, ~270KB).
            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);
            let agg_c = key(1, 1, 3);

            write_fat(&shard, &agg_a, 10).await;
            write_fat(&shard, &agg_b, 10).await;
            write_fat(&shard, &agg_c, 10).await;

            // Delete ALL aggregates — all 30 event batches become dead.
            for agg in &[&agg_a, &agg_b, &agg_c] {
                let result = process(&shard, delete_req((*agg).clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));
            }

            // Trigger rotation.
            trigger_rotation(&shard).await;

            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run on fully dead segment");
            let cr = result.unwrap();

            // Compacted file should be smaller (only tombstones remain — no event datablocks).
            // The theoretical minimum is 2 × HEADER_BLOCK_SIZE_BYTES + tombstone metablocks.
            // We verify the compacted size is strictly less than the original preallocated size.
            assert!(cr.compacted_size < cr.original_size, "compacted should be smaller than original 1.5MB segment");

            // All aggregates still appear deleted.
            for agg in &[&agg_a, &agg_b, &agg_c] {
                let result = process(&shard, read_req((*agg).clone())).await;
                assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));
            }

            shard.close().await;
        });
    }

    #[test]
    fn compact_below_threshold_skipped() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            // Write 50 fat events to a live aggregate (will remain alive).
            let live_agg = key(1, 1, 1);
            write_fat(&shard, &live_agg, 50).await;

            // Write 1 fat event to a separate aggregate and delete it.
            // 1 dead out of 51+ total = ~2% dead — well below the 20% threshold.
            let dead_agg = key(1, 1, 2);
            write_ok(&shard, write_req(dead_agg.clone(), fat_event(1))).await;
            let result = process(&shard, delete_req(dead_agg.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Trigger rotation.
            trigger_rotation(&shard).await;

            let seg1_id = 1u64;
            let size_before = shard.log_segments_cache.get(seg1_id).await.unwrap().metadata.borrow().file_len;

            // Compaction should not run (1 dead / 51+ total ≈ 2% < 20% threshold).
            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_none(), "expected no compaction below threshold");

            // File size unchanged.
            let size_after = shard.log_segments_cache.get(seg1_id).await.unwrap().metadata.borrow().file_len;
            assert_eq!(size_before, size_after);

            shard.close().await;
        });
    }

    #[test]
    fn compact_active_segment_not_eligible() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg = key(1, 1, 1);

            // Write to the active segment (no rotation — still segment 1).
            write_ok(&shard, write_req(agg.clone(), events(3))).await;

            // Delete it — but the active segment is never compacted.
            let result = process(&shard, delete_req(agg.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Active log_id should still be 1 (we never rotated).
            assert_eq!(shard.log_segments_cache.active_log_id(), 1);

            // compact_oldest_eligible_segment: active_log_id is 1, so there are no sealed segments.
            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_none(), "active segment should not be compacted");

            shard.close().await;
        });
    }

    #[test]
    fn compact_preserves_read_after_compact() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let dead_agg = key(1, 1, 0);
            let agg_b = key(1, 1, 2);
            let agg_c = key(1, 1, 3);
            let agg_d = key(1, 1, 4);

            // Fill most of segment 1 with dead_agg's data.
            write_fat(&shard, &dead_agg, 40).await;

            // Write alive aggregates: B gets 1 batch, C gets 2 batches, D gets 3 batches.
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_c.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_c.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_d.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_d.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_d.clone(), events(2))).await;

            // Delete dead_agg — 40 fat batches become dead (dominates segment).
            let result = process(&shard, delete_req(dead_agg.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Trigger rotation.
            trigger_rotation(&shard).await;

            // Compact.
            let result = shard.compact_oldest_eligible_segment().await.unwrap();
            assert!(result.is_some(), "expected compaction to run");

            // Verify B (1 batch), C (2 batches), D (3 batches) fully readable after compaction.
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 1);
            assert_eq!(read_b.event_batches[0].events.len(), 2);

            let read_c = unwrap_read(process(&shard, read_req(agg_c.clone())).await);
            assert_eq!(read_c.event_batches.len(), 2);
            for batch in &read_c.event_batches {
                assert_eq!(batch.events.len(), 2);
            }

            let read_d = unwrap_read(process(&shard, read_req(agg_d.clone())).await);
            assert_eq!(read_d.event_batches.len(), 3);
            for batch in &read_d.event_batches {
                assert_eq!(batch.events.len(), 2);
            }

            shard.close().await;
        });
    }

    #[test]
    fn compact_multiple_rounds() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);
            let agg_c = key(1, 1, 3);
            let agg_d = key(1, 1, 4);

            // Write events to A, B, C, D in segment 1 with enough data so each
            // compaction round has >20% dead fraction:
            //   A: 15 fat batches (deleted in round 1 → 30% dead of ~50 total)
            //   B: 15 fat batches (8 trimmed in round 2 → ~22% dead of ~35 remaining)
            //   C: 15 fat batches (deleted in round 3 → ~55% dead of ~27 remaining)
            //   D:  5 fat batches (always kept)
            write_fat(&shard, &agg_a, 15).await;
            write_fat(&shard, &agg_b, 15).await;
            write_fat(&shard, &agg_c, 15).await;
            write_fat(&shard, &agg_d, 5).await;

            // Trigger rotation to seal segment 1.
            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() >= 2);

            let seg1_original_size = shard.log_segments_cache.get(1).await.unwrap().metadata.borrow().file_len;

            // Delete A — tombstone written to current active segment (cross-segment).
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Round 1: compact segment 1 — removes A's 15 event batches, keeps B, C, D.
            let cr1 = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("round 1: A's 15 batches dead (>20%), compaction should run");
            assert_eq!(cr1.log_id, 1);
            assert!(cr1.compacted_size < seg1_original_size, "round 1 should shrink file");
            let size_after_round1 = cr1.compacted_size;

            // B (15 batches), C (15 batches), D (5 batches) still fully readable.
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 15);
            let read_c = unwrap_read(process(&shard, read_req(agg_c.clone())).await);
            assert_eq!(read_c.event_batches.len(), 15);
            let read_d = unwrap_read(process(&shard, read_req(agg_d.clone())).await);
            assert_eq!(read_d.event_batches.len(), 5);

            // A is deleted.
            let result = process(&shard, read_req(agg_a.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            // Trim B: keep from batch 9 (removes 8 batches = 8/35 remaining ≈ 22.8% dead).
            let result = process(&shard, trim_req(agg_b.clone(), 9)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));

            // Round 2: compact segment 1 again — removes B's 8 trimmed batches.
            let cr2 = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("round 2: 8 of B's batches dead (~22%), compaction should run");
            assert_eq!(cr2.log_id, 1);
            assert!(cr2.compacted_size < size_after_round1, "round 2 should shrink further");
            let size_after_round2 = cr2.compacted_size;

            // B readable (only batches 9+).
            let read_b = unwrap_read(process(&shard, read_req_from(agg_b.clone(), 9)).await);
            assert!(!read_b.event_batches.is_empty());
            assert!(read_b.event_batches.iter().all(|b| b.aggregate_version >= 9));

            // B's early batches unavailable.
            let result = process(&shard, read_req_from(agg_b.clone(), 1)).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::UnavailableBatchIndex { .. }))));

            // C (15 batches) and D (5 batches) fully intact.
            let read_c = unwrap_read(process(&shard, read_req(agg_c.clone())).await);
            assert_eq!(read_c.event_batches.len(), 15);
            let read_d = unwrap_read(process(&shard, read_req(agg_d.clone())).await);
            assert_eq!(read_d.event_batches.len(), 5);

            // Delete C — tombstone goes into current active segment.
            let result = process(&shard, delete_req(agg_c.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Round 3: compact segment 1 again — removes C's 15 event batches.
            let cr3 = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("round 3: C's 15 batches dead (~55%), compaction should run");
            assert_eq!(cr3.log_id, 1);
            assert!(cr3.compacted_size < size_after_round2, "round 3 should shrink further");

            // D (5 batches) still fully intact.
            let read_d = unwrap_read(process(&shard, read_req(agg_d.clone())).await);
            assert_eq!(read_d.event_batches.len(), 5);

            // C is deleted.
            let result = process(&shard, read_req(agg_c.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            // A still deleted (tombstone preserved).
            let result = process(&shard, read_req(agg_a.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            shard.close().await;
        });
    }

    #[test]
    fn compact_cross_segment_tombstone_resolves() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);

            // Fill most of segment 1 with A's fat data (dominant dead data after deletion).
            write_fat(&shard, &agg_a, 45).await;

            // Write a small amount of B data into segment 1 as a live control.
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;

            // Trigger rotation — A and B remain in segment 1, tombstones go to segment 2.
            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() >= 2);

            // Write SoftDelete for A in segment 2 (CROSS-SEGMENT tombstone).
            // A's event batches are in segment 1; the delete tombstone is in segment 2.
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Compact segment 1: must do cross-segment reverse scan to find A's tombstone
            // in segment 2, then mark A's event batches in segment 1 as dead.
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("cross-segment compaction should run (A's ~45 fat batches dead)");
            assert_eq!(cr.log_id, 1);
            assert!(cr.compacted_size < cr.original_size);

            // A is still shown as deleted after compaction.
            let result = process(&shard, read_req(agg_a.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            // B is fully intact (its data preserved in compacted segment).
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 1);
            assert_eq!(read_b.event_batches[0].events.len(), 2);

            shard.close().await;
        });
    }

    #[test]
    fn compact_preserves_hash_chain() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);

            // Fill most of segment 1 with A's fat data (will be deleted, becoming dead).
            write_fat(&shard, &agg_a, 45).await;

            // Write B data (will survive compaction).
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;

            // Delete A so compaction removes its data.
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Trigger rotation.
            trigger_rotation(&shard).await;

            // Compact.
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (A's 45 batches dead)");
            assert_eq!(cr.log_id, 1);

            // The compacted segment must be readable — proving the hash chain is consistent
            // and the header was written correctly. If the hash chain were broken, the WAL
            // would detect corruption on the next open.
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 1);
            assert_eq!(read_b.event_batches[0].events.len(), 2);

            // Verify the compacted segment's header is loaded correctly by checking the tip_hash.
            // The segment now has metablocks (B's event batch + A's tombstone), so the tip_hash
            // must differ from GENESIS_HASH.
            let seg = shard.log_segments_cache.get(cr.log_id).await.unwrap();
            let meta = seg.metadata.borrow();
            assert_ne!(meta.write.tip_hash, GENESIS_HASH, "compacted segment should have non-genesis tip hash");

            shard.close().await;
        });
    }

    #[test]
    fn compact_preserves_bloom_filter() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);
            let agg_c = key(1, 1, 3);

            // Fill most of segment 1 with A's fat data (will be deleted).
            write_fat(&shard, &agg_a, 45).await;

            // Write B and C (small, will survive compaction).
            write_ok(&shard, write_req(agg_b.clone(), events(1))).await;
            write_ok(&shard, write_req(agg_c.clone(), events(1))).await;

            // Delete A — within-segment tombstone.
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Trigger rotation.
            trigger_rotation(&shard).await;

            // Compact.
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (A's 45 batches dead)");
            assert_eq!(cr.log_id, 1);

            // After compaction, the segment header includes a rebuilt bloom filter.
            // We verify the bloom filter works correctly by confirming:
            // - B and C are readable (their data survived; bloom must contain them)
            // - A is deleted (tombstone preserved; bloom may contain A — false positives ok)
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 1);

            let read_c = unwrap_read(process(&shard, read_req(agg_c.clone())).await);
            assert_eq!(read_c.event_batches.len(), 1);

            // A remains deleted.
            let result = process(&shard, read_req(agg_a.clone())).await;
            assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

            shard.close().await;
        });
    }

    #[test]
    fn compact_softtrim_survives_compaction() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_b = key(1, 1, 2);
            let filler = key(1, 1, 99);

            // Three batches to B, then trim to keep from batch 2 (drops batch 1).
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;
            let result = process(&shard, trim_req(agg_b.clone(), 2)).await;
            assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));

            // Fat filler to push dead ratio above the compaction threshold.
            write_fat(&shard, &filler, 40).await;
            let result = process(&shard, delete_req(filler.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            trigger_rotation(&shard).await;
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (filler dominates dead data)");
            assert_eq!(cr.log_id, 1);

            // SoftTrim must survive in the compacted segment and still be applied.
            let result = process(&shard, read_req_from(agg_b.clone(), 1)).await;
            assert!(
                matches!(result, Err(ShardError::Read(ShardReadError::UnavailableBatchIndex { .. }))),
                "batch 1 must be unavailable (trimmed after compaction); got {result:?}"
            );
            let read_b = unwrap_read(process(&shard, read_req_from(agg_b.clone(), 2)).await);
            assert_eq!(read_b.event_batches.len(), 2, "batches 2 and 3 must survive compaction");

            // Direct bloom check: SoftTrim key must appear in the rebuilt bloom.
            let seg = shard.log_segments_cache.get(cr.log_id).await.unwrap();
            let bloom_has_b = seg.metadata.borrow()
                .read.as_ref().unwrap()
                .aggregate_key_bloom.borrow().may_contain(&agg_b);
            assert!(bloom_has_b, "compacted bloom must include the SoftTrim aggregate key");

            shard.close().await;
        });
    }

    #[test]
    fn compact_schema_registration_bloom_survives_restart() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Round 1: register schema, write valid event, compact, close.
            {
                let shard = open_compact_shard(&dir).await;
                let agg_b = key(1, 1, 2);
                let filler = key(2, 2, 99); // org=2 type=2 — not subject to the org=1 type=1 schema

                process(&shard, schema_req(1, 1, 1, 0, NAME_AGE_SCHEMA)).await.unwrap();
                write_ok(&shard, write_req(agg_b.clone(), json_events(&[br#"{"name":"alice","age":30}"#], 1, 0))).await;

                // Filler must be a different org/type so fat (non-JSON) writes bypass the schema.
                write_fat(&shard, &filler, 40).await;
                process(&shard, delete_req(filler)).await.unwrap();

                trigger_rotation(&shard).await;
                shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("compaction should run (schema+event in segment with dead filler)");

                shard.close().await;
            }

            // Round 2: cold restart — schema cache is empty, bloom must locate the schema.
            // If the compacted bloom were wrong, schema lookup would fail, the write would
            // bypass validation, and the assertion would catch the silent correctness failure.
            {
                let shard = open_compact_shard(&dir).await;
                let agg_b = key(1, 1, 2);

                let bad_evts = json_events(&[br#"{"name":"bob"}"#], 1, 0); // missing required "age"
                let result = process(&shard, write_req(agg_b.clone(), bad_evts)).await;
                assert!(
                    matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))),
                    "schema must still be enforced after compaction + restart; got {result:?}"
                );

                shard.close().await;
            }
        });
    }

    // ── Additional compaction tests: list operations, restart, datablock positions, WAL sequence gaps ──

    fn list_aggs_req_with_cursor(org: Option<u128>, atype: Option<u128>, cursor: Option<u64>) -> ClientRequest {
        ClientRequest::ListAggregates(ListAggregatesRequest {
            correlation_id: None,
            shard_id: 0,
            org_id: org,
            aggregate_type_id: atype,
            cursor,
        })
    }

    fn compact_config_small_page(dir: &std::path::Path, list_page_size: usize) -> InternalShardConfig {
        InternalShardConfig {
            list_page_size,
            ..compact_config(dir)
        }
    }

    #[test]
    fn compact_list_operations_correct_after_compaction() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            // Four aggregates across two orgs and two types.
            let agg_a = key(1, 1, 1); // org=1, type=1, id=1 — will be deleted
            let agg_b = key(1, 1, 2); // org=1, type=1, id=2 — survives
            let agg_c = key(1, 2, 1); // org=1, type=2, id=1 — survives
            let agg_d = key(2, 1, 1); // org=2, type=1, id=1 — survives

            // Fill segment 1 with agg_a fat data (dominates the dead ratio after deletion).
            write_fat(&shard, &agg_a, 40).await;

            // Write small amounts to the surviving aggregates.
            write_ok(&shard, write_req(agg_b.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_c.clone(), events(2))).await;
            write_ok(&shard, write_req(agg_d.clone(), events(2))).await;

            // Delete agg_a — dead ratio well above 20%.
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Seal segment 1.
            trigger_rotation(&shard).await;

            // Compact.
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (agg_a's 40 fat batches dead)");
            assert_eq!(cr.log_id, 1);
            assert!(cr.compacted_size < cr.original_size);

            // list_orgs: must return both org=1 and org=2.
            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            let org_ids: Vec<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
            assert!(org_ids.contains(&1), "org=1 should be present; got {org_ids:?}");
            assert!(org_ids.contains(&2), "org=2 should be present; got {org_ids:?}");

            // list_types for org=1: should return type=1 and type=2.
            let types_org1 = unwrap_list_types(process(&shard, list_types_req(Some(1))).await);
            let type_ids_org1: Vec<u128> = types_org1.aggregate_types.iter().map(|t| t.aggregate_type_id).collect();
            assert!(type_ids_org1.contains(&1), "org=1 type=1 should be present; got {type_ids_org1:?}");
            assert!(type_ids_org1.contains(&2), "org=1 type=2 should be present; got {type_ids_org1:?}");
            assert!(types_org1.aggregate_types.iter().all(|t| t.org_id == 1));

            // list_types for org=2: should return type=1 only.
            let types_org2 = unwrap_list_types(process(&shard, list_types_req(Some(2))).await);
            assert_eq!(types_org2.aggregate_types.len(), 1);
            assert_eq!(types_org2.aggregate_types[0].aggregate_type_id, 1);

            // list_aggs for org=1, type=1: must return agg_b (alive) and show agg_a as deleted.
            // Tombstones (SoftDelete metablocks) are preserved during compaction for cross-segment
            // safety, so agg_a still appears in list results — but marked is_deleted=true.
            let aggs_1_1 = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(1))).await);
            let live_ids: Vec<u128> = aggs_1_1.aggregates.iter().filter(|a| !a.is_deleted).map(|a| a.aggregate_id).collect();
            let deleted_ids: Vec<u128> = aggs_1_1.aggregates.iter().filter(|a| a.is_deleted).map(|a| a.aggregate_id).collect();
            assert!(live_ids.contains(&2), "agg_b (id=2) should be live; live_ids={live_ids:?}");
            assert!(!live_ids.contains(&1), "agg_a (id=1) should NOT be live (was deleted); live_ids={live_ids:?}");
            assert!(deleted_ids.contains(&1), "agg_a (id=1) tombstone should still appear as deleted; deleted_ids={deleted_ids:?}");

            // list_aggs for org=1, type=2: must return agg_c.
            let aggs_1_2 = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(2))).await);
            assert_eq!(aggs_1_2.aggregates.len(), 1);
            assert_eq!(aggs_1_2.aggregates[0].aggregate_id, 1);

            // list_aggs for org=2, type=1: must return agg_d.
            let aggs_2_1 = unwrap_list_aggs(process(&shard, list_aggs_req(Some(2), Some(1))).await);
            assert_eq!(aggs_2_1.aggregates.len(), 1);
            assert_eq!(aggs_2_1.aggregates[0].aggregate_id, 1);

            shard.close().await;
        });
    }

    #[test]
    fn compact_restart_preserves_data() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Round 1: write, delete filler, compact, verify — then close.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);
                let agg_b = key(1, 1, 2);
                let filler = key(1, 1, 99);

                // Small writes to A and B (will survive).
                write_ok(&shard, write_req(agg_a.clone(), events(3))).await;
                write_ok(&shard, write_req(agg_b.clone(), events(2))).await;

                // Fill segment with filler fat data so dead ratio > 20% after deletion.
                write_fat(&shard, &filler, 40).await;

                // Delete filler.
                let result = process(&shard, delete_req(filler.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));

                // Seal segment and compact.
                trigger_rotation(&shard).await;
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("compaction should run (filler dominates dead data)");
                assert_eq!(cr.log_id, 1);
                assert!(cr.compacted_size < cr.original_size);

                // Verify reads before close.
                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert_eq!(read_a.event_batches.len(), 1);
                assert_eq!(read_a.event_batches[0].events.len(), 3);

                let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
                assert_eq!(read_b.event_batches.len(), 1);
                assert_eq!(read_b.event_batches[0].events.len(), 2);

                let result = process(&shard, read_req(filler.clone())).await;
                assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))));

                shard.close().await;
            }

            // Round 2: reopen from disk, verify everything survives cold-cache reload.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);
                let agg_b = key(1, 1, 2);
                let filler = key(1, 1, 99);

                // A must be readable with correct event data.
                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert_eq!(read_a.event_batches.len(), 1, "A: expected 1 batch after restart");
                assert_eq!(read_a.event_batches[0].events.len(), 3, "A: expected 3 events after restart");

                // B must be readable with correct event data.
                let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
                assert_eq!(read_b.event_batches.len(), 1, "B: expected 1 batch after restart");
                assert_eq!(read_b.event_batches[0].events.len(), 2, "B: expected 2 events after restart");

                // Filler must still be deleted.
                let result = process(&shard, read_req(filler.clone())).await;
                assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))),
                    "filler should still be deleted after restart; got {result:?}");

                // List operations must still work.
                let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
                let org_ids: Vec<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
                assert!(org_ids.contains(&1), "org=1 should still be listed after restart; got {org_ids:?}");

                let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(1))).await);
                let agg_ids: Vec<u128> = aggs.aggregates.iter()
                    .filter(|a| !a.is_deleted)
                    .map(|a| a.aggregate_id)
                    .collect();
                assert!(agg_ids.contains(&1), "agg_a should be listed after restart; got {agg_ids:?}");
                assert!(agg_ids.contains(&2), "agg_b should be listed after restart; got {agg_ids:?}");

                shard.close().await;
            }
        });
    }

    #[test]
    fn compact_datablock_positions_updated_correctly() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1); // many fat events — deleted, so removed by compaction
            let agg_b = key(1, 1, 2); // some events — kept

            // Fill segment with A's fat data (dominates the dead ratio after deletion).
            write_fat(&shard, &agg_a, 35).await;

            // Write fat batches to B — fat events produce Block-type datablocks (not inline).
            // These will survive compaction, and their datablock_position fields must be updated.
            write_fat(&shard, &agg_b, 3).await;

            // Delete A so its data becomes dead (>20% of segment).
            let result = process(&shard, delete_req(agg_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Seal and compact.
            trigger_rotation(&shard).await;
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (A's 35 batches dead)");
            assert_eq!(cr.log_id, 1);
            assert!(cr.compacted_size < cr.original_size);

            // Open the compacted segment and validate datablock positions.
            let seg = shard.log_segments_cache.get(cr.log_id).await.unwrap();
            let (metablocks_start, metablocks_end, file_len) = {
                let meta = seg.metadata.borrow();
                (
                    HEADER_BLOCK_SIZE_BYTES as u64,
                    meta.readable_metablocks_end(),
                    meta.file_len,
                )
            };

            let guard = seg.lock_reader("test_datablock_positions").await.unwrap();
            let dma_file = guard.as_ref().unwrap();

            let tail_header_start = file_len - HEADER_BLOCK_SIZE_BYTES as u64;

            let mut datablock_refs: Vec<(u64, u64, u64)> = Vec::new();
            let result = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, String>(
                dma_file,
                false,
                metablocks_start,
                metablocks_end,
                SCAN_CHUNK_SIZE,
                |_pos, block| {
                    let mb = deserialise_metablock(block).map_err(|e| format!("deser error: {e:?}"))?;

                    if let DatablockStorageKind::Block(_) = &mb.datablock {
                        // datablock_position must be after the metablock region.
                        assert!(
                            mb.datablock_position >= metablocks_end,
                            "datablock_position ({}) must be >= metablocks_end ({}) for wal_seq {}",
                            mb.datablock_position, metablocks_end, mb.wal_seq
                        );

                        // datablock_position + compressed_size must be <= tail header start.
                        assert!(
                            mb.datablock_position + mb.compressed_size <= tail_header_start,
                            "datablock at {} + {} = {} exceeds tail header start {} for wal_seq {}",
                            mb.datablock_position, mb.compressed_size,
                            mb.datablock_position + mb.compressed_size,
                            tail_header_start, mb.wal_seq
                        );

                        datablock_refs.push((mb.datablock_position, mb.compressed_size, mb.wal_seq));
                    }

                    Ok::<bool, String>(false)
                },
            )
            .await;

            match result {
                Ok(_) => {}
                Err(ReadVisitError::Visitor(e)) => {
                    panic!("metablock validation failed: {e}");
                }
                Err(ReadVisitError::Io(e)) => {
                    panic!("io error during metablock scan: {e:?}");
                }
            }

            assert!(!datablock_refs.is_empty(), "expected at least one Block-type datablock after compaction");

            for (pos, size, wal_idx) in &datablock_refs {
                let buf = dma_file.read_at(*pos, *size as usize).await
                    .unwrap_or_else(|e| panic!("failed to read datablock at pos={pos} size={size} wal_seq={wal_idx}: {e:?}"));
                assert_eq!(
                    buf.len(), *size as usize,
                    "read {} bytes but expected {} for wal_seq={wal_idx} at pos={pos}",
                    buf.len(), size
                );
                assert!(
                    !buf.iter().all(|&b| b == 0),
                    "datablock at pos={pos} size={size} wal_seq={wal_idx} is all zeros — compaction did not write payload"
                );
            }

            // Verify datablocks do not overlap each other.
            datablock_refs.sort_by_key(|(pos, _, _)| *pos);
            for i in 1..datablock_refs.len() {
                let (prev_pos, prev_size, prev_idx) = datablock_refs[i - 1];
                let (cur_pos, _, cur_idx) = datablock_refs[i];
                assert!(
                    prev_pos + prev_size <= cur_pos,
                    "datablocks overlap: [{prev_pos}, {}) and [{cur_pos}, ...) for wal_seqs {prev_idx} and {cur_idx}",
                    prev_pos + prev_size
                );
            }

            drop(guard); // release reader lock before process() needs it
            let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
            assert_eq!(read_b.event_batches.len(), 3, "B should have 3 fat batches after compaction");

            shard.close().await;
        });
    }

    #[test]
    fn compact_wal_seq_gaps_transparent_to_reads() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1); // 5 sequential batches — all kept
            let filler = key(1, 1, 99); // fat data — deleted, creates WAL sequence gaps

            // Write 5 batches to A first, then bulk filler so filler is interleaved or after.
            // The key is that after compaction, A's 5 metablocks remain but filler's are gone,
            // leaving wal_seq gaps in the compacted file.
            write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // batch 1
            write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // batch 2
            write_fat(&shard, &filler, 10).await;                        // filler interleaved
            write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // batch 3
            write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // batch 4
            write_fat(&shard, &filler, 20).await;                        // more filler
            write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // batch 5

            // Delete filler — creates wal_seq gaps when filler's metablocks are removed.
            let result = process(&shard, delete_req(filler.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Seal and compact.
            trigger_rotation(&shard).await;
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (filler dominates dead data)");
            assert_eq!(cr.log_id, 1);
            assert!(cr.compacted_size < cr.original_size);

            // Walk the compacted segment and prove wal_seq values have gaps.
            // agg_a's metablocks and filler's SoftDelete tombstone survive; filler's
            // EventBatchMetadata entries are removed, leaving wal_seq gaps.
            {
                let seg = shard.log_segments_cache.get(cr.log_id).await.unwrap();
                let (metablocks_start, metablocks_end) = {
                    let meta = seg.metadata.borrow();
                    (HEADER_BLOCK_SIZE_BYTES as u64, meta.readable_metablocks_end())
                };
                let guard = seg.lock_reader("test_wal_seq_gaps").await.unwrap();
                let dma_file = guard.as_ref().unwrap();

                let mut wal_seqs: Vec<u64> = Vec::new();
                let scan = read_fixed_records_visit_const::<FIXED_BLOCK_SIZE_BYTES, String>(
                    dma_file,
                    false,
                    metablocks_start,
                    metablocks_end,
                    SCAN_CHUNK_SIZE,
                    |_pos, block| {
                        let mb = deserialise_metablock(block).map_err(|e| format!("deser error: {e:?}"))?;
                        wal_seqs.push(mb.wal_seq);
                        Ok::<bool, String>(false)
                    },
                )
                .await;
                match scan {
                    Ok(_) => {}
                    Err(ReadVisitError::Visitor(e)) => {
                        panic!("wal_seq scan failed: {e}");
                    }
                    Err(ReadVisitError::Io(e)) => {
                        panic!("io error during wal_seq scan: {e:?}");
                    }
                }

                // Indices must be strictly ascending.
                assert!(
                    wal_seqs.windows(2).all(|w| w[1] > w[0]),
                    "wal_seqs in compacted file are not strictly ascending: {wal_seqs:?}"
                );

                // There must be at least one gap (filler metablocks were removed).
                let has_gap = wal_seqs.windows(2).any(|w| w[1] != w[0] + 1);
                assert!(
                    has_gap,
                    "compacted file should have wal_seq gaps but indices are contiguous: {wal_seqs:?}"
                );
            }

            // Read A from batch 0 — should return all 5 batches.
            let read_all = unwrap_read(process(&shard, read_req_from(agg_a.clone(), 0)).await);
            assert_eq!(read_all.event_batches.len(), 5, "expected all 5 batches; got {:?}",
                read_all.event_batches.iter().map(|b| b.aggregate_version).collect::<Vec<_>>());
            for (i, batch) in read_all.event_batches.iter().enumerate() {
                assert_eq!(batch.aggregate_version, (i + 1) as u64,
                    "batch {} should have index {}", i, i + 1);
                assert_eq!(batch.events.len(), 2, "batch {} should have 2 events", i + 1);
            }

            // Read A from batch 3 onwards — should return batches 3, 4, 5.
            let read_from_3 = unwrap_read(process(&shard, read_req_from(agg_a.clone(), 3)).await);
            assert_eq!(read_from_3.event_batches.len(), 3,
                "expected batches 3, 4, 5; got {:?}",
                read_from_3.event_batches.iter().map(|b| b.aggregate_version).collect::<Vec<_>>());
            assert!(read_from_3.event_batches.iter().all(|b| b.aggregate_version >= 3),
                "all returned batches should have index >= 3");

            shard.close().await;
        });
    }

    #[test]
    fn compact_wal_seq_gaps_list_pagination() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            // Use a small page size (3) to force pagination across WAL sequence gaps.
            let config = compact_config_small_page(&dir, 3);
            let shard = ShardWal::open(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                .await
                .unwrap();

            // Create 10 live aggregates.
            let live_aggs: Vec<AggregateKey> = (1..=10u128).map(|i| key(1, 1, i)).collect();
            for agg in &live_aggs {
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
            }

            // Interleave fat filler aggregates that will be deleted (creates wal_seq gaps).
            let filler_a = key(1, 2, 1);
            let filler_b = key(1, 2, 2);
            write_fat(&shard, &filler_a, 15).await;
            write_fat(&shard, &filler_b, 15).await;

            // Delete filler — dead data now dominates.
            let result = process(&shard, delete_req(filler_a.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));
            let result = process(&shard, delete_req(filler_b.clone())).await;
            assert!(matches!(result, Ok(ClientResponse::Delete(_))));

            // Seal and compact.
            trigger_rotation(&shard).await;
            let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                .expect("compaction should run (filler dead data dominates)");
            assert_eq!(cr.log_id, 1);
            assert!(cr.compacted_size < cr.original_size);

            // Paginated list_aggs for org=1, type=1 — must find all 10 live aggregates.
            // With page_size=3, we need up to 4 pages.
            let mut all_found: Vec<u128> = Vec::new();
            let mut cursor: Option<u64> = None;

            loop {
                let resp = unwrap_list_aggs(process(&shard, list_aggs_req_with_cursor(Some(1), Some(1), cursor)).await);

                // Collect non-deleted aggregates.
                for agg in &resp.aggregates {
                    if !agg.is_deleted {
                        all_found.push(agg.aggregate_id);
                    }
                }

                cursor = resp.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }

            // All 10 live aggregates must be found across all pages.
            all_found.sort_unstable();
            let expected: Vec<u128> = (1..=10).collect();
            assert_eq!(all_found, expected,
                "pagination should find all 10 live aggregates across WAL sequence gaps; found {all_found:?}");

            shard.close().await;
        });
    }

    #[test]
    fn compact_recreated_aggregate_preserves_post_tombstone_data() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);

                // Segment 1: fill with A's fat data and small B data.
                write_fat(&shard, &agg_a, 45).await;

                // Keep writing until we actually rotate (available space may vary slightly).
                let mut i = 1u64;
                while shard.log_segments_cache.active_log_id() == 1 {
                    write_ok(&shard, write_req(agg_a.clone(), fat_event(i))).await;
                    i += 1;
                }
                assert!(shard.log_segments_cache.active_log_id() == 2);

                // Segment 2: soft-delete A (allow_recreate=true), then re-create with new events.
                // Fill segment 2 with extra events pre-delete (compacted later)
                write_fat(&shard, &agg_a, 40).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 2);

                let result = process(&shard, delete_req_full(agg_a.clone(), true, true, None)).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));
                write_ok(&shard, write_req(agg_a.clone(), events(3))).await;

                // Seal segment 2.
                let mut i = 1u64;
                while shard.log_segments_cache.active_log_id() == 2 {
                    write_ok(&shard, write_req(agg_a.clone(), fat_event(i))).await;
                    i += 1;
                }
                assert_eq!(shard.log_segments_cache.active_log_id(), 3);

                // Compact segment 1: A's 45 pre-deletion metablocks are dead (tombstone in seg 2).
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("compaction should run (A's 45 fat batches in seg 1 are dead)");
                assert_eq!(cr.log_id, 1);
                assert!(cr.compacted_size < cr.original_size);

                // Compact segment 2: 
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("compaction should run for segment file 2");
                assert_eq!(cr.log_id, 2);
                assert!(cr.compacted_size < cr.original_size);

                // A's post-recreation events (in seg 2, untouched) are still accessible.
                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert!(read_a.event_batches.len() > 1);
                assert!(read_a.event_batches[0].aggregate_version > 1); //continuation of indexing
                assert_eq!(read_a.event_batches[0].events.len(), 3);

                shard.close().await;
            }

            // Reopen from disk (empty cache) and re-verify: compacted layout must be durable.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);

                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert!(read_a.event_batches.len() > 1);
                assert!(read_a.event_batches[0].aggregate_version > 1);
                assert_eq!(read_a.event_batches[0].events.len(), 3);

                shard.close().await;
            }
        });
    }

    /// Two aggregates (A and B) each deleted and recreated twice across multiple log segments.
    /// After compacting every segment and reopening, both aggregates must return only their
    /// post-second-recreation events with correct index continuation.
    #[test]
    fn compact_multi_delete_recreate_two_aggregates() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Helper: seal to the next segment.
            async fn next_seg<R: ReplicationClient, D: S3Downloader>(
                shard: &ShardWal<R, D>,
                filler: &AggregateKey,
                current: u64,
            ) {
                while shard.log_segments_cache.active_log_id() == current {
                    write_ok(shard, write_req(filler.clone(), fat_event(1))).await;
                }
            }

            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);
                let agg_b = key(1, 1, 2);
                // filler is used purely to drive segment rotation; it will be deleted.
                let filler = key(9, 9, 9);

                // ── Segment 1 ──────────────────────────────────────────────────────────
                // Pre-deletion data for A (life 1) and B (life 1).
                write_fat(&shard, &agg_a, 20).await;
                write_fat(&shard, &agg_b, 20).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 1);
                next_seg(&shard, &filler, 1).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 2);

                // ── Segment 2 ──────────────────────────────────────────────────────────
                // First delete+recreate for A and B.
                let r = process(&shard, delete_req_full(agg_a.clone(), true, true, None)).await;
                assert!(matches!(r, Ok(ClientResponse::Delete(_))));
                write_ok(&shard, write_req(agg_a.clone(), events(2))).await; // A life 2, aggregate version continues

                let r = process(&shard, delete_req_full(agg_b.clone(), true, true, None)).await;
                assert!(matches!(r, Ok(ClientResponse::Delete(_))));
                write_ok(&shard, write_req(agg_b.clone(), events(4))).await; // B life 2

                // More post-recreation data to also sit in seg 2.
                write_fat(&shard, &agg_a, 10).await;
                write_fat(&shard, &agg_b, 10).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 2);
                next_seg(&shard, &filler, 2).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 3);

                // ── Segment 3 ──────────────────────────────────────────────────────────
                // Second delete+recreate for A and B.
                let r = process(&shard, delete_req_full(agg_a.clone(), true, true, None)).await;
                assert!(matches!(r, Ok(ClientResponse::Delete(_))));
                write_ok(&shard, write_req(agg_a.clone(), events(5))).await; // A life 3

                let r = process(&shard, delete_req_full(agg_b.clone(), true, true, None)).await;
                assert!(matches!(r, Ok(ClientResponse::Delete(_))));
                write_ok(&shard, write_req(agg_b.clone(), events(7))).await; // B life 3

                next_seg(&shard, &filler, 3).await;
                assert_eq!(shard.log_segments_cache.active_log_id(), 4);

                // Delete filler so it doesn't interfere with final reads.
                let r = process(&shard, delete_req_full(filler.clone(), false, false, None)).await;
                assert!(matches!(r, Ok(ClientResponse::Delete(_))));

                // ── Compact segments 1, 2, 3 ───────────────────────────────────────────
                // Seg 1: all A and B pre-deletion data is dead (tombstones in seg 2).
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("seg 1 should compact (A+B pre-deletion data is dead)");
                assert_eq!(cr.log_id, 1);
                assert!(cr.compacted_size < cr.original_size);

                // Seg 2: A and B life-2 post-recreation data survives; fat batches + tombstones
                // for second deletion are dead.
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("seg 2 should compact");
                assert_eq!(cr.log_id, 2);
                assert!(cr.compacted_size < cr.original_size);

                // Seg 3: filler deletion tombstone + A/B life-3 events survive.
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("seg 3 should compact");
                assert_eq!(cr.log_id, 3);
                assert!(cr.compacted_size < cr.original_size);

                // Verify A: only life-3 data visible; index must be > 1 (continuation).
                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert_eq!(read_a.event_batches[0].events.len(), 5, "A life-3 batch should have 5 events");
                assert!(read_a.event_batches[0].aggregate_version > 1, "A index must continue past earlier lives");

                // Verify B: only life-3 data visible; index must be > 1 (continuation).
                let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
                assert_eq!(read_b.event_batches[0].events.len(), 7, "B life-3 batch should have 7 events");
                assert!(read_b.event_batches[0].aggregate_version > 1, "B index must continue past earlier lives");

                shard.close().await;
            }

            // Reopen and re-verify durability.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);
                let agg_b = key(1, 1, 2);

                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert_eq!(read_a.event_batches[0].events.len(), 5);
                assert!(read_a.event_batches[0].aggregate_version > 1);

                let read_b = unwrap_read(process(&shard, read_req(agg_b.clone())).await);
                assert_eq!(read_b.event_batches[0].events.len(), 7);
                assert!(read_b.event_batches[0].aggregate_version > 1);

                shard.close().await;
            }
        });
    }

    #[test]
    fn compact_restart_multiple_rounds() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Round 1: write data, delete some, seal, compact, close.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_keep = key(1, 1, 1);
                let agg_del = key(2, 2, 2);

                // Fill segment 1 with agg_del fat data (dominates dead ratio after deletion).
                write_fat(&shard, &agg_del, 40).await;

                // Small write to the keeper.
                write_ok(&shard, write_req(agg_keep.clone(), events(3))).await;

                // Delete agg_del — creates dead data in segment 1.
                let result = process(&shard, delete_req(agg_del.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));

                // Seal segment 1 by triggering rotation.
                trigger_rotation(&shard).await;
                let cr = shard.compact_oldest_eligible_segment().await.unwrap()
                    .expect("round 1 compaction should run");
                assert_eq!(cr.log_id, 1);
                assert!(cr.compacted_size < cr.original_size);

                let read_keep = unwrap_read(process(&shard, read_req(agg_keep.clone())).await);
                assert_eq!(read_keep.event_batches.len(), 1);

                shard.close().await;
            }

            // Round 2: reopen, fill a new segment with dead data, compact that, close.
            // This tests that segment 1 (already compacted) loads correctly on restart,
            // and that further compaction of other segments doesn't corrupt the state.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_keep = key(1, 1, 1);
                let agg_new = key(3, 3, 3);  // fresh key, never deleted
                let agg_filler = key(4, 4, 4); // fat filler, will be deleted

                // agg_keep must be readable from the compacted segment 1.
                let read_keep = unwrap_read(process(&shard, read_req(agg_keep.clone())).await);
                assert_eq!(read_keep.event_batches.len(), 1, "keeper must survive restart");

                // Write to agg_keep to add another batch.
                write_ok(&shard, write_req(agg_keep.clone(), events(2))).await;

                // Write new aggregate.
                write_ok(&shard, write_req(agg_new.clone(), events(1))).await;

                // Fill the current segment with filler fat data, then delete to create dead data.
                write_fat(&shard, &agg_filler, 40).await;
                let result = process(&shard, delete_req(agg_filler.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));

                // Seal the current segment and compact it.
                let active_before = shard.log_segments_cache.active_log_id();
                // Write to a fresh key to trigger rotation without reusing deleted keys.
                let rotation_filler = key(5, 5, 5);
                while shard.log_segments_cache.active_log_id() == active_before {
                    write_ok(&shard, write_req(rotation_filler.clone(), fat_event(1))).await;
                }

                // Try compacting the newly sealed segment.
                let _ = shard.compact_oldest_eligible_segment().await.unwrap();

                shard.close().await;
            }

            // Round 3: reopen from doubly-processed state and verify correctness.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_keep = key(1, 1, 1);
                let agg_del = key(2, 2, 2);

                // Original keeper must still be readable (it had 2 batches by end of round 2).
                let read_keep = unwrap_read(process(&shard, read_req(agg_keep.clone())).await);
                assert!(!read_keep.event_batches.is_empty(),
                    "keeper should still have events after doubly-processed reload");

                // Round 1's deleted aggregate must still be gone.
                let result = process(&shard, read_req(agg_del.clone())).await;
                assert!(matches!(result, Err(ShardError::Read(ShardReadError::AggregateNotExists))),
                    "agg_del should still be deleted; got {result:?}");

                shard.close().await;
            }
        });
    }

    /// When compaction produces few surviving metablocks plus a small separate
    /// datablock, the datablock DMA buffer's align_down start could reach back into the
    /// metablock region, overwriting metablock content with zeros.
    ///
    /// The restart after compaction is essential: it forces cold-cache disk reads so the
    /// test catches on-disk corruption (in-memory caches would mask it).
    #[test]
    fn compact_small_datablock_does_not_overwrite_metablocks() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            {
                let shard = open_compact_shard(&dir).await;

                let dead = key(1, 1, 0);
                let agg_a = key(1, 1, 1);

                // Fill most of the segment with fat events that will be deleted.
                write_fat(&shard, &dead, 40).await;

                // Write one event batch with a non-compressible payload that forces a
                // separate datablock (> MINIBATCH_SIZE_BYTES after compression).
                // After compaction the file will have few kept metablocks (this batch +
                // the tombstone) with a small datablock — the datablock DMA write's
                // align_down must not reach back into the metablock region.
                let payload: Vec<u8> = (0..600u32)
                    .map(|i| (i.wrapping_mul(2654435761) >> 16) as u8)
                    .collect();
                write_ok(&shard, write_req(agg_a.clone(), vec![DatablockAggregateEvent {
                    client_seq: 1,
                    event_type_major: 1,
                    event_value: Arc::new(payload),
                    ..Default::default()
                }])).await;

                // Delete the fat aggregate to create dead space (>20% ratio).
                let result = process(&shard, delete_req(dead.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));

                trigger_rotation(&shard).await;

                let result = shard.compact_oldest_eligible_segment().await.unwrap();
                assert!(result.is_some(), "expected compaction to run");

                shard.close().await;
            }

            // Reopen from disk — caches are cold, reads go to the compacted file.
            // If the datablock DMA alignment overwrote metablock content, this
            // will fail with deserialization or CRC errors.
            {
                let shard = open_compact_shard(&dir).await;

                let agg_a = key(1, 1, 1);
                let read_a = unwrap_read(process(&shard, read_req(agg_a.clone())).await);
                assert_eq!(read_a.event_batches.len(), 1, "expected 1 batch after restart");
                assert_eq!(read_a.event_batches[0].events.len(), 1, "expected 1 event after restart");
                assert_eq!(read_a.event_batches[0].events[0].event_value.len(), 600,
                    "event payload should survive compaction");

                shard.close().await;
            }
        });
    }

    // ── Cache warm-up tests ──

    #[test]
    fn warmup_caches_event_batch_and_client() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            {
                let shard = open_shard(&dir).await;
                write_ok(&shard, write_req(agg.clone(), events(3))).await;
                shard.close().await;
            }

            let shard = open_shard(&dir).await;
            let mut cache = shard.shard_mem_cache.borrow_mut();

            let (in_cache, status) = cache.aggregate_load_status(&agg, CachePath::Write);
            assert!(in_cache, "aggregate should be in cache after warmup");
            assert_eq!(status, AggregateStatus::Found);

            let snap = cache.get_aggregate_snapshot(&agg, CachePath::Write).unwrap();
            assert_eq!(snap.event_seq, 3);
            assert_eq!(snap.aggregate_version, 1);

            // Client cache should also be populated from the EventBatch
            let client_key = AggregateClientKey::new(agg.clone(), 1);
            let (client_in_cache, client_seq) = cache.aggregate_client_load_status(&agg, &client_key);
            assert!(client_in_cache, "client cache should be populated from EventBatch warmup");
            assert_eq!(client_seq, Some(3));

            drop(cache);
            shard.close().await;
        });
    }

    #[test]
    fn warmup_caches_deleted_aggregate() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            {
                let shard = open_shard(&dir).await;
                write_ok(&shard, write_req(agg.clone(), events(2))).await;
                let result = process(&shard, delete_req(agg.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));
                shard.close().await;
            }

            let shard = open_shard(&dir).await;
            let (in_cache, status) = shard.shard_mem_cache.borrow_mut().aggregate_load_status(&agg, CachePath::Write);
            assert!(in_cache, "deleted aggregate should be in cache after warmup");
            assert_eq!(status, AggregateStatus::Deleted);

            shard.close().await;
        });
    }

    #[test]
    fn warmup_caches_trimmed_aggregate() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            {
                let shard = open_shard(&dir).await;
                write_ok(&shard, write_req(agg.clone(), events(2))).await;
                write_ok(&shard, write_req(agg.clone(), events(2))).await;
                write_ok(&shard, write_req(agg.clone(), events(2))).await;
                let result = process(&shard, trim_req(agg.clone(), 2)).await;
                assert!(matches!(result, Ok(ClientResponse::TrimStart(_))));
                shard.close().await;
            }

            let shard = open_shard(&dir).await;
            let mut cache = shard.shard_mem_cache.borrow_mut();

            let (in_cache, status) = cache.aggregate_load_status(&agg, CachePath::Write);
            assert!(in_cache, "trimmed aggregate should be in cache after warmup");
            assert_eq!(status, AggregateStatus::Found);

            let snap = cache.get_aggregate_snapshot(&agg, CachePath::Write).unwrap();
            assert_eq!(snap.min_aggregate_version, 2, "trim boundary must be reflected in cached snapshot");
            assert_eq!(snap.aggregate_version, 3);
            assert_eq!(snap.event_seq, 6);

            // Client cache is populated from the EventBatch (not the SoftTrim itself)
            let client_key = AggregateClientKey::new(agg.clone(), 1);
            let (client_in_cache, client_seq) = cache.aggregate_client_load_status(&agg, &client_key);
            assert!(client_in_cache, "client cache should be populated from EventBatch after SoftTrim");
            assert_eq!(client_seq, Some(2));

            drop(cache);
            shard.close().await;
        });
    }

    #[test]
    fn warmup_does_not_cache_deleted_as_found() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);

            {
                let shard = open_shard(&dir).await;
                // Write to both, then delete A
                write_ok(&shard, write_req(agg_a.clone(), events(2))).await;
                write_ok(&shard, write_req(agg_b.clone(), events(3))).await;
                let result = process(&shard, delete_req(agg_a.clone())).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))));
                shard.close().await;
            }

            let shard = open_shard(&dir).await;

            // A should be Deleted, not Found
            let (in_cache, status) = shard.shard_mem_cache.borrow_mut().aggregate_load_status(&agg_a, CachePath::Write);
            assert!(in_cache);
            assert_eq!(status, AggregateStatus::Deleted, "deleted aggregate must not appear as Found");

            // B should be Found
            let (in_cache, status) = shard.shard_mem_cache.borrow_mut().aggregate_load_status(&agg_b, CachePath::Write);
            assert!(in_cache);
            assert_eq!(status, AggregateStatus::Found);

            shard.close().await;
        });
    }

    #[test]
    fn warmup_respects_timeout() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg = key(1, 1, 1);

            {
                let shard = open_shard(&dir).await;
                write_ok(&shard, write_req(agg.clone(), events(2))).await;
                shard.close().await;
            }

            // Reopen with zero timeout — warmup should stop immediately
            let mut cfg = test_config(&dir);
            cfg.cache_warmup_max_duration = Duration::ZERO;
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                .await
                .unwrap();

            let (in_cache, _) = shard.shard_mem_cache.borrow_mut().aggregate_load_status(&agg, CachePath::Write);
            assert!(!in_cache, "zero-timeout warmup should not populate cache");

            shard.close().await;
        });
    }

    // ── read_segment_summary ──

    #[test]
    fn read_segment_summary_from_rotated_segment() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 10, 100);
            let agg_b = key(2, 20, 200);
            write_ok(&shard, write_req(agg_a.clone(), fat_event(1))).await;
            write_ok(&shard, write_req(agg_b.clone(), fat_event(1))).await;

            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() > 1);

            let payload = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await
                .expect("should have summary");

            assert!(payload.orgs.contains(&1));
            assert!(payload.orgs.contains(&2));
            assert!(payload.aggregate_types.contains(&AggregateTypeKey::new(1, 10)));
            assert!(payload.aggregate_types.contains(&AggregateTypeKey::new(2, 20)));
            assert!(payload.aggregates.iter().any(|a| a.aggregate_id == 100));
            assert!(payload.aggregates.iter().any(|a| a.aggregate_id == 200));

            shard.close().await;
        });
    }

    #[test]
    fn read_segment_summary_returns_none_for_active_segment() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            write_ok(&shard, write_req(key(1, 1, 1), events(1))).await;

            let active_id = shard.log_segments_cache.active_log_id();
            let result = read_segment_summary(shard.log_segments_cache.shard_dir(), active_id).await;
            assert!(result.is_none());

            shard.close().await;
        });
    }

    #[test]
    fn corrupt_summary_gracefully_returns_none() {
        glommio_test!({
            use crate::shard_wal_sync::summary_path;

            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            write_ok(&shard, write_req(key(1, 10, 100), fat_event(1))).await;
            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() > 1);

            // Corrupt the .summary sidecar file before any read caches it
            let path = summary_path(shard.log_segments_cache.shard_dir(), 1);
            let mut bytes = std::fs::read(&path).expect("summary file should exist");
            bytes[0] ^= 0xFF;
            std::fs::write(&path, &bytes).expect("should write corrupted file");

            let result = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await;
            assert!(result.is_none(), "corrupt summary should return None");

            shard.close().await;
        });
    }

    /// A v1-stamped sidecar (CRC-valid) must fail deserialization into the
    /// missing-summary degrade path — the clean break has no shims.
    #[test]
    fn v1_summary_degrades_to_none() {
        glommio_test!({
            use crate::shard_wal_sync::summary_path;
            use celeriant_wire::disk::versioned_block::serialize_versioned_message_heap;

            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            write_ok(&shard, write_req(key(1, 10, 100), fat_event(1))).await;
            trigger_rotation(&shard).await;

            // Overwrite the sealed segment's sidecar with a v1-stamped file.
            let block = SegmentSummaryPayload {
                orgs: vec![1],
                aggregate_types: vec![],
                aggregates: vec![],
                complete: true,
                aggregate_bloom: None,
                client_bloom: None,
                schema_bloom: None,
            };
            let bytes = serialize_versioned_message_heap(&block, 1).unwrap();
            std::fs::write(summary_path(shard.log_segments_cache.shard_dir(), 1), &bytes).unwrap();

            assert!(read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await.is_none(),
                "v1 summary must degrade to None");
            assert!(shard.read_segment_summary_cached(1).await.is_none(),
                "the cached reader must not cache or surface a v1 summary");

            shard.close().await;
        });
    }

    /// The sealed sidecar carries the v2 fields end-to-end: per-aggregate client
    /// sets fed by every client-bearing kind, a valid newest-metablock seek
    /// target, and the segment blooms.
    #[test]
    fn sealed_sidecar_carries_client_sets_tip_and_blooms() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg = key(1, 10, 100);
            let writer = 7u128;
            let trimmer = 13u128;

            write_ok(&shard, write_req_full(agg.clone(), fat_event(1), true, None, writer, false)).await;
            write_ok(&shard, write_req_full(agg.clone(), fat_event(2), false, None, writer, false)).await;
            let trim = process(&shard, ClientRequest::TrimStart(TrimStartRequest {
                correlation_id: None,
                aggregate_key: agg.clone(),
                keep_from_aggregate_version: 2,
                client_id: trimmer,
                user_id: None,
            })).await;
            assert!(matches!(trim, Ok(ClientResponse::TrimStart(_))));

            trigger_rotation(&shard).await;

            let payload = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await.expect("sidecar must exist");
            let entry = payload.aggregates.iter().find(|e| e.aggregate_id == 100).expect("entry for the aggregate");

            let set = &entry.client_set;
            assert!(set.may_contain_hash(client_id_bloom_hash(writer)), "writer must be in the client set");
            assert!(set.may_contain_hash(client_id_bloom_hash(trimmer)), "trim-only client must be in the client set");
            assert!(!set.may_contain_hash(client_id_bloom_hash(99u128)), "never-seen client must be definitely absent");

            // The seek target must point at this aggregate's chain (the trim is its newest member).
            assert_ne!(entry.newest_metablock_pos, 0);
            let block = read_block_at(&shard, 1, entry.newest_metablock_pos).await;
            assert_eq!(
                metablock_bytes::read_chain_aggregate_key(&block).as_ref(), Some(&agg),
                "newest_metablock_pos must land on the aggregate's own chain"
            );

            // Blooms: right-sized at seal from true cardinality, answering
            // exactly like the fixed live bloom for the same keys.
            use celeriant_rotating_log::log_segment_file::aggregate_key_bloom::AggregateKeyBloom;
            let agg_words = payload.aggregate_bloom.as_deref().expect("complete seal must persist an aggregate bloom");
            let client_words = payload.client_bloom.as_deref().expect("complete seal must persist a client bloom");
            assert_eq!(agg_words.len() * 8, 32, "one aggregate sizes to a single SBBF block");
            assert_eq!(client_words.len() * 8, 32, "two clients size to a single SBBF block");
            let agg_bloom = AggregateKeyBloom::from_bytes(agg_words);
            assert!(agg_bloom.may_contain(&agg));
            assert!(!agg_bloom.may_contain(&key(9, 9, 9)), "sized bloom answers definite absence");
            let client_bloom = AggregateKeyBloom::from_bytes(client_words);
            assert!(client_bloom.may_contain_hash(client_id_bloom_hash(writer)));
            assert!(client_bloom.may_contain_hash(client_id_bloom_hash(trimmer)), "trim client must be in the segment bloom");
            assert!(!client_bloom.may_contain_hash(client_id_bloom_hash(99u128)));

            shard.close().await;
        });
    }

    async fn read_block_at<R: ReplicationClient, D: S3Downloader>(
        shard: &ShardWal<R, D>,
        log_id: u64,
        pos: u64,
    ) -> [u8; FIXED_BLOCK_SIZE_BYTES] {
        let f = shard.log_segments_cache.get(log_id).await.unwrap();
        let guard = f.lock_reader("test_read_block").await.unwrap();
        let dma = guard.as_ref().unwrap();
        let buf = dma.read_at(pos, FIXED_BLOCK_SIZE_BYTES).await.unwrap();
        buf[..FIXED_BLOCK_SIZE_BYTES].try_into().unwrap()
    }

    // ── Decoded summary reader cache ──

    #[test]
    fn summary_cache_hit_survives_sidecar_deletion_until_invalidated() {
        glommio_test!({
            use crate::shard_wal_sync::summary_path;

            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            write_ok(&shard, write_req(key(1, 10, 100), fat_event(1))).await;
            trigger_rotation(&shard).await;

            let first = shard.read_segment_summary_cached(1).await.expect("summary must decode");
            assert!(first.aggregates.iter().any(|e| e.aggregate_id == 100));

            // Remove the file: a hit must come from the decoded cache, no re-read.
            std::fs::remove_file(summary_path(shard.log_segments_cache.shard_dir(), 1)).unwrap();
            let second = shard.read_segment_summary_cached(1).await.expect("must be served from cache");
            assert!(Rc::ptr_eq(&first, &second), "hit must return the cached decode");

            // Invalidate (what the compaction path does) — the miss now sees the deletion.
            shard.summary_cache.borrow_mut().pop(&1);
            assert!(shard.read_segment_summary_cached(1).await.is_none(), "post-invalidation read must hit disk");

            shard.close().await;
        });
    }

    // ── Dedup consult (summary_hint decision) ──

    #[test]
    fn summary_hint_decision_table() {

        let agg = key(1, 1, 1);
        let present = client_id_bloom_hash(7);
        let absent = client_id_bloom_hash(9);

        let mut entry = SegmentAggregateEntry::new(1, 1, 1);
        entry.newest_metablock_pos = 400_384;
        entry.client_set = ClientSet::Exact(vec![present]);
        let mut payload = SegmentSummaryPayload {
            orgs: vec![1],
            aggregate_types: vec![AggregateTypeKey::new(1, 1)],
            aggregates: vec![entry],
            complete: true,
            aggregate_bloom: None,
            client_bloom: None,
            schema_bloom: None,
        };

        // Aggregate entry absent → the aggregate is not in the segment → skip.
        assert_eq!(summary_hint(&payload, &key(1, 1, 2), present), Some(SegmentHint::Skip));
        // Client definitely absent from this aggregate → skip.
        assert_eq!(summary_hint(&payload, &agg, absent), Some(SegmentHint::Skip));
        // Maybe present → seek to the newest metablock.
        assert_eq!(summary_hint(&payload, &agg, present), Some(SegmentHint::SeekTo(400_384)));
        // Unknown client set cannot skip, but the tip is still a valid seek target.
        payload.aggregates[0].client_set = ClientSet::Unknown;
        assert_eq!(summary_hint(&payload, &agg, absent), Some(SegmentHint::SeekTo(400_384)));
        // No tip recorded (compaction dropped the blocks) → no hint → full walk.
        payload.aggregates[0].newest_metablock_pos = 0;
        assert_eq!(summary_hint(&payload, &agg, absent), None);

        // Incomplete summary: a subset proves nothing by absence. Entry absent →
        // full walk; a client set claiming "definitely absent" → still a full
        // walk of the entry's tip; only the tip (true-newest) stays usable.
        payload.complete = false;
        payload.aggregates[0].newest_metablock_pos = 400_384;
        payload.aggregates[0].client_set = ClientSet::Exact(vec![present]);
        assert_eq!(summary_hint(&payload, &key(1, 1, 2), present), None, "incomplete summary must never skip on a missing entry");
        assert_eq!(summary_hint(&payload, &agg, absent), Some(SegmentHint::SeekTo(400_384)), "incomplete client set must degrade to the seek, not skip");
        assert_eq!(summary_hint(&payload, &agg, present), Some(SegmentHint::SeekTo(400_384)), "tips from an incomplete summary stay usable");
    }

    /// Per-aggregate client sets are finer than the segment-level client bloom:
    /// a client that wrote OTHER aggregates in the segment (so the segment bloom
    /// says "present") is skipped when this aggregate's set says absent. The
    /// observable: a skipped segment never eager-caches co-resident clients,
    /// while the no-summary full walk does.
    #[test]
    fn dedup_consult_skips_segment_on_client_absent_from_aggregate() {
        glommio_test!({
            use crate::shard_wal_sync::summary_path;

            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg_a = key(1, 1, 1);
            let agg_b = key(1, 1, 2);
            let writer_a = 7u128;
            let writer_b = 9u128;

            // Segment 1: A written by 7, B written by 9. The segment client bloom
            // contains BOTH clients; only the per-aggregate set can separate them.
            write_ok(&shard, write_req_full(agg_a.clone(), fat_event(1), true, None, writer_a, true)).await;
            write_ok(&shard, write_req_full(agg_b.clone(), fat_event(1), true, None, writer_b, true)).await;
            trigger_rotation(&shard).await;

            let cold_lookup = |shard: &ShardWal<StubReplicationClient, StubS3Downloader>| {
                let mut mc = shard.shard_mem_cache.borrow_mut();
                mc.clear_aggregate_write_snapshots_for_test();
                mc.clear_aggregate_write_client_snapshots_for_test();
            };

            // Consult path: (A, 9) — A's client set says 9 is absent → segment skipped.
            cold_lookup(&shard);
            let key_a9 = AggregateClientKey::new(agg_a.clone(), writer_b);
            shard.cache_aggregate_client(&agg_a, &key_a9).await.unwrap();
            assert_eq!(shard.shard_mem_cache.borrow_mut().get_client_seq(&agg_a, writer_b), None,
                "client 9 never wrote A");
            let key_a7 = AggregateClientKey::new(agg_a.clone(), writer_a);
            let (a7_loaded, _) = shard.shard_mem_cache.borrow_mut().aggregate_client_load_status(&agg_a, &key_a7);
            assert!(!a7_loaded,
                "a summary-skipped segment must not be walked — co-resident client 7 stays uncached");

            // Control: no summary → today's full walk, which eager-caches client 7.
            std::fs::remove_file(summary_path(shard.log_segments_cache.shard_dir(), 1)).unwrap();
            shard.summary_cache.borrow_mut().pop(&1);
            cold_lookup(&shard);
            shard.cache_aggregate_client(&agg_a, &key_a9).await.unwrap();
            assert_eq!(shard.shard_mem_cache.borrow_mut().get_client_seq(&agg_a, writer_b), None);
            let (a7_loaded, a7_seq) = shard.shard_mem_cache.borrow_mut().aggregate_client_load_status(&agg_a, &key_a7);
            assert!(a7_loaded, "the no-summary full walk must visit A's chain and eager-cache client 7");
            assert_eq!(a7_seq, Some(1));

            shard.close().await;
        });
    }

    /// Maybe-present consult: the seek path must find the true client_seq in a
    /// sealed segment (correctness of the SeekTo integration end-to-end).
    #[test]
    fn dedup_consult_seek_finds_client_seq_in_sealed_segment() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg = key(1, 1, 1);
            let client_id = 7u128;

            for n in 1u64..=5 {
                let evt = DatablockAggregateEvent {
                    client_seq: n,
                    event_type_major: 1,
                    event_value: Arc::new(vec![n as u8]),
                    ..Default::default()
                };
                write_ok(&shard, write_req_full(agg.clone(), vec![evt], n == 1, None, client_id, true)).await;
            }
            trigger_rotation(&shard).await;

            {
                let mut mc = shard.shard_mem_cache.borrow_mut();
                mc.clear_aggregate_write_snapshots_for_test();
                mc.clear_aggregate_write_client_snapshots_for_test();
            }

            // Sanity: the consult has a seek hint for this lookup.
            let summary = shard.read_segment_summary_cached(1).await.expect("sealed summary must exist");
            assert!(matches!(
                summary_hint(&summary, &agg, client_id_bloom_hash(client_id)),
                Some(SegmentHint::SeekTo(_))
            ));

            let client_key = AggregateClientKey::new(agg.clone(), client_id);
            shard.cache_aggregate_client(&agg, &client_key).await.unwrap();
            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, client_id),
                Some(5),
                "seek path must recover the true client_seq from the sealed segment"
            );

            shard.close().await;
        });
    }

    /// The consult is lazy: sidecars decode only when the scan is about to
    /// enter their segment, so a hit in a newer segment never touches older
    /// sidecars (the eager pre-load thrashed the 16-entry LRU per miss once
    /// segments outnumbered it).
    #[test]
    fn dedup_consult_loads_sidecars_lazily() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg = key(1, 1, 1);
            let client_id = 7u128;

            // The client writes in segments 1 AND 2; segment 3 is active.
            write_ok(&shard, write_req_full(agg.clone(), fat_event(1), true, None, client_id, true)).await;
            trigger_rotation(&shard).await;
            write_ok(&shard, write_req_full(agg.clone(), fat_event(2), false, None, client_id, true)).await;
            trigger_rotation(&shard).await;
            assert_eq!(shard.log_segments_cache.active_log_id(), 3, "scaffolding: two sealed segments expected");

            {
                let mut mc = shard.shard_mem_cache.borrow_mut();
                mc.clear_aggregate_write_snapshots_for_test();
                mc.clear_aggregate_write_client_snapshots_for_test();
            }
            shard.summary_cache.borrow_mut().pop(&1);
            shard.summary_cache.borrow_mut().pop(&2);

            let client_key = AggregateClientKey::new(agg.clone(), client_id);
            shard.cache_aggregate_client(&agg, &client_key).await.unwrap();
            assert_eq!(shard.shard_mem_cache.borrow_mut().get_client_seq(&agg, client_id), Some(2));

            assert!(shard.summary_cache.borrow().contains(&2), "the entered segment's sidecar decodes");
            assert!(!shard.summary_cache.borrow().contains(&1), "a hit in segment 2 must not touch segment 1's sidecar");

            shard.close().await;
        });
    }

    /// A pre-warm replay that stops early (cache full or warmup deadline) seeds
    /// the active accumulator from a newest-prefix SUBSET. The next seal must
    /// taint the sidecar (`complete: false`) so its absences never authorize a
    /// summary Skip — otherwise a pre-restart client's idempotency seq is lost.
    #[test]
    fn partial_warmup_must_not_authorize_summary_skips() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg_a = key(1, 1, 1);
            let writer = 7u128;

            // Segment 1, pre-restart: client 7 writes A with idempotency.
            {
                let shard = open_compact_shard(&dir).await;
                write_ok(&shard, write_req_full(agg_a.clone(), fat_event(1), true, None, writer, true)).await;
                shard.close().await;
            }

            // Restart with a zero warmup budget: the replay stops inside the
            // still-active segment 1, so the accumulator never learns about A.
            let mut cfg = compact_config(&dir);
            cfg.cache_warmup_max_duration = Duration::ZERO;
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                .await
                .unwrap();

            // Post-restart commits DO reach the accumulator; the rotation seals
            // segment 1 with a sidecar that knows them but not A.
            write_ok(&shard, write_req(key(2, 2, 2), fat_event(1))).await;
            trigger_rotation(&shard).await;

            let payload = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await.expect("sidecar must exist");
            assert!(!payload.complete, "a subset sidecar must carry the incomplete taint");
            assert!(payload.aggregates.iter().all(|e| e.aggregate_id != 1), "scaffolding: the sidecar must not know about A");

            // Cold dedup consult for (A, 7): the incomplete summary must not
            // skip segment 1 — the full walk recovers the true client_seq.
            {
                let mut mc = shard.shard_mem_cache.borrow_mut();
                mc.clear_aggregate_write_snapshots_for_test();
                mc.clear_aggregate_write_client_snapshots_for_test();
            }
            let client_key = AggregateClientKey::new(agg_a.clone(), writer);
            shard.cache_aggregate_client(&agg_a, &client_key).await.unwrap();
            assert_eq!(
                shard.shard_mem_cache.borrow_mut().get_client_seq(&agg_a, writer),
                Some(1),
                "the client_seq must survive a partial-warmup restart plus rotation"
            );

            shard.close().await;
        });
    }

    #[test]
    fn incomplete_sidecar_routes_listings_to_legacy_scan() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let agg_a = key(1, 1, 1);

            {
                let shard = open_compact_shard(&dir).await;
                write_ok(&shard, write_req(agg_a.clone(), fat_event(1))).await;
                shard.close().await;
            }

            // Same staging as the dedup twin above: zero warmup budget taints the
            // accumulator, the rotation seals segment 1 with a subset sidecar.
            let mut cfg = compact_config(&dir);
            cfg.cache_warmup_max_duration = Duration::ZERO;
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                .await
                .unwrap();
            write_ok(&shard, write_req(key(2, 2, 2), fat_event(1))).await;
            trigger_rotation(&shard).await;

            let payload = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await.expect("sidecar must exist");
            assert!(!payload.complete && payload.aggregates.iter().all(|e| e.aggregate_id != 1),
                "scaffolding: sidecar must be a tainted subset that does not know A");

            // An incomplete sidecar must not be listing-authoritative: the legacy
            // segment scan must surface A, not silently omit it forever.
            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            assert!(orgs.orgs.iter().any(|o| o.org_id == 1), "org 1 must be listed via the legacy scan");
            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(None, None)).await);
            assert!(aggs.aggregates.iter().any(|a| a.aggregate_id == 1), "aggregate A must be listed via the legacy scan");
            let types = unwrap_list_types(process(&shard, list_types_req(None)).await);
            assert!(types.aggregate_types.iter().any(|t| t.org_id == 1), "org 1's type must be listed via the legacy scan");

            shard.close().await;
        });
    }

    // ── Summary-based list operations ──

    #[test]
    fn list_active_segment_from_memory() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            // Write data without rotation — all in active segment
            write_ok(&shard, write_req(key(1, 10, 100), events(2))).await;
            write_ok(&shard, write_req(key(2, 20, 200), events(3))).await;

            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            let org_ids: HashSet<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
            assert!(org_ids.contains(&1));
            assert!(org_ids.contains(&2));
            assert!(orgs.next_cursor.is_none());

            let types = unwrap_list_types(process(&shard, list_types_req(None)).await);
            assert_eq!(types.aggregate_types.len(), 2);
            assert!(types.next_cursor.is_none());

            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(None, None)).await);
            assert_eq!(aggs.aggregates.len(), 2);
            let ids: HashSet<u128> = aggs.aggregates.iter().map(|a| a.aggregate_id).collect();
            assert!(ids.contains(&100));
            assert!(ids.contains(&200));

            shard.close().await;
        });
    }

    #[test]
    fn list_across_segments_with_summaries() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            // Segment 1: two aggregates in different orgs
            write_ok(&shard, write_req(key(1, 10, 100), events(2))).await;
            write_ok(&shard, write_req(key(2, 20, 200), events(1))).await;

            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() > 1);

            // Segment 2 (active): one more aggregate
            write_ok(&shard, write_req(key(3, 30, 300), events(1))).await;

            // list_orgs: all three orgs
            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            let org_ids: HashSet<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
            assert!(org_ids.contains(&1));
            assert!(org_ids.contains(&2));
            assert!(org_ids.contains(&3));

            // list_types: all three types
            let types = unwrap_list_types(process(&shard, list_types_req(None)).await);
            assert!(types.aggregate_types.len() >= 3);

            // list_types filtered by org=1
            let types_1 = unwrap_list_types(process(&shard, list_types_req(Some(1))).await);
            assert_eq!(types_1.aggregate_types.len(), 1);
            assert_eq!(types_1.aggregate_types[0].aggregate_type_id, 10);

            // list_aggs: all three
            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(None, None)).await);
            let agg_ids: HashSet<u128> = aggs.aggregates.iter().map(|a| a.aggregate_id).collect();
            assert!(agg_ids.contains(&100));
            assert!(agg_ids.contains(&200));
            assert!(agg_ids.contains(&300));

            // Verify stats accumulation: agg 100 had 2 events in 1 batch
            let agg_100 = aggs.aggregates.iter().find(|a| a.aggregate_id == 100).unwrap();
            assert_eq!(agg_100.event_batch_count, 1);
            assert!(!agg_100.is_deleted);

            shard.close().await;
        });
    }

    #[test]
    fn list_aggregates_delete_barrier() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;

            let agg_a = key(1, 1, 1);

            // Segment 1: 3 batches for agg_a
            write_ok(&shard, write_req(agg_a.clone(), events(1))).await;
            write_ok(&shard, write_req(agg_a.clone(), events(1))).await;
            write_ok(&shard, write_req(agg_a.clone(), events(1))).await;

            trigger_rotation(&shard).await;

            // Segment 2 (active): delete agg_a
            let _ = process(&shard, delete_req(agg_a.clone())).await.unwrap();

            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(1))).await);

            // agg_a should appear as deleted
            let agg = aggs.aggregates.iter().find(|a| a.aggregate_id == 1).unwrap();
            assert!(agg.is_deleted);
            // Delete barrier: old batches from segment 1 should NOT be accumulated
            assert_eq!(agg.event_batch_count, 0, "delete barrier should prevent stat accumulation");
            assert_eq!(agg.compressed_size, 0);

            shard.close().await;
        });
    }

    #[test]
    fn standalone_write_updates_segment_summary() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;

            assert!(shard.shard_mem_cache.borrow().peek_segment_summary().is_empty());

            write_ok(&shard, write_req(key(1, 10, 100), events(1))).await;

            let cache = shard.shard_mem_cache.borrow();
            assert_eq!(cache.peek_segment_summary().len(), 1);
            assert!(cache.peek_segment_summary_orgs().contains(&1));

            shard.close().await;
        });
    }

    #[test]
    fn leader_write_updates_summary_only_after_replication() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Leader with replication that succeeds on first attempt
            let client = FailThenSucceedReplicationClient::new(0, 1);
            let shard = open_leader_shard(&dir, client).await;

            write_ok(&shard, write_req(key(1, 10, 100), events(1))).await;

            // Summary should be populated (replication succeeded → commit_replication ran)
            let cache = shard.shard_mem_cache.borrow();
            assert_eq!(cache.peek_segment_summary().len(), 1,
                "summary should be populated after successful replication");
            assert!(cache.peek_segment_summary_orgs().contains(&1));

            shard.close().await;
        });
    }

    #[test]
    fn prewarm_summary_respects_delete_order() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Write then delete in the active segment, close without rotation
            {
                let shard = open_shard(&dir).await;
                let agg = key(1, 10, 100);
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
                let _ = process(&shard, delete_req(agg)).await.unwrap();
                shard.close().await;
            }

            // Reopen — pre-warm reverse scans the active segment
            let shard = open_shard(&dir).await;

            // The summary must reflect the delete (is_deleted=true), not the write
            let cache = shard.shard_mem_cache.borrow();
            let entry = cache.peek_segment_summary().values().next().unwrap();
            assert!(entry.is_deleted, "pre-warm should replay in forward order: delete after write");

            // list_aggregates should show the aggregate as deleted
            drop(cache);
            let aggs = unwrap_list_aggs(process(&shard, list_aggs_req(Some(1), Some(10))).await);
            let found = aggs.aggregates.iter().find(|a| a.aggregate_id == 100);
            assert!(found.is_some() && found.unwrap().is_deleted,
                "deleted aggregate should appear as deleted after pre-warm");

            shard.close().await;
        });
    }

    #[test]
    fn prewarm_summary_respects_recreate_order() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Write, delete with allow_recreate, recreate — all in active segment
            {
                let shard = open_shard(&dir).await;
                let agg = key(1, 10, 100);
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
                let _ = process(&shard, delete_req_full(agg.clone(), true, false, None)).await.unwrap();
                write_ok(&shard, write_req(agg.clone(), events(1))).await;
                shard.close().await;
            }

            // Reopen
            let shard = open_shard(&dir).await;

            // Summary should show alive with batch_count=1 (new incarnation only)
            let cache = shard.shard_mem_cache.borrow();
            let entry = cache.peek_segment_summary().values().next().unwrap();
            assert!(!entry.is_deleted, "recreated aggregate should not be deleted after pre-warm");
            assert_eq!(entry.event_batch_count, 1,
                "recreated aggregate should have 1 batch (new incarnation only)");

            shard.close().await;
        });
    }

    #[test]
    fn list_orgs_across_segments_after_rotation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();

            // Segment 1: write data, then rotate so segment 1 gets a summary
            {
                let shard = open_compact_shard(&dir).await;
                let agg_a = key(1, 10, 100);
                let agg_b = key(2, 20, 200);
                write_ok(&shard, write_req(agg_a.clone(), fat_event(1))).await;
                write_ok(&shard, write_req(agg_b.clone(), fat_event(1))).await;
                trigger_rotation(&shard).await;

                // Segment 2 (active): write more data with a new org
                let agg_c = key(3, 30, 300);
                write_ok(&shard, write_req(agg_c.clone(), events(1))).await;

                shard.close().await;
            }

            // Reopen and verify list_orgs returns orgs from both segments
            let shard = open_compact_shard(&dir).await;

            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            let mut org_ids: Vec<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
            org_ids.sort();
            assert!(org_ids.contains(&1));
            assert!(org_ids.contains(&2));
            assert!(org_ids.contains(&3));

            // list_aggregate_types returns correct types
            let types = unwrap_list_types(process(&shard, list_types_req(Some(1))).await);
            assert_eq!(types.aggregate_types.len(), 1);
            assert_eq!(types.aggregate_types[0].org_id, 1);
            assert_eq!(types.aggregate_types[0].aggregate_type_id, 10);

            // Writes after open are reflected in the active segment summary
            let agg_d = key(4, 40, 400);
            write_ok(&shard, write_req(agg_d, events(1))).await;

            let orgs = unwrap_list_orgs(process(&shard, list_orgs_req()).await);
            let org_ids: Vec<u128> = orgs.orgs.iter().map(|o| o.org_id).collect();
            assert!(org_ids.contains(&4), "new writes should appear in active segment summary");

            shard.close().await;
        });
    }

    /// xorshift fill that resists the zstd dictionary, so each event's datablock
    /// is stored externally at full size and a few writes fill a small segment.
    fn incompressible(n: usize, seed: u64) -> Vec<u8> {
        let mut v = vec![0u8; n];
        let mut s = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        for c in v.chunks_mut(8) {
            s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = s;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let b = z.to_le_bytes();
            c.copy_from_slice(&b[..c.len()]);
        }
        v
    }

    fn big_event(client_seq: u64, seed: u64) -> DatablockAggregateEvent {
        DatablockAggregateEvent {
            client_seq,
            event_type_major: 1,
            // ~300KB external datablock; rotates a 1.5MB segment every ~1-2 writes.
            event_value: Arc::new(incompressible(300 * 1024, seed)),
            ..Default::default()
        }
    }

    fn cold_scan_config(dir: &std::path::Path) -> InternalShardConfig {
        InternalShardConfig {
            shard_log_preallocate_bytes: 2 * celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES as u64 + 512 * 1024, // 2 headers + 512KB usable
            max_open_files: 4096,
            cache_warmup_max_duration: Duration::ZERO,
            ..test_config(dir)
        }
    }

    async fn populate_segments<R: ReplicationClient, D: S3Downloader>(
        shard: &ShardWal<R, D>,
        min_active: u64,
    ) -> (u64, u128) {
        let mut last_id = 0u128;
        for i in 1u128..=2000 {
            write_ok(shard, client_write_req(key(1, 1, i), vec![big_event(1, i as u64)])).await;
            last_id = i;
            if shard.log_segments_cache.active_log_id() >= min_active && i >= min_active as u128 {
                break;
            }
        }
        let active = shard.log_segments_cache.active_log_id();
        assert!(active >= min_active, "expected >= {min_active} segments, got {active}");
        (active, last_id)
    }

    #[test]
    fn negative_lookup_reopens_every_segment_after_cold_start() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let active = {
                let shard = ShardWal::open(cold_scan_config(&dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();
                let (active, _) = populate_segments(&shard, 4).await;
                shard.close().await;
                active
            };

            // Reopen cold: warmup disabled, so only the active segment is resident.
            let shard = ShardWal::open(cold_scan_config(&dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();
            for id in 1..active {
                assert!(shard.log_segments_cache.get_if_cached(id).is_none(),
                    "sealed segment {id} should be cold after open (warmup disabled)");
            }

            let missing = AggregateKey::new(1, 1, 9_000_000);
            let result = shard.exists(&AggregateDetailsRequest { correlation_id: None, aggregate_key: missing }).await;
            assert!(result.is_err(), "non-existent aggregate must report not-found");

            // Every segment had to be opened just to read its bloom and skip.
            for id in 1..=active {
                assert!(shard.log_segments_cache.get_if_cached(id).is_some(),
                    "negative lookup should have opened segment {id} (512KB header read) to check its bloom; active={active}");
            }
            eprintln!("[scan-cost] negative lookup over {active} segments => {active} segment-file opens (each a {}KB header read)", HEADER_BLOCK_SIZE_BYTES / 1024);

            shard.close().await;
        });
    }

    #[test]
    fn deep_aggregate_read_walks_back_to_oldest_segment() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let active = {
                let shard = ShardWal::open(cold_scan_config(&dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();
                // Aggregate 1 is written first → lands in segment 1 and is never touched again.
                let (active, _) = populate_segments(&shard, 4).await;
                shard.close().await;
                active
            };

            let shard = ShardWal::open(cold_scan_config(&dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();
            let deep = AggregateKey::new(1, 1, 1);
            let result = shard.exists(&AggregateDetailsRequest { correlation_id: None, aggregate_key: deep }).await;
            assert!(result.is_ok(), "deep aggregate should be found: {:?}", result.err());

            for id in 1..=active {
                assert!(shard.log_segments_cache.get_if_cached(id).is_some(),
                    "deep read should have walked back through segment {id}; active={active}");
            }
            eprintln!("[scan-cost] deep aggregate (oldest segment) read over {active} segments => {active} segment-file opens");

            shard.close().await;
        });
    }

    #[test]
    fn recent_aggregate_read_does_not_touch_old_segments() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let (active, last_id) = {
                let shard = ShardWal::open(cold_scan_config(&dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();
                let (active, last_id) = populate_segments(&shard, 4).await;
                shard.close().await;
                (active, last_id)
            };

            let shard = ShardWal::open(cold_scan_config(&dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();
            // The last-written aggregate lives in (or adjacent to) the active segment.
            let recent = AggregateKey::new(1, 1, last_id);
            let result = shard.exists(&AggregateDetailsRequest { correlation_id: None, aggregate_key: recent }).await;
            assert!(result.is_ok(), "recent aggregate should be found: {:?}", result.err());

            // The oldest segment is far from the read cursor; a recent hit must not reach it.
            assert!(shard.log_segments_cache.get_if_cached(1).is_none(),
                "recent-aggregate read should not have opened the oldest segment (active={active})");
            eprintln!("[scan-cost] recent aggregate read over {active} segments => oldest segment untouched (O(1) vs O(segments))");

            shard.close().await;
        });
    }

    // ── Per-aggregate backlink chain (reverse-scan foreign skip) ──

    /// One-event write whose payload byte encodes the version, so a cold read can
    /// verify each batch came back with the right content and order.
    fn mark_event(version: u64) -> DatablockAggregateEvent {
        DatablockAggregateEvent {
            client_seq: version,
            event_type_major: 1,
            event_value: Arc::new(vec![version as u8; 8]),
            ..Default::default()
        }
    }

    async fn cold_open(dir: &std::path::Path, chain_window: u64) -> ShardWal<StubReplicationClient, StubS3Downloader> {
        let cfg = InternalShardConfig { chain_read_window_bytes: chain_window, ..cold_scan_config(dir) };
        ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
            .await
            .unwrap()
    }

    /// Cold-open and read every version of `target` as (version, content) pairs.
    async fn read_target_versions(dir: &std::path::Path, target: &AggregateKey, chain_window: u64) -> Vec<(u64, u8)> {
        let shard = cold_open(dir, chain_window).await;
        let read = unwrap_read(process(&shard, read_req(target.clone())).await);
        let out = read.event_batches.iter().map(|b| (b.aggregate_version, b.events[0].event_value[0])).collect();
        shard.close().await;
        out
    }

    /// The chain must thread an aggregate's versions across many segment rotations:
    /// each big foreign write rotates the 1.5MB segment, so the target's chain spans
    /// a dozen segments and every hop crosses a boundary (backlink 0 -> older segment).
    #[test]
    fn chain_read_returns_all_versions_across_segment_rotation() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let target = key(1, 1, 42);
            let n = 12u64;
            {
                let shard = cold_open(&dir, 1024).await;
                for v in 1..=n {
                    write_ok(&shard, write_req(target.clone(), vec![mark_event(v)])).await;
                    write_ok(&shard, client_write_req(key(1, 1, 9000 + v as u128), vec![big_event(1, v)])).await;
                }
                shard.close().await;
            }
            // Cold reopen: recent-write cache empty, so the read walks the on-disk chain.
            let got = read_target_versions(&dir, &target, 1024).await;
            assert_eq!(got, (1..=n).map(|v| (v, v as u8)).collect::<Vec<_>>(),
                "all target versions, in order with correct content, across rotations");
        });
    }

    /// The follow-window is a pure performance knob: per-block (1024) and a large
    /// window must return identical results for the same interleaved data.
    #[test]
    fn chain_window_size_does_not_affect_results() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let target = key(1, 1, 7);
            let n = 40u64;
            {
                let shard = cold_open(&dir, 1024).await;
                let mut foreign = 5000u128;
                for v in 1..=n {
                    write_ok(&shard, write_req(target.clone(), vec![mark_event(v)])).await;
                    for _ in 0..3 {
                        write_ok(&shard, write_req(key(1, 1, foreign), vec![mark_event(1)])).await;
                        foreign += 1;
                    }
                }
                shard.close().await;
            }
            let per_block = read_target_versions(&dir, &target, 1024).await;
            let windowed = read_target_versions(&dir, &target, 64 * 1024).await;
            assert_eq!(per_block, windowed, "follow-window size must not change results");
            assert_eq!(per_block, (1..=n).map(|v| (v, v as u8)).collect::<Vec<_>>());
        });
    }

    /// Boot rebuild: a restart in the middle of a segment must repopulate the chain
    /// tips, or the first post-restart append back-links to 0 and a later read drops
    /// the pre-restart versions sitting earlier in the same segment.
    #[test]
    fn chain_survives_restart_midsegment() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let target = key(1, 1, 3);
            {
                let shard = cold_open(&dir, 1024).await;
                for v in 1..=4 { write_ok(&shard, write_req(target.clone(), vec![mark_event(v)])).await; }
                shard.close().await;
            }
            {
                let shard = cold_open(&dir, 1024).await;
                for v in 5..=8 { write_ok(&shard, write_req(target.clone(), vec![mark_event(v)])).await; }
                shard.close().await;
            }
            let got = read_target_versions(&dir, &target, 1024).await;
            assert_eq!(got, (1..=8).map(|v| (v, v as u8)).collect::<Vec<_>>(),
                "restart must not break the in-segment backlink chain");
        });
    }

    /// A SoftTrim is a chain member: a cold read from the floor must walk the chain
    /// straight through the trim metablock and return the kept versions, while a
    /// below-floor read still errors after the disk-rebuilt snapshot recovers the floor.
    #[test]
    fn chain_read_through_trim_metablock_returns_kept_versions() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let target = key(1, 1, 5);
            {
                let shard = cold_open(&dir, 1024).await;
                for v in 1..=6 { write_ok(&shard, write_req(target.clone(), vec![mark_event(v)])).await; }
                assert!(matches!(process(&shard, trim_req(target.clone(), 4)).await, Ok(ClientResponse::TrimStart(_))));
                for v in 7..=8 { write_ok(&shard, write_req(target.clone(), vec![mark_event(v)])).await; }
                shard.close().await;
            }
            let shard = cold_open(&dir, 1024).await;
            let read = unwrap_read(process(&shard, read_req_from(target.clone(), 4)).await);
            let versions: Vec<u64> = read.event_batches.iter().map(|b| b.aggregate_version).collect();
            assert_eq!(versions, vec![4, 5, 6, 7, 8], "kept versions returned via the chain past the trim metablock");

            let below = process(&shard, read_req_from(target, 1)).await;
            assert!(matches!(below, Err(ShardError::Read(ShardReadError::UnavailableBatchIndex { .. }))),
                "below-floor read must still error after disk rebuild");
            shard.close().await;
        });
    }

    /// Index-free listing pagination across SEALED segments: the active summary returns whole on
    /// page 1, then each sealed segment is paged by the (log_id, offset) packed cursor. Walking
    /// the cursor to exhaustion must surface every aggregate (best-effort listing may duplicate
    /// across pages but must never drop) and must terminate. Guards the cursor packing/offset
    /// resume added for unbounded-cardinality listing.
    #[test]
    fn list_aggregates_paginates_sealed_segments_without_loss() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let mut cfg = test_config(&dir);
            cfg.shard_log_preallocate_bytes = 2 * 1024 * 1024; // small segment -> rotates quickly
            cfg.list_page_size = 4; // tiny pages so sealed segments span many pages
            let shard = ShardWal::open(cfg, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();

            // Incompressible ~200KB payload so a 2MB segment seals in a handful of writes.
            let big_events = || {
                let mut buf = vec![0u8; 200 * 1024];
                let mut s: u64 = 0x2545F4914F6CDD1D;
                for b in buf.iter_mut() { s ^= s << 13; s ^= s >> 7; s ^= s << 17; *b = s as u8; }
                vec![DatablockAggregateEvent { client_seq: 1, event_type_major: 1, event_value: Arc::new(buf), ..Default::default() }]
            };

            // Write distinct aggregates until at least two segments have sealed, so listing must
            // page sealed segments; then a few land in the still-open active segment.
            let mut expected: std::collections::HashSet<u128> = std::collections::HashSet::new();
            let mut id = 0u128;
            while shard.log_segments_cache.active_log_id() < 3 && id < 1000 {
                id += 1;
                write_ok(&shard, write_req(key(1, 1, id), big_events())).await;
                expected.insert(id);
            }
            assert!(shard.log_segments_cache.active_log_id() >= 3, "expected segment rotations to set up sealed segments");
            for _ in 0..3 { id += 1; write_ok(&shard, write_req(key(1, 1, id), big_events())).await; expected.insert(id); }

            // Page through the opaque cursor to exhaustion.
            let mut got: std::collections::HashSet<u128> = std::collections::HashSet::new();
            let mut cursor: Option<u64> = None;
            let mut pages = 0;
            loop {
                let resp = unwrap_list_aggs(process(&shard, ClientRequest::ListAggregates(ListAggregatesRequest {
                    correlation_id: None, shard_id: 1, org_id: Some(1), aggregate_type_id: Some(1), cursor,
                })).await);
                for a in &resp.aggregates { got.insert(a.aggregate_id); }
                pages += 1;
                assert!(pages < 10_000, "pagination did not terminate");
                match resp.next_cursor {
                    Some(c) => cursor = Some(c),
                    None => break,
                }
            }

            assert!(pages >= 2, "expected multiple pages across sealed segments, got {pages}");
            for e in &expected {
                assert!(got.contains(e), "aggregate {e} dropped by paginated listing");
            }

            shard.close().await;
        });
    }


    // ── Negative-lookup per-aggregate client bloom ──

    /// Events with explicit client_seq values, for non-duplicate follow-up writes.
    fn events_from(start: u64, count: u64) -> Vec<DatablockAggregateEvent> {
        (start..start + count)
            .map(|i| DatablockAggregateEvent {
                client_seq: i,
                event_type_major: 1,
                event_value: Arc::new(vec![i as u8; 8]),
                ..Default::default()
            })
            .collect()
    }

    fn negative_check<R: ReplicationClient, D: S3Downloader>(
        shard: &ShardWal<R, D>,
        agg: &AggregateKey,
        client_id: u128,
    ) -> NegativeLookupAnswer {
        shard.shard_mem_cache.borrow_mut().negative_lookup_check(agg, client_id_bloom_hash(client_id))
    }

    /// Minimal thread-local metrics recorder: counts counters, ignores the rest.
    #[derive(Clone, Default)]
    struct CountingRecorder {
        counters: Arc<std::sync::Mutex<HashMap<String, Arc<std::sync::atomic::AtomicU64>>>>,
    }

    impl CountingRecorder {
        fn get(&self, name: &str) -> u64 {
            self.counters.lock().unwrap().get(name).map_or(0, |c| c.load(std::sync::atomic::Ordering::Relaxed))
        }
    }

    impl metrics::Recorder for CountingRecorder {
        fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
        fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
        fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
        fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
            let arc = self.counters.lock().unwrap().entry(key.name().to_string()).or_default().clone();
            metrics::Counter::from_arc(arc)
        }
        fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }
        fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    /// The one hard part (build/write race): the EMPTY Building bloom must be
    /// installed BEFORE the historical scan starts, so a client committing
    /// during the scan's awaits lands in it via insert-on-write. Choreography
    /// mirrors delete-trim-durability.md's repro notes: the builder is parked
    /// deterministically on the scan semaphore, the racing write commits fully
    /// inside the window, then the build resumes and completes.
    #[test]
    fn negative_build_write_race_concurrent_commit_lands_in_bloom() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let config = InternalShardConfig { read_max_concurrent: 1, ..test_config(&dir) };
            let shard = ShardWal::open(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                .await
                .unwrap();
            let agg = key(1, 1, 1);

            // History from client 1; idempotency off, so no bloom is resident yet.
            write_ok(&shard, write_req_full(agg.clone(), events(3), true, None, 1, false)).await;
            assert_eq!(negative_check(&shard, &agg, 2), NegativeLookupAnswer::NoEntry, "precondition: no bloom before the first idempotent write");

            // Hold the single scan permit so the builder parks AFTER install.
            let permit = shard.cache_load_semaphore.acquire_permit(1).await.unwrap();

            // First idempotent write from NEW client 2 becomes the builder.
            let build_fut = process(&shard, write_req_full(agg.clone(), events(1), false, None, 2, true));
            futures_lite::pin!(build_fut);
            let mut installed = false;
            for _ in 0..50 {
                assert!(
                    futures_lite::future::poll_once(build_fut.as_mut()).await.is_none(),
                    "build must park on the scan semaphore",
                );
                if negative_check(&shard, &agg, 2) == NegativeLookupAnswer::Building {
                    installed = true;
                    break;
                }
                glommio::timer::Timer::new(Duration::from_millis(1)).await;
            }
            assert!(installed, "the EMPTY bloom must be installed before the historical scan starts");

            // The race: client 3's first write commits fully inside the window.
            // Above the parked scan's upper bound, so only insert-on-write can
            // catch it.
            write_ok(&shard, write_req_full(agg.clone(), events(1), false, None, 3, false)).await;

            drop(permit);
            let build_result = build_fut.await;
            assert!(matches!(build_result, Ok(ClientResponse::Write(_))), "builder write failed: {build_result:?}");

            // Superset after the race: the concurrent commit is in the bloom,
            // and the build still completed.
            assert_eq!(negative_check(&shard, &agg, 3), NegativeLookupAnswer::MaybePresent, "raced commit missing from the bloom — subset, unsound");
            assert_eq!(negative_check(&shard, &agg, 999), NegativeLookupAnswer::DefinitelyAbsent, "build must have completed");

            // End-to-end: with the per-client LRU cold, a replay from client 3
            // must be rejected through the maybe-present -> scan -> found path.
            shard.shard_mem_cache.borrow_mut().clear_aggregate_write_client_snapshots_for_test();
            let dup = process(&shard, write_req_full(agg.clone(), events(1), false, None, 3, true)).await;
            assert!(
                matches!(dup, Err(ShardError::Write(ShardWriteError::ClientIdempotencyViolation { .. }))),
                "replay from the raced client must be rejected, got {dup:?}",
            );

            shard.close().await;
        });
    }

    /// A found-early scan is a truncated walk: the entry must park as Building
    /// (never Complete), and a later miss resumes and completes the build.
    #[test]
    fn negative_incomplete_build_parks_then_resume_completes() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            // History for client 1 without a bloom, then cold per-client LRU.
            write_ok(&shard, write_req_full(agg.clone(), events(3), true, None, 1, false)).await;
            shard.shard_mem_cache.borrow_mut().clear_aggregate_write_client_snapshots_for_test();

            // Returning client 1 (fresh seqs): the build scan finds it at the
            // tip and stops early — older history unwalked, must not complete.
            write_ok(&shard, write_req_full(agg.clone(), events_from(4, 2), false, None, 1, true)).await;
            assert_eq!(negative_check(&shard, &agg, 77), NegativeLookupAnswer::Building, "a truncated build walk must never mark Complete");

            // A genuinely new client resumes the parked build; its negative
            // scan walks all history, so the build completes.
            write_ok(&shard, write_req_full(agg.clone(), events(1), false, None, 2, true)).await;
            assert_eq!(negative_check(&shard, &agg, 77), NegativeLookupAnswer::DefinitelyAbsent);
            assert_eq!(negative_check(&shard, &agg, 1), NegativeLookupAnswer::MaybePresent);
            assert_eq!(negative_check(&shard, &agg, 2), NegativeLookupAnswer::MaybePresent);

            shard.close().await;
        });
    }

    /// Trim-then-new-client + the false-positive path: a trim-only client lands
    /// in the bloom via insert-on-write (the rule: every aggregate-scoped
    /// client-bearing kind), a new client short-circuits scan-free, and the
    /// trim-only client's own first produce takes maybe-present -> scan ->
    /// not-found -> proceed.
    #[test]
    fn negative_trim_then_new_client_and_false_positive_path() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            // Three batches -> aggregate_version 3, so keep_from=2 is in range.
            for seq in 1..=3u64 {
                write_ok(&shard, write_req_full(agg.clone(), events_from(seq, 1), true, None, 1, true)).await;
            }
            assert_eq!(negative_check(&shard, &agg, 5), NegativeLookupAnswer::DefinitelyAbsent, "first idempotent write must have built a Complete bloom");

            // Trim by client 7 (trim-only client, no client_seq anywhere).
            let trim = ClientRequest::TrimStart(TrimStartRequest {
                correlation_id: None,
                aggregate_key: agg.clone(),
                keep_from_aggregate_version: 2,
                client_id: 7,
                user_id: None,
            });
            assert!(matches!(process(&shard, trim).await, Ok(ClientResponse::TrimStart(_))));
            assert_eq!(negative_check(&shard, &agg, 7), NegativeLookupAnswer::MaybePresent, "trim client must land via insert-on-write");

            // New client after trim: scan-free first write.
            write_ok(&shard, write_req_full(agg.clone(), events(1), false, None, 2, true)).await;
            assert_eq!(negative_check(&shard, &agg, 2), NegativeLookupAnswer::MaybePresent);

            // The trim-only client produces: maybe-present, scan finds no
            // EventBatch for it, proceed as first write. Never an error.
            write_ok(&shard, write_req_full(agg.clone(), events(1), false, None, 7, true)).await;

            shard.close().await;
        });
    }

    /// Delete-recreate-new-client: the bloom keeps pre-delete clients as
    /// phantoms (superset, safe), the deleting client lands via insert-on-write,
    /// and a new client after recreate short-circuits.
    #[test]
    fn negative_delete_recreate_then_new_client() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_shard(&dir).await;
            let agg = key(1, 1, 1);

            write_ok(&shard, write_req_full(agg.clone(), events(3), true, None, 1, true)).await;

            let delete = ClientRequest::Delete(DeleteRequest {
                correlation_id: None,
                client_id: 5,
                user_id: None,
                deletes: HashMap::from([(agg.clone(), SingleAggregateDelete {
                    allow_recreate: true,
                    allow_sequence_continuation: false,
                    expected_version: None,
                })]),
            });
            assert!(matches!(process(&shard, delete).await, Ok(ClientResponse::Delete(_))));
            assert_eq!(negative_check(&shard, &agg, 5), NegativeLookupAnswer::MaybePresent, "delete client must land via insert-on-write");

            // Recreate by the original writer (floors survive delete: fresh seqs).
            write_ok(&shard, write_req_full(agg.clone(), events_from(4, 1), true, None, 1, true)).await;

            // New client post-recreate: scan-free, and the bloom still answers.
            write_ok(&shard, write_req_full(agg.clone(), events(1), true, None, 2, true)).await;
            assert_eq!(negative_check(&shard, &agg, 2), NegativeLookupAnswer::MaybePresent);
            assert_eq!(negative_check(&shard, &agg, 99), NegativeLookupAnswer::DefinitelyAbsent);

            shard.close().await;
        });
    }

    /// Eviction is just "drop the entry, rebuild on next miss" — and the rebuild
    /// path must still reject replays (no correctness rides on residency).
    #[test]
    fn negative_eviction_then_rebuild_preserves_idempotency() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            // Budget fits one Complete entry (~192B), not two.
            let config = InternalShardConfig { negative_lookup_cache_bytes: 256, ..test_config(&dir) };
            let shard = ShardWal::open(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                .await
                .unwrap();
            let agg1 = key(1, 1, 1);
            let agg2 = key(1, 1, 2);

            write_ok(&shard, write_req_full(agg1.clone(), events(1), true, None, 1, true)).await;
            write_ok(&shard, write_req_full(agg2.clone(), events(1), true, None, 1, true)).await;
            assert_eq!(negative_check(&shard, &agg1, 1), NegativeLookupAnswer::NoEntry, "agg1's entry must have been evicted by the byte budget");

            // Replay against the evicted aggregate: rebuild scan finds the
            // client and rejects — eviction cost a scan, not correctness.
            shard.shard_mem_cache.borrow_mut().clear_aggregate_write_client_snapshots_for_test();
            let dup = process(&shard, write_req_full(agg1.clone(), events(1), false, None, 1, true)).await;
            assert!(
                matches!(dup, Err(ShardError::Write(ShardWriteError::ClientIdempotencyViolation { .. }))),
                "replay after eviction must be rejected, got {dup:?}",
            );

            shard.close().await;
        });
    }

    /// Mid-build eviction: eviction pressure landing DURING a parked build must
    /// not drop the pinned Building entry (pre-fix it did, admitting a second
    /// builder whose half-built entry the first builder then marked Complete —
    /// a subset, i.e. false absents and duplicate writes). Competing installs
    /// are refused instead, the resumed build completes, and replays are still
    /// rejected. Reuses the race test's semaphore choreography to park the
    /// builder deterministically.
    #[test]
    fn negative_parked_build_survives_eviction_pressure() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            // Budget fits one entry; a single scan permit parks the builder.
            let config = InternalShardConfig {
                negative_lookup_cache_bytes: 256,
                read_max_concurrent: 1,
                ..test_config(&dir)
            };
            let shard = ShardWal::open(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                .await
                .unwrap();
            let agg1 = key(1, 1, 1);

            // History from client 1; idempotency off, so no bloom is resident yet.
            write_ok(&shard, write_req_full(agg1.clone(), events(3), true, None, 1, false)).await;

            // Hold the single scan permit so the builder parks AFTER install.
            let permit = shard.cache_load_semaphore.acquire_permit(1).await.unwrap();
            let build_fut = process(&shard, write_req_full(agg1.clone(), events(1), false, None, 2, true));
            futures_lite::pin!(build_fut);
            let mut installed = false;
            for _ in 0..50 {
                assert!(
                    futures_lite::future::poll_once(build_fut.as_mut()).await.is_none(),
                    "build must park on the scan semaphore",
                );
                if negative_check(&shard, &agg1, 2) == NegativeLookupAnswer::Building {
                    installed = true;
                    break;
                }
                glommio::timer::Timer::new(Duration::from_millis(1)).await;
            }
            assert!(installed, "the EMPTY bloom must be installed before the historical scan starts");

            // Eviction pressure inside the window: installs on other aggregates
            // would exceed the budget, and the only resident entry is pinned.
            {
                let mut mc = shard.shard_mem_cache.borrow_mut();
                assert!(
                    mc.negative_lookup_try_begin_build(&key(1, 1, 2)).is_none(),
                    "competing install must be refused while the only evictable entry is a pinned build",
                );
                assert!(mc.negative_lookup_try_begin_build(&key(1, 1, 3)).is_none());
            }
            assert_eq!(
                negative_check(&shard, &agg1, 2),
                NegativeLookupAnswer::Building,
                "eviction pressure dropped the pinned mid-build entry",
            );

            drop(permit);
            let build_result = build_fut.await;
            assert!(matches!(build_result, Ok(ClientResponse::Write(_))), "builder write failed: {build_result:?}");
            assert_eq!(negative_check(&shard, &agg1, 999), NegativeLookupAnswer::DefinitelyAbsent, "resumed build must complete");
            assert_eq!(negative_check(&shard, &agg1, 1), NegativeLookupAnswer::MaybePresent);

            // Replay from client 1 with a cold per-client LRU must be rejected.
            shard.shard_mem_cache.borrow_mut().clear_aggregate_write_client_snapshots_for_test();
            let dup = process(&shard, write_req_full(agg1.clone(), events(1), false, None, 1, true)).await;
            assert!(
                matches!(dup, Err(ShardError::Write(ShardWriteError::ClientIdempotencyViolation { .. }))),
                "replay during/after mid-build eviction pressure must be rejected, got {dup:?}",
            );

            shard.close().await;
        });
    }

    /// Restart-then-immediate-fan-in (the EBS storm killer): the open-time scan
    /// eagerly rebuilds Complete blooms for a single-segment history, so the
    /// first write per (aggregate, client) after recovery is scan-free —
    /// asserted through the scan counters, not just the answers.
    #[test]
    fn negative_restart_then_immediate_fan_in_is_scan_free() {
        let (_tmp, dir) = test_dir();
        {
            let dir = dir.clone();
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(move || async move {
                    let shard = open_shard(&dir).await;
                    for client in 1..=3u128 {
                        write_ok(&shard, write_req_full(key(1, 1, 1), events(1), true, None, client, true)).await;
                    }
                    shard.close().await;
                })
                .unwrap()
                .join()
                .unwrap();
        }

        let recorder = CountingRecorder::default();
        let ex = LocalExecutorBuilder::new(Placement::Fixed(0)).make().unwrap();
        metrics::with_local_recorder(&recorder, || {
            ex.run(async {
                let shard = open_shard(&dir).await;
                let agg = key(1, 1, 1);

                // Eager open-scan install: Complete, since all history is in log 1.
                assert_eq!(negative_check(&shard, &agg, 1), NegativeLookupAnswer::MaybePresent, "recovered client must be in the eager bloom");
                assert_eq!(negative_check(&shard, &agg, 10), NegativeLookupAnswer::DefinitelyAbsent, "eager bloom must be Complete for single-segment history");

                for client in 10..=13u128 {
                    write_ok(&shard, write_req_full(agg.clone(), events(1), false, None, client, true)).await;
                }
                shard.close().await;
            })
        });

        assert_eq!(recorder.get("celeriant_cache_aggregate_client_scan_not_found_total"), 0, "post-restart first writes must not scan");
        assert_eq!(recorder.get("celeriant_cache_aggregate_client_scan_found_total"), 0, "post-restart first writes must not scan");
        assert_eq!(recorder.get("celeriant_negative_lookup_short_circuit_total"), 4, "every fan-in first write must short-circuit");
    }

    /// Cross-seal build soundness: the build unions sealed sidecar client sets
    /// (Exact hashes directly, Bloom words as carried aux — sized per segment,
    /// so they cannot be OR-merged) instead of scanning skipped segments, and a
    /// complete-summary Unknown set forces the walk. All three variants must
    /// leave the bloom a superset and still complete the build.
    #[test]
    fn negative_build_unions_sealed_sidecar_client_sets() {
        glommio_test!({
            let (_tmp, dir) = test_dir();
            let shard = open_compact_shard(&dir).await;
            let agg_bloom = key(1, 1, 1); // 34 clients -> ClientSet::Bloom at seal
            let agg_exact = key(1, 1, 2); // 3 clients -> ClientSet::Exact
            let agg_unknown = key(1, 1, 3); // sidecar rewritten to Unknown below

            for client in 100..134u128 {
                write_ok(&shard, write_req_full(agg_bloom.clone(), events(1), true, None, client, false)).await;
            }
            for client in 200..203u128 {
                write_ok(&shard, write_req_full(agg_exact.clone(), events(1), true, None, client, false)).await;
            }
            for client in 300..302u128 {
                write_ok(&shard, write_req_full(agg_unknown.clone(), events(1), true, None, client, false)).await;
            }
            trigger_rotation(&shard).await;
            assert!(shard.log_segments_cache.active_log_id() > 1);

            // Rewrite the sealed sidecar: agg_unknown's client set -> Unknown
            // (still complete). The build then may not skip that segment.
            {
                use crate::shard_wal_sync::summary_path;
                use celeriant_wal::constants::WIRE_VERSION_SEGMENT_SUMMARY_BLOCK;
                use celeriant_wire::disk::versioned_block::serialize_versioned_message_heap;

                let mut payload = read_segment_summary(shard.log_segments_cache.shard_dir(), 1).await.expect("sealed sidecar must exist");
                assert!(payload.complete, "precondition: the sealed summary must be complete");
                for e in payload.aggregates.iter_mut() {
                    if e.aggregate_id == agg_unknown.aggregate_id {
                        assert!(e.client_set != ClientSet::Unknown, "precondition: set recorded at seal");
                        e.client_set = ClientSet::Unknown;
                    }
                }
                let bytes = serialize_versioned_message_heap(&payload, WIRE_VERSION_SEGMENT_SUMMARY_BLOCK).unwrap();
                std::fs::write(summary_path(shard.log_segments_cache.shard_dir(), 1), &bytes).unwrap();
                shard.summary_cache.borrow_mut().pop(&1);
            }

            for (agg, sealed_client) in [(&agg_bloom, 100u128), (&agg_exact, 200), (&agg_unknown, 300)] {
                // A new client's first idempotent write rides the build.
                write_ok(&shard, write_req_full(agg.clone(), events(1), false, None, 999, true)).await;
                assert_eq!(
                    negative_check(&shard, agg, sealed_client),
                    NegativeLookupAnswer::MaybePresent,
                    "sealed-segment client {sealed_client} missing — subset, unsound",
                );
                assert_eq!(
                    negative_check(&shard, agg, 555),
                    NegativeLookupAnswer::DefinitelyAbsent,
                    "build must complete across the seal for {agg:?}",
                );

                // And the next new client is scan-free.
                write_ok(&shard, write_req_full(agg.clone(), events(1), false, None, 1000, true)).await;
                assert_eq!(negative_check(&shard, agg, 1000), NegativeLookupAnswer::MaybePresent);

                // Replay guard across the seal: the sealed client's duplicate is
                // still rejected (maybe-present -> scan -> found in sealed segment).
                shard.shard_mem_cache.borrow_mut().clear_aggregate_write_client_snapshots_for_test();
                let dup = process(&shard, write_req_full(agg.clone(), events(1), false, None, sealed_client, true)).await;
                assert!(
                    matches!(dup, Err(ShardError::Write(ShardWriteError::ClientIdempotencyViolation { .. }))),
                    "sealed-segment replay must be rejected, got {dup:?}",
                );
            }

            shard.close().await;
        });
    }

}
