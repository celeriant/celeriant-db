use crate::cache_path::CachePath;
use crate::cached_schema::{CachedSchema, Validate};
use crate::mem_snapshot_aggregate::AggregateStatus;
use crate::metablock_position::MetablockPosition;
use crate::pending_commit_data::PendingCommitData;
use crate::{
    aggregate_recent_write::AggregateRecentWrites, mem_snapshot_aggregate::MemSnapshotAggregate, queue_aggregate_positions::QueueAggregatePositions,
    recent_write::RecentWrite, shard_log_queue_item::ShardLogQueueItem, sync_positions_snapshot::{SyncPositionsSnapshot},
};
use crate::negative_lookup::NegativeClientBloom;
use celeriant_distributed::node_status::NodeStatus;
use celeriant_wal::aggregate_client_key::client_id_bloom_hash;
use celeriant_wal::constants::{AGGREGATE_BLOOM_BYTES, CLIENT_BLOOM_BYTES};
use celeriant_wal::metablocks::metablock_event_batch::MetablockEventBatch;
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_wal::precomputed_hash::{PrecomputedBuildHasher, PrecomputedMap, PrecomputedSet};
use celeriant_wal::schema_key::SchemaKey;
use celeriant_wal::segment_summary::client_set::{ClientSet, sized_bloom_from_hashes};
use celeriant_wal::segment_summary::schema_hash_accumulator::SchemaHashAccumulator;
use celeriant_wal::segment_summary::segment_aggregate_entry::SegmentAggregateEntry;
use celeriant_wal::segment_summary::segment_summary_payload::SegmentSummaryPayload;
use celeriant_wal::{
    aggregate_client_key::AggregateClientKey, aggregate_key::AggregateKey, aggregate_type_key::AggregateTypeKey,
    constants::FIXED_BLOCK_SIZE_BYTES, datablocks::datablock::Datablock,
    metablocks::metablock::Metablock,
};
use lru::LruCache;
use std::hash::Hash;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    num::NonZeroUsize,
};

/// Result of `get_client_seq_entry`. The variant gates the OCC decision: a queue
/// entry is by definition not yet fsynced, while an LRU entry may be durable
/// (wal_seq=0 disk-scan or wal_seq <= read cursor) or in-flight (wal_seq > read cursor).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientSeqStatus {
    InflightInQueue { client_seq: u64 },
    Fsynced { client_seq: u64, wal_seq: u64 },
}

impl ClientSeqStatus {
    pub fn client_seq(self) -> u64 {
        match self {
            Self::InflightInQueue { client_seq } | Self::Fsynced { client_seq, .. } => client_seq,
        }
    }
}

pub struct ShardMemCache<V: Validate> {
    recent_write_cache_bytes: u64,

    /// Cache of recent writes indexed by aggregate key.
    /// Only populated after successful durable write.
    aggregate_recent_writes: PrecomputedMap<AggregateKey, AggregateRecentWrites>,

    /// Current size of the recent write cache in bytes
    cache_current_bytes: u64,

    /// Eviction queue: (aggregate_key, aggregate_version, size_bytes) in insertion order
    cache_eviction_queue: VecDeque<(AggregateKey, u64, u64)>,

    /// LRU cache of aggregate positions committed to file (batch and event sequences)
    /// Updated after fsync - used by write path (OCC, idempotency)
    aggregate_write_snapshots: LruCache<AggregateKey, MemSnapshotAggregate, PrecomputedBuildHasher>,

    /// LRU cache of aggregate positions visible to readers
    /// Updated after replication - used by read path
    aggregate_read_snapshots: LruCache<AggregateKey, MemSnapshotAggregate, PrecomputedBuildHasher>,

    /// LRU cache of client event sequences committed to file
    /// Missing here does not mean client hasn't written to aggregate, just not in cache
    /// Stores (client_seq, wal_seq). wal_seq=0 means loaded from disk (always durable).
    aggregate_write_client_snapshots: LruCache<AggregateClientKey, (u64, u64), PrecomputedBuildHasher>,

    /// Indexes representing the in-memory positions of the next write for each aggregate
    /// These are writes yet to be written to disk
    /// This is unbounded as we expect quick flush to disk
    aggregate_queue_positions: PrecomputedMap<AggregateKey, QueueAggregatePositions>,

    /// Writes from clients that are pending write to disk
    /// This is unbounded as we expect quick flush to disk
    pending_append_queue: Vec<ShardLogQueueItem>,

    /// LRU cache of compiled schemas (Validated/CompilationFailed only).
    schema_cache: LruCache<SchemaKey, CachedSchema<V>, PrecomputedBuildHasher>,

    /// LRU cache of keys confirmed to have no schema in WAL.
    /// Separated from schema_cache so large validators can't evict these tiny entries.
    no_schema_cache: LruCache<SchemaKey, (), PrecomputedBuildHasher>,

    /// Pending schema registrations not yet fsynced (D4)
    /// Checked alongside schema_cache to prevent concurrent duplicate registrations.
    /// Cleared on fsync rollback.
    pending_schema_registrations: PrecomputedSet<SchemaKey>,

    /// Batches awaiting replication (post-fsync, pre-commit). Bounded indirectly
    /// by the inflight cap: writes are rejected at entry when
    /// `pending_append_bytes + pending_replication_bytes >= internode_max_request_size`,
    /// so the snapshot captured from this queue always fits in one TCP request.
    pending_replication_batches: Vec<PendingCommitData>,

    /// Total bytes in pending replication queue
    pending_replication_bytes: u64,

    /// Follower's deferred read-side commits (post-fsync, pre-leader-confirmation),
    /// wal_seq-ordered by construction (pushed in fsync order). Drained when a
    /// carrier's `leader_confirmed_wal_seq` covers a batch's tip. Bounded by
    /// in-flight replication (the leader sends one snapshot at a time and its
    /// confirmation rides the next carrier); `push_parked_commit` reports an
    /// overflow of the inflight cap as a tripwire without dropping data.
    parked_commits: VecDeque<PendingCommitData>,

    /// Total bytes in the parked commit queue
    parked_commit_bytes: u64,

    /// Total bytes in the pre-fsync queue (`pending_append_queue`). Updated on
    /// every push and reset to zero in `take_sync_positions_snapshot` when fsync
    /// drains the queue into a PCD.
    pending_append_bytes: u64,

    /// Inflight cap. The sum `pending_append_bytes + pending_replication_bytes`
    /// must stay below this value; checked at write entry and surfaced as
    /// `ReplicationBackpressure` (wire: ServerBusy) when reached. Equal to the
    /// inter-node TCP request cap so the next replication snapshot always fits
    /// in one TCP request.
    internode_max_request_size: u64,

    /// Flag set when fsync rollback occurs, cleared by following leader
    /// Used to distinguish "empty queue due to rollback" from "empty queue due to race".
    fsync_rollback_occurred: bool,

    /// Flag set when replication rollback occurs, cleared by following leader
    /// Used to distinguish "empty queue due to rollback" from "empty queue due to race".
    replication_rollback_occurred: bool,

    /// Monotonically increasing counter bumped on every fsync or replication rollback
    rollback_generation: u64,

    /// Running segment summary: per-aggregate stats accumulated since last rotation
    segment_summary: HashMap<AggregateKey, SegmentAggregateEntry>,
    segment_summary_orgs: HashSet<u128>,
    segment_summary_types: HashSet<AggregateTypeKey>,
    /// Per-aggregate distinct client hashes for the active segment. 8 bytes per
    /// distinct (aggregate, client) per segment; drained at seal with the rest
    /// of the accumulator. Converted to the wire ClientSet at take time.
    segment_summary_clients: HashMap<AggregateKey, HashSet<u64>>,
    /// Schema hashes registered in the active segment (bounded; volume
    /// overflow degrades to a saturating max-size bloom). Fed at commit and by
    /// the open-time rebuild scan; converted to the schema bloom at take time.
    segment_summary_schemas: SchemaHashAccumulator,
    /// Chain hashes the summary map deliberately drops but the seal-time
    /// segment blooms must still cover: a SoftTrim landing in a segment where
    /// its aggregate has no other block gets no summary entry (dedup-scan
    /// semantics), yet the aggregate-load scan reads that trim's floor — a
    /// bloom missing the key would skip the segment and hand out a STALE trim
    /// floor. Same for its client in the segment client bloom.
    segment_summary_loose: LooseChainHashes,
    /// True when the active accumulator provably missed commits (truncated
    /// pre-warm replay, post-truncate re-activation). Carried into the payload
    /// as `!complete` at drain time; drains reset it — a fresh segment's fold
    /// sees every commit from birth.
    segment_summary_incomplete: bool,
    /// True from a seal's accumulator drain until the shard layer reports the
    /// rotation complete. In that window `active_log_id` still names the
    /// SEALING segment while the drained accumulator describes no segment at
    /// all, so the active-segment schema consult must not answer definite
    /// absence from it (a committed registration in the sealing segment would
    /// read as absent — false no_schema, cached).
    segment_summary_draining: bool,

    /// Sealed segment summaries waiting for the deferred read-side commit
    /// (leader: replication ACK; follower: leader confirmation) before sidecar
    /// write. Stored at rotation time, keyed by sealed log_id.
    sealed_segment_summaries: HashMap<u64, SealedSegmentSummary>,

    /// Per-aggregate negative-lookup client blooms (idempotency-negative-lookup.md).
    /// Demand/eager-built, in-memory only; bounded by `negative_lookup_cache_bytes`
    /// via manual byte-tracked eviction (entries are variable-size). Eviction just
    /// drops the entry — superset safety means no invalidation obligations anywhere.
    negative_lookup: LruCache<AggregateKey, NegativeClientBloom, PrecomputedBuildHasher>,
    negative_lookup_bytes: u64,
    negative_lookup_cache_bytes: u64,
    /// Monotonic builder-identity counter; each begin-build stamps its entry
    /// with the next value (see `NegativeClientBloom::build_generation`).
    negative_lookup_build_generation: u64,
}

/// Answer for the produce path's negative lookup (see `negative_lookup_check`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NegativeLookupAnswer {
    /// No bloom resident: today's full negative scan, which should build one.
    NoEntry,
    /// A bloom exists but is not complete: fall back to scan, trust nothing.
    Building,
    /// Complete bloom says the client never wrote here: scan-free first write.
    DefinitelyAbsent,
    /// Complete bloom says maybe (real member or false positive): scan.
    MaybePresent,
}

/// Trim-only chain hashes for the seal-time segment blooms (see the field doc
/// on `segment_summary_loose`). Bounded by distinct trim-only (key, client)
/// pairs per segment — same 8-bytes-per-distinct scale the client map accepts.
#[derive(Debug, Default)]
pub struct LooseChainHashes {
    keys: HashSet<u64>,
    clients: HashSet<u64>,
}

impl LooseChainHashes {
    fn merge(&mut self, other: LooseChainHashes) {
        self.keys.extend(other.keys);
        self.clients.extend(other.clients);
    }
}

/// In-memory accumulator for a sealed segment's summary, mirroring the active segment fields.
/// Converted to SegmentSummaryPayload (with seal-time right-sized blooms built from the
/// exact key knowledge here) when the segment becomes fully replicated. Outside the
/// DeepSizeOf-accounted caps — bounded by the count of sealed segments awaiting
/// replication confirmation.
pub struct SealedSegmentSummary {
    aggregates: HashMap<AggregateKey, SegmentAggregateEntry>,
    orgs: HashSet<u128>,
    aggregate_types: HashSet<AggregateTypeKey>,
    clients: HashMap<AggregateKey, HashSet<u64>>,
    schemas: SchemaHashAccumulator,
    loose: LooseChainHashes,
    complete: bool,
}

impl SealedSegmentSummary {
    /// Fold a colliding store for the same log_id into this slot. Reachable
    /// only via the seal-retry trace (rotation failed after the drain; the
    /// re-entered seal branch drains again for the SAME still-active segment
    /// — truncation clears slots, so no other flow collides). Everything in
    /// `other` was folded strictly AFTER this slot's contents, so unions and
    /// last-wins temporal fields are faithful; replacing the slot instead
    /// would persist a complete=true SUBSET — false absence for ACKed data.
    fn merge(&mut self, other: SealedSegmentSummary) {
        for (key, entry) in other.aggregates {
            match self.aggregates.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut o) => merge_entry_later_era(o.get_mut(), entry),
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(entry);
                }
            }
        }
        self.orgs.extend(other.orgs);
        self.aggregate_types.extend(other.aggregate_types);
        for (key, hashes) in other.clients {
            self.clients.entry(key).or_default().extend(hashes);
        }
        self.schemas.merge(other.schemas);
        self.loose.merge(other.loose);
        self.complete &= other.complete;
    }
}

impl<V: Validate> ShardMemCache<V> {
    /// Returns (is_loaded, last_client_seq)
    /// - is_loaded: true if we've already checked disk for this aggregate+client
    /// - last_client_seq: Some(idx) if client has written, None if not found
    pub fn aggregate_client_load_status(&mut self, aggregate_key: &AggregateKey, aggregate_client_key: &AggregateClientKey) -> (bool, Option<u64>) {
        // Check queue first
        if let Some(queue_pos) = self.aggregate_queue_positions.get(aggregate_key) {
            if let Some(&idx) = queue_pos.client_seqes.get(&aggregate_client_key.client_id) {
                return (true, Some(idx));
            }
        }

        // Check LRU cache
        if let Some(&(client_seq, _wal_seq)) = self.aggregate_write_client_snapshots.get(&aggregate_client_key) {
            // client_seq=0 is sentinel for "checked but client never wrote"
            let result = if client_seq == 0 { None } else { Some(client_seq) };
            return (true, result);
        }

        (false, None)
    }

    /// Clear all caches including read snapshots and recent writes.
    /// Used during WAL truncation and the demotion cull, where the local chain
    /// diverged from the authoritative one: parked deferred commits reference
    /// entries that may be discarded, so their watch events must never fire.
    pub fn clear_all_caches(&mut self) {
        self.execute_replication_rollback();
        self.clear_parked_commits();
        self.aggregate_read_snapshots.clear();
        self.aggregate_recent_writes.clear();
        self.cache_eviction_queue.clear();
        self.cache_current_bytes = 0;
    }

    /// Returns (is_loaded, status)
    /// - is_loaded: true if we've already checked disk for this aggregate
    /// - status: Found/NotFound/Deleted based on cache state
    pub fn aggregate_load_status(&mut self, aggregate_key: &AggregateKey, cache_path: CachePath) -> (bool, AggregateStatus) {
        // Check if in queue (being created/modified) - only for write path
        if cache_path == CachePath::Write {
            if let Some(queue_pos) = self.aggregate_queue_positions.get(aggregate_key) {
                metrics::counter!("celeriant_cache_aggregate_snapshot_hits_total").increment(1);
                if queue_pos.pending_delete {
                    return (true, AggregateStatus::Deleted);
                }
                return (true, AggregateStatus::Found);
            }
        }

        // Check if in snapshots cache
        let cache = match cache_path {
            CachePath::Read => &mut self.aggregate_read_snapshots,
            CachePath::Write => &mut self.aggregate_write_snapshots,
        };
        if let Some(snapshot) = cache.get(aggregate_key) {
            metrics::counter!("celeriant_cache_aggregate_snapshot_hits_total").increment(1);
            return (true, snapshot.status);
        }

        metrics::counter!("celeriant_cache_aggregate_snapshot_misses_total").increment(1);
        (false, AggregateStatus::NotFound)
    }

    /// Insert a write into the recent write cache. Call only after durable write.
    pub fn cache_recent_write(
        &mut self,
        aggregate_key: AggregateKey,
        aggregate_version: u64,
        metablock: Metablock,
        datablock: Option<Datablock>,
        size_bytes: u64,
    ) {
        let max_bytes = self.recent_write_cache_bytes;
        if max_bytes == 0 {
            return;
        }

        // Evict until we have room
        while self.cache_current_bytes + size_bytes > max_bytes {
            if !self.evict_oldest_cache_entry() {
                break; // Cache is empty, nothing to evict
            }
        }

        // Insert new entry
        let aggregate_writes = self
            .aggregate_recent_writes
            .entry(aggregate_key.clone())
            .or_insert_with(|| AggregateRecentWrites::new(aggregate_version));

        aggregate_writes.push(RecentWrite {
            metablock,
            datablock,
            size_bytes,
        });

        self.cache_current_bytes = self.cache_current_bytes.saturating_add(size_bytes);
        self.cache_eviction_queue.push_back((aggregate_key, aggregate_version, size_bytes));
        metrics::gauge!("celeriant_cache_recent_write_bytes").set(self.cache_current_bytes as f64);
    }

    fn evict_oldest_cache_entry(&mut self) -> bool {
        let Some((aggregate_key, _aggregate_version, size_bytes)) = self.cache_eviction_queue.pop_front() else {
            return false;
        };

        if let Some(aggregate_writes) = self.aggregate_recent_writes.get_mut(&aggregate_key) {
            if aggregate_writes.pop_front() {
                self.cache_current_bytes = self.cache_current_bytes.saturating_sub(size_bytes);
            }
            if aggregate_writes.is_empty() {
                self.aggregate_recent_writes.remove(&aggregate_key);
            }
        }

        // Periodically reclaim memory from data structures
        if self.cache_eviction_queue.capacity() > self.cache_eviction_queue.len() * 2 {
            self.cache_eviction_queue.shrink_to_fit();
        }
        if self.aggregate_recent_writes.capacity() > self.aggregate_recent_writes.len() * 2 {
            self.aggregate_recent_writes.shrink_to_fit();
        }

        true
    }

    /// Add a pending trim to the queue. Carries the aggregate's current
    /// version/event_seq so a trim-only entry doesn't shadow durable state
    /// in `get_write_event_seqes` (it returns the queue entry early).
    pub fn add_pending_trim_to_queue(
        &mut self,
        aggregate_key: &AggregateKey,
        keep_from_aggregate_version: u64,
        aggregate_version: u64,
        event_seq: u64,
        shard_log_queue_item: ShardLogQueueItem,
    ) {
        let aggregate = self
            .aggregate_queue_positions
            .entry(aggregate_key.clone())
            .or_insert_with(|| QueueAggregatePositions::default());

        // Update min_aggregate_version if the new trim is higher
        if keep_from_aggregate_version > aggregate.min_aggregate_version {
            aggregate.min_aggregate_version = keep_from_aggregate_version;
        }
        if aggregate_version > aggregate.aggregate_version {
            aggregate.aggregate_version = aggregate_version;
        }
        if event_seq > aggregate.event_seq {
            aggregate.event_seq = event_seq;
        }

        self.pending_append_bytes = self.pending_append_bytes.saturating_add(shard_log_queue_item.size_bytes());
        self.pending_append_queue.push(shard_log_queue_item);
    }

    /// Commit a SoftTrim into the snapshot cache. Unlike
    /// `update_aggregate_min_aggregate_version`, a cache miss INSERTS the
    /// snapshot from the trim's recorded state; the SoftTrim metablock
    /// carries full aggregate state, and a silently-skipped floor bump on an
    /// LRU-evicted snapshot loses an acked trim: the next write embeds the
    /// stale floor and every node that rebuilds from it inherits the loss.
    /// Version/event_seq/min all max-merge so a late replay can't regress.
    #[allow(clippy::too_many_arguments)]
    pub fn commit_trim_snapshot(
        &mut self,
        aggregate_key: &AggregateKey,
        keep_from_aggregate_version: u64,
        aggregate_version: u64,
        event_seq: u64,
        log_id: u64,
        metablock_absolute_pos: u64,
        cache_path: CachePath,
    ) {
        let cache = match cache_path {
            CachePath::Read => &mut self.aggregate_read_snapshots,
            CachePath::Write => &mut self.aggregate_write_snapshots,
        };
        if let Some(snapshot) = cache.get_mut(aggregate_key) {
            if keep_from_aggregate_version > snapshot.min_aggregate_version {
                snapshot.min_aggregate_version = keep_from_aggregate_version;
            }
            if aggregate_version > snapshot.aggregate_version {
                snapshot.aggregate_version = aggregate_version;
            }
            if event_seq > snapshot.event_seq {
                snapshot.event_seq = event_seq;
            }
        } else {
            cache.put(aggregate_key.clone(), MemSnapshotAggregate::found(
                log_id,
                metablock_absolute_pos,
                event_seq,
                aggregate_version,
                keep_from_aggregate_version,
            ));
        }
        self.evict_trimmed_recent_writes(aggregate_key, keep_from_aggregate_version, cache_path);
    }

    /// Update min_aggregate_version in the aggregate snapshot cache
    pub fn update_aggregate_min_aggregate_version(&mut self, aggregate_key: &AggregateKey, min_aggregate_version: u64, cache_path: CachePath) {
        let cache = match cache_path {
            CachePath::Read => &mut self.aggregate_read_snapshots,
            CachePath::Write => &mut self.aggregate_write_snapshots,
        };
        if let Some(snapshot) = cache.get_mut(aggregate_key) {
            if min_aggregate_version > snapshot.min_aggregate_version {
                snapshot.min_aggregate_version = min_aggregate_version;
            }
        }

        self.evict_trimmed_recent_writes(aggregate_key, min_aggregate_version, cache_path);
    }

    fn evict_trimmed_recent_writes(&mut self, aggregate_key: &AggregateKey, min_aggregate_version: u64, cache_path: CachePath) {
        if cache_path == CachePath::Read {
            // Also evict any cached writes that are now trimmed
            if let Some(writes) = self.aggregate_recent_writes.get_mut(aggregate_key) {
                while writes.first_version < min_aggregate_version && !writes.is_empty() {
                    if let Some(front) = writes.writes.front() {
                        self.cache_current_bytes = self.cache_current_bytes.saturating_sub(front.size_bytes);
                    }
                    writes.pop_front();
                }
                if writes.is_empty() {
                    self.aggregate_recent_writes.remove(aggregate_key);
                }
            }

            // Clean up eviction queue for trimmed entries
            self.cache_eviction_queue
                .retain(|(k, batch_idx, _)| k != aggregate_key || *batch_idx >= min_aggregate_version);
        }
    }

    /// Get cached writes for an aggregate from a starting aggregate version.
    /// Returns writes in batch order as (aggregate_version, &RecentWrite).
    /// Writes with `wal_seq > visible_wal_seq` are excluded.
    /// Returns None if aggregate not in cache.
    pub fn get_cached_writes_from(&self, aggregate_key: &AggregateKey, from_version: u64, visible_wal_seq: u64) -> impl Iterator<Item = (u64, &RecentWrite)> {
        self.aggregate_recent_writes
            .get(aggregate_key)
            .into_iter()
            .flat_map(move |aggregate_writes| aggregate_writes.iter_from(from_version))
            .filter(move |(_batch_idx, write)| write.metablock.wal_seq <= visible_wal_seq)
    }

    pub fn pending_append_queue_is_empty(&self) -> bool {
        self.pending_append_queue.is_empty()
    }

    /// Add prepared items directly to the pending queue (used for replication).
    /// Does not update aggregate/client tracking - those are handled on commit.
    pub fn add_to_pending_queue(&mut self, items: Vec<ShardLogQueueItem>) {
        let added: u64 = items.iter().map(|i| i.size_bytes()).sum();
        self.pending_append_bytes = self.pending_append_bytes.saturating_add(added);
        self.pending_append_queue.extend(items);
    }

    /// Add a pending delete to the queue
    pub fn add_pending_delete_to_queue(
        &mut self,
        aggregate_key: &AggregateKey,
        event_seq: u64,
        aggregate_version: u64,
        allow_recreate: bool,
        allow_sequence_continuation: bool,
        shard_log_queue_item: ShardLogQueueItem,
    ) {
        let aggregate = self
            .aggregate_queue_positions
            .entry(aggregate_key.clone())
            .or_insert_with(|| QueueAggregatePositions::default());

        aggregate.pending_delete = true;
        aggregate.allow_recreate = allow_recreate;
        aggregate.allow_sequence_continuation = allow_sequence_continuation;
        aggregate.aggregate_version = aggregate_version;
        aggregate.event_seq = event_seq;

        self.pending_append_bytes = self.pending_append_bytes.saturating_add(shard_log_queue_item.size_bytes());
        self.pending_append_queue.push(shard_log_queue_item);
    }

    /// Even though we haven't written to disk yet,
    /// we need to track the aggregate index positions
    /// and the client position for idempotency checks.
    /// We do this and add the new queue item entry for later write
    pub fn add_to_pending_append_queue(
        &mut self,
        aggregate_key: &AggregateKey,
        event_seq: u64,
        aggregate_version: u64,
        min_aggregate_version: u64,
        client_id: u128,
        client_seq: u64,
        shard_log_queue_item: ShardLogQueueItem,
    ) {
        let aggregate = self
            .aggregate_queue_positions
            .entry(aggregate_key.clone())
            .or_insert_with(|| QueueAggregatePositions::default());

        if aggregate_version > aggregate.aggregate_version {
            aggregate.aggregate_version = aggregate_version;
        }
        if event_seq > aggregate.event_seq {
            aggregate.event_seq = event_seq;
        }

        aggregate.min_aggregate_version = min_aggregate_version;
        aggregate.pending_delete = false;
        aggregate.had_event_batch = true;

        aggregate
            .client_seqes
            .entry(client_id)
            .and_modify(|existing| {
                if client_seq > *existing {
                    *existing = client_seq;
                }
            })
            .or_insert(client_seq);

        self.pending_append_bytes = self.pending_append_bytes.saturating_add(shard_log_queue_item.size_bytes());
        self.pending_append_queue.push(shard_log_queue_item);
    }

    /// When we begin writing to disk, we need to take the queue positions
    /// While disk is writing the queue is still available for the next batch
    pub fn take_sync_positions_snapshot(&mut self) -> SyncPositionsSnapshot {
        // Clone instead of swap - queue positions must remain visible for new writes
        // that arrive while this sync is in progress. The queue always represents
        // the latest indexes, even if they haven't been committed to disk yet.
        let aggregate_queue_positions = self.aggregate_queue_positions.clone();

        // Clear out the pending queue immediately, leaving it ready for new writes to be queued
        let mut pending_append_queue = vec![];
        std::mem::swap(&mut pending_append_queue, &mut self.pending_append_queue);
        self.pending_append_bytes = 0;

        // Drain pending schemas — schema_cache is the primary duplicate guard from here
        let mut pending_schema_registrations = PrecomputedSet::default();
        std::mem::swap(&mut pending_schema_registrations, &mut self.pending_schema_registrations);

        SyncPositionsSnapshot {
            aggregate_queue_positions,
            pending_append_queue,
            pending_schema_registrations,
        }
    }

    pub fn buffer_size_total(&self) -> u64 {
        self.buffer_size_datablocks().saturating_add(self.buffer_size_metablocks())
    }

    pub fn buffer_size_datablocks(&self) -> u64 {
        self.pending_append_queue
            .iter()
            .map(|item| item.datablock_bytes.as_ref().map_or(0, |bytes| bytes.len() as u64))
            .sum()
    }

    pub fn buffer_size_metablocks(&self) -> u64 {
        (self.pending_append_queue.len() * FIXED_BLOCK_SIZE_BYTES) as u64
    }

    /// If we have any failure to write to disk, set had_fsync_failure and clear
    /// out all our aggregate_queue_positions, falling back to the aggregate_file_positions store
    /// This also invalidates our pending_append_queue for our next fsync batch, as it'll have entries
    /// with the wrong indexes. Technically it could still be valid if we are writing different aggregates,
    /// but we would have to implement an 'aggregate aware' coordinator to error back to pending client tasks
    /// on a per-aggregate basis. And since the fsync failure mode is rare and generally a 'server ending' 
    /// situation, it's not worth the extra logic... just go scorched earth
    pub fn execute_fsync_rollback(&mut self) {
        self.aggregate_queue_positions.clear();
        self.pending_schema_registrations.clear();
        self.schema_cache.clear();
        self.no_schema_cache.clear();
        if !self.pending_append_queue.is_empty() {
            self.pending_append_queue.clear();
            self.pending_append_bytes = 0;
            self.fsync_rollback_occurred = true;
        }
        self.rollback_generation = self.rollback_generation.wrapping_add(1);
    }

    /// Snapshot for in-flight `write()` futures to detect "a rollback crossed me".
    pub fn rollback_generation(&self) -> u64 {
        self.rollback_generation
    }

    pub fn is_aggregate_snapshot_cache_full(&self, cache_path: CachePath) -> bool {
        let cache = match cache_path {
            CachePath::Read => &self.aggregate_read_snapshots,
            CachePath::Write => &self.aggregate_write_snapshots,
        };
        cache.len() == cache.cap().get()
    }

    pub fn is_aggregate_client_cache_full(&self) -> bool {
        self.aggregate_write_client_snapshots.len() == self.aggregate_write_client_snapshots.cap().get()
    }

    pub fn aggregate_write_client_snapshots_len(&self) -> usize {
        self.aggregate_write_client_snapshots.len()
    }

    pub fn aggregate_write_snapshots_len(&self) -> usize {
        self.aggregate_write_snapshots.len()
    }

    pub fn is_aggregate_snapshot_full_or_contains(&self, aggregate_key: &AggregateKey, cache_path: CachePath) -> bool {
        let cache = match cache_path {
            CachePath::Read => &self.aggregate_read_snapshots,
            CachePath::Write => &self.aggregate_write_snapshots,
        };
        if cache.len() == cache.cap().get() {
            return true;
        }
        cache.contains(&aggregate_key)
    }

    pub fn is_aggregate_client_cache_full_or_contains(&self, aggregate_client_key: &AggregateClientKey) -> bool {
        if self.aggregate_write_client_snapshots.len() == self.aggregate_write_client_snapshots.cap().get() {
            return true;
        }
        self.aggregate_write_client_snapshots.contains(&aggregate_client_key)
    }

    pub fn put_aggregate_into_cache_as_not_found(&mut self, aggregate_key: AggregateKey, cache_path: CachePath) {
        let snapshot = MemSnapshotAggregate::not_found();
        let cache = match cache_path {
            CachePath::Read => &mut self.aggregate_read_snapshots,
            CachePath::Write => &mut self.aggregate_write_snapshots,
        };
        put_with_priority(cache, aggregate_key, snapshot, false);
    }

    pub fn put_aggregate_client_into_cache(&mut self, aggregate_client_key: AggregateClientKey, last_client_seq: u64, low_priority: bool) {
        // wal_seq=0: data came from disk scan, always considered durable
        put_with_priority(
            &mut self.aggregate_write_client_snapshots,
            aggregate_client_key,
            (last_client_seq, 0u64),
            low_priority,
        );
    }

    /// Record `client_seq` for a client on aggregate. Does max-merge
    pub fn merge_aggregate_client_seq_max(&mut self, aggregate_client_key: AggregateClientKey, client_seq: u64, wal_seq: u64) {
        if let Some(existing) = self.aggregate_write_client_snapshots.get_mut(&aggregate_client_key) {
            if client_seq > existing.0 {
                *existing = (client_seq, wal_seq);
            }
        } else {
            self.aggregate_write_client_snapshots.put(aggregate_client_key, (client_seq, wal_seq));
        }
    }

    pub fn put_aggregate_into_cache_as_deleted(
        &mut self,
        aggregate_key: AggregateKey,
        log_id: u64,
        metablock_absolute_pos: u64,
        event_seq: u64,
        aggregate_version: u64,
        allow_recreate: bool,
        allow_sequence_continuation: bool,
        cache_path: CachePath,
    ) {
        let snapshot = MemSnapshotAggregate::deleted(log_id, metablock_absolute_pos, event_seq, aggregate_version, allow_recreate, allow_sequence_continuation);
        let cache = match cache_path {
            CachePath::Read => &mut self.aggregate_read_snapshots,
            CachePath::Write => &mut self.aggregate_write_snapshots,
        };
        // A tombstone landing over a snapshot with a higher version regresses cached
        // state; the stale-tombstone corruption signature. Detect, don't block.
        if let Some(existing) = cache.peek(&aggregate_key) {
            if existing.aggregate_version > aggregate_version {
                metrics::counter!("celeriant_tombstone_snapshot_regression_total").increment(1);
            }
        }
        put_with_priority(cache, aggregate_key.clone(), snapshot, false);

        if cache_path == CachePath::Read {
            // Also clear any recent writes for this aggregate
            if let Some(writes) = self.aggregate_recent_writes.remove(&aggregate_key) {
                let bytes_removed: u64 = writes.writes.iter().map(|w| w.size_bytes).sum();
                self.cache_current_bytes = self.cache_current_bytes.saturating_sub(bytes_removed);
            }

            // Remove from eviction queue (will be cleaned up lazily, but mark for skip)
            self.cache_eviction_queue.retain(|(k, _, _)| k != &aggregate_key);
        }
    }

    pub fn get_aggregate_snapshot(&mut self, aggregate_key: &AggregateKey, cache_path: CachePath) -> Option<MemSnapshotAggregate> {
        let cache = match cache_path {
            CachePath::Read => &mut self.aggregate_read_snapshots,
            CachePath::Write => &mut self.aggregate_write_snapshots,
        };
        cache.get(aggregate_key).cloned()
    }

    pub fn put_aggregate_snapshot_only(
        &mut self,
        aggregate_key: AggregateKey,
        snapshot: MemSnapshotAggregate,
        low_priority: bool,
        cache_path: CachePath,
    ) {
        let cache = match cache_path {
            CachePath::Read => &mut self.aggregate_read_snapshots,
            CachePath::Write => &mut self.aggregate_write_snapshots,
        };
        put_with_priority(cache, aggregate_key, snapshot, low_priority);
    }

    pub fn put_aggregate_into_cache(
        &mut self,
        aggregate_key: AggregateKey,
        snapshot: MemSnapshotAggregate,
        client_id: u128,
        last_client_seq: u64,
        low_priority: bool,
        cache_path: CachePath,
    ) {
        let cache = match cache_path {
            CachePath::Read => &mut self.aggregate_read_snapshots,
            CachePath::Write => &mut self.aggregate_write_snapshots,
        };
        put_with_priority(cache, aggregate_key.clone(), snapshot, low_priority);

        if cache_path == CachePath::Write {
            let client_key = AggregateClientKey::new(aggregate_key, client_id);
            // wal_seq=0: data came from disk scan, always considered durable
            put_with_priority(&mut self.aggregate_write_client_snapshots, client_key, (last_client_seq, 0u64), low_priority);
        }
    }

    pub fn commit_position_snapshot(&mut self, event_batch: &MetablockEventBatch, log_id: u64, metablock_absolute_pos: u64, cache_path: CachePath) {
        let cache = match cache_path {
            CachePath::Read => &mut self.aggregate_read_snapshots,
            CachePath::Write => &mut self.aggregate_write_snapshots,
        };
        if let Some(existing) = cache.get_mut(&event_batch.aggregate_key)
            && existing.status != AggregateStatus::NotFound {
            // A commit carrying a version at or below the cached one means a stale
            // batch was fsynced after newer state; upstream of the max-merge clamp.
            if event_batch.aggregate_version <= existing.aggregate_version && cache_path == CachePath::Write {
                metrics::counter!("celeriant_position_snapshot_stale_commit_total").increment(1);
            }
            existing.status = AggregateStatus::Found;
            if event_batch.aggregate_version > existing.aggregate_version {
                existing.aggregate_version = event_batch.aggregate_version;
            }
            if event_batch.max_event_seq > existing.event_seq {
                existing.event_seq = event_batch.max_event_seq;
            }
            if event_batch.trimmed_below_version > existing.min_aggregate_version {
                existing.min_aggregate_version = event_batch.trimmed_below_version;
            }
            existing.log_id = log_id;
            existing.metablock_absolute_pos = metablock_absolute_pos;
        } else {
            cache.put(event_batch.aggregate_key.clone(), MemSnapshotAggregate {
                log_id,
                metablock_absolute_pos,
                event_seq: event_batch.max_event_seq,
                aggregate_version: event_batch.aggregate_version,
                min_aggregate_version: event_batch.trimmed_below_version,
                status: AggregateStatus::Found,
                allow_sequence_continuation: false,
                allow_recreate: false,
            });
        }
    }

    /// Provide the aggregate_queue_positions snapshotted before disk write begun
    /// and this will update the aggregate_file_positions with the committed data.
    pub fn commit_sync_positions_snapshot(&mut self, node_status: NodeStatus, sync_positions_snapshot: SyncPositionsSnapshot) {
        for (key, queue_positions) in sync_positions_snapshot.aggregate_queue_positions {
            if queue_positions.pending_delete {
                continue; // Will be handled by put_aggregate_into_cache_as_deleted
            }

            // Trim-only entries never get disk positions assigned during sync
            // (log_id/pos stay default) — touching the snapshot caches from one
            // manufactures a (0,0) Found snapshot on a cache miss, which exists()
            // then chases to a MetablockReadError. The SoftTrim commit arm owns
            // the snapshot instead, with the trim metablock's full state and real
            // position. Client-seq tracking and the queue-entry cleanup below
            // still run for every entry.
            let has_event_batch = queue_positions.had_event_batch;

            if has_event_batch {
                // Always update write cache
                if let Some(existing) = self.aggregate_write_snapshots.get_mut(&key)
                && existing.status != AggregateStatus::NotFound {
                    existing.status = AggregateStatus::Found;
                    if queue_positions.aggregate_version > existing.aggregate_version {
                        existing.aggregate_version = queue_positions.aggregate_version;
                    }
                    if queue_positions.event_seq > existing.event_seq {
                        existing.event_seq = queue_positions.event_seq;
                    }
                    existing.log_id = queue_positions.log_id;
                    existing.metablock_absolute_pos = queue_positions.metablock_absolute_pos;
                } else {
                    let snapshot = MemSnapshotAggregate {
                        log_id: queue_positions.log_id,
                        metablock_absolute_pos: queue_positions.metablock_absolute_pos,
                        event_seq: queue_positions.event_seq,
                        aggregate_version: queue_positions.aggregate_version,
                        min_aggregate_version: queue_positions.min_aggregate_version,
                        status: AggregateStatus::Found,
                        allow_sequence_continuation: false,
                        allow_recreate: false,
                    };
                    self.aggregate_write_snapshots.put(key.clone(), snapshot);
                }

                if !node_status.is_leader() {
                    // Single-node: update read cache immediately.
                    // Follower does not enter here as aggregate_queue_positions is not populated during replication
                    if let Some(existing) = self.aggregate_read_snapshots.get_mut(&key)
                    && existing.status != AggregateStatus::NotFound {
                        existing.status = AggregateStatus::Found;
                        if queue_positions.aggregate_version > existing.aggregate_version {
                            existing.aggregate_version = queue_positions.aggregate_version;
                        }
                        if queue_positions.event_seq > existing.event_seq {
                            existing.event_seq = queue_positions.event_seq;
                        }
                        existing.log_id = queue_positions.log_id;
                        existing.metablock_absolute_pos = queue_positions.metablock_absolute_pos;
                    } else {
                        self.aggregate_read_snapshots.put(key.clone(), MemSnapshotAggregate {
                            log_id: queue_positions.log_id,
                            metablock_absolute_pos: queue_positions.metablock_absolute_pos,
                            event_seq: queue_positions.event_seq,
                            aggregate_version: queue_positions.aggregate_version,
                            min_aggregate_version: queue_positions.min_aggregate_version,
                            status: AggregateStatus::Found,
                            allow_sequence_continuation: false,
                            allow_recreate: false,
                        });
                    }
                }
            }

            // Tag each (client_seq, wal_seq) so the OCC check can distinguish inflight
            // (wal_seq > read cursor) from durable.
            let batch_wal_seq = queue_positions.wal_seq;
            for (client_id, client_seq) in queue_positions.client_seqes {
                self.merge_aggregate_client_seq_max(AggregateClientKey::new(key.clone(), client_id), client_seq, batch_wal_seq);
            }

            // Clean up queue entry only if it hasn't been updated by a newer write.
            // If a new write came in during sync, the queue will have higher indexes.
            if let Some(current_queue_pos) = self.aggregate_queue_positions.get(&key) {
                if current_queue_pos.aggregate_version == queue_positions.aggregate_version {
                    self.aggregate_queue_positions.remove(&key);
                }
            }
        }

        // Periodically reclaim memory from the queue HashMap
        if self.aggregate_queue_positions.capacity() > self.aggregate_queue_positions.len().saturating_mul(2) {
            self.aggregate_queue_positions.shrink_to_fit();
        }
    }

    /// Latest client_seq state for (aggregate, client). The variant distinguishes
    /// a pre-fsync queue entry (always in-flight) from an LRU entry (wal_seq decides
    /// in-flight vs durable; wal_seq=0 in the LRU means disk-scan-loaded and durable).
    pub fn get_client_seq_entry(&mut self, aggregate_key: &AggregateKey, client_id: u128) -> Option<ClientSeqStatus> {
        if let Some(queue_pos) = self.aggregate_queue_positions.get(aggregate_key) {
            if let Some(&client_seq) = queue_pos.client_seqes.get(&client_id) {
                return Some(ClientSeqStatus::InflightInQueue { client_seq });
            }
        }

        let client_key = AggregateClientKey::new(aggregate_key.clone(), client_id);
        self.aggregate_write_client_snapshots.get(&client_key).copied()
            .filter(|&(client_seq, _)| client_seq > 0)
            .map(|(client_seq, wal_seq)| ClientSeqStatus::Fsynced { client_seq, wal_seq })
    }

    pub fn get_client_seq(&mut self, aggregate_key: &AggregateKey, client_id: u128) -> Option<u64> {
        self.get_client_seq_entry(aggregate_key, client_id).map(|s| s.client_seq())
    }

    /// The log file and position of the last known written metablock for an aggregate
    pub fn get_aggregate_last_metablock_pos(&mut self, aggregate_key: &AggregateKey, cache_path: CachePath) -> MetablockPosition {
        let cache = match cache_path {
            CachePath::Read => &mut self.aggregate_read_snapshots,
            CachePath::Write => &mut self.aggregate_write_snapshots,
        };
        if let Some(file_pos) = cache.get(aggregate_key) {
            return MetablockPosition {
                log_id: file_pos.log_id,
                metablock_absolute_pos: file_pos.metablock_absolute_pos,
                aggregate_version: file_pos.aggregate_version,
                event_seq: file_pos.event_seq,
                min_aggregate_version: file_pos.min_aggregate_version,
            };
        }

        MetablockPosition {
            log_id: 0,
            metablock_absolute_pos: 0,
            aggregate_version: 0,
            event_seq: 0,
            min_aggregate_version: 0,
        }
    }

    /// Repoint cached metablock positions after `target_log_id` was compacted in place.
    /// Remaps each tip to its new offset, or evicts the entry if the block was dropped.
    /// Call synchronously right after the atomic swap (no await in between).
    pub fn remap_compacted_positions(&mut self, target_log_id: u64, new_tips: &HashMap<AggregateKey, u64>) {
        Self::remap_snapshot_lru(&mut self.aggregate_read_snapshots, target_log_id, new_tips);
        Self::remap_snapshot_lru(&mut self.aggregate_write_snapshots, target_log_id, new_tips);

        let mut dropped = Vec::new();
        for (key, pos) in self.aggregate_queue_positions.iter_mut() {
            if pos.log_id == target_log_id {
                match new_tips.get(key) {
                    Some(&new_pos) => pos.metablock_absolute_pos = new_pos,
                    None => dropped.push(key.clone()),
                }
            }
        }
        for key in dropped {
            self.aggregate_queue_positions.remove(&key);
        }
    }

    fn remap_snapshot_lru(
        cache: &mut LruCache<AggregateKey, MemSnapshotAggregate, PrecomputedBuildHasher>,
        target_log_id: u64,
        new_tips: &HashMap<AggregateKey, u64>,
    ) {
        let mut dropped = Vec::new();
        for (key, snapshot) in cache.iter_mut() {
            if snapshot.log_id == target_log_id {
                match new_tips.get(key) {
                    Some(&new_pos) => snapshot.metablock_absolute_pos = new_pos,
                    None => dropped.push(key.clone()),
                }
            }
        }
        for key in dropped {
            cache.pop(&key);
        }
    }

    /// Get the latest batch and event index for an aggregate
    /// Preference the queue first, then fallback to file if no queued items for aggregate
    pub fn get_write_event_seqes(&mut self, aggregate_key: &AggregateKey) -> EventIndexes {
        // Check queue first
        if let Some(queue_pos) = self.aggregate_queue_positions.get(aggregate_key) {
            return EventIndexes {
                pending_delete_or_deleted: queue_pos.pending_delete,
                allow_recreate: queue_pos.allow_recreate,
                allow_sequence_continuation: queue_pos.allow_sequence_continuation,
                aggregate_version: queue_pos.aggregate_version,
                event_seq: queue_pos.event_seq,
                min_aggregate_version: queue_pos.min_aggregate_version,
            };
        }

        // Fall back to file LRU
        if let Some(file_pos) = self.aggregate_write_snapshots.get(aggregate_key) {
            return EventIndexes {
                pending_delete_or_deleted: file_pos.status == AggregateStatus::Deleted,
                allow_recreate: file_pos.allow_recreate,
                allow_sequence_continuation: file_pos.allow_sequence_continuation,
                aggregate_version: file_pos.aggregate_version,
                event_seq: file_pos.event_seq,
                min_aggregate_version: file_pos.min_aggregate_version,
            };
        }

        EventIndexes {
            pending_delete_or_deleted: false,
            allow_recreate: false,
            allow_sequence_continuation: false,
            aggregate_version: 0,
            event_seq: 0,
            min_aggregate_version: 0,
        }
    }

    /// Get cached schema (Validated/CompilationFailed) for a schema key.
    pub fn schema_cache_get(&mut self, key: &SchemaKey) -> Option<&CachedSchema<V>> {
        self.schema_cache.get(key)
    }

    // INVARIANT: schema_cache and no_schema_cache are mutually exclusive per
    // key. A schema_cache entry (any state — Validated is inserted at write
    // time, NotYetLoaded means a registration block exists on disk) always
    // outranks an absence conclusion: an absence scan racing that
    // registration necessarily snapshotted the WAL before its blocks landed.
    // Both writers enforce it — insert pops the opposite cache's entry, and
    // the absence insert yields to an existing schema entry — so the caches
    // converge to schema-wins under either interleaving. Without this, a
    // stale no_schema entry could outlive the schema entry's LRU eviction and
    // `schema_cache_contains` would silently skip validation.

    /// Insert a Validated/CompilationFailed schema. Pops any no_schema entry
    /// (see the mutual-exclusion invariant above).
    pub fn schema_cache_insert(&mut self, key: SchemaKey, value: CachedSchema<V>) {
        self.no_schema_cache.pop(&key);
        self.schema_cache.put(key, value);
    }

    /// Record "no registration found by the absence scan" — a no-op when a
    /// schema entry exists (see the mutual-exclusion invariant above).
    pub fn no_schema_cache_insert(&mut self, key: SchemaKey) {
        if self.schema_cache.contains(&key) {
            return;
        }
        self.no_schema_cache.put(key, ());
    }

    /// Check if either cache contains a specific key. Promotes on hit.
    pub fn schema_cache_contains(&mut self, key: &SchemaKey) -> bool {
        self.schema_cache.get(key).is_some() || self.no_schema_cache.get(key).is_some()
    }

    /// Check if a real schema (Validated or CompilationFailed) exists for this key.
    pub fn schema_cache_has_schema(&self, key: &SchemaKey) -> bool {
        self.schema_cache.contains(key)
    }

    pub fn is_schema_cache_full(&self) -> bool {
        self.schema_cache.len() == self.schema_cache.cap().get()
    }

    /// Check if a schema registration is pending fsync (D4)
    pub fn schema_is_pending(&self, key: &SchemaKey) -> bool {
        self.pending_schema_registrations.contains(key)
    }

    pub fn schema_cache_clear(&mut self) {
        self.schema_cache.clear();
        self.no_schema_cache.clear();
    }

    /// Mark a schema as pending fsync (D4)
    pub fn schema_mark_pending(&mut self, key: SchemaKey) {
        self.pending_schema_registrations.insert(key);
    }

    /// Add a batch to the pending replication queue
    /// Returns true if high water mark exceeded
    pub fn push_pending_replication(&mut self, batch: PendingCommitData) {
        self.pending_replication_bytes = self.pending_replication_bytes.saturating_add(batch.size_bytes());
        self.pending_replication_batches.push(batch);
    }

    /// Take all pending batches for replication
    pub fn take_pending_replication(&mut self) -> Vec<PendingCommitData> {
        self.pending_replication_bytes = 0;
        let drained = std::mem::take(&mut self.pending_replication_batches);
        if !drained.is_empty() {
            metrics::counter!("celeriant_take_pending_replication_dropped_batches").increment(drained.len() as u64);
        }
        drained
    }

    /// Return batches to the front of the pending queue after a failed replication
    /// cycle. Preserves WAL order: returned batches will be replicated before any
    /// new batches that arrived while the failed cycle was in progress.
    pub fn return_to_pending_replication(&mut self, mut batches: Vec<PendingCommitData>) {
        let returned_bytes: u64 = batches.iter().map(|b| b.size_bytes()).sum();
        batches.append(&mut self.pending_replication_batches);
        self.pending_replication_batches = batches;
        self.pending_replication_bytes = self.pending_replication_bytes.saturating_add(returned_bytes);
    }

    /// Peek at oldest batch (for timeout checking)
    pub fn peek_pending_replication(&self) -> Option<&PendingCommitData> {
        self.pending_replication_batches.first()
    }

    pub fn pending_replication_bytes(&self) -> u64 {
        self.pending_replication_bytes
    }

    pub fn pending_replication_count(&self) -> usize {
        self.pending_replication_batches.len()
    }

    /// Park a follower's deferred read-side commit until a carrier confirms it.
    /// Returns true when the queue exceeds the inflight cap (tripwire only; the
    /// batch is parked regardless — dropping it would lose watch events).
    pub fn push_parked_commit(&mut self, batch: PendingCommitData) -> bool {
        debug_assert!(
            self.parked_commits.back().map_or(true, |b| b.log_metadata.write.wal_seq < batch.log_metadata.write.wal_seq),
            "parked commits must be pushed in ascending wal_seq order"
        );
        self.parked_commit_bytes = self.parked_commit_bytes.saturating_add(batch.size_bytes());
        self.parked_commits.push_back(batch);
        let over_cap = self.parked_commit_bytes > self.internode_max_request_size;
        if over_cap {
            metrics::counter!("celeriant_parked_commit_overflow_total").increment(1);
        }
        over_cap
    }

    /// Pop parked commits whose fsync-time write tip the carrier confirms.
    /// The queue is wal_seq-ordered, so this is a prefix drain.
    pub fn drain_parked_commits_up_to(&mut self, target_wal_seq: u64) -> Vec<PendingCommitData> {
        let mut drained = Vec::new();
        while self.parked_commits.front().map_or(false, |b| b.log_metadata.write.wal_seq <= target_wal_seq) {
            let batch = self.parked_commits.pop_front().expect("front checked above");
            self.parked_commit_bytes = self.parked_commit_bytes.saturating_sub(batch.size_bytes());
            drained.push(batch);
        }
        if self.parked_commits.is_empty() {
            debug_assert_eq!(self.parked_commit_bytes, 0, "parked byte accounting drifted from queue contents");
            self.parked_commit_bytes = 0;
        }
        drained
    }

    /// Take every parked commit (promotion: the whole durable tail commits).
    pub fn take_all_parked_commits(&mut self) -> Vec<PendingCommitData> {
        self.parked_commit_bytes = 0;
        self.parked_commits.drain(..).collect()
    }

    /// Discard parked commits (WAL truncation / demotion cull): the entries are
    /// gone from the chain, their watch events must never fire.
    pub fn clear_parked_commits(&mut self) -> usize {
        let dropped = self.parked_commits.len();
        self.parked_commits.clear();
        self.parked_commit_bytes = 0;
        dropped
    }

    pub fn parked_commit_bytes(&self) -> u64 {
        self.parked_commit_bytes
    }

    pub fn parked_commit_count(&self) -> usize {
        self.parked_commits.len()
    }

    pub fn pending_append_bytes(&self) -> u64 {
        self.pending_append_bytes
    }

    /// Combined inflight bytes: PCDs awaiting replication plus uncommitted
    /// pre-fsync queue. Both contribute to the next replication snapshot.
    pub fn inflight_bytes(&self) -> u64 {
        self.pending_append_bytes.saturating_add(self.pending_replication_bytes)
    }

    /// True when combined inflight bytes have reached the inter-node TCP cap.
    /// Reactive: a slight overshoot of one write is possible because the
    /// rejected write is already inflight when it's checked.
    pub fn is_inflight_pressured(&self) -> bool {
        self.inflight_bytes() >= self.internode_max_request_size
    }

    /// Clear caches that may contain un-replicated data (used during rollback).
    pub fn execute_replication_rollback(&mut self) {

        self.execute_fsync_rollback();

        // Written to disk but not replicated
        self.aggregate_write_snapshots.clear();
        self.aggregate_write_client_snapshots.clear();

        // Pending replication, now cannot proceed
        if !self.pending_replication_batches.is_empty() {
            self.replication_rollback_occurred = true;
        }
        self.pending_replication_batches.clear();
        self.pending_replication_bytes = 0;
        self.sealed_segment_summaries.clear();
    }

    pub fn clear_aggregate_write_client_snapshots_for_test(&mut self) {
        self.aggregate_write_client_snapshots.clear();
    }

    pub fn clear_aggregate_write_snapshots_for_test(&mut self) {
        self.aggregate_write_snapshots.clear();
    }

    pub fn clear_aggregate_read_snapshots_for_test(&mut self) {
        self.aggregate_read_snapshots.clear();
    }

    pub fn aggregate_read_snapshots_len(&self) -> usize {
        self.aggregate_read_snapshots.len()
    }

    pub fn aggregate_recent_writes_len(&self) -> usize {
        self.aggregate_recent_writes.len()
    }

    /// Cull-side clear. Drains pending_replication and clears the OCC/idempotency
    /// LRUs that point at the discarded speculative tail. Leaves aggregate_queue_positions
    /// and schema caches alone (those belong to a real rollback).
    ///
    /// When the drain orphans queued PCDs, their writers are still in flight;
    /// set the rollback flag and bump the generation so they resolve with
    /// `RollbackInProgress` instead of a false `NoCaptureRaceButOk` ack
    pub fn clear_speculative_write_caches_for_cull(&mut self) -> usize {
        let drained = std::mem::take(&mut self.pending_replication_batches).len();
        self.pending_replication_bytes = 0;
        self.aggregate_write_snapshots.clear();
        self.aggregate_write_client_snapshots.clear();
        if drained > 0 {
            self.replication_rollback_occurred = true;
            self.rollback_generation = self.rollback_generation.wrapping_add(1);
        }
        drained
    }

    pub fn put_aggregate_write_client_snapshot_for_test(&mut self, aggregate_key: AggregateKey, client_id: u128, client_seq: u64, wal_seq: u64) {
        let key = AggregateClientKey::new(aggregate_key, client_id);
        self.aggregate_write_client_snapshots.put(key, (client_seq, wal_seq));
    }

    pub fn put_aggregate_queue_client_seq_for_test(&mut self, aggregate_key: AggregateKey, client_id: u128, client_seq: u64) {
        let q = self.aggregate_queue_positions.entry(aggregate_key).or_insert_with(QueueAggregatePositions::default);
        q.client_seqes.insert(client_id, client_seq);
    }

    /// Copy a single aggregate's write snapshot to read snapshot.
    /// Used during commit completion to make writes visible to readers.
    pub fn copy_write_to_read_snapshot(&mut self, key: &AggregateKey) {
        if let Some(write_snapshot) = self.aggregate_write_snapshots.get(key) {
            self.aggregate_read_snapshots.put(key.clone(), write_snapshot.clone());
        }
    }

    pub fn new(
        recent_write_cache_bytes: u64,
        aggregate_write_snapshots_cache_bytes: u64,
        aggregate_client_snapshots_cache_bytes: u64,
        schema_cache_bytes: u64,
        negative_lookup_cache_bytes: u64,
        internode_max_request_size: u64,
    ) -> Self {
        let aggregate_cap = NonZeroUsize::new((aggregate_write_snapshots_cache_bytes / 112) as usize).unwrap_or(NonZeroUsize::new(10_000).unwrap());
        let client_cap = NonZeroUsize::new((aggregate_client_snapshots_cache_bytes / 128) as usize).unwrap_or(NonZeroUsize::new(100_000).unwrap());
        let schema_half = schema_cache_bytes / 2;
        // Validated/CompilationFailed entries: ~100 bytes each (SchemaKey 56 + validator ref + LRU overhead)
        let schema_cap = NonZeroUsize::new((schema_half / 100) as usize).unwrap_or(NonZeroUsize::new(1_000).unwrap());
        // NoSchema entries: ~80 bytes each (SchemaKey 56 + unit + LRU overhead)
        let no_schema_cap = NonZeroUsize::new((schema_half / 80) as usize).unwrap_or(NonZeroUsize::new(1_000).unwrap());

        Self {
            recent_write_cache_bytes,
            aggregate_recent_writes: PrecomputedMap::default(),
            cache_current_bytes: 0,
            cache_eviction_queue: VecDeque::new(),
            aggregate_queue_positions: PrecomputedMap::default(),
            pending_append_queue: vec![],
            aggregate_write_snapshots: LruCache::with_hasher(aggregate_cap, PrecomputedBuildHasher::default()),
            aggregate_read_snapshots: LruCache::with_hasher(aggregate_cap, PrecomputedBuildHasher::default()),
            aggregate_write_client_snapshots: LruCache::with_hasher(client_cap, PrecomputedBuildHasher::default()),
            schema_cache: LruCache::with_hasher(schema_cap, PrecomputedBuildHasher::default()),
            no_schema_cache: LruCache::with_hasher(no_schema_cap, PrecomputedBuildHasher::default()),
            pending_schema_registrations: PrecomputedSet::default(),
            pending_replication_batches: Vec::new(),
            pending_replication_bytes: 0,
            parked_commits: VecDeque::new(),
            parked_commit_bytes: 0,
            pending_append_bytes: 0,
            internode_max_request_size,
            fsync_rollback_occurred: false,
            replication_rollback_occurred: false,
            rollback_generation: 0,
            segment_summary: HashMap::new(),
            segment_summary_orgs: HashSet::new(),
            segment_summary_types: HashSet::new(),
            segment_summary_clients: HashMap::new(),
            segment_summary_schemas: SchemaHashAccumulator::default(),
            segment_summary_loose: LooseChainHashes::default(),
            segment_summary_incomplete: false,
            segment_summary_draining: false,
            sealed_segment_summaries: HashMap::new(),
            negative_lookup: LruCache::unbounded_with_hasher(PrecomputedBuildHasher::default()),
            negative_lookup_bytes: 0,
            negative_lookup_cache_bytes,
            negative_lookup_build_generation: 0,
        }
    }

    // ── Negative-lookup client blooms ──────────────────────────────────────
    //
    // AUDIT SURFACE (insert-on-write exhaustiveness): every path that makes a
    // client-bearing metablock durable routes its commit through
    // `update_segment_summary` / `update_segment_summary_for_log`, which call
    // `negative_lookup_note_commit` first:
    //   - leader/standalone fsync FullCommit  (shard_wal_sync.rs commit fold)
    //   - follower confirmation drain + promotion drain (commit_pcd,
    //     shard_wal_replicate.rs)
    //   - S3-catchup commit                  (shard_wal_s3_catchup.rs)
    //   - open-time pre-warm replay          (shard_wal.rs pre_warm_cache)
    // A follower's fsync defers the fold until confirmation; in that window the
    // follower serves no writes, and every promotion path drains parked PCDs
    // through commit_pcd before the role flip, so the bloom catches up before
    // any lookup can consult it. Insert-before-replication is fine: a rolled
    // back write leaves a phantom, which is a superset, which is safe.

    /// The commit-side insert. No-op when no bloom is resident for the
    /// aggregate. Unlike the summary fold, SoftTrim is NOT gated on an existing
    /// per-segment entry — the bloom spans the aggregate's whole history.
    fn negative_lookup_note_commit(&mut self, metablock: &Metablock) {
        let (key, client_id) = match &metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(eb) => (&eb.aggregate_key, eb.client_id),
            MetablockKind::SoftDelete(sd) => (&sd.aggregate_key, sd.client_id),
            MetablockKind::SoftTrim(st) => (&st.aggregate_key, st.client_id),
            MetablockKind::SchemaRegistration(_) => return, // touches no aggregate
        };
        // Residency check first: skip the hash when no bloom is resident.
        if !self.negative_lookup.contains(key) {
            return;
        }
        self.negative_lookup_insert(key, client_id_bloom_hash(client_id));
    }

    /// Insert a client hash into the resident bloom for `key`; no-op when
    /// absent. Does not promote the entry (commit traffic is background).
    pub fn negative_lookup_insert(&mut self, key: &AggregateKey, client_hash: u64) {
        let Some(entry) = self.negative_lookup.peek_mut(key) else { return };
        let old = entry.byte_cost();
        entry.insert_hash(client_hash);
        let new = entry.byte_cost();
        self.negative_lookup_bytes = self.negative_lookup_bytes + new - old;
        self.negative_lookup_evict_to_budget();
    }

    /// The produce path's negative lookup. Promotes the entry on hit.
    pub fn negative_lookup_check(&mut self, key: &AggregateKey, client_hash: u64) -> NegativeLookupAnswer {
        match self.negative_lookup.get(key) {
            None => NegativeLookupAnswer::NoEntry,
            Some(entry) if !entry.is_complete() => NegativeLookupAnswer::Building,
            Some(entry) if entry.may_contain_hash(client_hash) => NegativeLookupAnswer::MaybePresent,
            Some(_) => NegativeLookupAnswer::DefinitelyAbsent,
        }
    }

    /// Become the builder for `key`, installing an EMPTY Building entry first
    /// (install-empty-then-populate: from this instant insert-on-write lands in
    /// it, so commits during the build scan's awaits are never lost). Returns
    /// the builder's generation token, or None when a build is already in
    /// flight, the entry is Complete, or the byte budget is exhausted with
    /// nothing evictable — callers then scan without building. Synchronous, so
    /// the check-and-set is atomic on the single-threaded executor.
    pub fn negative_lookup_try_begin_build(&mut self, key: &AggregateKey) -> Option<u64> {
        if let Some(entry) = self.negative_lookup.peek_mut(key) {
            if entry.is_complete() || entry.is_builder_active() {
                return None;
            }
            // Resume a parked build: keep collected members, re-union sidecars.
            self.negative_lookup_build_generation += 1;
            entry.set_builder(self.negative_lookup_build_generation);
            let old = entry.byte_cost();
            entry.reset_aux();
            let new = entry.byte_cost();
            self.negative_lookup_bytes = self.negative_lookup_bytes - (old - new);
            return Some(self.negative_lookup_build_generation);
        }
        let mut entry = NegativeClientBloom::new_building();
        self.negative_lookup_build_generation += 1;
        entry.set_builder(self.negative_lookup_build_generation);
        let cost = entry.byte_cost();
        // Make room first (pinned in-flight builds are never displaced); if the
        // budget still can't fit the entry, refuse — the caller falls back to a
        // plain scan and no entry is installed.
        self.negative_lookup_evict_until(self.negative_lookup_cache_bytes.saturating_sub(cost));
        if self.negative_lookup_bytes + cost > self.negative_lookup_cache_bytes {
            metrics::counter!("celeriant_negative_lookup_build_refused_no_budget_total").increment(1);
            return None;
        }
        self.negative_lookup_bytes += cost;
        self.negative_lookup.put(key.clone(), entry);
        Some(self.negative_lookup_build_generation)
    }

    /// Union a sealed sidecar's Exact client hashes into the building entry.
    pub fn negative_lookup_union_exact(&mut self, key: &AggregateKey, hashes: &[u64]) {
        let Some(entry) = self.negative_lookup.peek_mut(key) else { return };
        let old = entry.byte_cost();
        for h in hashes {
            entry.insert_hash(*h);
        }
        let new = entry.byte_cost();
        self.negative_lookup_bytes = self.negative_lookup_bytes + new - old;
        self.negative_lookup_evict_to_budget();
    }

    /// Union a sealed sidecar's bloom words. `false` = refused (aux cap or
    /// malformed, or the entry was evicted mid-build): the build is then not
    /// exhaustive and must not complete.
    pub fn negative_lookup_union_bloom(&mut self, key: &AggregateKey, words: &[u64]) -> bool {
        let Some(entry) = self.negative_lookup.peek_mut(key) else { return false };
        let old = entry.byte_cost();
        let ok = entry.try_union_bloom_words(words);
        let new = entry.byte_cost();
        self.negative_lookup_bytes = self.negative_lookup_bytes + new - old;
        self.negative_lookup_evict_to_budget();
        ok
    }

    /// Finish a build. `complete=true` only when the scan provably reached the
    /// start of the aggregate's history. `generation` must be the token
    /// `try_begin_build` handed this builder: a mismatch means the resident
    /// entry belongs to a successor builder, and finishing (either flavor)
    /// would corrupt it — no-op instead. Returns whether the entry is now
    /// Complete (false when it was evicted mid-build — safe, rebuilt on the
    /// next miss).
    pub fn negative_lookup_finish_build(&mut self, key: &AggregateKey, generation: u64, complete: bool) -> bool {
        let Some(entry) = self.negative_lookup.peek_mut(key) else { return false };
        if entry.build_generation() != generation {
            metrics::counter!("celeriant_negative_lookup_stale_finish_total").increment(1);
            return false;
        }
        let old = entry.byte_cost();
        entry.finish_build(complete);
        let done = entry.is_complete();
        let new = entry.byte_cost();
        self.negative_lookup_bytes = self.negative_lookup_bytes + new - old;
        self.negative_lookup_evict_to_budget();
        done
    }

    /// Eager population from the open-time forward scan. Merges into an
    /// existing entry without changing its state; installs a new entry only
    /// while the byte budget has room (the scan feeds aggregates in arbitrary
    /// order, so past the budget it just stops installing). `complete` may only
    /// be true when the scanned segment provably holds the aggregate's ENTIRE
    /// history (the caller's check: no sealed segments exist at all).
    /// Returns true when a NEW entry was installed as Complete.
    pub fn negative_lookup_seed(&mut self, key: &AggregateKey, hashes: &HashSet<u64>, complete: bool) -> bool {
        if let Some(entry) = self.negative_lookup.peek_mut(key) {
            let old = entry.byte_cost();
            for h in hashes {
                entry.insert_hash(*h);
            }
            let new = entry.byte_cost();
            self.negative_lookup_bytes = self.negative_lookup_bytes + new - old;
            self.negative_lookup_evict_to_budget();
            return false;
        }
        let mut entry = NegativeClientBloom::new_building();
        for h in hashes {
            entry.insert_hash(*h);
        }
        if complete {
            entry.finish_build(true);
        }
        // Refuse rather than evict: the eager scan feeds aggregates in
        // arbitrary order, so displacing earlier installs would just churn.
        if self.negative_lookup_bytes + entry.byte_cost() > self.negative_lookup_cache_bytes {
            return false;
        }
        self.negative_lookup_bytes += entry.byte_cost();
        self.negative_lookup.put(key.clone(), entry);
        complete
    }

    fn negative_lookup_evict_to_budget(&mut self) {
        self.negative_lookup_evict_until(self.negative_lookup_cache_bytes);
    }

    /// Evict LRU entries until the cache holds at most `target_bytes`. Entries
    /// with an active builder are pinned — evicting one would break the
    /// one-builder latch (a fresh same-key entry admits a second builder) —
    /// so they rotate to MRU instead. The rotation budget bounds the loop:
    /// once only pinned entries remain the cache may stay over target (bounded
    /// by the number of concurrent build scans).
    fn negative_lookup_evict_until(&mut self, target_bytes: u64) {
        let mut rotations = self.negative_lookup.len();
        while self.negative_lookup_bytes > target_bytes {
            let Some((key, entry)) = self.negative_lookup.pop_lru() else { break };
            if entry.is_builder_active() {
                self.negative_lookup.put(key, entry);
                if rotations == 0 {
                    break;
                }
                rotations -= 1;
                continue;
            }
            self.negative_lookup_bytes -= entry.byte_cost();
            metrics::counter!("celeriant_negative_lookup_evictions_total").increment(1);
        }
    }

    pub fn negative_lookup_len(&self) -> usize {
        self.negative_lookup.len()
    }

    pub fn negative_lookup_bytes(&self) -> u64 {
        self.negative_lookup_bytes
    }

    pub fn update_segment_summary(&mut self, metablock: &Metablock, metablock_absolute_pos: u64) {
        self.negative_lookup_note_commit(metablock);
        fold_segment_summary(
            &mut self.segment_summary,
            &mut self.segment_summary_orgs,
            &mut self.segment_summary_types,
            &mut self.segment_summary_clients,
            &mut self.segment_summary_schemas,
            &mut self.segment_summary_loose,
            metablock,
            metablock_absolute_pos,
        );
    }

    pub fn take_segment_summary(&mut self) -> SegmentSummaryPayload {
        let orgs: Vec<u128> = self.segment_summary_orgs.drain().collect();
        let aggregate_types: Vec<AggregateTypeKey> = self.segment_summary_types.drain().collect();
        let mut clients = std::mem::take(&mut self.segment_summary_clients);
        let schema_bloom = std::mem::take(&mut self.segment_summary_schemas).to_schema_bloom(!self.segment_summary_incomplete);
        let complete = !self.segment_summary_incomplete;
        // Seal-time right-sizing: the persisted segment blooms are built from
        // the accumulator's exact key knowledge, not the fixed-size live words.
        let loose = std::mem::take(&mut self.segment_summary_loose);
        let (aggregate_bloom, client_bloom) = seal_segment_blooms(&self.segment_summary, &clients, &loose, complete);
        self.segment_summary_incomplete = false;
        self.segment_summary_draining = true;
        let aggregates: Vec<SegmentAggregateEntry> = self
            .segment_summary
            .drain()
            .map(|(key, mut entry)| {
                entry.client_set = clients.remove(&key).map_or(ClientSet::Unknown, |h| ClientSet::from_client_hashes(&h));
                entry
            })
            .collect();
        SegmentSummaryPayload { orgs, aggregate_types, aggregates, complete, aggregate_bloom, client_bloom, schema_bloom }
    }

    pub fn peek_segment_summary(&self) -> &HashMap<AggregateKey, SegmentAggregateEntry> {
        &self.segment_summary
    }

    pub fn peek_segment_summary_orgs(&self) -> &HashSet<u128> {
        &self.segment_summary_orgs
    }

    pub fn peek_segment_summary_types(&self) -> &HashSet<AggregateTypeKey> {
        &self.segment_summary_types
    }

    /// Store the current active segment summary for a sealed segment. The slot's
    /// exact key knowledge (still fed by late deferred commits) is what the
    /// sidecar sweep builds the right-sized segment blooms from.
    /// Called at rotation time on the leader to defer sidecar write until replication confirms.
    pub fn store_sealed_segment_summary(&mut self, log_id: u64) {
        let sealed = SealedSegmentSummary {
            aggregates: std::mem::take(&mut self.segment_summary),
            orgs: std::mem::take(&mut self.segment_summary_orgs),
            aggregate_types: std::mem::take(&mut self.segment_summary_types),
            clients: std::mem::take(&mut self.segment_summary_clients),
            schemas: std::mem::take(&mut self.segment_summary_schemas),
            loose: std::mem::take(&mut self.segment_summary_loose),
            complete: !self.segment_summary_incomplete,
        };
        // Rotation resets the taint: the new segment's fold sees every commit from birth.
        self.segment_summary_incomplete = false;
        self.segment_summary_draining = true;
        // Store even when empty: deferred commits for this segment may still be
        // in flight, and update_segment_summary_for_log must route them here,
        // not into the next segment's active accumulator. An empty payload is
        // dropped at sidecar-write time. A colliding slot (seal retried after
        // a failed rotation) MERGES — see SealedSegmentSummary::merge.
        match self.sealed_segment_summaries.entry(log_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => o.get_mut().merge(sealed),
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(sealed);
            }
        }
    }

    /// Update the segment summary for a specific log segment.
    /// Routes to the sealed snapshot if one exists for this log_id,
    /// otherwise updates the active segment accumulator — but only when the
    /// commit actually belongs to the active segment.
    pub fn update_segment_summary_for_log(&mut self, log_id: u64, active_log_id: u64, metablock: &Metablock, metablock_absolute_pos: u64) {
        if self.sealed_segment_summaries.contains_key(&log_id) {
            self.negative_lookup_note_commit(metablock);
            self.update_sealed_segment_summary(log_id, metablock, metablock_absolute_pos);
        } else if log_id == active_log_id {
            self.update_segment_summary(metablock, metablock_absolute_pos);
        } else {
            // Still a durable client-bearing commit even though its summary
            // slot is gone — the bloom insert must not be dropped with it.
            self.negative_lookup_note_commit(metablock);
            // Defensive only: the sidecar sweep gate reloads evicted segments, so
            // a sealed segment's late commit always finds its slot above. Folding
            // it here would plant cross-file positions in the active accumulator,
            // breaking the SeekTo same-file proof — drop it instead.
            tracing::debug!(log_id, active_log_id, "dropping segment-summary update for a sealed segment with no staged slot");
        }
    }

    /// Discard the active accumulator and mark it incomplete. For the truncate
    /// path that unwinds onto a re-activated sealed segment: the accumulator's
    /// contents describe the discarded segment, and the re-activated file
    /// already holds commits the accumulator never saw.
    pub fn reset_segment_summary_after_unwind(&mut self) {
        self.segment_summary.clear();
        self.segment_summary_orgs.clear();
        self.segment_summary_types.clear();
        self.segment_summary_clients.clear();
        self.segment_summary_schemas = SchemaHashAccumulator::default();
        self.segment_summary_loose = LooseChainHashes::default();
        self.segment_summary_incomplete = true;
        // Re-activation replaces rotation as the window's end; the taint set
        // above already forces maybe-present, so the latch can drop.
        self.segment_summary_draining = false;
    }

    /// Taint the active accumulator: it provably missed commits (e.g. the
    /// pre-warm replay stopped before covering the whole active segment).
    pub fn mark_segment_summary_incomplete(&mut self) {
        self.segment_summary_incomplete = true;
    }

    /// Active-segment consult for the schema-absence proof: `false` = the
    /// segment definitely holds no registration for this hash. Only a
    /// taint-free, non-overflowed accumulator may answer absence.
    pub fn active_segment_may_contain_schema(&self, hash: u64) -> bool {
        self.segment_summary_draining
            || self.segment_summary_incomplete
            || self.segment_summary_schemas.may_contain(hash)
    }

    /// Rotation completed: `active_log_id` now names the new segment the
    /// (drained, empty) accumulator legitimately describes, and absence
    /// answers are sound again. Called by the seal path AFTER
    /// `rotate_to_next_log` returns.
    pub fn note_active_segment_rotated(&mut self) {
        self.segment_summary_draining = false;
    }

    /// Open-time rebuild feed: the full forward scan of the active segment
    /// inserts every schema-registration hash it sees (idempotent superset of
    /// the pre-warm replay's commit-fed inserts).
    pub fn segment_summary_insert_schema_hash(&mut self, hash: u64) {
        self.segment_summary_schemas.insert(hash);
    }

    /// Update a sealed segment's summary with a metablock that was replicated for that segment.
    /// Mirrors update_segment_summary but targets the stored sealed snapshot.
    pub fn update_sealed_segment_summary(&mut self, log_id: u64, metablock: &Metablock, metablock_absolute_pos: u64) {
        let Some(sealed) = self.sealed_segment_summaries.get_mut(&log_id) else { return };
        fold_segment_summary(
            &mut sealed.aggregates,
            &mut sealed.orgs,
            &mut sealed.aggregate_types,
            &mut sealed.clients,
            &mut sealed.schemas,
            &mut sealed.loose,
            metablock,
            metablock_absolute_pos,
        );
    }

    /// Snapshot the log_ids of sealed segments whose summary is staged in memcache and
    /// awaiting sidecar write. Caller decides eligibility against read/write cursors and
    /// then calls `take_sealed_segment_summary` for each accepted id.
    pub fn pending_sealed_summary_log_ids(&self) -> Vec<u64> {
        self.sealed_segment_summaries.keys().copied().collect()
    }

    /// Take the sealed segment summary, converting to SegmentSummaryPayload
    /// (seal-time right-sized blooms included) for sidecar write.
    pub fn take_sealed_segment_summary(&mut self, log_id: u64) -> Option<SegmentSummaryPayload> {
        let sealed = self.sealed_segment_summaries.remove(&log_id)?;
        let (aggregate_bloom, client_bloom) =
            seal_segment_blooms(&sealed.aggregates, &sealed.clients, &sealed.loose, sealed.complete);
        let mut clients = sealed.clients;
        let aggregates: Vec<SegmentAggregateEntry> = sealed
            .aggregates
            .into_iter()
            .map(|(key, mut entry)| {
                entry.client_set = clients.remove(&key).map_or(ClientSet::Unknown, |h| ClientSet::from_client_hashes(&h));
                entry
            })
            .collect();
        Some(SegmentSummaryPayload {
            orgs: sealed.orgs.into_iter().collect(),
            aggregate_types: sealed.aggregate_types.into_iter().collect(),
            aggregates,
            complete: sealed.complete,
            aggregate_bloom,
            client_bloom,
            schema_bloom: sealed.schemas.to_schema_bloom(sealed.complete),
        })
    }

    /// Check if a rollback has occurred since items were last added.
    /// Used to distinguish "empty queue due to rollback" from "empty queue due to race".
    pub fn take_fsync_rollback_flag(&mut self) -> bool {
        let state = self.fsync_rollback_occurred;
        self.fsync_rollback_occurred = false;
        state
    }

    /// Check if a rollback has occurred since items were last added.
    /// Used to distinguish "empty queue due to rollback" from "empty queue due to race".
    pub fn take_replication_rollback_flag(&mut self) -> bool {
        let state = self.replication_rollback_occurred;
        self.replication_rollback_occurred = false;
        state
    }
}

pub struct EventIndexes {
    pub pending_delete_or_deleted: bool,
    pub allow_recreate: bool,
    pub allow_sequence_continuation: bool,
    pub aggregate_version: u64,
    pub min_aggregate_version: u64,
    pub event_seq: u64,
}

/// Merge a colliding slot's aggregate entry, where `later` was folded strictly
/// after `entry` (same segment, forward time): temporal fields take the later
/// era, monotone fields max, additive fields sum. A later-era delete stands
/// wholesale — the fold zeroes counts at delete, and nothing newer exists.
fn merge_entry_later_era(entry: &mut SegmentAggregateEntry, later: SegmentAggregateEntry) {
    if later.is_deleted {
        *entry = later;
        return;
    }
    entry.is_deleted = false;
    entry.event_batch_count += later.event_batch_count;
    entry.last_aggregate_version = entry.last_aggregate_version.max(later.last_aggregate_version);
    entry.min_aggregate_version = entry.min_aggregate_version.max(later.min_aggregate_version);
    entry.last_server_timestamp = entry.last_server_timestamp.max(later.last_server_timestamp);
    entry.compressed_size += later.compressed_size;
    entry.uncompressed_size += later.uncompressed_size;
    if later.newest_metablock_pos != 0 {
        entry.newest_metablock_pos = later.newest_metablock_pos;
    }
}

/// Seal-time right-sized segment blooms from an accumulator's exact key
/// knowledge (all three blooms share one sizing formula). Sound
/// only for a COMPLETE accumulator — an incomplete one is a subset and any
/// bloom built from it could answer a false "absent", so both persist as None
/// (maybe-present everywhere). The per-aggregate client sets are exact in
/// memory (no cap), so the client union is exact; trim-only keys/clients ride
/// in via `loose`.
fn seal_segment_blooms(
    aggregates: &HashMap<AggregateKey, SegmentAggregateEntry>,
    clients: &HashMap<AggregateKey, HashSet<u64>>,
    loose: &LooseChainHashes,
    complete: bool,
) -> (Option<Vec<u64>>, Option<Vec<u64>>) {
    if !complete {
        return (None, None);
    }
    let aggregate_bloom = sized_bloom_from_hashes(
        aggregates.len() + loose.keys.len(),
        aggregates.keys().map(AggregateKey::bloom_hash).chain(loose.keys.iter().copied()),
        AGGREGATE_BLOOM_BYTES,
    );
    let client_union: HashSet<u64> =
        clients.values().flatten().chain(loose.clients.iter()).copied().collect();
    let client_bloom = sized_bloom_from_hashes(
        client_union.len(),
        client_union.iter().copied(),
        CLIENT_BLOOM_BYTES,
    );
    (Some(aggregate_bloom), Some(client_bloom))
}

/// One metablock's contribution to a segment summary accumulator (active or sealed
/// slot). Every aggregate-scoped client-bearing kind (EventBatch, SoftDelete,
/// SoftTrim) feeds the per-aggregate client-hash set — a tombstone-only client
/// missing from it would be a subset, answering a false "absent". Positions are
/// last-wins: the fold runs in write order, so the last position is the newest.
fn fold_segment_summary(
    aggregates: &mut HashMap<AggregateKey, SegmentAggregateEntry>,
    orgs: &mut HashSet<u128>,
    aggregate_types: &mut HashSet<AggregateTypeKey>,
    clients: &mut HashMap<AggregateKey, HashSet<u64>>,
    schemas: &mut SchemaHashAccumulator,
    loose: &mut LooseChainHashes,
    metablock: &Metablock,
    metablock_absolute_pos: u64,
) {
    match &metablock.wal_metablock_type {
        MetablockKind::EventBatchMetadata(eb) => {
            let key = &eb.aggregate_key;
            orgs.insert(key.org_id);
            aggregate_types.insert(AggregateTypeKey::new(key.org_id, key.aggregate_type_id));
            let entry = aggregates.entry(key.clone()).or_insert_with(|| {
                let mut e = SegmentAggregateEntry::new(key.org_id, key.aggregate_type_id, key.aggregate_id);
                e.min_aggregate_version = eb.trimmed_below_version;
                e
            });
            entry.is_deleted = false;
            entry.event_batch_count += 1;
            if eb.aggregate_version > entry.last_aggregate_version {
                entry.last_aggregate_version = eb.aggregate_version;
            }
            if metablock.server_timestamp > entry.last_server_timestamp {
                entry.last_server_timestamp = metablock.server_timestamp;
            }
            entry.compressed_size += metablock.compressed_size;
            entry.uncompressed_size += metablock.uncompressed_size;
            entry.newest_metablock_pos = metablock_absolute_pos;
            clients.entry(key.clone()).or_default().insert(client_id_bloom_hash(eb.client_id));
        }
        MetablockKind::SoftDelete(sd) => {
            let key = &sd.aggregate_key;
            orgs.insert(key.org_id);
            aggregate_types.insert(AggregateTypeKey::new(key.org_id, key.aggregate_type_id));
            let entry = aggregates.entry(key.clone())
                .or_insert_with(|| SegmentAggregateEntry::new(key.org_id, key.aggregate_type_id, key.aggregate_id));
            entry.is_deleted = true;
            entry.event_batch_count = 0;
            entry.compressed_size = 0;
            entry.uncompressed_size = 0;
            entry.newest_metablock_pos = metablock_absolute_pos;
            clients.entry(key.clone()).or_default().insert(client_id_bloom_hash(sd.client_id));
        }
        MetablockKind::SoftTrim(st) => {
            // No entry means the aggregate has nothing else in this segment; the
            // summary then reports it absent and consumers skip the segment, which
            // matches today's scan (client seqs are read from EventBatch blocks only).
            let key = &st.aggregate_key;
            if let Some(entry) = aggregates.get_mut(key) {
                if st.keep_from_aggregate_version > entry.min_aggregate_version {
                    entry.min_aggregate_version = st.keep_from_aggregate_version;
                }
                entry.newest_metablock_pos = metablock_absolute_pos;
                clients.entry(key.clone()).or_default().insert(client_id_bloom_hash(st.client_id));
            } else {
                // No summary entry (see above) — but the segment blooms must
                // stay supersets of every chain key and client in the file:
                // the aggregate-load scan reads this trim's floor, and a bloom
                // skip here would hand out a stale one.
                loose.keys.insert(key.bloom_hash());
                loose.clients.insert(client_id_bloom_hash(st.client_id));
            }
        }
        // No aggregate entry (registrations carry none); the segment's schema
        // set is what the absence proof consults.
        MetablockKind::SchemaRegistration(sr) => {
            schemas.insert(sr.schema_key.bloom_hash());
        }
    }
}

/// Inserts into the cache. If `low_priority` is true, only inserts when there's
/// spare capacity and immediately demotes the entry to LRU position.
/// Will not change the position if the low priority key already is in the lru
fn put_with_priority<K, V, S>(cache: &mut LruCache<K, V, S>, key: K, value: V, low_priority: bool)
where
    K: Hash + Eq + Clone,
    S: std::hash::BuildHasher,
{
    if low_priority {
        if cache.contains(&key) {
            return;
        }
        if cache.len() < cache.cap().get() {
            cache.put(key.clone(), value);
            cache.demote(&key);
        }
    } else {
        cache.put(key, value);
    }
}
