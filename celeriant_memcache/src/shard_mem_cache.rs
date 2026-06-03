use crate::cache_path::CachePath;
use crate::cached_schema::{CachedSchema, Validate};
use crate::mem_snapshot_aggregate::AggregateStatus;
use crate::metablock_position::MetablockPosition;
use crate::pending_commit_data::PendingCommitData;
use crate::{
    aggregate_recent_write::AggregateRecentWrites, mem_snapshot_aggregate::MemSnapshotAggregate, queue_aggregate_positions::QueueAggregatePositions,
    recent_write::RecentWrite, shard_log_queue_item::ShardLogQueueItem, sync_positions_snapshot::{SyncPositionsSnapshot},
};
use celeriant_distributed::node_status::NodeStatus;
use celeriant_wal::segment_summary::{SegmentAggregateEntry, SegmentSummaryPayload};
use celeriant_wal::metablocks::metablock_event_batch::MetablockEventBatch;
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_wal::schema_key::SchemaKey;
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
    aggregate_recent_writes: HashMap<AggregateKey, AggregateRecentWrites>,

    /// Current size of the recent write cache in bytes
    cache_current_bytes: u64,

    /// Eviction queue: (aggregate_key, aggregate_version, size_bytes) in insertion order
    cache_eviction_queue: VecDeque<(AggregateKey, u64, u64)>,

    /// LRU cache of aggregate positions committed to file (batch and event sequences)
    /// Updated after fsync - used by write path (OCC, idempotency)
    aggregate_write_snapshots: LruCache<AggregateKey, MemSnapshotAggregate>,

    /// LRU cache of aggregate positions visible to readers
    /// Updated after replication - used by read path
    aggregate_read_snapshots: LruCache<AggregateKey, MemSnapshotAggregate>,

    /// LRU cache of client event sequences committed to file
    /// Missing here does not mean client hasn't written to aggregate, just not in cache
    /// Stores (client_seq, wal_seq). wal_seq=0 means loaded from disk (always durable).
    aggregate_write_client_snapshots: LruCache<AggregateClientKey, (u64, u64)>,

    /// Indexes representing the in-memory positions of the next write for each aggregate
    /// These are writes yet to be written to disk
    /// This is unbounded as we expect quick flush to disk
    aggregate_queue_positions: HashMap<AggregateKey, QueueAggregatePositions>,

    /// Writes from clients that are pending write to disk
    /// This is unbounded as we expect quick flush to disk
    pending_append_queue: Vec<ShardLogQueueItem>,

    /// LRU cache of compiled schemas (Validated/CompilationFailed only).
    schema_cache: LruCache<SchemaKey, CachedSchema<V>>,

    /// LRU cache of keys confirmed to have no schema in WAL.
    /// Separated from schema_cache so large validators can't evict these tiny entries.
    no_schema_cache: LruCache<SchemaKey, ()>,

    /// Pending schema registrations not yet fsynced (D4)
    /// Checked alongside schema_cache to prevent concurrent duplicate registrations.
    /// Cleared on fsync rollback.
    pending_schema_registrations: HashSet<SchemaKey>,

    /// Batches awaiting replication (post-fsync, pre-commit). Bounded indirectly
    /// by the inflight cap: writes are rejected at entry when
    /// `pending_append_bytes + pending_replication_bytes >= internode_max_request_size`,
    /// so the snapshot captured from this queue always fits in one TCP request.
    pending_replication_batches: Vec<PendingCommitData>,

    /// Total bytes in pending replication queue
    pending_replication_bytes: u64,

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

    /// Sealed segment summaries waiting for replication to confirm before sidecar write.
    /// Leader-only: stores summaries drained at rotation time, keyed by sealed log_id.
    sealed_segment_summaries: HashMap<u64, SealedSegmentSummary>,
}

/// In-memory accumulator for a sealed segment's summary, mirroring the active segment fields.
/// Converted to SegmentSummaryPayload when the segment becomes fully replicated.
pub struct SealedSegmentSummary {
    aggregates: HashMap<AggregateKey, SegmentAggregateEntry>,
    orgs: HashSet<u128>,
    aggregate_types: HashSet<AggregateTypeKey>,
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
    /// Used during WAL truncation where read cache is also invalidated.
    pub fn clear_all_caches(&mut self) {
        self.execute_replication_rollback();
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

    /// Add a pending trim to the queue
    pub fn add_pending_trim_to_queue(
        &mut self,
        aggregate_key: &AggregateKey,
        keep_from_aggregate_version: u64,
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

        self.pending_append_bytes = self.pending_append_bytes.saturating_add(shard_log_queue_item.size_bytes());
        self.pending_append_queue.push(shard_log_queue_item);
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
        let mut pending_schema_registrations = HashSet::new();
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
            existing.status = AggregateStatus::Found;
            if event_batch.aggregate_version > existing.aggregate_version {
                existing.aggregate_version = event_batch.aggregate_version;
            }
            if event_batch.max_event_seq > existing.event_seq {
                existing.event_seq = event_batch.max_event_seq;
            }
            existing.log_id = log_id;
            existing.metablock_absolute_pos = metablock_absolute_pos;
        } else {
            cache.put(event_batch.aggregate_key.clone(), MemSnapshotAggregate {
                log_id,
                metablock_absolute_pos,
                event_seq: event_batch.max_event_seq,
                aggregate_version: event_batch.aggregate_version,
                min_aggregate_version: 0,
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

            // Only update disk position when this batch had an EventBatch write.
            // Trim-only batches have default log_id=0 which would corrupt the position.
            let has_event_batch = queue_positions.aggregate_version > 0;

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
                if has_event_batch {
                    existing.log_id = queue_positions.log_id;
                    existing.metablock_absolute_pos = queue_positions.metablock_absolute_pos;
                }
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
                    if has_event_batch {
                        existing.log_id = queue_positions.log_id;
                        existing.metablock_absolute_pos = queue_positions.metablock_absolute_pos;
                    }
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

            // Tag each (client_seq, wal_seq) so the OCC check can distinguish inflight
            // (wal_seq > read cursor) from durable.
            let batch_wal_seq = queue_positions.wal_seq;
            for (client_id, client_seq) in queue_positions.client_seqes {
                let client_key = AggregateClientKey::new(key.clone(), client_id);
                if let Some(existing) = self.aggregate_write_client_snapshots.get_mut(&client_key) {
                    if client_seq > existing.0 {
                        *existing = (client_seq, batch_wal_seq);
                    }
                } else {
                    self.aggregate_write_client_snapshots.put(client_key, (client_seq, batch_wal_seq));
                }
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

    /// Insert a Validated/CompilationFailed schema. Removes from no_schema_cache
    /// since a schema was just registered for a previously-empty key.
    pub fn schema_cache_insert(&mut self, key: SchemaKey, value: CachedSchema<V>) {
        self.no_schema_cache.pop(&key);
        self.schema_cache.put(key, value);
    }

    /// Insert a key into the no-schema cache.
    pub fn no_schema_cache_insert(&mut self, key: SchemaKey) {
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

    pub fn aggregate_read_snapshots_len(&self) -> usize {
        self.aggregate_read_snapshots.len()
    }

    pub fn aggregate_recent_writes_len(&self) -> usize {
        self.aggregate_recent_writes.len()
    }

    /// Cull-side clear. Drains pending_replication and clears the OCC/idempotency
    /// LRUs that point at the discarded speculative tail. Leaves aggregate_queue_positions,
    /// schema caches, and rollback_generation alone (those belong to a real rollback).
    pub fn clear_speculative_write_caches_for_cull(&mut self) -> usize {
        let drained = std::mem::take(&mut self.pending_replication_batches).len();
        self.pending_replication_bytes = 0;
        self.aggregate_write_snapshots.clear();
        self.aggregate_write_client_snapshots.clear();
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
            aggregate_recent_writes: HashMap::new(),
            cache_current_bytes: 0,
            cache_eviction_queue: VecDeque::new(),
            aggregate_queue_positions: HashMap::new(),
            pending_append_queue: vec![],
            aggregate_write_snapshots: LruCache::new(aggregate_cap),
            aggregate_read_snapshots: LruCache::new(aggregate_cap),
            aggregate_write_client_snapshots: LruCache::new(client_cap),
            schema_cache: LruCache::new(schema_cap),
            no_schema_cache: LruCache::new(no_schema_cap),
            pending_schema_registrations: HashSet::new(),
            pending_replication_batches: Vec::new(),
            pending_replication_bytes: 0,
            pending_append_bytes: 0,
            internode_max_request_size,
            fsync_rollback_occurred: false,
            replication_rollback_occurred: false,
            rollback_generation: 0,
            segment_summary: HashMap::new(),
            segment_summary_orgs: HashSet::new(),
            segment_summary_types: HashSet::new(),
            sealed_segment_summaries: HashMap::new(),
        }
    }

    pub fn update_segment_summary(&mut self, metablock: &Metablock) {
        match &metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(eb) => {
                let key = &eb.aggregate_key;
                self.segment_summary_orgs.insert(key.org_id);
                self.segment_summary_types.insert(AggregateTypeKey::new(key.org_id, key.aggregate_type_id));
                let entry = self.segment_summary.entry(key.clone()).or_insert_with(|| {
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
            }
            MetablockKind::SoftDelete(sd) => {
                let key = &sd.aggregate_key;
                self.segment_summary_orgs.insert(key.org_id);
                self.segment_summary_types.insert(AggregateTypeKey::new(key.org_id, key.aggregate_type_id));
                let entry = self.segment_summary.entry(key.clone())
                    .or_insert_with(|| SegmentAggregateEntry::new(key.org_id, key.aggregate_type_id, key.aggregate_id));
                entry.is_deleted = true;
                entry.event_batch_count = 0;
                entry.compressed_size = 0;
                entry.uncompressed_size = 0;
            }
            MetablockKind::SoftTrim(st) => {
                let key = &st.aggregate_key;
                if let Some(entry) = self.segment_summary.get_mut(key) {
                    if st.keep_from_aggregate_version > entry.min_aggregate_version {
                        entry.min_aggregate_version = st.keep_from_aggregate_version;
                    }
                }
            }
            MetablockKind::SchemaRegistration(_) => {}
        }
    }

    pub fn take_segment_summary(&mut self) -> SegmentSummaryPayload {
        let orgs: Vec<u128> = self.segment_summary_orgs.drain().collect();
        let aggregate_types: Vec<AggregateTypeKey> = self.segment_summary_types.drain().collect();
        let aggregates: Vec<SegmentAggregateEntry> = self.segment_summary.drain().map(|(_, v)| v).collect();
        SegmentSummaryPayload { orgs, aggregate_types, aggregates }
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

    /// Store the current active segment summary for a sealed segment.
    /// Called at rotation time on the leader to defer sidecar write until replication confirms.
    pub fn store_sealed_segment_summary(&mut self, log_id: u64) {
        let sealed = SealedSegmentSummary {
            aggregates: std::mem::take(&mut self.segment_summary),
            orgs: std::mem::take(&mut self.segment_summary_orgs),
            aggregate_types: std::mem::take(&mut self.segment_summary_types),
        };
        if !sealed.aggregates.is_empty() {
            self.sealed_segment_summaries.insert(log_id, sealed);
        }
    }

    /// Update the segment summary for a specific log segment.
    /// Routes to the sealed snapshot if one exists for this log_id,
    /// otherwise updates the active segment accumulator.
    pub fn update_segment_summary_for_log(&mut self, log_id: u64, metablock: &Metablock) {
        if self.sealed_segment_summaries.contains_key(&log_id) {
            self.update_sealed_segment_summary(log_id, metablock);
        } else {
            self.update_segment_summary(metablock);
        }
    }

    /// Update a sealed segment's summary with a metablock that was replicated for that segment.
    /// Mirrors update_segment_summary but targets the stored sealed snapshot.
    pub fn update_sealed_segment_summary(&mut self, log_id: u64, metablock: &Metablock) {
        let Some(sealed) = self.sealed_segment_summaries.get_mut(&log_id) else { return };
        match &metablock.wal_metablock_type {
            MetablockKind::EventBatchMetadata(eb) => {
                let key = &eb.aggregate_key;
                sealed.orgs.insert(key.org_id);
                sealed.aggregate_types.insert(AggregateTypeKey::new(key.org_id, key.aggregate_type_id));
                let entry = sealed.aggregates.entry(key.clone()).or_insert_with(|| {
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
            }
            MetablockKind::SoftDelete(sd) => {
                let key = &sd.aggregate_key;
                sealed.orgs.insert(key.org_id);
                sealed.aggregate_types.insert(AggregateTypeKey::new(key.org_id, key.aggregate_type_id));
                let entry = sealed.aggregates.entry(key.clone())
                    .or_insert_with(|| SegmentAggregateEntry::new(key.org_id, key.aggregate_type_id, key.aggregate_id));
                entry.is_deleted = true;
                entry.event_batch_count = 0;
                entry.compressed_size = 0;
                entry.uncompressed_size = 0;
            }
            MetablockKind::SoftTrim(st) => {
                let key = &st.aggregate_key;
                if let Some(entry) = sealed.aggregates.get_mut(key) {
                    if st.keep_from_aggregate_version > entry.min_aggregate_version {
                        entry.min_aggregate_version = st.keep_from_aggregate_version;
                    }
                }
            }
            MetablockKind::SchemaRegistration(_) => {}
        }
    }

    /// Snapshot the log_ids of sealed segments whose summary is staged in memcache and
    /// awaiting sidecar write. Caller decides eligibility against read/write cursors and
    /// then calls `take_sealed_segment_summary` for each accepted id.
    pub fn pending_sealed_summary_log_ids(&self) -> Vec<u64> {
        self.sealed_segment_summaries.keys().copied().collect()
    }

    /// Take the sealed segment summary, converting to SegmentSummaryPayload for sidecar write.
    pub fn take_sealed_segment_summary(&mut self, log_id: u64) -> Option<SegmentSummaryPayload> {
        let sealed = self.sealed_segment_summaries.remove(&log_id)?;
        Some(SegmentSummaryPayload {
            orgs: sealed.orgs.into_iter().collect(),
            aggregate_types: sealed.aggregate_types.into_iter().collect(),
            aggregates: sealed.aggregates.into_values().collect(),
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

/// Inserts into the cache. If `low_priority` is true, only inserts when there's
/// spare capacity and immediately demotes the entry to LRU position.
/// Will not change the position if the low priority key already is in the lru
fn put_with_priority<K, V>(cache: &mut LruCache<K, V>, key: K, value: V, low_priority: bool)
where
    K: Hash + Eq + Clone,
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
