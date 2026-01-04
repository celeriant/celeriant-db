use crate::{
    aggregate_recent_write::AggregateRecentWrites, mem_snapshot_aggregate::MemSnapshotAggregate,
    queue_aggregate_positions::QueueAggregatePositions, recent_write::RecentWrite, shard_log_queue_item::ShardLogQueueItem,
    sync_positions_snapshot::SyncPositionsSnapshot,
};
use celeriant_wal::{
    aggregate_client_key::AggregateClientKey, aggregate_key::AggregateKey, constants::FIXED_BLOCK_SIZE_BYTES, datablocks::datablock::Datablock,
    metablock_position::MetablockPosition, metablocks::metablock::Metablock,
};
use lru::LruCache;
use std::hash::Hash;
use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
};

pub struct ShardMemCache {
    recent_write_cache_bytes: u64, 

    /// Cache of recent writes indexed by aggregate key.
    /// Only populated after successful durable write.
    aggregate_recent_writes: HashMap<AggregateKey, AggregateRecentWrites>,

    /// Current size of the recent write cache in bytes
    cache_current_bytes: u64,

    /// Eviction queue: (aggregate_key, batch_index, size_bytes) in insertion order
    cache_eviction_queue: VecDeque<(AggregateKey, u64, u64)>,

    /// LRU cache of aggregate positions committed to file (batch and event indexes)
    aggregate_snapshots: LruCache<AggregateKey, MemSnapshotAggregate>,

    /// LRU cache of client event indexes committed to file
    /// Missing here does not mean client hasn't written to aggregate, just not in cache
    aggregate_client_snapshots: LruCache<AggregateClientKey, u64>,

    /// Indexes representing the in-memory positions of the next write for each aggregate
    /// These are writes yet to be written to disk
    /// This is unbounded as we expect quick flush to disk
    aggregate_queue_positions: HashMap<AggregateKey, QueueAggregatePositions>,

    /// Writes from clients that are pending write to disk
    /// This is unbounded as we expect quick flush to disk
    pending_append_queue: Vec<ShardLogQueueItem>,
}

impl ShardMemCache {

    /// Returns (is_loaded, last_client_event_index)
    /// - is_loaded: true if we've already checked disk for this aggregate+client
    /// - last_client_event_index: Some(idx) if client has written, None if not found
    pub fn aggregate_client_load_status(&mut self, aggregate_key: &AggregateKey, aggregate_client_key: &AggregateClientKey) -> (bool, Option<u64>) {
        // Check queue first
        if let Some(queue_pos) = self.aggregate_queue_positions.get(aggregate_key) {
            if let Some(&idx) = queue_pos.client_event_indexes.get(&aggregate_client_key.client_id) {
                return (true, Some(idx));
            }
        }

        // Check LRU cache
        if let Some(&client_event_index) = self.aggregate_client_snapshots.get(&aggregate_client_key) {
            // 0 is sentinel for "checked but client never wrote"
            let result = if client_event_index == 0 { None } else { Some(client_event_index) };
            return (true, result);
        }

        (false, None)
    }

    /// Returns (is_loaded, exists)
    /// - is_loaded: true if we've already checked disk for this aggregate  
    /// - exists: true if the aggregate has actual data (on disk or in queue)
    pub fn aggregate_load_status(&mut self, aggregate_key: &AggregateKey) -> (bool, bool) {
        // Check if in queue (being created/modified)
        if self.aggregate_queue_positions.contains_key(aggregate_key) {
            return (true, true);
        }

        // Check if in snapshots cache
        if let Some(snapshot) = self.aggregate_snapshots.get(aggregate_key) {
            // event_batch_index > 0 means real data exists on disk
            let exists = snapshot.event_batch_index > 0;
            return (true, exists);
        }

        (false, false)
    }

    /// Insert a write into the recent write cache. Call only after durable write.
    pub fn cache_recent_write(
        &mut self,
        aggregate_key: AggregateKey,
        batch_index: u64,
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
            .or_insert_with(|| AggregateRecentWrites::new(batch_index));

        aggregate_writes.push(RecentWrite {
            metablock,
            datablock,
            size_bytes,
        });

        self.cache_current_bytes = self.cache_current_bytes.saturating_add(size_bytes);
        self.cache_eviction_queue.push_back((aggregate_key, batch_index, size_bytes));
    }

    fn evict_oldest_cache_entry(&mut self) -> bool {
        let Some((aggregate_key, _batch_index, size_bytes)) = self.cache_eviction_queue.pop_front() else {
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

    /// Get cached writes for an aggregate from a starting batch index.
    /// Returns writes in batch order as (batch_index, &RecentWrite).
    /// Returns None if aggregate not in cache.
    pub fn get_cached_writes_from(&self, aggregate_key: &AggregateKey, from_batch_index: u64) -> impl Iterator<Item = (u64, &RecentWrite)> {
        self.aggregate_recent_writes
            .get(aggregate_key)
            .into_iter()
            .flat_map(move |aggregate_writes| aggregate_writes.iter_from(from_batch_index))
    }

    pub fn pending_append_queue_is_empty(&self) -> bool {
        self.pending_append_queue.is_empty()
    }

    /// Even though we haven't written to disk yet,
    /// we need to track the aggregate index positions
    /// and the client position for idempotency checks.
    /// We do this and add the new queue item entry for later write
    pub fn add_to_pending_append_queue(
        &mut self,
        aggregate_key: &AggregateKey,
        event_index: u64,
        event_batch_index: u64,
        client_id: u128,
        client_event_index: u64,
        shard_log_queue_item: ShardLogQueueItem,
    ) {
        let aggregate = self
            .aggregate_queue_positions
            .entry(aggregate_key.clone())
            .or_insert_with(|| QueueAggregatePositions::default());

        if event_batch_index > aggregate.event_batch_index {
            aggregate.event_batch_index = event_batch_index;
        }
        if event_index > aggregate.event_index {
            aggregate.event_index = event_index;
        }

        aggregate
            .client_event_indexes
            .entry(client_id)
            .and_modify(|existing| {
                if client_event_index > *existing {
                    *existing = client_event_index;
                }
            })
            .or_insert(client_event_index);

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

        // We need the aggregate_queue_positions to
        SyncPositionsSnapshot {
            aggregate_queue_positions,
            pending_append_queue,
        }
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
    pub fn rollback_queue_positions(&mut self) {
        self.aggregate_queue_positions.clear();
    }

    pub fn is_aggregate_snapshot_full_or_contains(&self, aggregate_key: &AggregateKey) -> bool {
        if self.aggregate_snapshots.len() == self.aggregate_snapshots.cap().get() {
            return true;
        }
        self.aggregate_snapshots.contains(&aggregate_key)
    }

    pub fn is_aggregate_client_cache_full_or_contains(&self, aggregate_client_key: &AggregateClientKey) -> bool {
        if self.aggregate_client_snapshots.len() == self.aggregate_client_snapshots.cap().get() {
            return true;
        }
        self.aggregate_client_snapshots.contains(&aggregate_client_key)
    }

    pub fn put_aggregate_into_cache_as_not_found(&mut self, aggregate_key: AggregateKey) {
        let snapshot = MemSnapshotAggregate::not_found();
        put_with_priority(&mut self.aggregate_snapshots, aggregate_key, snapshot, false);
    }

    pub fn put_aggregate_client_into_cache(&mut self, aggregate_client_key: AggregateClientKey, last_client_event_index: u64, low_priority: bool) {
        put_with_priority(
            &mut self.aggregate_client_snapshots,
            aggregate_client_key,
            last_client_event_index,
            low_priority,
        );
    }

    pub fn put_aggregate_into_cache(
        &mut self,
        aggregate_key: AggregateKey,
        snapshot: MemSnapshotAggregate,
        client_id: u128,
        last_client_event_index: u64,
        low_priority: bool,
    ) {
        let client_key = AggregateClientKey::new(aggregate_key.clone(), client_id);
        put_with_priority(&mut self.aggregate_client_snapshots, client_key, last_client_event_index, low_priority);
        put_with_priority(&mut self.aggregate_snapshots, aggregate_key, snapshot, low_priority);
    }

    /// Provide the aggregate_queue_positions snapshotted before disk write begun
    /// and this will update the aggregate_file_positions with the committed data
    pub fn commit_sync_positions_snapshot(&mut self, sync_positions_snapshot: SyncPositionsSnapshot) {

        for (key, queue_positions) in sync_positions_snapshot.aggregate_queue_positions {
            // Update aggregate positions LRU
            if let Some(existing) = self.aggregate_snapshots.get_mut(&key) {
                if queue_positions.event_batch_index > existing.event_batch_index {
                    existing.event_batch_index = queue_positions.event_batch_index;
                }
                if queue_positions.event_index > existing.event_index {
                    existing.event_index = queue_positions.event_index;
                }
            } else {
                self.aggregate_snapshots.put(
                    key.clone(),
                    MemSnapshotAggregate {
                        log_id: queue_positions.log_id,
                        metablock_absolute_pos: queue_positions.metablock_absolute_pos,
                        event_index: queue_positions.event_index,
                        event_batch_index: queue_positions.event_batch_index,
                    },
                );
            }

            // Update client event indexes LRU
            for (client_id, client_event_index) in queue_positions.client_event_indexes {
                let client_key = AggregateClientKey::new(key.clone(), client_id);
                if let Some(existing) = self.aggregate_client_snapshots.get_mut(&client_key) {
                    if client_event_index > *existing {
                        *existing = client_event_index;
                    }
                } else {
                    self.aggregate_client_snapshots.put(client_key, client_event_index);
                }
            }

            // Clean up queue entry only if it hasn't been updated by a newer write.
            // If a new write came in during sync, the queue will have higher indexes.
            if let Some(current_queue_pos) = self.aggregate_queue_positions.get(&key) {
                if current_queue_pos.event_batch_index == queue_positions.event_batch_index {
                    self.aggregate_queue_positions.remove(&key);
                }
            }
        }

        // Periodically reclaim memory from the queue HashMap
        if self.aggregate_queue_positions.capacity() > self.aggregate_queue_positions.len().saturating_mul(2) {
            self.aggregate_queue_positions.shrink_to_fit();
        }
    }

    /// Get the latest event index for a client within an aggregate
    /// Preference the queue first, then fallback to file if no queued items for client
    pub fn get_client_event_index(&mut self, aggregate_key: &AggregateKey, client_id: u128) -> Option<u64> {
        // Check queue first
        if let Some(queue_pos) = self.aggregate_queue_positions.get(aggregate_key) {
            if let Some(&idx) = queue_pos.client_event_indexes.get(&client_id) {
                return Some(idx);
            }
        }

        // Fall back to file LRU (peek to avoid promoting on read)
        let client_key = AggregateClientKey::new(aggregate_key.clone(), client_id);
        self.aggregate_client_snapshots.get(&client_key).copied().filter(|&idx| idx > 0) // 0 is sentinel for "checked but not found"
    }

    /// The log file and position of the last known written metablock for an aggregate
    pub fn get_aggregate_last_metablock_pos(&mut self, aggregate_key: &AggregateKey) -> MetablockPosition {
        if let Some(file_pos) = self.aggregate_snapshots.get(aggregate_key) {
            return MetablockPosition {
                log_id: file_pos.log_id,
                metablock_absolute_pos: file_pos.metablock_absolute_pos,
                event_batch_index: file_pos.event_batch_index,
                event_index: file_pos.event_index,
            };
        }

        MetablockPosition {
            log_id: 0,
            metablock_absolute_pos: 0,
            event_batch_index: 0,
            event_index: 0,
        }
    }

    /// Get the latest batch and event index for an aggregate
    /// Preference the queue first, then fallback to file if no queued items for aggregate
    pub fn get_event_indexes(&mut self, aggregate_key: &AggregateKey) -> EventIndexes {
        // Check queue first
        if let Some(queue_pos) = self.aggregate_queue_positions.get(aggregate_key) {
            return EventIndexes {
                event_batch_index: queue_pos.event_batch_index,
                event_index: queue_pos.event_index,
            };
        }

        // Fall back to file LRU
        if let Some(file_pos) = self.aggregate_snapshots.get(aggregate_key) {
            return EventIndexes {
                event_batch_index: file_pos.event_batch_index,
                event_index: file_pos.event_index,
            };
        }

        EventIndexes {
            event_batch_index: 0,
            event_index: 0,
        }
    }

    pub fn new(
        recent_write_cache_bytes: u64, aggregate_snapshots_cache_bytes: u64, aggregate_client_snapshots_cache_bytes: u64,
    ) -> Self {
        let aggregate_cap = NonZeroUsize::new((aggregate_snapshots_cache_bytes / 112) as usize).unwrap_or(NonZeroUsize::new(10_000).unwrap());
        let client_cap =
            NonZeroUsize::new((aggregate_client_snapshots_cache_bytes / 128) as usize).unwrap_or(NonZeroUsize::new(100_000).unwrap());

        Self {
            recent_write_cache_bytes,
            aggregate_recent_writes: HashMap::new(),
            cache_current_bytes: 0,
            cache_eviction_queue: VecDeque::new(),
            aggregate_queue_positions: HashMap::new(),
            pending_append_queue: vec![],
            aggregate_snapshots: LruCache::new(aggregate_cap),
            aggregate_client_snapshots: LruCache::new(client_cap),
        }
    }
}

pub struct EventIndexes {
    pub event_batch_index: u64,
    pub event_index: u64,
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
