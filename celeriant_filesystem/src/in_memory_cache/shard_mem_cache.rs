use crate::{in_memory_cache::{
    aggregate_positions::AggregatePositions, recent_write::RecentWrite, shard_log_queue_item::ShardLogQueueItem, sync_positions_snapshot::SyncPositionsSnapshot
}, shard_config::ShardConfig};
use celeriant_wal::{aggregate_key::AggregateKey, constants::FIXED_BLOCK_SIZE_BYTES, datablocks::wal_datablock::WalDatablock, metablocks::wal_metablock::WalMetablock};
use std::{collections::{BTreeMap, HashMap, VecDeque}, path::PathBuf, usize};

pub struct ShardMemCache {
    config: ShardConfig,
    
    /// The next write position for metablocks in the shard log
    metablocks_position: u64,

    /// The position of the last written datablock in the shard log
    datablocks_position: u64,

    /// The length of the shard log file, cached to avoid system calls
    file_len: u64,

    /// Partial bytes of the last written datablock to allow for efficient
    /// Direct I/O aligned writes. Saves reading the bytes for the next datablock write
    datablocks_carry_over: Option<Vec<u8>>,

    /// Cache of recent writes indexed by aggregate key and batch index.
    /// Only populated after successful durable write.
    aggregate_recent_writes: HashMap<AggregateKey, BTreeMap<u64, RecentWrite>>,
    
    /// Current size of the recent write cache in bytes
    cache_current_bytes: u64,
    
    /// Eviction queue: (aggregate_key, batch_index, size_bytes) in insertion order
    cache_eviction_queue: VecDeque<(AggregateKey, u64, u64)>,

    /// Current positions of indexes committed to file (batch, event, client event indexes)
    aggregate_file_positions: HashMap<AggregateKey, AggregatePositions>,

    /// Indexes representing the in-memory positions of the next write for each aggregate
    /// These are writes yet to be written to disk
    aggregate_queue_positions: HashMap<AggregateKey, AggregatePositions>,

    /// Writes from clients that are pending write to disk
    pending_append_queue: Vec<ShardLogQueueItem>,

    /// When writing in early ack mode (eg. timeseries data) client's don't know
    /// if fsync has failed until the next write. This flag is to force fsync on the next write
    /// so clients get notified and can take action
    had_fsync_failure: bool,

    /// The active log file id for this shard. We will increment when it gets full.
    current_log_id: u64,
    
}

impl ShardMemCache {
    /// Insert a write into the recent write cache. Call only after durable write.
    pub fn cache_recent_write(
        &mut self,
        aggregate_key: AggregateKey,
        batch_index: u64,
        metablock: WalMetablock,
        datablock: Option<WalDatablock>,
        size_bytes: u64,
    )
     {
        let max_bytes = self.config.recent_write_cache_bytes;
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
        let batches = self.aggregate_recent_writes
            .entry(aggregate_key.clone())
            .or_insert_with(BTreeMap::new);
        
        batches.insert(batch_index, RecentWrite {
            metablock,
            datablock,
            size_bytes,
        });
        
        self.cache_current_bytes += size_bytes;
        self.cache_eviction_queue.push_back((aggregate_key, batch_index, size_bytes));
    }

    fn evict_oldest_cache_entry(&mut self) -> bool {
        let Some((aggregate_key, batch_index, size_bytes)) = self.cache_eviction_queue.pop_front() else {
            return false;
        };

        if let Some(batches) = self.aggregate_recent_writes.get_mut(&aggregate_key) {
            if batches.remove(&batch_index).is_some() {
                self.cache_current_bytes = self.cache_current_bytes.saturating_sub(size_bytes);
            }
            if batches.is_empty() {
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
    /// Returns writes in batch order. Returns None if aggregate not in cache.
    pub fn get_cached_writes_from(
        &self,
        aggregate_key: &AggregateKey,
        from_batch_index: u64,
    ) -> Option<impl Iterator<Item = (&u64, &RecentWrite)>> {
        self.aggregate_recent_writes
            .get(aggregate_key)
            .map(|batches| batches.range(from_batch_index..))
    }
    
    pub fn shard_dir(&self) -> PathBuf {
        self.config.shard_dir.clone()
    }

    pub fn preallocate_bytes(&self) -> u64 {
        self.config.preallocate_bytes
    }

    pub fn current_log_id(&self) -> u64 {
        self.current_log_id
    }

    pub fn rotate_to_next_log(&mut self, current_log_id: u64, metablocks_position: u64, datablocks_position: u64, file_len: u64) {
        
        self.metablocks_position = metablocks_position;
        self.datablocks_position = datablocks_position;
        self.file_len = file_len;
        self.datablocks_carry_over = None;
        self.current_log_id = current_log_id;

    }

    pub fn requires_write(&self) -> bool {
        !self.pending_append_queue.is_empty()
    }

    pub fn force_durable_on_next_write(&self) -> bool {
        self.had_fsync_failure
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
            .or_insert_with(|| AggregatePositions::default());

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
        let mut aggregate_queue_positions = HashMap::new();
        std::mem::swap(
            &mut aggregate_queue_positions,
            &mut self.aggregate_queue_positions,
        );

        let mut pending_append_queue = vec![];
        std::mem::swap(&mut pending_append_queue, &mut self.pending_append_queue);

        SyncPositionsSnapshot {
            aggregate_queue_positions,
            pending_append_queue,
            metablocks_position: self.metablocks_position,
            datablocks_position: self.datablocks_position,
            datablocks_carry_over: self.datablocks_carry_over.clone(), //On rollback we want to keep this, not clear it
            file_len: self.file_len,
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

    pub fn has_enough_free_space(&self) -> bool {
        let free_space = self.datablocks_position.saturating_sub(self.metablocks_position);
        let required_space = self.buffer_size_datablocks().saturating_sub(self.buffer_size_metablocks());
        free_space.saturating_sub(required_space) > 0
    }

    /// If we have any failure to write to disk, set had_fsync_failure and clear
    /// out all our aggregate_queue_positions, falling back to the aggregate_file_positions store
    pub fn rollback_queue_positions(&mut self) {
        self.had_fsync_failure = true;
        self.aggregate_queue_positions.clear();
    }

    /// Provide the aggregate_queue_positions snapshotted before disk write begun
    /// and this will update the aggregate_file_positions with the committed data
    pub fn commit_sync_positions_snapshot(
        &mut self,
        sync_positions_snapshot: SyncPositionsSnapshot,
    ) {
        self.datablocks_position = sync_positions_snapshot.datablocks_position;
        self.metablocks_position = sync_positions_snapshot.metablocks_position;
        self.file_len = sync_positions_snapshot.file_len;
        self.datablocks_carry_over = sync_positions_snapshot.datablocks_carry_over;
        self.had_fsync_failure = false;

        for (key, positions) in sync_positions_snapshot.aggregate_queue_positions {
            match self.aggregate_file_positions.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let aggregate = entry.get_mut();
                    if positions.event_batch_index > aggregate.event_batch_index {
                        aggregate.event_batch_index = positions.event_batch_index;
                    }
                    if positions.event_index > aggregate.event_index {
                        aggregate.event_index = positions.event_index;
                    }
                    for (client_id, client_event_index) in positions.client_event_indexes {
                        aggregate
                            .client_event_indexes
                            .entry(client_id)
                            .and_modify(|existing| {
                                if client_event_index > *existing {
                                    *existing = client_event_index;
                                }
                            })
                            .or_insert(client_event_index);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(positions);
                }
            }
        }
    }

    /// Get the latest event index for a client within an aggregate
    /// Preference the queue first, then fallback to file if no queued items for client
    pub fn get_client_event_index(
        &self,
        aggregate_key: &AggregateKey,
        client_id: u128,
    ) -> Option<u64> {
        let mut client_event_index = self
            .aggregate_queue_positions
            .get(aggregate_key)
            .and_then(|aggregate| aggregate.client_event_indexes.get(&client_id).copied());

        if client_event_index.is_none() {
            client_event_index = self
                .aggregate_file_positions
                .get(aggregate_key)
                .and_then(|aggregate| aggregate.client_event_indexes.get(&client_id).copied());
        }

        client_event_index
    }

    /// Get the latest batch and event index for an aggregate
    /// Preference the queue first, then fallback to file if no queued items for aggregate
    pub fn get_event_indexes(&self, aggregate_key: &AggregateKey) -> EventIndexes {
        let mut event_indexes =
            self.aggregate_queue_positions
                .get(aggregate_key)
                .map(|aggregate| EventIndexes {
                    event_batch_index: aggregate.event_batch_index,
                    event_index: aggregate.event_index,
                });

        if event_indexes.is_none() {
            event_indexes = self
                .aggregate_file_positions
                .get(aggregate_key)
                .map(|aggregate| EventIndexes {
                    event_batch_index: aggregate.event_batch_index,
                    event_index: aggregate.event_index,
                });
        }

        event_indexes.unwrap_or(EventIndexes {
            event_batch_index: 0,
            event_index: 0,
        })
    }

    pub(crate) fn new(
        file_len: u64,
        metablocks_position: u64,
        datablocks_position: u64,
        config: ShardConfig,
        current_log_id: u64,
    ) -> Self {
        Self {
            metablocks_position,
            datablocks_position,
            file_len,
            datablocks_carry_over: None,
            aggregate_recent_writes: HashMap::new(),
            cache_current_bytes: 0,
            cache_eviction_queue: VecDeque::new(),
            aggregate_file_positions: HashMap::new(),
            aggregate_queue_positions: HashMap::new(),
            pending_append_queue: vec![],
            had_fsync_failure: false,
            config,
            current_log_id,
        }
    }
}

pub struct EventIndexes {
    pub event_batch_index: u64,
    pub event_index: u64,
}
