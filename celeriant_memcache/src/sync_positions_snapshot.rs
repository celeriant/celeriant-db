use std::collections::HashMap;

use celeriant_wal::{aggregate_key::AggregateKey, constants::FIXED_BLOCK_SIZE_BYTES};

use crate::{queue_aggregate_positions::QueueAggregatePositions, shard_log_queue_item::ShardLogQueueItem};

pub struct SyncPositionsSnapshot {
    pub pending_append_queue: Vec<ShardLogQueueItem>,
    pub aggregate_queue_positions: HashMap<AggregateKey, QueueAggregatePositions>,
    pub metablocks_position: u64,
    pub datablocks_position: u64,
    pub wal_index: u64,
    pub file_len: u64,
    pub datablocks_carry_over: Option<Vec<u8>>,
}

impl SyncPositionsSnapshot {
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
        let required_space = self.buffer_size_datablocks().saturating_add(self.buffer_size_metablocks());
        free_space.saturating_sub(required_space) > 0
    }
}