use std::collections::HashMap;

use celeriant_wal::{aggregate_key::AggregateKey, constants::FIXED_BLOCK_SIZE_BYTES};

use crate::{queue_aggregate_positions::QueueAggregatePositions, shard_log_queue_item::ShardLogQueueItem};

pub struct SyncPositionsSnapshot {
    pub pending_append_queue: Vec<ShardLogQueueItem>,
    pub aggregate_queue_positions: HashMap<AggregateKey, QueueAggregatePositions>,
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
}