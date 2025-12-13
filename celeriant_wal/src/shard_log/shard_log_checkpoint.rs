use std::collections::HashMap;

use bincode::{Decode, Encode};

use crate::{aggregate_key::AggregateKey, shard_log::shard_log_aggregate::ShardLogAggregate};

/// The goal is the checkpoint is the authoritative source of truth
/// for where to write next in the shard log file. It also contains
/// all the metadata for aggregates in the shard log so we know what's
/// the next index, and we can do client idempotency checks
#[derive(Debug, Clone, Encode, Decode)]
pub struct ShardLogCheckpoint {

    /// The checkpoint stores the file size as we can't trust the OS
    /// as we do direct I/O and might have padded bytes at the end
    pub file_size: u64,

    pub checkpoint_start_pos: u64,

    /// This is a logical cache of aggregate data for quick lookup
    pub aggregates: HashMap<AggregateKey, ShardLogAggregate>,

    /// Metadata is 256 byte fixed size, written from the start of the file
    /// This position indicates the end of the last written metadata entry
    pub metadata_pos: u64,

    /// The position where new event batches can be written to
    /// Note that event batches are written to end of the file
    /// so this position indicates the start of the most recently written batches
    pub event_batches_pos: u64,
}

impl ShardLogCheckpoint {
    pub fn new(file_size: u64, metadata_start_pos: u64, checkpoint_start_pos: u64) -> Self {
        Self {
            file_size,
            checkpoint_start_pos,
            aggregates: HashMap::new(),
            metadata_pos: metadata_start_pos,
            event_batches_pos: checkpoint_start_pos,
        }
    }

    pub fn available_space(&self) -> u64 {
        let batches_size = self.checkpoint_start_pos.saturating_sub(self.event_batches_pos);
        self.checkpoint_start_pos
            .saturating_sub(self.metadata_pos.saturating_add(batches_size))
    }

    pub fn has_space(&self, metadata_size: u64, compressed_batch_size: u64) -> bool {
        let required = metadata_size.saturating_add(compressed_batch_size);
        self.available_space() >= required
    }

    pub fn append_event_batches(
        &mut self,
        additional_metadata_size_bytes: u64,
        additional_event_batches_size_bytes: u64,
    ) {
        self.metadata_pos = self
            .metadata_pos
            .saturating_add(additional_metadata_size_bytes);
        self.event_batches_pos = self
            .event_batches_pos
            .saturating_sub(additional_event_batches_size_bytes);
    }
}
