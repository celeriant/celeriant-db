use std::{collections::HashMap};

/// Metadata for an aggregate, representing the queued writes.
/// Write initially queues items, and bumps event, batch and cliet indexes
/// On write to disk, we set the log id and metablock positions for cache update
#[derive(Default, Clone)]
pub struct QueueAggregatePositions {
    pub pending_delete: bool,
    pub allow_recreate: bool,
    pub allow_sequence_continuation: bool,
    pub log_id: u64,
    pub metablock_absolute_pos: u64,
    pub wal_seq: u64,
    pub event_seq: u64,
    pub aggregate_version: u64,
    pub min_aggregate_version: u64,
    pub client_seqes: HashMap<u128, u64>,
}