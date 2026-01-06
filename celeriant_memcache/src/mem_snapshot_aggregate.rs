/// Status of an aggregate in the cache
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateStatus {
    /// Aggregate exists with data
    Found,
    /// Aggregate was never created or doesn't exist
    NotFound,
    /// Aggregate was soft deleted
    Deleted,
}

/// We store the log id and tail of metablock position for this
/// aggregate in the mem cache, it allows us to skip right to
/// the last batch for this aggregate!
/// We also store the current event and batch indexes for writes,
/// used when adding writes to the in-memory queue
pub struct MemSnapshotAggregate {
    pub status: AggregateStatus,
    pub log_id: u64,
    pub metablock_absolute_pos: u64,
    pub event_index: u64,
    pub event_batch_index: u64,
    pub min_event_batch_index: u64,
    pub allow_recreate: bool,
    pub allow_index_continuation: bool,
}

impl MemSnapshotAggregate {
    pub fn not_found() -> Self {
        Self {
            status: AggregateStatus::NotFound,
            event_batch_index: 0,
            event_index: 0,
            log_id: 0,
            metablock_absolute_pos: 0,
            min_event_batch_index: 0,
            allow_recreate: false,
            allow_index_continuation: false,
        }
    }

    pub fn deleted(
        event_index: u64,
        event_batch_index: u64,
        allow_recreate: bool,
        allow_index_continuation: bool,
    ) -> Self {
        Self {
            status: AggregateStatus::Deleted,
            event_batch_index,
            event_index,
            log_id: 0,
            metablock_absolute_pos: 0,
            min_event_batch_index: 0,
            allow_recreate,
            allow_index_continuation,
        }
    }

    pub fn found(
        log_id: u64,
        metablock_absolute_pos: u64,
        event_index: u64,
        event_batch_index: u64,
        min_event_batch_index: u64,
    ) -> Self {
        Self {
            status: AggregateStatus::Found,
            log_id,
            metablock_absolute_pos,
            event_index,
            event_batch_index,
            min_event_batch_index,
            allow_recreate: false,
            allow_index_continuation: false,
        }
    }
}