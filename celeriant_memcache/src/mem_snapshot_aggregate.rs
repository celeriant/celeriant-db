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
/// We also store the current event and aggregate versiones for writes,
/// used when adding writes to the in-memory queue
#[derive(Clone)]
pub struct MemSnapshotAggregate {
    pub status: AggregateStatus,
    pub log_id: u64,
    pub metablock_absolute_pos: u64,
    pub event_seq: u64,
    pub aggregate_version: u64,
    pub min_aggregate_version: u64,
    pub allow_recreate: bool,
    pub allow_sequence_continuation: bool,
}

impl MemSnapshotAggregate {
    pub fn not_found() -> Self {
        Self {
            status: AggregateStatus::NotFound,
            aggregate_version: 0,
            event_seq: 0,
            log_id: 0,
            metablock_absolute_pos: 0,
            min_aggregate_version: 0,
            allow_recreate: false,
            allow_sequence_continuation: false,
        }
    }

    pub fn deleted(
        log_id: u64,
        metablock_absolute_pos: u64,
        event_seq: u64,
        aggregate_version: u64,
        allow_recreate: bool,
        allow_sequence_continuation: bool,
    ) -> Self {
        Self {
            status: AggregateStatus::Deleted,
            aggregate_version,
            event_seq,
            log_id,
            metablock_absolute_pos,
            min_aggregate_version: 0,
            allow_recreate,
            allow_sequence_continuation,
        }
    }

    pub fn found(
        log_id: u64,
        metablock_absolute_pos: u64,
        event_seq: u64,
        aggregate_version: u64,
        min_aggregate_version: u64,
    ) -> Self {
        Self {
            status: AggregateStatus::Found,
            log_id,
            metablock_absolute_pos,
            event_seq,
            aggregate_version,
            min_aggregate_version,
            allow_recreate: false,
            allow_sequence_continuation: false,
        }
    }
}