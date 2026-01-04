/// We store the log id and tail of metablock position for this
/// aggregate in the mem cache, it allows us to skip right to
/// the last batch for this aggregate!
/// We also store the current event and batch indexes for writes,
/// used when adding writes to the in-memory queue
pub struct MemSnapshotAggregate {
    pub log_id: u64,
    pub metablock_absolute_pos: u64,
    pub event_index: u64,
    pub event_batch_index: u64,
}
impl MemSnapshotAggregate {
    pub fn not_found() -> Self {
        Self {
            event_batch_index: 0,
            event_index: 0,
            log_id: 0,
            metablock_absolute_pos: 0,
        }
    }
}