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