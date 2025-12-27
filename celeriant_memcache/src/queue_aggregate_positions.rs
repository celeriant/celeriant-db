use std::{collections::HashMap};

#[derive(Default)]
pub struct QueueAggregatePositions {
    pub log_id: u64,
    pub metablock_absolute_pos: u64,
    pub event_index: u64,
    pub event_batch_index: u64,
    pub client_event_indexes: HashMap<u128, u64>,
}