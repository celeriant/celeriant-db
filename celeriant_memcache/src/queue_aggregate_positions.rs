use std::{collections::HashMap};

#[derive(Default)]
pub struct QueueAggregatePositions {
    pub event_index: u64,
    pub event_batch_index: u64,
    pub client_event_indexes: HashMap<u128, u64>,
}