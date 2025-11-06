use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct AggregateInfo {
    pub aggregate_id: u128,
    pub created_on: u64,
    pub modified_on: u64,
    pub disk_size_bytes: u64,
    pub first_event_batch_index: u64,
    pub last_event_batch_index: u64,
}