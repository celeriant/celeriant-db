use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct AppendResult {
    pub event_batch_index: u64,
    pub events_written: usize,
}