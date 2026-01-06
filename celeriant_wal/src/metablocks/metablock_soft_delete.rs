use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::aggregate_key::AggregateKey;

#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct MetablockSoftDelete {
    pub aggregate_key: AggregateKey,
    pub allow_recreate: bool,
    pub allow_index_continuation: bool,
    pub event_batch_index: u64,
    pub event_index: u64,
    pub client_id: u128,
    pub user_id: Option<u128>,
}
