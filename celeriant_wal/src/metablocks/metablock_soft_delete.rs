use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::aggregate_key::AggregateKey;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct MetablockSoftDelete {
    pub aggregate_key: AggregateKey,
    pub allow_recreate: bool,
    pub allow_sequence_continuation: bool,
    pub aggregate_version: u64,
    pub event_seq: u64,
    pub client_id: u128,
    pub user_id: Option<u128>,
}
