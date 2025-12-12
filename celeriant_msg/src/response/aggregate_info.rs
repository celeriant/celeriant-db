use bincode::{Decode, Encode};
use celeriant_wal::aggregate_key::AggregateKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct AggregateInfo {
    pub key: AggregateKey,
    pub created_at: u64,
    pub modified_at: u64,
    pub disk_usage: u64,
}