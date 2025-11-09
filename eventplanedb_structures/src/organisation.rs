use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Organisation {
    pub org_id: u128,
    pub created_at: u64,
    pub modified_at: u64,
    pub disk_usage: u64,
}