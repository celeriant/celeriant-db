use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Organisation {
    pub org_id: u128,
    pub created_on: u64,
    pub modified_on: u64,
    pub disk_size_bytes: u64,
}