use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DirectoryFilters {
    pub created_after_or_on: Option<u64>,
    pub created_before_or_on: Option<u64>,
    pub modified_after_or_on: Option<u64>,
    pub modified_before_or_on: Option<u64>,
    pub disk_usage_less_than_or_equal: Option<u64>,
    pub disk_usage_greater_than_or_equal: Option<u64>,
}

impl Default for DirectoryFilters {
    fn default() -> Self {
        Self {
            created_after_or_on: None,
            created_before_or_on: None,
            modified_after_or_on: None,
            modified_before_or_on: None,
            disk_usage_less_than_or_equal: None,
            disk_usage_greater_than_or_equal: None,
        }
    }
}