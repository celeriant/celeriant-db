use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct MetablockSegmentSummary {
    pub segment_log_id: u64,
    pub unique_org_count: u32,
    pub unique_aggregate_type_count: u32,
    pub unique_aggregate_count: u32,
    pub datablock_position: u64,
    pub datablock_compressed_size: u64,
    pub datablock_uncompressed_size: u64,
}
