use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::metablocks::{
    metablock_event_batch::MetablockEventBatch,
    metablock_schema_registration::MetablockSchemaRegistration,
    metablock_segment_summary::MetablockSegmentSummary,
    metablock_soft_delete::MetablockSoftDelete, metablock_soft_trim::MetablockSoftTrim,
};

/// Different kinds of WAL metablocks
/// All metablocks are fixed size 512 byte blocks
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub enum MetablockKind {
    EventBatchMetadata(MetablockEventBatch),
    SchemaRegistration(MetablockSchemaRegistration),
    SoftDelete(MetablockSoftDelete),
    SoftTrim(MetablockSoftTrim),
    SegmentSummary(MetablockSegmentSummary),
}
