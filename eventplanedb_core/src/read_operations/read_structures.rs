use std::collections::HashMap;

use eventplanedb_structures::{
    event_batch_metadata::EventBatchMetadata
};

#[derive(Debug, Clone)]
pub struct MetadataWithAbsolutePosition {
    pub event_batch_metadata: EventBatchMetadata,
    pub event_batch_absolute_position: u64,
    pub format_version_on_disk: u32,
}

#[derive(Debug)]
pub struct FilePositions {
    pub metadata_position: u64,
    pub event_batch_position: u64,
}

#[derive(Clone)]
pub struct AggregateReadConfig {
    pub max_chunk_size: u64,
}

/// Holds metadata about the aggregate that is required for
/// the writer to function.
pub struct WriteOperationsDataRequirements {
    pub file_len_metadata: u64,
    pub file_len_event_batch: u64,
    pub metadata_buffer: Vec<u8>,
    pub event_batch_buffer: Vec<u8>,
    pub minimum_available_event_batch_index: u64,
    pub next_event_index: u64,
    pub next_event_batch_index: u64,
    pub client_event_indexes: HashMap<u128, u64>,
}
