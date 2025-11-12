use std::collections::HashMap;

use eventplanedb_structures::{
    event_batch_item::EventBatchItem, event_batch_metadata::EventBatchMetadata,
};

#[derive(Debug)]
pub struct MetadataWithAbsolutePosition {
    pub event_batch_metadata: EventBatchMetadata,
    pub event_batch_absolute_position: u64,
}

#[derive(Debug)]
pub struct FilePositions {
    pub metadata_position: u64,
    pub event_batch_position: u64,
}

#[derive(Clone)]
pub struct AggregateReadConfig {
    pub max_data_cache_size_bytes: usize,
    pub max_chunk_size: u64,
}

#[derive(Debug)]
pub struct CacheableReadResult {
    pub uncached_metadata_set: Vec<MetadataWithAbsolutePosition>,
    pub filtered_event_batches: Vec<EventBatchItem>,
    pub next_event_batch_index: Option<u64>,
}

#[derive(Debug)]
pub struct CacheableReadAllResult {
    pub uncached_metadata_set: Vec<MetadataWithAbsolutePosition>,
    pub batches: Vec<(EventBatchMetadata, EventBatchItem)>,
    pub next_event_batch_index: Option<u64>,
}

/// Holds metadata about the aggregate that is required for
/// the writer to function.
pub struct WriteOperationsDataRequirements {
    pub file_len_metadata: u64,
    pub file_len_event_batch: u64,
    pub minimum_available_event_batch_index: u64,
    pub next_event_index: u64,
    pub next_event_batch_index: u64,
    pub client_event_indexes: HashMap<u128, u64>,
}

pub struct WriteOperationsDataRequirementsAndCachedData {
    pub write_operations_data_requirements: WriteOperationsDataRequirements,
    pub uncached_metadata_set: Vec<MetadataWithAbsolutePosition>,
}
