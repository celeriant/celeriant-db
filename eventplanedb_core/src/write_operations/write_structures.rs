use eventplanedb_structures::compression_type::CompressionType;

#[derive(Debug, Clone)]
pub struct AggregateWriteConfig {
    pub max_data_cache_size_bytes: usize,
    pub cache_trim_factor: usize,
    pub max_chunk_size: usize
}

pub struct WriteOptions {
    pub client_id: u128,
    pub user_id: Option<u128>,
    pub expected_event_batch_index: Option<u64>,
    pub enforce_client_idempotency: bool,
    pub server_timestamp_millis: u64,
    pub compression_type: CompressionType,
}