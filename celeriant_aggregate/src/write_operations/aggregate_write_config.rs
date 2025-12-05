#[derive(Debug, Clone)]
pub struct AggregateWriteConfig {
    pub max_data_cache_size_bytes: usize,
    pub cache_trim_factor: usize,
    pub max_chunk_size: usize
}