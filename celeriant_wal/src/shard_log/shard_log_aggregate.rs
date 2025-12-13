use bincode::{Decode, Encode};

#[derive(Debug, Clone, Encode, Decode)]
pub struct ShardLogAggregate {
    pub last_event_index: u64,
    pub last_event_batch_index: u64,

    /// Aggregates can get trimmed
    pub min_available_event_batch_index: u64,

    pub compressed_size_bytes: u64,
    pub uncompressed_size_bytes: u64,

    pub created_at: u64,
    pub updated_at: u64,

    /// Read at is usefull to determine which aggregates don't 
    /// need their metadata set to be kept in memory
    pub read_at: Option<u64>,
}

impl ShardLogAggregate {
    pub fn new(
        current_time_ms: u64,
    ) -> Self {
        Self {
            last_event_index: 0,
            last_event_batch_index: 0,
            min_available_event_batch_index: 0,
            compressed_size_bytes: 0,
            uncompressed_size_bytes: 0,
            created_at: current_time_ms,
            updated_at: current_time_ms,
            read_at: None,
        }
    }

    pub fn append_event_batches(
        &mut self,
        current_time_ms: u64,
        last_event_index: u64,
        last_event_batch_index: u64,
        additional_compressed_size_bytes: u64,
        additional_uncompressed_size_bytes: u64,
    ) {
        self.updated_at = current_time_ms;
        self.last_event_index = last_event_index;
        self.last_event_batch_index = last_event_batch_index;
        self.compressed_size_bytes = self.compressed_size_bytes.saturating_add(additional_compressed_size_bytes);
        self.uncompressed_size_bytes = self.uncompressed_size_bytes.saturating_add(additional_uncompressed_size_bytes);
    }

    pub fn trim_start(
        &mut self,
        min_available_event_batch_index: u64,
        saved_compressed_size_bytes: u64,
        saved_uncompressed_size_bytes: u64,
    ) {
        self.min_available_event_batch_index = min_available_event_batch_index;
        self.compressed_size_bytes = self.compressed_size_bytes.saturating_sub(saved_compressed_size_bytes);
        self.uncompressed_size_bytes = self.uncompressed_size_bytes.saturating_sub(saved_uncompressed_size_bytes);
    }
}
