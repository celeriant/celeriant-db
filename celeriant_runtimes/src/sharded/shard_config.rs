use std::path::PathBuf;

use celeriant_aggregate::{node_config::NodeConfig, read_operations::read_structures::AggregateReadConfig, write_operations::aggregate_write_config::AggregateWriteConfig};

/// Per-node configuration that gets cloned to each shard.
/// Does not include shard_id since that's determined at runtime.
#[derive(Clone, Debug)]
pub struct ShardConfig {
    pub node_id: u128,
    pub num_shards: usize,
    pub data_root: PathBuf,
    pub listen_address: String,
    pub async_flush_ms: u64,
    pub max_open_aggregates: usize,
    pub aggregate_read_max_chunk_size: u64,
    pub aggregate_write_max_chunk_size: usize,
    pub aggregate_write_max_data_cache_size_bytes: usize,
    pub cache_trim_factor: usize,
    pub max_request_size: Option<u32>,
    pub max_event_batches_response_size: usize,
    pub max_requested_latency_ms: u64,
}

impl ShardConfig {
    pub fn aggregate_read_config(&self) -> AggregateReadConfig {
        AggregateReadConfig {
            max_chunk_size: self.aggregate_read_max_chunk_size,
        }
    }

    pub fn aggregate_write_config(&self) -> AggregateWriteConfig {
        AggregateWriteConfig {
            max_data_cache_size_bytes: self.aggregate_write_max_data_cache_size_bytes,
            cache_trim_factor: self.cache_trim_factor,
            max_chunk_size: self.aggregate_write_max_chunk_size,
        }
    }

    pub fn node_config(&self) -> NodeConfig {
        NodeConfig {
            data_root_folder: self
                .data_root
                .to_string_lossy()
                .into_owned(),
            node_id: self.node_id,
            async_flush_ms: self.async_flush_ms,
            max_open_aggregates: self.max_open_aggregates,
            max_request_size: self.max_request_size,
            listen_address: self.listen_address.clone(),
            max_event_batches_response_size: Some(self.max_event_batches_response_size),            
        }
    }
}