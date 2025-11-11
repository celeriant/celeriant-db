use std::path::PathBuf;
use clap::Parser;

#[derive(Clone, Debug, Parser)]
#[command(name = "eventplanedb")]
#[command(about = "EventPlaneDB Server", long_about = None)]
pub struct EventPlaneDBConfig {
    // Filesystem / naming
    #[arg(long, default_value = "data", help = "Data directory path")]
    pub data_root: PathBuf,

    // Server networking / shards
    #[arg(long, default_value = "0.0.0.0:10000", help = "Server listen address")]
    pub listen_address: String,
    
    #[arg(long, default_value = "1024", help = "Mesh channel size for inter-shard communication")]
    pub mesh_channel_size: usize,
    
    #[arg(long, help = "Number of shards (defaults to CPU count)")]
    pub num_shards: Option<usize>,

    #[arg(long, default_value = "10000", help = "Maximum number of open aggregates")]
    pub max_open_aggregates: usize,

    // Aggregate I/O tuning (read/write)
    #[arg(long, default_value_t = 32 * 1024, help = "Max chunk size for aggregate reads (32kb)")]
    pub aggregate_read_max_chunk_size: u64,
    
    #[arg(long, default_value_t = 1024 * 1024 * 3, help = "Max data cache size for aggregate reads (3 MiB)")]
    pub aggregate_read_max_data_cache_size_bytes: usize,
    
    #[arg(long, default_value_t = 32 * 1024, help = "Max chunk size for aggregate writes (32kb)")]
    pub aggregate_write_max_chunk_size: usize,
    
    #[arg(long, default_value_t = 1024 * 1024 * 128, help = "Max data cache size for aggregate writes (128 MiB)")]
    pub aggregate_write_max_data_cache_size_bytes: usize,

    #[arg(long, default_value = "10", help = "Cache trim factor")]
    pub cache_trim_factor: usize,

    // Wire protocol constants
    #[arg(long, default_value_t = 64 * 1024 * 1024, help = "Maximum message size (64 MiB)")]
    pub max_message_size: u32,

    // Misc / logging
    #[arg(long, default_value = "info", help = "Default log level (trace, debug, info, warn, error)")]
    pub default_log_level: String,
}

impl Default for EventPlaneDBConfig {
    fn default() -> Self {
        Self {
            data_root: PathBuf::from("data"),
            listen_address: "0.0.0.0:10000".to_string(),
            mesh_channel_size: 1024,
            num_shards: None,
            aggregate_read_max_chunk_size: 32 * 1024,
            aggregate_read_max_data_cache_size_bytes: 1024 * 1024 * 3,
            aggregate_write_max_chunk_size: 32 * 1024,
            aggregate_write_max_data_cache_size_bytes: 1024 * 1024 * 128,
            cache_trim_factor: 10,
            max_open_aggregates: 10000,
            max_message_size: 64 * 1024 * 1024,
            default_log_level: "info".to_string(),
        }
    }
}