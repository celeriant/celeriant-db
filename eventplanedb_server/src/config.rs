use std::path::PathBuf;
use clap::Parser;

#[derive(Clone, Debug, Parser)]
#[command(name = "eventplanedb")]
#[command(about = "EventPlaneDB Server", long_about = None)]
pub struct EventPlaneDBConfig {
    // Filesystem / naming
    #[arg(long, default_value = "data", help = "Data directory path")]
    pub data_root: PathBuf,

    #[arg(long, default_value_t = 100, help = "Asynchronous flush interval in milliseconds")]
    pub async_flush_ms: u64,

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
    
    #[arg(long, default_value_t = 32 * 1024, help = "Max chunk size for aggregate writes (32kb)")]
    pub aggregate_write_max_chunk_size: usize,
    
    #[arg(long, default_value_t = 1024 * 1024 * 128, help = "Max data cache size for aggregate writes (128 MiB)")]
    pub aggregate_write_max_data_cache_size_bytes: usize,

    #[arg(long, default_value = "10", help = "Cache trim factor")]
    pub cache_trim_factor: usize,

    // Wire protocol constants
    #[arg(long, help = "Maximum request message size (16 MiB)")]
    pub max_request_size: Option<u32>,

    // Wire protocol constants
    #[arg(long, help = "Maximum size of event batches set to return (64 MiB)")]
    pub max_event_batches_response_size: Option<u64>,

    // Misc / logging
    #[arg(long, default_value = "info", help = "Default log level (trace, debug, info, warn, error)")]
    pub default_log_level: String,

    // S3 object-store integration
    #[arg(long, default_value_t = false, help = "Enable Amazon S3 object-store integration")]
    pub s3_enabled: bool,

    #[arg(long, requires = "s3_enabled", help = "Amazon S3 region (e.g. us-east-1)")]
    pub s3_region: Option<String>,

    #[arg(long, requires = "s3_enabled", help = "Amazon S3 bucket name")]
    pub s3_bucket: Option<String>,

    #[arg(long, requires = "s3_enabled", help = "AWS access key ID for S3 object store")]
    pub s3_access_key_id: Option<String>,

    #[arg(long, requires = "s3_enabled", help = "AWS secret access key for S3 object store")]
    pub s3_secret_access_key: Option<String>,

    #[arg(long, requires = "s3_enabled", help = "Single-level subfolder to isolate cluster data inside the bucket")]
    pub s3_subfolder: Option<String>,
}

impl Default for EventPlaneDBConfig {
    fn default() -> Self {
        Self {
            data_root: PathBuf::from("data"),
            async_flush_ms: 100,
            listen_address: "0.0.0.0:10000".to_string(),
            mesh_channel_size: 1024,
            num_shards: None,
            aggregate_read_max_chunk_size: 32 * 1024,
            aggregate_write_max_chunk_size: 32 * 1024,
            aggregate_write_max_data_cache_size_bytes: 1024 * 1024 * 128,
            cache_trim_factor: 10,
            max_open_aggregates: 10000,
            max_request_size: Some(16 * 1024 * 1024),
            max_event_batches_response_size: Some(64 * 1024 * 1024),
            default_log_level: "info".to_string(),
            s3_enabled: false,
            s3_region: None,
            s3_bucket: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_subfolder: None,
        }
    }
}