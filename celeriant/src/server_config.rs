use celeriant_runtimes::{ShardConfig, SidecarConfig};
use celeriant_sidecar::s3_config::S3Config;
use clap::Parser;
use std::{path::PathBuf, time::Duration};

#[derive(Clone, Debug, Parser)]
#[command(name = "celeriant")]
#[command(about = "Celeriant TCP Server", long_about = None)]
pub struct ServerConfig {
    // Filesystem / naming
    #[arg(
        long,
        default_value = "data",
        env = "CELERIANT_DATA_ROOT",
        help = "Data directory path"
    )]
    pub data_root: PathBuf,

    #[arg(
        long,
        default_value_t = 100,
        env = "CELERIANT_ASYNC_FLUSH_MS",
        help = "Asynchronous flush interval in milliseconds"
    )]
    pub async_flush_ms: u64,

    // Server networking / shards
    #[arg(
        long,
        default_value = "0.0.0.0:10000",
        env = "CELERIANT_LISTEN_ADDRESS",
        help = "Server listen address"
    )]
    pub listen_address: String,

    #[arg(
        long,
        default_value = "1024",
        env = "CELERIANT_MESH_CHANNEL_SIZE",
        help = "Mesh channel size for inter-shard communication"
    )]
    pub mesh_channel_size: usize,

    #[arg(
        long,
        env = "CELERIANT_NUM_SHARDS",
        help = "Number of shards (defaults to CPU count)"
    )]
    pub num_shards: Option<usize>,

    #[arg(
        long,
        default_value = "1000",
        env = "CELERIANT_MAX_OPEN_FILES",
        help = "Maximum number of open files per shard"
    )]
    pub max_open_files: u64,

    // Aggregate I/O tuning (read/write)
    #[arg(long, default_value_t = 32 * 1024, env = "CELERIANT_AGGREGATE_READ_MAX_CHUNK_SIZE", help = "Max chunk size for aggregate reads (32kb)")]
    pub aggregate_read_max_chunk_size: u64,

    #[arg(long, default_value_t = 32 * 1024, env = "CELERIANT_AGGREGATE_WRITE_MAX_CHUNK_SIZE", help = "Max chunk size for aggregate writes (32kb)")]
    pub aggregate_write_max_chunk_size: usize,

    #[arg(long, default_value_t = 1024 * 1024 * 128, env = "CELERIANT_AGGREGATE_WRITE_MAX_DATA_CACHE_SIZE_BYTES", help = "Max data cache size for aggregate writes (128 MiB)")]
    pub aggregate_write_max_data_cache_size_bytes: usize,

    #[arg(
        long,
        default_value = "10",
        env = "CELERIANT_CACHE_TRIM_FACTOR",
        help = "Cache trim factor"
    )]
    pub cache_trim_factor: usize,

    // Wire protocol constants
    #[arg(long, default_value_t = 1024 * 1024 * 16, env = "CELERIANT_MAX_REQUEST_SIZE", help = "Maximum request message size (16 MiB)")]
    pub max_request_size: u32,

    // Wire protocol constants
    #[arg(long, default_value_t = 1024 * 1024 * 64, env = "CELERIANT_MAX_EVENT_BATCHES_RESPONSE_SIZE", help = "Maximum size of event batches set to return (64 MiB)")]
    pub max_event_batches_response_size: usize,

    // Maximum latency setting for watch connections
    #[arg(
        long,
        default_value_t = 2000,
        env = "CELERIANT_MAX_REQUESTED_LATENCY_MS",
        help = "Maximum latency a client can use for watch connections (2s)"
    )]
    pub max_requested_latency_ms: u64,

    #[arg(
        long,
        default_value_t = 30000,
        env = "CELERIANT_SLOW_CLIENT_TIMEOUT_MS",
        help = "Maximum time a client has to pull down server messages over tcp (30s)"
    )]
    pub slow_client_timeout_ms: u64,

    // Size of each individual log file on disk
    #[arg(long, default_value_t = 1024 * 1024 * 1024, env = "CELERIANT_SHARD_LOG_PREALLOCATE_BYTES", help = "Size of each individual log file on disk (1GB)")]
    pub shard_log_preallocate_bytes: u64,

    // Amount of recent write data to keep in memory for each shard
    #[arg(long, default_value_t = 128 * 1024 * 1024, env = "CELERIANT_RECENT_WRITE_CACHE_BYTES", help = "Amount of recent write data to keep in memory for each shard (128MB)")]
    pub recent_write_cache_bytes: u64,

    // Maximum latency setting for watch connections
    #[arg(
        long,
        default_value_t = 10000,
        env = "CELERIANT_DURABLE_WRITE_WITH_DELAY_US",
        help = "Amortised fsync duration block (10ms)"
    )]
    pub durable_write_with_delay_us: u64,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        env = "CELERIANT_ALLOW_NON_DURABLE_WRITES",
        help = "Acknowledge writes to client before persisting to disk",
    )]
    pub allow_non_durable_writes: bool,

    // Misc / logging
    #[arg(
        long,
        default_value = "info",
        env = "CELERIANT_DEFAULT_LOG_LEVEL",
        help = "Default log level (trace, debug, info, warn, error)"
    )]
    pub default_log_level: String,

    // S3 object-store integration
    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        env = "CELERIANT_S3_ENABLED",
        help = "Enable Amazon S3 object-store integration",
        requires_ifs = [("true", "s3_region"), ("true", "s3_bucket")]
    )]
    pub s3_enabled: bool,

    #[arg(
        long,
        requires = "s3_enabled",
        env = "CELERIANT_S3_REGION",
        help = "Amazon S3 region (e.g. us-east-1)"
    )]
    pub s3_region: Option<String>,

    #[arg(
        long,
        requires = "s3_enabled",
        env = "CELERIANT_S3_BUCKET",
        help = "Amazon S3 bucket name"
    )]
    pub s3_bucket: Option<String>,

    #[arg(
        long,
        requires = "s3_enabled",
        env = "CELERIANT_S3_ACCESS_KEY_ID",
        help = "AWS access key ID for S3 object store"
    )]
    pub s3_access_key_id: Option<String>,

    #[arg(
        long,
        requires = "s3_enabled",
        env = "CELERIANT_S3_SECRET_ACCESS_KEY",
        help = "AWS secret access key for S3 object store"
    )]
    pub s3_secret_access_key: Option<String>,

    #[arg(
        long,
        requires = "s3_enabled",
        env = "CELERIANT_S3_SUBFOLDER",
        help = "Single-level subfolder to isolate cluster data inside the bucket"
    )]
    pub s3_subfolder: Option<String>,
}

impl ServerConfig {
    pub fn to_sidecar_config(&self, num_shards: usize) -> SidecarConfig {
        SidecarConfig {
            worker_threads: std::cmp::max(2, num_shards / 2),
            control_lane_capacity: 256,
            data_lane_capacity: 1024,
        }
    }

    pub fn to_sidecar_store_config(&self) -> celeriant_sidecar::store_config::StoreConfig {
        let s3 = if self.s3_enabled {
            let subfolder = self
                .s3_subfolder
                .as_ref()
                .filter(|s| !s.is_empty())
                .cloned();

            Some(S3Config {
                region: self.s3_region.clone().unwrap(),
                bucket: self.s3_bucket.clone().unwrap(),
                access_key_id: self.s3_access_key_id.clone(),
                secret_access_key: self.s3_secret_access_key.clone(),
                subfolder,
            })
        } else {
            None
        };

        celeriant_sidecar::store_config::StoreConfig { s3 }
    }

    pub fn to_shard_config(&self, node_id: u128, num_shards: usize) -> ShardConfig {
        ShardConfig {
            node_id,
            num_shards,
            data_root: self.data_root.clone(),
            listen_address: self.listen_address.clone(),
            async_flush_ms: self.async_flush_ms,
            max_open_files: self.max_open_files,
            aggregate_read_max_chunk_size: self.aggregate_read_max_chunk_size,
            aggregate_write_max_chunk_size: self.aggregate_write_max_chunk_size,
            aggregate_write_max_data_cache_size_bytes: self
                .aggregate_write_max_data_cache_size_bytes,
            cache_trim_factor: self.cache_trim_factor,
            max_request_size: Some(self.max_request_size),
            max_event_batches_response_size: self.max_event_batches_response_size,
            max_requested_latency_ms: self.max_requested_latency_ms,
            shard_log_preallocate_bytes: self.shard_log_preallocate_bytes,
            durable_write_with_delay_us: if self.allow_non_durable_writes {
                None
            } else {
                Some(self.durable_write_with_delay_us)
            },
            recent_write_cache_bytes: self.recent_write_cache_bytes,
            slow_client_timeout: Duration::from_millis(self.slow_client_timeout_ms),
        }
    }

    /// Returns a list of (field_name, value) pairs for fields that differ from defaults
    pub fn non_default_entries(&self) -> Vec<(&'static str, String)> {
        let defaults = Self::default();
        let mut entries = Vec::new();

        macro_rules! check_field {
            ($field:ident) => {
                if self.$field != defaults.$field {
                    entries.push((stringify!($field), format!("{:?}", self.$field)));
                }
            };
            ($field:ident, sensitive) => {
                if self.$field != defaults.$field {
                    entries.push((stringify!($field), "[REDACTED]".to_string()));
                }
            };
        }

        check_field!(data_root);
        check_field!(async_flush_ms);
        check_field!(listen_address);
        check_field!(mesh_channel_size);
        check_field!(num_shards);
        check_field!(max_open_files);
        check_field!(aggregate_read_max_chunk_size);
        check_field!(aggregate_write_max_chunk_size);
        check_field!(aggregate_write_max_data_cache_size_bytes);
        check_field!(cache_trim_factor);
        check_field!(max_request_size);
        check_field!(max_event_batches_response_size);
        check_field!(max_event_batches_response_size);
        check_field!(max_requested_latency_ms);
        check_field!(shard_log_preallocate_bytes);
        check_field!(durable_write_with_delay_us);
        check_field!(allow_non_durable_writes);
        check_field!(default_log_level);
        check_field!(s3_enabled);
        check_field!(s3_region);
        check_field!(s3_bucket);
        check_field!(s3_access_key_id, sensitive);
        check_field!(s3_secret_access_key, sensitive);
        check_field!(s3_subfolder);

        entries
    }

    pub fn log_non_defaults(&self) {
        let entries = self.non_default_entries();
        if entries.is_empty() {
            tracing::info!("Server starting with default configuration");
        } else {
            tracing::info!("Server starting with custom configuration:");
            for (name, value) in entries {
                tracing::info!("  {}: {}", name, value);
            }
        }
    }
}

impl Default for ServerConfig {
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
            max_open_files: 1000,
            max_request_size: 16 * 1024 * 1024,
            max_event_batches_response_size: 64 * 1024 * 1024,
            max_requested_latency_ms: 2000,
            default_log_level: "info".to_string(),
            s3_enabled: false,
            s3_region: None,
            s3_bucket: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_subfolder: None,
            shard_log_preallocate_bytes: 1024 * 1024 * 1024,
            durable_write_with_delay_us: 10000,
            allow_non_durable_writes: false,
            recent_write_cache_bytes: 128 * 1024 * 1024,
            slow_client_timeout_ms: 30000,
        }
    }
}
