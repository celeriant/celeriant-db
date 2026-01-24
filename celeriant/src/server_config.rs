use celeriant_runtimes::RoutingRule;
use celeriant_runtimes::{ShardConfig, SidecarConfig};
use celeriant_shard::timestamp_config::{TimestampConfig, TimestampPrecision};
use celeriant_sidecar::s3_config::S3Config;
use clap::Parser;
use std::{path::PathBuf, time::Duration};
use clap::ValueEnum;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, ValueEnum)]
pub enum ConfigTimestampPrecision {
    #[default]
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, ValueEnum)]
pub enum ConfigClusterRole {
    #[default]
    Standalone,
    Leader,
    Follower,
}

#[derive(Clone, Debug, Parser)]
#[command(name = "celeriant")]
#[command(about = "Celeriant TCP Server", long_about = None)]
pub struct ServerConfig {

    #[arg(
        long,
        default_value = "milliseconds",
        env = "CELERIANT_TIMESTAMP_PRECISION",
        help = "Timestamp precision: milliseconds, microseconds, or nanoseconds"
    )]
    pub timestamp_precision: ConfigTimestampPrecision,

    #[arg(
        long,
        default_value_t = 0,
        env = "CELERIANT_TIMESTAMP_EPOCH_OFFSET_SECS",
        help = "Custom epoch as seconds offset from Unix epoch"
    )]
    pub timestamp_epoch_offset_secs: i64,

    #[arg(
        long,
        default_value = "data",
        env = "CELERIANT_DATA_ROOT",
        help = "Data directory path"
    )]
    pub data_root: PathBuf,

    #[arg(
        long,
        default_value = "0.0.0.0",
        env = "CELERIANT_LISTEN_ADDRESS",
        help = "Server listen address"
    )]
    pub listen_address: String,

    #[arg(
        long,
        default_value_t = 10000,
        env = "CELERIANT_CLIENT_PORT",
        help = "Port for client connections"
    )]
    pub client_port: u16,

    #[arg(
        long,
        default_value_t = 10001,
        env = "CELERIANT_REPLICATION_PORT",
        help = "Port for leader-to-follower replication"
    )]
    pub replication_port: u16,

    #[arg(
        long,
        default_value = "standalone",
        env = "CELERIANT_CLUSTER_ROLE",
        help = "Cluster role: standalone, leader, or follower"
    )]
    pub cluster_role: ConfigClusterRole,

    #[arg(
        long,
        env = "CELERIANT_FOLLOWER_ADDRESS",
        help = "Address of follower node for replication (required when cluster_role=leader)"
    )]
    pub follower_address: Option<String>,

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

    #[arg(long, default_value_t = 32 * 1024, env = "CELERIANT_READ_MAX_CHUNK_SIZE", help = "Max chunk size for disk reads (32kb)")]
    pub read_max_chunk_size: u64,

    #[arg(long, default_value_t = 32 * 1024, env = "CELERIANT_WRITE_MAX_CHUNK_SIZE", help = "Max chunk size for disk writes (32kb)")]
    pub write_max_chunk_size: u64,

    #[arg(
        long,
        default_value = "aggregate_id",
        env = "CELERIANT_ROUTING_RULE",
        help = "Shard routing rule: org_id, aggregate_type_id, or aggregate_id"
    )]
    pub routing_rule: RoutingRule,

    #[arg(long, default_value_t = 1024 * 1024 * 16, env = "CELERIANT_MAX_REQUEST_SIZE", help = "Maximum request message size (16 MiB)")]
    pub max_request_size: u64,

    #[arg(long, default_value_t = 1024 * 1024 * 64, env = "CELERIANT_MAX_RESPONSE_SIZE", help = "Maximum response message size (64 MiB)")]
    pub max_response_size: u64,

    #[arg(
        long,
        default_value_t = 2000,
        env = "CELERIANT_MAX_REQUESTED_LATENCY_MS",
        help = "Maximum latency a client can use for watch connections (2s)"
    )]
    pub max_requested_latency_ms: u64,

    #[arg(
        long,
        default_value_t = 2000,
        env = "CELERIANT_LIST_MAX_DURATION_MS",
        help = "Maximum duration to scan the WAL for listings (2000ms)"
    )]
    pub list_max_duration_ms: u64,

    #[arg(
        long,
        default_value_t = 20000,
        env = "CELERIANT_LIST_PAGE_SIZE",
        help = "Maximum number of entities to collate for listings per page (20000)"
    )]
    pub list_page_size: u64,

    #[arg(
        long,
        default_value_t = 12 * 1024 * 1024,
        env = "CELERIANT_LIST_WAL_INDEX_CACHE_BYTES",
        help = "Memory to use to keep wal_index positions for listings paging optimisation (12MB)"
    )]
    pub list_wal_index_cache_bytes: u64,

    #[arg(
        long,
        default_value_t = 30000,
        env = "CELERIANT_CLIENT_CONNECTION_TIMEOUT_MS",
        help = "Maximum time a client has to pull down server messages over tcp (30s)"
    )]
    pub client_connection_timeout_ms: u64,

    #[arg(long, default_value_t = 1024 * 1024 * 1024, env = "CELERIANT_SHARD_LOG_PREALLOCATE_BYTES", help = "Size of each individual log file on disk (1GB)")]
    pub shard_log_preallocate_bytes: u64,

    #[arg(long, default_value_t = 512 * 1024 * 1024, env = "CELERIANT_RECENT_WRITE_CACHE_BYTES", help = "Amount of recent write data to keep in memory for each shard (512MB)")]
    pub recent_write_cache_bytes: u64,

    #[arg(long, default_value_t = 64 * 1024 * 1024, env = "CELERIANT_AGGREGATE_CLIENT_SNAPSHOTS_CACHE_BYTES", help = "Amount of recent client idempotency data to keep in memory for each shard (64MB)")]
    pub aggregate_client_snapshots_cache_bytes: u64,

    #[arg(long, default_value_t = 64 * 1024 * 1024, env = "CELERIANT_AGGREGATE_SNAPSHOTS_CACHE_BYTES", help = "Amount of recent aggregate metadata to keep in memory for each shard (64MB)")]
    pub aggregate_snapshots_cache_bytes: u64,

    #[arg(long, default_value_t = 64 * 1024 * 1024, env = "CELERIANT_PENDING_REPLICATION_HIGH_WATER_BYTES", help = "High water mark for pending replication queue before triggering S3 fallback (64MB)")]
    pub pending_replication_high_water_bytes: u64,

    #[arg(long, default_value_t = 5000, env = "CELERIANT_MAX_CLUSTER_TIME_DRIFT_MS", help = "Maximum allowed clock drift between leader and follower nodes (5s)")]
    pub max_cluster_time_drift_ms: u64,

    #[arg(
        long,
        default_value_t = 17000,
        env = "CELERIANT_FSYNC_DELAY_US",
        help = "Amortised fsync duration block (17ms)"
    )]
    pub fsync_delay_us: u64,

    #[arg(
        long,
        default_value_t = 17000,
        env = "CELERIANT_REPLICATION_DELAY_US",
        help = "Amortised replication send duration block (17ms)"
    )]
    pub replication_delay_us: u64,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        env = "CELERIANT_NON_DURABLE_WRITES",
        help = "Acknowledge writes to client before persisting to disk",
    )]
    pub non_durable_writes: bool,

    #[arg(
        long,
        default_value = "info",
        env = "CELERIANT_LOG_LEVEL",
        help = "Log level (trace, debug, info, warn, error)"
    )]
    pub log_level: String,

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
        env = "CELERIANT_S3_REGION",
        help = "Amazon S3 region (e.g. us-east-1)"
    )]
    pub s3_region: Option<String>,

    #[arg(long, env = "CELERIANT_S3_BUCKET", help = "Amazon S3 bucket name")]
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

    #[arg(
        long,
        requires = "s3_enabled",
        env = "CELERIANT_S3_ENDPOINT_OVERRIDE",
        help = "Override the default AWS S3 endpoint URL."
    )]
    pub s3_endpoint_override: Option<String>,

    #[arg(
        long,
        requires = "s3_enabled",
        env = "CELERIANT_S3_SKIP_SIGNATURE",
        help = "Skip AWS Signature Version 4 authentication on requests."
    )]
    pub s3_skip_signature: bool,

    #[arg(
        long,
        requires = "s3_enabled",
        env = "CELERIANT_S3_ALLOW_HTTP",
        help = "Allow data transmitted in plaintext using http instead of https."
    )]
    pub s3_allow_http: bool,
}

impl ServerConfig {
    pub fn to_sidecar_config(&self, num_shards: u32) -> SidecarConfig {
        SidecarConfig {
            worker_threads: std::cmp::max(2, num_shards as usize / 2),
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
                endpoint: self.s3_endpoint_override.clone(),
                skip_signature: self.s3_skip_signature,
                allow_http: self.s3_allow_http,
            })
        } else {
            None
        };

        celeriant_sidecar::store_config::StoreConfig { s3 }
    }

    pub fn to_shard_config(&self, node_id: u128, num_shards: u32) -> ShardConfig {
        use celeriant_runtimes::ClusterRole;
        ShardConfig {
            node_id,
            num_shards,
            cluster_role: match self.cluster_role {
                ConfigClusterRole::Standalone => ClusterRole::Standalone,
                ConfigClusterRole::Leader => ClusterRole::Leader,
                ConfigClusterRole::Follower => ClusterRole::Follower,
            },
            follower_address: self.follower_address.clone(),
            data_root: self.data_root.clone(),
            listen_address: self.listen_address.clone(),
            client_port: self.client_port,
            replication_port: self.replication_port,
            max_open_files: self.max_open_files,
            read_max_chunk_size: self.read_max_chunk_size,
            write_max_chunk_size: self.write_max_chunk_size,
            max_request_size: self.max_request_size,
            max_response_size: self.max_response_size,
            shard_log_preallocate_bytes: self.shard_log_preallocate_bytes,
            recent_write_cache_bytes: self.recent_write_cache_bytes,
            slow_client_timeout: Duration::from_millis(self.client_connection_timeout_ms),
            max_requested_latency: Duration::from_millis(self.max_requested_latency_ms),
            fsync_delay: Duration::from_micros(self.fsync_delay_us),
            replication_delay: Duration::from_micros(self.replication_delay_us),
            routing_rule: self.routing_rule,
            non_durable_writes: self.non_durable_writes,
            aggregate_client_snapshots_cache_bytes: self.aggregate_client_snapshots_cache_bytes,
            aggregate_snapshots_cache_bytes: self.aggregate_snapshots_cache_bytes,
            timestamp_config: TimestampConfig {
                precision: match self.timestamp_precision {
                    ConfigTimestampPrecision::Milliseconds => TimestampPrecision::Milliseconds,
                    ConfigTimestampPrecision::Microseconds => TimestampPrecision::Microseconds,
                    ConfigTimestampPrecision::Nanoseconds => TimestampPrecision::Nanoseconds,
                },
                epoch_offset_secs: self.timestamp_epoch_offset_secs,
            },
            list_max_duration: Duration::from_millis(self.list_max_duration_ms),
            list_page_size: self.list_page_size as usize,
            list_wal_index_cache_bytes: self.list_wal_index_cache_bytes,
            pending_replication_high_water_bytes: self.pending_replication_high_water_bytes,
            max_cluster_time_drift_ms: self.max_cluster_time_drift_ms,
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
        check_field!(listen_address);
        check_field!(client_port);
        check_field!(replication_port);
        check_field!(mesh_channel_size);
        check_field!(num_shards);
        check_field!(max_open_files);
        check_field!(read_max_chunk_size);
        check_field!(write_max_chunk_size);
        check_field!(recent_write_cache_bytes);
        check_field!(aggregate_client_snapshots_cache_bytes);
        check_field!(aggregate_snapshots_cache_bytes);
        check_field!(max_request_size);
        check_field!(max_response_size);
        check_field!(max_requested_latency_ms);
        check_field!(shard_log_preallocate_bytes);
        check_field!(fsync_delay_us);
        check_field!(replication_delay_us);
        check_field!(non_durable_writes);
        check_field!(log_level);
        check_field!(s3_enabled);
        check_field!(s3_region);
        check_field!(s3_bucket);
        check_field!(s3_access_key_id, sensitive);
        check_field!(s3_secret_access_key, sensitive);
        check_field!(s3_subfolder);
        check_field!(client_connection_timeout_ms);
        check_field!(routing_rule);

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
            listen_address: "0.0.0.0".to_string(),
            client_port: 10000,
            replication_port: 10001,
            cluster_role: ConfigClusterRole::Standalone,
            follower_address: None,
            mesh_channel_size: 1024,
            num_shards: None,
            read_max_chunk_size: 32 * 1024,
            write_max_chunk_size: 32 * 1024,
            max_open_files: 1000,
            max_request_size: 16 * 1024 * 1024,
            max_response_size: 64 * 1024 * 1024,
            max_requested_latency_ms: 2000,
            log_level: "info".to_string(),
            s3_enabled: false,
            s3_region: None,
            s3_bucket: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_subfolder: None,
            shard_log_preallocate_bytes: 1024 * 1024 * 1024,
            fsync_delay_us: 17000,
            replication_delay_us: 17000,
            non_durable_writes: false,
            recent_write_cache_bytes: 512 * 1024 * 1024,
            client_connection_timeout_ms: 30000,
            routing_rule: RoutingRule::AggregateId,
            aggregate_client_snapshots_cache_bytes: 64 * 1024 * 1024,
            aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
            pending_replication_high_water_bytes: 64 * 1024 * 1024,
            max_cluster_time_drift_ms: 5000,
            timestamp_precision: ConfigTimestampPrecision::Milliseconds,
            timestamp_epoch_offset_secs: 0,
            list_max_duration_ms: 2000,
            list_page_size: 20000,
            list_wal_index_cache_bytes: 12 * 1024 * 1024,
            s3_endpoint_override: None,
            s3_skip_signature: false,
            s3_allow_http: false,
        }
    }
}
