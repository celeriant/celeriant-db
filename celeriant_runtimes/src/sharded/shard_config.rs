use std::{cell::RefCell, path::PathBuf, sync::Arc, time::Duration};

use celeriant_crypto::pki::ClientAuthMode;
use celeriant_msg::response::responses::AccessLevel;
use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_distributed::s3_lease_config::S3LeaseConfig;
use celeriant_wal::compression_type::CompressionType;

use crate::sharded::routing_rule::RoutingRule;
use crate::sharded::tls_config::TlsConfig;

/// SHA-256 hashes of the 4 API keys (primary_rw, secondary_rw, primary_ro, secondary_ro)
#[derive(Debug, Clone)]
pub struct ApiKeyHashes {
    pub read_write: [[u8; 32]; 2],
    pub read_only: [[u8; 32]; 2],
}

impl ApiKeyHashes {
    /// Check if a key hash matches any configured key, returning the access level if found
    pub fn validate(&self, key_hash: &[u8; 32]) -> Option<AccessLevel> {
        use celeriant_crypto::constant_time_compare;

        for rw_hash in &self.read_write {
            if constant_time_compare(key_hash, rw_hash) {
                return Some(AccessLevel::ReadWrite);
            }
        }

        for ro_hash in &self.read_only {
            if constant_time_compare(key_hash, ro_hash) {
                return Some(AccessLevel::ReadOnly);
            }
        }

        None
    }
}

/// File paths for TLS certificate hot-reload.
#[derive(Clone, Debug)]
pub struct TlsCertPaths {
    pub ca_cert: PathBuf,
    pub intracluster_ca_cert: Option<PathBuf>,
    pub node_cert: PathBuf,
    pub node_key: PathBuf,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
}

/// Per-node configuration that gets cloned to each shard.
/// Does not include shard_id since that's determined at runtime.
#[derive(Clone, Debug)]
pub struct ShardConfig {
    pub node_id: u128,
    pub num_shards: u32,
    pub replication_config: Option<S3LeaseConfig>,
    pub heartbeat_lease_duration: Duration,
    pub heartbeat_interval_duration: Duration,
    pub heartbeat_timeout: Duration,
    pub advertised_replication_address: Option<String>,
    pub data_root: PathBuf,
    pub listen_address: String,
    pub client_port: u16,
    pub replication_port: u16,
    pub max_open_files: u64,
    pub read_max_chunk_size: u64,
    pub write_max_chunk_size: u64,
    pub max_request_size: u64,
    pub max_response_size: u64,
    pub internode_connection_timeout: Option<Duration>,
    pub internode_request_timeout: Duration,
    pub server_compression_algorithm: CompressionType,
    pub slow_client_timeout: Duration,
    pub max_requested_latency: Duration,
    pub shard_log_preallocate_bytes: u64,
    pub fsync_delay: Duration,
    pub s3_download_max_rounds: u32,
    pub replication_delay: Duration,
    pub recent_write_cache_bytes: u64,
    pub routing_rule: RoutingRule,
    pub aggregate_client_snapshots_cache_bytes: u64,
    pub aggregate_snapshots_cache_bytes: u64,
    pub timestamp_config: TimestampConfig,
    pub list_max_duration: Duration,
    pub list_page_size: usize,
    pub list_max_concurrent: u64,
    pub read_max_concurrent: u64,
    pub list_wal_index_cache_bytes: u64,
    pub schema_cache_bytes: u64,
    pub max_schema_size_bytes: u64,
    pub pending_replication_high_water_bytes: u64,
    pub max_clock_drift_ms: u64,
    pub max_catchup_gap_bytes: u64,
    pub tls_config: Option<Arc<TlsConfig>>,
    pub tls_cert_paths: Option<TlsCertPaths>,
    pub tls_client_auth: ClientAuthMode,
    pub tls_cert_reload_interval: std::time::Duration,
    pub require_client_identity: bool,
    pub api_key_hashes: RefCell<Option<Arc<ApiKeyHashes>>>,
    pub compaction_check_interval: Duration,
    pub compaction_min_reclaimable_ratio: f64,
    pub compaction_temp_dir: Option<std::path::PathBuf>,
    pub s3_retry_max_duration: Option<Duration>,
    pub cache_warmup_max_duration: Option<Duration>,
}