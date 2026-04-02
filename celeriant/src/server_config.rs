use celeriant_crypto::pki::{ClientAuthMode, PkiManager};
use celeriant_distributed::s3_lease_config::S3LeaseConfig;
use celeriant_runtimes::RoutingRule;
use celeriant_runtimes::{TlsConfig, TlsMode};
use celeriant_runtimes::{ShardConfig, SidecarConfig};
use celeriant_shard::timestamp_config::{TimestampConfig, TimestampPrecision};
use celeriant_sidecar::s3_config::S3Config;
use clap::Parser;
use std::{path::PathBuf, sync::Arc, time::Duration};
use clap::ValueEnum;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, ValueEnum)]
pub enum ConfigTimestampPrecision {
    #[default]
    Milliseconds,
    Microseconds,
    Nanoseconds,
}


#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, ValueEnum)]
pub enum ConfigCompressionType {
    None,
    Zstd,
    #[default]
    Snappy,
    Brotli,
    Gzip,
}

/// TLS mode for the server listeners.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, ValueEnum)]
pub enum ConfigTlsMode {
    /// Plaintext only (default, backward compatible).
    #[default]
    Disabled,
    /// TLS only; reject plaintext connections.
    Strict,
}

/// Client certificate authentication mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, ValueEnum)]
pub enum ConfigClientAuth {
    /// Clients must present a certificate signed by the CA (full mTLS).
    #[default]
    Require,
    /// Verify cert if presented; allow anonymous clients.
    Optional,
    /// Do not request or verify client certificates.
    None,
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
        env = "CELERIANT_ADVERTISED_REPLICATION_ADDRESS",
        help = "Override the replication address advertised in S3 membership. If not set, defaults to {listen_address}:{replication_port}. Used in testing to route through a TCP proxy."
    )]
    pub advertised_replication_address: Option<String>,

    #[arg(
        long,
        env = "CELERIANT_ADVERTISED_CLIENT_ADDRESS",
        help = "Override the client address advertised in S3 membership and returned in NotLeader errors. If not set, defaults to {listen_address}:{client_port}. Set this when clients connect through a load balancer or reverse proxy."
    )]
    pub advertised_client_address: Option<String>,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        env = "CELERIANT_STANDALONE",
        help = "Run in standalone mode (no replication, no S3 election)"
    )]
    pub standalone: bool,

    #[arg(
        long,
        default_value = "8192",
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

    #[arg(long, default_value_t = false, env = "CELERIANT_RESERVE_COORDINATOR_SHARD", help = "Reserve shard 0 for cluster coordination (heartbeat, schema). Client data routes to shards 1..n-1. Requires num_shards >= 2.")]
    pub reserve_coordinator_shard: bool,

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
        default_value_t = 2000,
        env = "CELERIANT_LIST_PAGE_SIZE",
        help = "Maximum number of entities to collate for listings per page (2000)"
    )]
    pub list_page_size: u64,

    #[arg(
        long,
        default_value_t = 16,
        env = "CELERIANT_LIST_MAX_CONCURRENT",
        help = "Maximum concurrent list operations per shard (16)"
    )]
    pub list_max_concurrent: u64,

    #[arg(
        long,
        default_value_t = 64,
        env = "CELERIANT_READ_MAX_CONCURRENT",
        help = "Maximum concurrent in-flight backwards metablock scans per shard (64)"
    )]
    pub read_max_concurrent: u64,

    #[arg(
        long,
        default_value_t = 16384,
        env = "CELERIANT_MAX_SCHEMA_SIZE_BYTES",
        help = "Maximum size of a single schema definition in bytes (16KB)"
    )]
    pub max_schema_size_bytes: u64,

    #[arg(
        long,
        default_value_t = 30000,
        env = "CELERIANT_CLIENT_CONNECTION_TIMEOUT_MS",
        help = "Maximum time a client has to pull down server messages over tcp (30s)"
    )]
    pub client_connection_timeout_ms: u64,

    #[arg(long, default_value_t = 1024 * 1024 * 1024, env = "CELERIANT_SHARD_LOG_PREALLOCATE_BYTES", help = "Size of each individual log file on disk (1GB)")]
    pub shard_log_preallocate_bytes: u64,

    #[arg(long, env = "CELERIANT_PENDING_REPLICATION_HIGH_WATER_BYTES", hide = true)]
    pub pending_replication_high_water_bytes: Option<u64>,

    #[arg(long, env = "CELERIANT_MAX_CATCHUP_GAP_BYTES", hide = true)]
    pub max_catchup_gap_bytes: Option<u64>,

    #[arg(
        long,
        env = "CELERIANT_INTERNODE_CONNECTION_TIMEOUT_MS",
        help = "Timeout for inter-node TCP connection establishment in milliseconds",
        default_value_t = 5_000
    )]
    pub internode_connection_timeout_ms: u64,

    #[arg(
        long,
        default_value_t = 10_000,
        env = "CELERIANT_INTERNODE_REQUEST_TIMEOUT_MS",
        help = "Timeout for inter-node request/response round-trips in milliseconds (10s)"
    )]
    pub internode_request_timeout_ms: u64,

    #[arg(
        long,
        default_value = "snappy",
        env = "CELERIANT_SERVER_COMPRESSION_ALGORITHM",
        help = "Compression algorithm for server responses: none, zstd, snappy, brotli, gzip"
    )]
    pub server_compression_algorithm: ConfigCompressionType,

    #[arg(
        long,
        env = "CELERIANT_SERVER_COMPRESSION_LEVEL",
        help = "Compression level for zstd, brotli, or gzip (ignored for none/snappy)"
    )]
    pub server_compression_level: Option<i32>,

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

    #[arg(
        long,
        default_value_t = 3,
        env = "CELERIANT_S3_CATCHUP_MAX_ROUNDS",
        help = "Maximum S3 List -> Download rounds performed in shard catchup (3)"
    )]
    pub s3_catchup_max_rounds: u32,

    #[arg(long, default_value_t = 500, env = "CELERIANT_HEARTBEAT_INTERVAL_MS", help = "Interval between leader heartbeats to followers (500ms)")]
    pub heartbeat_interval_ms: u64,

    #[arg(long, env = "CELERIANT_HEARTBEAT_TIMEOUT_MS", help = "Timeout for heartbeat connect+request. Defaults to heartbeat_interval_ms. Set lower than heartbeat_lease_duration_ms.")]
    pub heartbeat_timeout_ms: Option<u64>,

    #[arg(long, default_value_t = 1500, env = "CELERIANT_HEARTBEAT_LEASE_DURATION_MS", help = "Duration before a missed heartbeat is considered a lease expiry (1500ms)")]
    pub heartbeat_lease_duration_ms: u64,

    #[arg(long, default_value_t = 30000, env = "CELERIANT_S3_LEASE_DURATION_MS", help = "S3 lease TTL for leader election, independent of heartbeat timing (30s)")]
    pub s3_lease_duration_ms: u64,

    #[arg(long, default_value_t = 500, env = "CELERIANT_MAX_CLOCK_DRIFT_MS", help = "Allowed clock drift added to heartbeat lease checks (500ms)")]
    pub max_clock_drift_ms: u64,

    #[arg(long, env = "CELERIANT_S3_RETRY_MAX_DURATION_SECS", help = "Maximum total duration (seconds) for retrying S3 operations when S3 is unreachable, with exponential backoff. Unset = retry indefinitely.")]
    pub s3_retry_max_duration_secs: Option<u64>,

    #[arg(
        long,
        default_value = "disabled",
        env = "CELERIANT_TLS_MODE",
        help = "TLS mode: disabled (plaintext only) or strict (TLS only)"
    )]
    pub tls_mode: ConfigTlsMode,

    #[arg(
        long,
        env = "CELERIANT_TLS_CA_CERT",
        help = "Path to CA certificate (PEM, supports concatenated CA bundles)"
    )]
    pub tls_ca_cert: Option<PathBuf>,

    #[arg(
        long,
        env = "CELERIANT_TLS_INTRACLUSTER_CA_CERT",
        help = "Path to intracluster CA certificate (PEM). When set, the replication \
                listener and outbound replication client trust only this CA, while the \
                client listener trusts only --tls-ca-cert. When not set, both listeners \
                use --tls-ca-cert (current behaviour)."
    )]
    pub tls_intracluster_ca_cert: Option<PathBuf>,

    #[arg(
        long,
        env = "CELERIANT_TLS_NODE_CERT",
        help = "Path to node certificate (PEM)"
    )]
    pub tls_node_cert: Option<PathBuf>,

    #[arg(
        long,
        env = "CELERIANT_TLS_NODE_KEY",
        help = "Path to node private key (PEM)"
    )]
    pub tls_node_key: Option<PathBuf>,

    #[arg(
        long,
        env = "CELERIANT_TLS_CLIENT_CERT",
        help = "Path to client-facing server certificate (PEM), signed by the client CA. \
                When set, the client listener presents this cert instead of the node cert, \
                enforcing CA isolation between client and intracluster trust domains."
    )]
    pub tls_client_cert: Option<PathBuf>,

    #[arg(
        long,
        env = "CELERIANT_TLS_CLIENT_KEY",
        help = "Path to client-facing server private key (PEM). Required with --tls-client-cert.",
        requires = "tls_client_cert"
    )]
    pub tls_client_key: Option<PathBuf>,

    #[arg(
        long,
        default_value = "require",
        env = "CELERIANT_TLS_CLIENT_AUTH",
        help = "Client certificate auth: require (mTLS), optional (verify if presented), none (server-auth TLS only)"
    )]
    pub tls_client_auth: ConfigClientAuth,

    #[arg(
        long,
        default_value_t = 0,
        env = "CELERIANT_TLS_CERT_RELOAD_INTERVAL_SECS",
        help = "How often to check TLS cert files for changes and hot-reload (seconds). 0 = disabled."
    )]
    pub tls_cert_reload_interval_secs: u64,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        env = "CELERIANT_REQUIRE_CLIENT_IDENTITY",
        help = "Require clients to send an IdentifyRequest as their first message"
    )]
    pub require_client_identity: bool,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        env = "CELERIANT_INSECURE_ALLOW_PLAINTEXT_AUTH",
        help = "Allow API key auth without TLS (INSECURE - development only)"
    )]
    pub insecure_allow_plaintext_auth: bool,

    #[arg(
        long,
        default_value_t = 7200,
        env = "CELERIANT_COMPACTION_CHECK_INTERVAL_SECS",
        help = "How often to scan for compaction-eligible segments (seconds). Default: 7200 (2 hours)."
    )]
    pub compaction_check_interval_secs: u64,

    #[arg(
        long,
        default_value_t = 0.20,
        env = "CELERIANT_COMPACTION_MIN_RECLAIMABLE_RATIO",
        help = "Minimum fraction of reclaimable bytes in a segment to trigger compaction. Default: 0.20 (20%)."
    )]
    pub compaction_min_reclaimable_ratio: f64,

    #[arg(
        long,
        env = "CELERIANT_COMPACTION_TEMP_DIR",
        help = "Temp directory for in-progress compaction files. Must be on the same filesystem as data_root. Defaults to {shard_dir}/.compaction_tmp/."
    )]
    pub compaction_temp_dir: Option<PathBuf>,

    #[arg(
        long,
        env = "CELERIANT_CACHE_WARMUP_MAX_SECS",
        help = "Maximum time (seconds) to spend warming caches on shard open. Unset = no limit."
    )]
    pub cache_warmup_max_secs: Option<u64>,

    #[arg(
        long,
        default_value_t = 80,
        env = "CELERIANT_MEMORY_CONSUMPTION_PERCENT",
        help = "Percentage of detected available memory to use for caches (1-95, default: 80)"
    )]
    pub memory_consumption_percent: u8,

    #[arg(
        long,
        env = "CELERIANT_MEMORY_BUDGET_BYTES",
        help = "Explicit total memory budget in bytes (overrides system detection)"
    )]
    pub memory_budget_bytes: Option<u64>,

    #[arg(
        long,
        default_value_t = true,
        env = "CELERIANT_METRICS_ENABLED",
        help = "Enable Prometheus metrics and health HTTP endpoint"
    )]
    pub metrics_enabled: bool,

    #[arg(
        long,
        default_value_t = 9090,
        env = "CELERIANT_METRICS_PORT",
        help = "Port for Prometheus /metrics and /health HTTP server"
    )]
    pub metrics_port: u16,
}

impl ServerConfig {
    /// Computes memory budget and validates configuration.
    /// Returns ShardMemoryBudget or error message.
    pub fn compute_memory_budget(&self, num_shards: u32) -> Result<crate::memory_budget::ShardMemoryBudget, String> {
        // Validate memory_consumption_percent
        if self.memory_consumption_percent < 1 || self.memory_consumption_percent > 95 {
            return Err(format!(
                "memory_consumption_percent must be in range 1-95, got {}",
                self.memory_consumption_percent
            ));
        }

        let total_budget = if let Some(explicit_budget) = self.memory_budget_bytes {
            explicit_budget
        } else {
            let detected = crate::memory_budget::detect_available_memory()?;
            (detected as f64 * (self.memory_consumption_percent as f64 / 100.0)) as u64
        };

        let per_shard_budget = total_budget / num_shards as u64;

        // Warn if per-shard budget is very small
        const MIN_RECOMMENDED_BUDGET: u64 = 100 * 1024 * 1024; // 100 MB
        if per_shard_budget < MIN_RECOMMENDED_BUDGET {
            tracing::warn!(
                "Per-shard memory budget is only {} MB (< 100 MB) - caches will be very small",
                per_shard_budget / (1024 * 1024)
            );
        }

        Ok(crate::memory_budget::compute_shard_budgets(total_budget, num_shards))
    }

    pub fn to_sidecar_config(&self, num_shards: u32, node_id: u128) -> SidecarConfig {
        SidecarConfig {
            worker_threads: std::cmp::max(2, num_shards as usize / 2),
            control_lane_capacity: 256,
            data_lane_capacity: 1024,
            metrics_enabled: self.metrics_enabled,
            metrics_port: self.metrics_port,
            num_shards,
            node_id,
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

    /// Build a `TlsConfig` from the TLS CLI fields, or return `None` if TLS is disabled.
    ///
    /// Returns an error if TLS is enabled but required cert paths are missing or invalid.
    pub fn build_tls_config(&self) -> Result<Option<Arc<TlsConfig>>, String> {
        if self.tls_mode == ConfigTlsMode::Disabled {
            return Ok(None);
        }

        let ca_path = self.tls_ca_cert.as_ref()
            .ok_or("--tls-ca-cert is required when TLS is enabled")?;
        let cert_path = self.tls_node_cert.as_ref()
            .ok_or("--tls-node-cert is required when TLS is enabled")?;
        let key_path = self.tls_node_key.as_ref()
            .ok_or("--tls-node-key is required when TLS is enabled")?;

        let client_ca = PkiManager::load_ca_bundle(ca_path)
            .map_err(|e| format!("Failed to load CA bundle from {:?}: {:?}", ca_path, e))?;
        let intracluster_ca = match &self.tls_intracluster_ca_cert {
            Some(path) => PkiManager::load_ca_bundle(path)
                .map_err(|e| format!("Failed to load intracluster CA bundle from {:?}: {:?}", path, e))?,
            None => client_ca.clone(),
        };
        let (node_cert_chain, node_key) = PkiManager::load_identity(cert_path, key_path)
            .map_err(|e| format!("Failed to load node identity from {:?}/{:?}: {:?}", cert_path, key_path, e))?;

        // Client-facing cert: use dedicated client cert if provided, otherwise fall back to node cert.
        let (client_cert_chain, client_key) = match (&self.tls_client_cert, &self.tls_client_key) {
            (Some(cert), Some(key)) => PkiManager::load_identity(cert, key)
                .map_err(|e| format!("Failed to load client-facing identity from {:?}/{:?}: {:?}", cert, key, e))?,
            _ => (node_cert_chain.clone(), node_key.clone_key()),
        };

        let client_auth = match self.tls_client_auth {
            ConfigClientAuth::Require => ClientAuthMode::Require,
            ConfigClientAuth::Optional => ClientAuthMode::Optional,
            ConfigClientAuth::None => ClientAuthMode::None,
        };

        // Client-facing server config trusts the client CA and presents the client-facing cert.
        let mut client_server_config = PkiManager::build_server_config(&client_ca, client_cert_chain, client_key, client_auth)
            .map_err(|e| format!("Failed to build client-facing TLS server config: {:?}", e))?;
        let client_cfg = Arc::get_mut(&mut client_server_config)
            .ok_or("BUG: Arc<ServerConfig> was cloned before secret extraction could be enabled")?;
        client_cfg.enable_secret_extraction = true;
        client_cfg.send_tls13_tickets = 0; // kTLS: tickets desync seq counters

        // Replication server config trusts the intracluster CA. Always requires client auth.
        let mut replication_server_config = PkiManager::build_server_config(&intracluster_ca, node_cert_chain.clone(), node_key.clone_key(), ClientAuthMode::Require)
            .map_err(|e| format!("Failed to build replication TLS server config: {:?}", e))?;
        let repl_cfg = Arc::get_mut(&mut replication_server_config)
            .ok_or("BUG: Arc<ServerConfig> was cloned before secret extraction could be enabled")?;
        repl_cfg.enable_secret_extraction = true;
        repl_cfg.send_tls13_tickets = 0; // kTLS-to-kTLS: tickets desync seq counters

        // Outbound replication client config trusts the intracluster CA.
        let mut replication_client_config = PkiManager::build_client_config(&intracluster_ca, node_cert_chain, node_key)
            .map_err(|e| format!("Failed to build replication TLS client config: {:?}", e))?;
        Arc::get_mut(&mut replication_client_config)
            .ok_or("BUG: Arc<ClientConfig> was cloned before secret extraction could be enabled")?
            .enable_secret_extraction = true;

        let tls_mode = match self.tls_mode {
            ConfigTlsMode::Disabled => TlsMode::Disabled,
            ConfigTlsMode::Strict => TlsMode::Strict,
        };

        Ok(Some(Arc::new(TlsConfig { client_server_config, replication_server_config, replication_client_config, tls_mode })))
    }

    pub fn to_shard_config(
        &self,
        node_id: u128,
        num_shards: u32,
        tls_config: Option<Arc<TlsConfig>>,
        api_keys: Option<crate::api_keys::ApiKeysConfig>,
        memory_budget: crate::memory_budget::ShardMemoryBudget,
    ) -> ShardConfig {
        use celeriant_runtimes::{CompressionType, TlsCertPaths, ApiKeyHashes};
        use celeriant_crypto::pki::ClientAuthMode;

        let replication_config = if self.standalone {
            None
        } else {
            let client_address = self.advertised_client_address.clone()
                .unwrap_or_else(|| format!("{}:{}", self.listen_address, self.client_port));
            let replication_address = self.advertised_replication_address.clone()
                .unwrap_or_else(|| format!("{}:{}", self.listen_address, self.replication_port));
            Some(S3LeaseConfig {
                node_id,
                advertised_client_address: client_address,
                advertised_replication_address: replication_address,
                max_clock_drift: Duration::from_millis(self.max_clock_drift_ms),
                s3_lease_duration: Duration::from_millis(self.s3_lease_duration_ms)
            })
        };

        let api_key_hashes = api_keys.map(|keys| {
            Arc::new(ApiKeyHashes {
                read_write: [keys.primary_rw, keys.secondary_rw],
                read_only: [keys.primary_ro, keys.secondary_ro],
            })
        });

        ShardConfig {
            node_id,
            num_shards,
            s3_download_max_rounds: self.s3_catchup_max_rounds,
            replication_config,
            advertised_replication_address: self.advertised_replication_address.clone(),
            data_root: std::path::absolute(&self.data_root)
                .expect("Failed to resolve data_root to an absolute path"),
            listen_address: self.listen_address.clone(),
            client_port: self.client_port,
            replication_port: self.replication_port,
            max_open_files: self.max_open_files,
            read_max_chunk_size: self.read_max_chunk_size,
            write_max_chunk_size: self.write_max_chunk_size,
            max_request_size: self.max_request_size,
            max_response_size: self.max_response_size,
            shard_log_preallocate_bytes: self.shard_log_preallocate_bytes,
            recent_write_cache_bytes: memory_budget.recent_write_cache_bytes,
            slow_client_timeout: Duration::from_millis(self.client_connection_timeout_ms),
            max_requested_latency: Duration::from_millis(self.max_requested_latency_ms),
            fsync_delay: Duration::from_micros(self.fsync_delay_us),
            replication_delay: Duration::from_micros(self.replication_delay_us),
            routing_rule: self.routing_rule,
            aggregate_client_snapshots_cache_bytes: memory_budget.aggregate_client_snapshots_cache_bytes,
            aggregate_snapshots_cache_bytes: memory_budget.aggregate_snapshots_cache_bytes,
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
            list_max_concurrent: self.list_max_concurrent,
            read_max_concurrent: self.read_max_concurrent,
            list_wal_index_cache_bytes: memory_budget.list_wal_index_cache_bytes,
            schema_cache_bytes: memory_budget.schema_cache_bytes,
            max_schema_size_bytes: self.max_schema_size_bytes,
            pending_replication_high_water_bytes: self.pending_replication_high_water_bytes.unwrap_or(memory_budget.replication_high_water_bytes),
            max_clock_drift_ms: self.max_clock_drift_ms,
            max_catchup_gap_bytes: self.max_catchup_gap_bytes.unwrap_or(memory_budget.max_catchup_gap_bytes),
            internode_connection_timeout: Some(Duration::from_millis(self.internode_connection_timeout_ms)),
            internode_request_timeout: Duration::from_millis(self.internode_request_timeout_ms),
            server_compression_algorithm: match self.server_compression_algorithm {
                ConfigCompressionType::None => CompressionType::None,
                ConfigCompressionType::Zstd => CompressionType::Zstd { level: self.server_compression_level.unwrap_or(6) },
                ConfigCompressionType::Snappy => CompressionType::Snappy,
                ConfigCompressionType::Brotli => CompressionType::Brotli { level: self.server_compression_level.unwrap_or(6) },
                ConfigCompressionType::Gzip => CompressionType::Gzip { level: self.server_compression_level.unwrap_or(6) },
            },
            tls_config,
            tls_cert_paths: if self.tls_cert_reload_interval_secs > 0 {
                if let (Some(ca), Some(cert), Some(key)) = (
                    self.tls_ca_cert.clone(),
                    self.tls_node_cert.clone(),
                    self.tls_node_key.clone(),
                ) {
                    Some(TlsCertPaths {
                        ca_cert: ca,
                        intracluster_ca_cert: self.tls_intracluster_ca_cert.clone(),
                        node_cert: cert,
                        node_key: key,
                        client_cert: self.tls_client_cert.clone(),
                        client_key: self.tls_client_key.clone(),
                    })
                } else {
                    tracing::warn!(
                        "tls_cert_reload_interval_secs is set but cert paths are incomplete \
                         (tls_ca_cert, tls_node_cert, tls_node_key); cert reload will not be enabled"
                    );
                    None
                }
            } else {
                None
            },
            tls_client_auth: match self.tls_client_auth {
                ConfigClientAuth::Require => ClientAuthMode::Require,
                ConfigClientAuth::Optional => ClientAuthMode::Optional,
                ConfigClientAuth::None => ClientAuthMode::None,
            },
            tls_cert_reload_interval: std::time::Duration::from_secs(self.tls_cert_reload_interval_secs),
            require_client_identity: self.require_client_identity,
            api_key_hashes: std::cell::RefCell::new(api_key_hashes),
            compaction_check_interval: Duration::from_secs(self.compaction_check_interval_secs),
            compaction_min_reclaimable_ratio: self.compaction_min_reclaimable_ratio,
            compaction_temp_dir: self.compaction_temp_dir.clone(),
            s3_retry_max_duration: self.s3_retry_max_duration_secs.map(Duration::from_secs),
            cache_warmup_max_duration: self.cache_warmup_max_secs.map(Duration::from_secs),
            heartbeat_interval_duration: Duration::from_millis(self.heartbeat_interval_ms),
            heartbeat_timeout: Duration::from_millis(self.heartbeat_timeout_ms.unwrap_or(self.heartbeat_interval_ms)),
            heartbeat_lease_duration: Duration::from_millis(self.heartbeat_lease_duration_ms),
            reserve_coordinator_shard: self.reserve_coordinator_shard,
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

        check_field!(timestamp_precision);
        check_field!(timestamp_epoch_offset_secs);
        check_field!(data_root);
        check_field!(listen_address);
        check_field!(client_port);
        check_field!(replication_port);
        check_field!(advertised_replication_address);
        check_field!(standalone);
        check_field!(mesh_channel_size);
        check_field!(num_shards);
        check_field!(reserve_coordinator_shard);
        check_field!(max_open_files);
        check_field!(read_max_chunk_size);
        check_field!(write_max_chunk_size);
        check_field!(routing_rule);
        check_field!(max_request_size);
        check_field!(max_response_size);
        check_field!(max_requested_latency_ms);
        check_field!(list_max_duration_ms);
        check_field!(list_page_size);
        check_field!(list_max_concurrent);
        check_field!(read_max_concurrent);
        check_field!(max_schema_size_bytes);
        check_field!(client_connection_timeout_ms);
        check_field!(shard_log_preallocate_bytes);
        check_field!(internode_connection_timeout_ms);
        check_field!(internode_request_timeout_ms);
        check_field!(server_compression_algorithm);
        check_field!(server_compression_level);
        check_field!(fsync_delay_us);
        check_field!(replication_delay_us);
        check_field!(log_level);
        check_field!(s3_enabled);
        check_field!(s3_region);
        check_field!(s3_bucket);
        check_field!(s3_access_key_id, sensitive);
        check_field!(s3_secret_access_key, sensitive);
        check_field!(s3_subfolder);
        check_field!(s3_endpoint_override);
        check_field!(s3_skip_signature);
        check_field!(s3_allow_http);
        check_field!(s3_catchup_max_rounds);
        check_field!(heartbeat_interval_ms);
        check_field!(heartbeat_lease_duration_ms);
        check_field!(s3_lease_duration_ms);
        check_field!(max_clock_drift_ms);
        check_field!(tls_mode);
        check_field!(tls_ca_cert);
        check_field!(tls_intracluster_ca_cert);
        check_field!(tls_node_cert);
        check_field!(tls_node_key);
        check_field!(tls_client_cert);
        check_field!(tls_client_key);
        check_field!(tls_client_auth);
        check_field!(tls_cert_reload_interval_secs);
        check_field!(require_client_identity);
        check_field!(insecure_allow_plaintext_auth);
        check_field!(compaction_check_interval_secs);
        check_field!(compaction_min_reclaimable_ratio);
        check_field!(compaction_temp_dir);
        check_field!(s3_retry_max_duration_secs);
        check_field!(cache_warmup_max_secs);
        check_field!(memory_consumption_percent);
        check_field!(memory_budget_bytes);
        check_field!(metrics_enabled);
        check_field!(metrics_port);

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
            advertised_replication_address: None,
            advertised_client_address: None,
            standalone: false,
            mesh_channel_size: 8192,
            num_shards: None,
            reserve_coordinator_shard: false,
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
            pending_replication_high_water_bytes: None,
            max_catchup_gap_bytes: None,
            fsync_delay_us: 17000,
            replication_delay_us: 17000,
            client_connection_timeout_ms: 30000,
            routing_rule: RoutingRule::AggregateId,
            timestamp_precision: ConfigTimestampPrecision::Milliseconds,
            timestamp_epoch_offset_secs: 0,
            list_max_duration_ms: 2000,
            list_page_size: 2000,
            list_max_concurrent: 16,
            read_max_concurrent: 64,
            max_schema_size_bytes: 16384,
            s3_endpoint_override: None,
            s3_skip_signature: false,
            s3_allow_http: false,
            internode_connection_timeout_ms: 5_000,
            internode_request_timeout_ms: 10_000,
            server_compression_algorithm: ConfigCompressionType::Snappy,
            server_compression_level: None,
            s3_catchup_max_rounds: 3,
            heartbeat_interval_ms: 500,
            heartbeat_timeout_ms: None,
            heartbeat_lease_duration_ms: 1500,
            s3_lease_duration_ms: 30000,
            max_clock_drift_ms: 500,
            tls_mode: ConfigTlsMode::Disabled,
            tls_ca_cert: None,
            tls_intracluster_ca_cert: None,
            tls_node_cert: None,
            tls_node_key: None,
            tls_client_cert: None,
            tls_client_key: None,
            tls_client_auth: ConfigClientAuth::Require,
            tls_cert_reload_interval_secs: 0,
            require_client_identity: false,
            insecure_allow_plaintext_auth: false,
            compaction_check_interval_secs: 7200,
            compaction_min_reclaimable_ratio: 0.20,
            compaction_temp_dir: None,
            s3_retry_max_duration_secs: None,
            cache_warmup_max_secs: None,
            memory_consumption_percent: 80,
            memory_budget_bytes: None,
            metrics_enabled: true,
            metrics_port: 9090,
        }
    }
}
