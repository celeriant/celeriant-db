//! Shared test utilities for celeriant_integration_tests integration tests.

pub mod registry;

pub mod api_key_test;
pub mod batch;
pub mod bug_kick_after_restart;
pub mod batch_standalone_cleartext;
pub mod chaos;
pub mod chaos_delete;
pub mod compaction_replicated;
pub mod compaction_restart;
pub mod compaction_standalone;
pub mod connection_test;
pub mod debug_follower_pressure;
pub mod stale_lease_restart_split_brain;
pub mod edge_concurrent_heartbeat_replication_s3;
pub mod edge_corrupted_s3_batch;
pub mod edge_s3_batch_boundary_contiguity;
pub mod edge_s3_catchup_after_partition;
pub mod edge_s3_overlap_after_partition;
pub mod edge_empty_replication_batch;
pub mod edge_heartbeat_lock_contention;
pub mod edge_wal_divergence_and_recovery;
pub mod edge_list_pagination_cache_eviction;
pub mod edge_log_eviction_before_s3;
pub mod edge_log_rotation_mid_replication;
pub mod edge_s3_batch_ordering;
pub mod edge_s3_missing_batches;
pub mod edge_split_brain_s3_unavailable;
pub mod edge_stale_cache_rotation;
pub mod edge_wal_tip_hash_divergence;
pub mod follower_read_snapshot;
pub mod identity_test;
pub mod invariant_clock_drift_rejection;
pub mod invariant_concurrent_write;
pub mod invariant_occ_before_idempotency;
pub mod invariant_read_count;
pub mod invariant_replication_convergence;
pub mod invariant_replication_queue_pressure;
pub mod invariant_s3_fallback_dedup;
pub mod leader_read_visibility;
pub mod metamorphic_common;
pub mod metamorphic_divergence_recovery_parity;
pub mod metamorphic_follower_crash_catchup_parity;
pub mod metamorphic_leader_follower_parity;
pub mod metamorphic_post_failover_parity;
pub mod metamorphic_standalone_vs_cluster;
pub mod mtls_test;
pub mod multi_shard_watch_test;
pub mod p1_1_dcb_rollback;
pub mod p1_2_concurrent_dcb;
pub mod p1_3_cross_shard_rejection;
pub mod p1_4_exactly_once;
pub mod p1_6_ordering_verification;
pub mod p1_7_multitenancy_isolation;
pub mod p2_1_write_survival;
pub mod p2_5_blackout_acked_writes_survive;
pub mod p2_2_dual_restart;
pub mod p2_3_wal_corruption;
pub mod p2_4_s3_capacity;
pub mod p3_1_cold_read_latency;
pub mod p3_2_bloom_filter;
pub mod p3_3_sequential_cold_reads;
pub mod p4_1_rolling_upgrade;
pub mod pool_test;
pub mod read_list_benchmark;
pub mod rpi_cluster_bench;
pub mod rpi_cluster_pool_bench;
pub mod s3_concurrent_cas;
pub mod s3_degraded_segment_summaries;
pub mod s3_election;
pub mod s3_failover_and_recovery;
pub mod s3_failover_latency;
pub mod s3_fallback_catchup;
pub mod s3_fallback_createonly;
pub mod s3_fallback_s3_down;
pub mod s3_fencing_writes;
pub mod s3_follower_crash;
pub mod s3_follower_kick;
pub mod s3_leader_solo;
pub mod s3_lease_monotonicity;
pub mod s3_lease_renewal_backoff;
pub mod s3_stale_lease;
pub mod s3_unreachable_failover;
pub mod schema_bank_bench;
pub mod schema_follower_crash;
pub mod schema_old_leader_recovery;
pub mod schema_validation;
pub mod schema_zero_cache;
pub mod segment_summary_correctness;
pub mod single;
pub mod standalone_to_distributed;
pub mod typed_operations;
pub mod watch_test;

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::Notify;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::read_filters::ReadFilters,
    request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest},
    response::aggregate_event_batch::AggregateEventBatch,
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
pub use celeriant_lib::server_config::ServerConfig;
pub use celeriant_runtimes::RoutingRule;
use std::path::PathBuf;

use celeriant_client_tokio::ClientTlsConfig;
use celeriant_crypto::pki::PkiManager;
use celeriant_lib::server_config::ConfigTlsMode;
use rustls_pki_types::ServerName;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::time::sleep;

/// A self-managed test server instance with automatic cleanup.
///
/// Creates a temporary data directory, spawns the server as a subprocess,
/// and automatically cleans up when dropped.
///
/// # Example
///
/// ```no_run
/// use celeriant_integration_tests::{TestServer, ServerConfig};
///
/// #[tokio::main]
/// async fn main() {
///     // Start with defaults (1 shard, non-durable writes, warn log level)
///     let server = TestServer::start().await.unwrap();
///     println!("Server running at {}", server.address());
///
///     // Or with custom config
///     let config = ServerConfig {
///         num_shards: Some(4),
///         log_level: "debug".to_string(),
///         ..Default::default()
///     };
///     let server = TestServer::start_with_config(10200, config).await.unwrap();
/// }
/// ```
pub struct TestServer {
    _temp_dir: TempDir,
    address: String,
    child: Child,
    config: ServerConfig,
    label: String,
    _log_thread: Option<JoinHandle<()>>,
}

impl TestServer {
    /// Resolve the path to the pre-built server binary.
    /// Uses the release binary next to the running test binary, falling back to cargo run.
    fn server_binary_path() -> std::path::PathBuf {
        let mut path = std::env::current_exe().unwrap();
        path.pop(); // remove test binary name
        // In release mode, binary is in target/release/
        // In debug mode with deps, go up from target/debug/deps/ to target/debug/
        if path.ends_with("deps") {
            path.pop();
        }
        path.push("celeriant");
        if path.exists() {
            path
        } else {
            // Fallback: search workspace target directory
            let workspace = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("target/release/celeriant");
            if workspace.exists() {
                workspace
            } else {
                panic!(
                    "Server binary not found. Build it first: cargo build --release -p celeriant"
                );
            }
        }
    }

    /// Start a new test server with sensible test defaults.
    ///
    /// Uses a unique port based on the process ID to avoid conflicts.
    /// Default config: 1 shard, non-durable writes, warn log level.
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let port = 10100 + (std::process::id() % 100) as u16;
        Self::start_with_port(port).await
    }

    /// Start a new test server on a specific port with sensible test defaults.
    pub async fn start_with_port(port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let config = ServerConfig {
            num_shards: Some(1),
            log_level: "warn".to_string(),
            standalone: true,
            ..Default::default()
        };
        Self::start_with_config(port, config).await
    }

    /// Start a new test server with custom configuration.
    ///
    /// The `data_root`, `client_port`, and `replication_port` fields in the config
    /// will be overridden - data_root uses a temp directory, and ports use the
    /// provided port parameter.
    pub async fn start_with_config(
        port: u16,
        config: ServerConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let label = format!("server:{}", port);
        Self::start_with_config_labeled(port, config, label).await
    }

    /// Start a new test server with custom configuration and a label for log output.
    ///
    /// Server stderr is captured and printed line-by-line prefixed with `[{label}]`.
    ///
    /// BEST PRACTICE: New integration tests should use this labeled variant for better
    /// log readability in multi-node scenarios. Unlabeled variants exist for backward
    /// compatibility with existing tests.
    pub async fn start_with_config_labeled(
        port: u16,
        mut config: ServerConfig,
        label: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let data_root = temp_dir.path().to_path_buf();
        let address = format!("127.0.0.1:{}", port);

        // Override paths and ports with test-specific values
        config.data_root = data_root.clone();
        config.client_port = port;
        config.replication_port = port + 1;
        // Tests run multiple servers on one host; the default metrics_port (9090) collides.
        // Only derive a unique port when the caller hasn't set one explicitly.
        if config.metrics_port == 9090 {
            config.metrics_port = port + 2;
        }

        println!("  Starting test server on port {}...", port);
        println!("  Data directory: {:?}", data_root);

        let args = config.to_cli_args();

        println!("  Args: {:?}", args);

        let server_bin = Self::server_binary_path();
        let mut child = Command::new(&server_bin)
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let log_thread = spawn_log_reader(&label, &mut child);

        let ready_start = std::time::Instant::now();
        Self::poll_ready(&address, &mut child, &config).await?;
        println!("  Server is ready (took {:?})", ready_start.elapsed());
        Ok(Self {
            _temp_dir: temp_dir,
            address,
            child,
            config,
            label,
            _log_thread: log_thread,
        })
    }

    /// Get the server address (host:port).
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Get just the port number.
    pub fn port(&self) -> u16 {
        self.config.client_port
    }

    /// Get the server configuration.
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// Check if the server process is still running.
    /// Returns Ok(()) if alive, or Err with exit status if the process exited.
    pub fn check_alive(&mut self) -> Result<(), String> {
        match self.child.try_wait() {
            Ok(Some(status)) => Err(format!(
                "[{}] Server process exited unexpectedly: {}",
                self.label, status
            )),
            Ok(None) => Ok(()),
            Err(e) => Err(format!(
                "[{}] Failed to check server process status: {}",
                self.label, e
            )),
        }
    }

    /// Stop the server process (can be restarted later).
    pub fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        println!("  Server stopped on port {}", self.config.client_port);
    }

    /// Restart the server process after stopping.
    pub async fn restart(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let args = self.config.to_cli_args();

        let server_bin = Self::server_binary_path();
        self.child = Command::new(&server_bin)
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        self._log_thread = spawn_log_reader(&self.label, &mut self.child);

        let ready_start = std::time::Instant::now();
        Self::poll_ready(&self.address, &mut self.child, &self.config).await?;
        println!("  Server restarted on port {} (took {:?})", self.config.client_port, ready_start.elapsed());
        Ok(())
    }

    /// Restart the server with a new configuration.
    ///
    /// Preserves the existing data_root, client_port, and replication_port
    /// (the data directory is owned by _temp_dir and must not change).
    /// Use this to change a server's mode (e.g. standalone -> distributed)
    /// while keeping the same data on disk.
    pub async fn restart_with_config(
        &mut self,
        mut config: ServerConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        config.data_root = self.config.data_root.clone();
        config.client_port = self.config.client_port;
        config.replication_port = self.config.replication_port;
        self.config = config;

        let args = self.config.to_cli_args();
        let server_bin = Self::server_binary_path();
        self.child = Command::new(&server_bin)
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        self._log_thread = spawn_log_reader(&self.label, &mut self.child);

        let ready_start = std::time::Instant::now();
        Self::poll_ready(&self.address, &mut self.child, &self.config).await?;
        println!(
            "  Server restarted with new config on port {} (took {:?})",
            self.config.client_port,
            ready_start.elapsed()
        );
        Ok(())
    }

    /// Start a test server using a caller-provided TempDir.
    ///
    /// Like `start_with_config_labeled` but uses an existing data directory
    /// instead of creating a fresh one. The TempDir ownership transfers to
    /// TestServer for cleanup on drop. Use for tests that pre-populate data
    /// directories (e.g. copying WAL files from another node).
    pub async fn start_with_existing_dir(
        port: u16,
        mut config: ServerConfig,
        label: String,
        temp_dir: TempDir,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let data_root = temp_dir.path().to_path_buf();
        let address = format!("127.0.0.1:{}", port);

        config.data_root = data_root.clone();
        config.client_port = port;
        config.replication_port = port + 1;

        println!("  Starting test server on port {} (existing dir)...", port);
        println!("  Data directory: {:?}", data_root);

        let args = config.to_cli_args();
        println!("  Args: {:?}", args);

        let server_bin = Self::server_binary_path();
        let mut child = Command::new(&server_bin)
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let log_thread = spawn_log_reader(&label, &mut child);

        let ready_start = std::time::Instant::now();
        Self::poll_ready(&address, &mut child, &config).await?;
        println!("  Server is ready (took {:?})", ready_start.elapsed());
        Ok(Self {
            _temp_dir: temp_dir,
            address,
            child,
            config,
            label,
            _log_thread: log_thread,
        })
    }

    /// Poll until the server is accepting connections.
    ///
    /// For plaintext servers, uses a bare TCP connect probe.
    /// For TLS-strict servers, avoids TCP probing (which triggers spurious
    /// "TLS handshake failed" errors) and instead waits for the process to
    /// start listening by checking process liveness + a port probe via /proc/net.
    async fn poll_ready(
        address: &str,
        child: &mut Child,
        config: &ServerConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();
        let max_wait = Duration::from_secs(30);
        let tls_strict = matches!(config.tls_mode, ConfigTlsMode::Strict);

        while start.elapsed() < max_wait {
            if let Ok(Some(status)) = child.try_wait() {
                return Err(format!("Server exited during startup: {}", status).into());
            }

            if tls_strict {
                // For TLS servers, probe whether the port is in LISTEN state
                // without completing a TCP handshake (which would cause the server
                // to start a TLS handshake we can't complete without client certs).
                if port_is_listening(config.client_port) {
                    return Ok(());
                }
                sleep(Duration::from_millis(100)).await;
            } else {
                match TcpStream::connect(address).await {
                    Ok(_) => return Ok(()),
                    Err(_) => sleep(Duration::from_millis(100)).await,
                }
            }
        }

        Err("Server failed to start within timeout".into())
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Kill the server process
        let _ = self.child.kill();
        let _ = self.child.wait();
        println!("  Test server shut down");
    }
}

/// Check if a port is in LISTEN state by probing /proc/net/tcp.
/// This avoids completing a TCP handshake, which is important for TLS servers
/// where a bare TCP connect + drop triggers spurious TLS errors.
fn port_is_listening(port: u16) -> bool {
    let hex_port = format!("{:04X}", port);
    let Ok(contents) = std::fs::read_to_string("/proc/net/tcp") else {
        return false;
    };
    // /proc/net/tcp format: local_address (hex IP:PORT) state (0A = LISTEN)
    contents.lines().skip(1).any(|line| {
        let parts: Vec<&str> = line.split_whitespace().collect();
        parts.len() >= 4
            && parts[1].ends_with(&format!(":{}", hex_port))
            && parts[3] == "0A"
    })
}

/// Take stdout from a child process and spawn a thread that prints each line with a label prefix.
/// The server uses `tracing_subscriber::fmt().init()` which writes to stdout, not stderr.
/// Returns `None` if stdout is not available (already taken or not piped).
fn spawn_log_reader(label: &str, child: &mut Child) -> Option<JoinHandle<()>> {
    let stdout = child.stdout.take()?;
    let label = label.to_string();
    Some(std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            match line {
                Ok(line) => eprintln!("  [{}] {}", label, line),
                Err(_) => break,
            }
        }
    }))
}

/// Extension trait for ServerConfig to convert to CLI arguments.
pub trait ServerConfigExt {
    fn to_cli_args(&self) -> Vec<String>;
}

impl ServerConfigExt for ServerConfig {
    /// Convert the config to CLI arguments for spawning the server.
    fn to_cli_args(&self) -> Vec<String> {
        let mut args = Vec::new();

        args.push("--data-root".to_string());
        args.push(self.data_root.to_str().unwrap().to_string());

        args.push("--listen-address".to_string());
        args.push(self.listen_address.clone());

        args.push("--client-port".to_string());
        args.push(self.client_port.to_string());

        args.push("--replication-port".to_string());
        args.push(self.replication_port.to_string());

        if let Some(num_shards) = self.num_shards {
            args.push("--num-shards".to_string());
            args.push(num_shards.to_string());
        }

        args.push("--routing-rule".to_string());
        args.push(self.routing_rule.to_string());

        args.push("--mesh-channel-size".to_string());
        args.push(self.mesh_channel_size.to_string());

        args.push("--max-open-files".to_string());
        args.push(self.max_open_files.to_string());

        args.push("--read-max-chunk-size".to_string());
        args.push(self.read_max_chunk_size.to_string());

        args.push("--write-max-chunk-size".to_string());
        args.push(self.write_max_chunk_size.to_string());

        args.push("--max-request-size".to_string());
        args.push(self.max_request_size.to_string());

        args.push("--internode-max-request-size".to_string());
        args.push(self.internode_max_request_size.to_string());

        args.push("--max-response-size".to_string());
        args.push(self.max_response_size.to_string());

        args.push("--max-requested-latency-ms".to_string());
        args.push(self.max_requested_latency_ms.to_string());

        args.push("--client-connection-timeout-ms".to_string());
        args.push(self.client_connection_timeout_ms.to_string());

        args.push("--shard-log-preallocate-bytes".to_string());
        args.push(self.shard_log_preallocate_bytes.to_string());

        args.push("--fsync-delay-us".to_string());
        args.push(self.fsync_delay_us.to_string());

        args.push("--log-level".to_string());
        args.push(self.log_level.clone());

        args.push("--list-max-duration-ms".to_string());
        args.push(self.list_max_duration_ms.to_string());

        args.push("--list-page-size".to_string());
        args.push(self.list_page_size.to_string());

        args.push("--max-schema-size-bytes".to_string());
        args.push(self.max_schema_size_bytes.to_string());

        args.push("--memory-consumption-percent".to_string());
        args.push(self.memory_consumption_percent.to_string());

        if let Some(budget) = self.memory_budget_bytes {
            args.push("--memory-budget-bytes".to_string());
            args.push(budget.to_string());
        }

        // Cluster mode configuration
        if self.standalone {
            args.push("--standalone".to_string());
        }

        if let Some(addr) = &self.advertised_replication_address {
            args.push("--advertised-replication-address".to_string());
            args.push(addr.clone());
        }

        // S3 configuration (only if enabled)
        if self.s3_enabled {
            args.push("--s3-enabled".to_string());

            if let Some(region) = &self.s3_region {
                args.push("--s3-region".to_string());
                args.push(region.to_string());
            }

            if let Some(bucket) = &self.s3_bucket {
                args.push("--s3-bucket".to_string());
                args.push(bucket.to_string());
            }

            if let Some(access_key) = &self.s3_access_key_id {
                args.push("--s3-access-key-id".to_string());
                args.push(access_key.to_string());
            }

            if let Some(secret_key) = &self.s3_secret_access_key {
                args.push("--s3-secret-access-key".to_string());
                args.push(secret_key.to_string());
            }

            if let Some(subfolder) = &self.s3_subfolder {
                args.push("--s3-subfolder".to_string());
                args.push(subfolder.to_string());
            }

            if let Some(endpoint) = &self.s3_endpoint_override {
                args.push("--s3-endpoint-override".to_string());
                args.push(endpoint.to_string());
            }

            if self.s3_skip_signature {
                args.push("--s3-skip-signature".to_string());
            }

            if self.s3_allow_http {
                args.push("--s3-allow-http".to_string());
            }

        }

        if let Some(v) = self.max_catchup_gap_bytes {
            args.push("--max-catchup-gap-bytes".to_string());
            args.push(v.to_string());
        }

        args.push("--internode-connection-timeout-ms".to_string());
        args.push(self.internode_connection_timeout_ms.to_string());

        args.push("--internode-request-timeout-ms".to_string());
        args.push(self.internode_request_timeout_ms.to_string());

        args.push("--replication-delay-us".to_string());
        args.push(self.replication_delay_us.to_string());

        args.push("--heartbeat-interval-ms".to_string());
        args.push(self.heartbeat_interval_ms.to_string());

        args.push("--heartbeat-lease-duration-ms".to_string());
        args.push(self.heartbeat_lease_duration_ms.to_string());

        args.push("--s3-lease-duration-ms".to_string());
        args.push(self.s3_lease_duration_ms.to_string());

        args.push("--max-clock-drift-ms".to_string());
        args.push(self.max_clock_drift_ms.to_string());

        // TLS configuration (only emit non-default fields; default is disabled)
        args.push("--tls-mode".to_string());
        args.push(match self.tls_mode {
            celeriant_lib::server_config::ConfigTlsMode::Disabled => "disabled",
            celeriant_lib::server_config::ConfigTlsMode::Strict => "strict",
        }.to_string());

        if let Some(ca_cert) = &self.tls_ca_cert {
            args.push("--tls-ca-cert".to_string());
            args.push(ca_cert.to_str().unwrap().to_string());
        }

        if let Some(intracluster_ca_cert) = &self.tls_intracluster_ca_cert {
            args.push("--tls-intracluster-ca-cert".to_string());
            args.push(intracluster_ca_cert.to_str().unwrap().to_string());
        }

        if let Some(node_cert) = &self.tls_node_cert {
            args.push("--tls-node-cert".to_string());
            args.push(node_cert.to_str().unwrap().to_string());
        }

        if let Some(node_key) = &self.tls_node_key {
            args.push("--tls-node-key".to_string());
            args.push(node_key.to_str().unwrap().to_string());
        }

        args.push("--tls-client-auth".to_string());
        args.push(match self.tls_client_auth {
            celeriant_lib::server_config::ConfigClientAuth::Require => "require",
            celeriant_lib::server_config::ConfigClientAuth::Optional => "optional",
            celeriant_lib::server_config::ConfigClientAuth::None => "none",
        }.to_string());

        if self.tls_cert_reload_interval_secs > 0 {
            args.push("--tls-cert-reload-interval-secs".to_string());
            args.push(self.tls_cert_reload_interval_secs.to_string());
        }

        if self.require_client_identity {
            args.push("--require-client-identity".to_string());
        }

        if self.insecure_allow_plaintext_auth {
            args.push("--insecure-allow-plaintext-auth".to_string());
        }

        args.push("--compaction-check-interval-secs".to_string());
        args.push(self.compaction_check_interval_secs.to_string());

        args.push("--compaction-min-reclaimable-ratio".to_string());
        args.push(self.compaction_min_reclaimable_ratio.to_string());

        args.push("--metrics-port".to_string());
        args.push(self.metrics_port.to_string());

        args
    }
}

/// A self-managed MinIO container for S3 integration tests.
///
/// Starts a MinIO Docker container, creates the test bucket, and provides
/// methods for verifying S3 uploads. Automatically cleans up on drop.
pub struct MinioContainer {
    port: u16,
    container_name: String,
    bucket_name: String,
}

impl MinioContainer {
    /// Start a MinIO container on the given port with the default bucket name.
    ///
    /// Waits for MinIO to accept connections and creates the test bucket.
    /// Uses port allocation offset +10 from base to avoid collision with server ports.
    /// Default bucket name: "test-fallback"
    pub async fn start(port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_bucket(port, "test-fallback").await
    }

    /// Start a MinIO container on the given port with a custom bucket name.
    ///
    /// Waits for MinIO to accept connections and creates the test bucket.
    /// Uses port allocation offset +10 from base to avoid collision with server ports.
    pub async fn start_with_bucket(port: u16, bucket_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let container_name = format!("celeriant-test-minio-{}", port);

        println!("  Starting MinIO container {} on port {}...", container_name, port);

        // Remove any leftover container from a previous run
        let _ = Command::new("docker")
            .args(["rm", "-f", &container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        // Start MinIO container
        let status = Command::new("docker")
            .args([
                "run", "-d",
                "--name", &container_name,
                "-p", &format!("{}:9000", port),
                "-e", "MINIO_ROOT_USER=minioadmin",
                "-e", "MINIO_ROOT_PASSWORD=minioadmin",
                "minio/minio",
                "server", "/data",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;

        if !status.success() {
            return Err(format!("Failed to start MinIO container: exit code {:?}", status.code()).into());
        }

        // Wait for MinIO to be ready by polling health endpoint
        let client = reqwest::Client::new();
        let health_url = format!("http://127.0.0.1:{}/minio/health/live", port);
        let start = std::time::Instant::now();
        let max_wait = Duration::from_secs(30);

        while start.elapsed() < max_wait {
            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    println!("  MinIO is ready (took {:?})", start.elapsed());
                    break;
                }
                _ => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }

        if start.elapsed() >= max_wait {
            let _ = Command::new("docker")
                .args(["rm", "-f", &container_name])
                .status();
            return Err("MinIO failed to start within timeout".into());
        }

        // Create test bucket via docker exec (MinIO standalone uses dirs under /data)
        let bucket_path = format!("/data/{}", bucket_name);
        let bucket_status = Command::new("docker")
            .args(["exec", &container_name, "mkdir", "-p", &bucket_path])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;

        if !bucket_status.success() {
            let _ = Command::new("docker")
                .args(["rm", "-f", &container_name])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            return Err("Failed to create test bucket in MinIO".into());
        }

        println!("  MinIO bucket '{}' created", bucket_name);

        let container = Self {
            port,
            container_name,
            bucket_name: bucket_name.to_string(),
        };

        Ok(container)
    }

    /// Returns the MinIO endpoint URL.
    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Returns S3 config fields for ServerConfig.
    ///
    /// Returns: (region, bucket, access_key, secret_key, endpoint_override, allow_http)
    pub fn s3_config_fields(&self) -> (String, String, String, String, String, bool) {
        (
            "us-east-1".to_string(),
            self.bucket_name.clone(),
            "minioadmin".to_string(),
            "minioadmin".to_string(),
            self.endpoint(),
            true, // allow_http
        )
    }

    /// List all object paths under the given prefix.
    ///
    /// Uses tokio runtime and object_store directly for test verification.
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        use futures::StreamExt;
        use object_store::ObjectStore;

        let store = self.build_object_store()?;
        let prefix_path = object_store::path::Path::from(prefix);

        let mut objects = Vec::new();
        let mut stream = store.list(Some(&prefix_path));

        while let Some(meta) = stream.next().await {
            let meta = meta?;
            objects.push(meta.location.to_string());
        }

        Ok(objects)
    }

    /// Get object content at the given path.
    ///
    /// Uses tokio runtime and object_store directly for test verification.
    pub async fn get_object(&self, path: &str) -> Result<bytes::Bytes, Box<dyn std::error::Error>> {
        use object_store::ObjectStoreExt;

        let store = self.build_object_store()?;
        let object_path = object_store::path::Path::from(path);

        let get_result = store.get(&object_path).await?;
        let bytes = get_result.bytes().await?;

        Ok(bytes)
    }

    /// Put arbitrary bytes at the given path.
    ///
    /// Used for pre-seeding test objects in S3.
    pub async fn put_object(&self, path: &str, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        use object_store::{ObjectStoreExt, PutPayload};

        let store = self.build_object_store()?;
        let object_path = object_store::path::Path::from(path);
        let payload = PutPayload::from(data);

        store.put(&object_path, payload).await?;

        Ok(())
    }

    /// Delete the object at the given path.
    ///
    /// Used for simulating missing S3 batches in gap-detection tests.
    pub async fn delete_object(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        use object_store::ObjectStoreExt;

        let store = self.build_object_store()?;
        let object_path = object_store::path::Path::from(path);

        store.delete(&object_path).await?;

        Ok(())
    }

    fn build_object_store(&self) -> Result<Arc<dyn object_store::ObjectStore>, Box<dyn std::error::Error>> {
        use object_store::aws::AmazonS3Builder;

        let store = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket_name)
            .with_region("us-east-1")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_endpoint(self.endpoint())
            .with_allow_http(true)
            .build()?;

        Ok(Arc::new(store))
    }

    /// Pause the MinIO container (all S3 requests will timeout).
    pub fn pause(&self) -> Result<(), Box<dyn std::error::Error>> {
        let status = Command::new("docker")
            .args(["pause", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        if !status.success() {
            return Err("Failed to pause MinIO container".into());
        }
        Ok(())
    }

    /// Unpause the MinIO container (S3 requests resume).
    pub fn unpause(&self) -> Result<(), Box<dyn std::error::Error>> {
        let status = Command::new("docker")
            .args(["unpause", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        if !status.success() {
            return Err("Failed to unpause MinIO container".into());
        }
        Ok(())
    }
}

impl Drop for MinioContainer {
    fn drop(&mut self) {
        println!("  Stopping MinIO container {}...", self.container_name);
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// A controllable TCP proxy for integration tests.
/// Routes traffic between a listen port and a target port.
/// Can be blocked/unblocked to simulate network partitions,
/// or throttled to simulate slow followers.
pub struct TcpProxy {
    listen_port: u16,
    blocked: Arc<AtomicBool>,
    throttle_ms: Arc<AtomicU64>,
    /// Notified when block() is called; wakes any forwarding tasks blocked in read().
    kill_notify: Arc<Notify>,
}

impl TcpProxy {
    /// Start a TCP proxy that forwards connections from listen_port to target_address.
    pub async fn start(listen_port: u16, target_address: String) -> Result<Self, Box<dyn std::error::Error>> {
        let blocked = Arc::new(AtomicBool::new(false));
        let blocked_clone = blocked.clone();
        let throttle_ms = Arc::new(AtomicU64::new(0));
        let throttle_clone = throttle_ms.clone();
        let kill_notify = Arc::new(Notify::new());
        let kill_notify_clone = kill_notify.clone();

        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", listen_port)).await?;

        tokio::spawn(async move {
            loop {
                let (client, _) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(_) => break,
                };

                if blocked_clone.load(Ordering::Relaxed) {
                    drop(client);
                    continue;
                }

                let target = target_address.clone();
                let blocked_inner = blocked_clone.clone();
                let throttle_inner = throttle_clone.clone();
                let kill_notify_inner = kill_notify_clone.clone();

                let _ = client.set_nodelay(true);
                tokio::spawn(async move {
                    let server = match tokio::net::TcpStream::connect(&target).await {
                        Ok(s) => {
                            let _ = s.set_nodelay(true);
                            s
                        }
                        Err(_) => return,
                    };

                    let (mut client_read, mut client_write) = tokio::io::split(client);
                    let (mut server_read, mut server_write) = tokio::io::split(server);

                    let blocked_a = blocked_inner.clone();
                    let blocked_b = blocked_inner;
                    let throttle_a = throttle_inner.clone();
                    let throttle_b = throttle_inner;
                    let kill_a = kill_notify_inner.clone();
                    let kill_b = kill_notify_inner;

                    let client_to_server = tokio::spawn(async move {
                        let mut buf = [0u8; 8192];
                        loop {
                            if blocked_a.load(Ordering::Relaxed) { break; }
                            let read_fut = tokio::io::AsyncReadExt::read(&mut client_read, &mut buf);
                            let n = tokio::select! {
                                _ = kill_a.notified() => break,
                                result = read_fut => match result {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => n,
                                },
                            };
                            if tokio::io::AsyncWriteExt::write_all(&mut server_write, &buf[..n]).await.is_err() {
                                break;
                            }
                            let delay = throttle_a.load(Ordering::Relaxed);
                            if delay > 0 {
                                tokio::time::sleep(Duration::from_millis(delay)).await;
                            }
                        }
                    });

                    let server_to_client = tokio::spawn(async move {
                        let mut buf = [0u8; 8192];
                        loop {
                            if blocked_b.load(Ordering::Relaxed) { break; }
                            let read_fut = tokio::io::AsyncReadExt::read(&mut server_read, &mut buf);
                            let n = tokio::select! {
                                _ = kill_b.notified() => break,
                                result = read_fut => match result {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => n,
                                },
                            };
                            if tokio::io::AsyncWriteExt::write_all(&mut client_write, &buf[..n]).await.is_err() {
                                break;
                            }
                            let delay = throttle_b.load(Ordering::Relaxed);
                            if delay > 0 {
                                tokio::time::sleep(Duration::from_millis(delay)).await;
                            }
                        }
                    });

                    let _ = tokio::join!(client_to_server, server_to_client);
                });
            }
        });

        Ok(Self { listen_port, blocked, throttle_ms, kill_notify })
    }

    /// Block new connections and wake every forwarding task currently waiting in read().
    pub fn block(&self) {
        self.blocked.store(true, Ordering::Relaxed);
        self.kill_notify.notify_waiters();
        println!("  TcpProxy on port {}: BLOCKED (existing connections killed)", self.listen_port);
    }

    /// Unblock traffic (new connections will be accepted and forwarded).
    pub fn unblock(&self) {
        self.blocked.store(false, Ordering::Relaxed);
        println!("  TcpProxy on port {}: UNBLOCKED", self.listen_port);
    }

    /// Throttle traffic by adding a delay (ms) after forwarding each 8KB chunk.
    /// Slows down replication without severing connections.
    pub fn throttle(&self, delay_per_chunk_ms: u64) {
        self.throttle_ms.store(delay_per_chunk_ms, Ordering::Relaxed);
        println!("  TcpProxy on port {}: THROTTLED ({}ms/chunk)", self.listen_port, delay_per_chunk_ms);
    }

    /// Remove throttle — forward at full speed.
    pub fn unthrottle(&self) {
        self.throttle_ms.store(0, Ordering::Relaxed);
        println!("  TcpProxy on port {}: UNTHROTTLED", self.listen_port);
    }

    /// Get the proxy's listen address.
    pub fn address(&self) -> String {
        format!("127.0.0.1:{}", self.listen_port)
    }
}

/// Write a single event to a server via the client protocol.
///
/// Helper function for integration tests that need to write events.
pub async fn write_event(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
    event_num: u64,
    allow_create: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = DatablockAggregateEvent {
        client_seq: event_num,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(format!("{{\"event\":{}}}", event_num).into_bytes()),
        iv: None,
    };

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create,
            expected_version: if event_num == 1 { Some(0) } else { None },
            enforce_client_idempotency: false,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(event_num as u128),
        client_id: 999,
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&ClientRequest::Write(write_req))
        .await?;

    match response {
        ClientResponse::Write(_) => Ok(()),
        other => Err(format!("Write failed: {:?}", other).into()),
    }
}

/// SplitMix64-style deterministic non-compressible fill.
/// Defeats zstd-dict so payloads survive past MINIBATCH_SIZE_BYTES into external Block storage,
/// which is what tests targeting segment rotation / replication pressure actually want.
pub fn fill_incompressible(buf: &mut [u8], seed: u64) {
    let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    for chunk in buf.chunks_mut(8) {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let bytes = z.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
}

/// Write a single event with a large payload to create replication pressure.
/// The payload_bytes parameter controls how many bytes the event value occupies.
/// Payload bytes are non-compressible so the datablock is forced out of the inline
/// minibatch into external Block storage.
pub async fn write_large_event(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
    event_num: u64,
    payload_bytes: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut payload = vec![0u8; payload_bytes];
    fill_incompressible(&mut payload, event_num);

    let event = DatablockAggregateEvent {
        client_seq: event_num,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(payload),
        iv: None,
    };

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: false,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(event_num as u128),
        client_id: 999,
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&ClientRequest::Write(write_req))
        .await?;

    match response {
        ClientResponse::Write(_) => Ok(()),
        other => Err(format!("Write failed: {:?}", other).into()),
    }
}

/// Read every event batch for an aggregate, paging through `next_aggregate_version`
/// until exhausted. Returns the concatenated list in WAL order.
///
/// Prefer this over hand-rolled pagination — tests that stop after the first
/// page silently miss data once the response exceeds `list_page_size`.
pub async fn read_all_batches(
    client: &mut CeleriantClient,
    key: &AggregateKey,
) -> Result<Vec<AggregateEventBatch>, Box<dyn std::error::Error>> {
    let mut batches = Vec::new();
    let mut from_batch: u64 = 1;
    loop {
        let req = ReadRequest {
            correlation_id: Some(1),
            aggregate_key: key.clone(),
            filters: ReadFilters::new(from_batch),
        };
        let resp = client
            .send_request(&ClientRequest::Read(req))
            .await?;
        match resp {
            ClientResponse::Read(r) => {
                batches.extend(r.event_batches);
                match r.next_aggregate_version {
                    Some(next) => from_batch = next,
                    None => return Ok(batches),
                }
            }
            other => return Err(format!("unexpected response reading {:?}: {:?}", key, other).into()),
        }
    }
}

/// Sleep long enough for S3 election to complete and the leader↔follower
/// replication TCP connection to establish. Called after starting the second
/// `TestServer` in a distributed test.
pub async fn wait_for_election_and_replication() {
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
}

/// Count the total number of events for an aggregate.
///
/// Helper function for integration tests that need to verify replication.
pub async fn count_events(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut total = 0usize;
    let mut from_batch = 1u64;

    loop {
        let read_req = ReadRequest {
            correlation_id: Some(999),
            aggregate_key: aggregate_key.clone(),
            filters: celeriant_msg::request::read_filters::ReadFilters::new(from_batch),
        };

        let response = client
            .send_request(&ClientRequest::Read(read_req))
            .await;

        match response {
            Ok(o) => match o {
                ClientResponse::Read(read_resp) => {
                    total += read_resp
                        .event_batches
                        .iter()
                        .map(|b| b.events.len())
                        .sum::<usize>();
                    match read_resp.next_aggregate_version {
                        Some(next) => from_batch = next,
                        None => return Ok(total),
                    }
                }
                other => return Err(format!("Unexpected response: {:?}", other).into()),
            },
            Err(e) => match &e {
                celeriant_client_tokio::client_error::ClientError::Server(
                    celeriant_client_tokio::server_error::ServerError::Read { kind, error_message: _ }
                ) => {
                    use celeriant_client_tokio::server_error::ReadError;
                    match kind {
                        ReadError::AggregateNotExists => return Ok(total),
                        ReadError::UnavailableBatchIndex { minimum_available_version, .. } => {
                            if let Some(&min) = minimum_available_version.as_ref() {
                                from_batch = min;
                                continue;
                            }
                            return Err(Box::new(e));
                        }
                        _ => return Err(Box::new(e)),
                    }
                }
                _ => return Err(Box::new(e)),
            },
        }
    }
}

/// Poll a node until it has at least `expected` events for the given aggregate.
///
/// Connects fresh on each attempt (handles reconnects after restart).
/// Returns the final event count, or panics after `timeout`.
pub async fn poll_event_count(
    address: &str,
    aggregate_key: &AggregateKey,
    expected: usize,
    timeout: std::time::Duration,
) -> usize {
    let start = std::time::Instant::now();
    let mut last_count = 0usize;
    while start.elapsed() < timeout {
        if let Ok(mut client) = CeleriantClient::connect(address).await {
            if let Ok(c) = count_events(&mut client, aggregate_key).await {
                last_count = c;
                if c >= expected {
                    return c;
                }
            }
        }
        sleep(std::time::Duration::from_secs(2)).await;
    }
    panic!(
        "Timed out after {:.0}s waiting for {} events at {} (last seen: {})",
        timeout.as_secs_f64(),
        expected,
        address,
        last_count,
    );
}

/// Benchmark-tuned shard count and fsync delay, overridable via env vars.
///
/// Defaults to `cpus * 2/3` shards (clamped to 4–24) so shard executors don't
/// saturate every SMT sibling, leaving headroom for the OS, tokio test client,
/// and sidecar threads. Fsync delay defaults to 17ms (server default).
pub fn bench_tuning() -> (u64, Option<usize>) {
    let fsync_delay: u64 = std::env::var("CELERIANT_FSYNC_DELAY_US")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(17000);
    let num_shards: Option<usize> = std::env::var("CELERIANT_NUM_SHARDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| {
            let cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
            Some((cpus * 2 / 3).clamp(4, 24))
        });
    (fsync_delay, num_shards)
}

/// Build a ServerConfig for S3-backed cluster tests.
///
/// Sets up S3 connection fields, routing rule, and heartbeat lease duration.
/// The caller provides MinIO connection details from `MinioContainer::s3_config_fields()`.
pub fn s3_cluster_config(
    num_shards: usize,
    region: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    endpoint: &str,
    allow_http: bool,
) -> ServerConfig {
    ServerConfig {
        num_shards: Some(num_shards),
        log_level: "info".to_string(),
        routing_rule: RoutingRule::AggregateTypeId,
        // S3 lease: 10s TTL so tests don't need 30s+ waits for initial lease expiry.
        // Heartbeat lease: default 1500ms. Failover ≈ 2s after leader death.
        s3_lease_duration_ms: 10_000,
        s3_enabled: true,
        s3_region: Some(region.to_string()),
        s3_bucket: Some(bucket.to_string()),
        s3_access_key_id: Some(access_key.to_string()),
        s3_secret_access_key: Some(secret_key.to_string()),
        s3_endpoint_override: Some(endpoint.to_string()),
        s3_allow_http: allow_http,
        ..Default::default()
    }
}

/// Copy only `shard_*` subdirectories from `src` to `dst`.
///
/// Skips node identity files so each server generates a fresh node_id.
pub fn copy_shard_dirs(src: &std::path::Path, dst: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("shard_") || !entry.file_type()?.is_dir() {
            continue;
        }
        let dst_shard = dst.join(&name);
        std::fs::create_dir_all(&dst_shard)?;
        for file in std::fs::read_dir(entry.path())? {
            let file = file?;
            if file.file_type()?.is_file() {
                std::fs::copy(file.path(), dst_shard.join(file.file_name()))?;
            }
        }
        println!("  Copied shard dir: {}", name.to_string_lossy());
    }
    Ok(())
}

/// Ephemeral PKI for tests: one CA, arbitrary node/client certs.
///
/// All files live in a `TempDir` that is cleaned up when `TestPki` is dropped.
/// The CA directory is at `<temp>/ca/`, cert directories at `<temp>/<name>/`.
pub struct TestPki {
    temp_dir: TempDir,
}

impl TestPki {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        PkiManager::create_ca(&temp_dir.path().join("ca"), 90)?;
        Ok(Self { temp_dir })
    }

    pub fn ca_dir(&self) -> PathBuf {
        self.temp_dir.path().join("ca")
    }

    pub fn ca_cert_path(&self) -> PathBuf {
        self.ca_dir().join("ca.crt")
    }

    pub fn create_node_cert(&self, name: &str) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
        let cert_dir = self.temp_dir.path().join(name);
        PkiManager::create_node_cert(
            &self.ca_dir(),
            &cert_dir,
            &["127.0.0.1".to_string(), "localhost".to_string()],
            90,
        )?;
        Ok((cert_dir.join("node.crt"), cert_dir.join("node.key")))
    }

    pub fn create_client_cert(&self, name: &str) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
        let cert_dir = self.temp_dir.path().join(name);
        PkiManager::create_client_cert(&self.ca_dir(), &cert_dir, name, 90)?;
        Ok((
            cert_dir.join(format!("client-{name}.crt")),
            cert_dir.join(format!("client-{name}.key")),
        ))
    }

    pub fn build_client_tls_config(
        &self,
        client_cert_path: &std::path::Path,
        client_key_path: &std::path::Path,
        server_name: &str,
    ) -> Result<ClientTlsConfig, Box<dyn std::error::Error>> {
        let ca_bundle = PkiManager::load_ca_bundle(&self.ca_cert_path())?;
        let (cert_chain, key) = PkiManager::load_identity(client_cert_path, client_key_path)?;
        let client_config = PkiManager::build_client_config(&ca_bundle, cert_chain, key)?;
        let sni = ServerName::try_from(server_name.to_string())
            .map_err(|e| format!("Invalid server name '{}': {e}", server_name))?;
        Ok(ClientTlsConfig::new(client_config, sni))
    }
}

/// Verify segment file sizes after compaction.
///
/// For each `shard_*/` directory under `data_root`:
/// - Active segment (highest log_id): must be exactly `preallocate_bytes`.
/// - Sealed segments: at least one must be < `preallocate_bytes` (proof compaction ran).
///   Segments that didn't meet the reclaimable ratio are left at full size — that is fine.
///
/// Panics if no sealed segments are found, or if none of them were compacted.
pub fn verify_compacted_segment_sizes(
    data_root: &std::path::Path,
    label: &str,
    preallocate_bytes: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(data_root)? {
        let entry = entry?;
        if !entry.file_name().to_string_lossy().starts_with("shard_")
            || !entry.file_type()?.is_dir()
        {
            continue;
        }
        let shard_dir = entry.path();
        let mut segments: Vec<(u64, std::path::PathBuf)> = Vec::new();
        for file in std::fs::read_dir(&shard_dir)? {
            let file = file?;
            let name = file.file_name();
            let name_str = name.to_string_lossy();
            if let Some(id_str) = name_str
                .strip_prefix("log_")
                .and_then(|s| s.strip_suffix(".wal"))
            {
                if let Ok(log_id) = id_str.parse::<u64>() {
                    segments.push((log_id, file.path()));
                }
            }
        }
        assert!(!segments.is_empty(), "{}: no .wal files found in {:?}", label, shard_dir);
        let active_id = segments.iter().map(|(id, _)| *id).max().unwrap();
        let sealed: Vec<_> = segments.iter().filter(|(id, _)| *id != active_id).collect();
        assert!(
            !sealed.is_empty(),
            "{}: no sealed segments found in {:?} — compaction could not have run",
            label,
            shard_dir
        );
        let (_, active_path) = segments.iter().find(|(id, _)| *id == active_id).unwrap();
        let active_size = std::fs::metadata(active_path)?.len();
        println!("  {}: log_{}.wal (active) = {} bytes", label, active_id, active_size);
        assert_eq!(
            active_size,
            preallocate_bytes,
            "{}: active segment log_{}.wal is {} bytes, expected {}",
            label, active_id, active_size, preallocate_bytes
        );
        let mut any_compacted = false;
        for (log_id, path) in &sealed {
            let size = std::fs::metadata(path)?.len();
            if size < preallocate_bytes {
                any_compacted = true;
                println!("  {}: log_{}.wal (sealed, compacted) = {} bytes", label, log_id, size);
            } else {
                println!("  {}: log_{}.wal (sealed, uncompacted) = {} bytes", label, log_id, size);
            }
        }
        assert!(
            any_compacted,
            "{}: no sealed segment was compacted in {:?} — compaction did not run",
            label, shard_dir
        );
    }
    Ok(())
}

/// Scrape a single Prometheus counter from a server's metrics sidecar.
///
/// Sums values across every label set, mirroring `celeriant_chaos::sample`
/// semantics. Returns `Err` if the endpoint is unreachable or the body isn't
/// parseable — propagate it so tests surface transport problems distinctly
/// from "counter didn't increment".
///
/// The metrics sidecar binds `0.0.0.0:{metrics_port}` (default 9090). Every
/// `TestServer` in one host/process pool inherits the same default, so only
/// the first server started successfully binds it; later servers log a
/// bind error but otherwise run. Scrape only the server that owns the port.
pub async fn scrape_counter(
    metrics_host: &str,
    metrics_port: u16,
    metric_name: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let url = format!("http://{}:{}/metrics", metrics_host, metrics_port);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        return Err(format!("metrics scrape {}: HTTP {}", url, resp.status()).into());
    }
    let body = resp.text().await?;

    let mut total: u64 = 0;
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(3, ' ');
        let name_part = match parts.next() { Some(p) => p, None => continue };
        let value_str = match parts.next() { Some(v) => v, None => continue };
        let name = match name_part.find('{') {
            Some(i) => &name_part[..i],
            None => name_part,
        };
        if name == metric_name
            && let Ok(v) = value_str.parse::<f64>()
        {
            total = total.saturating_add(v as u64);
        }
    }
    Ok(total)
}

/// Probe whether a node is the leader by attempting a write.
///
/// Returns `true` if the node accepts the write (is leader),
/// `false` if it rejects (is follower or fenced).
pub async fn is_leader(address: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let probe_key = AggregateKey::new(999, 999, 999);
    let mut client = CeleriantClient::connect(address).await?;
    match write_event(&mut client, &probe_key, 1, true).await {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}
