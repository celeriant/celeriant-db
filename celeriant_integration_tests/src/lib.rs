//! Shared test utilities for celeriant_integration_tests integration tests.

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::{
    process_requests::Request,
    request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
pub use celeriant_lib::server_config::ServerConfig;
pub use celeriant_runtimes::RoutingRule;
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

        // Wait for server to start by polling the port
        let start = std::time::Instant::now();
        let max_wait = Duration::from_secs(30);

        while start.elapsed() < max_wait {
            match TcpStream::connect(&address).await {
                Ok(_) => {
                    println!("  Server is ready (took {:?})", start.elapsed());
                    return Ok(Self {
                        _temp_dir: temp_dir,
                        address,
                        child,
                        config,
                        label,
                        _log_thread: log_thread,
                    });
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }

        Err("Server failed to start within timeout".into())
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

        // Wait for server to start
        let start = std::time::Instant::now();
        let max_wait = Duration::from_secs(30);

        while start.elapsed() < max_wait {
            match TcpStream::connect(&self.address).await {
                Ok(_) => {
                    println!("  Server restarted on port {} (took {:?})", self.config.client_port, start.elapsed());
                    return Ok(());
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }

        Err("Server failed to restart within timeout".into())
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

        let start = std::time::Instant::now();
        let max_wait = Duration::from_secs(30);

        while start.elapsed() < max_wait {
            match TcpStream::connect(&self.address).await {
                Ok(_) => {
                    println!(
                        "  Server restarted with new config on port {} (took {:?})",
                        self.config.client_port,
                        start.elapsed()
                    );
                    return Ok(());
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }

        Err("Server failed to restart within timeout".into())
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

        let start = std::time::Instant::now();
        let max_wait = Duration::from_secs(30);

        while start.elapsed() < max_wait {
            match TcpStream::connect(&address).await {
                Ok(_) => {
                    println!("  Server is ready (took {:?})", start.elapsed());
                    return Ok(Self {
                        _temp_dir: temp_dir,
                        address,
                        child,
                        config,
                        label,
                        _log_thread: log_thread,
                    });
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
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

        args.push("--max-response-size".to_string());
        args.push(self.max_response_size.to_string());

        args.push("--max-requested-latency-ms".to_string());
        args.push(self.max_requested_latency_ms.to_string());

        args.push("--client-connection-timeout-ms".to_string());
        args.push(self.client_connection_timeout_ms.to_string());

        args.push("--shard-log-preallocate-bytes".to_string());
        args.push(self.shard_log_preallocate_bytes.to_string());

        args.push("--recent-write-cache-bytes".to_string());
        args.push(self.recent_write_cache_bytes.to_string());

        args.push("--aggregate-client-snapshots-cache-bytes".to_string());
        args.push(self.aggregate_client_snapshots_cache_bytes.to_string());

        args.push("--aggregate-snapshots-cache-bytes".to_string());
        args.push(self.aggregate_snapshots_cache_bytes.to_string());

        args.push("--fsync-delay-us".to_string());
        args.push(self.fsync_delay_us.to_string());

        args.push("--log-level".to_string());
        args.push(self.log_level.clone());

        args.push("--list-max-duration-ms".to_string());
        args.push(self.list_max_duration_ms.to_string());

        args.push("--list-page-size".to_string());
        args.push(self.list_page_size.to_string());

        args.push("--list-wal-index-cache-bytes".to_string());
        args.push(self.list_wal_index_cache_bytes.to_string());

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

            args.push("--s3-catchup-max-rounds".to_string());
            args.push(self.s3_catchup_max_rounds.to_string());

            args.push("--max-s3-fallback-batch-bytes".to_string());
            args.push(self.max_s3_fallback_batch_bytes.to_string());
        }

        args.push("--pending-replication-high-water-bytes".to_string());
        args.push(self.pending_replication_high_water_bytes.to_string());

        args.push("--max-cluster-time-drift-ms".to_string());
        args.push(self.max_cluster_time_drift_ms.to_string());

        args.push("--max-catchup-gap-bytes".to_string());
        args.push(self.max_catchup_gap_bytes.to_string());

        if let Some(timeout) = self.internode_connection_timeout_ms {
            args.push("--internode-connection-timeout-ms".to_string());
            args.push(timeout.to_string());
        }

        args.push("--internode-request-timeout-ms".to_string());
        args.push(self.internode_request_timeout_ms.to_string());

        args.push("--replication-delay-us".to_string());
        args.push(self.replication_delay_us.to_string());

        args.push("--heartbeat-interval-ms".to_string());
        args.push(self.heartbeat_interval_ms.to_string());

        args.push("--heartbeat-lease-duration-ms".to_string());
        args.push(self.heartbeat_lease_duration_ms.to_string());

        args.push("--max-clock-drift-ms".to_string());
        args.push(self.max_clock_drift_ms.to_string());

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
}

impl TcpProxy {
    /// Start a TCP proxy that forwards connections from listen_port to target_address.
    pub async fn start(listen_port: u16, target_address: String) -> Result<Self, Box<dyn std::error::Error>> {
        let blocked = Arc::new(AtomicBool::new(false));
        let blocked_clone = blocked.clone();
        let throttle_ms = Arc::new(AtomicU64::new(0));
        let throttle_clone = throttle_ms.clone();

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

                    let client_to_server = tokio::spawn(async move {
                        let mut buf = [0u8; 8192];
                        loop {
                            if blocked_a.load(Ordering::Relaxed) { break; }
                            match tokio::io::AsyncReadExt::read(&mut client_read, &mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => {
                                    if tokio::io::AsyncWriteExt::write_all(&mut server_write, &buf[..n]).await.is_err() {
                                        break;
                                    }
                                    let delay = throttle_a.load(Ordering::Relaxed);
                                    if delay > 0 {
                                        tokio::time::sleep(Duration::from_millis(delay)).await;
                                    }
                                }
                            }
                        }
                    });

                    let server_to_client = tokio::spawn(async move {
                        let mut buf = [0u8; 8192];
                        loop {
                            if blocked_b.load(Ordering::Relaxed) { break; }
                            match tokio::io::AsyncReadExt::read(&mut server_read, &mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => {
                                    if tokio::io::AsyncWriteExt::write_all(&mut client_write, &buf[..n]).await.is_err() {
                                        break;
                                    }
                                    let delay = throttle_b.load(Ordering::Relaxed);
                                    if delay > 0 {
                                        tokio::time::sleep(Duration::from_millis(delay)).await;
                                    }
                                }
                            }
                        }
                    });

                    let _ = tokio::join!(client_to_server, server_to_client);
                });
            }
        });

        Ok(Self { listen_port, blocked, throttle_ms })
    }

    /// Block all traffic through the proxy (existing connections are dropped).
    pub fn block(&self) {
        self.blocked.store(true, Ordering::Relaxed);
        println!("  TcpProxy on port {}: BLOCKED", self.listen_port);
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
        client_event_index: event_num,
        event_index: 0,
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
            expected_event_batch_index: if event_num == 1 { Some(0) } else { None },
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(event_num as u128),
        client_id: 999,
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&Request::Write(write_req), CompressionType::None)
        .await?;

    match response {
        celeriant_msg::process_responses::Response::Write(_) => Ok(()),
        other => Err(format!("Write failed: {:?}", other).into()),
    }
}

/// Write a single event with a large payload to create replication pressure.
/// The payload_bytes parameter controls how many bytes the event value occupies.
pub async fn write_large_event(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
    event_num: u64,
    payload_bytes: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut payload = format!("{{\"event\":{},\"pad\":\"", event_num);
    let pad_len = payload_bytes.saturating_sub(payload.len() + 2); // 2 for closing "}
    payload.extend(std::iter::repeat('x').take(pad_len));
    payload.push_str("\"}");

    let event = DatablockAggregateEvent {
        client_event_index: event_num,
        event_index: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(payload.into_bytes()),
        iv: None,
    };

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: false,
            expected_event_batch_index: None,
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(event_num as u128),
        client_id: 999,
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&Request::Write(write_req), CompressionType::None)
        .await?;

    match response {
        celeriant_msg::process_responses::Response::Write(_) => Ok(()),
        other => Err(format!("Write failed: {:?}", other).into()),
    }
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
            .send_request(&Request::Read(read_req), CompressionType::None)
            .await;

        match response {
            Ok(o) => match o {
                celeriant_msg::process_responses::Response::Read(read_resp) => {
                    total += read_resp
                        .event_batches
                        .iter()
                        .map(|b| b.events.len())
                        .sum::<usize>();
                    match read_resp.next_event_batch_index {
                        Some(next) => from_batch = next,
                        None => return Ok(total),
                    }
                }
                other => return Err(format!("Unexpected response: {:?}", other).into()),
            },
            Err(e) => match &e {
                celeriant_client_tokio::client_error::ClientError::CeleriantError(error_response) => {
                    if error_response.error_code == 1001 {
                        return Ok(total);
                    } else {
                        return Err(Box::new(e));
                    }
                }
                _ => return Err(Box::new(e)),
            },
        }
    }
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
        // S3 lease: 10s initial TTL (enough for discovery + first heartbeat)
        // Heartbeat status TTL: ~2s from defaults (heartbeat_interval=500ms × 3 + clock_drift=500ms)
        heartbeat_lease_duration_ms: 10_000,
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
