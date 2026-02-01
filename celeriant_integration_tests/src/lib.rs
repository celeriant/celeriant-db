//! Shared test utilities for celeriant_integration_tests integration tests.

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

pub use celeriant_lib::server_config::{ConfigClusterRole, ServerConfig};
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
///         non_durable_writes: true,
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
}

impl TestServer {
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
        mut config: ServerConfig,
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

        let child = Command::new("cargo")
            .args(["run", "-p", "celeriant", "--release", "--"])
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

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

    /// Stop the server process (can be restarted later).
    pub fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        println!("  Server stopped on port {}", self.config.client_port);
    }

    /// Restart the server process after stopping.
    pub async fn restart(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let args = self.config.to_cli_args();

        self.child = Command::new("cargo")
            .args(["run", "-p", "celeriant", "--release", "--"])
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

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
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Kill the server process
        let _ = self.child.kill();
        let _ = self.child.wait();
        println!("  Test server shut down");
    }
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

        if self.non_durable_writes {
            args.push("--non-durable-writes".to_string());
        }

        args.push("--log-level".to_string());
        args.push(self.log_level.clone());

        args.push("--list-max-duration-ms".to_string());
        args.push(self.list_max_duration_ms.to_string());

        args.push("--list-page-size".to_string());
        args.push(self.list_page_size.to_string());

        args.push("--list-wal-index-cache-bytes".to_string());
        args.push(self.list_wal_index_cache_bytes.to_string());

        // Cluster role configuration
        let role_str = match self.cluster_role {
            ConfigClusterRole::Standalone => "standalone",
            ConfigClusterRole::Leader => "leader",
            ConfigClusterRole::Follower => "follower",
        };
        args.push("--cluster-role".to_string());
        args.push(role_str.to_string());

        if let Some(follower_addr) = &self.follower_address {
            args.push("--follower-address".to_string());
            args.push(follower_addr.clone());
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
}

impl MinioContainer {
    /// Start a MinIO container on the given port.
    ///
    /// Waits for MinIO to accept connections and creates the test bucket.
    /// Uses port allocation offset +10 from base to avoid collision with server ports.
    pub async fn start(port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let container_name = format!("celeriant-test-minio-{}", port);

        println!("  Starting MinIO container {} on port {}...", container_name, port);

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
        let bucket_status = Command::new("docker")
            .args(["exec", &container_name, "mkdir", "-p", "/data/test-fallback"])
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

        println!("  MinIO bucket 'test-fallback' created");

        let container = Self {
            port,
            container_name,
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
            "test-fallback".to_string(),
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

    fn build_object_store(&self) -> Result<Arc<dyn object_store::ObjectStore>, Box<dyn std::error::Error>> {
        use object_store::aws::AmazonS3Builder;

        let store = AmazonS3Builder::new()
            .with_bucket_name("test-fallback")
            .with_region("us-east-1")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_endpoint(self.endpoint())
            .with_allow_http(true)
            .build()?;

        Ok(Arc::new(store))
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
