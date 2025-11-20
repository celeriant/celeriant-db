use crate::{ClientConfig, ClientError, ClientResult};
use eventplanedb_structures::{compression_type::CompressionType, request, response};
use futures_util::lock::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

/// A single TCP connection with pipelining support
pub struct Connection {
    stream: Arc<Mutex<TcpStream>>,
    address: String,
    last_used: Arc<Mutex<Instant>>,
    request_timeout: Duration,
    compression_type: CompressionType,
    max_request_size: u32,
}

impl Connection {
    pub async fn new(
        address: String,
        request_timeout: Duration,
        compression_type: CompressionType,
        max_request_size: u32,
    ) -> ClientResult<Self> {
        let stream = tokio::time::timeout(
            request_timeout,
            TcpStream::connect(&address)
        )
        .await
        .map_err(|_| ClientError::ConnectionTimeout)?
        .map_err(|e| ClientError::ConnectionFailed(e.to_string()))?;

        // Set TCP_NODELAY for low latency
        stream.set_nodelay(true)
            .map_err(|e| ClientError::ConnectionFailed(e.to_string()))?;

        Ok(Self {
            stream: Arc::new(Mutex::new(stream)),
            address,
            last_used: Arc::new(Mutex::new(Instant::now())),
            request_timeout,
            compression_type,
            max_request_size,
        })
    }

    pub async fn execute(
        &mut self,
        request: request::Request,
    ) -> ClientResult<response::Response> {
        // Update last used time
        *self.last_used.lock().await = Instant::now();

        // Lock the stream for exclusive access during request/response
        let mut stream = self.stream.lock().await;

        // Write request with timeout
        tokio::time::timeout(
            self.request_timeout,
            request::write_request(
                &mut (&mut *stream).compat_write(),
                &request,
                self.compression_type,
                self.max_request_size,
            )
        )
        .await
        .map_err(|_| ClientError::RequestTimeout)?
        .map_err(|e| ClientError::WriteError(format!("{:?}", e)))?;

        // Flush to ensure request is sent
        stream.flush().await
            .map_err(|e| ClientError::WriteError(e.to_string()))?;

        // Read response with timeout
        let response = tokio::time::timeout(
            self.request_timeout,
            response::read_response(&mut (&mut *stream).compat())
        )
        .await
        .map_err(|_| ClientError::ResponseTimeout)?
        .map_err(|e| ClientError::ReadError(format!("{:?}", e)))?;

        Ok(response)
    }

    pub async fn is_alive(&self) -> bool {
        // Check if connection is still valid by trying to peek at the stream
        let stream = self.stream.lock().await;
        stream.peer_addr().is_ok()
    }

    pub async fn last_used(&self) -> Instant {
        *self.last_used.lock().await
    }

    pub fn address(&self) -> &str {
        &self.address
    }
}

/// Connection pool with automatic reconnection and health checking
pub struct ConnectionPool {
    config: ClientConfig,
    connections: Arc<Mutex<Vec<Connection>>>,
    semaphore: Arc<Semaphore>,
    stats: Arc<Mutex<PoolStats>>,
}

#[derive(Debug, Default)]
struct PoolStats {
    total_requests: u64,
    failed_requests: u64,
}

impl ConnectionPool {
    pub async fn new(config: ClientConfig) -> ClientResult<Self> {
        let pool_size = config.pool_config.max_connections;
        let semaphore = Arc::new(Semaphore::new(pool_size));
        
        // Pre-create initial connections
        let mut connections = Vec::new();
        for _ in 0..config.pool_config.min_connections {
            match Connection::new(
                config.address.clone(),
                Duration::from_millis(config.request_timeout_ms),
                config.compression_type,
                config.max_request_size,
            ).await {
                Ok(conn) => connections.push(conn),
                Err(e) => {
                    if config.pool_config.fail_fast {
                        return Err(e);
                    }
                    // Continue with fewer connections if fail_fast is false
                    break;
                }
            }
        }

        let pool = Self {
            config,
            connections: Arc::new(Mutex::new(connections)),
            semaphore,
            stats: Arc::new(Mutex::new(PoolStats::default())),
        };

        // Start background health check task
        pool.start_health_check();

        Ok(pool)
    }

    pub async fn get(&self) -> ClientResult<Connection> {
        // Acquire semaphore permit
        let _permit = self.semaphore.acquire().await
            .map_err(|_| ClientError::PoolExhausted)?;

        let mut connections = self.connections.lock().await;

        // Try to reuse an existing connection
        if let Some(conn) = connections.pop() {
            if conn.is_alive().await {
                self.stats.lock().await.total_requests += 1;
                return Ok(conn);
            }
            // Connection is dead, create a new one
        }

        // Create a new connection
        let conn = Connection::new(
            self.config.address.clone(),
            Duration::from_millis(self.config.request_timeout_ms),
            self.config.compression_type,
            self.config.max_request_size,
        ).await?;

        self.stats.lock().await.total_requests += 1;
        Ok(conn)
    }

    pub async fn return_connection(&self, conn: Connection) {
        let mut connections = self.connections.lock().await;
        
        // Only return healthy connections
        if conn.is_alive().await 
            && connections.len() < self.config.pool_config.max_connections 
        {
            connections.push(conn);
        }
    }

    pub async fn stats(&self) -> crate::ConnectionStats {
        let connections = self.connections.lock().await;
        let stats = self.stats.lock().await;

        crate::ConnectionStats {
            active_connections: self.semaphore.available_permits(),
            idle_connections: connections.len(),
            total_requests: stats.total_requests,
            failed_requests: stats.failed_requests,
        }
    }

    pub async fn close(&self) {
        let mut connections = self.connections.lock().await;
        connections.clear();
    }

    fn start_health_check(&self) {
        let connections = self.connections.clone();
        let check_interval = Duration::from_secs(
            self.config.pool_config.health_check_interval_secs
        );
        let max_idle = Duration::from_secs(
            self.config.pool_config.max_idle_time_secs
        );

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(check_interval).await;

                let mut conns = connections.lock().await;
                let now = Instant::now();

                // Remove dead or idle connections
                conns.retain(|conn| {
                    futures::executor::block_on(async {
                        conn.is_alive().await 
                            && now.duration_since(conn.last_used().await) < max_idle
                    })
                });
            }
        });
    }
}

// Implement Drop to return connection to pool
impl Drop for Connection {
    fn drop(&mut self) {
        // In a real implementation, we'd return the connection to the pool here
        // For now, the connection will just be closed
    }
}