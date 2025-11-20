use eventplanedb_structures::compression_type::CompressionType;

#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Server address (e.g., "127.0.0.1:10000")
    pub address: String,
    
    /// Request timeout in milliseconds
    pub request_timeout_ms: u64,
    
    /// Maximum number of retry attempts
    pub max_retries: u32,
    
    /// Initial retry delay in milliseconds (doubles on each retry)
    pub retry_delay_ms: u64,
    
    /// Compression type for requests
    pub compression_type: CompressionType,
    
    /// Maximum request size in bytes
    pub max_request_size: u32,
    
    /// Connection pool configuration
    pub pool_config: ConnectionPoolConfig,
}

impl ClientConfig {
    pub fn new(address: String) -> Self {
        Self {
            address,
            request_timeout_ms: 5000,
            max_retries: 3,
            retry_delay_ms: 100,
            compression_type: CompressionType::None,
            max_request_size: 16 * 1024 * 1024, // 16 MB
            pool_config: ConnectionPoolConfig::default(),
        }
    }

    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.request_timeout_ms = timeout_ms;
        self
    }

    pub fn with_retries(mut self, max_retries: u32, delay_ms: u64) -> Self {
        self.max_retries = max_retries;
        self.retry_delay_ms = delay_ms;
        self
    }

    pub fn with_compression(mut self, compression_type: CompressionType) -> Self {
        self.compression_type = compression_type;
        self
    }

    pub fn with_pool_config(mut self, pool_config: ConnectionPoolConfig) -> Self {
        self.pool_config = pool_config;
        self
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionPoolConfig {
    /// Minimum number of connections to maintain in the pool
    pub min_connections: usize,
    
    /// Maximum number of connections in the pool
    pub max_connections: usize,
    
    /// Health check interval in seconds
    pub health_check_interval_secs: u64,
    
    /// Maximum idle time before closing a connection (seconds)
    pub max_idle_time_secs: u64,
    
    /// Fail fast if initial connections can't be established
    pub fail_fast: bool,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 2,
            max_connections: 10,
            health_check_interval_secs: 30,
            max_idle_time_secs: 300,
            fail_fast: false,
        }
    }
}