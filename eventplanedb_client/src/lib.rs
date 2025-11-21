use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use eventplanedb_structures::{
    request::{Request, write_request},
    response::{Response, read_response},
    compression_type::CompressionType,
    wire_error::WireError,
    eventplanedb_error::EventPlaneDBError,
};

/// Minimal, high-performance EventPlaneDB client
/// 
/// Establishes a single TCP connection for multiple request/response pairs.
/// Connection lifetime is tied to the struct (RAII). No retries, no heartbeat.
/// Developers handle connection timeouts and reconnection logic.
pub struct EventPlaneDBClient {
    stream: Compat<TcpStream>,
    max_request_size: u32,
    timeout: Option<Duration>,
}

#[derive(Debug)]
pub enum ClientError {
    ConnectionFailed(std::io::Error),
    WireError(WireError),
    EventPlaneDBError(EventPlaneDBError),
    ProtocolError,
    Timeout,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::ConnectionFailed(e) => write!(f, "Connection failed: {}", e),
            ClientError::WireError(e) => write!(f, "Wire error: {:?}", e),
            ClientError::EventPlaneDBError(e) => write!(f, "EventPlaneDB error: {:?}", e),
            ClientError::ProtocolError => write!(f, "Protocol error"),
            ClientError::Timeout => write!(f, "Request timeout"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<WireError> for ClientError {
    fn from(e: WireError) -> Self {
        ClientError::WireError(e)
    }
}

impl From<EventPlaneDBError> for ClientError {
    fn from(e: EventPlaneDBError) -> Self {
        ClientError::EventPlaneDBError(e)
    }
}

impl EventPlaneDBClient {
    /// Connect to EventPlaneDB server at the given address (e.g., "127.0.0.1:50051")
    pub async fn connect(address: &str) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(address)
            .await
            .map_err(ClientError::ConnectionFailed)?;
        
        Ok(Self {
            stream: stream.compat(),
            max_request_size: 10_000_000, // 10MB default
            timeout: None,
        })
    }

    /// Set maximum request size in bytes (default: 10MB)
    pub fn with_max_request_size(mut self, max_request_size: u32) -> Self {
        self.max_request_size = max_request_size;
        self
    }

    /// Set request timeout (default: none)
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Send a request and await the response
    /// 
    /// Compression is specified per-request. Returns the response or an error.
    /// EventPlaneDBError from the response is surfaced as ClientError::EventPlaneDBError.
    /// 
    /// This is a blocking operation on the connection. For concurrent requests,
    /// create multiple client instances (one per connection).
    pub async fn send_request(
        &mut self,
        request: &Request,
        compression_type: CompressionType,
    ) -> Result<Response, ClientError> {
        let request_future = async {
            // Write request to server
            write_request(
                &mut self.stream,
                request,
                compression_type,
                self.max_request_size,
            )
            .await?;

            // Read response from server
            let response = read_response(&mut self.stream).await?;

            // Check for protocol error
            if matches!(response, Response::ProtocolError(_)) {
                return Err(ClientError::ProtocolError);
            }

            // Use the new helper to check for errors
            response.into_result().map_err(ClientError::EventPlaneDBError)
        };

        // Apply timeout if configured
        if let Some(duration) = self.timeout {
            timeout(duration, request_future)
                .await
                .map_err(|_| ClientError::Timeout)?
        } else {
            request_future.await
        }
    }
}