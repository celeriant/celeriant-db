use celeriant_msg::process_requests::Request;
use celeriant_msg::process_responses::Response;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::constants::PROTOCOL_VERSION_V2;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::client_error::ClientError;

/// Minimal, high-performance Celeriant TCP client
/// 
/// Establishes a single TCP connection for multiple request/response pairs.
/// Connection lifetime is tied to the struct (RAII). No retries, no heartbeat.
/// Developers need to handle connection timeouts and reconnection logic.
/// TCP connections are a limited resource, only hold one open as long as you need it.
pub struct CeleriantClient {
    stream: Compat<TcpStream>,
    max_request_size: u32,
    timeout: Option<Duration>,
}

impl CeleriantClient {
    /// Connect to Celeriant server at the given address (e.g., "127.0.0.1:10000")
    pub async fn connect(address: &str) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(address)
            .await
            .map_err(ClientError::ConnectionFailed)?;

        // Set TCP_NODELAY to disable Nagle's algorithm
        stream.set_nodelay(true).map_err(ClientError::ConnectionFailed)?;
        
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
    /// CeleriantError from the response is surfaced as ClientError::CeleriantError.
    /// 
    /// This is a blocking operation on the connection. For concurrent requests,
    /// create multiple client instances (one per connection).
    pub async fn send_request(
        &mut self,
        request: &Request,
        compression_type: CompressionType,
    ) -> Result<Response, ClientError> {
        let request_future = async {
            // Write request to server with V2 protocol
            Request::write_request(
                &mut self.stream,
                request,
                compression_type,
                self.max_request_size,
                PROTOCOL_VERSION_V2,
            )
            .await?;

            // Read response from server
            let response = Response::read_response(&mut self.stream).await?;

            match response {
                Response::ProtocolError(_) => return Err(ClientError::ProtocolError),
                Response::GenericError(error) => return Err(ClientError::CeleriantError(error)),
                _ => {}
            }

            Ok(response)
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