use celeriant_msg::process_requests::Request;
use celeriant_msg::process_responses::Response;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::constants::PROTOCOL_VERSION_V2;
use futures_lite::future::or;
use glommio::GlommioError;
use glommio::net::TcpStream;
use glommio::timer::{Timer};
use std::time::Duration;

use crate::client_error::ClientError;

/// Minimal, high-performance Celeriant TCP client for Glommio
///
/// Establishes a single TCP connection for multiple request/response pairs.
/// Connection lifetime is tied to the struct (RAII). No retries, no heartbeat.
/// Developers need to handle connection timeouts and reconnection logic.
/// TCP connections are a limited resource, only hold one open as long as you need it.
///
/// Note: This client must be used within a Glommio LocalExecutor context.
pub struct CeleriantClient {
    stream: TcpStream,
    max_request_size: u64,
    timeout_duration: Option<Duration>,
}

impl CeleriantClient {
    /// Connect to Celeriant server at the given address (e.g., "127.0.0.1:10000")
    pub async fn connect(address: &str) -> Result<Self, ClientError> {

        //TODO: Apply timeout to connection attempt
        let stream = TcpStream::connect(address)
            .await
            .map_err(|e| ClientError::ConnectionFailed(e))?;

        Ok(Self {
            stream,
            max_request_size: 10_000_000, // 10MB default
            timeout_duration: None,
        })
    }

    /// Connect to Celeriant server with a connection timeout
    pub async fn connect_with_timeout(
        address: &str,
        connect_timeout: Duration,
    ) -> Result<Self, ClientError> {
        let stream = TcpStream::connect_timeout(address, connect_timeout)
            .await
            .map_err(|e| match e {
                GlommioError::TimedOut(dur) => ClientError::Timeout(dur),
                other => ClientError::ConnectionFailed(other),
            })?;

        Ok(Self {
            stream,
            max_request_size: 10_000_000,
            timeout_duration: None,
        })
    }

    /// Set maximum request size in bytes (default: 10MB)
    pub fn with_max_request_size(mut self, max_request_size: u64) -> Self {
        self.max_request_size = max_request_size;
        self
    }

    /// Set request timeout (default: none)
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout_duration = Some(duration);
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
        // Apply timeout if configured
        if let Some(duration) = self.timeout_duration {
            let request_future = self.send_request_inner(request, compression_type);
            
            let result = or(
                async { Some(request_future.await) },
                async { Timer::new(duration).await; None }
            ).await;

            match result {
                Some(response) => response,
                None => Err(ClientError::Timeout(duration)),
            }
        } else {
            self.send_request_inner(request, compression_type).await
        }
    }

    async fn send_request_inner(
        &mut self,
        request: &Request,
        compression_type: CompressionType,
    ) -> Result<Response, ClientError> {
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
            Response::ProtocolError(_) => Err(ClientError::ProtocolError),
            Response::GenericError(error) => Err(ClientError::CeleriantError(error)),
            _ => Ok(response),
        }
    }

    /// Set TCP_NODELAY option (disable Nagle's algorithm)
    pub fn set_nodelay(&self, nodelay: bool) -> Result<(), ClientError> {
        self.stream
            .set_nodelay(nodelay)
            .map_err(|e| ClientError::ConnectionFailed(e))
    }

    /// Get the local address of this connection
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, ClientError> {
        self.stream
            .local_addr()
            .map_err(|e| ClientError::ConnectionFailed(e))
    }

    /// Get the peer address of this connection
    pub fn peer_addr(&self) -> Result<std::net::SocketAddr, ClientError> {
        self.stream
            .peer_addr()
            .map_err(|e| ClientError::ConnectionFailed(e))
    }
}