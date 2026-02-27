use std::sync::Arc;

use celeriant_ktls::ktls_connect;
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::process_cluster_requests::ClusterRequest;
use celeriant_msg::process_cluster_responses::ClusterResponse;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::network::wire_header::PROTOCOL_VERSION_V2;
use futures_lite::future::or;
use glommio::net::TcpStream;
use glommio::timer::Timer;
use rustls_pki_types::ServerName;
use std::time::Duration;

use crate::client_error::ClientError;

#[derive(Clone)]
pub struct GlommioTlsConfig {
    pub client_config: Arc<rustls::ClientConfig>,
    pub server_name: ServerName<'static>,
}

impl GlommioTlsConfig {
    pub fn new(client_config: Arc<rustls::ClientConfig>, server_name: ServerName<'static>) -> Self {
        Self { client_config, server_name }
    }

    /// Build a `GlommioTlsConfig` for a node-to-node replication connection.
    ///
    /// Parses the host portion of `address` (e.g. `"10.0.0.1:12000"` or
    /// `"[::1]:12000"`) into a `ServerName`. The node certificate must have a
    /// matching SAN.
    pub fn from_address(
        client_config: Arc<rustls::ClientConfig>,
        address: &str,
    ) -> Result<Self, String> {
        let host = extract_host(address)?;

        let server_name: ServerName<'static> = host
            .to_string()
            .try_into()
            .map_err(|e| format!("invalid server name '{}': {:?}", host, e))?;

        Ok(Self { client_config, server_name })
    }
}

/// Extract the host portion from a `"host:port"` or `"[ipv6]:port"` address string.
///
/// IPv6 bracket notation `"[::1]:12000"` → `"::1"`
/// IPv4 / hostname `"10.0.0.1:12000"` → `"10.0.0.1"`
pub(crate) fn extract_host(address: &str) -> Result<&str, String> {
    if address.starts_with('[') {
        address
            .get(1..)
            .and_then(|s| s.split_once(']'))
            .map(|(h, _)| h)
            .ok_or_else(|| format!("invalid IPv6 address '{}'", address))
    } else {
        Ok(address.rsplit_once(':').map(|(h, _)| h).unwrap_or(address))
    }
}

#[cfg(test)]
mod tests {
    use super::extract_host;

    #[test]
    fn test_ipv4_address() {
        assert_eq!(extract_host("10.0.0.1:12000").unwrap(), "10.0.0.1");
    }

    #[test]
    fn test_ipv6_bracket_notation() {
        assert_eq!(extract_host("[::1]:12000").unwrap(), "::1");
    }

    #[test]
    fn test_plain_hostname() {
        assert_eq!(extract_host("my-node.cluster.local:12000").unwrap(), "my-node.cluster.local");
    }

    #[test]
    fn test_ipv6_bracket_invalid() {
        assert!(extract_host("[::1-no-bracket").is_err());
    }
}

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
    max_response_size: u64,
    timeout_duration: Option<Duration>,
}

impl CeleriantClient {
    pub async fn connect_with_timeout(
        address: &str,
        connection_timeout: Option<Duration>,
        max_request_size: u64,
        max_response_size: u64,
    ) -> Result<Self, ClientError> {
        Self::connect_with_timeout_tls(address, connection_timeout, max_request_size, max_response_size, None).await
    }

    pub async fn connect_with_timeout_tls(
        address: &str,
        connection_timeout: Option<Duration>,
        max_request_size: u64,
        max_response_size: u64,
        tls_config: Option<GlommioTlsConfig>,
    ) -> Result<Self, ClientError> {
        let stream = if let Some(duration) = connection_timeout {
            TcpStream::connect_timeout(address, duration)
                .await
                .map_err(|e| {
                    use std::io::ErrorKind;
                    if let glommio::GlommioError::IoError(ref io_err) = e {
                        if io_err.kind() == ErrorKind::TimedOut {
                            return ClientError::ConnectionTimeout;
                        }
                    }
                    ClientError::ConnectionFailed(e)
                })?
        } else {
            TcpStream::connect(address)
                .await
                .map_err(ClientError::ConnectionFailed)?
        };

        // Set TCP_NODELAY to disable Nagle's algorithm
        stream
            .set_nodelay(true)
            .map_err(ClientError::SetNoDelayError)?;

        let stream = match tls_config {
            None => stream,
            Some(cfg) => ktls_connect(stream, cfg.client_config, cfg.server_name)
                .await
                .map_err(ClientError::KtlsError)?,
        };

        Ok(Self {
            stream,
            max_request_size,
            max_response_size,
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
        request: &ClientRequest,
        compression_type: CompressionType,
    ) -> Result<ClientResponse, ClientError> {
        if let Some(duration) = self.timeout_duration {
            let request_future = self.send_request_inner(request, compression_type);

            let result = or(
                async { Some(request_future.await) },
                async { Timer::new(duration).await; None }
            ).await;

            match result {
                Some(response) => response,
                None => Err(ClientError::RequestTimeout),
            }
        } else {
            self.send_request_inner(request, compression_type).await
        }
    }

    async fn send_request_inner(
        &mut self,
        request: &ClientRequest,
        compression_type: CompressionType,
    ) -> Result<ClientResponse, ClientError> {
        ClientRequest::write_request(
            &mut self.stream,
            request,
            compression_type,
            self.max_request_size,
            PROTOCOL_VERSION_V2,
        )
        .await
        .map_err(ClientError::WriteRequestError)?;

        let response = ClientResponse::read_response(&mut self.stream, self.max_response_size).await
            .map_err(ClientError::ReadResponseError)?;

        match response {
            ClientResponse::ProtocolError(_) => Err(ClientError::RequestProtocolError),
            ClientResponse::GenericError(error) => Err(ClientError::from_error_response(error)),
            _ => Ok(response),
        }
    }

    pub async fn send_cluster_request(
        &mut self,
        request: &ClusterRequest,
        compression_type: CompressionType,
    ) -> Result<ClusterResponse, ClientError> {
        if let Some(duration) = self.timeout_duration {
            let request_future = self.send_cluster_request_inner(request, compression_type);
            let result = or(
                async { Some(request_future.await) },
                async { Timer::new(duration).await; None }
            ).await;
            match result {
                Some(response) => response,
                None => Err(ClientError::RequestTimeout),
            }
        } else {
            self.send_cluster_request_inner(request, compression_type).await
        }
    }

    async fn send_cluster_request_inner(
        &mut self,
        request: &ClusterRequest,
        compression_type: CompressionType,
    ) -> Result<ClusterResponse, ClientError> {
        ClusterRequest::write_request(
            &mut self.stream,
            request,
            compression_type,
            self.max_request_size,
            PROTOCOL_VERSION_V2,
        )
        .await
        .map_err(ClientError::WriteRequestError)?;

        let response = ClusterResponse::read_response(&mut self.stream, self.max_response_size)
            .await
            .map_err(ClientError::ReadResponseError)?;

        match response {
            ClusterResponse::ProtocolError(_) => Err(ClientError::RequestProtocolError),
            ClusterResponse::GenericError(error) => Err(ClientError::from_error_response(error)),
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

    /// Close the connection explicitly
    ///
    /// Consumes the client, ensuring it cannot be used after closing.
    /// The connection is also closed automatically when dropped.
    pub async fn close(self) -> Result<(), ClientError> {
        use std::net::Shutdown;
        self.stream
            .shutdown(Shutdown::Both)
            .await
            .map_err(ClientError::ConnectionFailed)
    }
}
