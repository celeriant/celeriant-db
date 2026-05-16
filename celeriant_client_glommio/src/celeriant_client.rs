use std::rc::Rc;
use std::sync::Arc;

use celeriant_ktls::ktls_connect;
use celeriant_msg::process_cluster_requests::ClusterRequest;
use celeriant_msg::process_cluster_responses::ClusterResponse;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::codec::compression::DictCodec;
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
fn extract_host(address: &str) -> Result<&str, String> {
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

pub struct CeleriantClient {
    stream: TcpStream,
    max_request_size: u64,
    max_response_size: u64,
    timeout_duration: Option<Duration>,
    dict_codec: Rc<DictCodec>,
}

impl CeleriantClient {
    pub async fn connect_with_timeout_tls(
        address: &str,
        connection_timeout: Option<Duration>,
        max_request_size: u64,
        max_response_size: u64,
        tls_config: Option<GlommioTlsConfig>,
        tcp_user_timeout: Option<Duration>,
        dict_codec: Rc<DictCodec>,
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

        {
            use std::os::unix::io::AsRawFd;
            let fd = stream.as_raw_fd();
            let enabled: libc::c_int = 1;
            let idle_secs: libc::c_int = 10;
            let interval_secs: libc::c_int = 3;
            unsafe {
                libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, &enabled as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t);
                libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, &idle_secs as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t);
                libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, &interval_secs as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as libc::socklen_t);
            }
            if let Some(user_timeout) = tcp_user_timeout {
                let user_timeout_ms: libc::c_uint = user_timeout.as_millis().min(u32::MAX as u128) as libc::c_uint;
                unsafe {
                    libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_USER_TIMEOUT, &user_timeout_ms as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_uint>() as libc::socklen_t);
                }
            }
        }

        let stream = match tls_config {
            None => stream,
            Some(cfg) => ktls_connect(stream, cfg.client_config, cfg.server_name)
                .await
                .map_err(ClientError::KtlsError)?
                .0,
        };

        Ok(Self {
            stream,
            max_request_size,
            max_response_size,
            timeout_duration: None,
            dict_codec,
        })
    }

    /// Set request timeout (default: none)
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout_duration = Some(duration);
        self
    }

    pub async fn send_cluster_request(
        &mut self,
        request: &ClusterRequest,
    ) -> Result<ClusterResponse, ClientError> {
        if let Some(duration) = self.timeout_duration {
            let request_future = self.send_cluster_request_inner(request);
            let result = or(
                async { Some(request_future.await) },
                async { Timer::new(duration).await; None }
            ).await;
            match result {
                Some(response) => response,
                None => Err(ClientError::RequestTimeout),
            }
        } else {
            self.send_cluster_request_inner(request).await
        }
    }

    async fn send_cluster_request_inner(
        &mut self,
        request: &ClusterRequest,
    ) -> Result<ClusterResponse, ClientError> {
        // Only variable-size variants (ReplicationBatch) can be compressed.
        // Fixed-size variants (Heartbeat, KickFollower) always use None.
        let compression = match request {
            ClusterRequest::ReplicationBatch(_) => CompressionType::ZstdDict,
            _ => CompressionType::None,
        };
        ClusterRequest::write_request(
            &mut self.stream,
            request,
            compression,
            self.max_request_size,
            PROTOCOL_VERSION_V2,
            &self.dict_codec,
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
