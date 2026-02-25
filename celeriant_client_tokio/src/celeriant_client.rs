use std::sync::Arc;

use celeriant_msg::process_requests::Request;
use celeriant_msg::process_responses::Response;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::network::wire_header::PROTOCOL_VERSION_V2;
use rustls_pki_types::ServerName;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_rustls::TlsConnector;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::client_error::ClientError;

#[derive(Clone)]
pub struct ClientTlsConfig {
    pub connector: TlsConnector,
    pub server_name: ServerName<'static>,
}

impl ClientTlsConfig {
    pub fn new(client_config: Arc<rustls::ClientConfig>, server_name: ServerName<'static>) -> Self {
        Self {
            connector: TlsConnector::from(client_config),
            server_name,
        }
    }
}

enum ClientStream {
    Plain(Compat<TcpStream>),
    Tls(Compat<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl futures_util::io::AsyncRead for ClientStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ClientStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl futures_util::io::AsyncWrite for ClientStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ClientStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(s) => std::pin::Pin::new(s).poll_close(cx),
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_close(cx),
        }
    }
}

impl Unpin for ClientStream {}

/// Minimal, high-performance Celeriant TCP client
///
/// Establishes a single TCP connection for multiple request/response pairs.
/// Connection lifetime is tied to the struct (RAII). No retries, no heartbeat.
/// Developers need to handle connection timeouts and reconnection logic.
/// TCP connections are a limited resource, only hold one open as long as you need it.
pub struct CeleriantClient {
    stream: ClientStream,
    max_request_size: u64,
    timeout: Option<Duration>,
}

impl CeleriantClient {
    /// Connect to Celeriant server at the given address (e.g., "127.0.0.1:10000")
    pub async fn connect(address: &str) -> Result<Self, ClientError> {
        Self::connect_with_timeout(address, None, None).await
    }

    /// Connect to Celeriant server, optionally with TLS
    pub async fn connect_tls(
        address: &str,
        tls_config: ClientTlsConfig,
    ) -> Result<Self, ClientError> {
        Self::connect_with_timeout(address, None, Some(tls_config)).await
    }

    /// Connect to Celeriant server with an optional connection timeout and optional TLS
    pub async fn connect_with_timeout(
        address: &str,
        connection_timeout: Option<Duration>,
        tls_config: Option<ClientTlsConfig>,
    ) -> Result<Self, ClientError> {
        let connect_future = TcpStream::connect(address);

        let tcp = if let Some(duration) = connection_timeout {
            timeout(duration, connect_future)
                .await
                .map_err(|_| ClientError::ConnectionTimeout)?
                .map_err(ClientError::ConnectionFailed)?
        } else {
            connect_future
                .await
                .map_err(ClientError::ConnectionFailed)?
        };

        // Set TCP_NODELAY to disable Nagle's algorithm
        tcp.set_nodelay(true).map_err(ClientError::ConnectionFailed)?;

        let stream = match tls_config {
            None => ClientStream::Plain(tcp.compat()),
            Some(cfg) => {
                let tls = cfg
                    .connector
                    .connect(cfg.server_name, tcp)
                    .await
                    .map_err(ClientError::ConnectionFailed)?;
                ClientStream::Tls(tls.compat())
            }
        };

        Ok(Self {
            stream,
            max_request_size: 10_000_000, // 10MB default
            timeout: connection_timeout,
        })
    }

    /// Set maximum request size in bytes (default: 10MB)
    pub fn with_max_request_size(mut self, max_request_size: u64) -> Self {
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
            let response = Response::read_response(&mut self.stream, self.max_request_size).await?;

            match response {
                Response::ProtocolError(_) => return Err(ClientError::ProtocolError),
                Response::GenericError(error) => return Err(ClientError::from_error_response(error)),
                _ => {}
            }

            Ok(response)
        };

        // Apply timeout if configured
        if let Some(duration) = self.timeout {
            timeout(duration, request_future)
                .await
                .map_err(|_| ClientError::RequestTimeout)?
        } else {
            request_future.await
        }
    }
}
