use std::sync::Arc;

use celeriant_crypto::Crypto;
use celeriant_msg::process_identify::{
    IDENTIFY_RESPONSE_TYPE_ID, read_identify_response, write_identify_request,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::IdentifyRequest;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WireHeader};
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

impl std::fmt::Debug for ClientTlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTlsConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl ClientTlsConfig {
    pub fn new(client_config: Arc<rustls::ClientConfig>, server_name: ServerName<'static>) -> Self {
        Self {
            connector: TlsConnector::from(client_config),
            server_name,
        }
    }
}

pub(crate) enum ClientStream {
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

/// Establish a TCP/TLS connection, returning a raw ClientStream.
/// Shared by CeleriantClient and WatchConnection.
pub(crate) async fn connect_stream(
    address: &str,
    connection_timeout: Option<Duration>,
    tls_config: Option<&ClientTlsConfig>,
) -> Result<ClientStream, ClientError> {
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

    tcp.set_nodelay(true).map_err(ClientError::ConnectionFailed)?;

    match tls_config {
        None => Ok(ClientStream::Plain(tcp.compat())),
        Some(cfg) => {
            let tls = cfg
                .connector
                .connect(cfg.server_name.clone(), tcp)
                .await
                .map_err(ClientError::ConnectionFailed)?;
            Ok(ClientStream::Tls(tls.compat()))
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientIdentityConfig {
    pub public_key: Option<String>,
    pub private_key: Option<String>,
    pub api_key: Option<String>,
}

impl ClientIdentityConfig {
    pub fn from_api_key(api_key: impl Into<String>) -> Self {
        Self { public_key: None, private_key: None, api_key: Some(api_key.into()) }
    }

    pub fn from_key_pair(public_key: impl Into<String>, private_key: impl Into<String>) -> Self {
        Self { public_key: Some(public_key.into()), private_key: Some(private_key.into()), api_key: None }
    }
}

/// Minimal, high-performance Celeriant TCP client
///
/// Establishes a single TCP connection for multiple request/response pairs.
/// Connection lifetime is tied to the struct (RAII). No retries, no heartbeat.
/// Developers need to handle connection timeouts and reconnection logic.
/// TCP connections are a limited resource, only hold one open as long as you need it.
pub struct CeleriantClient {
    stream: ClientStream,
    max_request_size: u64,
    max_response_size: u64,
    timeout: Option<Duration>,
    pub(crate) compression: CompressionType,
    pub(crate) auto_compression_threshold: u64,
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
        let stream = connect_stream(address, connection_timeout, tls_config.as_ref()).await?;

        Ok(Self {
            stream,
            max_request_size: 10_000_000,        // 10 MB default
            max_response_size: 64 * 1024 * 1024, // 64 MB — matches server default
            timeout: connection_timeout,
            compression: CompressionType::Zstd { level: 3 },
            auto_compression_threshold: 1024,
        })
    }

    /// Set maximum request size in bytes (default: 10 MB)
    pub fn with_max_request_size(mut self, max_request_size: u64) -> Self {
        self.max_request_size = max_request_size;
        self
    }

    /// Set maximum response size in bytes (default: 64 MB)
    pub fn with_max_response_size(mut self, max_response_size: u64) -> Self {
        self.max_response_size = max_response_size;
        self
    }

    /// Set request timeout (default: none)
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Set compression algorithm used when payload exceeds the auto-compression threshold (default: Zstd level 3)
    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Set the payload size threshold in bytes above which auto-compression is applied (default: 1024)
    pub fn with_auto_compression_threshold(mut self, bytes: u64) -> Self {
        self.auto_compression_threshold = bytes;
        self
    }

    /// Send a request and await the response
    ///
    /// Compression is specified per-request. Returns the response or an error.
    /// This is a blocking operation on the connection. For concurrent requests,
    /// create multiple client instances (one per connection).
    pub async fn send_request(
        &mut self,
        request: &ClientRequest,
        compression_type: CompressionType,
    ) -> Result<ClientResponse, ClientError> {
        // Apply timeout if configured
        if let Some(duration) = self.timeout {
            timeout(duration, self.send_request_inner(request, compression_type))
                .await
                .map_err(|_| ClientError::RequestTimeout)?
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
        .await?;

        let response = ClientResponse::read_response(&mut self.stream, self.max_response_size).await?;

        match response {
            ClientResponse::ProtocolError(_) => Err(ClientError::ProtocolError),
            ClientResponse::GenericError(error) => Err(ClientError::from_error_response(error)),
            _ => Ok(response),
        }
    }

    /// Perform client identity verification handshake
    ///
    /// Generates a nonce, signs it with the private key, and sends an IdentifyRequest
    /// to the server. Returns the server-verified client_id on success.
    ///
    /// This should be called after connection establishment and before any data operations
    /// if identity verification is required.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use celeriant_client_tokio::{CeleriantClient, ClientIdentityConfig};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut client = CeleriantClient::connect("127.0.0.1:10000").await?;
    ///
    /// let identity_config = ClientIdentityConfig {
    ///     public_key: Some("MIIBIjANBg...".to_string()),  // Base64-encoded public key
    ///     private_key: Some("MIIEvgIBAD...".to_string()), // Base64-encoded private key
    ///     api_key: None, // Optional API key for authentication
    /// };
    ///
    /// let client_id = client.identify(&identity_config).await?;
    /// println!("Verified client_id: {:?}", client_id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn identify(&mut self, identity_config: &ClientIdentityConfig) -> Result<Option<u128>, ClientError> {
        let (public_key, nonce, signature) = match (&identity_config.public_key, &identity_config.private_key) {
            (Some(pub_key), Some(priv_key)) => {
                let nonce = Crypto::generate_nonce()?;
                let sig = Crypto::sign_nonce(priv_key, &nonce)?;
                (Some(pub_key.clone()), Some(nonce), Some(sig))
            }
            _ => (None, None, None),
        };

        let req = IdentifyRequest {
            correlation_id: None,
            public_key,
            nonce,
            signature,
            api_key: identity_config.api_key.clone(),
        };

        let identify_inner = async {
            write_identify_request(&mut self.stream, &req, PROTOCOL_VERSION_V2).await?;

            let header = WireHeader::from_reader(&mut self.stream, self.max_response_size).await?;
            if header.message_type == IDENTIFY_RESPONSE_TYPE_ID {
                let resp = read_identify_response(header, &mut self.stream).await?;
                return Ok(resp.client_id);
            }
            // Server sent a non-Identify response — must be an error
            let response = ClientResponse::read_from_header(header, &mut self.stream).await?;
            match response {
                ClientResponse::ProtocolError(_) => Err(ClientError::ProtocolError),
                ClientResponse::GenericError(err) => Err(ClientError::from_error_response(err)),
                _ => Err(ClientError::ProtocolError),
            }
        };

        if let Some(duration) = self.timeout {
            timeout(duration, identify_inner)
                .await
                .map_err(|_| ClientError::RequestTimeout)?
        } else {
            identify_inner.await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_crypto::Crypto;

    #[test]
    fn identity_config_can_be_created() {
        let keypair = Crypto::generate_keypair(None).expect("keypair generation should succeed");
        let config = ClientIdentityConfig {
            public_key: Some(keypair.public_key_base64.clone()),
            private_key: Some(keypair.private_key_base64.clone()),
            api_key: None,
        };
        assert!(config.public_key.as_ref().is_some_and(|k| !k.is_empty()));
        assert!(config.private_key.as_ref().is_some_and(|k| !k.is_empty()));
    }

    #[test]
    fn identity_config_can_sign_nonce() {
        let keypair = Crypto::generate_keypair(None).expect("keypair generation should succeed");
        let config = ClientIdentityConfig {
            public_key: Some(keypair.public_key_base64.clone()),
            private_key: Some(keypair.private_key_base64.clone()),
            api_key: None,
        };

        let nonce = Crypto::generate_nonce().expect("nonce generation should succeed");
        let signature = Crypto::sign_nonce(config.private_key.as_deref().unwrap(), &nonce)
            .expect("signing should succeed");

        assert!(!signature.is_empty());
    }
}
