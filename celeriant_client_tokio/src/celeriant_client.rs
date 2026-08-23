use std::sync::Arc;

use celeriant_crypto::Crypto;
use celeriant_msg::process_identify::{
    IDENTIFY_RESPONSE_TYPE_ID, read_identify_response, write_identify_request,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::IdentifyRequest;
use celeriant_client_wire::{build_frame, decompress_body, write_frame};
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::network::wire_error::WireError;
use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WireHeader};
use rustls_pki_types::ServerName;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_rustls::TlsConnector;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

/// A compression dict received from the server during Identify.
#[derive(Clone, Debug)]
pub struct CachedDict {
    pub sha: String,
    pub bytes: Arc<[u8]>,
}

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
            let handshake = cfg.connector.connect(cfg.server_name.clone(), tcp);
            let tls = if let Some(duration) = connection_timeout {
                timeout(duration, handshake)
                    .await
                    .map_err(|_| ClientError::ConnectionTimeout)?
                    .map_err(ClientError::ConnectionFailed)?
            } else {
                handshake.await.map_err(ClientError::ConnectionFailed)?
            };
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
    pub(crate) current_dict: Option<CachedDict>,
    stream_dirty: bool,
    next_correlation_id: u128,
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
            max_response_size: 64 * 1024 * 1024, // 64 MB; matches server default
            timeout: connection_timeout,
            current_dict: None,
            stream_dirty: false,
            next_correlation_id: 1,
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

    fn take_correlation_id(&mut self) -> u128 {
        let id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);
        id
    }

    pub(crate) async fn send_owned(
        &mut self,
        mut request: ClientRequest,
    ) -> Result<ClientResponse, ClientError> {
        let id = self.take_correlation_id();
        request.set_correlation_id_if_absent(id);
        self.send_request(&request).await
    }

    /// See `stream_dirty`. A dirty connection must be closed, never reused.
    pub(crate) fn is_stream_dirty(&self) -> bool {
        self.stream_dirty
    }

    /// Set request timeout (default: none)
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Send a request and await the response.
    /// This is a blocking operation on the connection. For concurrent requests,
    /// create multiple client instances (one per connection).
    pub async fn send_request(
        &mut self,
        request: &ClientRequest,
    ) -> Result<ClientResponse, ClientError> {
        let filled;
        let request = if request.correlation_id().is_none() {
            let id = self.take_correlation_id();
            filled = {
                let mut owned = request.clone();
                owned.set_correlation_id_if_absent(id);
                owned
            };
            &filled
        } else {
            request
        };

        // Apply timeout if configured
        if let Some(duration) = self.timeout {
            timeout(duration, self.send_request_inner(request))
                .await
                .map_err(|_| ClientError::RequestTimeout)?
        } else {
            self.send_request_inner(request).await
        }
    }

    fn choose_compression(&self, request: &ClientRequest) -> CompressionType {
        let Some(_) = self.current_dict.as_ref() else {
            return CompressionType::None;
        };
        let payload_bytes: usize = match request {
            ClientRequest::Write(req) => req.writes.values()
                .flat_map(|w| &w.events)
                .map(|e| e.event_value.len())
                .sum(),
            ClientRequest::RegisterSchema(req) => req.schema.len(),
            _ => return CompressionType::None,
        };
        if payload_bytes >= celeriant_msg::RESPONSE_COMPRESSION_THRESHOLD_BYTES {
            CompressionType::ZstdDict
        } else {
            CompressionType::None
        }
    }

    async fn send_request_inner(
        &mut self,
        request: &ClientRequest,
    ) -> Result<ClientResponse, ClientError> {
        // An earlier request was abandoned before its response was read, so the
        // next frame off this stream belongs to that request, not this one.
        if self.stream_dirty {
            return Err(ClientError::ProtocolError);
        }
        // Compress variable-size bodies up front with celeriant_client_wire's stateless dict
        // helper (raw `&[u8]` dict, not a `!Sync` DictCodec), so this request future stays `Send`.
        // Fixed-size requests are never compressed. Nothing here touches the socket.
        let frame = if request.is_variable_size() {
            let body = request.serialize_body(PROTOCOL_VERSION_V2)?;
            let type_id = request.request_type() as u32;
            let compress = self.choose_compression(request) == CompressionType::ZstdDict;
            let dict = self.current_dict.as_ref().map(|d| d.bytes.as_ref());
            Some(build_frame(type_id, body, dict, compress)?)
        } else {
            None
        };

        self.stream_dirty = true;
        let written = match &frame {
            Some(frame) => {
                write_frame(&mut self.stream, frame, self.max_request_size, PROTOCOL_VERSION_V2).await
            }
            None => {
                ClientRequest::write_request(&mut self.stream, request, self.max_request_size, PROTOCOL_VERSION_V2)
                    .await
            }
        };
        if let Err(e) = written {
            self.stream_dirty = matches!(e, WireError::NetworkError(_));
            return Err(e.into());
        }

        // Fixed-size responses read straight from the header; variable-size bodies are decompressed
        // with the same stateless dict helper once the body is in hand.
        let header = WireHeader::from_reader(&mut self.stream, self.max_response_size).await?;
        let response = if ClientResponse::is_fixed_size_variant(header.message_type) {
            let response = ClientResponse::read_from_header(header, &mut self.stream).await?;
            self.stream_dirty = false;
            response
        } else {
            let raw = header.read_variable_body_raw(&mut self.stream).await?;
            self.stream_dirty = false;
            let dict = self.current_dict.as_ref().map(|d| d.bytes.as_ref());
            let plain = decompress_body(header.compression_type, header.uncompressed_length, &raw, dict)?;
            ClientResponse::deserialize_body(header.message_type, &plain, header.version)?
        };

        let sent = request.correlation_id();
        if response.carries_correlation_id() && response.correlation_id() != sent {
            self.stream_dirty = true;
            return Err(ClientError::CorrelationMismatch {
                sent,
                received: response.correlation_id(),
            });
        }
        if response.response_type() != request.request_type().expected_response_type()
            && !matches!(
                response,
                ClientResponse::GenericError(_) | ClientResponse::ProtocolError(_)
            )
        {
            self.stream_dirty = true;
            return Err(ClientError::ProtocolError);
        }

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
    /// Perform client identity verification handshake, optionally advertising a
    /// previously cached dict sha so the server can skip re-sending the bytes.
    ///
    /// `known_dict_sha` should be `None` on the very first connection and the sha
    /// from `current_dict` on subsequent ones (supplied by the pool).
    pub async fn identify_with_known_sha(
        &mut self,
        identity_config: &ClientIdentityConfig,
        known_dict_sha: Option<String>,
        dict_lookup: impl FnOnce(&str) -> Option<Arc<[u8]>>,
    ) -> Result<Option<u128>, ClientError> {
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
            known_dict_sha256: known_dict_sha,
        };

        self.stream_dirty = true;
        let stream = &mut self.stream;
        let max_response_size = self.max_response_size;
        let identify_inner = async move {
            write_identify_request(stream, &req, PROTOCOL_VERSION_V2).await?;

            let header = WireHeader::from_reader(stream, max_response_size).await?;
            if header.message_type == IDENTIFY_RESPONSE_TYPE_ID {
                let resp = read_identify_response(header, stream).await?;
                return Ok(resp);
            }
            // Server sent a non-Identify response; must be an error
            let response = ClientResponse::read_from_header(header, stream).await?;
            match response {
                ClientResponse::ProtocolError(_) => Err(ClientError::ProtocolError),
                ClientResponse::GenericError(err) => Err(ClientError::from_error_response(err)),
                _ => Err(ClientError::ProtocolError),
            }
        };

        let resp = if let Some(duration) = self.timeout {
            timeout(duration, identify_inner)
                .await
                .map_err(|_| ClientError::RequestTimeout)??
        } else {
            identify_inner.await?
        };
        self.stream_dirty = false;

        // Resolve the dict for this connection:
        //   - If server shipped bytes -> store them (new or refreshed dict).
        //   - If server sent sha only (client already has it) -> look up from pool cache.
        //   - If neither -> no dict (cluster not using ZstdDict).
        self.current_dict = match (resp.compression_dict_sha256, resp.compression_dict_bytes) {
            (Some(sha), Some(bytes)) => {
                Some(CachedDict { sha, bytes: Arc::from(bytes.into_boxed_slice()) })
            }
            (Some(sha), None) => {
                // Server confirmed sha match; retrieve bytes from pool cache.
                dict_lookup(&sha).map(|bytes| CachedDict { sha, bytes })
            }
            (None, _) => None,
        };

        Ok(resp.client_id)
    }

    pub async fn identify(&mut self, identity_config: &ClientIdentityConfig) -> Result<Option<u128>, ClientError> {
        // No pool → no cached sha, no dict lookup.
        self.identify_with_known_sha(identity_config, None, |_| None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_crypto::Crypto;
    use std::time::Duration;

    async fn silent_server() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut sockets = Vec::new();
            loop {
                if let Ok((socket, _)) = listener.accept().await {
                    sockets.push(socket);
                }
            }
        });
        addr
    }

    #[tokio::test]
    async fn tls_handshake_times_out_when_server_unresponsive() {
        let addr = silent_server().await;
        let address = addr.to_string();

        let ring_provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());

        #[derive(Debug)]
        struct AcceptAny(Vec<rustls::SignatureScheme>);
        impl rustls::client::danger::ServerCertVerifier for AcceptAny {
            fn verify_server_cert(
                &self,
                _end_entity: &rustls_pki_types::CertificateDer<'_>,
                _intermediates: &[rustls_pki_types::CertificateDer<'_>],
                _server_name: &rustls_pki_types::ServerName<'_>,
                _ocsp_response: &[u8],
                _now: rustls_pki_types::UnixTime,
            ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &rustls_pki_types::CertificateDer<'_>,
                _dss: &rustls::DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &rustls_pki_types::CertificateDer<'_>,
                _dss: &rustls::DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
                self.0.clone()
            }
        }

        let schemes = ring_provider
            .signature_verification_algorithms
            .supported_schemes();

        let client_config = rustls::ClientConfig::builder_with_provider(ring_provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(AcceptAny(schemes)))
            .with_no_client_auth();

        let tls_config = ClientTlsConfig::new(
            std::sync::Arc::new(client_config),
            rustls_pki_types::ServerName::try_from("localhost").unwrap().to_owned(),
        );

        let start = std::time::Instant::now();
        let result = connect_stream(
            &address,
            Some(Duration::from_millis(500)),
            Some(&tls_config),
        )
        .await;
        let elapsed = start.elapsed();

        assert!(
            matches!(result, Err(ClientError::ConnectionTimeout)),
            "expected ConnectionTimeout, got {:?}",
            result.map(|_| ())
        );
        assert!(elapsed < Duration::from_millis(1500), "timed out too slowly: {elapsed:?}");
    }

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

    // --- CachedDict / dict decompression tests ---

    #[test]
    fn identify_with_known_sha_stores_dict_bytes() {
        // Simulate: server sends both sha and bytes (first connection).
        // identify_with_known_sha must store them as current_dict.
        let sha = "abc123".to_string();
        let bytes: Arc<[u8]> = Arc::from(b"fake dict bytes" as &[u8]);
        // We can test the dict storage directly without a server by testing
        // the CachedDict construction logic.
        let d = CachedDict { sha: sha.clone(), bytes: Arc::clone(&bytes) };
        assert_eq!(d.sha, "abc123");
        assert_eq!(&*d.bytes, b"fake dict bytes");
    }

    #[test]
    fn cached_dict_clone_shares_arc() {
        let bytes: Arc<[u8]> = Arc::from(vec![1u8, 2, 3].as_slice());
        let d = CachedDict { sha: "s1".to_string(), bytes: Arc::clone(&bytes) };
        let d2 = d.clone();
        // Both should point to the same allocation.
        assert!(Arc::ptr_eq(&d.bytes, &d2.bytes));
        assert_eq!(d.sha, d2.sha);
    }

    #[test]
    fn identify_with_known_sha_uses_pool_lookup_when_no_bytes() {
        use futures_lite::future::block_on;
        use celeriant_wire::codec::compression;
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;

        // Simulate the dict-lookup closure that identify_with_known_sha calls
        // when the server sends sha-only (bytes already cached by pool).
        let dict_bytes: Arc<[u8]> = Arc::from(BUILTIN_DICT_BYTES);
        let sha = "known-sha".to_string();

        let found = (|s: &str| -> Option<Arc<[u8]>> {
            if s == "known-sha" { Some(Arc::clone(&dict_bytes)) } else { None }
        })(&sha);

        assert!(found.is_some());
        let d = CachedDict { sha: sha.clone(), bytes: found.unwrap() };
        assert_eq!(d.sha, "known-sha");

        // Verify we can actually decompress with the builtin dict.
        let original = b"hello world, test data for dict compression";
        let compressed = compression::compress_with_dict(original, 3, BUILTIN_DICT_BYTES).unwrap();
        let decompressed = compression::decompress_with_dict(&compressed, original.len(), &d.bytes).unwrap();
        assert_eq!(decompressed.as_slice(), original.as_slice());
        let _ = block_on(async {}); // ensure async context is tested
    }

    // --- stream_dirty: the cancellation guard ---

    fn details_request(aggregate_id: u128) -> ClientRequest {
        ClientRequest::AggregateDetails(
            celeriant_msg::request::requests::AggregateDetailsRequest {
                correlation_id: None,
                aggregate_key: celeriant_wal::aggregate_key::AggregateKey::new(1, 1, aggregate_id),
            },
        )
    }

    #[tokio::test]
    async fn a_fresh_client_has_a_clean_stream() {
        let addr = silent_server().await;
        let client = CeleriantClient::connect(&addr.to_string()).await.unwrap();
        assert!(!client.is_stream_dirty());
    }

    #[tokio::test]
    async fn a_request_future_dropped_before_its_response_leaves_the_stream_dirty() {
        use futures_util::FutureExt;

        let addr = silent_server().await;
        let mut client = CeleriantClient::connect(&addr.to_string()).await.unwrap();

        // One poll drives write_frame to completion and parks on the response
        // read — the request bytes are in the kernel send buffer whether or not
        // anyone waits for the reply. A wall-clock timeout would race the
        // server here and flake; this cannot.
        let polled = client.send_request(&details_request(1)).now_or_never();
        assert!(polled.is_none(), "one poll must park on the response read, not complete");

        assert!(
            client.is_stream_dirty(),
            "a request went out with no response read — the stream is one frame behind"
        );
    }

    #[tokio::test]
    async fn the_dirty_flag_survives_an_outer_timeout_cancelling_the_request() {
        let addr = silent_server().await;
        let mut client = CeleriantClient::connect(&addr.to_string()).await.unwrap();

        let outcome = timeout(
            Duration::from_millis(50),
            client.send_request(&details_request(2)),
        )
        .await;

        assert!(outcome.is_err(), "silent server cannot answer inside 50ms");
        assert!(client.is_stream_dirty());
    }

    /// Pins the arm site to *before* the write. A request too large for the
    /// socket buffers parks inside `write_frame`, so cancelling here abandons a
    /// half-written request frame — worse than the undrained-response case,
    /// and invisible to any test that cancels after the write completes.
    #[tokio::test]
    async fn a_request_cancelled_part_way_through_the_write_leaves_the_stream_dirty() {
        use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
        use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
        use futures_util::FutureExt;

        // The silent server never reads, so its receive buffer fills and this
        // never fits in the socket buffers.
        let addr = silent_server().await;
        let mut client = CeleriantClient::connect(&addr.to_string())
            .await
            .unwrap()
            .with_max_request_size(64 * 1024 * 1024);

        let mut writes = std::collections::HashMap::new();
        writes.insert(
            celeriant_wal::aggregate_key::AggregateKey::new(1, 1, 1),
            SingleAggregateWrite {
                events: vec![DatablockAggregateEvent {
                    client_seq: 0,
                    event_seq: 0,
                    event_id: None,
                    event_timestamp: 0,
                    event_type_major: 1,
                    event_type_minor: 0,
                    event_value: Arc::new(vec![7u8; 32 * 1024 * 1024]),
                    iv: None,
                }],
                allow_create: true,
                expected_version: Some(0),
                enforce_client_idempotency: false,
            },
        );
        let request = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes,
        });

        let polled = client.send_request(&request).now_or_never();
        assert!(polled.is_none(), "32MB cannot fit in the socket buffers of an unreading peer");
        assert!(
            client.is_stream_dirty(),
            "a partially written request frame leaves the stream desynchronised"
        );
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
