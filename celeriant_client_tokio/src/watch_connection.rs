use std::sync::Arc;

use celeriant_crypto::Crypto;
use celeriant_msg::error_codes;
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::process_identify::{IDENTIFY_RESPONSE_TYPE_ID, read_identify_response, write_identify_request};
use celeriant_msg::request::requests::{IdentifyRequest, WatchRequest};
use celeriant_msg::response::responses::WatchResponse;
use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WireHeader};
use tokio::time::{timeout, Duration};

use crate::celeriant_client::{CachedDict, ClientIdentityConfig, ClientStream, ClientTlsConfig};
use crate::client_error::ClientError;

/// Options for configuring a watch connection
#[derive(Clone)]
pub struct WatchOptions {
    pub timeout: Option<Duration>,
    pub start_shard: u64,
    pub max_shard_hint: Option<u64>,
    pub tls_config: Option<ClientTlsConfig>,
    pub identity_config: Option<ClientIdentityConfig>,
}

impl Default for WatchOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            start_shard: 0,
            max_shard_hint: None,
            tls_config: None,
            identity_config: None,
        }
    }
}

struct CountingRead<'a, R> {
    inner: &'a mut R,
    consumed: &'a std::sync::atomic::AtomicUsize,
}

impl<R: futures_util::io::AsyncRead + Unpin> futures_util::io::AsyncRead for CountingRead<'_, R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let me = self.get_mut();
        let polled = std::pin::Pin::new(&mut *me.inner).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(n)) = &polled {
            me.consumed.fetch_add(*n, std::sync::atomic::Ordering::Relaxed);
        }
        polled
    }
}

struct ShardStream {
    stream: crate::celeriant_client::ClientStream,
    max_request_size: u64,
    current_dict: Option<CachedDict>,
    partial_frame_bytes: std::sync::atomic::AtomicUsize,
}

impl ShardStream {
    async fn read_next(&mut self) -> Result<WatchResponse, ClientError> {
        let ShardStream { stream, max_request_size, current_dict, partial_frame_bytes } = self;
        if partial_frame_bytes.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            return Err(ClientError::ProtocolError);
        }
        let dict = current_dict.as_ref().map(|d| d.bytes.as_ref());
        let response = {
            let mut counting = CountingRead { inner: stream, consumed: partial_frame_bytes };
            let header = celeriant_wire::network::wire_header::WireHeader::from_reader(
                &mut counting,
                *max_request_size,
            )
            .await
            .map_err(celeriant_msg::read_wire_data_error::ReadWireDataError::ReadHeaderFailure);
            match header {
                Err(e) => Err(e),
                Ok(header) => {
                    crate::tokio_wire::read_from_header_with(header, &mut counting, dict, || {
                        partial_frame_bytes.store(0, std::sync::atomic::Ordering::Relaxed)
                    })
                    .await
                }
            }
        };
        match response? {
            ClientResponse::Watch(watch_resp) => Ok(watch_resp),
            ClientResponse::ProtocolError(_) => Err(ClientError::ProtocolError),
            ClientResponse::GenericError(error) => Err(ClientError::from_error_response(error)),
            _ => Err(ClientError::ProtocolError),
        }
    }
}

/// Watch connection that handles single-shard and multi-shard watches transparently
pub struct WatchConnection {
    mode: WatchMode,
    address: String,
}

enum WatchMode {
    SingleShard(ShardStream),
    MultiShard(MultiShardState),
}

struct MultiShardState {
    receiver: tokio::sync::mpsc::UnboundedReceiver<Result<WatchResponse, ClientError>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    poisoned: bool,
}

impl MultiShardState {
    fn poison(&mut self) {
        self.poisoned = true;
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Drop for MultiShardState {
    fn drop(&mut self) {
        // Dropping a JoinHandle does not cancel the task. A per-shard reader
        // parked in read_next() on a quiet shard would otherwise hold its
        // ShardStream/socket forever. Abort them so the fds are released.
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn identify_stream<F>(
    stream: &mut ClientStream,
    identity: &ClientIdentityConfig,
    known_sha: Option<String>,
    dict_lookup: F,
) -> Result<Option<CachedDict>, ClientError>
where
    F: FnOnce(&str) -> Option<Arc<[u8]>>,
{
    let (public_key, nonce, signature) = match (&identity.public_key, &identity.private_key) {
        (Some(pub_key), Some(priv_key)) => {
            let n = Crypto::generate_nonce()?;
            let sig = Crypto::sign_nonce(priv_key, &n)?;
            (Some(pub_key.clone()), Some(n), Some(sig))
        }
        _ => (None, None, None),
    };

    let req = IdentifyRequest {
        correlation_id: None,
        public_key,
        nonce,
        signature,
        api_key: identity.api_key.clone(),
        known_dict_sha256: known_sha,
    };

    write_identify_request(stream, &req, PROTOCOL_VERSION_V2).await?;

    let header = WireHeader::from_reader(stream, 10_000_000).await?;
    if header.message_type == IDENTIFY_RESPONSE_TYPE_ID {
        let resp = read_identify_response(header, stream).await?;
        let cached = match (resp.compression_dict_sha256, resp.compression_dict_bytes) {
            (Some(sha), Some(bytes)) => {
                Some(CachedDict { sha, bytes: Arc::from(bytes.into_boxed_slice()) })
            }
            (Some(sha), None) => {
                match dict_lookup(&sha) {
                    Some(bytes) => Some(CachedDict { sha, bytes }),
                    None => None
                }
            }
            (None, _) => None,
        };
        return Ok(cached);
    }
    let response = ClientResponse::read_from_header(header, stream).await?;
    match response {
        ClientResponse::GenericError(err) => Err(ClientError::from_error_response(err)),
        _ => Err(ClientError::ProtocolError),
    }
}

fn spawn_shard_readers(streams: Vec<ShardStream>) -> MultiShardState {
    let (tx, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut tasks = Vec::with_capacity(streams.len());

    for mut stream in streams {
        let tx = tx.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                match stream.read_next().await {
                    Ok(response) => {
                        if tx.send(Ok(response)).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        break;
                    }
                }
            }
        }));
    }
    drop(tx);

    MultiShardState { receiver, tasks, poisoned: false }
}

impl WatchConnection {
    /// Connect and establish a watch stream
    ///
    /// Tries the standard single-shard path first. If the server returns
    /// a shard routing error, automatically falls back to opening
    /// one connection per shard.
    pub async fn connect(
        address: &str,
        request: WatchRequest,
        options: WatchOptions,
    ) -> Result<Self, ClientError> {
        Self::connect_with_dict(address, request, options, None, |_| None).await
    }

    /// Like `connect` but supplies a cached dict sha and lookup closure (used by the pool).
    pub(crate) async fn connect_with_dict<F>(
        address: &str,
        request: WatchRequest,
        options: WatchOptions,
        known_sha: Option<String>,
        dict_lookup: F,
    ) -> Result<Self, ClientError>
    where
        F: Fn(&str) -> Option<Arc<[u8]>> + Clone + Send + 'static,
    {
        let max_request_size = 10_000_000;

        // If max_shard_hint is provided, skip probe and open N connections directly
        if let Some(max_shard) = options.max_shard_hint {
            let num_shards = max_shard + 1;
            return Self::connect_multi_shard(
                address,
                &request,
                &options,
                num_shards,
                max_request_size,
                known_sha,
                dict_lookup,
            )
            .await;
        }

        // Try single-shard path first
        let mut stream = crate::celeriant_client::connect_stream(
            address,
            options.timeout,
            options.tls_config.as_ref(),
        )
        .await?;

        let current_dict = if let Some(ref identity) = options.identity_config {
            identify_stream(&mut stream, identity, known_sha.clone(), dict_lookup.clone()).await?
        } else {
            None
        };

        // Send initial watch request without shard_id. Watch is fixed-size — never compressed.
        ClientRequest::write_request(
            &mut stream,
            &ClientRequest::Watch(request.clone()),
            max_request_size,
            PROTOCOL_VERSION_V2,
        )
        .await?;

        let response = crate::tokio_wire::read_response(&mut stream, max_request_size, current_dict.as_ref().map(|d| d.bytes.as_ref())).await?;

        match response {
            ClientResponse::Watch(_) => Ok(Self::new(
                WatchMode::SingleShard(ShardStream {
                    stream,
                    max_request_size,
                    current_dict,
                    partial_frame_bytes: std::sync::atomic::AtomicUsize::new(0),
                }),
                address,
            )),
            ClientResponse::GenericError(error)
                if error.error_code == error_codes::SHARD_ROUTING_MULTIPLE_SHARDS
                    || error.error_code == error_codes::SHARD_ROUTING_INCOMPATIBLE_FILTERS =>
            {
                let num_shards = Self::parse_num_shards(&error.error_message)?;

                // Reuse connection for shard 0
                let mut shard0_request = request.clone();
                shard0_request.shard_id = Some(0);

                ClientRequest::write_request(
                    &mut stream,
                    &ClientRequest::Watch(shard0_request),
                    max_request_size,
                    PROTOCOL_VERSION_V2,
                )
                .await?;

                let response = crate::tokio_wire::read_response(&mut stream, max_request_size, current_dict.as_ref().map(|d| d.bytes.as_ref())).await?;
                match response {
                    ClientResponse::Watch(_) => {}
                    ClientResponse::GenericError(error) => {
                        return Err(ClientError::from_error_response(error));
                    }
                    _ => return Err(ClientError::ProtocolError),
                }

                let shard0_stream = ShardStream {
                    stream,
                    max_request_size,
                    current_dict,
                    partial_frame_bytes: std::sync::atomic::AtomicUsize::new(0),
                };

                // Open connections for shards 1..N-1 in parallel
                let mut futures = Vec::new();
                for shard_id in 1..num_shards {
                    futures.push(Self::connect_shard(
                        address,
                        &request,
                        &options,
                        shard_id,
                        max_request_size,
                        known_sha.clone(),
                        dict_lookup.clone(),
                    ));
                }
                let results = futures_util::future::join_all(futures).await;

                let mut all_streams = vec![shard0_stream];
                for result in results {
                    all_streams.push(result?);
                }

                Ok(Self::new(
                    WatchMode::MultiShard(spawn_shard_readers(all_streams)),
                    address,
                ))
            }
            ClientResponse::GenericError(error) => Err(ClientError::from_error_response(error)),
            ClientResponse::ProtocolError(_) => Err(ClientError::ProtocolError),
            _ => Err(ClientError::ProtocolError),
        }
    }

    fn new(mode: WatchMode, address: &str) -> Self {
        Self { mode, address: address.to_string() }
    }

    /// The node this subscription is attached to.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Read the next response from the watch stream
    pub async fn next(&mut self) -> Result<WatchResponse, ClientError> {
        match &mut self.mode {
            WatchMode::SingleShard(stream) => stream.read_next().await,
            WatchMode::MultiShard(state) => {
                if state.poisoned {
                    return Err(ClientError::ProtocolError);
                }
                match state.receiver.recv().await {
                    Some(Ok(response)) => Ok(response),
                    Some(Err(e)) => {
                        state.poison();
                        Err(e)
                    }
                    None => {
                        state.poison();
                        Err(ClientError::ProtocolError)
                    }
                }
            }
        }
    }

    /// True when a read was abandoned part-way through a frame, leaving the stream mid-message
    pub fn is_desynchronised(&self) -> bool {
        match &self.mode {
            WatchMode::SingleShard(stream) => stream.partial_frame_bytes.load(std::sync::atomic::Ordering::Relaxed) > 0,
            WatchMode::MultiShard(_) => false,
        }
    }

    /// Read with a timeout. Returns None on timeout.
    pub async fn next_timeout(
        &mut self,
        duration: Duration,
    ) -> Result<Option<WatchResponse>, ClientError> {
        match timeout(duration, self.next()).await {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None),
        }
    }

    async fn connect_shard<F>(
        address: &str,
        request: &WatchRequest,
        options: &WatchOptions,
        shard_id: u64,
        max_request_size: u64,
        known_sha: Option<String>,
        dict_lookup: F,
    ) -> Result<ShardStream, ClientError>
    where
        F: FnOnce(&str) -> Option<Arc<[u8]>>,
    {
        let mut stream = crate::celeriant_client::connect_stream(
            address,
            options.timeout,
            options.tls_config.as_ref(),
        )
        .await?;

        let current_dict = if let Some(ref identity) = options.identity_config {
            identify_stream(&mut stream, identity, known_sha, dict_lookup).await?
        } else {
            None
        };

        let mut shard_request = request.clone();
        shard_request.shard_id = Some(shard_id);

        ClientRequest::write_request(
            &mut stream,
            &ClientRequest::Watch(shard_request),
            max_request_size,
            PROTOCOL_VERSION_V2,
        )
        .await?;

        let response = crate::tokio_wire::read_response(&mut stream, max_request_size, current_dict.as_ref().map(|d| d.bytes.as_ref())).await?;
        match response {
            ClientResponse::Watch(_) => Ok(ShardStream {
                stream,
                max_request_size,
                current_dict,
                partial_frame_bytes: std::sync::atomic::AtomicUsize::new(0),
            }),
            ClientResponse::GenericError(error) => Err(ClientError::from_error_response(error)),
            _ => Err(ClientError::ProtocolError),
        }
    }

    async fn connect_multi_shard<F>(
        address: &str,
        request: &WatchRequest,
        options: &WatchOptions,
        num_shards: u64,
        max_request_size: u64,
        known_sha: Option<String>,
        dict_lookup: F,
    ) -> Result<Self, ClientError>
    where
        F: Fn(&str) -> Option<Arc<[u8]>> + Clone + Send + 'static,
    {
        let mut futures = Vec::new();
        for shard_id in options.start_shard..num_shards {
            futures.push(Self::connect_shard(
                address,
                request,
                options,
                shard_id,
                max_request_size,
                known_sha.clone(),
                dict_lookup.clone(),
            ));
        }

        let results = futures_util::future::join_all(futures).await;
        let mut streams = Vec::new();
        for result in results {
            streams.push(result?);
        }

        Ok(Self::new(
            WatchMode::MultiShard(spawn_shard_readers(streams)),
            address,
        ))
    }

    fn parse_num_shards(error_message: &str) -> Result<u64, ClientError> {
        let key = "\"num_shards\":";
        let start = error_message
            .find(key)
            .ok_or(ClientError::ProtocolError)?
            + key.len();
        let rest = &error_message[start..];
        let end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        rest[..end]
            .parse::<u64>()
            .map_err(|_| ClientError::ProtocolError)
    }
}

#[cfg(test)]
mod tests {
    use celeriant_msg::process_client_responses::ClientResponse;
    use celeriant_msg::response::responses::WatchResponse;
    use celeriant_msg::response::watch_event::WatchResponseEvent;
    use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WIRE_HEADER_SIZE};
    use futures_lite::io::Cursor;

    fn test_codec() -> celeriant_wire::codec::compression::DictCodec {
        celeriant_wire::codec::compression::DictCodec::new(BUILTIN_DICT_BYTES, 3)
            .expect("builtin dict must compile")
    }

    /// Build a wire frame for a WatchResponse using ZstdDict compression with the builtin dict.
    fn make_zstd_dict_watch_frame(response: &WatchResponse) -> Vec<u8> {
        let codec = test_codec();
        futures_lite::future::block_on(async {
            let mut buf = Vec::new();
            ClientResponse::write_response(
                &mut buf,
                &ClientResponse::Watch(response.clone()),
                true,
                &codec,
                64 * 1024 * 1024,
                PROTOCOL_VERSION_V2,
            )
            .await
            .expect("write_response");
            buf
        })
    }

    #[test]
    fn shard_stream_decompresses_zstd_dict_response() {
        let expected = WatchResponse {
            events: (0u64..50).map(|i| WatchResponseEvent {
                org_id: i as u128,
                aggregate_type_id: (i + 1) as u128,
                aggregate_id: (i + 2) as u128,
                operation: 1,
                from_aggregate_version: Some(i),
                to_aggregate_version: Some(i + 10),
                keep_from_aggregate_version: None,
            }).collect(),
        };

        let frame = make_zstd_dict_watch_frame(&expected);

        let compression_byte = frame[16];
        if compression_byte != 1 {
            let parsed = futures_lite::future::block_on(async {
                let header = celeriant_wire::network::wire_header::WireHeader::from_reader(
                    &mut Cursor::new(frame.clone()),
                    u64::MAX,
                ).await.expect("header");
                crate::tokio_wire::read_from_header(
                    header,
                    &mut Cursor::new(frame[WIRE_HEADER_SIZE..].to_vec()),
                    Some(BUILTIN_DICT_BYTES),
                ).await.expect("tokio_wire::read_from_header")
            });
            assert!(matches!(parsed, ClientResponse::Watch(_)));
            return;
        }

        let with_dict = futures_lite::future::block_on(async {
            let header = celeriant_wire::network::wire_header::WireHeader::from_reader(
                &mut Cursor::new(frame.clone()),
                u64::MAX,
            ).await.expect("header");
            crate::tokio_wire::read_from_header(
                header,
                &mut Cursor::new(frame[WIRE_HEADER_SIZE..].to_vec()),
                Some(BUILTIN_DICT_BYTES),
            ).await.expect("tokio_wire::read_from_header")
        });
        match with_dict {
            ClientResponse::Watch(parsed) => {
                assert_eq!(parsed.events.len(), expected.events.len());
            }
            other => panic!("expected Watch, got {:?}", other.response_type()),
        }
    }

    // ---- Cancellation of a watch read ----
    //
    // `next_timeout` is `timeout(duration, self.next())`. When the deadline
    // lands inside a partially-read frame the bytes already consumed are gone,
    // and `Ok(None)` reports "nothing arrived" for a stream that is now offset.

    use crate::watch_connection::{WatchConnection, WatchOptions};
    use celeriant_msg::request::requests::WatchRequest;
    use std::time::Duration;

    const WATCH_MAX: u64 = 10_000_000;

    /// Uncompressed, because the test connection carries no identity and so has
    /// no dict to decompress with.
    fn watch_frame(response: &WatchResponse) -> Vec<u8> {
        let codec = test_codec();
        futures_lite::future::block_on(async {
            let mut buf = Vec::new();
            ClientResponse::write_response(
                &mut buf,
                &ClientResponse::Watch(response.clone()),
                false,
                &codec,
                64 * 1024 * 1024,
                PROTOCOL_VERSION_V2,
            )
            .await
            .expect("write_response");
            buf
        })
    }

    fn watch_events(count: u64) -> WatchResponse {
        WatchResponse {
            events: (0..count)
                .map(|i| WatchResponseEvent {
                    org_id: i as u128,
                    aggregate_type_id: 1,
                    aggregate_id: i as u128,
                    operation: 1,
                    from_aggregate_version: Some(i),
                    to_aggregate_version: Some(i + 1),
                    keep_from_aggregate_version: None,
                })
                .collect(),
        }
    }

    fn watch_request() -> WatchRequest {
        WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        }
    }

    /// A watch peer that hands the test byte-level control of the push stream.
    /// Everything after the connect handshake comes from the channel, so a
    /// frame can be cut anywhere and left unfinished for as long as the test
    /// likes.
    async fn scripted_watch_server() -> (std::net::SocketAddr, tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
        use celeriant_wire::network::wire_header::WireHeader;
        use futures_lite::{AsyncReadExt, AsyncWriteExt};
        use tokio_util::compat::TokioAsyncReadCompatExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (script, mut pending) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let ack = watch_frame(&WatchResponse { events: Vec::new() });
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = socket.compat();
            // Drain the client's WatchRequest and acknowledge it, which is all
            // `connect` needs to settle on the single-shard path.
            let header = WireHeader::from_reader(&mut stream, WATCH_MAX).await.unwrap();
            let mut body = vec![0u8; header.compressed_length as usize];
            stream.read_exact(&mut body).await.unwrap();
            stream.write_all(&ack).await.unwrap();
            stream.flush().await.unwrap();
            while let Some(bytes) = pending.recv().await {
                if stream.write_all(&bytes).await.is_err() || stream.flush().await.is_err() {
                    return;
                }
            }
        });
        (addr, script)
    }

    #[tokio::test]
    async fn a_watch_read_cancelled_mid_frame_must_not_hand_back_the_next_frame() {
        let (addr, wire) = scripted_watch_server().await;
        let mut watch = WatchConnection::connect(&addr.to_string(), watch_request(), WatchOptions::default())
            .await
            .expect("the scripted peer completes the watch handshake");

        let split = watch_frame(&watch_events(4000));
        let marker = watch_frame(&watch_events(3));
        assert!(
            split.len() > WIRE_HEADER_SIZE + 64 + marker.len(),
            "the split frame must stay outstanding by more than everything sent after it"
        );

        // A whole header and 64 body bytes, then silence. Nothing further is
        // ever sent for this frame, so the read below parks having consumed
        // exactly those bytes: the deadline decides when the cancel happens,
        // never what was consumed by then.
        wire.send(split[..WIRE_HEADER_SIZE + 64].to_vec()).unwrap();

        assert!(
            matches!(watch.next_timeout(Duration::from_millis(300)).await, Ok(None)),
            "a deadline that expires with no complete frame must report that nothing arrived"
        );

        // The remainder of the split frame is abandoned. What follows is a
        // complete, valid frame beginning exactly where the cancelled read
        // stopped, so the desync never announces itself as a bad header.
        wire.send(marker).unwrap();

        match watch.next_timeout(Duration::from_secs(5)).await {
            Err(_) => {}
            Ok(None) => panic!(
                "test premise not met: the timed-out read consumed nothing, so no frame was split"
            ),
            Ok(Some(response)) => panic!(
                "a stream desynchronised by a cancelled read returned {} events as if they were \
                 the next ones on the watch",
                response.events.len()
            ),
        }
    }

    use super::{MultiShardState, WatchMode};

    /// The address is how a caller notices its subscription moved node — a watch
    /// that moved restarted from the new node's tip and may have missed
    /// notifications in between.
    #[test]
    fn the_connected_address_is_reported() {
        let w = WatchConnection::new(
            WatchMode::MultiShard(MultiShardState {
                receiver: tokio::sync::mpsc::unbounded_channel().1,
                tasks: Vec::new(),
                poisoned: false,
            }),
            "10.0.0.1:12000",
        );
        assert_eq!(w.address(), "10.0.0.1:12000");
    }

    /// One shard reader dying must kill the whole watch. Without this, events
    /// from the surviving shards keep flowing after the error and a caller that
    /// treats it as transient reads on, silently blind to one shard.
    #[tokio::test]
    async fn a_multi_shard_watch_is_poisoned_by_the_first_shard_error() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut w = WatchConnection::new(
            WatchMode::MultiShard(MultiShardState { receiver: rx, tasks: Vec::new(), poisoned: false }),
            "10.0.0.1:12000",
        );
        tx.send(Ok(watch_events(1))).unwrap();
        tx.send(Err(crate::client_error::ClientError::ProtocolError)).unwrap();
        // A healthy shard's event, already queued behind the failure.
        tx.send(Ok(watch_events(2))).unwrap();

        assert!(w.next().await.is_ok(), "events received before the failure are genuine");
        assert!(w.next().await.is_err(), "the shard failure must surface");
        assert!(
            w.next().await.is_err(),
            "the queued healthy event must not be delivered: the watch is half-blind and must stay dead"
        );
    }

    /// `is_desynchronised()` is the only way a caller can ask, since
    /// `next_timeout` deliberately keeps returning `Ok(None)` on a mid-frame
    /// cancel. A version that always answered `false` would pass every other
    /// test in this file.
    #[tokio::test]
    async fn is_desynchronised_reports_the_mid_frame_cancel_it_exists_for() {
        let (addr, wire) = scripted_watch_server().await;
        let mut watch = WatchConnection::connect(&addr.to_string(), watch_request(), WatchOptions::default())
            .await
            .expect("the scripted peer completes the watch handshake");
        assert!(!watch.is_desynchronised(), "a fresh watch is on a message boundary");

        let split = watch_frame(&watch_events(4000));
        wire.send(split[..WIRE_HEADER_SIZE + 64].to_vec()).unwrap();
        assert!(
            matches!(watch.next_timeout(Duration::from_millis(300)).await, Ok(None)),
            "test premise: the deadline must expire with the frame incomplete"
        );

        assert!(
            watch.is_desynchronised(),
            "the read stopped part-way through a frame and nothing else reports it"
        );
    }

    /// Decompression and deserialisation happen after the frame is off the
    /// socket, so failing there leaves the reader on a message boundary. Arming
    /// the counter on those errors refuses a connection that is perfectly
    /// usable — the same shape as arming the pooled client's dirty flag on a
    /// zero-byte local failure.
    #[tokio::test]
    async fn a_decode_error_on_a_fully_consumed_frame_leaves_the_watch_usable() {
        let (addr, wire) = scripted_watch_server().await;
        let mut watch = WatchConnection::connect(&addr.to_string(), watch_request(), WatchOptions::default())
            .await
            .expect("the scripted peer completes the watch handshake");

        // A complete, correctly-framed message whose compression byte says
        // ZstdDict. `read_variable_body_raw` consumes every byte of the body
        // before `read_from_header` discovers this client has no cached dict,
        // so the socket ends up exactly on the next message boundary.
        let mut undecodable = watch_frame(&watch_events(3));
        undecodable[16] = celeriant_wal::compression_type::CompressionType::ZstdDict.to_byte();
        wire.send(undecodable).unwrap();

        assert!(
            watch.next_timeout(Duration::from_secs(5)).await.is_err(),
            "test premise: the frame must fail to decode"
        );

        // The stream is clean. Before phase 3 this frame was delivered.
        wire.send(watch_frame(&watch_events(3))).unwrap();
        match watch.next_timeout(Duration::from_secs(5)).await {
            Ok(Some(r)) => assert_eq!(r.events.len(), 3),
            other => panic!(
                "a decode error consumed the whole frame, leaving the stream on a message \
                 boundary, yet the connection is permanently unusable: {other:?}"
            ),
        }
    }

    /// Guard against a fix that retires every timed-out watch. This one PASSES
    /// today; it exists so that "mark the connection unusable" cannot be
    /// implemented as "mark the connection unusable on every expiry".
    #[tokio::test]
    async fn a_watch_timeout_with_nothing_on_the_wire_leaves_the_connection_usable() {
        let (addr, wire) = scripted_watch_server().await;
        let mut watch = WatchConnection::connect(&addr.to_string(), watch_request(), WatchOptions::default())
            .await
            .expect("the scripted peer completes the watch handshake");

        assert!(
            matches!(watch.next_timeout(Duration::from_millis(50)).await, Ok(None)),
            "nothing was sent, so the deadline must expire empty"
        );

        wire.send(watch_frame(&watch_events(3))).unwrap();

        match watch.next_timeout(Duration::from_secs(5)).await {
            Ok(Some(response)) => assert_eq!(response.events.len(), 3),
            other => panic!("a clean timeout must not cost the connection: {other:?}"),
        }
    }
}
