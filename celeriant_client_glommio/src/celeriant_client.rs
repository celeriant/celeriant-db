use std::rc::Rc;
use std::sync::Arc;

use celeriant_ktls::ktls_connect;
use celeriant_msg::process_cluster_requests::ClusterRequest;
use celeriant_msg::process_cluster_responses::ClusterResponse;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::network::wire_error::WireError;
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
    stream_dirty: bool,
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
            Some(cfg) => {
                let (stream, trailing) = ktls_connect(stream, cfg.client_config, cfg.server_name)
                    .await
                    .map_err(ClientError::KtlsError)?;
                debug_assert!(
                    trailing.is_empty(),
                    "server sent {} bytes before the client's first request; they are being dropped",
                    trailing.len()
                );
                stream
            }
        };

        Ok(Self {
            stream,
            max_request_size,
            max_response_size,
            stream_dirty: false,
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

    /// True when a request went out whose response was never fully read. Such a
    /// connection must be reset before reuse, never sent another request.
    pub fn is_stream_dirty(&self) -> bool {
        self.stream_dirty
    }

    async fn send_cluster_request_inner(
        &mut self,
        request: &ClusterRequest,
    ) -> Result<ClusterResponse, ClientError> {
        //Defence in depth, ensure_connected handles it primarily
        if self.stream_dirty {
            return Err(ClientError::RequestProtocolError);
        }

        // Only variable-size variants (ReplicationBatch) can be compressed.
        // Fixed-size variants (Heartbeat, KickFollower) always use None.
        let compression = match request {
            ClusterRequest::ReplicationBatch(_) => CompressionType::ZstdDict,
            _ => CompressionType::None,
        };

        self.stream_dirty = true;
        if let Err(e) = ClusterRequest::write_request(
            &mut self.stream,
            request,
            compression,
            self.max_request_size,
            PROTOCOL_VERSION_V2,
            &self.dict_codec,
        )
        .await
        {
            // Only a socket error can have put bytes on the wire; the rest are
            // raised by validation or serialisation before the first write.
            self.stream_dirty = matches!(e, WireError::NetworkError(_));
            return Err(ClientError::WriteRequestError(e));
        }

        let response = ClusterResponse::read_response(&mut self.stream, self.max_response_size)
            .await
            .map_err(ClientError::ReadResponseError)?;
        self.stream_dirty = false;

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

/// Cancellation contract for cluster requests.
///
/// A `send_cluster_request` future that is dropped between the write and the
/// read leaves the response sitting in the socket. These tests drive that exact
/// state and then ask the client for something else.
#[cfg(test)]
mod cluster_cancellation_tests {
    use super::*;
    use celeriant_msg::request::requests::{HeartbeatRequest, KickFollowerRequest};
    use celeriant_msg::response::responses::{HeartbeatResponse, HeartbeatResult, KickFollowerResponse};
    use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
    use celeriant_wire::network::wire_header::WireHeader;
    use futures_lite::AsyncWriteExt;
    use futures_lite::future::poll_once;
    use glommio::{LocalExecutorBuilder, Placement};
    use std::cell::{Cell, RefCell};

    const MAX: u64 = 4 * 1024 * 1024;

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    /// A stand-in follower running on the test's own executor.
    ///
    /// `gate` holds every answer back until the test opens it, so a request can
    /// be abandoned at a point where the write has provably landed — the
    /// follower read the frame — and not one byte of the response exists yet.
    /// No wall clock decides anything.
    #[derive(Clone)]
    struct MockFollower {
        read: Rc<RefCell<Vec<ClusterRequest>>>,
        answered: Rc<Cell<usize>>,
        gate: Rc<Cell<bool>>,
    }

    fn matching_response(request: &ClusterRequest) -> ClusterResponse {
        match request {
            ClusterRequest::KickFollower(r) => ClusterResponse::KickFollower(KickFollowerResponse {
                correlation_id: r.correlation_id,
                acknowledged: true,
            }),
            ClusterRequest::Heartbeat(r) => ClusterResponse::Heartbeat(HeartbeatResponse {
                correlation_id: r.correlation_id,
                result: HeartbeatResult::Ack {
                    follower_timestamp_ms: 1,
                    follower_can_accept_tcp_replication: true,
                },
            }),
            ClusterRequest::ReplicationBatch(_) => unreachable!("these tests only send fixed-size requests"),
        }
    }

    fn spawn_follower(codec: Rc<DictCodec>) -> (String, MockFollower) {
        let listener = glommio::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let follower = MockFollower {
            read: Rc::new(RefCell::new(Vec::new())),
            answered: Rc::new(Cell::new(0)),
            gate: Rc::new(Cell::new(true)),
        };
        let server = follower.clone();
        glommio::spawn_local(async move {
            while let Ok(mut stream) = listener.accept().await {
                loop {
                    let Ok(header) = WireHeader::from_reader(&mut stream, MAX).await else { break };
                    let Ok(request) = ClusterRequest::read_from_header(header, &mut stream, &codec).await else { break };
                    let response = matching_response(&request);
                    server.read.borrow_mut().push(request);
                    while !server.gate.get() {
                        park_briefly().await;
                    }
                    if ClusterResponse::write_response(&mut stream, &response, MAX, PROTOCOL_VERSION_V2).await.is_err() {
                        break;
                    }
                    let _ = stream.flush().await;
                    server.answered.set(server.answered.get() + 1);
                }
            }
        })
        .detach();
        (address, follower)
    }

    /// A real park, short enough to be irrelevant to any assertion. A task that
    /// re-wakes itself with `yield_now` never empties glommio's run queue, and
    /// the reactor only drains its io_uring completions when it does.
    async fn park_briefly() {
        glommio::timer::sleep(Duration::from_micros(50)).await;
    }

    fn test_codec() -> Rc<DictCodec> {
        Rc::new(DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile"))
    }

    async fn connect(address: &str, codec: Rc<DictCodec>) -> CeleriantClient {
        CeleriantClient::connect_with_timeout_tls(address, None, MAX, MAX, None, None, codec)
            .await
            .expect("the mock follower accepts connections")
    }

    fn heartbeat(correlation_id: u128) -> ClusterRequest {
        ClusterRequest::Heartbeat(HeartbeatRequest {
            correlation_id: Some(correlation_id),
            shard_id: 0,
            leader_timestamp_ms: 1,
            lease_epoch: 1,
        })
    }

    fn kick(correlation_id: u128) -> ClusterRequest {
        ClusterRequest::KickFollower(KickFollowerRequest { correlation_id: Some(correlation_id) })
    }

    /// Write `request`, then abandon it parked on the response read.
    ///
    /// The follower is gated shut first, so the only thing that can end the
    /// polling loop is the follower reporting that it read the whole request
    /// frame. That is the write completing, observed from the far end. The gate
    /// then opens and the response lands in a socket nobody is reading.
    async fn abandon_after_write(client: &mut CeleriantClient, follower: &MockFollower, request: &ClusterRequest) {
        let read_before = follower.read.borrow().len();
        let answered_before = follower.answered.get();
        follower.gate.set(false);
        {
            let mut request_future = std::pin::pin!(client.send_cluster_request(request));
            for _ in 0..100_000 {
                if follower.read.borrow().len() > read_before {
                    break;
                }
                assert!(
                    poll_once(request_future.as_mut()).await.is_none(),
                    "the request ended before it could be abandoned"
                );
                park_briefly().await;
            }
            assert!(
                follower.read.borrow().len() > read_before,
                "the request frame never reached the follower, so nothing was abandoned mid-flight"
            );
        }
        follower.gate.set(true);
        for _ in 0..100_000 {
            if follower.answered.get() > answered_before {
                break;
            }
            park_briefly().await;
        }
        assert!(
            follower.answered.get() > answered_before,
            "the follower never wrote the answer to the abandoned request"
        );
    }

    /// Cross-variant crosstalk: the loud half of the bug.
    #[test]
    fn a_cancelled_cluster_request_does_not_answer_the_next_one() {
        glommio_test!({
            let codec = test_codec();
            let (address, follower) = spawn_follower(codec.clone());
            let mut client = connect(&address, codec).await;

            // Warm the connection so the abandoned request starts at the write.
            assert!(matches!(
                client.send_cluster_request(&heartbeat(1)).await,
                Ok(ClusterResponse::Heartbeat(_))
            ));

            abandon_after_write(&mut client, &follower, &kick(2)).await;

            let outcome = client.send_cluster_request(&heartbeat(3)).await;
            assert!(
                !matches!(outcome, Ok(ClusterResponse::KickFollower(_))),
                "the heartbeat was answered with the abandoned kick's response"
            );
        });
    }

    /// Same-variant crosstalk: the silent half, and the one that reached
    /// production. Nothing about the response looks wrong except whose it is.
    #[test]
    fn a_cancelled_cluster_request_does_not_answer_a_later_request_of_its_own_type() {
        glommio_test!({
            let codec = test_codec();
            let (address, follower) = spawn_follower(codec.clone());
            let mut client = connect(&address, codec).await;

            assert!(matches!(
                client.send_cluster_request(&heartbeat(1)).await,
                Ok(ClusterResponse::Heartbeat(_))
            ));

            abandon_after_write(&mut client, &follower, &heartbeat(2)).await;

            if let Ok(ClusterResponse::Heartbeat(response)) = client.send_cluster_request(&heartbeat(3)).await {
                assert_ne!(
                    response.correlation_id,
                    Some(2),
                    "heartbeat 3 was handed heartbeat 2's response"
                );
            }
        });
    }

    /// Guard against a fix that retires everything. This one PASSES today; it
    /// exists so that "mark the stream unusable" cannot be implemented as
    /// "mark the stream unusable after every request".
    #[test]
    fn a_completed_cluster_request_leaves_the_client_usable() {
        glommio_test!({
            let codec = test_codec();
            let (address, _follower) = spawn_follower(codec.clone());
            let mut client = connect(&address, codec).await;

            for id in 1..=3u128 {
                match client.send_cluster_request(&heartbeat(id)).await {
                    Ok(ClusterResponse::Heartbeat(response)) => {
                        assert_eq!(response.correlation_id, Some(id));
                    }
                    other => panic!("request {id} on a clean stream failed: {other:?}"),
                }
            }
        });
    }
}
