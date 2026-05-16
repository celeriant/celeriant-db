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

struct ShardStream {
    stream: crate::celeriant_client::ClientStream,
    max_request_size: u64,
    current_dict: Option<CachedDict>,
}

impl ShardStream {
    async fn read_next(&mut self) -> Result<WatchResponse, ClientError> {
        let dict = self.current_dict.as_ref().map(|d| d.bytes.as_ref());
        let response = crate::tokio_wire::read_response(&mut self.stream, self.max_request_size, dict).await?;
        match response {
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
}

enum WatchMode {
    SingleShard(ShardStream),
    MultiShard(MultiShardState),
}

struct MultiShardState {
    receiver: tokio::sync::mpsc::UnboundedReceiver<Result<WatchResponse, ClientError>>,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
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

    MultiShardState { receiver, _tasks: tasks }
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
            ClientResponse::Watch(_) => Ok(Self {
                mode: WatchMode::SingleShard(ShardStream {
                    stream,
                    max_request_size,
                    current_dict,
                }),
            }),
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

                Ok(Self {
                    mode: WatchMode::MultiShard(spawn_shard_readers(all_streams)),
                })
            }
            ClientResponse::GenericError(error) => Err(ClientError::from_error_response(error)),
            ClientResponse::ProtocolError(_) => Err(ClientError::ProtocolError),
            _ => Err(ClientError::ProtocolError),
        }
    }

    /// Read the next response from the watch stream
    pub async fn next(&mut self) -> Result<WatchResponse, ClientError> {
        match &mut self.mode {
            WatchMode::SingleShard(stream) => stream.read_next().await,
            WatchMode::MultiShard(state) => state
                .receiver
                .recv()
                .await
                .ok_or(ClientError::ProtocolError)?,
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

        Ok(Self {
            mode: WatchMode::MultiShard(spawn_shard_readers(streams)),
        })
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
                from_event_batch_index: Some(i),
                to_event_batch_index: Some(i + 10),
                keep_from_event_batch_index: None,
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
}
