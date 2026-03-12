use celeriant_crypto::Crypto;
use celeriant_msg::error_codes;
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::process_identify::{IDENTIFY_RESPONSE_TYPE_ID, read_identify_response, write_identify_request};
use celeriant_msg::request::requests::{IdentifyRequest, WatchRequest};
use celeriant_msg::response::responses::WatchResponse;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WireHeader};
use tokio::time::{timeout, Duration};

use crate::celeriant_client::{ClientIdentityConfig, ClientStream, ClientTlsConfig};
use crate::client_error::ClientError;

/// Options for configuring a watch connection
#[derive(Clone)]
pub struct WatchOptions {
    pub compression: CompressionType,
    pub timeout: Option<Duration>,
    pub start_shard: u64,
    pub max_shard_hint: Option<u64>,
    pub tls_config: Option<ClientTlsConfig>,
    pub identity_config: Option<ClientIdentityConfig>,
}

impl Default for WatchOptions {
    fn default() -> Self {
        Self {
            compression: CompressionType::None,
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
}

impl ShardStream {
    async fn read_next(&mut self) -> Result<WatchResponse, ClientError> {
        let response = ClientResponse::read_response(&mut self.stream, self.max_request_size).await?;

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

async fn identify_stream(
    stream: &mut ClientStream,
    identity: &ClientIdentityConfig,
) -> Result<(), ClientError> {
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
    };

    write_identify_request(stream, &req, PROTOCOL_VERSION_V2).await?;

    let header = WireHeader::from_reader(stream, 10_000_000).await?;
    if header.message_type == IDENTIFY_RESPONSE_TYPE_ID {
        read_identify_response(header, stream).await?;
        return Ok(());
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

        if let Some(ref identity) = options.identity_config {
            identify_stream(&mut stream, identity).await?;
        }

        // Send initial watch request without shard_id
        ClientRequest::write_request(
            &mut stream,
            &ClientRequest::Watch(request.clone()),
            options.compression,
            max_request_size,
            PROTOCOL_VERSION_V2,
        )
        .await?;

        let response = ClientResponse::read_response(&mut stream, max_request_size).await?;

        match response {
            ClientResponse::Watch(_) => Ok(Self {
                mode: WatchMode::SingleShard(ShardStream {
                    stream,
                    max_request_size,
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
                    options.compression,
                    max_request_size,
                    PROTOCOL_VERSION_V2,
                )
                .await?;

                let response = ClientResponse::read_response(&mut stream, max_request_size).await?;
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

    async fn connect_shard(
        address: &str,
        request: &WatchRequest,
        options: &WatchOptions,
        shard_id: u64,
        max_request_size: u64,
    ) -> Result<ShardStream, ClientError> {
        let mut stream = crate::celeriant_client::connect_stream(
            address,
            options.timeout,
            options.tls_config.as_ref(),
        )
        .await?;

        if let Some(ref identity) = options.identity_config {
            identify_stream(&mut stream, identity).await?;
        }

        let mut shard_request = request.clone();
        shard_request.shard_id = Some(shard_id);

        ClientRequest::write_request(
            &mut stream,
            &ClientRequest::Watch(shard_request),
            options.compression,
            max_request_size,
            PROTOCOL_VERSION_V2,
        )
        .await?;

        let response = ClientResponse::read_response(&mut stream, max_request_size).await?;
        match response {
            ClientResponse::Watch(_) => Ok(ShardStream {
                stream,
                max_request_size,
            }),
            ClientResponse::GenericError(error) => Err(ClientError::from_error_response(error)),
            _ => Err(ClientError::ProtocolError),
        }
    }

    async fn connect_multi_shard(
        address: &str,
        request: &WatchRequest,
        options: &WatchOptions,
        num_shards: u64,
        max_request_size: u64,
    ) -> Result<Self, ClientError> {
        let mut futures = Vec::new();
        for shard_id in options.start_shard..num_shards {
            futures.push(Self::connect_shard(
                address,
                request,
                options,
                shard_id,
                max_request_size,
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
