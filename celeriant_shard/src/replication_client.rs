use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use celeriant_client_glommio::{CeleriantClient, ClientError, GlommioTlsConfig};
use celeriant_disk::files::rwlock_timeout::write_with_timeout;
use celeriant_distributed::paths::fallback_batch_path;
use celeriant_msg::{
    process_cluster_requests::ClusterRequest,
    process_cluster_responses::ClusterResponse,
    request::requests::{HeartbeatRequest, KickFollowerRequest, ReplicationBatchItem, ReplicationBatchRequest},
    response::responses::{HeartbeatResult, ReplicationResult},
};
use celeriant_wal::s3::fallback_batch::{FallbackBatch, FallbackItem};
use glommio::sync::RwLock;
use tracing::debug;

use celeriant_wire::codec::compression::DictCodec;
use crate::error::{replication_to_follower_error::ReplicateToFollowerError, replication_to_s3_error::ReplicateToS3Error, send_heartbeat_error::SendHeartbeatError};
use crate::s3_uploader::S3Uploader;

#[allow(async_fn_in_trait)]
pub trait ReplicationClient {
    fn set_follower_address(&self, address: Option<String>);
    fn set_follower_reachable(&self, reachable: bool);
    fn is_follower_reachable(&self) -> bool;
    fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64>;
    fn set_heartbeat_in_flight(&self, unix_ms: Option<u64>);
    fn reset_heartbeat_state(&self);
    fn try_acquire_kick(&self) -> bool { true }
    fn release_kick(&self) {}
    async fn replicate_to_follower(&self, batches: Vec<ReplicationBatchItem>, leader_confirmed_wal_index: u64) -> Result<(), ReplicateToFollowerError>;
    async fn replicate_to_s3(&self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error>;
    async fn send_heartbeat(&self, unix_epoch_now_ms: u64, lease_index: u64) -> Result<HeartbeatResult, SendHeartbeatError>;
    async fn send_kick(&self) -> Result<bool, SendHeartbeatError>;
}

pub struct StubReplicationClient;

impl ReplicationClient for StubReplicationClient {
    fn set_follower_address(&self, _address: Option<String>) {}
    fn set_follower_reachable(&self, _reachable: bool) {}
    fn is_follower_reachable(&self) -> bool { true }
    fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> { None }
    fn set_heartbeat_in_flight(&self, _unix_ms: Option<u64>) {}
    fn reset_heartbeat_state(&self) {}

    async fn replicate_to_follower(&self, _batches: Vec<ReplicationBatchItem>, _leader_confirmed_wal_index: u64) -> Result<(), ReplicateToFollowerError> {
        glommio::timer::sleep(std::time::Duration::from_millis(30)).await;
        Ok(())
    }

    async fn replicate_to_s3(&self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
        glommio::timer::sleep(std::time::Duration::from_millis(230)).await;
        Ok(())
    }

    async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _lease_index: u64) -> Result<HeartbeatResult, SendHeartbeatError> {
        glommio::timer::sleep(std::time::Duration::from_millis(100)).await;
        Ok(HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms + 100, follower_can_accept_tcp_replication: true })
    }

    async fn send_kick(&self) -> Result<bool, SendHeartbeatError> { Ok(true) }
}

struct ConnState {
    client: Option<CeleriantClient>,
    connected_to: Option<String>,
}

impl ConnState {
    fn new() -> Self {
        Self { client: None, connected_to: None }
    }

    async fn ensure_connected(
        &mut self,
        address: &Option<String>,
        reset: bool,
        connection_timeout: Option<Duration>,
        request_timeout: Option<Duration>,
        max_request_size: u64,
        max_response_size: u64,
        replication_client_config: Option<&Arc<rustls::ClientConfig>>,
        tcp_user_timeout: Option<Duration>,
        dict_codec: Rc<DictCodec>,
    ) -> Result<(), ClientError> {
        if self.connected_to.as_ref() != address.as_ref() {
            self.client = None;
            self.connected_to = None;
        }
        if reset {
            if let Some(client) = self.client.take() {
                client.close().await?;
            }
            self.connected_to = None;
        }
        if self.client.is_none() {
            let addr = address.as_deref().ok_or(ClientError::NoAddress)?;
            let has_tls = replication_client_config.is_some();
            debug!(addr, has_tls, reset, "internode: connecting");
            let tls_config = replication_client_config
                .map(|c| GlommioTlsConfig::from_address(c.clone(), addr))
                .transpose()
                .map_err(|e| ClientError::ConnectionFailed(glommio::GlommioError::IoError(
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                )))?;
            let client = CeleriantClient::connect_with_timeout_tls(
                addr,
                connection_timeout,
                max_request_size,
                max_response_size,
                tls_config,
                tcp_user_timeout,
                dict_codec,
            )
            .await?;
            debug!(addr, "internode: connected");
            self.client = Some(match request_timeout {
                Some(t) => client.with_timeout(t),
                None => client,
            });
            self.connected_to = address.clone();
        }
        Ok(())
    }
}

pub struct FollowerConnection<S: S3Uploader> {
    follower_address: RefCell<Option<String>>,
    follower_reachable: Cell<bool>,
    kick_in_flight: Cell<bool>,
    node_id: u128,
    shard_id: u64,
    replication_conn: RwLock<ConnState>,
    heartbeat_conn: RwLock<ConnState>,
    connection_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    heartbeat_timeout: Duration,
    tcp_user_timeout: Option<Duration>,
    max_request_size: u64,
    max_response_size: u64,
    replication_client_config: Option<Arc<rustls::ClientConfig>>,
    dict_codec: Rc<DictCodec>,
    s3_uploader: Option<S>,
    s3_upload_sequence: Cell<u64>,
    heartbeat_in_flight_since_unix_ms: Cell<Option<u64>>,
}

impl<S: S3Uploader> FollowerConnection<S> {
    pub fn new(
        follower_address: Option<String>,
        connection_timeout: Option<Duration>,
        request_timeout: Option<Duration>,
        heartbeat_timeout: Duration,
        tcp_user_timeout: Option<Duration>,
        max_request_size: u64,
        max_response_size: u64,
        shard_id: u64,
        node_id: u128,
        replication_client_config: Option<Arc<rustls::ClientConfig>>,
        dict_codec: Rc<DictCodec>,
        s3_uploader: Option<S>,
    ) -> Self {
        assert!(shard_id <= u32::MAX as u64, "shard_id {} exceeds u32::MAX", shard_id);
        Self {
            follower_address: RefCell::new(follower_address),
            follower_reachable: Cell::new(false),
            kick_in_flight: Cell::new(false),
            node_id,
            shard_id,
            replication_conn: RwLock::new(ConnState::new()),
            heartbeat_conn: RwLock::new(ConnState::new()),
            connection_timeout,
            request_timeout,
            heartbeat_timeout,
            tcp_user_timeout,
            max_request_size,
            max_response_size,
            replication_client_config,
            dict_codec,
            s3_uploader,
            s3_upload_sequence: Cell::new(0),
            heartbeat_in_flight_since_unix_ms: Cell::new(None),
        }
    }
}

impl<S: S3Uploader> ReplicationClient for FollowerConnection<S> {
    fn set_follower_address(&self, address: Option<String>) {
        *self.follower_address.borrow_mut() = address;
    }

    fn set_follower_reachable(&self, reachable: bool) {
        self.follower_reachable.set(reachable);
    }

    fn is_follower_reachable(&self) -> bool {
        self.follower_reachable.get()
    }

    fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> {
        self.heartbeat_in_flight_since_unix_ms.get()
    }

    fn set_heartbeat_in_flight(&self, unix_ms: Option<u64>) {
        self.heartbeat_in_flight_since_unix_ms.set(unix_ms);
    }

    fn reset_heartbeat_state(&self) {
        self.heartbeat_in_flight_since_unix_ms.set(None);
    }

    fn try_acquire_kick(&self) -> bool {
        if self.kick_in_flight.get() {
            false
        } else {
            self.kick_in_flight.set(true);
            true
        }
    }

    fn release_kick(&self) {
        self.kick_in_flight.set(false);
    }

    async fn replicate_to_follower(&self, batches: Vec<ReplicationBatchItem>, leader_confirmed_wal_index: u64) -> Result<(), ReplicateToFollowerError> {
        if batches.is_empty() {
            return Ok(());
        }

        let mut guard = write_with_timeout(&self.replication_conn, "replicate_to_follower").await
            .map_err(|_| ReplicateToFollowerError::LockTimeout)?;

        let address = self.follower_address.borrow().clone();
        let shard_id = self.shard_id;

        guard.ensure_connected(&address, false, self.connection_timeout, self.request_timeout, self.max_request_size, self.max_response_size, self.replication_client_config.as_ref(), self.tcp_user_timeout, self.dict_codec.clone()).await?;

        let mut request = ClusterRequest::ReplicationBatch(ReplicationBatchRequest {
            correlation_id: None,
            shard_id,
            leader_timestamp_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            leader_confirmed_wal_index,
            batches,
        });

        let response = match guard.client.as_mut().unwrap().send_cluster_request(&request).await {
            Ok(r) => r,
            Err(_) => {
                guard.ensure_connected(&address, true, self.connection_timeout, self.request_timeout, self.max_request_size, self.max_response_size, self.replication_client_config.as_ref(), self.tcp_user_timeout, self.dict_codec.clone()).await?;
                if let ClusterRequest::ReplicationBatch(ref mut req) = request {
                    req.leader_timestamp_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
                }
                guard.client.as_mut().unwrap().send_cluster_request(&request).await?
            }
        };

        match response {
            ClusterResponse::ReplicationBatch(resp) => match resp.result {
                ReplicationResult::Success { .. } => Ok(()),
                ReplicationResult::Rejected(rejection) => Err(ReplicateToFollowerError::FollowerRejected(rejection)),
            },
            _ => Err(ReplicateToFollowerError::FollowerUnexpectedResponse),
        }
    }

    async fn replicate_to_s3(&self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
        if batches.is_empty() {
            return Ok(());
        }

        // Refuse to upload a batch with internal wal_index gaps. A gap here means
        // upstream code produced items from two chain generations in one workset —
        // a latent bug that would plant an unrecoverable file in S3 and panic any
        // follower that consumes it. Fail loudly at the source instead.
        if let Some((i, w)) = batches.windows(2).enumerate().find(|(_, w)| w[0].metablock.wal_index + 1 != w[1].metablock.wal_index) {
            let expected = w[0].metablock.wal_index + 1;
            let actual = w[1].metablock.wal_index;
            tracing::error!(shard_id = self.shard_id, at_index = i + 1, expected, actual, "refusing S3 upload: batch has internal wal_index gap");
            return Err(ReplicateToS3Error::BatchNotContiguous { at_index: i + 1, expected_wal_index: expected, actual_wal_index: actual });
        }

        let s3_uploader = self.s3_uploader.as_ref()
            .ok_or(ReplicateToS3Error::S3NotConfigured)?;

        let fallback_index = batches[0].metablock.wal_index;
        let end_wal_index = batches.last().unwrap().metablock.wal_index;
        let lease_index = batches[0].metablock.lease_index;
        if let Some((i, b)) = batches.iter().enumerate().skip(1).find(|(_, b)| b.metablock.lease_index != lease_index) {
            tracing::error!(shard_id = self.shard_id, at_index = i, first = lease_index, found = b.metablock.lease_index, "refusing S3 upload: batch spans multiple lease_indexes");
            return Err(ReplicateToS3Error::LeaseIndexInconsistent { at_index: i, first: lease_index, found: b.metablock.lease_index });
        }
        let shard_id = self.shard_id as u32;

        let items: Vec<FallbackItem> = batches.into_iter()
            .map(|b| FallbackItem {
                metablock: b.metablock,
                datablock: b.datablock,
            })
            .collect();

        let seq = self.s3_upload_sequence.get().saturating_add(1);
        self.s3_upload_sequence.set(seq);

        let fallback_batch = FallbackBatch {
            fallback_index,
            end_wal_index,
            shard_id,
            uploaded_by_node_id: self.node_id,
            items,
            upload_sequence: seq,
            lease_index,
        };

        let batch_count = fallback_batch.items.len();
        let serialized = celeriant_wire::disk::versioned_block::serialize_versioned_message_heap(
            &fallback_batch,
            celeriant_wal::constants::WIRE_VERSION_S3_FALLBACK_BATCH,
        ).map_err(|e| ReplicateToS3Error::SerializationFailed(e.to_string()))?;

        let total_bytes = serialized.len();
        let path = fallback_batch_path(shard_id, fallback_index, end_wal_index, self.node_id);

        debug!(
            shard_id, batch_count, total_bytes, fallback_index, end_wal_index,
            lease_index, upload_seq = seq, path = %path,
            "S3 fallback upload"
        );

        s3_uploader.upload(path, Bytes::from(serialized)).await
    }

    async fn send_heartbeat(&self, unix_epoch_now_ms: u64, lease_index: u64) -> Result<HeartbeatResult, SendHeartbeatError> {
        let mut guard = write_with_timeout(&self.heartbeat_conn, "send_heartbeat").await
            .map_err(|_| SendHeartbeatError::LockTimeout)?;

        let address = self.follower_address.borrow().clone();
        let hb_timeout = Some(self.heartbeat_timeout);

        // Always reset: forces a fresh TCP connection on each heartbeat attempt.
        // This avoids stale connections hanging for the full internode_request_timeout
        // (10s) when the peer is unreachable, which would prevent timely self-fencing.
        guard.ensure_connected(&address, true, hb_timeout, hb_timeout, self.max_request_size, self.max_response_size, self.replication_client_config.as_ref(), self.tcp_user_timeout, self.dict_codec.clone()).await?;

        let request = ClusterRequest::Heartbeat(HeartbeatRequest {
            correlation_id: None,
            shard_id: self.shard_id,
            leader_timestamp_ms: unix_epoch_now_ms,
            lease_index,
        });

        let response = guard.client.as_mut().unwrap().send_cluster_request(&request).await?;

        match response {
            ClusterResponse::Heartbeat(resp) => Ok(resp.result),
            _ => Err(SendHeartbeatError::UnexpectedResponse),
        }
    }

    async fn send_kick(&self) -> Result<bool, SendHeartbeatError> {
        let mut guard = write_with_timeout(&self.replication_conn, "send_kick").await
            .map_err(|_| SendHeartbeatError::LockTimeout)?;

        let address = self.follower_address.borrow().clone();

        guard.ensure_connected(&address, false, self.connection_timeout, self.request_timeout, self.max_request_size, self.max_response_size, self.replication_client_config.as_ref(), self.tcp_user_timeout, self.dict_codec.clone()).await?;

        let request = ClusterRequest::KickFollower(KickFollowerRequest {
            correlation_id: None,
        });

        let response = match guard.client.as_mut().unwrap().send_cluster_request(&request).await {
            Ok(r) => r,
            Err(_) => {
                guard.ensure_connected(&address, true, self.connection_timeout, self.request_timeout, self.max_request_size, self.max_response_size, self.replication_client_config.as_ref(), self.tcp_user_timeout, self.dict_codec.clone()).await?;
                guard.client.as_mut().unwrap().send_cluster_request(&request).await?
            }
        };

        match response {
            ClusterResponse::KickFollower(resp) => Ok(resp.acknowledged),
            _ => Err(SendHeartbeatError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::metablocks::metablock_kind::MetablockKind;
    use celeriant_wal::metablocks::metablock_event_batch::MetablockEventBatch;
    use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
    use celeriant_wal::metablocks::metablock::Metablock;
    use celeriant_wal::datablocks::datablock::Datablock;
    use celeriant_wal::datablocks::datablock_kind::DatablockKind;
    use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
    use celeriant_wire::codec::compression::DictCodec;
    use glommio::LocalExecutor;
    use std::rc::Rc;

    fn test_dict_codec() -> Rc<DictCodec> {
        Rc::new(DictCodec::new(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES, 3).unwrap())
    }

    type MockCalls = Rc<RefCell<Vec<(String, Bytes)>>>;

    struct MockS3Uploader {
        calls: MockCalls,
        result: RefCell<Option<Result<(), ReplicateToS3Error>>>,
    }

    impl MockS3Uploader {
        fn new() -> (Self, MockCalls) {
            let calls = Rc::new(RefCell::new(Vec::new()));
            (Self { calls: calls.clone(), result: RefCell::new(None) }, calls)
        }

        fn with_result(result: Result<(), ReplicateToS3Error>) -> Self {
            Self {
                calls: Rc::new(RefCell::new(Vec::new())),
                result: RefCell::new(Some(result)),
            }
        }
    }

    impl S3Uploader for MockS3Uploader {
        async fn upload(&self, path: String, data: Bytes) -> Result<(), ReplicateToS3Error> {
            self.calls.borrow_mut().push((path, data));
            self.result.borrow_mut().take().unwrap_or(Ok(()))
        }
    }

    fn create_test_metablock(wal_index: u64) -> Metablock {
        let aggregate_key = AggregateKey::new(1, 2, 3);
        Metablock {
            wal_index,
            server_timestamp: 1000,
            lease_index: 5,
            node_id: 999,
            uncompressed_size: 1024,
            compressed_size: 512,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [1u8; 32],
            datablock_position: 0,
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key,
                event_batch_index: 10,
                min_event_batch_index: 1,
                min_client_event_index: 1,
                max_client_event_index: 5,
                min_event_timestamp: 100,
                max_event_timestamp: 500,
                min_event_index: 1,
                max_event_index: 5,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::None,
        }
    }

    fn create_test_datablock() -> Datablock {
        Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                event_batch_index: 10,
                events: vec![],
            }),
        }
    }

    #[test]
    fn empty_batches_returns_ok() {
        LocalExecutor::default().run(async {
            let (mock_uploader, calls) = MockS3Uploader::new();

            let client = FollowerConnection::new(
                Some("127.0.0.1:8080".to_string()),
                None,
                None,
                Duration::from_millis(500),
                None,
                1024,
                1024,
                7,
                1,
                None,
                test_dict_codec(),
                Some(mock_uploader),
            );

            let result = client.replicate_to_s3(vec![]).await;

            assert!(result.is_ok());
            assert_eq!(calls.borrow().len(), 0);
        });
    }

    #[test]
    fn no_uploader_returns_s3_not_configured() {
        LocalExecutor::default().run(async {
            let client: FollowerConnection<MockS3Uploader> = FollowerConnection::new(
                Some("127.0.0.1:8080".to_string()),
                None,
                None,
                Duration::from_millis(500),
                None,
                1024,
                1024,
                7,
                1,
                None,
                test_dict_codec(),
                None,
            );

            let batches = vec![ReplicationBatchItem {
                metablock: create_test_metablock(42),
                datablock: None,
            }];

            let result = client.replicate_to_s3(batches).await;

            assert!(matches!(result, Err(ReplicateToS3Error::S3NotConfigured)));
        });
    }

    #[test]
    fn upload_called_with_correct_path_and_data() {
        LocalExecutor::default().run(async {
            let (mock_uploader, calls) = MockS3Uploader::new();

            let client = FollowerConnection::new(
                Some("127.0.0.1:8080".to_string()),
                None,
                None,
                Duration::from_millis(500),
                None,
                1024,
                1024,
                7,
                42,
                None,
                test_dict_codec(),
                Some(mock_uploader),
            );

            let batches = vec![
                ReplicationBatchItem {
                    metablock: create_test_metablock(42),
                    datablock: Some(create_test_datablock()),
                },
                ReplicationBatchItem {
                    metablock: create_test_metablock(43),
                    datablock: None,
                },
            ];

            let result = client.replicate_to_s3(batches).await;

            assert!(result.is_ok());

            let calls = calls.borrow();
            assert_eq!(calls.len(), 1);

            let (path, data) = &calls[0];
            assert_eq!(path, "cluster/fallback/shard_007/batch_000000042_000000043_00000000-0000-0000-0000-00000000002a.bin");

            let deserialized = celeriant_wire::disk::versioned_block::deserialise_fallback_batch(data)
                .expect("should deserialize");
            assert_eq!(deserialized.shard_id, 7);
            assert_eq!(deserialized.uploaded_by_node_id, 42);
            assert_eq!(deserialized.fallback_index, 42);
            assert_eq!(deserialized.end_wal_index, 43);
            assert_eq!(deserialized.items.len(), 2);
        });
    }

    #[test]
    fn fallback_index_is_first_wal_index() {
        LocalExecutor::default().run(async {
            let (mock_uploader, calls) = MockS3Uploader::new();

            let client = FollowerConnection::new(
                Some("127.0.0.1:8080".to_string()),
                None,
                None,
                Duration::from_millis(500),
                None,
                1024,
                1024,
                5,
                1,
                None,
                test_dict_codec(),
                Some(mock_uploader),
            );

            let batches = vec![
                ReplicationBatchItem {
                    metablock: create_test_metablock(100),
                    datablock: None,
                },
                ReplicationBatchItem {
                    metablock: create_test_metablock(101),
                    datablock: None,
                },
                ReplicationBatchItem {
                    metablock: create_test_metablock(102),
                    datablock: None,
                },
            ];

            let result = client.replicate_to_s3(batches).await;

            assert!(result.is_ok());

            let calls = calls.borrow();
            assert_eq!(calls.len(), 1);

            let (path, data) = &calls[0];
            assert_eq!(path, "cluster/fallback/shard_005/batch_000000100_000000102_00000000-0000-0000-0000-000000000001.bin");

            let deserialized = celeriant_wire::disk::versioned_block::deserialise_fallback_batch(data)
                .expect("should deserialize");
            assert_eq!(deserialized.fallback_index, 100);
            assert_eq!(deserialized.uploaded_by_node_id, 1);
            assert_eq!(deserialized.end_wal_index, 102);
            assert_eq!(deserialized.items[0].metablock.wal_index, 100);
            assert_eq!(deserialized.items[1].metablock.wal_index, 101);
            assert_eq!(deserialized.items[2].metablock.wal_index, 102);
        });
    }

    #[test]
    fn replicate_to_s3_rejects_cross_lease_items() {
        LocalExecutor::default().run(async {
            let (mock_uploader, calls) = MockS3Uploader::new();

            let client = FollowerConnection::new(
                Some("127.0.0.1:8080".to_string()),
                None, None,
                Duration::from_millis(500),
                None,
                1024, 1024,
                7, 1,
                None,
                test_dict_codec(),
                Some(mock_uploader),
            );

            let mut mb_a = create_test_metablock(42);
            mb_a.lease_index = 5;
            let mut mb_b = create_test_metablock(43);
            mb_b.lease_index = 6;

            let batches = vec![
                ReplicationBatchItem { metablock: mb_a, datablock: None },
                ReplicationBatchItem { metablock: mb_b, datablock: None },
            ];

            let result = client.replicate_to_s3(batches).await;

            assert!(matches!(
                result,
                Err(ReplicateToS3Error::LeaseIndexInconsistent { at_index: 1, first: 5, found: 6 })
            ), "got {:?}", result);
            assert_eq!(calls.borrow().len(), 0, "no upload should have happened");
        });
    }

    #[test]
    fn s3_error_propagated() {
        LocalExecutor::default().run(async {
            let mock_uploader = MockS3Uploader::with_result(
                Err(ReplicateToS3Error::S3PutFailed {
                    path: "test".to_string(),
                    message: "network error".to_string(),
                })
            );

            let client = FollowerConnection::new(
                Some("127.0.0.1:8080".to_string()),
                None,
                None,
                Duration::from_millis(500),
                None,
                1024,
                1024,
                7,
                1,
                None,
                test_dict_codec(),
                Some(mock_uploader),
            );

            let batches = vec![ReplicationBatchItem {
                metablock: create_test_metablock(42),
                datablock: None,
            }];

            let result = client.replicate_to_s3(batches).await;

            assert!(matches!(result, Err(ReplicateToS3Error::S3PutFailed { .. })));
        });
    }

    fn test_follower_conn() -> FollowerConnection<MockS3Uploader> {
        FollowerConnection::new(
            Some("127.0.0.1:8080".to_string()),
            None, None,
            Duration::from_millis(500),
            None,
            1024, 1024,
            7, 1,
            None, test_dict_codec(), None,
        )
    }

    #[test]
    fn try_acquire_kick_is_exclusive() {
        let client = test_follower_conn();
        assert!(client.try_acquire_kick(), "first acquire must succeed");
        assert!(!client.try_acquire_kick(), "second acquire must fail while in-flight");
        assert!(!client.try_acquire_kick(), "still locked");
    }

    #[test]
    fn release_kick_resets_latch() {
        let client = test_follower_conn();
        assert!(client.try_acquire_kick());
        client.release_kick();
        assert!(client.try_acquire_kick(), "acquire after release must succeed");
        client.release_kick();
    }

    #[test]
    fn release_without_acquire_is_noop() {
        let client = test_follower_conn();
        client.release_kick();
        assert!(client.try_acquire_kick(), "latch still usable after spurious release");
    }
}
