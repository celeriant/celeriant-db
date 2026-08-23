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
    async fn replicate_to_follower(&self, batches: Vec<ReplicationBatchItem>, leader_confirmed_wal_seq: u64, sender_lease_epoch: u64) -> Result<(), ReplicateToFollowerError>;
    async fn replicate_to_s3(&self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error>;
    async fn send_heartbeat(&self, unix_epoch_now_ms: u64, lease_epoch: u64) -> Result<HeartbeatResult, SendHeartbeatError>;
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

    async fn replicate_to_follower(&self, _batches: Vec<ReplicationBatchItem>, _leader_confirmed_wal_seq: u64, _sender_lease_epoch: u64) -> Result<(), ReplicateToFollowerError> {
        glommio::timer::sleep(std::time::Duration::from_millis(30)).await;
        Ok(())
    }

    async fn replicate_to_s3(&self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
        glommio::timer::sleep(std::time::Duration::from_millis(230)).await;
        Ok(())
    }

    async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _lease_epoch: u64) -> Result<HeartbeatResult, SendHeartbeatError> {
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

    async fn reset(&mut self) {
        if let Some(client) = self.client.take() {
            let _ = client.close().await;
        }
        self.connected_to = None;
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
        // Can be dirty due to previous client usage that got cancelled
        let dirty = self.client.as_ref().is_some_and(|c| c.is_stream_dirty());
        if reset || dirty {
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

    async fn replicate_to_follower(&self, batches: Vec<ReplicationBatchItem>, leader_confirmed_wal_seq: u64, sender_lease_epoch: u64) -> Result<(), ReplicateToFollowerError> {
        // Empty batches are legal: a commit-notify carries only the header
        // (leader_confirmed_wal_seq) so an idle follower commits its parked
        // tail without waiting for the probe.
        let mut guard = write_with_timeout(&self.replication_conn, "replicate_to_follower").await
            .map_err(|_| ReplicateToFollowerError::LockTimeout)?;

        let address = self.follower_address.borrow().clone();
        let shard_id = self.shard_id;

        guard.ensure_connected(&address, false, self.connection_timeout, self.request_timeout, self.max_request_size, self.max_response_size, self.replication_client_config.as_ref(), self.tcp_user_timeout, self.dict_codec.clone()).await?;

        let mut request = ClusterRequest::ReplicationBatch(ReplicationBatchRequest {
            correlation_id: None,
            shard_id,
            leader_timestamp_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            leader_confirmed_wal_seq,
            sender_lease_epoch,
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
            _ => {
                guard.reset().await;
                Err(ReplicateToFollowerError::FollowerUnexpectedResponse)
            }
        }
    }

    async fn replicate_to_s3(&self, batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
        if batches.is_empty() {
            return Ok(());
        }

        // Refuse to upload a batch with internal wal_seq gaps. A gap here means
        // upstream code produced items from two chain generations in one workset —
        // a latent bug that would plant an unrecoverable file in S3 and panic any
        // follower that consumes it. Fail loudly at the source instead.
        if let Some((i, w)) = batches.windows(2).enumerate().find(|(_, w)| w[0].metablock.wal_seq + 1 != w[1].metablock.wal_seq) {
            let expected = w[0].metablock.wal_seq + 1;
            let actual = w[1].metablock.wal_seq;
            tracing::error!(shard_id = self.shard_id, at_index = i + 1, expected, actual, "refusing S3 upload: batch has internal wal_seq gap");
            return Err(ReplicateToS3Error::BatchNotContiguous { at_index: i + 1, expected_wal_seq: expected, actual_wal_seq: actual });
        }

        let s3_uploader = self.s3_uploader.as_ref()
            .ok_or(ReplicateToS3Error::S3NotConfigured)?;

        let fallback_index = batches[0].metablock.wal_seq;
        let end_wal_seq = batches.last().unwrap().metablock.wal_seq;
        let lease_epoch = batches[0].metablock.lease_epoch;
        if let Some((i, b)) = batches.iter().enumerate().skip(1).find(|(_, b)| b.metablock.lease_epoch != lease_epoch) {
            tracing::error!(shard_id = self.shard_id, at_index = i, first = lease_epoch, found = b.metablock.lease_epoch, "refusing S3 upload: batch spans multiple lease_epochs");
            return Err(ReplicateToS3Error::LeaseIndexInconsistent { at_index: i, first: lease_epoch, found: b.metablock.lease_epoch });
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
            end_wal_seq,
            shard_id,
            uploaded_by_node_id: self.node_id,
            items,
            upload_sequence: seq,
            lease_epoch,
        };

        let batch_count = fallback_batch.items.len();
        let serialized = celeriant_wire::disk::versioned_block::serialize_versioned_message_heap(
            &fallback_batch,
            celeriant_wal::constants::WIRE_VERSION_S3_FALLBACK_BATCH,
        ).map_err(|e| ReplicateToS3Error::SerializationFailed(e.to_string()))?;

        let total_bytes = serialized.len();
        let path = fallback_batch_path(shard_id, fallback_index, end_wal_seq, self.node_id);

        debug!(
            shard_id, batch_count, total_bytes, fallback_index, end_wal_seq,
            lease_epoch, upload_seq = seq, path = %path,
            "S3 fallback upload"
        );

        s3_uploader.upload(path, Bytes::from(serialized)).await
    }

    async fn send_heartbeat(&self, unix_epoch_now_ms: u64, lease_epoch: u64) -> Result<HeartbeatResult, SendHeartbeatError> {
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
            lease_epoch,
        });

        let response = guard.client.as_mut().unwrap().send_cluster_request(&request).await?;

        match response {
            ClusterResponse::Heartbeat(resp) => Ok(resp.result),
            _ => {
                guard.reset().await;
                Err(SendHeartbeatError::UnexpectedResponse)
            }
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
            _ => {
                guard.reset().await;
                Err(SendHeartbeatError::UnexpectedResponse)
            }
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

    fn create_test_metablock(wal_seq: u64) -> Metablock {
        let aggregate_key = AggregateKey::new(1, 2, 3);
        Metablock {
            wal_seq,
            server_timestamp: 1000,
            lease_epoch: 5,
            node_id: 999,
            uncompressed_size: 1024,
            compressed_size: 512,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [1u8; 32],
            datablock_position: 0,
            previous_aggregate_metablock_pos: 0,
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key,
                aggregate_version: 10,
                trimmed_below_version: 1,
                min_client_seq: 1,
                max_client_seq: 5,
                min_event_timestamp: 100,
                max_event_timestamp: 500,
                min_event_seq: 1,
                max_event_seq: 5,
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
                aggregate_version: 10,
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
            assert_eq!(deserialized.end_wal_seq, 43);
            assert_eq!(deserialized.items.len(), 2);
        });
    }

    #[test]
    fn fallback_index_is_first_wal_seq() {
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
            assert_eq!(deserialized.end_wal_seq, 102);
            assert_eq!(deserialized.items[0].metablock.wal_seq, 100);
            assert_eq!(deserialized.items[1].metablock.wal_seq, 101);
            assert_eq!(deserialized.items[2].metablock.wal_seq, 102);
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
            mb_a.lease_epoch = 5;
            let mut mb_b = create_test_metablock(43);
            mb_b.lease_epoch = 6;

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

    // ---- Cancellation and misalignment of the shared replication connection ----
    //
    // `send_kick` and `replicate_to_follower` share one `ConnState`. A kick whose
    // future is dropped between the write and the read leaves its response in the
    // socket, and the next batch down that socket reads it. A reply of the wrong
    // variant means the same thing has already happened.

    use celeriant_msg::process_cluster_requests::ClusterRequestType;
    use celeriant_msg::response::responses::{HeartbeatResponse, KickFollowerResponse, ReplicationBatchResponse};
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WireHeader};
    use futures_lite::AsyncWriteExt;
    use futures_lite::future::poll_once;
    use glommio::{LocalExecutorBuilder, Placement};

    const MOCK_MAX: u64 = 4 * 1024 * 1024;

    macro_rules! glommio_test {
        ($body:expr) => {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(|| async move { $body })
                .unwrap()
                .join()
                .unwrap()
        };
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum FollowerScript {
        /// Every request gets the reply a real follower would send.
        Correct,
        /// The first connection always replies with the wrong variant; every
        /// connection after it replies correctly. That is what separates
        /// "the client retired the misaligned connection" from "the client
        /// kept using it".
        WrongVariantOnFirstConnection,
    }

    /// A stand-in follower on the test's own executor.
    ///
    /// `gate` holds every answer back until the test opens it, so a request can
    /// be abandoned at a point where the write has provably landed — the
    /// follower read the whole frame — and not one byte of the response exists
    /// yet. Nothing here depends on the wall clock.
    #[derive(Clone)]
    struct MockFollower {
        /// One entry per accepted connection, holding the requests read on it.
        conns: Rc<RefCell<Vec<Vec<ClusterRequestType>>>>,
        /// The connection index of every request read, in arrival order.
        arrivals: Rc<RefCell<Vec<usize>>>,
        answered: Rc<Cell<usize>>,
        gate: Rc<Cell<bool>>,
    }

    impl MockFollower {
        fn requests_on(&self, connection: usize) -> usize {
            self.conns.borrow().get(connection).map_or(0, |c| c.len())
        }

        fn requests_read(&self) -> usize {
            self.arrivals.borrow().len()
        }

        fn last_connection_used(&self) -> usize {
            *self.arrivals.borrow().last().expect("the follower has read at least one request")
        }
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
            ClusterRequest::ReplicationBatch(r) => ClusterResponse::ReplicationBatch(ReplicationBatchResponse {
                correlation_id: r.correlation_id,
                follower_timestamp_ms: 1,
                result: ReplicationResult::Success { last_follower_metablock: None },
            }),
        }
    }

    /// What an offset stream hands back: a well-formed reply to somebody else's
    /// request.
    fn wrong_variant_response(request: &ClusterRequest) -> ClusterResponse {
        match request {
            ClusterRequest::KickFollower(r) => ClusterResponse::Heartbeat(HeartbeatResponse {
                correlation_id: r.correlation_id,
                result: HeartbeatResult::Ack {
                    follower_timestamp_ms: 1,
                    follower_can_accept_tcp_replication: true,
                },
            }),
            _ => ClusterResponse::KickFollower(KickFollowerResponse {
                correlation_id: request.correlation_id(),
                acknowledged: true,
            }),
        }
    }

    fn spawn_follower(script: FollowerScript, codec: Rc<DictCodec>) -> (String, MockFollower) {
        let listener = glommio::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let follower = MockFollower {
            conns: Rc::new(RefCell::new(Vec::new())),
            arrivals: Rc::new(RefCell::new(Vec::new())),
            answered: Rc::new(Cell::new(0)),
            gate: Rc::new(Cell::new(true)),
        };
        let acceptor = follower.clone();
        glommio::spawn_local(async move {
            while let Ok(mut stream) = listener.accept().await {
                let index = {
                    let mut conns = acceptor.conns.borrow_mut();
                    conns.push(Vec::new());
                    conns.len() - 1
                };
                let wrong = script == FollowerScript::WrongVariantOnFirstConnection && index == 0;
                let server = acceptor.clone();
                let codec = codec.clone();
                glommio::spawn_local(async move {
                    loop {
                        let Ok(header) = WireHeader::from_reader(&mut stream, MOCK_MAX).await else { break };
                        let Ok(request) = ClusterRequest::read_from_header(header, &mut stream, &codec).await else { break };
                        server.conns.borrow_mut()[index].push(request.request_type());
                        server.arrivals.borrow_mut().push(index);
                        let response = if wrong { wrong_variant_response(&request) } else { matching_response(&request) };
                        while !server.gate.get() {
                            park_briefly().await;
                        }
                        if ClusterResponse::write_response(&mut stream, &response, MOCK_MAX, PROTOCOL_VERSION_V2).await.is_err() {
                            break;
                        }
                        let _ = stream.flush().await;
                        server.answered.set(server.answered.get() + 1);
                    }
                })
                .detach();
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

    fn follower_conn_to(address: &str, codec: Rc<DictCodec>) -> FollowerConnection<MockS3Uploader> {
        let conn = FollowerConnection::new(
            Some(address.to_string()),
            None,
            None,
            Duration::from_millis(500),
            None,
            MOCK_MAX,
            MOCK_MAX,
            7,
            1,
            None,
            codec,
            None,
        );
        conn.set_follower_reachable(true);
        conn
    }

    fn test_batches() -> Vec<ReplicationBatchItem> {
        vec![ReplicationBatchItem { metablock: create_test_metablock(1), datablock: None }]
    }

    /// Write the kick, then abandon it parked on the response read.
    ///
    /// The follower is gated shut first, so the only thing that can end the
    /// polling loop is the follower reporting it read the whole request frame.
    /// That is the write completing, observed from the far end. The gate then
    /// opens and the `KickFollowerResponse` lands in a socket nobody is reading.
    async fn abandon_send_kick(conn: &FollowerConnection<MockS3Uploader>, follower: &MockFollower) -> usize {
        let read_before = follower.requests_read();
        let answered_before = follower.answered.get();
        follower.gate.set(false);
        {
            let mut kick = std::pin::pin!(conn.send_kick());
            for _ in 0..20_000 {
                if follower.requests_read() > read_before {
                    break;
                }
                assert!(
                    poll_once(kick.as_mut()).await.is_none(),
                    "the kick ended before it could be abandoned"
                );
                park_briefly().await;
            }
            assert!(
                follower.requests_read() > read_before,
                "the kick frame never reached the follower, so nothing was abandoned mid-flight"
            );
        }
        follower.gate.set(true);
        for _ in 0..20_000 {
            if follower.answered.get() > answered_before {
                break;
            }
            park_briefly().await;
        }
        assert!(
            follower.answered.get() > answered_before,
            "the follower never wrote the answer to the abandoned kick"
        );
        follower.last_connection_used()
    }

    /// `with_budget(b, rc.send_kick())` drops the kick future on budget expiry.
    /// The replication batch that follows must not go down that socket.
    #[test]
    fn a_cancelled_kick_does_not_leave_the_next_batch_reading_its_response() {
        glommio_test!({
            let codec = test_dict_codec();
            let (address, follower) = spawn_follower(FollowerScript::Correct, codec.clone());
            let conn = follower_conn_to(&address, codec);

            // Warm the connection so the abandoned poll starts at the write.
            assert!(conn.send_kick().await.is_ok(), "the mock follower acknowledges kicks");

            let poisoned = abandon_send_kick(&conn, &follower).await;
            let delivered = follower.requests_on(poisoned);

            let _ = conn.replicate_to_follower(test_batches(), 1, 5).await;

            // A batch written to the poisoned socket is read by the follower
            // within microseconds — both tasks are on this executor — so waiting
            // far past that is the whole of the timing dependency, and a wait
            // that is too short can only make this test pass, never fail.
            for _ in 0..400 {
                if follower.requests_on(poisoned) > delivered {
                    break;
                }
                park_briefly().await;
            }

            assert_eq!(
                follower.requests_on(poisoned),
                delivered,
                "the batch was written down the socket that still held the abandoned kick's response"
            );
        });
    }

    /// The only end-to-end pin of the live cancellation source. The test above
    /// abandons the kick with a hand-rolled `poll_once` loop; production drops it
    /// via `with_budget(b, rc.send_kick())` at shard_wal_replicate.rs:503 and
    /// :899, which is `or(fut, Timer)`. This drives that exact wrapper, so the
    /// cancellation arises the way production makes it arise.
    #[test]
    fn a_kick_dropped_by_with_budget_does_not_poison_the_next_batch() {
        glommio_test!({
            let codec = test_dict_codec();
            let (address, follower) = spawn_follower(FollowerScript::Correct, codec.clone());
            let conn = follower_conn_to(&address, codec);

            assert!(conn.send_kick().await.is_ok(), "warm the connection so the drop lands after the write");

            let read_before = follower.requests_read();
            let answered_before = follower.answered.get();
            follower.gate.set(false);

            let outcome = celeriant_disk::files::rwlock_timeout::with_budget(
                Duration::from_millis(100),
                conn.send_kick(),
            )
            .await;
            assert!(outcome.is_none(), "test premise: the budget must expire while the follower is gated");
            assert!(
                follower.requests_read() > read_before,
                "test premise: the kick frame never reached the follower, so nothing was abandoned"
            );
            let poisoned = follower.last_connection_used();

            follower.gate.set(true);
            for _ in 0..20_000 {
                if follower.answered.get() > answered_before {
                    break;
                }
                park_briefly().await;
            }
            assert!(
                follower.answered.get() > answered_before,
                "the abandoned kick's response never landed in the socket"
            );

            let delivered = follower.requests_on(poisoned);
            let batch = conn.replicate_to_follower(test_batches(), 1, 5).await;
            for _ in 0..400 {
                if follower.requests_on(poisoned) > delivered {
                    break;
                }
                park_briefly().await;
            }
            assert_eq!(
                follower.requests_on(poisoned),
                delivered,
                "the batch was written down the socket that still held the abandoned kick's response"
            );
            assert!(batch.is_ok(), "and the batch must still land, on a fresh connection: {batch:?}");
        });
    }

    /// A batch answered with the wrong variant proves the stream is already
    /// offset. Leaving it connected keeps it offset forever.
    #[test]
    fn a_wrong_variant_reply_retires_the_replication_connection() {
        glommio_test!({
            let codec = test_dict_codec();
            let (address, follower) = spawn_follower(FollowerScript::WrongVariantOnFirstConnection, codec.clone());
            let conn = follower_conn_to(&address, codec);

            let first = conn.replicate_to_follower(test_batches(), 1, 5).await;
            assert!(
                matches!(first, Err(ReplicateToFollowerError::FollowerUnexpectedResponse)),
                "test premise: the first batch must hit the wrong-variant arm, got {first:?}"
            );

            let second = conn.replicate_to_follower(test_batches(), 2, 5).await;
            assert!(
                second.is_ok(),
                "the follower answers correctly on every connection after the first, so this can \
                 only fail if the misaligned connection was reused: {second:?}"
            );
            assert_eq!(follower.requests_on(0), 1, "the misaligned connection took a second request");
        });
    }

    /// Same contract on the kick's own wrong-variant arm.
    #[test]
    fn a_wrong_variant_reply_retires_the_kick_connection() {
        glommio_test!({
            let codec = test_dict_codec();
            let (address, follower) = spawn_follower(FollowerScript::WrongVariantOnFirstConnection, codec.clone());
            let conn = follower_conn_to(&address, codec);

            let first = conn.send_kick().await;
            assert!(
                matches!(first, Err(SendHeartbeatError::UnexpectedResponse)),
                "test premise: the first kick must hit the wrong-variant arm, got {first:?}"
            );

            let second = conn.send_kick().await;
            assert!(
                matches!(second, Ok(true)),
                "the follower acknowledges kicks on every connection after the first, so this can \
                 only fail if the misaligned connection was reused: {second:?}"
            );
            assert_eq!(follower.requests_on(0), 1, "the misaligned connection took a second request");
        });
    }
}
