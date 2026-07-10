//! Blind contract tests for idle commit-notify (phase 3
//! brief oracles 1-3). These test the PROMISED behavior: an empty-batches
//! ReplicationBatchRequest that passes all existing guards (time/drift/epoch)
//! is a COMMIT-NOTIFY carrier — the follower returns Success, runs the same
//! floor-update/parked-drain/read-persistence the data path runs, and never
//! extends the chain. An empty batch failing a guard is still rejected with
//! that guard's reason.
//!
//! All three tests are expected RED on current code: empty batches are
//! rejected EmptyBatch, and (contrary to the phase 3 brief's guard-order
//! claim) that rejection fires BEFORE the drift and epoch guards today — only
//! the NotAFollower role check precedes it. The stale-epoch fencing test is
//! therefore red too; it encodes the contracted guard order and must go (and
//! stay) green when the notify path lands.
//!
//! Scaffolding is copied from follower_commit_contract_tests.rs: real wire
//! batches captured from a real leader shard, disk-truth cursor observation
//! via a reopened LogSegmentsCache.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use celeriant_distributed::node_status::NodeStatus;
use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{
    ReadRequest, ReplicationBatchItem, ReplicationBatchRequest, SingleAggregateWrite,
    WatchRequest, WriteRequest,
};
use celeriant_msg::response::responses::{FollowerRejection, HeartbeatResult, ReplicationResult};
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::EntryHashBytes;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_watch::aggregate_watch_event::{AggregateWatchEvent, AggregateWatchEventOperation};
use celeriant_watch::subscribed_client::SubscribedClient;
use futures_lite::future::poll_once;
use glommio::{LocalExecutorBuilder, Placement};

use crate::error::replication_to_follower_error::ReplicateToFollowerError;
use crate::error::replication_to_s3_error::ReplicateToS3Error;
use crate::error::send_heartbeat_error::SendHeartbeatError;
use crate::error::shard_error::ShardError;
use crate::error::shard_read_error::ShardReadError;
use crate::internal_shard_config::InternalShardConfig;
use crate::replication_client::{ReplicationClient, StubReplicationClient};
use crate::s3_downloader::StubS3Downloader;
use crate::shard_wal::ShardWal;
use crate::timestamp_config::TimestampConfig;

macro_rules! glommio_test {
    ($body:expr) => {
        LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move { $body })
            .unwrap()
            .join()
            .unwrap()
    };
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn test_dirs() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let leader = tmp.path().join("leader");
    let follower = tmp.path().join("follower");
    (tmp, leader, follower)
}

fn test_config(dir: &std::path::Path) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        shard_id: 1,
        max_open_files: 4,
        shard_log_preallocate_bytes: 4 * 1024 * 1024,
        fsync_delay: Duration::ZERO,
        replication_delay: Duration::ZERO,
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::ZERO,
        heartbeat_starve_threshold: Duration::ZERO,
        recent_write_cache_bytes: 64 * 1024 * 1024,
        shard_dir: dir.to_path_buf(),
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes: 1024,
        timestamp_config: TimestampConfig::default(),
        list_page_size: 100,
        list_max_concurrent: 16,
        list_max_duration: Duration::from_secs(2),
        schema_cache_bytes: 4 * 1024 * 1024,
        max_schema_size_bytes: 16384,
        max_catchup_gap_bytes: Some(100 * 1024 * 1024),
        max_promotion_batch_bytes: None,
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_compaction"),
        max_clock_drift_ms: 500,
        read_max_concurrent: 64,
        cache_warmup_max_duration: Duration::MAX,
        wal_compression_level: 3,
        dict_bytes: Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

fn key(org: u128, atype: u128, id: u128) -> AggregateKey {
    AggregateKey::new(org, atype, id)
}

fn events(count: usize) -> Vec<DatablockAggregateEvent> {
    (1..=count as u64)
        .map(|i| DatablockAggregateEvent {
            client_seq: i,
            event_type_major: 1,
            event_value: Arc::new(vec![i as u8; 8]),
            ..Default::default()
        })
        .collect()
}

fn write_req(agg: AggregateKey, evts: Vec<DatablockAggregateEvent>) -> ClientRequest {
    let mut writes = HashMap::new();
    writes.insert(
        agg,
        SingleAggregateWrite {
            events: evts,
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );
    ClientRequest::Write(WriteRequest {
        correlation_id: None,
        client_id: 1,
        user_id: None,
        writes,
    })
}

fn read_req(agg: AggregateKey) -> ClientRequest {
    ClientRequest::Read(ReadRequest {
        correlation_id: None,
        aggregate_key: agg,
        filters: ReadFilters::new(0),
    })
}

// ── Leader-side capture of real wire batches ──

/// Records every `replicate_to_follower` call: the wire batch items and the
/// `leader_confirmed_wal_seq` each carried.
#[derive(Default)]
struct CaptureToFollowerClient {
    calls: RefCell<Vec<(Vec<ReplicationBatchItem>, u64)>>,
}

impl CaptureToFollowerClient {
    /// Data-bearing calls only. Post-burst commit-notify sends ride with empty
    /// batches and are wire-legal; they stay recorded in `calls` but do not
    /// count as replication cycles.
    fn data_calls(&self) -> Vec<(Vec<ReplicationBatchItem>, u64)> {
        self.calls.borrow().iter().filter(|(batches, _)| !batches.is_empty()).cloned().collect()
    }
}

impl ReplicationClient for CaptureToFollowerClient {
    fn set_follower_address(&self, _address: Option<String>) {}
    fn set_follower_reachable(&self, _: bool) {}
    fn is_follower_reachable(&self) -> bool {
        true
    }
    fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> {
        None
    }
    fn set_heartbeat_in_flight(&self, _unix_ms: Option<u64>) {}
    fn reset_heartbeat_state(&self) {}
    async fn replicate_to_follower(
        &self,
        batches: Vec<ReplicationBatchItem>,
        leader_confirmed_wal_seq: u64,
        _sender_lease_epoch: u64,
    ) -> Result<(), ReplicateToFollowerError> {
        self.calls.borrow_mut().push((batches, leader_confirmed_wal_seq));
        Ok(())
    }
    async fn replicate_to_s3(&self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
        Ok(())
    }
    async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _lease_epoch: u64) -> Result<HeartbeatResult, SendHeartbeatError> {
        Ok(HeartbeatResult::Ack {
            follower_timestamp_ms: unix_epoch_now_ms + 10,
            follower_can_accept_tcp_replication: true,
        })
    }
    async fn send_kick(&self) -> Result<bool, SendHeartbeatError> {
        Ok(true)
    }
}

/// Runs a real leader shard over the given writes and returns, per data-bearing
/// replication cycle in order: the wire batch items and the
/// leader_confirmed_wal_seq that rode with them.
async fn capture_leader_batches(
    dir: &std::path::Path,
    writes: Vec<ClientRequest>,
) -> Vec<(Vec<ReplicationBatchItem>, u64)> {
    let expected_calls = writes.len();
    let shard = ShardWal::open(
        test_config(dir),
        ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 10_000),
        CaptureToFollowerClient::default(),
        StubS3Downloader,
    )
    .await
    .unwrap();
    for w in writes {
        let result = shard.process_client_request(w).await;
        assert!(
            matches!(result, Ok(ClientResponse::Write(_))),
            "leader write failed: {:?}",
            result.err()
        );
    }
    shard.close().await;
    let calls = shard.replication_client.data_calls();
    assert_eq!(
        calls.len(),
        expected_calls,
        "scaffolding: sequential awaited leader writes should replicate one data-bearing cycle each"
    );
    calls
}

fn batch_tip(batches: &[ReplicationBatchItem]) -> u64 {
    batches.last().expect("captured batch must not be empty").metablock.wal_seq
}

// ── Follower-side helpers ──

async fn open_follower(dir: &std::path::Path, leader_lease_epoch: u64) -> ShardWal<StubReplicationClient, StubS3Downloader> {
    ShardWal::open(
        test_config(dir),
        ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch }, 500, now_ms() + 10_000),
        StubReplicationClient,
        StubS3Downloader,
    )
    .await
    .unwrap()
}

fn carrier(batches: Vec<ReplicationBatchItem>, leader_confirmed_wal_seq: u64, sender_lease_epoch: u64) -> ReplicationBatchRequest {
    ReplicationBatchRequest {
        correlation_id: None,
        shard_id: 0,
        leader_timestamp_ms: now_ms(),
        leader_confirmed_wal_seq,
        sender_lease_epoch,
        batches,
    }
}

/// The commit-notify shape under test: batches empty, only the header rides.
fn notify(leader_confirmed_wal_seq: u64, sender_lease_epoch: u64) -> ReplicationBatchRequest {
    carrier(vec![], leader_confirmed_wal_seq, sender_lease_epoch)
}

/// Applies a chain-extending batch; the follower must ACK it.
async fn apply_ok<R: ReplicationClient, D: crate::s3_downloader::S3Downloader>(
    shard: &ShardWal<R, D>,
    req: ReplicationBatchRequest,
) {
    let resp = shard.handle_replication_batch(req).await.expect("replication should not error");
    assert!(
        matches!(resp.result, ReplicationResult::Success { .. }),
        "expected replication Success, got {:?}",
        resp.result
    );
}

/// Disk-truth observation: (write.wal_seq, read.wal_seq, write.tip_hash)
/// recovered from the persisted header. Only call after the owning shard has
/// been closed.
async fn observed_state(dir: &std::path::Path) -> (u64, u64, EntryHashBytes) {
    let cache = LogSegmentsCache::ready_up(dir.to_path_buf(), 4 * 1024 * 1024, 4, 1)
        .await
        .unwrap();
    let state = {
        let active = cache.active();
        let meta = active.metadata.borrow();
        (meta.write.wal_seq, meta.read.as_ref().map_or(0, |r| r.wal_seq), meta.write.tip_hash)
    };
    cache.close().await;
    state
}

fn watch_writes_request() -> WatchRequest {
    let mut ops = HashSet::new();
    ops.insert(AggregateWatchEvent::WRITE);
    WatchRequest {
        correlation_id: None,
        requested_latency_ms: None,
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: None,
        operation_types: Some(ops),
    }
}

/// Non-blocking watch poll after letting the executor settle.
async fn next_watch_event(subscriber: &Rc<RefCell<SubscribedClient>>) -> Option<AggregateWatchEvent> {
    glommio::timer::sleep(Duration::from_millis(20)).await;
    poll_once(subscriber.borrow().receiver.recv()).await.flatten()
}

/// Follower with a parked deferred tail: two captured batches applied, the
/// second confirming only the first's tip, so (tip1, tip2] is durable but
/// invisible. Returns tip1 (confirmed floor) and tip2 (durable tip).
async fn park_deferred_tail<R: ReplicationClient, D: crate::s3_downloader::S3Downloader>(
    shard: &ShardWal<R, D>,
    calls: &[(Vec<ReplicationBatchItem>, u64)],
    sender_lease_epoch: u64,
) -> (u64, u64) {
    let tip1 = batch_tip(&calls[0].0);
    let tip2 = batch_tip(&calls[1].0);
    apply_ok(shard, carrier(calls[0].0.clone(), 0, sender_lease_epoch)).await;
    apply_ok(shard, carrier(calls[1].0.clone(), tip1, sender_lease_epoch)).await;
    (tip1, tip2)
}

// ── Oracle 1: a guarded empty-batches notify commits the parked tail ──

/// INVARIANT: an empty-batches request passing all existing guards, carrying a
/// covering leader_confirmed_wal_seq, is a commit-notify: the follower returns
/// Success and commits the parked tail — entries readable, parked watch events
/// fire, read persisted to the confirmed tip.
/// Expected RED on current code: rejected EmptyBatch.
#[test]
fn contract_commit_notify_commits_parked_tail() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let (agg_a, agg_b) = (key(1, 1, 1), key(1, 1, 2));
        let calls =
            capture_leader_batches(&leader_dir, vec![write_req(agg_a.clone(), events(1)), write_req(agg_b.clone(), events(1))]).await;

        let shard = open_follower(&follower_dir, 0).await;
        let (_id, subscriber) = shard.watched_aggregates().add_subscriber(watch_writes_request());
        let (_tip1, tip2) = park_deferred_tail(&shard, &calls, 0).await;

        // Scaffolding sanity: A's event fired at its confirmation, B stays parked.
        let event = next_watch_event(&subscriber).await.expect("scaffolding: confirming A must release A's event");
        assert_eq!(event.aggregate_key, agg_a);
        let parked = next_watch_event(&subscriber).await;
        assert!(parked.is_none(), "scaffolding: B is unconfirmed, its event must be parked: {parked:?}");
        let invisible = shard.process_client_request(read_req(agg_b.clone())).await;
        assert!(
            matches!(invisible, Err(ShardError::Read(ShardReadError::AggregateNotExists))),
            "scaffolding: the parked tail must be invisible before the notify, got {invisible:?}"
        );

        // The empty-batches commit-notify, covering the durable tip.
        let resp = shard.handle_replication_batch(notify(tip2, 0)).await.expect("notify transport must not error");
        assert!(
            matches!(resp.result, ReplicationResult::Success { .. }),
            "a guarded empty-batches commit-notify must return Success, got {:?}",
            resp.result
        );

        // The notify commits the tail: readable, and B's parked event fires exactly once.
        let read = shard.process_client_request(read_req(agg_b.clone())).await;
        assert!(
            matches!(read, Ok(ClientResponse::Read(_))),
            "the notify must commit the parked tail: {agg_b:?} must be readable, got {read:?}"
        );
        let event = next_watch_event(&subscriber).await.expect("the notify must release B's parked watch event");
        assert_eq!(event.aggregate_key, agg_b);
        assert!(matches!(event.operation, AggregateWatchEventOperation::Write { .. }));
        let extra = next_watch_event(&subscriber).await;
        assert!(extra.is_none(), "exactly once per parked entry: {extra:?}");
        // Leg 3: the read-cursor header fsync now runs detached off the notify
        // response path, so let it complete before reading the durable cursor.
        glommio::timer::sleep(std::time::Duration::from_millis(50)).await;
        shard.close().await;

        let (write, read, _) = observed_state(&follower_dir).await;
        assert_eq!(write, tip2, "the notify must not touch the durable write tip");
        assert_eq!(read, tip2, "the notify persists the committed read cursor (detached, eventual)");
    });
}

// ── Oracle 2: guard order — a stale-epoch empty notify is still fenced ──

/// INVARIANT: an empty-batches request with a STALE sender epoch is rejected
/// StaleLease — the epoch guard must run before any empty-batch handling — and
/// the parked tail is untouched (still invisible, cursors unmoved).
/// The brief predicted this green today; observed current behavior is
/// Rejected(EmptyBatch) (the empty check precedes the epoch guard), so this is
/// RED today. It must go green with the notify path and stay green.
#[test]
fn contract_stale_epoch_empty_notify_rejected_tail_untouched() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let (agg_a, agg_b) = (key(1, 1, 1), key(1, 1, 2));
        let calls =
            capture_leader_batches(&leader_dir, vec![write_req(agg_a.clone(), events(1)), write_req(agg_b.clone(), events(1))]).await;

        // Follower under an epoch-2 leader; a zombie old leader sends the notify.
        let shard = open_follower(&follower_dir, 2).await;
        let (tip1, tip2) = park_deferred_tail(&shard, &calls, 2).await;

        let resp = shard.handle_replication_batch(notify(tip2, 1)).await.expect("notify transport must not error");
        assert!(
            matches!(resp.result, ReplicationResult::Rejected(FollowerRejection::StaleLease { .. })),
            "a stale-epoch empty notify must be fenced as StaleLease, got {:?}",
            resp.result
        );

        // The parked tail is untouched: still invisible, cursors unmoved on disk.
        let read = shard.process_client_request(read_req(agg_b.clone())).await;
        assert!(
            matches!(read, Err(ShardError::Read(ShardReadError::AggregateNotExists))),
            "a fenced notify must not commit the parked tail, got {read:?}"
        );
        shard.close().await;

        let (write, read, _) = observed_state(&follower_dir).await;
        assert_eq!(write, tip2, "a fenced notify must not touch the durable write tip");
        assert_eq!(read, tip1, "a fenced notify must not advance the read cursor");
    });
}

// ── Oracle 3: a notify never extends the chain ──

/// INVARIANT: a commit-notify structurally cannot be mistaken for a
/// chain-extending batch: after a Success notify the write cursor and tip hash
/// are unchanged, and a subsequent REAL batch chaining on the pre-notify tip
/// applies cleanly.
/// Expected RED on current code: the notify is rejected EmptyBatch, so the
/// Success assertion fails before anything else can be observed.
#[test]
fn contract_commit_notify_never_extends_chain() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let calls = capture_leader_batches(
            &leader_dir,
            vec![
                write_req(key(1, 1, 1), events(1)),
                write_req(key(1, 1, 2), events(1)),
                write_req(key(1, 1, 3), events(1)),
            ],
        )
        .await;
        let tip3 = batch_tip(&calls[2].0);

        let shard = open_follower(&follower_dir, 0).await;
        let (_tip1, tip2) = park_deferred_tail(&shard, &calls[..2], 0).await;
        shard.close().await;
        let (pre_write, _, pre_tip_hash) = observed_state(&follower_dir).await;
        assert_eq!(pre_write, tip2, "scaffolding: durable tip must sit at tip2 before the notify");

        let shard = open_follower(&follower_dir, 0).await;
        let resp = shard.handle_replication_batch(notify(tip2, 0)).await.expect("notify transport must not error");
        assert!(
            matches!(resp.result, ReplicationResult::Success { .. }),
            "a guarded empty-batches commit-notify must return Success, got {:?}",
            resp.result
        );
        // Leg 3: let the detached read-cursor fsync complete before reading disk.
        glommio::timer::sleep(std::time::Duration::from_millis(50)).await;
        shard.close().await;

        let (write, read, tip_hash) = observed_state(&follower_dir).await;
        assert_eq!(write, tip2, "the notify must not move the write cursor");
        assert_eq!(tip_hash, pre_tip_hash, "the notify must not extend the hash chain");
        assert_eq!(read, tip2, "the notify commits, it does not write");

        // The real third batch, authored against the pre-notify tip, chains cleanly.
        let shard = open_follower(&follower_dir, 0).await;
        apply_ok(&shard, carrier(calls[2].0.clone(), calls[2].1, 0)).await;
        shard.close().await;
        let (write, _, _) = observed_state(&follower_dir).await;
        assert_eq!(write, tip3, "a real batch must chain cleanly on the pre-notify tip");
    });
}
