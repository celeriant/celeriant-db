//! Blind contract tests for role-transition tail reconciliation
//! (Phase 2, edges 2/3/5/8). These test the PROMISED behavior of
//! `ShardWal::reconcile_durable_tail(TailReconciliation)` per the enum's doc
//! comments: `CommitForPromotion` commits a peer-received deferred tail
//! (read == write, tail readable, parked watch events fire exactly once in
//! order); `ReconcileAsFollower` keeps a peer tail parked but still culls an
//! own-speculation tail; `RewindToAckBarrier` is unchanged.
//!
//! All `contract_*` tests are expected RED on the current code (which still
//! culls any `read < write` tail on promotion); `unchanged_*` tests must be
//! green throughout.
//!
//! Scaffolding is copied from follower_commit_contract_tests.rs: real wire
//! batches captured from a real leader shard, disk-truth cursor observation
//! via a reopened LogSegmentsCache.

use std::cell::{Cell, RefCell};
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
    AggregateDetailsRequest, ListAggregatesRequest, ReadRequest, ReplicationBatchItem,
    ReplicationBatchRequest, SingleAggregateWrite, WatchRequest, WriteRequest,
};
use celeriant_msg::response::responses::{HeartbeatResult, ListAggregatesResponse, ReplicationResult};
use celeriant_rotating_log::log_segments_cache::LogSegmentsCache;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_watch::aggregate_watch_event::{AggregateWatchEvent, AggregateWatchEventOperation};
use celeriant_watch::subscribed_client::SubscribedClient;
use futures_lite::future::poll_once;
use glommio::{LocalExecutorBuilder, Placement};

use crate::error::replication_to_follower_error::ReplicateToFollowerError;
use crate::error::replication_to_s3_error::ReplicateToS3Error;
use crate::error::send_heartbeat_error::SendHeartbeatError;
use crate::error::shard_error::ShardError;
use crate::error::shard_exists_error::ShardAggregateDetailsError;
use crate::error::shard_read_error::ShardReadError;
use crate::internal_shard_config::InternalShardConfig;
use crate::replication_client::{ReplicationClient, StubReplicationClient};
use crate::s3_downloader::StubS3Downloader;
use crate::shard_wal::{ShardWal, TailReconciliation};
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
        negative_lookup_cache_bytes: 2 * 1024 * 1024,
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

fn exists_req(agg: AggregateKey) -> ClientRequest {
    ClientRequest::AggregateDetails(AggregateDetailsRequest {
        correlation_id: None,
        aggregate_key: agg,
    })
}

fn list_aggs_req() -> ClientRequest {
    ClientRequest::ListAggregates(ListAggregatesRequest {
        correlation_id: None,
        shard_id: 0,
        org_id: None,
        aggregate_type_id: None,
        cursor: None,
    })
}

fn unwrap_list_aggs(result: Result<ClientResponse, ShardError>) -> ListAggregatesResponse {
    match result.expect("list_aggregates should succeed") {
        ClientResponse::ListAggregates(r) => r,
        other => panic!("expected ListAggregates, got {other:?}"),
    }
}

fn leader_status() -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 10_000)
}

fn follower_status() -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000)
}

// ── Leader-side capture of real wire batches ──

/// The captured leader is a DIFFERENT node from the follower under test:
/// node_ids are distinct per-data_root identities, so peer batches must carry
/// a node_id the follower does not own.
fn leader_config(dir: &std::path::Path) -> InternalShardConfig {
    let mut c = test_config(dir);
    c.node_id = 2;
    c
}

/// Records every `replicate_to_follower` call: the wire batch items and the
/// `leader_confirmed_wal_seq` each carried. When `fail` is set, both
/// replication channels error, so a leader write cannot be acked anywhere —
/// the black-box construction for an own-speculation durable tail.
#[derive(Default)]
struct CaptureToFollowerClient {
    calls: RefCell<Vec<(Vec<ReplicationBatchItem>, u64)>>,
    fail: Cell<bool>,
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
        if self.fail.get() {
            return Err(ReplicateToFollowerError::FollowerUnexpectedResponse);
        }
        self.calls.borrow_mut().push((batches, leader_confirmed_wal_seq));
        Ok(())
    }
    async fn replicate_to_s3(&self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
        if self.fail.get() {
            return Err(ReplicateToS3Error::S3Unavailable);
        }
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

async fn open_leader(dir: &std::path::Path) -> ShardWal<CaptureToFollowerClient, StubS3Downloader> {
    ShardWal::open(leader_config(dir), leader_status(), CaptureToFollowerClient::default(), StubS3Downloader)
        .await
        .unwrap()
}

/// Runs a real leader shard over the given writes and returns, per data-bearing
/// replication cycle in order: the wire batch items and the
/// leader_confirmed_wal_seq that rode with them.
async fn capture_leader_batches(
    dir: &std::path::Path,
    writes: Vec<ClientRequest>,
) -> Vec<(Vec<ReplicationBatchItem>, u64)> {
    let expected_calls = writes.len();
    let shard = open_leader(dir).await;
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

async fn open_follower(dir: &std::path::Path) -> ShardWal<StubReplicationClient, StubS3Downloader> {
    ShardWal::open(test_config(dir), follower_status(), StubReplicationClient, StubS3Downloader)
        .await
        .unwrap()
}

fn carrier(batches: Vec<ReplicationBatchItem>, leader_confirmed_wal_seq: u64) -> ReplicationBatchRequest {
    ReplicationBatchRequest {
        correlation_id: None,
        shard_id: 0,
        leader_timestamp_ms: now_ms(),
        leader_confirmed_wal_seq,
        sender_lease_epoch: 0,
        batches,
    }
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

/// Delivers a duplicate/probe-style carrier; only the transport must not error.
async fn deliver_carrier<R: ReplicationClient, D: crate::s3_downloader::S3Downloader>(
    shard: &ShardWal<R, D>,
    req: ReplicationBatchRequest,
) {
    shard.handle_replication_batch(req).await.expect("carrier transport must not error");
}

/// Disk-truth cursor observation: (write.wal_seq, read.wal_seq) recovered from
/// the persisted header. Only call after the owning shard has been closed.
async fn observed_cursors(dir: &std::path::Path) -> (u64, u64) {
    let cache = LogSegmentsCache::ready_up(dir.to_path_buf(), 4 * 1024 * 1024, 4, 1)
        .await
        .unwrap();
    let (write, read) = {
        let active = cache.active();
        let meta = active.metadata.borrow();
        (meta.write.wal_seq, meta.read.as_ref().map_or(0, |r| r.wal_seq))
    };
    cache.close().await;
    (write, read)
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

// ── Own-speculation tail construction ──

/// Black-box own-speculation tail: a leader whose replication channels go dark
/// mid-write. The first write fully acks (read == write == acked == 1); the
/// second fsyncs durably but can never ack and fences at lease expiry, leaving
/// read=1 < write=2 with last_received_replication=0 — an own-provenance tail
/// on disk. Lease validity is short so the fence fires fast.
async fn open_leader_with_own_speculation(
    dir: &std::path::Path,
    acked_agg: AggregateKey,
    spec_agg: AggregateKey,
) -> ShardWal<CaptureToFollowerClient, StubS3Downloader> {
    let status = ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 1200);
    let shard = ShardWal::open(test_config(dir), status, CaptureToFollowerClient::default(), StubS3Downloader)
        .await
        .unwrap();
    let ok = shard.process_client_request(write_req(acked_agg, events(1))).await;
    assert!(
        matches!(ok, Ok(ClientResponse::Write(_))),
        "scaffolding: acked leader write failed: {:?}",
        ok.err()
    );
    shard.replication_client.fail.set(true);
    let fenced = shard.process_client_request(write_req(spec_agg, events(1))).await;
    assert!(
        fenced.is_err(),
        "scaffolding: a dark-replication leader write must not ack, got {fenced:?}"
    );
    shard
}

/// Reopens an existing leader dir with dead replication channels and authors
/// one unackable speculative write on top of whatever the dir already holds.
async fn add_own_speculation(
    dir: &std::path::Path,
    spec_agg: AggregateKey,
) -> ShardWal<CaptureToFollowerClient, StubS3Downloader> {
    let status = ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 1200);
    let client = CaptureToFollowerClient::default();
    client.fail.set(true);
    let shard = ShardWal::open(test_config(dir), status, client, StubS3Downloader).await.unwrap();
    let fenced = shard.process_client_request(write_req(spec_agg, events(1))).await;
    assert!(
        fenced.is_err(),
        "scaffolding: a dark-replication leader write must not ack, got {fenced:?}"
    );
    shard
}

fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_recursive(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

// ── Contract 1: promotion commits the peer-received durable tail ──

/// INVARIANT: `CommitForPromotion` on a follower holding a peer-received
/// deferred tail commits the ENTIRE durable tail: read == write after, every
/// tail entry readable, nothing culled. Rows:
///   edge3 — entries durable to N, last carrier confirmed N-1 (old leader
///           acked N and died before broadcasting the commit index);
///   edge2 — wholly unacked tail (no carrier ever confirmed anything).
/// Expected RED on current code: the old cull rewinds write down to read,
/// destroying the peer tail (and with it acked writes).
#[test]
fn contract_promotion_commits_peer_received_deferred_tail() {
    glommio_test!({
        struct Row {
            name: &'static str,
            confirmed: fn(&[(Vec<ReplicationBatchItem>, u64)], usize) -> u64,
        }
        let rows = [
            Row { name: "edge3_last_carrier_confirmed_n_minus_1", confirmed: |calls, i| calls[i].1 },
            Row { name: "edge2_wholly_unacked_tail", confirmed: |_, _| 0 },
        ];
        for row in rows {
            let aggs = [key(1, 1, 1), key(1, 1, 2), key(1, 1, 3)];
            let (_tmp, leader_dir, follower_dir) = test_dirs();
            let calls = capture_leader_batches(
                &leader_dir,
                aggs.iter().map(|a| write_req(a.clone(), events(1))).collect(),
            )
            .await;
            let tip = batch_tip(&calls.last().unwrap().0);

            let shard = open_follower(&follower_dir).await;
            for i in 0..calls.len() {
                apply_ok(&shard, carrier(calls[i].0.clone(), (row.confirmed)(&calls, i))).await;
            }

            shard.node_status.set(leader_status());
            shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();

            for agg in &aggs {
                let read = shard.process_client_request(read_req(agg.clone())).await;
                assert!(
                    matches!(read, Ok(ClientResponse::Read(_))),
                    "[{}] promotion must commit the durable tail: {agg:?} must be readable, got {read:?}",
                    row.name
                );
            }
            let listing = unwrap_list_aggs(shard.process_client_request(list_aggs_req()).await);
            assert_eq!(
                listing.aggregates.len(),
                aggs.len(),
                "[{}] every tail aggregate must be listed after promotion",
                row.name
            );
            shard.close().await;

            let (write, read) = observed_cursors(&follower_dir).await;
            assert_eq!(write, tip, "[{}] promotion must not cull the durable tail", row.name);
            assert_eq!(read, tip, "[{}] promotion must commit read up to write", row.name);
        }
    });
}

// ── Contract 2: parked watch events fire at promotion (edge 8) ──

/// INVARIANT: a follower subscriber's parked events for the tail committed by
/// promotion FIRE at promotion, exactly once each, in wal_seq order; entries
/// already confirmed pre-promotion are not re-fired.
/// Expected RED on current code: the cull discards the parked tail and its
/// events never fire.
#[test]
fn contract_promotion_fires_parked_watch_events_exactly_once_in_order() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let (agg_a, agg_b, agg_c) = (key(1, 1, 1), key(1, 1, 2), key(1, 1, 3));
        let calls = capture_leader_batches(
            &leader_dir,
            vec![
                write_req(agg_a.clone(), events(1)),
                write_req(agg_b.clone(), events(1)),
                write_req(agg_c.clone(), events(1)),
            ],
        )
        .await;
        let tip1 = batch_tip(&calls[0].0);

        let shard = open_follower(&follower_dir).await;
        let (_id, subscriber) = shard.watched_aggregates().add_subscriber(watch_writes_request());

        // A parked, then confirmed by B's carrier; B and C stay parked.
        apply_ok(&shard, carrier(calls[0].0.clone(), 0)).await;
        apply_ok(&shard, carrier(calls[1].0.clone(), tip1)).await;
        apply_ok(&shard, carrier(calls[2].0.clone(), tip1)).await;

        let event = next_watch_event(&subscriber).await.expect("scaffolding: confirming A must release A's event");
        assert_eq!(event.aggregate_key, agg_a);
        let extra = next_watch_event(&subscriber).await;
        assert!(extra.is_none(), "scaffolding: B and C are unconfirmed, their events must be parked: {extra:?}");

        shard.node_status.set(leader_status());
        shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();

        let event = next_watch_event(&subscriber)
            .await
            .expect("promotion must fire the parked event for B (first of the committed tail)");
        assert_eq!(event.aggregate_key, agg_b, "parked events must fire in wal_seq order: B before C");
        assert!(matches!(event.operation, AggregateWatchEventOperation::Write { .. }));
        let event = next_watch_event(&subscriber)
            .await
            .expect("promotion must fire the parked event for C (second of the committed tail)");
        assert_eq!(event.aggregate_key, agg_c);
        let extra = next_watch_event(&subscriber).await;
        assert!(
            extra.is_none(),
            "exactly once: no duplicates for already-confirmed A, none for B/C: {extra:?}"
        );

        shard.close().await;
    });
}

// ── Contract 3: crash-restart then promote, real disk ──

/// INVARIANT: promotion commits the durable tail even when the deferred state
/// exists only on disk — the follower crashed and restarted (header shows
/// read < write, no in-memory parked state) before `CommitForPromotion`.
/// Expected RED on current code: the cull destroys the on-disk tail.
#[test]
fn contract_promotion_after_restart_commits_tail_from_disk() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let (agg_a, agg_b) = (key(1, 1, 1), key(1, 1, 2));
        let calls =
            capture_leader_batches(&leader_dir, vec![write_req(agg_a.clone(), events(1)), write_req(agg_b.clone(), events(1))]).await;
        let tip1 = batch_tip(&calls[0].0);
        let tip2 = batch_tip(&calls[1].0);

        let shard = open_follower(&follower_dir).await;
        apply_ok(&shard, carrier(calls[0].0.clone(), 0)).await;
        apply_ok(&shard, carrier(calls[1].0.clone(), tip1)).await;
        shard.close().await;
        let (write, read) = observed_cursors(&follower_dir).await;
        assert_eq!((write, read), (tip2, tip1), "scaffolding: deferred tail must be on disk before restart");

        // Restart: parked state is gone, only the header's read < write remains.
        let shard = open_follower(&follower_dir).await;
        shard.node_status.set(leader_status());
        shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();

        for agg in [&agg_a, &agg_b] {
            let read = shard.process_client_request(read_req(agg.clone())).await;
            assert!(
                matches!(read, Ok(ClientResponse::Read(_))),
                "promotion after restart must commit the on-disk tail: {agg:?} must be readable, got {read:?}"
            );
        }
        shard.close().await;

        let (write, read) = observed_cursors(&follower_dir).await;
        assert_eq!(write, tip2, "promotion after restart must not cull the on-disk tail");
        assert_eq!(read, tip2, "promotion after restart must commit read up to write");
    });
}

// ── Contract 4: ReconcileAsFollower keeps a peer-received tail ──

/// INVARIANT: `ReconcileAsFollower` with a peer-received tail KEEPS it: write
/// cursor unchanged (entries still durable), read unchanged (still invisible),
/// and a later covering carrier commits it.
/// Expected RED on current code: the old behavior destroys the tail (write
/// rewinds to read).
#[test]
fn contract_reconcile_as_follower_keeps_peer_tail_durable_and_parked() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let (agg_a, agg_b) = (key(1, 1, 1), key(1, 1, 2));
        let calls =
            capture_leader_batches(&leader_dir, vec![write_req(agg_a.clone(), events(1)), write_req(agg_b.clone(), events(1))]).await;
        let tip2 = batch_tip(&calls[1].0);

        let shard = open_follower(&follower_dir).await;
        apply_ok(&shard, carrier(calls[0].0.clone(), 0)).await;
        apply_ok(&shard, carrier(calls[1].0.clone(), 0)).await;

        shard.reconcile_durable_tail(TailReconciliation::ReconcileAsFollower).await.unwrap();

        // Still invisible: the tail is kept parked, not committed.
        let read = shard.process_client_request(read_req(agg_a.clone())).await;
        assert!(
            matches!(read, Err(ShardError::Read(ShardReadError::AggregateNotExists))),
            "ReconcileAsFollower must not commit the parked tail, got {read:?}"
        );
        shard.close().await;

        let (write, read) = observed_cursors(&follower_dir).await;
        assert_eq!(
            write, tip2,
            "ReconcileAsFollower must KEEP the peer-received durable tail (write unchanged)"
        );
        assert_eq!(read, 0, "ReconcileAsFollower must not advance read (tail stays invisible)");

        // A later covering carrier commits the kept tail.
        let shard = open_follower(&follower_dir).await;
        deliver_carrier(&shard, carrier(calls[1].0.clone(), tip2)).await;
        for agg in [&agg_a, &agg_b] {
            let read = shard.process_client_request(read_req(agg.clone())).await;
            assert!(
                matches!(read, Ok(ClientResponse::Read(_))),
                "a covering carrier must commit the kept tail: {agg:?} must be readable, got {read:?}"
            );
        }
        shard.close().await;
        let (write, read) = observed_cursors(&follower_dir).await;
        assert_eq!((write, read), (tip2, tip2), "covering carrier must converge read to the kept tail's tip");
    });
}

/// INVARIANT (edge 8, follower side): `ReconcileAsFollower` fires no watch
/// events (the tail stays parked); the covering carrier that later commits the
/// kept tail fires each parked event exactly once, in wal_seq order.
/// Expected RED on current code: the cull destroys the tail, so the covering
/// carrier (a duplicate whose chain no longer matches) commits nothing.
#[test]
fn contract_reconcile_as_follower_tail_commits_later_with_events_exactly_once() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let (agg_a, agg_b) = (key(1, 1, 1), key(1, 1, 2));
        let calls =
            capture_leader_batches(&leader_dir, vec![write_req(agg_a.clone(), events(1)), write_req(agg_b.clone(), events(1))]).await;
        let tip2 = batch_tip(&calls[1].0);

        let shard = open_follower(&follower_dir).await;
        let (_id, subscriber) = shard.watched_aggregates().add_subscriber(watch_writes_request());
        apply_ok(&shard, carrier(calls[0].0.clone(), 0)).await;
        apply_ok(&shard, carrier(calls[1].0.clone(), 0)).await;

        shard.reconcile_durable_tail(TailReconciliation::ReconcileAsFollower).await.unwrap();
        let premature = next_watch_event(&subscriber).await;
        assert!(
            premature.is_none(),
            "ReconcileAsFollower must not fire parked events (nothing committed): {premature:?}"
        );

        // Covering carrier: transport tolerated either way, the contract is its effect.
        let _ = shard.handle_replication_batch(carrier(calls[1].0.clone(), tip2)).await;

        let read = shard.process_client_request(read_req(agg_a.clone())).await;
        assert!(
            matches!(read, Ok(ClientResponse::Read(_))),
            "the tail must have survived ReconcileAsFollower for the covering carrier to commit it, got {read:?}"
        );
        let event = next_watch_event(&subscriber).await.expect("covering carrier must release A's parked event");
        assert_eq!(event.aggregate_key, agg_a, "parked events must fire in wal_seq order: A before B");
        let event = next_watch_event(&subscriber).await.expect("covering carrier must release B's parked event");
        assert_eq!(event.aggregate_key, agg_b);
        let extra = next_watch_event(&subscriber).await;
        assert!(extra.is_none(), "exactly once per parked entry: {extra:?}");

        shard.close().await;
    });
}

// ── Contract 5 (unchanged): ReconcileAsFollower culls an OWN-speculation tail ──

/// INVARIANT (unchanged behavior): `ReconcileAsFollower` on a node holding an
/// OWN-speculation tail (entries it authored as leader that never replicated —
/// the boot-after-leader-crash case) culls the tail: write rewinds to read.
/// GREEN on current code and must stay green.
#[test]
fn unchanged_reconcile_as_follower_culls_own_speculation_tail() {
    glommio_test!({
        let (_tmp, leader_dir, _follower_dir) = test_dirs();
        let (acked_agg, spec_agg) = (key(1, 1, 1), key(1, 1, 99));
        let shard = open_leader_with_own_speculation(&leader_dir, acked_agg.clone(), spec_agg.clone()).await;
        shard.close().await;
        let (write, read) = observed_cursors(&leader_dir).await;
        assert_eq!((write, read), (2, 1), "scaffolding: own-speculation tail must be on disk");

        // Boot after leader crash: reopen as follower under a peer's lease.
        let shard = open_follower(&leader_dir).await;
        shard.reconcile_durable_tail(TailReconciliation::ReconcileAsFollower).await.unwrap();

        let spec = shard.process_client_request(read_req(spec_agg.clone())).await;
        assert!(
            matches!(spec, Err(ShardError::Read(ShardReadError::AggregateNotExists))),
            "own speculation must stay invisible after the cull, got {spec:?}"
        );
        let acked = shard.process_client_request(read_req(acked_agg.clone())).await;
        assert!(
            matches!(acked, Ok(ClientResponse::Read(_))),
            "the acked prefix must survive the cull, got {acked:?}"
        );
        shard.close().await;

        let (write, read) = observed_cursors(&leader_dir).await;
        assert_eq!(write, 1, "own-speculation tail must be culled: write rewinds to read");
        assert_eq!(read, 1, "read unchanged by the own-tail cull");
    });
}

// ── Contract 6 (unchanged): demotion rewinds to the ack barrier ──

/// INVARIANT (unchanged behavior): demotion (`RewindToAckBarrier`) rewinds the
/// demoted leader's own unacked speculation before peer data is accepted.
/// GREEN on current code and must stay green.
#[test]
fn unchanged_demotion_rewind_to_ack_barrier_culls_own_tail() {
    glommio_test!({
        let (_tmp, leader_dir, _follower_dir) = test_dirs();
        let (acked_agg, spec_agg) = (key(1, 1, 1), key(1, 1, 99));
        let shard = open_leader_with_own_speculation(&leader_dir, acked_agg.clone(), spec_agg.clone()).await;

        // Graceful demotion in place.
        shard.node_status.set(follower_status());
        let culled = shard.reconcile_durable_tail(TailReconciliation::RewindToAckBarrier).await.unwrap();
        assert!(culled, "demotion must cull the unacked own tail");

        let spec = shard.process_client_request(read_req(spec_agg.clone())).await;
        assert!(
            matches!(spec, Err(ShardError::Read(ShardReadError::AggregateNotExists))),
            "culled speculation must not be readable after demotion, got {spec:?}"
        );
        shard.close().await;

        let (write, read) = observed_cursors(&leader_dir).await;
        assert_eq!((write, read), (1, 1), "demotion must rewind write and read to the ack barrier");
    });
}

// ── Contract 7: demotion then re-promotion (edge 5) ──

/// INVARIANT: after a demotion cull, a re-promotion commits exactly the peer
/// batches received since — nothing from the pre-demotion speculation
/// resurrects. Order must hold: cull first, clean peer tail after, commit on
/// re-promote.
/// Expected RED on current code: `CommitForPromotion` culls the parked peer
/// tail instead of committing it.
#[test]
fn contract_repromotion_commits_only_peer_tail_after_demotion_cull() {
    glommio_test!({
        let (_tmp, x_dir, y_dir) = test_dirs();
        let agg_p0 = key(1, 1, 1); // acked before demotion, shared prefix
        let spec_agg = key(1, 1, 99); // X's pre-demotion speculation
        let (agg_p1, agg_p2) = (key(1, 1, 2), key(1, 1, 3)); // the new leader Y's writes

        // X as leader: one fully acked write, cleanly closed.
        let calls = capture_leader_batches(&x_dir, vec![write_req(agg_p0.clone(), events(1))]).await;
        assert_eq!(batch_tip(&calls[0].0), 1);
        // Y starts as an exact copy of that prefix (it held the same replicated chain).
        copy_dir_recursive(&x_dir, &y_dir);

        // X speculates unackably, then demotes: the cull must remove the speculation.
        let shard = add_own_speculation(&x_dir, spec_agg.clone()).await;
        shard.node_status.set(follower_status());
        shard.reconcile_durable_tail(TailReconciliation::RewindToAckBarrier).await.unwrap();
        shard.close().await;
        let (write, read) = observed_cursors(&x_dir).await;
        assert_eq!((write, read), (1, 1), "scaffolding: demotion cull must leave the acked prefix only");

        // Y (the new leader) extends the shared prefix; X receives and parks the batches.
        let y_calls = capture_leader_batches(&y_dir, vec![write_req(agg_p1.clone(), events(1)), write_req(agg_p2.clone(), events(1))]).await;
        let y_tip = batch_tip(&y_calls[1].0);
        let unconfirmed = y_calls[0].1; // Y's read before its first new write: the shared prefix tip

        let shard = open_follower(&x_dir).await;
        apply_ok(&shard, carrier(y_calls[0].0.clone(), unconfirmed)).await;
        apply_ok(&shard, carrier(y_calls[1].0.clone(), unconfirmed)).await;

        // Re-promotion commits exactly the parked peer tail.
        shard.node_status.set(leader_status());
        shard.reconcile_durable_tail(TailReconciliation::CommitForPromotion).await.unwrap();

        for agg in [&agg_p0, &agg_p1, &agg_p2] {
            let read = shard.process_client_request(read_req(agg.clone())).await;
            assert!(
                matches!(read, Ok(ClientResponse::Read(_))),
                "re-promotion must commit the parked peer tail: {agg:?} must be readable, got {read:?}"
            );
        }
        let spec = shard.process_client_request(exists_req(spec_agg.clone())).await;
        assert!(
            matches!(spec, Err(ShardError::AggregateDetails(ShardAggregateDetailsError::AggregateNotExists))),
            "pre-demotion speculation must NOT resurrect on re-promotion, got {spec:?}"
        );
        let listing = unwrap_list_aggs(shard.process_client_request(list_aggs_req()).await);
        assert_eq!(listing.aggregates.len(), 3, "exactly the acked prefix plus the peer tail must be listed");
        shard.close().await;

        let (write, read) = observed_cursors(&x_dir).await;
        assert_eq!((write, read), (y_tip, y_tip), "re-promotion must commit read up to the peer tail's tip");
    });
}
