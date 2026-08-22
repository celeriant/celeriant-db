//! client_seq deduplication state is not carried across
//! a leader handover. A write whose ack was lost is retried with the same
//! (client_id, client_seq); the old leader rejects it, but the node that
//! promotes accepts it as new even though the original is already durably
//! committed in its own WAL — same (client_id, client_seq) committed twice,
//! final aggregate_version = acks + 1. Confirmed on disk by three chaos
//! reproductions (session-v4/progress.md).
//!
//! Contract under test, stated behaviorally so it holds whichever cache path a
//! fix chooses: once data containing (client C, client_seq N) for aggregate A
//! is durable and committed on this node — however it arrived — a leader-path
//! write for (C, seq <= N) on A must be rejected, never committed as a new
//! version.
//!
//! `arrives_via_*` tests are expected RED; `arrives_via_boot_warmup_*` pins the
//! one path that already works, so a fix cannot regress it.
//!
//! Scaffolding follows follower_commit_contract_tests.rs: real wire batches
//! authored by a real leader shard, replayed into the node under test either as
//! S3 fallback files or as live TCP replication carriers.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use celeriant_distributed::node_status::NodeStatus;
use celeriant_distributed::paths::fallback_batch_path;
use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::{
    AggregateDetailsRequest, ReplicationBatchItem, ReplicationBatchRequest, SingleAggregateWrite,
    WriteRequest,
};
use celeriant_msg::response::responses::{HeartbeatResult, ReplicationResult};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::s3::fallback_batch::{FallbackBatch, FallbackItem};
use glommio::{LocalExecutorBuilder, Placement};

use crate::catchup_test_support::{serialize_fallback_batch, MockDownloader};
use crate::error::replication_to_follower_error::ReplicateToFollowerError;
use crate::error::replication_to_s3_error::ReplicateToS3Error;
use crate::error::send_heartbeat_error::SendHeartbeatError;
use crate::error::shard_error::ShardError;
use crate::error::shard_write_error::ShardWriteError;
use crate::internal_shard_config::InternalShardConfig;
use crate::replication_client::{ReplicationClient, StubReplicationClient};
use crate::s3_downloader::{S3Downloader, StubS3Downloader};
use crate::shard_wal::{ShardWal, TailReconciliation};
use crate::shard_wal_s3_catchup::CatchupRole;
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

const SHARD_ID: u32 = 1;
const NODE_UNDER_TEST: u128 = 1;
const PEER_NODE: u128 = 2;
const CLIENT: u128 = 7;
const LAST_SEQ: u64 = 3;

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn test_dirs() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let peer = tmp.path().join("peer");
    let node = tmp.path().join("node");
    (tmp, peer, node)
}

fn test_config(dir: &std::path::Path, node_id: u128) -> InternalShardConfig {
    InternalShardConfig {
        node_id,
        shard_id: SHARD_ID,
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

fn agg() -> AggregateKey {
    AggregateKey::new(1, 1, 1)
}

/// One event carrying exactly `client_seq`: the client's Nth idempotent write.
fn idem_write(client_seq: u64) -> ClientRequest {
    let mut writes = HashMap::new();
    writes.insert(
        agg(),
        SingleAggregateWrite {
            events: vec![DatablockAggregateEvent {
                client_seq,
                event_type_major: 1,
                event_value: Arc::new(vec![client_seq as u8; 8]),
                ..Default::default()
            }],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: true,
        },
    );
    ClientRequest::Write(WriteRequest {
        correlation_id: None,
        client_id: CLIENT,
        user_id: None,
        writes,
    })
}

fn details_req() -> ClientRequest {
    ClientRequest::AggregateDetails(AggregateDetailsRequest {
        correlation_id: None,
        aggregate_key: agg(),
    })
}

async fn current_version<R: ReplicationClient, D: S3Downloader>(shard: &ShardWal<R, D>) -> u64 {
    match shard.process_client_request(details_req()).await {
        Ok(ClientResponse::AggregateDetails(d)) => d.max_aggregate_version,
        other => panic!("aggregate details must be readable, got {other:?}"),
    }
}

/// The decisive assertion: the replayed write must be refused as a duplicate.
/// Either idempotency rejection counts (both mean "already have this seq");
/// what must never happen is a commit at a fresh version.
fn assert_rejected_as_duplicate(result: Result<ClientResponse, ShardError>, arrival: &str) {
    match result {
        Err(ShardError::Write(ShardWriteError::ClientIdempotencyViolation { .. }))
        | Err(ShardError::Write(ShardWriteError::InflightDuplicateWrite { .. })) => {}
        Ok(ClientResponse::Write(w)) => panic!(
            "replayed (client {CLIENT}, seq {LAST_SEQ}) was COMMITTED as a new version \
             {:?} after arriving via {arrival}; the original is already durable on this node",
            w.max_aggregate_version
        ),
        other => panic!("expected an idempotency rejection after {arrival}, got {other:?}"),
    }
}

// ── Peer-authored history ──

/// Records the wire batches a real leader emits, one data-bearing cycle per
/// awaited write.
#[derive(Default)]
struct CaptureClient {
    calls: RefCell<Vec<Vec<ReplicationBatchItem>>>,
}

impl ReplicationClient for CaptureClient {
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
        _leader_confirmed_wal_seq: u64,
        _sender_lease_epoch: u64,
    ) -> Result<(), ReplicateToFollowerError> {
        if !batches.is_empty() {
            self.calls.borrow_mut().push(batches);
        }
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

fn leader_status() -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 0 }, 500, now_ms() + 10_000)
}

/// Runs a real leader over client `CLIENT`'s idempotent writes seq 1..=LAST_SEQ
/// and returns the wire batches it replicated, in order. The last one is the
/// write whose ack the client never sees.
async fn peer_history(dir: &std::path::Path) -> Vec<Vec<ReplicationBatchItem>> {
    let shard = ShardWal::open(test_config(dir, PEER_NODE), leader_status(), CaptureClient::default(), StubS3Downloader)
        .await
        .unwrap();
    for seq in 1..=LAST_SEQ {
        let result = shard.process_client_request(idem_write(seq)).await;
        assert!(
            matches!(result, Ok(ClientResponse::Write(_))),
            "scaffolding: leader write seq {seq} failed: {:?}",
            result.err()
        );
    }
    // The old leader rejects the client's retry — the behavior the promoted
    // node must also show.
    let retry = shard.process_client_request(idem_write(LAST_SEQ)).await;
    assert!(
        matches!(
            retry,
            Err(ShardError::Write(ShardWriteError::ClientIdempotencyViolation { .. }))
                | Err(ShardError::Write(ShardWriteError::InflightDuplicateWrite { .. }))
        ),
        "scaffolding: the authoring leader must reject the replay, got {retry:?}"
    );
    assert_eq!(current_version(&shard).await, LAST_SEQ, "scaffolding: one version per accepted write");
    shard.close().await;
    let calls = shard.replication_client.calls.borrow().clone();
    assert_eq!(calls.len(), LAST_SEQ as usize, "scaffolding: one data-bearing cycle per awaited write");
    calls
}

fn tip(batches: &[ReplicationBatchItem]) -> u64 {
    batches.last().expect("captured batch must not be empty").metablock.wal_seq
}

/// One S3 fallback file per replication cycle, uploaded by the peer — exactly
/// what a leader's S3 fallback path lands for a node that is behind.
fn fallback_file(batches: &[ReplicationBatchItem]) -> (String, Bytes) {
    let start = batches.first().unwrap().metablock.wal_seq;
    let end = tip(batches);
    let mut batch = FallbackBatch::new(start, end, SHARD_ID, PEER_NODE, start, 0);
    for item in batches {
        batch.push_item(FallbackItem {
            metablock: item.metablock.clone(),
            datablock: item.datablock.clone(),
        });
    }
    (fallback_batch_path(SHARD_ID, start, end, PEER_NODE), serialize_fallback_batch(&batch))
}

fn carrier(batches: Vec<ReplicationBatchItem>, leader_confirmed_wal_seq: u64) -> ReplicationBatchRequest {
    ReplicationBatchRequest {
        correlation_id: None,
        shard_id: SHARD_ID as u64,
        leader_timestamp_ms: now_ms(),
        leader_confirmed_wal_seq,
        sender_lease_epoch: 0,
        batches,
    }
}

fn promoting_status() -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(NodeStatus::Promoting { lease_epoch: 1 }, 500, now_ms() + 10_000)
}

fn follower_status() -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000)
}

/// Finish the promotion: flip to Leader at the next epoch and commit whatever
/// the catchup/replication left durable-but-unconfirmed.
async fn finish_promotion<R: ReplicationClient, D: S3Downloader>(shard: &ShardWal<R, D>) {
    shard
        .node_status
        .set(ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 1 }, 500, now_ms() + 10_000));
    shard
        .reconcile_durable_tail(TailReconciliation::CommitForPromotion)
        .await
        .expect("promotion reconciliation must not error");
}

/// Applies the peer's cycles as live TCP carriers, each confirming the previous
/// tip, then a final commit-notify confirming the last one — the leader's view
/// right before it dies.
async fn replicate_cycles<R: ReplicationClient, D: S3Downloader>(shard: &ShardWal<R, D>, cycles: &[Vec<ReplicationBatchItem>]) {
    let mut confirmed = 0;
    for batches in cycles {
        let batch_tip = tip(batches);
        let resp = shard
            .handle_replication_batch(carrier(batches.clone(), confirmed))
            .await
            .expect("replication must not error");
        assert!(
            matches!(resp.result, ReplicationResult::Success { .. }),
            "scaffolding: follower must accept the peer's batch, got {:?}",
            resp.result
        );
        confirmed = batch_tip;
    }
    shard
        .handle_replication_batch(carrier(Vec::new(), tip(cycles.last().expect("at least one cycle"))))
        .await
        .expect("commit-notify must not error");
}

/// The field shape's precondition: this node already holds part of the client's
/// history locally and has RESTARTED over it, so boot warmup has a live — and
/// now stale — dedup entry for (CLIENT, seq 1) in cache.
async fn seed_local_prefix_then_restart(dir: &std::path::Path, cycles: &[Vec<ReplicationBatchItem>]) {
    let shard = ShardWal::open(test_config(dir, NODE_UNDER_TEST), follower_status(), StubReplicationClient, StubS3Downloader)
        .await
        .unwrap();
    replicate_cycles(&shard, cycles).await;
    shard.close().await;
}

// ── A1: newer history arrives via S3 catchup over a warm dedup entry ──

/// INVARIANT: a restarted node whose warmup cached (CLIENT, seq 1) then
/// consumed the peer's remaining acked history from S3 during its promotion
/// holds (CLIENT, seq 1..=LAST_SEQ) durably. The client's retry of seq
/// LAST_SEQ — whose ack was lost at the old leader — must be rejected as a
/// duplicate, not committed at version LAST_SEQ+1.
///
/// This is the field shape: the catchup apply path never refreshes the
/// client_seq dedup entry, and a warm entry means the cache-miss disk scan
/// (which the cold-cache test below shows does find the truth) never runs.
#[test]
fn a_replayed_client_seq_is_rejected_after_promotion_via_s3_catchup() {
    glommio_test!({
        let (_tmp, peer_dir, node_dir) = test_dirs();
        let history = peer_history(&peer_dir).await;
        seed_local_prefix_then_restart(&node_dir, &history[..1]).await;

        let downloader = MockDownloader::new();
        for batches in &history[1..] {
            let (path, bytes) = fallback_file(batches);
            downloader.insert(path, bytes);
        }

        let shard = ShardWal::open(test_config(&node_dir, NODE_UNDER_TEST), promoting_status(), StubReplicationClient, downloader)
            .await
            .unwrap();
        // Warm the dedup entry exactly as a live node does: a read of the
        // aggregate the client is writing to, served before the handover.
        assert_eq!(current_version(&shard).await, 1, "scaffolding: the restart must recover the local prefix");

        shard.enter_s3_catchup(CatchupRole::Promoting).await.expect("promotion catchup must not error");
        finish_promotion(&shard).await;

        assert_eq!(
            current_version(&shard).await,
            LAST_SEQ,
            "scaffolding: catchup must have applied the peer's whole acked history"
        );

        let replay = shard.process_client_request(idem_write(LAST_SEQ)).await;
        assert_rejected_as_duplicate(replay, "S3 catchup over a warm dedup entry");
        assert_eq!(
            current_version(&shard).await,
            LAST_SEQ,
            "a rejected replay must leave the aggregate at version {LAST_SEQ}"
        );

        shard.close().await;
    });
}

// ── A2: newer history arrives via TCP replication over a warm dedup entry ──

/// INVARIANT: same contract when the remaining history arrived over live TCP
/// replication and the follower is then promoted. Distinguishes the two apply
/// paths: if only one of A1/A2 is red, only that apply path fails to refresh
/// the dedup state.
#[test]
fn a_replayed_client_seq_is_rejected_after_promotion_via_tcp_replication() {
    glommio_test!({
        let (_tmp, peer_dir, node_dir) = test_dirs();
        let history = peer_history(&peer_dir).await;
        seed_local_prefix_then_restart(&node_dir, &history[..1]).await;

        let shard = ShardWal::open(test_config(&node_dir, NODE_UNDER_TEST), follower_status(), StubReplicationClient, StubS3Downloader)
            .await
            .unwrap();
        assert_eq!(current_version(&shard).await, 1, "scaffolding: the restart must recover the local prefix");

        replicate_cycles(&shard, &history[1..]).await;
        finish_promotion(&shard).await;

        assert_eq!(
            current_version(&shard).await,
            LAST_SEQ,
            "scaffolding: replication must have applied the peer's whole acked history"
        );

        let replay = shard.process_client_request(idem_write(LAST_SEQ)).await;
        assert_rejected_as_duplicate(replay, "TCP replication over a warm dedup entry");
        assert_eq!(
            current_version(&shard).await,
            LAST_SEQ,
            "a rejected replay must leave the aggregate at version {LAST_SEQ}"
        );

        shard.close().await;
    });
}

/// with nothing cached for (CLIENT, agg), the
/// write path's cache-miss disk scan reads the catchup-applied truth off the
/// local WAL and rejects the replay
#[test]
fn a_replayed_client_seq_is_rejected_after_cold_cache_promotion_via_s3_catchup() {
    glommio_test!({
        let (_tmp, peer_dir, node_dir) = test_dirs();
        let history = peer_history(&peer_dir).await;

        let downloader = MockDownloader::new();
        for batches in &history {
            let (path, bytes) = fallback_file(batches);
            downloader.insert(path, bytes);
        }

        let shard = ShardWal::open(test_config(&node_dir, NODE_UNDER_TEST), promoting_status(), StubReplicationClient, downloader)
            .await
            .unwrap();
        shard.enter_s3_catchup(CatchupRole::Promoting).await.expect("promotion catchup must not error");
        finish_promotion(&shard).await;

        assert_eq!(current_version(&shard).await, LAST_SEQ, "scaffolding: catchup must have applied the whole history");

        let replay = shard.process_client_request(idem_write(LAST_SEQ)).await;
        assert_rejected_as_duplicate(replay, "S3 catchup with a cold dedup entry");
        assert_eq!(current_version(&shard).await, LAST_SEQ, "a rejected replay must leave the aggregate at version {LAST_SEQ}");

        shard.close().await;
    });
}

/// INVARIANT (already honored): the cold-cache counterpart for the follower
/// replication apply path. GREEN today; pinned alongside its S3 twin.
#[test]
fn a_replayed_client_seq_is_rejected_after_cold_cache_promotion_via_tcp_replication() {
    glommio_test!({
        let (_tmp, peer_dir, node_dir) = test_dirs();
        let history = peer_history(&peer_dir).await;

        let shard = ShardWal::open(test_config(&node_dir, NODE_UNDER_TEST), follower_status(), StubReplicationClient, StubS3Downloader)
            .await
            .unwrap();
        replicate_cycles(&shard, &history).await;
        finish_promotion(&shard).await;

        assert_eq!(current_version(&shard).await, LAST_SEQ, "scaffolding: replication must have applied the whole history");

        let replay = shard.process_client_request(idem_write(LAST_SEQ)).await;
        assert_rejected_as_duplicate(replay, "TCP replication with a cold dedup entry");
        assert_eq!(current_version(&shard).await, LAST_SEQ, "a rejected replay must leave the aggregate at version {LAST_SEQ}");

        shard.close().await;
    });
}

/// a node that authored the history itself and
/// restarted rebuilds its dedup state from the local WAL during warmup, so the
/// replay is rejected
#[test]
fn a_replayed_client_seq_is_rejected_after_restart_via_boot_warmup() {
    glommio_test!({
        let (_tmp, node_dir, _unused) = test_dirs();
        let shard = ShardWal::open(test_config(&node_dir, NODE_UNDER_TEST), leader_status(), CaptureClient::default(), StubS3Downloader)
            .await
            .unwrap();
        for seq in 1..=LAST_SEQ {
            let result = shard.process_client_request(idem_write(seq)).await;
            assert!(matches!(result, Ok(ClientResponse::Write(_))), "scaffolding: write seq {seq} failed: {:?}", result.err());
        }
        shard.close().await;

        let shard = ShardWal::open(test_config(&node_dir, NODE_UNDER_TEST), leader_status(), CaptureClient::default(), StubS3Downloader)
            .await
            .unwrap();
        assert_eq!(current_version(&shard).await, LAST_SEQ, "scaffolding: the restart must recover the history");

        let replay = shard.process_client_request(idem_write(LAST_SEQ)).await;
        assert_rejected_as_duplicate(replay, "boot warmup over the local WAL");
        assert_eq!(current_version(&shard).await, LAST_SEQ, "a rejected replay must leave the aggregate at version {LAST_SEQ}");

        shard.close().await;
    });
}
