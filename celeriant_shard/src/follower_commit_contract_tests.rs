//! Blind contract tests for the follower live-TCP deferred commit
//! (Phase 1). These test the PROMISED behavior: a follower
//! applying a live replication batch advances its read (visible) cursor to
//! `max(read, min(leader_confirmed_wal_seq, write))`, never straight to `write`.
//! All `contract_*` tests are expected RED on unmodified main (today the
//! follower fully commits at fsync); `unchanged_*` tests must be green
//! throughout.
//!
//! Scaffolding notes:
//! - Real wire batches are produced by running an actual leader shard against a
//!   capturing replication client, so the follower applies exactly what a leader
//!   sends (metablocks + datablocks), not synthetic metablocks.
//! - Cursor observation is disk truth: close the shard, re-open the log segments
//!   cache on the shard dir, read the recovered header metadata. Re-opening the
//!   follower between carriers is contract-aligned (crash-restart must show
//!   less, never more).

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
    AggregateDetailsRequest, ListAggregatesRequest, ReadRequest, ReplicationBatchItem,
    ReplicationBatchRequest, SingleAggregateWrite, WatchRequest, WriteRequest,
};
use celeriant_msg::response::responses::{
    AggregateDetailsResponse, HeartbeatResult, ListAggregatesResponse, ReadResponse,
    ReplicationResult,
};
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

/// Two shard dirs under one tempdir: a leader to author real wire batches, and
/// the follower under test.
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

fn unwrap_read(result: Result<ClientResponse, ShardError>) -> ReadResponse {
    match result.expect("read should succeed") {
        ClientResponse::Read(r) => r,
        other => panic!("expected Read, got {other:?}"),
    }
}

fn unwrap_exists(result: Result<ClientResponse, ShardError>) -> AggregateDetailsResponse {
    match result.expect("exists should succeed") {
        ClientResponse::AggregateDetails(r) => r,
        other => panic!("expected AggregateDetails, got {other:?}"),
    }
}

fn unwrap_list_aggs(result: Result<ClientResponse, ShardError>) -> ListAggregatesResponse {
    match result.expect("list_aggregates should succeed") {
        ClientResponse::ListAggregates(r) => r,
        other => panic!("expected ListAggregates, got {other:?}"),
    }
}

// ── Leader-side capture of real wire batches ──

/// Records every `replicate_to_follower` call: the wire batch items and the
/// `leader_confirmed_wal_seq` (the leader's own read cursor) each carried.
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
/// leader_confirmed_wal_seq that rode with them. One awaited write per cycle,
/// so returned calls map 1:1 to writes.
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

async fn open_follower(dir: &std::path::Path) -> ShardWal<StubReplicationClient, StubS3Downloader> {
    ShardWal::open(
        test_config(dir),
        ValidatedNodeStatus::create_custom_status(NodeStatus::Follower { leader_lease_epoch: 0 }, 500, now_ms() + 10_000),
        StubReplicationClient,
        StubS3Downloader,
    )
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

/// Delivers a duplicate/stale carrier. The chain part may legitimately be
/// rejected (already applied); only the transport must not error. The contract
/// under test is the effect of its `leader_confirmed_wal_seq` on the read cursor.
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

/// Non-blocking watch poll after letting the executor settle. Broadcast happens
/// inside `handle_replication_batch` on this single-threaded executor, so after
/// the settle sleep an event is either in the channel or was never sent.
async fn next_watch_event(subscriber: &Rc<RefCell<SubscribedClient>>) -> Option<AggregateWatchEvent> {
    glommio::timer::sleep(Duration::from_millis(20)).await;
    poll_once(subscriber.borrow().receiver.recv()).await.flatten()
}

// ── Contract 1: deferred commit ──

/// INVARIANT: a follower applying a live TCP batch advances read to
/// `min(leader_confirmed_wal_seq, write)`, NOT to `write`. Entries applied and
/// durable but not yet leader-confirmed stay above the read cursor.
/// Expected RED on unmodified main (read == write after fsync today).
#[test]
fn contract_follower_apply_defers_read_to_leader_confirmed() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let calls =
            capture_leader_batches(&leader_dir, vec![write_req(key(1, 1, 1), events(1)), write_req(key(1, 1, 2), events(1))]).await;
        let tip1 = batch_tip(&calls[0].0);
        let tip2 = batch_tip(&calls[1].0);

        let shard = open_follower(&follower_dir).await;
        apply_ok(&shard, carrier(calls[0].0.clone(), 0)).await;
        apply_ok(&shard, carrier(calls[1].0.clone(), tip1)).await;
        shard.close().await;

        let (write, read) = observed_cursors(&follower_dir).await;
        assert_eq!(write, tip2, "durable write tip must cover both batches");
        assert_eq!(
            read, tip1,
            "read must sit at leader_confirmed_wal_seq ({tip1}), not commit to write ({tip2}) at fsync"
        );
    });
}

// ── Contract 2: a later carrier commits parked entries ──

/// INVARIANT: entries applied-but-unconfirmed are committed by a later carrier
/// (here a duplicate/probe-style batch) bearing a higher leader_confirmed_wal_seq.
/// Expected RED on unmodified main (first observation: read already at write).
#[test]
fn contract_covering_carrier_advances_read_over_parked_entries() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let calls =
            capture_leader_batches(&leader_dir, vec![write_req(key(1, 1, 1), events(1)), write_req(key(1, 1, 2), events(1))]).await;
        let tip1 = batch_tip(&calls[0].0);
        let tip2 = batch_tip(&calls[1].0);

        let shard = open_follower(&follower_dir).await;
        apply_ok(&shard, carrier(calls[0].0.clone(), 0)).await;
        apply_ok(&shard, carrier(calls[1].0.clone(), tip1)).await;
        shard.close().await;
        let (_, read) = observed_cursors(&follower_dir).await;
        assert_eq!(read, tip1, "tail (tip1, tip2] must be parked before the covering carrier");

        // Duplicate of the last batch, now confirming the tip — the probe/retry shape.
        let shard = open_follower(&follower_dir).await;
        deliver_carrier(&shard, carrier(calls[1].0.clone(), tip2)).await;
        shard.close().await;
        let (write, read) = observed_cursors(&follower_dir).await;
        assert_eq!(write, tip2, "covering carrier must not extend the durable chain");
        assert_eq!(read, tip2, "covering carrier must commit the parked tail");
    });
}

// ── Contract 3: monotonic guard ──

/// INVARIANT: read is monotonic non-decreasing. A stale/reordered carrier with a
/// LOWER leader_confirmed_wal_seq never moves read backwards; and it still does
/// not commit to write. Exact expected value: max(read, min(confirmed, write)).
/// Expected RED on unmodified main (read lands on write, not on tip1).
#[test]
fn contract_stale_carrier_never_regresses_read() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let calls =
            capture_leader_batches(&leader_dir, vec![write_req(key(1, 1, 1), events(1)), write_req(key(1, 1, 2), events(1))]).await;
        let tip1 = batch_tip(&calls[0].0);
        let tip2 = batch_tip(&calls[1].0);

        let shard = open_follower(&follower_dir).await;
        apply_ok(&shard, carrier(calls[0].0.clone(), 0)).await;
        apply_ok(&shard, carrier(calls[1].0.clone(), tip1)).await;
        // Reordered/retried duplicate of the first batch with its stale commit index.
        deliver_carrier(&shard, carrier(calls[0].0.clone(), 0)).await;
        shard.close().await;

        let (write, read) = observed_cursors(&follower_dir).await;
        assert_eq!(write, tip2);
        assert_eq!(
            read, tip1,
            "stale carrier must neither regress read below {tip1} nor commit it to write {tip2}"
        );
    });
}

// ── Contract 4: clamp at durable write ──

/// INVARIANT: a leader_confirmed_wal_seq ahead of the follower's durable write
/// clamps at write — read never exceeds write. The deferral stage (read == 0
/// while write == tip1) is what makes this RED on unmodified main; the clamp
/// stage's final value coincides with old behavior and constrains the new
/// implementation.
#[test]
fn contract_confirmed_ahead_of_durable_write_clamps_at_write() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let calls =
            capture_leader_batches(&leader_dir, vec![write_req(key(1, 1, 1), events(1)), write_req(key(1, 1, 2), events(1))]).await;
        let tip1 = batch_tip(&calls[0].0);
        let tip2 = batch_tip(&calls[1].0);

        let shard = open_follower(&follower_dir).await;
        apply_ok(&shard, carrier(calls[0].0.clone(), 0)).await;
        shard.close().await;
        let (write, read) = observed_cursors(&follower_dir).await;
        assert_eq!(write, tip1);
        assert_eq!(read, 0, "nothing is leader-confirmed yet, nothing may be visible");

        // Carrier claims a commit index far past what the follower holds durably.
        let shard = open_follower(&follower_dir).await;
        apply_ok(&shard, carrier(calls[1].0.clone(), tip2 + 100)).await;
        shard.close().await;
        let (write, read) = observed_cursors(&follower_dir).await;
        assert_eq!(write, tip2);
        assert_eq!(read, tip2, "read must clamp at the durable write tip");
        assert!(read <= write, "read must never exceed write");
    });
}

// ── Contract 5: positive lag and convergence ──

/// INVARIANT: under steady replication where each batch carries the previous
/// batch's tip as leader_confirmed_wal_seq (the captured, real leader behavior),
/// the follower's read cursor lags its write cursor by exactly one batch, and
/// converges to the tip once a final carrier confirms it.
/// Expected RED on unmodified main (no lag exists: read == write every step).
#[test]
fn contract_read_lags_write_by_one_batch_and_converges() {
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

        // Scaffolding sanity: the real leader confirms exactly the previous tip.
        assert_eq!(calls[0].1, 0, "first batch must carry no confirmation");
        for i in 1..calls.len() {
            assert_eq!(
                calls[i].1,
                batch_tip(&calls[i - 1].0),
                "leader must confirm the previous batch's tip on the next batch"
            );
        }

        for (batches, confirmed) in &calls {
            let tip = batch_tip(batches);
            let shard = open_follower(&follower_dir).await;
            apply_ok(&shard, carrier(batches.clone(), *confirmed)).await;
            shard.close().await;
            let (write, read) = observed_cursors(&follower_dir).await;
            assert_eq!(write, tip);
            assert_eq!(
                read, *confirmed,
                "read must lag write by one batch (read {read} at write {tip}, confirmed {confirmed})"
            );
            assert!(read < write, "under steady replication the follower must be behind its own tip");
        }

        // A final carrier confirming the tip drains the lag.
        let (last_batches, _) = calls.last().unwrap();
        let tip = batch_tip(last_batches);
        let shard = open_follower(&follower_dir).await;
        deliver_carrier(&shard, carrier(last_batches.clone(), tip)).await;
        shard.close().await;
        let (write, read) = observed_cursors(&follower_dir).await;
        assert_eq!((write, read), (tip, tip), "a covering carrier must converge read to the tip");
    });
}

// ── Contract 6a: visibility boundary — fully unconfirmed aggregate ──

/// INVARIANT: an aggregate whose entire history lies above the read cursor is
/// INVISIBLE through every reachable read surface — point read, existence
/// check, listing — and becomes visible only after a covering carrier. The
/// cursor is not the whole commit: this falsifies "deferred the cursor but not
/// the read-side set". Expected RED on unmodified main (visible at fsync).
#[test]
fn contract_unconfirmed_aggregate_invisible_on_every_read_surface() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let agg = key(1, 1, 1);
        let calls = capture_leader_batches(&leader_dir, vec![write_req(agg.clone(), events(2))]).await;
        let tip = batch_tip(&calls[0].0);

        let shard = open_follower(&follower_dir).await;
        apply_ok(&shard, carrier(calls[0].0.clone(), 0)).await;

        // Durable but unconfirmed: every read surface must say "not there".
        let read_result = shard.process_client_request(read_req(agg.clone())).await;
        assert!(
            matches!(read_result, Err(ShardError::Read(ShardReadError::AggregateNotExists))),
            "point read must not see an unconfirmed aggregate, got {read_result:?}"
        );
        let exists_result = shard.process_client_request(exists_req(agg.clone())).await;
        assert!(
            matches!(
                exists_result,
                Err(ShardError::AggregateDetails(ShardAggregateDetailsError::AggregateNotExists))
            ),
            "existence check must not see an unconfirmed aggregate, got {exists_result:?}"
        );
        let listing = unwrap_list_aggs(shard.process_client_request(list_aggs_req()).await);
        assert!(
            listing.aggregates.is_empty(),
            "listing must not surface unconfirmed aggregates, got {:?}",
            listing.aggregates
        );

        // Covering carrier confirms the tip: now, and only now, visible everywhere.
        deliver_carrier(&shard, carrier(calls[0].0.clone(), tip)).await;
        let read = unwrap_read(shard.process_client_request(read_req(agg.clone())).await);
        assert_eq!(read.event_batches.len(), 1, "confirmed aggregate must serve its batch");
        assert_eq!(read.event_batches[0].events.len(), 2);
        let details = unwrap_exists(shard.process_client_request(exists_req(agg.clone())).await);
        assert_eq!(details.max_aggregate_version, 1);
        let listing = unwrap_list_aggs(shard.process_client_request(list_aggs_req()).await);
        assert_eq!(listing.aggregates.len(), 1, "confirmed aggregate must be listed");

        shard.close().await;
    });
}

// ── Contract 6b: visibility boundary — committed prefix, uncommitted tail ──

/// INVARIANT: an aggregate with confirmed history and an unconfirmed tail
/// serves ONLY the committed prefix — point read, version details — until a
/// covering carrier confirms the tail. Expected RED on unmodified main
/// (the whole history is visible at fsync).
#[test]
fn contract_committed_prefix_served_uncommitted_tail_hidden() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let agg = key(1, 1, 1);
        let calls =
            capture_leader_batches(&leader_dir, vec![write_req(agg.clone(), events(1)), write_req(agg.clone(), events(1))]).await;
        let tip1 = batch_tip(&calls[0].0);
        let tip2 = batch_tip(&calls[1].0);

        let shard = open_follower(&follower_dir).await;
        apply_ok(&shard, carrier(calls[0].0.clone(), 0)).await;
        apply_ok(&shard, carrier(calls[1].0.clone(), tip1)).await;

        // Version 1 is leader-confirmed, version 2 is durable-but-unconfirmed.
        let read = unwrap_read(shard.process_client_request(read_req(agg.clone())).await);
        assert_eq!(
            read.event_batches.len(),
            1,
            "only the committed prefix may be served, got versions {:?}",
            read.event_batches.iter().map(|b| b.aggregate_version).collect::<Vec<_>>()
        );
        assert_eq!(read.event_batches[0].aggregate_version, 1);
        let details = unwrap_exists(shard.process_client_request(exists_req(agg.clone())).await);
        assert_eq!(details.max_aggregate_version, 1, "details must not reveal the unconfirmed tail");

        // Covering carrier confirms the tail.
        deliver_carrier(&shard, carrier(calls[1].0.clone(), tip2)).await;
        let read = unwrap_read(shard.process_client_request(read_req(agg.clone())).await);
        assert_eq!(read.event_batches.len(), 2, "confirmed tail must become readable");
        assert_eq!(read.event_batches[1].aggregate_version, 2);
        let details = unwrap_exists(shard.process_client_request(exists_req(agg.clone())).await);
        assert_eq!(details.max_aggregate_version, 2);

        shard.close().await;
    });
}

// ── Contract 7: watch fires on confirmation, not fsync ──

/// INVARIANT: a follower watch subscriber receives the event for an entry only
/// at-or-after a carrier confirms that entry — never at fsync/apply time —
/// exactly once, in wal_seq order across the confirmation boundary.
/// Expected RED on unmodified main (events fire at fsync today).
#[test]
fn contract_follower_watch_fires_on_confirmation_not_fsync() {
    glommio_test!({
        let (_tmp, leader_dir, follower_dir) = test_dirs();
        let agg_a = key(1, 1, 1);
        let agg_b = key(1, 1, 2);
        let calls =
            capture_leader_batches(&leader_dir, vec![write_req(agg_a.clone(), events(1)), write_req(agg_b.clone(), events(1))]).await;
        let tip1 = batch_tip(&calls[0].0);
        let tip2 = batch_tip(&calls[1].0);

        let shard = open_follower(&follower_dir).await;
        let (_id, subscriber) = shard.watched_aggregates().add_subscriber(watch_writes_request());

        // Apply A, unconfirmed: durable, but no watch event may fire yet.
        apply_ok(&shard, carrier(calls[0].0.clone(), 0)).await;
        let premature = next_watch_event(&subscriber).await;
        assert!(
            premature.is_none(),
            "watch event fired at fsync/apply time for an unconfirmed entry: {premature:?}"
        );

        // Apply B carrying confirmed = tip1: exactly A's event fires, B stays parked.
        apply_ok(&shard, carrier(calls[1].0.clone(), tip1)).await;
        let event = next_watch_event(&subscriber).await.expect("confirming A must release A's watch event");
        assert_eq!(event.aggregate_key, agg_a, "events must drain in wal_seq order: A before B");
        assert!(matches!(event.operation, AggregateWatchEventOperation::Write { .. }));
        let extra = next_watch_event(&subscriber).await;
        assert!(extra.is_none(), "B is not confirmed yet, its event must stay parked: {extra:?}");

        // Covering carrier confirms B: exactly B's event fires, exactly once.
        deliver_carrier(&shard, carrier(calls[1].0.clone(), tip2)).await;
        let event = next_watch_event(&subscriber).await.expect("confirming B must release B's watch event");
        assert_eq!(event.aggregate_key, agg_b);
        assert!(matches!(event.operation, AggregateWatchEventOperation::Write { .. }));
        let extra = next_watch_event(&subscriber).await;
        assert!(extra.is_none(), "no duplicates after the boundary drains: {extra:?}");

        shard.close().await;
    });
}

// ── Contract 8: standalone unchanged ──

/// INVARIANT (unchanged behavior): a Standalone shard still fully commits at
/// fsync — read == write immediately after a write, and the write is readable
/// at once. GREEN on unmodified main and must stay green.
#[test]
fn unchanged_standalone_commits_read_to_write_at_fsync() {
    glommio_test!({
        let (_tmp, _leader_dir, dir) = test_dirs();
        let shard = ShardWal::open(test_config(&dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
            .await
            .unwrap();
        let agg = key(1, 1, 1);

        let result = shard.process_client_request(write_req(agg.clone(), events(1))).await;
        assert!(matches!(result, Ok(ClientResponse::Write(_))), "standalone write failed: {:?}", result.err());

        let read = unwrap_read(shard.process_client_request(read_req(agg)).await);
        assert_eq!(read.event_batches.len(), 1, "standalone writes are visible immediately");
        shard.close().await;

        let (write, read) = observed_cursors(&dir).await;
        assert!(write > 0);
        assert_eq!(read, write, "standalone commits fully at fsync: read == write");
    });
}
