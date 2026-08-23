//! Single-flight lease elections on shard 0.
//!
//! ## What is under test
//!
//! Shard 0's executor runs several tasks that can each reach
//! `run_election_to_acquire_s3_lease`: the boot orchestrator loop, the self-renewal
//! handler (`spawn_self_renewal_handler`), and one intrashard message pump per source
//! shard (`IntrashardMessages::RenewS3LeaseNow`). Their awaits interleave, so concurrent
//! GET/CAS traffic hits `cluster/lease.json` and collides on etags.
//!
//!   (a) At most ONE lease election may be in flight per node at any instant.
//!   (b) `renew_s3_lease_on_demand` COALESCES: a request arriving while an election is
//!       in flight starts NO new lease-store traffic and returns.
//!   (c) Orchestrator-initiated elections are serialized, never dropped.
//!   (d) A CAS-written election updates `s3_cas_confirmed_at_ms` exactly as today.
//!
//! ## Coverage boundary (stated, not hidden)
//!
//! (c) is NOT asserted here. Both orchestrator entry points — `set_node_role_via_s3`
//! and the between-attempts renewal in `run_s3_catchup` — are private to `shard.rs` and
//! only reachable by driving a whole `Shard::run` against a live S3 fake, where the
//! election that fires is not addressable by the test. The nearest black-box proxy is
//! `guard_does_not_latch_after_the_in_flight_election_completes` below: it proves the
//! guard releases, which is the property a dropped-instead-of-serialized orchestrator
//! election would violate in the other direction (starvation). An implementation that
//! DROPS orchestrator elections passes these tests; a reviewer must check that arm by
//! reading it.
//!
//! Overlap is observed inside the lease store, not inferred: `GatedLeaseStore` counts
//! store calls that are concurrently in flight and remembers the maximum. A gate parks
//! the first `get_lease` mid-election so a second invocation is guaranteed to arrive
//! while the first is unfinished — the interleaving the defect needs, made deterministic.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use celeriant_crypto::pki::ClientAuthMode;
use celeriant_distributed::lease_store::{LeaseStore, LeaseStoreError, LeaseWithEtag, MembershipWithEtag};
use celeriant_distributed::node_status::NodeStatus;
use celeriant_distributed::s3_lease_config::S3LeaseConfig;
use celeriant_distributed::s3_lease_manager::S3LeaseManager;
use celeriant_distributed::validated_node_status::{unix_epoch_now_ms, ValidatedNodeStatus};
use celeriant_msg::request::requests::ReplicationBatchItem;
use celeriant_msg::response::responses::HeartbeatResult;
use celeriant_shard::error::replication_to_follower_error::ReplicateToFollowerError;
use celeriant_shard::error::replication_to_s3_error::ReplicateToS3Error;
use celeriant_shard::error::s3_catchup_error::S3CatchupError;
use celeriant_shard::error::send_heartbeat_error::SendHeartbeatError;
use celeriant_shard::internal_shard_config::InternalShardConfig;
use celeriant_shard::replication_client::ReplicationClient;
use celeriant_shard::s3_downloader::{S3Downloader, S3ObjectRef};
use celeriant_shard::shard_wal::ShardWal;
use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_wal::s3::lease::Lease;
use celeriant_wal::s3::membership::Membership;
use celeriant_wire::codec::compression::DictCodec;
use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::{LocalExecutorBuilder, Placement};

use crate::sharded::connection_handler::ConnectionContext;
use crate::sharded::routing_rule::RoutingRule;
use crate::sharded::shard::renew_s3_lease_on_demand;
use crate::sharded::shard_config::ShardConfig;

const NODE_ID: u128 = 77;
const S3_LEASE_MS: u64 = 30_000;
const DRIFT_MS: u64 = 500;

/// Only reached on a failing run; a passing run leaves as soon as the flag flips.
const PARK_DEADLINE: Duration = Duration::from_secs(5);
/// A coalesced request must return promptly. Generous by three orders of magnitude:
/// the contract says it does no store I/O at all, so anything it waits on is a bug.
const COALESCE_BUDGET: Duration = Duration::from_secs(2);

macro_rules! glommio_test {
    ($body:expr) => {
        LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async move { $body })
            .unwrap()
            .join()
            .unwrap()
    };
}

// ── Lease store: counts concurrency, and can park one election mid-flight ─────

#[derive(Default)]
struct StoreProbe {
    get_calls: Cell<u32>,
    put_conditional_calls: Cell<u32>,
    in_flight: Cell<u32>,
    max_in_flight: Cell<u32>,
    /// Set by the test; the next `get_lease` consumes it and parks.
    gate_armed: Cell<bool>,
    /// Observed by the test: a call is now parked inside `get_lease`.
    gate_entered: Cell<bool>,
    /// Set by the test to release the parked call.
    gate_open: Cell<bool>,
}

impl StoreProbe {
    fn enter(&self) {
        let n = self.in_flight.get() + 1;
        self.in_flight.set(n);
        self.max_in_flight.set(self.max_in_flight.get().max(n));
    }

    fn leave(&self) {
        self.in_flight.set(self.in_flight.get() - 1);
    }

    fn traffic(&self) -> (u32, u32) {
        (self.get_calls.get(), self.put_conditional_calls.get())
    }
}

struct GatedLeaseStore {
    probe: Rc<StoreProbe>,
    lease: RefCell<Option<LeaseWithEtag>>,
    membership: RefCell<Option<MembershipWithEtag>>,
    etag_seq: Cell<u64>,
}

impl GatedLeaseStore {
    fn next_etag(&self) -> String {
        let n = self.etag_seq.get() + 1;
        self.etag_seq.set(n);
        format!("etag-{n}")
    }
}

impl LeaseStore for GatedLeaseStore {
    async fn get_lease(&self) -> Result<Option<LeaseWithEtag>, LeaseStoreError> {
        self.probe.enter();
        self.probe.get_calls.set(self.probe.get_calls.get() + 1);
        if self.probe.gate_armed.replace(false) {
            self.probe.gate_entered.set(true);
            while !self.probe.gate_open.get() {
                glommio::timer::sleep(Duration::from_millis(1)).await;
            }
        }
        let lease = self.lease.borrow().clone();
        self.probe.leave();
        Ok(lease)
    }

    async fn put_lease_create_only(&self, lease: &Lease) -> Result<String, LeaseStoreError> {
        self.probe.enter();
        if self.lease.borrow().is_some() {
            self.probe.leave();
            return Err(LeaseStoreError::AlreadyExists);
        }
        let etag = self.next_etag();
        *self.lease.borrow_mut() = Some(LeaseWithEtag { lease: lease.clone(), etag: etag.clone() });
        self.probe.leave();
        Ok(etag)
    }

    async fn put_lease_conditional(&self, lease: &Lease, etag: &str) -> Result<String, LeaseStoreError> {
        self.probe.enter();
        self.probe.put_conditional_calls.set(self.probe.put_conditional_calls.get() + 1);
        if !self.lease.borrow().as_ref().is_some_and(|held| held.etag == etag) {
            self.probe.leave();
            return Err(LeaseStoreError::PreconditionFailed);
        }
        let new_etag = self.next_etag();
        *self.lease.borrow_mut() = Some(LeaseWithEtag { lease: lease.clone(), etag: new_etag.clone() });
        self.probe.leave();
        Ok(new_etag)
    }

    async fn get_membership(&self) -> Result<Option<MembershipWithEtag>, LeaseStoreError> {
        Ok(self.membership.borrow().clone())
    }

    async fn put_membership(&self, membership: &Membership, _etag: Option<&str>) -> Result<(), LeaseStoreError> {
        let etag = self.next_etag();
        *self.membership.borrow_mut() = Some(MembershipWithEtag { membership: membership.clone(), etag });
        Ok(())
    }
}

// ── Shard-side fakes: the WAL needs a client and a downloader, neither is exercised ──

struct IdleReplicationClient {
    reachable: Cell<bool>,
    heartbeat_in_flight: Cell<Option<u64>>,
}

impl ReplicationClient for IdleReplicationClient {
    fn set_follower_address(&self, _address: Option<String>) {}
    fn set_follower_reachable(&self, reachable: bool) { self.reachable.set(reachable); }
    fn is_follower_reachable(&self) -> bool { self.reachable.get() }
    fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> { self.heartbeat_in_flight.get() }
    fn set_heartbeat_in_flight(&self, unix_ms: Option<u64>) { self.heartbeat_in_flight.set(unix_ms); }
    fn reset_heartbeat_state(&self) { self.heartbeat_in_flight.set(None); }

    async fn replicate_to_follower(&self, _batches: Vec<ReplicationBatchItem>, _leader_confirmed_wal_seq: u64, _sender_lease_epoch: u64) -> Result<(), ReplicateToFollowerError> {
        Err(ReplicateToFollowerError::LockTimeout)
    }
    async fn replicate_to_s3(&self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> { Ok(()) }
    async fn send_heartbeat(&self, _unix_epoch_now_ms: u64, _lease_epoch: u64) -> Result<HeartbeatResult, SendHeartbeatError> {
        Err(SendHeartbeatError::UnexpectedResponse)
    }
    async fn send_kick(&self) -> Result<bool, SendHeartbeatError> { Ok(true) }
}

struct EmptyDownloader;

impl S3Downloader for EmptyDownloader {
    async fn list_objects(&self, _prefix: &str) -> Result<Vec<S3ObjectRef>, S3CatchupError> { Ok(vec![]) }
    async fn download(&self, path: &str) -> Result<Bytes, S3CatchupError> {
        Err(S3CatchupError::S3GetFailed { path: path.to_string(), message: "no objects".to_string() })
    }
    async fn delete(&self, _path: &str) -> Result<(), S3CatchupError> { Ok(()) }
}

// ── Harness ───────────────────────────────────────────────────────────────────

fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "celeriant_lease_singleflight_{tag}_{}_{}",
        std::process::id(),
        unix_epoch_now_ms()
    ));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn internal_config(dir: &std::path::Path) -> InternalShardConfig {
    InternalShardConfig {
        wal_join_data_meta_writes: true,
        node_id: NODE_ID,
        shard_id: 0,
        max_open_files: 4,
        shard_log_preallocate_bytes: 4 * 1024 * 1024,
        fsync_delay: Duration::ZERO,
        replication_delay: Duration::ZERO,
        s3_replication_delay: Duration::ZERO,
        recent_write_cache_bytes: 8 * 1024 * 1024,
        shard_dir: dir.to_path_buf(),
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 8 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 4 * 1024 * 1024,
        negative_lookup_cache_bytes: 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes: 1024,
        timestamp_config: TimestampConfig::default(),
        list_page_size: 100,
        list_max_concurrent: 16,
        read_max_concurrent: 64,
        list_max_duration: Duration::from_secs(2),
        schema_cache_bytes: 1024 * 1024,
        max_schema_size_bytes: 16384,
        max_catchup_gap_bytes: Some(100 * 1024 * 1024),
        max_promotion_batch_bytes: None,
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: dir.join("compaction"),
        max_clock_drift_ms: DRIFT_MS,
        cache_warmup_max_duration: Duration::MAX,
        replication_rollback_cooldown: Duration::ZERO,
        heartbeat_starve_threshold: Duration::ZERO,
        wal_compression_level: 3,
        dict_bytes: Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: S3_LEASE_MS,
    }
}

fn lease_config() -> S3LeaseConfig {
    S3LeaseConfig {
        node_id: NODE_ID,
        advertised_client_address: "127.0.0.1:1".to_string(),
        advertised_replication_address: "127.0.0.1:2".to_string(),
        s3_lease_duration: Duration::from_millis(S3_LEASE_MS),
        max_clock_drift: Duration::from_millis(DRIFT_MS),
    }
}

fn shard_config(dir: &std::path::Path) -> ShardConfig {
    ShardConfig {
        wal_join_data_meta_writes: true,
        node_id: NODE_ID,
        num_shards: 1,
        replication_config: Some(lease_config()),
        heartbeat_lease_duration: Duration::from_secs(5),
        heartbeat_interval_duration: Duration::from_millis(50),
        heartbeat_timeout: Duration::from_millis(500),
        heartbeat_hard_timeout_multiplier: 4,
        s3_max_concurrent_fallback_uploads: 4,
        advertised_replication_address: Some("127.0.0.1:2".to_string()),
        data_root: dir.to_path_buf(),
        listen_address: "127.0.0.1".to_string(),
        client_port: 0,
        replication_port: 0,
        max_open_files: 4,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes: 1024,
        write_max_chunk_size: 32 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        max_response_size: 16 * 1024 * 1024,
        internode_connection_timeout: Some(Duration::from_millis(200)),
        internode_request_timeout: Duration::from_millis(200),
        slow_client_timeout: Duration::from_secs(1),
        max_requested_latency: Duration::from_millis(100),
        max_watch_subscribers: 8,
        shard_log_preallocate_bytes: 4 * 1024 * 1024,
        fsync_delay: Duration::ZERO,
        preempt_timer: Duration::from_millis(100),
        replication_delay: Duration::ZERO,
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::ZERO,
        heartbeat_starve_threshold: Duration::ZERO,
        recent_write_cache_bytes: 8 * 1024 * 1024,
        routing_rule: RoutingRule::default(),
        reserve_coordinator_shard: false,
        aggregate_client_snapshots_cache_bytes: 4 * 1024 * 1024,
        negative_lookup_cache_bytes: 1024 * 1024,
        aggregate_snapshots_cache_bytes: 8 * 1024 * 1024,
        timestamp_config: TimestampConfig::default(),
        list_max_duration: Duration::from_secs(2),
        list_page_size: 100,
        list_max_concurrent: 16,
        read_max_concurrent: 64,
        schema_cache_bytes: 1024 * 1024,
        max_schema_size_bytes: 16384,
        max_clock_drift_ms: DRIFT_MS,
        max_catchup_gap_bytes: Some(100 * 1024 * 1024),
        max_promotion_batch_bytes: None,
        tls_config: None,
        tls_cert_paths: None,
        tls_client_auth: ClientAuthMode::None,
        tls_cert_reload_interval: Duration::ZERO,
        require_client_identity: false,
        api_key_hashes: RefCell::new(None),
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: Some(dir.join("compaction")),
        s3_retry_max_duration: Some(Duration::from_millis(200)),
        cache_warmup_max_duration: Some(Duration::MAX),
        dict_bytes: Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        dict_sha256: Arc::from("test-dict"),
        wal_compression_level: 3,
    }
}

type TestContext = ConnectionContext<IdleReplicationClient, EmptyDownloader, GatedLeaseStore>;

/// Shard 0 of a single-shard node, holding its own live lease, sitting at Leader with a
/// STALE CAS stamp — the exact state in which the fallback gate pokes for a renewal.
async fn leader_shard_zero(dir: &std::path::Path) -> (TestContext, Rc<StoreProbe>) {
    let shard_dir = dir.join("shard0");
    std::fs::create_dir_all(&shard_dir).expect("shard dir");

    let mesh = MeshBuilder::<crate::sharded::intrashard_messages::IntrashardMessages, Full>::full(1, 16);
    let (sender, _receivers) = mesh.join().await.expect("join mesh");

    let shard_wal = ShardWal::open(
        internal_config(&shard_dir),
        ValidatedNodeStatus::create_custom_status(
            NodeStatus::Leader { lease_epoch: 1 },
            DRIFT_MS,
            unix_epoch_now_ms() + S3_LEASE_MS,
        ),
        IdleReplicationClient { reachable: Cell::new(true), heartbeat_in_flight: Cell::new(None) },
        EmptyDownloader,
    )
    .await
    .expect("ShardWal::open");

    // Stale by construction: the debounce in `renew_s3_lease_on_demand` must not be the
    // thing that suppresses the second request. Only the single-flight guard may.
    shard_wal.s3_cas_confirmed_at_ms.set(0);

    let probe = Rc::new(StoreProbe::default());
    let store = GatedLeaseStore {
        probe: probe.clone(),
        lease: RefCell::new(Some(LeaseWithEtag {
            lease: Lease::new_initial(NODE_ID, unix_epoch_now_ms(), S3_LEASE_MS),
            etag: "seed".to_string(),
        })),
        membership: RefCell::new(None),
        etag_seq: Cell::new(0),
    };

    let ctx = ConnectionContext {
        config: Rc::new(shard_config(dir)),
        current_shard_id: 0,
        intrashard_sender: Rc::new(sender),
        shutdown_requested: Rc::new(Cell::new(false)),
        shard_wal: Rc::new(shard_wal),
        catchup_completion_tx: None,
        schema_registration_pending: None,
        lease_manager: Some(Rc::new(S3LeaseManager::new(store, lease_config()))),
        dict_codec: Rc::new(
            DictCodec::new(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES, 3).expect("builtin dict"),
        ),
        extension_redirect_sink: None,
    };
    (ctx, probe)
}

/// Polls until `cond` or the deadline; returns whether it was met. Present so a violated
/// contract terminates the test instead of hanging.
async fn wait_until(mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + PARK_DEADLINE;
    while !cond() {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        glommio::timer::sleep(Duration::from_millis(1)).await;
    }
    true
}

// ── (a) + (b): concurrent on-demand renewals ─────────────────────────────────

/// INVARIANT (a, b): two on-demand renewals that overlap must produce ONE election's
/// worth of lease-store traffic. The first parks inside `get_lease`; the second arrives
/// with that election unfinished and must start no GET and no CAS of its own.
///
/// The store's `max_in_flight` is the direct reading of (a): more than one concurrent
/// store call is the etag collision, observed rather than inferred.
#[test]
fn contract_two_concurrent_on_demand_renewals_run_exactly_one_election() {
    let dir = scratch_dir("two_concurrent");
    let d = dir.clone();
    let (max_in_flight, during_coalesce, total, coalesced_returned) = glommio_test!({
        let (ctx, probe) = leader_shard_zero(&d).await;

        probe.gate_armed.set(true);
        let first = glommio::spawn_local({
            let ctx = ctx.clone();
            async move { renew_s3_lease_on_demand(&ctx, 0).await }
        });

        assert!(
            wait_until(|| probe.gate_entered.get()).await,
            "scaffolding: the first renewal never reached the lease store, so nothing was in flight"
        );
        let before = probe.traffic();
        assert_eq!(before, (1, 0), "scaffolding: the parked election must be mid-GET, pre-CAS");

        let coalesced_returned = glommio::timer::timeout(COALESCE_BUDGET, async {
            renew_s3_lease_on_demand(&ctx, 0).await;
            Ok(())
        })
        .await
        .is_ok();
        let during_coalesce = probe.traffic();

        probe.gate_open.set(true);
        first.await;
        ctx.shard_wal.close().await;

        (probe.max_in_flight.get(), during_coalesce, probe.traffic(), coalesced_returned)
    });
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        coalesced_returned,
        "a renewal arriving during an in-flight election must RETURN, not wait on it: the \
         in-flight election's completion is what publishes the stamp the requester polls"
    );
    assert_eq!(
        max_in_flight, 1,
        "two lease-store operations were concurrently in flight — this is the concurrent \
         GET/CAS on cluster/lease.json that collides on etags"
    );
    assert_eq!(
        during_coalesce,
        (1, 0),
        "the coalesced renewal issued lease-store traffic of its own (get, put_conditional): \
         {during_coalesce:?} against (1, 0) before it ran"
    );
    assert_eq!(
        total,
        (1, 1),
        "two overlapping on-demand renewals must total ONE election: one GET, one CAS \
         (got {total:?})"
    );
}

/// INVARIANT (a, b), fan-in shape: production reaches this handler from one intrashard
/// message pump per source shard plus the self-renewal handler, so a burst of requests
/// lands during a single in-flight election. Every one of them must coalesce.
///
/// This stands in for "an on-demand renewal arriving while an ORCHESTRATOR election is
/// in flight": the orchestrator entry points are private to `shard.rs` and unreachable
/// black-box (see module header). What is shared with that case, and what this asserts,
/// is that an election already in flight suppresses all on-demand store traffic.
#[test]
fn contract_on_demand_renewals_arriving_during_an_in_flight_election_all_coalesce() {
    const PUMPS: usize = 4;

    let dir = scratch_dir("fan_in");
    let d = dir.clone();
    let (max_in_flight, during_coalesce, total, all_returned) = glommio_test!({
        let (ctx, probe) = leader_shard_zero(&d).await;

        probe.gate_armed.set(true);
        let first = glommio::spawn_local({
            let ctx = ctx.clone();
            async move { renew_s3_lease_on_demand(&ctx, 0).await }
        });
        assert!(
            wait_until(|| probe.gate_entered.get()).await,
            "scaffolding: the first renewal never reached the lease store"
        );

        let mut all_returned = true;
        for requesting_shard in 1..=PUMPS {
            all_returned &= glommio::timer::timeout(COALESCE_BUDGET, async {
                renew_s3_lease_on_demand(&ctx, requesting_shard).await;
                Ok(())
            })
            .await
            .is_ok();
        }
        let during_coalesce = probe.traffic();

        probe.gate_open.set(true);
        first.await;
        ctx.shard_wal.close().await;

        (probe.max_in_flight.get(), during_coalesce, probe.traffic(), all_returned)
    });
    let _ = std::fs::remove_dir_all(&dir);

    assert!(all_returned, "every coalesced renewal must return while the election is still in flight");
    assert_eq!(
        during_coalesce,
        (1, 0),
        "{PUMPS} renewals arriving during one in-flight election issued their own lease-store \
         traffic (get, put_conditional): {during_coalesce:?} against (1, 0)"
    );
    assert_eq!(
        max_in_flight, 1,
        "the {PUMPS}-way pump fan-in put concurrent operations on cluster/lease.json"
    );
    assert_eq!(
        total,
        (1, 1),
        "{PUMPS} coalesced renewals plus the in-flight one must total ONE election (got {total:?})"
    );
}

// ── liveness: the guard releases ──────────────────────────────────────────────

/// INVARIANT: single-flight must not latch. Once the in-flight election finishes, a later
/// request with a stale stamp runs a FRESH election — the guard is a mutex, not a fuse.
///
/// This is the liveness half of (c): an implementation that leaves the flag set, or that
/// deadlocks a coalesced caller against the survivor, strands shard 0 without a lease.
#[test]
fn contract_guard_does_not_latch_after_the_in_flight_election_completes() {
    let dir = scratch_dir("no_latch");
    let d = dir.clone();
    let (after_first_round, after_third) = glommio_test!({
        let (ctx, probe) = leader_shard_zero(&d).await;

        probe.gate_armed.set(true);
        let first = glommio::spawn_local({
            let ctx = ctx.clone();
            async move { renew_s3_lease_on_demand(&ctx, 0).await }
        });
        assert!(wait_until(|| probe.gate_entered.get()).await, "scaffolding: nothing parked");

        let _ = glommio::timer::timeout(COALESCE_BUDGET, async {
            renew_s3_lease_on_demand(&ctx, 1).await;
            Ok(())
        })
        .await;

        probe.gate_open.set(true);
        first.await;
        let after_first_round = probe.traffic();

        // The requester is still starved: stamp back to stale, gate disarmed.
        ctx.shard_wal.s3_cas_confirmed_at_ms.set(0);
        let ran = glommio::timer::timeout(COALESCE_BUDGET, async {
            renew_s3_lease_on_demand(&ctx, 1).await;
            Ok(())
        })
        .await;
        assert!(ran.is_ok(), "a renewal issued after the in-flight election completed never returned");

        let after_third = probe.traffic();
        ctx.shard_wal.close().await;
        (after_first_round, after_third)
    });
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(
        (after_third.0 - after_first_round.0, after_third.1 - after_first_round.1),
        (1, 1),
        "a stale-stamp renewal issued AFTER the in-flight election completed must run a fresh \
         election (one GET, one CAS); the guard latched instead"
    );
}

// ── (d): the survivor publishes the stamp the coalesced requester polls ──────

/// INVARIANT (b, d): coalescing costs the requester nothing. The requester that was told
/// nothing (no store traffic, immediate return) must still observe a fresh
/// `s3_cas_confirmed_at_ms` once the surviving election lands its CAS — that stamp is
/// what `run_s3_fallback` spin-waits on.
#[test]
fn contract_coalesced_requester_observes_the_stamp_from_the_surviving_election() {
    let dir = scratch_dir("stamp");
    let d = dir.clone();
    let (stamp_when_coalesced, stamp_after_survivor, cas_writes, issued_at) = glommio_test!({
        let (ctx, probe) = leader_shard_zero(&d).await;

        probe.gate_armed.set(true);
        let first = glommio::spawn_local({
            let ctx = ctx.clone();
            async move { renew_s3_lease_on_demand(&ctx, 0).await }
        });
        assert!(wait_until(|| probe.gate_entered.get()).await, "scaffolding: nothing parked");

        let issued_at = unix_epoch_now_ms();
        let _ = glommio::timer::timeout(COALESCE_BUDGET, async {
            renew_s3_lease_on_demand(&ctx, 1).await;
            Ok(())
        })
        .await;
        let stamp_when_coalesced = ctx.shard_wal.s3_cas_confirmed_at_ms.get();

        probe.gate_open.set(true);
        first.await;
        let stamp_after_survivor = ctx.shard_wal.s3_cas_confirmed_at_ms.get();
        let cas_writes = probe.put_conditional_calls.get();
        ctx.shard_wal.close().await;

        (stamp_when_coalesced, stamp_after_survivor, cas_writes, issued_at)
    });
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        stamp_after_survivor >= issued_at,
        "the coalesced requester was left on a stale stamp: it saw {stamp_when_coalesced} at its \
         own return and {stamp_after_survivor} after the survivor completed, against a request \
         issued at {issued_at}. Coalescing must not cost the requester its green light"
    );
    assert_eq!(cas_writes, 1, "the survivor must be the only CAS write");
}
