//! Blind contract tests for the lease orchestrator's status handling.
//!
//! C1 — For every status the system can install, the orchestrator loop must
//!      have a branch that does real work. A status matching no branch means
//!      the loop body reaches its end having done nothing.
//! C2 — When the election helper returns `Err`, the status the node is left
//!      holding must be one the orchestrator can act on: never a transient,
//!      intermediate status only the success path knows how to clear.
//! C3 — Every call site of that helper must uphold C2, whatever it does with
//!      the error (propagate, panic, swallow).
//!
//! Everything here is observed from outside the shard: a real `Shard` is built
//! from `Shard::new` with the same wiring `celeriant_runtimes::lib` uses (real
//! full mesh, real `ShardWal` on a real directory, real TCP listeners, real
//! `S3LeaseManager`), and driven through the public `Shard::run`. The only
//! substitutions are at the process boundary — S3 (`FakeLeaseStore`,
//! `ScriptedDownloader`) and the peer link (`CountingReplicationClient`) —
//! following the established `StubReplicationClient` / `StubS3Downloader`
//! pattern in `celeriant_shard`.
//!
//! "Real work" is deliberately defined mechanism-agnostically, so these tests
//! survive a refactor of the loop: within the observation window the node must
//! either move off the status it started on, touch the lease object, or send a
//! heartbeat. A branch that renews, discovers, challenges, catches up or
//! heartbeats produces at least one of those. A status that matches no branch
//! produces none.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::net::TcpListener;
use glommio::{LocalExecutorBuilder, Placement};

use crate::sharded::routing_rule::RoutingRule;
use crate::sharded::shard::Shard;
use crate::sharded::shard_config::ShardConfig;

/// One node per test, and each one binds listeners, an io_uring and a shard
/// directory. Serialised so a slow row cannot be mistaken for a starved one.
static NODE_LOCK: Mutex<()> = Mutex::new(());

const NODE_ID: u128 = 7;
const S3_LEASE_MS: u64 = 30_000;
const DRIFT_MS: u64 = 500;

// ── Observation ──

/// Everything the outside world can see about one run. Shared with the shard's
/// executor, so it survives that executor dying.
#[derive(Default)]
struct Probe {
    /// Reads and writes of the lease object only. Boot-time membership
    /// registration never touches it, so this counts election activity and
    /// nothing else.
    lease_object_calls: AtomicU32,
    heartbeats: AtomicU32,
    /// Ticks of a 10ms timer co-resident on the shard's executor. A loop that
    /// spins without awaiting starves it.
    ticks: AtomicU32,
    /// Raw status last sampled by the watcher, as a `node_status_code`.
    last_raw: AtomicU64,
    last_effective: AtomicU64,
    saw_promoting: AtomicBool,
}

/// Stable ordinal per status variant so the watcher can publish a sample into
/// an atomic without a lock on the shard's executor.
fn code(status: NodeStatus) -> u64 {
    match status {
        NodeStatus::BootCatchup => 0,
        NodeStatus::Follower { .. } => 1,
        NodeStatus::FollowerCatchingUp { .. } => 2,
        NodeStatus::Promoting { .. } => 3,
        NodeStatus::Leader { .. } => 4,
        NodeStatus::Fenced => 5,
        NodeStatus::Standalone => 6,
    }
}

fn status_name(code: u64) -> &'static str {
    match code {
        0 => "BootCatchup",
        1 => "Follower",
        2 => "FollowerCatchingUp",
        3 => "Promoting",
        4 => "Leader",
        5 => "Fenced",
        6 => "Standalone",
        _ => "<unsampled>",
    }
}

/// Statuses covered by the orchestrator's arms: leader, follower-or-fenced,
/// and catching-up. This restates the loop's own classification, so it needs
/// updating if the loop grows an arm — the behavioural tests are the
/// authority; this is the fast reading of the same fact.
fn orchestrator_can_act_on(effective: u64) -> bool {
    matches!(effective, 4 /* Leader */ | 1 /* Follower */ | 5 /* Fenced */ | 0 /* BootCatchup */ | 2 /* FollowerCatchingUp */)
}

// ── Test doubles at the process boundary ──

/// The faults one run injects at the process boundary.
#[derive(Clone, Copy)]
struct Scenario {
    /// S3 outage for the catchup path: every list fails.
    list_fails: bool,
    /// S3 outage for the election path: every lease-object read/write fails.
    lease_ops_fail: bool,
    /// Heartbeats ack this many times, then fail — the peer going unreachable
    /// under a leader, which is what arms the preemptive renewal call site.
    heartbeat_acks: u32,
    /// Seed `lease.json` as a live lease already held by this node.
    seed_own_live_lease: bool,
}

impl Default for Scenario {
    /// A healthy node talking to a healthy, empty S3.
    fn default() -> Self {
        Self { list_fails: false, lease_ops_fail: false, heartbeat_acks: u32::MAX, seed_own_live_lease: false }
    }
}

struct FakeLeaseStore {
    lease: std::cell::RefCell<Option<LeaseWithEtag>>,
    membership: std::cell::RefCell<Option<MembershipWithEtag>>,
    etag_seq: std::cell::Cell<u64>,
    fail_lease_ops: bool,
    probe: Arc<Probe>,
}

impl FakeLeaseStore {
    fn new(probe: Arc<Probe>, seed: Option<Lease>, fail_lease_ops: bool) -> Self {
        Self {
            lease: std::cell::RefCell::new(seed.map(|lease| LeaseWithEtag { lease, etag: "seed".to_string() })),
            membership: std::cell::RefCell::new(None),
            etag_seq: std::cell::Cell::new(0),
            fail_lease_ops,
            probe,
        }
    }

    fn injected_outage(&self) -> Result<(), LeaseStoreError> {
        if self.fail_lease_ops {
            return Err(LeaseStoreError::Unavailable { message: "injected S3 outage".to_string() });
        }
        Ok(())
    }

    fn next_etag(&self) -> String {
        let n = self.etag_seq.get() + 1;
        self.etag_seq.set(n);
        format!("etag-{n}")
    }
}

impl LeaseStore for FakeLeaseStore {
    async fn get_lease(&self) -> Result<Option<LeaseWithEtag>, LeaseStoreError> {
        self.probe.lease_object_calls.fetch_add(1, Ordering::SeqCst);
        self.injected_outage()?;
        Ok(self.lease.borrow().clone())
    }

    async fn put_lease_create_only(&self, lease: &Lease) -> Result<String, LeaseStoreError> {
        self.probe.lease_object_calls.fetch_add(1, Ordering::SeqCst);
        self.injected_outage()?;
        if self.lease.borrow().is_some() {
            return Err(LeaseStoreError::AlreadyExists);
        }
        let etag = self.next_etag();
        *self.lease.borrow_mut() = Some(LeaseWithEtag { lease: lease.clone(), etag: etag.clone() });
        Ok(etag)
    }

    async fn put_lease_conditional(&self, lease: &Lease, etag: &str) -> Result<String, LeaseStoreError> {
        self.probe.lease_object_calls.fetch_add(1, Ordering::SeqCst);
        self.injected_outage()?;
        let matches = self.lease.borrow().as_ref().is_some_and(|held| held.etag == etag);
        if !matches {
            return Err(LeaseStoreError::PreconditionFailed);
        }
        let new_etag = self.next_etag();
        *self.lease.borrow_mut() = Some(LeaseWithEtag { lease: lease.clone(), etag: new_etag.clone() });
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

/// Empty S3 view, or a hard list failure standing in for an S3 outage.
struct ScriptedDownloader {
    list_fails: bool,
}

impl S3Downloader for ScriptedDownloader {
    async fn list_objects(&self, prefix: &str) -> Result<Vec<S3ObjectRef>, S3CatchupError> {
        glommio::timer::sleep(Duration::from_millis(1)).await;
        if self.list_fails {
            return Err(S3CatchupError::S3ListFailed {
                prefix: prefix.to_string(),
                message: "injected S3 outage".to_string(),
            });
        }
        Ok(vec![])
    }

    async fn download(&self, path: &str) -> Result<Bytes, S3CatchupError> {
        Err(S3CatchupError::S3GetFailed { path: path.to_string(), message: "no objects".to_string() })
    }

    async fn delete(&self, _path: &str) -> Result<(), S3CatchupError> {
        Ok(())
    }
}

/// Counts heartbeats and acks the first `acks` of them; nothing else on the
/// peer link is exercised by the orchestrator.
struct CountingReplicationClient {
    probe: Arc<Probe>,
    reachable: std::cell::Cell<bool>,
    heartbeat_in_flight: std::cell::Cell<Option<u64>>,
    acks: u32,
}

impl ReplicationClient for CountingReplicationClient {
    fn set_follower_address(&self, _address: Option<String>) {}
    fn set_follower_reachable(&self, reachable: bool) { self.reachable.set(reachable); }
    fn is_follower_reachable(&self) -> bool { self.reachable.get() }
    fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> { self.heartbeat_in_flight.get() }
    fn set_heartbeat_in_flight(&self, unix_ms: Option<u64>) { self.heartbeat_in_flight.set(unix_ms); }
    fn reset_heartbeat_state(&self) { self.heartbeat_in_flight.set(None); }

    async fn replicate_to_follower(&self, _batches: Vec<ReplicationBatchItem>, _leader_confirmed_wal_seq: u64, _sender_lease_epoch: u64) -> Result<(), ReplicateToFollowerError> {
        Ok(())
    }

    async fn replicate_to_s3(&self, _batches: Vec<ReplicationBatchItem>) -> Result<(), ReplicateToS3Error> {
        Ok(())
    }

    async fn send_heartbeat(&self, unix_epoch_now_ms: u64, _lease_epoch: u64) -> Result<HeartbeatResult, SendHeartbeatError> {
        let n = self.probe.heartbeats.fetch_add(1, Ordering::SeqCst);
        glommio::timer::sleep(Duration::from_millis(5)).await;
        if n >= self.acks {
            return Err(SendHeartbeatError::UnexpectedResponse);
        }
        Ok(HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms, follower_can_accept_tcp_replication: true })
    }

    async fn send_kick(&self) -> Result<bool, SendHeartbeatError> { Ok(true) }
}

// ── Node harness ──

struct NodeRun {
    probe: Arc<Probe>,
    /// False when the shard's executor died (a panic in a detached task).
    executor_survived: bool,
}

fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "celeriant_orchestrator_contract_{tag}_{}_{}",
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
        s3_replication_delay: Duration::from_millis(500),
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

fn shard_config(dir: &std::path::Path) -> ShardConfig {
    ShardConfig {
        wal_join_data_meta_writes: true,
        node_id: NODE_ID,
        num_shards: 1,
        replication_config: Some(S3LeaseConfig {
            node_id: NODE_ID,
            advertised_client_address: "127.0.0.1:1".to_string(),
            advertised_replication_address: "127.0.0.1:2".to_string(),
            s3_lease_duration: Duration::from_millis(S3_LEASE_MS),
            max_clock_drift: Duration::from_millis(DRIFT_MS),
        }),
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
        api_key_hashes: std::cell::RefCell::new(None),
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

/// Boots one real single-shard node holding `initial`, runs it until the
/// watcher sees progress or `budget` elapses, then shuts it down.
///
/// `stop_on_progress` is false for the C2/C3 run, which must let the node keep
/// running past its first S3 contact to reach the election's error path.
fn run_node(
    tag: &str,
    make_initial: fn() -> ValidatedNodeStatus,
    scenario: Scenario,
    budget: Duration,
    stop_on_progress: bool,
) -> NodeRun {
    let _guard = NODE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Built under the lock: a lease TTL must be measured from this node's boot,
    // not from whenever the test was queued behind another node's run.
    let initial = make_initial();
    let probe = Arc::new(Probe::default());
    let dir = scratch_dir(tag);
    let initial_code = code(initial.raw());

    let executor_survived = {
        let probe = probe.clone();
        let dir = dir.clone();
        LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let mesh = MeshBuilder::<crate::sharded::intrashard_messages::IntrashardMessages, Full>::full(1, 16);
                let (sender, receivers) = mesh.join().await.expect("join mesh");

                let client_listener = TcpListener::bind("127.0.0.1:0").expect("client listener");
                let replication_listener = TcpListener::bind("127.0.0.1:0").expect("replication listener");

                let shard_dir = dir.join("shard0");
                std::fs::create_dir_all(&shard_dir).expect("shard dir");
                let shard_wal = ShardWal::open(
                    internal_config(&shard_dir),
                    initial,
                    CountingReplicationClient {
                        probe: probe.clone(),
                        reachable: std::cell::Cell::new(true),
                        heartbeat_in_flight: std::cell::Cell::new(None),
                        acks: scenario.heartbeat_acks,
                    },
                    ScriptedDownloader { list_fails: scenario.list_fails },
                )
                .await
                .expect("ShardWal::open");

                let seed_lease = scenario.seed_own_live_lease.then(|| {
                    Lease::new_initial(NODE_ID, unix_epoch_now_ms(), S3_LEASE_MS)
                });
                let lease_manager = S3LeaseManager::new(
                    FakeLeaseStore::new(probe.clone(), seed_lease, scenario.lease_ops_fail),
                    S3LeaseConfig {
                        node_id: NODE_ID,
                        advertised_client_address: "127.0.0.1:1".to_string(),
                        advertised_replication_address: "127.0.0.1:2".to_string(),
                        s3_lease_duration: Duration::from_millis(S3_LEASE_MS),
                        max_clock_drift: Duration::from_millis(DRIFT_MS),
                    },
                );

                let mut shard = Shard::new(
                    shard_config(&dir),
                    0,
                    sender,
                    receivers,
                    client_listener,
                    replication_listener,
                    shard_wal,
                    Some(lease_manager),
                    Arc::new(AtomicBool::new(false)),
                );

                let node_status = shard.shard_wal_rc().node_status.clone();
                let shutdown = shard.shutdown_flag();

                // Co-resident watcher: samples the status, counts its own
                // wakeups (a spinning loop starves it), and stops the node
                // once the contract's progress signal appears.
                glommio::spawn_local({
                    let probe = probe.clone();
                    async move {
                        let deadline = std::time::Instant::now() + budget;
                        loop {
                            glommio::timer::sleep(Duration::from_millis(10)).await;
                            probe.ticks.fetch_add(1, Ordering::SeqCst);
                            let status = node_status.get();
                            probe.last_raw.store(code(status.raw()), Ordering::SeqCst);
                            probe.last_effective.store(code(status.effective_node_status()), Ordering::SeqCst);
                            if status.raw().is_promoting() {
                                probe.saw_promoting.store(true, Ordering::SeqCst);
                            }
                            let progressed = code(status.raw()) != initial_code
                                || probe.lease_object_calls.load(Ordering::SeqCst) > 0
                                || probe.heartbeats.load(Ordering::SeqCst) > 0;
                            if (stop_on_progress && progressed) || std::time::Instant::now() >= deadline {
                                shutdown.set(true);
                                break;
                            }
                        }
                    }
                })
                .detach();

                shard.run().await;
            })
            .expect("spawn shard executor")
            .join()
            .is_ok()
    };

    let _ = std::fs::remove_dir_all(&dir);
    NodeRun { probe, executor_survived }
}

/// A TTL far enough out that it cannot decay during an observation window.
fn live_ttl() -> u64 {
    unix_epoch_now_ms() + 60_000
}

// Status builders. Functions, not values: a TTL must be anchored to the boot
// of the node under test, not to when the test was queued.
fn live_leader() -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(NodeStatus::Leader { lease_epoch: 3 }, DRIFT_MS, live_ttl())
}

fn live_promoting() -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(NodeStatus::Promoting { lease_epoch: 4 }, DRIFT_MS, live_ttl())
}

// ── C1 ──

/// A status the loop cannot act on must at least not take the executor with
/// it: co-resident tasks on the same (single-vCPU) executor must keep running.
/// Separated from the C1 assertion above because pacing a dead branch is a
/// containment measure, not the branch.
#[test]
fn stranded_status_does_not_starve_co_resident_tasks() {
    let budget = Duration::from_secs(3);
    let run = run_node(
        "starvation",
        live_promoting,
        Scenario::default(),
        budget,
        false,
    );

    // The watcher wakes every 10ms; anything above a third of the nominal
    // count means it was scheduled, not starved.
    let nominal = budget.as_millis() as u32 / 10;
    let ticks = run.probe.ticks.load(Ordering::SeqCst);
    assert!(
        ticks * 3 >= nominal,
        "a co-resident 10ms timer got only {ticks} of ~{nominal} expected wakeups while the node held \
         Promoting — the orchestrator loop is spinning without yielding"
    );
}

/// INVARIANT (C2 + C3, the preemptive call site — the one that SWALLOWS the
/// error and keeps looping): the same contract, at the call site where nothing
/// propagates. A swallowed failure is only safe if the status it leaves behind
/// is actionable and the loop keeps making progress on it.
///
/// The run: a leader holding a live lease loses its follower, which arms the
/// preemptive renewal; the lease store is then unreachable, so that election
/// returns `Err` and the caller only logs it.
#[test]
fn contract_swallowed_election_failure_leaves_an_actionable_status() {
    let run = run_node(
        "swallowed_election",
        live_leader,
        Scenario {
            heartbeat_acks: 1,
            lease_ops_fail: true,
            seed_own_live_lease: true,
            ..Scenario::default()
        },
        Duration::from_secs(4),
        false,
    );

    let raw = status_name(run.probe.last_raw.load(Ordering::SeqCst));
    let effective_code = run.probe.last_effective.load(Ordering::SeqCst);
    let effective = status_name(effective_code);
    let heartbeats = run.probe.heartbeats.load(Ordering::SeqCst);
    let lease_attempts = run.probe.lease_object_calls.load(Ordering::SeqCst);

    // The preemptive site is the only one armed here: it fires on the
    // ack -> failure transition (so at least two heartbeats), and the lease
    // stays live, so no other arm reaches S3 in this window.
    assert!(
        heartbeats >= 2 && lease_attempts > 0,
        "scaffolding: the preemptive call site was never reached ({heartbeats} heartbeats, \
         {lease_attempts} election attempts, raw {raw})"
    );

    let mut violations = Vec::new();
    if !run.executor_survived {
        violations.push(format!("the shard executor died; last status raw {raw} / effective {effective}"));
    }
    if !orchestrator_can_act_on(effective_code) {
        violations.push(format!(
            "the node is left holding raw {raw} / effective {effective}, which matches no orchestrator arm"
        ));
    }
    // A swallowed error must not stop the loop: it kept heartbeating past the
    // failure, so it is still acting on the status it holds.
    if heartbeats < 3 {
        violations.push(format!(
            "the loop stopped acting after the swallowed failure: only {heartbeats} heartbeats in the \
             observation window"
        ));
    }
    assert!(
        violations.is_empty(),
        "a swallowed election failure must leave an actionable status and a loop that keeps working \
         (path: Leader -> follower unreachable -> preemptive renewal -> lease store unavailable -> Err, logged):\n  {}",
        violations.join("\n  ")
    );
}
