//! Proof that the lease orchestrator's fall-through arm is a live backstop:
//! the preemptive-renewal caller CAN install `Promoting`.
//!
//! `became_leader` requires a non-leader entry status, and the preemptive
//! caller runs inside the leader arm — so it reads as unable to open the
//! promotion window. But the arm's predicate and `set_node_role_via_s3`'s
//! `previous_status` are not sampled at the same instant: a heartbeat interval,
//! a heartbeat with a 4x hard timeout, and two broadcasts sit between them,
//! and all of them await. Production has two writers of `node_status` that can
//! land in that gap, both on shard 0: the connection handler demoting a Leader
//! on a higher-epoch peer heartbeat, and `renew_s3_lease_on_demand` adopting a
//! superseded status. The injection below stands in for either; either leaves
//! raw non-Leader, the preemptive election reacquires our own lease, and
//! `became_leader` is true.
//!
//! The fall-through comment in `spawn_boot_orchestrator` states this mechanism;
//! this test is the evidence it rests on.
//!
//! Harness shape copied from `orchestrator_status_contract_tests.rs`: a real
//! `Shard` driven through `Shard::run`, with substitutions only at the process
//! boundary (S3, peer link).

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
use celeriant_distributed::validated_node_status::{set_node_status_and_metric, unix_epoch_now_ms, ValidatedNodeStatus};
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

static NODE_LOCK: Mutex<()> = Mutex::new(());

const NODE_ID: u128 = 11;
const S3_LEASE_MS: u64 = 30_000;
const DRIFT_MS: u64 = 500;
/// Epoch the node holds when it enters `set_node_role_via_s3` on the preemptive
/// path — what `previous_status` captures.
const ENTRY_EPOCH: u64 = 3;

#[derive(Default)]
struct Probe {
    heartbeats: AtomicU32,
    lease_object_calls: AtomicU32,
    saw_promoting: AtomicBool,
    demotion_injected: AtomicBool,
    heartbeats_at_first_promoting: AtomicU64,
}

#[derive(Clone, Copy)]
struct Scenario {
    list_fails: bool,
    heartbeat_acks: u32,
    seed_own_live_lease: bool,
}

struct FakeLeaseStore {
    lease: std::cell::RefCell<Option<LeaseWithEtag>>,
    membership: std::cell::RefCell<Option<MembershipWithEtag>>,
    etag_seq: std::cell::Cell<u64>,
    probe: Arc<Probe>,
}

impl FakeLeaseStore {
    fn next_etag(&self) -> String {
        let n = self.etag_seq.get() + 1;
        self.etag_seq.set(n);
        format!("etag-{n}")
    }
}

impl LeaseStore for FakeLeaseStore {
    async fn get_lease(&self) -> Result<Option<LeaseWithEtag>, LeaseStoreError> {
        self.probe.lease_object_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.lease.borrow().clone())
    }

    async fn put_lease_create_only(&self, lease: &Lease) -> Result<String, LeaseStoreError> {
        self.probe.lease_object_calls.fetch_add(1, Ordering::SeqCst);
        if self.lease.borrow().is_some() {
            return Err(LeaseStoreError::AlreadyExists);
        }
        let etag = self.next_etag();
        *self.lease.borrow_mut() = Some(LeaseWithEtag { lease: lease.clone(), etag: etag.clone() });
        Ok(etag)
    }

    async fn put_lease_conditional(&self, lease: &Lease, etag: &str) -> Result<String, LeaseStoreError> {
        self.probe.lease_object_calls.fetch_add(1, Ordering::SeqCst);
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
        // The await the injector needs: production's heartbeat is a network
        // round trip with a 4x hard timeout, so this window is generous there.
        glommio::timer::sleep(Duration::from_millis(20)).await;
        if n >= self.acks {
            return Err(SendHeartbeatError::UnexpectedResponse);
        }
        Ok(HeartbeatResult::Ack { follower_timestamp_ms: unix_epoch_now_ms, follower_can_accept_tcp_replication: true })
    }

    async fn send_kick(&self) -> Result<bool, SendHeartbeatError> { Ok(true) }
}

fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "celeriant_adversarial_window_{tag}_{}_{}",
        std::process::id(),
        unix_epoch_now_ms()
    ));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn internal_config(dir: &std::path::Path) -> InternalShardConfig {
    InternalShardConfig {
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

/// A live follower status. Live TTL, so the follower arm cannot act on it: any
/// subsequent status change is somebody overwriting it.
fn live_follower_at(epoch: u64) -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(
        NodeStatus::Follower { leader_lease_epoch: epoch },
        DRIFT_MS,
        unix_epoch_now_ms() + 120_000,
    )
}

struct NodeRun {
    probe: Arc<Probe>,
    executor_survived: bool,
}

fn run_node(
    tag: &str,
    make_initial: fn() -> ValidatedNodeStatus,
    scenario: Scenario,
    budget: Duration,
    stop_on_promoting: bool,
) -> NodeRun {
    let _guard = NODE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let initial = make_initial();
    let probe = Arc::new(Probe::default());
    let dir = scratch_dir(tag);

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

                let seed_lease = scenario
                    .seed_own_live_lease
                    .then(|| Lease::new_initial(NODE_ID, unix_epoch_now_ms(), S3_LEASE_MS));
                let lease_manager = S3LeaseManager::new(
                    FakeLeaseStore {
                        lease: std::cell::RefCell::new(
                            seed_lease.map(|lease| LeaseWithEtag { lease, etag: "seed".to_string() }),
                        ),
                        membership: std::cell::RefCell::new(None),
                        etag_seq: std::cell::Cell::new(0),
                        probe: probe.clone(),
                    },
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

                // Co-resident injector/watcher. Writes `node_status` exactly the
                // way a heartbeat connection task or the on-demand renewal
                // handler does in production: same executor, same cell, same
                // setter.
                //
                // The demotion fires as soon as the second heartbeat starts —
                // after the leader arm's predicate passed and before the
                // preemptive call. The injected follower is LIVE, not expired,
                // so the follower arm cannot challenge: any election in this
                // run must be the preemptive one.
                glommio::spawn_local({
                    let probe = probe.clone();
                    async move {
                        let deadline = std::time::Instant::now() + budget;
                        loop {
                            glommio::timer::sleep(Duration::from_millis(2)).await;
                            let status = node_status.get();
                            if status.raw().is_promoting() && !probe.saw_promoting.swap(true, Ordering::SeqCst) {
                                probe.heartbeats_at_first_promoting
                                    .store(probe.heartbeats.load(Ordering::SeqCst) as u64, Ordering::SeqCst);
                            }

                            if !probe.demotion_injected.load(Ordering::SeqCst)
                                && probe.heartbeats.load(Ordering::SeqCst) >= 2
                            {
                                set_node_status_and_metric(&node_status, live_follower_at(ENTRY_EPOCH), 0);
                                probe.demotion_injected.store(true, Ordering::SeqCst);
                                continue;
                            }

                            if (stop_on_promoting && probe.saw_promoting.load(Ordering::SeqCst))
                                || std::time::Instant::now() >= deadline
                            {
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

fn live_leader() -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(
        NodeStatus::Leader { lease_epoch: 3 },
        DRIFT_MS,
        unix_epoch_now_ms() + 60_000,
    )
}

/// The run: a Leader holding its own live S3 lease loses its follower (which
/// arms the preemptive site), and a co-resident task demotes it to a LIVE
/// Follower while the failing heartbeat is in flight. The lease is still ours,
/// so the preemptive election reacquires it and `became_leader` is true.
///
/// The injected follower's TTL is live, so the follower/fenced arm's
/// lease-expiry guard declines to challenge — the preemptive site is the only
/// election path open in this window.
#[test]
fn preemptive_caller_can_install_promoting() {
    let run = run_node(
        "preemptive_promoting",
        live_leader,
        Scenario {
            list_fails: true,
            heartbeat_acks: 1,
            seed_own_live_lease: true,
        },
        Duration::from_secs(20),
        true,
    );

    assert!(run.executor_survived, "the shard executor died before the observation window closed");
    assert!(
        run.probe.demotion_injected.load(Ordering::SeqCst),
        "scaffolding: the demotion never fired ({} heartbeats)",
        run.probe.heartbeats.load(Ordering::SeqCst)
    );
    eprintln!(
        "A1: heartbeats={} lease_object_calls={} heartbeats_at_first_promoting={} saw_promoting={}",
        run.probe.heartbeats.load(Ordering::SeqCst),
        run.probe.lease_object_calls.load(Ordering::SeqCst),
        run.probe.heartbeats_at_first_promoting.load(Ordering::SeqCst),
        run.probe.saw_promoting.load(Ordering::SeqCst),
    );
    assert!(
        run.probe.saw_promoting.load(Ordering::SeqCst),
        "scaffolding OR the claim holds: no Promoting was observed ({} heartbeats, {} lease-object calls)",
        run.probe.heartbeats.load(Ordering::SeqCst),
        run.probe.lease_object_calls.load(Ordering::SeqCst)
    );
}
