//! CONTRACT: a leadership challenge that cannot finish is an ordinary,
//! recoverable event — never a process death.
//!
//! `docs/leadership-replication-design.md`, state machine:
//!
//! ```text
//! Promoting ──► Follower | Fenced             lost the race / overran
//! ```
//!
//! and, on the catchup exits: *"A promotion whose catchup spans multiple
//! attempts renews its lease between them ... If the other node took the lease
//! in the meantime, the promotion yields: the flip gate resolves it as
//! `LostRace`, never a zombie leader."*
//!
//! So a node that won the S3 CAS, entered `Promoting`, could not complete the
//! S3 WAL catch-up, and/or lost to a higher-epoch leader must stay alive, fall
//! back to a follower/fenced state, and remain able to challenge again later.
//! `docs/invariants.md` records the same judgement for the ENOSPC path: *"The
//! shard stays alive ... Prior behaviour was a `panic!` which crash-looped the
//! shard."* Killing the node instead converts a recoverable election loss into
//! cluster-wide write unavailability plus a restart.
//!
//! # The field incident this reproduces
//!
//! RPi cluster, twice, ~31s of client-visible write unavailability against a
//! suite that asserts 1600ms failover; the node then hung until systemd
//! SIGKILLed it (`session-v2/goal.md`, DEFECT 2):
//!
//! ```text
//! 10:24:01  Heartbeat TTL expired — auto-fenced
//! 10:24:02  Follower or fenced node detected expired lease, challenging for leadership
//! 10:24:02  Lease epoch 1 -> 2, shards -> Promoting
//! 10:24:35  Promotion lost the race to a higher-epoch leader, stepping down (remote epoch 3)
//! 10:25:32  S3 catchup completion barrier timed out; bailing (role=Promoting)
//!           Election failed after retries: unavailable: Could not catch up WAL via S3   <- died here
//! ```
//!
//! How this test maps onto it, driving only the process-boundary fakes:
//!
//! * the node boots as a follower whose heartbeat TTL has already lapsed
//!   (effective `Fenced`), so shard 0's orchestrator takes the same
//!   "detected expired lease, challenging for leadership" arm;
//! * `FakeLeaseStore` holds node 22's **expired epoch-1** lease, so the
//!   challenge wins the CAS and moves the epoch 1 -> 2, exactly as in the log;
//! * `ScriptedDownloader` fails every `list_objects`: S3 answers, but the
//!   catch-up can never drain — the same dead end a stalled/limping MinIO
//!   produces. The catchup orchestrator exhausts its unreachable-round budget
//!   and bails, producing the identical `CatchupRunOutcome` the field's
//!   completion-barrier timeout produced, and therefore the identical
//!   `Could not catch up WAL via S3` election error;
//! * while the node is `Promoting`, a co-resident task adopts it into node 22's
//!   higher-epoch (3) lease — precisely what `connection_handler.rs` does on a
//!   higher-epoch peer heartbeat ("Promotion lost the race to a higher-epoch
//!   leader, stepping down to follower"), and the state the design says the
//!   flip gate must resolve as `LostRace`.
//!
//! Everything scripted here is something the real system does: S3 that lists
//! but cannot serve a usable view, and a peer that takes the lease at a higher
//! epoch mid-promotion.
//!
//! Harness shape — a real `Shard` driven through `Shard::run`, substituted only
//! at the process boundary (S3 lease store, S3 downloader, peer link) — is
//! copied from `adversarial_promotion_window_tests.rs`.
//!
//! Acceptance set: this test plus
//! `review_evidence_tests::d2fix::stepdown_must_not_block_the_retry_until_the_won_lease_expires`,
//! which drives the same harness without the rival takeover and so covers the
//! step-down branch this one never reaches (here the status is `Follower` at the
//! error). Both must be green for the Defect-2 fix.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};
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
use glommio::{BuilderErrorKind, GlommioError, LocalExecutorBuilder, Placement};

use crate::sharded::routing_rule::RoutingRule;
use crate::sharded::shard::Shard;
use crate::sharded::shard_config::ShardConfig;

/// One node at a time: the run below binds listeners and owns a whole executor.
static NODE_LOCK: Mutex<()> = Mutex::new(());

const NODE_ID: u128 = 21;
/// The peer that held the (now expired) lease and then comes back at epoch 3.
const PEER_NODE_ID: u128 = 22;
const S3_LEASE_MS: u64 = 30_000;
const DRIFT_MS: u64 = 500;
/// Epoch of the expired lease found in S3 — the field's "Lease epoch 1 -> 2".
const STALE_EPOCH: u64 = 1;
/// Epoch of the leader that adopts this node mid-promotion (field: remote epoch 3).
const RIVAL_EPOCH: u64 = 3;

/// The challenge fails ~15s in: 4 catchup attempts paced by 3 x 5s
/// inter-attempt sleeps (`run_s3_catchup`), then `MAX_S3_UNREACHABLE_ROUNDS`
/// bails. The budget only has to outlast that; a surviving node is observed at
/// the deadline and asked to shut down, a dying one ends the run early.
const OBSERVATION_BUDGET: Duration = Duration::from_secs(25);

/// Status codes for the last observed node status. A code, not a `String`, so
/// the 2ms watcher poll allocates nothing.
mod code {
    pub const NONE: u8 = 0;
    pub const LEADER: u8 = 1;
    pub const FOLLOWER: u8 = 2;
    pub const FOLLOWER_CATCHING_UP: u8 = 3;
    pub const PROMOTING: u8 = 4;
    pub const BOOT_CATCHUP: u8 = 5;
    pub const FENCED: u8 = 6;
    pub const STANDALONE: u8 = 7;
}

fn status_code(status: NodeStatus) -> u8 {
    match status {
        NodeStatus::Leader { .. } => code::LEADER,
        NodeStatus::Follower { .. } => code::FOLLOWER,
        NodeStatus::FollowerCatchingUp { .. } => code::FOLLOWER_CATCHING_UP,
        NodeStatus::Promoting { .. } => code::PROMOTING,
        NodeStatus::BootCatchup => code::BOOT_CATCHUP,
        NodeStatus::Fenced => code::FENCED,
        NodeStatus::Standalone => code::STANDALONE,
    }
}

fn status_name(code: u8) -> &'static str {
    match code {
        code::LEADER => "Leader",
        code::FOLLOWER => "Follower",
        code::FOLLOWER_CATCHING_UP => "FollowerCatchingUp",
        code::PROMOTING => "Promoting",
        code::BOOT_CATCHUP => "BootCatchup",
        code::FENCED => "Fenced",
        code::STANDALONE => "Standalone",
        _ => "<never observed>",
    }
}

#[derive(Default)]
struct Probe {
    /// S3 CAS/read traffic — evidence the challenge actually ran.
    lease_object_calls: AtomicU32,
    /// Catch-up attempts that reached S3 and were refused a usable view.
    s3_list_calls: AtomicU32,
    saw_promoting: AtomicBool,
    /// The higher-epoch leader has taken over (lease + our adopted status).
    rival_took_over: AtomicBool,
    /// Raw status last seen by the watcher, and the one seen at the deadline.
    last_status: AtomicU8,
    status_at_deadline: AtomicU8,
}

/// `cluster/lease.json`. Once the rival takes over it serves the peer's live
/// epoch-3 lease, which is what the real object holds after the peer wins the
/// CAS on the other side of a partition.
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

    fn rival_lease(&self) -> LeaseWithEtag {
        let now = unix_epoch_now_ms();
        LeaseWithEtag {
            lease: Lease {
                leader_node_id: PEER_NODE_ID,
                lease_epoch: RIVAL_EPOCH,
                acquired_at_ms: now,
                expires_at_ms: now + S3_LEASE_MS,
            },
            etag: "rival".to_string(),
        }
    }
}

impl LeaseStore for FakeLeaseStore {
    async fn get_lease(&self) -> Result<Option<LeaseWithEtag>, LeaseStoreError> {
        self.probe.lease_object_calls.fetch_add(1, Ordering::SeqCst);
        if self.probe.rival_took_over.load(Ordering::SeqCst) {
            return Ok(Some(self.rival_lease()));
        }
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
        if self.probe.rival_took_over.load(Ordering::SeqCst) {
            // The peer holds the object at a higher epoch: our etag is stale.
            return Err(LeaseStoreError::PreconditionFailed);
        }
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

/// S3 that answers but cannot serve the catch-up: every listing errors. A
/// retriable `S3ListFailed` is what a limping or throttling MinIO returns, and
/// it is the shortest honest route to a catch-up that cannot complete.
struct StalledDownloader {
    probe: Arc<Probe>,
}

impl S3Downloader for StalledDownloader {
    async fn list_objects(&self, prefix: &str) -> Result<Vec<S3ObjectRef>, S3CatchupError> {
        self.probe.s3_list_calls.fetch_add(1, Ordering::SeqCst);
        glommio::timer::sleep(Duration::from_millis(1)).await;
        Err(S3CatchupError::S3ListFailed {
            prefix: prefix.to_string(),
            message: "injected S3 stall: catch-up view unavailable".to_string(),
        })
    }

    async fn download(&self, path: &str) -> Result<Bytes, S3CatchupError> {
        Err(S3CatchupError::S3GetFailed { path: path.to_string(), message: "injected S3 stall".to_string() })
    }

    async fn delete(&self, _path: &str) -> Result<(), S3CatchupError> {
        Ok(())
    }
}

/// The peer is gone (that is why the TTL lapsed), so every heartbeat fails.
struct DeadPeerReplicationClient {
    reachable: std::cell::Cell<bool>,
    heartbeat_in_flight: std::cell::Cell<Option<u64>>,
}

impl ReplicationClient for DeadPeerReplicationClient {
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

    async fn send_heartbeat(&self, _unix_epoch_now_ms: u64, _lease_epoch: u64) -> Result<HeartbeatResult, SendHeartbeatError> {
        glommio::timer::sleep(Duration::from_millis(5)).await;
        Err(SendHeartbeatError::UnexpectedResponse)
    }

    async fn send_kick(&self) -> Result<bool, SendHeartbeatError> { Ok(true) }
}

fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "celeriant_promotion_failure_{tag}_{}_{}",
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

/// Boot state: a follower of the dead epoch-1 leader whose heartbeat TTL has
/// already lapsed. Effective status is `Fenced`, which is the state the field
/// log shows one line before the challenge ("Heartbeat TTL expired —
/// auto-fenced").
fn fenced_follower() -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(
        NodeStatus::Follower { leader_lease_epoch: STALE_EPOCH },
        DRIFT_MS,
        unix_epoch_now_ms().saturating_sub(1_000),
    )
}

/// What `connection_handler.rs` installs when a higher-epoch peer heartbeats a
/// promoting node: adoption as that leader's follower, on a live TTL.
fn adopted_by_rival() -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(
        NodeStatus::Follower { leader_lease_epoch: RIVAL_EPOCH },
        DRIFT_MS,
        unix_epoch_now_ms() + S3_LEASE_MS,
    )
}

struct NodeRun {
    probe: Arc<Probe>,
    /// The executor thread joined without panicking.
    survived: bool,
    /// Panic payload, when the thread died.
    panic_message: Option<String>,
}

fn panic_message(err: &GlommioError<()>) -> Option<String> {
    match err {
        GlommioError::BuilderError(BuilderErrorKind::ThreadPanic(payload)) => payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&'static str>().map(|s| (*s).to_string())),
        _ => None,
    }
}

fn run_failed_challenge() -> NodeRun {
    let _guard = NODE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let probe = Arc::new(Probe::default());
    let dir = scratch_dir("failed_challenge");

    let joined = {
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
                    fenced_follower(),
                    DeadPeerReplicationClient {
                        reachable: std::cell::Cell::new(false),
                        heartbeat_in_flight: std::cell::Cell::new(None),
                    },
                    StalledDownloader { probe: probe.clone() },
                )
                .await
                .expect("ShardWal::open");

                // The peer's lease, expired: it died holding epoch 1.
                let now = unix_epoch_now_ms();
                let stale_lease = Lease {
                    leader_node_id: PEER_NODE_ID,
                    lease_epoch: STALE_EPOCH,
                    acquired_at_ms: now.saturating_sub(2 * S3_LEASE_MS),
                    expires_at_ms: now.saturating_sub(S3_LEASE_MS),
                };
                let lease_manager = S3LeaseManager::new(
                    FakeLeaseStore {
                        lease: std::cell::RefCell::new(Some(LeaseWithEtag { lease: stale_lease, etag: "seed".to_string() })),
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

                // Co-resident watcher: same executor, same cell, same setter the
                // connection handler uses when a higher-epoch peer heartbeats a
                // promoting node.
                glommio::spawn_local({
                    let probe = probe.clone();
                    async move {
                        let deadline = std::time::Instant::now() + OBSERVATION_BUDGET;
                        loop {
                            glommio::timer::sleep(Duration::from_millis(2)).await;
                            let status = node_status.get();
                            probe.last_status.store(status_code(status.raw()), Ordering::SeqCst);

                            // The rival takes the lease at epoch 3 and adopts us
                            // while the promotion is still open — the field's
                            // "Promotion lost the race to a higher-epoch leader".
                            // Both writes land in one poll with no await between
                            // them, so the promotion never observes a half-applied
                            // takeover.
                            if status.raw().is_promoting() && !probe.saw_promoting.swap(true, Ordering::SeqCst) {
                                probe.rival_took_over.store(true, Ordering::SeqCst);
                                set_node_status_and_metric(&node_status, adopted_by_rival(), 0);
                            }

                            if std::time::Instant::now() >= deadline {
                                probe.status_at_deadline.store(status_code(node_status.get().raw()), Ordering::SeqCst);
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
    };

    let _ = std::fs::remove_dir_all(&dir);
    match joined {
        Ok(()) => NodeRun { probe, survived: true, panic_message: None },
        Err(e) => {
            let message = panic_message(&e).unwrap_or_else(|| format!("{e:?}"));
            NodeRun { probe, survived: false, panic_message: Some(message) }
        }
    }
}

/// CONTRACT: a node whose leadership challenge fails — CAS won, `Promoting`
/// entered, S3 WAL catch-up unable to complete, race lost to a higher-epoch
/// leader — must not die. It steps back to a follower/fenced state and stays
/// available to challenge again.
///
/// Red today: `shard.rs`'s challenge arm turns the failed election into an
/// unconditional `panic!`, which takes the shard task and the process with it.
#[test]
fn contract_failed_leadership_challenge_must_not_kill_the_shard() {
    let run = run_failed_challenge();
    let probe = &run.probe;

    eprintln!(
        "promotion-failure survival: survived={} saw_promoting={} rival_took_over={} \
         lease_object_calls={} s3_list_calls={} last_status={} status_at_deadline={}",
        run.survived,
        probe.saw_promoting.load(Ordering::SeqCst),
        probe.rival_took_over.load(Ordering::SeqCst),
        probe.lease_object_calls.load(Ordering::SeqCst),
        probe.s3_list_calls.load(Ordering::SeqCst),
        status_name(probe.last_status.load(Ordering::SeqCst)),
        status_name(probe.status_at_deadline.load(Ordering::SeqCst)),
    );
    if let Some(message) = run.panic_message.as_deref() {
        eprintln!("promotion-failure survival: executor panic payload: {message}");
    }

    // Scaffolding first, so a harness failure never masquerades as the defect.
    assert!(
        probe.lease_object_calls.load(Ordering::SeqCst) > 0,
        "scaffolding: the node never touched the S3 lease object, so no challenge ran"
    );
    assert!(
        probe.saw_promoting.load(Ordering::SeqCst),
        "scaffolding: the challenge never reached Promoting ({} lease-object calls, {} S3 list calls, last status {})",
        probe.lease_object_calls.load(Ordering::SeqCst),
        probe.s3_list_calls.load(Ordering::SeqCst),
        status_name(probe.last_status.load(Ordering::SeqCst)),
    );
    assert!(
        probe.s3_list_calls.load(Ordering::SeqCst) > 0,
        "scaffolding: the promotion never attempted an S3 catch-up"
    );

    assert!(
        run.survived,
        "CONTRACT VIOLATED (docs/leadership-replication-design.md: 'Promoting -> Follower | Fenced ... \
         lost the race / overran'): the node won lease epoch {}, entered Promoting, was adopted by the \
         epoch-{} leader, could not complete its S3 WAL catch-up ({} list attempts refused) — and then \
         KILLED THE SHARD TASK instead of stepping down. A failed leadership challenge is recoverable; \
         the node must stay alive as Follower or Fenced and be able to challenge again. Executor panic: {}",
        STALE_EPOCH + 1,
        RIVAL_EPOCH,
        probe.s3_list_calls.load(Ordering::SeqCst),
        run.panic_message.as_deref().unwrap_or("<no payload>"),
    );

    // Alive is not enough: a node that could not verify its WAL must never be
    // serving as leader (the design's "never a zombie leader").
    let deadline_status = probe.status_at_deadline.load(Ordering::SeqCst);
    assert_ne!(
        deadline_status,
        code::LEADER,
        "the node opened writes as Leader even though its S3 catch-up never completed"
    );
    assert_ne!(
        deadline_status, code::NONE,
        "scaffolding: the run ended without ever sampling a final status"
    );
}
