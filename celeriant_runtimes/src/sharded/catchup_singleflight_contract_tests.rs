//! One S3 catchup task per shard at a time.
//!
//! ## The defect this pins
//!
//! `run_s3_catchup` re-broadcasts `EnterS3Catchup` on every attempt, and
//! `handle_enter_s3_catchup` spawns a detached task for every message it receives.
//! A shard whose attempt outlives the 90s completion barrier is therefore
//! re-broadcast to while its first task is still draining.
//!
//! `catchup_from_s3` takes no lock: it snapshots the WAL tip on entry and carries
//! `next_wal_seq` / `processed_paths` as task locals across every await. Two
//! concurrent tasks on one shard each truncate to their own divergence point and
//! re-apply the batches the other already appended. Observed on the cluster
//! (run 1789859811, `follower_sigkill`): every shard-3 catchup log line is an exact
//! duplicate pair and the follower's shard-3 wal_seq finished 2071 entries ABOVE
//! the leader's durable tip.
//!
//! ## How the overlap is made deterministic
//!
//! Concurrency is observed inside the downloader, not inferred: `GatedDownloader`
//! counts `list_objects` calls in flight and remembers the maximum. The gate is
//! never opened, so the first task parks at the first S3 touch and the second
//! request is guaranteed to arrive with it unfinished, the exact interleaving the
//! barrier timeout produces.

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use celeriant_distributed::lease_store::{LeaseStore, LeaseStoreError, LeaseWithEtag, MembershipWithEtag};
use celeriant_distributed::node_status::NodeStatus;
use celeriant_distributed::validated_node_status::{unix_epoch_now_ms, ValidatedNodeStatus};
use celeriant_shard::error::s3_catchup_error::S3CatchupError;
use celeriant_shard::s3_downloader::{S3Downloader, S3ObjectRef};
use celeriant_shard::shard_wal::ShardWal;
use celeriant_shard::shard_wal_s3_catchup::CatchupRole;
use celeriant_wal::s3::lease::Lease;
use celeriant_wal::s3::membership::Membership;
use celeriant_wire::codec::compression::DictCodec;
use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::{LocalExecutorBuilder, Placement};

use super::lease_singleflight_contract_tests::{internal_config, scratch_dir, shard_config, IdleReplicationClient};
use crate::sharded::connection_handler::{handle_enter_s3_catchup, ConnectionContext, ConnectionGauges};

/// The follower shard that overran the barrier in run 1789859811.
const SHARD_ID: usize = 3;
const DRIFT_MS: u64 = 500;
/// Only reached on a failing run; a passing run leaves as soon as the flag flips.
const PARK_DEADLINE: Duration = Duration::from_secs(5);
/// Time granted to a second task to reach the downloader. Generous: the contract
/// says it must never start, so anything it needs this long for is already a bug.
const SETTLE: Duration = Duration::from_millis(300);

macro_rules! glommio_test {
    ($body:expr) => {
        LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async move { $body })
            .unwrap()
            .join()
            .unwrap()
    };
}

#[derive(Default)]
struct DownloaderProbe {
    list_calls: Cell<u32>,
    in_flight: Cell<u32>,
    max_in_flight: Cell<u32>,
    /// Observed by the test: a catchup task is now parked inside `list_objects`.
    gate_entered: Cell<bool>,
    /// Never set by these tests; the parked tasks are dropped with the executor.
    gate_open: Cell<bool>,
}

/// Parks every caller at the first S3 touch of `catchup_from_s3` and records how
/// many were parked at once.
struct GatedDownloader {
    probe: Rc<DownloaderProbe>,
}

impl S3Downloader for GatedDownloader {
    async fn list_objects(&self, _prefix: &str) -> Result<Vec<S3ObjectRef>, S3CatchupError> {
        let probe = &self.probe;
        probe.list_calls.set(probe.list_calls.get() + 1);
        let n = probe.in_flight.get() + 1;
        probe.in_flight.set(n);
        probe.max_in_flight.set(probe.max_in_flight.get().max(n));
        probe.gate_entered.set(true);
        while !probe.gate_open.get() {
            glommio::timer::sleep(Duration::from_millis(1)).await;
        }
        probe.in_flight.set(probe.in_flight.get() - 1);
        Ok(vec![])
    }

    async fn download(&self, path: &str) -> Result<Bytes, S3CatchupError> {
        Err(S3CatchupError::S3GetFailed { path: path.to_string(), message: "gated".to_string() })
    }

    async fn delete(&self, _path: &str) -> Result<(), S3CatchupError> {
        Ok(())
    }
}

/// No election runs here; present only to satisfy the context's type parameter.
struct NoLeaseStore;

impl LeaseStore for NoLeaseStore {
    async fn get_lease(&self) -> Result<Option<LeaseWithEtag>, LeaseStoreError> {
        Ok(None)
    }
    async fn put_lease_create_only(&self, _lease: &Lease) -> Result<String, LeaseStoreError> {
        Err(LeaseStoreError::AlreadyExists)
    }
    async fn put_lease_conditional(&self, _lease: &Lease, _etag: &str) -> Result<String, LeaseStoreError> {
        Err(LeaseStoreError::PreconditionFailed)
    }
    async fn get_membership(&self) -> Result<Option<MembershipWithEtag>, LeaseStoreError> {
        Ok(None)
    }
    async fn put_membership(&self, _membership: &Membership, _etag: Option<&str>) -> Result<(), LeaseStoreError> {
        Ok(())
    }
}

type TestContext = ConnectionContext<IdleReplicationClient, GatedDownloader, NoLeaseStore>;

/// A data shard mid-catchup behind a leader on epoch 1: the state a kicked
/// follower is in when the orchestrator broadcasts `EnterS3Catchup`.
async fn catching_up_shard(dir: &std::path::Path) -> (TestContext, Rc<DownloaderProbe>) {
    let shard_dir = dir.join("shard");
    std::fs::create_dir_all(&shard_dir).expect("shard dir");

    // Single-peer mesh: no completion is ever sent (the gate stays shut), so the
    // shard-3 identity below is for logs and metrics only.
    let mesh = MeshBuilder::<crate::sharded::intrashard_messages::IntrashardMessages, Full>::full(1, 16);
    let (sender, _receivers) = mesh.join().await.expect("join mesh");

    let probe = Rc::new(DownloaderProbe::default());
    let shard_wal = ShardWal::open(
        internal_config(&shard_dir),
        ValidatedNodeStatus::create_custom_status(
            NodeStatus::Follower { leader_lease_epoch: 1 },
            DRIFT_MS,
            unix_epoch_now_ms() + 30_000,
        ),
        IdleReplicationClient { reachable: Cell::new(true), heartbeat_in_flight: Cell::new(None) },
        GatedDownloader { probe: probe.clone() },
    )
    .await
    .expect("ShardWal::open");

    let ctx = ConnectionContext {
        config: Rc::new(shard_config(dir)),
        current_shard_id: SHARD_ID,
        intrashard_sender: Rc::new(sender),
        shutdown_requested: Rc::new(Cell::new(false)),
        shard_wal: Rc::new(shard_wal),
        catchup_completion_tx: None,
        schema_registration_pending: None,
        lease_manager: None,
        dict_codec: Rc::new(DictCodec::new(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES, 3).expect("builtin dict")),
        extension_redirect_sink: None,
        connection_gauges: Rc::new(ConnectionGauges::new(SHARD_ID)),
    };
    (ctx, probe)
}

/// Polls until `cond` or the deadline; returns whether it was met. Present so a
/// violated contract terminates the test instead of hanging.
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

/// INVARIANT: a re-attempt must not start a second catchup task for a shard whose
/// first task is still running.
///
/// The first `EnterS3Catchup` parks inside the downloader, standing in for the
/// 91s WAL-cache warmup that overran the barrier on the cluster. The orchestrator then
/// re-broadcasts (generation 2) because the barrier reported shard 3 unreported.
///
/// PASS: the second request starts no task, so the downloader sees exactly one
/// `list_objects`, one in flight. FAIL (today): both tasks run `catchup_from_s3`
/// concurrently against one `ShardWal`, each with its own `next_wal_seq`, and the
/// duplicated truncate/re-apply inflates the follower's WAL past the leader's tip.
#[test]
fn contract_re_attempt_starts_no_second_catchup_task_while_the_first_runs() {
    let dir = scratch_dir("catchup_singleflight");
    let d = dir.clone();
    let (max_in_flight, list_calls, entered) = glommio_test!({
        let (ctx, probe) = catching_up_shard(&d).await;

        handle_enter_s3_catchup(ctx.clone(), CatchupRole::Following, 1);
        let entered = wait_until(|| probe.gate_entered.get()).await;

        // The completion barrier timed out on this shard; the orchestrator
        // re-broadcasts while the first task is still draining.
        handle_enter_s3_catchup(ctx.clone(), CatchupRole::Following, 2);
        glommio::timer::sleep(SETTLE).await;

        (probe.max_in_flight.get(), probe.list_calls.get(), entered)
    });
    let _ = std::fs::remove_dir_all(&dir);

    assert!(entered, "scaffolding: the first catchup never reached S3, so nothing was in flight");
    assert_eq!(
        max_in_flight, 1,
        "two catchup tasks ran concurrently on one shard: both truncate and re-apply against the same WAL"
    );
    assert_eq!(list_calls, 1, "the re-attempt started a second catchup task for a shard already catching up");
}

/// INVARIANT: a refused re-attempt still closes the TCP replication gate.
///
/// `enter_s3_catchup` is what re-asserts `FollowerCatchingUp`, and the skip path
/// returns before it. A shard whose run StallBailed back to Follower and is then
/// re-kicked would otherwise skip and stay Follower: replication open, with the
/// first task still draining and free to truncate under the incoming batches.
///
/// PASS: the status is catching-up after the refused re-attempt. FAIL: the
/// single-flight guard traded one corruption path for another.
#[test]
fn contract_a_refused_re_attempt_still_closes_the_replication_gate() {
    let dir = scratch_dir("catchup_skip_gate");
    let d = dir.clone();
    let (status, entered) = glommio_test!({
        let (ctx, probe) = catching_up_shard(&d).await;

        handle_enter_s3_catchup(ctx.clone(), CatchupRole::Following, 1);
        let entered = wait_until(|| probe.gate_entered.get()).await;

        // The run bailed to the TCP path and resumed Follower, then a kick
        // re-broadcasts while the first task is still draining.
        ctx.shard_wal.node_status.set(ValidatedNodeStatus::create_custom_status(
            NodeStatus::Follower { leader_lease_epoch: 1 },
            DRIFT_MS,
            unix_epoch_now_ms() + 30_000,
        ));
        handle_enter_s3_catchup(ctx.clone(), CatchupRole::Following, 2);
        glommio::timer::sleep(SETTLE).await;

        (ctx.shard_wal.node_status.get().raw(), entered)
    });
    let _ = std::fs::remove_dir_all(&dir);

    assert!(entered, "scaffolding: the first catchup never reached S3, so nothing was in flight");
    assert!(
        status.is_catching_up(),
        "a refused re-attempt left the shard at {status:?} with TCP replication open under a draining catchup"
    );
}
