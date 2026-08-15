//! Blind contract tests for on-demand S3 lease renewal delivery.
//!
//! The promised behaviour (doc comments on `run_s3_fallback` in
//! `celeriant_shard/src/shard_wal_replicate.rs` and on `renew_s3_lease_on_demand`
//! below in `shard.rs`): a data shard that must S3-fallback but whose lease CAS
//! confirmation has gone stale "pokes shard 0 to renew lease.json now, then
//! spin-waits for the green light".
//!
//! C1 — a `LeaseRenewalRequester::request_renewal()` from ANY data shard must be
//!      deliverable to, and handled by, shard 0's intrashard message loop.
//!      Best-effort dropping under transient queue pressure is allowed;
//!      structurally-impossible delivery is not.
//! C2 — C1 must hold for every requesting shard id, INCLUDING when the requester
//!      is shard 0 itself, and including `num_shards == 1` (the 1-vCPU default,
//!      `celeriant/src/lib.rs:101`), where shard 0 is the only data shard.
//! C3 — observable via `celeriant_s3_lease_renewal_requested_total{shard_id,result}`:
//!      a shard that repeatedly requests renewal on a completely idle executor
//!      must not report `result="dropped"` on every single call.
//!
//! ## Two of these are KNOWINGLY RED (decision 2026-08-14)
//!
//! C3 passes: shard 0 now gets a depth-1 local channel drained on its own executor
//! (`IntrashardLeaseRenewalRequester::new` + `spawn_self_renewal_handler`), and the
//! delivery it asserts genuinely happens.
//!
//! `contract_renewal_message_addressed_to_shard_zero_reaches_shard_zero` and
//! `contract_single_shard_node_can_renew_its_own_lease_on_demand` remain RED **by
//! decision, not by neglect**. They assert that `try_send_to(0, ..)` issued from shard 0
//! must succeed. That is a property of glommio's `Full` mesh, not of this codebase: the
//! self slot is a deliberate `None` placeholder, so the send has no channel and
//! `Receivers::streams()` yields no self stream to receive on. Their own C1 is
//! mechanism-agnostic ("deliverable to, and handled by"), so the assertion is narrower
//! than the contract it serves. The fix routes around the mesh — consistent with every
//! other `try_send_to` site in this crate, all of which guard on
//! `target != current_shard_id`. They were left unedited rather than weakened to match
//! the implementation.
//!
//! Real objects only: the mesh here is built with the SAME call production uses
//! (`MeshBuilder::<IntrashardMessages, Full>::full`, `celeriant_runtimes/src/lib.rs:112`),
//! joined on a real executor pool, and the receive loops are registered from
//! `Receivers::streams()` exactly as `Shard::run` does (`shard.rs:164`). The
//! requester under test in `contract_shard_zero_*` is the production
//! `IntrashardLeaseRenewalRequester`, not a stand-in.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use celeriant_shard::shard_wal::LeaseRenewalRequester;
use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::{LocalExecutorPoolBuilder, PoolPlacement};
use metrics_exporter_prometheus::PrometheusHandle;

use crate::sharded::intrashard_messages::IntrashardMessages;
use crate::sharded::shard::IntrashardLeaseRenewalRequester;

/// Production default is 1024 (`--mesh-channel-size`); anything non-trivial is
/// fine here because these tests issue a handful of messages on an idle
/// executor — a full queue would be a scaffolding bug, not the contract.
const MESH_CHANNEL_SIZE: usize = 128;

/// Bound on how long a shard waits for the messages it expects. Only reached on
/// a failing run; a passing run leaves as soon as the count is met.
const DELIVERY_DEADLINE: Duration = Duration::from_secs(5);

/// One observed arrival on a receive loop: which shard's loop saw it, which mesh
/// stream it came off, and which shard the request claims to come from.
type Arrival = (usize, usize, usize);

#[derive(Default)]
struct Probe {
    arrivals: Mutex<Vec<Arrival>>,
    finished: AtomicBool,
}

impl Probe {
    fn arrivals_at_shard_zero(&self) -> Vec<Arrival> {
        self.arrivals.lock().unwrap().iter().copied().filter(|(dst, _, _)| *dst == 0).collect()
    }
}

/// Polls until `cond` or the deadline. Present so a violated contract terminates
/// the test instead of hanging; a satisfied contract never spends the budget.
async fn wait_until(deadline: Instant, mut cond: impl FnMut() -> bool) {
    while !cond() && Instant::now() < deadline {
        glommio::timer::sleep(Duration::from_millis(1)).await;
    }
}

/// Runs `num_shards` real executors over a real full mesh. Every shard registers
/// the same per-stream receive loops `Shard::run` registers, then `body` runs on
/// each shard with its own id and sender.
fn run_mesh<F, Fut>(num_shards: usize, probe: Arc<Probe>, body: F)
where
    F: FnOnce(usize, Arc<Probe>, std::rc::Rc<glommio::channels::channel_mesh::Senders<IntrashardMessages>>) -> Fut
        + Clone
        + Send
        + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let mesh = MeshBuilder::<IntrashardMessages, Full>::full(num_shards, MESH_CHANNEL_SIZE);
    let results = LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(num_shards))
        .on_all_shards({
            let mesh = mesh.clone();
            let probe = probe.clone();
            move || async move {
                let (sender, mut receivers) = mesh.join().await.expect("failed to join intrashard mesh");
                let shard_id = sender.peer_id();

                // Exactly what Shard::run does: one handler task per stream
                // yielded by Receivers::streams().
                for (src_shard, stream) in receivers.streams() {
                    let probe = probe.clone();
                    glommio::spawn_local(async move {
                        while let Some(msg) = stream.recv().await {
                            if let IntrashardMessages::RenewS3LeaseNow { requesting_shard } = msg {
                                probe.arrivals.lock().unwrap().push((shard_id, src_shard, requesting_shard));
                            }
                        }
                    })
                    .detach();
                }

                body(shard_id, probe.clone(), std::rc::Rc::new(sender)).await;

                // Hold every executor open until the probe is done, so a shard
                // exiting early cannot close a channel the contract needs.
                probe.finished.store(true, Ordering::SeqCst);
                let deadline = Instant::now() + DELIVERY_DEADLINE;
                wait_until(deadline, || probe.finished.load(Ordering::SeqCst)).await;
            }
        })
        .expect("failed to spawn executor pool")
        .join_all();

    for (shard_id, result) in results.iter().enumerate() {
        assert!(result.is_ok(), "shard {shard_id} executor thread panicked: {result:?}");
    }
}

// ── C3: the production requester, observed through the counters ──

static RECORDER: OnceLock<PrometheusHandle> = OnceLock::new();

fn recorder() -> &'static PrometheusHandle {
    RECORDER.get_or_init(|| {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .install_recorder()
            .expect("failed to install prometheus recorder for lease-renewal contract tests")
    })
}

/// Sums every rendered sample of `metric` whose line carries all of `labels`.
fn counter_total(render: &str, metric: &str, labels: &[&str]) -> u64 {
    render
        .lines()
        .filter(|l| l.starts_with(metric) && labels.iter().all(|lab| l.contains(lab)))
        .filter_map(|l| l.rsplit_once(' ').and_then(|(_, v)| v.trim().parse::<f64>().ok()))
        .map(|v| v as u64)
        .sum()
}

/// INVARIANT (C3): the PRODUCTION `IntrashardLeaseRenewalRequester`, constructed
/// for shard 0 exactly as `Shard::new` constructs it, must not report
/// `result="dropped"` on every single call when issued repeatedly on a
/// completely idle executor. "Best-effort" covers transient queue pressure; a
/// 100% drop rate with an empty queue means the request has nowhere to go.
///
/// Rows: `num_shards == 1` (1-vCPU default) and `num_shards == 2` (shard 0 is
/// still a data shard by default — `reserve_coordinator_shard` is false).
#[test]
fn contract_shard_zero_requester_does_not_drop_every_request_when_idle() {
    let handle = recorder();
    const CALLS: usize = 8;
    let mut violations = Vec::new();

    for num_shards in [1usize, 2] {
        let before_sent = counter_total(
            &handle.render(),
            "celeriant_s3_lease_renewal_requested_total",
            &[r#"shard_id="0""#, r#"result="sent""#],
        );
        let before_dropped = counter_total(
            &handle.render(),
            "celeriant_s3_lease_renewal_requested_total",
            &[r#"shard_id="0""#, r#"result="dropped""#],
        );

        let probe = Arc::new(Probe::default());
        run_mesh(num_shards, probe.clone(), |shard_id, probe, sender| async move {
            if shard_id == 0 {
                let (requester, self_rx) = IntrashardLeaseRenewalRequester::new(sender, shard_id);
                // Shard 0 cannot reach itself over the mesh, so `Shard::run` drains its
                // self-delivery channel on this executor instead. Mirror that here, or the
                // probe watches only the mesh and cannot see a renewal that arrives at all.
                if let Some(rx) = self_rx {
                    let probe = probe.clone();
                    glommio::spawn_local(async move {
                        while let Some(requesting_shard) = rx.recv().await {
                            probe.arrivals.lock().unwrap().push((0, 0, requesting_shard));
                        }
                    })
                    .detach();
                }
                for _ in 0..CALLS {
                    requester.request_renewal();
                }
                let deadline = Instant::now() + DELIVERY_DEADLINE;
                wait_until(deadline, || !probe.arrivals_at_shard_zero().is_empty()).await;
            }
        });

        let render = handle.render();
        let sent = counter_total(&render, "celeriant_s3_lease_renewal_requested_total", &[r#"shard_id="0""#, r#"result="sent""#])
            - before_sent;
        let dropped = counter_total(&render, "celeriant_s3_lease_renewal_requested_total", &[r#"shard_id="0""#, r#"result="dropped""#])
            - before_dropped;

        assert_eq!(
            sent + dropped,
            CALLS as u64,
            "[num_shards={num_shards}] scaffolding: every request_renewal() must record exactly one \
             requested_total sample"
        );
        if sent == 0 {
            violations.push(format!(
                "[num_shards={num_shards}] every one of shard 0's renewal requests was reported \
                 dropped on an idle executor: {sent} sent, {dropped} dropped out of {CALLS}"
            ));
        }
        if probe.arrivals_at_shard_zero().is_empty() {
            violations.push(format!(
                "[num_shards={num_shards}] {CALLS} request_renewal() calls from shard 0 produced no \
                 message handled by shard 0"
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "the production shard-0 requester must produce handled renewal requests, not solely \
         result=\"dropped\":\n  {}",
        violations.join("\n  ")
    );
}
