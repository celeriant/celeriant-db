//! A node whose leadership challenge fails must not terminate.
//!
//! `docs/leadership-replication-design.md` lists `Promoting -> Follower | Fenced`
//! ("lost the race / overran") as an ordinary, recoverable transition. A node
//! that wins the S3 CAS, enters `Promoting`, then cannot complete its S3 WAL
//! catch-up is in exactly that state: it must step back and retry, not die. A
//! process that exits takes the whole node's shards with it and turns a
//! recoverable election stumble into an outage that outlives the trigger.
//!
//! The field incident this test is written against: the survivor of a leader
//! kill won the lease, sat ~30s in `Promoting` on an S3 catch-up, logged
//! "S3 catchup completion barrier timed out" followed by
//! "Election failed after retries: unavailable: Could not catch up WAL via S3",
//! and the process PANICKED. `failover_pressure_matrix` walked 20 cells of
//! cluster state at kill time and never reproduced it, because its MinIO sat on
//! loopback and was always healthy — the catch-up could not fail. The missing
//! ingredient is S3 being unavailable DURING the promotion window, which is
//! what this test injects.
//!
//! Scenario:
//!   1. Two nodes + MinIO, replication link routed through a `TcpProxy`.
//!   2. Verify the cluster is healthy and replicating (harness gate).
//!   3. Cut the replication link and push load, so the leader falls back to S3
//!      and the follower falls behind (harness gates: events acked while cut,
//!      S3 fallback objects present, follower missing the newest writes).
//!   4. SIGKILL the leader.
//!   5. Poll `cluster/lease.json` for the epoch bump — the instant the
//!      challenger WINS the CAS — and stall S3 (`docker pause` on the MinIO
//!      container) the moment it appears. The catch-up that follows the CAS now
//!      has nowhere to read from.
//!   6. Hold the stall well past the field's ~90s challenge-to-panic gap while
//!      watching the challenger's process, then unpause.
//!
//! The assertion is the contract: the challenger does not abort on the election
//! path. Two observables, both required — its process is still alive, AND its
//! captured log carries no "Election failed after retries" panic. The log check
//! is not belt-and-braces: under this harness the panic lands on a shard
//! executor and an in-process supervisor restarts the shards under it, so the
//! process survives a defect that killed the node on the rig. Liveness alone
//! reads GREEN through a live reproduction. The log ring is bounded, so the scan
//! runs continuously rather than once at the end.
//!
//! Recovery after the unpause is reported but deliberately not asserted, so the
//! test does not over-constrain how the defect gets fixed.
//!
//! Every setup step that fails reports as HARNESS, not as the defect: a rig
//! that never got the follower behind, never killed the leader, or never saw
//! the epoch bump has not tested anything.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wire::disk::versioned_block::deserialise_lease;

use crate::{
    fill_incompressible, is_leader, MinioContainer, RoutingRule, ServerConfig, TcpProxy, TestServer,
};

const NUM_SHARDS: usize = 4;

/// Election + initial S3 lease expiry (TTL 10s) + heartbeat establishment.
const SETTLE: Duration = Duration::from_secs(12);

/// How long the leader takes writes with the replication link severed. Long
/// enough to accumulate a real S3 fallback backlog for the promoting node to
/// have to read back.
const PARTITION_LOAD: Duration = Duration::from_secs(25);
const WRITE_WORKERS: usize = 8;
const PAYLOAD_BYTES: usize = 4 * 1024;
/// Below this the leader never really fell back to S3 and the setup is void.
const MIN_PARTITIONED_EVENTS: usize = 100;

/// After the kill, how long to watch the lease object for the challenger's CAS
/// win. Failover here is heartbeat lease (1.5s) + drift (0.5s) + one CAS.
const EPOCH_WATCH: Duration = Duration::from_secs(40);
const EPOCH_POLL: Duration = Duration::from_millis(50);

/// How long S3 stays stalled. The field showed ~90s from challenge to panic;
/// this is generous on top of that so a slow retry ladder still lands inside.
const STALL: Duration = Duration::from_secs(150);
const LIVENESS_POLL: Duration = Duration::from_millis(100);
const WRITE_PROBE_INTERVAL: Duration = Duration::from_secs(2);
const WRITE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// A node that starts serving writes this soon after the stall began had
/// already finished its catch-up when MinIO was paused: the stall landed too
/// late to touch the promotion path, and the run proves nothing.
const MISSED_WINDOW_GRACE: Duration = Duration::from_secs(8);

/// After the unpause, how long to watch for recovery. Reported, not asserted.
const RECOVERY_BUDGET: Duration = Duration::from_secs(90);

/// The panic the field incident ended on.
const PANIC_MARKER: &str = "election failed after retries";
/// The in-process supervisor reacting to that panic. On the rig the process
/// died outright; here the shard executors are restarted under it, so process
/// liveness alone would miss the defect entirely.
const SHARD_RESTART_MARKER: &str = "Shard executor panic detected";

/// A latched scan of a node's captured log. The log buffer is a bounded ring,
/// so a marker that appears mid-run is gone by the end: it has to be caught
/// while it is still in the window.
#[derive(Default)]
struct LogWatch {
    panic_line: Option<String>,
    restart_line: Option<String>,
    context: Vec<String>,
}

impl LogWatch {
    fn poll(&mut self, server: &TestServer) {
        if self.panic_line.is_some() && self.restart_line.is_some() {
            return;
        }
        let tail = server.log_tail(400);
        if self.panic_line.is_none()
            && let Some(line) = tail.iter().find(|l| l.to_lowercase().contains(PANIC_MARKER))
        {
            self.panic_line = Some(line.clone());
            self.context = tail.clone();
        }
        if self.restart_line.is_none()
            && let Some(line) = tail.iter().find(|l| l.contains(SHARD_RESTART_MARKER))
        {
            self.restart_line = Some(line.clone());
        }
    }
}

fn agg_key(i: usize) -> AggregateKey {
    AggregateKey::new(1, 1, i as u128 + 1)
}

fn event(payload_bytes: usize, seq: u64) -> DatablockAggregateEvent {
    let mut payload = vec![0u8; payload_bytes];
    fill_incompressible(&mut payload, seq);
    DatablockAggregateEvent {
        client_seq: seq,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1_000 + seq,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(payload),
        iv: None,
    }
}

fn node_config(s3: &S3Fields) -> ServerConfig {
    ServerConfig {
        num_shards: Some(NUM_SHARDS),
        log_level: "info".to_string(),
        routing_rule: RoutingRule::AggregateId,
        // Production-realistic: failover gap = 1500 + 500 + one S3 CAS.
        heartbeat_lease_duration_ms: 1500,
        heartbeat_interval_ms: 500,
        max_clock_drift_ms: 500,
        s3_lease_duration_ms: 10_000,
        s3_enabled: true,
        s3_region: Some(s3.region.clone()),
        s3_bucket: Some(s3.bucket.clone()),
        s3_access_key_id: Some(s3.access_key.clone()),
        s3_secret_access_key: Some(s3.secret_key.clone()),
        s3_endpoint_override: Some(s3.endpoint.clone()),
        s3_allow_http: s3.allow_http,
        ..Default::default()
    }
}

struct S3Fields {
    region: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    endpoint: String,
    allow_http: bool,
}

fn harness(msg: impl std::fmt::Display) -> Box<dyn std::error::Error> {
    format!("HARNESS FAILURE (setup never reached the defect's preconditions): {}", msg).into()
}

fn indent(lines: &[String]) -> String {
    lines.iter().map(|l| format!("      | {}", l)).collect::<Vec<_>>().join("\n")
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== A failed leadership challenge must not kill the process ===\n");

    let port_base = 21_500 + (std::process::id() % 20) as u16 * 10;
    let leader_port = port_base;
    let minio_port = port_base + 3;
    let challenger_port = port_base + 4;
    let proxy_port = port_base + 8;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "promotion-failure").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    let s3 = S3Fields { region, bucket, access_key, secret_key, endpoint, allow_http };

    // ── 1. Two nodes, replication link through the proxy ───────────────────
    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{}", challenger_port + 1)).await?;

    let mut leader =
        TestServer::start_with_config_labeled(leader_port, node_config(&s3), "leader".into())
            .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut challenger_config = node_config(&s3);
    challenger_config.advertised_replication_address = Some(format!("127.0.0.1:{}", proxy_port));
    let mut challenger = TestServer::start_with_config_labeled(
        challenger_port,
        challenger_config,
        "challenger".into(),
    )
    .await?;

    println!("Settling {:?} (election, S3 lease expiry, heartbeats)...", SETTLE);
    tokio::time::sleep(SETTLE).await;

    let leader_addr = format!("127.0.0.1:{}", leader_port);
    let challenger_addr = format!("127.0.0.1:{}", challenger_port);

    if !is_leader(&leader_addr).await? {
        return Err(harness(format!("node on {} never became leader", leader_addr)));
    }

    // ── 2. Cluster healthy and replicating ─────────────────────────────────
    println!("\nGATE: cluster healthy and replicating");
    let warm = agg_key(0);
    {
        let mut c = CeleriantClient::connect(&leader_addr).await?;
        for seq in 1..=5u64 {
            let opts = WriteEventsOptions { allow_create: seq == 1, ..Default::default() };
            c.write_events_with(warm.clone(), vec![event(64, seq)], 1, opts).await?;
        }
    }
    if !poll_visible(&challenger_addr, &warm, Duration::from_secs(20)).await {
        return Err(harness("follower never received the warm-up writes over TCP"));
    }
    println!("  Warm-up writes replicated to the follower.");

    // ── 3. Sever the link and push load: leader falls back to S3 ───────────
    println!("\nGATE: leader accepts load with the replication link severed");
    proxy.block();
    println!("  Replication link severed.");

    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::with_capacity(WRITE_WORKERS);
    for w in 0..WRITE_WORKERS {
        tasks.push(tokio::spawn(drive_writer(
            leader_addr.clone(),
            w,
            stop.clone(),
            acked.clone(),
        )));
    }
    tokio::time::sleep(PARTITION_LOAD).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks.drain(..) {
        let _ = t.await;
    }
    let partitioned_events = acked.load(Ordering::Relaxed);
    println!("  {} events acked while the link was down.", partitioned_events);
    if partitioned_events < MIN_PARTITIONED_EVENTS {
        return Err(harness(format!(
            "only {} events acked with the link severed (need {}); the leader never built an S3 backlog",
            partitioned_events, MIN_PARTITIONED_EVENTS
        )));
    }
    if let Err(status) = leader.check_alive() {
        return Err(harness(format!(
            "leader died during the partitioned load: {}\n{}",
            status,
            indent(&leader.log_tail(40))
        )));
    }

    let mut fallback_objects = 0usize;
    for shard in 0..NUM_SHARDS {
        let prefix = format!("cluster/fallback/shard_{:03}/", shard);
        fallback_objects += minio.list_objects(&prefix).await?.len();
    }
    println!("  {} S3 fallback objects across {} shards.", fallback_objects, NUM_SHARDS);
    if fallback_objects == 0 {
        return Err(harness(
            "no S3 fallback objects; the promoting node would have no S3 catch-up work to stall",
        ));
    }

    // ── 4. The follower is genuinely behind ────────────────────────────────
    println!("\nGATE: follower is behind at kill time");
    let lag_probe = AggregateKey::new(5, 5, 1);
    {
        let mut c = CeleriantClient::connect(&leader_addr).await?;
        for seq in 1..=3u64 {
            let opts = WriteEventsOptions { allow_create: seq == 1, ..Default::default() };
            c.write_events_with(lag_probe.clone(), vec![event(64, seq)], 2, opts).await?;
        }
    }
    let probe_visible = visible(&challenger_addr, &lag_probe).await;
    println!("  Newest leader writes visible on the follower: {}", probe_visible);
    if probe_visible {
        return Err(harness(
            "follower already had the leader's newest writes; it is not behind, so its promotion \
             has no meaningful catch-up to fail",
        ));
    }

    // ── 5. Record the lease epoch, then kill the leader ────────────────────
    let lease_before = deserialise_lease(&minio.get_object("cluster/lease.json").await?)
        .map_err(|e| harness(format!("could not read the pre-kill lease: {:?}", e)))?;
    let epoch_before = lease_before.lease_epoch;
    println!(
        "\n  Pre-kill lease: epoch={} leader_node_id={:x}",
        epoch_before, lease_before.leader_node_id
    );

    println!("\nGATE: leader dies");
    println!("  SIGKILL to leader (pid {})", leader.pid());
    leader.stop();
    let kill_at = Instant::now();
    // The old leader is gone; its link is moot. Restore the proxy so a severed
    // link cannot be mistaken for a permanently broken topology.
    proxy.unblock();
    // No probing here: the challenger's next challenge can land within a
    // second, and the CAS-win window this test has to hit is milliseconds wide.
    // `stop()` already SIGKILLed and reaped, so the exit status is authoritative.
    if leader.check_alive().is_ok() {
        return Err(harness("leader process still running after SIGKILL"));
    }
    println!("  Leader is gone.");

    // ── 6. Stall S3 the instant the challenger wins the CAS ────────────────
    println!("\nGATE: challenger wins the S3 CAS (epoch bump)");
    let mut epoch_after = None;
    while kill_at.elapsed() < EPOCH_WATCH {
        if let Ok(bytes) = minio.get_object("cluster/lease.json").await
            && let Ok(lease) = deserialise_lease(&bytes)
            && lease.lease_epoch > epoch_before
        {
            epoch_after = Some(lease.lease_epoch);
            break;
        }
        tokio::time::sleep(EPOCH_POLL).await;
    }
    let Some(epoch_after) = epoch_after else {
        return Err(harness(format!(
            "lease epoch never rose above {} within {:?} of the kill; no challenge was made, so \
             there was no failed challenge to observe",
            epoch_before, EPOCH_WATCH
        )));
    };
    let bump_at = Instant::now();
    println!(
        "  Epoch {} -> {} observed {:.2}s after the kill. Stalling S3 NOW.",
        epoch_before,
        epoch_after,
        kill_at.elapsed().as_secs_f64()
    );

    minio.pause()?;
    let stall_at = Instant::now();
    println!(
        "  MinIO paused {:.0}ms after the epoch bump; the challenger is in Promoting with no S3.",
        stall_at.duration_since(bump_at).as_secs_f64() * 1000.0
    );

    // ── 7. Watch the challenger across the stall and the recovery ─────────
    // The panic can land at either end: during the stall while the catch-up
    // errors out, or the moment S3 returns and a promotion that has already
    // decayed out of `Promoting` finally completes its catch-up.
    let mut watch = LogWatch::default();

    println!("\nHolding the stall for {:?}, watching the challenger...", STALL);
    let stall_watch =
        watch_node(&mut challenger, &challenger_addr, STALL, 0, &mut watch).await;
    if let Some(d) = &stall_watch.death {
        println!("\n  CHALLENGER PROCESS DIED during the stall:\n{}", d);
    }
    match stall_watch.first_writable {
        Some(t) => println!("  It began accepting writes {:.1}s into the stall.", t.as_secs_f64()),
        None => println!("  It never served writes during the stall (expected: no S3)."),
    }

    println!("\n  Unpausing MinIO.");
    minio.unpause()?;

    println!("\nAftermath watch ({:?} after the unpause)...", RECOVERY_BUDGET);
    let after_watch = if stall_watch.death.is_some() {
        NodeWatch::default()
    } else {
        watch_node(&mut challenger, &challenger_addr, RECOVERY_BUDGET, 1_000_000, &mut watch).await
    };

    // ── 8. Verdict ─────────────────────────────────────────────────────────
    let preconditions = format!(
        "Preconditions all held: {} events acked with the replication link severed, {} S3 \
         fallback objects for the promoting node to read back, follower behind, leader SIGKILLed, \
         lease epoch {} -> {}, S3 stalled {:.0}ms after the CAS win.",
        partitioned_events,
        fallback_objects,
        epoch_before,
        epoch_after,
        stall_at.duration_since(bump_at).as_secs_f64() * 1000.0
    );

    if let Some(line) = &watch.panic_line {
        let died = stall_watch.death.as_ref().or(after_watch.death.as_ref());
        return Err(format!(
            "CONTRACT VIOLATED: the challenger's leadership challenge failed and the node \
             PANICKED on the election path.\n\
             docs/leadership-replication-design.md makes `Promoting -> Follower | Fenced` \
             (\"lost the race / overran\") an ordinary, recoverable transition; a node that cannot \
             complete its S3 WAL catch-up must step back and retry, not abort.\n\
             {}\n\
             Panic: {}\n\
             Supervisor: {}\n\
             Process exited: {}\n\
             Log around the panic:\n{}",
            preconditions,
            line,
            watch.restart_line.as_deref().unwrap_or("(no shard-restart line seen)"),
            died.map(|d| d.as_str()).unwrap_or(
                "no — the in-process supervisor restarted the shard executors under it. \
                 On the rig the process died outright and hung until systemd SIGKILLed it; \
                 the panic is the defect either way."
            ),
            indent(&watch.context)
        )
        .into());
    }

    if let Some(d) = stall_watch.death.or(after_watch.death) {
        return Err(format!(
            "CONTRACT VIOLATED: the challenger's PROCESS TERMINATED after its failed leadership \
             challenge (no \"{}\" line was captured, so the cause is something else on the same \
             path).\n{}\nExit and log tail:\n{}",
            PANIC_MARKER, preconditions, d
        )
        .into());
    }

    if let Some(t) = stall_watch.first_writable
        && t < MISSED_WINDOW_GRACE
    {
        return Err(harness(format!(
            "the challenger was serving writes {:.1}s into the stall (< {:?}), so its S3 catch-up \
             had already completed when MinIO was paused. The stall landed after the promotion \
             window, not inside it; nothing about a FAILED challenge was exercised",
            t.as_secs_f64(),
            MISSED_WINDOW_GRACE
        )));
    }

    println!("\n  Challenger survived, with no election panic on its log.");
    match after_watch.first_writable {
        Some(d) => println!("  Writes accepted {:.1}s after the unpause.", d.as_secs_f64()),
        None if stall_watch.first_writable.is_some() => {}
        None => println!(
            "  WARNING: still not writable {:?} after the unpause. The process survived and did \
             not panic (the contract under test), but the node has not recovered.",
            RECOVERY_BUDGET
        ),
    }

    println!("\n=== PASS: a failed leadership challenge neither panicked nor killed the node ===");
    Ok(())
}

#[derive(Default)]
struct NodeWatch {
    /// Set when the process exited on its own; carries the status and log tail.
    death: Option<String>,
    /// When the node first accepted a write, measured from the watch start.
    first_writable: Option<Duration>,
}

/// Watch one node for `budget`, keeping the log scan hot the whole time.
///
/// The write probe is spawned rather than awaited inline: a probe against a
/// wedged node blocks for `WRITE_PROBE_TIMEOUT`, which is long enough for a
/// panic to scroll out of the node's bounded log ring and go unnoticed.
async fn watch_node(
    server: &mut TestServer,
    address: &str,
    budget: Duration,
    nonce_base: u64,
    log: &mut LogWatch,
) -> NodeWatch {
    let start = Instant::now();
    let mut out = NodeWatch::default();
    let mut next_probe = start;
    let mut probe: Option<tokio::task::JoinHandle<bool>> = None;

    while start.elapsed() < budget {
        log.poll(server);
        if let Err(status) = server.check_alive() {
            out.death = Some(format!(
                "{} (after {:.1}s)\n{}",
                status,
                start.elapsed().as_secs_f64(),
                indent(&server.log_tail(80))
            ));
            break;
        }
        match probe.take() {
            Some(h) if h.is_finished() => {
                if h.await.unwrap_or(false) {
                    out.first_writable = Some(start.elapsed());
                    break;
                }
            }
            Some(h) => probe = Some(h),
            None if out.first_writable.is_none() && Instant::now() >= next_probe => {
                next_probe = Instant::now() + WRITE_PROBE_INTERVAL;
                let addr = address.to_string();
                let nonce = nonce_base + start.elapsed().as_millis() as u64;
                probe = Some(tokio::spawn(async move { write_probe(&addr, nonce).await }));
            }
            None => {}
        }
        tokio::time::sleep(LIVENESS_POLL).await;
    }
    out
}

/// One load worker, hammering its own slice of aggregates.
async fn drive_writer(addr: String, worker: usize, stop: Arc<AtomicBool>, acked: Arc<AtomicUsize>) {
    let Ok(mut client) = CeleriantClient::connect(&addr).await else { return };
    let client_id = 1_000 + worker as u128;
    let mut agg = worker + 1;
    while !stop.load(Ordering::Relaxed) {
        let key = agg_key(agg);
        let events: Vec<_> = (1..=4u64).map(|s| event(PAYLOAD_BYTES, s)).collect();
        let n = events.len();
        let opts = WriteEventsOptions { allow_create: true, ..Default::default() };
        match client.write_events_with(key, events, client_id, opts).await {
            Ok(_) => {
                acked.fetch_add(n, Ordering::Relaxed);
            }
            Err(_) => {
                if let Ok(c) = CeleriantClient::connect(&addr).await {
                    client = c;
                }
            }
        }
        agg += WRITE_WORKERS;
    }
}

/// Can this node accept a write right now? Bounded, so a wedged node cannot
/// stall the liveness watch.
async fn write_probe(address: &str, nonce: u64) -> bool {
    let key = AggregateKey::new(7, 7, nonce as u128);
    let attempt = async {
        let mut c = CeleriantClient::connect(address).await.ok()?;
        let opts = WriteEventsOptions { allow_create: true, ..Default::default() };
        c.write_events_with(key, vec![event(64, 1)], 42, opts).await.ok()
    };
    matches!(tokio::time::timeout(WRITE_PROBE_TIMEOUT, attempt).await, Ok(Some(_)))
}

async fn visible(address: &str, key: &AggregateKey) -> bool {
    let Ok(mut c) = CeleriantClient::connect(address).await else { return false };
    crate::common::read_all(&mut c, key).await.is_ok_and(|batches| !batches.is_empty())
}

async fn poll_visible(address: &str, key: &AggregateKey, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if visible(address, key).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}
