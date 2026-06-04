//! Metamorphic oracle: speculative-tail cull parity.
//!
//! Successor to the deleted `metamorphic_rollback_parity` (`fe90ecc`):
//! replication rollback was replaced by an in-place spin retry plus
//! `cull_speculative_tail_for_promotion` (`shard_wal.rs`). Covers
//! `docs/pending-testing/remaining-tests.md` item 1.
//!
//! Empirical note (first run of this test): the two recovery mechanisms map
//! to *how the leader lost leadership*, not to promotion-vs-demotion as the
//! plan doc assumed:
//!
//! - **Phase F (fence-demotion → the cull).** The leader stays alive, loses
//!   both replication paths, self-fences, and demotes in-process when the
//!   promoted peer's higher-epoch heartbeat arrives. The cull
//!   (`WriteToRead` arm) rewinds `write` to `read` before any catchup.
//!   A SIGKILLed leader never takes this path: its in-process transition
//!   never happens.
//! - **Phase K (kill → boot divergence truncation).** A SIGKILLed leader
//!   restarts with the `read < write` gap restored from the header. *Boot*
//!   catchup (BootCatchup state, before any lease discovery or Follower
//!   transition) meets the forked tail first and removes it via the S3
//!   divergence machinery — common-ancestor truncation when an S3 batch
//!   overlaps the tail base, the `be3c6b4` reframe-at-read-cursor when none
//!   does. Which flavor fires per shard depends on what S3 batches exist;
//!   phase K asserts the outcome and the ack barrier, and reports the
//!   mechanism counters.
//! - **Phase R (kill → forced reframe).** Phase K's orchestration, plus:
//!   before the old leader restarts, every S3 batch at/below the tail base
//!   is deleted — simulating S3 GC (the documented ancestor-skew edge,
//!   `evaluation-todo.md` item 3). Find-ancestor-by-download then has
//!   nothing to match, so only the reframe (an S3 batch chaining onto the
//!   read tip) can recover the node. Pins the `be3c6b4` liveness fix:
//!   reframe fires on every shard, the node rejoins instead of looping on
//!   "no common ancestor".
//!
//! Both phases share the oracles: speculative writes must never be ACKed;
//! the tail's distinct payload marker (4KB incompressible vs ~15B
//! legitimate) must be absent from both nodes; both nodes byte-identical
//! (`diff_aggregate`, `DiffMode::SameRun`); no acked events dropped and no
//! ack-barrier refusals (`celeriant_truncate_*` counters). Each phase has a
//! precondition gate proving the tail existed (phase F: fsync-time
//! `celeriant_wal_seq` gauge; phase K: on-disk headers read while the node
//! is dead) so a pass can't be vacuous.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events,
    metamorphic_common::{diff_aggregate, format_key, wait_for_promotion, DiffMode},
    poll_event_count, read_all_batches, s3_cluster_config, scrape_counter,
    wait_for_election_and_replication, write_event, write_large_event, MinioContainer,
    TcpProxy, TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES;
use celeriant_wal::shard_log_header::ShardLogHeader;
use celeriant_wire::disk::versioned_block::deserialise_shard_log_header;
use std::io::{Read, Seek, SeekFrom};
use std::time::Duration;

const NUM_SHARDS: usize = 2;
const BASELINE_EVENTS: u64 = 10;
const FINAL_EVENTS: u64 = 20; // baseline 1..=10 via the first leader, 11..=20 via the second
const SPEC_PAYLOAD_BYTES: usize = 4096;
const MAX_LEGIT_PAYLOAD_BYTES: usize = 1024;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Metamorphic: Speculative-Tail Cull Parity ===\n");
    // %25, not the sibling tests' %100: three 50-port cluster blocks span
    // 150 ports, and 18000 + 24 + 150 stays clear of the 18300-based tests.
    let port_base = 18000 + (std::process::id() % 25) as u16;

    println!("─── Phase F: fence-demotion → promotion cull ───\n");
    phase_fence_demotion_cull(port_base).await?;

    println!("\n─── Phase K: SIGKILL → boot divergence truncation ───\n");
    phase_kill_boot_truncation(port_base + 50, false).await?;

    println!("\n─── Phase R: SIGKILL → S3 GC of the tail base → forced reframe ───\n");
    phase_kill_boot_truncation(port_base + 100, true).await?;

    println!("\n=== PASS: all tail-removal paths hold byte parity ===");
    Ok(())
}

/// Compact per-cluster port block (span < 50 so three blocks fit the range):
/// node A at +0 (+1 repl, +2 metrics), MinIO at +10, node B at +20 (+1, +2),
/// proxy fronting B's repl at +40, proxy fronting A's S3 at +41.
struct PhasePorts {
    node_a: u16,
    node_b: u16,
    minio: u16,
    proxy_repl: u16,
    proxy_s3_a: u16,
}

impl PhasePorts {
    fn new(base: u16) -> Self {
        Self {
            node_a: base,
            node_b: base + 20,
            minio: base + 10,
            proxy_repl: base + 40,
            proxy_s3_a: base + 41,
        }
    }
}

/// s3_cluster_config routes by aggregate_type_id; distinct type ids spread
/// the aggregates across both shards so the tail exists on every shard.
fn workload_keys() -> Vec<AggregateKey> {
    (1..=NUM_SHARDS as u128).map(|id| AggregateKey::new(1, id, id)).collect()
}

/// Leader alive throughout: severing TCP (proxy to B's replication port) and
/// S3 (A-only proxy to MinIO) holds the spin retry open; A self-fences at
/// lease expiry (~10s), B promotes via direct S3, and B's higher-epoch
/// heartbeat demotes A in-process — the only path where
/// `cull_speculative_tail_for_promotion` meets the tail.
async fn phase_fence_demotion_cull(port_base: u16) -> Result<(), Box<dyn std::error::Error>> {
    let ports = PhasePorts::new(port_base);
    let (node_a_port, node_b_port, minio_port) = (ports.node_a, ports.node_b, ports.minio);
    let (proxy_repl_port, proxy_s3_port) = (ports.proxy_repl, ports.proxy_s3_a);

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let mut config = s3_cluster_config(
        NUM_SHARDS, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.heartbeat_lease_duration_ms = 10_000;
    config.s3_lease_duration_ms = 10_000;

    // A-only S3 path: blocking it fences A while B can still win the CAS.
    let proxy_s3 = TcpProxy::start(proxy_s3_port, format!("127.0.0.1:{}", minio_port)).await?;
    let mut a_config = config.clone();
    a_config.client_port = node_a_port;
    a_config.s3_endpoint_override = Some(format!("http://127.0.0.1:{}", proxy_s3_port));
    println!("Starting node A (initial leader) on port {} (S3 via proxy {})...", node_a_port, proxy_s3_port);
    let node_a = TestServer::start_with_config_labeled(node_a_port, a_config, "node-a".into()).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let proxy_repl = TcpProxy::start(proxy_repl_port, format!("127.0.0.1:{}", node_b_port + 1)).await?;
    let mut b_config = config.clone();
    b_config.client_port = node_b_port;
    b_config.advertised_replication_address = Some(format!("127.0.0.1:{}", proxy_repl_port));
    println!("Starting node B (follower) on port {}...", node_b_port);
    let node_b = TestServer::start_with_config_labeled(node_b_port, b_config, "node-b".into()).await?;

    println!("Waiting for election + replication connection...");
    wait_for_election_and_replication().await;

    let keys = workload_keys();
    let mut a_client = CeleriantClient::connect(node_a.address()).await?;
    println!("Baseline: writing events 1..={} × {} aggregates to leader A...", BASELINE_EVENTS, keys.len());
    for event_num in 1..=BASELINE_EVENTS {
        for key in &keys {
            write_event(&mut a_client, key, event_num, event_num == 1).await?;
        }
    }
    let mut b_client = CeleriantClient::connect(node_b.address()).await?;
    for key in &keys {
        let n = count_events(&mut b_client, key).await?;
        if n as u64 != BASELINE_EVENTS {
            return Err(format!(
                "baseline not replicated to B: aggregate {} has {} events, expected {}",
                format_key(key), n, BASELINE_EVENTS
            ).into());
        }
    }

    println!("\nSevering A's TCP replication and A's S3 path (B keeps S3)...");
    proxy_repl.block();
    proxy_s3.block();

    println!("Issuing {} speculative writes (4KB markers) — fsync then spin retry...", keys.len());
    let mut spec_handles = Vec::new();
    for key in &keys {
        let key = key.clone();
        let addr = node_a.address().to_string();
        spec_handles.push(tokio::spawn(async move {
            let mut client = CeleriantClient::connect(&addr).await.map_err(|e| e.to_string())?;
            write_large_event(&mut client, &key, 2_000 + key.aggregate_id as u64, SPEC_PAYLOAD_BYTES)
                .await
                .map_err(|e| e.to_string())
        }));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Precondition gate: the tail must be fsynced. celeriant_wal_seq is a
    // per-shard GAUGE set at fsync time with the write cursor (scrape_counter
    // just sums the per-shard series — fine for a gauge snapshot). At-least:
    // baseline (2×10) + one speculative write per shard.
    let a_metrics_port = node_a.config().metrics_port;
    let walseq_sum = scrape_counter("127.0.0.1", a_metrics_port, "celeriant_wal_seq").await?;
    let expected_walseq = NUM_SHARDS as u64 * BASELINE_EVENTS + keys.len() as u64;
    if walseq_sum < expected_walseq {
        return Err(format!(
            "speculative tail not fsynced: wal_seq sum {} < expected {} — spin retry isn't \
             holding the tail (precondition failed; parity below would be vacuous)",
            walseq_sum, expected_walseq
        ).into());
    }
    println!("  Tail fsynced (wal_seq sum = {}).", walseq_sum);

    // A self-fences at ~10s; the spin aborts with LeaderFenced and the writes
    // come back unacked. An Ok here means the cull would drop an acked write.
    println!("Waiting for A to fence and the speculative writes to fail...");
    for (i, handle) in spec_handles.into_iter().enumerate() {
        match tokio::time::timeout(Duration::from_secs(40), handle).await {
            Ok(Ok(Ok(()))) => {
                return Err(format!(
                    "speculative write {} was ACKed despite both replication paths blocked — \
                     culling it later would lose an acknowledged write", i
                ).into());
            }
            Ok(Ok(Err(e))) => println!("  speculative write {} unacked as expected ({})", i, e),
            Ok(Err(join_err)) => println!("  speculative write {} task ended: {}", i, join_err),
            Err(_) => return Err(format!("speculative write {} still pending 40s after sever", i).into()),
        }
    }

    println!("Waiting for B to win the S3 lease (epoch 2) and serve writes...");
    let mut b_client = wait_for_promotion(node_b.address()).await?;
    println!("  Node B promoted.");

    // Heal the network BEFORE writing through B: B's higher-epoch heartbeat
    // demotes A in-process and the cull fires there, with no backlog of
    // stale heartbeats to burst through later (a backlogged burst is
    // correctly rejected by clock-drift fencing, but it makes the run
    // nondeterministic — observed once wedging the probe→catchup loop, see
    // the F5 finding).
    println!("Unblocking A's S3 and TCP replication; B's heartbeat demotes A (cull fires)...");
    proxy_s3.unblock();
    proxy_repl.unblock();
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("Phase-F writes: events {}..={} × {} aggregates to leader B...", BASELINE_EVENTS + 1, FINAL_EVENTS, keys.len());
    for event_num in (BASELINE_EVENTS + 1)..=FINAL_EVENTS {
        for key in &keys {
            write_event(&mut b_client, key, event_num, false).await?;
        }
    }

    for key in &keys {
        let count = poll_event_count(node_a.address(), key, FINAL_EVENTS as usize, Duration::from_secs(60)).await;
        println!("  aggregate {}: {} events on A", format_key(key), count);
    }

    let mut a_client = CeleriantClient::connect(node_a.address()).await?;
    let mismatches = verify_parity_and_marker(&mut a_client, &mut b_client, &keys).await?;
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("  MISMATCH: {}", m);
        }
        return Err(format!("phase F parity failed with {} mismatch(es)", mismatches.len()).into());
    }
    println!("  Byte parity holds; no speculative marker on either node.");

    // Mechanism pinning: A stayed alive, so the only legal tail-remover is
    // the cull. Any truncation/reframe activity means the cull missed the
    // tail and the divergence machinery cleaned up after it.
    let m = scrape_mechanism_counters(a_metrics_port).await?;
    println!("  A counters: {}", m.report());
    if m.reframed != 0 || m.divergence_advanced != 0 {
        return Err(format!(
            "tail removed by truncation/reframe ({}) — cull_speculative_tail_for_promotion \
             did not fire before catchup on the in-process demotion", m.report()
        ).into());
    }
    m.assert_barrier_clean()?;

    println!("Phase F passed.");
    Ok(())
}

/// Leader SIGKILLed mid-spin: the crash-restored `read < write` gap is met by
/// *boot* catchup before any status transition, and removed by the S3
/// divergence machinery (ancestor truncation or reframe, per shard).
///
/// `force_reframe`: before the restart, delete every S3 batch at/below the
/// tail base (simulated S3 GC) so no download can match an ancestor — the
/// reframe-at-read-cursor is then the only recovery, and it is asserted to
/// fire on every shard.
async fn phase_kill_boot_truncation(port_base: u16, force_reframe: bool) -> Result<(), Box<dyn std::error::Error>> {
    let ports = PhasePorts::new(port_base);
    let (node_a_port, node_b_port, minio_port) = (ports.node_a, ports.node_b, ports.minio);
    let proxy_repl_port = ports.proxy_repl;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let mut config = s3_cluster_config(
        NUM_SHARDS, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.heartbeat_lease_duration_ms = 10_000;
    config.s3_lease_duration_ms = 10_000;

    let mut a_config = config.clone();
    a_config.client_port = node_a_port;
    println!("Starting node A (initial leader) on port {}...", node_a_port);
    let mut node_a = TestServer::start_with_config_labeled(node_a_port, a_config, "node-a".into()).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let proxy_repl = TcpProxy::start(proxy_repl_port, format!("127.0.0.1:{}", node_b_port + 1)).await?;
    let mut b_config = config.clone();
    b_config.client_port = node_b_port;
    b_config.advertised_replication_address = Some(format!("127.0.0.1:{}", proxy_repl_port));
    println!("Starting node B (follower) on port {}...", node_b_port);
    let node_b = TestServer::start_with_config_labeled(node_b_port, b_config, "node-b".into()).await?;

    println!("Waiting for election + replication connection...");
    wait_for_election_and_replication().await;

    let keys = workload_keys();
    let mut a_client = CeleriantClient::connect(node_a.address()).await?;
    println!("Baseline: writing events 1..={} × {} aggregates to leader A...", BASELINE_EVENTS, keys.len());
    for event_num in 1..=BASELINE_EVENTS {
        for key in &keys {
            write_event(&mut a_client, key, event_num, event_num == 1).await?;
        }
    }
    let mut b_client = CeleriantClient::connect(node_b.address()).await?;
    for key in &keys {
        let n = count_events(&mut b_client, key).await?;
        if n as u64 != BASELINE_EVENTS {
            return Err(format!(
                "baseline not replicated to B: aggregate {} has {} events, expected {}",
                format_key(key), n, BASELINE_EVENTS
            ).into());
        }
    }

    println!("\nBlocking TCP replication (proxy) and pausing MinIO (S3)...");
    proxy_repl.block();
    minio.pause()?;

    println!("Issuing {} speculative writes (4KB markers) — fsync then spin retry...", keys.len());
    let mut spec_handles = Vec::new();
    for key in &keys {
        let key = key.clone();
        let addr = node_a.address().to_string();
        spec_handles.push(tokio::spawn(async move {
            let mut client = CeleriantClient::connect(&addr).await.map_err(|e| e.to_string())?;
            write_large_event(&mut client, &key, 1_000 + key.aggregate_id as u64, SPEC_PAYLOAD_BYTES)
                .await
                .map_err(|e| e.to_string())
        }));
    }

    // Let the writes fsync and enter the spin. Must stay well below A's
    // self-fence horizon (~10s) so the kill lands mid-spin while A is Leader.
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("SIGKILL leader A mid-spin.");
    drop(a_client);
    node_a.stop();

    // 10s (vs phase F's 40s): the SIGKILL resets the clients' TCP connections
    // immediately, so the tasks fail fast; phase F has to outwait the ~10s
    // self-fence before its writes abort.
    for (i, handle) in spec_handles.into_iter().enumerate() {
        match tokio::time::timeout(Duration::from_secs(10), handle).await {
            Ok(Ok(Ok(()))) => {
                return Err(format!(
                    "speculative write {} was ACKed despite both replication paths blocked — \
                     dropping it at boot would lose an acknowledged write", i
                ).into());
            }
            Ok(Ok(Err(e))) => println!("  speculative write {} unacked as expected ({})", i, e),
            Ok(Err(join_err)) => println!("  speculative write {} task ended: {}", i, join_err),
            Err(_) => return Err(format!("speculative write {} still pending 10s after leader kill", i).into()),
        }
    }

    // Precondition gate: on-disk headers must show the crash-restored gap.
    println!("\nVerifying A's on-disk cursors show a speculative tail (read < write)...");
    let snapshots = shard_cursor_snapshots(&node_a.config().data_root)?;
    let mut shards_with_gap = 0;
    for s in &snapshots {
        println!(
            "  {}: read={} write={} last_self_acked={}",
            s.shard, s.read_wal_seq, s.write_wal_seq, s.last_self_acked
        );
        if s.write_wal_seq > s.read_wal_seq {
            shards_with_gap += 1;
            if s.last_self_acked != s.read_wal_seq {
                return Err(format!(
                    "{}: last_self_acked={} != read={} — acked writes sit inside the speculative tail",
                    s.shard, s.last_self_acked, s.read_wal_seq
                ).into());
            }
        }
    }
    if shards_with_gap != keys.len() {
        return Err(format!(
            "expected a speculative tail on {} shards, found {} — the spin retry did not hold \
             the tail open (precondition failed; parity below would be vacuous)",
            keys.len(), shards_with_gap
        ).into());
    }
    println!("  Speculative tail confirmed on {} shards.", shards_with_gap);

    println!("\nUnpausing MinIO, unblocking proxy...");
    minio.unpause()?;
    proxy_repl.unblock();

    println!("Waiting for B to win the S3 lease and serve writes...");
    let mut b_client = wait_for_promotion(node_b.address()).await?;
    println!("  Node B promoted.");

    println!("Phase-K writes: events {}..={} × {} aggregates to leader B...", BASELINE_EVENTS + 1, FINAL_EVENTS, keys.len());
    for event_num in (BASELINE_EVENTS + 1)..=FINAL_EVENTS {
        for key in &keys {
            write_event(&mut b_client, key, event_num, false).await?;
        }
    }

    if force_reframe {
        // Simulated S3 GC: remove every batch at/below the tail base so the
        // divergence scan cannot match an ancestor by download. Coverage from
        // read+1 upward stays intact, so the reframe's contiguity gate holds.
        let mut deleted = 0;
        for shard_id in 0..NUM_SHARDS {
            let prefix = format!("cluster/fallback/shard_{:03}/", shard_id);
            for key in minio.list_objects(&prefix).await? {
                if let Some((_, last_wal)) = parse_batch_range(&key) {
                    if last_wal <= BASELINE_EVENTS {
                        minio.delete_object(&key).await?;
                        deleted += 1;
                    }
                }
            }
        }
        if deleted == 0 {
            return Err("forced-reframe setup found no S3 batches at/below the tail base to GC — \
                        the reframe precondition was not created"
                .into());
        }
        println!("\nSimulated S3 GC: deleted {} batch(es) at/below wal_seq {}.", deleted, BASELINE_EVENTS);
    }

    println!("\nRestarting node A — boot catchup must drop the tail, then converge...");
    node_a.restart().await?;
    for key in &keys {
        let count = poll_event_count(node_a.address(), key, FINAL_EVENTS as usize, Duration::from_secs(60)).await;
        println!("  aggregate {}: {} events on A", format_key(key), count);
    }

    let mut a_client = CeleriantClient::connect(node_a.address()).await?;
    let mismatches = verify_parity_and_marker(&mut a_client, &mut b_client, &keys).await?;
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("  MISMATCH: {}", m);
        }
        return Err(format!("phase K parity failed with {} mismatch(es)", mismatches.len()).into());
    }
    println!("  Byte parity holds; no speculative marker on either node.");

    let m = scrape_mechanism_counters(node_a.config().metrics_port).await?;
    println!("  A counters (post-restart): {}", m.report());
    m.assert_barrier_clean()?;
    if force_reframe {
        // With the ancestors GC'd, the reframe is the only legal recovery and
        // must have fired on every shard; rejoining at all (poll above) is
        // the be3c6b4 liveness assertion.
        if m.reframed < NUM_SHARDS as u64 {
            return Err(format!(
                "reframe fired on {} of {} shards despite GC'd ancestors — node recovered \
                 through a path that should have been impossible", m.reframed, NUM_SHARDS
            ).into());
        }
        println!("Phase R passed.");
    } else {
        // Which divergence flavor fired per shard (ancestor truncation vs
        // reframe) depends on the S3 batch layout — report, don't pin.
        println!("Phase K passed.");
    }
    Ok(())
}

/// `cluster/fallback/shard_NNN/batch_{first:09}_{last:09}_{uuid}.bin` → (first, last).
fn parse_batch_range(key: &str) -> Option<(u64, u64)> {
    let name = key.rsplit('/').next()?;
    let rest = name.strip_prefix("batch_")?;
    let mut parts = rest.splitn(3, '_');
    let first = parts.next()?.parse().ok()?;
    let last = parts.next()?.parse().ok()?;
    Some((first, last))
}

struct MechanismCounters {
    reframed: u64,
    divergence_advanced: u64,
    dropped_self_acked: u64,
    barrier_refusals: u64,
}

impl MechanismCounters {
    fn report(&self) -> String {
        format!(
            "reframed_at_read={} divergence_advanced={} dropped_self_acked={} barrier_refusals={}",
            self.reframed, self.divergence_advanced, self.dropped_self_acked, self.barrier_refusals
        )
    }

    /// The ack barrier must never be crossed (acked events dropped) nor hit
    /// (truncation refused at the barrier) in either phase: nothing acked
    /// ever sits above the cut point in these scenarios.
    fn assert_barrier_clean(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.dropped_self_acked != 0 {
            return Err("truncation dropped self-acked events".into());
        }
        if self.barrier_refusals != 0 {
            return Err("truncation was refused at the ack barrier — acked data sat above the cut point".into());
        }
        Ok(())
    }
}

async fn scrape_mechanism_counters(metrics_port: u16) -> Result<MechanismCounters, Box<dyn std::error::Error>> {
    Ok(MechanismCounters {
        reframed: scrape_counter("127.0.0.1", metrics_port, "celeriant_s3_catchup_reframed_at_read_total").await?,
        divergence_advanced: scrape_counter("127.0.0.1", metrics_port, "celeriant_truncate_divergence_advanced_total").await?,
        dropped_self_acked: scrape_counter("127.0.0.1", metrics_port, "celeriant_truncate_dropped_self_acked_events_total").await?,
        barrier_refusals: scrape_counter("127.0.0.1", metrics_port, "celeriant_truncate_refused_due_to_ack_barrier_total").await?,
    })
}

/// Read every aggregate from both nodes; collect parity diffs, count
/// mismatches, and any event whose payload exceeds the legitimate maximum
/// (i.e. a culled speculative marker that leaked into the final chain).
async fn verify_parity_and_marker(
    a_client: &mut CeleriantClient,
    b_client: &mut CeleriantClient,
    keys: &[AggregateKey],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut mismatches = Vec::new();
    for key in keys {
        let a_batches = read_all_batches(a_client, key).await?;
        let b_batches = read_all_batches(b_client, key).await?;
        if (a_batches.len() as u64) != FINAL_EVENTS {
            mismatches.push(format!(
                "aggregate {}: A returned {} batches, expected {}",
                format_key(key), a_batches.len(), FINAL_EVENTS
            ));
        }
        if let Err(msg) = diff_aggregate(key, &a_batches, &b_batches, DiffMode::SameRun) {
            mismatches.push(msg);
        }
        for (label, batches) in [("A", &a_batches), ("B", &b_batches)] {
            for (bi, batch) in batches.iter().enumerate() {
                for (ei, ev) in batch.events.iter().enumerate() {
                    if ev.event_value.len() > MAX_LEGIT_PAYLOAD_BYTES {
                        mismatches.push(format!(
                            "aggregate {} node {} batch[{}] event[{}]: {}B payload — culled \
                             speculative marker leaked into the final chain",
                            format_key(key), label, bi, ei, ev.event_value.len()
                        ));
                    }
                }
            }
        }
    }
    Ok(mismatches)
}

struct ShardCursorSnapshot {
    shard: String,
    read_wal_seq: u64,
    write_wal_seq: u64,
    last_self_acked: u64,
}

/// Parse the active segment's on-disk header for every shard under
/// `data_root`. Front header first, rear as fallback — same policy as
/// `celeriant-wal-inspect`. Only call while the node is stopped.
fn shard_cursor_snapshots(
    data_root: &std::path::Path,
) -> Result<Vec<ShardCursorSnapshot>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(data_root)? {
        let entry = entry?;
        let shard = entry.file_name().to_string_lossy().into_owned();
        if !shard.starts_with("shard_") || !entry.file_type()?.is_dir() {
            continue;
        }
        let mut active: Option<(u64, std::path::PathBuf)> = None;
        for file in std::fs::read_dir(entry.path())? {
            let file = file?;
            let name = file.file_name().to_string_lossy().into_owned();
            if let Some(id) = name
                .strip_prefix("log_")
                .and_then(|s| s.strip_suffix(".wal"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                if active.as_ref().is_none_or(|(aid, _)| id > *aid) {
                    active = Some((id, file.path()));
                }
            }
        }
        let Some((_, path)) = active else { continue };
        let header = read_segment_header(&path)
            .ok_or_else(|| format!("{}: both headers unreadable in {:?}", shard, path))?;
        out.push(ShardCursorSnapshot {
            shard,
            read_wal_seq: header.read.wal_seq,
            write_wal_seq: header.write.wal_seq,
            last_self_acked: header.last_self_acked_wal_seq,
        });
    }
    Ok(out)
}

fn read_segment_header(path: &std::path::Path) -> Option<ShardLogHeader> {
    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    let mut read_at = |pos: u64| -> Option<ShardLogHeader> {
        let mut buf = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        file.seek(SeekFrom::Start(pos)).ok()?;
        file.read_exact(&mut buf).ok()?;
        deserialise_shard_log_header(&buf).ok()
    };
    read_at(0).or_else(|| read_at(len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64)))
}
