//! Stale-lease-on-restart split-brain reproducer.
//!
//! Bug: in the leader heartbeat loop (`shard.rs:788-833`), when a heartbeat
//! succeeds (`HeartbeatResult::Ack`), the code max-merges the local TTL then
//! hits `continue` at line 833 — the S3 lease renewal block below is NEVER
//! reached during a healthy two-node cluster. After `s3_lease_duration_ms`
//! wall-clock time, `lease.bin` is stale.
//!
//! A restarting follower reads the stale lease and CAS-bumps the epoch — split brain.
//! `decide_post_catchup_action` (BootWaitThenReevaluate path) guards against this and is
//! now wired in (called from shard.rs and connection_handler.rs). An earlier review note
//! flagged it as unreachable dead code; that is no longer accurate.
//!
//! Sequence:
//!   Phase 1: Start cs1 (leader) + cs2 (follower). Warmup writes. Cluster
//!            healthy. s3_lease_duration_ms=3000 → lease becomes stale after 3s
//!            without the fix.
//!
//!   Phase 2: Sleep 8s. During this time heartbeats succeed on every interval
//!            (500ms) so the `continue` path fires every iteration. lease.bin
//!            is never refreshed. It is now stale.
//!
//!   Phase 3: Block cs1→MinIO via TcpProxy so cs1 cannot do a preemptive
//!            S3 renewal when it detects cs2 disappeared.
//!            Kill cs2. Restart cs2 (same data_root via `restart()`).
//!            cs2 boots, reads stale lease → ChallengeViaCAS → grabs epoch N+1.
//!
//!   Phase 4: While cs1→MinIO is still blocked, write to cs1 (it still thinks
//!            it is leader via its local TTL from the last heartbeat ACK).
//!            cs1 self-acks with old epoch. Then unblock cs1→MinIO.
//!            cs1 discovers it lost the CAS → self-fences.
//!
//!   Phase 5: Write to the new leader (cs2) to get a second self-ack at the
//!            same WAL position with a different epoch.
//!
//!   Phase 6: Stop both nodes. Inspect WALs with celeriant-wal-inspect.
//!            If both have last_self_acked >= the contested wal_seq AND the
//!            lease_epoch at that wal_seq differs → SPLIT BRAIN CONFIRMED.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::requests::{SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::aggregate_key::AggregateKey;

use crate::{s3_cluster_config, write_event, MinioContainer, TestServer, TcpProxy};

const PORT_BASE: u16 = 22000;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Stale-Lease-on-Restart Split-Brain Reproducer ===\n");
    println!("Bug: leader heartbeat Ack path hits `continue` before S3 renewal → lease.bin goes stale.\n");

    let cs1_port = PORT_BASE;
    let cs2_port = PORT_BASE + 100;
    let minio_port = PORT_BASE + 10;
    let proxy_minio_cs1_port = PORT_BASE + 20;

    let aggregate_key = AggregateKey::new(1, 1, 42);

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-stale-restart").await?;
    let (region, bucket, access_key, secret_key, minio_endpoint, allow_http) =
        minio.s3_config_fields();
    println!("MinIO ready at {}\n", minio_endpoint);

    // cs1→MinIO proxy so we can block cs1's S3 access during the restart window.
    println!("Starting cs1→MinIO proxy on port {}...", proxy_minio_cs1_port);
    let proxy_minio_cs1 =
        TcpProxy::start(proxy_minio_cs1_port, format!("127.0.0.1:{}", minio_port)).await?;
    let cs1_minio_endpoint = format!("http://127.0.0.1:{}", proxy_minio_cs1_port);

    // s3_lease_duration_ms=3000: lease becomes stale after 3s without renewal.
    // heartbeat_interval_ms=500 (default): HBs succeed frequently, keeping the
    // `continue` path firing and never reaching S3 renewal code.
    // heartbeat_lease_duration_ms=60000: cs1's LOCAL TTL stays warm long after
    // lease.bin is stale — cs1 won't self-fence via local check.
    let mut cs1_config = s3_cluster_config(
        1,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &cs1_minio_endpoint,
        allow_http,
    );
    cs1_config.s3_lease_duration_ms = 3_000;
    cs1_config.heartbeat_lease_duration_ms = 60_000;
    cs1_config.heartbeat_interval_ms = 500;

    // cs2 uses direct MinIO — fast CAS so it can grab the lease quickly on restart.
    let mut cs2_config = s3_cluster_config(
        1,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &minio_endpoint,
        allow_http,
    );
    cs2_config.s3_lease_duration_ms = 3_000;
    cs2_config.heartbeat_lease_duration_ms = 60_000;
    cs2_config.heartbeat_interval_ms = 500;

    // cs1 starts first so it wins initial S3 election → becomes leader epoch=1.
    println!("Starting cs1 (will become leader epoch=1) on port {}...", cs1_port);
    let cs1 = TestServer::start_with_config_labeled(cs1_port, cs1_config, "cs1".into()).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    println!("Starting cs2 (follower) on port {}...", cs2_port);
    let mut cs2 = TestServer::start_with_config_labeled(cs2_port, cs2_config, "cs2".into()).await?;

    // ── Phase 1: warmup ──────────────────────────────────────────────────────
    println!("\nPhase 1: Warmup — wait for election then write events 1-3 via cs1");
    println!("  Waiting 5s for election and replication to stabilise...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut cs1_client = CeleriantClient::connect(cs1.address()).await?;
    for seq in 1u64..=3 {
        write_event(&mut cs1_client, &aggregate_key, seq, seq == 1).await?;
    }
    println!("  Warmup writes 1-3 acked by cs1");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── Phase 2: wait for S3 lease to go stale ────────────────────────────
    // s3_lease_duration_ms=3000. We wait 8s → 2.6× the lease duration.
    // If the bug is present, lease.bin has not been touched since initial
    // election (every HB ack hits `continue` before the S3 renewal block).
    println!("\nPhase 2: Waiting 8s for S3 lease to go stale (s3_lease_duration_ms=3000)...");
    println!("  (Heartbeats succeed → `continue` fires → lease.bin NEVER renewed)");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Verify the S3 lease is actually stale before proceeding.
    {
        use celeriant_wire::disk::versioned_block::deserialise_lease;
        let lease_bytes = minio.get_object("cluster/lease.json").await?;
        let lease = deserialise_lease(&lease_bytes)
            .map_err(|e| format!("Failed to deserialise lease: {:?}", e))?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let stale_by_ms = now_ms.saturating_sub(lease.expires_at_ms);
        println!(
            "  S3 lease: epoch={}, expires_at_ms={}, now_ms={}, stale_by={}ms",
            lease.lease_epoch, lease.expires_at_ms, now_ms, stale_by_ms
        );
        if stale_by_ms == 0 {
            println!("  WARNING: S3 lease is NOT stale — bug may already be fixed!");
            println!("  (expires_at_ms is in the future; the Ack path renewed it)");
            return Ok(());
        }
        println!("  S3 lease IS stale by {}ms — bug confirmed, proceeding...", stale_by_ms);
    }

    // ── Phase 3a: block cs1→MinIO, then write seq=4 via TCP only ───────
    // With cs1→MinIO blocked, the write to seq=4 goes via TCP replication to cs2.
    // cs1 self-acks seq=4@epoch=1 after cs2 acks.
    // seq=4 NEVER reaches S3 (cs1→MinIO is blocked, and cs2 replication fallback
    // would also use cs1's proxied path which is blocked).
    // This ensures S3 only has seq=3 when cs2 restarts.
    println!("\nPhase 3a: Blocking cs1→MinIO, then writing seq=4 via TCP to cs2...");
    proxy_minio_cs1.block();
    println!("  cs1→MinIO BLOCKED");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write seq=4. Since cs1→MinIO is blocked, this can only succeed via TCP
    // replication (cs2 is still running as follower at this point).
    write_event(&mut cs1_client, &aggregate_key, 4, false).await?;
    println!("  seq=4 acked by cs1 (via TCP replication to cs2; S3 was blocked)");
    println!("  cs1 now has self_acked=4@epoch=1; S3 still only has seq=3");

    // Small pause to let the replication sync settle.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Phase 3b: restart cs2 ─────────────────────────────────────────────
    // cs2 restarts, reads stale lease (only seq=3 in S3) → ChallengeViaCAS
    // → epoch=2 → truncates its local WAL to seq=3 (S3 only has seq=3)
    // → writes its own seq=4@epoch=2 → self-acks it.
    println!("\nPhase 3b: Killing+restarting cs2 (reads stale lease → epoch=2)...");

    // Kill cs2.
    println!("  Stopping cs2...");
    cs2.stop();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Restart cs2 — boots, reads stale lease (3s duration, stale by >8s), CAS-bumps to epoch=2.
    // cs2 truncates to what's in S3 (seq=3; seq=4 was TCP-only), then writes its own
    // seq=4 at epoch=2 and self-acks it.
    println!("  Restarting cs2 (reads stale lease → ChallengeViaCAS → epoch=2)...");
    cs2.restart().await?;
    println!("  cs2 restarted");

    // ── Phase 4: write seq=4 to cs2 (new leader at epoch=2) ────────────
    // cs2 is now leader at epoch=2. It will write its own seq=4 (after truncating
    // its WAL to seq=3 since S3 only has seq=3). Both cs1 and cs2 now have
    // self_acked=4 with DIFFERENT content (cs1: epoch=1; cs2: epoch=2).
    println!("\nPhase 4: Writing seq=4 to cs2 (new leader at epoch=2)...");
    let cs2_addr = cs2.address().to_string();
    let agg_key_clone2 = aggregate_key.clone();
    let cs2_write_handle = tokio::spawn(async move {
        for _ in 0..10u32 {
            let ok = match CeleriantClient::connect(&cs2_addr).await {
                Ok(mut client) => write_event_direct(&mut client, &agg_key_clone2, 4).await.is_ok(),
                Err(_) => false,
            };
            if ok { return true; }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        false
    });

    // Wait for cs2 to become leader and write. cs1→MinIO stays blocked.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let cs2_write_acked = cs2_write_handle.await.map_err(|e| format!("join: {e}"))?;
    println!("  cs2 write seq=4 acked: {}", cs2_write_acked);

    // Stop BEFORE unblocking: if cs1 gets MinIO back, it discovers the CAS
    // conflict and self-fences, potentially truncating and re-applying cs2's
    // chain, which would erase the divergence we want to capture on disk.
    println!("  Stopping nodes BEFORE unblocking MinIO to preserve divergent state...");
    let cs1_write_acked = true; // We wrote seq=4 to cs1 above via TCP; it acked.

    // ── Phase 5: stop nodes and inspect WALs ─────────────────────────────
    println!("\nPhase 5: Stopping nodes and inspecting WALs...");
    let cs1_data_root = cs1.config().data_root.clone();
    let cs2_data_root = cs2.config().data_root.clone();

    let mut cs1 = cs1;
    cs1.stop();
    cs2.stop();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let cs1_wal = find_active_wal(&cs1_data_root, 0)?;
    let cs2_wal = find_active_wal(&cs2_data_root, 0)?;
    println!("  cs1 WAL: {:?}", cs1_wal);
    println!("  cs2 WAL: {:?}", cs2_wal);

    let cs1_header = inspect_wal_header(&cs1_wal)?;
    let cs2_header = inspect_wal_header(&cs2_wal)?;

    println!("\ncs1 WAL header:");
    for line in &cs1_header { println!("  {line}"); }
    println!("\ncs2 WAL header:");
    for line in &cs2_header { println!("  {line}"); }

    let cs1_self_acked = parse_last_self_acked(&cs1_header);
    let cs2_self_acked = parse_last_self_acked(&cs2_header);
    let cs1_write_seq = parse_write_wal_seq(&cs1_header);
    let cs2_write_seq = parse_write_wal_seq(&cs2_header);

    println!(
        "\ncs1: write_wal_seq={}, last_self_acked={}",
        cs1_write_seq, cs1_self_acked
    );
    println!(
        "cs2: write_wal_seq={}, last_self_acked={}",
        cs2_write_seq, cs2_self_acked
    );

    if cs1_self_acked == 0 {
        println!("\n--- NOT REPRODUCED ---");
        println!("  cs1 has no self-acked writes (cs1_self_acked={})", cs1_self_acked);
        println!("  Timing suggestion: try shortening s3_lease_duration_ms further or");
        println!("  increasing the stale-wait period.");
        return Ok(());
    }

    // Check the overlap range: both must have self-acked the same wal_seq.
    let overlap = cs1_self_acked.min(cs2_self_acked);
    println!(
        "\nBoth nodes self-acked. Checking metablock at wal_seq={}...",
        overlap
    );

    let cs1_mb = inspect_wal_seq(&cs1_wal, overlap)?;
    let cs2_mb = inspect_wal_seq(&cs2_wal, overlap)?;

    println!("\ncs1 metablock at wal_seq={}:", overlap);
    for line in &cs1_mb { println!("  {line}"); }
    println!("\ncs2 metablock at wal_seq={}:", overlap);
    for line in &cs2_mb { println!("  {line}"); }

    let cs1_lease = parse_field_from_lines(&cs1_mb, "lease");
    let cs2_lease = parse_field_from_lines(&cs2_mb, "lease");
    let cs1_node = parse_field_from_lines(&cs1_mb, "node");
    let cs2_node = parse_field_from_lines(&cs2_mb, "node");
    let cs1_prev = parse_field_from_lines(&cs1_mb, "previous_tip_hash");
    let cs2_prev = parse_field_from_lines(&cs2_mb, "previous_tip_hash");

    println!("\nAt wal_seq={}:", overlap);
    println!(
        "  cs1: lease_epoch={}, node={}, prev_hash={}",
        cs1_lease, cs1_node, &cs1_prev[..cs1_prev.len().min(16)]
    );
    println!(
        "  cs2: lease_epoch={}, node={}, prev_hash={}",
        cs2_lease, cs2_node, &cs2_prev[..cs2_prev.len().min(16)]
    );

    let lease_diverges = !cs1_lease.is_empty() && !cs2_lease.is_empty() && cs1_lease != cs2_lease;
    let node_diverges = !cs1_node.is_empty() && !cs2_node.is_empty() && cs1_node != cs2_node;
    let hash_diverges = !cs1_prev.is_empty() && !cs2_prev.is_empty() && cs1_prev != cs2_prev;

    if lease_diverges || node_diverges || hash_diverges {
        println!("\n!!! STALE-LEASE RESTART SPLIT-BRAIN CONFIRMED (same wal_seq diverges) !!!");
        if lease_diverges {
            println!("  lease_epoch: cs1={} vs cs2={}", cs1_lease, cs2_lease);
        }
        if node_diverges {
            println!("  node_id: cs1={} vs cs2={}", cs1_node, cs2_node);
        }
        if hash_diverges {
            println!("  prev_tip_hash: cs1={} vs cs2={}", cs1_prev, cs2_prev);
        }
        println!("\nRoot cause: S3 lease.bin was never renewed during heartbeat-Ack path");
        println!("  (shard.rs:833 `continue` skips the S3 renewal block below it).");
        return Err(format!(
            "STALE-LEASE SPLIT-BRAIN: both nodes self-acked wal_seq={} \
             (cs1 epoch={} node={}) vs (cs2 epoch={} node={}) — content diverges",
            overlap, cs1_lease, cs1_node, cs2_lease, cs2_node
        )
        .into());
    }

    // Even if same wal_seq content is identical, check for divergent self_acked ranges:
    // if cs2 self-acked at higher epoch on entries that cs1 never received, that's
    // also evidence of split brain (two leaders both accepted client writes concurrently).
    if cs2_self_acked > cs1_self_acked && cs2_self_acked > 0 {
        // Inspect cs2's highest self-acked entry to see its epoch.
        let cs2_top_mb = inspect_wal_seq(&cs2_wal, cs2_self_acked)?;
        let cs2_top_lease = parse_field_from_lines(&cs2_top_mb, "lease");
        let cs2_top_lease_epoch: u64 = cs2_top_lease.parse().unwrap_or(0);

        // cs1's highest self-acked entry epoch.
        let cs1_top_mb = inspect_wal_seq(&cs1_wal, cs1_self_acked)?;
        let cs1_top_lease = parse_field_from_lines(&cs1_top_mb, "lease");
        let cs1_top_lease_epoch: u64 = cs1_top_lease.parse().unwrap_or(0);

        println!("\ncs2 top (wal_seq={}): lease_epoch={}", cs2_self_acked, cs2_top_lease);
        println!("cs1 top (wal_seq={}): lease_epoch={}", cs1_self_acked, cs1_top_lease);

        if cs2_top_lease_epoch > cs1_top_lease_epoch && cs1_write_acked {
            println!("\n!!! STALE-LEASE SPLIT-BRAIN CONFIRMED (divergent chains) !!!");
            println!("  cs1 self-acked up to wal_seq={} at epoch={}", cs1_self_acked, cs1_top_lease_epoch);
            println!("  cs2 self-acked up to wal_seq={} at epoch={}", cs2_self_acked, cs2_top_lease_epoch);
            println!("  Both were operating as leader simultaneously:");
            println!("  cs1 at epoch={} (stale local TTL from last HB ack)", cs1_top_lease_epoch);
            println!("  cs2 at epoch={} (CAS-stole stale S3 lease on restart)", cs2_top_lease_epoch);
            println!("\nRoot cause: S3 lease.bin was never renewed during heartbeat-Ack path");
            println!("  (shard.rs:833 `continue` skips the S3 renewal block below it).");
            return Err(format!(
                "STALE-LEASE SPLIT-BRAIN: cs1 self-acked seq={} at epoch={}, \
                 cs2 self-acked seq={} at epoch={} — two leaders operated simultaneously; \
                 client writes acknowledged by both; WAL chains diverge",
                cs1_self_acked, cs1_top_lease_epoch, cs2_self_acked, cs2_top_lease_epoch
            )
            .into());
        }
    }

    println!("\nContent at wal_seq={} is identical on both nodes.", overlap);
    println!(
        "\nNOT REPRODUCED (chains converged before stop or no divergence at same wal_seq)."
    );
    println!("  cs1_self_acked={}, cs2_self_acked={}", cs1_self_acked, cs2_self_acked);
    println!("  cs2_write_acked={}", cs2_write_acked);
    println!("  Possible causes:");
    println!("  - cs2 caught up from S3 and applied cs1's data (no truncation happened)");
    println!("  - cs1's write at seq=4 got to S3 before cs2 restarted");

    Ok(())
}

/// Write event at wal_seq=4 without allow_create or expected_version constraints.
async fn write_event_direct(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
    event_num: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = DatablockAggregateEvent {
        client_seq: event_num,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(format!("{{\"event\":{}}}", event_num).into_bytes()),
        iv: None,
    };

    let mut writes = std::collections::HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: false,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(event_num as u128),
        client_id: 999,
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&ClientRequest::Write(write_req))
        .await?;

    match response {
        ClientResponse::Write(_) => Ok(()),
        other => Err(format!("Write failed: {:?}", other).into()),
    }
}

fn wal_inspect_binary() -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push("celeriant-wal-inspect");
    if path.exists() {
        return path;
    }
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/release/celeriant-wal-inspect");
    if workspace.exists() {
        return workspace;
    }
    panic!(
        "celeriant-wal-inspect not found; build: cargo build --release -p celeriant_wal_inspect"
    );
}

fn find_active_wal(
    data_root: &std::path::Path,
    shard_id: u32,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let shard_dir = data_root.join(format!("shard_{}", shard_id));
    let mut segments: Vec<(u64, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(&shard_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(id_str) = name_str
            .strip_prefix("log_")
            .and_then(|s| s.strip_suffix(".wal"))
        {
            if let Ok(id) = id_str.parse::<u64>() {
                segments.push((id, entry.path()));
            }
        }
    }
    if segments.is_empty() {
        return Err(format!("No .wal files in {:?}", shard_dir).into());
    }
    let (_, path) = segments.into_iter().max_by_key(|(id, _)| *id).unwrap();
    Ok(path)
}

fn run_wal_inspect(
    wal_path: &std::path::Path,
    args: &[&str],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let bin = wal_inspect_binary();
    let output = Command::new(&bin)
        .arg(wal_path)
        .args(args)
        .output()
        .map_err(|e| format!("celeriant-wal-inspect failed: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.is_empty() {
        println!("  [wal-inspect stderr] {}", stderr.trim());
    }

    Ok(stdout.lines().map(|l| l.to_string()).collect())
}

fn inspect_wal_header(
    wal_path: &std::path::Path,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    run_wal_inspect(wal_path, &["header"])
}

fn inspect_wal_seq(
    wal_path: &std::path::Path,
    wal_seq: u64,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    run_wal_inspect(wal_path, &["wal", &wal_seq.to_string()])
}

fn parse_last_self_acked(header_lines: &[String]) -> u64 {
    for line in header_lines {
        let t = line.trim();
        if t.starts_with("last_self_acked_wal_seq") {
            if let Some(v) = t.split('=').nth(1).and_then(|s| s.trim().parse::<u64>().ok()) {
                return v;
            }
        }
    }
    0
}

fn parse_write_wal_seq(header_lines: &[String]) -> u64 {
    for line in header_lines {
        let t = line.trim();
        if t.starts_with("write_wal_seq") {
            if let Some(v) = t.split('=').nth(1).and_then(|s| s.trim().parse::<u64>().ok()) {
                return v;
            }
        }
    }
    0
}

fn parse_field_from_lines(lines: &[String], field: &str) -> String {
    for line in lines {
        for segment in line.split('|') {
            let s = segment.trim();
            if let Some(eq_pos) = s.find('=') {
                let key = s[..eq_pos].trim();
                if key == field {
                    return s[eq_pos + 1..].trim().to_string();
                }
            }
        }
    }
    String::new()
}
