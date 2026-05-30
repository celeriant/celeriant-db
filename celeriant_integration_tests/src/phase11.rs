//! Phase 11 — distributed safety: leader self-fence on a lost lease, the
//! NotLeader redirect a follower returns, replication backpressure, and
//! compaction / physical reclamation.
//!
//! Oracle:
//! - operations/leader-election-s3.md: "A leader that cannot renew its lease
//!   fences itself and stops accepting writes, so you never get two writers. The
//!   lease, not the network, is the source of truth for who may write."
//! - operations/two-node-cluster.md + clients/rust.md + reference/error-codes.md
//!   (2011 NotLeader): a follower refuses a write and the rejection carries the
//!   leader's advertised address so the client can find the real leader
//!   ("The advertised addresses are ... what the leader returns in a NotLeader
//!   redirect"). The tokio client surfaces it as `ClientError::NotLeader {
//!   leader_address: Some(addr) }`.
//! - reference/error-codes.md 2012 WriteReplicationBackpressure + operations/
//!   troubleshooting.md + monitoring.md: when the follower cannot keep up the
//!   leader sheds load; the client is told to back off (the tokio client maps
//!   2012 to `ClientError::ServerBusy`), corroborated by the
//!   `celeriant_replication_follower_pressured` metric.
//! - concepts/retention-and-deletion.md: trim/delete are logical; the bytes are
//!   reclaimed later by background compaction gated by
//!   `--compaction-check-interval-secs` and a minimum reclaimable ratio.
//!
//! Every wait is bounded and FAILS on timeout. Distributed/timing tests are
//! slower and flakier than the happy path.

use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use std::sync::Arc;
use crate::{
    is_leader, poll_event_count, s3_cluster_config, verify_compacted_segment_sizes, MinioContainer,
    ServerConfig, TcpProxy, TestServer,
};

use crate::common::{event, port_for, R};

const TYPE: u64 = 110;

fn run_subfolder(test: &str) -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("p11-{test}-{}-{nonce}", std::process::id())
}

/// Find which address currently accepts writes (the leader), polling up to
/// `timeout`. Folds the same 12s replication-link settle phases 6/8 use so the
/// first write replicates over the live link instead of racing the S3 fallback.
async fn find_leader(addrs: &[&str], timeout: Duration) -> Option<String> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for a in addrs {
            if is_leader(a).await.unwrap_or(false) {
                tokio::time::sleep(Duration::from_secs(12)).await;
                return Some(a.to_string());
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    None
}

/// Write client_seq i..=hi to `key` on `addr`, conditional on each prior version.
async fn write_seq(addr: &str, key: &AggregateKey, lo: u64, hi: u64) -> Result<(), Box<dyn std::error::Error>> {
    let mut c = CeleriantClient::connect(addr).await?;
    for i in lo..=hi {
        c.write_events_with(
            key.clone(),
            vec![event(i, TYPE, 1000 + i, &format!("{{\"n\":{i}}}"))],
            WriteEventsOptions { allow_create: i == 1, expected_version: Some(i - 1), ..Default::default() },
        )
        .await
        .map_err(|e| format!("write seq {i}: {e:?}"))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 11.1 — Leader self-fences when it can no longer renew its S3 lease.
// ---------------------------------------------------------------------------

/// A leader that can no longer renew its S3 lease must fence itself — stop
/// accepting writes — so two writers never coexist. This is the split-brain
/// prevention property: "you never get two writers."
///
/// To engineer "cannot renew the lease" cleanly we isolate the leader from BOTH
/// the object store and the follower, so it cannot lean on the live replication
/// link as a substitute for the lease (an early version that cut S3 only — while
/// the follower link stayed up — observed the leader keep serving for 50s+ via
/// the link; see FINDINGS F8). Each node gets its OWN S3 proxy and the
/// leader↔follower link is fronted by a proxy too. We block the leader's S3
/// proxy and the inter-node link, leaving the FOLLOWER's S3 reachable. Then:
///   1. the old leader can no longer renew its lease and must fence (writes to it
///      become rejected and stay rejected), and
///   2. the follower, which can still reach S3, takes the lease and becomes the
///      sole writer.
/// We assert both within bounded waits. (leader-election-s3.md: "A leader that
/// cannot renew its lease fences itself and stops accepting writes, so you never
/// get two writers. The lease, not the network, is the source of truth for who
/// may write.")
pub async fn leader_self_fences_on_lost_lease() -> R {
    let pa = port_for("p11_fence_a");
    let pb = port_for("p11_fence_b");
    let pm = port_for("p11_fence_minio");
    let sub = run_subfolder("fence");

    let _minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    // Per-node S3 proxies (both forward to MinIO) so we can cut S3 for ONE node.
    let s3_proxy_a = TcpProxy::start(pa + 6, format!("127.0.0.1:{pm}")).await?;
    let s3_proxy_b = TcpProxy::start(pb + 6, format!("127.0.0.1:{pm}")).await?;
    // Per-node replication proxies so we can cut the inter-node link.
    let repl_proxy_a = TcpProxy::start(pa + 5, format!("127.0.0.1:{}", pa + 1)).await?;
    let repl_proxy_b = TcpProxy::start(pb + 5, format!("127.0.0.1:{}", pb + 1)).await?;

    let mut cfg_a = s3_cluster_config(1, "us-east-1", "celeriant-test", "minioadmin", "minioadmin", &format!("http://{}", s3_proxy_a.address()), true);
    cfg_a.s3_subfolder = Some(sub.clone());
    cfg_a.advertised_replication_address = Some(repl_proxy_a.address());
    let mut cfg_b = s3_cluster_config(1, "us-east-1", "celeriant-test", "minioadmin", "minioadmin", &format!("http://{}", s3_proxy_b.address()), true);
    cfg_b.s3_subfolder = Some(sub.clone());
    cfg_b.advertised_replication_address = Some(repl_proxy_b.address());

    let a = TestServer::start_with_config_labeled(pa, cfg_a, "node-a".into()).await?;
    let b = TestServer::start_with_config_labeled(pb, cfg_b, "node-b".into()).await?;

    let leader = find_leader(&[a.address(), b.address()], Duration::from_secs(40)).await
        .ok_or("no leader elected within 40s")?;
    let leader_is_a = leader == a.address();
    let follower = if leader_is_a { b.address().to_string() } else { a.address().to_string() };

    let key = AggregateKey::new(1, 1, 11001);
    // Confirm the leader is accepting writes before we isolate it.
    write_seq(&leader, &key, 1, 2).await?;

    // Isolate the OLD leader: cut its S3 (lease renewal impossible) AND the
    // inter-node link (cannot use the heartbeat link as a crutch). The follower's
    // S3 stays reachable so it can take the lease.
    if leader_is_a {
        s3_proxy_a.block();
        repl_proxy_b.block(); // leader-a -> follower-b path
        repl_proxy_a.block(); // follower-b -> leader-a path
    } else {
        s3_proxy_b.block();
        repl_proxy_a.block();
        repl_proxy_b.block();
    }

    // (1) The old leader must fence: writes to it become rejected and stay
    // rejected. Bound: lease TTL (10s) + drift + slack. FAIL on timeout.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut next_seq = 3u64;
    let mut fenced = false;
    let mut last = String::from("(no probe yet)");
    while std::time::Instant::now() < deadline {
        match write_seq(&leader, &key, next_seq, next_seq).await {
            Ok(()) => { next_seq += 1; last = format!("still accepting at seq {}", next_seq - 1); }
            Err(e) => { fenced = true; last = format!("rejected at seq {next_seq}: {e}"); break; }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if !fenced {
        return Err(format!(
            "old leader never fenced after losing its lease within 60s (it must stop accepting writes): {last}"
        ).into());
    }

    // Fence holds: a second probe is also rejected (not a momentary blip).
    let mut still_fenced = false;
    let recheck = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < recheck {
        if write_seq(&leader, &key, next_seq, next_seq).await.is_err() { still_fenced = true; break; }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if !still_fenced {
        return Err("old leader resumed accepting writes while its lease was un-renewable (fence must hold)".into());
    }

    // (2) The follower (still able to reach S3) must take over and become the
    // sole writer — proves the lease moved and there is exactly one writer.
    let new_key = AggregateKey::new(1, 1, 11005);
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut promoted = false;
    let mut last = String::from("(no probe)");
    while std::time::Instant::now() < deadline {
        match CeleriantClient::connect(&follower).await {
            Ok(mut c) => match c.write_events_with(new_key.clone(), vec![event(1, TYPE, 9000, "{}")],
                WriteEventsOptions { allow_create: true, ..Default::default() }).await {
                Ok(_) => { promoted = true; break; }
                Err(e) => last = format!("{e:?}"),
            },
            Err(e) => last = format!("connect: {e:?}"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    if !promoted {
        return Err(format!("follower never took over after the leader fenced: {last}").into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 11.2 — The NotLeader redirect a follower returns carries the leader address.
// ---------------------------------------------------------------------------

/// When a follower refuses a write it returns NotLeader (2011) carrying the
/// leader's address, so a raw client can find the real leader. The tokio client
/// surfaces this as `ClientError::NotLeader { leader_address: Some(addr) }`.
/// We assert the redirect is present AND that the address it points at is the
/// actual leader (a write to that address is accepted). (two-node-cluster.md:
/// the advertised address is "what the leader returns in a NotLeader redirect";
/// error-codes 2011; clients/rust.md NotLeader.)
pub async fn notleader_redirect_carries_leader_address() -> R {
    let pa = port_for("p11_nl_a");
    let pb = port_for("p11_nl_b");
    let pm = port_for("p11_nl_minio");
    let sub = run_subfolder("notleader");

    let minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    let endpoint = minio.endpoint();
    let mk = |repl_port: u16| {
        let mut cfg = s3_cluster_config(1, "us-east-1", "celeriant-test", "minioadmin", "minioadmin", &endpoint, true);
        cfg.s3_subfolder = Some(sub.clone());
        cfg.advertised_replication_address = Some(format!("127.0.0.1:{repl_port}"));
        cfg
    };
    let a = TestServer::start_with_config_labeled(pa, mk(pa + 1), "node-a".into()).await?;
    let b = TestServer::start_with_config_labeled(pb, mk(pb + 1), "node-b".into()).await?;

    let leader = find_leader(&[a.address(), b.address()], Duration::from_secs(40)).await
        .ok_or("no leader elected within 40s")?;
    let follower = if leader == a.address() { b.address().to_string() } else { a.address().to_string() };

    // A raw write straight to the follower must be refused with NotLeader, and
    // the rejection must carry a leader address. Poll briefly: right after
    // election the follower may not yet know the leader's address.
    let key = AggregateKey::new(1, 1, 11002);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut redirect: Option<String> = None;
    let mut last = String::from("(no probe)");
    while std::time::Instant::now() < deadline {
        let mut fc = CeleriantClient::connect(&follower).await?;
        let res = fc
            .write_events_with(key.clone(), vec![event(1, TYPE, 2000, "{}")],
                WriteEventsOptions { allow_create: true, ..Default::default() })
            .await;
        match res {
            Err(ClientError::NotLeader { leader_address: Some(addr), .. }) => {
                redirect = Some(addr);
                break;
            }
            Err(ClientError::NotLeader { leader_address: None, .. }) => {
                last = "follower returned NotLeader but no leader address yet".into();
            }
            Ok(_) => return Err("write to the follower was ACCEPTED (a follower must refuse writes)".into()),
            Err(e) => last = format!("unexpected error from follower write: {e:?}"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let redirect = redirect.ok_or_else(|| format!("follower never returned a NotLeader redirect with a leader address: {last}"))?;

    // The redirect must point at the ACTUAL leader: connecting there and writing
    // succeeds (and that address is the one find_leader identified as leader).
    // Normalise host (advertised may be 127.0.0.1/localhost): compare ports and
    // confirm by behavior (the redirect target accepts the write).
    let mut lc = CeleriantClient::connect(&redirect).await
        .map_err(|e| format!("could not connect to the redirect target {redirect}: {e}"))?;
    lc.write_events_with(key.clone(), vec![event(1, TYPE, 2000, "{}")],
        WriteEventsOptions { allow_create: true, ..Default::default() })
        .await
        .map_err(|e| format!("redirect target {redirect} did not accept the write (it is not the real leader): {e:?}"))?;

    // And the redirect must NOT point at the follower we just got refused by.
    let follower_port = follower.rsplit(':').next().unwrap_or("");
    let redirect_port = redirect.rsplit(':').next().unwrap_or("");
    if redirect_port == follower_port {
        return Err(format!("NotLeader redirect points back at the follower ({redirect}), not the leader").into());
    }
    // Sanity: the redirect port equals the leader's port.
    let leader_port = leader.rsplit(':').next().unwrap_or("");
    if redirect_port != leader_port {
        return Err(format!("redirect {redirect} port != actual leader {leader} port").into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 11.3 — Throttled replication link: acked writes stay durable & gap-free.
//        (Backpressure 2012 could NOT be forced reliably — see FINDINGS F9.)
// ---------------------------------------------------------------------------

/// Documented replication backpressure (error 2012 `WriteReplicationBackpressure`,
/// surfaced by the tokio client as `ClientError::ServerBusy`) could NOT be forced
/// deterministically in this harness — see FINDINGS F9 for everything tried
/// (heavy throttle + volume, 32 concurrent writers + a mid-flight link block) and
/// why it is hard: the knobs that govern shedding (`--replication-rollback-
/// cooldown-us`, `--s3-replication-delay-us`, `--s3-max-concurrent-fallback-
/// uploads`, `--heartbeat-starve-threshold-ms`) are not emitted by the harness's
/// `to_cli_args`, and with the default S3 fallback the leader absorbs a slow
/// follower rather than rejecting writes. Shipping a test that asserts 2012 fires
/// would be flaky, so we do NOT.
///
/// What this test DOES assert is the always-true safety contract on the same
/// degraded path: under a throttled leader→follower link, every write the leader
/// ACKNOWLEDGED is durable and the stream is gap-free — the system never silently
/// drops or corrupts an acked write while replication lags. (durability-and-
/// safety.md: an acknowledged write is durable; reads-and-ordering.md: gap-free.)
/// If `ServerBusy` IS observed it is counted and logged (bonus), but the pass
/// condition is the durability/gap-free check, which is meaningful regardless.
pub async fn throttled_link_preserves_acked_writes() -> R {
    let pa = port_for("p11_bp_a");
    let pb = port_for("p11_bp_b");
    let pm = port_for("p11_bp_minio");
    let sub = run_subfolder("backpressure");

    let minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    let endpoint = minio.endpoint();

    // Throttle the follower's replication link heavily so it cannot keep up.
    let proxy_a = TcpProxy::start(pa + 5, format!("127.0.0.1:{}", pa + 1)).await?;
    let proxy_b = TcpProxy::start(pb + 5, format!("127.0.0.1:{}", pb + 1)).await?;

    let mut cfg_a = s3_cluster_config(1, "us-east-1", "celeriant-test", "minioadmin", "minioadmin", &endpoint, true);
    cfg_a.s3_subfolder = Some(sub.clone());
    cfg_a.advertised_replication_address = Some(proxy_a.address());
    let mut cfg_b = s3_cluster_config(1, "us-east-1", "celeriant-test", "minioadmin", "minioadmin", &endpoint, true);
    cfg_b.s3_subfolder = Some(sub.clone());
    cfg_b.advertised_replication_address = Some(proxy_b.address());

    let a = TestServer::start_with_config_labeled(pa, cfg_a, "node-a".into()).await?;
    let b = TestServer::start_with_config_labeled(pb, cfg_b, "node-b".into()).await?;

    let leader = find_leader(&[a.address(), b.address()], Duration::from_secs(40)).await
        .ok_or("no leader elected within 40s")?;
    let follower_proxy = if leader == a.address() { &proxy_b } else { &proxy_a };

    let key = AggregateKey::new(1, 1, 11003);
    // Establish the aggregate on the healthy link first.
    write_seq(&leader, &key, 1, 1).await?;

    // Now throttle the follower link to a crawl and pour in volume with large
    // payloads so the queue backs up.
    follower_proxy.throttle(200); // 200ms per 8KB chunk
    let big = vec![b'x'; 64 * 1024]; // 64 KiB payload per event
    let mut c = CeleriantClient::connect(&leader).await?;

    let mut next = 2u64;
    let mut acked = 0u64;
    let mut server_busy = 0u64;
    let mut other_rejects = 0u64;
    let total = 200u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    for _ in 0..total {
        if std::time::Instant::now() >= deadline {
            break;
        }
        let ev = DatablockAggregateEvent {
            client_seq: next,
            event_seq: 0,
            event_id: None,
            event_timestamp: 1000 + next,
            event_type_major: TYPE,
            event_type_minor: 0,
            event_value: Arc::new(big.clone()),
            iv: None,
        };
        // Unconditional append (no expected_version) so we can keep firing even
        // if some are shed; idempotency off.
        match c.write_events_with(key.clone(), vec![ev],
            WriteEventsOptions { allow_create: false, ..Default::default() }).await {
            Ok(_) => { acked += 1; next += 1; }
            Err(ClientError::ServerBusy) => { server_busy += 1; }
            // A throttled link can also surface as a replication error / timeout;
            // those are legitimate rejections (the write was NOT acked), not
            // silent loss. Count them but do not advance `next`.
            Err(_) => { other_rejects += 1; }
        }
    }
    follower_proxy.unthrottle();
    println!(
        "  backpressure probe: acked={acked} server_busy(2012)={server_busy} other_rejects={other_rejects}"
    );

    // Whatever the mix, the safety contract is: every write the leader ACKED is
    // durable and the stream is gap-free. Heal the link, let the follower catch
    // up, and verify the leader holds exactly the acked count (1 initial + acked
    // big events), gap-free.
    let expected = 1 + acked as usize;
    let n = poll_event_count(&leader, &key, expected, Duration::from_secs(60)).await;
    if n != expected {
        return Err(format!(
            "after backpressure: leader has {n} events, expected {expected} acked — an acked write was lost or duplicated"
        ).into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 11.4 — Compaction physically reclaims space after a trim.
// ---------------------------------------------------------------------------

/// Trim is logical; the bytes are reclaimed later by background compaction gated
/// by `--compaction-check-interval-secs` and a minimum reclaimable ratio. With a
/// short check interval and a low reclaimable ratio, after trimming a large
/// prefix the on-disk segment must physically shrink. We make this observable by
/// using a SMALL WAL preallocation so multiple segments seal quickly, writing
/// enough large events to fill and seal several, trimming away nearly all of
/// them, and then waiting (bounded) for compaction to rewrite a sealed segment to
/// a smaller size. (retention-and-deletion.md: "the original bytes stay on disk
/// until compaction rewrites the segment and drops them ... gated by
/// --compaction-check-interval-secs ... and a minimum reclaimable ratio".)
///
/// The harness `verify_compacted_segment_sizes` reads only the SIZES of the
/// test's own `shard_*/log_*.wal` files under its temp data dir — it does not
/// inspect server internals or decode any file. If compaction never produced an
/// observably smaller sealed segment within the bound, the test FAILS (it does
/// not pass vacuously).
pub async fn compaction_reclaims_after_trim() -> R {
    // Small preallocation so segments seal after a modest amount of data; short
    // compaction interval; low reclaimable ratio so a heavily-trimmed segment
    // qualifies. Standalone single-node keeps it deterministic.
    let preallocate: u64 = 4 * 1024 * 1024; // 4 MiB segments
    let cfg = ServerConfig {
        standalone: true,
        num_shards: Some(1),
        log_level: "warn".to_string(),
        shard_log_preallocate_bytes: preallocate,
        compaction_check_interval_secs: 2,
        compaction_min_reclaimable_ratio: 0.05,
        ..Default::default()
    };
    let port = port_for("p11_compaction");
    let server = TestServer::start_with_config_labeled(port, cfg, "compactor".into()).await?;
    let data_root = server.config().data_root.clone();

    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = AggregateKey::new(1, 1, 11004);

    // Write enough large events to fill and seal multiple 4 MiB segments.
    // Payloads MUST be incompressible: the WAL is zstd-compressed, so repetitive
    // bytes squash into a single preallocated segment and nothing ever seals
    // (verified empirically). Random bytes force the log to roll, so there are
    // sealed segments for compaction to rewrite. 300 x 64 KiB random ≈ 19 MiB ->
    // several sealed 4 MiB segments.
    let count = 300u64;
    let mut seed: u64 = 0x1234_5678_9abc_def0;
    let mut rnd = |n: usize| -> Vec<u8> {
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            v.push((seed & 0xff) as u8);
        }
        v
    };
    for i in 1..=count {
        let ev = DatablockAggregateEvent {
            client_seq: i,
            event_seq: 0,
            event_id: None,
            event_timestamp: 1000 + i,
            event_type_major: TYPE,
            event_type_minor: 0,
            event_value: Arc::new(rnd(64 * 1024)),
            iv: None,
        };
        c.write_events_with(key.clone(), vec![ev],
            WriteEventsOptions { allow_create: i == 1, expected_version: Some(i - 1), ..Default::default() })
            .await
            .map_err(|e| format!("compaction setup write {i}: {e:?}"))?;
    }

    // Trim away almost the whole stream, keeping only the last event. This makes
    // the early sealed segments overwhelmingly reclaimable, well past the 5%
    // ratio, so compaction should rewrite at least one to a smaller size.
    use celeriant_msg::request::requests::TrimStartRequest;
    c.trim_start(TrimStartRequest {
        correlation_id: None,
        aggregate_key: key.clone(),
        keep_from_aggregate_version: count, // keep only the final event
        client_id: 1,
        user_id: None,
    })
    .await
    .map_err(|e| format!("trim failed: {e:?}"))?;

    // Wait (bounded) for background compaction to physically shrink a sealed
    // segment. `verify_compacted_segment_sizes` panics on a failed check, so we
    // poll with the lightweight, non-panicking `any_sealed_shrank` first and only
    // call the full verifier once we see a shrunk segment (it then re-asserts the
    // active-segment invariant). FAIL on timeout — never pass vacuously.
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        if any_sealed_shrank(&data_root, preallocate)? {
            // Confirm the full invariant (active segment exactly preallocate; at
            // least one sealed segment compacted) via the harness verifier.
            return verify_compacted_segment_sizes(&data_root, "compactor", preallocate);
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "compaction did not physically reclaim a sealed segment within 90s after trim \
                 (no sealed log_*.wal under {data_root:?} shrank below {preallocate} bytes)"
            ).into());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Non-panicking probe: does any SEALED segment (not the highest-id active one)
/// have a file size below `preallocate`? Reads only file sizes of the test's own
/// `shard_*/log_*.wal` files; no server internals are inspected or decoded.
fn any_sealed_shrank(data_root: &std::path::Path, preallocate: u64) -> Result<bool, Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(data_root)? {
        let entry = entry?;
        if !entry.file_name().to_string_lossy().starts_with("shard_") || !entry.file_type()?.is_dir() {
            continue;
        }
        let mut segments: Vec<(u64, std::path::PathBuf)> = Vec::new();
        for file in std::fs::read_dir(entry.path())? {
            let file = file?;
            let name = file.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = name.strip_prefix("log_").and_then(|s| s.strip_suffix(".wal")) {
                if let Ok(id) = id.parse::<u64>() {
                    segments.push((id, file.path()));
                }
            }
        }
        if segments.len() < 2 {
            continue; // need at least one sealed + the active segment
        }
        let active = segments.iter().map(|(id, _)| *id).max().unwrap();
        for (id, path) in &segments {
            if *id == active {
                continue;
            }
            if std::fs::metadata(path)?.len() < preallocate {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
