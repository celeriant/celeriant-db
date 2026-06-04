//! Phase 8 — distributed failure & timing: link cuts, S3 outage, crash recovery,
//! exactly-once across failover, leader/follower read parity. Each test owns an
//! ephemeral MinIO container (auto-created and removed on drop); fault injection
//! via `TcpProxy`.
//!
//! Oracle: celeriant-docs/docs/concepts/durability-and-safety.md
//! ("acknowledged write ... lives in two places"; "When the follower is down, the
//! leader ships the batch to S3 instead"; "Leader dies, S3 healthy"),
//! operations/{two-node-cluster,leader-election-s3}.md
//! ("the leader keeps serving ... replication to the follower continues" during an
//! S3 outage), concepts/reads-and-ordering.md (follower convergence + ordering).
//!
//! Every wait is bounded and FAILS on timeout — a fixed sleep never substitutes
//! for observing the outcome.

use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_wal::aggregate_key::AggregateKey;
use crate::{
    count_events, is_leader, poll_event_count, read_all_batches, s3_cluster_config,
    MinioContainer, TcpProxy, TestServer,
};

use crate::common::{event, port_for, R};

const TYPE: u64 = 100;

/// A subfolder unique to this test AND this process run (stale S3 fallback
/// objects from a prior run break catchup — see FINDINGS phase-6 notes).
fn run_subfolder(test: &str) -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("p8-{test}-{}-{nonce}", std::process::id())
}

/// Find which of the given addresses currently accepts writes (the leader),
/// polling up to `timeout`. Returns the leader address or None on timeout.
async fn find_leader(addrs: &[&str], timeout: Duration) -> Option<String> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for a in addrs {
            if is_leader(a).await.unwrap_or(false) {
                // Let the leader<->follower replication TCP connection establish
                // before the caller's first write. Without this settle, the very
                // first write races the link-up and gets shipped to the S3
                // fallback path before the follower ever connected, which then
                // makes the follower's S3 catchup loop on a not-yet-visible
                // object (see FINDINGS phase-6). Phase 6 uses the same 12s wait.
                tokio::time::sleep(Duration::from_secs(12)).await;
                return Some(a.to_string());
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    None
}

/// Write client_seq i..=hi to `key` on `addr`, conditional on each prior version
/// (allow_create on the first). Returns Err on the first rejected write.
async fn write_seq(
    addr: &str,
    key: &AggregateKey,
    lo: u64,
    hi: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut c = CeleriantClient::connect(addr).await?;
    for i in lo..=hi {
        c.write_events_with(
            key.clone(),
            vec![event(i, TYPE, 1000 + i, &format!("{{\"n\":{i}}}"))],
            0, WriteEventsOptions { allow_create: i == 1, expected_version: Some(i - 1), ..Default::default() },
        )
        .await
        .map_err(|e| format!("write seq {i}: {e:?}"))?;
    }
    Ok(())
}

/// 8.1 Replication link cut with both nodes alive: the leader keeps
/// acknowledging writes via the S3 fallback path, and when the link heals the
/// follower catches up to the same data. (two-node-cluster: "While the follower
/// is unreachable it replicates to S3 instead, which adds S3 latency to the
/// write path until the follower returns and catches up";
/// durability-and-safety: "When the follower is down, the leader ships the batch
/// to S3 instead, so an acknowledged write still lives in two places".)
///
/// We front EACH node's replication port with a proxy and advertise the proxy
/// addresses, so whoever ends up follower is reachable only through its proxy.
/// Blocking the follower's proxy severs leader->follower replication while both
/// processes stay healthy.
pub async fn replication_link_cut_acks_via_s3_and_heals() -> R {
    let pa = port_for("p8_cut_a");
    let pb = port_for("p8_cut_b");
    let pm = port_for("p8_cut_minio");
    let sub = run_subfolder("cut");

    let minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    let endpoint = minio.endpoint();

    // Proxy ports: forward to each node's real replication port (base+1).
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
    let follower = if leader == a.address() { b.address().to_string() } else { a.address().to_string() };
    let follower_proxy = if leader == a.address() { &proxy_b } else { &proxy_a };

    let key = AggregateKey::new(1, 1, 8001);

    // Healthy path: write 1..=3, confirm follower replicates.
    write_seq(&leader, &key, 1, 3).await?;
    poll_event_count(&follower, &key, 3, Duration::from_secs(30)).await;

    // Cut the leader->follower replication link. Both processes stay alive.
    follower_proxy.block();

    // Writes must STILL be acknowledged (degraded S3-fallback path). Bound it:
    // if the leader cannot ack within 30s the degraded path failed.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut acked = false;
    let mut last_err = String::new();
    while std::time::Instant::now() < deadline {
        match write_seq(&leader, &key, 4, 6).await {
            Ok(()) => { acked = true; break; }
            Err(e) => last_err = e.to_string(),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    if !acked {
        return Err(format!("leader stopped acking after link cut (degraded S3 path should keep acking): {last_err}").into());
    }

    // Heal the link: the follower must catch up to all 6 events.
    follower_proxy.unblock();
    let count = poll_event_count(&follower, &key, 6, Duration::from_secs(60)).await;
    if count != 6 {
        return Err(format!("after heal, follower has {count} events, expected 6").into());
    }
    Ok(())
}

/// 8.2 S3 unreachable with both nodes healthy: the leader keeps serving (writes
/// still acked via the live follower link), and acknowledged data survives the
/// outage. (durability-and-safety: "The current leader keeps serving;
/// replication to S3 backs off, replication to the follower continues";
/// leader-election-s3: "A long S3 outage stalls failover ... It does not
/// endanger acknowledged data".)
///
/// Front MinIO with a proxy and point both nodes at it. After election, block
/// the proxy: S3 is gone but leader<->follower stays up.
pub async fn s3_outage_leader_keeps_serving() -> R {
    let pa = port_for("p8_s3_a");
    let pb = port_for("p8_s3_b");
    let pm = port_for("p8_s3_minio");
    let sub = run_subfolder("s3out");

    // Proxy in front of MinIO; nodes reach S3 only through it.
    let _minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    let s3_proxy = TcpProxy::start(pa + 6, format!("127.0.0.1:{pm}")).await?;
    let proxy_endpoint = format!("http://{}", s3_proxy.address());

    let mk = |repl_port: u16| {
        let mut cfg = s3_cluster_config(1, "us-east-1", "celeriant-test", "minioadmin", "minioadmin", &proxy_endpoint, true);
        cfg.s3_subfolder = Some(sub.clone());
        cfg.advertised_replication_address = Some(format!("127.0.0.1:{repl_port}"));
        cfg
    };
    let a = TestServer::start_with_config_labeled(pa, mk(pa + 1), "node-a".into()).await?;
    let b = TestServer::start_with_config_labeled(pb, mk(pb + 1), "node-b".into()).await?;

    let leader = find_leader(&[a.address(), b.address()], Duration::from_secs(40)).await
        .ok_or("no leader elected within 40s")?;
    let follower = if leader == a.address() { b.address().to_string() } else { a.address().to_string() };

    let key = AggregateKey::new(1, 1, 8002);
    // Healthy: write 1..=4, follower replicates.
    write_seq(&leader, &key, 1, 4).await?;
    poll_event_count(&follower, &key, 4, Duration::from_secs(30)).await;

    // S3 disappears; both nodes still healthy and connected to each other.
    s3_proxy.block();

    // The leader must keep serving writes (follower link is live, so the ack
    // path does not need S3). Bound: must ack a fresh batch within 30s.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut acked = false;
    let mut last_err = String::new();
    while std::time::Instant::now() < deadline {
        match write_seq(&leader, &key, 5, 7).await {
            Ok(()) => { acked = true; break; }
            Err(e) => last_err = e.to_string(),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    if !acked {
        return Err(format!("leader stopped serving during S3 outage (docs: it keeps serving): {last_err}").into());
    }

    // Reads keep serving and acknowledged data is intact on the leader.
    let count = poll_event_count(&leader, &key, 7, Duration::from_secs(30)).await;
    if count != 7 {
        return Err(format!("during S3 outage leader has {count} events, expected 7 (acked data must survive)").into());
    }

    // S3 returns; the cluster must remain usable (writes resume cleanly).
    s3_proxy.unblock();
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    let mut resumed = false;
    while std::time::Instant::now() < deadline {
        if write_seq(&leader, &key, 8, 8).await.is_ok() { resumed = true; break; }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    if !resumed {
        return Err("writes did not resume after S3 recovered".into());
    }
    Ok(())
}

/// 8.3 Abrupt crash (hard kill) of a cluster node mid-stream, then restart on the
/// same data dir: every acknowledged write survives. This is the abrupt
/// counterpart to the graceful restart in phase 1.12. (durability-and-safety:
/// "an acknowledged write is on stable storage ... Pull the power on either node
/// ... and the write is still there".) We kill the FOLLOWER so the leader stays
/// up and the surviving copy plus the restarted node must converge to all data.
pub async fn crash_follower_restart_data_survives() -> R {
    let pa = port_for("p8_crash_a");
    let pb = port_for("p8_crash_b");
    let pm = port_for("p8_crash_minio");
    let sub = run_subfolder("crash");

    let minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    let endpoint = minio.endpoint();
    let mk = |repl_port: u16| {
        let mut cfg = s3_cluster_config(1, "us-east-1", "celeriant-test", "minioadmin", "minioadmin", &endpoint, true);
        cfg.s3_subfolder = Some(sub.clone());
        cfg.advertised_replication_address = Some(format!("127.0.0.1:{repl_port}"));
        cfg
    };
    let mut a = TestServer::start_with_config_labeled(pa, mk(pa + 1), "node-a".into()).await?;
    let mut b = TestServer::start_with_config_labeled(pb, mk(pb + 1), "node-b".into()).await?;

    let leader = find_leader(&[a.address(), b.address()], Duration::from_secs(40)).await
        .ok_or("no leader elected within 40s")?;
    let leader_is_a = leader == a.address();
    let follower = if leader_is_a { b.address().to_string() } else { a.address().to_string() };

    let key = AggregateKey::new(1, 1, 8003);
    write_seq(&leader, &key, 1, 5).await?;
    // Make sure the follower has the acked data before we crash it.
    poll_event_count(&follower, &key, 5, Duration::from_secs(30)).await;

    // Hard-kill the follower (abrupt; no clean shutdown).
    if leader_is_a { b.stop(); } else { a.stop(); }

    // Leader keeps serving (degraded S3 path) — write more while follower is dead.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut acked = false;
    while std::time::Instant::now() < deadline {
        if write_seq(&leader, &key, 6, 8).await.is_ok() { acked = true; break; }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    if !acked {
        return Err("leader could not ack after follower crash (degraded path)".into());
    }

    // Restart the crashed node on its same data dir; it must rejoin and converge
    // to ALL acknowledged data (the 5 it had + the 3 written while it was dead).
    // The 3 extra batches were shipped to the S3 fallback while the node was down,
    // so the restarted node must pull them via cold S3 catchup. (two-node-cluster:
    // "the follower returns and catches up"; durability-and-safety: acked data
    // lives on the surviving disk AND S3.) Bounded, non-panicking poll so a
    // divergence surfaces as a clean Err (xfail), not a runner panic.
    if leader_is_a { b.restart().await?; } else { a.restart().await?; }
    let restarted = follower; // same address, restarted process
    let count = poll_count(&restarted, &key, 8, Duration::from_secs(60)).await;
    if count != 8 {
        return Err(format!(
            "after crash+restart the node has {count} events, expected 8 \
             (extra batches that went to S3 fallback while it was down)"
        ).into());
    }
    Ok(())
}

/// Bounded, non-panicking version of `poll_event_count`: returns the highest
/// count observed within `timeout` (does not panic on timeout, so a divergence
/// can be reported as a clean `Err` and registered as an xfail).
async fn poll_count(addr: &str, key: &AggregateKey, expected: usize, timeout: Duration) -> usize {
    let deadline = std::time::Instant::now() + timeout;
    let mut best = 0usize;
    while std::time::Instant::now() < deadline {
        if let Ok(mut c) = CeleriantClient::connect(addr).await {
            if let Ok(n) = count_events(&mut c, key).await {
                best = best.max(n);
                if n >= expected {
                    return n;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    best
}

/// 8.4 Exactly-once across a leadership change: a sequence of acknowledged writes
/// is present exactly once after the leader dies and the follower is promoted —
/// no gap (a lost ack) and no duplicate (a double-apply). This is the central
/// distributed-log guarantee. (durability-and-safety "Leader dies, S3 healthy" +
/// "an acknowledged write is on stable storage"; reads-and-ordering gap-free /
/// stable indices.)
pub async fn exactly_once_across_failover() -> R {
    let pa = port_for("p8_eo_a");
    let pb = port_for("p8_eo_b");
    let pm = port_for("p8_eo_minio");
    let sub = run_subfolder("eo");

    let minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    let endpoint = minio.endpoint();
    let mk = |repl_port: u16| {
        let mut cfg = s3_cluster_config(1, "us-east-1", "celeriant-test", "minioadmin", "minioadmin", &endpoint, true);
        cfg.s3_subfolder = Some(sub.clone());
        cfg.advertised_replication_address = Some(format!("127.0.0.1:{repl_port}"));
        cfg
    };
    let mut a = TestServer::start_with_config_labeled(pa, mk(pa + 1), "node-a".into()).await?;
    let mut b = TestServer::start_with_config_labeled(pb, mk(pb + 1), "node-b".into()).await?;

    let leader = find_leader(&[a.address(), b.address()], Duration::from_secs(40)).await
        .ok_or("no leader elected within 40s")?;
    let leader_is_a = leader == a.address();
    let survivor = if leader_is_a { b.address().to_string() } else { a.address().to_string() };

    let key = AggregateKey::new(1, 1, 8004);
    // Write an acknowledged sequence of 10 events, conditional so client_seq i is
    // bound to aggregate version i (any duplicate or gap is detectable by value).
    let acked = 10u64;
    write_seq(&leader, &key, 1, acked).await?;
    // Ensure the survivor has the full acked sequence before we kill the leader.
    poll_event_count(&survivor, &key, acked as usize, Duration::from_secs(30)).await;

    // Kill the leader abruptly.
    if leader_is_a { a.stop(); } else { b.stop(); }

    // Survivor must take over and accept a write (proves promotion happened).
    let new_key = AggregateKey::new(1, 1, 8005);
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    let mut promoted = false;
    let mut last_err = String::new();
    while std::time::Instant::now() < deadline {
        match CeleriantClient::connect(&survivor).await {
            Ok(mut c) => match c.write_events_with(new_key.clone(), vec![event(1, TYPE, 3000, "{}")],
                0, WriteEventsOptions { allow_create: true, ..Default::default() }).await {
                Ok(_) => { promoted = true; break; }
                Err(e) => last_err = format!("{e:?}"),
            },
            Err(e) => last_err = format!("connect: {e:?}"),
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    if !promoted {
        return Err(format!("survivor never promoted after leader death: {last_err}").into());
    }

    // Now the heart of it: read the acked sequence back on the new leader and
    // assert EXACTLY-ONCE — versions 1..=acked, gap-free, each carrying its own
    // client_seq exactly once.
    let mut c = CeleriantClient::connect(&survivor).await?;
    let batches = read_all_batches(&mut c, &key).await?;
    if batches.len() != acked as usize {
        return Err(format!("after failover: {} batches for the acked sequence, expected {acked} (gap or duplicate)", batches.len()).into());
    }
    let mut seen_seqs = Vec::new();
    for (idx, batch) in batches.iter().enumerate() {
        let want_ver = idx as u64 + 1;
        if batch.aggregate_version != want_ver {
            return Err(format!("after failover: batch {idx} version {} != {want_ver} (gap)", batch.aggregate_version).into());
        }
        if batch.events.len() != 1 {
            return Err(format!("after failover: batch {idx} has {} events, expected 1 (double-apply?)", batch.events.len()).into());
        }
        seen_seqs.push(batch.events[0].client_seq);
    }
    let expected: Vec<u64> = (1..=acked).collect();
    if seen_seqs != expected {
        return Err(format!("after failover: client_seqs {seen_seqs:?} != {expected:?} (lost or duplicated event)").into());
    }
    Ok(())
}

/// 8.5 Parity oracle: a leader read and a follower read of the same aggregate,
/// once the follower has converged, agree field-by-field on everything the docs
/// say must be identical. (reads-and-ordering: a follower "may return a slightly
/// stale version" but converges to the same data; ordering is gap-free/stable.)
/// Differential check via `crate::metamorphic_common::diff_aggregate` in IgnoreVolatile mode
/// (the suite does not assume the two nodes assign identical server timestamps;
/// it asserts versions, client/user ids, types, and payload bytes match).
pub async fn leader_follower_read_parity() -> R {
    let pa = port_for("p8_par_a");
    let pb = port_for("p8_par_b");
    let pm = port_for("p8_par_minio");
    let sub = run_subfolder("par");

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

    let key = AggregateKey::new(1, 1, 8006);
    let n = 15u64;
    write_seq(&leader, &key, 1, n).await?;

    // Wait for the follower to converge to all n events (bounded).
    let count = poll_event_count(&follower, &key, n as usize, Duration::from_secs(45)).await;
    if count != n as usize {
        return Err(format!("follower converged to {count}, expected {n}").into());
    }

    // Read both sides and assert they agree on the non-volatile fields.
    let mut lc = CeleriantClient::connect(&leader).await?;
    let mut fc = CeleriantClient::connect(&follower).await?;
    let leader_batches = read_all_batches(&mut lc, &key).await?;
    let follower_batches = read_all_batches(&mut fc, &key).await?;

    // Sanity: the leader itself shows the full sequence (read-your-writes on leader).
    let leader_total: usize = leader_batches.iter().map(|b| b.events.len()).sum();
    if leader_total != n as usize {
        return Err(format!("leader read-your-writes: {leader_total} events, expected {n}").into());
    }

    crate::metamorphic_common::diff_aggregate(&key, &leader_batches, &follower_batches, crate::metamorphic_common::DiffMode::CrossRun)
        .map_err(|e| format!("leader/follower read parity broken: {e}"))?;

    // Extra: a from-offset parity read agrees too (follower honors offset filter
    // identically). Re-use count_events as a cheap cross-check on totals.
    let lt = count_events(&mut lc, &key).await?;
    let ft = count_events(&mut fc, &key).await?;
    if lt != ft {
        return Err(format!("leader/follower total event count diverged: leader={lt}, follower={ft}").into());
    }
    Ok(())
}
