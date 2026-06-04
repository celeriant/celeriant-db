//! Phase 6 — distributed: replication durability, leader election, failover,
//! follower reads. Each test owns an ephemeral MinIO container, auto-created
//! and removed on drop.
//!
//! Oracle: celeriant-docs/docs/concepts/durability-and-safety.md,
//! reads-and-ordering.md, operations/{leader-election-s3,two-node-cluster}.md,
//! reference/error-codes.md. Topology per AGENT_BRIEF "Topologies".

use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_wal::aggregate_key::AggregateKey;
use crate::{is_leader, poll_event_count, s3_cluster_config, MinioContainer, ServerConfig, TestServer};

use crate::common::{event, port_for, R};

const TYPE: u64 = 100;

/// A subfolder unique to this test AND this process run, so a test never sees
/// objects left by a previous run (stale fallback batches break S3 catchup).
fn run_subfolder(test: &str) -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("p6-{test}-{}-{nonce}", std::process::id())
}

fn cluster_config(endpoint: &str, subfolder: &str, repl_port: u16) -> ServerConfig {
    let mut cfg = s3_cluster_config(1, "us-east-1", "celeriant-test", "minioadmin", "minioadmin", endpoint, true);
    cfg.s3_subfolder = Some(subfolder.to_string());
    cfg.advertised_replication_address = Some(format!("127.0.0.1:{repl_port}"));
    cfg
}

/// Start a 2-node cluster on the two given base ports (replication = base+1).
/// Returns (node_a, node_b). Caller waits for election.
async fn start_pair(
    endpoint: &str,
    subfolder: &str,
    port_a: u16,
    port_b: u16,
) -> Result<(TestServer, TestServer), Box<dyn std::error::Error>> {
    let a = TestServer::start_with_config_labeled(
        port_a,
        cluster_config(endpoint, subfolder, port_a + 1),
        "node-a".into(),
    )
    .await?;
    let b = TestServer::start_with_config_labeled(
        port_b,
        cluster_config(endpoint, subfolder, port_b + 1),
        "node-b".into(),
    )
    .await?;
    Ok((a, b))
}

/// Find which of the two addresses currently accepts writes (the leader).
async fn find_leader(addrs: &[&str]) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        for a in addrs {
            if is_leader(a).await.unwrap_or(false) {
                return Ok(Some(a.to_string()));
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(None)
}

/// 6.3 A 2-node cluster elects exactly one leader through the S3 lease
/// (leader-election-s3; durability-and-safety "Failover ... lease").
pub async fn cluster_elects_single_leader() -> R {
    let pa = port_for("p6_elect_a");
    let pb = port_for("p6_elect_b");
    let pm = port_for("p6_elect_minio");
    let sub = run_subfolder("elect");
    let minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    let (a, b) = start_pair(&minio.endpoint(), &sub, pa, pb).await?;

    // Poll until exactly one node holds the lease. `is_leader` is idempotent and
    // side-effect-free, so we can sample repeatedly: both-false means we caught a
    // lease hand-off mid-flight (keep waiting); both-true is a real split-brain
    // and fails immediately; exactly-one is the documented outcome. A single
    // snapshot would flake on the hand-off window.
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    loop {
        let la = is_leader(a.address()).await?;
        let lb = is_leader(b.address()).await?;
        if la && lb {
            return Err("split-brain: both nodes accept writes (two leaders)".into());
        }
        if la ^ lb {
            return Ok(()); // exactly one leader
        }
        if std::time::Instant::now() >= deadline {
            return Err("no single leader emerged within 40s (both nodes rejected writes)".into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// 6.1 + 6.2 An acknowledged write to the leader replicates to the follower; a
/// write to the follower is rejected as NotLeader (durability-and-safety "two
/// machines have it"; reads-and-ordering; error-codes 2011).
pub async fn cluster_replicates_and_rejects_follower_write() -> R {
    let pa = port_for("p6_repl_a");
    let pb = port_for("p6_repl_b");
    let pm = port_for("p6_repl_minio");
    let sub = run_subfolder("repl");
    let minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    let (a, b) = start_pair(&minio.endpoint(), &sub, pa, pb).await?;
    tokio::time::sleep(Duration::from_secs(12)).await;

    let leader = find_leader(&[a.address(), b.address()]).await?
        .ok_or("no leader elected within timeout")?;
    let follower = if leader == a.address() { b.address() } else { a.address() }.to_string();

    let key = AggregateKey::new(1, 1, 6001);
    {
        let mut lc = CeleriantClient::connect(&leader).await?;
        for i in 1..=5u64 {
            lc.write_events_with(key.clone(), vec![event(i, TYPE, 1000 + i, &format!("{{\"n\":{i}}}"))],
                0, WriteEventsOptions { allow_create: i == 1, ..Default::default() }).await?;
        }
    }

    // Writing to the follower must be rejected as NotLeader.
    {
        let mut fc = CeleriantClient::connect(&follower).await?;
        let res = fc
            .write_events_with(AggregateKey::new(1, 1, 6002), vec![event(1, TYPE, 1000, "{}")],
                0, WriteEventsOptions { allow_create: true, ..Default::default() })
            .await;
        match res {
            Err(ClientError::NotLeader { .. }) => {}
            other => return Err(format!("follower accepted a write or wrong error: {other:?}").into()),
        }
    }

    // The 5 events must show up on the follower (replicated).
    let count = poll_event_count(&follower, &key, 5, Duration::from_secs(30)).await;
    if count != 5 {
        return Err(format!("follower has {count} events, expected 5").into());
    }
    Ok(())
}

/// 6.5 A follower read may lag but converges to the same data the leader holds
/// (reads-and-ordering "a follower read may return a slightly stale version").
pub async fn follower_read_converges() -> R {
    let pa = port_for("p6_conv_a");
    let pb = port_for("p6_conv_b");
    let pm = port_for("p6_conv_minio");
    let sub = run_subfolder("conv");
    let minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    let (a, b) = start_pair(&minio.endpoint(), &sub, pa, pb).await?;
    tokio::time::sleep(Duration::from_secs(12)).await;

    let leader = find_leader(&[a.address(), b.address()]).await?
        .ok_or("no leader elected within timeout")?;
    let follower = if leader == a.address() { b.address() } else { a.address() }.to_string();

    let key = AggregateKey::new(1, 1, 6003);
    let n = 12u64;
    {
        let mut lc = CeleriantClient::connect(&leader).await?;
        for i in 1..=n {
            lc.write_events_with(key.clone(), vec![event(i, TYPE, 1000 + i, "{}")],
                0, WriteEventsOptions { allow_create: i == 1, ..Default::default() }).await?;
        }
    }

    // Follower converges to all n events.
    let count = poll_event_count(&follower, &key, n as usize, Duration::from_secs(30)).await;
    if count != n as usize {
        return Err(format!("follower converged to {count}, expected {n}").into());
    }
    Ok(())
}

/// 6.4 Failover: when the leader dies (S3 healthy), the follower takes over
/// after the lease window and accepts writes, with prior data intact
/// (durability-and-safety "Leader dies, S3 healthy").
pub async fn failover_promotes_follower() -> R {
    let pa = port_for("p6_fail_a");
    let pb = port_for("p6_fail_b");
    let pm = port_for("p6_fail_minio");
    let sub = run_subfolder("fail");
    let minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    let (mut a, mut b) = start_pair(&minio.endpoint(), &sub, pa, pb).await?;
    tokio::time::sleep(Duration::from_secs(12)).await;

    let leader = find_leader(&[a.address(), b.address()]).await?
        .ok_or("no leader elected within timeout")?;

    // Seed data through the leader.
    let key = AggregateKey::new(1, 1, 6004);
    {
        let mut lc = CeleriantClient::connect(&leader).await?;
        for i in 1..=4u64 {
            lc.write_events_with(key.clone(), vec![event(i, TYPE, 1000 + i, "{}")],
                0, WriteEventsOptions { allow_create: i == 1, ..Default::default() }).await?;
        }
    }
    // Make sure the follower has it before we kill the leader.
    let surviving = if leader == a.address() { b.address().to_string() } else { a.address().to_string() };
    poll_event_count(&surviving, &key, 4, Duration::from_secs(30)).await;

    // Kill the leader.
    if leader == a.address() {
        a.stop();
    } else {
        b.stop();
    }

    // The surviving node must take over and accept a write. With the peer gone,
    // the new leader acks via the S3 fallback path (durability-and-safety:
    // "When the follower is down, the leader ships the batch to S3 instead").
    // Allow generous time for lease TTL expiry + CAS takeover + degraded-path ack.
    let new_key = AggregateKey::new(1, 1, 6005);
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    let mut accepted = false;
    let mut last_err = String::new();
    while std::time::Instant::now() < deadline {
        match CeleriantClient::connect(&surviving).await {
            Ok(mut sc) => {
                match sc
                    .write_events_with(new_key.clone(), vec![event(1, TYPE, 3000, "{}")],
                        0, WriteEventsOptions { allow_create: true, ..Default::default() })
                    .await
                {
                    Ok(_) => { accepted = true; break; }
                    Err(e) => last_err = format!("{e:?}"),
                }
            }
            Err(e) => last_err = format!("connect: {e:?}"),
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    if !accepted {
        return Err(format!("surviving node never accepted a write after leader death (last error: {last_err})").into());
    }

    // Prior data written through the old leader is still present on the new one.
    let count = poll_event_count(&surviving, &key, 4, Duration::from_secs(30)).await;
    if count < 4 {
        return Err(format!("after failover the new leader has {count} events, expected >=4").into());
    }
    Ok(())
}
