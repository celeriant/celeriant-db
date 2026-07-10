//! S3→TCP catchup failback: a follower holding a gap whose entries went to
//! S3 must still converge when S3 dies — the leader's TCP extended catchup
//! is the bridge of last resort.
//!
//! The follower's replication port sits behind a TcpProxy. Blocking the
//! proxy makes the leader declare the follower unreachable and commit via
//! S3 fallback; S3 is then killed and the proxy unblocked. Whichever
//! recovery path fires — gap rejection → TCP extended catchup, or a kick
//! into S3 catchup that bails on dead S3 and resumes as Follower — the
//! follower must converge with no working S3. (A cold boot refuses to start
//! without S3, so this path is only reachable on a running process.)
//!
//! Scenario:
//! 1. MinIO + two-node cluster, follower replication behind TcpProxy;
//!    events 1-3 replicate over TCP.
//! 2. Block proxy; events 4-8 commit via S3 fallback (retried through the
//!    unreachable-detection window).
//! 3. Stop MinIO (fast connection-refused), unblock proxy: follower must
//!    reach 8 events with S3 dead.
//! 4. Still S3-dead: events 9-10 over live TCP replication.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{poll_converged_count, poll_event_count, s3_cluster_config, write_event, MinioContainer, TestServer, TcpProxy, FOLLOWER_CONVERGENCE_TIMEOUT};
use celeriant_wal::aggregate_key::AggregateKey;
use std::process::{Command, Stdio};
use std::time::Duration;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== S3→TCP catchup failback ===\n");

    let port_base = 10800 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let proxy_port = port_base + 200;
    let minio_port = port_base + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-failback").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    // MinioContainer names containers deterministically; stopping the daemon
    // out-of-band gives fast connection-refused (a paused container would
    // hang the S3 client until its timeout instead).
    let container_name = format!("celeriant-test-minio-{minio_port}");
    println!("MinIO ready at {endpoint}\n");

    let num_shards = 4;
    let key = AggregateKey::new(1, 1, 1);

    println!("Starting two-node cluster (follower replication behind proxy)...");
    let leader_config = s3_cluster_config(num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);
    let leader = TestServer::start_with_config_labeled(leader_port, leader_config.clone(), "leader".into()).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_repl_port = follower_port + 1;
    let proxy = TcpProxy::start(proxy_port, format!("127.0.0.1:{follower_repl_port}")).await?;
    let mut follower_config = leader_config;
    follower_config.client_port = follower_port;
    follower_config.advertised_replication_address = Some(format!("127.0.0.1:{proxy_port}"));
    let follower = TestServer::start_with_config_labeled(follower_port, follower_config, "follower".into()).await?;
    println!("  Leader at {}, Follower at {} (repl via proxy :{proxy_port})", leader.address(), follower.address());

    println!("Waiting for election + discovery + replication connection...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    println!("PHASE 1: events 1-3 over live TCP replication");
    for i in 1..=3 {
        write_retry(&mut leader_client, &leader, &key, i, i == 1).await?;
    }
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let n =
        poll_converged_count(&mut follower_client, &key, 3, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    assert_eq!(n, 3, "follower should have 3 events after TCP replication");
    drop(follower_client);

    println!("PHASE 2: block replication proxy; events 4-8 via S3 fallback");
    proxy.block();
    tokio::time::sleep(Duration::from_secs(3)).await;
    for i in 4..=8 {
        write_retry(&mut leader_client, &leader, &key, i, false).await?;
    }

    println!("PHASE 3: stopping MinIO, unblocking proxy — convergence must ride TCP");
    docker(&["stop", &container_name])?;
    proxy.unblock();

    // The S3-catchup bail takes ~20s of bounded unreachable rounds if that
    // path fires; the direct gap-rejection → extended-catchup path is
    // faster. 90s covers both without masking a wedge.
    let count = poll_event_count(follower.address(), &key, 8, Duration::from_secs(90)).await;
    println!("  follower converged to {count} events over TCP with S3 down");

    println!("PHASE 4: still S3-dead — events 9-10 over live TCP replication");
    for i in 9..=10 {
        write_retry(&mut leader_client, &leader, &key, i, false).await?;
    }
    let count = poll_event_count(follower.address(), &key, 10, Duration::from_secs(30)).await;
    println!("  follower at {count} events");

    drop(minio);
    println!("\n=== PASS: follower bridged an S3-only gap over TCP while S3 was down ===");
    Ok(())
}

/// `write_event` with retries through transient windows (unreachable
/// detection, replication backpressure), reconnecting as needed.
async fn write_retry(
    client: &mut CeleriantClient,
    leader: &TestServer,
    key: &AggregateKey,
    event_num: u64,
    allow_create: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match write_event(client, key, event_num, allow_create).await {
            Ok(()) => return Ok(()),
            Err(e) if std::time::Instant::now() < deadline => {
                println!("  write {event_num} retrying: {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
                if let Ok(c) = CeleriantClient::connect(leader.address()).await {
                    *client = c;
                }
            }
            Err(e) => return Err(format!("write {event_num} never acked: {e}").into()),
        }
    }
}

fn docker(args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("docker")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(format!("docker {args:?} failed: {status}").into());
    }
    Ok(())
}
