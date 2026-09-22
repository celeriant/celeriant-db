//! Black-box contract for the local pool wait timeout. Needs
//! `ClientError::PoolTimeout` to compile.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use celeriant_client_tokio::{CeleriantPool, ClientError, PoolOptions};
use common::{Answer, spawn, write_request};

/// A pool timeout must tell the caller which address it waited for and that
/// nothing was sent, so the caller knows the retry is safe.
#[test]
fn a_pool_timeout_names_its_address_and_says_nothing_was_sent() {
    let text = ClientError::PoolTimeout { address: "127.0.0.1:65000".to_string() }.to_string();

    assert!(text.contains("Pool timeout"), "got: {text}");
    assert!(text.contains("127.0.0.1:65000"), "the caller must learn which address it waited for, got: {text}");
    assert!(text.contains("not sent"), "the caller must learn the request never left, got: {text}");
}

/// Waiting out the local connection permit is this node's problem, not a
/// leader change: it returns at once and no follower is dialled for a write
/// only the leader can serve.
#[tokio::test]
async fn a_local_pool_timeout_returns_at_once_and_never_dials_a_follower() {
    let release = Arc::new(AtomicBool::new(false));
    let leader = spawn(Answer::HoldFirst(release.clone()));
    let follower = spawn(Answer::Ok);
    let leader_addr = leader.addr.to_string();

    let pool = Arc::new(CeleriantPool::new(
        PoolOptions::new(&leader_addr)
            .with_seed_addresses(vec![follower.addr.to_string()])
            .with_max_connections(1)
            .with_connection_timeout(Duration::from_millis(300))
            .with_request_timeout(Duration::from_secs(10)),
    ));

    let holder = tokio::spawn({
        let pool = pool.clone();
        async move { pool.write(write_request(1, vec![7])).await }
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let started = Instant::now();
    let err = tokio::time::timeout(Duration::from_secs(5), pool.write(write_request(2, vec![7])))
        .await
        .expect("a saturated pool must not wedge the caller")
        .expect_err("the only connection is in use");

    match &err {
        ClientError::PoolTimeout { address } => {
            assert_eq!(*address, leader_addr, "the timeout must name the node it waited for")
        }
        other => panic!("a local pool wait is not a leader failure, got: {other:?}"),
    }
    assert!(started.elapsed() < Duration::from_secs(2), "it must return on the connection timeout, not the request timeout");
    assert_eq!(follower.accepts(), 0, "a follower cannot serve the write and must not be dialled");

    release.store(true, Ordering::SeqCst);
    holder.await.unwrap().expect("the held write completes once the leader answers");

    pool.write(write_request(3, vec![7])).await.expect("the freed connection serves the next write");
    assert_eq!(follower.accepts(), 0, "the leader cache survived the pool timeout");
}
