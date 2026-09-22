//! Pool stats, black box against the public API only.

mod common;

use std::time::Duration;

use celeriant_client_tokio::{CeleriantPool, ClientError, PoolOptions};
use celeriant_client_tokio::pool::{ConnectionStats, NodeStats, PoolStats, WAIT_BUCKETS};
use common::{Answer, dead_address, spawn, write_request};

fn waits(s: &ConnectionStats) -> u64 {
    s.wait_buckets.iter().sum()
}

/// Every `get()` records a wait sample, successful ones included. The unit test
/// `pool::tests::stats_count_a_pool_timeout_and_its_wait_bucket` asserts
/// `wait_buckets[bucket(50ms)] == 1` after one successful get and one pool
/// timeout, so it only passes while the successful get lands in a different
/// bucket. Under thread contention it does not.
#[tokio::test]
async fn a_successful_get_also_records_a_wait_sample() {
    let leader = spawn(Answer::Ok);
    let pool = CeleriantPool::new(PoolOptions::new(leader.addr.to_string()));

    let conn = pool.get_leader_connection().await.expect("the leader accepts");
    drop(conn);

    let c = pool.stats().connections;
    assert_eq!(waits(&c), 1, "a successful get is a wait sample too: {:?}", c.wait_buckets);
}

/// The histogram must cover every exit path, including the circuit-breaker
/// fast-fail that does no waiting at all.
#[tokio::test]
async fn the_wait_histogram_covers_the_breaker_rejection_path() {
    let pool = CeleriantPool::new(
        PoolOptions::new(dead_address()).with_connection_timeout(Duration::from_millis(200)),
    );

    assert!(pool.get_leader_connection().await.is_err(), "nothing is listening");
    assert!(pool.get_leader_connection().await.is_err(), "the breaker is open");

    let c = pool.stats().connections;
    assert_eq!(c.circuit_breaker_rejections, 1, "the second get is a breaker reject");
    assert_eq!(waits(&c), 2, "both exits are samples: {:?}", c.wait_buckets);
}

/// `attempted` must never be less than `succeeded + failed`: an operator who
/// sees otherwise cannot trust any of the three.
#[tokio::test]
async fn attempts_account_for_every_outcome() {
    let leader = spawn(Answer::Ok);
    let pool = CeleriantPool::new(
        PoolOptions::new(leader.addr.to_string())
            .with_seed_addresses(vec![dead_address()])
            .with_connection_timeout(Duration::from_millis(200)),
    );

    pool.write(write_request(1, vec![1])).await.expect("the leader answers");
    pool.write(write_request(2, vec![1])).await.expect("reuse, no dial");
    let _ = CeleriantPool::new(PoolOptions::new(dead_address()));

    let c = pool.stats().connections;
    assert_eq!(c.attempted, c.succeeded + c.failed, "{c:?}");
    assert_eq!((c.attempted, c.succeeded, c.pooled_reuse), (1, 1, 1), "{c:?}");
}

fn stats_of(address: &str, attempted: u64, reuse: u64, bucket: usize) -> PoolStats {
    let mut wait_buckets = [0u64; WAIT_BUCKETS];
    wait_buckets[bucket] = attempted + reuse;
    let stats = ConnectionStats {
        attempted,
        succeeded: attempted,
        pooled_reuse: reuse,
        wait_buckets,
        ..Default::default()
    };
    PoolStats {
        connections: stats.clone(),
        leader_redirects_followed: 1,
        leader_hints_skipped: 0,
        leader_cache_clears: 2,
        leader_pinned_to_seed: 0,
        walks_exhausted: 0,
        post_send_losses: 0,
        nodes: vec![NodeStats { address: address.to_string(), stats }],
    }
}

/// `merge` is what the bench uses to fold several pools into one scenario
/// row. The same address appearing twice must sum, not duplicate, and the
/// aggregate must stay equal to the sum of the rows.
#[test]
fn merge_sums_a_repeated_address_and_keeps_the_aggregate_consistent() {
    let mut a = stats_of("10.0.0.1:9000", 3, 5, 2);
    a.merge(stats_of("10.0.0.1:9000", 4, 6, 2));
    a.merge(stats_of("10.0.0.2:9000", 1, 1, 3));

    assert_eq!(a.nodes.len(), 2, "the repeated address must fold into one row");
    assert_eq!(a.nodes[0].address, "10.0.0.1:9000", "rows must sort by address");
    assert_eq!(a.nodes[0].stats.attempted, 7);
    assert_eq!(a.nodes[0].stats.pooled_reuse, 11);
    assert_eq!(a.connections.attempted, 8, "aggregate must equal the sum of the rows");
    assert_eq!(a.connections.pooled_reuse, 12);
    assert_eq!(a.leader_redirects_followed, 3);
    assert_eq!(a.connections.wait_buckets.iter().sum::<u64>(), 20);

    // Associativity: (a+b)+c == a+(b+c).
    let mut left = stats_of("x:1", 1, 0, 0);
    left.merge(stats_of("y:1", 2, 0, 1));
    left.merge(stats_of("x:1", 4, 0, 0));
    let mut right = stats_of("y:1", 2, 0, 1);
    right.merge(stats_of("x:1", 4, 0, 0));
    let mut a_first = stats_of("x:1", 1, 0, 0);
    a_first.merge(right);
    assert_eq!(left.connections.attempted, a_first.connections.attempted);
    assert_eq!(
        left.nodes.iter().map(|n| (n.address.clone(), n.stats.attempted)).collect::<Vec<_>>(),
        a_first.nodes.iter().map(|n| (n.address.clone(), n.stats.attempted)).collect::<Vec<_>>(),
    );
}

/// A `PoolTimeout` must not be counted as a remote connect failure, or the
/// operator reads a local queue as a dead node.
#[tokio::test]
async fn a_pool_timeout_is_not_a_connect_failure() {
    let leader = spawn(Answer::Ok);
    let pool = CeleriantPool::new(
        PoolOptions::new(leader.addr.to_string())
            .with_max_connections(1)
            .with_connection_timeout(Duration::from_millis(80)),
    );

    let _held = pool.get_leader_connection().await.expect("the leader accepts");
    let err = pool.write(write_request(1, vec![1])).await.expect_err("the only permit is held");
    assert!(matches!(err, ClientError::PoolTimeout { .. }), "got {err}");

    let c = pool.stats().connections;
    assert_eq!((c.failed, c.timed_out), (0, 0), "a local wait is not a node failure: {c:?}");
    assert_eq!((c.pool_timeouts(), c.pool_timeouts_permit), (1, 1), "{c:?}");
}
