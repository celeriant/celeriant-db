//! Black-box contract for reusing a pooled connection the peer has closed.

mod common;

use std::time::Duration;

use celeriant_client_tokio::{CeleriantPool, ClientError, PoolOptions};
use common::{Answer, spawn, write_request};

fn pool(primary: String, seeds: Vec<String>) -> CeleriantPool {
    CeleriantPool::new(
        PoolOptions::new(primary)
            .with_seed_addresses(seeds)
            .with_connection_timeout(Duration::from_millis(500))
            .with_request_timeout(Duration::from_secs(5)),
    )
}

/// A pooled connection the server closed while it sat idle must be retired on
/// checkout, not written into and reported as an unknown outcome.
#[tokio::test]
async fn a_pooled_connection_closed_while_idle_is_replaced_not_written_to() {
    let leader = spawn(Answer::Ok);
    let pool = pool(leader.addr.to_string(), vec![]);

    pool.write(write_request(1, vec![7])).await.expect("the first write lands");
    leader.close_all().await;

    let second = tokio::time::timeout(Duration::from_secs(5), pool.write(write_request(2, vec![7])))
        .await
        .expect("the second write must not hang");

    assert!(second.is_ok(), "a server-closed idle connection is the pool's problem to fix: {second:?}");
    assert_eq!(leader.requests(), 2, "each write reached the leader exactly once");
}

/// The honest residual: a connection that was healthy at checkout and dies after
/// the request went out still leaves the outcome unknown, and is never re-sent.
#[tokio::test]
async fn a_connection_that_dies_after_the_second_request_was_sent_stays_unknown() {
    let leader = spawn(Answer::CloseAfterNth(2));
    let seed = spawn(Answer::NotLeader(leader.addr.to_string()));
    let pool = pool(leader.addr.to_string(), vec![seed.addr.to_string()]);

    pool.write(write_request(1, vec![7])).await.expect("the first write lands");

    let err = tokio::time::timeout(Duration::from_secs(5), pool.write(write_request(2, vec![7])))
        .await
        .expect("a lost connection must not wedge the caller")
        .expect_err("the leader closed before answering the second write");

    assert!(
        matches!(err, ClientError::ConnectionLostAfterSend(_)),
        "a post-send loss must stay typed as unknown, not become a retry: {err:?}"
    );
    assert_eq!(seed.accepts(), 0, "a write that may have landed must not be sent anywhere else");
}
