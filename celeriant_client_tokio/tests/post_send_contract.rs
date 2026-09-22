//! Black-box contract for a failure after the request bytes went out. Needs
//! `ClientError::ConnectionLostAfterSend` to compile.

mod common;

use std::time::Duration;

use celeriant_client_tokio::{CeleriantPool, ClientError, PoolOptions};
use common::{Answer, spawn, write_request};

/// The write may already have been applied, so the library must hand the
/// uncertainty to the caller instead of re-sending it to the seed.
#[tokio::test]
async fn a_connection_lost_after_the_request_was_sent_is_never_re_sent_elsewhere() {
    let leader = spawn(Answer::CloseAfterNth(1));
    let seed = spawn(Answer::NotLeader(leader.addr.to_string()));

    let pool = CeleriantPool::new(
        PoolOptions::new(leader.addr.to_string())
            .with_seed_addresses(vec![seed.addr.to_string()])
            .with_connection_timeout(Duration::from_millis(300))
            .with_request_timeout(Duration::from_secs(5)),
    );

    let err = tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(1, vec![7])))
        .await
        .expect("a lost connection must not wedge the caller")
        .expect_err("the leader closed before answering");

    assert!(
        matches!(err, ClientError::ConnectionLostAfterSend(_)),
        "a post-send loss must be typed as such, not as a retryable connection failure: {err:?}"
    );
    assert!(err.to_string().contains("outcome unknown"), "the caller must be told the write may have landed, got: {err}");
    assert_eq!(leader.requests(), 1, "the request was sent once");
    assert_eq!(seed.accepts(), 0, "a write that may have landed must not be sent anywhere else");
}
