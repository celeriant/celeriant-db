//! Leader-walk regressions. Each test pins routing behaviour that an earlier
//! build got wrong.

mod common;

use std::time::Duration;

use celeriant_client_tokio::{
    CeleriantPool, ClientError, ClientIdentityConfig, PoolOptions,
};
use common::{Answer, dead_address, spawn, write_request};

/// Re-entering a hinted address costs one slot of `max_leader_retries` per
/// hinting follower. With four addresses the walk burned its whole budget
/// re-dialling the dead primary and never reached the live leader.
#[tokio::test]
async fn a_stale_hint_must_not_starve_an_untried_seed() {
    let primary = dead_address();
    let s1 = spawn(Answer::NotLeader(primary.clone()));
    let s2 = spawn(Answer::NotLeader(primary.clone()));
    let s3 = spawn(Answer::Ok);

    let pool = CeleriantPool::new(
        PoolOptions::new(&primary)
            .with_seed_addresses(vec![
                s1.addr.to_string(),
                s2.addr.to_string(),
                s3.addr.to_string(),
            ])
            .with_connection_timeout(Duration::from_millis(300))
            .with_request_timeout(Duration::from_secs(5)),
    );

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        pool.write(write_request(1, vec![7])),
    )
    .await
    .expect("the walk must not hang");

    assert_eq!(s3.requests(), 1, "the live leader must be tried; it never was");
    result.expect("a live leader in the seed list must serve the write");
}

/// An address that answered `NotLeader` is never re-entered in the same walk.
/// The hint is compared against `answered` *before* the current candidate is
/// pushed, so a node whose hint names itself (a stale local view during an
/// election) used to be dialled twice.
#[tokio::test]
async fn a_node_that_hints_at_itself_must_not_be_re_entered() {
    let node = common::bind();
    let addr = node.local_addr().unwrap().to_string();
    let node = common::serve(node, Answer::NotLeader(addr.clone()));

    let pool = CeleriantPool::new(
        PoolOptions::new(&addr)
            .with_connection_timeout(Duration::from_millis(300))
            .with_request_timeout(Duration::from_secs(5)),
    );

    tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(2, vec![7])))
        .await
        .expect("the walk must not hang")
        .expect_err("a node that redirects to itself cannot serve the write");

    assert_eq!(node.requests(), 1, "a definitive NotLeader must retire the address");
}

#[tokio::test]
async fn a_handshake_timeout_is_a_pre_send_failure() {
    let listener = common::bind();
    let addr = listener.local_addr().unwrap().to_string();
    // Accept and hold: the identify request is never answered.
    std::thread::spawn(move || {
        let listener = listener;
        listener.set_nonblocking(false).unwrap();
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept() {
            held.push(socket);
        }
    });
    let seed = spawn(Answer::Ok);

    let pool = CeleriantPool::new(
        PoolOptions::new(&addr)
            .with_seed_addresses(vec![seed.addr.to_string()])
            .with_identity(ClientIdentityConfig::from_api_key("k"))
            .with_connection_timeout(Duration::from_secs(5))
            .with_request_timeout(Duration::from_millis(300)),
    );

    let err = tokio::time::timeout(
        Duration::from_secs(10),
        pool.write(write_request(5, vec![7])),
    )
    .await
    .expect("the handshake timeout must not hang")
    .expect_err("the handshake never completes");

    assert!(
        !matches!(err, ClientError::RequestTimeout),
        "a write that was never serialised must not be reported as an ambiguous \
         request timeout, got: {err:?}"
    );
}
