//! Black-box leader-routing contract against the public `CeleriantPool` API.
//! Every server here is a scripted fake.

mod common;

use std::time::Duration;

use celeriant_client_tokio::{CeleriantPool, ClientError, PoolOptions};
use celeriant_wire::network::wire_error::WireError;
use common::{Answer, bind, dead_address, incompressible, spawn, write_request};

/// The leader refuses the connection before any byte is sent and the
/// seed hints straight back at it. The breaker stays authoritative, so the walk
/// ends naming the leader rather than silently succeeding or looping.
#[tokio::test]
async fn a_hint_back_to_a_leader_that_refused_the_connection_ends_the_walk_naming_it() {
    let leader_addr = dead_address();
    let seed = spawn(Answer::NotLeader(leader_addr.clone()));

    let pool = CeleriantPool::new(
        PoolOptions::new(&leader_addr)
            .with_seed_addresses(vec![seed.addr.to_string()])
            .with_connection_timeout(Duration::from_millis(300))
            .with_request_timeout(Duration::from_secs(5)),
    );

    let err = tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(1, vec![7])))
        .await
        .expect("the walk must not hang")
        .expect_err("nothing is listening on the leader address");

    assert!(err.to_string().contains(&leader_addr), "the walk must name the leader it could not reach, got: {err}");
    assert_eq!(seed.requests(), 1, "the seed is asked once, not once per re-entry");
}

/// The breaker only delays recovery: once its cooldown has passed and
/// the leader is back on the same address, a fresh write lands on it.
#[tokio::test]
async fn a_leader_that_comes_back_after_the_breaker_cooldown_serves_the_next_write() {
    let leader_listener = bind();
    let leader_addr = leader_listener.local_addr().unwrap().to_string();
    drop(leader_listener);
    let seed = spawn(Answer::NotLeader(leader_addr.clone()));

    let pool = CeleriantPool::new(
        PoolOptions::new(&leader_addr)
            .with_seed_addresses(vec![seed.addr.to_string()])
            .with_connection_timeout(Duration::from_millis(300))
            .with_request_timeout(Duration::from_secs(5)),
    );

    tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(1, vec![7])))
        .await
        .expect("the walk must not hang")
        .expect_err("the leader is down for the first write");

    tokio::time::sleep(Duration::from_millis(2100)).await;
    let listener = std::net::TcpListener::bind(leader_addr.as_str()).expect("the leader's port is free again");
    listener.set_nonblocking(true).unwrap();
    let leader = common::serve(listener, Answer::Ok);

    tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(2, vec![7])))
        .await
        .expect("the recovered write must not hang")
        .expect("a leader back on its address must serve the next write");

    assert_eq!(leader.requests(), 1, "the write went to the recovered leader");
}

/// Two nodes hinting at each other must terminate, and neither may be asked
/// twice once it has given a definitive `NotLeader`.
#[tokio::test]
async fn a_node_that_answered_not_leader_is_never_asked_again_in_the_same_walk() {
    let (a_listener, b_listener) = (bind(), bind());
    let a_addr = a_listener.local_addr().unwrap().to_string();
    let b_addr = b_listener.local_addr().unwrap().to_string();
    let a = common::serve(a_listener, Answer::NotLeader(b_addr.clone()));
    let b = common::serve(b_listener, Answer::NotLeader(a_addr.clone()));

    let pool = CeleriantPool::new(
        PoolOptions::new(&a_addr)
            .with_seed_addresses(vec![b_addr])
            .with_connection_timeout(Duration::from_millis(500))
            .with_request_timeout(Duration::from_secs(5)),
    );

    let result = tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(2, vec![7])))
        .await
        .expect("a hint cycle must not hang");

    assert!(result.is_err(), "a cycle of hints cannot produce a write: {result:?}");
    assert_eq!((a.requests(), b.requests()), (1, 1), "each node answered NotLeader once and was not re-entered");
}

/// When every candidate fails, the error must say which address produced
/// what, not just that the walk ended.
#[tokio::test]
async fn an_exhausted_walk_names_an_address_it_tried() {
    let primary = dead_address();
    let seed = dead_address();

    let pool = CeleriantPool::new(
        PoolOptions::new(&primary)
            .with_seed_addresses(vec![seed.clone()])
            .with_connection_timeout(Duration::from_millis(200))
            .with_request_timeout(Duration::from_secs(2)),
    );

    let err = tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(3, vec![7])))
        .await
        .expect("the walk must not hang")
        .expect_err("no node is listening");

    let text = err.to_string();
    assert!(
        text.contains(&primary) || text.contains(&seed),
        "the exhausted walk must name an address it tried, got: {text}"
    );
}

/// A request over the client's own cap is a caller error: it is refused
/// before any node is dialled, not masked by a failover walk.
#[tokio::test]
async fn an_oversized_request_is_refused_without_dialling_anyone() {
    let node = spawn(Answer::Ok);
    let mut options = PoolOptions::new(node.addr.to_string())
        .with_connection_timeout(Duration::from_millis(500))
        .with_request_timeout(Duration::from_secs(2));
    options.max_request_size = 1024;
    let pool = CeleriantPool::new(options);

    let err = tokio::time::timeout(
        Duration::from_secs(10),
        pool.write(write_request(4, incompressible(8 * 1024))),
    )
    .await
    .expect("an oversized request must not hang")
    .expect_err("8KB past a 1KB cap must error");

    assert!(
        matches!(err, ClientError::WireError(WireError::MessageTooLarge { .. })),
        "an oversized request must surface as MessageTooLarge, got: {err:?}"
    );
    assert_eq!(node.requests(), 0, "no node should have been asked");
}

/// A followed redirect must be remembered: the follower is asked once across
/// two writes, not once per write.
#[tokio::test]
async fn a_followed_redirect_caches_the_leader_for_the_next_write() {
    let leader = spawn(Answer::Ok);
    let follower = spawn(Answer::NotLeader(leader.addr.to_string()));

    let pool = CeleriantPool::new(
        PoolOptions::new(follower.addr.to_string())
            .with_connection_timeout(Duration::from_millis(500))
            .with_request_timeout(Duration::from_secs(5)),
    );

    for aggregate in [5, 6] {
        tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(aggregate, vec![7])))
            .await
            .expect("the write must not hang")
            .expect("the leader accepts both writes");
    }

    assert_eq!(follower.requests(), 1, "the second write must go straight to the cached leader");
    assert_eq!(leader.requests(), 2, "both writes landed on the leader");
}
