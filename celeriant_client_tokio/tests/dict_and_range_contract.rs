//! Dict handshake, frame cap and shard-range checks on the pooled path. Every
//! server here is a scripted fake; nothing touches a real cluster.

use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use celeriant_client_tokio::list_operations::ListOptions;
use celeriant_client_tokio::{CeleriantPool, ClientError, ClientIdentityConfig, PoolOptions};
use celeriant_client_wire::{build_frame, write_frame};
use celeriant_msg::process_identify::{read_identify_request, write_identify_response};
use celeriant_msg::response::responses::IdentifyResponse;
use celeriant_wire::network::wire_error::WireError;
use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WireHeader};
use futures_lite::AsyncWriteExt;
use tokio_util::compat::TokioAsyncReadCompatExt;

const UNCACHED_SHA: &str = "a-sha-no-pool-cache-can-resolve";
const MAX: u64 = 64 * 1024 * 1024;

struct Fake {
    addr: SocketAddr,
    accepts: Arc<AtomicUsize>,
}

/// Answers every Identify with a dict sha and no bytes: "you already have this
/// one". A pool whose cache cannot resolve the sha must fail the handshake.
fn dict_sha_only_server() -> Fake {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let accepts_t = Arc::clone(&accepts);

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        while let Ok((socket, _)) = listener.accept().await {
            accepts_t.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut stream = socket.compat();
                let Ok(header) = WireHeader::from_reader(&mut stream, MAX).await else {
                    return;
                };
                if read_identify_request(header, &mut stream).await.is_err() {
                    return;
                }
                let resp = IdentifyResponse {
                    correlation_id: None,
                    client_id: Some(7),
                    access_level: None,
                    compression_dict_sha256: Some(UNCACHED_SHA.to_string()),
                    compression_dict_bytes: None,
                };
                let _ = write_identify_response(&mut stream, &resp, PROTOCOL_VERSION_V2).await;
                let _ = stream.flush().await;
                std::future::pending::<()>().await
            });
        }
    });

    Fake { addr, accepts }
}

fn identity() -> ClientIdentityConfig {
    ClientIdentityConfig { public_key: None, private_key: None, api_key: None }
}

fn write_request(aggregate_id: u128) -> celeriant_msg::request::requests::WriteRequest {
    use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

    let mut writes = std::collections::HashMap::new();
    writes.insert(
        AggregateKey::new(1, 1, aggregate_id),
        SingleAggregateWrite {
            events: vec![DatablockAggregateEvent {
                client_seq: 0,
                event_seq: 0,
                event_id: None,
                event_timestamp: 1,
                event_type_major: 1,
                event_type_minor: 0,
                event_value: Arc::new(vec![7u8; 16]),
                iv: None,
            }],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );
    WriteRequest { correlation_id: None, client_id: 1, user_id: None, writes }
}

// ---------------------------------------------------------------------------
// (a) DictUnavailable
// ---------------------------------------------------------------------------

/// A handshake that cannot resolve the confirmed dict is a remote answer, not a
/// dial failure. It must return to the caller and the walk must not spend the
/// budget re-dialling seeds that would fail identically.
#[tokio::test]
async fn a_dict_failure_returns_to_the_caller_without_dialling_a_seed() {
    let leader = dict_sha_only_server();
    let seed = dict_sha_only_server();

    let pool = CeleriantPool::new(
        PoolOptions::new(leader.addr.to_string())
            .with_seed_addresses(vec![seed.addr.to_string()])
            .with_identity(identity())
            .with_connection_timeout(Duration::from_millis(500))
            .with_request_timeout(Duration::from_secs(2)),
    );

    let err = tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(1)))
        .await
        .expect("the walk must not hang")
        .expect_err("an unresolvable dict sha must fail the request");

    assert!(
        matches!(err, ClientError::DictUnavailable { ref sha } if sha == UNCACHED_SHA),
        "expected DictUnavailable back at the caller, got: {err:?}"
    );
    assert_eq!(seed.accepts.load(Ordering::SeqCst), 0, "a seed would fail identically; the walk must not dial it");
}

/// `NodePool::checkout` used to record *every* `create_client` error as a
/// connect failure, so a remote handshake answer tripped the node's circuit
/// breaker. Inside the cooldown the precise `DictUnavailable` became a
/// fabricated `ConnectionFailed("circuit breaker open")` about a node that is up
/// and answering, and with seeds configured that connection-class error sent
/// `leader_route!` walking them.
#[tokio::test]
async fn a_dict_failure_must_not_trip_the_circuit_breaker() {
    let leader = dict_sha_only_server();

    let pool = CeleriantPool::new(
        PoolOptions::new(leader.addr.to_string())
            .with_identity(identity())
            .with_connection_timeout(Duration::from_millis(500))
            .with_request_timeout(Duration::from_secs(2)),
    );

    let first = tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(1)))
        .await
        .expect("no hang")
        .expect_err("first write fails the handshake");
    assert!(matches!(first, ClientError::DictUnavailable { .. }), "got {first:?}");

    let second = tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(2)))
        .await
        .expect("no hang")
        .expect_err("second write fails the handshake too");

    assert!(
        matches!(second, ClientError::DictUnavailable { .. }),
        "a dict-confirmation miss is the node's answer, not a dead dial: the second \
         write must report the same cause, got: {second:?} ({second})"
    );
    assert_eq!(
        pool.stats().connections.circuit_breaker_rejections,
        0,
        "no breaker should be open: the node accepted the connection and answered"
    );
}

// ---------------------------------------------------------------------------
// (b) build_frame's pre-compression cap
// ---------------------------------------------------------------------------

/// The pre-check must be indistinguishable from the write-side check it front-runs,
/// or the routing macro's `MessageTooLarge` early return stops matching.
#[tokio::test]
async fn the_pre_and_post_compression_rejections_are_the_same_error() {
    const TINY_MAX: u64 = 4096;
    let body = vec![0u8; 1_048_576];

    let pre = build_frame(3, body.clone(), None, false, TINY_MAX)
        .err()
        .expect("build_frame rejects the oversized body");

    // The same body waved past build_frame's cap, judged by write_frame instead.
    let frame = build_frame(3, body, None, false, u64::MAX).expect("no cap, no rejection");
    let mut sink = Vec::new();
    let post = write_frame(&mut sink, &frame, TINY_MAX, PROTOCOL_VERSION_V2)
        .await
        .expect_err("write_frame rejects it too");

    match (&pre, &post) {
        (
            WireError::MessageTooLarge { message_length: a, max_size_bytes: am },
            WireError::MessageTooLarge { message_length: b, max_size_bytes: bm },
        ) => {
            assert_eq!((a, am), (b, bm), "the two guards must report identical fields");
            assert_eq!(*a, 1_048_576, "the uncompressed length is what both judge");
        }
        _ => panic!("both guards must yield MessageTooLarge, got {pre:?} / {post:?}"),
    }
    assert!(sink.is_empty(), "a refused frame must not put a byte on the wire");
}

// ---------------------------------------------------------------------------
// (c) check_shard_range
// ---------------------------------------------------------------------------

/// The pool must reject an impossible range before it takes a connection out of
/// the pool, so a caller-input error costs neither a permit nor a dial.
#[tokio::test]
async fn an_impossible_list_range_is_refused_without_dialling_anyone() {
    let node = dict_sha_only_server();
    let pool = CeleriantPool::new(
        PoolOptions::new(node.addr.to_string())
            .with_connection_timeout(Duration::from_millis(500)),
    );

    let bad = ListOptions { start_shard: 5, max_shard_hint: Some(4), ..Default::default() };
    for err in [
        pool.list_orgs(bad.clone()).await.err(),
        pool.list_aggregate_types(None, bad.clone()).await.err(),
        pool.list_aggregates(None, None, bad.clone()).await.err(),
    ] {
        let err = err.expect("an impossible shard range must not yield an iterator");
        assert!(matches!(err, ClientError::InvalidShardRange { .. }), "got {err:?}");
    }
    assert_eq!(node.accepts.load(Ordering::SeqCst), 0, "no connection may be dialled for a caller-input error");
}

/// Boundaries the check must not over-reject: a single-shard range, shard 0 as a
/// real hint rather than "unset", and the top of the u64 range. The constructors
/// touch no socket, so an accept-only listener is server enough.
#[tokio::test]
async fn the_shard_range_check_accepts_every_satisfiable_range() {
    use celeriant_client_tokio::CeleriantClient;
    use celeriant_client_tokio::list_operations::{
        ListAggregateTypesIterator, ListAggregatesIterator, ListOrgsIterator,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((s, _)) = listener.accept().await {
            held.push(s);
        }
    });
    let mut client = CeleriantClient::connect(&addr).await.unwrap();

    for (start, hint) in [(0u64, None), (7, Some(7)), (0, Some(0)), (u64::MAX, None), (u64::MAX, Some(u64::MAX))] {
        let opts = ListOptions { start_shard: start, max_shard_hint: hint, ..Default::default() };
        assert!(
            ListOrgsIterator::new(&mut client, opts.clone()).is_ok(),
            "start {start} hint {hint:?} names at least one shard"
        );
        assert!(ListAggregateTypesIterator::new(&mut client, None, opts.clone()).is_ok());
        assert!(ListAggregatesIterator::new(&mut client, None, None, opts).is_ok());
    }

    for (start, hint) in [(1u64, Some(0u64)), (u64::MAX, Some(u64::MAX - 1))] {
        let opts = ListOptions { start_shard: start, max_shard_hint: hint, ..Default::default() };
        assert!(
            ListOrgsIterator::new(&mut client, opts).is_err(),
            "start {start} hint {hint:?} names no shard at all"
        );
    }
}
