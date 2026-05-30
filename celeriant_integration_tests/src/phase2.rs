//! Phase 2 — optimistic concurrency, idempotent retries, multi-aggregate atomicity.
//!
//! Oracle: celeriant-docs/docs/concepts/{optimistic-concurrency,idempotent-writes,
//! consistency-boundaries}.md, guides/{handling-conflicts,multi-aggregate-writes}.md,
//! reference/error-codes.md.

use std::collections::HashMap;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{ServerError, WriteError};
use celeriant_msg::request::requests::{AggregateDetailsRequest, SingleAggregateWrite, WriteRequest};
use celeriant_wal::aggregate_key::AggregateKey;
use crate::{ServerConfig, TestServer};

use crate::common::{event, port_for, read_all, unique_key, R};

const TYPE: u64 = 100;

async fn version(c: &mut CeleriantClient, key: &AggregateKey) -> Result<u64, Box<dyn std::error::Error>> {
    let d = c
        .aggregate_details(AggregateDetailsRequest { correlation_id: None, aggregate_key: key.clone() })
        .await?;
    Ok(d.max_aggregate_version)
}

/// 2.1 A write whose expected_version matches the current version commits and
/// advances the version by exactly one (optimistic-concurrency "How it works").
pub async fn occ_match_commits_and_advances() -> R {
    let server = TestServer::start_with_port(port_for("occ_match_commits_and_advances")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("occ_match_commits_and_advances");

    // Create at version 0 -> 1.
    c.write_events_with(
        key.clone(),
        vec![event(1, TYPE, 1000, "{}")],
        WriteEventsOptions { allow_create: true, expected_version: Some(0), ..Default::default() },
    )
    .await?;
    let v = version(&mut c, &key).await?;
    if v != 1 {
        return Err(format!("after create, version {v} != 1").into());
    }
    // Conditional append guarded on v=1.
    c.write_events_with(
        key.clone(),
        vec![event(2, TYPE, 1001, "{}")],
        WriteEventsOptions { allow_create: false, expected_version: Some(1), ..Default::default() },
    )
    .await?;
    let v2 = version(&mut c, &key).await?;
    if v2 != 2 {
        return Err(format!("after guarded write, version {v2} != 2").into());
    }
    Ok(())
}

/// 2.2 A stale expected_version is rejected with OptimisticConcurrencyViolation
/// (2003) and nothing is appended (optimistic-concurrency; error-codes 2003).
pub async fn occ_stale_rejected_no_append() -> R {
    let server = TestServer::start_with_port(port_for("occ_stale_rejected_no_append")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("occ_stale_rejected_no_append");

    // Build the aggregate up to version 2.
    c.write_events_with(
        key.clone(),
        vec![event(1, TYPE, 1000, "{}")],
        WriteEventsOptions { allow_create: true, expected_version: Some(0), ..Default::default() },
    )
    .await?;
    c.write_events_with(
        key.clone(),
        vec![event(2, TYPE, 1001, "{}")],
        WriteEventsOptions { expected_version: Some(1), ..Default::default() },
    )
    .await?;
    let before = version(&mut c, &key).await?; // 2

    // Guard on a stale version (1).
    let res = c
        .write_events_with(
            key.clone(),
            vec![event(3, TYPE, 1002, "{}")],
            WriteEventsOptions { expected_version: Some(1), ..Default::default() },
        )
        .await;
    match res {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
        })) => {}
        other => return Err(format!("expected OptimisticConcurrencyViolation, got {other:?}").into()),
    }
    let after = version(&mut c, &key).await?;
    if after != before {
        return Err(format!("rejected write still changed version {before} -> {after}").into());
    }
    Ok(())
}

/// 2.3 expected_version=0 + allow_create creates only if absent; a second
/// creator racing the same key loses with 2003 (optimistic-concurrency
/// "brand-new aggregate"; handling-conflicts "new-aggregate case").
pub async fn occ_create_guard_races_cleanly() -> R {
    let server = TestServer::start_with_port(port_for("occ_create_guard_races_cleanly")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("occ_create_guard_races_cleanly");

    c.write_events_with(
        key.clone(),
        vec![event(1, TYPE, 1000, "{}")],
        WriteEventsOptions { allow_create: true, expected_version: Some(0), ..Default::default() },
    )
    .await?;

    // Second creator with the same expected_version: 0 must lose.
    let res = c
        .write_events_with(
            key.clone(),
            vec![event(1, TYPE, 1001, "{}")],
            WriteEventsOptions { allow_create: true, expected_version: Some(0), ..Default::default() },
        )
        .await;
    match res {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
        })) => Ok(()),
        other => Err(format!("expected OptimisticConcurrencyViolation on duplicate create, got {other:?}").into()),
    }
}

/// 2.4 enforce_client_idempotency: replaying the same (client_id, client_seq)
/// is rejected ClientIdempotencyViolation (2002) and does not double-write
/// (idempotent-writes; error-codes 2002).
pub async fn idempotency_dedupes_replay() -> R {
    let server = TestServer::start_with_port(port_for("idempotency_dedupes_replay")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("idempotency_dedupes_replay");

    let opts = WriteEventsOptions {
        client_id: 7,
        allow_create: true,
        enforce_client_idempotency: true,
        ..Default::default()
    };
    c.write_events_with(key.clone(), vec![event(1, TYPE, 1000, r#"{"v":1}"#)], opts.clone()).await?;
    // Replay the exact same client_seq=1.
    let res = c
        .write_events_with(key.clone(), vec![event(1, TYPE, 1000, r#"{"v":1}"#)], opts.clone())
        .await;
    match res {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::ClientIdempotencyViolation { .. }, ..
        })) => {}
        other => return Err(format!("expected ClientIdempotencyViolation, got {other:?}").into()),
    }
    // Exactly one event landed.
    let batches = read_all(&mut c, &key).await?;
    let total: usize = batches.iter().map(|b| b.events.len()).sum();
    if total != 1 {
        return Err(format!("replay double-wrote: {total} events present, expected 1").into());
    }
    Ok(())
}

/// 2.5 Idempotency is scoped per (aggregate, client_id): two different clients
/// using the same client_seq both land (idempotent-writes: "the dedup key is
/// the pair (clientId, ClientSeq)").
pub async fn idempotency_scoped_per_client() -> R {
    let server = TestServer::start_with_port(port_for("idempotency_scoped_per_client")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("idempotency_scoped_per_client");

    c.write_events_with(
        key.clone(),
        vec![event(1, TYPE, 1000, r#"{"who":"A"}"#)],
        WriteEventsOptions { client_id: 100, allow_create: true, enforce_client_idempotency: true, ..Default::default() },
    )
    .await?;
    // Different client_id, same client_seq=1 — must NOT be deduped.
    c.write_events_with(
        key.clone(),
        vec![event(1, TYPE, 1001, r#"{"who":"B"}"#)],
        WriteEventsOptions { client_id: 200, allow_create: false, enforce_client_idempotency: true, ..Default::default() },
    )
    .await?;

    let batches = read_all(&mut c, &key).await?;
    let total: usize = batches.iter().map(|b| b.events.len()).sum();
    if total != 2 {
        return Err(format!("expected 2 events (one per client), got {total}").into());
    }
    Ok(())
}

/// 2.6 A multi-aggregate write on one shard is all-or-nothing: if one guard is
/// stale, neither aggregate changes (consistency-boundaries; multi-aggregate-writes).
/// Uses org_id routing so two aggregates in the same org share a shard.
pub async fn multi_aggregate_atomic_rollback() -> R {
    let config = ServerConfig {
        num_shards: Some(4),
        standalone: true,
        routing_rule: crate::RoutingRule::OrgId,
        ..Default::default()
    };
    let server = TestServer::start_with_config(port_for("multi_aggregate_atomic_rollback"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    // Same org_id => same shard under org_id routing.
    let org = 555u128;
    let a = AggregateKey::new(org, 1, 1);
    let b = AggregateKey::new(org, 1, 2);

    // Create both at version 1.
    c.write_events_with(a.clone(), vec![event(1, TYPE, 1000, "{}")],
        WriteEventsOptions { allow_create: true, expected_version: Some(0), ..Default::default() }).await?;
    c.write_events_with(b.clone(), vec![event(1, TYPE, 1000, "{}")],
        WriteEventsOptions { allow_create: true, expected_version: Some(0), ..Default::default() }).await?;

    // Atomic write: a guarded correctly (v=1), b guarded stale (v=0) -> whole thing rejected.
    let mut writes = HashMap::new();
    writes.insert(a.clone(), SingleAggregateWrite {
        events: vec![event(2, TYPE, 1001, "{}")],
        allow_create: false,
        expected_version: Some(1),
        enforce_client_idempotency: false,
    });
    writes.insert(b.clone(), SingleAggregateWrite {
        events: vec![event(2, TYPE, 1001, "{}")],
        allow_create: false,
        expected_version: Some(0), // stale: b is at 1
        enforce_client_idempotency: false,
    });
    let res = c.write(WriteRequest { correlation_id: None, client_id: 1, user_id: None, writes }).await;
    match res {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
        })) => {}
        other => return Err(format!("expected OptimisticConcurrencyViolation for the batch, got {other:?}").into()),
    }
    // Neither moved: both still at version 1.
    let va = version(&mut c, &a).await?;
    let vb = version(&mut c, &b).await?;
    if va != 1 || vb != 1 {
        return Err(format!("partial apply: a@{va}, b@{vb}, expected both @1").into());
    }
    Ok(())
}

/// 2.7 A multi-aggregate write whose keys land on different shards is rejected
/// ShardRoutingMultipleShards (9001) before anything is appended
/// (multi-aggregate-writes "The constraint"; error-codes 9001).
/// Default routing is aggregate_id; pick ids whose modulus differs.
pub async fn multi_aggregate_cross_shard_rejected() -> R {
    let config = ServerConfig { num_shards: Some(4), standalone: true, ..Default::default() };
    let server = TestServer::start_with_config(port_for("multi_aggregate_cross_shard_rejected"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    // aggregate_id routing, 4 shards: 1000 % 4 == 0, 1001 % 4 == 1 -> different shards.
    let a = AggregateKey::new(1, 1, 1000);
    let b = AggregateKey::new(1, 1, 1001);

    let mut writes = HashMap::new();
    writes.insert(a.clone(), SingleAggregateWrite {
        events: vec![event(1, TYPE, 1000, "{}")],
        allow_create: true,
        expected_version: Some(0),
        enforce_client_idempotency: false,
    });
    writes.insert(b.clone(), SingleAggregateWrite {
        events: vec![event(1, TYPE, 1000, "{}")],
        allow_create: true,
        expected_version: Some(0),
        enforce_client_idempotency: false,
    });
    let res = c.write(WriteRequest { correlation_id: None, client_id: 1, user_id: None, writes }).await;
    match res {
        Err(ClientError::Server(ServerError::ShardRouting { error_code: 9001, .. })) => {}
        other => return Err(format!("expected ShardRouting 9001, got {other:?}").into()),
    }
    // Nothing was created: both reads must say AggregateNotExists.
    for k in [&a, &b] {
        match read_all(&mut c, k).await {
            Err(e) => {
                let s = e.to_string();
                if !s.contains("aggregate not exists") {
                    return Err(format!("expected AggregateNotExists for {k:?}, got {s}").into());
                }
            }
            Ok(batches) if batches.is_empty() => {}
            Ok(b) => return Err(format!("cross-shard write leaked {} batches into {k:?}", b.len()).into()),
        }
    }
    Ok(())
}

/// 2.8 OCC and idempotency compose: a conditional write that also enforces
/// idempotency, when replayed verbatim, is rejected as an idempotency violation
/// (2002) rather than re-applied (idempotent-writes "Combined with OCC").
pub async fn occ_and_idempotency_compose() -> R {
    let server = TestServer::start_with_port(port_for("occ_and_idempotency_compose")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("occ_and_idempotency_compose");

    // Create (v0->1) with idempotency on.
    c.write_events_with(
        key.clone(),
        vec![event(1, TYPE, 1000, "{}")],
        WriteEventsOptions { client_id: 9, allow_create: true, expected_version: Some(0), enforce_client_idempotency: true },
    )
    .await?;
    // Conditional append guarded on v=1, idempotency seq=2.
    c.write_events_with(
        key.clone(),
        vec![event(2, TYPE, 1001, "{}")],
        WriteEventsOptions { client_id: 9, allow_create: false, expected_version: Some(1), enforce_client_idempotency: true },
    )
    .await?;
    // Replay that exact conditional write. The aggregate has moved to 2, so the
    // OCC guard (1) is now stale AND the client_seq (2) is a duplicate. Either
    // way it must be rejected, not applied; assert no third event lands.
    let _ = c
        .write_events_with(
            key.clone(),
            vec![event(2, TYPE, 1001, "{}")],
            WriteEventsOptions { client_id: 9, allow_create: false, expected_version: Some(1), enforce_client_idempotency: true },
        )
        .await;
    let batches = read_all(&mut c, &key).await?;
    let total: usize = batches.iter().map(|b| b.events.len()).sum();
    if total != 2 {
        return Err(format!("compose replay changed event count to {total}, expected 2").into());
    }
    // And the specific error must be one of the two documented rejections.
    let res = c
        .write_events_with(
            key.clone(),
            vec![event(2, TYPE, 1001, "{}")],
            WriteEventsOptions { client_id: 9, allow_create: false, expected_version: Some(1), enforce_client_idempotency: true },
        )
        .await;
    match res {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::ClientIdempotencyViolation { .. } | WriteError::OptimisticConcurrencyViolation { .. }, ..
        })) => Ok(()),
        other => Err(format!("expected idempotency or OCC violation on replay, got {other:?}").into()),
    }
}

/// 2.9 An unconditional write (expected_version omitted) appends regardless of
/// the current version (optimistic-concurrency "Omit expectedVersion and the
/// write is unconditional"). Three unconditional writes to one aggregate all
/// land, with no OCC guard ever consulted.
pub async fn unconditional_write_appends() -> R {
    let server = TestServer::start_with_port(port_for("unconditional_write_appends")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("unconditional_write_appends");

    // expected_version stays None (Default) on every write.
    for i in 1..=3u64 {
        c.write_events_with(key.clone(), vec![event(i, TYPE, 1000 + i, "{}")],
            WriteEventsOptions { allow_create: i == 1, ..Default::default() }).await?;
    }
    let batches = read_all(&mut c, &key).await?;
    let versions: Vec<u64> = batches.iter().map(|b| b.aggregate_version).collect();
    if versions != vec![1, 2, 3] {
        return Err(format!("unconditional writes produced versions {versions:?}, expected [1,2,3]").into());
    }
    Ok(())
}
