//! Phase 3 — retention: trim, delete, recreate, sequence continuation.
//!
//! Oracle: celeriant-docs/docs/concepts/retention-and-deletion.md,
//! events-and-the-log.md, reference/error-codes.md.

use std::collections::HashMap;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{DeleteError, ReadError, ServerError, TrimError, WriteError};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{
    AggregateDetailsRequest, DeleteRequest, ReadRequest, SingleAggregateDelete, TrimStartRequest,
};
use celeriant_wal::aggregate_key::AggregateKey;
use crate::TestServer;

use crate::common::{event, port_for, read_all, unique_key, R};

const TYPE: u64 = 100;

async fn make_stream(c: &mut CeleriantClient, key: &AggregateKey, n: u64) -> R {
    for i in 1..=n {
        let opts = WriteEventsOptions { allow_create: i == 1, ..Default::default() };
        c.write_events_with(key.clone(), vec![event(i, TYPE, 1000 + i, &format!("{{\"n\":{i}}}"))], 0, opts)
            .await?;
    }
    Ok(())
}

async fn read_from(c: &mut CeleriantClient, key: &AggregateKey, from: u64) -> Result<celeriant_msg::response::responses::ReadResponse, ClientError> {
    c.read(ReadRequest { correlation_id: None, aggregate_key: key.clone(), filters: ReadFilters::new(from) }).await
}

fn delete_one(key: &AggregateKey, d: SingleAggregateDelete) -> DeleteRequest {
    let mut deletes = HashMap::new();
    deletes.insert(key.clone(), d);
    DeleteRequest { correlation_id: None, client_id: 1, user_id: None, deletes }
}

/// 3.1 TrimStart drops a prefix: reads below keep_from become unavailable (1000),
/// reads from keep_from onward still work (retention-and-deletion "Trim";
/// error-codes 1000).
pub async fn trim_drops_prefix() -> R {
    let server = TestServer::start_with_port(port_for("trim_drops_prefix")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("trim_drops_prefix");
    make_stream(&mut c, &key, 10).await?;

    c.trim_start(TrimStartRequest {
        correlation_id: None,
        aggregate_key: key.clone(),
        keep_from_aggregate_version: 5,
        client_id: 1,
        user_id: None,
    })
    .await?;

    // Reading from version 1 (below the kept range) must be unavailable.
    match read_from(&mut c, &key, 1).await {
        Err(ClientError::Server(ServerError::Read { kind: ReadError::UnavailableBatchIndex { .. }, .. })) => {}
        other => return Err(format!("read from 1 after trim: expected UnavailableBatchIndex, got {other:?}").into()),
    }
    // Reading from version 5 works and returns 5..=10.
    let resp = read_from(&mut c, &key, 5).await?;
    let versions: Vec<u64> = resp.event_batches.iter().map(|b| b.aggregate_version).collect();
    if versions.first() != Some(&5) {
        return Err(format!("after trim, read from 5 began at {:?}, expected 5", versions.first()).into());
    }
    if versions.last() != Some(&10) {
        return Err(format!("after trim, read from 5 ended at {:?}, expected 10", versions.last()).into());
    }
    Ok(())
}

/// 3.2 Trim does not rewrite the remaining events: the events from keep_from
/// onward are byte-identical to before the trim (retention-and-deletion:
/// "Trimming does not rewrite the events that remain").
pub async fn trim_preserves_remaining() -> R {
    let server = TestServer::start_with_port(port_for("trim_preserves_remaining")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("trim_preserves_remaining");
    make_stream(&mut c, &key, 8).await?;

    let before = read_from(&mut c, &key, 4).await?;
    let before_payloads: Vec<(u64, Vec<u8>)> = before
        .event_batches
        .iter()
        .map(|b| (b.aggregate_version, b.events[0].event_value.as_slice().to_vec()))
        .collect();

    c.trim_start(TrimStartRequest {
        correlation_id: None,
        aggregate_key: key.clone(),
        keep_from_aggregate_version: 4,
        client_id: 1,
        user_id: None,
    })
    .await?;

    let after = read_from(&mut c, &key, 4).await?;
    let after_payloads: Vec<(u64, Vec<u8>)> = after
        .event_batches
        .iter()
        .map(|b| (b.aggregate_version, b.events[0].event_value.as_slice().to_vec()))
        .collect();

    if before_payloads != after_payloads {
        return Err("trim altered retained events".into());
    }
    if before_payloads.first().map(|(v, _)| *v) != Some(4) {
        return Err("retained range did not start at the kept version".into());
    }
    Ok(())
}

/// 3.3 A trim index outside the stream's range -> TrimIndexOutOfRange (3004).
pub async fn trim_out_of_range_rejected() -> R {
    let server = TestServer::start_with_port(port_for("trim_out_of_range_rejected")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("trim_out_of_range_rejected");
    make_stream(&mut c, &key, 3).await?;

    let res = c
        .trim_start(TrimStartRequest {
            correlation_id: None,
            aggregate_key: key.clone(),
            keep_from_aggregate_version: 99, // far beyond max version 3
            client_id: 1,
            user_id: None,
        })
        .await;
    match res {
        Err(ClientError::Server(ServerError::Trim { kind: TrimError::IndexOutOfRange, .. })) => Ok(()),
        other => Err(format!("expected TrimError::IndexOutOfRange, got {other:?}").into()),
    }
}

/// 3.4 Trim on a missing aggregate -> TrimAggregateNotExists (3000).
pub async fn trim_missing_rejected() -> R {
    let server = TestServer::start_with_port(port_for("trim_missing_rejected")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("trim_missing_rejected");

    let res = c
        .trim_start(TrimStartRequest {
            correlation_id: None,
            aggregate_key: key,
            keep_from_aggregate_version: 1,
            client_id: 1,
            user_id: None,
        })
        .await;
    match res {
        Err(ClientError::Server(ServerError::Trim { kind: TrimError::AggregateNotExists, .. })) => Ok(()),
        other => Err(format!("expected TrimError::AggregateNotExists, got {other:?}").into()),
    }
}

/// 3.5 Delete removes the whole aggregate; a subsequent read -> AggregateNotExists
/// (retention-and-deletion "Delete"; error-codes 1001).
pub async fn delete_removes_aggregate() -> R {
    let server = TestServer::start_with_port(port_for("delete_removes_aggregate")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("delete_removes_aggregate");
    make_stream(&mut c, &key, 3).await?;

    c.delete(delete_one(&key, SingleAggregateDelete {
        allow_recreate: false,
        allow_sequence_continuation: false,
        expected_version: None,
    }))
    .await?;

    match read_from(&mut c, &key, 1).await {
        Err(ClientError::Server(ServerError::Read { kind: ReadError::AggregateNotExists, .. })) => Ok(()),
        other => Err(format!("after delete, read: expected AggregateNotExists, got {other:?}").into()),
    }
}

/// 3.6 Delete with allow_recreate=false: writing the key again is rejected
/// AggregateRecreateNotAllowed (2006) (retention-and-deletion "AllowRecreate";
/// error-codes 2006).
pub async fn delete_no_recreate_blocks_rewrite() -> R {
    let server = TestServer::start_with_port(port_for("delete_no_recreate_blocks_rewrite")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("delete_no_recreate_blocks_rewrite");
    make_stream(&mut c, &key, 2).await?;

    c.delete(delete_one(&key, SingleAggregateDelete {
        allow_recreate: false,
        allow_sequence_continuation: false,
        expected_version: None,
    }))
    .await?;

    let res = c
        .write_events_with(
            key.clone(),
            vec![event(1, TYPE, 2000, "{}")],
            0, WriteEventsOptions { allow_create: true, ..Default::default() },
        )
        .await;
    match res {
        Err(ClientError::Server(ServerError::Write { kind: WriteError::AggregateRecreateNotAllowed, .. })) => Ok(()),
        other => Err(format!("expected WriteError::AggregateRecreateNotAllowed, got {other:?}").into()),
    }
}

/// 3.7 Delete with allow_recreate=true: the key can be written again as a new
/// aggregate (retention-and-deletion "AllowRecreate").
pub async fn delete_recreate_allows_rewrite() -> R {
    let server = TestServer::start_with_port(port_for("delete_recreate_allows_rewrite")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("delete_recreate_allows_rewrite");
    make_stream(&mut c, &key, 2).await?;

    c.delete(delete_one(&key, SingleAggregateDelete {
        allow_recreate: true,
        allow_sequence_continuation: false,
        expected_version: None,
    }))
    .await?;

    // Re-create succeeds.
    c.write_events_with(
        key.clone(),
        vec![event(1, TYPE, 2000, r#"{"reborn":true}"#)],
        0, WriteEventsOptions { allow_create: true, ..Default::default() },
    )
    .await?;

    let batches = read_all(&mut c, &key).await?;
    if batches.is_empty() {
        return Err("recreated aggregate has no events".into());
    }
    let last = batches.last().unwrap();
    let payload = String::from_utf8_lossy(&last.events[0].event_value);
    if !payload.contains("reborn") {
        return Err(format!("recreated aggregate missing the new event, last payload: {payload}").into());
    }
    Ok(())
}

/// 3.8 Delete is guarded by expected_version like any conditional write: a stale
/// guard -> DeleteOptimisticConcurrencyViolation (4002) and the aggregate
/// survives (retention-and-deletion "guarded by ExpectedVersion"; error-codes 4002).
pub async fn delete_conditional_guard() -> R {
    let server = TestServer::start_with_port(port_for("delete_conditional_guard")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("delete_conditional_guard");
    make_stream(&mut c, &key, 3).await?; // version 3

    let res = c
        .delete(delete_one(&key, SingleAggregateDelete {
            allow_recreate: false,
            allow_sequence_continuation: false,
            expected_version: Some(1), // stale: it's at 3
        }))
        .await;
    match res {
        Err(ClientError::Server(ServerError::Delete {
            kind: DeleteError::OptimisticConcurrencyViolation { .. }, ..
        })) => {}
        other => return Err(format!("expected DeleteError::OptimisticConcurrencyViolation, got {other:?}").into()),
    }
    // Aggregate must still be readable.
    let resp = read_from(&mut c, &key, 1).await?;
    if resp.event_batches.is_empty() {
        return Err("guarded delete still removed the aggregate".into());
    }
    Ok(())
}

/// 3.9 Delete on a missing aggregate -> DeleteAggregateNotExists (4000).
pub async fn delete_missing_rejected() -> R {
    let server = TestServer::start_with_port(port_for("delete_missing_rejected")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("delete_missing_rejected");

    let res = c
        .delete(delete_one(&key, SingleAggregateDelete {
            allow_recreate: false,
            allow_sequence_continuation: false,
            expected_version: None,
        }))
        .await;
    match res {
        Err(ClientError::Server(ServerError::Delete { kind: DeleteError::AggregateNotExists, .. })) => Ok(()),
        other => Err(format!("expected DeleteError::AggregateNotExists, got {other:?}").into()),
    }
}

/// 3.10 allow_sequence_continuation controls whether a recreated stream
/// continues its server event_seq or restarts (retention-and-deletion
/// "AllowSequenceContinuation"). We assert the two modes differ: continuation
/// keeps event_seq climbing past the deleted stream's last; restart drops it
/// back below that high-water mark.
pub async fn delete_sequence_continuation() -> R {
    let server = TestServer::start_with_port(port_for("delete_sequence_continuation")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    // Helper: build a 3-event stream, capture the max event_seq, delete with the
    // given continuation flag, recreate one event, return (pre_delete, post_recreate).
    async fn cycle(c: &mut CeleriantClient, key: &AggregateKey, continuation: bool) -> Result<(u64, u64), Box<dyn std::error::Error>> {
        for i in 1..=3u64 {
            c.write_events_with(key.clone(), vec![event(i, TYPE, 1000 + i, "{}")],
                0, WriteEventsOptions { allow_create: i == 1, ..Default::default() }).await?;
        }
        let pre = c.aggregate_details(AggregateDetailsRequest { correlation_id: None, aggregate_key: key.clone() }).await?.max_event_seq;

        let mut deletes = HashMap::new();
        deletes.insert(key.clone(), SingleAggregateDelete {
            allow_recreate: true,
            allow_sequence_continuation: continuation,
            expected_version: None,
        });
        c.delete(DeleteRequest { correlation_id: None, client_id: 1, user_id: None, deletes }).await?;

        c.write_events_with(key.clone(), vec![event(1, TYPE, 2000, "{}")],
            0, WriteEventsOptions { allow_create: true, ..Default::default() }).await?;
        let post = c.aggregate_details(AggregateDetailsRequest { correlation_id: None, aggregate_key: key.clone() }).await?.max_event_seq;
        Ok((pre, post))
    }

    let (pre_c, post_c) = cycle(&mut c, &unique_key("seq_cont_yes"), true).await?;
    let (pre_r, post_r) = cycle(&mut c, &unique_key("seq_cont_no"), false).await?;

    // Continuation: the recreated event's seq is exactly one past where the
    // deleted stream left off (retention-and-deletion: "continues from where the
    // deleted one left off"). This is the strong, exact promise.
    if post_c != pre_c + 1 {
        return Err(format!("continuation=true: max_event_seq {post_c}, expected pre+1 = {}", pre_c + 1).into());
    }
    // Restart: the seq does NOT continue. `max_event_seq` is a high-watermark, so
    // it cannot drop below `pre` here; the observable property is that it does not
    // advance past it the way continuation does. The docs say "or restarts" but
    // don't pin the reset value, so we assert only the documented distinction.
    if post_r > pre_r {
        return Err(format!("continuation=false: max_event_seq advanced to {post_r} past pre {pre_r}; expected no continuation").into());
    }
    if post_c == post_r {
        return Err(format!("continuation flag had no effect: both ended at max_event_seq {post_c}").into());
    }
    Ok(())
}

/// 3.11 A delete request spanning multiple shards is rejected wholesale before
/// any aggregate is deleted — the delete-side analogue of the cross-shard write
/// constraint (consistency-boundaries: all keys in one request must route to the
/// same shard). The docs imply ShardRoutingMultipleShards (9001); see F2 — the
/// server returns 9002 on this path too. Registered xfail (asserts documented
/// 9001) if the divergence holds for delete.
pub async fn delete_cross_shard_rejected() -> R {
    let config = crate::ServerConfig { num_shards: Some(4), standalone: true, ..Default::default() };
    let server = TestServer::start_with_config(port_for("p3_delete_cross_shard"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    // aggregate_id routing, 4 shards: 1000 % 4 == 0, 1001 % 4 == 1 -> different shards.
    let a = AggregateKey::new(1, 1, 1000);
    let b = AggregateKey::new(1, 1, 1001);
    for k in [&a, &b] {
        c.write_events_with(k.clone(), vec![event(1, TYPE, 1000, "{}")],
            0, WriteEventsOptions { allow_create: true, ..Default::default() }).await?;
    }

    // Delete both in ONE request — spans two shards.
    let mut deletes = HashMap::new();
    deletes.insert(a.clone(), SingleAggregateDelete { allow_recreate: false, allow_sequence_continuation: false, expected_version: None });
    deletes.insert(b.clone(), SingleAggregateDelete { allow_recreate: false, allow_sequence_continuation: false, expected_version: None });
    let res = c.delete(DeleteRequest { correlation_id: None, client_id: 1, user_id: None, deletes }).await;
    match res {
        Err(ClientError::Server(ServerError::ShardRouting { error_code: 9001, .. })) => {}
        other => return Err(format!("expected ShardRouting 9001 for cross-shard delete, got {other:?}").into()),
    }
    // Wholesale rejection: neither aggregate was deleted (both still readable).
    for k in [&a, &b] {
        if read_all(&mut c, k).await?.len() != 1 {
            return Err(format!("cross-shard delete affected {k:?}; the whole request must be rejected").into());
        }
    }
    Ok(())
}
