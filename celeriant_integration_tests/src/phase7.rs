//! Phase 7 — concurrency for real, multi-shard ordering, large values & long streams.
//!
//! Single-node (standalone) so the only variable under test is genuine
//! concurrency / volume, not the cluster. Every test drives real concurrent
//! tasks (or large volume) and asserts the documented outcome holds under the
//! race — no lost update, no double-apply, ordering preserved.
//!
//! Oracle: celeriant-docs/docs/concepts/{optimistic-concurrency,idempotent-writes,
//! reads-and-ordering,consistency-boundaries,aggregates}.md, reference/error-codes.md.

use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{ServerError, WriteError};
use celeriant_msg::request::requests::AggregateDetailsRequest;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use crate::{ServerConfig, TestServer};

use crate::common::{event, port_for, read_all, unique_key, R};

const TYPE: u64 = 100;

async fn version(c: &mut CeleriantClient, key: &AggregateKey) -> Result<u64, Box<dyn std::error::Error>> {
    let d = c
        .aggregate_details(AggregateDetailsRequest { correlation_id: None, aggregate_key: key.clone() })
        .await?;
    Ok(d.max_aggregate_version)
}

/// Classify a write result as: won, lost-to-OCC (2003), or something else.
enum Outcome {
    Won,
    OccLost,
    Other(String),
}

fn classify(res: Result<celeriant_msg::response::responses::SuccessResponse, ClientError>) -> Outcome {
    match res {
        Ok(_) => Outcome::Won,
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation { .. }, ..
        })) => Outcome::OccLost,
        Err(e) => Outcome::Other(format!("{e:?}")),
    }
}

/// 7.1 Concurrent conditional writers racing the SAME expected_version: exactly
/// one commits, every other is rejected 2003, and the aggregate advances by
/// exactly one. (optimistic-concurrency: "If another writer moved it ... the
/// write is rejected whole and nothing is appended"; error-codes 2003.) The
/// existing 2.x OCC tests were sequential — this one fires the writers
/// concurrently so the guard is exercised under a real race.
pub async fn concurrent_occ_writers_one_wins() -> R {
    let server = TestServer::start_with_port(port_for("p7_occ_race")).await?;
    let addr = server.address().to_string();
    let key = unique_key("p7_occ_race");

    // Seed the aggregate to version 1.
    {
        let mut c = CeleriantClient::connect(&addr).await?;
        c.write_events_with(key.clone(), vec![event(1, TYPE, 1000, "{}")],
            0, WriteEventsOptions { allow_create: true, expected_version: Some(0), ..Default::default() }).await?;
    }

    // N writers all guard on version 1 concurrently.
    let n = 16u64;
    let mut handles = Vec::new();
    for i in 0..n {
        let addr = addr.clone();
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            let mut c = CeleriantClient::connect(&addr).await.map_err(|e| format!("connect: {e:?}"))?;
            let res = c
                .write_events_with(
                    key,
                    vec![event(100 + i, TYPE, 2000 + i, &format!("{{\"w\":{i}}}"))],
                    0, WriteEventsOptions { allow_create: false, expected_version: Some(1), ..Default::default() },
                )
                .await;
            Ok::<Outcome, String>(classify(res))
        }));
    }

    let mut won = 0usize;
    let mut lost = 0usize;
    for h in handles {
        match h.await.map_err(|e| format!("join: {e}"))?? {
            Outcome::Won => won += 1,
            Outcome::OccLost => lost += 1,
            Outcome::Other(e) => return Err(format!("unexpected write outcome under race: {e}").into()),
        }
    }
    if won != 1 {
        return Err(format!("OCC race: {won} writers won (expected exactly 1), {lost} lost").into());
    }
    if lost != (n as usize - 1) {
        return Err(format!("OCC race: {lost} writers lost (expected {})", n - 1).into());
    }

    // The aggregate advanced by exactly one batch: version 1 -> 2, total 2 events.
    let mut c = CeleriantClient::connect(&addr).await?;
    let v = version(&mut c, &key).await?;
    if v != 2 {
        return Err(format!("after OCC race, version {v} != 2 (a lost write must not append)").into());
    }
    let total: usize = read_all(&mut c, &key).await?.iter().map(|b| b.events.len()).sum();
    if total != 2 {
        return Err(format!("after OCC race, {total} events present, expected 2").into());
    }
    Ok(())
}

/// 7.2 Concurrent creators racing expected_version=0 on a brand-new key: exactly
/// one creates it, the rest get 2003. (optimistic-concurrency: "set
/// expectedVersion: 0: that succeeds only when the aggregate does not yet
/// exist"; leader-election "two nodes cannot both win" is the cluster analogue,
/// but the create guard itself is enforced here on one node.)
pub async fn concurrent_creators_one_wins() -> R {
    let server = TestServer::start_with_port(port_for("p7_create_race")).await?;
    let addr = server.address().to_string();
    let key = unique_key("p7_create_race");

    let n = 16u64;
    let mut handles = Vec::new();
    for i in 0..n {
        let addr = addr.clone();
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            let mut c = CeleriantClient::connect(&addr).await.map_err(|e| format!("connect: {e:?}"))?;
            let res = c
                .write_events_with(
                    key,
                    vec![event(1, TYPE, 1000 + i, &format!("{{\"creator\":{i}}}"))],
                    0, WriteEventsOptions { allow_create: true, expected_version: Some(0), ..Default::default() },
                )
                .await;
            Ok::<Outcome, String>(classify(res))
        }));
    }

    let mut won = 0usize;
    for h in handles {
        match h.await.map_err(|e| format!("join: {e}"))?? {
            Outcome::Won => won += 1,
            Outcome::OccLost => {}
            Outcome::Other(e) => return Err(format!("unexpected create outcome under race: {e}").into()),
        }
    }
    if won != 1 {
        return Err(format!("create race: {won} creators won (expected exactly 1)").into());
    }
    let mut c = CeleriantClient::connect(&addr).await?;
    let v = version(&mut c, &key).await?;
    if v != 1 {
        return Err(format!("after create race, version {v} != 1").into());
    }
    let total: usize = read_all(&mut c, &key).await?.iter().map(|b| b.events.len()).sum();
    if total != 1 {
        return Err(format!("after create race, {total} events present, expected 1").into());
    }
    Ok(())
}

/// 7.3 Concurrent idempotent retries of the SAME (client_id, client_seq): the
/// write lands exactly once no matter how many copies race. (idempotent-writes:
/// "the same write applied twice lands once"; the dedup key is the pair.) Models
/// a client that fired retries before the first ack came back.
pub async fn concurrent_idempotent_retries_dedupe() -> R {
    let server = TestServer::start_with_port(port_for("p7_idem_race")).await?;
    let addr = server.address().to_string();
    let key = unique_key("p7_idem_race");

    // Seed the aggregate's existence with a DIFFERENT writer so the racing retries
    // hit the pure idempotency path — not a create race. This isolates the dedup
    // guarantee: the losers must fail SPECIFICALLY with ClientIdempotencyViolation
    // (2002), proving idempotency (not OCC) produced "exactly one".
    {
        let mut c = CeleriantClient::connect(&addr).await?;
        c.write_events_with(key.clone(), vec![event(1, TYPE, 500, r#"{"seed":1}"#)],
            99, WriteEventsOptions { allow_create: true, ..Default::default() }).await?;
    }

    // N concurrent retries of the identical (client_id=7, client_seq=1) write
    // against the now-existing aggregate. Exactly one appends; the rest dedupe.
    let n = 16u64;
    let mut handles = Vec::new();
    for _ in 0..n {
        let addr = addr.clone();
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            let mut c = CeleriantClient::connect(&addr).await.map_err(|e| format!("connect: {e:?}"))?;
            let res = c
                .write_events_with(
                    key,
                    vec![event(1, TYPE, 1000, r#"{"v":1}"#)],
                    7, WriteEventsOptions { allow_create: false, enforce_client_idempotency: true, ..Default::default() },
                )
                .await;
            // The single winner appends (Ok); every other racer is deduped. A loser
            // is rejected with ClientIdempotencyViolation (2002) if the winner has
            // already committed, OR InflightDuplicateWrite (2013) if the winner's
            // identical write is still in flight when the duplicate arrives. Both
            // mean "deduped, no double-apply". OCC (2003) must NOT occur — the seed
            // already created the aggregate, so there is no create race.
            // NB (FINDINGS F10): 2013 fires here on a STANDALONE node, though the
            // docs frame it as a replication/failover-only code.
            match res {
                Ok(_) => Ok::<&'static str, String>("won"),
                Err(ClientError::Server(ServerError::Write {
                    kind: WriteError::ClientIdempotencyViolation { .. }
                        | WriteError::InflightDuplicateWrite { .. }, ..
                })) => Ok("deduped"),
                Err(e) => Err(format!("expected dedup (2002/2013) or win, got {e:?}")),
            }
        }));
    }
    let mut won = 0usize;
    let mut deduped = 0usize;
    for h in handles {
        match h.await.map_err(|e| format!("join: {e}"))?? {
            "won" => won += 1,
            _ => deduped += 1,
        }
    }
    if won != 1 {
        return Err(format!("idempotent race: {won} writers won (expected exactly 1), {deduped} deduped").into());
    }

    // Seed (1) + exactly one idempotent write = 2 events; no double-apply.
    let mut c = CeleriantClient::connect(&addr).await?;
    let total: usize = read_all(&mut c, &key).await?.iter().map(|b| b.events.len()).sum();
    if total != 2 {
        return Err(format!("idempotent retries: {total} events present, expected 2 (seed + one)").into());
    }
    Ok(())
}

/// 7.4 Per-aggregate ordering and isolation across many shards under concurrent
/// load. With several shards and many aggregates written concurrently, each
/// aggregate's events come back strictly ordered (1..K, gap-free) and carry only
/// that aggregate's data — no cross-aggregate bleed. (reads-and-ordering: "event
/// 5 is always event 5"; aggregates: "ordering is per aggregate";
/// consistency-boundaries: placement is `id % shard_count`.) The docs make no
/// cross-shard ordering promise, so we assert only the per-aggregate guarantee.
pub async fn per_aggregate_order_across_shards() -> R {
    let config = ServerConfig { num_shards: Some(8), standalone: true, ..Default::default() };
    let server = TestServer::start_with_config(port_for("p7_shard_order"), config).await?;
    let addr = server.address().to_string();

    // aggregate_id routing (default), 8 shards: ids 0..n_aggs spread across shards.
    let n_aggs = 24u64;
    let k = 10u64; // batches per aggregate
    let base = 700_000u128;

    let mut handles = Vec::new();
    for a in 0..n_aggs {
        let addr = addr.clone();
        let key = AggregateKey::new(1, 1, base + a as u128);
        handles.push(tokio::spawn(async move {
            let mut c = CeleriantClient::connect(&addr).await.map_err(|e| format!("connect: {e:?}"))?;
            for i in 1..=k {
                // Each event tagged with its aggregate and its sequence, so a
                // cross-aggregate bleed or reordering is detectable in the payload.
                let payload = format!("{{\"agg\":{a},\"seq\":{i}}}");
                c.write_events_with(
                    key.clone(),
                    vec![event(i, TYPE, 1000 + i, &payload)],
                    0, WriteEventsOptions { allow_create: i == 1, expected_version: Some(i - 1), ..Default::default() },
                )
                .await
                .map_err(|e| format!("agg {a} seq {i}: {e:?}"))?;
            }
            Ok::<(), String>(())
        }));
    }
    for h in handles {
        h.await.map_err(|e| format!("join: {e}"))??;
    }

    // Read each aggregate back and assert strict per-aggregate order + isolation.
    let mut c = CeleriantClient::connect(&addr).await?;
    for a in 0..n_aggs {
        let key = AggregateKey::new(1, 1, base + a as u128);
        let batches = read_all(&mut c, &key).await?;
        if batches.len() != k as usize {
            return Err(format!("agg {a}: {} batches, expected {k}", batches.len()).into());
        }
        for (idx, b) in batches.iter().enumerate() {
            let want = idx as u64 + 1;
            if b.aggregate_version != want {
                return Err(format!("agg {a}: batch {idx} version {} != {want} (order broken)", b.aggregate_version).into());
            }
            let payload = String::from_utf8_lossy(&b.events[0].event_value);
            let expect = format!("{{\"agg\":{a},\"seq\":{want}}}");
            if payload != expect {
                return Err(format!("agg {a}: batch {idx} payload {payload:?} != {expect:?} (bleed or reorder)").into());
            }
        }
    }
    Ok(())
}

/// 7.5 Large values across a long-lived stream read back intact and in order.
/// Many batches of multi-KB incompressible payloads — enough that the on-disk
/// log must grow well past a single inline minibatch — then a full paginated
/// read must return every batch once, in order, byte-for-byte. (reads-and-ordering
/// "gap-free and stable"; events-and-the-log; pagination streams the whole
/// aggregate.) We do not assume any on-disk layout; we just drive enough data
/// that the contract is the only thing holding.
pub async fn large_values_long_stream_intact() -> R {
    let server = TestServer::start_with_port(port_for("p7_large")).await?;
    let addr = server.address().to_string();
    let key = unique_key("p7_large");

    let mut c = CeleriantClient::connect(&addr).await?;
    let n = 60u64;
    let payload_len = 64 * 1024usize; // 64 KiB per event, incompressible

    // Build deterministic incompressible payloads we can verify byte-for-byte.
    let make_payload = |i: u64| -> Vec<u8> {
        let mut buf = vec![0u8; payload_len];
        crate::fill_incompressible(&mut buf, i.wrapping_mul(0x9E37_79B9));
        buf
    };

    for i in 1..=n {
        let payload = make_payload(i);
        let ev = DatablockAggregateEvent {
            client_seq: i,
            event_seq: 0,
            event_id: None,
            event_timestamp: 1000 + i,
            event_type_major: TYPE,
            event_type_minor: 0,
            event_value: Arc::new(payload),
            iv: None,
        };
        c.write_events_with(
            key.clone(),
            vec![ev],
            0, WriteEventsOptions { allow_create: i == 1, expected_version: Some(i - 1), ..Default::default() },
        )
        .await
        .map_err(|e| format!("write {i}: {e:?}"))?;
    }

    // Full paginated read: every batch once, in order, bytes intact.
    let batches = read_all(&mut c, &key).await?;
    if batches.len() != n as usize {
        return Err(format!("long stream: {} batches read, expected {n}", batches.len()).into());
    }
    for (idx, b) in batches.iter().enumerate() {
        let want = idx as u64 + 1;
        if b.aggregate_version != want {
            return Err(format!("long stream: batch {idx} version {} != {want}", b.aggregate_version).into());
        }
        if b.events.len() != 1 {
            return Err(format!("long stream: batch {idx} has {} events, expected 1", b.events.len()).into());
        }
        let got = b.events[0].event_value.as_slice();
        let expect = make_payload(want);
        if got != expect.as_slice() {
            return Err(format!("long stream: batch {idx} payload corrupted (len {} vs {})", got.len(), expect.len()).into());
        }
    }
    Ok(())
}
