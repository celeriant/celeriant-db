//! Phase 1 — append, read, ordering, basic write validation, durability.
//!
//! Oracle: celeriant-docs/docs/concepts/{events-and-the-log,reads-and-ordering,
//! aggregates,durability-and-safety}.md and reference/{error-codes,limits-defaults}.md.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{ReadError, ServerError, WriteError};
use celeriant_msg::request::requests::AggregateDetailsRequest;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;
use crate::{ServerConfig, TestServer};

use crate::common::{event, flatten, port_for, read_all, unique_key, R};

const TYPE: u64 = 100;

/// 1.1 An appended event reads back byte-identical: payload, type, and the
/// CLIENT's event_timestamp are preserved (events-and-the-log: "the timestamp
/// you stamp is the timestamp it keeps").
pub async fn append_reads_back_unchanged() -> R {
    let server = TestServer::start_with_port(port_for("append_reads_back_unchanged")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("append_reads_back_unchanged");

    let ev = event(1, TYPE, 4242, r#"{"hello":"world"}"#);
    c.write_events(key.clone(), vec![ev.clone()]).await?;

    let batches = read_all(&mut c, &key).await?;
    let events = flatten(&batches);
    if events.len() != 1 {
        return Err(format!("expected 1 event, got {}", events.len()).into());
    }
    let got = &events[0];
    if got.event_value.as_slice() != ev.event_value.as_slice() {
        return Err("payload was altered".into());
    }
    if got.event_type_major != TYPE {
        return Err(format!("event_type_major changed to {}", got.event_type_major).into());
    }
    if got.event_timestamp != 4242 {
        return Err(format!("server rewrote client event_timestamp to {}", got.event_timestamp).into());
    }
    Ok(())
}

/// 1.2 Events come back in strict, gap-free order with stable 1-based indices
/// (reads-and-ordering: "event 5 is always event 5 ... gap-free and stable").
pub async fn reads_are_ordered_and_gap_free() -> R {
    let server = TestServer::start_with_port(port_for("reads_are_ordered_and_gap_free")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("reads_are_ordered_and_gap_free");

    let n = 25u64;
    for i in 1..=n {
        c.write_events(key.clone(), vec![event(i, TYPE, 1000 + i, &format!("{{\"n\":{i}}}"))])
            .await?;
    }
    let batches = read_all(&mut c, &key).await?;
    if batches.len() as u64 != n {
        return Err(format!("expected {n} batches, got {}", batches.len()).into());
    }
    for (idx, b) in batches.iter().enumerate() {
        let expected_version = idx as u64 + 1;
        if b.aggregate_version != expected_version {
            return Err(format!(
                "batch {idx}: aggregate_version {} != {expected_version} (gap or reorder)",
                b.aggregate_version
            )
            .into());
        }
        let payload = String::from_utf8_lossy(&b.events[0].event_value);
        if payload != format!("{{\"n\":{expected_version}}}") {
            return Err(format!("batch {idx}: payload {payload} out of order").into());
        }
    }
    Ok(())
}

/// 1.3 The aggregate version is the 1-based latest batch index; AggregateDetails
/// reports it (events-and-the-log "Batches and the version"; limits-defaults).
pub async fn version_tracks_batch_count() -> R {
    let server = TestServer::start_with_port(port_for("version_tracks_batch_count")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("version_tracks_batch_count");

    let k = 7u64;
    for i in 1..=k {
        c.write_events(key.clone(), vec![event(i, TYPE, 1000 + i, "{}")]).await?;
    }
    let details = c
        .aggregate_details(AggregateDetailsRequest { correlation_id: None, aggregate_key: key.clone() })
        .await?;
    if details.max_aggregate_version != k {
        return Err(format!("max_aggregate_version {} != {k}", details.max_aggregate_version).into());
    }
    Ok(())
}

/// 1.4 Reading a non-existent aggregate -> AggregateNotExists (error 1001).
pub async fn read_missing_aggregate_errors() -> R {
    let server = TestServer::start_with_port(port_for("read_missing_aggregate_errors")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("read_missing_aggregate_errors");

    let res = c
        .read(ReadRequest { correlation_id: None, aggregate_key: key, filters: ReadFilters::new(1) })
        .await;
    match res {
        Err(ClientError::Server(ServerError::Read { kind: ReadError::AggregateNotExists, .. })) => Ok(()),
        other => Err(format!("expected ReadError::AggregateNotExists, got {other:?}").into()),
    }
}

/// 1.5 A write carrying no events -> EmptyEventsList (error 2000).
pub async fn empty_write_rejected() -> R {
    let server = TestServer::start_with_port(port_for("empty_write_rejected")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("empty_write_rejected");

    let res = c.write_events(key, vec![]).await;
    match res {
        Err(ClientError::Server(ServerError::Write { kind: WriteError::EmptyEventsList, .. })) => Ok(()),
        other => Err(format!("expected WriteError::EmptyEventsList, got {other:?}").into()),
    }
}

/// 1.6 An event with event_type_major == 0 is reserved -> ZeroEventType (2001).
pub async fn zero_event_type_rejected() -> R {
    let server = TestServer::start_with_port(port_for("zero_event_type_rejected")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("zero_event_type_rejected");

    let res = c.write_events(key, vec![event(1, 0, 1000, "{}")]).await;
    match res {
        Err(ClientError::Server(ServerError::Write { kind: WriteError::ZeroEventType, .. })) => Ok(()),
        other => Err(format!("expected WriteError::ZeroEventType, got {other:?}").into()),
    }
}

/// 1.7 Write to a missing aggregate without allow_create -> AggregateNotExists (2005).
pub async fn write_no_create_rejected() -> R {
    let server = TestServer::start_with_port(port_for("write_no_create_rejected")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("write_no_create_rejected");

    let res = c
        .write_events_with(
            key,
            vec![event(1, TYPE, 1000, "{}")],
            WriteEventsOptions { allow_create: false, ..Default::default() },
        )
        .await;
    match res {
        Err(ClientError::Server(ServerError::Write { kind: WriteError::AggregateNotExists, .. })) => Ok(()),
        other => Err(format!("expected WriteError::AggregateNotExists, got {other:?}").into()),
    }
}

/// 1.8 Pagination: an aggregate too large for one response is fully streamed by
/// following the next_aggregate_version cursor, with every batch returned
/// exactly once and in order (reads-and-ordering: "A read returns a cursor (the
/// next batch index); pass it back to continue. You stream a million-event
/// aggregate in bounded memory instead of loading it whole.").
///
/// We shrink the server's max response size so a modest stream must span
/// several pages, rather than writing gigabytes.
pub async fn pagination_streams_whole_aggregate() -> R {
    let config = ServerConfig {
        num_shards: Some(1),
        standalone: true,
        // A bounded response (bounded-memory reads are documented). The docs do
        // not define what exactly bounds one page (FINDINGS F1), so this is only
        // a pragmatic lever to make a multi-page read likely — we assert the
        // documented cursor contract, not a page count or a specific threshold.
        max_response_size: 1024 * 1024,
        ..Default::default()
    };
    let server = TestServer::start_with_config(port_for("pagination_streams_whole_aggregate"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("pagination_streams_whole_aggregate");

    // The guarantee under test is the cursor contract (reads-and-ordering
    // "Pagination"): following next_aggregate_version streams the whole
    // aggregate, every batch exactly once, in order. A large aggregate (40 sizable
    // batches) makes paging likely; whether it pages is observed, not required.
    let n = 40u64;
    for i in 1..=n {
        let mut blob = vec![0u8; 64 * 1024];
        crate::fill_incompressible(&mut blob, i);
        let ev = celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent {
            client_seq: i,
            event_seq: 0,
            event_id: None,
            event_timestamp: 1000 + i,
            event_type_major: TYPE,
            event_type_minor: 0,
            event_value: std::sync::Arc::new(blob),
            iv: None,
        };
        c.write_events(key.clone(), vec![ev]).await?;
    }

    let mut seen = Vec::new();
    let mut pages = 0u32;
    let mut from = 1u64;
    loop {
        let resp = c
            .read(ReadRequest { correlation_id: None, aggregate_key: key.clone(), filters: ReadFilters::new(from) })
            .await?;
        pages += 1;
        for b in &resp.event_batches {
            seen.push(b.aggregate_version);
        }
        match resp.next_aggregate_version {
            Some(next) => {
                if next <= from {
                    return Err(format!("cursor did not advance: from={from} next={next}").into());
                }
                from = next;
            }
            None => break,
        }
    }
    // Page count is observed, not asserted (the threshold is undocumented, F1).
    if pages < 2 {
        eprintln!("    note: read returned in {pages} page(s); cursor contract still verified");
    }
    let expected: Vec<u64> = (1..=n).collect();
    if seen != expected {
        return Err(format!("paged versions {seen:?} != contiguous 1..={n}").into());
    }
    Ok(())
}

/// 1.9 Offset filter from/to returns only the requested inclusive range
/// (reads-and-ordering "by offset"; read_filters doc-comments).
pub async fn offset_filter_bounds_range() -> R {
    let server = TestServer::start_with_port(port_for("offset_filter_bounds_range")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("offset_filter_bounds_range");

    for i in 1..=10u64 {
        c.write_events(key.clone(), vec![event(i, TYPE, 1000 + i, "{}")]).await?;
    }
    let resp = c
        .read(ReadRequest {
            correlation_id: None,
            aggregate_key: key.clone(),
            filters: ReadFilters::new(3).to_aggregate_version(6),
        })
        .await?;
    let versions: Vec<u64> = resp.event_batches.iter().map(|b| b.aggregate_version).collect();
    if versions != vec![3, 4, 5, 6] {
        return Err(format!("offset filter 3..=6 returned {versions:?}").into());
    }
    Ok(())
}

/// 1.10 include_event_types returns only the named types (reads-and-ordering
/// "by event type").
pub async fn event_type_filter() -> R {
    let server = TestServer::start_with_port(port_for("event_type_filter")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("event_type_filter");

    // Alternate two event types across 6 batches.
    for i in 1..=6u64 {
        let t = if i % 2 == 0 { 200 } else { 100 };
        c.write_events(key.clone(), vec![event(i, t, 1000 + i, "{}")]).await?;
    }
    let resp = c
        .read(ReadRequest {
            correlation_id: None,
            aggregate_key: key.clone(),
            filters: ReadFilters::new(1).include_event_types(vec![200]),
        })
        .await?;
    let events = flatten(&resp.event_batches);
    if events.is_empty() {
        return Err("type filter returned nothing".into());
    }
    if !events.iter().all(|e| e.event_type_major == 200) {
        return Err("type filter returned a non-200 event".into());
    }
    // There were 3 even-indexed (type 200) batches.
    if events.len() != 3 {
        return Err(format!("expected 3 type-200 events, got {}", events.len()).into());
    }
    Ok(())
}

/// 1.11 Writer filter: include_client_id returns only that client's batches
/// (reads-and-ordering "by writer").
pub async fn client_id_filter() -> R {
    let server = TestServer::start_with_port(port_for("client_id_filter")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("client_id_filter");

    // client 11 creates and writes seq 1, client 22 writes seq 1 (its own space).
    c.write_events_with(
        key.clone(),
        vec![event(1, TYPE, 1000, r#"{"w":"a"}"#)],
        WriteEventsOptions { client_id: 11, allow_create: true, ..Default::default() },
    )
    .await?;
    c.write_events_with(
        key.clone(),
        vec![event(1, TYPE, 1001, r#"{"w":"b"}"#)],
        WriteEventsOptions { client_id: 22, allow_create: false, ..Default::default() },
    )
    .await?;

    let resp = c
        .read(ReadRequest {
            correlation_id: None,
            aggregate_key: key.clone(),
            filters: ReadFilters::new(1).include_client_id(22),
        })
        .await?;
    if resp.event_batches.len() != 1 {
        return Err(format!("include_client_id=22 returned {} batches", resp.event_batches.len()).into());
    }
    if resp.event_batches[0].client_id != 22 {
        return Err(format!("filtered batch has client_id {}", resp.event_batches[0].client_id).into());
    }
    Ok(())
}

/// 1.12 Durability: writes acknowledged before a clean restart are still there
/// after the server comes back on the same data dir
/// (durability-and-safety: "an acknowledged write is on stable storage").
pub async fn writes_survive_restart() -> R {
    let mut server = TestServer::start_with_port(port_for("writes_survive_restart")).await?;
    let key = unique_key("writes_survive_restart");

    {
        let mut c = CeleriantClient::connect(server.address()).await?;
        for i in 1..=5u64 {
            c.write_events(key.clone(), vec![event(i, TYPE, 1000 + i, &format!("{{\"n\":{i}}}"))])
                .await?;
        }
    }

    server.stop();
    // Let the OS release the listening socket before the same port is rebound;
    // otherwise restart() can race the dying process and fail to bind.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    server.restart().await?;

    let mut c = CeleriantClient::connect(server.address()).await?;
    let batches = read_all(&mut c, &key).await?;
    if batches.len() != 5 {
        return Err(format!("after restart expected 5 batches, got {}", batches.len()).into());
    }
    for (idx, b) in batches.iter().enumerate() {
        let want = idx as u64 + 1;
        if b.aggregate_version != want {
            return Err(format!("after restart batch {idx} version {} != {want}", b.aggregate_version).into());
        }
        let payload = String::from_utf8_lossy(&b.events[0].event_value);
        if payload != format!("{{\"n\":{want}}}") {
            return Err(format!("after restart payload {payload} corrupted").into());
        }
    }
    Ok(())
}

/// 1.13 Writer filter, exclude side: exclude_client_id drops that client's
/// batches and keeps the rest (reads-and-ordering "by writer": "include or
/// exclude a given client's events"). Complements 1.11 (include).
pub async fn exclude_client_id_filter() -> R {
    let server = TestServer::start_with_port(port_for("exclude_client_id_filter")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("exclude_client_id_filter");

    c.write_events_with(key.clone(), vec![event(1, TYPE, 1000, r#"{"w":"a"}"#)],
        WriteEventsOptions { client_id: 11, allow_create: true, ..Default::default() }).await?;
    c.write_events_with(key.clone(), vec![event(1, TYPE, 1001, r#"{"w":"b"}"#)],
        WriteEventsOptions { client_id: 22, allow_create: false, ..Default::default() }).await?;

    let resp = c.read(ReadRequest {
        correlation_id: None,
        aggregate_key: key.clone(),
        filters: ReadFilters::new(1).exclude_client_id(22),
    }).await?;
    if resp.event_batches.len() != 1 {
        return Err(format!("exclude_client_id=22 returned {} batches, expected 1", resp.event_batches.len()).into());
    }
    if resp.event_batches[0].client_id != 11 {
        return Err(format!("excluded the wrong writer: kept client_id {}", resp.event_batches[0].client_id).into());
    }
    Ok(())
}

/// 1.14 Multiple events written together land in one batch at a single
/// aggregate_version, and are read back in the order they were written
/// (events-and-the-log "Events written together land in a batch ... ordered
/// within the batch").
pub async fn multi_event_batch_preserves_order() -> R {
    let server = TestServer::start_with_port(port_for("multi_event_batch_preserves_order")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("multi_event_batch_preserves_order");

    // One write carrying three events => one batch.
    c.write_events(key.clone(), vec![
        event(1, TYPE, 1001, r#"{"i":1}"#),
        event(2, TYPE, 1002, r#"{"i":2}"#),
        event(3, TYPE, 1003, r#"{"i":3}"#),
    ]).await?;

    let batches = read_all(&mut c, &key).await?;
    if batches.len() != 1 {
        return Err(format!("three events written together produced {} batches, expected 1", batches.len()).into());
    }
    if batches[0].aggregate_version != 1 {
        return Err(format!("single batch has aggregate_version {}, expected 1", batches[0].aggregate_version).into());
    }
    let seqs: Vec<u64> = batches[0].events.iter().map(|e| e.client_seq).collect();
    if seqs != vec![1, 2, 3] {
        return Err(format!("intra-batch order {seqs:?} != [1,2,3]").into());
    }
    Ok(())
}

/// 1.15 Time filter: event_time_range returns only events whose event_timestamp
/// falls in the range (reads-and-ordering "by time"). The fourth documented read
/// filter dimension.
pub async fn event_time_range_filter() -> R {
    let server = TestServer::start_with_port(port_for("event_time_range_filter")).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("event_time_range_filter");

    // event_timestamps 1000,1010,1020,1030,1040 across 5 batches.
    for i in 0..5u64 {
        c.write_events_with(key.clone(), vec![event(i + 1, TYPE, 1000 + i * 10, "{}")],
            WriteEventsOptions { allow_create: i == 0, ..Default::default() }).await?;
    }
    // Range [1010, 1030] inclusive should select the middle three.
    let resp = c.read(ReadRequest {
        correlation_id: None,
        aggregate_key: key.clone(),
        filters: ReadFilters::new(1).event_time_range(1010, 1030),
    }).await?;
    let ts: Vec<u64> = flatten(&resp.event_batches).iter().map(|e| e.event_timestamp).collect();
    if ts.iter().any(|t| *t < 1010 || *t > 1030) {
        return Err(format!("event_time_range(1010,1030) returned out-of-range timestamps: {ts:?}").into());
    }
    if ts.is_empty() {
        return Err("event_time_range returned nothing".into());
    }
    Ok(())
}
