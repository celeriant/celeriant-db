//! Shared helpers for the integration tests.
//!
//! Thin conveniences over the public client API. Nothing here encodes server
//! internals: events are plain `DatablockAggregateEvent`s and all assertions go
//! through `CeleriantClient`.

use std::error::Error;
use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::request::requests::ReadRequest;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

pub type R = Result<(), Box<dyn Error>>;

/// A distinct client port for each call, allocated sequentially so no two
/// servers in a run ever share a port (a name hash collides too easily across
/// ~50 tests, and a collision makes the second server race the first's socket in
/// TIME_WAIT). The `seed` is kept only for call-site readability. Base ports are
/// spaced 10 apart to clear each server's trio (replication = +1, metrics = +2),
/// and the per-run base is offset by pid so back-to-back suite runs don't reuse
/// a port still cooling down.
///
/// Each test is its own subprocess, so the pid offset strides a whole 10-slot
/// block: consecutive pids differ by 1, and a bare `pid + slot` would hand test
/// N+1 a port test N's server or MinIO container is still holding.
///
/// Every port must land below `EPHEMERAL_FLOOR`: a listener above it races the
/// kernel handing the same port to an outbound socket, and these tests open tens
/// of thousands. That ceiling caps how many pid blocks fit, so blocks do recycle
/// within a long run. A stale sibling is rarer than the ephemeral race it replaces.
pub fn port_for(_seed: &str) -> u16 {
    let slot = NEXT_PORT_SLOT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    debug_assert!(slot < SLOTS_PER_PROCESS, "test exceeded its {SLOTS_PER_PROCESS}-port block");
    let pid_off = (std::process::id() % PID_BLOCKS) as u16;
    let port = PORT_BASE + (pid_off * SLOTS_PER_PROCESS + slot) * PORT_STRIDE;
    debug_assert!(port < EPHEMERAL_FLOOR, "port {port} is in the kernel ephemeral range");
    port
}

/// Clear of the `1xxxx + pid % 100` bases the older hand-rolled tests still use.
const PORT_BASE: u16 = 20000;
/// `/proc/sys/net/ipv4/ip_local_port_range` low water mark on stock Linux.
const EPHEMERAL_FLOOR: u16 = 32768;
const PORT_STRIDE: u16 = 10;
const SLOTS_PER_PROCESS: u16 = 10;
/// Largest value keeping `PORT_BASE + (PID_BLOCKS * SLOTS_PER_PROCESS) * PORT_STRIDE`
/// under `EPHEMERAL_FLOOR`.
const PID_BLOCKS: u32 = 127;

static NEXT_PORT_SLOT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

/// A unique aggregate key per test name, so concurrently-run tests on a shared
/// server never collide. The three id parts derive from the test name hash.
pub fn unique_key(seed: &str) -> AggregateKey {
    let h = fnv(seed);
    AggregateKey::new(h as u128 + 1, (h.rotate_left(21)) as u128 + 1, (h.rotate_left(42)) as u128 + 1)
}

fn fnv(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Build a JSON event with explicit client_seq / type / timestamp.
pub fn event(client_seq: u64, type_major: u64, event_timestamp: u64, payload: &str) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq,
        event_seq: 0,
        event_id: None,
        event_timestamp,
        event_type_major: type_major,
        event_type_minor: 0,
        event_value: Arc::new(payload.as_bytes().to_vec()),
        iv: None,
    }
}

/// Read every batch for an aggregate, following the pagination cursor.
pub async fn read_all(
    client: &mut CeleriantClient,
    key: &AggregateKey,
) -> Result<Vec<AggregateEventBatch>, Box<dyn Error>> {
    let mut out = Vec::new();
    let mut from = 1u64;
    loop {
        let resp = client
            .read(ReadRequest {
                correlation_id: None,
                aggregate_key: key.clone(),
                filters: ReadFilters::new(from),
            })
            .await?;
        out.extend(resp.event_batches);
        match resp.next_aggregate_version {
            Some(next) => from = next,
            None => return Ok(out),
        }
    }
}

/// Like `read_all` but starting from an explicit aggregate version.
pub async fn read_all_from(
    client: &mut CeleriantClient,
    key: &AggregateKey,
    start: u64,
) -> Result<Vec<AggregateEventBatch>, Box<dyn Error>> {
    let mut out = Vec::new();
    let mut from = start.max(1);
    loop {
        let resp = client
            .read(ReadRequest {
                correlation_id: None,
                aggregate_key: key.clone(),
                filters: ReadFilters::new(from),
            })
            .await?;
        out.extend(resp.event_batches);
        match resp.next_aggregate_version {
            Some(next) => from = next,
            None => return Ok(out),
        }
    }
}

/// Flatten all events across batches in WAL order.
pub fn flatten(batches: &[AggregateEventBatch]) -> Vec<DatablockAggregateEvent> {
    batches.iter().flat_map(|b| b.events.clone()).collect()
}
