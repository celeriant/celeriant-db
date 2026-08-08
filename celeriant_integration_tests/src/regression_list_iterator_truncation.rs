//! Regression: shard-listing iterators must not truncate on a failed discovery probe.
//!
//! Guards the fix in `celeriant_client_tokio/src/list_operations.rs` for
//! [BUG-list-iterators-truncate.md]. The iterator discovers its shard range by probing one
//! speculative shard id past the highest it has seen. Before the fix, a routing error from that
//! probe ended the whole walk, silently discarding every shard still mid-pagination and returning
//! `Ok(prefix)` — a wrong answer with no error. Six iterators shared the defect and three of them
//! sit behind the CLI's `list` commands.
//!
//! WHY THIS TEST LOOKS THE WAY IT DOES. Two properties are load-bearing and neither is incidental:
//!
//! 1. **The listing must span more than two pages, and `list_page_size` ALONE WILL NOT DO IT.**
//!    `shard_wal.rs:list_aggregates` returns the whole active segment summary on the first page —
//!    its own comment says so: *"the first page returns up to the active segment's size; every
//!    later page is capped at page_size"*. Only SEALED segments are paged. So on a dataset that
//!    never rotates the log, every shard answers in one page with no cursor, the discovery probe
//!    finds nothing left mid-pagination, and the bug is invisible however small `list_page_size`
//!    is. This was verified the hard way: a first version of this test set `list_page_size = 20`
//!    against 300 small aggregates and **passed against deliberately re-broken code**. The log must
//!    be forced to rotate — hence the 2 MB segment and the large payloads below.
//!
//! 2. **It must use the ITERATOR, not manual cursor pagination.** The pre-existing
//!    `edge_list_pagination_cache_eviction` test paginates by hand with raw `ListAggregatesRequest`
//!    plus a cursor, which never runs the discovery-probe code path at all. A test can exercise
//!    pagination thoroughly and still be blind to this bug.
//!
//! The assertion is three-way rather than a bare count, so a failure says which half is wrong:
//! ground truth (what was written), the discovery listing (probing, the buggy path), and a
//! `max_shard_hint` listing (which suppresses the probe entirely). If discovery is short while
//! hinted is complete, the client iterator truncated. If both are short, the server or the write
//! path lost data and this test is pointing at a different bug.
//!
//! Run with: cargo run --bin celeriant-integration-tests -- --test regression_list_iterator_truncation

use std::collections::HashSet;

use crate::{write_event, write_large_event, ServerConfig, TestServer};
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::list_operations::{ListAggregatesIterator, ListOptions};
use celeriant_wal::aggregate_key::AggregateKey;

const PORT_BASE: u16 = 18900;
const NUM_SHARDS: usize = 2;
const NUM_AGGREGATES: u128 = 300;
const LIST_PAGE_SIZE: u64 = 20;
/// Small enough that the writes below seal several segments. Sealed segments are the only ones
/// that paginate, and pagination is the whole point of the test.
const SEGMENT_BYTES: u64 = 2 * 1024 * 1024;
/// 300 x 24 KB is ~7 MB, so ~3 sealed segments per shard on top of the active one.
const PAYLOAD_BYTES: usize = 24 * 1024;
const ORG_ID: u128 = 1;
const AGG_TYPE_ID: u128 = 1;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Regression: list iterator truncation on discovery probe ===\n");

    let port = PORT_BASE + (std::process::id() % 100) as u16;
    let config = ServerConfig {
        num_shards: Some(NUM_SHARDS),
        standalone: true,
        log_level: "warn".to_string(),
        // Default routing is AggregateId, so distinct ids spread over both shards and the walk has
        // more than one live cursor when the probe fails -- which is what the fix's `retain` has to
        // preserve.
        list_page_size: LIST_PAGE_SIZE,
        shard_log_preallocate_bytes: SEGMENT_BYTES,
        // Full log scans on sealed segments are slower than the default allows.
        list_max_duration_ms: 10_000,
        ..Default::default()
    };

    println!("Starting {NUM_SHARDS}-shard standalone server on port {port} (list_page_size={LIST_PAGE_SIZE})...");
    let server = TestServer::start_with_config_labeled(port, config, "standalone".into()).await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    println!("Writing {NUM_AGGREGATES} aggregates with {PAYLOAD_BYTES}-byte payloads to force rotation...");
    let mut expected: HashSet<u128> = HashSet::new();
    for i in 1..=NUM_AGGREGATES {
        let key = AggregateKey::new(ORG_ID, AGG_TYPE_ID, i);
        // write_large_event does not create, so the aggregate has to exist first.
        write_event(&mut client, &key, 1, true).await?;
        write_large_event(&mut client, &key, 2, PAYLOAD_BYTES).await?;
        expected.insert(i);
    }
    let mb = NUM_AGGREGATES as usize * PAYLOAD_BYTES / (1024 * 1024);
    println!("  {NUM_AGGREGATES} written, ~{mb} MB over {SEGMENT_BYTES}-byte segments");
    println!("  sealed segments paginate at {LIST_PAGE_SIZE}/page, so each shard needs many fetches\n");

    // The listing under test: shard range discovered by probing, which is the path that could
    // truncate.
    let discovery: HashSet<u128> =
        ListAggregatesIterator::new(&mut client, Some(ORG_ID), Some(AGG_TYPE_ID), ListOptions::default())
            .collect()
            .await?
            .into_iter()
            .map(|a| a.aggregate_id)
            .collect();

    // The control: `max_shard_hint` suppresses the probe entirely, so this path cannot hit the bug.
    let max_shard = (NUM_SHARDS - 1) as u64;
    let hinted_options = ListOptions { max_shard_hint: Some(max_shard), ..Default::default() };
    let hinted: HashSet<u128> =
        ListAggregatesIterator::new(&mut client, Some(ORG_ID), Some(AGG_TYPE_ID), hinted_options)
            .collect()
            .await?
            .into_iter()
            .map(|a| a.aggregate_id)
            .collect();

    println!("  ground truth      {}", expected.len());
    println!("  discovery listing {}", discovery.len());
    println!("  hinted listing    {} (max_shard_hint={max_shard})\n", hinted.len());

    let mut failures: Vec<String> = Vec::new();

    if discovery != expected {
        let missing = expected.difference(&discovery).count();
        let extra = discovery.difference(&expected).count();
        failures.push(format!(
            "discovery listing wrong: {} missing, {} unexpected (got {} of {})",
            missing,
            extra,
            discovery.len(),
            expected.len()
        ));
    }

    if hinted != expected {
        failures.push(format!(
            "hinted listing wrong: got {} of {} -- the probe is suppressed on this path, so this \
             points at the server or the write path, NOT at the iterator",
            hinted.len(),
            expected.len()
        ));
    }

    // The invariant the fix restores: whether or not the range is known in advance changes how many
    // requests are made, never what is returned.
    if discovery != hinted {
        failures.push(format!(
            "discovery and hinted listings disagree ({} vs {}) -- shard discovery is not \
             answer-preserving, which is the defect this test guards",
            discovery.len(),
            hinted.len()
        ));
    }

    if !failures.is_empty() {
        for f in &failures {
            println!("FAIL: {f}");
        }
        return Err(format!("{} assertion(s) failed", failures.len()).into());
    }

    println!("PASS: all three listings agree at {} aggregates", expected.len());
    Ok(())
}
