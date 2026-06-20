//! LEADER-side replication batch build/send path benchmark.
//!
//! ## What this benchmark measures (and what it does NOT)
//!
//! The leader-side replication-batch code in `shard_wal_replicate.rs`
//! (`snapshot_to_batch_items`, `validate_chain_or_err`, `tcp_send_snapshot`,
//! `single_send`, the S3 serialize in `replicate_to_s3`) is entirely private
//! (`fn`, not `pub`). There is no public, callable entry point to build or
//! serialize a replication batch in isolation, and the structures it consumes
//! (`PendingCommitData`, populated `MemCache`) cannot be constructed standalone
//! from a bench crate without a live `ShardWal`.
//!
//! The ONLY honest way to exercise the leader batch-build pipeline from outside
//! the crate is to drive it the way production does: open a `ShardWal` whose
//! `node_status` is `Leader`, then call `write()`. A leader write runs:
//!
//!   write() -> sync_durable() (fsync) -> replicate_durable_leader()
//!     -> capture_replication_snapshot()         [drain pending queue]
//!     -> commit_replication() -> replicate_loop()
//!       -> tcp_send_snapshot() -> snapshot_to_batch_items()   <-- BATCH BUILD
//!         (clone metablock + datablock per item into ReplicationBatchItem)
//!       -> single_send() -> ReplicationClient::replicate_to_follower()
//!
//! So this is **Option (a)**: measure leader write throughput and contrast it
//! with standalone (no replication) write throughput. The DELTA between the two
//! is the cost a leader pays for the replication-batch pipeline.
//!
//! ### HONEST LIMITATION — read this before trusting the numbers
//!
//! Benches use `StubReplicationClient`, whose `replicate_to_follower` does a
//! **fixed 30ms `sleep`** and returns Ok (replication_client.rs:50-53). That
//! synthetic network latency, plus the 17ms `replication_delay` coalescing
//! window in the coordinator, DOMINATE the wall-clock of any leader write. The
//! actual CPU cost of building the batch (cloning N `ReplicationBatchItem`s and
//! validating the intra-batch chain) is small relative to that fixed sleep.
//!
//! Therefore:
//!   * The leader-vs-standalone DELTA is real and meaningful as "what the
//!     replication pipeline costs you per write under this stub", but it is
//!     gated by the stub's 30ms sleep, NOT by serialization/clone CPU. You are
//!     measuring the pipeline being driven end-to-end, with a synthetic network.
//!   * The batch-size sweep (events-per-write / aggregates-per-write) is the
//!     genuinely informative axis for build cost: at a fixed write count, more
//!     events/aggregates per write means more bytes fsynced and more items
//!     cloned into the batch, so the marginal cost there reflects real
//!     build/clone/fsync work rather than the constant sleep.
//!
//! A faithful microbenchmark of *just* `snapshot_to_batch_items` +
//! serialization would require either making those helpers `pub(crate)` and
//! adding an in-crate bench, or a real 2-node TCP harness. Neither is in scope
//! (the task constrains edits to this one file). This is the closest honest
//! proxy obtainable from outside the crate.

use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use celeriant_distributed::node_status::NodeStatus;
use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use celeriant_shard::internal_shard_config::InternalShardConfig;
use celeriant_shard::replication_client::StubReplicationClient;
use celeriant_shard::s3_downloader::StubS3Downloader;
use celeriant_shard::shard_wal::ShardWal;
use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use glommio::timer::sleep;
use glommio::{LocalExecutorBuilder, Placement};
use tempfile::tempdir;

criterion_group!(
    benches,
    bench_leader_vs_standalone,
    bench_leader_batch_size,
);
criterion_main!(benches);

// =============================================================================
// CONFIGURATION
// =============================================================================

const EVENT_SIZE_BYTES: usize = 256;
const EVENTS_PER_WRITE: usize = 5;

/// Total writes per benchmark iteration. Kept modest because a leader write is
/// gated by the stub's 30ms `replicate_to_follower` sleep coalesced over the
/// 17ms `replication_delay` window; too many writes makes each iteration
/// multi-second.
const TOTAL_WRITES: usize = 800;

/// Writes are submitted in waves to give the replication coordinator a chance
/// to coalesce multiple pending writes into a single batch send. Kept small so
/// the bounded in-flight replication queue (`internode_max_request_size`) does
/// not overflow under the 30ms-per-send stub drain rate at wide batch breadth.
const WRITES_PER_WAVE: usize = 20;

/// Delay between waves (NOT included in the measured time). Wide enough to let
/// the stub drain a wave's replication batch before the next wave piles on,
/// keeping the in-flight queue below the backpressure bound.
const INTER_WAVE_DELAY: Duration = Duration::from_millis(35);

/// Number of distinct aggregates writes are spread across (multi-aggregate).
const NBR_AGGREGATES: usize = 5000;

const SEGMENT_SIZE_BYTES: u64 = 128 * 1024 * 1024;

/// epoch ms now — used to give a leader lease a comfortably-future expiry so it
/// never decays to Fenced mid-benchmark.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// A leader node status with a 30s lease — stable for the whole bench run.
fn leader_status() -> ValidatedNodeStatus {
    ValidatedNodeStatus::create_custom_status(
        NodeStatus::Leader { lease_epoch: 1 },
        500,
        now_ms() + 30_000,
    )
}

// =============================================================================
// HELPERS
// =============================================================================

// base_config copied verbatim from write_benchmark.rs::create_config (the
// current canonical InternalShardConfig shape for benches).
fn create_config(
    shard_dir: PathBuf,
    fsync_delay: Duration,
    recent_write_cache_bytes: u64,
) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        max_open_files: 256,
        shard_log_preallocate_bytes: SEGMENT_SIZE_BYTES,
        fsync_delay,
        replication_delay: Duration::from_millis(17),
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::from_millis(500),
        heartbeat_starve_threshold: Duration::ZERO,
        recent_write_cache_bytes,
        shard_dir,
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes: 1024,
        timestamp_config: TimestampConfig::default(),
        list_max_duration: Duration::from_millis(2000),
        list_page_size: 20000,
        list_max_concurrent: 16,
        read_max_concurrent: 64,
        schema_cache_bytes: 4 * 1024 * 1024,
        max_schema_size_bytes: 16384,
        max_catchup_gap_bytes: Some(104_857_600),
        max_promotion_batch_bytes: None,
        max_clock_drift_ms: 500,
        shard_id: 1,
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_compaction"),
        cache_warmup_max_duration: Duration::MAX,
        wal_compression_level: 3,
        dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

fn create_events(count: usize, size: usize, base_index: u64) -> Vec<DatablockAggregateEvent> {
    (0..count)
        .map(|i| DatablockAggregateEvent {
            client_seq: base_index + i as u64,
            event_seq: 0,
            event_id: None,
            event_timestamp: 1_700_000_000_000 + i as u64,
            event_type_major: 1,
            event_type_minor: 0,
            event_value: Arc::new(vec![0xABu8; size]),
            iv: None,
        })
        .collect()
}

/// A write spanning `aggregates` distinct aggregate keys, each with
/// `events_per_agg` events. The number of items cloned into the replication
/// batch scales with `aggregates` (one metablock+datablock per aggregate write).
fn create_write_request(
    base_agg: u128,
    aggregates: usize,
    events_per_agg: usize,
    base_index: u64,
    client_id: u128,
) -> WriteRequest {
    let mut writes = HashMap::new();
    for a in 0..aggregates {
        let key = AggregateKey::new(1, 1, base_agg.wrapping_add(a as u128));
        writes.insert(
            key,
            SingleAggregateWrite {
                events: create_events(events_per_agg, EVENT_SIZE_BYTES, base_index),
                allow_create: true,
                expected_version: None,
                enforce_client_idempotency: false,
            },
        );
    }
    WriteRequest {
        correlation_id: None,
        client_id,
        user_id: None,
        writes,
    }
}

/// Run `TOTAL_WRITES` multi-aggregate writes in waves against a freshly-opened
/// shard with the given node status, returning the per-write average latency.
///
/// `events_per_write` / `aggregates_per_write` control batch size. fsync_delay
/// and replication coalescing windows come from `create_config`.
fn run_write_workload(
    status: ValidatedNodeStatus,
    events_per_write: usize,
    aggregates_per_write: usize,
) -> Duration {
    let tempdir = tempdir().unwrap();
    let shard_dir = tempdir.path().to_path_buf();

    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let config = create_config(shard_dir, Duration::from_millis(10), 64 * 1024 * 1024);
            let shard_wal = Rc::new(
                ShardWal::open(config, status, StubReplicationClient, StubS3Downloader)
                    .await
                    .unwrap(),
            );

            let num_waves = TOTAL_WRITES / WRITES_PER_WAVE;
            let mut cumulative = Duration::ZERO;

            // Each wave is launched concurrently, then awaited before the next
            // wave starts. This bounds the in-flight replication queue so it
            // stays below `internode_max_request_size` (the backpressure bound)
            // even at wide batch breadth, while still letting the coordinator
            // coalesce a whole wave into batched sends.
            for wave in 0..num_waves {
                let mut wave_handles = Vec::with_capacity(WRITES_PER_WAVE);
                for i in 0..WRITES_PER_WAVE {
                    let write_id = wave * WRITES_PER_WAVE + i;
                    // Spread the leading aggregate so multi-agg writes don't all
                    // collide on the same key range (which would serialize on
                    // per-aggregate locks rather than exercise batch breadth).
                    let base_agg =
                        ((write_id * aggregates_per_write) % NBR_AGGREGATES) as u128;
                    let shard_wal = shard_wal.clone();

                    let handle = glommio::spawn_local(async move {
                        let req = create_write_request(
                            base_agg,
                            aggregates_per_write,
                            events_per_write,
                            (write_id * events_per_write) as u64,
                            write_id as u128,
                        );
                        // Measure only the accepted write. A ReplicationBackpressure
                        // rejection is a fast no-batch-built path (the queue is full);
                        // a real client retries, so we retry without counting the
                        // rejected attempt's time, keeping the measurement on the
                        // batch-build path rather than the bound.
                        loop {
                            let start = Instant::now();
                            match shard_wal.write(req.clone()).await {
                                Ok(resp) => {
                                    let elapsed = start.elapsed();
                                    black_box(resp);
                                    break elapsed;
                                }
                                Err(celeriant_shard::error::shard_write_error::ShardWriteError::ReplicationBackpressure) => {
                                    sleep(Duration::from_millis(30)).await;
                                }
                                Err(e) => panic!("unexpected write error: {e:?}"),
                            }
                        }
                    });
                    wave_handles.push(handle);
                }

                for h in wave_handles {
                    cumulative += h.await;
                }

                if wave < num_waves - 1 {
                    sleep(INTER_WAVE_DELAY).await;
                }
            }

            shard_wal.close().await;
            cumulative / TOTAL_WRITES as u32
        })
        .unwrap()
        .join()
        .unwrap()
}

// =============================================================================
// BENCHMARKS
// =============================================================================

/// Leader vs standalone write throughput.
///
/// Standalone: no replication — `replicate_durable` short-circuits.
/// Leader: full replication pipeline runs per write (snapshot capture, batch
/// build via `snapshot_to_batch_items`, chain validation, `single_send` into
/// the stub which sleeps 30ms and acks).
///
/// The DELTA is the cost of the replication-batch pipeline under the stub.
/// Wall-time is dominated by the stub's fixed 30ms sleep + 17ms coalescing
/// window, so read this as "leader writes pay the replication round-trip",
/// not "batch serialization costs X CPU". See file header.
fn bench_leader_vs_standalone(c: &mut Criterion) {
    let mut group = c.benchmark_group("replication_batch");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));

    let bytes_per_iter = EVENT_SIZE_BYTES * EVENTS_PER_WRITE * TOTAL_WRITES;
    group.throughput(Throughput::Bytes(bytes_per_iter as u64));

    let roles: [(&str, fn() -> ValidatedNodeStatus); 2] = [
        ("standalone", ValidatedNodeStatus::create_standalone),
        ("leader", leader_status),
    ];

    for (role_name, make_status) in roles {
        eprintln!("\n=== role: {} ({} writes in waves of {}) ===", role_name, TOTAL_WRITES, WRITES_PER_WAVE);
        group.bench_function(BenchmarkId::new("role", role_name), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_write_workload(make_status(), EVENTS_PER_WRITE, 1);
                }
                total
            });
        });
    }

    group.finish();
}

/// Leader batch-size sweep: vary the number of aggregates per write, which is
/// the number of items cloned into each replication batch. This is the axis
/// where the genuine batch-BUILD cost (per-item `metablock.clone()` +
/// `datablock.clone()` in `snapshot_to_batch_items`, plus per-item chain
/// validation) scales, rather than the constant stub sleep.
///
/// All variants are LEADER, so all pay the replication pipeline; the difference
/// across variants reflects build/clone/fsync work growing with batch breadth.
fn bench_leader_batch_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("replication_batch");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));

    // (label, aggregates_per_write, events_per_aggregate)
    let configs: [(&str, usize, usize); 4] = [
        ("1agg_5ev", 1, 5),
        ("1agg_50ev", 1, 50),
        ("8agg_5ev", 8, 5),
        ("32agg_5ev", 32, 5),
    ];

    for (label, aggs, evs) in configs {
        let items_per_write = aggs;
        let bytes_per_iter = EVENT_SIZE_BYTES * evs * aggs * TOTAL_WRITES;
        group.throughput(Throughput::Bytes(bytes_per_iter as u64));
        eprintln!(
            "\n=== batch_size: {} ({} agg/write, {} ev/agg -> {} batch items/write) ===",
            label, aggs, evs, items_per_write
        );
        group.bench_function(BenchmarkId::new("leader_batch", label), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_write_workload(leader_status(), evs, aggs);
                }
                total
            });
        });
    }

    group.finish();
}
