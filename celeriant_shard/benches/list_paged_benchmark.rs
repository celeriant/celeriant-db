//! Isolates the win from the index-free list pagination fix: per-page cost over SEALED segments.
//!
//! `list_aggregates` returns the active-segment summary whole on page 1 (bounded per segment,
//! same before and after the fix), so page 1 can't show the change. The fix bounds work WITHIN
//! each sealed segment: pre-fix a page processed a whole sealed segment's summary Vec (the page
//! limit was only checked BETWEEN segments); post-fix it stops at `page_size` via a (log_id,
//! offset) cursor.
//!
//! So this bench times PAGE 2 — `list_aggregates` with the cursor returned by page 1, which skips
//! the active summary and lands in sealed segments. Setup writes many tiny aggregates so several
//! segments seal with thousands of aggregates each. Listing is read-only, so the same WAL is
//! reused across iterations (no per-iter copy).
//!
//! Expected:
//! - post-fix: page-2 latency scales with `page_size` (small page = fast).
//! - pre-fix:  page-2 latency ~ a whole sealed segment, regardless of `page_size`.
//! Run pre/post and `critcmp` the two baselines to see the difference.
//!
//! Run: cargo bench -p celeriant_shard --bench list_paged_benchmark

use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::request::requests::{ListAggregatesRequest, SingleAggregateWrite, WriteRequest};
use celeriant_shard::internal_shard_config::InternalShardConfig;
use celeriant_shard::replication_client::StubReplicationClient;
use celeriant_shard::s3_downloader::StubS3Downloader;
use celeriant_shard::shard_wal::ShardWal;
use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use glommio::{LocalExecutorBuilder, Placement};
use tempfile::tempdir;

criterion_group!(benches, bench_list_paged);
criterion_main!(benches);

/// Small segment so a few thousand tiny aggregates seal it.
const SEGMENT_SIZE_BYTES: u64 = 2 * 1024 * 1024;
const ORG: u128 = 1;
const ATYPE: u128 = 1;
/// Total aggregates written. Tiny events => ~4k metablocks per 2MB segment => several sealed
/// segments of a few thousand aggregates each (large sealed summaries — the case the fix bounds).
const TOTAL_AGGREGATES: u128 = 20_000;

/// Requested page sizes. Post-fix tracks these; pre-fix ignores them (whole-segment per page).
fn page_sizes() -> Vec<(&'static str, usize)> {
    vec![("page_10", 10), ("page_100", 100), ("page_1000", 1000)]
}

fn base_config(shard_dir: PathBuf, list_page_size: usize) -> InternalShardConfig {
    InternalShardConfig {
        wal_join_data_meta_writes: true,
        node_id: 1,
        shard_id: 1,
        max_open_files: 4096,
        shard_log_preallocate_bytes: SEGMENT_SIZE_BYTES,
        fsync_delay: Duration::ZERO,
        replication_delay: Duration::ZERO,
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::ZERO,
        heartbeat_starve_threshold: Duration::ZERO,
        recent_write_cache_bytes: 8 * 1024 * 1024,
        shard_dir,
        max_response_size: 64 * 1024 * 1024,
        max_request_size: 64 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        negative_lookup_cache_bytes: 2 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes: 1024,
        timestamp_config: TimestampConfig::default(),
        list_max_duration: Duration::from_secs(10),
        list_page_size,
        list_max_concurrent: 16,
        read_max_concurrent: 64,
        schema_cache_bytes: 4 * 1024 * 1024,
        max_schema_size_bytes: 16384,
        max_catchup_gap_bytes: Some(104_857_600),
        max_promotion_batch_bytes: None,
        max_clock_drift_ms: 500,
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_list_paged_bench"),
        cache_warmup_max_duration: Duration::MAX,
        wal_compression_level: 3,
        dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

fn write_req(aggregate_id: u128) -> WriteRequest {
    let event = DatablockAggregateEvent {
        client_seq: 1,
        event_type_major: 1,
        event_value: Arc::new(vec![0u8; 8]),
        ..Default::default()
    };
    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(ORG, ATYPE, aggregate_id),
        SingleAggregateWrite { events: vec![event], allow_create: true, expected_version: None, enforce_client_idempotency: false },
    );
    WriteRequest { correlation_id: None, client_id: aggregate_id, user_id: None, writes }
}

fn list_req(cursor: Option<u64>) -> ListAggregatesRequest {
    ListAggregatesRequest { correlation_id: None, shard_id: 1, org_id: Some(ORG), aggregate_type_id: Some(ATYPE), cursor }
}

/// Write TOTAL_AGGREGATES distinct aggregates so several segments seal.
fn setup_wal(shard_dir: PathBuf) {
    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let shard = Rc::new(
                ShardWal::open(base_config(shard_dir, 100), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                    .await
                    .unwrap(),
            );
            for id in 1..=TOTAL_AGGREGATES {
                shard.write(write_req(id)).await.unwrap();
            }
            shard.close().await;
        })
        .unwrap()
        .join()
        .unwrap();
}

fn bench_list_paged(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_paged");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    let template = tempdir().unwrap();
    let shard_dir = template.path().join("wal");
    eprintln!("\n=== Setup: {TOTAL_AGGREGATES} aggregates across sealed 2MB segments ===");
    setup_wal(shard_dir.clone());

    for (name, page_size) in page_sizes() {
        group.bench_with_input(BenchmarkId::new("page2", name), &shard_dir.to_path_buf(), |b, shard_dir| {
            b.iter_custom(|iters| {
                let shard_dir = shard_dir.clone();
                LocalExecutorBuilder::new(Placement::Fixed(0))
                    .spawn(move || async move {
                        let shard = ShardWal::open(base_config(shard_dir, page_size), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                            .await
                            .unwrap();

                        // Untimed: page 1 returns the active summary and yields a cursor into the
                        // sealed segments. Page 2 (timed below) is where the within-segment bound bites.
                        let cursor1 = shard.list_aggregates(list_req(None)).await.unwrap().next_cursor;
                        assert!(cursor1.is_some(), "setup must produce sealed segments so page 1 sets a cursor");

                        // Listing is read-only, so the same page-2 request is repeatable.
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let start = std::time::Instant::now();
                            let resp = shard.list_aggregates(list_req(cursor1)).await.unwrap();
                            total += start.elapsed();
                            std::hint::black_box(resp);
                        }

                        shard.close().await;
                        total
                    })
                    .unwrap()
                    .join()
                    .unwrap()
            });
        });
    }

    drop(template);
    group.finish();
}
