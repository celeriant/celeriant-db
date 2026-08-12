//! Full-stream aggregate read / event-replay cost — the core event-sourcing read.
//!
//! To rebuild a projection, a consumer reads versions 1..N for a single
//! aggregate and folds every event. `ShardWal::read` with
//! `ReadFilters::new(1).to_aggregate_version(N)` returns the WHOLE stream in one
//! response. This walks the aggregate's metablock chain
//! (`collect_metablocks_bounded`), fetches the backing datablocks
//! (`fetch_datablocks_for_metablocks`), then decompresses + deserialises every
//! event into the response (`build_filtered_response`) — see
//! `celeriant_shard/src/collect_from_disk.rs`.
//!
//! This bench writes N versions to ONE aggregate (one event per write, no foreign
//! interleaving), warms the snapshot, then times the full 1..N read. It sweeps
//! N (stream length) against event payload size (small 16B vs ~1KB) to show how
//! replay cost scales with both the chain depth and the bytes moved.
//!
//! NOTE: `base_config` ships `max_response_size`/`max_request_size` at 16MB. A
//! 5000 x 1KB stream plus per-event framing overshoots that, so this bench copies
//! the config but RAISES both limits to 256MB, guaranteeing the whole stream comes
//! back in a single read. All other fields are verbatim from the reverse bench.
//!
//! Run: cargo bench -p celeriant_shard --bench read_stream_benchmark

use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest};
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

criterion_group!(benches, bench_full_stream_read);
criterion_main!(benches);

const SEGMENT_SIZE_BYTES: u64 = 2 * 1024 * 1024;
const TARGET_ID: u128 = 1;

/// Stream lengths to sweep: how many versions one aggregate has accumulated.
const STREAM_LENGTHS: [u64; 3] = [100, 1000, 5000];

/// Event payload sizes to sweep.
fn payload_sizes() -> Vec<(&'static str, usize)> {
    vec![("16B", 16), ("1KB", 1024)]
}

fn base_config(shard_dir: PathBuf) -> InternalShardConfig {
    InternalShardConfig {
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
        // Raised from the reverse bench's 16MB so a full 5000 x 1KB stream
        // (~5MB raw + framing) returns in a single read response.
        max_response_size: 256 * 1024 * 1024,
        max_request_size: 256 * 1024 * 1024,
        internode_max_request_size: 256 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        negative_lookup_cache_bytes: 2 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes: 32 * 1024,
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
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_read_stream_bench"),
        cache_warmup_max_duration: Duration::ZERO,
        wal_compression_level: 3,
        dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

fn write_req(aggregate_id: u128, client_seq: u64, payload_bytes: usize) -> WriteRequest {
    let event = DatablockAggregateEvent {
        client_seq,
        event_type_major: 1,
        event_value: Arc::new(vec![0u8; payload_bytes]),
        ..Default::default()
    };
    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(1, 1, aggregate_id),
        SingleAggregateWrite { events: vec![event], allow_create: true, expected_version: None, enforce_client_idempotency: false },
    );
    WriteRequest { correlation_id: None, client_id: aggregate_id, user_id: None, writes }
}

/// Write `stream_len` versions to ONE aggregate, one event per write.
fn setup_wal(shard_dir: PathBuf, stream_len: u64, payload_bytes: usize) {
    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let shard = Rc::new(
                ShardWal::open(base_config(shard_dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                    .await
                    .unwrap(),
            );
            for version in 1..=stream_len {
                shard.write(write_req(TARGET_ID, version, payload_bytes)).await.unwrap();
            }
            shard.close().await;
        })
        .unwrap()
        .join()
        .unwrap();
}

fn bench_full_stream_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_stream");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));
    group.warm_up_time(Duration::from_secs(3));

    for (size_name, payload_bytes) in payload_sizes() {
        for stream_len in STREAM_LENGTHS {
            let tempdir = tempdir().unwrap();
            let shard_dir = tempdir.path().to_path_buf();
            eprintln!("\n=== Setup {size_name}: stream of {stream_len} versions, {payload_bytes}B payload each ===");
            setup_wal(shard_dir.clone(), stream_len, payload_bytes);

            // Throughput in events/sec so the per-event cost is visible across N.
            group.throughput(criterion::Throughput::Elements(stream_len));
            bench_read(&mut group, size_name, &shard_dir, stream_len);

            drop(tempdir);
        }
    }

    group.finish();
}

fn bench_read(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    size_name: &str,
    shard_dir: &std::path::Path,
    stream_len: u64,
) {
    group.bench_with_input(BenchmarkId::new(size_name, stream_len), &shard_dir.to_path_buf(), |b, shard_dir| {
        b.iter_custom(|iters| {
            let shard_dir = shard_dir.clone();
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(move || async move {
                    let shard = ShardWal::open(base_config(shard_dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                        .await
                        .unwrap();

                    // Full stream: from version 1 THROUGH stream_len, all events.
                    let mk_req = || ReadRequest {
                        correlation_id: None,
                        aggregate_key: AggregateKey::new(1, 1, TARGET_ID),
                        filters: ReadFilters::new(1).to_aggregate_version(stream_len),
                    };

                    // Warm the aggregate snapshot (tip lookup) so we measure the
                    // stream read, not the one-off cold exists() scan.
                    shard.read(&mk_req()).await.unwrap();

                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        let resp = shard.read(&mk_req()).await.unwrap();
                        total += start.elapsed();
                        black_box(resp.event_batches.len());
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
