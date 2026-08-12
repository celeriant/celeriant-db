//! Isolates the WAL write-path compression cost.
//!
//! The write path serialises each event batch via `SerialisedDatablock::new(... Auto ...)`,
//! which — when the uncompressed batch exceeds the inline threshold MINIBATCH_SIZE_BYTES
//! (718 bytes on the default, non-`small-metablock` build) — runs a single
//! `DictCodec::compress` (zstd-with-dictionary at `wal_compression_level`). This bench
//! varies the compression knobs while holding everything else at the authoritative
//! current `InternalShardConfig` (copied verbatim from write_benchmark.rs) so we can read
//! the marginal cost of the compressor itself.
//!
//! Knobs swept:
//!   1. `wal_compression_level` (i32, fed straight to zstd; 0 == zstd-default == level 3,
//!      default config value is 3). Sweep 1, 3, 9.
//!   2. dict on (BUILTIN_DICT_BYTES) vs off (empty dict).
//!   3. payload compressibility: highly-compressible (repeated byte), incompressible
//!      (deterministic xorshift), realistic JSON-ish.
//!   4. payload size: one below and one above the 718-byte inline threshold.
//!
//! We also print observed compressed-size (uncompressed -> compressed bytes) per
//! (level x dict x payload x size) via a direct `SerialisedDatablock` probe, so the
//! space/latency tradeoff is visible alongside the throughput numbers.

use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use celeriant_shard::internal_shard_config::InternalShardConfig;
use celeriant_shard::replication_client::StubReplicationClient;
use celeriant_shard::s3_downloader::StubS3Downloader;
use celeriant_shard::shard_wal::ShardWal;
use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock::Datablock;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
use celeriant_wal::datablocks::datablock_kind::DatablockKind;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::disk::serialised_datablock::{CompressionPolicy, SerialisedDatablock};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use glommio::{LocalExecutorBuilder, Placement};
use tempfile::tempdir;

criterion_group!(benches, bench_write_compression);
criterion_main!(benches);

// =============================================================================
// CONFIGURATION
// =============================================================================

/// Inline-vs-block threshold (celeriant_wal MINIBATCH_SIZE_BYTES, default build = 718).
/// A serialised datablock <= this stays inline; uncompressed batches bigger than this
/// are the only ones that exercise the compressor at all.
const MINIBATCH_THRESHOLD: usize = 718;

const EVENTS_PER_WRITE: usize = 5;

/// Writes per benchmark iteration. Sequential so we isolate per-write codec cost rather
/// than fsync-amortisation dynamics.
const TOTAL_WRITES: usize = 400;

const SEGMENT_SIZE_BYTES: u64 = 128 * 1024 * 1024;

/// Default fsync delay for the compression sweep. We keep a non-zero amortisation window
/// constant across all cells so the codec cost is the only thing varying.
const FSYNC_DELAY: Duration = Duration::from_millis(10);

/// Payload event_value sizes: one safely below the 718B inline threshold (whole batch
/// stays inline, no compression), one well above (compressor always fires).
/// 5 events * 24B ~= 120B batch payload + framing => stays inline.
/// 5 events * 512B = 2560B batch payload => crosses threshold => compressor runs.
const SIZE_BELOW: usize = 24;
const SIZE_ABOVE: usize = 512;

/// Compression levels to sweep. 3 is the current config default. 0 == zstd default (==3).
fn levels() -> Vec<i32> {
    vec![1, 3, 9]
}

#[derive(Clone, Copy)]
enum Dict {
    Builtin,
    Empty,
}

impl Dict {
    fn name(self) -> &'static str {
        match self {
            Dict::Builtin => "dict",
            Dict::Empty => "nodict",
        }
    }
    fn bytes(self) -> Arc<[u8]> {
        match self {
            Dict::Builtin => Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
            Dict::Empty => Arc::from(&[][..]),
        }
    }
}

#[derive(Clone, Copy)]
enum Payload {
    /// Repeated single byte -> highly compressible.
    Compressible,
    /// Deterministic xorshift bytes -> effectively incompressible.
    Random,
    /// Realistic JSON-ish event body -> partially compressible, dict-friendly.
    Json,
}

impl Payload {
    fn name(self) -> &'static str {
        match self {
            Payload::Compressible => "compressible",
            Payload::Random => "random",
            Payload::Json => "json",
        }
    }
}

// =============================================================================
// CONFIG (copied verbatim from write_benchmark.rs, then compression knobs varied)
// =============================================================================

fn create_config(
    shard_dir: PathBuf,
    wal_compression_level: i32,
    dict_bytes: Arc<[u8]>,
) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        max_open_files: 256,
        shard_log_preallocate_bytes: SEGMENT_SIZE_BYTES,
        fsync_delay: FSYNC_DELAY,
        replication_delay: Duration::from_millis(17),
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::from_millis(500),
        heartbeat_starve_threshold: Duration::ZERO,
        recent_write_cache_bytes: 64 * 1024 * 1024,
        shard_dir,
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        negative_lookup_cache_bytes: 2 * 1024 * 1024,
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
        wal_compression_level,
        dict_bytes,
        s3_lease_duration_ms: 0,
    }
}

// =============================================================================
// PAYLOAD GENERATION
// =============================================================================

/// Deterministic xorshift-filled bytes, seeded by index. Avoids any RNG; reproducible.
fn random_bytes(size: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
    let mut out = Vec::with_capacity(size);
    for _ in 0..size {
        // xorshift64
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push((state & 0xFF) as u8);
    }
    out
}

/// Realistic JSON-ish body padded to roughly `size` bytes with repeated key/value text.
fn json_bytes(size: usize, seed: u64) -> Vec<u8> {
    let mut s = format!(
        r#"{{"event":"page_view","url":"/products/item","user_id":{},"session":"{:016x}","ts":1700000000,"referrer":"https://example.com/search","ua":"Mozilla/5.0","tags":["a","b","c"]"#,
        seed % 100000,
        seed
    );
    while s.len() < size {
        s.push_str(r#","kv":"filler-value-text""#);
    }
    s.push('}');
    s.truncate(size);
    s.into_bytes()
}

fn make_event_value(payload: Payload, size: usize, seed: u64) -> Vec<u8> {
    match payload {
        Payload::Compressible => vec![0x41u8; size],
        Payload::Random => random_bytes(size, seed),
        Payload::Json => json_bytes(size, seed),
    }
}

fn create_events(
    count: usize,
    size: usize,
    base_index: u64,
    payload: Payload,
) -> Vec<DatablockAggregateEvent> {
    (0..count)
        .map(|i| DatablockAggregateEvent {
            client_seq: base_index + i as u64,
            event_seq: 0,
            event_id: None,
            event_timestamp: 1_700_000_000_000 + i as u64,
            event_type_major: 1,
            event_type_minor: 0,
            event_value: Arc::new(make_event_value(payload, size, base_index + i as u64)),
            iv: None,
        })
        .collect()
}

fn create_write_request(
    aggregate_key: AggregateKey,
    events: Vec<DatablockAggregateEvent>,
    client_id: u128,
) -> WriteRequest {
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key,
        SingleAggregateWrite {
            events,
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    WriteRequest {
        correlation_id: None,
        client_id,
        user_id: None,
        writes,
    }
}

// =============================================================================
// COMPRESSED-SIZE PROBE
// =============================================================================

/// Serialise one representative batch through the exact same Auto policy the write path
/// uses, and report (uncompressed, compressed, inline?) so the space tradeoff is visible.
fn probe_sizes(
    level: i32,
    dict_bytes: &[u8],
    size: usize,
    payload: Payload,
) -> (u64, u64, bool) {
    let codec = DictCodec::new(dict_bytes, level).unwrap();
    let events = create_events(EVENTS_PER_WRITE, size, 0, payload);
    let batch = DatablockAggregateEventBatch {
        aggregate_version: 1,
        events,
    };
    let datablock = Datablock {
        datablock_kind: DatablockKind::EventBatchItem(batch),
    };
    let serialised = SerialisedDatablock::new(
        &datablock,
        CompressionPolicy::Auto {
            compression_allowed: true,
        },
        &codec,
    )
    .unwrap();
    use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
    let inline = matches!(serialised.storage_kind, DatablockStorageKind::Inline(_));
    (
        serialised.uncompressed_size,
        serialised.compressed_size,
        inline,
    )
}

// =============================================================================
// BENCHMARK
// =============================================================================

fn bench_write_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_compression");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(4));
    group.warm_up_time(Duration::from_secs(1));

    // Print the inline/block threshold + observed compressed sizes once up front.
    eprintln!(
        "\n=== inline/block threshold (MINIBATCH_SIZE_BYTES) = {} bytes ===",
        MINIBATCH_THRESHOLD
    );
    eprintln!("=== compressed-size probe (uncompressed -> compressed, inline?) ===");
    for &size in &[SIZE_BELOW, SIZE_ABOVE] {
        for payload in [Payload::Compressible, Payload::Random, Payload::Json] {
            for dict in [Dict::Builtin, Dict::Empty] {
                for level in levels() {
                    let (u, comp, inline) =
                        probe_sizes(level, &dict.bytes(), size, payload);
                    eprintln!(
                        "  evsize={:<4} {:<12} {:<6} L{:<2} : {:>5} -> {:>5} ({:.2}x) {}",
                        size,
                        payload.name(),
                        dict.name(),
                        level,
                        u,
                        comp,
                        u as f64 / comp.max(1) as f64,
                        if inline { "INLINE" } else { "BLOCK" }
                    );
                }
            }
        }
    }
    eprintln!();

    for &size in &[SIZE_BELOW, SIZE_ABOVE] {
        // Throughput = event_value bytes written per iteration (the payload we feed in).
        let bytes_per_iteration = size * EVENTS_PER_WRITE * TOTAL_WRITES;
        group.throughput(Throughput::Bytes(bytes_per_iteration as u64));

        for payload in [Payload::Compressible, Payload::Random, Payload::Json] {
            for dict in [Dict::Builtin, Dict::Empty] {
                for level in levels() {
                    let id = format!(
                        "sz{}/{}/{}/L{}",
                        size,
                        payload.name(),
                        dict.name(),
                        level
                    );
                    let dict_bytes = dict.bytes();

                    group.bench_with_input(
                        BenchmarkId::new("seq", id),
                        &(level, size, payload),
                        |b, &(level, size, payload)| {
                            b.iter_custom(|iters| {
                                let mut total_duration = Duration::ZERO;

                                for _ in 0..iters {
                                    let tempdir = tempdir().unwrap();
                                    let shard_dir = tempdir.path().to_path_buf();
                                    let dict_bytes = dict_bytes.clone();

                                    let iteration_duration =
                                        LocalExecutorBuilder::new(Placement::Fixed(0))
                                            .spawn(move || async move {
                                                let config = create_config(
                                                    shard_dir, level, dict_bytes,
                                                );
                                                let shard_wal = Rc::new(
                                                    ShardWal::open(
                                                        config,
                                                        ValidatedNodeStatus::create_standalone(),
                                                        StubReplicationClient,
                                                        StubS3Downloader,
                                                    )
                                                    .await
                                                    .unwrap(),
                                                );

                                                let aggregate_key = AggregateKey::new(1, 1, 1);
                                                let mut cumulative = Duration::ZERO;

                                                for write_id in 0..TOTAL_WRITES {
                                                    let base_index =
                                                        (write_id * EVENTS_PER_WRITE) as u64;
                                                    let events = create_events(
                                                        EVENTS_PER_WRITE,
                                                        size,
                                                        base_index,
                                                        payload,
                                                    );
                                                    let req = create_write_request(
                                                        aggregate_key.clone(),
                                                        events,
                                                        write_id as u128,
                                                    );

                                                    let start = Instant::now();
                                                    let result = shard_wal.write(req).await;
                                                    cumulative += start.elapsed();

                                                    black_box(result.unwrap());
                                                }

                                                shard_wal.close().await;
                                                cumulative / TOTAL_WRITES as u32
                                            })
                                            .unwrap()
                                            .join()
                                            .unwrap();

                                    total_duration += iteration_duration;
                                }

                                total_duration
                            });
                        },
                    );
                }
            }
        }
    }

    group.finish();
}
