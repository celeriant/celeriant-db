//! Two scenarios, ONE variable: a client's first write to a large aggregate it has never
//! written before (a negative idempotency lookup). The only difference between the two is
//! whether that client has written ANYTHING to the DB before.
//!
//!   - `new_client`      the client_id is brand new: it has never written any aggregate, so it
//!                       is absent from every segment's header client_id bloom. The negative
//!                       lookup short-circuits at each segment's bloom and reads no metablocks.
//!   - `existing_client` the client_id has written OTHER aggregates already, so it is present
//!                       in every segment's client_id bloom. The bloom can short-circuit
//!                       nothing; proving it never wrote THIS aggregate requires reverse-walking
//!                       the aggregate's chain across every segment of the WAL.
//!
//! Everything else is identical: same template WAL, same large target aggregate spanning every
//! segment, same long chain of segment files, same untimed warm-up that loads the aggregate
//! snapshot so the timed write measures ONLY the client negative lookup (not a cold
//! aggregate-version reconstruction). The client_id handed to the write is the sole variable.
//!
//! The proof is the WAL-length sweep: `new_client` stays flat as the log grows (it skips every
//! segment regardless of size), while `existing_client` grows with the log (it walks it). That
//! divergence IS the demonstration that the per-segment header client bloom does not close this
//! case - it helps the new client and does nothing for the existing one. A per-AGGREGATE client
//! bloom is what collapses `existing_client` back down to `new_client`.
//!
//! High foreign interleaving keeps the target's blocks sparse, so the per-segment
//! `find_first_chain_member` (the part the per-aggregate backlink does NOT bound) has to dig
//! through each segment - that is where the bytes go. Direct I/O means the walk hits the device.
//! fsync is ZERO so write latency reflects the scan, not a batch timer.
//!
//! Run: cargo bench -p celeriant_shard --bench negative_scan_present_client_benchmark

use std::collections::HashMap;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
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

criterion_group!(benches, bench_negative_scan_present_client);
criterion_main!(benches);

const SEGMENT_SIZE_BYTES: u64 = 2 * 1024 * 1024;
const TARGET_ID: u128 = 1;
const FOREIGN_BASE_ID: u128 = 1_000_000;
/// Has written other aggregates -> present in every segment's client bloom. Never writes target.
const EXISTING_CLIENT: u128 = 7_000_000;
/// Never writes anything in setup -> absent from every segment's client bloom.
const NEW_CLIENT: u128 = 9_000_000;
/// Used only by the untimed warm-up to load the target aggregate snapshot. Fresh, so its own
/// client lookup short-circuits and the warm-up stays cheap.
const WARM_CLIENT: u128 = 8_000_000;
/// Distinct foreign aggregates between each target version. High, so the target is sparse and
/// the per-segment find_first has to dig (the cost the backlink does not remove). Kept under a
/// segment's block capacity so every segment still contains the target (no aggregate-bloom skip).
const FOREIGN_PER_ROUND: u128 = 1024;

/// WAL length, in target versions (= rounds). Each round writes the target once, FOREIGN_PER_ROUND
/// foreign aggregates, and one carrier write by EXISTING_CLIENT, so the target and EXISTING_CLIENT
/// both land in every segment. The sweep is the proof: new_client flat, existing_client linear.
fn length_configs() -> Vec<(&'static str, u64)> {
    vec![("short", 16), ("medium", 32), ("long", 64)]
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
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_negative_scan_present_bench"),
        cache_warmup_max_duration: Duration::ZERO,
        wal_compression_level: 3,
        dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

/// One tiny inline event = one dense ~512B metablock, recording `client_id`/`client_seq`.
fn write_req(aggregate_id: u128, client_id: u128, client_seq: u64, enforce_idempotency: bool) -> WriteRequest {
    let event = DatablockAggregateEvent {
        client_seq,
        event_type_major: 1,
        event_value: Arc::new(vec![0u8; 16]),
        ..Default::default()
    };
    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(1, 1, aggregate_id),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: enforce_idempotency,
        },
    );
    WriteRequest { correlation_id: None, client_id, user_id: None, writes }
}

/// Build the template WAL: `rounds` target versions, each followed by FOREIGN_PER_ROUND distinct
/// foreign aggregates and one carrier write by EXISTING_CLIENT. The target and EXISTING_CLIENT
/// land in every segment. Idempotency off: setup only lays down the chain.
fn setup_wal(shard_dir: PathBuf, rounds: u64) {
    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let shard = Rc::new(
                ShardWal::open(base_config(shard_dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                    .await
                    .unwrap(),
            );
            let mut foreign = FOREIGN_BASE_ID;
            for round in 0..rounds {
                shard.write(write_req(TARGET_ID, 10_000 + round as u128, 1, false)).await.unwrap();
                for _ in 0..FOREIGN_PER_ROUND {
                    shard.write(write_req(foreign, foreign, 1, false)).await.unwrap();
                    foreign += 1;
                }
                // Carrier: EXISTING_CLIENT writes a foreign aggregate, landing it in this
                // segment's client bloom (so it is present across the whole WAL).
                shard.write(write_req(foreign, EXISTING_CLIENT, 1, false)).await.unwrap();
                foreign += 1;
            }
            shard.close().await;
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Copy every file in `src` into a fresh `dst` (flat dir of WAL segments).
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), dst.join(entry.file_name())).unwrap();
        }
    }
}

fn bench_negative_scan_present_client(c: &mut Criterion) {
    let mut group = c.benchmark_group("negative_scan_present_client");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.warm_up_time(Duration::from_secs(3));

    for (name, rounds) in length_configs() {
        let template = tempdir().unwrap();
        let template_dir = template.path().join("wal");
        eprintln!("\n=== Setup {name}: {rounds} target versions, {FOREIGN_PER_ROUND}x foreign interleave, EXISTING_CLIENT in every segment ===");
        setup_wal(template_dir.clone(), rounds);

        // Bloom WINS: brand-new client, absent everywhere -> every segment skipped. Should be
        // flat across the sweep.
        bench_first_write(&mut group, "new_client", name, &template_dir, NEW_CLIENT);
        // Bloom DEFEATED: client present in every segment -> full reverse walk. Should grow with
        // the log.
        bench_first_write(&mut group, "existing_client", name, &template_dir, EXISTING_CLIENT);

        drop(template);
    }

    group.finish();
}

fn bench_first_write(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    variant: &str,
    size_name: &str,
    template_dir: &Path,
    client_id: u128,
) {
    group.bench_with_input(BenchmarkId::new(variant, size_name), &template_dir.to_path_buf(), |b, template_dir| {
        b.iter_custom(|iters| {
            let template_dir = template_dir.clone();
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(move || async move {
                    let mut total = Duration::ZERO;
                    for i in 0..iters {
                        // Fresh WAL copy per iteration (the timed write appends a block for this
                        // client; reusing state would short-circuit the next scan).
                        let work = tempdir().unwrap();
                        let work_dir = work.path().join("wal");
                        copy_dir(&template_dir, &work_dir);

                        let shard = ShardWal::open(base_config(work_dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                            .await
                            .unwrap();

                        // Untimed warm-up: a fresh client's first write to the target loads the
                        // aggregate snapshot (tip + version) WITHOUT paying a client walk (its own
                        // lookup short-circuits on the bloom). Removes the cold aggregate
                        // reconstruction from the timed region, leaving only the client scan.
                        shard.write(write_req(TARGET_ID, WARM_CLIENT, 1, true)).await.unwrap();

                        // Timed: the probe client's first write to the target. Negative lookup.
                        // new_client -> bloom skips every segment. existing_client -> full walk.
                        let req = write_req(TARGET_ID, client_id, 2 + i, true);
                        let start = std::time::Instant::now();
                        let resp = shard.write(req).await.unwrap();
                        total += start.elapsed();
                        black_box(resp);

                        shard.close().await;
                        drop(work);
                    }
                    total
                })
                .unwrap()
                .join()
                .unwrap()
        });
    });
}
