//! **Layer 1 of the session-6 layer ladder: a fixed-iteration append probe.**
//!
//! Performs exactly `PROBE_APPENDS` appends against one in-process `ShardWal` and exits. There is
//! no criterion here on purpose: instruction counts are near-deterministic, and criterion's
//! warmup, sampling and outlier analysis exist to characterise a *wall-clock distribution*. The
//! measurement is taken from outside, with `perf stat -e instructions,cycles`.
//!
//! **What this layer contains, stated because the obvious name for it is wrong.** It is
//! *no device flush*, not *no device*: all WAL I/O is O_DIRECT, so `write_at` has already DMA'd
//! to the drive before `fdatasync` is even reached (`shard_wal_sync.rs:599-603`). Layer 1 still
//! has a device, DMA writes, the amortisation coordinator and a delay window. What it does not
//! have is a client process, a TCP connection, the intrashard mesh, or a follower.
//!
//! **Read one number from one run and you will read a wrong number.** `perf stat` over a whole
//! process counts executor spawn, io_uring setup, `tempdir`, preallocate, dict load,
//! `ShardWal::open`, first-touch faults, `close` and teardown. All of it is fixed, so
//! instructions/append is beautifully reproducible and systematically inflated. Run at N and 4N
//! and take the slope; `~/s6-probe.sh` does that.

use std::hint::black_box;
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_shard::s3_downloader::StubS3Downloader;
use celeriant_shard::shard_wal::ShardWal;
use celeriant_wal::aggregate_key::AggregateKey;
use glommio::{LocalExecutorBuilder, Placement};
use mimalloc::MiMalloc;

mod bench_support;
use bench_support::{
    create_config_with_preallocate, create_write_request, workload_event, workload_event_with,
    CountingReplicationClient,
    ReplicationCallCounts, WORKLOAD_AGG_TYPE, WORKLOAD_ORG,
};

/// Production runs mimalloc (`celeriant/src/main.rs`); a bench in `celeriant_shard` would
/// otherwise run glibc malloc. Phase 1's leading suspect is per-event allocation, and tuning
/// that against the wrong allocator is worthless.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Bytes the process has sent to the storage layer, so a segment roll or an unexpected read
/// cannot hide inside a stable instruction count.
fn proc_io(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix(field)?.trim().parse::<u64>().ok())
        })
        .unwrap_or(0)
}

fn main() {
    let appends = env_usize("PROBE_APPENDS", 200_000);
    let concurrency = env_usize("PROBE_CONCURRENCY", 1024).max(1);
    let fsync_delay = Duration::from_micros(env_u64("PROBE_FSYNC_DELAY_US", 17_000));
    let cache_bytes = env_u64("PROBE_CACHE_BYTES", 64 * 1024 * 1024);
    // 4 GiB, deliberately larger than production's 256 MiB: 800,000 appends move 1.44 GB through
    // `write_at`, so anything smaller rolls a segment somewhere between N and 4N and the slope
    // measures a step change instead of the steady state. Segment-roll cost is therefore OUTSIDE
    // layer 1 and must not be quoted as if it were in it. Verified by `files=1` in the output.
    let preallocate = env_u64("PROBE_PREALLOCATE_BYTES", 4 * 1024 * 1024 * 1024);
    let cpu = env_usize("PROBE_CPU", 24);
    // The payload's LENGTH is a pinned parameter and was previously wrong: with ids 0..1023 and
    // seqs 0..194, `format!("[t-{id}-r-{seq}] hello")` yields 15-20 bytes, mean 18.35 -- not the
    // ~25 the loaded server produces at 16,000 tasks with seq in the millions. Same format
    // string, different id/seq ranges, so "byte-identical to celeriant_bench" was true of the
    // template and false of the bytes. These offsets put the probe on the server's length.
    let id_offset = env_usize("PROBE_ID_OFFSET", 10_000);
    let seq_offset = env_u64("PROBE_SEQ_OFFSET", 1_000_000);
    // Build each task's payload once instead of `format!`-ing it per append. The server decodes
    // its payload off the wire; it never formats a string. Set PROBE_FORMAT_IN_LOOP=1 to restore
    // the old behaviour and price what the harness was contributing.
    let format_in_loop = std::env::var("PROBE_FORMAT_IN_LOOP").is_ok();

    let tempdir = tempfile::tempdir().expect("tempdir");
    let shard_dir = tempdir.path().to_path_buf();

    let wall_start = Instant::now();
    let wb0 = proc_io("write_bytes:");

    // The counters live inside the executor: `Rc` is not `Send`, and glommio's builder needs a
    // `Send` closure. They come back out as plain integers.
    let (append_secs, files, counts, payload_total) = LocalExecutorBuilder::new(Placement::Fixed(cpu))
        .spawn(move || async move {
            let counts = ReplicationCallCounts::default();
            let config =
                create_config_with_preallocate(shard_dir.clone(), fsync_delay, cache_bytes, preallocate);
            let shard_wal = Rc::new(
                ShardWal::open(
                    config,
                    ValidatedNodeStatus::create_standalone(),
                    CountingReplicationClient::new(counts.clone()),
                    StubS3Downloader,
                )
                .await
                .expect("ShardWal::open"),
            );

            // One aggregate per task, fixed for the task's lifetime — the shape
            // `celeriant_bench` uses, and deliberately so: making the aggregate a function of
            // the request count is the defect commit aa22a63 fixed, after which almost every
            // write targeted another shard and the benchmark measured connection handover.
            let base = appends / concurrency;
            let remainder = appends % concurrency;

            let started = Instant::now();
            let mut handles = Vec::with_capacity(concurrency);
            let payload_bytes = Rc::new(Cell::new(0u64));
            for task_id in 0..concurrency {
                let shard_wal = shard_wal.clone();
                let payload_bytes = payload_bytes.clone();
                let my_appends = base + usize::from(task_id < remainder);
                handles.push(glommio::spawn_local(async move {
                    let key = AggregateKey::new(WORKLOAD_ORG, WORKLOAD_AGG_TYPE, task_id as u128);
                    // Pre-built once per task, cloned per append: an Arc clone is what the server
                    // pays after its own decode, a `format!` is not.
                    let prebuilt = workload_event(id_offset + task_id, seq_offset).event_value;
                    payload_bytes.set(payload_bytes.get() + prebuilt.len() as u64 * my_appends as u64);
                    for seq in 0..my_appends {
                        let event = if format_in_loop {
                            workload_event(id_offset + task_id, seq_offset + seq as u64)
                        } else {
                            workload_event_with(prebuilt.clone())
                        };
                        let request = create_write_request(key.clone(), vec![event], 0);
                        black_box(shard_wal.write(request).await.expect("write"));
                    }
                }));
            }
            for h in handles {
                h.await;
            }
            let append_secs = started.elapsed().as_secs_f64();
            let payload_total = payload_bytes.get();

            shard_wal.close().await;

            let files = std::fs::read_dir(&shard_dir)
                .map(|d| d.filter_map(|e| e.ok()).count())
                .unwrap_or(0);
            let counts = [
                counts.follower.get(),
                counts.s3.get(),
                counts.heartbeat.get(),
                counts.kick.get(),
            ];
            (append_secs, files, counts, payload_total)
        })
        .expect("spawn executor")
        .join()
        .expect("join executor");

    let wall_secs = wall_start.elapsed().as_secs_f64();
    let write_bytes = proc_io("write_bytes:").saturating_sub(wb0);

    println!(
        "probe appends={appends} concurrency={concurrency} fsync_delay_us={} cpu={cpu} \
         preallocate={preallocate} wall_s={wall_secs:.4} append_s={append_secs:.4} \
         appends_per_s={:.0} write_bytes={write_bytes} bytes_per_append={:.1} \
         payload_bytes={payload_total} payload_per_append={:.2} amplification={:.1} files={files} \
         repl_follower={} repl_s3={} repl_heartbeat={} repl_kick={}",
        fsync_delay.as_micros(),
        appends as f64 / append_secs,
        write_bytes as f64 / appends as f64,
        payload_total as f64 / appends as f64,
        write_bytes as f64 / payload_total.max(1) as f64,
        counts[0],
        counts[1],
        counts[2],
        counts[3],
    );

    // Layer 1 is only "no follower" if the stubs never fired. StubReplicationClient sleeps 30 ms
    // in replicate_to_follower, 230 ms in replicate_to_s3 and 100 ms in send_heartbeat, so a
    // single call is both a correctness problem for the layer definition and a large timing one.
    let total: u64 = counts.iter().sum();
    if total > 0 {
        eprintln!("PROBE WARNING: replication stubs fired ({total} calls) — this run is NOT layer 1");
    }
}
