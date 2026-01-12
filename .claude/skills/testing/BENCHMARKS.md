# Benchmarks

Benchmarks use **Criterion** for statistical measurement and **Glommio** for async execution.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Criterion                                          │
│  - Sample collection                                │
│  - Statistical analysis                             │
│  - HTML reports                                     │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  iter_custom()                                      │
│  - Manual timing control                            │
│  - Exclude setup from measurement                   │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  Glommio LocalExecutor                              │
│  - Single-threaded async runtime                    │
│  - io_uring for I/O                                 │
│  - spawn_local for concurrent tasks                 │
└─────────────────────────────────────────────────────┘
```

## Benchmark Template

```rust
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use glommio::{LocalExecutorBuilder, Placement};
use tempfile::tempdir;

// Register benchmark groups and main
criterion_group!(benches, bench_my_operation);
criterion_main!(benches);

// =============================================================================
// CONFIGURATION
// =============================================================================

const ITERATIONS_PER_SAMPLE: usize = 1000;
const PAYLOAD_SIZE_BYTES: usize = 256;

fn config_variants() -> Vec<(&'static str, u64)> {
    vec![
        ("small", 64 * 1024),
        ("medium", 1024 * 1024),
        ("large", 64 * 1024 * 1024),
    ]
}

// =============================================================================
// HELPERS
// =============================================================================

fn create_config(shard_dir: PathBuf, variant_param: u64) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        shard_dir,
        // ... configure based on variant_param
        ..Default::default()
    }
}

// =============================================================================
// BENCHMARKS
// =============================================================================

fn bench_my_operation(c: &mut Criterion) {
    let mut group = c.benchmark_group("my_operation");

    // Criterion settings
    group.sample_size(10);                              // Number of samples (default 100)
    group.measurement_time(Duration::from_secs(5));     // Time per sample
    group.warm_up_time(Duration::from_secs(1));         // Warmup before measuring

    // Set throughput for Criterion to calculate ops/sec or bytes/sec
    let bytes_per_iteration = PAYLOAD_SIZE_BYTES * ITERATIONS_PER_SAMPLE;
    group.throughput(Throughput::Bytes(bytes_per_iteration as u64));

    for (variant_name, variant_param) in config_variants() {
        group.bench_with_input(
            BenchmarkId::new("variant", variant_name),
            &variant_param,
            |b, &variant_param| {
                // iter_custom: we control timing manually
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        // Create fresh temp directory for each iteration
                        let tempdir = tempdir().unwrap();
                        let shard_dir = tempdir.path().to_path_buf();

                        // Run in Glommio executor
                        let iteration_duration = LocalExecutorBuilder::new(Placement::Fixed(0))
                            .spawn(move || async move {
                                // Setup (not measured)
                                let config = create_config(shard_dir, variant_param);
                                let shard = Rc::new(ShardWal::open(config).await.unwrap());

                                // === MEASURED SECTION ===
                                let start = Instant::now();

                                for i in 0..ITERATIONS_PER_SAMPLE {
                                    let result = shard.some_operation(i).await;
                                    black_box(result.unwrap());
                                }

                                let elapsed = start.elapsed();
                                // === END MEASURED SECTION ===

                                // Cleanup
                                shard.close().await.unwrap();
                                elapsed
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

    group.finish();
}
```

## Key Patterns

### Use `iter_custom` for Async Code

Criterion's normal `iter()` doesn't work with async. Use `iter_custom` with manual timing:

```rust
b.iter_custom(|iters| {
    let mut total = Duration::ZERO;
    for _ in 0..iters {
        let duration = run_glommio_benchmark();
        total += duration;
    }
    total
});
```

### Spawn Glommio Executor Per Iteration

Each iteration gets a fresh executor. This ensures clean state:

```rust
let duration = LocalExecutorBuilder::new(Placement::Fixed(0))
    .spawn(move || async move {
        // Benchmark code
    })
    .unwrap()
    .join()
    .unwrap();
```

### Use `black_box` to Prevent Optimization

Prevent the compiler from optimizing away results:

```rust
let result = shard.write(request).await;
black_box(result.unwrap());
```

### Concurrent Operations with spawn_local

For benchmarking throughput under concurrency:

```rust
let mut handles = Vec::new();
for i in 0..CONCURRENT_OPS {
    let shard = shard.clone();
    let handle = glommio::spawn_local(async move {
        let start = Instant::now();
        let result = shard.write(make_request(i)).await;
        black_box(result.unwrap());
        start.elapsed()
    });
    handles.push(handle);
}

// Collect results
let mut total_time = Duration::ZERO;
for h in handles {
    total_time += h.await;
}
```

### Wave-Based Arrival Pattern

Simulate realistic request arrival (not all at once):

```rust
let num_waves = TOTAL_OPS / OPS_PER_WAVE;
for wave in 0..num_waves {
    // Submit wave of operations
    for i in 0..OPS_PER_WAVE {
        let handle = glommio::spawn_local(async move { ... });
        handles.push(handle);
    }

    // Delay before next wave
    if wave < num_waves - 1 {
        glommio::timer::sleep(INTER_WAVE_DELAY).await;
    }
}
```

## Cargo.toml Setup

```toml
[[bench]]
name = "my_benchmark"
harness = false  # Use Criterion's main, not default test harness

[dev-dependencies]
criterion = "0.5"
tempfile = "3"
```

## Running Benchmarks

```bash
# Run specific benchmark
cargo bench -p celeriant_shard --bench write_benchmark

# Run all benchmarks in a crate
cargo bench -p celeriant_shard

# Generate HTML report (in target/criterion/)
cargo bench -p celeriant_shard --bench write_benchmark -- --plotting-backend plotters
```

## Example Output

```
write_fsync_delay/multi_aggregate/0ms_sync
                        time:   [2.4521 ms 2.5134 ms 2.5891 ms]
                        thrpt:  [39.54 MB/s 40.73 MB/s 41.74 MB/s]

write_fsync_delay/multi_aggregate/5ms
                        time:   [1.2341 ms 1.2567 ms 1.2834 ms]
                        thrpt:  [79.78 MB/s 81.45 MB/s 82.93 MB/s]
```

## Existing Benchmarks

| Benchmark | Crate | Purpose |
|-----------|-------|---------|
| `write_benchmark` | celeriant_shard | Fsync delay and cache impact on writes |
| `bloom_size_benchmark` | celeriant_shard | Bloom filter effectiveness vs aggregate count |
| `exists_benchmark` | celeriant_shard | Exists check performance |
| `aggregate_count_benchmark` | celeriant_shard | Scaling with aggregate count |
| `wire_format_benchmark` | celeriant_wire | Serialization performance |
| `wire_header_benchmark` | celeriant_wire | Header encoding performance |
| `read_objects_benchmark` | celeriant_disk | DMA read performance |
