---
name: testing
description: How to write and run tests in Celeriant. Unit tests with glommio_test! macro, integration tests via the test runner with category registry, Criterion benchmarks. Use when creating tests or benchmarks.
---

# Testing

## Unit Tests

Async code needs a Glommio executor. Use the `glommio_test!` macro defined locally in test modules:

```rust
macro_rules! glommio_test {
    ($body:expr) => {
        LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move { $body })
            .unwrap()
            .join()
            .unwrap()
    };
}

#[test]
fn my_async_test() {
    glommio_test!({
        let result = some_async_fn().await;
        assert!(result.is_ok());
    });
}
```

The macro is defined per test module (not crate-wide). See `celeriant_shard/src/shard_wal_sync.rs`, `celeriant_rotating_log/src/log_segment_file.rs`, `celeriant_watch/src/tests.rs` for examples.

Test philosophy: test observable behavior, not implementation details. Understand the design spec before writing tests. Extract common helpers (`test_dir()`, `test_config()`, `make_aggregate_key()`). Descriptive function names (`write_with_empty_events_returns_error`).

## Integration Tests

Single binary with a test runner and category-based registry. Not separate binaries.

Each test is a module with `pub async fn run()` that spawns real server processes via `TestServer`, connects with `CeleriantClient`, and gets a fresh temp dir. The runner dispatches via `--run-test <name>` as subprocess, capturing output to `/tmp/celeriant_test_<name>.log`.

### Build First

Integration tests spawn the Celeriant server as a subprocess. Build it before running tests:

```bash
cargo build -p celeriant --release
```

### Running

```bash
# Run all standalone tests
cargo run -p celeriant_integration_tests --release -- --standalone

# Run a specific test
cargo run -p celeriant_integration_tests --release -- --test single

# Filter by category (AND)
cargo run -p celeriant_integration_tests --release -- --include core,correctness

# Filter by category (OR)
cargo run -p celeriant_integration_tests --release -- --include-or edge,invariant

# List matching tests without running
cargo run -p celeriant_integration_tests --release -- --standalone --list

# List all categories
cargo run -p celeriant_integration_tests --release -- --list-categories
```

### Adding a New Integration Test

1. Create `celeriant_integration_tests/src/my_test.rs` with `pub async fn run() -> Result<(), Box<dyn std::error::Error>>`
2. Add `pub mod my_test;` to `lib.rs`
3. Add a `TestEntry` to `registry.rs` with name, description, estimated_secs, categories, and whether it requires distributed (MinIO + multi-node)
4. Add the dispatch arm in `main.rs` `dispatch_test()`: `"my_test" => my_test::run().await`

Categories: Core, Replication, Election, Fencing, Invariant, Edge, Correctness, Durability, Performance, Operations, Security, Schema, Compaction, Debug.

## Benchmarks

Criterion for statistical measurement, Glommio for async execution. Use `iter_custom` with manual timing since Criterion's `iter()` doesn't work with async. Fresh Glommio executor per iteration for clean state. `black_box` to prevent optimisation.

Existing benchmarks live under `celeriant_shard/benches/` and `celeriant_wire/benches/`. Look at them for patterns including wave-based arrival and concurrent `spawn_local`.

Baseline workflow:
```bash
cargo bench --package celeriant_shard --bench aggregate_count_benchmark -- --save-baseline my_baseline
critcmp --export my_baseline > ./celeriant_shard/benches/my_baseline.json
critcmp my_baseline > ./celeriant_shard/benches/my_baseline.txt
```
