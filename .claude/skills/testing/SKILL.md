---
name: testing
description: Guidelines for writing tests in Celeriant. Use when creating unit tests, integration tests, or benchmarks. Covers test utility patterns, Glommio async testing, live server integration tests, and Criterion benchmarks.
---

# Testing in Celeriant

## Before Writing Tests

**Verify functionality first.** Don't write tests that blindly replicate existing code behavior.

1. Read the design spec (README.md in the crate, or linked documentation)
2. Understand the intended behavior, not just the current implementation
3. Ask clarifying questions if behavior seems incorrect or unclear
4. If the code looks buggy, flag it before writing tests that cement bugs

## Test Philosophy

### Keep Tests Readable

Tests should be **concise** and **scannable**. Avoid:
- Verbose comments explaining obvious things
- Boilerplate that obscures what's being tested
- Overly generic helper names like `setup()` or `make_thing()`

Prefer:
- Descriptive test function names that explain the scenario
- Specific helper functions with clear names (`make_write_request_for_aggregate`)
- Let the code speak for itself

### Extract Common Utilities Proactively

When you see patterns repeated across tests:
- Extract into module-level helper functions
- Place shared utilities in a `test_helpers` module
- Keep utilities close to where they're used (same file or `tests/` directory)

```rust
// Good: Specific, reusable helper
fn create_events(count: usize, size: usize) -> Vec<DatablockAggregateEvent> { ... }

// Good: Clear factory with sensible defaults
fn make_aggregate_key(org: u128, agg_type: u128, agg_id: u128) -> AggregateKey { ... }

// Bad: Generic name, unclear purpose
fn setup() -> TestContext { ... }
```

## Test Types

### Unit Tests

For testing pure functions, data structures, and isolated logic. Use Glommio executor wrapper for async code.

**When to add unit tests:**
- Pure functions with clear inputs/outputs
- Error handling paths
- Edge cases (empty collections, boundary values, invalid inputs)
- Serialization roundtrips

**See [UNIT_TESTS.md](UNIT_TESTS.md)** for Glommio executor patterns and test utilities.

### Integration Tests

For testing the full system with a live server and real client connections.

**Key concepts:**
- Tests spawn a real server subprocess with `TestServer`
- Connect via `CeleriantClient` (tokio-based)
- Each test gets a fresh temp directory
- Server cleans up automatically on drop

**See [INTEGRATION_TESTS.md](INTEGRATION_TESTS.md)** for skeleton examples.

### Benchmarks

For measuring performance with Criterion and Glommio.

**When to benchmark:**
- Write/read throughput
- Latency under load
- Cache effectiveness
- Fsync amortization impact

**See [BENCHMARKS.md](BENCHMARKS.md)** for the benchmark template.

## Running Tests

```bash
# Unit tests for a specific crate
cargo test -p celeriant_wire

# All unit tests
cargo test

# Integration tests (each is a separate binary)
cargo run --bin single_main -p celeriant_integration_tests --release
cargo run --bin batch_main -p celeriant_integration_tests --release

# Benchmarks
cargo bench -p celeriant_shard --bench write_benchmark

# Benchmark baseline updates
cargo bench --package celeriant_shard --bench aggregate_count_benchmark -- --save-baseline aggregate_count_baseline
critcmp --export aggregate_count_baseline > ./celeriant_shard/benches/celeriant_shard_aggregate_count.json

# For LLM analysis
critcmp aggregate_count_baseline > ./celeriant_shard/benches/celeriant_shard_aggregate_count.txt
```

## Anti-Patterns

### Don't Write Verbose AI-Style Comments

```rust
// Bad: Explaining obvious things
#[test]
fn test_write_succeeds() {
    // First, we need to create a new cache instance which will be used
    // to store our test data. This cache is configured with default settings.
    let cache = new_cache();

    // Now we create an aggregate key with org=1, type=1, id=1
    let key = make_aggregate_key(1, 1, 1);
    ...
}

// Good: Let the code speak
#[test]
fn pending_append_sets_requires_write_flag() {
    let mut cache = new_cache();
    let key = make_aggregate_key(1, 1, 1);

    assert!(!cache.requires_write());
    cache.add_to_pending_append_queue(&key, 1, 1, 100, 1, make_queue_item(Some(100)));
    assert!(cache.requires_write());
}
```

### Don't Duplicate Test Logic

```rust
// Bad: Copy-pasted setup in every test
#[test]
fn test_scenario_a() {
    let file_len = 1024 * 1024 * 1024;
    let metablocks_position = FIXED_BLOCK_SIZE_BYTES as u64;
    let datablocks_position = file_len - FIXED_BLOCK_SIZE_BYTES as u64;
    let cache = ShardMemCache::new(file_len, metablocks_position, datablocks_position, 0, None, test_config(), 0);
    ...
}

// Good: Extracted helper
fn new_cache() -> ShardMemCache {
    let file_len = 1024 * 1024 * 1024;
    let metablocks_position = FIXED_BLOCK_SIZE_BYTES as u64;
    let datablocks_position = file_len - FIXED_BLOCK_SIZE_BYTES as u64;
    ShardMemCache::new(file_len, metablocks_position, datablocks_position, 0, None, test_config(), 0)
}

#[test]
fn test_scenario_a() {
    let cache = new_cache();
    ...
}
```

### Don't Test Implementation Details

```rust
// Bad: Testing internal state that could change
assert_eq!(cache.internal_buffer_size(), 4096);

// Good: Testing observable behavior
let result = cache.write(&data);
assert!(result.is_ok());
let read_back = cache.read();
assert_eq!(read_back, data);
```

## Checklist for New Tests

1. [ ] Read the design spec/README for the component
2. [ ] Verify the functionality makes sense before testing it
3. [ ] Ask questions if behavior is unclear
4. [ ] Extract shared utilities (don't duplicate setup code)
5. [ ] Use descriptive test names (not `test_1`, `test_2`)
6. [ ] Keep comments minimal and meaningful
7. [ ] Test behavior, not implementation details
