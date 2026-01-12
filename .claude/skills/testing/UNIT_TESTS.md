# Unit Tests

## Glommio Async Test Pattern

Celeriant uses Glommio for async I/O. Unit tests for async code need a Glommio executor:

```rust
#[cfg(test)]
mod tests {
    use glommio::{LocalExecutorBuilder, Placement};

    #[test]
    fn async_operation_succeeds() {
        LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                // Async test code here
                let result = some_async_fn().await;
                assert!(result.is_ok());
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
```

## Test Utility Patterns

### Temp Directory Helper

```rust
fn create_test_dir() -> (tempfile::TempDir, PathBuf) {
    let tempdir = tempfile::tempdir().unwrap();
    let shard_dir = tempdir.path().join("test_shard");
    (tempdir, shard_dir)
}
```

### Config Factories

Create base config with sensible defaults, then variant helpers:

```rust
fn test_config() -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        max_open_files: 100,
        shard_log_preallocate_bytes: 1024 * 1024 * 1024,
        fsync_delay: Duration::from_millis(5),
        recent_write_cache_bytes: 10000,
        non_durable_writes: false,
        shard_dir: PathBuf::from("/tmp/test_shard"),
        max_response_size: 10 * 1024 * 1024,
        ..Default::default()
    }
}

fn test_config_no_cache() -> InternalShardConfig {
    InternalShardConfig {
        recent_write_cache_bytes: 0,
        ..test_config()
    }
}

fn test_config_small_cache(cache_bytes: u64) -> InternalShardConfig {
    InternalShardConfig {
        recent_write_cache_bytes: cache_bytes,
        ..test_config()
    }
}
```

### Domain Object Factories

```rust
fn make_aggregate_key(org: u128, agg_type: u128, agg_id: u128) -> AggregateKey {
    AggregateKey::new(org, agg_type, agg_id)
}

fn make_events(count: usize, size: usize, base_index: u64) -> Vec<DatablockAggregateEvent> {
    (0..count)
        .map(|i| DatablockAggregateEvent {
            client_event_index: base_index + i as u64,
            event_index: 0,
            event_id: None,
            event_timestamp: 1_700_000_000_000 + i as u64,
            event_type_major: 1,
            event_type_minor: 0,
            event_value: Arc::new(vec![0xABu8; size]),
            iv: None,
        })
        .collect()
}
```

## Test Organization

### Group Related Tests

```rust
#[cfg(test)]
mod initialization_tests {
    use super::*;

    #[test]
    fn new_cache_initializes_positions() { ... }

    #[test]
    fn shard_dir_returns_configured_path() { ... }
}

#[cfg(test)]
mod write_tests {
    use super::*;

    #[test]
    fn pending_append_sets_requires_write() { ... }

    #[test]
    fn pending_append_updates_positions() { ... }
}
```

### Naming Conventions

Use descriptive names that explain the scenario and expected outcome:

```rust
// Pattern: {action}_{condition}_{expected_result}
#[test]
fn write_with_empty_events_returns_error() { ... }

#[test]
fn read_from_nonexistent_aggregate_returns_not_found() { ... }

#[test]
fn rotation_closes_previous_file_and_opens_new() { ... }
```

## Common Assertions

### Testing Error Types

```rust
// Match specific error variants
let result = some_operation();
assert!(matches!(
    result,
    Err(SomeError::SpecificVariant { field: 42 })
));

// Check error kind without matching all fields
assert!(matches!(result, Err(SomeError::NotFound { .. })));
```

### Testing Async State Changes

```rust
#[test]
fn rotation_updates_active_log_id() {
    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(|| async move {
            let (_tempdir, shard_dir) = create_test_dir();
            let cache = RotatingLogCache::new(shard_dir.clone(), 64 * 1024, 2).await.unwrap();

            let initial_id = cache.active_log_id();
            assert_eq!(initial_id, 1);

            // Perform rotation
            {
                let active = cache.active();
                let mut guard = active.write().await.unwrap();
                cache.rotate_to_next_log(&mut guard, &shard_dir, 64 * 1024).await.unwrap();
            }

            assert_eq!(cache.active_log_id(), 2);
            cache.close().await.unwrap();
        })
        .unwrap()
        .join()
        .unwrap();
}
```

## Serialization Roundtrip Tests

For wire format and persistence code:

```rust
#[test]
fn bincode_roundtrip_preserves_data() {
    let original = sample_message();
    let mut buffer = [0u8; 1024];

    let written = bincode_fixed_serialise(&original, &mut buffer).unwrap();
    let decoded: TestMessage = bincode_fixed_deserialise(&buffer[..written]).unwrap().0;

    assert_eq!(original, decoded);
}

#[test]
fn all_compression_types_roundtrip() {
    let original = sample_message();

    for compression in [
        CompressionType::None,
        CompressionType::Zstd { level: 3 },
        CompressionType::Snappy,
    ] {
        let (size, encoded) = bincode_variable_serialise(&original, compression).unwrap();
        let decoded: TestMessage = bincode_variable_deserialise(&encoded, compression, size).unwrap();
        assert_eq!(original, decoded, "Failed for {:?}", compression);
    }
}
```
