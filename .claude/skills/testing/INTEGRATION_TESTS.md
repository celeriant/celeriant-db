# Integration Tests

Integration tests run against a **live server** with a **real client**. Each test spawns its own server subprocess with a temporary data directory.

## Architecture

```
Test Binary (tokio) -> Celeriant Server (subprocess) -> tempdir (auto-delete)
```

## TestServer

The `TestServer` struct handles:
- Creating a temp directory for data
- Spawning the server with `cargo run`
- Waiting for the TCP port to become available
- Killing the server and cleaning up on drop

```rust
// Reference: celeriant_integration_tests/src/lib.rs
pub struct TestServer {
    _temp_dir: TempDir,    // Cleaned up on drop
    address: String,       // "127.0.0.1:PORT"
    child: Child,          // Server process
    config: ServerConfig,
}
```

## Basic Test Skeleton

```rust
use celeriant_integration_tests::TestServer;
use celeriant_client_tokio::celeriant_client::CeleriantClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Start server (temp dir + subprocess)
    let server = TestServer::start().await?;

    // 2. Connect client
    let mut client = CeleriantClient::connect(server.address()).await?;

    // 3. Run test operations
    let aggregate_key = AggregateKey::new(1, 2, 101);
    // ... test code ...

    // 4. Server cleans up automatically when `server` is dropped
    Ok(())
}
```

## Server Configuration

Default test config uses 1 shard with durable writes:

```rust
// Start with defaults
let server = TestServer::start().await?;

// Or customize
let config = ServerConfig {
    num_shards: Some(4),
    non_durable_writes: true,
    log_level: "debug".to_string(),
    ..Default::default()
};
let server = TestServer::start_with_config(10200, config).await?;
```

## Write/Read Test Pattern

```rust
async fn test_write_and_read(client: &mut CeleriantClient) -> Result<(), Box<dyn std::error::Error>> {
    let aggregate = AggregateKey::new(1, 2, 101);
    let client_id: u128 = 999;

    // Write
    let event = DatablockAggregateEvent {
        client_event_index: 0,
        event_index: 0,
        event_id: Some(rand::random()),
        event_timestamp: now_millis(),
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(b"test data".to_vec()),
        iv: None,
    };

    let mut writes = HashMap::new();
    writes.insert(aggregate.clone(), SingleAggregateWrite {
        events: vec![event],
        allow_create: true,
        expected_event_batch_index: Some(0),
        enforce_client_idempotency: true,
        compression_type: CompressionType::None,
    });

    let write_request = Request::Write(WriteRequest {
        correlation_id: Some(1),
        client_id,
        user_id: None,
        writes,
    });

    client.send_request(&write_request, CompressionType::None).await?;

    // Read back
    let read_request = Request::Read(ReadRequest {
        correlation_id: None,
        aggregate_key: aggregate,
        filters: ReadFilters::new(1),
    });

    let response = client.send_request(&read_request, CompressionType::None).await?;
    // Verify response...

    Ok(())
}
```

## List Operations Pattern

```rust
async fn test_list_operations(client: &mut CeleriantClient) -> Result<(), Box<dyn std::error::Error>> {
    // List organizations
    let orgs_iter = ListOrgsIterator::new(client, ListOptions::default());
    let orgs = orgs_iter.collect().await?;
    assert!(orgs.iter().any(|o| o.org_id == 1));

    // List aggregate types for an org
    let types_iter = ListAggregateTypesIterator::new(client, Some(1), ListOptions::default());
    let agg_types = types_iter.collect().await?;

    // List aggregates (can filter by org, type)
    let aggs_iter = ListAggregatesIterator::new(client, Some(1), None, ListOptions::default());
    let aggregates = aggs_iter.collect().await?;

    // Include deleted aggregates
    let options = ListOptions { include_deleted: true, ..Default::default() };
    let aggs_iter = ListAggregatesIterator::new(client, Some(1), None, options);
    let all_aggregates = aggs_iter.collect().await?;

    Ok(())
}
```

## Concurrent Connections Test Pattern

```rust
async fn test_concurrent_writes(server: &TestServer, num_connections: usize) {
    let barrier = Arc::new(Barrier::new(num_connections));

    // Establish connections in parallel
    let mut tasks = Vec::new();
    for id in 0..num_connections {
        let addr = server.address().to_string();
        let barrier = barrier.clone();

        tasks.push(tokio::spawn(async move {
            let mut client = CeleriantClient::connect(&addr).await.unwrap();

            // Synchronize start
            barrier.wait().await;

            // Run workload
            for i in 0..100 {
                let request = make_write_request(id, i);
                client.send_request(&request, CompressionType::None).await.unwrap();
            }
        }));
    }

    // Wait for all tasks
    for task in tasks {
        task.await.unwrap();
    }
}
```

## Running Integration Tests

Integration tests are separate binaries, not `#[test]` functions:

```bash
# Run specific test
cargo run --bin single_main -p celeriant_integration_tests --release

# Available tests:
# - single_main: Basic CRUD, idempotency, listing
# - batch_main: Write throughput benchmark
# - chaos_main: Concurrent read/write stress test
# - chaos_delete_main: Write/delete concurrency
# - watch_test_main: Streaming subscriptions
# - connection_test_main: Connection handling, pipelining
```

## Adding New Integration Tests

1. Add a new `[[bin]]` entry in `celeriant_integration_tests/Cargo.toml`:

```toml
[[bin]]
name = "my_new_test"
path = "src/my_new_test.rs"
```

2. Create the test file with `#[tokio::main]` and `TestServer::start()`.
