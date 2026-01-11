# Celeriant Client Demo

Integration tests for the Celeriant database. Each test spawns its own server instance with a temporary data directory.

## Tests

### single_main

Basic CRUD operations, idempotency, and listing functionality. Tests writes, reads, deletes, and verifies list operations work correctly.

```bash
cargo run --bin single_main -p celeriant_integration_tests --release
```

### batch_main

Write throughput benchmark. Opens thousands of concurrent connections and measures request latency percentiles.

```bash
cargo run --bin batch_main -p celeriant_integration_tests --release
```

Set `NUM_CONNECTIONS` to control connection count, or enable `SWEEP_MODE` to test multiple connection counts:
```bash
NUM_CONNECTIONS=4096 cargo run --bin batch_main -p celeriant_integration_tests --release
SWEEP_MODE=1 cargo run --bin batch_main -p celeriant_integration_tests --release
```

### chaos_main

Concurrent read/write stress test with variable payload sizes (1 byte to 5MB). Runs paired reader/writer tasks per aggregate for the configured duration.

```bash
cargo run --bin chaos_main -p celeriant_integration_tests --release
```

### chaos_delete_main

Write/delete/read concurrency test with verification. Creates aggregates across multiple orgs and types, randomly deletes some, then verifies the final state matches expectations.

```bash
cargo run --bin chaos_delete_main -p celeriant_integration_tests --release
```

### watch_test_main

Watch API tests. Verifies streaming event subscriptions, filtering by aggregate, heartbeats, and multiple concurrent watchers.

```bash
cargo run --bin watch_test_main -p celeriant_integration_tests --release
```

### connection_test_main

Connection handling tests. Covers pipelining, cross-shard routing, connection churn, and long-lived connections.

```bash
cargo run --bin connection_test_main -p celeriant_integration_tests --release
```
