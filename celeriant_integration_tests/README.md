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

### S3 Fallback Replication

These tests require Docker (for MinIO). Each test manages its own MinIO container lifecycle.

#### s3_fallback_main

Happy path: stops the follower, writes events, verifies they land in S3 at the correct paths (`batch_{start}_{end}.bin`), and confirms lexicographic ordering.

```bash
cargo run --bin s3_fallback_main -p celeriant_integration_tests --release
```

#### s3_fallback_catchup_main

Full cycle: normal replication, follower goes down (S3 fallback), follower restarts, catches up via WAL, and normal replication resumes. Verifies follower has all events and no new S3 objects appear after catchup.

```bash
cargo run --bin s3_fallback_catchup_main -p celeriant_integration_tests --release
```

#### s3_fallback_no_s3_main

S3 not configured. Follower goes down, leader has no S3 fallback, writes are rolled back and the client sees an error. Verifies writes resume after follower restarts.

```bash
cargo run --bin s3_fallback_no_s3_main -p celeriant_integration_tests --release
```

#### s3_fallback_s3_down_main

S3 configured but unreachable (dead endpoint). Follower goes down, S3 put fails, writes are rolled back and the client sees an error.

```bash
cargo run --bin s3_fallback_s3_down_main -p celeriant_integration_tests --release
```

#### s3_fallback_createonly_main

Pre-seeds S3 objects with garbage at paths the leader will target, then triggers fallback. Verifies `CreateOnly` is in effect: the write succeeds (`AlreadyExists` treated as OK) but the pre-seeded content is not overwritten.

```bash
cargo run --bin s3_fallback_createonly_main -p celeriant_integration_tests --release
```

### Spinning up leader/follower manually
cargo run --release -p celeriant -- --data-root data_follower --client-port 10002 --replication-port 10003 --cluster-role follower --num-shards 1
cargo run --release -p celeriant -- --data-root data_leader --client-port 10000 --replication-port 10001   --cluster-role leader --follower-address 127.0.0.1:10003 --num-shards 1