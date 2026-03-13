# Celeriant Integration Tests

Integration tests for the Celeriant database. All tests are compiled into a single binary with CLI-driven filtering by category, deployment mode, or individual test name. Each test spawns its own server instance(s) in a subprocess with temporary data directories. Tests requiring S3 also manage their own MinIO container lifecycle via Docker.

## Prerequisites

- Server binary built: `cargo build --release -p celeriant`
- Docker installed (required for distributed/S3 tests)

## Running Tests

```bash
# Build and run all tests
cargo run --release -p celeriant_integration_tests --

# Run a specific test
cargo run --release -p celeriant_integration_tests -- --test single

# Run with a custom timeout (seconds, applied to every test)
cargo run --release -p celeriant_integration_tests -- --timeout 120
```

## Filtering

Tests can be filtered by category, deployment mode, or name. Multiple filters compose together.

### Category filters

```bash
# Include tests matching ALL listed categories (AND)
cargo run --release -p celeriant_integration_tests -- --include core,performance

# Include tests matching ANY listed category (OR)
cargo run --release -p celeriant_integration_tests -- --include-or correctness,security

# Exclude tests matching ALL listed categories (AND)
cargo run --release -p celeriant_integration_tests -- --exclude core,performance

# Exclude tests matching ANY listed category (OR)
cargo run --release -p celeriant_integration_tests -- --exclude-or performance

# Combine: election tests, but not performance-tagged ones
cargo run --release -p celeriant_integration_tests -- --include-or election --exclude-or performance
```

### Deployment mode filters

```bash
# Only standalone tests (no Docker/MinIO required)
cargo run --release -p celeriant_integration_tests -- --standalone

# Only distributed tests (require MinIO + multi-node)
cargo run --release -p celeriant_integration_tests -- --distributed
```

### Listing

```bash
# List matching tests without running them
cargo run --release -p celeriant_integration_tests -- --list

# List with filters
cargo run --release -p celeriant_integration_tests -- --include-or edge --standalone --list

# List all categories with test counts
cargo run --release -p celeriant_integration_tests -- --list-categories
```

---

## Categories

| Category | Description |
|---|---|
| `core` | Basic CRUD, connections, watch API, typed operations, connection pooling |
| `replication` | S3 fallback, follower catchup, leader solo, reconvergence, read visibility |
| `election` | S3 lease election, failover, stale lease, network partition |
| `fencing` | Write rejection on fenced nodes, concurrent CAS |
| `invariant` | Property checks: event counts, convergence, deduplication, queue pressure |
| `edge` | Corner cases: cache eviction, WAL divergence, lock contention, corruption detection |
| `correctness` | Pilot Phase 1: DCB rollback, OCC conflicts, cross-shard rejection, exactly-once, ordering, multi-tenancy |
| `durability` | Pilot Phase 2: write survival, dual restart, WAL corruption, S3 capacity |
| `performance` | Pilot Phase 3 + benchmarks: cold reads, bloom filters, throughput |
| `operations` | Rolling upgrade, standalone-to-distributed migration |
| `security` | mTLS, client identity, API key auth |
| `schema` | Schema registration, enforcement, failover, crash recovery |
| `compaction` | Space reclamation, restart survival, replicated compaction |
| `debug` | Follower pressure debugging (not part of regular suite) |

---

## Test Infrastructure

All shared infrastructure lives in `src/lib.rs`.

### TestServer

Spawns the `celeriant` binary as a subprocess with a temporary data directory. Cleans up on drop.

```rust
// Start with defaults (1 shard, standalone, warn log level)
let server = TestServer::start().await?;

// Start with custom config and label
let server = TestServer::start_with_config_labeled(port, config, "leader".into()).await?;

// Start against an existing data directory (e.g. pre-populated WAL)
let server = TestServer::start_with_existing_dir(port, config, "follower".into(), temp_dir).await?;

// Restart after stop (same data directory, same config)
server.stop();
server.restart().await?;

// Restart with a new config (e.g. standalone -> distributed)
server.restart_with_config(new_config).await?;

// Health check
server.check_alive()?;
```

Server logs are printed to stderr prefixed with `[label]`. Use `start_with_config_labeled` for multi-node tests to keep logs readable.

The server binary is located automatically next to the running test binary. If not found there, it falls back to `target/release/celeriant`. Build it with:

```bash
cargo build --release -p celeriant
```

### MinioContainer

Manages a MinIO Docker container for S3 tests. Cleans up on drop.

```rust
// Start with default bucket name "test-fallback"
let minio = MinioContainer::start(port).await?;

// Start with custom bucket
let minio = MinioContainer::start_with_bucket(port, "my-bucket").await?;

// Get S3 config fields to pass to ServerConfig
let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

// Inspect S3 state from tests
let paths = minio.list_objects("shard_0/").await?;
let bytes = minio.get_object("shard_0/batch_1_10.bin").await?;
minio.put_object("shard_0/batch_1_10.bin", garbage_bytes).await?;
minio.delete_object("shard_0/batch_1_10.bin").await?;

// Simulate S3 outage
minio.pause()?;
minio.unpause()?;
```

### TcpProxy

An in-process TCP proxy for simulating network conditions between nodes.

```rust
// Start proxy: connections to listen_port are forwarded to target_address
let proxy = TcpProxy::start(listen_port, target_address).await?;

// Get the address to give to the follower config
let proxy_addr = proxy.address(); // "127.0.0.1:{listen_port}"

// Simulate a network partition (new connections dropped, existing connections killed)
proxy.block();
proxy.unblock();

// Simulate a slow link (adds delay per 8KB chunk forwarded)
proxy.throttle(50); // 50ms per chunk
proxy.unthrottle();
```

### Helper Functions

```rust
// Write a single numbered event to an aggregate
write_event(&mut client, &aggregate_key, event_num, allow_create).await?;

// Write a large event to create replication backpressure
write_large_event(&mut client, &aggregate_key, event_num, payload_bytes).await?;

// Count all events stored for an aggregate (handles pagination)
let count = count_events(&mut client, &aggregate_key).await?;

// Probe whether a node is the current leader (attempts a write)
let leader = is_leader("127.0.0.1:10000").await?;

// Build a ServerConfig pre-filled with S3 credentials from MinioContainer
let config = s3_cluster_config(num_shards, region, bucket, access_key, secret_key, endpoint, allow_http);

// Copy shard_* directories between temp dirs (skips node identity files)
copy_shard_dirs(src_path, dst_path)?;
```

---

## Manual Cluster Setup

Spin up a leader/follower pair locally for manual testing:

```bash
cargo run --release -p celeriant -- \
  --data-root data_follower --client-port 10002 --replication-port 10003 \
  --cluster-role follower --num-shards 1

cargo run --release -p celeriant -- \
  --data-root data_leader --client-port 10000 --replication-port 10001 \
  --cluster-role leader --follower-address 127.0.0.1:10003 --num-shards 1
```

## Manual MinIO Setup

```bash
docker run -d \
  --name celeriant-minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Create the bucket
docker exec celeriant-minio mkdir -p /data/celeriant

# API:     http://127.0.0.1:9000
# Console: http://127.0.0.1:9001  (minioadmin / minioadmin)

# Tear down
docker rm -f celeriant-minio
```
