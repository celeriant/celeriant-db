# Celeriant Integration Tests

Integration tests for the Celeriant database. Each test binary spawns its own server instance(s) with temporary data directories. Tests requiring S3 also manage their own MinIO container lifecycle via Docker.

## Prerequisites

- Server binary built: `cargo build --release -p celeriant`
- Docker installed (required for any test in the S3 categories)

## Running Tests

All tests use `--release` for realistic timing:

```bash
cargo run --bin <test_name> -p celeriant_integration_tests --release
```

---

## Test Categories

### Core Tests

Basic correctness, concurrency, and connection handling. No Docker required.

| Binary | What it tests |
|---|---|
| `single_main` | CRUD operations, idempotency, list functionality |
| `batch_main` | Write throughput; measures request latency percentiles across many concurrent connections |
| `chaos_main` | Concurrent read/write stress with variable payload sizes (1 byte to 5MB) |
| `chaos_delete_main` | Write/delete/read concurrency with final state verification across multiple orgs and types |
| `watch_test_main` | Watch API: streaming subscriptions, aggregate filtering, heartbeats, multiple concurrent watchers |
| `connection_test_main` | Connection handling: pipelining, cross-shard routing, connection churn, long-lived connections |

```bash
cargo run --bin single_main -p celeriant_integration_tests --release
cargo run --bin chaos_main -p celeriant_integration_tests --release
```

`batch_main` supports environment variable tuning:

```bash
NUM_CONNECTIONS=4096 cargo run --bin batch_main -p celeriant_integration_tests --release
SWEEP_MODE=1 cargo run --bin batch_main -p celeriant_integration_tests --release
```

---

### S3 Fallback Tests

Tests for the S3 fallback replication path: what happens when a follower goes down and the leader buffers batches to S3 for later catchup. Requires Docker.

| Binary | What it tests |
|---|---|
| `s3_fallback_main` | Happy path: follower down, events land in S3 at correct paths (`batch_{start}_{end}.bin`) in lexicographic order |
| `s3_fallback_catchup_main` | Full cycle: normal replication -> follower down -> S3 fallback -> follower restart -> WAL catchup -> normal replication resumes |
| `s3_fallback_no_s3_main` | No S3 configured: follower down, writes roll back with client error; writes resume after follower restarts |
| `s3_fallback_s3_down_main` | S3 configured but unreachable: follower down, S3 put fails, writes roll back with client error |
| `s3_fallback_createonly_main` | Pre-seeds S3 at target paths with garbage; verifies CreateOnly semantics: write succeeds (AlreadyExists treated as OK) but content is not overwritten |

```bash
cargo run --bin s3_fallback_main -p celeriant_integration_tests --release
cargo run --bin s3_fallback_catchup_main -p celeriant_integration_tests --release
```

---

### S3 Leadership and Replication Tests

Tests for distributed consensus via S3 leases: leader election, failover, fencing, and split-brain scenarios. Requires Docker.

| Binary | What it tests |
|---|---|
| `s3_election_main` | Leader election via S3 lease acquisition; verifies only one node holds the lease at a time |
| `s3_failover_main` | Leader failure triggers follower promotion via lease expiry |
| `s3_leader_solo_main` | Leader operating without any follower connected; S3 writes still proceed |
| `s3_follower_crash_main` | Follower crashes mid-replication; leader detects and recovers |
| `s3_stale_lease_main` | Stale lease (old leader's lease still in S3); new leader correctly fences the old one |
| `s3_fencing_writes_main` | Fenced old leader rejects writes correctly |
| `s3_lease_monotonicity_main` | Lease epoch numbers are strictly monotonically increasing across elections |
| `s3_unreachable_failover_main` | Leader becomes unreachable (not crashed); follower promotes after lease expires |
| `s3_network_partition_main` | Network partition between leader and follower via TcpProxy; verifies correct behavior on each side |
| `s3_reconvergence_main` | After a partition heals, cluster reconverges to a single leader with consistent state |
| `s3_old_leader_recovery_main` | Old leader rejoins as follower after being fenced; catches up via WAL or S3 |
| `s3_writes_during_fencing_main` | Writes issued during the fencing window get the correct error response |
| `s3_concurrent_cas_main` | Concurrent compare-and-swap operations from multiple clients; verifies exactly-once semantics |
| `s3_follower_kick_main` | Follower that falls too far behind gets kicked and must re-sync |

```bash
cargo run --bin s3_election_main -p celeriant_integration_tests --release
cargo run --bin s3_network_partition_main -p celeriant_integration_tests --release
```

---

### Invariant Tests

Property checks that must hold across all cluster states. Each test runs a workload then asserts a system invariant. Requires Docker for replication variants.

| Binary | What it tests |
|---|---|
| `invariant_read_count_main` | Event count on reads matches what was written; no phantom or missing events |
| `invariant_concurrent_write_main` | Concurrent writers to the same aggregate preserve ordering and no events are lost |
| `invariant_replication_convergence_main` | Leader and follower eventually agree on event count and content |
| `invariant_s3_fallback_dedup_main` | S3 fallback batches are not applied twice after a follower catches up |
| `invariant_replication_queue_pressure_main` | Under high write pressure the replication queue stays bounded and does not deadlock |

```bash
cargo run --bin invariant_replication_convergence_main -p celeriant_integration_tests --release
```

---

### Edge Case Tests

Targeted tests for specific failure modes and corner cases. Requires Docker for S3 and replication variants.

| Binary | What it tests |
|---|---|
| `edge_empty_replication_batch_main` | Empty batches in the replication stream are handled without errors or hangs |
| `edge_stale_cache_rotation_main` | Cache entries become stale after log rotation; reads return correct data |
| `edge_s3_missing_batches_main` | S3 batch deleted after upload; follower detects the gap and errors appropriately |
| `edge_s3_batch_ordering_main` | S3 batches applied out of order; verifies ordering enforcement |
| `edge_log_rotation_mid_replication_main` | Log file rotates while replication is in progress; no data loss |
| `edge_log_eviction_before_s3_main` | LRU evicts a log segment before S3 upload completes; read still succeeds |
| `edge_heartbeat_lock_contention_main` | Heartbeat and write paths contend on the same locks; no deadlock |
| `edge_concurrent_heartbeat_replication_s3_main` | Heartbeat, replication, and S3 uploads run concurrently; no data corruption |
| `edge_split_brain_s3_unavailable_main` | S3 unavailable during a split-brain window; verifies the cluster does not corrupt data |
| `edge_corrupted_s3_batch_main` | S3 batch contains corrupt bytes; follower rejects and surfaces the error |
| `edge_list_pagination_cache_eviction_main` | Cache eviction mid-pagination; list continues to return correct results |
| `edge_wal_tip_hash_divergence_main` | Tip hash on leader and follower diverge; divergence is detected |
| `edge_wal_divergence_recovery_main` | After WAL divergence is detected, the follower recovers to a consistent state |

```bash
cargo run --bin edge_log_rotation_mid_replication_main -p celeriant_integration_tests --release
```

---

### Phase / Qualification Tests

Structured qualification suite. Tests are grouped by phase (P1 correctness, P2 durability, P3 read performance, P4 operations). Run these before releasing a new version.

**P1 - Correctness**

| Binary | What it tests |
|---|---|
| `p1_1_dcb_rollback_main` | Deterministic conditional batch (DCB) rollback on precondition failure |
| `p1_2_concurrent_dcb_main` | Concurrent DCB writes; only one succeeds per epoch |
| `p1_3_cross_shard_rejection_main` | Writes targeting the wrong shard are rejected |
| `p1_4_exactly_once_main` | Client idempotency: retried writes are not applied twice |
| `p1_6_ordering_verification_main` | Events within an aggregate are always read in write order |
| `p1_7_multitenancy_isolation_main` | Events from one tenant are not visible to another |

**P2 - Durability**

| Binary | What it tests |
|---|---|
| `p2_1_write_survival_main` | Writes survive a server restart (WAL replay) |
| `p2_2_dual_restart_main` | Both leader and follower restart; cluster resumes with no data loss |
| `p2_3_wal_corruption_main` | WAL file partially corrupted on disk; server detects and refuses to start with bad data |
| `p2_4_s3_capacity_main` | S3 bucket fills up during fallback; error surfaces correctly and writes do not hang |

**P3 - Read Performance**

| Binary | What it tests |
|---|---|
| `p3_1_cold_read_latency_main` | Cold read latency from disk (cache empty) stays within acceptable bounds |
| `p3_2_bloom_filter_main` | Bloom filter eliminates disk I/O for non-existent aggregates |
| `p3_3_sequential_cold_reads_main` | Sequential cold reads across many aggregates; measures throughput |

**P4 - Operations**

| Binary | What it tests |
|---|---|
| `p4_1_rolling_upgrade_main` | Rolling upgrade: follower upgraded first while leader still runs old version |

```bash
# Run the full P1 suite
for t in p1_1_dcb_rollback_main p1_2_concurrent_dcb_main p1_3_cross_shard_rejection_main p1_4_exactly_once_main p1_6_ordering_verification_main p1_7_multitenancy_isolation_main; do
  cargo run --bin $t -p celeriant_integration_tests --release
done
```

---

### Debugging and Migration Tests

Tools for investigating specific issues, not part of the regular qualification suite.

| Binary | What it does |
|---|---|
| `debug_follower_pressure_main` | Applies sustained write pressure to expose follower queue backpressure issues |
| `standalone_to_distributed_main` | Starts a node in standalone mode, migrates it to a distributed cluster, verifies data survives |

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
