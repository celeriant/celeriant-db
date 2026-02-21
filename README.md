# Celeriant

A fast, distributed, append-only write-ahead log built specifically for event sourcing. It is dedicated to the write side of [CQRS](https://www.martinfowler.com/bliki/CQRS.html).

Not a relational database. Not a message broker. Just the write side of event sourcing, done properly.

## Why Celeriant

PostgreSQL gives you correctness but not throughput. Kafka gives you throughput but not correctness. Celeriant gives you both.

**PostgreSQL** — Transactions and ACID are great, but append-only, high-cardinality, small-write workloads are its worst case. You will spend more time tuning vacuum and fighting WAL amplification than building your product. Real-world event stores hit 10–20k writes/sec on PostgreSQL before it buckles.

**Kafka** — Scales to millions of writes/sec, but it has no conditional writes and no per-aggregate ordering guarantee. Before a service can write events it needs to validate state, and that state may have changed between the read and the write. Kafka cannot help with that. There is also no way to filter events per-aggregate—every consumer gets the whole topic firehose.

**Celeriant** — Per-aggregate ordering, optimistic concurrency control, exactly-once writes, and durable cluster replication. Scales to millions of aggregates with bounded memory regardless of cardinality.

## What Celeriant Is

A distributed, append-only log organised by aggregate:

```
org_id / aggregate_type_id / aggregate_id → ordered event stream
```

Each aggregate is an independent, totally-ordered stream. Writes are acknowledged after durable disk write and quorum replication.

**You get:**
- Per-aggregate total ordering (no gaps, no reordering)
- Optimistic concurrency control (expected batch index)
- Dynamic consistency boundaries — conditionally, atomically write events to multiple aggregates
- Exactly-once writes (client idempotency, duplicate writes rejected)
- Infinite cardinality (millions of aggregates, no tuning, bounded memory)
- Explicit offsets (you control your read position)
- Event type filtering (bloom filter accelerated)
- Watch API (real-time change notifications)
- Compression (zstd, snappy, brotli, gzip)
- In-memory read cache (recent events served from memory)
- Per-event encryption support (AES-GCM, stored opaquely by server)
- Blake3 hash chain linking every event to its predecessor (tamper-evident audit log)

**You don't get:**
- Queries or indexes
- Projections
- Consumer groups or automatic offset management
- Schema validation (not yet implemented)
- mTLS or OAuth (not yet implemented)

It is not a state machine, a message streaming platform, or a queue. You have to build your read side yourself.

## Performance

Benchmarked on a 32-core CPU with a single NVMe over PCIe5. Single "Hello World" event payload per acknowledged write. Clients and servers on localhost.

### Throughput benchmark (25,000 open connections)

| Mode | Throughput | Avg latency | p99 latency |
|------|------------|-------------|-------------|
| Single node | 350,000 writes/sec | 68ms | 170ms |
| Replicated cluster | 190,000 writes/sec | 125ms | 272ms |

### Latency benchmark (1,000 open connections)

| Mode | Throughput | Avg latency | p99 latency |
|------|------------|-------------|-------------|
| Single node | 50,000 writes/sec | 20ms | 27ms |
| Replicated cluster | 15,000 writes/sec | 63ms | 88ms |

Kafka on comparable hardware with default settings: ~40,000 writes/sec with no OCC, no per-aggregate ordering.

## Architecture

### Thread-Per-Core

One glommio executor per CPU core. Each core owns a subset of aggregates. No locks on the hot path. Inspired by [ScyllaDB](https://www.scylladb.com/) and [TigerBeetle](https://tigerbeetle.com/).

### Durability Guarantees

Celeriant uses **Direct I/O**, bypassing the Linux kernel page cache. Buffered I/O through the page cache is vulnerable to [silent data loss on fsync failure](https://lwn.net/Articles/752063/). Direct I/O ensures fsync failures surface immediately.

Events are batched and written durably to disk on the leader. The leader replicates to the follower, which also does a durable write. The leader ACKs to clients only after both writes confirm. Fsync and replication are amortised across concurrent writers to avoid paying the full fsync cost per write.

Every metablock in the WAL includes `previous_tip_hash`, forming a Blake3 hash chain. This makes the log tamper-evident and allows followers to verify integrity without requiring identical on-disk layout.

### Replication and Cluster Coordination

Two nodes: leader and follower. No Raft. No Paxos.

Leader election and coordination use [S3 conditional writes](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/). A single CAS-protected S3 object grants leader exclusivity. This is operationally simpler than consensus protocols and sufficient for append-only workloads.

If the follower is unreachable, the leader replicates to S3 instead. No write is acknowledged until it is on two storage systems.

| Scenario | Behaviour |
|----------|-----------|
| Leader crash | Follower takes over after lease expiry (~15s) |
| Follower crash | Leader replicates to S3, continues accepting writes |
| Network partition | Fencing tokens prevent split-brain data corruption |
| S3 outage | Replication continues to follower, coordination degrades gracefully |

Clock skew must be < 5 seconds. Use NTP.

### Memory and Indexing Design

Celeriant is designed for very high aggregate cardinality without memory growth proportional to aggregate count.

- **Bloom filters**: per-log-segment bloom filters skip entire WAL segments during existence checks and event type filtering
- **LRU caches**: fixed-size bounds on all caches; cold aggregates fall back to reverse WAL scanning
- **Reverse WAL scanning**: disk reads for cold aggregates are efficient; the WAL is structured so reverse scans are O(log n) via bloom filter pruning

One aggregate or ten million — the same memory footprint. You can model one stream per user, per device, per order, per game match, without worrying about whether your infrastructure will fall over.

## Quick Start

Build and run standalone:

```bash
cargo build --release -p celeriant

./target/release/celeriant \
  --standalone \
  --data-root /var/lib/celeriant \
  --client-port 10000
```

Run a clustered pair with S3 coordination:

```bash
celeriant \
  --data-root /var/lib/celeriant \
  --listen-address 10.0.0.1 \
  --client-port 10000 \
  --replication-port 10001 \
  --s3-enabled \
  --s3-region us-east-1 \
  --s3-bucket my-celeriant-cluster \
  --s3-subfolder prod
```

Or via environment variables in a `.env` file:

```
CELERIANT_DATA_ROOT=/var/lib/celeriant
CELERIANT_LISTEN_ADDRESS=10.0.0.1
CELERIANT_S3_ENABLED=true
CELERIANT_S3_REGION=us-east-1
CELERIANT_S3_BUCKET=my-celeriant-cluster
CELERIANT_S3_SUBFOLDER=prod
CELERIANT_S3_ACCESS_KEY_ID=AKIA...
CELERIANT_S3_SECRET_ACCESS_KEY=...
```

## API Examples

```rust
// Write events
client.send_request(Request::Write(WriteRequest {
    writes: hashmap! {
        aggregate_key => SingleAggregateWrite {
            events: vec![Event { event_type: 1, payload: bytes }],
            expected_event_batch_index: Some(0), // OCC
            allow_create: true,
            ..Default::default()
        },
    },
    ..Default::default()
})).await?;

// Read events with filtering
client.send_request(Request::Read(ReadRequest {
    aggregate_key,
    filters: ReadFilters::new(1)
        .include_event_types(vec![1, 2, 3]),
    ..Default::default()
})).await?;
```

## Multi-Aggregate Writes

Atomic writes across multiple aggregates with OCC on all of them:

```rust
client.send_request(Request::Write(WriteRequest {
    writes: hashmap! {
        aggregate_a => SingleAggregateWrite {
            events: vec![...],
            expected_event_batch_index: Some(5),
            ..Default::default()
        },
        aggregate_b => SingleAggregateWrite {
            events: vec![...],
            expected_event_batch_index: Some(12),
            ..Default::default()
        },
    },
    ..Default::default()
})).await?;
```

All aggregates must hash to the same shard. If any OCC check fails, the entire write is rejected. No partial writes.

## Watch API

Subscribe to aggregate changes in real-time:

```rust
// Sends WatchRequest, then streams WatchResponse messages
client.send_request(Request::Watch(WatchRequest {
    aggregates: Some(hashset![aggregate_key]),
    requested_latency_ms: Some(10),
    ..Default::default()
})).await?;

// Events arrive as WatchResponse with merged batch index ranges per aggregate
```

Backpressure via bounded channels. If a client cannot keep up, it gets disconnected.

## Listing and Discovery

```rust
// List all orgs in a shard
client.send_request(Request::ListOrgs(ListOrgsRequest { .. })).await?;

// List aggregate types
client.send_request(Request::ListAggregateTypes(ListAggregateTypesRequest {
    org_id: Some(org_id),
    ..Default::default()
})).await?;

// List aggregates with metadata (paginated)
client.send_request(Request::ListAggregates(ListAggregatesRequest {
    org_id: Some(org_id),
    aggregate_type_id: Some(type_id),
    cursor: None,
    limit: 1000,
    ..Default::default()
})).await?;

// Aggregate existence and metadata
client.send_request(Request::AggregateDetails(AggregateDetailsRequest {
    aggregate_key,
    ..Default::default()
})).await?;
```

## Sharding

Aggregates are assigned to shards by configurable routing:

| Rule | Routes By |
|------|-----------|
| `OrgId` | `org_id % num_shards` |
| `AggregateTypeId` | `aggregate_type_id % num_shards` |
| `AggregateId` | `aggregate_id % num_shards` |

Clients connect to any shard. Requests are redirected to the owning shard automatically.

## Multi-Tenancy

Aggregates are namespaced by `org_id`. No shared state between orgs. No cross-org queries. Each org's data is independent.

## When Not to Use Celeriant

- You need ad-hoc queries over aggregate state — use a read database
- You want server-managed consumer groups — use Kafka
- You need transactions across arbitrary keys — use a relational database
- You are not doing event sourcing — this is the wrong tool
- You cannot tolerate an eventually consistent read model — use PostgreSQL

Kafka remains a good choice when you do not need OCC and systems do not rely on shared state for their invariants.

## Current State

The project is approximately 80% complete. It is suitable for non-production pilot projects. Missing features:

- WAL compaction
- mTLS and OAuth
- Schema validation for event types

The plan is to release as Apache 2.0. Target: mid-2026.

## Crate Structure

Ordered by dependency level, from lowest to highest.

| Crate | Description |
|-------|-------------|
| [`celeriant_wal`](celeriant_wal/README.md) | Data structures for the WAL — types and serialization only, no I/O |
| [`celeriant_crypto`](celeriant_crypto/README.md) | Cryptographic key generation, node identity, and nonce-based client signing |
| [`celeriant_disk`](celeriant_disk/README.md) | Low-level DMA I/O primitives using Direct I/O via glommio |
| [`celeriant_wire`](celeriant_wire/README.md) | Serialization, compression, and wire protocol framing for network and WAL persistence |
| [`celeriant_msg`](celeriant_msg/README.md) | Request and response message types for the Celeriant wire protocol |
| [`celeriant_watch`](celeriant_watch/README.md) | Watch and subscription system for real-time aggregate change notifications |
| [`celeriant_memcache`](celeriant_memcache/README.md) | In-memory caching layer: write queues, aggregate positions, client idempotency, replication visibility |
| [`celeriant_rotating_log`](celeriant_rotating_log/README.md) | Rotating WAL log segments with LRU caching, DMA I/O, bloom filter optimization, and crash recovery |
| [`celeriant_sidecar`](celeriant_sidecar/README.md) | Object store abstraction for S3: conditional puts, batch deletes, listing. Runs in a tokio sidecar |
| [`celeriant_distributed`](celeriant_distributed/README.md) | Leader/follower coordination: S3 lease election, membership, heartbeat, node status state machine |
| [`celeriant_shard`](celeriant_shard/README.md) | Shard-level WAL orchestrator: validation, durability, replication, S3 catchup, caching, read filtering |
| [`celeriant_runtimes`](celeriant_runtimes/README.md) | Runtime orchestration: sharded glommio executors, inter-shard routing, cluster coordination, sidecar bridge |
| [`celeriant`](celeriant/README.md) | Server executable: CLI parsing, environment validation, DIO check, executor launch |
| [`celeriant_client_glommio`](celeriant_client_glommio/README.md) | Async TCP client for Celeriant using the glommio runtime — used internally for replication |
| [`celeriant_client_tokio`](celeriant_client_tokio/README.md) | Async TCP client for Celeriant using the tokio runtime — for application use |
| [`celeriant_cli`](celeriant_cli/README.md) | Command-line interface and terminal UI for interacting with the event store |
| [`celeriant_embedded`](celeriant_embedded/README.md) | Reserved for future in-process embedded mode |
| [`celeriant_integration_tests`](celeriant_integration_tests/README.md) | Integration tests: correctness, chaos, S3 replication, failover, edge cases, qualification suite |

## License

Apache-2.0
