# Celeriant

A fast, correct write-ahead log for event sourcing.

Not a database. Not a stream. Just the write side of CQRS, done properly.

## The Problem

Event sourcing keeps getting built on the wrong foundations.

**Postgres** — Transactions and ACID are great. But append-only, high-cardinality, small-write workloads are its worst case. You'll spend more time tuning vacuum and fighting WAL amplification than building your product.

**Kafka** — Excellent at fan-out and replay. Terrible at per-aggregate ordering and optimistic concurrency. Partitions aren't aggregates. No way for consumers to filter by aggregate or event types. Operationally complex. And it dies at ~200k topics per partition. Want one topic per aggregate? Good luck.

**KurrentDB** — Closest competitor. But it mixes read and write concerns, it's slow, and the licensing is complicated.

## What Celeriant Is

A distributed, append-only log organised by aggregate.

```
org_id / aggregate_type_id / aggregate_id → ordered event stream
```

Each aggregate is an independent, totally-ordered stream. Writes are acknowledged after durable disk write and quorum replication.

**You get:**
- Per-aggregate total ordering (no gaps, no reordering)
- Optimistic concurrency control (expected batch index)
- Dynamic consistency boundaries - Conditionally, atomically write events to multiple aggregates
- Client idempotency (duplicate writes rejected)
- Infinite cardinality (millions of aggregates, no tuning)
- Schema validation (aggregate type x event type)
- Explicit offsets (you control your read position)
- Event type filtering (bloom filter accelerated)
- Watch API (real-time change notifications)
- Compression (zstd, snappy, brotli, gzip)
- In-memory read cache (recent events served from memory)

**You don't get:**
- Queries
- Indexes
- Projections
- Consumer groups
- Automatic offset management

It's not a state machine, a message streaming platform or a queue. You have to build your read side yourself.

## Infinite Cardinality

Kafka partitions don't scale past ~200k topics. Postgres tables with millions of aggregate IDs need careful indexing and partitioning. Most event stores require memory proportional to the number of aggregates.

Celeriant doesn't.

Memory usage is bounded and predictable regardless of how many aggregates you have. One aggregate or ten million - it's same memory footprint.

How:
- Bloom filters
- LRU caches with fixed size bounds (not unbounded maps)
- Reverse WAL scanning for cold aggregates (disk is cheap, RAM isn't)

The tradeoff: cold aggregate reads hit disk. Hot aggregates are cached. You get predictable performance without capacity planning.

This means you can model:
- One stream per user
- One stream per device
- One stream per game match
- One stream per order
- One stream per anything

Without worrying about whether your infrastructure will fall over.

## Performance

Single node, NVMe, 16 cores:

| Mode | Throughput | Latency (p99) |
|------|------------|---------------|
| Durable (fsync before ACK) | 370,000 writes/sec | < 30ms |
| Async (fsync in background) | 700,000 writes/sec | < 10ms |

Kafka on the same hardware with default settings: ~40,000 writes/sec.

## Quick Start

```bash
docker run -d \
  -p 10000:10000 \
  -v celeriant-data:/data \
  celeriant/celeriant:latest
```

```rust
// Write events
let response = client.write(WriteRequest {
    aggregate_key: AggregateKey { org_id, aggregate_type_id, aggregate_id },
    events: vec![Event { event_type: 1, payload: bytes }],
    expected_event_batch_index: Some(0), // OCC
    allow_create: true,
    ..Default::default()
}).await?;

// Read events
let events = client.read(ReadRequest {
    aggregate_key,
    from_event_batch_index: Some(0),
    include_event_types: Some(vec![1, 2, 3]),
    ..Default::default()
}).await?;
```

## Schema Validation

Enforce schemas at write time. No invalid events hit the log.

```rust
// Register a schema for an event type
client.register_schema(RegisterSchemaRequest {
    org_id,
    aggregate_type_id,
    event_type: 1,
    schema_format: SchemaFormat::JsonSchema,
    schema: json_schema_bytes,
}).await?;

// Writes with event_type=1 are now validated against this schema
// Invalid payloads are rejected before persistence
```

Supported formats:
- JSON Schema
- Avro
- Protobuf
- MessagePack (with schema)

Schemas are versioned. Event types can bump their major version for breaking changes.

## Architecture

### Thread-Per-Core

One executor per CPU core. Each core owns a subset of aggregates. No locks on the hot path. Similar to ScyllaDB and TigerBeetle.

### Replication

Two nodes: leader and follower. Synchronous replication before client ACK.

No Raft. No Paxos. We use S3 conditional writes for lease-based coordination.

Why? Consensus protocols are operationally complex and mostly overkill for append-only workloads. S3 provides strong read-after-write consistency and compare-and-swap via ETags. That's enough.

If the follower is unreachable, we replicate to S3 instead. No write is acknowledged until it's on two storage systems.

### Failure Modes

We document them. Explicitly.

| Scenario | Behaviour |
|----------|-----------|
| Leader crash | Follower takes over after lease expiry (~15s) |
| Follower crash | Leader replicates to S3, continues accepting writes |
| Network partition | Fencing tokens prevent split-brain data corruption |
| S3 outage | Replication continues to follower, coordination degrades gracefully |

Clock skew must be < 15 seconds. Use NTP.

## Multi-Aggregate Writes

Atomic writes across multiple aggregates with OCC on all of them.

```rust
client.write(WriteRequest {
    writes: hashmap! {
        aggregate_a => WriteData { events: [...], expected_event_batch_index: Some(5) },
        aggregate_b => WriteData { events: [...], expected_event_batch_index: Some(12) },
    },
    ..Default::default()
}).await?;
```

All aggregates must hash to the same shard. If any OCC check fails, the entire write is rejected. No partial writes.

## Watch API

Subscribe to aggregate changes in real-time.

```rust
let mut watch = client.watch(WatchRequest {
    aggregates: Some(hashset![aggregate_key]),
    requested_latency_ms: 10,
}).await?;

while let Some(event) = watch.next().await {
    // event.aggregate_key, event.operation, event.from_batch_index, event.to_batch_index
}
```

Backpressure via bounded channels. If you can't keep up, you get disconnected.

## Listing and Discovery

Filesystem-style navigation for when you need to find things.

```rust
// List all orgs
let orgs = client.list_orgs().await?;

// List aggregate types in an org
let types = client.list_aggregate_types(org_id).await?;

// List aggregates of a type (paginated)
let aggregates = client.list_aggregates(ListAggregatesRequest {
    org_id,
    aggregate_type_id,
    cursor: None,
    limit: 1000,
}).await?;

// Check if an aggregate exists (without loading it)
let exists = client.exists(aggregate_key).await?;
```

## Sharding

Aggregates are assigned to shards by configurable routing:

| Rule | Routes By |
|------|-----------|
| `OrgId` | `org_id % num_shards` |
| `AggregateTypeId` | `aggregate_type_id % num_shards` |
| `AggregateId` | `aggregate_id % num_shards` |

Clients connect to any node. Requests are redirected to the owning shard automatically.

## Multi-Tenancy

Aggregates are namespaced by `org_id`. Isolation is at the aggregate level, not the connection level.

No shared state between orgs. No cross-org queries. Each org's data is completely independent.

## Other Operations

### Retention

```rust
client.trim_start(TrimStartRequest {
    aggregate_key,
    trim_to_event_batch_index: 1000, // Delete batches 0-999
}).await?;
```

### Deletion

```rust
client.delete(DeleteRequest { aggregate_key }).await?;
```

### Metrics

Prometheus endpoint at `/metrics`. Tracks:
- Write/read throughput and latency
- Fsync batch size and amortisation
- Cache hit rates
- Replication lag
- Watcher backpressure

### Backups

- Copy the data directory at any time.
- Spin up a server with the Watch API running
- Or run a follower and snapshot it.

## Client Libraries

| Language | Status |
|----------|--------|
| Rust | Stable |
| Go | Stable |
| Node.js | Stable |
| .NET | Beta |
| Java | Beta |

## When Not to Use Celeriant

- You need ad-hoc queries over aggregate state → use a read database
- You want server-managed consumer groups → use Kafka
- You need transactions across arbitrary keys → use a database
- You're not doing event sourcing → this is the wrong tool

## License

Apache-2.0

## Links

- [Documentation](https://docs.celeriant.io)
- [GitHub Discussions](https://github.com/celeriant/celeriant/discussions)
- [Discord](https://discord.gg/celeriant)