# EventPlaneDB

A distributed, append-only event store with per-aggregate leadership. Built for teams who need a fast, correct WAL substrate for event sourcing without the complexity of traditional distributed databases.

## What It Is

EventPlaneDB is a write-ahead log designed specifically for event sourcing workloads. Think of it as a distributed, redundant WAL with per-aggregate total ordering and optimistic concurrency control.

It is **not** a general-purpose database. It stores opaque byte arrays organised by aggregate. No indexes. No query language. No read-side projections. Just a fast, correct log you can build on top of.

```
org_id / aggregate_type_id / aggregate_id → append-only event stream
```

Each aggregate is an independent, totally-ordered event stream. Different aggregates can have different leaders across the cluster. Writes are acknowledged only after fsync and replication.

## Why It Exists

Event sourcing has a tooling problem.

**PostgreSQL** - Teams reach for it because they need transactions and OCC. But Postgres wasn't built for append-only workloads with millions of small writes. You end up fighting WAL amplification, vacuum pressure, and replication lag.

**EventStoreDB** - The closest competitor. But it conflates write and read concerns, the OSS version is crippled, and it's built on .NET with all the operational baggage that entails. Including projections in the core was a design mistake.

**Kafka** - Great for streaming, wrong abstraction for event sourcing. No per-aggregate ordering guarantees without careful partitioning. No optimistic concurrency control. Consumer groups are opaque broker magic. No fsync by default (dangerous). No filtering by aggregate or event type.

**RDBMS + Event Tables** - Works, but heavy. Chatty commit protocols. Not serverless-friendly. You're paying for B-trees and query planners you don't need.

EventPlaneDB solves the write side of CQRS. Nothing more.

## Architecture

### Thread-Per-Core, Share Nothing

Each server runs one executor per CPU core. Each core owns a subset of aggregates. No locks. No cross-thread coordination for hot-path operations. Similar to ScyllaDB and TigerBeetle's approach.

### Per-Aggregate Leadership

Leadership operates at the aggregate level, not the node level. Different aggregates can have different leaders across the cluster. This distributes load and limits blast radius.

### S3 as Control Plane

We don't implement Raft or Paxos. Instead, we use S3's conditional writes (If-Match/If-None-Match) for lease-based coordination. This sounds crazy until you realise:

1. S3 provides strong read-after-write consistency
2. Conditional writes give you compare-and-swap semantics
3. S3 is more available than your cluster will ever be

Leases are stored in S3. Data replication is synchronous TCP to all followers. If followers are unreachable, we fall back to S3 for durability (degraded mode).

This gives us two-node clusters that actually work, without the odd-number quorum requirement of consensus algorithms.

### Data Path

```
Write Request
    │
    ▼
Lease Check (cached, validated against S3)
    │
    ▼
Local Append + Parallel Replication to Followers
    │
    ▼
All ACK'd? → fsync → ACK to client
    │
    └─ Timeout? → Write to S3 (degraded) → fsync → ACK
```

Clients get explicit control. No broker-managed offsets. No consumer groups. You read from a batch index, you get events from that point forward.

## Performance

Single node, NVMe, 16 cores:

| Mode | Throughput | Notes |
|------|------------|-------|
| Durable (fsync before ACK) | 310,000 writes/sec | Amortized fsync via batching |
| Async (fsync in background) | 2,500,000 writes/sec | For when durability can lag |

Latency (durable mode, p99): < 10ms

For comparison, Kafka on the same hardware with default settings (no fsync): ~40,000 writes/sec.

The throughput difference comes from our batching strategy. NVMe fsync costs ~100μs regardless of batch size. We batch writes across a time window (default 1ms), amortizing the fsync cost across many events.

## Features

### What You Get

- **Per-aggregate total ordering** - Events within an aggregate are strictly ordered
- **Optimistic concurrency control** - Writes can specify expected batch index
- **Client idempotency** - Deduplication via client-assigned event indexes
- **Event type filtering** - Read only events of specific types, bloom filter acceleration
- **Compression** - Zstd, Snappy, Brotli, Gzip per-batch
- **In-memory read cache** - Recent events served from memory
- **Explicit offsets** - You control your read position
- **Replication** - Synchronous to all replicas or S3 fallback
- **Lease-based leadership** - Per-aggregate, automatic failover

### What You Don't Get

- Query language
- Indexes
- Projections / read models
- Consumer groups
- Automatic offset management
- Transactions across aggregates
- Data tiering (yet)
- Hosted service (yet)
- Admin UI (yet)
- Fine-grained permissions (yet)

This is intentional. We're a primitive you build on, not a complete platform.

## API

The protocol is binary over TCP. Request types:

| Operation | Description |
|-----------|-------------|
| `Write` | Append events to an aggregate |
| `Read` | Read events with filters |
| `Exists` | Check if aggregate exists |
| `TrimStart` | Remove old events (retention) |
| `Delete` | Remove entire aggregate |
| `WriteBatches` | Prepend historical batches |

Events are opaque `Vec<u8>` payloads. You handle serialization.

```rust
// Rust client example
let request = Request::Write(WriteRequest {
    org_id: 1,
    aggregate_type_id: 3,
    aggregate_id: 6,
    client_id: 123,
    events: vec![
        EventItem::new(
            0,                              // client_event_index
            0,                              // event_index (server assigns)
            None,                           // event_id
            timestamp_ms,                   // event_timestamp
            1,                              // event_type_major
            0,                              // event_type_minor
            payload.to_vec(),               // event_value
        ),
    ],
    allow_create: true,
    expected_event_batch_index: None,       // OCC: set to enforce ordering
    enforce_client_idempotency: false,
    durable_write_with_delay_us: Some(20),  // fsync before ACK
    compression_type: CompressionType::Zstd { level: 3 },
    ..Default::default()
});

let response = client.send_request(&request).await?;
```

## Client Libraries

| Language | Package |
|----------|---------|
| Rust | `eventplanedb_client` |
| Go | `github.com/eventplanedb/go-client` |
| Java | `io.eventplanedb:client` |
| .NET | `EventPlaneDB.Client` |
| Node.js | `@eventplanedb/client` |
| C++ | `eventplanedb-cpp` |

All clients support automatic leader discovery, connection pooling, and retry with backoff.

## Deployment

### Single Node

```bash
./eventplanedb_server \
    --data-root /var/lib/eventplanedb \
    --listen-address 0.0.0.0:10000
```

### Replicated Cluster

```bash
./eventplanedb_server \
    --data-root /var/lib/eventplanedb \
    --listen-address 0.0.0.0:10000 \
    --replication-enabled \
    --replication-port 10001 \
    --s3-bucket my-eventplanedb-cluster \
    --s3-region ap-southeast-2 \
    --s3-cluster-prefix prod
```

Nodes discover each other via S3 membership. No static configuration required.

### Docker

```bash
docker run -d \
    --name eventplanedb \
    -p 10000:10000 \
    -v eventplanedb-data:/app/data \
    eventplanedb/eventplanedb:latest
```

## Replication Model

- **Synchronous replication** to all followers before ACK
- **S3 degraded mode** when followers unreachable (maintains durability)
- **Lease-based leadership** with 30s default TTL
- **Automatic failover** when leader lease expires (~32s)
- **Per-aggregate fencing** prevents split-brain writes

Two-node clusters are viable. S3 acts as the third vote. When one node is down, the other continues with degraded mode writes to S3.

Detailed design: [replication-design.md](./docs/replication-design.md)

## Use Cases

EventPlaneDB is the write side for:

- **Event-sourced microservices** - Each aggregate type maps to a service
- **Financial systems** - Per-account event logs with strict ordering
- **Audit trails** - Immutable, append-only records
- **Game state** - Per-player or per-match event streams
- **IoT event ingestion** - Per-device event series
- **Collaborative applications** - Per-document operation logs (CRDT-style)

If you need a fast, correct log per "thing" and you'll build the read side yourself, this is for you.

## When Not to Use It

- You need ad-hoc queries over events (use a read database)
- You need cross-aggregate transactions (use a different architecture)
- You want managed consumer groups (use Kafka)
- You need sub-millisecond latency (we fsync)
- You want a complete event sourcing platform (we're just the log)

## Status

**Alpha**. The core is stable and we're running it in production, but the API may change.

Production:
- Single-node operation
- TCP protocol
- All client libraries
- Compression
- Event filtering
- In-memory caching

In Development:
- Replication (S3-coordinated)
- Automatic failover

Roadmap:
- Data tiering to object storage
- Hosted service
- Admin UI
- Fine-grained access control

## Building

Requires Rust 1.91+ and Linux (for io_uring via glommio).

```bash
cargo build --release -p eventplanedb_server
```

## Contributing

Issues and PRs welcome. The codebase prioritises correctness and simplicity over micro-optimisation. If you're proposing a change, include tests.

## License

MIT OR Apache-2.0

---

EventPlaneDB is built by [UtilityDelta](https://utilitydelta.io). We're building the infrastructure layer for event-sourced systems.