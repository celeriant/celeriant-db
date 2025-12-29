# Celeriant

A distributed, append-only event store. Built for teams who need a fast, correct WAL substrate for event sourcing without the complexity of traditional distributed databases or messaging systems.

Celeriant is a write-ahead log designed specifically for event sourcing workloads. Think of it as a distributed, redundant WAL with per-aggregate total ordering and optimistic concurrency control.

It is **not** a general-purpose database. It stores opaque byte arrays organised by aggregate. Minimal indexes. No query language. No read-side projections. Just a fast, correct log you can build on top of. It's the write side of CQRS.

```
org_id / aggregate_type_id / aggregate_id → append-only event stream
```

Each aggregate is an independent, totally-ordered event stream. Writes are acknowledged only after a durable disk write and quorum replication.

## Why It Exists

Event sourcing has a tooling problem.

**PostgreSQL** - Teams reach for it because they need transactions and OCC. But Postgres wasn't built for append-only workloads with millions of small writes. You end up fighting WAL amplification, vacuum pressure, and replication lag. It works, but its heavy. Chatty commit protocols. Not serverless-friendly. You're paying for B-trees and query planners you don't need.

**EventStoreDB** - The closest competitor. But it conflates write and read concerns, it's not really open source, and it's built on .NET with all the operational baggage that entails. Including projections in the core was a design mistake.

**Kafka** - Great for streaming, wrong abstraction for event sourcing. No per-aggregate ordering guarantees without careful partitioning. No optimistic concurrency control. Consumer groups are opaque broker magic. No fsync by default. No filtering by aggregate or event type.

Celeriant solves the write side of CQRS. Nothing more.

## Architecture

### Thread-Per-Core, Share Nothing

Each server runs one executor per CPU core. Each core owns a subset of aggregates. No locks. No cross-thread coordination for hot-path operations. Similar to ScyllaDB and TigerBeetle's approach.

### S3 as Control Plane

We don't implement Raft or Paxos. Instead, we use S3's conditional writes (If-Match/If-None-Match) for lease-based coordination.

1. S3 provides strong read-after-write consistency
2. Conditional writes give you compare-and-swap semantics
3. Leader doesn't renew lease with S3 constantly, only in leader transition. If S3 goes down, your cluster stays up.
4. The cluster only runs with two nodes, a leader and a follower. If the follower goes down, data is replicated to S3 before ack to client. If the leader goes down, the follower becomes leader and the cluster continues to operate.

Leases are stored in S3. This design requires syncronised clocks but simplifies everything else. 

### Data Path

```
Client Write Request
    │
    ▼
Lease Check (cached, validated against S3)
    │
    └─ Not leader? Tell client who is leader.
    │
    ▼
Local Append + amortised fsync
    │
    ▼
Concurrent Replication to Follower
    │
    └─ Timeout? Replicate to S3 instead
    │
    ▼
Client ACK
```

Clients get explicit control. No broker-managed offsets. No consumer groups. You read from a batch index, you get events from that point forward. Server side filtering allows clients to get the event types they are interested in.

## Performance

Single node, NVMe, 16 cores:

| Mode | Throughput | Notes |
|------|------------|-------|
| Durable (fsync before ACK) | 370,000 writes/sec | Amortized fsync via batching for strong guarantees |
| Async (fsync in background) | 700,000 writes/sec | For when durability can lag by 100-200ms |

Latency (durable mode, p99): < 10ms

Kafka on the same hardware with default settings (no fsync): ~40,000 writes/sec.

## Features

### What You Get

- **Per-aggregate total ordering** - Events within an aggregate are strictly ordered, no gaps
- **Optimistic concurrency control** - Writes can specify expected batch index
- **Dynamic Consistency Boundaries** - Conditionally, atomically write events to multiple aggregates
- **Client idempotency** - Deduplication via client-assigned event indexes
- **Event type filtering** - Read only events of specific types, bloom filter acceleration
- **Schema Validation** - Enforce schemas at the event type level per aggregate type
- **Compression** - Zstd, Snappy, Brotli, Gzip per-batch
- **In-memory read cache** - Recent events served from memory
- **Explicit offsets** - You control your read position for each aggregate
- **Distributed, Redundant Replication** - Synchronous replication to follower node
- **Watch API** - Get notified immediately when other clients perform operations on aggregates

### What You Don't Get

- Query language
- Indexes
- Projections / read models
- Consumer groups
- Automatic offset management

It's not a state machine, a message streaming platform or a queue.

## API

The protocol is binary over TCP. Request types:

| Operation | Description |
|-----------|-------------|
| `Write` | Append events to an aggregate |
| `Read` | Read events with filters |
| `Exists` | Check if aggregate exists |
| `TrimStart` | Remove old events (retention) |
| `Delete` | Remove the entire aggregate |
| `Watch` | Get notified immediately with new events for an aggregate |

Events are opaque byte array payloads. You handle serialization.

Clients support automatic leader discovery and timeouts.

## Deployment

This server is LINUX ONLY due to io_uring use. It can run inside mac/windows using docker/wsl however.

### Single Node

```bash
./celeriant --data-root /var/lib/celeriant
```

By default Celeriant will utilise all CPU cores on your server.

### Testing Replicated Clusters

Make sure your AWS CLI is setup and you've got your creds in `~/.aws/credentials` and region in `~/.aws/config`.

```bash
chmod +x s3_server.sh
./s3_server.sh test-bucket test-folder --listen-address 0.0.0.0:9001 --data-root data1
./s3_server.sh test-bucket test-folder --listen-address 0.0.0.0:9002 --data-root data2
```

Nodes discover each other via S3 membership. No static configuration required.

### Docker

```bash
docker run -d \
    --name celeriant \
    -p 10000:10000 \
    -v celeriant-data:/app/data \
    celeriant/celeriant-db:latest
```

## Use Cases

Celeriant is the write side for:

- **Event-sourced microservices** - Aggregates shared between services, adding events requires current aggregate state as invariant
- **Financial systems** - Per-account event logs with strict ordering
- **Audit trails** - Immutable, append-only records
- **Game state** - Low latency, high throughput per-player or per-match event streams
- **Offline first and collaborative apps** - Avoid CRDTs and store events instead with local projections
- **IoT event ingestion** - Per-device event series with no complex indexing or bucketing

If you need a fast, correct log per "thing" and you'll build the read side yourself, this is for you.

## When Not to Use It

- You need ad-hoc queries over aggregate state (use a read database)
- You want server-managed consumer groups for message fan-out (use Kafka)

## Status

**Alpha**. The core is stable and we're running it in production, but the API may change.

### Roadmap
- Data tiering - reduce costs by offloading data to object storage / glacier
- Hosted services - managed Celeriant as a service
- Admin UI / control plane
- Fine-grained permissions - OAuth2, etc.

## Building

Requires Rust 1.91+ and Linux (for io_uring via glommio).

```bash
cargo build --release -p celeriant_server
```

## Contributing

Discussions and PRs welcome. The codebase prioritises correctness and simplicity. If you're proposing a change, include tests and updated benchmarks.

- Questions or bugs? [Open a GitHub Discussion](https://github.com/celeriant/celeriant-server/discussions)
- Commercial inquiries? [LinkedIn](https://www.linkedin.com/in/tyson-brown-208b88b6)

## License

Apache-2.0

## Readme TODOs

- IoT, Running on low power devices, green, cheap database attributes
- Notes on infinite cardinality design ethos
- Guiding devs on what to do for read side? eg. DuckDB, etc.
- Notes on AI usage in celeriant, best practices & techniques
- Embedding instead of using as server
- Client use + serverless friendly notes
- Notes on head-of-line blocking mitigation