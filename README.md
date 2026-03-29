# Celeriant

A fast, distributed, append-only write-ahead log built specifically for event sourcing. It is dedicated to the write side of [CQRS](https://www.martinfowler.com/bliki/CQRS.html).

Not a relational database. Not a message broker. Just the write side of event sourcing, done properly.

## Why Celeriant

PostgreSQL gives you correctness but not throughput. Kafka gives you throughput but not correctness. Celeriant gives you both.

**PostgreSQL** - Transactions and ACID are great, but append-only, high-cardinality, small-write workloads are its worst case. You will spend more time tuning vacuum and fighting WAL amplification than building your product. Real-world event stores hit 50k writes/sec on PostgreSQL before it buckles.

**Kafka** - Scales to millions of writes/sec, but it has no conditional writes and no per-aggregate ordering guarantee. Before a service can write events it needs to validate state, and that state may have changed between the read and the write. Kafka cannot help with that. There is also no way to filter events per-aggregate, every consumer gets the whole topic firehose. Also Kafka only works because of producer batching, single event writes max out at 25k events/sec.

**Celeriant** - Per-aggregate ordering, optimistic concurrency control, exactly-once writes, and durable cluster replication. Scales to millions of aggregates with bounded memory regardless of cardinality.

## What Celeriant Is

A distributed, append-only log organised by aggregate:

```
org_id / aggregate_type_id / aggregate_id → ordered event stream
```

Each aggregate is an independent, totally-ordered stream. Writes are acknowledged after durable disk write and quorum replication.

**You get:**
- Per-aggregate total ordering (no gaps, no reordering)
- Optimistic concurrency control (expected batch index)
- Dynamic consistency boundaries - conditionally, atomically write events to multiple aggregates
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
- SQL Queries
- Tables with arbitary transaction semantics
- key-value storage
- Consumer groups or automatic offset management

It is not a state machine, a message streaming platform, or a queue. You have to build your read side yourself.

## 'Hello World' write benchmark on AWS

Per-operation throughput. No batching, no pipelining. Each write appends a single event,
waits for the durable ack, then sends the next. This is the pattern real microservices use.

- 2x i4i data nodes (NVMe, XFS, Direct I/O via io_uring)
- 3-4x c7i.4xlarge client nodes (16 vCPU each)
- mTLS with kTLS offload (TLS 1.3) on all connections
- Every write is `fdatasync()`'d to disk on both nodes before ack
- AWS ap-southeast-2, single AZ

| System | Peak req/s | P99 at peak | Nodes | TLS | Fsync | OCC |
|---|---|---|---|---|---|---|
| **Celeriant (64c)** | **535,292** | **210ms** | 2 | mTLS (kTLS) | Both nodes | Yes |
| **Celeriant (32c)** | **389,759** | **217ms** | 2 | mTLS (kTLS) | Both nodes | Yes |
| PostgreSQL/Marten | 42,721 | 46ms | 2 | mTLS (OpenSSL) | Both nodes | Yes |
| Kafka | ~24,000 | ~1,342ms | 3 | TLS | None | No |

Celeriant P99 stays under 110ms up to 24,000 concurrent connections on both configurations.
PostgreSQL delivers excellent latency at low concurrency but collapses at 12,000 connections
(throughput drops 98%). Kafka plateaus at ~24k req/s regardless of concurrency,
without fsync or per-aggregate ordering.

Full results: [ec2-benchmark](docs/benchmark-results/ec2-benchmark.md),
[marten-benchmark](docs/benchmark-results/marten-benchmark.md),
[kafka-benchmark](docs/benchmark-results/kafka-benchmark.md).

## Architecture

### Thread-Per-Core

One glommio executor per CPU core. Each core owns a subset of aggregates. No locks on the hot path. Inspired by [ScyllaDB](https://www.scylladb.com/) and [TigerBeetle](https://tigerbeetle.com/).

### Durability Guarantees

Celeriant uses **Direct I/O**, bypassing the Linux kernel page cache. Buffered I/O through the page cache is vulnerable to [silent data loss on fsync failure](https://lwn.net/Articles/752063/). Direct I/O ensures fsync failures surface immediately.

Events are batched and written durably to disk on the leader. The leader replicates to the follower, which also does a durable write. The leader ACKs to clients only after both writes confirm. Fsync and replication are amortised across concurrent writers to avoid paying the full fsync cost per write.

### Replication and Cluster Coordination

Two nodes: leader and follower. No Raft. No Paxos.

Leader election and coordination use [S3 conditional writes](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/). A single CAS-protected S3 object grants leader exclusivity. This is operationally simpler than consensus protocols and sufficient for append-only workloads.

If the follower is unreachable, the leader replicates to S3 instead. No write is acknowledged until it is on two storage systems.

### Memory and Indexing Design

Celeriant is designed for very high aggregate cardinality without memory growth proportional to aggregate count. One aggregate or ten million - the same memory footprint. You can model one stream per user, per device, per order, per game match, without worrying about whether your infrastructure will fall over.

## Quick Start

### 1. Start the server

**Linux (bare metal)** — Celeriant runs natively on any Linux system with io_uring support (kernel 5.11+):

```bash
cargo build --release -p celeriant
./target/release/celeriant --standalone --data-root /var/lib/celeriant --num-shards 1
```

**macOS / Windows** — Use Docker (the container provides the Linux kernel):

```bash
docker build -t celeriant:local .
docker run -d --name celeriant \
  --security-opt seccomp=unconfined \
  -p 10000:10000 \
  celeriant:local \
  --standalone --data-root /var/lib/celeriant --num-shards 1
```

`--standalone` runs a single node with no S3 or replication. For a full two-node cluster with Grafana, Prometheus, and MinIO, see [deploy/local-cluster](deploy/local-cluster/docker-compose.yml).

### 2. Verify with the CLI

The CLI and TUI run natively on all platforms:

```bash
cargo build --release -p celeriant_cli

# Write an event
./target/release/celeriant_cli write --org 1 --type 1 --id 1 \
    --client-id 1 --event-type 1 \
    --data '{"order_id": 42, "amount": 99.95}' --allow-create

# Read it back
./target/release/celeriant_cli read --org 1 --type 1 --id 1 --from 1

# Interactive TUI
./target/release/celeriant_cli
```

See [celeriant_cli](celeriant_cli/README.md) for the full CLI and TUI reference.

### 3. Add the Rust client

```bash
cargo add celeriant_client_tokio
```

```rust
use celeriant_client_tokio::{CeleriantClient, json_event, from_json};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;
use celeriant_wal::AggregateKey;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct OrderPlaced { order_id: u64, amount: f64 }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CeleriantClient::connect("localhost:10000").await?;

    let key = AggregateKey::new(1, 1, 1001);

    // Write
    let events = vec![json_event(1, &OrderPlaced { order_id: 42, amount: 99.95 })?];
    client.write_events(key.clone(), events).await?;

    // Read
    let response = client.read(ReadRequest {
        correlation_id: None,
        aggregate_key: key,
        filters: ReadFilters::new(1),
    }).await?;

    let order: OrderPlaced = from_json(&response.event_batches[0].events[0])?;
    println!("order_id={}, amount={}", order.order_id, order.amount);
    Ok(())
}
```

For connection pooling, leader failover, TLS, and production patterns, see the [client guide](docs/guide.md).

For multi-aggregate writes, watch API, and more, see the [client guide](docs/guide.md).

## Sharding

Aggregates are assigned to shards by configurable routing:

| Rule | Routes By |
|------|-----------|
| `OrgId` | `org_id % num_shards` |
| `AggregateTypeId` | `aggregate_type_id % num_shards` |
| `AggregateId` | `aggregate_id % num_shards` |

Clients connect to any shard. Requests are redirected to the owning shard automatically.

## When Not to Use Celeriant

- You need ad-hoc queries over aggregate state - use a read database
- You need transactions across arbitrary keys - use a relational database
- You want server-managed consumer groups - use Kafka
- You need to pipe large amounts of messages between unrelated systems - use Kafka


## License

Apache-2.0
