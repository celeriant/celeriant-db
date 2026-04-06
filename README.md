# Celeriant

A fast, distributed, append-only write-ahead log built specifically for event sourcing. It is dedicated to the write side of [CQRS](https://www.martinfowler.com/bliki/CQRS.html).

Not a relational database. Not a message broker. Just the write side of event sourcing, done properly.

## Release Status

Celeriant is new. Really new. It's ready to experiment with, in non-production workloads. A lot might change in the next few months, and we won't be supporting backwards compatibility until 1.0. Use at your own risk.

## Why Celeriant

PostgreSQL gives you correctness but not throughput. Kafka gives you throughput but not correctness. Celeriant gives you both.

**PostgreSQL** - It does all the things. You can design a single table that acts like an event store, add indexes, do conditional writes. PostgreSQL dies at scale. Every event append generates [8-10x the logical data size](docs/postgresql-event-sourcing-structural-mismatch.md) in actual I/O: heap writes, index updates, WAL, full page images, autovacuum. That continuous write pressure means vacuum can never catch up, and at 10,000 events/sec you hit [transaction ID wraparound](docs/postgresql-event-sourcing-structural-mismatch.md) every few days. Mailchimp lost 40 hours to it. The feature you need most for projections, `LISTEN/NOTIFY`, acquires a [global exclusive lock on the entire database](docs/postgresql-event-sourcing-structural-mismatch.md). Recall.ai had three outages in four days before ripping it out entirely. On Aurora I/O-Optimized the per-I/O charges go away, but storage never shrinks, MVCC dead tuple bloat compounds, and there's no lifecycle tiering. A modest 30k writes/sec workload costs [$16,000/month in month 1 and $205,000/month by month 12](docs/benchmark-summary.md).

**Kafka** - Claims millions of writes/sec, but that number counts records inside batches. Per-operation throughput, one write, wait for the ack, maxes out at ~24k req/s on our hardware ([benchmark](docs/benchmark-results/kafka-benchmark.md)). No conditional writes, no per-aggregate ordering. Two services read the same aggregate, both write version 6, both succeed. Silent data corruption. This has been an [open issue since 2015](https://issues.apache.org/jira/browse/KAFKA-2260). There is no way to read events for a single aggregate either. You get the whole partition firehose and filter client-side. One slow event blocks every aggregate in that partition (head-of-line blocking). The standard fix is a Dead Letter Queue, but skipping an event breaks the causal chain. [Axon's docs](https://docs.axoniq.io/axon-framework-reference/4.11/events/event-processors/dead-letter-queue/) acknowledge you have to halt the entire aggregate stream, which defeats the purpose. Re-partitioning breaks ordering permanently, so you can't scale after the fact. Teams end up with a lot of code to handle the sharp edges.

**Celeriant** - Per-aggregate ordering, optimistic concurrency control, exactly-once writes, and durable cluster replication. Scales to millions of aggregates with bounded memory regardless of cardinality. Cheap to run, predictable costs, no sharp edges or gotchas as you scale. A single Celeriant cluster writes up to 500k events/sec.

## What Celeriant Is

A distributed, append-only log organised by aggregate:

```
org_id / aggregate_type_id / aggregate_id -> ordered event stream
```

Each aggregate is an independent, totally-ordered stream. Writes are acknowledged after durable disk write and quorum replication.

**You get:**

- Per-aggregate total ordering (no gaps, no reordering)
- Optimistic concurrency control (expected batch index)
- Dynamic consistency boundaries - conditionally, atomically write events to multiple aggregates
- Exactly-once writes (client idempotency, duplicate writes rejected)
- Infinite cardinality (millions of aggregates, no tuning, bounded memory)
- Explicit offsets (you control your read position)
- Event type filtering
- Watch API (real-time change notifications)
- Compression (zstd, snappy, brotli, gzip)
- In-memory read cache (recent events served from memory)
- Per-event encryption support (AES-GCM, stored opaquely by server)
- Blake3 hash chain linking every event to its predecessor (tamper-evident audit log)

**You don't get:**

- SQL Queries
- Tables with arbitary transaction semantics
- Key-value storage
- Consumer groups or automatic offset management

It is not a state machine, a message streaming platform, or a queue. You have to build your read side yourself.

## 'Hello World' write benchmark on AWS

Per-operation throughput. No batching, no pipelining. Each write appends a single event,
waits for the durable ack, then sends the next. This is the pattern real microservices use.

- Two i4i data nodes (NVMe, XFS, Direct I/O via io_uring)
- Three c7i.4xlarge client nodes (16 vCPU each)
- mTLS for both client and cluster network traffic
- Every write is `fdatasync()`'d to disk on both nodes before ack
- AWS ap-southeast-2, single AZ

| System              | Peak req/s  | P99 at peak | Nodes | TLS            | Fsync      | OCC |
| ------------------- | ----------- | ----------- | ----- | -------------- | ---------- | --- |
| **Celeriant (64c)** | **535,292** | **210ms**   | 2     | mTLS (kTLS)    | Both nodes | Yes |
| **Celeriant (32c)** | **389,759** | **217ms**   | 2     | mTLS (kTLS)    | Both nodes | Yes |
| PostgreSQL/Marten   | 42,721      | 46ms        | 2     | mTLS (OpenSSL) | Both nodes | Yes |
| Kafka               | ~24,000     | ~1,342ms    | 3     | TLS            | None       | No  |

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

Celeriant is designed for very high aggregate cardinality without memory growth proportional to aggregate count. It does this with LRU bounded caches and bloom filters. One aggregate or ten million - the same memory footprint. You can model one stream per user, per device, per order, per game match, without worrying about whether your infrastructure will fall over.

## Quick Start

### 1. Start the server

**Linux (bare metal)** Celeriant runs natively on any Linux system with io_uring support (kernel 5.11+):

```bash
cargo build --release -p celeriant
./target/release/celeriant --standalone --data-root /var/lib/celeriant --num-shards 1
```

**macOS / Windows** Use Docker (the container provides the Linux kernel):

```bash
docker build -t celeriant:local .
docker run -d --name celeriant \
  --security-opt seccomp=unconfined \
  -p 10000:10000 \
  celeriant:local \
  --standalone --data-root /var/lib/celeriant --num-shards 1
```

`--standalone` runs a single node with no S3 or replication. For a full localhost two-node cluster with Grafana, Prometheus, and MinIO, see [deploy/local-cluster](deploy/local-cluster/docker-compose.yml).

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

For connection pooling, leader failover, TLS, multi-aggregate writes, watch API and production patterns, see the [client guide](docs/guide.md).

**.NET** A .NET client is available at [celeriant-dotnet-client](https://github.com/celeriant/celeriant-dotnet-client).

### Next steps

- **[Client Guide](docs/guide.md)** connections, pooling, TLS, OCC, multi-aggregate writes, watch API, and production patterns
- **[Demo App](celeriant_demo/README.md)** browser-based banking demo showing basic read/write patterns, OCC conflicts, and live watch via SSE
- **[Reference API](celeriant_reference/README.md)** production-grade example with Postgres read projections, exactly-once writes, OCC retry loops, and HTTP idempotency

## Sharding

Aggregates are assigned to shards by configurable routing:

| Rule              | Routes By                        |
| ----------------- | -------------------------------- |
| `OrgId`           | `org_id % num_shards`            |
| `AggregateTypeId` | `aggregate_type_id % num_shards` |
| `AggregateId`     | `aggregate_id % num_shards`      |

Clients connect to any shard. Requests are redirected to the owning shard automatically.

## When Not to Use Celeriant

- You need ad-hoc queries over aggregate state - use an OLAP database
- You have IoT data and need live data analysis/dashboards - use a time-series database
- You need transactions across arbitrary keys - use an OLTP relational database
- Your data patterns are state-first and map to a single primary key - use a key-value database
- You want server-managed consumer groups - use Kafka
- You need to pipe large amounts of messages between unrelated systems - use Kafka

## Celeriant Author

Celeriant is built by [Tyson Brown](https://www.linkedin.com/in/tyson-brown-208b88b6/). 20yrs XP in enterprise, high performance systems. Based in Australia.

LLM's don't perform well when working on complex, distributed systems with complex invariants that have a time dimension. Celeriant is overwhelmingly hand-crafted, it's not written via autonomous agentic systems.

LLMs to prototype; automate the boilerplate; critical analysis and retrospective.

Humans still write the critical components by hand; Always read the code; Unit and integration tests are mandatory; Humans always in the loop.

## License

Apache-2.0
