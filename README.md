<!-- BEGIN PROVENANCE PREAMBLE (managed by consolidation/finalise.sh, do not hand-edit) -->
> **This repository carries 580 commits going back to 2024-05-27, across seven predecessor
> repositories joined at their tips.** Seven root commits are GPG-signed by GitHub itself, which
> fixes each repository's creation date independently of anything I control. Import GitHub's key and
> check them:
>
> ```sh
> curl -sL https://github.com/web-flow.gpg | gpg --import
> git log --format='%G? %h %ad %s' --date=short --all | grep -E '^[EGU]'
> ```
>
> Browsing older history needs `git log --full-history -- <path>`, and `--first-parent` will only
> show you one lineage. See [PROVENANCE.md](PROVENANCE.md) for why, what was removed before
> publication, and where AI tooling was used.
<!-- END PROVENANCE PREAMBLE -->

# Celeriant - An experimental event sourcing database

A fast, distributed, append-only write-ahead log built specifically for event sourcing. 

This is a serious, but somewhat still experimental project and is not vibe-coded. See [PROVENANCE.md](PROVENANCE.md). This readme is written by me.

My main interests trace back to:

- at work I see clients reach to Kafka or PostgreSQL to implement event sourcing and/or distribution across microservices. Always ends badly.
- I have an interest in offline-first apps and event sourcing is a viable alternative to CRDTs (Conflict-free Replicated Data Types)

The goal is to build a fundamental, open source substrate that is fast, safe, correct and can linearlise events across 1 or many aggregates via OCC.

**fast** - up to 500k writes/sec. stable at 325k with p99 201ms. Redis speed recent write cache for catchups.

**safe** - write to disk before ack. No 'web scale' in-mem buffer bullshit. Doesn't blow up when you get to 100 million aggregates.

**correct** - schema validation, exactly once events, optimistic concurrency control. All server-side.

**linearlisable** - per shard total ordering, dynamic consistency boundaries, no out of order or sequence gaps.

This is built by a team of 1 over ~3 years. Future self: never build a database from scratch.

Unlike most new-ish databases, everything is written from scrach. It doesn't sit on other embedded database. It's not a Kafka fork. Main dependencies are glommio (thread-per-core + io_uring + direct I/O) and bincode (serialisation).

## Celeriant vs xxx

**PostgreSQL** - performance pretty meh and can hit a wall. Use of sequences for event tables has gotchas for read catchup as sequence allocation is before commit, read catchups can miss events. Storing events in JSONB columns sucks too. `LISTEN/NOTIFY` can kill your server.

**Kafka** - Not designed around aggregates. So you get a firehose of events and need DLQs. DLQ's then break aggregate causality if you are not careful. Usual pattern is outbox table + debezium -> kafka, but what happens is services have subtle shared state that creeps in over time; so services can raise events on the same aggregate in parallel, based on an outdated read model. No optimistic concurrency control means your aggregate invariants can be violated, and you only see it in prod when the system gets busy. This has been an [open issue since 2015](https://issues.apache.org/jira/browse/KAFKA-2260). No event schema validation server-side unless you go enterprise. Expensive.

**KurrentDB** - Single WAL, global ordering but tops out at 15k writes/sec. Postgres does 50k. Celeriant can hit 500k. No server side schema enforcement. Not open source. Memory scales with stream cardinality, expect it to die if you keep adding new aggregates. Built on .Net, GC pauses can trigger false elections.

## Should I use this

Yes I know you are thinking that. Depends. Would a high performance event sourcing architecture give whatever you are building a competitive advantage? Is your boss ok with you risking it on an un-proven database, built by 1 guy out in Australia that nobody else uses?

Do you need OCC+DCB over your aggregates? Are you considering event sourcing patterns over outbox+CDC+message bus?

I'm keen to chat with you if you want to try it. I'll even help you build it. Reach out to me on [LinkedIn](https://www.linkedin.com/in/tyson-brown-208b88b6/) or email me tyson@celeriant.io. 

## Getting into the details

Celeriant is an append-only log organised by aggregate:

```
org_id / aggregate_type_id / aggregate_id
```

**You get:**

- Per-aggregate total ordering (no gaps, no reordering)
- Optimistic concurrency control (expected version)
- Dynamic consistency boundaries - conditionally, atomically write events to multiple aggregates
- Exactly-once writes (client idempotency, duplicate writes rejected)
- Infinite cardinality (millions of aggregates, no tuning, bounded memory)
- Explicit offsets (you control your read position)
- Event type filtering
- Watch API (real-time change notifications)
- Compression (zstd)
- In-memory read cache (recent events served from memory)
- Per-event encryption support (for e2e encryption and crypto-shredding)
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
| PostgreSQL/Marten   | 42,721      | 46ms        | 2     | mTLS (OpenSSL) | Both nodes | Yes |
| Kafka               | ~24,000     | ~1,342ms    | 3     | TLS            | None       | No  |

Celeriant P99 stays under 110ms up to 24,000 concurrent connections on both configurations.
PostgreSQL delivers excellent latency at low concurrency but collapses at 12,000 connections
(throughput drops 98%). Kafka plateaus at ~24k req/s regardless of concurrency,
without fsync or per-aggregate ordering.

Full results: [ec2-benchmark](docs/benchmark-results/ec2-benchmark.md),
[marten-benchmark](docs/benchmark-results/marten-benchmark.md),
[kafka-benchmark](docs/benchmark-results/kafka-benchmark.md).

## Technical Architecture

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
use celeriant_client_tokio::{CeleriantPool, PoolOptions, from_json, json_event};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;
use celeriant_wal::aggregate_key::AggregateKey;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct OrderPlaced { order_id: u64, amount_cents: u64 }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = CeleriantPool::new(PoolOptions::new("localhost:10000"));

    let key = AggregateKey::new(1, 1, 1001);

    // Write
    let order_event = OrderPlaced { order_id: 42, amount_cents: 9995 };
    let events = vec![json_event(1, &order_event)?];
    pool.write_events(key.clone(), events).await?;

    // Read
    let response = pool.read(ReadRequest {
        correlation_id: None,
        aggregate_key: key,
        filters: ReadFilters::new(1),
    }).await?;

    let order: OrderPlaced = from_json(&response.event_batches[0].events[0])?;
    println!("order_id={}, amount_cents={}", order.order_id, order.amount_cents);
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

The modulo is on the low bits of the routing field, so pick IDs with uniform low bits or you'll hot-shard. See [Choosing IDs so shards stay balanced](docs/guide.md#choosing-ids-so-shards-stay-balanced).

You can't do DCB if aggregates in the commit group cross shard boundaries. Make sure you co-locate.

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
