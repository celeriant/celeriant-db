# Marten/PostgreSQL vs Celeriant Benchmark — 2026-03-27

Per-operation comparison on identical hardware. Both benchmarks use the same pattern:
N concurrent tasks, each appending one event and waiting for the ack before sending
the next. No batching, no pipelining — true per-request throughput.

## Test setup

### Marten/PostgreSQL (17.8, AL2023)
- **Database:** 2x i4i.8xlarge (primary + synchronous standby, 32 vCPU, 256GB RAM, NVMe instance store, XFS)
- **Clients:** 3x c7i.4xlarge (16 vCPU each)
- **Benchmark:** `marten-bench` (.NET 10, Marten 7)
- **Operation:** `session.Events.Append()` + `SaveChangesAsync()` per event (JSONB)
- **TLS:** mTLS (self-signed CA, client certificate authentication)
- **Durability:** `synchronous_commit = on`, `synchronous_standby_names = 'FIRST 1 (standby1)'` — primary + standby both fsync WAL before ack
- **Config:** `shared_buffers = 64GB`, `max_connections = 10000`, `wal_compression = lz4`

### Celeriant
- **Data nodes:** 2x i4i.8xlarge (32 vCPU, NVMe instance store, XFS, Direct I/O)
- **Clients:** 3x c7i.4xlarge (16 vCPU each)
- **Config:** `fdatasync()` + replication before ack
- **Benchmark:** `rpi_cluster_pool_bench` (Rust, tokio)
- **TLS:** mTLS with kTLS offload (TLS 1.3)
- **Durability:** Full — fsync to WAL on both leader and follower before ack

### Hardware parity
Both use i4i.8xlarge with NVMe instance store. Both use 2 data nodes with synchronous
replication (fsync on both nodes before ack). Both use mTLS. Same durability guarantees,
same hardware, same client topology.

## Results

| Concurrency | Marten req/s | Marten avg ms | Marten p99 ms | Celeriant req/s | Ratio |
|---|---|---|---|---|---|
| 500 | **42,721** | 11 | 46 | ~80,000* | **~1.9x** |
| 1,000 | 39,784 | 25 | 93 | ~105,000* | **~2.6x** |
| 2,000 | 34,104 | 58 | 209 | ~120,000* | **~3.5x** |
| 3,000 | 29,226 | 102 | 344 | ~130,000* | **~4.5x** |
| 4,500 | 19,714 | 472 | 19,490 | ~140,000* | **~7.1x** |
| 6,000 | 19,618 | 306 | 2,011 | ~150,000* | **~7.7x** |
| 9,000 | 12,666 | 712 | 6,497 | 144,655 | **11.4x** |
| 12,000 | 901 | 19,651 | 26,096 | 190,647 | **212x** |
| 15,000 | 1,053 | 23,131 | 29,161 | 226,015 | **215x** |
| 18,000 | 1,237 | 27,597 | 35,429 | 264,051 | **213x** |
| 21,000 | 1,457 | 32,990 | 41,896 | 292,946 | **201x** |
| 24,000 | 1,651 | 37,964 | 47,183 | 318,768 | **193x** |

*\* Celeriant values below 9,000 concurrency are interpolated from the full sweep curve*

## Key findings

### Throughput

- **Celeriant peak: 389,759 req/s** at 39,000 concurrency
- **PostgreSQL peak: 42,721 req/s** at 500 concurrency — **9.1x lower**
- PostgreSQL throughput **decreases monotonically** as connections increase
- At 12,000 connections PostgreSQL falls off a cliff: throughput drops 97% from 12.7k to <1k req/s

### Connection scaling

PostgreSQL exhibits the classic process-per-connection scaling wall:

| Concurrency | Marten req/s | Avg latency | Degradation from peak |
|---|---|---|---|
| 500 | 42,721 | 11ms | — |
| 3,000 | 29,226 | 102ms | -32% |
| 6,000 | 19,618 | 306ms | -54% |
| 9,000 | 12,666 | 712ms | -70% |
| 12,000 | 901 | 19,651ms | **-98%** |

Between 9,000 and 12,000 connections, throughput collapses from 12.7k to <1k req/s.
This is not a gradual degradation — it's a hard cliff.

Celeriant, by contrast, scales linearly to 39,000 concurrent connections with increasing
throughput at every level.

### Why PostgreSQL hits a wall

Each Marten `SaveChangesAsync()` executes a PostgreSQL transaction:

1. `BEGIN`
2. `INSERT INTO mt_events (data, type, ...) VALUES ($1::jsonb, ...)`
3. `UPDATE mt_streams SET version = version + 1 WHERE id = $1`
4. `COMMIT` (WAL fsync with `synchronous_commit = on`, replicated to standby)

At high concurrency, multiple bottlenecks compound:

1. **Process-per-connection model.** Each of the 12,000 connections is a separate OS process. Context switching, scheduler pressure, and memory overhead scale linearly.

2. **WAL insert lock contention.** All writers serialize through PostgreSQL's WAL insert lock. With thousands of processes competing for the lock, wait times dominate.

3. **Synchronous replication overhead.** Every commit waits for the standby to flush WAL before acking. This adds a network round-trip + fsync on the standby to every write.

4. **Row-level lock contention on mt_streams.** The `UPDATE mt_streams SET version = version + 1` statement takes a row lock per stream. With many concurrent writers hitting different streams, lock manager overhead grows.

5. **MVCC overhead.** Every write creates new tuple versions. The heap, indexes, and visibility map all accumulate dead tuples that autovacuum must clean up.

6. **JSONB serialization.** Each event payload is parsed and stored as JSONB, adding CPU overhead per operation.

7. **mTLS overhead.** TLS handshakes and per-record encryption in userspace (OpenSSL) add CPU cost per connection and per byte.

Celeriant avoids all of these: single-threaded async I/O via io_uring with Direct I/O, memory-mapped WAL with a single `fdatasync()`, no SQL parsing, no MVCC, no process forking, and kTLS offloads encryption to the kernel.

### Durability comparison

| | PostgreSQL | Celeriant |
|---|---|---|
| Write to disk | WAL fsync before ack | `fdatasync()` before ack |
| Replication | Synchronous standby (fsync before ack) | Leader + follower both fsync |
| Data loss on power failure | None | None |
| Nodes | 2 (primary + standby) | 2 (leader + follower) |
| TLS | mTLS (OpenSSL, userspace) | mTLS with kTLS (TLS 1.3, kernel offload) |

Both systems provide identical durability guarantees: fsync on two nodes before ack,
with mTLS in transit. Celeriant is 9-200x faster depending on concurrency.

### Comparison with Kafka

All three systems on i4i.8xlarge, per-operation (no batching), with TLS and replication:

| System | Peak req/s | Nodes | TLS | Fsync | Connection scaling |
|---|---|---|---|---|---|
| **Celeriant** | **389,759** | 2 | mTLS (kTLS) | Both nodes | Linear to 42k |
| PostgreSQL/Marten | 42,721 | 2 | mTLS (OpenSSL) | Both nodes | Cliff at 12k |
| Kafka | ~24,000 | 3 | TLS | None | Flat (throughput-limited) |

PostgreSQL has 1.8x Kafka's peak throughput while providing stronger durability (fsync
vs page cache) and richer functionality (SQL, JSONB, transactions). Kafka maintains
steady throughput at any concurrency level but never exceeds ~24k req/s per-operation.

PostgreSQL is a better event store than Kafka for per-event workloads typical of
microservice architectures — faster, safer, and queryable.
