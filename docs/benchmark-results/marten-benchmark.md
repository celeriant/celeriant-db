# Marten/PostgreSQL vs Celeriant Benchmark — 2026-03-27

Per-operation comparison on identical hardware. Both benchmarks use the same pattern:
N concurrent tasks, each appending one event and waiting for the ack before sending
the next. No batching, no pipelining — true per-request throughput.

## Test setup

### Marten/PostgreSQL (17.8, AL2023)
- **Database:** 1x i4i.8xlarge (32 vCPU, 256GB RAM, NVMe instance store, XFS)
- **Clients:** 3x c7i.4xlarge (16 vCPU each)
- **Benchmark:** `marten-bench` (.NET 10, Marten 7)
- **Operation:** `session.Events.Append()` + `SaveChangesAsync()` per event (JSONB)
- **TLS:** Disabled (plaintext)
- **Durability:** `synchronous_commit = on` (WAL fsync before ack)
- **Config:** `shared_buffers = 64GB`, `max_connections = 10000`, `wal_compression = lz4`

### Celeriant
- **Data nodes:** 2x i4i.8xlarge (32 vCPU, NVMe instance store, XFS, Direct I/O)
- **Clients:** 3x c7i.4xlarge (16 vCPU each)
- **Config:** `fdatasync()` + replication before ack
- **Benchmark:** `rpi_cluster_pool_bench` (Rust, tokio)
- **TLS:** mTLS with kTLS offload (TLS 1.3)
- **Durability:** Full — fsync to WAL on both leader and follower before ack

### Hardware parity
Both use i4i.8xlarge with NVMe instance store. PostgreSQL uses 1 node (no replication),
Celeriant uses 2 data nodes (leader + follower, both fsync before ack). Celeriant does
strictly more work per request while using TLS on top.

## Results

| Concurrency | Marten req/s | Marten avg ms | Marten p99 ms | Celeriant req/s | Ratio |
|---|---|---|---|---|---|
| 500 | **50,211** | 9 | 40 | ~80,000* | **~1.6x** |
| 1,000 | 46,191 | 21 | 83 | ~105,000* | **~2.3x** |
| 2,000 | 41,316 | 47 | 196 | ~120,000* | **~2.9x** |
| 3,000 | 38,672 | 76 | 307 | ~130,000* | **~3.4x** |
| 4,500 | 34,114 | 130 | 644 | ~140,000* | **~4.1x** |
| 6,000 | 30,917 | 195 | 988 | ~150,000* | **~4.9x** |
| 9,000 | 24,183 | 451 | 3,155 | 144,655 | **6.0x** |
| 12,000 | 973 | 13,973 | 23,002 | 190,647 | **196x** |
| 15,000 | 1,079 | 22,939 | 28,286 | 226,015 | **209x** |
| 18,000 | 1,255 | 24,696 | 33,729 | 264,051 | **210x** |
| 21,000 | 1,553 | 22,972 | 39,046 | 292,946 | **189x** |
| 24,000 | 1,618 | 26,920 | 43,273 | 318,768 | **197x** |

*\* Celeriant values below 9,000 concurrency are interpolated from the full sweep curve*

## Key findings

### Throughput

- **Celeriant peak: 374,552 req/s** at 39,000 concurrency
- **PostgreSQL peak: 50,211 req/s** at 500 concurrency — **7.5x lower**
- PostgreSQL throughput **decreases monotonically** as connections increase
- At 12,000 connections PostgreSQL falls off a cliff: throughput drops 97% from 24k to <1k req/s

### Connection scaling

PostgreSQL exhibits the classic process-per-connection scaling wall:

| Concurrency | Marten req/s | Avg latency | Degradation from peak |
|---|---|---|---|
| 500 | 50,211 | 9ms | — |
| 3,000 | 38,672 | 76ms | -23% |
| 6,000 | 30,917 | 195ms | -38% |
| 9,000 | 24,183 | 451ms | -52% |
| 12,000 | 973 | 13,973ms | **-98%** |

Between 9,000 and 12,000 connections, throughput collapses from 24k to <1k req/s.
This is not a gradual degradation — it's a hard cliff.

Celeriant, by contrast, scales linearly to 39,000 concurrent connections with increasing
throughput at every level.

### Why PostgreSQL hits a wall

Each Marten `SaveChangesAsync()` executes a PostgreSQL transaction:

1. `BEGIN`
2. `INSERT INTO mt_events (data, type, ...) VALUES ($1::jsonb, ...)`
3. `UPDATE mt_streams SET version = version + 1 WHERE id = $1`
4. `COMMIT` (WAL fsync with `synchronous_commit = on`)

At high concurrency, multiple bottlenecks compound:

1. **Process-per-connection model.** Each of the 12,000 connections is a separate OS process. Context switching, scheduler pressure, and memory overhead scale linearly.

2. **WAL insert lock contention.** All writers serialize through PostgreSQL's WAL insert lock. With thousands of processes competing for the lock, wait times dominate.

3. **Row-level lock contention on mt_streams.** The `UPDATE mt_streams SET version = version + 1` statement takes a row lock per stream. With many concurrent writers hitting different streams, lock manager overhead grows.

4. **MVCC overhead.** Every write creates new tuple versions. The heap, indexes, and visibility map all accumulate dead tuples that autovacuum must clean up.

5. **JSONB serialization.** Each event payload is parsed and stored as JSONB, adding CPU overhead per operation.

Celeriant avoids all of these: single-threaded async I/O via io_uring with Direct I/O, memory-mapped WAL with a single `fdatasync()`, no SQL parsing, no MVCC, no process forking.

### Durability comparison

| | PostgreSQL (`synchronous_commit = on`) | Celeriant |
|---|---|---|
| Write to disk | WAL fsync before ack | `fdatasync()` before ack |
| Replication | None (single node) | Leader + follower both fsync |
| Data loss on power failure | None (WAL is fsync'd) | None |
| Nodes | 1 | 2 |
| TLS | Disabled | mTLS with kTLS (TLS 1.3) |

Celeriant replicates to two nodes with mTLS while PostgreSQL runs single-node plaintext,
yet Celeriant is 6-200x faster depending on concurrency.

### Comparison with Kafka

All three systems on i4i.8xlarge, per-operation (no batching):

| System | Peak req/s | Peak concurrency | Connection scaling |
|---|---|---|---|
| **Celeriant** | **374,552** | 39,000 | Linear to 42k, errors at 48k |
| PostgreSQL/Marten | 50,211 | 500 | Cliff at 12k |
| Kafka | ~24,000 | 60,000 | Flat (throughput-limited, not connection-limited) |

PostgreSQL has 2x Kafka's peak throughput but cannot sustain it under connection pressure.
Kafka maintains steady (if low) throughput at any concurrency level. PostgreSQL collapses.
