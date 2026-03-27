# EC2 Benchmark — 2026-03-27

Full performance curve with 3 parallel client nodes on x86 32-core (i4i.8xlarge).
All writes are durable to two NVMe disks via `fdatasync()` + Direct I/O, replicated over mTLS
(kTLS-offloaded TLS 1.3), acknowledged only after both succeed.

## Test setup

- **Data nodes:** 2x i4i.8xlarge (32 vCPU, 256GB RAM, NVMe instance store, XFS, Direct I/O via io_uring)
- **Client nodes:** 3x c7i.4xlarge (16 vCPU each)
- **Network:** Same AZ, same VPC
- **Duration:** 15 seconds per level
- **Tasks split evenly:** total_concurrency / 3 per client
- **TLS:** mTLS with kTLS offload (TLS 1.3)
- **Durability:** `fdatasync()` on both leader and follower before ack

## Results

| Concurrency | req/s | avg ms | P50 ms | P95 ms | P99 ms | P99.9 ms |
|---|---|---|---|---|---|---|
| 9,000 | 147,678 | 61.1 | 59 | 62 | 90 | 539 |
| 12,000 | 190,035 | 62.8 | 61 | 65 | 77 | 422 |
| 15,000 | 224,696 | 66.3 | 64 | 71 | 89 | 549 |
| 18,000 | 260,502 | 68.6 | 66 | 71 | 78 | 709 |
| 21,000 | 293,573 | 71.0 | 68 | 75 | 84 | 734 |
| 24,000 | 320,887 | 75.0 | 71 | 80 | 89 | 1,028 |
| 27,000 | 335,762 | 80.1 | 74 | 90 | 107 | 1,197 |
| 30,000 | 354,566 | 83.9 | 76 | 92 | 105 | 1,401 |
| 33,000 | 369,274 | 89.5 | 79 | 100 | 126 | 1,716 |
| 36,000 | 379,490 | 94.9 | 83 | 112 | 128 | 1,845 |
| 39,000 | 380,367 | 102.8 | 86 | 120 | 139 | 2,636 |
| 42,000 | 384,247 | 108.3 | 88 | 126 | 146 | 2,734 |
| 48,000 | 388,003 | 123.5 | 95 | 143 | 175 | 4,131 |
| 54,000 | **389,759** | 138.3 | 103 | 158 | 217 | 4,794 |
| 60,000 | 74,493* | 816.9 | 63 | 11,180 | 15,871 | 17,950 |

*\* = errors present (15,336 client-side port exhaustion — `Cannot assign requested address`)*

## Key findings

### Peak performance

- **Peak: 389,759 req/s** at 54,000 concurrency (zero errors)
- **Sustained plateau: 380-390k req/s** from 39,000 to 54,000 concurrency
- **Error-free operation** from 9,000 to 54,000 connections
- At 60,000 connections, client-side port exhaustion (not server-side) causes errors

### Latency profile

Latency stays remarkably flat as throughput scales:

| Concurrency | req/s | P50 ms | P99 ms | P99 degradation |
|---|---|---|---|---|
| 9,000 | 147,678 | 59 | 90 | — |
| 24,000 | 320,887 | 71 | 89 | -1% |
| 36,000 | 379,490 | 83 | 128 | +42% |
| 48,000 | 388,003 | 95 | 175 | +94% |
| 54,000 | 389,759 | 103 | 217 | +141% |

P50 latency grows by less than 2x (59ms to 103ms) while throughput nearly triples.
P99 stays under 220ms across the entire error-free range. Tail latency (P99.9)
grows at high concurrency as `fdatasync()` batching reaches saturation, but median
latency remains predictable.

### Comparison with PostgreSQL and Kafka

All systems on i4i.8xlarge, per-operation (no batching), with TLS and replication:

| System | Peak req/s | P50 at peak | P99 at peak | Nodes | TLS | Fsync |
|---|---|---|---|---|---|---|
| **Celeriant** | **389,759** | **103ms** | **217ms** | 2 | mTLS (kTLS) | Both nodes |
| PostgreSQL/Marten | 42,721 | 5 | 46 | 2 | mTLS (OpenSSL) | Both nodes |
| Kafka | ~24,000 | ~1,177 | ~1,342 | 3 | TLS | None |

At comparable concurrency (24,000 connections):
- **Celeriant:** 320,887 req/s, 71ms P50, 89ms P99
- **PostgreSQL:** 1,651 req/s, 40,170ms P50 (collapsed)
- **Kafka:** 21,212 req/s, 1,177ms avg

PostgreSQL's low-concurrency latency (5ms P50 at 500 connections) is excellent for
light workloads but collapses under connection pressure. Celeriant maintains sub-220ms
P99 across the entire scaling range while delivering 9x the throughput of PostgreSQL's
peak and 16x Kafka's peak.
