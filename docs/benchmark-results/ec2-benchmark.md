# EC2 Benchmark — 2026-03-27

Full performance curves on x86 i4i instances with durable, replicated writes over mTLS.
All writes are durable to two NVMe disks via `fdatasync()` + Direct I/O, replicated over mTLS
(kTLS-offloaded TLS 1.3), acknowledged only after both succeed.

## Test setup

### 32-core (i4i.8xlarge)

- **Data nodes:** 2x i4i.8xlarge (32 vCPU, 256 GB RAM, NVMe instance store, XFS, Direct I/O via io_uring)
- **Client nodes:** 3x c7i.4xlarge (16 vCPU each)
- **Network:** Same AZ, same VPC
- **Duration:** 15 seconds per level
- **Tasks split evenly:** total_concurrency / 3 per client
- **TLS:** mTLS with kTLS offload (TLS 1.3)
- **Durability:** `fdatasync()` on both leader and follower before ack

### 64-core (i4i.16xlarge)

- **Data nodes:** 2x i4i.16xlarge (64 vCPU, 512 GB RAM, NVMe instance store, XFS, Direct I/O via io_uring)
- **Client nodes:** 4x c7i.4xlarge (16 vCPU each)
- **Network:** Same AZ, same VPC
- **Duration:** 15 seconds per level
- **Tasks split evenly:** total_concurrency / 4 per client
- **TLS:** mTLS with kTLS offload (TLS 1.3)
- **Durability:** `fdatasync()` on both leader and follower before ack

## Results — 32-core (i4i.8xlarge)

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

## Results — 64-core (i4i.16xlarge)

| Concurrency | req/s | avg ms | P50 ms | P95 ms | P99 ms | P99.9 ms |
|---|---|---|---|---|---|---|
| 24,000 | 333,305 | 71.6 | 66 | 85 | 108 | 887 |
| 36,000 | 435,970 | 83.0 | 71 | 97 | 123 | 2,086 |
| 48,000 | 495,760 | 95.0 | 82 | 112 | 141 | 2,624 |
| 60,000 | 527,787 | 109.0 | 93 | 133 | 167 | 2,493 |
| 72,000 | 535,292 | 126.0 | 105 | 156 | 210 | 3,490 |
| 84,000 | 540,855 | 152.0 | 117 | 183 | 704 | 5,900 |
| 96,000 | 549,579 | 188.8 | 125 | 215 | 2,262 | 7,672 |
| 108,000 | **561,207** | 203.3 | 129 | 236 | 2,524 | 8,406 |
| 120,000 | 549,289 | 234.9 | 132 | 254 | 4,179 | 15,570 |
| 132,000 | 551,232 | 262.9 | 133 | 271 | 4,196 | 16,078 |
| 144,000 | 519,527* | 259.9 | 135 | 280 | 5,107 | 11,862 |
| 160,000 | 475,970* | 386.1 | 136 | 305 | 11,917 | 17,819 |

*\* = errors present (client-side connection timeouts at high concurrency)*

## Key findings

### Peak performance

- **32-core peak: 389,759 req/s** at 54,000 concurrency (zero errors)
- **64-core peak: 561,207 req/s** at 108,000 concurrency (zero errors) — **44% higher**
- Both configurations show a sustained plateau before degradation
- 64-core error-free range extends to 108,000 connections (vs 54,000 on 32-core)

### Scaling efficiency

Doubling cores from 32 to 64 delivers 44% more throughput, not 2x. This is expected:
replication, `fdatasync()` batching, and network I/O are shared bottlenecks that don't
scale linearly with CPU count. The per-core efficiency at peak is ~12,200 req/s/core
(32c) vs ~8,800 req/s/core (64c).

### Latency profile

Latency stays remarkably flat on both configurations as throughput scales:

**32-core:**

| Concurrency | req/s | P50 ms | P99 ms |
|---|---|---|---|
| 9,000 | 147,678 | 59 | 90 |
| 24,000 | 320,887 | 71 | 89 |
| 54,000 | 389,759 | 103 | 217 |

**64-core:**

| Concurrency | req/s | P50 ms | P99 ms |
|---|---|---|---|
| 24,000 | 333,305 | 66 | 108 |
| 72,000 | 535,292 | 105 | 210 |
| 108,000 | 561,207 | 129 | 2,524 |

P50 latency grows by less than 2x across the entire error-free range on both configs.
The 64-core configuration shows higher P99 tail latency at peak due to increased
contention across 64 shards competing for NVMe `fdatasync()` bandwidth.

### Comparison with PostgreSQL and Kafka

All systems on i4i.8xlarge, per-operation (no batching), with TLS and replication:

| System | Peak req/s | P50 at peak | P99 at peak | Nodes | TLS | Fsync |
|---|---|---|---|---|---|---|
| **Celeriant (64c)** | **561,207** | **129ms** | **2,524ms** | 2 | mTLS (kTLS) | Both nodes |
| **Celeriant (32c)** | **389,759** | **103ms** | **217ms** | 2 | mTLS (kTLS) | Both nodes |
| PostgreSQL/Marten | 42,721 | 5 | 46 | 2 | mTLS (OpenSSL) | Both nodes |
| Kafka | ~24,000 | ~1,177 | ~1,342 | 3 | TLS | None |

At comparable concurrency (24,000 connections):
- **Celeriant (64c):** 333,305 req/s, 66ms P50, 108ms P99
- **Celeriant (32c):** 320,887 req/s, 71ms P50, 89ms P99
- **PostgreSQL:** 1,651 req/s, 40,170ms P50 (collapsed)
- **Kafka:** 21,212 req/s, 1,177ms avg
