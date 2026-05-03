# EC2 Benchmark — i4i

Full performance curves on x86 i4i instances with durable, replicated writes over mTLS.
All writes are durable to two NVMe disks via `fdatasync()` + Direct I/O, replicated over mTLS
(kTLS-offloaded TLS 1.3), acknowledged only after both succeed.

- **32-core results:** 2026-05-03 (latest replication pipeline rewrite + heartbeat/lease changes)
- **64-core results:** 2026-03-27 (not re-run)

## Test setup

### 32-core (i4i.8xlarge)

- **Data nodes:** 2x i4i.8xlarge (32 vCPU, 256 GB RAM, NVMe instance store, XFS, Direct I/O via io_uring)
- **Client nodes:** 3x c7i.4xlarge (16 vCPU each)
- **Network:** Same AZ, same VPC
- **Duration:** 15 seconds per level
- **Tasks split evenly:** total_concurrency / 3 per client
- **TLS:** mTLS with kTLS offload (TLS 1.3)
- **Durability:** `fdatasync()` on both leader and follower before ack
- **Profile:** `make infra PROFILE=i4i-32c` then `make run-sweep`

### 64-core (i4i.16xlarge)

- **Data nodes:** 2x i4i.16xlarge (64 vCPU, 512 GB RAM, NVMe instance store, XFS, Direct I/O via io_uring)
- **Client nodes:** 4x c7i.4xlarge (16 vCPU each)
- **Network:** Same AZ, same VPC
- **Duration:** 15 seconds per level
- **Tasks split evenly:** total_concurrency / 4 per client
- **TLS:** mTLS with kTLS offload (TLS 1.3)
- **Durability:** `fdatasync()` on both leader and follower before ack

## Results — 32-core (i4i.8xlarge), 2026-05-03

| Concurrency | req/s | avg ms | P50 ms | P95 ms | P99 ms | P99.9 ms |
|---|---|---|---|---|---|---|
| 9,000 | 138,023 | 65.0 | 59 | 62 | 106 | 1,567 |
| 12,000 | 181,130 | 66.0 | 61 | 67 | 72 | 1,327 |
| 15,000 | 218,723 | 68.3 | 63 | 68 | 92 | 1,321 |
| 18,000 | 245,790 | 72.9 | 66 | 72 | 125 | 1,720 |
| 21,000 | 276,580 | 76.0 | 68 | 75 | 110 | 2,004 |
| 24,000 | 298,047 | 80.1 | 71 | 80 | 98 | 2,289 |
| 27,000 | 317,594 | 84.4 | 75 | 90 | 105 | 2,191 |
| 30,000 | 334,875 | 89.3 | 77 | 92 | 147 | 2,658 |
| 33,000 | 350,599 | 93.4 | 78 | 103 | 182 | 2,844 |
| 36,000 | 357,911 | 99.7 | 82 | 112 | 135 | 3,061 |
| 39,000 | 369,995 | 106.0 | 86 | 122 | 156 | 3,430 |
| 42,000 | 369,740 | 114.0 | 88 | 129 | 249 | 4,043 |
| 48,000 | 386,013 | 124.5 | 95 | 144 | 866 | 4,269 |
| 54,000 | 384,544 | 140.6 | 103 | 158 | 1,692 | 5,267 |
| 60,000 | 383,813 | 156.9 | 107 | 173 | 2,477 | 6,088 |
| 66,000 | 384,234 | 172.2 | 108 | 188 | 3,017 | 6,833 |
| 72,000 | 390,521 | 183.9 | 109 | 198 | 3,658 | 7,363 |
| 84,000 | **398,471** | 211.7 | 109 | 206 | 4,969 | 9,727 |
| 96,000 | 387,096 | 248.6 | 109 | 209 | 6,485 | 12,651 |
| 108,000 | 381,685 | 282.7 | 108 | 207 | 8,031 | 15,109 |

Zero errors at every level — including 60k where the previous run (2026-03-27) collapsed
with 15,336 client-side port exhaustion errors. Throughput plateaus around 380–400k req/s
from ~48k all the way through 108k concurrency.

P99 and P99.9 from ~48k onward reflect saturation behavior — queue depth dominated, not
operational latency. At operational loads (≤36k, well below the plateau), P99 stays under
200ms and is essentially unchanged from the prior run.

## Results — 64-core (i4i.16xlarge), 2026-03-27

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

- **32-core peak: 398,471 req/s** at 84,000 concurrency (zero errors)
- **64-core peak: 561,207 req/s** at 108,000 concurrency (zero errors) — 41% higher
- 32-core sustains 380k+ req/s across a wide plateau (48k → 108k concurrency, zero errors)
- The 32-core port-exhaustion collapse at 60k seen in the 2026-03-27 run is resolved

### Scaling efficiency

Doubling cores from 32 to 64 delivers 41% more throughput, not 2×. This is expected:
replication, `fdatasync()` batching, and network I/O are shared bottlenecks that don't
scale linearly with CPU count. The per-core efficiency at peak is ~12,450 req/s/core
(32c) vs ~8,800 req/s/core (64c).

### Latency profile

Operational latency stays flat across the throughput-scaling region; tail latency
grows once the system enters the saturation plateau.

**32-core (operational range):**

| Concurrency | req/s | P50 ms | P99 ms |
|---|---|---|---|
| 9,000 | 138,023 | 59 | 106 |
| 24,000 | 298,047 | 71 | 98 |
| 36,000 | 357,911 | 82 | 135 |

P50 grows roughly with concurrency. P99 stays under 200ms across the whole operational
range. Once the system enters its plateau (48k+), tail latency reflects queue depth
rather than service time and is not a meaningful operational signal — by 84k (peak
throughput) P99 is ~5s.

**64-core:**

| Concurrency | req/s | P50 ms | P99 ms |
|---|---|---|---|
| 24,000 | 333,305 | 66 | 108 |
| 72,000 | 535,292 | 105 | 210 |
| 108,000 | 561,207 | 129 | 2,524 |

### Comparison with PostgreSQL and Kafka

All systems on i4i.8xlarge, per-operation (no batching), with TLS and replication:

| System | Peak req/s | P50 at peak | P99 at peak | Nodes | TLS | Fsync |
|---|---|---|---|---|---|---|
| **Celeriant (64c)** | **561,207** | **129ms** | **2,524ms** | 2 | mTLS (kTLS) | Both nodes |
| **Celeriant (32c)** | **398,471** | **109ms** | **4,969ms** | 2 | mTLS (kTLS) | Both nodes |
| PostgreSQL/Marten | 42,721 | 5 | 46 | 2 | mTLS (OpenSSL) | Both nodes |
| Kafka | ~24,000 | ~1,177 | ~1,342 | 3 | TLS | None |

P99 at peak reflects saturation queueing, not user-visible operational latency. At
matched concurrency below the knee:

At 24,000 connections:
- **Celeriant (64c):** 333,305 req/s, 66ms P50, 108ms P99
- **Celeriant (32c):** 298,047 req/s, 71ms P50, 98ms P99
- **PostgreSQL:** 1,651 req/s, 40,170ms P50 (collapsed)
- **Kafka:** 21,212 req/s, 1,177ms avg
