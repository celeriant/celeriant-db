# EC2 3-Client Benchmark — 2026-03-24

Full performance curves with 3 parallel client nodes across 4 configurations (16c and 32c, x86 and ARM).
All writes are durable to two NVMe disks via `fdatasync()` + Direct I/O, replicated over mTLS
(kTLS-offloaded TLS 1.3), acknowledged only after both succeed.

## Test setup

- **Data nodes:** 2x i4i/i4g (NVMe instance store, XFS, Direct I/O via io_uring)
- **Client nodes:** 3x c7i/c7g.4xlarge (16 vCPU each)
- **Network:** Same AZ, same VPC
- **Duration:** 15 seconds per level
- **Tasks split evenly:** total_concurrency / 3 per client
- **Sysctl tuned:** `ip_local_port_range=1024-65535`, `somaxconn=65535`

## Configurations

| Label | Data nodes | vCPU/node | NVMe | Client nodes | $/hr (data) |
|---|---|---|---|---|---|
| x86 16c | 2x i4i.4xlarge | 16 | 1x 937GB | 3x c7i.4xlarge | ~$2.88 |
| x86 32c | 2x i4i.8xlarge | 32 | 2x 1875GB | 3x c7i.4xlarge | ~$7.20 |
| ARM 16c | 2x i4g.4xlarge | 16 | 1x 937GB | 3x c7g.4xlarge | ~$2.30 |
| ARM 32c | 2x i4g.8xlarge | 32 | 2x 1875GB | 3x c7g.4xlarge | ~$5.76 |

## Combined throughput (req/s) — all configurations

| Total Concurrency | x86 16c | x86 32c | ARM 16c | ARM 32c |
|---|---|---|---|---|
| 9,000 | 131,077 | 144,655 | 119,664 | 131,989 |
| 12,000 | 163,608 | 190,647 | 149,805 | 176,970 |
| 15,000 | 193,700 | 226,015 | 166,855 | 206,357 |
| 18,000 | 204,811 | 264,051 | 178,784 | 227,798 |
| 21,000 | 228,124 | 292,946 | 186,483 | 250,456 |
| 24,000 | **231,798** | 318,768 | **192,769** | 267,695 |
| 27,000 | 225,913 | 331,769 | 181,861 | 248,506 |
| 30,000 | 239,294 | 338,689 | 176,031 | 255,807 |
| 33,000 | 226,300 | 354,384 | 176,999 | 272,056 |
| 36,000 | 230,998 | 366,626 | 176,408 | 274,711 |
| 39,000 | 232,535 | **374,552** | 193,262 | 269,000 |
| 42,000 | 231,111 | 371,393 | 179,856 | **280,883** |
| 48,000 | 158,883* | 275,782* | 180,883 | 266,710 |
| 54,000 | 178,519* | 296,534* | 155,809 | 256,323 |
| 60,000 | 12,011* | 17,513* | 112,903* | 22,355* |

*\* = errors present (client-side timeouts, not server crashes)*

## Peak summary

| Config | Peak req/s | @ Total Concurrency | $/hr (data) | req/s per $/hr | Cost per M writes |
|---|---|---|---|---|---|
| **x86 32c** | **374,552** | **39,000** | $7.20 | 52,021 | $0.0053 |
| ARM 32c | 280,883 | 42,000 | $5.76 | 48,764 | $0.0057 |
| x86 16c | 239,294 | 30,000 | $2.88 | 83,088 | $0.0033 |
| ARM 16c | 193,262 | 39,000 | $2.30 | 84,027 | $0.0033 |

## Key findings

### Scaling: 16c vs 32c

| Metric | x86 16c→32c | ARM 16c→32c |
|---|---|---|
| Peak throughput | 239k → 375k (+57%) | 193k → 281k (+46%) |
| Cost (data nodes) | $2.88 → $7.20 (+150%) | $2.30 → $5.76 (+150%) |
| req/s per $/hr | 83k → 52k (-37%) | 84k → 49k (-42%) |

Doubling cores gives 46-57% more throughput but costs 150% more. **16c is the cost-efficiency sweet spot** on both architectures.

### x86 vs ARM at each tier

| Tier | x86 peak | ARM peak | x86 advantage | ARM cost advantage |
|---|---|---|---|---|
| 16c | 239,294 | 193,262 | +24% | ARM 20% cheaper |
| 32c | 374,552 | 280,883 | +33% | ARM 20% cheaper |

x86 widens its lead at higher core counts — Sapphire Rapids benefits more from additional cores than Graviton3 for this workload.

### Performance curve shape

- **x86 32c:** Keeps climbing to 39k concurrency then plateaus at 370k. Clean error-free operation to 42k.
- **ARM 32c:** Plateaus earlier around 24-36k at ~270k, with a wider flat zone. More consistent under overload.
- **x86 16c:** Peaks at 24-30k then holds ~230k. Errors start at 48k.
- **ARM 16c:** Peaks at 24k (~193k) then gradually declines. Most graceful degradation of all configs — still 180k at 48k with zero errors.

### Stability

- **Zero `RwLock` contention** across all 4 configurations, all concurrency levels.
- **Zero S3 fallback uploads** — replication stayed on TCP for every run.
- **Zero panics, zero crashes** — data nodes healthy after every sweep.

### Best value recommendations

| Workload | Recommended | Peak | Cost |
|---|---|---|---|
| < 150k req/s | ARM 16c (i4g.4xlarge) | 193k | $2.30/hr |
| 150-240k req/s | x86 16c (i4i.4xlarge) | 239k | $2.88/hr |
| 240-280k req/s | ARM 32c (i4g.8xlarge) | 281k | $5.76/hr |
| 280-375k req/s | x86 32c (i4i.8xlarge) | 375k | $7.20/hr |
