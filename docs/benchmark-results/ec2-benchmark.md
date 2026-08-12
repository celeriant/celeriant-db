# EC2 Benchmark: i4i

Durable, replicated writes on x86 i4i instances. Covers the single-NVMe vs RAID0 storage
question on the 32 and 64 vCPU boxes, plus the cheapest tier that still runs a real cluster.

> **Superseded for 64-core latency.** The load generator used for this document let pool
> connections drift between writer tasks, so a large share of writes landed on a shard the
> connection was not on and the server migrated the TCP stream to reach it. The p99/p99.9
> curves below are inflated by that handover. See `ec2-benchmark-64c-20260810.md`, where
> pinning one connection per task holds p99 at ~250 ms from 24k to 132k concurrency. The
> throughput and disk-utilisation findings here still stand.

- **Date:** 2026-05-28
- **Pipeline:** current, post the replication/durability rewrite. Not comparable to older runs.
- **Durability:** every write is `fdatasync()` + Direct I/O on both nodes' NVMe, replicated over
  mTLS (kTLS-offloaded TLS 1.3), acknowledged only after both succeed.
- **One line:** RAID0 is throughput-neutral at 32 cores, +32% at 64 cores where a single drive
  saturates, and gives the full aggregate capacity either way. That capacity is the real win for
  an append-only event store. RAID0 across all NVMes is the deploy default; `-c raid0=false` opts out.

## Test setup

Same AZ and VPC, 15s per concurrency level, mTLS with kTLS offload, `fdatasync()` on leader and
follower before ack, XFS + io_uring Direct I/O. Tasks split evenly across clients, 1:1
connections to tasks. Cost is the two data nodes only, Sydney on-demand Linux, 730 h/month.

| Shape | Data nodes | NVMe/node | Clients | Cluster cost |
|---|---|---|---|---|
| Entry | 2× i4i.large (2 vCPU, 16 GB) | 1× 468 GB | 1× c7i.2xlarge | $0.41/hr, ~$301/mo |
| 32-core | 2× i4i.8xlarge (32 vCPU, 256 GB) | 2× 3.75 TB | 3× c7i.4xlarge | $6.58/hr, ~$4,800/mo |
| 64-core | 2× i4i.16xlarge (64 vCPU, 512 GB) | 4× 3.75 TB | 4× c7i.4xlarge | $13.16/hr, ~$9,600/mo |

## Capacity, the headline win for an event store

i4i instances ship multiple NVMes but the OS mounts only one unless you stripe them. For an
append-only event log, the aggregate capacity is what lets more events live on local NVMe
before compaction or S3 offload:

| Instance | NVMe drives | Single (`raid0=false`) | RAID0 (default) |
|---|---|---|---|
| i4i.large | 1× 468 GB | 436 GiB | single drive only |
| i4i.8xlarge | 2× 3.75 TB | 3.4 TiB | **6.8 TiB** |
| i4i.16xlarge | 4× 3.75 TB | 3.4 TiB | **13.6 TiB** |

## Entry tier: i4i.large, ~$300/month

The cheapest i4i box: 2 vCPU, 16 GB, one 468 GB NVMe. Two of them on-demand in Sydney run
$0.41 an hour, about $301 a month. One c7i.2xlarge client drives the load.

| Connections | req/s | avg ms | P50 ms | P95 ms | P99 ms | P99.9 ms |
|---|---|---|---|---|---|---|
| 1,000 | 14,986 | 66.3 | 63 | 67 | 96 | 856 |
| 2,000 | 22,669 | 87.9 | 83 | 94 | 108 | 1,265 |
| 4,000 | **30,200** | 132.6 | 115 | 144 | **158** | 3,488 |
| 6,000 | 30,287 | 198.8 | 159 | 195 | 1,417 | 7,140 |
| 8,000 | 30,336 | 265.1 | 181 | 238 | 3,845 | 11,465 |
| 12,000 | 29,952 | 405.7 | 195 | 258 | 11,736 | 15,318 |

Peak ~30,300 writes/s, saturating at ~4,000 connections, where p99 is still 158ms. Past that,
throughput stays flat and the tail blows up. Disk barely moves (nvme1n1 ~3% util): a 2 vCPU
node is CPU, fsync, and replication bound long before its NVMe. Zero errors.

Cost efficiency runs the opposite way from raw throughput. The entry tier is ~100 writes/s per
dollar/month (30k for $301). The 64-core RAID0 cluster is ~51 (492k for $9,600). The big box
buys ceiling, capacity, and headroom, not efficiency.

## 32-core (i4i.8xlarge), at 36k concurrency

Run at 36,000 concurrency (12k/client), a single level, not a full sweep. Throughput-neutral:
the single drive never saturates, so striping just spreads the same load. RAID0's win here is
capacity.

| Storage | req/s (repeated runs) | P50 ms | Leader disk %util |
|---|---|---|---|
| Single NVMe | ~329k (321.5 / 333.2 / 324.2 / 338.1k) | ~89 | nvme1n1 54-60% avg, **78% max**; 2nd drive idle |
| RAID0 (2 drives) | ~332k (330.4 / 336.5 / 329.0k) | ~89 | ~31% per drive |

Zero errors at every run.

## 64-core (i4i.16xlarge), connection sweep

### Single NVMe

Swept at 72k and up. The single drive is already saturated there, so lower levels add nothing.

| Concurrency | req/s | avg ms | P50 ms | P95 ms | P99 ms | P99.9 ms |
|---|---|---|---|---|---|---|
| 72,000 | 342,890 | 212.4 | 136 | 199 | 3,641 | 7,483 |
| 96,000 | **373,758** | 258.0 | 147 | 242 | 5,084 | 8,993 |
| 108,000 | 369,092 | 292.0 | 146 | 257 | 6,309 | 10,477 |
| 120,000 | 359,254 | 331.3 | 147 | 263 | 7,700 | 11,813 |
| 132,000 | 361,093 | 365.4 | 151 | 273 | 8,951 | 13,379 |

The single drive is the bottleneck: leader `nvme1n1` ran **78.6% avg, 101% max** across the
sweep (the other 3 drives idle); follower `nvme1n1` 42% avg, 71% max.

### RAID0 (4 drives)

Latency stays tight through ~48k connections, then the tail climbs past a second while
throughput keeps rising to a ~492k peak. The usable operating point is ~36k connections, the
number on the site: **325,563 writes/s at p99 201ms**.

| Concurrency | req/s | avg ms | P50 ms | P95 ms | P99 ms | P99.9 ms |
|---|---|---|---|---|---|---|
| 24,000 | 258,238 | 92.3 | 77 | 96 | 186 | 3,152 |
| 36,000 | 325,563 | 110.2 | 94 | 111 | **201** | 3,192 |
| 48,000 | 381,791 | 125.8 | 101 | 121 | 362 | 4,238 |
| 60,000 | 421,194 | 142.8 | 108 | 136 | 1,317 | 5,133 |
| 72,000 | 450,059 | 161.9 | 107 | 171 | 2,422 | 6,630 |
| 96,000 | 489,289 | 198.1 | 105 | 196 | 4,546 | 8,924 |
| 108,000 | 484,792 | 221.6 | 105 | 207 | 5,798 | 10,114 |
| 120,000 | **491,866** | 243.7 | 105 | 207 | 6,414 | 12,277 |
| 132,000 | 485,440 | 273.1 | 105 | 213 | 7,497 | 13,990 |

Disk load spreads across all 4 drives: leader ~26-28% avg, ~43-51% max each; follower ~17-18%
avg. No single drive is close to saturation, anywhere in the sweep.

### Single vs RAID0

| Concurrency | Single req/s | RAID0 req/s | Δ |
|---|---|---|---|
| 72,000 | 342,890 | 450,059 | +31% |
| 96,000 | 373,758 | 489,289 | +31% |
| 108,000 | 369,092 | 484,792 | +31% |
| 120,000 | 359,254 | 491,866 | +37% |
| 132,000 | 361,093 | 485,440 | +34% |
| **Peak** | **373,758** | **491,866** | **+32%** |

RAID0 also cuts latency, P50 from ~147ms to ~105ms, because the saturated drive was adding
queue delay. Zero errors in both configs.

## Key findings

- **Capacity scales with drive count regardless of load.** 2× on the 8xlarge, 4× on the
  16xlarge. For an append-only event store this is the primary reason to stripe.
- **The throughput crossover is core count.** At 32 cores one NVMe handles the write/fsync
  volume with headroom (78% peak), so RAID0 is throughput-neutral. At 64 cores the leader pushes
  ~2× the volume through one drive and saturates it (101%); RAID0 across 4 drives removes the disk
  as the bottleneck for +32% throughput and lower latency.
- **Past the disk, the ceiling is upstream:** replication round-trip, `fdatasync()` batching, and
  network. Consistent with P50s in the 100ms range vs microsecond-scale local fsync.
- **The entry tier is the cost-efficiency winner.** ~30k durable writes/s for ~$300/month, p99
  under 160ms, on a box whose disk sits at 3% util. The big clusters buy raw ceiling, not value
  per write.
- **RAID0 is the deploy default.** `-c raid0=false` reproduces the single-drive saturation above.

## Reproduce

```bash
# entry tier
make infra CDK_ARGS="-c keyPair=my-key -c instanceType=i4i.large -c clientInstanceType=c7i.2xlarge -c clientCount=1"
make deploy KEY_ARG="--key-file ~/.ssh/id_rsa" && make start
make run-sweep SWEEP_LEVELS=1000,2000,4000,6000,8000,12000

# 64-core RAID0 (add -c raid0=false for single-NVMe)
make infra PROFILE=i4i-64c CDK_ARGS="-c keyPair=my-key"
make deploy KEY_ARG="--key-file ~/.ssh/id_rsa" && make start
make run-sweep SWEEP_LEVELS=24000,36000,48000,60000,72000,96000,108000,120000,132000
```

Each run prints a per-device `%util` summary alongside the throughput CSV.
