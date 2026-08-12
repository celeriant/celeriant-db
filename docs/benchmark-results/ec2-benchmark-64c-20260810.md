# EC2 64-core re-run: connection pinning, and what the old tail was measuring

Full concurrency sweep on 2× `i4i.16xlarge` (64 vCPU, 4× NVMe RAID0), repeated three times.
Supersedes the 64-core section of `ec2-benchmark.md` for latency; see "Against the May sweep"
for what changed and what is not yet explained.

- **Date:** 2026-08-10
- **Pipeline:** current `main` (post schema-bloom separation and seal-time right-sizing).
- **Shape:** 2× i4i.16xlarge data + 4× c6i.4xlarge clients, all spot, all in `ap-southeast-2a`.
- **Storage:** 4× NVMe RAID0 (headline), plus an EBS gp3 comparison at 16,000 IOPS / 1,000 MB/s.
- **Durability:** unchanged — `fdatasync()` + Direct I/O on both nodes, replicated over mTLS
  with kTLS offload, acknowledged only after both succeed.
- **Errors:** zero in all 27 NVMe runs; 667 on EBS at 120k, from a leader lease fence.
- **One line:** with one connection pinned per writer task, p99 is **flat at ~250 ms from
  24k to 132k concurrency**. The old sweep's tail climbing to 7.5 s was the load generator
  handing connections between tasks, not the database.

## The load generator defect

`rpi_cluster_pool_bench` already gave each task its own aggregate for the run. It still
called `pool.write_events` per request, and `NodePool::get` returns a connection to a shared
FIFO after every call — so a connection drifts between tasks that own *different* aggregates.
The server answers a request for another shard's aggregate by moving the whole TCP stream
across the intrashard mesh (`check_client_redirect` → `IntrashardMessages::ClientConnectionRedirect`).
The benchmark was partly measuring connection handover.

This is the same defect `487f8c1` fixed in the batch benchmarks, where each task owns its
connection outright. The fix here pins one pooled connection per task for the run, re-dialling
only on transport failure (which is also how leader failover still reaches this path).

Measured directly via `celeriant_connection_redirects_total`:

| Mode | Redirects per connection |
|---|---|
| Pinned | **~1.1** — the one-time handover onto the owning shard at dial, then nothing |
| Pooled | ~7.4 across the sweep, rising with concurrency |

A caveat on scope: at low concurrency the defect barely bites. At 8,000 tasks with a 1:1
task-to-connection pool, pinned and pooled were within noise on throughput (23.9k vs 23.3k
req/s per client) because the idle deque is nearly always empty when a task returns and
immediately re-acquires its own connection. The cost appears at sweep concurrency.

## Pinned vs pooled, same cluster, same hour

| Concurrency | Pinned req/s | p99 | p99.9 | Pooled req/s | p99 | p99.9 | Δ req/s |
|---|---|---|---|---|---|---|---|
| 24,000 | 249,393 | 243 | 391 | 246,459 | 290 | 3,703 | +1.2% |
| 36,000 | 340,957 | 262 | 414 | 315,222 | 334 | 4,970 | +8.2% |
| 48,000 | 401,838 | 250 | 359 | 337,971 | 814 | 6,541 | +18.9% |
| 60,000 | **408,443** | 250 | 348 | 344,924 | 2,563 | 8,109 | +18.4% |
| 72,000 | 407,225 | 251 | 349 | 358,542 | 3,991 | 9,157 | +13.6% |
| 96,000 | 391,219 | 264 | 382 | 355,088 | 7,226 | 13,006 | +10.2% |
| 108,000 | 401,300 | 250 | 362 | 358,694 | 8,387 | 15,103 | +11.9% |
| 120,000 | 385,470 | 282 | 363 | 370,413 | 9,218 | 15,153 | +4.1% |
| 132,000 | 405,843 | 252 | 337 | 367,234 | 10,685 | 15,178 | +10.5% |

Throughput is 1–19% higher pinned. The tail is a different story entirely: pinned p99 never
leaves the 243–282 ms band, while pooled reaches 10.7 s. p99.9 tells the same story — 337–414 ms
pinned, 15.2 s pooled.

## Repeatability

The pinned sweep was run twice back to back.

| Concurrency | Run 1 | Run 2 | Δ | p99 r1 | p99 r2 |
|---|---|---|---|---|---|
| 24,000 | 249,393 | 263,537 | +5.7% | 243 | 228 |
| 36,000 | 340,957 | 361,118 | +5.9% | 262 | 233 |
| 48,000 | 401,838 | 409,709 | +2.0% | 250 | 236 |
| 60,000 | 408,443 | **416,434** | +2.0% | 250 | 245 |
| 72,000 | 407,225 | 409,487 | +0.6% | 251 | 249 |
| 96,000 | 391,219 | 397,434 | +1.6% | 264 | 258 |
| 108,000 | 401,300 | 393,741 | −1.9% | 250 | 256 |
| 120,000 | 385,470 | 405,736 | +5.3% | 282 | 250 |
| 132,000 | 405,843 | 403,201 | −0.7% | 252 | 258 |

Mean +2.3%, range −1.9% to +5.9%. Both runs plateau at ~410k and hold a flat tail.

## Where the ceiling is

The plateau starts at 48k and does not move through 132k. The leader is the constraint:

- **Leader CPU:** 4–15% idle under load, dominated by **system time (~50–61%)** with user time
  at 6–34%. Kernel-side network/TLS/io_uring work, not application compute.
- **Disk:** nowhere near it. Leader NVMes averaged 11–16% util per device across the sweep,
  follower 8%. RAID0 removed the disk as a bottleneck and it has stayed removed.
- **Clients:** 80–94% idle throughout, in both modes. Not a load-generation limit.

The `c6i.4xlarge` clients are a substitution — `c7i.4xlarge` spot capacity was exhausted in
every AZ that also had `i4i.16xlarge` capacity. Their idle time rules them out as a factor.

## Against the May sweep

`ec2-benchmark.md` reports a 491,866 req/s peak at 120k with p99 rising to 6,414 ms. Two
separate findings, and only one is explained:

1. **The tail there was largely artifact.** Its p99 curve (186 ms → 7,497 ms) tracks this
   run's *pooled* shape, not the pinned one. On the current pipeline with pinned connections,
   p99 stays at ~250 ms across the entire range. The published tail overstates real latency.
2. **Peak throughput is ~17% lower and this is not yet explained.** 416k vs 492k, well outside
   the ±6% run-to-run band, and it is not the pinning (pinned is the *faster* mode) and not the
   clients (idle). The most likely cause is per-write cost added to the pipeline since May, but
   attributing it needs a bisect. Recorded here as open.

## EBS vs local NVMe

Same instance type, same client shape, same pinned load generator, storage swapped for one
gp3 volume provisioned at **16,000 IOPS / 1,000 MB/s** — the most gp3 will do, chosen so the
comparison prices EBS rather than the 3,000 IOPS / 125 MB/s default. Throughput must be set
after launch (`aws ec2 modify-volume --throughput 1000`); CloudFormation's
`AWS::EC2::Instance` block device mapping has no `Throughput` field and CDK drops it.

| Concurrency | NVMe RAID0 req/s | p99 | EBS gp3 req/s | p99 | Δ req/s |
|---|---|---|---|---|---|
| 24,000 | 263,537 | 228 | 253,362 | 480 | −4% |
| 36,000 | 361,118 | 233 | 299,521 | 619 | −17% |
| 48,000 | 409,709 | 236 | **313,506** | 772 | −23% |
| 60,000 | **416,434** | 245 | 275,262 | 1,039 | −34% |
| 72,000 | 409,487 | 249 | 233,547 | 1,124 | −43% |
| 96,000 | 397,434 | 258 | 153,750 | 1,279 | −61% |
| 108,000 | 393,741 | 256 | 150,506 | 1,202 | −62% |
| 120,000 | 405,736 | 250 | 174,279 | 1,098 | −57% |
| 132,000 | 403,201 | 258 | 148,542 | 1,211 | −63% |

EBS peaks at **313,506/s at 48k** and then *declines* — it does not plateau. Local NVMe holds
~400k flat to 132k. The gap widens with load: −4% at 24k, −63% at 132k.

Where it goes:

- **The volume is the constraint, and it is latency-bound, not bandwidth-bound.** The data
  volume averaged **82% util on the leader** (77% follower) across the sweep while drawing only
  ~2,100–2,600 write IOPS of the 16,000 provisioned and 14–80 MB/s of the 1,000 available.
  High utilisation at low IOPS means each I/O is slow, not that there are too many. Provisioning
  more IOPS would not help; this is EBS round-trip latency against a per-write `fdatasync()`.
- **It also costs more CPU.** Leader system time ran ~88% with 3% idle, against ~50–61% sys on
  NVMe. More kernel work per write on top of a slower device.
- **The instance-store drives sat at 0%** — confirming the mount landed on EBS and the
  comparison is honest. Device selection is by device model, not by `/dev/nvme1n1` guessing,
  which on an i4i would otherwise risk silently benchmarking instance store.

### Stability

At 120,000 concurrency the leader could not hold its lease. Shards were fenced
(`LeaderFenced`, "Not leader, leader address unknown") and **667 writes failed** before the
cluster recovered on its own and finished the sweep. Both nodes stayed up.

Local NVMe took **zero errors across all 27 runs** at every level to 132k. This is the finding
that matters more than the throughput delta: on EBS, at saturation, the write path starves the
lease renewal and the cluster sheds client-visible errors. If you run on EBS, stay well below
the knee.

## Effect on the published figure

celeriant.io quotes the 36,000-connection operating point. Current numbers there:

| | Published | 2026-08-10 (pinned, best of 2) |
|---|---|---|
| Durable writes/s | 325,000 | **361,118** |
| p50 | 94 ms | 70 ms |
| p95 | 111 ms | 141 ms |
| p99 | 201 ms | 233 ms |

Throughput and p50 improved; p95 and p99 are worse. The p99 201 ms claim does not hold on the
current pipeline.

## Reproduce

```bash
make infra CDK_ARGS="-c keyPair=my-key -c instanceType=i4i.16xlarge \
  -c clientInstanceType=c6i.4xlarge -c clientCount=4 -c az=ap-southeast-2a"
make certs && make deploy KEY_ARG="--key-file ~/.ssh/id_rsa" && make start
make run-sweep SWEEP_LEVELS=24000,36000,48000,60000,72000,96000,108000,120000,132000
# add BENCH_PINNED=0 to reproduce the connection-drift numbers

# EBS comparison — gp3 throughput is NOT settable from CloudFormation, raise it after deploy
make infra CDK_ARGS="... -c storageType=ebs -c ebsDataVolumeSize=500"
aws ec2 modify-volume --volume-id <data-volume-id> --throughput 1000
```

Raw data: `ec2-benchmark-64c-20260810.csv`.
