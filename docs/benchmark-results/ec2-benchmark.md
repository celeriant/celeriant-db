**_LLM GENERATED_**

# Celeriant on EC2

**A pair of bare-metal AWS boxes sustains 1,057,417 durable, replicated, encrypted writes per
second — every one of them `fdatasync()`ed to disk on both nodes before the client hears "yes",
at a median of 48 ms.** On the same hardware where Kafka and PostgreSQL were measured, Celeriant
is **9.8× PostgreSQL/Marten and 17.3× Kafka**, while being the only one of the three that
actually flushes to disk on both nodes before acknowledging. And the cheapest cluster worth
running — two 4-core ARM boxes on stock EBS, about **$295 a month** — does 67,720 of the same
durable writes per second, which is more throughput per dollar than any of the big machines.

Every figure below is measured, not modelled — current `main`, Sydney (`ap-southeast-2`), August
2026. Raw data, one row per measured cell: `ec2-benchmark-unified.csv`.

---

## The short version

- **Ceiling:** 1.06 M writes/s on two `i4i.metal`. The knee is at 60,000 connections; pushing
  past it buys nothing and costs latency.
- **Value:** the entry tier is the efficiency winner — **230 writes/s per dollar per month**,
  4.7× better than the big clusters.
- **Storage is the decision that matters.** Local NVMe holds ~400 k/s flat; the same box on EBS
  peaks at 313 k/s and then *declines*, ending 63% behind. Stripe every NVMe you have: RAID0 is
  +32% throughput at 64 vCPU and 4× the capacity.
- **One shipped default is badly wrong.** The 17 ms fsync amortisation window gives up **a third
  of the machine** on instance store and −29.6% on gp3. Anything under ~6 ms is equally good.
- **Small clusters do not degrade gracefully.** They shed tens of thousands of errors past
  their knee, and a 1-vCPU node can wedge at 100% CPU and never come back. Know the ceiling for
  your tier and stay under it.

## Pick a tier

Peak *clean* throughput — the highest level that ran with zero client-visible errors.

| tier | cluster | storage | durable writes/s | at connections | p50 | p99 | cost |
|---|---|---|---|---|---|---|---|
| **Flagship** | 2× i4i.metal (128 vCPU) | 8× NVMe RAID0 | **1,057,417** | 60,000 | 48 ms | 108 ms | ~$3,700/mo spot |
| Large | 2× i4i.16xlarge (64 vCPU) | 4× NVMe RAID0 | 466,640 | 60,000 | 97 ms | 264 ms | ~$9,600/mo |
| Mid | 2× i4i.8xlarge (32 vCPU) | 4× NVMe RAID0 | 419,132 | 39,000 | 78 ms | 169 ms | ~$4,800/mo |
| **Entry** | 2× c7g.xlarge (4 vCPU ARM) | stock gp3 | **67,720** | 8,000 | 113 ms | 165 ms | **~$295/mo** |
| Single node | 1× c6i.xlarge (4 vCPU) | stock gp3 | 86,658 | 8,000 | 85 ms | 108 ms | ~$172/mo |
| Floor | 2× c6g.medium (1 vCPU) | stock gp3 | 12,289 | 3,000 | 183 ms | 284 ms | ~$82/mo |

Cost is the cluster only, Sydney on-demand Linux at 730 h/month, plus gp3 volumes where they
apply. The metal pair is quoted at the spot price this sweep actually ran on ($5.08/hr); on
demand it is $26.33/hr, about $19,200 a month.

Two things in that table are worth a second look.

**The 32-core box gets 90% of the 64-core box's throughput for half the money.** Doubling vCPUs
from 32 to 64 buys 11%; going to 64 *physical* cores on `i4i.metal` buys 2.3× over that. Cores
that share an SMT sibling are not cores.

**One un-replicated `c6i.xlarge` beats the replicated ARM pair** — 86,658 against 67,720 — because
it is not paying for a network round-trip per write. On EBS that is a defensible trade: the volume
outlives the instance, so an acknowledged write already survives instance loss. What the second
node buys on EBS is availability and rolling upgrades, not durability.

## Against Kafka and PostgreSQL

Measured on identical hardware — 2× i4i.8xlarge data nodes, 3× c7i.4xlarge clients, one write per
request, wait for the ack, no batching. Each system at *its own* best concurrency.

| system | peak writes/s | at connections | nodes | what "acknowledged" means |
|---|---|---|---|---|
| **Celeriant** | **419,132** | 39,000 | 2 | `fdatasync` on **both** nodes before ack |
| PostgreSQL / Marten | 42,721 | 500 | 2 | `synchronous_commit` + synchronous standby |
| Kafka | 24,162 | 60,000 | 3 | **none** — page cache, ack after replication |

Kafka is simultaneously the slowest and the weakest on durability, on one more node. Its p99
reaches 2.1 s at 39,000 connections against Celeriant's 169 ms.

**The shape matters more than the peak.** PostgreSQL is genuinely competitive at low concurrency
and then hits the process-per-connection wall:

| connections | Celeriant | Marten | ratio |
|---|---|---|---|
| 500 | 80,000 | **42,721** | 1.9× |
| 3,000 | 130,000 | 29,226 | 4.4× |
| 9,000 | 144,655 | 12,666 | 11.4× |
| **12,000** | 190,647 | **901** | **212×** |
| 24,000 | 318,768 | 1,651 | 193× |

Marten *peaks at 500 connections* and degrades monotonically from there; the 97% collapse between
9,000 and 12,000 is the wall itself. At 500 connections PostgreSQL is genuinely competitive — the
story is scaling, not raw speed. Kafka is the opposite shape: flat and unbothered from 17 k to
24 k across the whole range, but never fast.

That Celeriant column is a single internally consistent ladder measured against Marten's. The two
levels since re-measured on the same hardware came out ~20% higher (228,130 at 12,000; 374,957 at
24,000), so the ratios above understate.

## Cost efficiency runs the other way from throughput

Durable writes per second, per dollar of monthly on-demand spend:

| cluster | writes/s | $/month | writes/s per $/mo |
|---|---|---|---|
| **2× c7g.xlarge** | 67,720 | 295 | **230** |
| 2× c6g.xlarge | 46,715 | 269 | 174 |
| 2× c6g.medium | 12,289 | 82 | 150 |
| 2× i4i.large | 30,200 | 301 | 100 |
| 2× i4i.8xlarge | 419,132 | 4,800 | 87 |
| 2× i4i.metal | 1,057,417 | 19,200 | 55 |
| 2× i4i.16xlarge | 466,640 | 9,600 | 49 |

The big machines buy ceiling, capacity and headroom — not value per write. A Graviton3 pair on
stock EBS is **4.7× more efficient** per dollar than the 64-vCPU cluster.

The single `c6i.xlarge` is left out because it is not a replicated cluster and so is not
comparable; for the record it is the most efficient thing measured, at 504 writes/s per $/month.

## The flagship, in full

2× `i4i.metal` (64 physical cores / 128 vCPU each, dual-socket Xeon Platinum 8375C), eight local
NVMe striped RAID0, replicated, **mTLS on both the client and replication paths**, four
c6i.4xlarge load generators, single AZ.

| connections | writes/s | run-to-run spread | p50 | p95 | p99 | CPU busy |
|---|---|---|---|---|---|---|
| 32,000 | 813,019 | 1.2% | 36 ms | 43 ms | 60 ms | 82% |
| **60,000** | **1,057,417** | **0.8%** | **48 ms** | **72 ms** | **108 ms** | 87% |
| 100,000 | 1,018,950 | 5.3% | 77 ms | 146 ms | 220 ms | 94% |
| 132,000 | 916,122 | 10.1% | 112 ms | 206 ms | 432 ms | 95% |

60,000 is both the throughput peak and the most reproducible point. Past it the box sits at
94–95% CPU, throughput *falls*, and the run-to-run spread widens to 5–10% — those points are
neither faster nor trustworthy. **Spread width locates the knee independently of the throughput
curve**, which is a useful thing to watch for on any hardware.

Configuration: 128 shards (one per vCPU), fsync window 1,000 µs, replication window 15,000 µs.
Those last two are measured values that ship pinned to the `i4i-metal` deploy profile — they are
not generic defaults, and the section below explains why.

For scale: the same box standalone and in cleartext does 1,936,064 writes/s. Replication plus
mTLS costs about 45% of that. The replicated, encrypted number is the one worth quoting because
it is the one anybody would actually run.

## Storage is the biggest decision

### Local NVMe against EBS

Same instance type, same clients, same load generator — only the storage differs. EBS here is a
gp3 volume provisioned to its maximum, 16,000 IOPS and 1,000 MB/s, so this prices EBS fairly
rather than testing the free tier.

| connections | NVMe RAID0 | p99 | EBS gp3 (max provisioned) | p99 | Δ |
|---|---|---|---|---|---|
| 24,000 | 249,393 | 243 ms | 253,362 | 480 ms | +2% |
| 48,000 | 401,838 | 250 ms | **313,506** | 772 ms | −22% |
| 72,000 | 407,225 | 251 ms | 233,547 | 1,124 ms | −43% |
| 132,000 | 403,201 | 258 ms | 148,542 | 1,211 ms | −63% |

**EBS matches local NVMe at low load and then falls apart.** It peaks at 48,000 connections and
*declines* from there — it never plateaus — while local NVMe holds ~400 k flat all the way to
132,000. Note the tail as well: EBS is already 2× worse at p99 in the row where its throughput
is competitive.

The volume is the constraint, and it is **latency-bound, not bandwidth-bound**: it ran 82%
utilised while drawing only ~2,100–2,600 write IOPS of the 16,000 provisioned. High utilisation
at low IOPS means each I/O is slow, not that there are too many. **Provisioning more IOPS buys
nothing** — on a 16-vCPU box the fsync path never exceeded 3.5% of the 16,000 provisioned, with
the measured fsync rate matching `1/latency` to within 0.5% at every concurrency. Provisioned
*throughput* is different: bandwidth saturated at exactly the 1,000 MB/s paid for.

EBS also costs more CPU — ~88% system time against ~50–61% on NVMe — and it is less stable under
load. See "Where it breaks".

### Stripe every drive you have

i4i instances ship multiple NVMes but the OS mounts only one unless you stripe them. RAID0 across
all of them is the deploy default (`-c raid0=false` opts out).

| instance | drives | single drive | RAID0 |
|---|---|---|---|
| i4i.8xlarge | 2× 3.75 TB | 3.4 TiB | **6.8 TiB** |
| i4i.16xlarge | 4× 3.75 TB | 3.4 TiB | **13.6 TiB** |

Capacity scales with drive count regardless of load, and for an append-only event store that is
the primary reason to stripe — more events live on local NVMe before compaction or S3 offload.

Throughput follows core count. At 32 vCPU one drive absorbs the write volume with headroom (78%
peak utilisation) and striping is throughput-neutral. At 64 vCPU the leader pushes twice the
volume through one drive and saturates it (101% utilisation); RAID0 across four drives removes
the disk as a bottleneck for **+32% throughput and lower median latency**. At 128 vCPU on eight
striped drives the disks sit at ~17% utilisation and are nowhere near the constraint.

## The two tuning knobs

Celeriant amortises two things over short time windows: `fsync` calls, and replication messages.
They look like the same knob. They are not, they have opposite characters, and **they must never
be tuned together.**

### The fsync window: the shipped default costs half the machine

Swept on `i4i.metal`, standalone, at 32,000 connections:

| window | writes/s | vs best |
|---|---|---|
| 100 µs | 1,937,187 | −2.4% |
| 800 µs | 1,886,768 | −4.9% |
| 1,600 µs | 1,983,860 | — |
| 6,400 µs | 1,911,055 | −3.7% |
| **17,000 µs (default)** | **1,325,348** | **−33.2%** |

Putting the window back inside the fast band is worth **+49.7%** on this box.

Anything from 100 µs to 6,400 µs performs identically — the run-to-run spread inside that band
(up to 7.9%) is wider than the difference between its members, so **no optimum is resolvable and
naming one would be unsupported.** The recommendation is a bound, not a value: **keep the fsync
window under ~6 ms.**

The same knob on gp3 behaves the same way and punishes long windows harder: 250 µs through
8,000 µs are within 1.5% of each other, the 17,000 µs default costs −29.6%, and 68,000 µs costs
−73.6%. A prediction that gp3 would want a *longer* window than instance store, on the theory
that its IOPS cap binds, was measured and falsified — the path is latency-bound, not IOPS-bound.

Two things make this knob easy to get wrong:

- **The wasted time shows up as iowait, not idle.** At the default the box reads 57% busy / 42%
  iowait against ~99% / ~0.5% inside the fast band. A reactor parked on the amortisation timer is
  accounted as iowait, so a per-thread "is it idle?" check reports a busy, healthy machine.
- **p99 ranks the bad configuration first.** At 17,000 µs p99 is 32 ms — *better* than the fast
  band's 36–60 ms — because throughput is a third lower so there is less queueing. Only p50
  exposes the tax (22 ms against 13 ms). Compare medians when tuning this knob.

### Why the fsync window is free to shrink

Both storage classes advertise `write cache: write through`, so the kernel elides the NVMe FLUSH
entirely — `fsync()` is an XFS log force plus a synchronous write, and on gp3 the controller
reports `vwc: 0` directly. A single-threaded 4 k fsync completes in ~90 µs, far below the
200–1000 µs it takes to program TLC NAND, so the drive is acknowledging from DRAM made
non-volatile by on-board power-loss capacitors.

There is an inversion in that. The event those capacitors protect against — power loss — is
exactly the event where instance store is discarded anyway (AWS drops it on stop, terminate, or
host failure). So the enterprise durability feature delivers **no durability benefit and all of
the latency benefit**. The real durability boundary is replication and S3 offload, not the flush,
which is why shrinking the amortisation window costs nothing: there was never any device I/O to
coalesce.

### The replication window: opposite character, and it does not travel

Replication amortises a real network round-trip with genuine per-message cost, so batching
genuinely pays. On `i4i.metal` the curve is **bimodal** — two maxima with a trough between them:

| window | writes/s | CPU busy | iowait | regime |
|---|---|---|---|---|
| 250 µs | 340,787 | 82% | 1% | CPU-bound, many small sends |
| 8,000 µs | 286,905 | 83% | 12% | trough |
| **15,000 µs** | **407,158** | **21%** | 78% | batch-bound — the optimum |
| 17,000 µs (default) | 370,667 | 18% | 81% | |
| 34,000 µs | 205,174 | 11% | 89% | over-batched |

15,000 µs beats the shipped default by **+9.8%** and delivers +19% over the 250 µs peak **while
using a quarter of the CPU**. This knob does not trade CPU for throughput; it wins both and frees
most of the box.

**It does not transfer to other instance types.** Swept alone on 2× i4i.8xlarge at matched
concurrency, the curve declines monotonically — the peak is at or below 250 µs, and metal's
15,000 µs optimum is **−34.8%** there. The bimodal structure is absent entirely. Neither the
round-trip model nor the batch-size model predicts this, and the mechanism is unknown. What is
settled is the practical point: **`REPLICATION_DELAY_US=15000` belongs to the `i4i-metal` profile
and must not ship as a generic default.** On the i4i.16xlarge, the shipped defaults match or beat
the metal tuning at every level.

### Shard count

`num_shards` defaults to the vCPU count. Near saturation that is right — 128 shards beat 64 by
+13.3% on metal at 32,000 connections, and win on median latency too. **At moderate load the sign
flips, hard:**

| connections | 64 shards (1/core) | 128 shards (1/vCPU) | Δ |
|---|---|---|---|
| 8,000 | **1,814,901** | 606,535 | **−66.6%** |
| 32,000 | 1,708,566 | **1,936,064** | +13.3% |
| 64,000 | 1,684,046 | **1,859,721** | +10.4% |

At 8,000 connections the default is **three times slower**, with p50 at 12 ms against 4 ms, while
burning more CPU. The default is tuned for the top of the load curve. The mechanism is not
established; the leading candidate is the N(N−1) cross-shard channel mesh — 16,256 channels at
128 shards against 4,032 at 64 — whose overhead is quadratic and unamortised when per-shard load
is low.

`reserve_coordinator_shard` costs nothing measurable at this size (+1.4%, inside the noise). One
shard in 128 is 0.8% of the box.

### On small boxes, don't bother

The fsync window swept on a 4-vCPU EBS node at its peak is worth **+2.4% at best**, against a
run-to-run spread of 0.4–1.3%. At its peak that box runs 93% busy / 7% iowait — CPU binds first,
so the window is not the constraint. **Publish and ship the entry tiers stock.**

The same knob on a 16-vCPU box gave +42%. The knob's value depends entirely on whether the box
has CPU headroom to exploit it.

## Where it breaks

Every tier has a ceiling, and past it behaviour is not graceful.

- **Small ARM pairs shed load hard.** The c7g.xlarge pair runs perfectly clean to 8,000
  connections and then sheds **75,095 errors** at 16,000, with p99 blowing to 2.5 s. That cell is
  in the data as a failure record, not as a measurement. **8,000 is the supported ceiling.**
- **A 1-vCPU node can wedge and not come back.** Above 4,000 connections the c6g.medium pair fell
  to zero throughput, stopped answering SSH entirely while EC2 status checks still passed, and sat
  at 99.5% CPU for **15+ minutes after the last client disconnected**. Spinning, not working. It
  did not recover when the load was removed. Open robustness issue; larger boxes have enough spare
  capacity to mask it.
- **On EBS, saturation starves the leader lease.** At 120,000 connections on the gp3 16xlarge the
  leader could not renew its lease, shards were fenced, and **667 writes failed** before the
  cluster recovered on its own. Local NVMe took zero errors across every run at every level to
  132,000. If you run on EBS, stay well below the knee.
- **The degraded path itself works.** During the 1-vCPU collapse the leader offloaded 12 event
  batches (15.75 MB) to S3 to hold the durability contract, with WAL indices **fully contiguous —
  no gaps**. Capacity failed; the safety mechanism did not. One item to confirm: a fallback batch
  was written 4.5 minutes after the recorded lease expiry. No split-brain occurred because the
  follower was wedged too, but a healthy follower could have taken leadership in that window.

## How this was measured

- **Worse-of-N across ABBA-ordered repetitions.** Throughput is the *minimum* across
  repetitions, latency the *maximum*. Cells run forward then backward so drift cancels rather
  than landing on whichever setting ran last. The run-to-run spread is quoted throughout, so
  cells that are not distinguishable can be seen not to be. Where only one repetition survived,
  the CSV says so in its `stat` column.
- **One connection pinned per writer task.** A load generator that returns connections to a
  shared pool lets them drift between tasks that own different aggregates, and the server then
  migrates the whole TCP stream to reach the right shard. That inflates p99 by up to 40× — 10.7 s
  against 258 ms at the same concurrency — and it is the harness, not the database. Any run
  without pinning is not comparable.
- **Workload:** `rpi_cluster_pool_bench`. One event per acknowledged write, no batching. Every
  write is `fdatasync()` + Direct I/O on both nodes' storage, replicated over mTLS with kTLS
  offload, acknowledged only after both succeed. XFS on io_uring.
- **What the harness rejects:** zero throughput, replication not established, load shedding, and
  missing load generators. That last one is there because a benchmark that silently loses two
  thirds of its clients reports a plausible number with zero errors and a *tight* spread, and
  every server-side health check passes. It presented once as convincing "server degradation" and
  survived a full data wipe before the real cause — spot-reclaimed client nodes — was found.

## Reproducing

```bash
# flagship
make infra PROFILE=i4i-metal DATA_NODES=2 CDK_ARGS="-c keyPair=<key> -c spotClients=false"
make deploy KEY_ARG="--key-file <key.pem>" TLS_MODE=strict
make start
CELLS_FILE=cells/published-ladder.txt REPS=3 SWEEP_TLS_MODE=strict bash scripts/run-cell-sweep.sh

# entry tier
make build-arm    # ~40 min cold under QEMU; cached afterwards
make infra CDK_ARGS="-c keyPair=<key> -c instanceType=c7g.xlarge -c storageType=ebs \
  -c ebsDataVolumeSize=100 -c ebsIops=3000 -c spot=false"
make certs && make deploy KEY_ARG="--key-file <key.pem>" && make start
make run-sweep SWEEP_LEVELS=1000,2000,4000,8000
```

The `i4i-metal` profile carries the measured windows and pins the AZ. **Choose the AZ by spot
placement score, not by price** — pinning to the cheapest AZ ($1.32/hr against $2.54) failed to
launch anything, because the low price signalled a thin market:

```bash
aws ec2 get-spot-placement-scores --instance-types i4i.metal --target-capacity 2 \
  --single-availability-zone --region-names ap-southeast-2
```

Spot vCPU quota must be ≥ 320 for a metal pair plus clients. gp3 *throughput* cannot be set from
CloudFormation — raise it after deploy with `aws ec2 modify-volume --throughput 1000`. Run
`make s3-fallback` before any teardown you might want to explain later: the CDK bucket is
`removalPolicy=DESTROY`, so teardown erases the fallback batches and lease history.

## What this does not tell you

- **One workload shape only** — writes, one event per acknowledged write. No reads, no mixed
  load, no large events.
- **Nothing here tests correctness under failure.** Every cell is a clean cluster. Replication
  catch-up, S3 offload and crash paths were not exercised, apart from the accidental 1-vCPU
  collapse described above.
- **Cross-class throughput comparisons are not supported.** The gp3 fsync sweep ran on a 16-vCPU
  box against metal's 128; only the within-class curves are comparable.
- **Single-AZ.** Each run is internally consistent, but the metal ladder and the 16xlarge
  comparison ran in different AZs because that is where spot capacity was.
