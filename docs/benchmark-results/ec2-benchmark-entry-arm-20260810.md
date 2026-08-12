# Entry tier on EBS: a Raspberry Pi 5 equivalent, and one core below it

What the cheapest sensible EBS-backed ARM cluster does against the RPi 5 LAN cluster in
`rpi-benchmark.md`, plus a 1-vCPU run to find the floor.

- **Date:** 2026-08-10
- **Pipeline:** current `main`. Pinned-connection load generator (`BENCH_PINNED=1`).
- **Storage:** one gp3 volume per node, 100 GB at **baseline 3,000 IOPS / 125 MB/s** — the
  cheapest spec, deliberately not provisioned up.
- **Instances:** on-demand (spot placement scores for 4-vCPU ARM were 1 in every AZ).
- **Client:** 1× c6g.2xlarge. Same AZ, `ap-southeast-2a`.

## Why c6g

The Pi 5 is 4× Cortex-A76 at 2.4 GHz with 8 GB. `c6g.xlarge` is 4× Neoverse N1 with 8 GB —
and N1 is the server derivative of A76, so this is the closest thing EC2 sells. It is also the
cheapest non-burstable 4-core ARM option ($0.0873/hr spot, ~$0.171/hr on-demand). `t4g` is
cheaper but burstable, which would throttle mid-sweep and invalidate the run.

## c6g.xlarge (4 vCPU, EBS) vs RPi 5

| Concurrency | c6g.xlarge | p99 | RPi 5 | p99 | Δ req/s |
|---|---|---|---|---|---|
| 250 | 5,334 | 49 | 3,726 | 73 | +43% |
| 500 | 9,934 | 53 | 7,166 | 79 | +39% |
| 1,000 | 17,437 | 60 | 12,658 | 93 | +38% |
| 1,500 | 23,774 | 69 | 17,621 | 101 | +35% |
| 2,000 | 29,292 | 89 | 21,358 | 109 | +37% |
| 3,000 | 36,768 | 113 | 26,772 | 140 | +37% |
| 4,000 | 42,149 | 153 | 27,748 | 185 | +52% |
| 5,000 | 43,592 | 158 | 31,084 | 219 | +40% |
| 6,000 | 41,925 ⚠ | 170 | 34,202 | 263 | +23% |
| 8,000 | **46,715** | 199 | 34,632 | 375 | +35% |
| 10,000 | 46,215 | 280 | **35,382** | 462 | +31% |
| 12,000 | 41,147 ⚠ | 281 | 10,530 ⚠ | 7,912 | — |

⚠ = errors at that level (6,000: 3,096; 12,000: 12,000 — exactly one per task, so a single
fence hitting every task once). 8,000 and 10,000 ran completely clean, so **6,000 is not a
stability threshold** — that fence was transient.

**Peak 46,715/s at 8,000 concurrency, p99 199 ms** vs the Pi's 35,382 at 10,000, p99 462 ms:
+32% throughput at 57% of the tail latency. It beats the Pi at every level, and already
exceeds the Pi's whole-sweep peak by 3,000 concurrency.

### It is waiting on EBS, not computing

Mid-sweep the leader sat at **0% idle but 62% iowait**, 18% user and 15% system, while the
volume drew ~307 write IOPS of 3,000 provisioned at 26% util. Provisioning more IOPS would
buy nothing — the cost is EBS round-trip latency against a per-write `fdatasync()`. Only a
lower-latency device (io2, or local NVMe) moves this number.

That also means the four cores are mostly idle-waiting, which is why dropping to one core
costs far less than 4× (below).

## c6g.medium (1 vCPU, 1.8 GB) — the floor

Single vCPU means **a single shard**, so cross-shard routing does not exist and the
connection-pinning question is structurally absent.

| Concurrency | 1 vCPU | p99 | errors | vs RPi 5 |
|---|---|---|---|---|
| 250 | 4,355 | 63 | 0 | **+17%** |
| 500 | 7,368 | 79 | 0 | +3% |
| 1,000 | 10,560 | 157 | 0 | −17% |
| 1,500 | 12,068 | 187 | 0 | −32% |
| 2,000 | 11,973 | 263 | 0 | −44% |
| 3,000 | **12,289** | 284 | 0 | −54% |
| 4,000 | 11,972 | 349 | 0 | −57% |
| 5,000 | 7,406 | 307 | 7,691 | — |
| 6,000 | 631 | 1,018 | 8,454 | — |
| 8,000+ | **0** | — | dead | — |

One core beats the 4-core Pi 5 at 250 concurrency and matches it at 500. Past that the Pi
wins. Peak 12,289/s is 35% of the Pi's peak — consistent with a box that is genuinely
compute-bound here (33% user + 26% system, only 38% iowait, the inverse of the 4-core mix).

### It does not degrade gracefully — it wedges

Above 4,000 concurrency the 1-vCPU cluster fails hard, and **does not recover when load is
removed**:

- Throughput to zero from 8,000 onward.
- Both nodes stopped answering SSH entirely ("connection timed out during banner exchange"),
  while EC2 status checks still passed.
- CloudWatch shows the leader pinned at **99.5–99.7% CPU for 15+ minutes after the last
  client disconnected**. Spinning, not working.

The Pi 5 by comparison degraded gracefully at 12,000 — 10,530/s with 1,157 errors, still up
and manageable. Recorded as an open robustness issue: on a single core, an overloaded node can
enter a non-recovering 100%-CPU state. Multi-core boxes have spare capacity that masks it.

## Event batches offloaded to S3 during the collapse

The degraded-mode path engaged and behaved correctly. Under `cluster/fallback/shard_000/`:

- **12 batches, 15.75 MB**, written 09:49:30–09:55:32, all from node `59007b36`.
- WAL indices **1,170,058 → 1,184,520, fully contiguous — no gaps**.

So when the follower stopped answering, the leader offloaded batches to S3 to hold the
durability contract, and lost no range doing it. The safety mechanism worked; capacity is what
failed. Objects preserved in `deploy/ec2-cluster/results/20260810-1vcpu-s3-fallback/`.

### One thing to verify

`lease.json` records epoch 2 acquired 09:50:32, expiring 09:51:02. A fallback batch was
written at **09:55:32 — 4.5 minutes after that lease expired**, and the lease was never
renewed after. There is a `celeriant_s3_fallback_lease_unconfirmed_total` counter for this
path, so it is anticipated rather than unknown, and no split-brain occurred here because the
follower was wedged too. But a healthy follower could have taken leadership in that window,
which would make that batch a stale-writer artifact. Worth confirming the fencing rule covers
it.

> **Teardown erases this evidence.** The CDK bucket is `removalPolicy=DESTROY` +
> `autoDeleteObjects`, so `make teardown` deletes all fallback batches and lease history.
> Run `make s3-fallback` (added for this) before destroying anything you want to explain.

## Cost

| Cluster | Instances | Storage | On-demand |
|---|---|---|---|
| c6g.xlarge pair | ~$0.342/hr | 2× 100 GB gp3, ~$19/mo | **~$250/mo + $19 = ~$269/mo** |
| c6g.medium pair | ~$0.086/hr | 2× 100 GB gp3, ~$19/mo | **~$63/mo + $19 = ~$82/mo** |

The c6g.xlarge pair is the sensible EBS entry tier: ~$269/mo for 46,715 durable replicated
writes/s at p99 199 ms, beating the Pi 5 cluster by a third, with storage that survives
stop/start. The 1-vCPU pair is a curiosity — real throughput for $82/mo, but it wedges under
overload rather than degrading.

## Reproduce

```bash
make build-arm   # ~40 min cold under QEMU; cached afterwards
make infra CDK_ARGS="-c keyPair=my-key -c instanceType=c6g.xlarge \
  -c clientInstanceType=c6g.2xlarge -c clientCount=1 -c az=ap-southeast-2a \
  -c storageType=ebs -c ebsDataVolumeSize=100 -c ebsIops=3000 -c spot=false"
make certs && make deploy KEY_ARG="--key-file ~/.ssh/id_rsa" && make start
make run-sweep SWEEP_LEVELS=250,500,1000,1500,2000,3000,4000,5000,6000,8000,10000,12000
make s3-fallback    # BEFORE teardown
```
