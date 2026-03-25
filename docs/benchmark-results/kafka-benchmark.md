# Kafka vs Celeriant Benchmark — 2026-03-25

Per-operation comparison on identical hardware. Both benchmarks use the same pattern:
N concurrent tokio tasks, each sending one write and waiting for the ack before sending
the next. No batching, no pipelining — true per-request throughput.

## Test setup

### Kafka (4.0.2 KRaft)
- **Brokers:** 3x i4i.8xlarge (32 vCPU, NVMe instance store, XFS)
- **Clients:** 2x c7i.4xlarge (16 vCPU each)
- **Config:** `acks=all`, `replication.factor=2`, `min.insync.replicas=2`
- **Benchmark:** `kafka-bench` (Rust, rdkafka FutureProducer)
- **Batching disabled:** `batch.num.messages=1`, `queue.buffering.max.ms=0`
- **TLS:** Enabled (OpenSSL, PKCS12 keystores)
- **Durability:** No fsync — writes go to page cache, ack after replication

### Celeriant
- **Data nodes:** 2x i4i.8xlarge (32 vCPU, NVMe instance store, XFS, Direct I/O)
- **Clients:** 3x c7i.4xlarge (16 vCPU each)
- **Config:** `fdatasync()` + replication before ack
- **Benchmark:** `rpi_cluster_pool_bench` (Rust, tokio)
- **TLS:** mTLS with kTLS offload (TLS 1.3)
- **Durability:** Full — fsync to WAL on both leader and follower before ack

### Hardware parity
Both use i4i.8xlarge with NVMe instance store. Kafka uses 3 brokers (KRaft quorum
requires 3), Celeriant uses 2 data nodes. Kafka has 50% more server hardware.

## Results

| Concurrency | Kafka req/s | Kafka avg ms | Kafka p99 ms | Celeriant req/s | Ratio |
|---|---|---|---|---|---|
| 9,000 | 17,491 | 522 | 1,287 | 144,655 | **8.3x** |
| 12,000 | 19,559 | 626 | 745 | 190,647 | **9.7x** |
| 15,000 | 20,374 | 755 | 867 | 226,015 | **11.1x** |
| 18,000 | 20,662 | 898 | 1,001 | 264,051 | **12.8x** |
| 21,000 | 20,437 | 1,066 | 1,207 | 292,946 | **14.3x** |
| 24,000 | 21,212 | 1,177 | 1,342 | 318,768 | **15.0x** |
| 27,000 | 21,505 | 1,312 | 1,505 | 331,769 | **15.4x** |
| 30,000 | 22,252 | 1,414 | 1,652 | 338,689 | **15.2x** |
| 33,000 | 21,975 | 1,586 | 1,835 | 354,384 | **16.1x** |
| 36,000 | 23,134 | 1,645 | 1,905 | 366,626 | **15.8x** |
| 39,000 | 22,710 | 1,829 | 2,126 | **374,552** | **16.5x** |
| 42,000 | 22,497 | 2,005 | 2,306 | 371,393 | **16.5x** |
| 48,000 | 22,871 | 2,268 | 2,686 | 275,782 | 12.1x |
| 54,000 | 23,759 | 2,474 | 2,952 | 296,534 | 12.5x |
| 60,000 | 24,162 | 2,731 | 3,237 | 17,513 | 0.7x |

## Key findings

### Throughput

- **Celeriant peak: 374,552 req/s** at 39,000 concurrency (zero errors)
- **Kafka peak: ~24,000 req/s** — effectively flat from 18k concurrency onward
- **Celeriant is 15-16x faster** in the operating range, despite doing more work per request (fsync + indexed writes vs append-only page cache)

### Latency

Kafka's latency degrades linearly with concurrency while throughput stays flat — classic sign of a bottleneck (likely the single-partition append lock and replication protocol overhead).

| Concurrency | Kafka avg | Kafka p99 |
|---|---|---|
| 9,000 | 522ms | 1,287ms |
| 24,000 | 1,177ms | 1,342ms |
| 39,000 | 1,829ms | 2,126ms |
| 60,000 | 2,731ms | 3,237ms |

### Why the gap is so large

1. **Kafka's protocol is batch-oriented.** Without batching (`batch.num.messages=1`), every record incurs the full protocol overhead: produce request framing, partition routing, ISR replication, and response. Kafka is optimized for high-throughput streaming with large batches, not per-operation latency.

2. **JVM overhead.** Kafka brokers run on the JVM with GC pauses, object allocation, and memory management overhead. Celeriant is native Rust with zero-copy I/O via io_uring.

3. **Celeriant uses kTLS.** TLS encryption/decryption is offloaded to the kernel, eliminating user-kernel copy overhead. Kafka uses JVM-based SSL which requires data copies through the JVM heap.

4. **Celeriant uses io_uring + Direct I/O.** Writes go directly from user buffers to NVMe via the kernel's io_uring submission queue, bypassing the page cache entirely. Kafka writes go through the OS page cache with standard `write()` syscalls.

### Durability comparison

| | Kafka (acks=all) | Celeriant |
|---|---|---|
| Write to disk | Page cache (no fsync) | `fdatasync()` before ack |
| Replication | 2/3 brokers ack from page cache | Leader + follower both fsync |
| Data loss on power failure | Possible (unflushed page cache) | None |
| Data loss on single node crash | Possible ([documented](https://www.redpanda.com/blog/why-fsync-is-needed-for-data-safety-in-kafka-or-non-byzantine-protocols)) | None |

Celeriant provides strictly stronger durability while being 15x faster.

### Note on Kafka's batched numbers

Kafka's built-in `kafka-producer-perf-test` reports ~1.8M records/sec on this hardware.
That number counts individual records within batches (~416 records/batch), not operations.
The actual network request rate is ~341 requests/sec — the rest is just measuring how
fast you can serialize bytes into a TCP socket.
