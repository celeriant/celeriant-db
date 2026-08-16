# Kafka Benchmark — 2026-03-25

Per-operation throughput for Kafka on i4i.8xlarge. N concurrent tokio tasks, each sending
one write and waiting for the ack before sending the next. No batching, no pipelining —
true per-request throughput.

## Test setup

### Kafka (4.0.2 KRaft)
- **Brokers:** 3x i4i.8xlarge (32 vCPU, NVMe instance store, XFS) — KRaft quorum requires 3
- **Clients:** 2x c7i.4xlarge (16 vCPU each)
- **Config:** `acks=all`, `replication.factor=2`, `min.insync.replicas=2`
- **Benchmark:** `kafka-bench` (Rust, rdkafka FutureProducer)
- **Batching disabled:** `batch.num.messages=1`, `queue.buffering.max.ms=0`
- **TLS:** Enabled (OpenSSL, PKCS12 keystores)
- **Durability:** No fsync — writes go to page cache, ack after replication

## Results

| Concurrency | Kafka req/s | Kafka avg ms | Kafka p99 ms |
|---|---|---|---|
| 9,000 | 17,491 | 522 | 1,287 |
| 12,000 | 19,559 | 626 | 745 |
| 15,000 | 20,374 | 755 | 867 |
| 18,000 | 20,662 | 898 | 1,001 |
| 21,000 | 20,437 | 1,066 | 1,207 |
| 24,000 | 21,212 | 1,177 | 1,342 |
| 27,000 | 21,505 | 1,312 | 1,505 |
| 30,000 | 22,252 | 1,414 | 1,652 |
| 33,000 | 21,975 | 1,586 | 1,835 |
| 36,000 | 23,134 | 1,645 | 1,905 |
| 39,000 | 22,710 | 1,829 | 2,126 |
| 42,000 | 22,497 | 2,005 | 2,306 |
| 48,000 | 22,871 | 2,268 | 2,686 |
| 54,000 | 23,759 | 2,474 | 2,952 |
| 60,000 | **24,162** | 2,731 | 3,237 |

## Key findings

### Throughput

- **Kafka peak: 24,162 req/s** at 60,000 concurrency — effectively flat from 18k onward
- For reference, Celeriant on the same hardware peaks at **446,667 req/s** at 24,000 concurrency
  (2 data nodes, fsync on both before ack). See `ec2-benchmark-metal-20260813.md`.

### Latency

Kafka's latency degrades linearly with concurrency while throughput stays flat — classic sign of a bottleneck (likely the single-partition append lock and replication protocol overhead).

| Concurrency | Kafka avg | Kafka p99 |
|---|---|---|
| 9,000 | 522ms | 1,287ms |
| 24,000 | 1,177ms | 1,342ms |
| 39,000 | 1,829ms | 2,126ms |
| 60,000 | 2,731ms | 3,237ms |

### Why per-operation throughput is so low

1. **Kafka's protocol is batch-oriented.** Without batching (`batch.num.messages=1`), every record incurs the full protocol overhead: produce request framing, partition routing, ISR replication, and response. Kafka is optimized for high-throughput streaming with large batches, not per-operation latency.

2. **JVM overhead.** Kafka brokers run on the JVM with GC pauses, object allocation, and memory management overhead. TLS goes through JVM-based SSL, which requires data copies through the heap.

3. **Page-cache writes through standard `write()` syscalls.** No Direct I/O, no io_uring submission path.

### Durability

| | Kafka (acks=all) |
|---|---|
| Write to disk | Page cache (no fsync) |
| Replication | 2/3 brokers ack from page cache |
| Data loss on power failure | Possible (unflushed page cache) |
| Data loss on single node crash | Possible ([documented](https://www.redpanda.com/blog/why-fsync-is-needed-for-data-safety-in-kafka-or-non-byzantine-protocols)) |

`acks=all` acknowledges once replicas have the record in page cache, not on disk. Correlated power loss across the ISR loses acknowledged writes.

### Note on Kafka's batched numbers

Kafka's built-in `kafka-producer-perf-test` reports ~1.8M records/sec on this hardware.
That number counts individual records within batches (~416 records/batch), not operations.
The actual network request rate is ~341 requests/sec — the rest is just measuring how
fast you can serialize bytes into a TCP socket.
