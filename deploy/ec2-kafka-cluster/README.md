# Kafka KRaft Benchmark Cluster (EC2)

3-node Kafka 4.0.2 KRaft cluster for benchmark comparison against Celeriant.

## Architecture

- **3 Kafka brokers** — KRaft mode (combined controller+broker, no ZooKeeper)
- **2 client nodes** — running `kafka-bench` (Rust binary, mirrors Celeriant's benchmark)
- **TLS enabled by default** (disable with `--no-tls` in deploy)
- Same instance types as the Celeriant ec2-cluster for hardware parity

## Benchmark approach

The `kafka-bench` binary (`kafka_bench/` crate) is a 1-for-1 mirror of Celeriant's
`rpi_cluster_pool_bench`: N concurrent tokio tasks, each doing synchronous
produce-wait-for-ack in a tight loop, measuring per-request latency.

**Critical settings for fair comparison:**
- `batch.num.messages=1` — no batching (each record = one network round-trip)
- `queue.buffering.max.ms=0` — no linger
- `acks=all` — wait for all in-sync replicas to ack
- `replication.factor=2`, `min.insync.replicas=2`

Without these, Kafka's built-in `kafka-producer-perf-test` batches ~416 records per
request and reports the inflated per-record count as "throughput". That measures
network bandwidth, not per-operation performance.

### Durability note

Kafka does **not** fsync to disk before acknowledging writes, even with `acks=all`.
It relies on replication across brokers for durability. Celeriant fsyncs every write
to the WAL before ack. This is a fundamental difference — see
[Redpanda's analysis](https://www.redpanda.com/blog/why-fsync-is-needed-for-data-safety-in-kafka-or-non-byzantine-protocols).

## Quick start

```bash
cd deploy/ec2-kafka-cluster

# 1. Build the kafka-bench binary (Docker, amazonlinux:2023)
make build

# 2. Deploy infrastructure
make infra CDK_ARGS="-c keyPair=my-key -c instanceType=i4i.8xlarge"

# 3. Generate TLS certs
make certs

# 4. Deploy Kafka + configs + certs + kafka-bench binary
make deploy KEY_ARG="--key-file ~/.ssh/id_rsa"

# 5. Start the cluster
make start

# 6. Run benchmark (mirrors Celeriant's run-benchmark exactly)
make run-kafka-bench
make run-kafka-bench BENCH_TASKS=16000 BENCH_DURATION=30

# 7. Teardown
make teardown
```

## Makefile targets

| Target | Description |
|--------|-------------|
| `build` | Build kafka-bench binary in Docker (amazonlinux:2023) |
| `infra` | Deploy CDK stack |
| `certs` | Generate TLS certs and keystores |
| `deploy` | Deploy Kafka config, certs, kafka-bench binary, format KRaft storage |
| `start` | Start Kafka on all brokers |
| `stop` | Stop Kafka on all brokers |
| `restart` | Stop then start |
| `status` | Show service status |
| `logs` | Tail logs from all brokers |
| `run-kafka-bench` | Run kafka-bench (Rust, mirrors Celeriant benchmark) |
| `run-benchmark` | Run kafka-producer-perf-test (Kafka's built-in, batched) |
| `sync-env` | Re-read CDK outputs into .cluster-env |
| `teardown` | Stop + destroy CDK stack |
| `teardown-data` | Wipe Kafka data (keeps infra) |

## Benchmark overrides

```bash
make run-kafka-bench BENCH_TASKS=16000 BENCH_DURATION=30
```

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCH_TASKS` | `8000` | Total concurrent writer tasks (split across clients) |
| `BENCH_DURATION` | `15` | Test duration in seconds |
| `BENCH_RECORD_SIZE` | `256` | Bytes per record |

## CDK context overrides

| Flag | Default | Example |
|------|---------|---------|
| `instanceType` | `i4i.8xlarge` | `-c instanceType=i4i.4xlarge` |
| `clientInstanceType` | `c7i.4xlarge` | `-c clientInstanceType=c7i.8xlarge` |
| `clientCount` | `2` | `-c clientCount=3` (max 4) |
| `storageType` | `instance-store` | `-c storageType=ebs` |
| `keyPair` | *(none)* | `-c keyPair=my-key` |
| `kafkaVersion` | `4.0.2` | `-c kafkaVersion=4.1.2` |
