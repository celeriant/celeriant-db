# EC2 Performance Test Cluster

AWS CDK stack that deploys a Celeriant cluster for performance benchmarking
and kTLS testing. Mirrors the RPi LAN cluster (`deploy/rpi-cluster`) with a
Makefile-driven workflow, systemd services, and automated benchmark collection.

## Architecture

```mermaid
graph TB
    subgraph VPC["Default VPC — same AZ"]
        subgraph Leader["Leader (data node)"]
            L_CLIENT["Client port :10000<br/>mTLS (client CA)"]
            L_REPL["Replication port :10001<br/>mTLS (intracluster CA)"]
            L_METRICS["Metrics :9090"]
            L_STORE[("NVMe or EBS<br/>XFS → /var/lib/celeriant")]
        end

        subgraph Follower["Follower (data node)"]
            F_CLIENT["Client port :10000<br/>mTLS (client CA)"]
            F_REPL["Replication port :10001<br/>mTLS (intracluster CA)"]
            F_METRICS["Metrics :9090"]
            F_STORE[("NVMe or EBS<br/>XFS → /var/lib/celeriant")]
        end

        subgraph Client["Client (benchmark node)"]
            CLI["celeriant_cli"]
            BENCH["celeriant-integration-tests"]
        end

        L_REPL <-->|"kTLS"| F_REPL
        CLI -->|"mTLS"| L_CLIENT
        BENCH -->|"mTLS"| L_CLIENT
    end

    S3["AWS S3<br/>cluster coordination"]
    GC["Grafana Cloud<br/>metrics + logs"]

    Leader --> S3
    Follower --> S3
    L_METRICS -.->|"Alloy"| GC
    F_METRICS -.->|"Alloy"| GC
```

## Instance type flexibility

Use `-c instanceType=...` and `-c storageType=...` to match your budget:

| Use case | Instance | Storage | Approx. cost/hr (3 nodes) |
|---|---|---|---|
| Quick smoke test | `t3.xlarge` | `ebs` (100GB gp3) | ~$0.50 |
| Standard benchmark | `c6id.2xlarge` | `instance-store` (474GB NVMe) | ~$1.35 |
| High-core benchmark | `c6id.8xlarge` | `instance-store` (1x 950GB NVMe) | ~$5.40 |
| Max perf (32 core) | `c6id.16xlarge` | `instance-store` (2x 1900GB NVMe) | ~$10.80 |

**`instance-store`** (default): uses the instance's local NVMe — fast, ephemeral.
**`ebs`**: attaches a dedicated gp3 volume (size via `-c ebsDataVolumeSize=100`).

## Quick start

```bash
# 1. Build binaries (x86_64)
cd /home/utilitydelta/work/code/celeriant-db
cargo build --release -p celeriant -p celeriant_integration_tests -p celeriant_cli

# 2. Deploy infrastructure
cd deploy/ec2-cluster

# Cheap EBS instance for initial testing:
make infra CDK_ARGS="-c keyPair=my-key -c instanceType=t3.xlarge -c storageType=ebs"

# Or powerful NVMe instance for real benchmarks:
make infra CDK_ARGS="-c keyPair=my-key -c instanceType=c6id.2xlarge"

# 3. Generate certs and deploy everything
make certs
make deploy KEY_ARG="--key-file ~/.ssh/my-key.pem"

# 4. Start and benchmark
make start
make run-benchmark
make stop
```

## Structure

```
deploy/ec2-cluster/
├── Makefile                     # Orchestration (mirrors rpi-cluster)
├── bin/ec2-cluster.ts           # CDK app entry point
├── lib/ec2-cluster-stack.ts     # Stack: 3x EC2, S3, SG, IAM, user data
├── scripts/
│   ├── generate-certs.sh        # Dual-CA cert generation with IP SANs
│   ├── deploy.sh                # Deploys binaries, certs, env files, enables systemd
│   └── run-benchmark.sh         # Runs benchmark on client, collects results locally
├── certs/                       # Generated certs (gitignored)
├── results/                     # Benchmark results (gitignored)
├── .cluster-env                 # Cached IPs/config (gitignored, written by deploy.sh)
├── cdk.json
└── package.json
```

## Day-to-day commands

```bash
make help              # Show all targets
make infra             # Deploy CDK stack
make certs             # Generate TLS certificates
make deploy            # Deploy binaries, certs, env files
make start             # Start cluster (leader first)
make stop              # Stop cluster
make restart           # Stop then start
make status            # Check service status
make logs              # Tail logs from both nodes (Ctrl+C to stop)
make run-benchmark     # Run benchmark and save results
make sync-env          # Re-read CDK outputs into .cluster-env
make teardown          # Stop cluster + destroy CDK stack
make teardown-data     # Wipe data on data nodes
```

### Benchmark configuration

The benchmark runs `rpi_cluster_pool_bench` — a pool-based write benchmark with
automatic leader failover, connecting to both nodes (same test the RPi cluster uses).

Override defaults via Make variables:

```bash
make run-benchmark BENCH_TASKS=4000 BENCH_CONNS=4000 BENCH_DURATION=30
```

Results are saved to `results/<timestamp>_<instance-type>_<storage-type>.txt` with
metadata headers for easy comparison across instance types.

### CDK context overrides

| Flag | Default | Example |
|---|---|---|
| `instanceType` | `c6id.2xlarge` | `-c instanceType=t3.xlarge` |
| `storageType` | `instance-store` | `-c storageType=ebs` |
| `ebsDataVolumeSize` | `100` (GB) | `-c ebsDataVolumeSize=200` |
| `keyPair` | *(none, use SSM)* | `-c keyPair=my-key` |
| `grafanaPromUser` | | `-c grafanaPromUser=123456` |
| `grafanaPromUrl` | | `-c grafanaPromUrl=https://prometheus-prod-XX.grafana.net/api/prom/push` |
| `grafanaLokiUser` | | `-c grafanaLokiUser=123456` |
| `grafanaLokiUrl` | | `-c grafanaLokiUrl=https://logs-prod-XX.grafana.net/loki/api/v1/push` |
| `grafanaApiKey` | | `-c grafanaApiKey=glc_...` |

## Trust model (dual CA)

Same as rpi-cluster — two CAs isolate client and intracluster traffic:

- **Intracluster CA** signs `node.crt` (serverAuth + clientAuth) → presented on port 10001
- **Client CA** signs `client-server.crt` (serverAuth) → presented on port 10000
- **Client CA** signs `client.crt` (clientAuth) → used by benchmark client

A client cert cannot authenticate to the replication port.

## RPi comparison

| Aspect | RPi cluster | EC2 cluster |
|---|---|---|
| Nodes | 2x RPi 5 + RPi 4 infra | 2x EC2 data + 1x EC2 client |
| Storage | NVMe HAT | NVMe instance store or EBS gp3 |
| S3 | MinIO on infra node | AWS S3 |
| Monitoring | Self-hosted Grafana/Prometheus/Loki | Grafana Cloud (optional) |
| Network | LAN switch | Same AZ (LAN-equivalent) |
| Service mgmt | systemd | systemd |
| Orchestration | Makefile | Makefile |
| Build | Cross-compile ARM64 | Native x86_64 |
| Benchmark | `make run-test` (from dev machine) | `make run-benchmark` (from client EC2) |

## Testing

### Smoke test with CLI

```bash
ssh -i ~/.ssh/my-key.pem ec2-user@<client-public-ip>
export CELERIANT_SERVER=<leader-private-ip>:10000
export CELERIANT_TLS=true
export CELERIANT_CA_CERT=/etc/celeriant/certs/client-ca.crt
export CELERIANT_CLIENT_CERT=/etc/celeriant/certs/client.crt
export CELERIANT_CLIENT_KEY=/etc/celeriant/certs/client.key

celeriant_cli write --org 1 --type 1 --id 1 --event-type 1 --data '{"hello":"world"}' --allow-create
celeriant_cli read --org 1 --type 1 --id 1
```

### Logs and metrics

Without Grafana Cloud:
```bash
make logs                          # Tail both nodes
ssh ec2-user@<node> 'journalctl -u celeriant -n 100 --no-pager'
```

With Grafana Cloud (set `grafanaApiKey`, `grafanaPromUrl`, `grafanaLokiUrl`):
- Metrics: `{job="celeriant"}` in Prometheus
- Logs: `{unit="celeriant.service"}` in Loki

## Teardown

```bash
make teardown    # Stops services + destroys stack
```

NVMe instance store data is ephemeral. EBS data volumes are destroyed with the stack.
