# Local Development Cluster

Docker Compose stack that runs a 2-node Celeriant cluster with full observability on localhost. Builds from source using the repo root Dockerfile.

## What You Get

- Two Celeriant nodes with S3-based leader election
- MinIO for S3 (leader election + fallback replication)
- Prometheus scraping both nodes
- Loki + Promtail for log aggregation
- Grafana with a pre-provisioned cluster dashboard

## Quick Start

```bash
docker compose up -d --build
```

First build takes a while (full Rust release build). Subsequent rebuilds use Docker layer cache.

## Endpoints

| Service | URL |
|---------|-----|
| Node 1 (client) | localhost:10000 |
| Node 2 (client) | localhost:10002 |
| Node 1 metrics | localhost:19090/metrics |
| Node 2 metrics | localhost:29090/metrics |
| Grafana | localhost:3001 (admin/admin) |
| Grafana dashboard | localhost:3001/d/celeriant-cluster |
| Grafana logs | localhost:3001/explore |
| Prometheus | localhost:9091 |
| MinIO console | localhost:9101 (minioadmin/minioadmin) |

## Common Operations

```bash
# Rebuild and redeploy server only (keep observability running)
docker compose up -d --build celeriant-node-1 celeriant-node-2

# Tail server logs
docker compose logs -f celeriant-node-1 celeriant-node-2

# Stop everything (data preserved in volumes)
docker compose down

# Stop and wipe all data (fresh cluster)
docker compose down -v

# Override log level
CELERIANT_LOG_LEVEL=debug docker compose up -d --build
```

## Configuration

Nodes default to 4 shards, 20% memory, 128MB log segments. Override via environment variables in docker-compose.yml or on the command line:

```bash
CELERIANT_ROUTING_RULE=org_id docker compose up -d --build
```

TLS is disabled by default. Uncomment the TLS environment block and cert volume mounts in docker-compose.yml to enable.

## Requirements

Docker Desktop on Mac, Linux, or WSL2 on Windows. The containers run with `seccomp=unconfined` and unlimited memlock for io_uring support. On Mac and Windows, the Docker Linux VM handles this transparently.
