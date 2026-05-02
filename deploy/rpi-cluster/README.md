# RPi kTLS Cluster

Two RPi 5s run Celeriant (leader + follower) with NVMe storage. The infrastructure stack (MinIO, Prometheus, Loki, Grafana) runs via Docker on either a third Pi or the build machine — see [Infra location](#infra-location-rpi4-vs-build-machine) below.

```
Mode: INFRA_MODE=remote (default)

┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│  RPi 5 (cs1)     │   │  RPi 5 (cs2)     │   │  RPi 4 (cluster) │
│  Celeriant       │◄─►│  Celeriant       │   │  MinIO           │
│  Leader          │   │  Follower        │   │  Prometheus      │
│  NVMe + XFS      │   │  NVMe + XFS      │   │  Loki + Grafana  │
│  :10000 client   │   │  :10000 client   │   │  :9000 S3 API    │
│  :10001 repl     │   │  :10001 repl     │   │  :9001 console   │
│  :9090 metrics   │   │  :9090 metrics   │   │  :3000 grafana   │
└──────────────────┘   └──────────────────┘   └──────────────────┘
        │                      │                      │
        └──────────────────────┴──────────────────────┘
                        LAN (switch / direct)

Mode: INFRA_MODE=local

┌──────────────────┐   ┌──────────────────┐   ┌──────────────────────────┐
│  RPi 5 (cs1)     │   │  RPi 5 (cs2)     │   │  Build machine           │
│  Celeriant       │◄─►│  Celeriant       │   │  MinIO                   │
│  Leader          │   │  Follower        │   │  Prometheus              │
│  NVMe + XFS      │   │  NVMe + XFS      │   │  Loki + Grafana          │
│  :10000 client   │   │  :10000 client   │   │  :9000 S3 API            │
│  :10001 repl     │   │  :10001 repl     │   │  :9001 console           │
│  :9090 metrics   │   │  :9090 metrics   │   │  :3000 grafana           │
└──────────────────┘   └──────────────────┘   └──────────────────────────┘
        │                      │                      │
        └──────────────────────┴──────────────────────┘
                 LAN — cs1/cs2 reach build machine by LAN IP
```

## Infra location: rpi4 vs build machine

`INFRA_MODE=remote` (default): the infra stack runs on the rpi4 via SSH. Good for long-running or realistic tests — slower iteration but closer to production topology.

`INFRA_MODE=local`: the infra stack runs on the build machine via `docker compose`. Good for fast iteration — no Pi required for the infra side, and rebuilds skip the SSH deploy step.

### Requirements for local mode

- Docker on PATH (Docker Engine on Linux/macOS, Docker Desktop with WSL2 backend on Windows)
- Linux, macOS, or WSL2 only — native Windows (cmd.exe / PowerShell / Git Bash) is not supported
- `INFRA_HOST` must be the build machine's **LAN IP**, not `localhost`

The LAN IP requirement is not optional. cs1 and cs2 bake `S3_ENDPOINT_OVERRIDE=http://$INFRA_HOST:9000` into their systemd units during `make setup-nodes`. If `INFRA_HOST` is `localhost`, every S3 call from the RPi tries to reach itself — no deploy-time error, just silent S3 timeouts at runtime.

### config.env for each mode

Remote mode (infra on rpi4):

```sh
LEADER_HOST=192.168.88.214
FOLLOWER_HOST=192.168.88.213
INFRA_HOST=192.168.88.218    # rpi4 LAN IP
INFRA_MODE=remote
```

Local mode (infra on build machine):

```sh
LEADER_HOST=192.168.88.214
FOLLOWER_HOST=192.168.88.213
INFRA_HOST=192.168.88.100    # build machine LAN IP — NOT localhost
INFRA_MODE=local
```

`config.env` is the single source of truth for both `INFRA_MODE` and `INFRA_HOST`. Do not override them via environment variables — the systemd units on cs1/cs2 are generated from `config.env` and a mismatch causes silent failures.

The setup flow (see [Setup (first time)](#setup-first-time)) is the same for both modes.

### Switching infra location

1. Edit `config.env`: set `INFRA_MODE` and update `INFRA_HOST` to the new infra location.
2. `make setup-nodes` — regenerates the systemd units on cs1/cs2 with the new `INFRA_HOST` baked in.
3. `make setup-infra` — stands up the infra stack on the new location.

**To wipe both MinIO volumes before switching** (recommended for clean test runs):

```sh
make teardown-data          # wipes the currently active side
# edit config.env — flip INFRA_MODE and update INFRA_HOST
make teardown-data          # wipes the other side
```

Each `make teardown-data` invocation only wipes the side that `INFRA_MODE` currently points at. This is documented inline in the Makefile's `teardown-data` comment.

## Hardware

### Shopping list

| Qty | Item | Purpose |
|-----|------|---------|
| 2 | Raspberry Pi 5 (4 GB+ RAM) | Data nodes (Celeriant leader/follower) |
| 2 | NVMe M.2 SSD (any capacity) | WAL storage — must be on XFS with O_DIRECT |
| 2 | Pimoroni NVMe HAT for Pi 5 | Connects NVMe to Pi 5 via PCIe |
| 2 | microSD cards (32 GB+) | OS boot (Raspberry Pi OS Lite 64-bit) |
| 1 | Raspberry Pi 4+ (or any always-on node) | Infra node (MinIO, monitoring) — optional if using `INFRA_MODE=local` |
| 3 | USB-C power supplies (5V/5A for Pi 5, 5V/3A for Pi 4) | |
| 1 | Ethernet switch + 3 cables (or direct connections) | |

### OS setup

Flash **Raspberry Pi OS Lite (64-bit)** to each SD card using `rpi-imager`. During flashing:

1. Set a hostname (`cs1`, `cs2`, `cluster`)
2. Enable SSH with your public key
3. Set locale/timezone

Boot all Pis and verify SSH access using their IPs:

```sh
ssh <cs1-ip> 'uname -a'
ssh <cs2-ip> 'uname -a'
ssh <cluster-ip> 'uname -a'
```

**Important:** All deployment configs use IP addresses, not hostnames. RPi cloud-init maps all hostnames to `127.0.1.1` in `/etc/hosts`, which breaks inter-node communication. Update `config.env` with your actual IPs.

## Build machine prerequisites

The build machine (your laptop/desktop) needs:

- **Rust toolchain** with the `aarch64-unknown-linux-gnu` target
- **ARM64 cross-linker** (`aarch64-linux-gnu-gcc`)
- **OpenSSL CLI** (`openssl`) — used by `gen-certs.sh` to generate TLS certificates
- **SSH access** to all three Pis (key-based, no password prompts)
- **Docker** — required only if `INFRA_MODE=local` (Docker Engine on Linux/macOS, or Docker Desktop with WSL2 backend on Windows)

```sh
rustup target add aarch64-unknown-linux-gnu

# Ubuntu/Debian:
sudo apt install gcc-aarch64-linux-gnu

# macOS (Homebrew):
brew install messense/macos-cross-toolchains/aarch64-unknown-linux-gnu
```

## Configuration

Edit `config.env` to match your network:

```sh
# config.env
LEADER_HOST=192.168.88.214    # RPi 5 — Celeriant data node
FOLLOWER_HOST=192.168.88.213  # RPi 5 — Celeriant data node
INFRA_HOST=192.168.88.218     # RPi 4 — MinIO + monitoring (or build machine LAN IP for local mode)
INFRA_MODE=remote             # remote (rpi4) or local (build machine)
```

All other settings have sensible defaults. See the file for NVMe device paths, ports, and tuning parameters.

## Setup (first time)

All commands run from this directory (`deploy/rpi-cluster/`).

### Step 1: OS prep + NVMe + certs + infra (automated)

```sh
make setup-all
```

This runs four steps in sequence:

| Step | What it does |
|------|-------------|
| `setup-nodes` | Updates packages, sets file descriptor and memlock limits, installs xfsprogs, deploys the `celeriant` systemd service and Promtail (log shipper) to both data nodes. Runs in parallel on both Pis. |
| `setup-nvme` | Partitions and formats the NVMe drive as XFS, mounts it at `/var/lib/celeriant`, adds an fstab entry. **Destructive** — prompts for confirmation per node. Sequential. |
| `certs` | Generates two CA keypairs (client CA + intracluster CA), node certs, a client-facing server cert, and a benchmark client cert. Distributes to both data nodes. |
| `setup-infra` | In remote mode: installs Docker on the infra node (via SSH), deploys the compose stack. In local mode: runs `docker compose` on the build machine. Provisions Grafana dashboards in both cases. |

### Step 2: Build kTLS kernel

The stock Raspberry Pi OS kernel does not ship with `CONFIG_TLS=m`. This step clones the RPi kernel source, enables the kTLS module, and builds + installs the kernel on both data nodes.

```sh
make setup-ktls
```

The kernel branch is auto-detected from `uname -r` on each Pi (e.g. kernel `6.12.25-v8+` → branch `rpi-6.12.y`). Takes 30–60 minutes per node; both build in parallel.

After the build completes, **reboot both data nodes**:

```sh
ssh $LEADER_HOST 'sudo reboot now'
ssh $FOLLOWER_HOST 'sudo reboot now'
```

Verify kTLS is available after reboot:

```sh
ssh $LEADER_HOST 'sudo modprobe tls && lsmod | grep tls'
ssh $FOLLOWER_HOST 'sudo modprobe tls && lsmod | grep tls'
```

### Step 3: Build and deploy Celeriant

```sh
make deploy
```

This cross-compiles the binary for ARM64, copies it to both data nodes, and restarts the systemd services (leader first, then follower after a 5-second delay for S3 lease acquisition).

## Day-to-day commands

All via `make` from this directory:

| Command | Description |
|---------|-------------|
| `make deploy` | Build, ship, restart (the full cycle) |
| `make build` | Cross-compile only |
| `make ship` | Copy binary to nodes (no restart) |
| `make restart` | Stop + start Celeriant on both nodes |
| `make start` | Start services (leader first) |
| `make stop` | Stop services (follower first) |
| `make status` | Check if Celeriant is running on each node |
| `make logs` | Tail journald logs from both nodes |
| `make run-test` | Run the cluster benchmark (see below) |
| `make teardown` | Stop all services (Celeriant + infra) |
| `make teardown-data` | Stop everything and wipe all data (**destructive**) |

Run `make help` to see all targets.

## Running the benchmark

```sh
make run-test
```

This runs `rpi_cluster_bench` from the build machine against the live cluster. The test connects over mTLS, writes at maximum throughput, then measures tail latency under load.

Configuration (in `config.env`):

| Setting | Default | Description |
|---------|---------|-------------|
| `CLUSTER_THROUGHPUT_CONNECTIONS` | `8000` | Concurrent connections for throughput phase |
| `CLUSTER_LATENCY_CONNECTIONS` | `125` | Concurrent connections for latency phase |
| `CLUSTER_DURATION` | `15` | Duration of each phase in seconds |

The test uses the client certificates generated during setup (in `certs/`). Override with environment variables if needed:

```sh
CLUSTER_ADDRESS_1=$LEADER_HOST:10000 \
CLUSTER_THROUGHPUT_CONNECTIONS=4000 \
make run-test
```

## What to expect

Reference numbers from a 2-node RPi 5 cluster (Samsung 9100 PRO 1TB NVMe, Pimoroni NVMe HAT, Gigabit Ethernet), running `rpi_cluster_bench` for 360s per phase with mTLS enabled:

| Phase | Connections | Requests | Throughput | Avg | P50 | P95 | P99 | P99.9 |
|-------|-------------|----------|------------|-----|-----|-----|-----|-------|
| Throughput | 8,000 | 13.6M | 37,770 req/s | 211ms | 192ms | 312ms | 340ms | 377ms |
| Latency | 125 | 685K | 1,904 req/s | 65ms | 64ms | 70ms | 71ms | 82ms |

The throughput phase saturates the connection pool to find the ceiling. The latency phase uses fewer connections to show what tail latency looks like under moderate load. Your numbers will vary with NVMe model and network setup.

## Monitoring

Once the infra stack is running:

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | `http://$INFRA_HOST:3000` | admin / admin |
| Prometheus | `http://$INFRA_HOST:9090` | — |
| Loki | `http://$INFRA_HOST:3100` | — |
| MinIO Console | `http://$INFRA_HOST:9001` | minioadmin / minioadmin |
| MinIO API | `http://$INFRA_HOST:9000` | minioadmin / minioadmin |
| Celeriant metrics (leader) | `http://$LEADER_HOST:9090/metrics` | — |
| Celeriant metrics (follower) | `http://$FOLLOWER_HOST:9090/metrics` | — |

In remote mode, `$INFRA_HOST` is the rpi4's LAN IP. In local mode, `$INFRA_HOST` is the build machine's LAN IP — `http://localhost:3000` and `http://$INFRA_HOST:3000` both reach Grafana from the build machine itself, but the data nodes always use the LAN IP.

Grafana is pre-provisioned with Prometheus and Loki datasources. The cluster dashboard shows per-node and per-shard metrics. Use Explore → Loki to search server logs.

## TLS architecture

Two separate CAs enforce trust domain isolation:

```
Client CA                          Intracluster CA
    │                                     │
    ├── client-server.crt (port 10000)    ├── node.crt (port 10001)
    │   presented to clients              │   presented to peer nodes
    │                                     │   also used as client cert
    └── client.crt                        │   for outbound replication
        used by benchmark client          │
                                          └── (both nodes share the
                                               same node cert/key)
```

- Clients connecting to port 10000 verify the server against the **client CA** and present a client cert signed by the same CA (mTLS).
- Nodes connecting to each other on port 10001 verify against the **intracluster CA**. A compromised client cert cannot impersonate a node.

Certificates are generated with SANs covering both data node IPs, `localhost`, and `127.0.0.1`.

## Troubleshooting

**`Direct I/O verification failed`** — the NVMe is not mounted or is not formatted as XFS. Run `make setup-nvme` or check `lsblk` on the node.

**`kTLS module already loaded. Nothing to do.`** — the kernel already has `CONFIG_TLS`. `make setup-ktls` is idempotent; it skips the build if `modprobe tls` succeeds.

**`Kernel kTLS support check failed`** — the kTLS kernel module is not available. Reboot the node after `make setup-ktls`, or verify with `modprobe tls`.

**`Port 10000 is already in use`** — a previous Celeriant process is still running. Run `make stop` before `make start`.

**NVMe not detected** — check that the NVMe HAT is seated properly and the drive is recognised: `ssh $LEADER_HOST 'lsblk'`. The default device is `/dev/nvme0n1`.

**Leader election not happening** — verify MinIO is running and that both data nodes can reach it:

```sh
# Remote mode:
ssh $INFRA_HOST 'cd ~/celeriant-infra && docker compose ps'
# Local mode:
cd deploy/rpi-cluster && docker compose ps

# From a data node (both modes):
curl http://$INFRA_HOST:9000/minio/health/live
```

**Data nodes can't reach MinIO in local mode** — verify that `INFRA_HOST` in `config.env` is the build machine's LAN IP, not `localhost`. The systemd unit on each data node bakes `S3_ENDPOINT_OVERRIDE=http://$INFRA_HOST:9000` at setup time; if it resolves to `localhost` the data node tries to reach itself. Fix: update `INFRA_HOST` in `config.env` to the correct LAN IP, then re-run `make setup-nodes` followed by `make restart`. Also verify the host firewall allows inbound on ports 9000 (MinIO) and 3100 (Loki) from the data nodes.

**Clock synchronization warning** — Celeriant checks for NTP sync on startup. Ensure `systemd-timesyncd` or `chrony` is running: `timedatectl status`.

## File reference

| File | Purpose |
|------|---------|
| `config.env` | All cluster configuration (IPs, ports, paths, tuning, `INFRA_MODE`) |
| `Makefile` | Orchestrates all operations |
| `infra-exec.sh` | Helper that dispatches infra `docker compose` commands to the correct side based on `INFRA_MODE` |
| `setup-nodes.sh` | OS prep, systemd service, Promtail install (runs via SSH on each data node) |
| `setup-nvme.sh` | NVMe partition, XFS format, mount (runs via SSH, destructive) |
| `setup-ktls.sh` | Kernel rebuild with `CONFIG_TLS=m` (runs ON the Pi) |
| `setup-infra.sh` | Docker install, compose deploy on infra node (via SSH) or locally |
| `gen-certs.sh` | Generates dual-CA TLS certs and distributes to data nodes |
| `docker-compose.yml` | Infra stack: MinIO, Prometheus, Loki, Grafana |
| `docker-compose.local-override.yml` | Compose override for local mode (remaps prometheus bind mount to generated config) |
| `prometheus.yml` | Scrape config template for both Celeriant nodes |
| `certs/` | Generated certificates (gitignored) |
