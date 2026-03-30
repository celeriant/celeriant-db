# RPi kTLS Cluster

A 3-node Raspberry Pi cluster for testing Celeriant with kernel TLS (kTLS) on real hardware. Two RPi 5s run Celeriant (leader + follower) with NVMe storage; a third Pi runs the infrastructure stack (MinIO, Prometheus, Loki, Grafana).

```
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│  RPi 5 (cs1)     │   │  RPi 5 (cs2)     │   │  RPi 4 (infra)   │
│  10.0.0.50       │   │  10.0.0.51       │   │  10.0.0.52       │
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
```

## Hardware

### Shopping list

| Qty | Item | Purpose |
|-----|------|---------|
| 2 | Raspberry Pi 5 (4 GB+ RAM) | Data nodes (Celeriant leader/follower) |
| 1 | Raspberry Pi 4 or 5 (any RAM) | Infrastructure node (MinIO, monitoring) |
| 2 | NVMe M.2 SSD (any capacity) | WAL storage — must be on XFS with O_DIRECT |
| 2 | NVMe HAT / base board for Pi 5 | Connects NVMe to Pi 5 via PCIe |
| 3 | microSD cards (32 GB+) | OS boot (Raspberry Pi OS Lite 64-bit) |
| 3 | USB-C power supplies (5V/5A for Pi 5, 5V/3A for Pi 4) | |
| 1 | Ethernet switch + 4 cables (or direct connections) | |

The infra node does not need NVMe — MinIO stores to its SD card or a USB drive, which is fine for development.

### OS setup

Flash **Raspberry Pi OS Lite (64-bit)** to each SD card using `rpi-imager`. During flashing:

1. Set a hostname (e.g. `cs1`, `cs2`, `cluster`)
2. Enable SSH with your public key
3. Set locale/timezone

Boot all three Pis and verify SSH access using their static IPs:

```sh
ssh 10.0.0.50 'uname -a'   # cs1
ssh 10.0.0.51 'uname -a'   # cs2
ssh 10.0.0.52 'uname -a'   # infra
```

**Important:** All deployment configs use static IP addresses, not hostnames. RPi cloud-init maps all hostnames to `127.0.1.1` in `/etc/hosts`, which breaks inter-node communication. Update `config.env` if your IPs differ from the defaults.

## Build machine prerequisites

The build machine (your laptop/desktop) needs:

- **Rust toolchain** with the `aarch64-unknown-linux-gnu` target
- **ARM64 cross-linker** (`aarch64-linux-gnu-gcc`)
- **OpenSSL CLI** (`openssl`) — used by `gen-certs.sh` to generate TLS certificates
- **SSH access** to all three Pis (key-based, no password prompts)

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
LEADER_HOST=10.0.0.50    # RPi 5 — will be Celeriant leader
FOLLOWER_HOST=10.0.0.51  # RPi 5 — will be Celeriant follower
INFRA_HOST=10.0.0.52     # RPi 4 — MinIO + monitoring
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
| `setup-infra` | Installs Docker on the infra node, deploys the compose stack (MinIO, Prometheus, Loki, Grafana), and provisions Grafana dashboards. |

### Step 2: Build kTLS kernel

The stock Raspberry Pi OS kernel does not ship with `CONFIG_TLS=m`. This step clones the RPi kernel source, enables the kTLS module, and builds + installs the kernel on both data nodes.

```sh
make setup-ktls
```

The kernel branch is auto-detected from `uname -r` on each Pi (e.g. kernel `6.12.25-v8+` → branch `rpi-6.12.y`). Takes 30–60 minutes per node; both build in parallel.

After the build completes, **reboot both data nodes**:

```sh
ssh 10.0.0.50 'sudo reboot now'
ssh 10.0.0.51 'sudo reboot now'
```

Verify kTLS is available after reboot:

```sh
ssh 10.0.0.50 'sudo modprobe tls && lsmod | grep tls'
ssh 10.0.0.51 'sudo modprobe tls && lsmod | grep tls'
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
CLUSTER_ADDRESS_1=10.0.0.50:10000 \
CLUSTER_THROUGHPUT_CONNECTIONS=4000 \
make run-test
```

## Monitoring

Once the infra stack is running:

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | `http://10.0.0.52:3000` | admin / admin |
| Prometheus | `http://10.0.0.52:9090` | — |
| Loki | `http://10.0.0.52:3100` | — |
| MinIO Console | `http://10.0.0.52:9001` | minioadmin / minioadmin |
| MinIO API | `http://10.0.0.52:9000` | minioadmin / minioadmin |
| Celeriant metrics (leader) | `http://10.0.0.50:9090/metrics` | — |
| Celeriant metrics (follower) | `http://10.0.0.51:9090/metrics` | — |

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

**NVMe not detected** — check that the NVMe HAT is seated properly and the drive is recognised: `ssh 10.0.0.50 'lsblk'`. The default device is `/dev/nvme0n1`.

**Leader election not happening** — verify MinIO is running (`ssh 10.0.0.52 'docker compose -f ~/celeriant-infra/docker-compose.yml ps'`) and that both data nodes can reach it (`curl http://10.0.0.52:9000/minio/health/live` from a data node).

**Clock synchronization warning** — Celeriant checks for NTP sync on startup. Ensure `systemd-timesyncd` or `chrony` is running: `timedatectl status`.

## File reference

| File | Purpose |
|------|---------|
| `config.env` | All cluster configuration (IPs, ports, paths, tuning) |
| `Makefile` | Orchestrates all operations |
| `setup-nodes.sh` | OS prep, systemd service, Promtail install (runs via SSH on each data node) |
| `setup-nvme.sh` | NVMe partition, XFS format, mount (runs via SSH, destructive) |
| `setup-ktls.sh` | Kernel rebuild with `CONFIG_TLS=m` (runs ON the Pi) |
| `setup-infra.sh` | Docker install, compose deploy on infra node |
| `gen-certs.sh` | Generates dual-CA TLS certs and distributes to data nodes |
| `docker-compose.yml` | Infra stack: MinIO, Prometheus, Loki, Grafana |
| `prometheus.yml` | Scrape config for both Celeriant nodes |
| `certs/` | Generated certificates (gitignored) |