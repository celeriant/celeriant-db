# Celeriant Chaos

Chaos test orchestrator for Celeriant clusters. Drives a real two-node deployment (RPi or EC2) through a suite of named failure scenarios — process kills, network partitions, S3 outages, clock skew, disk-full — while a metric scraper records each node at 2 Hz and the bench drives sustained client load. Each scenario evaluates a tunable set of invariants (split-brain ticks, role flips, S3 fallbacks, WAL convergence, throughput floor, etc.) and writes a JSON + Markdown report under `<deploy>/runs/<timestamp>/`.

Unlike `celeriant_integration_tests`, which spawns server subprocesses on the local host, chaos runs against an **already-deployed cluster**. All cluster actions (start/stop/kill/partition/skew clock) are SSH-driven via the deploy directory's Makefile — the chaos binary itself does not touch the nodes directly.

## Prerequisites

- A provisioned, reachable cluster with a working deploy directory:
  - `config.env` (rpi convention) **or** `.cluster-env` (ec2 CDK output)
  - `Makefile` exposing `teardown-data`, `start-infra`, `start-cs1`, `start-cs2`, `stop-cs1`, `stop-cs2`, `kill-cs1`, `kill-cs2`, `stop`, plus partition / clock / disk targets used by the relevant scenarios
  - `certs/{client-ca.crt, client.crt, client.key}` for the bench mTLS client
- Passwordless SSH from the orchestrator host to every node referenced by `LEADER_HOST`, `FOLLOWER_HOST`, and (if set) `INFRA_HOST`
- Cluster nodes already provisioned: see `deploy/rpi-cluster/setup-*.sh` and `deploy/ec2-cluster/`

## Running

```bash
# Default: just the baseline scenario against deploy/rpi-cluster
cargo run --release -p celeriant_chaos

# Full suite (every scenario in order)
cargo run --release -p celeriant_chaos -- --full

# A specific scenario by name
cargo run --release -p celeriant_chaos -- --scenario leader_sigkill

# Tune the bench window and throughput floor
cargo run --release -p celeriant_chaos -- \
  --tasks 4000 --duration 60 --throughput-floor 500

# Point at a different deploy directory (relative paths resolve from the workspace root)
cargo run --release -p celeriant_chaos -- --deploy-dir deploy/ec2-cluster --full
```

Each invocation creates a new run directory under `<deploy_dir>/runs/<timestamp>/` containing one `<scenario>.json` per scenario plus a top-level `report.md` summary. On any scenario failure, `journalctl` from both nodes covering the run window is fetched into that directory.

## Soak mode

`--soak <seconds>` repeats the scenario set in a loop until the deadline passes. Each iteration is its own run directory.

```bash
# 24 hours of full-suite, abort on the first failing iteration
cargo run --release -p celeriant_chaos -- --full --soak 86400

# Same but keep going past failures (record them and move on)
cargo run --release -p celeriant_chaos -- --full --soak 86400 --soak-continue-on-failure
```

## Scenarios

Scenarios live in `src/scenario.rs`. Each one performs a clean teardown, brings up infra and both nodes, waits for a stable leader, runs the bench while applying its specific perturbation, then evaluates invariants over the bench window.

| Scenario | What it does |
|---|---|
| `baseline` | No chaos. Strict zero-tolerance — any role flip, election, S3 fallback, or rollback fails the run. |
| `follower_graceful_stop` | `systemctl stop` the follower mid-bench, then restart. Leader must retain leadership; follower must catch up before tear-down. |
| `follower_sigkill` | SIGKILL the follower mid-bench, then restart. |
| `leader_graceful_stop` | `systemctl stop` the leader mid-bench. Follower must promote and serve writes. |
| `leader_sigkill` | SIGKILL the leader mid-bench. |
| `leader_restart_loop` | Repeatedly kill+restart the leader. At least N distinct hosts must have held leadership. |
| `partition_leader_follower_replication` | iptables DROP on the replication port between leader and follower. Forces S3-fallback replication. |
| `partition_leader_minio` | Block leader → S3 (rpi: MinIO on infra host). |
| `partition_asymmetric` | One-directional drop. |
| `network_flap` | Repeatedly partition + heal. |
| `minio_outage_short` / `minio_outage_long` | `docker compose stop minio` then start. Tests S3 unavailability tolerance. |
| `partition_then_kill_minio` | Partition first, then kill MinIO. Exercises Mode B divergence recovery. |
| `rolling_restart` | Stop one node, wait for catchup, stop the other. Zero downtime if it works. |
| `sigstop_leader` | SIGSTOP the leader (frozen, not killed). Tests heartbeat-based eviction. |
| `clock_skew_follower` | Shift the follower's system clock. Pairs with `RestoreClock`. |
| `follower_disk_full` | `fallocate` the follower's data disk to within a small reserve. |
| `bench_load_sweep` | No chaos — sweeps `--tasks` to find the throughput knee on this hardware. |

## Invariants

Each scenario supplies a `ScenarioExpectations` (see `src/invariants.rs`) declaring the maximum tolerated counter deltas across the bench window plus optional checks like `EventualConvergence`, `LeaderRetained`, `FinalLeaderWroteDuringBench`, and `DistinctLeaderHosts`. `baseline` uses `Default` (zero everywhere). Chaos scenarios bump only the fields they expect to perturb — exceeding the bound fails just like a strict-zero violation.

## Architecture

| Module | Role |
|---|---|
| `main.rs` | CLI parsing, scenario dispatch, single-pass and soak loops |
| `config.rs` | Parses `config.env` / `.cluster-env`; resolves cert paths and host/port slots |
| `actions.rs` | Typed `Action` enum (StartCs1, KillCs2, Partition, SkewClock, ...) — each action shells out to `make <target>` in the deploy directory |
| `scrape.rs` | Background 2 Hz Prometheus scraper of both nodes' `/metrics` endpoints |
| `sample.rs` | `NodeSample` parsing — converts raw Prometheus text into typed gauges/counters with monotonic `t_ms` offsets |
| `scenario.rs` | `bring_up_cluster` / `tear_down_and_evaluate` plus one `run_*` per scenario |
| `invariants.rs` | `ScenarioExpectations` and the check engine that evaluates samples over the bench window |
| `logs.rs` | On failure, fetches `journalctl` from each node covering the run window |
| `report.rs` | Per-run directory layout; writes per-scenario JSON and the top-level `report.md` |

## Manual cluster control

Anything chaos does, you can also do by hand from the deploy directory. Useful for reproducing a failed scenario or inspecting state:

```bash
cd deploy/rpi-cluster

make start-infra
make start-cs1
make start-cs2

make kill-cs1                          # SIGKILL leader slot
make stop-cs2                          # graceful stop follower slot

make partition SRC=cs1 DST=infra PORT=9000   # block leader → MinIO
make heal      SRC=cs1 DST=infra PORT=9000

make stop                              # bring everything down
make teardown-data                     # destructive: wipes cs1, cs2, MinIO
```

Run reports under `deploy/<target>/runs/` are git-ignored and accumulate over time. Prune them when convenient.
