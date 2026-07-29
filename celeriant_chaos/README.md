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

# Replay a printed seed to get the same nemesis fault schedule and clock jitter
cargo run --release -p celeriant_chaos -- --scenario nemesis_composition --seed 0x1a2b3c4d
```

The crate ships a second binary, `replay`, for pushing stored run JSONs back through the comparator checks. `default-run` points at the orchestrator, so `cargo run -p celeriant_chaos` without `--bin` is the chaos runner; the dev tool needs `--bin replay`.

Other flags:

- `--seed <u64>` fixes the seed behind nemesis fault schedules, clock-skew jitter, and oracle sample selection. Defaults to wall-clock entropy and is printed at startup, so a Heisenbug run can be replayed exactly.
- `--connect-ramp <secs>` spreads bench task starts over a window (baseline only). Off by default: the cold-connect herd is part of the test.

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

`--full` runs the list below in order.

| Scenario | What it does |
|---|---|
| `baseline` | No chaos. Strict zero-tolerance — any role flip, election, S3 fallback, or rollback fails the run. |
| `watch_storm` | Happy cluster, adversarial watch flood: connection churn, slow/never-reading watchers, long-lived watchers that must keep receiving. Subscriber gauge must drain after the flood and fresh dials stay prompt. |
| `watch_storm_failover` | Same flood, leader SIGKILLed ~40% in. Watch servicing must recover on the promoted node with no leaked sessions. |
| `follower_graceful_stop` | `systemctl stop` the follower mid-bench, then restart. Leader must retain leadership; follower must catch up before tear-down. |
| `follower_sigkill` | SIGKILL the follower mid-bench, then restart. |
| `leader_graceful_stop` | `systemctl stop` the leader mid-bench. Follower must promote and serve writes. |
| `leader_sigkill` | SIGKILL the leader mid-bench. |
| `leader_restart_loop` | Repeatedly kill+restart the leader. At least N distinct hosts must have held leadership. |
| `partition_leader_minio` | Block leader → S3 (rpi: MinIO on infra host). |
| `partition_asymmetric` | One-directional drop. |
| `partition_leader_follower_replication` | iptables DROP on the replication port between leader and follower. Forces S3-fallback replication. |
| `bridge` | Replication severed in BOTH directions while both nodes keep S3. Leader must hold its lease purely on S3 renewal; follower challenges must lose the CAS. |
| `single_node_isolation` | Leader loses peer AND S3 while both stay healthy. It must self-fence inside the lease TTL; the survivor promotes with no dual-ack window. |
| `network_flap` | Repeatedly partition + heal. |
| `minio_outage_short` / `minio_outage_long` | `docker compose stop minio` then start. Tests S3 unavailability tolerance. |
| `partition_then_kill_minio` | Partition first, then kill MinIO. Exercises Mode B divergence recovery. |
| `rolling_restart` | Stop one node, wait for catchup, stop the other. Zero downtime if it works. |
| `clock_skew_follower` | Shift the follower's system clock. Pairs with `RestoreClock`. |
| `clock_scrambler` | Seeded bounded random skew on BOTH nodes, so drift-fencing is exercised in both directions at once. |
| `sigstop_leader` | SIGSTOP the leader (frozen, not killed). Tests heartbeat-based eviction. |
| `follower_disk_full` | `fallocate` the follower's data disk to within a small reserve. |
| `idempotency_audit_baseline` | Idempotent bench on a quiet cluster, then read back every aggregate and check the WAL holds every `client_seq` the client believed durable. |
| `idempotency_audit_minio_outage` | The same audit during a MinIO outage, where replication rides TCP throughout. |
| `idempotency_audit_partition_then_kill_minio` | The audit under the worst rollback shape in the suite: follower killed + MinIO stopped, 40s blackout, heal. |
| `duplicate_replay` | Every acked write resubmitted with the same `client_seq`. Exactly one WAL record per seq; the replay must come back 2002. |
| `cas_storm` | N writers contend on one aggregate at the same `expected_version` per round. OCC must admit exactly one, and losers must see a definitive `OccConflict`, not a timeout. |
| `cas_storm_partition` | The storm re-run across a leader→follower replication partition and heal. |
| `cold_segment_reads` | Writes a wide aggregate set, then churns a narrow one so the rest goes cold. Cold reads must show no seq gaps — a scanner/bloom/visibility probe. |
| `nemesis_composition` | Seeded concurrent fault loops (partition, kill, skew) composing over one bench window. Correctness counters must stay zero; timing budgets are loose. |
| `schema_under_partition` | Schema registered while replication is partitioned must reach the follower via recovery, and a follower promoted after the heal must enforce it. |

Two scenarios exist but sit outside `--full`, so they need `--scenario`:

| Scenario | What it does |
|---|---|
| `bench_load_sweep` | No chaos — sweeps `--tasks` to find the throughput knee on this hardware. |
| `idempotency_audit_fast_blackout` | ~3x shorter version of the partition-then-kill-MinIO audit, for packing iterations into a soak hour. |

## Invariants

Each scenario supplies a `ScenarioExpectations` (see `src/invariants.rs`) declaring the maximum tolerated counter deltas across the bench window plus optional checks like `EventualConvergence`, `LeaderRetained`, `FinalLeaderWroteDuringBench`, and `DistinctLeaderHosts`. `baseline` uses `Default` (zero everywhere). Chaos scenarios bump only the fields they expect to perturb — exceeding the bound fails just like a strict-zero violation.

Metrics alone can miss a fork that both nodes agree about, so the heavier scenarios stack oracles on top:

- **History checkers** (`checkers.rs`) run over the per-op client history: idempotency, OCC, WAL monotonicity, final-read parity. History is the client's ground truth; metrics only corroborate. Dropped records can hide evidence but never invent it, so `check_idempotency` fails closed when any record was dropped.
- **Post-quiesce disk oracles** run after StopAll, when WAL files are stable. `tip_fork.rs` compares write cursors and tip hashes across nodes to catch same-seq divergent tips. `epoch_oracle.rs` asserts lease epochs are non-decreasing per node and consistent across nodes. `disk_truth.rs` re-checks audit-flagged aggregates with `celeriant-wal-inspect`, bypassing the read path.
- **Journal and resource checks** scan for panics, aborts, and error storms, and compare fd count and RSS against a pre-bench snapshot.

## Architecture

`lib.rs` exports the harness so the `replay` dev binary shares these modules with the orchestrator.

| Module | Role |
|---|---|
| `main.rs` | CLI parsing, scenario dispatch, single-pass and soak loops |
| `config.rs` | Parses `config.env` / `.cluster-env`; resolves cert paths and host/port slots |
| `actions.rs` | Typed `Action` enum (StartCs1, KillCs2, Partition, SkewClock, ...) — each action shells out to `make <target>` in the deploy directory |
| `scrape.rs` | Background 2 Hz Prometheus scraper of both nodes' `/metrics` endpoints |
| `sample.rs` | `NodeSample` parsing — converts raw Prometheus text into typed gauges/counters with monotonic `t_ms` offsets |
| `scenario.rs` | `bring_up_cluster` / `tear_down_and_evaluate` plus one `run_*` per scenario |
| `invariants.rs` | `ScenarioExpectations` and the check engine that evaluates samples over the bench window |
| `checkers.rs` | History-based consistency checks: idempotency, OCC, WAL monotonicity, final-read parity |
| `final_read.rs` | Post-heal read of every bench aggregate from BOTH nodes through node-pinned pools, feeding the parity checks |
| `tip_fork.rs` | Post-quiesce WAL header comparison across nodes: divergent tips and fork wedges |
| `epoch_oracle.rs` | Post-quiesce lease-epoch monotonicity per node and agreement across nodes |
| `disk_truth.rs` | Reclassifies audit-flagged aggregates against `celeriant-wal-inspect` output, bypassing the server read path |
| `s3_lifecycle.rs` | Audits MinIO fallback objects per shard: file counts and seq ranges |
| `journal_assert.rs` | Textual journal checks — panics, aborts, error storms |
| `resource_baseline.rs` | Pre/post fd-count and RSS snapshots per node for leak detection |
| `logs.rs` | On failure, fetches `journalctl` from each node covering the run window |
| `report.rs` | Per-run directory layout; writes per-scenario JSON and the top-level `report.md` |
| `bin/replay.rs` | Dev tool: replays stored run JSONs through the comparator checks |

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
