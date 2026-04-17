# Jepsen-Style Testing for Celeriant

## Intent

Today's `celeriant_chaos` is fault-injection with server-side metric checks. Every
invariant is a predicate over Prometheus samples (`wal_index`, `node_role`,
`replication_rollbacks_total`, etc.). That is a low-effort way to catch
observable regressions but it trusts the server to report the truth and it
cannot verify claims the client API makes about ordering, atomicity,
idempotency, or concurrency control.

A full Jepsen-style suite checks the system's *externally visible behaviour*
against a formal model. The client's record of what it sent and what it saw
becomes the source of truth; the server's metrics become corroborating, not
authoritative.

## Where we are

`celeriant_chaos` has:
- 17 scenarios that manipulate leader/follower/MinIO/network (`scenario.rs`).
- A bench-style load generator (`celeriant_bench`) that records per-task success/error counts and latencies.
- Scrapers that poll `/metrics` at 2Hz and produce `RunData` samples.
- 13 `Check` predicates in `invariants.rs` covering liveness and a few convergence properties (`EventualConvergence`, `FinalLeaderWroteDuringBench`).

What we don't have:
- A per-operation **history** capturing call/return/timeout from the client's perspective (not just totals).
- A **model** of what the history is allowed to look like (linearizable register, OCC-conflict ordering, idempotency-window semantics, watch-delivery guarantees).
- A **checker** that validates the history against the model.
- Adversarial tests for documented client-API claims (idempotency, OCC, watch at-least-once, client redirect on `NotLeader`).

## Architecture

```
┌────────────────┐        ┌───────────────┐
│ Fault injector │──────▶│ Celeriant cluster │
│ (existing      │        │ (cs1, cs2, minio) │
│  scenario.rs)  │        └───────────────┘
└────────────────┘                │
                                   │ client RPCs
                                   ▼
                          ┌────────────────┐
                          │ Workload client│  ← records CALL/RETURN/TIMEOUT
                          │  (new)         │     per operation into a history
                          └────────────────┘
                                   │ history.jsonl
                                   ▼
                          ┌────────────────┐
                          │ Checker        │  ← validates against model
                          │  (new)         │
                          └────────────────┘
                                   │
                                   ▼
                             PASS / violation report
```

The fault injector is already good (scenarios + `actions.rs` already understand
leader/follower/partition/sigstop). What's missing is the right-hand side:
record history → check history.

## The gaps, concretely

### Gap 1: Operation-level history

Today the bench records *counts*. A Jepsen-style history is a **sequence of
events**. Field names match `jepsen.history` so the output loads into Jepsen's
EDN tooling directly (time is nanoseconds-since-test-start, not wall-clock):

```
{ :process 42, :type :invoke, :f :write,
  :value {aggregate, client_event_index, v}, :time 1234567 }
{ :process 42, :type :ok,     :f :write,
  :value {wal_index, aggregate_version}, :time 1234890 }
{ :process 43, :type :fail,   :f :write, :error :occ-conflict, :time 1234901 }
{ :process 44, :type :info,   :f :write, :error :timeout,      :time 1234999 }
```

Three key distinctions to get right:
- **`:ok`** — server returned success, operation *did* happen.
- **`:fail`** — server returned a typed error, operation *did not* happen (e.g. `OccConflict`, `ClientIdempotencyViolation`, `NotLeader` with no leader address).
- **`:info`** — we don't know. Timeout, connection reset mid-call, `NotLeader` with a retryable address we then retried. The checker must tolerate either outcome.

Today our bench treats `info` as `fail` (it counts every error the same). That
conflates two very different things for a linearizability checker: a `fail`
narrows the search space, an `info` expands it.

We emit JSONL during a run (ergonomic for inspection) and convert to EDN for
Jepsen/Elle consumption. Porcupine takes its own Go struct format, so the
converter is the same cost either way.

### Gap 2: Workload definitions

A history is only useful against a known model. Each workload is a
(client_gen, model) pair.

| Workload | Purpose | Model |
|---|---|---|
| **Register per aggregate** | Catch lost writes / stale reads. Read and write a single integer field on one aggregate. | Single-register linearizability (Knossos-style). |
| **Bank** | Catch torn cross-aggregate reads. Multi-aggregate listing under concurrent writes. Since Celeriant has no cross-shard transactions, the check is *per-shard* conservation (sum across aggregates on one shard stays consistent under that shard's OCC ordering). | Snapshot isolation per shard; list operations see a contiguous WAL prefix. |
| **OCC conflict** | Verify documented OCC invariant. Parallel CAS-style writes on the same aggregate. | Elle `rw-register` / `list-append`: no G-single, G2, or dirty-update anomalies. Version tags supplied via `wal_index` / `aggregate_version`. |
| **Idempotency** | Verify `ClientIdempotencyViolation` invariant. Same `(aggregate_key, client_id, client_event_index)` submitted multiple times (either by client retry or by us deliberately). | Exactly one success; duplicates rejected with `ClientIdempotencyViolation`. |
| **Watch** | Verify watch at-least-once + ordering. Subscribe, issue N writes, collect all events, verify at-least-once delivery and ordering by wal_index per aggregate. | Every committed write appears in the stream at least once; per-aggregate order matches WAL order. |
| **Redirect** | Verify client pool's leader-redirect behaviour under repeated `NotLeader`. | Every successful write eventually lands on the current leader; the client converges without operator intervention. |

### Gap 3: Checker implementations

- **Linearizability** (Register workload): Porcupine (Go) — fastest, model expressed as a Go state machine, right tool for a single integer register.
- **OCC and Bank (cross-aggregate consistency per shard)**: **Elle** (`jepsen-io/elle`). Elle infers Adya anomalies (G0, G1a/b/c, G2, G-Single, dirty updates, garbage reads) by constructing dependency graphs from version-tagged operations. We already have `wal_index` and `aggregate_version` as natural version tags, so this is Elle's sweet spot — runs at 100k+ ops/s vs Knossos-style search that chokes above a few hundred. Use the `rw-register` workload shape, or `list-append` if we can model writes as appends to a per-aggregate event list (we can — that's exactly what Celeriant is).
- **Idempotency window**: custom — tractable. For each `(aggregate_key, client_id)`, the successful events must have strictly increasing `client_event_index`, and every rejection must satisfy "some earlier-or-concurrent event with `>= client_event_index` succeeded".
- **Watch delivery**: custom — sort WAL-committed writes by `wal_index`, verify the watch stream is a subsequence of them per aggregate. Duplicates allowed (at-least-once). Gaps not allowed.

Knossos remains available for models Porcupine can't express (multi-register,
queues) — it's slower but more expressive. Not "obsolete"; just not the first
choice for our register workload.

### Gap 4: Adversarial patterns

Jepsen nemeses in chaos vocabulary (several we already have):

- `partition-majorities-ring` → already covered by existing partition scenarios.
- `kill` / `pause` / `clock-skew` → we have `sigkill`, `sigstop`, `clock_skew_follower`.
- **Stolen from Jepsen (`jepsen.nemesis`)**:
  - `partition-random-node`: isolates a single node rather than splitting halves. Different failure mode — exercises "I am alone, should I step down?" rather than "I am in a minority, I must step down". We only have the half-split shape today.
  - `bridge`: asymmetric reachability (A ↔ B, A ↔ C, but not B ↔ C). In our 2-node setup adapt as "leader can reach MinIO but not follower, follower can reach MinIO but not leader" — exposes split-brain handoff via the shared S3 coordinator.
  - `clock-scrambler`: randomises clocks on both nodes within a bound (vs our one-sided follower skew). Catches lease-math bugs that symmetric skew masks.
  - `bitflip` / `truncate-file`: on-disk corruption injection against WAL files between restarts. We have no corruption nemesis today — relevant given hash-chain invariants.
- **New: `noop-duplicate`** — client transparently retries on `info`, submitting the *same* `client_event_index` twice. Verifies idempotency rejections never produce a split state.
- **New: `cas-storm`** — N clients CAS the same aggregate at the same version. Verifies exactly one success, N-1 `OccConflict`.
- **New: `kill-during-ack`** — SIGKILL the leader between fsync and client ACK. Verifies the operation is either recorded in both nodes' WAL (server committed, we lost ACK) or in neither (server never committed). Never "in only one".

### Gap 5: Generator / final-read discipline

Two patterns Jepsen enforces that our plan glossed over:

- **Two-stream generator composition** (`jepsen.generator`): workload-gen and nemesis-gen are independent op-streams composed via `gen/mix` / `gen/stagger` / `gen/nemesis` / `gen/phases`. The workload client never decides when a partition happens; the nemesis never decides when a write fires. Adopt from Phase 1 — retrofitting later is a rewrite.
- **Final reads**: every test ends with phases — (a) stop nemesis and heal all faults, (b) quiesce period long enough for catchup to complete, (c) one read per key from *every* node, (d) feed those reads into the checker as the authoritative tail. Without final reads, in-flight `info` ops are unresolvable and convergence cannot be tested end-to-end. This is how Jepsen decides whether an ambiguous `info` was ultimately committed.

## Build-out plan

### Phase 1: History-recording workload

**Scope**: one workload (register), real histories, no model check yet.

- [ ] New crate `celeriant_jepsen` (alongside `celeriant_chaos`).
- [ ] Client wrapper that wraps `celeriant_client_tokio::CeleriantPool` and emits `{client, op, state, args, ret, time_ns}` to a JSONL file per scenario run.
- [ ] Register workload: one aggregate, N clients, each loops `read` / `write(value)` with uniform jitter.
- [ ] Wire into `celeriant_chaos::scenario.rs` as an opt-in via `--workload register` (default keeps bench).
- [ ] Emit `history.jsonl` next to `report.md` in the run dir.
- [ ] Validate by hand: kill the leader mid-run, confirm the history contains `info` entries spanning the promotion window.

### Phase 2: First model check — linearizable register

**Scope**: introduce a checker, validate one invariant end-to-end.

- [ ] Port `history.jsonl` to Porcupine's format (Go) or embed `knossos` (Clojure/JVM, heavier). Porcupine is lighter; call it from Rust via a thin subprocess.
- [ ] Checker crate: `celeriant_jepsen::check::register`. Input: history, linearizability spec. Output: pass or counter-example.
- [ ] Integrate into the chaos run: `FAIL` the scenario if the checker finds a counter-example. Archive the counter-example alongside the journalctl logs.
- [ ] Run across the five leader-disruption scenarios. Expected: some failures — this is the point.

### Phase 3: Domain-specific checkers

**Scope**: idempotency, OCC, watch.

- [ ] `check::idempotency`: enforce the documented rule from `invariants.md` ("any `client_event_index <= max stored` is rejected").
- [ ] `check::occ`: for every aggregate, reconstruct WAL order from `ok` returns; verify no two overlapping CAS operations both returned `ok`.
- [ ] `check::watch`: new workload — writer + subscriber clients; compare subscriber's delivered stream to writer's committed history.
- [ ] Promote each check to run per scenario. Keep the server-side Prometheus checks as a parallel layer — they still catch crashes / stuck metrics that the history-level checker can't see (e.g. a panic that leaves the process running but wedged).

### Phase 4: Client-pool-level claims

**Scope**: verify the client does what the server expects it to do. Moved
ahead of new nemeses because it has no checker dependency (just needs Phase 1
histories) and it directly targets the Bug B class of regression that
motivated this whole effort.

- [ ] Documented claim: `NotLeader { leader_address: Some }` is transparent to the caller. Test by pinning the workload to a cs2 seed when cs1 is leader; the write must succeed without the workload having to retry manually.
- [ ] Documented claim (Bug B fix): `WireError` / `ReadError` on a pooled connection is transparent. Test by forcing `slow_client_timeout` to fire mid-workload; no task should observe a terminal failure.
- [ ] Documented claim: under `ServerBusy` the client backs off. Test by saturating `pending_replication_high_water_bytes`; verify the workload's error rate is bounded by the backoff curve, not a thundering-herd retry storm.

### Phase 5: New adversarial scenarios

**Scope**: exercise claims the current scenarios don't stress.

- [ ] Import the Jepsen nemeses from Gap 4 (`partition-random-node`, `bridge`, `clock-scrambler`, `bitflip`/`truncate-file`).
- [ ] `kill_during_ack`: instrumentation mode where the leader SIGKILLs itself after fsync but before ACK. Requires an admin endpoint (guarded behind a test-only feature flag) — fatal for production.
- [ ] `cas_storm`: register workload with N clients, all submitting `write(v, expected_version=X)` within a 100ms window, repeated.
- [ ] `duplicate_replay`: the workload client retains its original request buffer; on any `info`, it resubmits *twice* (second with the same `client_event_index`). The checker verifies one `ok`, one `ClientIdempotencyViolation`, and the WAL contains exactly one record.
- [ ] `rolling_restart` with workload: current rolling_restart is bench-only. Rerun with register workload to see whether panics land on committed or uncommitted writes.

## What this buys us

- Every failure produces a **minimal counter-example** — a linearised prefix of operations that cannot have happened. Debugging a 60k-req chaos run is no longer "grep the log for panics".
- Claims in `invariants.md` become **machine-checked** rather than documentation.
- We can test the client library as aggressively as we test the server. Bug B (pooled zombie conn stranding the bench) would have been a Phase-5 workload failure, not a metrics-staring exercise.
- The same histories can be replayed offline against new checker versions, so improvements to the model are retroactive.

## Non-goals

- We are **not** going to implement our own consistency checker. Porcupine (linearizability via state-machine search) and Elle (transactional anomaly detection via dependency graphs) are the established tools.
- We are **not** going to use Maelstrom. It's Jepsen's toy-system harness (JSON-over-stdin protocol) and doesn't fit a production database with TLS, mTLS, and a real wire protocol.
- We are **not** going to run this continuously — chaos runs are slow and hardware-bound. History recording + checking is for PR gates and release qualification, not every CI commit.
- We do **not** model cross-shard consistency. The invariants doc is explicit: no cross-shard snapshot isolation. Workloads stay within a single shard unless the claim being tested is per-shard convergence.

## Future: tests to close the gap with a full Jepsen protocol

Cross-referenced against the [Antithesis reliability glossary](https://antithesis.com/docs/resources/reliability_glossary/).
Items below are outside the Phase 1-5 scope but worth pursuing once the
core history + checker infrastructure exists.

### Test environments

Three environments run the same logical tests at different fidelity levels.
The test logic (workload, checker, assertions) should be shared. What differs
is the fault injection mechanism and the cluster lifecycle.

| Environment | Faults | Cluster | Feedback loop |
|---|---|---|---|
| **Integration** (localhost) | TcpProxy block/throttle, MinIO docker pause, SIGKILL, filesystem manipulation | In-process spawn via `TestServer`, seconds per test | CI gate, every commit |
| **RPi cluster** | iptables, SIGKILL/SIGSTOP, `date -s`, `fallocate`, `tc netem` | Systemd services over SSH, minutes per scenario | Pre-merge, nightly soak |
| **EC2 cluster** | Security groups, instance stop/start, clock skew, EBS manipulation | CDK-managed instances, real S3 (no MinIO) | Release qualification |

The goal is that a test passing on localhost gives high confidence it will
pass on real hardware, and a failure on real hardware can be reproduced
locally by switching the fault backend. A test that only runs on one
environment is a last resort.

**Fault backend abstraction.** Each test declares what it needs (partition,
latency, kill, clock skew, disk corruption) and the environment provides
the mechanism:

| Fault | Integration | RPi/EC2 |
|---|---|---|
| Network partition | `TcpProxy.block()` | `iptables DROP` / security group |
| Latency injection | `TcpProxy.throttle(delay_ms)` | `tc netem delay` |
| Process crash | `TestServer.stop()` (SIGKILL) | `kill -9` / instance stop |
| Process freeze | Not yet (add SIGSTOP to TestServer) | `kill -STOP` / SIGSTOP |
| Clock skew | Not yet (inject via config override) | `date -s` / `chrony` |
| S3 outage | `MinioContainer.pause()` | `docker pause` / endpoint block |
| Disk corruption | Direct filesystem write on TempDir | SSH + write to data dir |
| Disk pressure | `fallocate` on TempDir | `fallocate` on data partition |

Gaps to close for full parity: integration tests need SIGSTOP support and
a clock skew injection path (either a test-only config flag that offsets
the lease clock, or a time source trait that tests can override).

### Fault injection — new scenarios

**Latency injection (message delay).** All current network faults are total
partitions (message omission). Adding 500ms-2s delay without dropping packets
exercises a different failure mode: lease renewals arriving late but not lost,
replication batches trickling through slowly enough to trigger timeouts but not
disconnections. This is how real cloud networks degrade. On localhost the
TcpProxy already supports per-chunk throttle. On RPi/EC2 this needs a new
Makefile target wrapping `tc netem`.

**Kill between fsync and replication start.** `kill_during_ack` targets the
window after replication but before client ACK. The complementary scenario kills
the leader after local fsync but before replication begins. The new leader must
detect that the old leader's WAL extends beyond the replication frontier and
either ignore or truncate those entries during catchup. This directly exercises
the rollback path and WAL divergence recovery. On localhost: `TestServer.stop()`
after a write with throttled replication. On RPi/EC2: SIGKILL under sustained
load.

**Storage fault injection.** The dual-header design exists to survive torn
writes and fsync lies. Test it:
- Corrupt one WAL header between restarts, verify recovery from the backup
  header.
- Use `libfiu` or a FUSE shim to make `fdatasync` return success without
  persisting, then crash. Verify the dual-header CRC32C check catches it on
  recovery.
- Truncate a WAL file mid-datablock. Verify the read cursor doesn't advance
  past the corruption.

All three are filesystem-level manipulations between restarts — identical
logic on localhost (`TempDir`) and real hardware (SSH + data dir path).

**Asymmetric network faults (bridge).** Leader can reach MinIO but not
follower; follower can reach MinIO but not leader. Both believe they should
lead. Exercises whether S3 lease coordination correctly prevents split-brain
when direct replication is down but the coordination plane is reachable from
both sides. On localhost: two TcpProxy instances with selective blocking. On
RPi/EC2: two asymmetric iptables rules.

**Single-node isolation.** Current partitions split the cluster in half.
Isolating one node entirely (it can reach nobody, including MinIO) tests the
"I am alone, should I self-fence?" path rather than the "I am in a minority"
path. On localhost: block both the replication proxy and the MinIO endpoint.
On RPi/EC2: iptables DROP all on the target node's OUTPUT chain.

**Node replacement (membership change).** Replace one node with a fresh node
(new `node_id`, empty data directory) while the surviving node holds S3
fallback batches uploaded by the old node. The new node must catch up from S3
despite `peer_node_id` pointing at the surviving node, not the old uploader.
Exercises two things: (a) the `peer_node_id` filter during S3 catchup — on
boot the peer is `None` so the new node accepts all non-self batches
regardless of uploader, but after the first election `peer_node_id` is set
and batches from the departed node are filtered out; and (b) the promotion
batch mechanism — the surviving node must have uploaded promotion batches
covering any TCP-only entries before the old node departed, otherwise the new
node has an unrecoverable gap. The scenario sequence is: A and B running, kill
B permanently, A continues writing (S3 fallback since B is gone), replace B
with C (new node_id, fresh data dir), C boots, catches up from S3, joins
cluster. Verify: C's WAL converges with A's, no gaps, no data loss. Then
reverse: kill A, C becomes leader, replace A with D, D catches up. On
localhost: stop TestServer B, start a new TestServer C with a different
node_id and a fresh TempDir. On RPi/EC2: wipe the data directory on the
target node, regenerate the node certificate with a new identity, restart the
service.

### Consistency model coverage

**Session guarantees across failover.** Monotonic reads and read-your-writes
are structural in steady state (the read cursor only advances). The interesting
test is the transition: after a leader failover, does a client that was ACKed by
the old leader observe its write when reading from the new leader? The new
leader's read cursor must have caught up past that write before serving reads.
Frame as: writer gets ACK, leader dies, client reconnects to new leader, reads
back. The value must be present. Identical test logic across all three
environments — only the kill mechanism differs.

**Stale read after rollback.** If a follower served a read from data that was
later rolled back on the leader (replication succeeded to follower, then leader
died before ACKing client, new leader diverges), the follower's read cursor
should retreat during catchup. Verify no client observes a value that was
subsequently erased by divergence recovery. On localhost: TcpProxy.block after
replication but before ACK, then kill leader. On RPi/EC2: same sequence via
iptables + SIGKILL.

### Testing technique gaps

**History shrinking.** The plan targets "minimal counter-examples" but only
at the operation level (Porcupine's output). The fault schedule itself (when
partitions fire, when kills happen) is not minimised. A shrinking pass that
replays the scenario with subsets of fault events, binary-searching for the
smallest schedule that still produces the violation, would make failures
dramatically easier to debug. This is environment-agnostic — the shrinking
logic replays against whichever fault backend produced the original failure.

**Wire protocol fuzzing.** No coverage of malformed messages hitting the
server. A fuzzer that mutates valid wire protocol frames (flip bytes, truncate,
duplicate fields, send oversized payloads) would test deserialisation robustness
and verify the server rejects garbage without panicking or corrupting state.
Localhost only — no reason to fuzz over a real network.

**Metamorphic testing.** Reading the same aggregate from leader and follower
after replication quiesces should return identical results. A metamorphic oracle
that issues paired reads and compares results is cheap to build and catches an
entire class of replica divergence bugs without needing a formal model. Runs
identically on all three environments.

**Clock scrambler (symmetric skew).** Current clock skew is one-sided
(follower only). Randomising clocks on both nodes within a bound catches
lease-math bugs that one-sided skew masks, particularly around simultaneous
lease expiry races. RPi/EC2 only until integration tests gain a clock
injection path.

## First step

Phase 1, first bullet: create `celeriant_jepsen` crate with a register workload
and `history.jsonl` emission. No model check yet — just prove we can generate
and store meaningful histories. Until we can inspect one by hand and recognise
what happened, the rest of the stack has nothing to stand on.
