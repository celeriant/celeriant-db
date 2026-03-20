# celeriant_distributed

S3-based leader election and self-fencing for two-node HA. A single CAS-protected S3 object grants leader exclusivity — no Raft, no Paxos, no coordinator process.

## Architecture

```
         ┌─────────────────────────────────────────────┐
         │              S3 Bucket                       │
         │  cluster/lease.json      (Lease, CAS-guarded)│
         │  cluster/membership.json (NodeInfo per node) │
         │  cluster/fallback/...    (S3 replication)    │
         └────────────┬────────────────────┬────────────┘
                      │                    │
              get/put_lease           get/put_membership
                      │                    │
         ┌────────────▼────────────────────▼────────────┐
         │            S3LeaseManager<S>                  │
         │  run_election_to_acquire_s3_lease()           │
         │  register_self_on_membership_s3_object()      │
         │  discover_peer()                              │
         └────────────────────┬──────────────────────────┘
                              │  ElectionOutcome
                              │  { status: ValidatedNodeStatus,
                              │    peer_info: Option<NodeInfo> }
                              ▼
         ┌────────────────────────────────────────┐
         │         ValidatedNodeStatus             │
         │  NodeStatus + lease_expires_at_ms       │
         │  + max_clock_drift_ms                   │
         │                                         │
         │  must_fence()       → leader threshold  │
         │  is_lease_expired() → follower threshold │
         │  effective_node_status() → TTL decay    │
         └─────────────────────────────────────────┘
```

## Asymmetric Fencing

The core correctness property. Two thresholds derived from the same lease expiry:

- **`must_fence()`** — triggers at `lease_expires_at_ms - max_clock_drift_ms`. Leaders use this: stop accepting writes early enough that even a clock-ahead follower cannot win the S3 race while writes are still in flight.
- **`is_lease_expired()`** — triggers at `lease_expires_at_ms`. Followers use this: wait the full TTL before racing to acquire the lease.

This asymmetry closes the window where a clock-ahead follower could win an S3 CAS race while the old leader still accepts writes.

### TTL Decay via `effective_node_status()`

The runtime calls `effective_node_status()`, not `raw()`, to get the current state. This applies TTL decay:

- **Leader** or **Follower** → decays to `Fenced` if `must_fence()` fires
- **Standalone**, **BootCatchup**, **Fenced**, **FollowerCatchingUp** → always returns raw status (TTL-exempt)

`can_accept_writes()` returns true only for `Leader` and `Standalone` (after TTL decay).

## Module Structure

| Module | Purpose | Key Types/Functions |
|--------|---------|---------------------|
| `s3_lease_config` | Node identity and lease tuning | `S3LeaseConfig` |
| `node_status` | State enum and transition validation | `NodeStatus`, `is_valid_transition_to()` |
| `validated_node_status` | Asymmetric fencing wrapper | `ValidatedNodeStatus`, `effective_node_status()`, `must_fence()` |
| `lease_store` | Storage backend trait | `LeaseStore`, `LeaseWithEtag`, `MembershipWithEtag`, `LeaseStoreError` |
| `s3_lease_manager` | Election orchestrator | `S3LeaseManager<S>`, `ElectionOutcome` |
| `paths` | S3 path constants and fallback batch paths | `LEASE_PATH`, `MEMBERSHIP_PATH`, `fallback_batch_path()` |

## S3LeaseConfig

```rust
pub struct S3LeaseConfig {
    pub node_id: u128,
    pub advertised_client_address: String,
    pub advertised_replication_address: String,
    pub s3_lease_duration: Duration,
    pub max_clock_drift: Duration,
}
```

`advertised_*` fields are what gets published to the S3 membership object — not necessarily the local bind addresses.

## NodeStatus

```rust
pub enum NodeStatus {
    Leader { lease_index: u64 },
    Follower { leader_lease_index: u64 },
    FollowerCatchingUp { leader_lease_index: u64 },
    BootCatchup,
    Fenced,
    Standalone,
}
```

### Helper Methods

- `is_leader()`, `is_follower()`, `is_fenced()`, `is_standalone()`, `is_catching_up()`
- `is_any_follower_state()` — matches both `Follower` and `FollowerCatchingUp` (used by heartbeat handler and watchdog)
- `lease_index()` → `Some(u64)` for Leader and Standalone only
- `is_valid_transition_to()` — validates state machine transitions

### State Machine

```
any state        → Fenced
Standalone       → Leader, Follower
BootCatchup      → Leader, Follower
Fenced           → Leader, Follower, BootCatchup
Leader           → Leader (renewal), Follower (lost CAS)
Follower         → Follower (lease update), FollowerCatchingUp (kick)
FollowerCatchingUp → Follower (catchup complete)
```

| State | Description | TTL-exempt |
|-------|-------------|-----------|
| `Standalone` | Single-node, no replication | yes |
| `Leader { lease_index }` | Holds S3 lease, accepts writes | no |
| `Follower { leader_lease_index }` | Follows leader, accepts TCP replication | no |
| `FollowerCatchingUp { leader_lease_index }` | Catching up from S3, rejects TCP replication | yes |
| `BootCatchup` | Pre-election S3 catchup at boot | yes |
| `Fenced` | Write-blocked; must re-run election to recover | yes |

## ValidatedNodeStatus Construction

TTL-exempt states have dedicated constructors (no lease expiry needed):

```rust
ValidatedNodeStatus::create_fenced()
ValidatedNodeStatus::create_standalone()
ValidatedNodeStatus::create_boot_catchup()
```

States with TTL use the general constructor:

```rust
ValidatedNodeStatus::create_custom_status(status, max_clock_drift_ms, lease_expires_at_ms)
```

## LeaseStore Trait

Backend abstraction for S3 (or any CAS-capable store):

```rust
pub trait LeaseStore {
    async fn get_lease(&self) -> Result<Option<LeaseWithEtag>, LeaseStoreError>;
    async fn put_lease_create_only(&self, lease: &Lease) -> Result<String, LeaseStoreError>;
    async fn put_lease_conditional(&self, lease: &Lease, etag: &str) -> Result<String, LeaseStoreError>;
    async fn get_membership(&self) -> Result<Option<MembershipWithEtag>, LeaseStoreError>;
    async fn put_membership(&self, membership: &Membership, etag: Option<&str>) -> Result<(), LeaseStoreError>;
}
```

```rust
pub enum LeaseStoreError {
    AlreadyExists,          // put_lease_create_only lost the race
    PreconditionFailed,     // CAS etag mismatch
    Unavailable { message: String },
}
```

## Election Semantics

| Scenario | Outcome |
|----------|---------|
| No lease | Race `put_lease_create_only`; winner → Leader, loser reads again → Follower |
| Valid lease, other node | Follower unconditionally |
| Valid lease, own node | CAS-extend; winner → Leader with incremented `lease_index` |
| Expired lease | CAS race; winner → Leader, loser reads winner's lease → Follower |

`run_election_to_acquire_s3_lease()` always calls `discover_peer()` after determining the election outcome, so `ElectionOutcome.peer_info` is populated when a peer exists in the membership object.

## S3 Paths

| Constant | Value |
|----------|-------|
| `LEASE_PATH` | `cluster/lease.json` |
| `MEMBERSHIP_PATH` | `cluster/membership.json` |
| `FALLBACK_PREFIX` | `cluster/fallback` |

### Fallback Batch Paths

`fallback_batch_path(shard_id, start_index, end_index)` generates:

```
cluster/fallback/shard_{shard_id:03}/batch_{start_index:09}_{end_index:09}.bin
```

Zero-padded so lexicographic ordering = temporal ordering (S3 list operations return sorted results).

## Design Decisions

### S3 as consensus medium

The S3 API's conditional write (`IfMatchETag`) guarantees exactly one winner per CAS round. Trade-off: S3 round-trip (~50-200ms) determines election latency. Acceptable for HA failover where second-scale recovery is fine.

### TTL-exempt states

`FollowerCatchingUp` and `BootCatchup` are driven by shard 0's catchup protocol, which installs a fresh `ValidatedNodeStatus` when done. Applying TTL would race with catchup duration, causing spurious fencing.

### Membership CAS with retries

`register_self_on_membership_s3_object` retries up to 5 times on `PreconditionFailed`. In a 2-node cluster one retry suffices; extras guard against transient S3 inconsistency. The S3 round-trip provides natural backoff.

## Feature Flags

| Feature | Default | Purpose |
|---------|---------|---------|
| `small-metablock` | off | Propagates to `celeriant_wal` for testing with smaller block sizes |

## Dependencies

- `celeriant_wal` — `Lease`, `Membership`, `NodeInfo` types (S3 lease/membership wire format)
