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
         └────────────────────┬──────────────────────────┘
                              │  ElectionOutcome
                              ▼
         ┌────────────────────────────────────────┐
         │         ValidatedNodeStatus             │
         │  NodeStatus + lease_expires_at_ms       │
         │  + max_clock_drift_ms                   │
         │                                         │
         │  must_fence()       → leader threshold  │
         │  is_lease_expired() → follower threshold │
         └─────────────────────────────────────────┘
```

## Asymmetric Fencing

The core correctness property. Two thresholds derived from the same lease expiry:

- **`must_fence()`** — triggers at `lease_expires_at_ms - max_clock_drift_ms`. Leaders use this: stop accepting writes early enough that even a clock-ahead follower cannot win the S3 race while writes are still in flight.
- **`is_lease_expired()`** — triggers at `lease_expires_at_ms`. Followers use this: wait the full TTL before racing to acquire the lease.

This asymmetry closes the window where a clock-ahead follower could win an S3 CAS race while the old leader still accepts writes.

## Module Structure

| Module | Purpose |
|--------|---------|
| `s3_lease_config` | `S3LeaseConfig` — node identity, advertised addresses, lease duration, drift tolerance |
| `node_status` | `NodeStatus` enum and state machine transition rules |
| `validated_node_status` | `ValidatedNodeStatus` with asymmetric fencing; `unix_epoch_now_ms()` |
| `lease_store` | `LeaseStore` trait, `LeaseWithEtag`, `MembershipWithEtag`, `LeaseStoreError` |
| `s3_lease_manager` | `S3LeaseManager<S>`, `ElectionOutcome`, election/membership logic |
| `paths` | S3 path constants and fallback batch path generation |

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

## Election Semantics

| Scenario | Outcome |
|----------|---------|
| No lease | Race `put_lease_create_only`; winner → Leader, loser reads again → Follower |
| Valid lease, other node | Follower unconditionally |
| Valid lease, own node | CAS-extend; winner → Leader with incremented `lease_index` |
| Expired lease | CAS race; winner → Leader, loser reads winner's lease → Follower |

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
