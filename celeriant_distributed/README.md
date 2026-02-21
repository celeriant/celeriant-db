# celeriant_distributed

Distributed replication coordination for leader/follower HA. Provides S3-based lease election, membership discovery, heartbeat liveness tracking, and node status state machine. No Raft/Paxos — a single CAS-protected S3 object grants leader exclusivity.

## Architecture

```
         ┌─────────────────────────────────────────────┐
         │              S3 Bucket                       │
         │  cluster/lease.bin      (Lease, CAS-guarded) │
         │  cluster/membership.bin (NodeInfo per node)  │
         │  cluster/fallback/...   (S3 replication)     │
         └────────────┬────────────────────┬────────────┘
                      │                    │
              get/put_lease           get/put_membership
                      │                    │
         ┌────────────▼────────────────────▼────────────┐
         │              LeaseManager<S>                  │
         │  run_election()    register_self()            │
         │  discover_peer()                              │
         └────────────────────┬──────────────────────────┘
                              │  ElectionOutcome
                              ▼
         ┌────────────────────────────────┐
         │       ValidatedNodeStatus      │
         │  NodeStatus + expires_at_ms    │
         │  effective() → Fenced if TTL   │
         │  can_accept_writes()           │
         └────────────────────────────────┘

         ┌────────────────────────────────┐
         │     HeartbeatLeaseTracker      │
         │  Per-peer: start / record /    │
         │  is_expired / reset            │
         └────────────────────────────────┘
```

**Election flow**: `run_election` reads the lease, then races with CAS (CreateOnly for fresh cluster, conditional CAS for expired lease). Exactly one node wins Leader; losers become Follower.

**Liveness**: `ValidatedNodeStatus` expires if shard 0 does not refresh before `expires_at_ms`. On expiry, `effective()` returns `Fenced` — the node self-fences without any external signal.

## Key Types

| Type | Purpose |
|------|---------|
| `ReplicationConfig` | Node ID, addresses, lease durations, heartbeat intervals, drift tolerance |
| `HeartbeatLeaseTracker` | Tracks peer liveness via wall-clock deadline; not started until first `start()` |
| `LeaseStore` | Trait abstracting S3 reads/writes for lease and membership objects |
| `LeaseWithEtag` | Lease + ETag for CAS-protected updates |
| `MembershipWithEtag` | Membership + ETag for CAS-protected updates |
| `LeaseStoreError` | `AlreadyExists`, `PreconditionFailed`, `Unavailable` |
| `LeaseManager<S>` | Orchestrates election, membership registration, peer discovery |
| `ElectionOutcome` | Result of `run_election`: `ValidatedNodeStatus` + optional peer `NodeInfo` |
| `NodeStatus` | Raw role enum: `Leader`, `Follower`, `FollowerCatchingUp`, `BootCatchup`, `Fenced`, `Standalone` |
| `ValidatedNodeStatus` | `NodeStatus` + TTL; `effective()` decays to `Fenced` when expired |

## Key Functions

| Function | Purpose |
|----------|---------|
| `ReplicationConfig::validate()` | Checks timing consistency: drift < timeout, 2x interval < timeout, lease bounds |
| `ReplicationConfig::heartbeat_timeout()` | `heartbeat_interval × max_missed_heartbeats` |
| `ReplicationConfig::status_ttl_ms()` | `heartbeat_timeout + max_clock_drift` — how long shards trust their status |
| `HeartbeatLeaseTracker::start()` | Set initial timestamp on connection established |
| `HeartbeatLeaseTracker::record_received()` | Refresh deadline on each heartbeat/ack |
| `HeartbeatLeaseTracker::is_expired()` | True if `now > last_received + lease_duration + clock_drift` |
| `HeartbeatLeaseTracker::reset()` | Clear state on reconnect or role change |
| `LeaseManager::run_election()` | Race for leader or follow; handles all CAS outcomes |
| `LeaseManager::register_self()` | CAS read-modify-write to add self to membership (up to 5 retries) |
| `LeaseManager::discover_peer()` | Read membership and return peer `NodeInfo` |
| `NodeStatus::is_valid_transition_to()` | State machine guard; validates role transitions |
| `ValidatedNodeStatus::effective()` | Time-aware status; returns `Fenced` if TTL elapsed |
| `ValidatedNodeStatus::can_accept_writes()` | True if `Leader` or `Standalone` |
| `now_ms()` | Unix epoch milliseconds via `SystemTime` |
| `fallback_batch_path(shard, start, end)` | S3 path for fallback replication batch |
| `fallback_shard_prefix(shard)` | S3 list prefix for all batches on a shard |

## NodeStatus State Machine

```
                    ┌──────────┐
               ┌───►│Standalone│
               │    └────┬─────┘
               │         │ run_election
               │         ▼
               │    ┌──────────┐  lost CAS / leader has valid lease
               │    │  Leader  │◄────────────────────────────────────┐
               │    └────┬─────┘                                     │
               │         │ renewal CAS                               │
               │         │ (lease_index++)                           │
               │         │                       ┌──────────────────►│
               │         ▼                       │ won CAS           │
               │    ┌──────────┐  S3 kick  ┌────┴────────────────┐  │
               │    │ Follower │──────────►│FollowerCatchingUp   │  │
               │    └────┬─────┘           └────────────────────┘   │
               │         │                  catchup complete         │
               │         │                        ▼                  │
               │    ┌──────────┐           ┌──────────────────┐     │
               └────┤  Fenced  │◄──────────│   BootCatchup    │     │
                    └──────────┘  any      └──────────────────┘     │
                         │        state ──► Fenced (TTL expiry)      │
                         └───────────────────────────────────────────┘
```

| State | Description | TTL-exempt |
|-------|-------------|-----------|
| `Standalone` | Single-node, no replication | yes |
| `Leader` | Holds S3 lease, accepts writes | no |
| `Follower` | Follows leader, accepts TCP replication | no |
| `FollowerCatchingUp` | Catching up from S3, rejects TCP replication | yes |
| `BootCatchup` | Pre-election S3 catchup at boot | yes |
| `Fenced` | Write-blocked; must re-run election to recover | yes |

## LeaseStore Trait

```rust
pub trait LeaseStore {
    async fn get_lease(&self) -> Result<Option<LeaseWithEtag>, LeaseStoreError>;
    async fn put_lease_create_only(&self, lease: &Lease) -> Result<String, LeaseStoreError>;
    async fn put_lease_conditional(&self, lease: &Lease, etag: &str) -> Result<String, LeaseStoreError>;
    async fn get_membership(&self) -> Result<Option<MembershipWithEtag>, LeaseStoreError>;
    async fn put_membership(&self, membership: &Membership, etag: Option<&str>) -> Result<(), LeaseStoreError>;
}
```

`etag: None` on `put_membership` → CreateOnly (file must not exist). `etag: Some(e)` → IfMatchETag CAS.

## S3 Paths

| Constant / Function | Path |
|---------------------|------|
| `LEASE_PATH` | `cluster/lease.bin` |
| `MEMBERSHIP_PATH` | `cluster/membership.bin` |
| `FALLBACK_PREFIX` | `cluster/fallback` |
| `fallback_batch_path(s, lo, hi)` | `cluster/fallback/shard_SSS/batch_LLLLLLLLL_HHHHHHHHH.bin` |
| `fallback_shard_prefix(s)` | `cluster/fallback/shard_SSS/` |

Fallback batch filenames are zero-padded so lexicographic order equals temporal order. Used for S3 replication fallback listing.

## Design Decisions

### S3 as consensus medium

No Raft, no Paxos, no coordinator process. A single CAS-protected S3 object (`cluster/lease.bin`) provides mutual exclusion. The S3 API's conditional write (`IfMatchETag`) guarantees exactly one winner per CAS round.

Trade-off: S3 round-trip (~50–200ms) determines election latency. Acceptable for HA failover where second-scale recovery is fine.

### Self-fencing via TTL

```rust
pub fn effective(&self) -> NodeStatus {
    match self.status {
        // TTL-exempt states
        NodeStatus::Standalone | NodeStatus::BootCatchup
        | NodeStatus::Fenced | NodeStatus::FollowerCatchingUp { .. } => self.status,
        // All others fence on expiry
        _ if now_ms() > self.expires_at_ms => NodeStatus::Fenced,
        _ => self.status,
    }
}
```

Shards check `effective()` before accepting writes. If shard 0 crashes mid-lease, surviving shards self-fence after `status_ttl_ms` without any coordination.

### Heartbeat TTL formula

```
status_ttl_ms = (heartbeat_interval × max_missed_heartbeats) + max_clock_drift
```

Sized so that a follower only fences after it has missed `max_missed_heartbeats` consecutive heartbeats, with extra headroom for clock skew between nodes.

### Membership CAS with retries

`register_self` retries up to 5 times on `PreconditionFailed`. In a 2-node cluster, one retry suffices (read the concurrent write, merge, retry). Extra retries guard transient S3 inconsistency. The S3 round-trip provides natural backoff — no sleep needed.

### TTL-exempt states

`FollowerCatchingUp` and `BootCatchup` are controlled by shard 0, which drives the catchup protocol and installs a fresh `ValidatedNodeStatus` when done. Applying TTL to these states would race with the catchup duration, causing spurious fencing.

### Election semantics

| Scenario | Outcome |
|----------|---------|
| No lease | Race `put_lease_create_only`; winner → Leader, loser reads again → Follower |
| Valid lease, other node | Follower unconditionally |
| Valid lease, own node | CAS-extend (boot/renewal); winner → Leader with incremented `lease_index` |
| Expired lease | CAS race; winner → Leader, loser reads winner's lease → Follower |
| CAS fails, lease vanishes | `Unavailable` error (pathological S3 inconsistency) |

## Dependencies

- `celeriant_wal` - `Lease`, `Membership`, `NodeInfo` types (S3 lease/membership wire format)
- `bincode` - Serialization for lease and membership objects
- `blake3` - Hashing (available for integrity checks on S3 objects)
