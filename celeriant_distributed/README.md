# celeriant_distributed

Distributed replication coordination for leader/follower HA. Provides S3-based lease election, membership discovery, heartbeat liveness tracking, and node status state machine. No Raft/Paxos — a single CAS-protected S3 object grants leader exclusivity.

## Architecture

```
         ┌─────────────────────────────────────────────┐
         │              S3 Bucket                       │
         │  cluster/lease.json      (Lease, CAS-guarded) │
         │  cluster/membership.json (NodeInfo per node)  │
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

## Module Structure

| Module | Purpose |
|--------|---------|
| `config` | `ReplicationConfig` with builder pattern and validation |
| `node_status` | `NodeStatus` enum and state machine transition rules |
| `validated_node_status` | `ValidatedNodeStatus` wrapper with TTL-based fencing |
| `lease_store` | `LeaseStore` trait, `LeaseWithEtag`, `MembershipWithEtag`, `LeaseStoreError` |
| `lease_manager` | `LeaseManager<S>`, `ElectionOutcome`, election/membership logic |
| `heartbeat` | `HeartbeatLeaseTracker`, `now_ms()` |
| `paths` | S3 path constants and fallback batch path generation |

## Key Types

| Type | Purpose |
|------|---------|
| `ReplicationConfig` | Node ID, addresses, shard count, lease durations, heartbeat intervals, drift tolerance |
| `HeartbeatLeaseTracker` | Tracks peer liveness via wall-clock deadline; not started until first `start()` |
| `LeaseStore` | Trait abstracting S3 reads/writes for lease and membership objects |
| `LeaseWithEtag` | Lease + ETag for CAS-protected updates |
| `MembershipWithEtag` | Membership + ETag for CAS-protected updates |
| `LeaseStoreError` | `AlreadyExists`, `PreconditionFailed`, `Unavailable { message }` |
| `LeaseManager<S>` | Orchestrates election, membership registration, peer discovery |
| `ElectionOutcome` | Result of `run_election`: `ValidatedNodeStatus` + optional peer `NodeInfo` |
| `NodeStatus` | Role enum with associated lease data (see state machine below) |
| `ValidatedNodeStatus` | `NodeStatus` + TTL; `effective()` decays to `Fenced` when expired |

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

| Method | Purpose |
|--------|---------|
| `is_leader()` | True for `Leader` |
| `is_follower()` | True for `Follower` only (not `FollowerCatchingUp`) |
| `is_any_follower_state()` | True for `Follower` or `FollowerCatchingUp` |
| `is_catching_up()` | True for `BootCatchup` or `FollowerCatchingUp` |
| `is_fenced()` | True for `Fenced` |
| `is_standalone()` | True for `Standalone` |
| `same_role(&self, other)` | True if same variant (ignores inner data) |
| `lease_index()` | `Some(index)` for `Leader`/`Standalone`, `None` otherwise |
| `lease_index_for_logging()` | Extracts lease_index or 0 |
| `is_valid_transition_to(new)` | State machine guard; validates role transitions |

### State Machine

```
  ┌────────────┐         ┌────────────┐
  │ Standalone │         │ BootCatchup│
  └─────┬──────┘         └──┬─────┬───┘
        │ election           │     │ election
        ├────────┐      ┌────┘     └────┐
        ▼        ▼      ▼              ▼
  ┌──────────┐  ┌──────────┐
  │  Leader  │◄─┤ Follower │◄───────────────────────┐
  └──┬───┬───┘  └──┬────┬──┘                        │
     │   │         │    │ S3 kick                    │
     │   │         │    ▼                            │
     │   │         │  ┌─────────────────────┐        │
     │   │         │  │FollowerCatchingUp   ├────────┘
     │   │         │  └─────────────────────┘ catchup
     │   │         │        complete
     │   └─────────┼───► Leader (renewal, lease_index++)
     │  lost CAS   │
     └─────────────┼───► Follower (lease_index update)
                   │
     ┌─────────────┘
     ▼ any state (TTL expiry or emergency)
  ┌──────────┐
  │  Fenced  │──► Leader | Follower | BootCatchup
  └──────────┘    (re-election / boot)
```

Valid transitions from code (`is_valid_transition_to`):

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

## ValidatedNodeStatus

Wraps `NodeStatus` with a TTL expiry. Constructed via `new(status, expires_at_ms)` or convenience constructors `fenced()`, `standalone()`, `boot_catchup()`.

| Method | Purpose |
|--------|---------|
| `effective()` | Returns `Fenced` if TTL elapsed; TTL-exempt states pass through |
| `raw()` | Raw status without time check |
| `can_accept_writes()` | True if effective status is `Leader` or `Standalone` |
| `expires_at_ms()` | Expiry timestamp |
| `is_leader()`, `is_follower()`, `is_fenced()`, `is_standalone()`, `is_catching_up()`, `is_any_follower_state()` | Delegate to inner `NodeStatus` |

## Key Functions

| Function | Purpose |
|----------|---------|
| `LeaseManager::run_election()` | Race for leader or follow; handles all CAS outcomes |
| `LeaseManager::register_self()` | CAS read-modify-write to add self to membership (up to `MEMBERSHIP_CAS_MAX_RETRIES` retries) |
| `LeaseManager::discover_peer()` | Read membership and return peer `NodeInfo` |
| `ReplicationConfig::validate()` | Checks timing consistency: drift < timeout, 2x interval < timeout, lease bounds |
| `ReplicationConfig::heartbeat_timeout()` | `heartbeat_interval * max_missed_heartbeats` |
| `ReplicationConfig::status_ttl_ms()` | `heartbeat_timeout + max_clock_drift` — how long shards trust their status |
| `HeartbeatLeaseTracker::start()` | Set initial timestamp on connection established |
| `HeartbeatLeaseTracker::record_received()` | Refresh deadline on each heartbeat/ack |
| `HeartbeatLeaseTracker::is_expired()` | True if `now > last_received + lease_duration + clock_drift`; false if not started |
| `HeartbeatLeaseTracker::reset()` | Clear state on reconnect or role change |
| `now_ms()` | Unix epoch milliseconds via `SystemTime` |
| `fallback_batch_path(shard, start, end)` | S3 path for fallback replication batch |
| `fallback_shard_prefix(shard)` | S3 list prefix for all batches on a shard |

## ReplicationConfig

```rust
pub struct ReplicationConfig {
    pub node_id: u128,
    pub client_address: String,
    pub replication_address: String,
    pub num_shards: u32,
    pub initial_lease_duration: Duration,
    pub min_lease_duration: Duration,
    pub max_lease_duration: Duration,
    pub max_clock_drift: Duration,
    pub replication_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub max_missed_heartbeats: u32,
}
```

Builder methods: `with_node_id(u128)`, `with_addresses(client, replication)`.

**Defaults**: node_id=0, client=`0.0.0.0:10000`, replication=`0.0.0.0:10001`, num_shards=1, initial_lease=5s, min_lease=1s, max_lease=30s, max_clock_drift=500ms, replication_timeout=2s, heartbeat_interval=500ms, max_missed_heartbeats=3.

**Validation rules** (`validate()`):
- `heartbeat_timeout > 2x heartbeat_interval`
- `max_clock_drift < heartbeat_timeout`
- `min_lease_duration < max_lease_duration`
- `initial_lease_duration` in range `[min, max]`

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

## Constants

| Constant | Value | Purpose |
|----------|-------|---------|
| `CLUSTER_PREFIX` | `"cluster"` | S3 key prefix for all cluster objects |
| `LEASE_PATH` | `"cluster/lease.json"` | S3 key for lease object |
| `MEMBERSHIP_PATH` | `"cluster/membership.json"` | S3 key for membership object |
| `FALLBACK_PREFIX` | `"cluster/fallback"` | S3 key prefix for fallback replication |
| `MEMBERSHIP_CAS_MAX_RETRIES` | 5 | Max CAS retries for `register_self` |

## S3 Paths

| Function | Path |
|----------|------|
| `fallback_batch_path(s, lo, hi)` | `cluster/fallback/shard_SSS/batch_LLLLLLLLL_HHHHHHHHH.bin` |
| `fallback_shard_prefix(s)` | `cluster/fallback/shard_SSS/` |

Fallback batch filenames are zero-padded so lexicographic order equals temporal order. Used for S3 replication fallback listing.

## Design Decisions

### S3 as consensus medium

No Raft, no Paxos, no coordinator process. A single CAS-protected S3 object (`cluster/lease.json`) provides mutual exclusion. The S3 API's conditional write (`IfMatchETag`) guarantees exactly one winner per CAS round.

Trade-off: S3 round-trip (~50-200ms) determines election latency. Acceptable for HA failover where second-scale recovery is fine.

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
status_ttl_ms = (heartbeat_interval * max_missed_heartbeats) + max_clock_drift
```

Sized so that a follower only fences after it has missed `max_missed_heartbeats` consecutive heartbeats, with extra headroom for clock skew between nodes.

### Membership CAS with retries

`register_self` retries up to `MEMBERSHIP_CAS_MAX_RETRIES` (5) times on `PreconditionFailed`. In a 2-node cluster, one retry suffices (read the concurrent write, merge, retry). Extra retries guard transient S3 inconsistency. The S3 round-trip provides natural backoff — no sleep needed.

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

### Lease index in NodeStatus

`Leader`, `Follower`, and `FollowerCatchingUp` carry the lease index from the S3 lease object. This enables lease fencing — a node can reject replication from a leader with a stale lease index, and leaders can detect when they've been superseded.

## Feature Flags

| Feature | Default | Purpose |
|---------|---------|---------|
| `small-metablock` | off | Propagates to `celeriant_wal` for testing with smaller block sizes |

## Dependencies

- `celeriant_wal` - `Lease`, `Membership`, `NodeInfo` types (S3 lease/membership wire format)
- `bincode` - Serialization for lease and membership objects
- `blake3` - Hashing for integrity checks
