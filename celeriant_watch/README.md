# celeriant_watch

Watch/subscription system for real-time aggregate change notifications. Handles client subscriptions, event filtering, batching, and delivery.

## Architecture

```
ShardWriteAheadLog                    WatchSession (per client)
       │                                      │
       │ notify_watchers(HashMap<...>)        │
       ▼                                      │
┌───────────────────┐                         │
│ AggregateWatchers │                         │
│  (per shard)      │                         │
└────────┬──────────┘                         │
         │ filters by org/type/aggregate/op   │
         ▼                                    │
┌──────────────────┐    LocalChannel    ┌─────┴─────────────┐
│  WatcherHandle   │───────────────────>│ SubscribedClient  │
│  (per subscriber)│   (bounded 10k)    │ (batches events)  │
└──────────────────┘                    └─────┬─────────────┘
                                              │ latency timer
                                              ▼
                                        WatchResponse
                                        (to TCP client)
```

**Hot path**: WAL write → notify_watchers → broadcast → filter → try_send (non-blocking)
**Cold path**: Client accumulates events → waits for latency → flushes response

## Invariants

- Watch subscriptions are shard-local. No cross-shard fan-out at the server level.
- Watch events fire after the write is durably replicated (leader) or after fsync (non-leader). Never before.
- Each client has a bounded channel of 10,000 pending events (`MAX_PENDING_EVENTS`). This is a hard cap.
- The write path uses non-blocking `try_send()`. If the channel is full, the client is immediately removed from the watcher list. No backpressure propagates to the writer.
- Broadcast filtering runs before `try_send()`. The write hot path never blocks on watch consumers.
- Watch subscriptions are not included in the per-shard memory budget.

## Key Types

| Type | Purpose |
|------|---------|
| `AggregateWatchers` | Per-shard registry of all active subscribers |
| `WatcherHandle` | Subscription filters + sender channel |
| `SubscribedClient` | Event accumulator with latency-based batching |
| `WatchSession` | Async session loop coordinating receive/flush/heartbeat |
| `AggregateWatchEvent` | Internal event with aggregate key + operation |
| `AggregateWatchEventOperation` | Enum of operation variants (Write, Read, Delete, TrimStart, AggregateDetails, Create) |
| `WatchEventAccumulator` | Merges events by (AggregateKey, operation) before flattening to `WatchResponse` |
| `WatchOutputType` | Session output: Response, Heartbeat, Done, Continue |

## Design Decisions

### Backpressure via channel capacity

```rust
pub const MAX_PENDING_EVENTS: usize = 10000;
```

Bounded local channels. If a client can't keep up, `try_send` fails and the subscriber is removed. No blocking in the WAL hot path.

### Event batching with latency control

Clients specify `requested_latency_ms`. Events accumulate until:
1. Latency timer expires since last send
2. Response is flushed to client

Reduces network overhead for high-frequency writes.

### Filter hierarchy

```rust
pub struct WatcherHandle {
    pub id: u64,
    pub local_sender_channel: LocalSender<AggregateWatchEvent>,
    pub orgs: Option<HashSet<u128>>,
    pub aggregate_types: Option<HashSet<u128>>,
    pub aggregates: Option<HashSet<u128>>,
    pub operation_types: Option<HashSet<u8>>,
}
```

`None` = match all. Filters are AND'd across categories: all non-None filters must match. Within a set, matching is OR (any element). Operation type filter applied first (cheapest check). Filter values for `aggregate_types` and `aggregates` are raw `u128` IDs, not typed key structs.

### Event merging

Multiple operations on the same aggregate are merged in `WatchEventAccumulator`, then flattened to a `WatchResponse` via `into_response()`. The map key is `(AggregateKey, operation_u8)`:

| Operation | Merge Strategy |
|-----------|---------------|
| Write | Extend batch index range (min from, max to) |
| Read | Extend batch index range (None to treated as open-ended) |
| TrimStart | Replace (destructive, latest wins) |
| Delete | Deduplicate (no payload, stored as None) |
| AggregateDetails | Deduplicate (no payload, stored as None) |
| Create | Deduplicate (no payload, stored as None) |

### Operation type constants

```rust
impl AggregateWatchEvent {
    pub const DELETE: u8 = 0;
    pub const WRITE: u8 = 1;
    pub const READ: u8 = 2;
    pub const TRIM_START: u8 = 3;
    pub const DETAILS: u8 = 4;
    pub const CREATE: u8 = 5;
}
```

Compact representation for wire format and filter sets.

### Single-threaded design

Uses `Rc<RefCell<_>>` and glommio `LocalSender`/`LocalReceiver`. Each shard runs on a dedicated CPU core - no cross-thread synchronization needed.

### Session lifecycle

```rust
impl<R: AggregateReader> Drop for WatchSession<R> {
    fn drop(&mut self) {
        self.cleanup(); // removes subscriber from AggregateWatchers
    }
}
```

RAII cleanup ensures subscribers are removed when session ends (client disconnect, error, etc.).

### WatchOutputType state machine

| State | Meaning |
|-------|---------|
| `Response(WatchResponse)` | Batched events ready, send to client |
| `Heartbeat` | Timeout with no events, send keepalive |
| `Continue` | Events received but latency not met, keep waiting |
| `Done` | Channel closed, clean shutdown |

### AggregateReader trait

```rust
pub trait AggregateReader {
    fn watched_aggregates(&self) -> Rc<AggregateWatchers>;
}
```

Abstraction allowing `WatchSession` to work with any WAL implementation. Implemented by `ShardWriteAheadLog`.

