# Exactly-Once Writes — Idempotency Guide

Celeriant provides exactly-once write semantics at the event store layer. This guide explains how to use `ClientEventIndex` and `EnforceClientIdempotency` correctly, and the internal behaviours your service relies on.

## The Problem

The classic failure mode in any event store:

1. Service writes event to Celeriant — succeeds
2. TCP connection drops before the ack reaches the service
3. Service doesn't know if the write landed
4. Service retries — duplicate event in the stream

Celeriant solves this with a client-controlled monotonic index per aggregate.

## How It Works

Every event you write carries a `ClientEventIndex` — a monotonically increasing integer you control. Combined with your `ClientId` and `EnforceClientIdempotency = true`, Celeriant guarantees that for a given `(AggregateKey, ClientId)`, no `ClientEventIndex` is accepted twice.

```
WriteAsync:
  AggregateKey    = (org, type, id)
  ClientId        = your service's identity
  ClientEventIndex = monotonically increasing per aggregate
  EnforceClientIdempotency = true
```

On duplicate, Celeriant returns `ClientIdempotencyViolation` with the `LastAcceptedClientEventIndex`. Your write already landed. Treat it as success.

## Deriving ClientEventIndex

During your read (catch-up), each `AggregateEventBatch` includes the `ClientId` that wrote it. Scan for batches matching your `ClientId` and track the max `ClientEventIndex` from those events. Use that value + 1 for your write.

No external sequence generator needed. The index is derived from the data you already read.

### Persisting in Your Projection Store

While you can derive `max(ClientEventIndex)` from a full aggregate scan on every request, this adds an extra read to Celeriant on every write path. A better approach: persist `last_client_event_index` alongside your projection state (e.g. in the same Postgres row as the account balance).

```
account_balances:
  account_id            UUID PRIMARY KEY
  balance_cents         BIGINT
  last_batch_index      BIGINT       -- catch-up cursor
  last_client_event_index BIGINT     -- max ClientEventIndex for our ClientId
```

Update it in two places:

1. **During catch-up replay** — as you replay new batches, scan any batch where `ClientId` matches yours and track the max `ClientEventIndex`. Persist it in the same UPSERT that updates the balance.

2. **After a successful write** — optimistically update the row with the `clientEventIndex` you just wrote.

This is safe because `last_client_event_index` and `last_batch_index` are always written atomically in the same row. If one is stale, the other is too — and the next catch-up replays the missing batches, correcting both. The value in Postgres is always <= the true max in Celeriant; it may lag, but catch-up replay brings it current before any write uses it.

### Concurrent Writers

Two service instances catching up the same aggregate at the same time will derive the same `max + 1`. This is fine — optimistic concurrency (OCC) handles it. One writer's `ExpectedEventBatchIndex` won't match after the other's write lands. The loser retries with fresh state, derives a new `max + 1`, and proceeds. See the retry strategy section for why the loser must **re-derive** on OCC (not hold the old value).

## Combining OCC and Idempotency

Most writes should use both:

```
WriteAsync:
  ExpectedEventBatchIndex  = batch index from your last read
  EnforceClientIdempotency = true
  ClientEventIndex         = max + 1 from your last read
```

These checks run in a specific order on the server, and that order matters.

### OCC Is Checked First

Celeriant validates `ExpectedEventBatchIndex` before `ClientEventIndex`. This is deliberate.

If two concurrent writers derive the same `ClientEventIndex` (same `max + 1` from the same catch-up), OCC catches the second writer — their `ExpectedEventBatchIndex` is stale. They get `OptimisticConcurrencyViolation`, not `ClientIdempotencyViolation`.

This distinction matters for your retry logic:

- **`OptimisticConcurrencyViolation`** — your read was stale. Catch up and retry. Safe to loop.
- **`ClientIdempotencyViolation`** — OCC passed (your read was current), but your `ClientEventIndex` was already used. This means your exact write already landed (crash recovery). Treat as success.

If idempotency were checked first, you couldn't tell these apart. A concurrent writer's event would look like your duplicate.

## The Write-Path / Read-Path Split

Celeriant maintains separate caches for the write path and read path. The write-path cache updates immediately when a write is accepted. The read-path cache updates after replication.

This split is critical for exactly-once semantics. Here's the scenario:

1. Your service writes an event — Celeriant accepts it, updates write-path cache
2. Replication begins but the TCP ack is lost — your service gets a timeout
3. Your service retries — catches up via the read path, which hasn't replicated yet
4. Your service derives the same `ExpectedEventBatchIndex` and `ClientEventIndex`
5. Celeriant checks OCC against the **write-path** cache — batch index already advanced
6. `OptimisticConcurrencyViolation` — retry blocked

Your retry can't succeed until replication completes and catch-up reveals the original event. At that point, catch-up absorbs the event, your projection updates, and the system proceeds with correct state.

No duplicate. No special recovery logic. The split between write visibility and read visibility creates natural backpressure that prevents exactly the class of bugs that plague other event stores.

## Retry Strategy

The retry loop must distinguish between two failure types, because they have opposite implications for `ClientEventIndex`:

- **OCC violation** — your write was definitively rejected. It never landed. Safe to re-derive `ClientEventIndex` from fresh state.
- **Timeout** — ambiguous. Your write may have landed but the ack was lost. You must hold `ClientEventIndex` constant so that `ClientIdempotencyViolation` can catch the already-landed write.

```
state = catch_up(aggregate)
client_event_index = state.max_client_event_index + 1
re_derive = false

for attempt in 1..MAX_RETRIES:
    if attempt > 1:
        state = catch_up(aggregate)  // fresh projection for retry
        if re_derive:
            client_event_index = state.max_client_event_index + 1
            re_derive = false

    if not valid(state, command):
        return validation_error

    try:
        write to Celeriant with OCC + idempotency
        return success
    catch OptimisticConcurrencyViolation:
        re_derive = true  // re-derive AFTER next catch-up, not before
        continue
    catch RequestTimeout:
        // DO NOT update client_event_index — hold constant
        continue
    catch ClientIdempotencyViolation:
        return success  // our write already landed (timeout recovery)

return conflict  // exhausted retries
```

### Why Re-Derive on OCC

If two concurrent requests derive the same `max + 1` and you hold `ClientEventIndex` constant on OCC retry, the loser catches up (sees the winner's event), retries with a fresh `ExpectedEventBatchIndex` (OCC passes), but hits `ClientIdempotencyViolation` from the winner's committed index. The loser falsely concludes "my write already landed" — but it was the other request's write. The loser's operation is **silently dropped**.

Re-deriving on OCC is safe because OCC rejection is unambiguous — your write was never accepted. The fresh `max + 1` from catch-up will be higher than the winner's index, and your retry succeeds with its own event.

### Why Hold Constant on Timeout

Timeout is ambiguous — your write may have been accepted but the ack was lost (the "K-FAIL" scenario). If you re-derived, the catch-up might reveal your landed event, advancing `max + 1` past it. Your retry would be accepted as a genuinely new event — a duplicate. Holding `ClientEventIndex` constant ensures `ClientIdempotencyViolation` catches the landed write.

### Transport Errors

Connection failures (refused, reset before send) are not ambiguous — the write never reached the server. These should propagate to the caller, not retry in this loop. The next request for that aggregate will catch up and see the correct state.

## Pattern: Offline-First Clients

A different pattern emerges when clients work offline and sync later. Here, the `ClientEventIndex` isn't derived from reading the aggregate — it's the client's own local sequence, generated as events are created offline.

### How It Works

Each client device gets a unique `ClientId`. Events are created locally with a monotonically increasing index — in practice, an auto-incrementing primary key from the local database (IndexedDB, SQLite, etc).

```
Client creates events offline:
  Event { ClientId: "device-A", ClientEventIndex: 1, ... }
  Event { ClientId: "device-A", ClientEventIndex: 2, ... }
  Event { ClientId: "device-A", ClientEventIndex: 3, ... }
```

The client tracks the highest index that's been successfully acknowledged by the server (`sentServerEventsUpTo`). On reconnect, it queries for events where `ClientEventIndex > sentServerEventsUpTo` and sends the batch.

### No OCC — Last Write Wins

Offline clients can't do optimistic concurrency. They didn't read the aggregate before writing — they were disconnected. So these writes skip `ExpectedEventBatchIndex` entirely. It's a last-write-wins model by design. The business domain has to tolerate concurrent offline edits.

This is a deliberate trade-off. Collaborative editing, field data collection, mobile apps — these domains accept that two users might edit the same thing offline and both edits land. Conflict resolution happens at the projection layer, not the write layer.

### Idempotency on Reconnect

The sync flow is at-least-once delivery made safe by `ClientEventIndex`:

1. User acts offline — local DB assigns `ClientEventIndex: 7, 8, 9`
2. Client comes online — queries events where `ClientEventIndex > 6` (last acked)
3. Sends batch with `ClientEventIndex: 7, 8, 9` to server
4. Network drops mid-send — client doesn't know if the batch landed
5. Client retries the same batch — same `ClientEventIndex: 7, 8, 9`
6. Celeriant sees `ClientEventIndex 7, 8, 9` from this `ClientId` again — `ClientIdempotencyViolation`
7. Treat as success — update `sentServerEventsUpTo` to 9

The `ClientEventIndex` is born at event creation time, not derived from a server read. It travels with the event through the local store, through the sync layer, and into Celeriant. Same index, same identity, from creation to persistence.

### Events From Other Clients

When a client receives events from the server that were written by other clients, it stores them locally without the originating `ClientEventIndex`. The local DB assigns fresh auto-increment values. This keeps the local sequence consistent — `ClientEventIndex` is a per-client concern, not a global one.

### Key Differences from Server-Side Pattern

| Aspect | Server-side (catch-up) | Offline-first |
|--------|----------------------|---------------|
| ClientEventIndex source | Derived from reading aggregate (max + 1) | Local DB auto-increment |
| OCC | Yes — `ExpectedEventBatchIndex` | No — last write wins |
| When index is assigned | At write time, after validation | At event creation time, offline |
| ClientId scope | Shared across service instances | Per device / per browser |
| Retry semantics | Internal loop with catch-up | Sync batch retry on reconnect |

### When to Use This Pattern

- Mobile apps with offline capability
- Collaborative tools where users edit concurrently
- Field data collection (inspections, surveys, asset tracking)
- Any domain where "both edits land, resolve later" is acceptable

Don't use this pattern when business invariants must be enforced at write time (e.g. account balance checks). Those need server-side validation with OCC.

## Note: HTTP-Level Idempotency

The server-side pattern doesn't protect against application-level retries from end users. If a browser user clicks "Transfer" and the HTTP response is lost, they'll click again. That's a new catch-up, a new `ClientEventIndex`. From Celeriant's perspective, it's a legitimate new write.

This is solved at the API layer — not in Celeriant. A short-lived in-memory cache of `idempotency_key → result` at the HTTP boundary covers button smashing and fast retries. This is a generic API concern, not specific to Celeriant. You'd do the same in front of any event store or database.

## Summary

Two patterns, one idempotency primitive.

| Pattern | ClientEventIndex Source | OCC | Use Case |
|---------|----------------------|-----|----------|
| Server-side with catch-up | Derived from aggregate read (max + 1), persisted in projection store | Yes | Backend services with validation |
| Offline-first sync | Local DB auto-increment | No | Mobile, collaborative, field apps |

| Mechanism | Layer | Protects Against |
|-----------|-------|-----------------|
| `ExpectedEventBatchIndex` (OCC) | Celeriant | Concurrent writers, stale reads |
| `ClientEventIndex` + `EnforceClientIdempotency` | Celeriant | Crash between write and ack, offline sync retries |
| Write-path / read-path cache split | Celeriant | Lost ack before replication |
| OCC retry re-derives / timeout retry holds constant | Application | Silent write drops (OCC) and duplicates (timeout) |
| `last_client_event_index` in projection store | Application | Extra Celeriant read on every write path |
