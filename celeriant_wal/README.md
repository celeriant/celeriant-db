# celeriant_wal

Data structures for the Celeriant write-ahead log. No I/O logic—just types and serialization.

**LLM GENERATED -> HUMAN REVIEWED 2025-12-20**

## Architecture

```
Shard Log File (1GB, fixed size)
┌───────────────────────────────────────────┐
│ Header (512 bytes)                        │
│    [crc][version][bincode payload]        │
├───────────────────────────────────────────┤
│ Metablocks → (512 bytes each, grow →)     │
│    [crc][version][bincode payload]        │
├───────────────────────────────────────────┤
│              Free space                   │
├───────────────────────────────────────────┤
│ ← Datablocks (variable size, grow ←)      │
│           [bincode payload]               │
├───────────────────────────────────────────┤
│ Header (512 bytes, duplicate for recovery)│
│    [crc][version][bincode payload]        │
└───────────────────────────────────────────┘
```

**Metablocks** (512 bytes fixed): Fast filtering/discovery without reading payloads.  
**Datablocks** (variable): Actual event payloads and snapshot data.

When they meet, file rotates. Files could be less than 1GB if they get trimmed and compacted.

## Key Types

| Type | Layer | Purpose |
|------|-------|---------|
| `Metablock` | Meta | Container with wal_index, timestamps, lease_index, node_id |
| `MetablockEventBatch` | Meta | Filtering metadata (min/max ranges, bloom filter, aggregate key) |
| `MetablockSnapshotAggregate` | Meta | Aggregate discovery + index tracking |
| `Datablock` | Data | Container for variable-length payloads |
| `DatablockAggregateEventBatch` | Data | Batch of events from one client |
| `DatablockAggregateEvent` | Data | Single event with payload |

## Design Decisions

### Client identity vs user identity

Two identity fields serve different purposes:

| Field | Source | Purpose |
|-------|--------|---------|
| `client_id` | Machine identity | Connection/app identity and idempotency tracking |
| `user_id` | Human identity (optional) | Authorization/auditing |

Both are stored as truncated 128-bit identifiers for storage efficiency.

### Indexes Explained

| Field | Assigned By | Purpose |
|-------|-------------|---------|
| `client_event_index` | Client | Idempotency—server rejects duplicates per client |
| `event_index` | Server | Ordering of individual events within an aggregate |
| `event_batch_index` | Server | Batch ordering within an aggregate |
| `wal_index` | Server | Global ordering across all metablocks in shard |

### Why `Arc<Vec<u8>>` for event_value?

Avoids copying payload bytes when moving events across thread boundaries.

### Why dual timestamps?

- `event_timestamp`: Client-assigned, when event occurred
- `server_timestamp`: Server-assigned, when persisted

### Minibatch optimization

If encoded, uncompressed batch ≤256 bytes, it's stored inline in the metablock via `DatablockInlineData`, avoiding an extra disk read.

### Optional event ID

`event_id: Option<u128>` is a client-supplied identifier for correlation/external references.

### Per-event encryption support

`iv: Option<[u8; 12]>` indicates the payload is encrypted (AES-GCM). The server stores encrypted payloads opaquely.

### Event type filtering

`EventTypesKind::Direct` for ≤4 unique types (exact match), `EventTypesKind::Bloom` for more (bloom filter).

## Snapshot Types

Snapshots avoid replaying entire WAL on startup:

- `SnapshotAggregate`: Idempotency tracking (`client_id → last_event_batch_index`)
- `SnapshotAggregateType`: Schema registry per event type
- `SnapshotOrg`: Reserved for org-level state

## Dependencies

- `bincode` - Rust-native binary serialization
- `base64` - Byte array to strings
- `serde` - Serialization framework
- `deepsize` - Struct+heap sizes of objects in memory