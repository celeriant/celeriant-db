# celeriant_wal

Core data structures for the Celeriant write-ahead log (WAL). This crate defines the event, batch, snapshot, and metadata types used throughout the server and client libraries.

## Overview

This crate provides the serializable data structures that form the foundation of Celeriant's storage and wire protocol. It contains no I/O logic—just types and their serialization implementations.

High-level layout:

```
WalMetablock (512 bytes, fixed-size)
├── EventBatchMetadata
│   ├── aggregate key + datablock pointer
│   ├── min/max ranges for filtering
│   ├── event type bloom filter or direct array
│   ├── compression info + minibatch fast-path
├── SnapshotOrg
├── SnapshotAggregateType
└── SnapshotAggregate

WalDatablock (variable-size)
├── EventBatch(EventBatchItem)
│   ├── batch-level headers (indexes, timestamps, identity)
│   └── Vec<EventItem)
├── SnapshotOrg
├── SnapshotAggregateType
└── SnapshotAggregate
```

Notes:
- **Metablocks** are always fixed-size and optimized for fast scanning/filtering without pulling large payloads from disk.
- **Datablocks** contain variable-length payloads (event batches and “full” snapshot payloads).

## Block Architecture

All blocks (both meta and data) are protected by a version byte and CRC32 checksum at the front. This enables:

- **Format upgrades**: Version byte allows forward-compatible schema evolution
- **Corruption detection**: CRC protects against bitrot and torn writes
- **Safe deserialization**: Validation happens before bincode decodes the payload

### Metablocks vs Datablocks

| Property | Metablock | Datablock |
|----------|-----------|-----------|
| Size | Fixed 512 bytes | Variable length |
| Growth direction | Forward from file start | Backward from file end |
| Purpose | Fast filtering, discovery, offsets | Event payloads, full snapshots |
| Compression | Never | Per-block optional |

### Block Types

**WalMetablock** variants:
- `EventBatchMetadata` — Filtering metadata for an event batch + pointer to datablock payload (or inline minibatch)
- `SnapshotOrg` — Organization/tenant discovery marker
- `SnapshotAggregateType` — Aggregate-type discovery marker (and whether schemas exist)
- `SnapshotAggregate` — Aggregate discovery + index/size tracking metadata

**WalDatablock** variants:
- `EventBatch` — The actual event batch payload (`EventBatchItem`)
- `SnapshotOrg` — Reserved for org-level snapshot payloads (currently empty in this crate)
- `SnapshotAggregateType` — Schema registry payloads (per aggregate type)
- `SnapshotAggregate` — Idempotency tracking payloads (per aggregate)

## Key Types

### EventItem

An individual event within a batch.

- `client_event_index` supports idempotent producers
- `event_index` is server-assigned ordering within an aggregate
- `event_value` is `Arc<Vec<u8>>` for cheap cross-thread moves
- `iv: Option<[u8; 12]>` supports per-event AES-GCM encrypted payloads (opaque to server)

### EventBatchItem

A batch of events from a single client, persisted atomically.

Headers include:
- `event_batch_index` (server-assigned, monotonically increasing per aggregate)
- `server_timestamp` (when persisted)
- `client_id` (machine identity)
- `user_id` (optional human identity)
- `node_id` + `lease_index` for fencing in replicated deployments

### EventBatchMetadata (metablock)

Metadata written alongside each batch for efficient filtering and validation. Stored in a **512-byte metablock**.

Includes:
- `AggregateKey` and `datablock_position` (where the batch payload lives)
- `uncompressed_size` / `compressed_size` and `compression_type`
- min/max ranges for binary-search and filtering:
  - `min/max_event_index`
  - `min/max_event_timestamp`
  - `min/max_client_event_index`
- `event_types_data` (bloom filter or direct list)

#### Minibatch fast-path

If the (encoded+compressed) batch payload is small enough, it can be inlined into the metablock’s `minibatch` area (256 bytes). This avoids an extra disk read for tiny batches.

### SnapshotAggregate (metablock)

Quick metadata about an aggregate, used for:
- Aggregate discovery (listing aggregates in an org)
- Write path index tracking (`last_event_index`, `last_event_batch_index`)
- Read path availability (`min_available_event_index`, `min_available_event_batch_index`)
- Size/timestamp “filesystem style” metadata

### SnapshotAggregate (datablock)

Full aggregate state snapshot payload used for recovery. Currently this contains idempotency tracking:

- `client_event_indexes: HashMap<u128, u64>` — last accepted `event_batch_index` per `client_id`

### SnapshotAggregateType (metablock + datablock)

Two layers:
- Metablock `SnapshotAggregateType` is the compact discovery record (`org_id`, `aggregate_type_id`, `has_schemas`).
- Datablock `SnapshotAggregateType` contains the actual schema registry payload:
  - `schemas: HashMap<u64, Option<EventTypeSchema>>`
  - `EventTypeSchema` tracks schema type and per-minor-version schemas.

### Shard Log Structures

The shard log structures define how WAL data is stored on disk. Each shard has its own WAL, split into multiple fixed-size files that rotate when full.

#### File Layout

```
┌─────────────────────────────────────────────────────────────┐
│ Header (512 bytes) - version + crc protected                │
├─────────────────────────────────────────────────────────────┤
│ Metablocks (512 bytes each, growing downward →)             │
│   [version][crc][WalMetablock bincode payload]              │
│   ...                                                       │
├─────────────────────────────────────────────────────────────┤
│                      Free space                             │
├─────────────────────────────────────────────────────────────┤
│                                    ... Datablocks           │
│   [version][crc][WalDatablock bincode payload]              │
│              (variable size, growing ← upward)              │
├─────────────────────────────────────────────────────────────┤
│ Header (512 bytes) - duplicate for torn write recovery      │
└─────────────────────────────────────────────────────────────┘
```

Files are preallocated. Metablocks grow from the top, Datablocks grow from the bottom. When they meet, the file rotates.

#### ShardLogHeader

Tracks file boundaries and current write positions:

```rust
pub struct ShardLogHeader {
    pub file_len: u64,           // Total file size (may differ after trimming)
    pub metablocks_position: u64, // End of last written metablock
    pub datablocks_position: u64, // Start of last written datablock
}
```

Written at both start and end of file, protected by CRC, enabling recovery from torn writes.

## Design Decisions

### Arc-wrapped event payloads

`event_value` is `Arc<Vec<u8>>` rather than `Vec<u8>` to avoid copying payload bytes across thread boundaries.

### Client identity vs user identity

Two identity fields serve different purposes:

| Field | Source | Purpose |
|-------|--------|---------|
| `client_id` | Machine identity | Connection/app identity and idempotency tracking |
| `user_id` | Human identity (optional) | Authorization/auditing |

Both are stored as truncated 128-bit identifiers for storage efficiency.

### Client event index for idempotency

`client_event_index` enables exactly-once semantics for clients that retry. The server can track the highest seen index per client and reject duplicates (policy-dependent).

### Dual timestamps

- `event_timestamp` (client): when the event occurred
- `server_timestamp` (server): when it was persisted

Use for filtering, not strict ordering.

### Optional event ID

`event_id: Option<u128>` is a client-supplied identifier for correlation/external references.

### Per-event encryption support

`iv: Option<[u8; 12]>` indicates the payload is encrypted (AES-GCM). The server stores encrypted payloads opaquely.

Avoid compression for encrypted payloads.

### Metadata separation

Batch metadata is stored separately from batch payload so the server can filter without decompressing/reading datablocks.

Event type filtering uses either:
- `Direct([u64; 4])` when a batch contains ≤ 4 unique event types (exact match)
- `Bloom([u64; 4])` when more types are present (false positives possible, no false negatives)

### Serialization choices

- Short serde keys reduce JSON/debug payload size
- `u128` fields are base64-encoded in JSON to preserve full precision across languages

## Dependencies

Minimal by design:
- `serde` - Serialization framework for attributes on structs
- `bincode` - Binary encoding/decoding (v2)
- `base64` - u128 JSON encoding support