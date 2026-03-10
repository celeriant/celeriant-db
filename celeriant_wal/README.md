# celeriant_wal

Data structures for the Celeriant write-ahead log. No I/O logic—just types and serialization.

## Architecture

```
Shard Log File (1GB, fixed size)
┌───────────────────────────────────────────┐
│ Header (512KB)                            │
│    [crc][version][bincode payload]        │
│    includes 256KB aggregate bloom filter  │
├───────────────────────────────────────────┤
│ Metablocks → (1024 bytes each, grow →)    │
│    [crc][version][bincode payload]        │
├───────────────────────────────────────────┤
│              Free space                   │
├───────────────────────────────────────────┤
│ ← Datablocks (variable size, grow ←)      │
│           [bincode payload]               │
├───────────────────────────────────────────┤
│ Header (512KB, duplicate for recovery)    │
│    [crc][version][bincode payload]        │
└───────────────────────────────────────────┘
```

**Metablocks** (1024 bytes fixed): Fast filtering/discovery without reading payloads.
**Datablocks** (variable): Actual event payloads and snapshot data.

When they meet, file rotates. Files could be less than 1GB if they get trimmed and compacted.

The header is duplicated at both start and end of file. On torn writes, CRC checks on both copies allow recovery.

## Key Types

### Core WAL Types

| Type | Layer | Purpose |
|------|-------|---------|
| `Metablock` | Meta | Container with wal_index, timestamps, lease_index, node_id, previous_tip_hash |
| `MetablockKind` | Meta | Enum: EventBatchMetadata, SnapshotOrg, SnapshotAggregateType, SnapshotAggregate, SoftDelete, SoftTrim |
| `MetablockEventBatch` | Meta | Filtering metadata (min/max ranges, bloom filter, aggregate key) |
| `MetablockSnapshotAggregate` | Meta | Cached aggregate position: last indexes, sizes, creation metadata |
| `MetablockSnapshotAggregateType` | Meta | Aggregate type snapshot: aggregate_type_key, has_schemas flag |
| `MetablockSnapshotOrg` | Meta | Org snapshot: org_id |
| `MetablockSoftDelete` | Meta | Marks aggregate deleted: allow_recreate, allow_index_continuation, last indexes |
| `MetablockSoftTrim` | Meta | Marks minimum available batch: keep_from_event_batch_index |
| `DatablockStorageKind` | Meta | Enum: None, Inline(DatablockInlineData), Block(DatablockBlockRef) |
| `DatablockInlineData` | Meta | 512-byte inline payload stored within the metablock |
| `DatablockBlockRef` | Meta | Reference to a separate datablock (crc32c checksum) |
| `Datablock` | Data | Container wrapping a DatablockKind |
| `DatablockKind` | Data | Enum: EventBatchItem, SnapshotOrg, SnapshotAggregateType, SnapshotAggregate |
| `DatablockAggregateEventBatch` | Data | Batch of events from one client |
| `DatablockAggregateEvent` | Data | Single event with payload |
| `ShardLogHeader` | Header | metablocks_position, datablocks_position, wal_index, tip_hash, aggregate_bloom |

### Key Types

| Type | Wire Size | Purpose |
|------|-----------|---------|
| `AggregateKey` | 48 bytes | Composite key: (org_id, aggregate_type_id, aggregate_id). Hash pre-computed, not serialized |
| `AggregateClientKey` | 64 bytes | AggregateKey + client_id. Hash pre-computed, not serialized |
| `AggregateTypeKey` | 32 bytes | (org_id, aggregate_type_id). Hash pre-computed, not serialized |
| `EntryHashBytes` | 32 bytes | Type alias `[u8; 32]` for Blake3 hash chain entries |

### Compression and Encoding

| Type | Purpose |
|------|---------|
| `CompressionType` | Enum: None, Zstd{level}, Snappy, Brotli{level}, Gzip{level} |
| `EventTypesKind` | Enum: Direct([u64; 4]) for ≤4 types, Bloom([u64; 4]) for more |

## S3 / Cluster Types

Stored in the `s3` module. Used for leader election and replication fallback.

| Type | S3 Path | Purpose |
|------|---------|---------|
| `Lease` | `cluster/lease.json` | Leader election state: leader_node_id, lease_index, acquired_at_ms, expires_at_ms |
| `Membership` | `cluster/membership.json` | Two-node cluster state: array of 2 `Option<NodeInfo>` |
| `NodeInfo` | — | Single node: node_id, client_address, replication_address |
| `FallbackBatch` | — | S3 replication fallback: fallback_index, end_wal_index, shard_id, items |
| `FallbackItem` | — | One entry in a FallbackBatch: metablock + optional datablock |

### Lease Methods

| Method | Description |
|--------|-------------|
| `new_initial(leader_node_id, now_ms, duration_ms)` | Create first lease with lease_index=1 |
| `promote(new_leader_node_id, now_ms, duration_ms)` | Create successor lease with incremented index |
| `is_expired(now_ms)` | True when now_ms >= expires_at_ms |
| `remaining_millis(now_ms)` | Milliseconds until expiry (0 if expired) |
| `supersedes(our_index, our_node_id)` | True if this lease has higher index and different leader |

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

Timestamps are by default Unix epoch ms. But the server could be configured to be us, ns or have a different offset.

### Minibatch optimization

If encoded, uncompressed batch ≤512 bytes (`MINIBATCH_SIZE_BYTES`), it's stored inline in the metablock via `DatablockInlineData`, avoiding an extra disk read. `DatablockStorageKind::Inline` holds a 512-byte fixed array.

### Optional event ID

`event_id: Option<u128>` is a client-supplied identifier for correlation/external references.

### Per-event encryption support

`iv: Option<[u8; 12]>` indicates the payload is encrypted (AES-GCM). The server stores encrypted payloads opaquely.

### Event type filtering

`EventTypesKind::Direct` for ≤4 unique types (exact match), `EventTypesKind::Bloom` for more (bloom filter). Both variants use the same `[u64; 4]` storage (32 bytes = `BLOOM_BYTES`), selected by discriminant.

### Pre-computed hashes on composite keys

`AggregateKey`, `AggregateClientKey`, and `AggregateTypeKey` all store a `hash: u64` field computed at construction time. The hash is **not serialized** — it is recomputed on `Decode`. This avoids repeated hashing on hot lookup paths.

### Dual bloom filters

Two separate bloom filters serve different purposes:

| Filter | Size | Hash count | Location | Purpose |
|--------|------|------------|----------|---------|
| Event type bloom | 32 bytes (4 hashes) | 4 | `MetablockEventBatch.event_types_data` | Filter event batches by event type within a log segment |
| Aggregate bloom | 256KB (10 hashes) | 10 | `ShardLogHeader.aggregate_bloom` | Skip entire log segments during aggregate existence checks |

A "definitely not in set" result for the aggregate bloom means no metablocks for that aggregate exist in the segment.

### Blake3 hash chain

`tip_hash` in `ShardLogHeader` links entries via:

```
GENESIS_HASH = [0u8; 32]
tip_hash(n) = Blake3(tip_hash(n-1) || metablock_n)
```

The `previous_tip_hash` field in each `Metablock` records the hash of the prior entry, forming a tamper-evident chain. `datablock_position` is excluded from hash computation because it varies between nodes.

### Snapshot metablocks

`MetablockKind` has three snapshot variants that cache state directly in the WAL without requiring a full replay:

- `SnapshotAggregate` — caches last event/batch indexes, sizes, creation metadata for fast aggregate discovery and write-path index tracking
- `SnapshotAggregateType` — records whether the type has schemas defined (`has_schemas: bool`)
- `SnapshotOrg` — placeholder for org-level metadata

### Soft operations

- `MetablockSoftDelete` — marks an aggregate deleted. `allow_recreate` permits a new aggregate with the same key. `allow_index_continuation` permits new events to continue from the last index rather than resetting.
- `MetablockSoftTrim` — records `keep_from_event_batch_index`; older batches are logically invisible.

## Constants

| Constant | Value | Purpose |
|----------|-------|---------|
| `FIXED_BLOCK_SIZE_BYTES` | 1024 | Fixed size of each metablock on disk |
| `HEADER_BLOCK_SIZE_BYTES` | 524288 (512KB) | Size of header block (accommodates 256KB bloom filter) |
| `MINIBATCH_SIZE_BYTES` | 512 | Max inline payload size stored within a metablock |
| `BLOOM_BYTES` | 32 | Event type bloom filter size |
| `BLOOM_HASH_COUNT` | 4 | Hash functions for event type bloom |
| `AGGREGATE_BLOOM_BYTES` | 262144 (256KB) | Aggregate bloom filter size; <1% false positive at 200K entries |
| `AGGREGATE_BLOOM_HASH_COUNT` | 10 | Hash functions for aggregate bloom |
| `GENESIS_HASH` | `[0u8; 32]` | Starting hash for the tip_hash chain |
| `FIRST_EVENT_BATCH_INDEX` | 1 | First valid event batch index |

## Dependencies

- `bincode` - Rust-native binary serialization
- `base64` - Byte array to strings
- `serde` - Serialization framework
- `deepsize` - Struct+heap sizes of objects in memory
