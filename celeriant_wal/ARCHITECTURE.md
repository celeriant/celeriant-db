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
**Datablocks** (variable): Actual event payloads and schema data.

When they meet, file rotates. Files could be less than 1GB if they get trimmed and compacted.

The header is duplicated at both start and end of file. On torn writes, CRC checks on both copies allow recovery.

## Invariants

- WAL entries are globally contiguous within a shard. Each new entry receives exactly `current_wal_seq + 1`. Gaps are fatal.
- Every metablock carries `previous_tip_hash`, forming a Blake3 hash chain over the entire WAL history.
- Hash computation excludes `datablock_position` so leader and follower produce identical hashes despite different on-disk layouts.
- Metablocks are fixed-size (1024 bytes). Unused trailing bytes are zero-padded.
- Datablocks use dual storage: inline (up to 512 bytes, stored within the metablock) or external (written to end of file, growing backward). External datablocks carry their own CRC32C.
- `AggregateKey` serializes as 3 contiguous u128 LE values (48 bytes). The in-memory `hash` field is never serialized.
- Enum discriminants are 4-byte u32 (fixed-int encoding). `Option<T>` is 1-byte discriminant + T.
- `lease_epoch` is strictly monotonically increasing and never reused. A fresh cluster starts at `lease_epoch = 1`.
- Membership is a fixed 2-slot array in S3. A third node cannot join.

## Key Types

### Core WAL Types

| Type | Purpose |
|------|---------|
| `Metablock` | Container with wal_seq, server_timestamp, lease_epoch, node_id, previous_tip_hash, compression info |
| `MetablockKind` | Enum: EventBatchMetadata, SchemaRegistration, SoftDelete, SoftTrim |
| `MetablockEventBatch` | Filtering metadata (min/max ranges, bloom filter, aggregate key, client/user ids) |
| `DatablockStorageKind` | Enum: None, Inline(DatablockInlineData), Block(DatablockBlockRef) |
| `Datablock` | Container wrapping a DatablockKind |
| `DatablockKind` | Enum: EventBatchItem, SchemaRegistration |
| `ShardLogHeader` | metablocks_position, datablocks_position, wal_seq, tip_hash, aggregate_bloom |

### Composite Keys

| Type | Wire Size | Purpose |
|------|-----------|---------|
| `AggregateKey` | 48 bytes | (org_id, aggregate_type_id, aggregate_id). Hash pre-computed, not serialized |
| `AggregateClientKey` | 64 bytes | AggregateKey + client_id. Hash pre-computed, not serialized |
| `AggregateTypeKey` | 32 bytes | (org_id, aggregate_type_id). Hash pre-computed, not serialized |
| `SchemaKey` | 48 bytes | (org_id, aggregate_type_id, event_type_major, event_type_minor). Hash pre-computed, not serialized |
| `EntryHashBytes` | 32 bytes | `[u8; 32]` alias for Blake3 hash chain entries |

### S3 / Cluster Types

| Type | S3 Path | Purpose |
|------|---------|---------|
| `Lease` | `cluster/lease.json` | Leader election state: leader_node_id, lease_epoch, acquired_at_ms, expires_at_ms |
| `Membership` | `cluster/membership.json` | Two-node cluster state: array of 2 `Option<NodeInfo>` |
| `FallbackBatch` | — | S3 replication fallback: fallback_index, end_wal_seq, shard_id, items |

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
| `client_seq` | Client | Idempotency—server rejects duplicates per client |
| `event_seq` | Server | Ordering of individual events within an aggregate |
| `aggregate_version` | Server | Batch ordering within an aggregate |
| `wal_seq` | Server | Global ordering across all metablocks in shard |

### Why `Arc<Vec<u8>>` for event_value?

Avoids copying payload bytes when moving events across thread boundaries.

### Why dual timestamps?

- `event_timestamp`: Client-assigned, when event occurred
- `server_timestamp`: Server-assigned, when persisted

Timestamps are by default Unix epoch ms. But the server could be configured to be us, ns or have a different offset.

### Minibatch optimization

If encoded, compressed batch is less than 512 bytes (`MINIBATCH_SIZE_BYTES`), it's stored inline in the metablock via `DatablockInlineData`, avoiding an extra disk read. `DatablockStorageKind::Inline` holds a 512-byte fixed array.

### Optional event ID

`event_id: Option<u128>` is a client-supplied identifier for correlation/external references.

### Per-event encryption support

`iv: Option<[u8; 12]>` indicates the payload is encrypted (AES-GCM). The server stores encrypted payloads opaquely.

### Event type filtering

`EventTypesKind::Direct` for ≤4 unique types (exact match), `EventTypesKind::Bloom` for more (bloom filter). Both variants use the same `[u64; 4]` storage (32 bytes = `BLOOM_BYTES`), selected by discriminant.

### Pre-computed hashes on composite keys

`AggregateKey`, `AggregateClientKey`, `AggregateTypeKey`, and `SchemaKey` all store a `hash: u64` field computed at construction time. The hash is **not serialized** — it is recomputed on `Decode`. This avoids repeated hashing on hot lookup paths.

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

### Schema registration

`MetablockKind::SchemaRegistration` pairs with `DatablockKind::SchemaRegistration` to store event schemas in the WAL. `MetablockSchemaRegistration` carries the `SchemaKey` (org + aggregate type + event type major/minor) plus client/user identity. `DatablockSchemaRegistration` holds the `SchemaType` (Json, Avro, Protobuf) and the schema definition string.

### Soft operations

- `MetablockSoftDelete` — marks an aggregate deleted. `allow_recreate` permits a new aggregate with the same key. `allow_sequence_continuation` permits new events to continue from the last index rather than resetting.
- `MetablockSoftTrim` — records `keep_from_aggregate_version`; older batches are logically invisible.

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
| `FIRST_AGGREGATE_VERSION` | 1 | First valid aggregate version |

### `small-metablock` feature

When enabled, reduces `FIXED_BLOCK_SIZE_BYTES` to 512 and `MINIBATCH_SIZE_BYTES` to 128. Used for testing.
