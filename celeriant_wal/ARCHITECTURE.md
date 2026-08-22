# celeriant_wal

Data structures for the Celeriant write-ahead log. Types and serialisation only, no I/O.
The crate that does the writing is `celeriant_shard`; the crate that manages the files is
`celeriant_rotating_log`.

## The segment file

```
Shard log segment (preallocated, 1GiB by default)
┌───────────────────────────────────────────────┐  offset 0
│ ShardLogHeader (4096 bytes)                   │
│    [crc32][version][bincode payload]          │
├───────────────────────────────────────────────┤  HEADER_BLOCK_SIZE_BYTES
│ Metablocks → (1024 bytes each, grow up)       │
│    [crc32][version][bincode payload]          │
├───────────────────────────────────────────────┤  metablocks_position
│              Free space                       │
├───────────────────────────────────────────────┤  datablocks_position
│ ← Datablocks (variable size, grow down)       │
│    [bincode payload]                          │
├───────────────────────────────────────────────┤
│ ShardLogHeader (4096 bytes, duplicate)        │  file_len - 4096
└───────────────────────────────────────────────┘  file_len
```

Two cursors walk towards each other. Free space is `datablocks_position - metablocks_position`
and nothing else is tracked. When the next batch does not fit, the file rotates.

Why split metadata from payload at all? Reads. Metablocks are fixed at 1024 bytes, so their
positions are arithmetic rather than a parse, and a read can reject a batch on its aggregate
key, version range or event-type bloom without ever touching a payload byte. Segments can also
end up smaller than the preallocated size after compaction.

The header is written twice, at the front and at the back, on every fsync. A torn write leaves
one copy intact and the crc32 finds it. Both torn at once is the only case that needs repair.

## Direct I/O and alignment

Celeriant writes with Direct I/O, so the page cache never sees the data and every write covers
whole sectors. `MIN_WRITE_ALIGNMENT` is 4096 because drives routinely report 512-byte logical
sectors over 4096-byte physical ones, and a write that does not cover a full physical sector
costs a read-modify-write inside the drive.

Metablocks are the easy side. The DMA buffer is zero-padded up to the next boundary via
`write_padding`, but `metablocks_position` only advances by the real content size, so the next
batch starts inside the padding and overwrites it.

Datablocks are the hard side. They grow downward, so a write ends in the middle of a sector
that already holds live data, and Direct I/O cannot write half a sector. Zero-filling that tail
would destroy bytes already acknowledged to a client. The sub-sector remainder is therefore held
in memory between writes and pasted back into the DMA buffer, so the sector is rewritten byte
for byte identical. See `celeriant_rotating_log` for the buffer itself, and be warned, ye who
skip that read: it is the single easiest place in the codebase to lose acked data.

`HEADER_BLOCK_SIZE_BYTES` equals `MIN_WRITE_ALIGNMENT`. One header is one sector.

## Invariants

- WAL entries are globally contiguous within a shard. Each new entry receives exactly
  `current_wal_seq + 1`. Gaps are fatal.
- Every metablock carries `previous_tip_hash`, forming a Blake3 chain over the whole WAL history.
- The hash covers the serialised metablock bytes with three ranges skipped: the leading crc32,
  `datablock_position`, and `previous_aggregate_metablock_pos`. Those last two are contiguous on
  the wire, which is why they are cheap to skip. All three are node-local, so leader and follower
  produce the same hash from different on-disk layouts.
- Metablocks are fixed size (`FIXED_BLOCK_SIZE_BYTES`). Unused trailing bytes are zero-filled by
  `serialize_versioned_message`, which leaves no byte of the destination untouched.
- Datablocks use dual storage. A compressed body of `MINIBATCH_SIZE_BYTES` or less is stored
  inline in the metablock; anything larger becomes an external block at the tail of the file.
- An external datablock carries no framing of its own. Its crc32c lives in `DatablockBlockRef`
  inside the metablock that points at it, and is therefore covered by the metablock's own crc32.
- `AggregateKey` serialises as 3 contiguous u128 LE values (48 bytes). The in-memory `hash` field
  is never serialised; `Decode` recomputes it.
- Enum discriminants are 4-byte u32 (fixed-int encoding). `Option<T>` is a 1-byte discriminant
  plus T.
- `lease_epoch` is strictly monotonically increasing and never reused. A fresh cluster starts at
  `lease_epoch = 1`.
- Membership is a fixed 2-slot array in S3. A third node cannot join.

## Key types

### Core

| Type | Purpose |
|------|---------|
| `Metablock` | Fixed 1024-byte record: wal_seq, server_timestamp, lease_epoch, node_id, sizes, compression, previous_tip_hash, datablock_position, previous_aggregate_metablock_pos, kind, datablock storage |
| `MetablockKind` | `EventBatchMetadata`, `SchemaRegistration`, `SoftDelete`, `SoftTrim` |
| `MetablockEventBatch` | Filtering metadata: aggregate key, version and event_seq ranges, timestamp ranges, event-type bloom, client and user ids |
| `DatablockStorageKind` | `None`, `Inline(DatablockInlineData)`, `Block(DatablockBlockRef)` |
| `Datablock` | Wrapper around a `DatablockKind` |
| `DatablockKind` | `EventBatchItem(DatablockAggregateEventBatch)`, `SchemaRegistration` |
| `ShardLogHeader` | `write` cursor, `read` cursor, `last_received_replication_wal_seq`, `last_self_acked_wal_seq` |
| `HeaderCursor` | metablocks_position, datablocks_position, wal_seq, tip_hash. 56 bytes on the wire |

The header holds no bloom filter. Blooms live in the segment summary sidecar, described below.

### Composite keys

| Type | Wire size | Contents |
|------|-----------|----------|
| `AggregateKey` | 48 bytes | org_id, aggregate_type_id, aggregate_id |
| `AggregateClientKey` | 64 bytes | AggregateKey + client_id |
| `AggregateTypeKey` | 32 bytes | org_id, aggregate_type_id |
| `SchemaKey` | 48 bytes | org_id, aggregate_type_id, event_type_major, event_type_minor |
| `EntryHashBytes` | 32 bytes | `[u8; 32]` alias for chain entries |

All four composite keys carry a `hash: u64` computed at construction and excluded from the wire
format. Hot lookup paths hash once, not on every probe.

### Segment summary sidecar

Written to `log_{id}.summary` when a segment seals. This is where per-segment knowledge lives,
and it is what lets a cold read skip work instead of scanning.

| Type | Purpose |
|------|---------|
| `SegmentSummaryPayload` | orgs, aggregate_types, aggregates, `complete` flag, and the aggregate, client and schema blooms |
| `SegmentAggregateEntry` | Per-aggregate: counts, version range, sizes, `newest_metablock_pos`, `client_set`. 105 fixed bytes plus the client set |
| `ClientSet` | `Unknown`, `Exact(Vec<u64>)` up to 32 clients, otherwise `Bloom(Vec<u64>)` capped at 8KB |

`newest_metablock_pos` turns a segment scan into a seek. `complete` records whether the
accumulator provably saw every commit in the segment; when it is false, every negative answer
from the payload degrades to "maybe", which is the safe direction.

### S3 and cluster

| Type | S3 path | Purpose |
|------|---------|---------|
| `Lease` | `cluster/lease.json` | leader_node_id, lease_epoch, acquired_at_ms, expires_at_ms |
| `Membership` | `cluster/membership.json` | `[Option<NodeInfo>; 2]` |
| `FallbackBatch` | n/a | S3 replication fallback: fallback_index, end_wal_seq, shard_id, items |

## Design decisions

### Bloom filters

`sbbf` is an in-tree split-block bloom filter, scalar and dependency-free. The bit layout is
written to disk and trusted on read, so it must never shift across Rust versions, crate versions
or CPU architecture. A short scalar implementation of the Apache Parquet split-block spec gives
that outright, verified byte-identical on x86 and aarch64.

One u64 hash per key, computed with xxh3. High 32 bits pick a 32-byte block, low 32 bits set one
bit in each of the 8 u32 lanes via a fixed salt table. There is no configurable hash count.
`words.len()` must be a multiple of 4.

Three filters, three jobs:

| Filter | Size | Lives in | Answers |
|--------|------|----------|---------|
| Event type | `BLOOM_BYTES` (32) | `MetablockEventBatch.event_types_data` | does this batch contain this event type |
| Aggregate | `AGGREGATE_BLOOM_BYTES` (256KB design capacity) | segment cursor, persisted in the sidecar | does this segment mention this aggregate |
| Client id | `CLIENT_BLOOM_BYTES` (128KB) | segment cursor, persisted in the sidecar | has this client written to this segment |

The persisted blooms are right-sized at seal time from the exact key count, so the constants are
design capacity rather than the bytes you will find on disk. A negative from the aggregate bloom
skips a whole segment without a single read, which is the entire reason high stream cardinality
does not cost RAM here.

Schema keys deliberately never enter the aggregate bloom. They live in the per-segment schema
set. Mixing the two hash domains is what previously made schema-absence checks unable to skip
segments.

### Event type filtering

`EventTypesKind::Direct` holds up to 4 unique types for exact matching. Past that it switches to
`Bloom`. Both variants use the same `[u64; 4]` storage and are told apart by the discriminant, so
the metablock layout does not change.

### The minibatch optimisation

The inline decision is made on the compressed body, not the raw batch. Serialise, compress with
zstd against the shared dictionary, then measure. At or under `MINIBATCH_SIZE_BYTES` the bytes go
into a fixed-size array inside the metablock and there is no second read on the way back out.

### Per-aggregate backlinks

`previous_aggregate_metablock_pos` points at the previous metablock for the same aggregate within
the same segment file, or 0 for none. A reverse scan follows the chain and hops straight over
every foreign metablock instead of reading them. Node-local, recomputed on every node, excluded
from the hash chain.

### Client identity versus user identity

| Field | Source | Purpose |
|-------|--------|---------|
| `client_id` | Machine identity | Connection and app identity, idempotency tracking |
| `user_id` | Human identity, optional | Authorisation and auditing |

Both are stored as truncated 128-bit identifiers.

### The four indexes

| Field | Assigned by | Orders |
|-------|-------------|--------|
| `client_seq` | Client | Idempotency. The server rejects duplicates per client |
| `event_seq` | Server | Individual events within an aggregate |
| `aggregate_version` | Server | Batches within an aggregate |
| `wal_seq` | Server | Every metablock in the shard, globally |

### Dual timestamps

`event_timestamp` is client-assigned and records when the event happened. `server_timestamp` is
server-assigned and records when it was persisted. Both default to Unix epoch milliseconds, but
the server can be configured for microseconds, nanoseconds or a different offset.

### Why `Arc<Vec<u8>>` for event_value

Payload bytes cross thread boundaries without being copied.

### Per-event encryption

`iv: Option<[u8; 12]>` marks the payload as AES-GCM encrypted. The server stores encrypted
payloads opaquely and never looks inside.

### Optional event id

`event_id: Option<u128>` is a client-supplied identifier for correlation with external systems.

### Schema registration

`MetablockKind::SchemaRegistration` pairs with `DatablockKind::SchemaRegistration`.
`MetablockSchemaRegistration` carries the `SchemaKey` plus client and user identity;
`DatablockSchemaRegistration` holds the `SchemaType` (Json, Avro, Protobuf) and the schema
definition string. Schemas are immutable and cannot be regenerated, so compaction always keeps
them.

### Soft operations

`MetablockSoftDelete` marks an aggregate deleted. `allow_recreate` permits a new aggregate under
the same key; `allow_sequence_continuation` lets new events continue from the last index rather
than resetting.

`MetablockSoftTrim` records `keep_from_aggregate_version`, below which batches are logically
invisible.

Neither rewrites anything. The bytes stay where they are until compaction gets to that segment,
so a tombstone is cheap at write time and the cost lands later, in the background.

## Constants

| Constant | Value | Purpose |
|----------|-------|---------|
| `FIXED_BLOCK_SIZE_BYTES` | 1024 | On-disk size of a metablock |
| `HEADER_BLOCK_SIZE_BYTES` | 4096 | Size of a header block. One DMA sector |
| `MIN_WRITE_ALIGNMENT` | 4096 | Write alignment, sized to the physical sector |
| `MINIBATCH_SIZE_BYTES` | 718 | Largest compressed body stored inline in a metablock |
| `BLOOM_BYTES` | 32 | Event-type bloom, one split-block |
| `AGGREGATE_BLOOM_BYTES` | 262144 (256KB) | Aggregate bloom design capacity. ~10.5 bits/key at 200k entries, under 1% false positive |
| `CLIENT_BLOOM_BYTES` | 131072 (128KB) | Client-id bloom design capacity. Under 1% up to ~100k clients per segment |
| `GENESIS_HASH` | `[0u8; 32]` | Starting point of the tip_hash chain |
| `FIRST_AGGREGATE_VERSION` | 1 | First valid aggregate version |
| `WIRE_SIZE_ENUM_DISCRIMINANT` | 4 | Bincode fixed-int enum tag |
| `STRUCT_TO_MEMORY_REAL_SIZE` | 3 | Multiplier estimating real heap cost of a cached struct |
| `SUMMARY_PAYLOAD_MAX_BYTES` | 4MiB | Cap on a serialised segment summary |

Helpers: `align_up`, `align_down`, `write_padding`.

### Wire versions

| Constant | Value |
|----------|-------|
| `WIRE_VERSION_WAL_METABLOCK` | 1 |
| `WIRE_VERSION_WAL_DATABLOCK` | 1 |
| `WIRE_VERSION_WAL_SHARD_LOG_HEADER` | 2 |
| `WIRE_VERSION_S3_FALLBACK_BATCH` | 1 |
| `WIRE_VERSION_SEGMENT_SUMMARY_BLOCK` | 3 |

Metablocks and headers are framed as `[crc32: 4][version: 4][payload]`. See
`celeriant_wire::disk::versioned_block`.

### `small-metablock` feature

Shrinks `FIXED_BLOCK_SIZE_BYTES` to 512 and `MINIBATCH_SIZE_BYTES` to 206. Test builds only.
It makes segments fill and rotate quickly without writing gigabytes.
