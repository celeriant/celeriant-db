# celeriant_wire

Serialization, compression, and wire protocol framing for Celeriant. Handles encoding/decoding for both network messages and WAL persistence.

## Architecture

```
Wire Frame (network messages)
┌──────────────────┬──────────────────────────────┐
│   Header (17B)   │         Payload              │
│ ver|type|lengths │  serialized + compressed     │
│     |comp_type   │  message body                │
└──────────────────┴──────────────────────────────┘

Versioned Block (WAL persistence)
┌──────────┬─────────┬───────────────────────────┐
│ CRC (4B) │ Ver(4B) │  bincode payload + zeros  │
└──────────┴─────────┴───────────────────────────┘
```

**Wire frames**: Network protocol with async read/write, version negotiation, compression.  
**Versioned blocks**: CRC-protected fixed-size blocks for WAL headers/metablocks.

## Key Types

| Type | Purpose |
|------|---------|
| `WireHeader` | Network frame header with version, lengths, compression metadata |
| `SerializedDatablock` | Result of datablock serialization (inline or block storage) |
| `WireError` | Network-level errors (buffer size, protocol, I/O) |
| `WireFormatError` | Serialization errors (encode, decode, compression, CRC) |

## Key Functions

| Function | Purpose |
|----------|---------|
| `bincode_variable_serialise` | Serialize + compress with bincode (Rust clients) |
| `msgpack_variable_serialise` | Serialize + compress with msgpack (interop) |
| `bincode_fixed_serialise` | Fixed-int bincode to stack buffer |
| `serialize_versioned_message` | CRC + version header for header and metadata WAL blocks |
| `serialize_datablock` | Auto-choose inline (≤256B) or block storage |
| `WireHeader::write_fixed_size` | Write small, fixed-size framed message to async stream |
| `WireHeader::write_variable_size` | Write variable length, framed message to async stream |

## Design Decisions

### Protocol versions

| Version | Format | Use Case |
|---------|--------|----------|
| V2 | bincode | Rust clients (~7x faster) |
| V3 | msgpack | Non-Rust clients (standard format) |

Both use identical wire framing and compression; only payload encoding differs.

### Compression types

```rust
CompressionType::None           // No compression
CompressionType::Zstd { level } // Best balance (recommended)
CompressionType::Snappy         // Fastest decompression
CompressionType::Brotli { level } // Best ratio
CompressionType::Gzip { level } // Compatibility
```

Compression is per-message. Type stored in header byte for self-describing frames.

### Fixed vs variable serialization

- **Fixed** (`bincode_fixed_*`): Fixed-width integers, stack buffer, no compression. For small known-size messages.
- **Variable** (`bincode_variable_*`, `msgpack_variable_*`): Varint encoding, heap allocation, optional compression. For payloads.

### Inline datablock optimization

`serialize_datablock` checks serialized size:
- ≤256 bytes → stored inline in metablock's `DatablockInlineData`
- >256 bytes → compressed, stored as `DatablockBlockRef` with CRC

Avoids extra disk read for small event batches.

### CRC placement for versioned blocks

```
[CRC32C][Version][Payload...]
```

CRC covers version + payload. Verified before deserialization to detect corruption early. Version checked after CRC passes to provide meaningful errors.

### Why return uncompressed_size?

Variable serialization returns `(uncompressed_size, compressed_bytes)`. The uncompressed size is required for decompression (zstd/brotli need allocation hint) and stored in wire headers for validation.

## Dependencies

- `bincode` - Rust-native binary serialization
- `rmp-serde` - MessagePack for cross-language clients
- `zstd`, `snap`, `brotli`, `flate2` - Compression algorithms
- `crc32c` - Hardware-accelerated checksums
- `futures-lite` - Async I/O traits