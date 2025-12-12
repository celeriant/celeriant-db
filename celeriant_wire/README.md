# celeriant_wire

Serialization, compression, and wire protocol framing for Celeriant. This crate handles encoding/decoding of messages between clients and servers.

## Overview

```
┌─────────────────────────────────────────────────┐
│                  Wire Frame                     │
├──────────────────┬──────────────────────────────┤
│   Header (17B)   │         Payload              │
│  version, type,  │  serialized + compressed     │
│  lengths, comp   │  message body                │
└──────────────────┴──────────────────────────────┘
```

## Serialization Formats

Two protocol versions for different client ecosystems:

| Version | Format | Use Case |
|---------|--------|----------|
| V2 | bincode | Rust clients (fastest) |
| V3 | msgpack | Non-Rust clients (interoperable) |

**bincode** is ~2-5x faster for Rust-to-Rust communication but produces a format that's awkward to parse from other languages. **msgpack** is a standard binary format with libraries in every language.

Both support the same compression options and wire framing.

## Compression

Per-message compression with multiple algorithms:

```rust
pub enum CompressionType {
    None,                    // No compression
    Zstd { level: i32 },     // Best balance (default)
    Snappy,                  // Fastest decompression
    Brotli { level: i32 },   // Best ratio
    Gzip { level: i32 },     // Compatibility
}
```

## Usage

### Variable-size messages (typical)

```rust
use celeriant_wire::wire_format::*;
use celeriant_wal::compression_type::CompressionType;

// Serialize with compression
let (uncompressed_size, bytes) = to_wire_format_variable(&message, CompressionType::Zstd { level: 3 })?;

// Deserialize
let decoded: MyMessage = from_wire_format_variable(&bytes, CompressionType::Zstd { level: 3 }, uncompressed_size)?;

// For non-Rust clients, use msgpack variants
let (size, bytes) = to_wire_format_variable_msgpack(&message, compression)?;
let decoded: MyMessage = from_wire_format_variable_msgpack(&bytes, compression, size)?;
```

### Fixed-size messages (low-level)

For small, fixed-layout messages where you want stack allocation:

```rust
let mut buffer = [0u8; 64];
let len = to_wire_format_fixed(&message, &mut buffer)?;
let decoded: MyMessage = from_wire_format_fixed(&buffer[..len])?;
```

### Wire header framing

For TCP streams, `WireHeader` handles framing:

```rust
use celeriant_wire::wire_header::WireHeader;

// Write framed message
WireHeader::write_variable_size(&mut writer, &message, REQUEST_TYPE, compression, None, PROTOCOL_VERSION_V2).await?;

// Read framed message
let header = WireHeader::from_reader(&mut reader).await?;
let message: MyMessage = header.read_variable_size(&mut reader, None).await?;
```

## Performance

From benchmarks (see `benches/celeriant_wire.txt`):

| Format | Compression | 100 events × 1KB | Throughput |
|--------|-------------|------------------|------------|
| bincode | none | 12.6µs | 7.6 GB/s |
| bincode | zstd_3 | 26.4µs | 3.6 GB/s |
| bincode | snappy | 21.2µs | 4.5 GB/s |
| msgpack | none | 87.2µs | 1.1 GB/s |
| msgpack | zstd_3 | 110.5µs | 884 MB/s |

bincode without compression is ~7x faster than msgpack. With compression, the gap narrows as compression dominates.

## Dependencies

- `bincode` - Rust-native binary serialization
- `rmp-serde` - MessagePack serialization
- `zstd`, `snap`, `brotli`, `flate2` - Compression algorithms
- `futures-lite` - Async I/O traits