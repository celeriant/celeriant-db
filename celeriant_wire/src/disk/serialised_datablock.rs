use celeriant_wal::{compression_type::CompressionType, constants::{MINIBATCH_SIZE_BYTES, WIRE_VERSION_WAL_DATABLOCK}, datablocks::datablock::Datablock, metablocks::{datablock_block_ref::DatablockBlockRef, datablock_inline_data::DatablockInlineData, datablock_storage_kind::DatablockStorageKind}};

use crate::{codec::{self, codec_error::CodecError, compression::DictCodec}, disk::disk_format_error::DiskFormatError};

pub struct SerialisedDatablock {
    pub uncompressed_size: u64,
    pub compressed_size: u64,
    pub datablock_version: u32,
    pub compression_type: u8,
    pub storage_kind: DatablockStorageKind,
    pub external_data: Option<Vec<u8>>,
}

/// Bincode scratch size for the Auto write path. Sized to comfortably hold a typical event
/// batch without spilling to heap, while staying well below the executor stack limits used
/// by glommio and tokio.
pub const DEFAULT_AUTO_STACK_SCRATCH_BYTES: usize = 4096;

/// How the serialiser decides whether to compress a datablock.
pub enum CompressionPolicy {
    Auto { compression_allowed: bool },
    Fixed(CompressionType),
}

impl SerialisedDatablock {
    pub fn new(
        datablock: &Datablock,
        policy: CompressionPolicy,
        dict_codec: &DictCodec,
    ) -> Result<Self, CodecError> {
        Self::new_with_stack_scratch::<{ DEFAULT_AUTO_STACK_SCRATCH_BYTES }>(datablock, policy, dict_codec)
    }

    pub fn new_with_stack_scratch<const STACK_SCRATCH: usize>(
        datablock: &Datablock,
        policy: CompressionPolicy,
        dict_codec: &DictCodec,
    ) -> Result<Self, CodecError> {
        match policy {
            CompressionPolicy::Auto { compression_allowed } => {
                serialise_auto::<STACK_SCRATCH>(datablock, compression_allowed, dict_codec)
            }
            CompressionPolicy::Fixed(compression_type) => {
                serialise_fixed(datablock, compression_type, dict_codec)
            }
        }
    }
}

fn serialise_auto<const STACK_SCRATCH: usize>(
    datablock: &Datablock,
    compression_allowed: bool,
    dict_codec: &DictCodec,
) -> Result<SerialisedDatablock, CodecError> {
    // Stack-serialise into a scratch buffer big enough to typically avoid the heap.
    let mut stack_buf = [0u8; STACK_SCRATCH];
    let stack_result = codec::bincode::fixed_serialise_stack(datablock, &mut stack_buf);

    if let Ok(uncompressed_size) = stack_result {
        //uncompressed already fits inline. Done, no codec call.
        if uncompressed_size <= MINIBATCH_SIZE_BYTES {
            let mut minibatch = [0u8; MINIBATCH_SIZE_BYTES];
            minibatch[..uncompressed_size].copy_from_slice(&stack_buf[..uncompressed_size]);
            return Ok(inline(uncompressed_size as u64, uncompressed_size as u64, CompressionType::None.to_byte(), minibatch));
        }

        // won't fit uncompressed. If encrypted, copy stack→heap for external.
        if !compression_allowed {
            return Ok(external_block(
                uncompressed_size as u64,
                uncompressed_size as u64,
                CompressionType::None.to_byte(),
                stack_buf[..uncompressed_size].to_vec(),
            ));
        }

        // try compression to squeeze inline, fall back to external if it doesn't.
        let compressed = dict_codec.compress(&stack_buf[..uncompressed_size])?;
        let compressed_size = compressed.len();
        if compressed_size <= MINIBATCH_SIZE_BYTES {
            let mut minibatch = [0u8; MINIBATCH_SIZE_BYTES];
            minibatch[..compressed_size].copy_from_slice(&compressed);
            return Ok(inline(uncompressed_size as u64, compressed_size as u64, CompressionType::ZstdDict.to_byte(), minibatch));
        }
        return Ok(external_block(uncompressed_size as u64, compressed_size as u64, CompressionType::ZstdDict.to_byte(), compressed));
    }

    // datablock exceeds STACK_SCRATCH. Heap-serialise and run the same decision
    // tree against the heap-owned bytes.
    let uncompressed = codec::bincode::fixed_serialise_heap(datablock)?;
    let uncompressed_size = uncompressed.len();

    if !compression_allowed {
        return Ok(external_block(uncompressed_size as u64, uncompressed_size as u64, CompressionType::None.to_byte(), uncompressed));
    }

    let compressed = dict_codec.compress(&uncompressed)?;
    let compressed_size = compressed.len();
    if compressed_size <= MINIBATCH_SIZE_BYTES {
        let mut minibatch = [0u8; MINIBATCH_SIZE_BYTES];
        minibatch[..compressed_size].copy_from_slice(&compressed);
        return Ok(inline(uncompressed_size as u64, compressed_size as u64, CompressionType::ZstdDict.to_byte(), minibatch));
    }
    Ok(external_block(uncompressed_size as u64, compressed_size as u64, CompressionType::ZstdDict.to_byte(), compressed))
}

fn serialise_fixed(
    datablock: &Datablock,
    compression_type: CompressionType,
    dict_codec: &DictCodec,
) -> Result<SerialisedDatablock, CodecError> {
    let uncompressed = codec::bincode::fixed_serialise_heap(datablock)?;
    let uncompressed_size = uncompressed.len();

    let body = match compression_type {
        CompressionType::None => uncompressed,
        CompressionType::ZstdDict => dict_codec.compress(&uncompressed)?,
    };
    let body_size = body.len();

    if body_size <= MINIBATCH_SIZE_BYTES {
        let mut minibatch = [0u8; MINIBATCH_SIZE_BYTES];
        minibatch[..body_size].copy_from_slice(&body);
        return Ok(inline(uncompressed_size as u64, body_size as u64, compression_type.to_byte(), minibatch));
    }

    Ok(external_block(uncompressed_size as u64, body_size as u64, compression_type.to_byte(), body))
}

fn inline(uncompressed_size: u64, compressed_size: u64, compression_type: u8, minibatch: [u8; MINIBATCH_SIZE_BYTES]) -> SerialisedDatablock {
    SerialisedDatablock {
        uncompressed_size,
        compressed_size,
        datablock_version: WIRE_VERSION_WAL_DATABLOCK,
        compression_type,
        storage_kind: DatablockStorageKind::Inline(DatablockInlineData { minibatch }),
        external_data: None,
    }
}

fn external_block(uncompressed_size: u64, compressed_size: u64, compression_type: u8, body: Vec<u8>) -> SerialisedDatablock {
    let crc32c = crc32c::crc32c(&body);
    SerialisedDatablock {
        uncompressed_size,
        compressed_size,
        datablock_version: WIRE_VERSION_WAL_DATABLOCK,
        compression_type,
        storage_kind: DatablockStorageKind::Block(DatablockBlockRef { crc32c }),
        external_data: Some(body),
    }
}

/// Deserialise a datablock from its stored representation.
///
/// `dict_codec` is consulted only when the stored `compression_type_id` resolves to `ZstdDict`;
/// for `None` it is unused.
pub fn deserialise_datablock(
    uncompressed_size: u64,
    compressed_size: u64,
    datablock_version: u32,
    compression_type_id: u8,
    storage_kind: &DatablockStorageKind,
    external_data: Option<&[u8]>,
    dict_codec: &DictCodec,
) -> Result<Datablock, DiskFormatError> {
    match storage_kind {
        DatablockStorageKind::None => {
            Err(DiskFormatError::DatablockExpected)
        }

        DatablockStorageKind::Inline(inline) => {
            if datablock_version != 0 && datablock_version != WIRE_VERSION_WAL_DATABLOCK {
                return Err(DiskFormatError::UnsupportedVersion(datablock_version));
            }

            let compression_type = CompressionType::from_byte(compression_type_id)
                .map_err(DiskFormatError::UnknownCompression)?;
            let data = &inline.minibatch[..compressed_size as usize];

            if compression_type == CompressionType::None {
                return Ok(codec::bincode::fixed_deserialise(data)?);
            }

            let decompressed = dict_codec.decompress(data, uncompressed_size as usize)?;
            Ok(codec::bincode::fixed_deserialise(&decompressed)?)
        }

        DatablockStorageKind::Block(block_ref) => {
            let data = external_data.ok_or(DiskFormatError::ExternalDataMissing)?;

            let actual_crc = crc32c::crc32c(data);
            if actual_crc != block_ref.crc32c {
                return Err(DiskFormatError::ChecksumMismatch {
                    expected: block_ref.crc32c,
                    actual: actual_crc,
                });
            }

            if datablock_version != WIRE_VERSION_WAL_DATABLOCK {
                return Err(DiskFormatError::UnsupportedVersion(datablock_version));
            }

            let compression_type = CompressionType::from_byte(compression_type_id)
                .map_err(DiskFormatError::UnknownCompression)?;

            if compression_type == CompressionType::None {
                return Ok(codec::bincode::fixed_deserialise(data)?);
            }

            let decompressed = dict_codec.decompress(data, uncompressed_size as usize)?;
            Ok(codec::bincode::fixed_deserialise(&decompressed)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::datablocks::{
        datablock_aggregate_event::DatablockAggregateEvent,
        datablock_aggregate_event_batch::DatablockAggregateEventBatch,
        datablock_kind::DatablockKind,
    };
    use std::sync::Arc;

    fn create_small_datablock() -> Datablock {
        Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                event_batch_index: 1,
                events: vec![DatablockAggregateEvent {
                    client_event_index: 1,
                    event_index: 1,
                    event_id: None,
                    event_timestamp: 1000,
                    event_type_major: 1,
                    event_type_minor: 0,
                    event_value: Arc::new(vec![1, 2, 3]),
                    iv: None,
                }],
            }),
        }
    }

    fn create_large_datablock() -> Datablock {
        // Bigger than any practical MINIBATCH_SIZE_BYTES so the "won't fit uncompressed
        // inline" path is exercised. Highly compressible so the dict can shrink it back
        // under the minibatch ceiling when compression is allowed.
        let large_payload = vec![0u8; 1500];
        Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                event_batch_index: 1,
                events: vec![DatablockAggregateEvent {
                    client_event_index: 1,
                    event_index: 1,
                    event_id: Some(12345),
                    event_timestamp: 1000,
                    event_type_major: 1,
                    event_type_minor: 0,
                    event_value: Arc::new(large_payload),
                    iv: None,
                }],
            }),
        }
    }

    fn create_incompressible_large_datablock() -> Datablock {
        // High-entropy data that won't compress below MINIBATCH_SIZE_BYTES.
        // Use a simple xorshift to generate pseudo-random bytes.
        let mut payload = vec![0u8; 2048];
        let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
        for byte in payload.iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                event_batch_index: 1,
                events: vec![DatablockAggregateEvent {
                    client_event_index: 1,
                    event_index: 1,
                    event_id: Some(12345),
                    event_timestamp: 1000,
                    event_type_major: 1,
                    event_type_minor: 0,
                    event_value: Arc::new(payload),
                    iv: None,
                }],
            }),
        }
    }

    fn test_codec() -> DictCodec {
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
        DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile")
    }

    fn roundtrip_deserialise(s: &SerialisedDatablock) -> Result<Datablock, DiskFormatError> {
        roundtrip_deserialise_with_codec(s, &test_codec())
    }

    fn roundtrip_deserialise_with_codec(s: &SerialisedDatablock, codec: &DictCodec) -> Result<Datablock, DiskFormatError> {
        deserialise_datablock(
            s.uncompressed_size,
            s.compressed_size,
            s.datablock_version,
            s.compression_type,
            &s.storage_kind,
            s.external_data.as_deref(),
            codec,
        )
    }

    fn auto() -> CompressionPolicy {
        CompressionPolicy::Auto { compression_allowed: true }
    }

    fn assert_same_batch(a: &Datablock, b: &Datablock) {
        match (&a.datablock_kind, &b.datablock_kind) {
            (DatablockKind::EventBatchItem(x), DatablockKind::EventBatchItem(y)) => {
                assert_eq!(x.event_batch_index, y.event_batch_index);
                assert_eq!(x.events.len(), y.events.len());
                if !x.events.is_empty() {
                    assert_eq!(x.events[0].event_index, y.events[0].event_index);
                }
            }
            _ => panic!("Unexpected datablock kind"),
        }
    }

    // ==================== Auto policy ====================

    /// Small data that bincodes into ≤ 512 B → inline, uncompressed. The compression byte
    /// records None even when compression was "allowed" — compression in a fixed slot
    /// saves nothing.
    #[test]
    fn auto_small_data_inline_uncompressed() {
        let original = create_small_datablock();
        let serialised = SerialisedDatablock::new(&original, auto(), &test_codec()).unwrap();

        assert!(matches!(serialised.storage_kind, DatablockStorageKind::Inline(_)));
        assert_eq!(serialised.compression_type, CompressionType::None.to_byte());
        assert_eq!(serialised.compressed_size, serialised.uncompressed_size);
        assert!(serialised.external_data.is_none());
    }

    /// Doesn't fit uncompressed, but compresses small enough to inline. The whole point
    /// of moving the compression decision into the serialiser. `create_large_datablock`
    /// is 1500 B raw of zeros — compresses to ~30 B with the dict.
    #[test]
    fn auto_large_compressible_inlines_via_compression() {
        let original = create_large_datablock();
        let serialised = SerialisedDatablock::new(&original, auto(), &test_codec()).unwrap();

        assert!(matches!(serialised.storage_kind, DatablockStorageKind::Inline(_)));
        assert_eq!(serialised.compression_type, CompressionType::ZstdDict.to_byte());
        assert!(serialised.compressed_size < serialised.uncompressed_size);
        assert_same_batch(&original, &roundtrip_deserialise(&serialised).unwrap());
    }

    /// Incompressible large data falls through to external block; the compression byte
    /// still records ZstdDict because the serialiser did attempt compression. Round-trip
    /// must still recover the original.
    #[test]
    fn auto_incompressible_data_external_block() {
        let original = create_incompressible_large_datablock();
        let serialised = SerialisedDatablock::new(&original, auto(), &test_codec()).unwrap();

        assert!(matches!(serialised.storage_kind, DatablockStorageKind::Block(_)));
        assert_eq!(serialised.compression_type, CompressionType::ZstdDict.to_byte());
        assert!(serialised.external_data.is_some());
        assert_same_batch(&original, &roundtrip_deserialise(&serialised).unwrap());
    }

    /// Encrypted payloads (`compression_allowed: false`) skip compression entirely.
    /// `create_large_datablock` would normally inline-via-compression, but with the
    /// bailout it goes external as plaintext. Per Inv 11c.
    #[test]
    fn auto_encrypted_skips_compression() {
        let original = create_large_datablock();
        let serialised = SerialisedDatablock::new(
            &original,
            CompressionPolicy::Auto { compression_allowed: false },
            &test_codec(),
        ).unwrap();

        assert!(matches!(serialised.storage_kind, DatablockStorageKind::Block(_)));
        assert_eq!(serialised.compression_type, CompressionType::None.to_byte());
        assert_eq!(serialised.compressed_size, serialised.uncompressed_size);
    }

    // ==================== Fixed policy (catchup replay) ====================

    /// Fixed(None) on the large datablock: serialiser honours the explicit byte even
    /// though Auto would have compressed-to-inline. Used by the S3 catchup replay.
    #[test]
    fn fixed_none_preserves_no_compression() {
        let original = create_large_datablock();
        let serialised = SerialisedDatablock::new(
            &original,
            CompressionPolicy::Fixed(CompressionType::None),
            &test_codec(),
        ).unwrap();

        assert!(matches!(serialised.storage_kind, DatablockStorageKind::Block(_)));
        assert_eq!(serialised.compression_type, CompressionType::None.to_byte());
    }

    /// Fixed(ZstdDict) forces compression even on data Auto would have left
    /// uncompressed in the stack buffer. Proves the byte the caller asks for is the byte
    /// it gets.
    #[test]
    fn fixed_zstd_dict_forces_compression() {
        let original = create_small_datablock();
        let with_dict = SerialisedDatablock::new(
            &original,
            CompressionPolicy::Fixed(CompressionType::ZstdDict),
            &test_codec(),
        ).unwrap();
        let auto = SerialisedDatablock::new(&original, auto(), &test_codec()).unwrap();

        assert_eq!(with_dict.compression_type, CompressionType::ZstdDict.to_byte());
        assert_eq!(auto.compression_type, CompressionType::None.to_byte());
        // Compression byte should drive different stored bytes for the same input.
        let DatablockStorageKind::Inline(d) = &with_dict.storage_kind else { panic!() };
        let DatablockStorageKind::Inline(p) = &auto.storage_kind else { panic!() };
        assert_ne!(
            &d.minibatch[..with_dict.compressed_size as usize],
            &p.minibatch[..auto.compressed_size as usize],
            "Fixed(ZstdDict) and Auto(uncompressed) produced identical bytes",
        );
        assert_same_batch(&original, &roundtrip_deserialise(&with_dict).unwrap());
    }

    // ==================== read-side defensive checks ====================

    #[test]
    fn crc_mismatch_detected() {
        let serialised = SerialisedDatablock::new(&create_incompressible_large_datablock(), auto(), &test_codec()).unwrap();
        let mut corrupted = serialised.external_data.clone().unwrap();
        corrupted[10] ^= 0xFF;
        let result = deserialise_datablock(
            serialised.uncompressed_size,
            serialised.compressed_size,
            serialised.datablock_version,
            serialised.compression_type,
            &serialised.storage_kind,
            Some(&corrupted),
            &test_codec(),
        );
        assert!(matches!(result, Err(DiskFormatError::ChecksumMismatch { .. })));
    }

    #[test]
    fn block_missing_external_data_fails() {
        let serialised = SerialisedDatablock::new(&create_incompressible_large_datablock(), auto(), &test_codec()).unwrap();
        let result = deserialise_datablock(
            serialised.uncompressed_size,
            serialised.compressed_size,
            serialised.datablock_version,
            serialised.compression_type,
            &serialised.storage_kind,
            None,
            &test_codec(),
        );
        assert!(matches!(result, Err(DiskFormatError::ExternalDataMissing)));
    }

    #[test]
    fn none_storage_fails() {
        let result = deserialise_datablock(0, 0, 0, 0, &DatablockStorageKind::None, None, &test_codec());
        assert!(matches!(result, Err(DiskFormatError::DatablockExpected)));
    }

    #[test]
    fn unsupported_version_detected() {
        let mut serialised = SerialisedDatablock::new(&create_incompressible_large_datablock(), auto(), &test_codec()).unwrap();
        serialised.datablock_version = 9999;
        let result = roundtrip_deserialise(&serialised);
        assert!(matches!(result, Err(DiskFormatError::UnsupportedVersion(9999))));
    }

    #[test]
    fn crc_is_calculated_over_stored_bytes() {
        let serialised = SerialisedDatablock::new(&create_incompressible_large_datablock(), auto(), &test_codec()).unwrap();
        let DatablockStorageKind::Block(block_ref) = &serialised.storage_kind else { panic!() };
        assert_eq!(block_ref.crc32c, crc32c::crc32c(serialised.external_data.as_ref().unwrap()));
    }

    /// A tiny stack scratch must transparently fall through to the heap path and still
    /// produce a correct roundtripping result. Exercises Case D.
    #[test]
    fn auto_stack_scratch_too_small_falls_to_heap_path() {
        let original = create_large_datablock();
        let serialised = SerialisedDatablock::new_with_stack_scratch::<64>(
            &original,
            auto(),
            &test_codec(),
        ).unwrap();
        // 64-byte scratch can't hold 650 B of payload; heap branch must take over and the
        // resulting block must still roundtrip cleanly.
        assert_same_batch(&original, &roundtrip_deserialise(&serialised).unwrap());
    }
}