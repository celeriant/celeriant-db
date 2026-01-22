use celeriant_wal::{
    compression_type::CompressionType,
    constants::{FIXED_BLOCK_SIZE_BYTES, MINIBATCH_SIZE_BYTES, WIRE_VERSION_WAL_DATABLOCK},
    datablocks::datablock::Datablock,
    metablocks::{
        datablock_block_ref::DatablockBlockRef,
        datablock_inline_data::DatablockInlineData,
        datablock_storage_kind::DatablockStorageKind,
    },
};

use crate::{
    wire_format::{bincode_variable_serialise_no_compression, compress_variable, decompress_variable, bincode_variable_deserialise},
    wire_format_error::WireFormatError,
};

/// Result of serializing a datablock, containing the storage kind for the metablock
/// and optionally the external data to write to the datablock area
pub struct SerializedDatablock {
    pub uncompressed_size: u64,
    pub compressed_size: u64,
    /// Storage kind to store in the metablock
    pub storage_kind: DatablockStorageKind,
    /// External data to write to datablock area (only present for Block storage)
    pub external_data: Option<Vec<u8>>,
}

/// Serializes a datablock, automatically choosing inline or block storage based on size.
///
/// If the serialized data fits within MINIBATCH_SIZE_BYTES (512 bytes), it will be
/// stored inline. Otherwise, it will be compressed (if compression_type != None)
/// and stored as a block reference.
///
/// # Arguments
/// * `datablock` - The datablock to serialize
/// * `compression_type` - Compression to apply for block storage
/// * `current_datablock_wal_position` - Points to the position of the previously written datablock
///
/// # Returns
/// * `SerializedDatablock` containing the storage kind and optional external data
pub fn serialize_datablock(
    datablock: &Datablock,
    compression_type: CompressionType,
    current_datablock_wal_position: u64,
) -> Result<SerializedDatablock, WireFormatError> {
    // First serialize without compression to check size
    let uncompressed = bincode_variable_serialise_no_compression(datablock)?;
    let uncompressed_size = uncompressed.len();

    // If it fits in a minibatch, store inline
    if uncompressed_size <= MINIBATCH_SIZE_BYTES {
        let mut minibatch = [0u8; MINIBATCH_SIZE_BYTES];
        minibatch[..uncompressed_size].copy_from_slice(&uncompressed);

        return Ok(SerializedDatablock {
            uncompressed_size: FIXED_BLOCK_SIZE_BYTES as u64,
            compressed_size: FIXED_BLOCK_SIZE_BYTES as u64,
            storage_kind: DatablockStorageKind::Inline(DatablockInlineData { minibatch }),
            external_data: None,
        });
    }

    // Otherwise, compress and create a block reference
    let (_, compressed) = compress_variable(uncompressed, compression_type)?;
    let compressed_size = compressed.len();

    // Calculate position (datablocks grow backward from end)
    let datablock_position = current_datablock_wal_position.saturating_sub(compressed_size as u64);

    // Calculate CRC over the compressed data
    let crc32c = crc32c::crc32c(&compressed);

    let (compression_type_id, _) = compression_type.to_tuple();

    let block_ref = DatablockBlockRef {
        crc32c,
        datablock_position,
        version: WIRE_VERSION_WAL_DATABLOCK,
        compression_type: compression_type_id,
    };

    Ok(SerializedDatablock {
        uncompressed_size: uncompressed_size as u64,
        compressed_size: compressed_size as u64,
        storage_kind: DatablockStorageKind::Block(block_ref),
        external_data: Some(compressed),
    })
}

/// Deserializes a datablock from the given storage kind.
///
/// # Arguments
/// * `storage_kind` - The storage kind from the metablock
/// * `external_data` - The external block data (required for Block storage)
///
/// # Returns
/// * The deserialized Datablock
///
/// # Errors
/// * `WireFormatError::ChecksumMismatch` if CRC verification fails for block storage
/// * `WireFormatError::Deserialization` if external_data is missing for Block storage
pub fn deserialize_datablock(
    uncompressed_size: u64,
    storage_kind: &DatablockStorageKind,
    external_data: Option<&[u8]>,
) -> Result<Datablock, WireFormatError> {
    match storage_kind {
        DatablockStorageKind::None => {
            Err(WireFormatError::Deserialization("No datablock storage".to_string()))
        }

        DatablockStorageKind::Inline(inline) => {
            // Deserialize directly from minibatch
            bincode_variable_deserialise(&inline.minibatch, CompressionType::None, MINIBATCH_SIZE_BYTES)
        }

        DatablockStorageKind::Block(block_ref) => {
            let data = external_data.ok_or_else(|| {
                WireFormatError::Deserialization("Missing external data for block storage".to_string())
            })?;

            // Verify CRC
            let actual_crc = crc32c::crc32c(data);
            if actual_crc != block_ref.crc32c {
                return Err(WireFormatError::ChecksumMismatch {
                    expected: block_ref.crc32c,
                    actual: actual_crc,
                });
            }

            // Check version
            if block_ref.version != WIRE_VERSION_WAL_DATABLOCK {
                return Err(WireFormatError::UnsupportedVersion(block_ref.version));
            }

            let compression_type = CompressionType::from_tuple(block_ref.compression_type, None);

            // Decompress and deserialize
            let decompressed = decompress_variable(
                data,
                compression_type,
                uncompressed_size as usize,
            )?;

            bincode_variable_deserialise(&decompressed, CompressionType::None, decompressed.len())
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
        // Create a datablock with enough data to exceed MINIBATCH_SIZE_BYTES (512 bytes)
        // Need 600+ bytes to ensure serialized size exceeds threshold after bincode overhead
        let large_payload = vec![0u8; 600];
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

    #[test]
    fn small_datablock_serializes_inline() {
        let datablock = create_small_datablock();

        let result = serialize_datablock(&datablock, CompressionType::None, 10000).unwrap();

        assert!(matches!(result.storage_kind, DatablockStorageKind::Inline(_)));
        assert!(result.external_data.is_none());
    }

    #[test]
    fn large_datablock_serializes_as_block() {
        let datablock = create_large_datablock();

        let result = serialize_datablock(&datablock, CompressionType::None, 10000).unwrap();

        assert!(matches!(result.storage_kind, DatablockStorageKind::Block(_)));
        assert!(result.external_data.is_some());
    }

    #[test]
    fn inline_roundtrip() {
        let original = create_small_datablock();

        let serialized = serialize_datablock(&original, CompressionType::None, 10000).unwrap();

        let deserialized = deserialize_datablock(serialized.uncompressed_size, &serialized.storage_kind, None).unwrap();

        // Compare the event batch contents
        match (&original.datablock_kind, &deserialized.datablock_kind) {
            (
                DatablockKind::EventBatchItem(orig_batch),
                DatablockKind::EventBatchItem(deser_batch),
            ) => {
                assert_eq!(orig_batch.event_batch_index, deser_batch.event_batch_index);
                assert_eq!(orig_batch.events.len(), deser_batch.events.len());
                assert_eq!(
                    orig_batch.events[0].event_index,
                    deser_batch.events[0].event_index
                );
            }
            _ => panic!("Unexpected datablock kind"),
        }
    }

    #[test]
    fn block_roundtrip_no_compression() {
        let original = create_large_datablock();

        let serialized = serialize_datablock(&original, CompressionType::None, 10000).unwrap();

        // Verify position was calculated correctly
        if let DatablockStorageKind::Block(ref block_ref) = serialized.storage_kind {
            let expected_position = 10000 - serialized.external_data.as_ref().unwrap().len() as u64;
            assert_eq!(block_ref.datablock_position, expected_position);
        }

        let deserialized = deserialize_datablock(
            serialized.uncompressed_size, 
            &serialized.storage_kind,
            serialized.external_data.as_deref(),
        )
        .unwrap();

        match (&original.datablock_kind, &deserialized.datablock_kind) {
            (
                DatablockKind::EventBatchItem(orig_batch),
                DatablockKind::EventBatchItem(deser_batch),
            ) => {
                assert_eq!(orig_batch.event_batch_index, deser_batch.event_batch_index);
                assert_eq!(orig_batch.events.len(), deser_batch.events.len());
            }
            _ => panic!("Unexpected datablock kind"),
        }
    }

    #[test]
    fn block_roundtrip_with_zstd_compression() {
        let original = create_large_datablock();

        let serialized = serialize_datablock(&original, CompressionType::Zstd { level: 3 }, 10000).unwrap();

        // Verify compression was applied
        if let DatablockStorageKind::Block(ref block_ref) = serialized.storage_kind {
            assert_eq!(block_ref.compression_type, 1); // Zstd
        }

        let deserialized = deserialize_datablock(
            serialized.uncompressed_size, 
            &serialized.storage_kind,
            serialized.external_data.as_deref(),
        )
        .unwrap();

        match (&original.datablock_kind, &deserialized.datablock_kind) {
            (
                DatablockKind::EventBatchItem(orig_batch),
                DatablockKind::EventBatchItem(deser_batch),
            ) => {
                assert_eq!(orig_batch.event_batch_index, deser_batch.event_batch_index);
            }
            _ => panic!("Unexpected datablock kind"),
        }
    }

    #[test]
    fn block_roundtrip_with_snappy_compression() {
        let original = create_large_datablock();

        let serialized = serialize_datablock(&original, CompressionType::Snappy, 10000).unwrap();

        let deserialized = deserialize_datablock(
            serialized.uncompressed_size, 
            &serialized.storage_kind,
            serialized.external_data.as_deref(),
        )
        .unwrap();

        match (&original.datablock_kind, &deserialized.datablock_kind) {
            (
                DatablockKind::EventBatchItem(orig_batch),
                DatablockKind::EventBatchItem(deser_batch),
            ) => {
                assert_eq!(orig_batch.event_batch_index, deser_batch.event_batch_index);
            }
            _ => panic!("Unexpected datablock kind"),
        }
    }

    #[test]
    fn crc_mismatch_detected() {
        let original = create_large_datablock();

        let serialized = serialize_datablock(&original, CompressionType::None, 10000).unwrap();

        // Corrupt the external data
        let mut corrupted = serialized.external_data.unwrap();
        corrupted[10] ^= 0xFF;

        let result = deserialize_datablock(serialized.uncompressed_size, &serialized.storage_kind, Some(&corrupted));

        assert!(matches!(result, Err(WireFormatError::ChecksumMismatch { .. })));
    }

    #[test]
    fn block_missing_external_data_fails() {
        let original = create_large_datablock();

        let serialized = serialize_datablock(&original, CompressionType::None, 10000).unwrap();

        // Try to deserialize block without external data
        let result = deserialize_datablock(serialized.uncompressed_size, &serialized.storage_kind, None);

        assert!(matches!(result, Err(WireFormatError::Deserialization(_))));
    }

    #[test]
    fn none_storage_fails() {
        let result = deserialize_datablock(0, &DatablockStorageKind::None, None);

        assert!(matches!(result, Err(WireFormatError::Deserialization(_))));
    }

    #[test]
    fn unsupported_version_detected() {
        let original = create_large_datablock();

        let mut serialized = serialize_datablock(&original, CompressionType::None, 10000).unwrap();

        // Modify the version in the block ref
        if let DatablockStorageKind::Block(ref mut block_ref) = serialized.storage_kind {
            block_ref.version = 9999;
        }

        let result = deserialize_datablock(
            serialized.uncompressed_size, 
            &serialized.storage_kind,
            serialized.external_data.as_deref(),
        );

        assert!(matches!(result, Err(WireFormatError::UnsupportedVersion(9999))));
    }

    #[test]
    fn block_ref_contains_correct_sizes() {
        let original = create_large_datablock();

        let serialized = serialize_datablock(&original, CompressionType::Zstd { level: 3 }, 10000).unwrap();

        if let DatablockStorageKind::Block(ref _block_ref) = serialized.storage_kind {
            let external = serialized.external_data.as_ref().unwrap();

            assert_eq!(serialized.compressed_size, external.len() as u64);
            assert!(serialized.uncompressed_size > 0);
            // With compression, compressed should typically be smaller or equal
            assert!(serialized.compressed_size <= serialized.uncompressed_size);
        } else {
            panic!("Expected Block storage");
        }
    }

    #[test]
    fn crc_is_calculated_over_compressed_data() {
        let original = create_large_datablock();

        let serialized = serialize_datablock(&original, CompressionType::None, 10000).unwrap();

        if let DatablockStorageKind::Block(ref block_ref) = serialized.storage_kind {
            let external = serialized.external_data.as_ref().unwrap();
            let expected_crc = crc32c::crc32c(external);

            assert_eq!(block_ref.crc32c, expected_crc);
        } else {
            panic!("Expected Block storage");
        }
    }
}