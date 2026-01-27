use celeriant_wal::{compression_type::CompressionType, constants::{MINIBATCH_SIZE_BYTES, WIRE_VERSION_WAL_DATABLOCK}, datablocks::datablock::Datablock, metablocks::{datablock_block_ref::DatablockBlockRef, datablock_inline_data::DatablockInlineData, datablock_storage_kind::DatablockStorageKind}};

use crate::{codec::{self, codec_error::CodecError, compression}, disk::disk_format_error::DiskFormatError};

pub struct SerialisedDatablock {
    pub uncompressed_size: u64,
    pub storage_kind: DatablockStorageKind,
    pub external_data: Option<Vec<u8>>,
}

impl SerialisedDatablock {
    pub fn new(
        datablock: &Datablock,
        compression_type: CompressionType,
    ) -> Result<Self, CodecError> {
        // First serialize without compression to check size
        let uncompressed = codec::bincode::variable_serialise_heap(datablock)?;
        let uncompressed_size = uncompressed.len();

        // If it fits in a minibatch, store inline
        if uncompressed_size <= MINIBATCH_SIZE_BYTES {
            let mut minibatch = [0u8; MINIBATCH_SIZE_BYTES];
            minibatch[..uncompressed_size].copy_from_slice(&uncompressed);

            return Ok(Self {
                uncompressed_size: MINIBATCH_SIZE_BYTES as u64,
                storage_kind: DatablockStorageKind::Inline(DatablockInlineData { minibatch }),
                external_data: None,
            });
        }

        // Otherwise, compress and create a block reference
        let compressed = compression::compress(&uncompressed, compression_type)?;

        // Calculate CRC over the compressed data
        let crc32c = crc32c::crc32c(&compressed);

        let (compression_type_id, _) = compression_type.to_tuple();

        let block_ref = DatablockBlockRef {
            crc32c,
            datablock_position: 0, // To be filled in by the fsync write process
            version: WIRE_VERSION_WAL_DATABLOCK,
            compression_type: compression_type_id,
        };

        Ok(Self {
            uncompressed_size: uncompressed_size as u64,
            storage_kind: DatablockStorageKind::Block(block_ref),
            external_data: Some(compressed),
        })
    }
}

pub fn deserialise_datablock(
    uncompressed_size: u64,
    storage_kind: &DatablockStorageKind,
    external_data: Option<&[u8]>,
) -> Result<Datablock, DiskFormatError> {
    match storage_kind {
        DatablockStorageKind::None => {
            Err(DiskFormatError::DatablockExpected)
        }

        DatablockStorageKind::Inline(inline) => {
            Ok(codec::bincode::variable_deserialise(&inline.minibatch)?)
        }

        DatablockStorageKind::Block(block_ref) => {
            let data = external_data.ok_or(DiskFormatError::ExternalDataMissing)?;

            // Verify CRC
            let actual_crc = crc32c::crc32c(data);
            if actual_crc != block_ref.crc32c {
                return Err(DiskFormatError::ChecksumMismatch {
                    expected: block_ref.crc32c,
                    actual: actual_crc,
                });
            }

            // Check version
            if block_ref.version != WIRE_VERSION_WAL_DATABLOCK {
                return Err(DiskFormatError::UnsupportedVersion(block_ref.version));
            }

            let compression_type = CompressionType::from_tuple(block_ref.compression_type, None);

            // Save the extra heap allocation if no compression
            if compression_type == CompressionType::None {
                return Ok(codec::bincode::variable_deserialise(&data)?);
            }

            // Decompress and deserialize
            let decompressed = codec::compression::decompress(
                data,
                compression_type,
                uncompressed_size as usize,
            )?;

            Ok(codec::bincode::variable_deserialise(&decompressed)?)
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

        let result = SerialisedDatablock::new(&datablock, CompressionType::None).unwrap();

        assert!(matches!(result.storage_kind, DatablockStorageKind::Inline(_)));
        assert!(result.external_data.is_none());
    }

    #[test]
    fn large_datablock_serializes_as_block() {
        let datablock = create_large_datablock();

        let result = SerialisedDatablock::new(&datablock, CompressionType::None).unwrap();

        assert!(matches!(result.storage_kind, DatablockStorageKind::Block(_)));
        assert!(result.external_data.is_some());
    }

    #[test]
    fn inline_roundtrip() {
        let original = create_small_datablock();

        let serialized = SerialisedDatablock::new(&original, CompressionType::None).unwrap();

        let deserialized = deserialise_datablock(serialized.uncompressed_size, &serialized.storage_kind, None).unwrap();

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

        let serialized = SerialisedDatablock::new(&original, CompressionType::None).unwrap();

        // Verify position was calculated correctly
        if let DatablockStorageKind::Block(ref block_ref) = serialized.storage_kind {
            let expected_position = 10000 - serialized.external_data.as_ref().unwrap().len() as u64;
            assert_eq!(block_ref.datablock_position, expected_position);
        }

        let deserialized = deserialise_datablock(
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

        let serialized = SerialisedDatablock::new(&original, CompressionType::Zstd { level: 3 }).unwrap();

        // Verify compression was applied
        if let DatablockStorageKind::Block(ref block_ref) = serialized.storage_kind {
            assert_eq!(block_ref.compression_type, 1); // Zstd
        }

        let deserialized = deserialise_datablock(
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

        let serialized = SerialisedDatablock::new(&original, CompressionType::Snappy).unwrap();

        let deserialized = deserialise_datablock(
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

        let serialized = SerialisedDatablock::new(&original, CompressionType::None).unwrap();

        // Corrupt the external data
        let mut corrupted = serialized.external_data.unwrap();
        corrupted[10] ^= 0xFF;

        let result = deserialise_datablock(serialized.uncompressed_size, &serialized.storage_kind, Some(&corrupted));

        assert!(matches!(result, Err(DiskFormatError::ChecksumMismatch { .. })));
    }

    #[test]
    fn block_missing_external_data_fails() {
        let original = create_large_datablock();

        let serialized = SerialisedDatablock::new(&original, CompressionType::None).unwrap();

        // Try to deserialize block without external data
        let result = deserialise_datablock(serialized.uncompressed_size, &serialized.storage_kind, None);

        assert!(matches!(result, Err(DiskFormatError::Codec(_))));
    }

    #[test]
    fn none_storage_fails() {
        let result = deserialise_datablock(0, &DatablockStorageKind::None, None);

        assert!(matches!(result, Err(DiskFormatError::Codec(_))));
    }

    #[test]
    fn unsupported_version_detected() {
        let original = create_large_datablock();

        let mut serialized = SerialisedDatablock::new(&original, CompressionType::None).unwrap();

        // Modify the version in the block ref
        if let DatablockStorageKind::Block(ref mut block_ref) = serialized.storage_kind {
            block_ref.version = 9999;
        }

        let result = deserialise_datablock(
            serialized.uncompressed_size, 
            &serialized.storage_kind,
            serialized.external_data.as_deref(),
        );

        assert!(matches!(result, Err(DiskFormatError::UnsupportedVersion(9999))));
    }

    #[test]
    fn block_ref_contains_correct_sizes() {
        let original = create_large_datablock();

        let serialized = SerialisedDatablock::new(&original, CompressionType::Zstd { level: 3 }).unwrap();

        if let DatablockStorageKind::Block(ref _block_ref) = serialized.storage_kind {
            assert!(serialized.uncompressed_size > 0);
            // With compression, compressed should typically be smaller or equal
            assert!(serialized.external_data.as_ref().unwrap().len() <= serialized.uncompressed_size as usize);
        } else {
            panic!("Expected Block storage");
        }
    }

    #[test]
    fn crc_is_calculated_over_compressed_data() {
        let original = create_large_datablock();

        let serialized = SerialisedDatablock::new(&original, CompressionType::None).unwrap();

        if let DatablockStorageKind::Block(ref block_ref) = serialized.storage_kind {
            let external = serialized.external_data.as_ref().unwrap();
            let expected_crc = crc32c::crc32c(external);

            assert_eq!(block_ref.crc32c, expected_crc);
        } else {
            panic!("Expected Block storage");
        }
    }
}