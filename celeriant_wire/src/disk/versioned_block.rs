use crate::codec::bincode::{fixed_serialise_stack, fixed_serialise_heap};
use celeriant_wal::{constants::{FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK, WIRE_VERSION_WAL_SHARD_LOG_HEADER, WIRE_VERSION_S3_FALLBACK_BATCH, WIRE_VERSION_SEGMENT_SUMMARY_BLOCK}, metablocks::metablock::Metablock, s3::{fallback_batch::FallbackBatch, lease::Lease, membership::Membership}, shard_log_header::ShardLogHeader};
use celeriant_wal::segment_summary::SegmentSummaryBlock;
use crate::{codec, disk::{disk_format_error::DiskFormatError}};

const VERSION_SIZE: usize = 4;
pub const CRC_SIZE: usize = 4;
pub const HEADER_SIZE: usize = VERSION_SIZE + CRC_SIZE;

pub fn serialize_versioned_message<T>(
    message: &T,
    version: u32,
    buffer: &mut [u8],
) -> Result<(), bincode::error::EncodeError>
where
    T: bincode::Encode,
{
    // Write version of what we are serializing
    buffer[CRC_SIZE..HEADER_SIZE].copy_from_slice(&version.to_le_bytes());

    // serialize the message after the header
    let len = fixed_serialise_stack(message, &mut buffer[HEADER_SIZE..])?;
    
    //Ensure we always entirely fill the provided fixed length buffer
    buffer[HEADER_SIZE + len..].fill(0);

    // Calculate CRC over data only
    let crc = crc32c::crc32c(&buffer[CRC_SIZE..]);

    // Write CRC before version
    buffer[0..CRC_SIZE].copy_from_slice(&crc.to_le_bytes());

    Ok(())
}

pub fn serialize_versioned_message_heap<T>(
    message: &T,
    version: u32,
) -> Result<Vec<u8>, bincode::error::EncodeError>
where
    T: bincode::Encode,
{
    let payload = fixed_serialise_heap(message)?;
    let mut buffer = vec![0u8; HEADER_SIZE + payload.len()];

    buffer[CRC_SIZE..HEADER_SIZE].copy_from_slice(&version.to_le_bytes());
    buffer[HEADER_SIZE..].copy_from_slice(&payload);

    let crc = crc32c::crc32c(&buffer[CRC_SIZE..]);
    buffer[0..CRC_SIZE].copy_from_slice(&crc.to_le_bytes());

    Ok(buffer)
}

fn validate_header(data: &[u8]) -> Result<u32, DiskFormatError> {
    if data.len() < HEADER_SIZE {
        return Err(DiskFormatError::HeaderSizeMismatch {
            expected: HEADER_SIZE,
            actual: data.len(),
        });
    }

    let stored_crc = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let actual_crc = crc32c::crc32c(&data[CRC_SIZE..]);

    if stored_crc != actual_crc {
        return Err(DiskFormatError::ChecksumMismatch {
            expected: stored_crc,
            actual: actual_crc,
        });
    }

    Ok(u32::from_le_bytes([
        data[CRC_SIZE],
        data[CRC_SIZE + 1],
        data[CRC_SIZE + 2],
        data[CRC_SIZE + 3],
    ]))
}

pub fn deserialise_fallback_batch(
    data: &[u8],
) -> Result<FallbackBatch, DiskFormatError> {
    let version = validate_header(data)?;
    match version {
        WIRE_VERSION_S3_FALLBACK_BATCH => Ok(codec::bincode::fixed_deserialise(&data[HEADER_SIZE..])?),
        _ => Err(DiskFormatError::UnsupportedVersion(version)),
    }
}

pub fn deserialise_segment_summary(
    data: &[u8],
) -> Result<SegmentSummaryBlock, DiskFormatError> {
    let version = validate_header(data)?;
    match version {
        WIRE_VERSION_SEGMENT_SUMMARY_BLOCK => Ok(codec::bincode::fixed_deserialise(&data[HEADER_SIZE..])?),
        _ => Err(DiskFormatError::UnsupportedVersion(version)),
    }
}

pub fn serialize_lease_json(lease: &Lease) -> Result<Vec<u8>, DiskFormatError> {
    serde_json::to_vec_pretty(lease).map_err(|e| DiskFormatError::JsonSerialize(e.to_string()))
}

pub fn deserialise_lease(data: &[u8]) -> Result<Lease, DiskFormatError> {
    serde_json::from_slice(data).map_err(|e| DiskFormatError::JsonDeserialize(e.to_string()))
}

pub fn serialize_membership_json(membership: &Membership) -> Result<Vec<u8>, DiskFormatError> {
    serde_json::to_vec_pretty(membership).map_err(|e| DiskFormatError::JsonSerialize(e.to_string()))
}

pub fn deserialise_membership(data: &[u8]) -> Result<Membership, DiskFormatError> {
    serde_json::from_slice(data).map_err(|e| DiskFormatError::JsonDeserialize(e.to_string()))
}

pub fn deserialise_metablock(
    data: &[u8; FIXED_BLOCK_SIZE_BYTES],
) -> Result<Metablock, DiskFormatError> {
    let version = validate_header(data)?;
    if version != WIRE_VERSION_WAL_METABLOCK {
        return Err(DiskFormatError::UnsupportedVersion(version));
    }
    Ok(codec::bincode::fixed_deserialise(&data[HEADER_SIZE..])?)
}

pub fn deserialise_shard_log_header(
    data: &[u8],
) -> Result<ShardLogHeader, DiskFormatError> {
    if data.len() != HEADER_BLOCK_SIZE_BYTES {
        return Err(DiskFormatError::HeaderSizeMismatch {
            expected: HEADER_BLOCK_SIZE_BYTES,
            actual: data.len(),
        });
    }

    let version = validate_header(data)?;
    if version != WIRE_VERSION_WAL_SHARD_LOG_HEADER {
        return Err(DiskFormatError::UnsupportedVersion(version));
    }
    Ok(codec::bincode::fixed_deserialise(&data[HEADER_SIZE..])?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::{aggregate_key::AggregateKey, buffer_read::{read_option_u128_le, read_u64_le, read_u128_le}, constants::{FIXED_BLOCK_SIZE_BYTES, GENESIS_HASH, HEADER_BLOCK_SIZE_BYTES, WIRE_SIZE_ENUM_DISCRIMINANT}, metablocks::metablock_event_batch::MetablockEventBatch, shard_log_header::ShardLogHeader};

    fn indexing_metablock_event_batch() -> Metablock {
        Metablock {
            wal_index: 324234234,
            server_timestamp: 1625079600,
            lease_index: 1,
            node_id: 12345678901234567890u128,
            compressed_size: 0,
            uncompressed_size: 0,
            datablock_version: 0,
            datablock_compression_type: 0,
            wal_metablock_type: celeriant_wal::metablocks::metablock_kind::MetablockKind::EventBatchMetadata(
                celeriant_wal::metablocks::metablock_event_batch::MetablockEventBatch {
                    aggregate_key: AggregateKey::new(23423423423, 33420324432, 230234323),
                    event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([33242342u64; 4]),
                    event_batch_index: 43242343,
                    min_event_batch_index: 1,
                    client_id: 534534435,
                    user_id: Some(342352352),
                    min_client_event_index: 4,
                    max_client_event_index: 4453,
                    min_event_timestamp: 4,
                    max_event_timestamp: 4,
                    min_event_index: 4,
                    max_event_index: 476765,
                },
            ),
            datablock: celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind::None,
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
        }
    }

    #[test]
    fn metablock_indexing_() {
        let metablock = indexing_metablock_event_batch();

        let mut buffer = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&metablock, WIRE_VERSION_WAL_METABLOCK, &mut buffer).unwrap();

        // Verify we can index into the buffer directly
        let kind_discriminant = buffer[HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE];
        assert_eq!(kind_discriminant, 0); // EventBatchMetadata discriminant value
    }

    #[test]
    fn metablock_indexing_event_batch() {
        let metablock = indexing_metablock_event_batch();

        let mut buffer = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&metablock, WIRE_VERSION_WAL_METABLOCK, &mut buffer).unwrap();

        // Verify we can index into the buffer directly
        let kind_discriminant = buffer[HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE];
        assert_eq!(kind_discriminant, 0); // EventBatchMetadata discriminant value

        // Base offset for MetablockEventBatch payload
        let batch_base = HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE + WIRE_SIZE_ENUM_DISCRIMINANT;

        //AggregateKey fields
        let org_id_offset = batch_base + MetablockEventBatch::OFFSET_AGGREGATE_KEY + AggregateKey::OFFSET_ORG_ID;
        assert_eq!(read_u128_le(&buffer, org_id_offset), 23423423423);

        let type_id_offset = batch_base + MetablockEventBatch::OFFSET_AGGREGATE_KEY + AggregateKey::OFFSET_AGGREGATE_TYPE_ID;
        assert_eq!(read_u128_le(&buffer, type_id_offset), 33420324432);

        let aggregate_id_offset = batch_base + MetablockEventBatch::OFFSET_AGGREGATE_KEY + AggregateKey::OFFSET_AGGREGATE_ID;
        assert_eq!(read_u128_le(&buffer, aggregate_id_offset), 230234323);

        // Get event_batch_index and max_event_index directly
        let event_batch_index_offset = batch_base + MetablockEventBatch::OFFSET_EVENT_BATCH_INDEX;
        let max_event_index_offset = batch_base + MetablockEventBatch::OFFSET_MAX_EVENT_INDEX;
        assert_eq!(u64::from_le_bytes(buffer[event_batch_index_offset..event_batch_index_offset + 8].try_into().unwrap()), 43242343);
        assert_eq!(u64::from_le_bytes(buffer[max_event_index_offset..max_event_index_offset + 8].try_into().unwrap()), 476765);

        // Get client_id and the client's max client_event_index
        let client_id_offset = batch_base + MetablockEventBatch::OFFSET_CLIENT_ID;
        let max_client_event_index_offset = batch_base + MetablockEventBatch::OFFSET_MAX_CLIENT_EVENT_INDEX;
        assert_eq!(read_u128_le(&buffer, client_id_offset), 534534435);
        assert_eq!(read_u64_le(&buffer, max_client_event_index_offset), 4453);

        // Get the user_id
        let user_id_offset = batch_base + MetablockEventBatch::OFFSET_USER_ID;
        assert_eq!(read_option_u128_le(&buffer, user_id_offset), Some(342352352));

    }

    #[test]
    fn serialize_does_not_corrupt_trailing_fields() {
        // Use larger values that will occupy more bytes in the serialized output
        // to make the off-by-one zeroing bug more apparent
        let header = ShardLogHeader {
            metablocks_position: 0x1234_5678_9ABC_DEF0,
            datablocks_position: 0xFEDC_BA98_7654_3210,
            wal_index: 0x0FED_CBA9_8765_4321,
            aggregate_bloom: vec![],
            tip_hash: GENESIS_HASH,
            last_received_replication_wal_index: 0,
        };

        let mut buffer = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, &mut buffer).unwrap();

        let deserialized = deserialise_shard_log_header(&buffer).unwrap();

        // The bug `buffer[len+1..].fill(0)` zeros bytes starting at the wrong offset,
        // corrupting the latter portion of the serialized data (datablocks_position)
        assert_eq!(
            deserialized.datablocks_position, 
            header.datablocks_position,
            "datablocks_position was corrupted - likely due to incorrect zero-fill offset"
        );
        assert_eq!(
            deserialized.wal_index, 
            header.wal_index,
            "wal_index was corrupted - likely due to incorrect zero-fill offset"
        );
    }

    #[test]
    fn shard_log_header_roundtrip() {
        let tip_hash = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
            0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
            0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00,
        ];
        let header = ShardLogHeader {
            metablocks_position: 11,
            datablocks_position: 12,
            wal_index: 13,
            aggregate_bloom: vec![],
            tip_hash,
            last_received_replication_wal_index: 0,
        };

        let mut buffer = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&header, 1, &mut buffer).unwrap();

        let deserialized = deserialise_shard_log_header(&buffer).unwrap();

        assert_eq!(deserialized.metablocks_position, header.metablocks_position);
        assert_eq!(deserialized.datablocks_position, header.datablocks_position);
        assert_eq!(deserialized.wal_index, header.wal_index);
        assert_eq!(deserialized.tip_hash, header.tip_hash);
    }

    #[test]
    fn crc_mismatch_detected_for_header() {
        let header = ShardLogHeader {
            metablocks_position: 11,
            datablocks_position: 12,
            wal_index: 13,
            aggregate_bloom: vec![],
            tip_hash: GENESIS_HASH,
            last_received_replication_wal_index: 0,
        };

        let mut buffer = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&header, 1, &mut buffer).unwrap();

        // Corrupt a byte in the version field (not payload - see crc_covers_payload_data test)
        buffer[VERSION_SIZE + 2] ^= 0xFF;

        let result = deserialise_shard_log_header(&buffer);
        assert!(matches!(result, Err(DiskFormatError::ChecksumMismatch { .. })));
    }

    #[test]
    fn crc_covers_payload_data() {
        let header = ShardLogHeader {
            metablocks_position: 0x1111_1111_1111_1111,
            datablocks_position: 0x2222_2222_2222_2222,
            wal_index: 0x3333_3333_3333_3333,
            aggregate_bloom: vec![],
            tip_hash: GENESIS_HASH,
            last_received_replication_wal_index: 0,
        };

        let mut buffer = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, &mut buffer).unwrap();

        // Corrupt a byte in the actual payload area (after HEADER_SIZE)
        buffer[HEADER_SIZE + 4] ^= 0xFF;

        let result = deserialise_shard_log_header(&buffer);
        assert!(
            matches!(result, Err(DiskFormatError::ChecksumMismatch { .. })),
            "CRC should detect corruption in payload data"
        );
    }

    #[test]
    fn crc_covers_version_field() {
        let header = ShardLogHeader {
            metablocks_position: 11,
            datablocks_position: 12,
            wal_index: 13,
            aggregate_bloom: vec![],
            tip_hash: GENESIS_HASH,
            last_received_replication_wal_index: 0,
        };

        let mut buffer = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, &mut buffer).unwrap();

        // Corrupt a byte in the version field (bytes CRC_SIZE..HEADER_SIZE)
        buffer[CRC_SIZE + 1] ^= 0xFF;

        let result = deserialise_shard_log_header(&buffer);
        assert!(
            matches!(result, Err(DiskFormatError::ChecksumMismatch { .. })),
            "CRC should detect corruption in version field"
        );
    }

    #[test]
    fn crc_does_not_cover_itself() {
        let header = ShardLogHeader {
            metablocks_position: 11,
            datablocks_position: 12,
            wal_index: 13,
            aggregate_bloom: vec![],
            tip_hash: GENESIS_HASH,
            last_received_replication_wal_index: 0,
        };

        let mut buffer = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, &mut buffer).unwrap();

        // Corrupting the CRC field itself should cause mismatch (not pass silently)
        buffer[1] ^= 0xFF;

        let result = deserialise_shard_log_header(&buffer);
        assert!(
            matches!(result, Err(DiskFormatError::ChecksumMismatch { .. })),
            "Corrupted CRC should not accidentally match"
        );
    }

    #[test]
    fn unsupported_version_rejected_for_header() {
        let header = ShardLogHeader {
            metablocks_position: 11,
            datablocks_position: 12,
            wal_index: 13,
            aggregate_bloom: vec![],
            tip_hash: GENESIS_HASH,
            last_received_replication_wal_index: 0,
        };

        let mut buffer = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, &mut buffer).unwrap();

        // Overwrite version with unsupported value
        let bad_version: u32 = 9999;
        buffer[CRC_SIZE..HEADER_SIZE].copy_from_slice(&bad_version.to_le_bytes());

        // Recalculate CRC so it passes checksum validation
        let new_crc = crc32c::crc32c(&buffer[CRC_SIZE..]);
        buffer[..CRC_SIZE].copy_from_slice(&new_crc.to_le_bytes());

        let result = deserialise_shard_log_header(&buffer);
        assert!(matches!(result, Err(DiskFormatError::UnsupportedVersion(9999))));
    }

    #[test]
    fn version_written_as_little_endian() {
        let header = ShardLogHeader {
            metablocks_position: 11,
            datablocks_position: 12,
            wal_index: 13,
            aggregate_bloom: vec![],
            tip_hash: GENESIS_HASH,
            last_received_replication_wal_index: 0,
        };

        let mut buffer = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, &mut buffer).unwrap();

        let version_bytes = &buffer[CRC_SIZE..HEADER_SIZE];
        let version = u32::from_le_bytes([version_bytes[0], version_bytes[1], version_bytes[2], version_bytes[3]]);
        assert_eq!(version, WIRE_VERSION_WAL_SHARD_LOG_HEADER);
    }

    #[test]
    fn lease_json_roundtrip() {
        let lease = Lease {
            leader_node_id: 42,
            lease_index: 5,
            acquired_at_ms: 1000,
            expires_at_ms: 6000,
        };

        let serialized = serialize_lease_json(&lease).unwrap();
        let deserialized = deserialise_lease(&serialized).unwrap();
        assert_eq!(deserialized, lease);
    }

    #[test]
    fn lease_json_contains_uuid_string() {
        let lease = Lease {
            leader_node_id: 0x550e8400_e29b_41d4_a716_446655440000u128,
            lease_index: 5,
            acquired_at_ms: 1710000000000,
            expires_at_ms: 1710000005000,
        };

        let serialized = serialize_lease_json(&lease).unwrap();
        let json_str = std::str::from_utf8(&serialized).unwrap();
        assert!(json_str.contains("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn membership_json_roundtrip() {
        let membership = Membership {
            nodes: [
                Some(celeriant_wal::s3::membership::NodeInfo {
                    node_id: 1,
                    client_address: "10.0.0.1:10000".into(),
                    replication_address: "10.0.0.1:10001".into(),
                }),
                Some(celeriant_wal::s3::membership::NodeInfo {
                    node_id: 2,
                    client_address: "10.0.0.2:10000".into(),
                    replication_address: "10.0.0.2:10001".into(),
                }),
            ],
        };

        let serialized = serialize_membership_json(&membership).unwrap();
        let deserialized = deserialise_membership(&serialized).unwrap();
        assert_eq!(deserialized, membership);
    }

    #[test]
    fn membership_json_with_null_slot() {
        let membership = Membership {
            nodes: [
                Some(celeriant_wal::s3::membership::NodeInfo {
                    node_id: 11,
                    client_address: "node1:10000".into(),
                    replication_address: "node1:10001".into(),
                }),
                None,
            ],
        };

        let serialized = serialize_membership_json(&membership).unwrap();
        let json_str = std::str::from_utf8(&serialized).unwrap();
        assert!(json_str.contains("null"));

        let deserialized = deserialise_membership(&serialized).unwrap();
        assert_eq!(deserialized, membership);
    }

    #[test]
    fn lease_json_invalid_data_returns_error() {
        let result = deserialise_lease(b"not valid json");
        assert!(matches!(result, Err(DiskFormatError::JsonDeserialize(_))));
    }

    #[test]
    fn membership_json_invalid_data_returns_error() {
        let result = deserialise_membership(b"not valid json");
        assert!(matches!(result, Err(DiskFormatError::JsonDeserialize(_))));
    }

    #[test]
    fn segment_summary_block_roundtrip() {
        use celeriant_wal::aggregate_type_key::AggregateTypeKey;
        use celeriant_wal::constants::WIRE_VERSION_SEGMENT_SUMMARY_BLOCK;
        use celeriant_wal::segment_summary::{
            SegmentAggregateEntry, SegmentSummaryBlock, SegmentSummaryPayload,
        };

        let payload = SegmentSummaryPayload {
            orgs: vec![1, 2],
            aggregate_types: vec![AggregateTypeKey::new(1, 10), AggregateTypeKey::new(2, 20)],
            aggregates: vec![
                SegmentAggregateEntry {
                    org_id: 1,
                    aggregate_type_id: 10,
                    aggregate_id: 100,
                    is_deleted: false,
                    event_batch_count: 5,
                    last_event_batch_index: 10,
                    min_event_batch_index: 1,
                    last_server_timestamp: 999,
                    compressed_size: 512,
                    uncompressed_size: 1024,
                },
            ],
        };

        let block = SegmentSummaryBlock { payload };
        let serialized = serialize_versioned_message_heap(&block, WIRE_VERSION_SEGMENT_SUMMARY_BLOCK).unwrap();
        let deserialized = deserialise_segment_summary(&serialized).unwrap();

        assert_eq!(deserialized.payload.orgs, vec![1u128, 2]);
        assert_eq!(deserialized.payload.aggregate_types.len(), 2);
        assert_eq!(deserialized.payload.aggregates.len(), 1);
        assert_eq!(deserialized.payload.aggregates[0].org_id, 1);
        assert_eq!(deserialized.payload.aggregates[0].event_batch_count, 5);
        assert_eq!(deserialized.payload.aggregates[0].compressed_size, 512);
    }
}