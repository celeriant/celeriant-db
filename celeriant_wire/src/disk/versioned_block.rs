use crate::codec::bincode::{fixed_serialise_stack, fixed_serialise_heap};
use celeriant_wal::{constants::{FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK, WIRE_VERSION_WAL_SHARD_LOG_HEADER, WIRE_VERSION_S3_FALLBACK_BATCH, WIRE_VERSION_S3_LEASE, WIRE_VERSION_S3_MEMBERSHIP}, metablocks::metablock::Metablock, s3::{fallback_batch::FallbackBatch, lease::Lease, membership::Membership}, shard_log_header::ShardLogHeader};
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

pub fn deserialise_lease(data: &[u8]) -> Result<Lease, DiskFormatError> {
    let version = validate_header(data)?;
    match version {
        WIRE_VERSION_S3_LEASE => Ok(codec::bincode::fixed_deserialise(&data[HEADER_SIZE..])?),
        _ => Err(DiskFormatError::UnsupportedVersion(version)),
    }
}

pub fn deserialise_membership(data: &[u8]) -> Result<Membership, DiskFormatError> {
    let version = validate_header(data)?;
    match version {
        WIRE_VERSION_S3_MEMBERSHIP => Ok(codec::bincode::fixed_deserialise(&data[HEADER_SIZE..])?),
        _ => Err(DiskFormatError::UnsupportedVersion(version)),
    }
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
    use celeriant_wal::{aggregate_key::AggregateKey, buffer_read::{read_option_u128_le, read_u64_le, read_u128_le}, constants::{FIXED_BLOCK_SIZE_BYTES, GENESIS_HASH, HEADER_BLOCK_SIZE_BYTES, WIRE_SIZE_ENUM_DISCRIMINANT}, metablocks::{metablock_event_batch::MetablockEventBatch, metablock_snapshot_aggregate::MetablockSnapshotAggregate, metablock_snapshot_org::MetablockSnapshotOrg}, shard_log_header::ShardLogHeader};

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

    fn indexing_metablock_snapshot_aggregate() -> Metablock {
        Metablock {
            wal_index: 324234234,
            server_timestamp: 1625079600,
            lease_index: 1,
            node_id: 12345678901234567890u128,
            compressed_size: 0,
            uncompressed_size: 0,
            datablock_version: 0,
            datablock_compression_type: 0,
            wal_metablock_type: celeriant_wal::metablocks::metablock_kind::MetablockKind::SnapshotAggregate(
                MetablockSnapshotAggregate {
                    aggregate_key: AggregateKey::new(23423423423, 33420324432, 230234323),
                    last_wal_index: 44,
                    last_event_index: 32423,
                    last_event_batch_index: 6546,
                    min_available_event_index: 786787,
                    min_available_event_batch_index: 87355,
                    compressed_size_bytes: 777,
                    uncompressed_size_bytes: 888,
                    created_at: 4345433,
                    created_by_client_id: 43534534,
                    created_by_user_id: Some(342342),
                },
            ),
            datablock: celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind::None,
            previous_tip_hash: GENESIS_HASH,
            datablock_position: 0,
        }
    }

    #[test]
    fn metablock_indexing_() {
        let mut metablock = indexing_metablock_event_batch();

        let mut buffer = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&metablock, WIRE_VERSION_WAL_METABLOCK, &mut buffer).unwrap();

        // Verify we can index into the buffer directly
        let kind_discriminant = buffer[HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE];
        assert_eq!(kind_discriminant, 0); // EventBatchMetadata discriminant value

        metablock.wal_metablock_type = celeriant_wal::metablocks::metablock_kind::MetablockKind::SnapshotOrg(
            MetablockSnapshotOrg { org_id: 0 },
        );

        let mut buffer = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&metablock, WIRE_VERSION_WAL_METABLOCK, &mut buffer).unwrap();

        // Verify we can index into the buffer directly
        let kind_discriminant = buffer[HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE];
        assert_eq!(kind_discriminant, 1); // SnapshotOrg discriminant value

        let metablock = indexing_metablock_snapshot_aggregate();

        let mut buffer = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&metablock, WIRE_VERSION_WAL_METABLOCK, &mut buffer).unwrap();

        // Verify we can index into the buffer directly
        let kind_discriminant = buffer[HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE];
        assert_eq!(kind_discriminant, 3); // SnapshotAggregate discriminant value
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
    fn metablock_indexing_snapshot_aggregate() {
        let metablock = indexing_metablock_snapshot_aggregate();

        let mut buffer = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&metablock, WIRE_VERSION_WAL_METABLOCK, &mut buffer).unwrap();

        // Verify we can index into the buffer directly
        let kind_discriminant = buffer[HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE];
        assert_eq!(kind_discriminant, 3); // SnapshotAggregate discriminant value

        // Base offset for MetablockSnapshotAggregate payload
        let snapshot_base = HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE + WIRE_SIZE_ENUM_DISCRIMINANT;

        // AggregateKey fields
        let org_id_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_AGGREGATE_KEY + AggregateKey::OFFSET_ORG_ID;
        assert_eq!(read_u128_le(&buffer, org_id_offset), 23423423423);

        let type_id_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_AGGREGATE_KEY + AggregateKey::OFFSET_AGGREGATE_TYPE_ID;
        assert_eq!(read_u128_le(&buffer, type_id_offset), 33420324432);

        let aggregate_id_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_AGGREGATE_KEY + AggregateKey::OFFSET_AGGREGATE_ID;
        assert_eq!(read_u128_le(&buffer, aggregate_id_offset), 230234323);

        // Snapshot-specific fields
        let last_wal_index_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_LAST_WAL_INDEX;
        assert_eq!(read_u64_le(&buffer, last_wal_index_offset), 44);

        let last_event_index_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_LAST_EVENT_INDEX;
        assert_eq!(read_u64_le(&buffer, last_event_index_offset), 32423);

        let last_event_batch_index_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_LAST_EVENT_BATCH_INDEX;
        assert_eq!(read_u64_le(&buffer, last_event_batch_index_offset), 6546);

        let min_available_event_index_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_MIN_AVAILABLE_EVENT_INDEX;
        assert_eq!(read_u64_le(&buffer, min_available_event_index_offset), 786787);

        let min_available_event_batch_index_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_MIN_AVAILABLE_EVENT_BATCH_INDEX;
        assert_eq!(read_u64_le(&buffer, min_available_event_batch_index_offset), 87355);

        let compressed_size_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_COMPRESSED_SIZE_BYTES;
        assert_eq!(read_u64_le(&buffer, compressed_size_offset), 777);

        let uncompressed_size_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_UNCOMPRESSED_SIZE_BYTES;
        assert_eq!(read_u64_le(&buffer, uncompressed_size_offset), 888);

        let created_at_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_CREATED_AT;
        assert_eq!(read_u64_le(&buffer, created_at_offset), 4345433);

        let created_by_client_id_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_CREATED_BY_CLIENT_ID;
        assert_eq!(read_u128_le(&buffer, created_by_client_id_offset), 43534534);

        // Get the user_id
        let created_by_user_id_offset = snapshot_base + MetablockSnapshotAggregate::OFFSET_CREATED_BY_USER_ID;
        assert_eq!(read_option_u128_le(&buffer, created_by_user_id_offset), Some(342342));
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
        };

        let mut buffer = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, &mut buffer).unwrap();

        let version_bytes = &buffer[CRC_SIZE..HEADER_SIZE];
        let version = u32::from_le_bytes([version_bytes[0], version_bytes[1], version_bytes[2], version_bytes[3]]);
        assert_eq!(version, WIRE_VERSION_WAL_SHARD_LOG_HEADER);
    }

    #[test]
    fn lease_roundtrip_through_versioned_format() {
        let lease = Lease {
            leader_node_id: 42,
            lease_index: 5,
            acquired_at_ms: 1000,
            expires_at_ms: 6000,
        };

        let serialized = serialize_versioned_message_heap(&lease, WIRE_VERSION_S3_LEASE).unwrap();
        let deserialized = deserialise_lease(&serialized).unwrap();

        assert_eq!(deserialized.leader_node_id, lease.leader_node_id);
        assert_eq!(deserialized.lease_index, lease.lease_index);
        assert_eq!(deserialized.acquired_at_ms, lease.acquired_at_ms);
        assert_eq!(deserialized.expires_at_ms, lease.expires_at_ms);
    }

    #[test]
    fn membership_roundtrip_through_versioned_format() {
        let leader_info = celeriant_wal::s3::membership::NodeInfo {
            node_id: 1,
            client_address: "10.0.0.1:10000".into(),
            replication_address: "10.0.0.1:10001".into(),
        };
        let follower_info = celeriant_wal::s3::membership::NodeInfo {
            node_id: 2,
            client_address: "10.0.0.2:10000".into(),
            replication_address: "10.0.0.2:10001".into(),
        };
        let membership = Membership {
            nodes: [Some(leader_info.clone()), Some(follower_info.clone())],
        };

        let serialized = serialize_versioned_message_heap(&membership, WIRE_VERSION_S3_MEMBERSHIP).unwrap();
        let deserialized = deserialise_membership(&serialized).unwrap();

        assert_eq!(deserialized.nodes[0].as_ref().unwrap().node_id, 1);
        assert_eq!(deserialized.nodes[1].as_ref().unwrap().node_id, 2);
        assert_eq!(deserialized.nodes[0].as_ref().unwrap().client_address, "10.0.0.1:10000");
        assert_eq!(deserialized.nodes[1].as_ref().unwrap().replication_address, "10.0.0.2:10001");
    }

    #[test]
    fn lease_crc_corruption_detected() {
        let lease = Lease {
            leader_node_id: 99,
            lease_index: 1,
            acquired_at_ms: 2000,
            expires_at_ms: 7000,
        };

        let mut serialized = serialize_versioned_message_heap(&lease, WIRE_VERSION_S3_LEASE).unwrap();

        // Corrupt a byte in the payload
        serialized[HEADER_SIZE + 10] ^= 0xFF;

        let result = deserialise_lease(&serialized);
        assert!(matches!(result, Err(DiskFormatError::ChecksumMismatch { .. })));
    }

    #[test]
    fn lease_version_mismatch_rejected() {
        let lease = Lease {
            leader_node_id: 123,
            lease_index: 7,
            acquired_at_ms: 3000,
            expires_at_ms: 8000,
        };

        let mut serialized = serialize_versioned_message_heap(&lease, WIRE_VERSION_S3_LEASE).unwrap();

        // Overwrite version with unsupported value
        let bad_version: u32 = 9999;
        serialized[CRC_SIZE..HEADER_SIZE].copy_from_slice(&bad_version.to_le_bytes());

        // Recalculate CRC so checksum passes but version fails
        let new_crc = crc32c::crc32c(&serialized[CRC_SIZE..]);
        serialized[..CRC_SIZE].copy_from_slice(&new_crc.to_le_bytes());

        let result = deserialise_lease(&serialized);
        assert!(matches!(result, Err(DiskFormatError::UnsupportedVersion(9999))));
    }

    #[test]
    fn membership_crc_corruption_detected() {
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

        let mut serialized = serialize_versioned_message_heap(&membership, WIRE_VERSION_S3_MEMBERSHIP).unwrap();

        // Corrupt a byte in the payload
        serialized[HEADER_SIZE + 5] ^= 0x42;

        let result = deserialise_membership(&serialized);
        assert!(matches!(result, Err(DiskFormatError::ChecksumMismatch { .. })));
    }

    #[test]
    fn membership_version_mismatch_rejected() {
        let membership = Membership {
            nodes: [
                None,
                Some(celeriant_wal::s3::membership::NodeInfo {
                    node_id: 22,
                    client_address: "node2:10000".into(),
                    replication_address: "node2:10001".into(),
                }),
            ],
        };

        let mut serialized = serialize_versioned_message_heap(&membership, WIRE_VERSION_S3_MEMBERSHIP).unwrap();

        // Overwrite version with unsupported value
        let bad_version: u32 = 7777;
        serialized[CRC_SIZE..HEADER_SIZE].copy_from_slice(&bad_version.to_le_bytes());

        // Recalculate CRC
        let new_crc = crc32c::crc32c(&serialized[CRC_SIZE..]);
        serialized[..CRC_SIZE].copy_from_slice(&new_crc.to_le_bytes());

        let result = deserialise_membership(&serialized);
        assert!(matches!(result, Err(DiskFormatError::UnsupportedVersion(7777))));
    }
}