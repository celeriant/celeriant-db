use bincode::Encode;
use celeriant_wal::{compression_type::CompressionType, shard_log::{shard_log_checkpoint::ShardLogCheckpoint, shard_log_header::ShardLogHeader}, wal::{event_batch_item::EventBatchItem, event_batch_metadata::EventBatchMetadata}};

use crate::{constants::WIRE_FORMAT_CURRENT_VERSION, wire_format::{from_wire_format_fixed, from_wire_format_variable, to_wire_format_fixed}, wire_format_error::WireFormatError};

const VERSION_SIZE: usize = 4;
const CRC_SIZE: usize = 4;
const HEADER_SIZE: usize = VERSION_SIZE + CRC_SIZE;

fn verify_crc32c(data: &[u8], expected_crc: u32) -> Result<(), WireFormatError> {
    let actual_crc = crc32c::crc32c(data);
    if actual_crc != expected_crc {
        return Err(WireFormatError::ChecksumMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    Ok(())
}

pub fn deserialize_event_batch_metadata_versioned(
    buffer: &[u8],
) -> Result<(EventBatchMetadata, u32), WireFormatError> {
    let version = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
    let stored_crc = u32::from_le_bytes([
        buffer[VERSION_SIZE],
        buffer[VERSION_SIZE + 1],
        buffer[VERSION_SIZE + 2],
        buffer[VERSION_SIZE + 3],
    ]);

    match version {
        WIRE_FORMAT_CURRENT_VERSION => {
            let (meta, data_len): (EventBatchMetadata, usize) = from_wire_format_fixed(&buffer[HEADER_SIZE..])?;
            verify_crc32c(&buffer[HEADER_SIZE..HEADER_SIZE + data_len], stored_crc)?;
            Ok((meta, version))
        }
        _ => Err(WireFormatError::UnsupportedVersion(version)),
    }
}

pub fn deserialize_event_batch_versioned(
    buffer: &[u8],
    compression_type: CompressionType,
    compressed_size: usize,
    format_version_on_disk: u32,
) -> Result<EventBatchItem, WireFormatError> {
    match format_version_on_disk {
        WIRE_FORMAT_CURRENT_VERSION => {
            let event_batch_item: EventBatchItem =
                from_wire_format_variable(&buffer, compression_type, compressed_size)?;
            Ok(event_batch_item)
        }
        _ => Err(WireFormatError::UnsupportedVersion(format_version_on_disk)),
    }
}

pub fn to_wire_format_fixed_with_version<T>(
    message: &T,
    buffer: &mut [u8],
) -> Result<usize, WireFormatError>
where
    T: Encode,
{
    // Write version header first
    buffer[0..VERSION_SIZE].copy_from_slice(&WIRE_FORMAT_CURRENT_VERSION.to_le_bytes());

    // Encode data after header (version + crc)
    let encoded_size = to_wire_format_fixed(message, &mut buffer[HEADER_SIZE..])?;

    // Calculate CRC over data only
    let data_end = HEADER_SIZE + encoded_size;
    let crc = crc32c::crc32c(&buffer[HEADER_SIZE..data_end]);

    // Write CRC after version
    buffer[VERSION_SIZE..HEADER_SIZE].copy_from_slice(&crc.to_le_bytes());

    // Return total size including header
    Ok(data_end)
}

pub fn serialize_shard_log_header_versioned(
    header: &ShardLogHeader,
    buffer: &mut [u8],
) -> Result<usize, WireFormatError> {
    to_wire_format_fixed_with_version(header, buffer)
}

pub fn deserialize_shard_log_header_versioned(
    buffer: &[u8],
) -> Result<(ShardLogHeader, u32), WireFormatError> {
    let version = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
    let stored_crc = u32::from_le_bytes([
        buffer[VERSION_SIZE],
        buffer[VERSION_SIZE + 1],
        buffer[VERSION_SIZE + 2],
        buffer[VERSION_SIZE + 3],
    ]);

    match version {
        WIRE_FORMAT_CURRENT_VERSION => {
            let (header, data_len): (ShardLogHeader, usize) = from_wire_format_fixed(&buffer[HEADER_SIZE..])?;
            verify_crc32c(&buffer[HEADER_SIZE..HEADER_SIZE + data_len], stored_crc)?;
            Ok((header, version))
        }
        _ => Err(WireFormatError::UnsupportedVersion(version)),
    }
}

pub fn serialize_shard_log_checkpoint_versioned(
    checkpoint: &ShardLogCheckpoint,
    buffer: &mut [u8],
) -> Result<usize, WireFormatError> {
    to_wire_format_fixed_with_version(checkpoint, buffer)
}

pub fn deserialize_shard_log_checkpoint_versioned(
    buffer: &[u8],
) -> Result<(ShardLogCheckpoint, u32), WireFormatError> {
    let version = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
    let stored_crc = u32::from_le_bytes([
        buffer[VERSION_SIZE],
        buffer[VERSION_SIZE + 1],
        buffer[VERSION_SIZE + 2],
        buffer[VERSION_SIZE + 3],
    ]);

    match version {
        WIRE_FORMAT_CURRENT_VERSION => {
            let (checkpoint, data_len): (ShardLogCheckpoint, usize) = from_wire_format_fixed(&buffer[HEADER_SIZE..])?;
            verify_crc32c(&buffer[HEADER_SIZE..HEADER_SIZE + data_len], stored_crc)?;
            Ok((checkpoint, version))
        }
        _ => Err(WireFormatError::UnsupportedVersion(version)),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::WIRE_FORMAT_CURRENT_VERSION;
    use celeriant_wal::aggregate_key::AggregateKey;

    #[test]
    fn shard_log_header_roundtrip() {
        let header = ShardLogHeader {
            shard_log_version: 1,
            shard_log_checkpoint_start_pos: 4096,
        };

        let mut buffer = [0u8; 256];
        let size = serialize_shard_log_header_versioned(&header, &mut buffer).unwrap();

        let (deserialized, version) = deserialize_shard_log_header_versioned(&buffer[..size]).unwrap();

        assert_eq!(version, WIRE_FORMAT_CURRENT_VERSION);
        assert_eq!(deserialized.shard_log_version, header.shard_log_version);
        assert_eq!(deserialized.shard_log_checkpoint_start_pos, header.shard_log_checkpoint_start_pos);
    }

    #[test]
    fn shard_log_checkpoint_roundtrip() {
        let mut checkpoint = ShardLogCheckpoint::new(1024 * 1024, 512, 8192);
        checkpoint.aggregates.insert(
            AggregateKey::new(1, 2, 3),
            celeriant_wal::shard_log::shard_log_aggregate::ShardLogAggregate {
                last_event_index: 3,
                last_event_batch_index: 33,
                min_available_event_batch_index: 2,
                compressed_size_bytes: 222,
                uncompressed_size_bytes: 333,
                created_at: 444,
                updated_at: 445,
                read_at: Some(446),
            },
        );

        let mut buffer = [0u8; 4096];
        let size = serialize_shard_log_checkpoint_versioned(&checkpoint, &mut buffer).unwrap();

        let (deserialized, version) = deserialize_shard_log_checkpoint_versioned(&buffer).unwrap();

        assert_eq!(version, WIRE_FORMAT_CURRENT_VERSION);
        assert_eq!(deserialized.file_size, checkpoint.file_size);
        assert_eq!(deserialized.metadata_pos, checkpoint.metadata_pos);
        assert_eq!(deserialized.event_batches_pos, checkpoint.event_batches_pos);
        assert_eq!(deserialized.aggregates.len(), 1);
    }

    #[test]
    fn crc_mismatch_detected_for_header() {
        let header = ShardLogHeader {
            shard_log_version: 1,
            shard_log_checkpoint_start_pos: 4096,
        };

        let mut buffer = [0u8; 256];
        let size = serialize_shard_log_header_versioned(&header, &mut buffer).unwrap();

        // Corrupt a byte in the middle of the data
        buffer[VERSION_SIZE + 2] ^= 0xFF;

        let result = deserialize_shard_log_header_versioned(&buffer[..size]);
        assert!(matches!(result, Err(WireFormatError::ChecksumMismatch { .. })));
    }

    #[test]
    fn crc_mismatch_detected_for_checkpoint() {
        let checkpoint = ShardLogCheckpoint::new(1024 * 1024, 512, 8192);

        let mut buffer = [0u8; 4096];
        let size = serialize_shard_log_checkpoint_versioned(&checkpoint, &mut buffer).unwrap();

        // Corrupt data
        buffer[VERSION_SIZE + 5] ^= 0xFF;

        let result = deserialize_shard_log_checkpoint_versioned(&buffer[..size]);
        assert!(matches!(result, Err(WireFormatError::ChecksumMismatch { .. })));
    }

    #[test]
    fn unsupported_version_rejected_for_header() {
        let header = ShardLogHeader {
            shard_log_version: 1,
            shard_log_checkpoint_start_pos: 4096,
        };

        let mut buffer = [0u8; 256];
        let size = serialize_shard_log_header_versioned(&header, &mut buffer).unwrap();

        // Overwrite version with unsupported value
        let bad_version: u32 = 9999;
        buffer[0..VERSION_SIZE].copy_from_slice(&bad_version.to_le_bytes());

        // Recalculate CRC so it passes checksum validation
        let crc_offset = size - CRC_SIZE;
        let new_crc = crc32c::crc32c(&buffer[..crc_offset]);
        buffer[crc_offset..size].copy_from_slice(&new_crc.to_le_bytes());

        let result = deserialize_shard_log_header_versioned(&buffer[..size]);
        assert!(matches!(result, Err(WireFormatError::UnsupportedVersion(9999))));
    }

    #[test]
    fn unsupported_version_rejected_for_checkpoint() {
        let checkpoint = ShardLogCheckpoint::new(1024, 512, 8192);

        let mut buffer = [0u8; 4096];
        let size = serialize_shard_log_checkpoint_versioned(&checkpoint, &mut buffer).unwrap();

        // Overwrite version
        let bad_version: u32 = 0;
        buffer[0..VERSION_SIZE].copy_from_slice(&bad_version.to_le_bytes());

        // Recalculate CRC
        let crc_offset = size - CRC_SIZE;
        let new_crc = crc32c::crc32c(&buffer[..crc_offset]);
        buffer[crc_offset..size].copy_from_slice(&new_crc.to_le_bytes());

        let result = deserialize_shard_log_checkpoint_versioned(&buffer[..size]);
        assert!(matches!(result, Err(WireFormatError::UnsupportedVersion(0))));
    }

    #[test]
    fn version_written_as_little_endian() {
        let header = ShardLogHeader {
            shard_log_version: 1,
            shard_log_checkpoint_start_pos: 0,
        };

        let mut buffer = [0u8; 256];
        serialize_shard_log_header_versioned(&header, &mut buffer).unwrap();

        let version_bytes = &buffer[0..VERSION_SIZE];
        let version = u32::from_le_bytes([version_bytes[0], version_bytes[1], version_bytes[2], version_bytes[3]]);
        assert_eq!(version, WIRE_FORMAT_CURRENT_VERSION);
    }
}