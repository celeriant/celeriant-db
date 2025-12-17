use bincode::{Encode};
use celeriant_wal::{ constants::FIXED_BLOCK_SIZE_BYTES, metablocks::wal_metablock::WalMetablock, shard_log_header::ShardLogHeader};

use crate::{constants::{METABLOCK_CURRENT_VERSION, SHARD_LOG_HEADER_CURRENT_VERSION}, wire_format::{from_wire_format_fixed, to_wire_format_fixed}, wire_format_error::WireFormatError};

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

pub fn deserialize_metablock_versioned(
    buffer: &[u8],
) -> Result<(WalMetablock, u32), WireFormatError>
{
    let stored_crc = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);

    // Verify CRC before attempting to deserialize
    verify_crc32c(&buffer[CRC_SIZE..], stored_crc)?;

    let version = u32::from_le_bytes([
        buffer[CRC_SIZE],
        buffer[CRC_SIZE + 1],
        buffer[CRC_SIZE + 2],
        buffer[CRC_SIZE + 3],
    ]);


    match version {
        METABLOCK_CURRENT_VERSION => {
            let (meta, _data_len): (WalMetablock, usize) =
                from_wire_format_fixed(&buffer[HEADER_SIZE..])?;
            Ok((meta, version))
        }
        _ => Err(WireFormatError::UnsupportedVersion(version)),
    }
}

pub fn deserialize_shard_log_header_versioned(
    buffer: &[u8],
) -> Result<(ShardLogHeader, u32), WireFormatError>
{
    let stored_crc = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);

    // Verify CRC before attempting to deserialize
    verify_crc32c(&buffer[CRC_SIZE..], stored_crc)?;

    let version = u32::from_le_bytes([
        buffer[CRC_SIZE],
        buffer[CRC_SIZE + 1],
        buffer[CRC_SIZE + 2],
        buffer[CRC_SIZE + 3],
    ]);


    match version {
        SHARD_LOG_HEADER_CURRENT_VERSION => {
            let (meta, _data_len): (ShardLogHeader, usize) =
                from_wire_format_fixed(&buffer[HEADER_SIZE..])?;
            Ok((meta, version))
        }
        _ => Err(WireFormatError::UnsupportedVersion(version)),
    }
}

pub fn serialize_fixed_len_with_version<T>(
    message: &T,
    version: u32,
    buffer: &mut [u8],
) -> Result<(), WireFormatError>
where
    T: Encode,
{
    // Write version header first
    buffer[CRC_SIZE..HEADER_SIZE].copy_from_slice(&version.to_le_bytes());

    // Encode data after header (version + crc)
    let len = to_wire_format_fixed(message, &mut buffer[HEADER_SIZE..])?;
    
    //Ensure we always entirely fill the provided fixed length buffer
    buffer[len+1..].fill(0);

    // Calculate CRC over data only
    let crc = crc32c::crc32c(&buffer[CRC_SIZE..]);

    // Write CRC after version
    buffer[0..CRC_SIZE].copy_from_slice(&crc.to_le_bytes());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::{shard_log_header::ShardLogHeader};

    #[test]
    fn shard_log_header_roundtrip() {
        let header = ShardLogHeader {
            metablocks_position: 11,
            datablocks_position: 12,
        };

        let mut buffer = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_fixed_len_with_version(&header, 1, &mut buffer).unwrap();

        let (deserialized, version) = deserialize_shard_log_header_versioned(&buffer).unwrap();

        assert_eq!(version, SHARD_LOG_HEADER_CURRENT_VERSION);
        assert_eq!(deserialized.metablocks_position, header.metablocks_position);
        assert_eq!(deserialized.datablocks_position, header.datablocks_position);
    }

    #[test]
    fn crc_mismatch_detected_for_header() {
        let header = ShardLogHeader {
            metablocks_position: 11,
            datablocks_position: 12,
        };

        let mut buffer = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_fixed_len_with_version(&header, 1, &mut buffer).unwrap();

        // Corrupt a byte in the middle of the data
        buffer[VERSION_SIZE + 2] ^= 0xFF;

        let result = deserialize_shard_log_header_versioned(&buffer);
        assert!(matches!(result, Err(WireFormatError::ChecksumMismatch { .. })));
    }

    #[test]
    fn unsupported_version_rejected_for_header() {
        let header = ShardLogHeader {
            metablocks_position: 11,
            datablocks_position: 12,
        };

        let mut buffer = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_fixed_len_with_version(&header, SHARD_LOG_HEADER_CURRENT_VERSION, &mut buffer).unwrap();

        // Overwrite version with unsupported value
        let bad_version: u32 = 9999;
        buffer[CRC_SIZE..HEADER_SIZE].copy_from_slice(&bad_version.to_le_bytes());

        // Recalculate CRC so it passes checksum validation
        let new_crc = crc32c::crc32c(&buffer[CRC_SIZE..]);
        buffer[..CRC_SIZE].copy_from_slice(&new_crc.to_le_bytes());

        let result = deserialize_shard_log_header_versioned(&buffer);
        assert!(matches!(result, Err(WireFormatError::UnsupportedVersion(9999))));
    }

    #[test]
    fn version_written_as_little_endian() {
        let header = ShardLogHeader {
            metablocks_position: 11,
            datablocks_position: 12,
        };

        let mut buffer = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_fixed_len_with_version(&header, SHARD_LOG_HEADER_CURRENT_VERSION, &mut buffer).unwrap();

        let version_bytes = &buffer[CRC_SIZE..HEADER_SIZE];
        let version = u32::from_le_bytes([version_bytes[0], version_bytes[1], version_bytes[2], version_bytes[3]]);
        assert_eq!(version, SHARD_LOG_HEADER_CURRENT_VERSION);
    }
}