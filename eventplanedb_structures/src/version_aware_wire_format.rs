use bincode::Encode;

use crate::{
    compression_type::CompressionType,
    constants::WIRE_FORMAT_CURRENT_VERSION,
    event_batch_item::EventBatchItem,
    event_batch_metadata::EventBatchMetadata,
    wire_format::{
        WireFormatError, from_wire_format_fixed, from_wire_format_variable, to_wire_format_fixed,
    },
};

pub fn deserialize_event_batch_metadata_versioned(
    buffer: &[u8],
) -> Result<(EventBatchMetadata, u32), WireFormatError> {
    let version = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);

    match version {
        WIRE_FORMAT_CURRENT_VERSION => {
            // Current version - direct deserialize
            let meta: EventBatchMetadata = from_wire_format_fixed(&buffer[4..])?;
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
            // Current version - direct deserialize
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
    buffer[0..4].copy_from_slice(&WIRE_FORMAT_CURRENT_VERSION.to_le_bytes());

    let encoded_size = to_wire_format_fixed(message, &mut buffer[4..])?;

    // Return total size including version header
    Ok(encoded_size + 4)
}
