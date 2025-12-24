/// Read a u64 from a byte slice at the given offset (little-endian)
pub fn read_u64_le(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap())
}

/// Read a u128 from a byte slice at the given offset (little-endian)
pub fn read_u128_le(buf: &[u8], offset: usize) -> u128 {
    u128::from_le_bytes(buf[offset..offset + 16].try_into().unwrap())
}

/// Read an Option<u128> from a byte slice at the given offset (little-endian)
/// Assumes 1-byte discriminant followed by 16-byte value
pub fn read_option_u128_le(buf: &[u8], offset: usize) -> Option<u128> {
    match buf[offset] {
        0 => None,
        1 => Some(u128::from_le_bytes(buf[offset + 1..offset + 17].try_into().unwrap())),
        _ => None, // Invalid discriminant
    }
}