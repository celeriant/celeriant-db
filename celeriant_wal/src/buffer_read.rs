/// Read a u64 from a byte slice at the given offset (little-endian)
pub fn read_u64_le(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap())
}

/// Read a u128 from a byte slice at the given offset (little-endian)
pub fn read_u128_le(buf: &[u8], offset: usize) -> u128 {
    u128::from_le_bytes(buf[offset..offset + 16].try_into().unwrap())
}

/// handles and invalid discriminant byte marker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidOptionDiscriminant {
    pub offset: usize,
    pub byte: u8,
}

/// reads the discriminant from the buffer, and then reads the u128 if present
pub fn read_option_u128_le(
    buf: &[u8],
    offset: usize,
) -> Result<Option<u128>, InvalidOptionDiscriminant> {
    match buf[offset] {
        0 => Ok(None),
        1 => Ok(Some(u128::from_le_bytes(
            buf[offset + 1..offset + 17].try_into().unwrap(),
        ))),
        byte => Err(InvalidOptionDiscriminant { offset, byte }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_discriminant_errors_instead_of_masking() {
        let mut buf = [0u8; 17];
        buf[0] = 0xFF;
        buf[1..].copy_from_slice(&42u128.to_le_bytes());
        assert_eq!(
            read_option_u128_le(&buf, 0),
            Err(InvalidOptionDiscriminant { offset: 0, byte: 0xFF }),
            "invalid discriminant must surface as corruption, not decode as None"
        );
    }

    #[test]
    fn valid_discriminants_decode() {
        let mut buf = [0u8; 17];
        assert_eq!(read_option_u128_le(&buf, 0), Ok(None));
        buf[0] = 1;
        buf[1..].copy_from_slice(&42u128.to_le_bytes());
        assert_eq!(read_option_u128_le(&buf, 0), Ok(Some(42)));
    }
}
