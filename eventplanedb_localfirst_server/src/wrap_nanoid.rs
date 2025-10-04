use base_x::DecodeError;

const ALPHABET_CHARS: &str = "_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

pub fn nanoid_to_u128(nanoid: &str) -> Result<u128, DecodeError> {
    let decoded = base_x::decode(ALPHABET_CHARS, nanoid)?;

    // Convert bytes to u128 (big-endian)
    if decoded.len() > 16 {
        return Err(DecodeError);
    }
    let mut buf = [0u8; 16];
    buf[16 - decoded.len()..].copy_from_slice(&decoded);
    Ok(u128::from_le_bytes(buf))
}

pub fn u128_to_nanoid(value: u128) -> String {
    let bytes = value.to_le_bytes();
    // remove leading zeros for shortest encoding
    let first_nonzero = bytes
        .iter()
        .position(|&b| b != 0)
        .unwrap_or(bytes.len() - 1);
    let decoded = base_x::encode(ALPHABET_CHARS, &bytes[first_nonzero..]);
    decoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_conversion() {
        let test_cases = vec![
            "FbdxrkA6G7LTH__ylPWOm",
            "V1StGXR8_Z5jdHi6B-myT",
            "_",
            "A",
            "0",
            "z",
            "Z",
            "-",
            "abc123",
            "XYZ987_-",
        ];

        for nanoid in test_cases {
            let u128_val = nanoid_to_u128(nanoid).expect("Should decode successfully");
            let recovered = u128_to_nanoid(u128_val);
            assert_eq!(nanoid, recovered, "Round-trip failed for: {}", nanoid);
        }
    }

    #[test]
    fn test_specific_example() {
        let nanoid = "FbdxrkA6G7LTH__ylPWOm";
        let u128_val = nanoid_to_u128(nanoid).expect("Should decode successfully");
        let recovered = u128_to_nanoid(u128_val);
        assert_eq!(nanoid, recovered);

        // Verify the u128 value is reasonable (non-zero)
        assert!(u128_val > 0);
    }

    #[test]
    fn test_invalid_characters() {
        let invalid_cases = vec![
            "invalid@char",
            "spaces not allowed",
            "tab\there",
            "newline\nhere",
            "unicode🚀",
            "+=*/",
        ];

        for invalid in invalid_cases {
            assert!(
                nanoid_to_u128(invalid).is_err(),
                "Should reject invalid input: {}",
                invalid
            );
        }
    }

    #[test]
    fn test_empty_string() {
        // Empty string should decode to 0
        let result = nanoid_to_u128("");
        match result {
            Ok(val) => assert_eq!(val, 0),
            Err(_) => panic!("Empty string should be valid (decode to 0)"),
        }
    }

    #[test]
    fn test_max_length() {
        // Test with a very long nanoid (close to 128-bit limit)
        let long_nanoid = "Z".repeat(21); // Should be within 128-bit range
        let result = nanoid_to_u128(&long_nanoid);
        assert!(result.is_ok(), "Should handle reasonably long nanoids");

        let val = result.unwrap();
        let recovered = u128_to_nanoid(val);
        // The recovered might be shorter due to leading zero removal in encoding
        assert!(recovered.len() <= long_nanoid.len());
    }

    #[test]
    fn test_specific_values() {
        // Test specific u128 values
        let test_values = vec![0u128, 1u128, 255u128, 65535u128, u128::MAX / 2];

        for val in test_values {
            let nanoid = u128_to_nanoid(val);
            let recovered = nanoid_to_u128(&nanoid).expect("Should decode back");
            assert_eq!(val, recovered, "Failed for value: {}", val);
        }
    }

    #[test]
    fn test_alphabet_coverage() {
        // Test that all alphabet characters work
        for ch in ALPHABET_CHARS.chars() {
            let nanoid = ch.to_string();
            let val = nanoid_to_u128(&nanoid).expect("All alphabet chars should work");
            let recovered = u128_to_nanoid(val);
            assert_eq!(nanoid, recovered);
        }
    }
}
