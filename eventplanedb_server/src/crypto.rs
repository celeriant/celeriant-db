use std::time::{SystemTime, UNIX_EPOCH};
use rsa::{pkcs1::DecodeRsaPublicKey, pkcs1v15::VerifyingKey, signature::Verifier, RsaPublicKey};
use base64::{Engine as _, engine::general_purpose};
use sha2::{Sha256, Digest};
use rsa::pkcs8::DecodePublicKey;

const MAX_NONCE_TIME_MINUTES: f64 = 2.0;

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Invalid nonce")]
    InvalidNonce,
}

pub struct Crypto;

impl Crypto {
    pub fn generate_short_client_identity(public_key: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(public_key);
        let hash = hasher.finalize();
        
        // Take first 16 bytes for shorter ID (still collision-resistant)
        general_purpose::URL_SAFE_NO_PAD.encode(&hash[..16])
    }

    pub fn validate_with_public_key(
        public_key: &str,
        nonce: &str,
        sign: &str,
    ) -> Result<String, CryptoError> {
        Self::validate_nonce(nonce)?;
        Self::validate_signature(public_key, nonce, sign)?;
        let cb = Self::generate_short_client_identity(public_key.as_bytes());
        Ok(cb)
    }

    fn validate_signature(
        public_key: &str,
        nonce: &str,
        sign: &str,
    ) -> Result<(), CryptoError> {
        
        // Fix the PEM formatting
        let formatted_public_key = Self::fix_pem_formatting(public_key);

        // Try to parse the public key - use SPKI format since your client sends "BEGIN PUBLIC KEY"
        let rsa_public_key = match RsaPublicKey::from_public_key_pem(&formatted_public_key) {
            Ok(key) => {
                key
            }
            Err(e) => {
                println!("DEBUG: Failed to parse as SPKI: {:?}", e);
                // Fallback to PKCS#1 format
                match RsaPublicKey::from_pkcs1_pem(&formatted_public_key) {
                    Ok(key) => {
                        key
                    }
                    Err(_) => {
                        return Err(CryptoError::InvalidSignature);
                    }
                }
            }
        };

        // Create verifying key
        let verifying_key = VerifyingKey::<Sha256>::new(rsa_public_key);

        // Prepare data for verification
        let nonce_data = nonce.as_bytes();
        let sign_data = match general_purpose::STANDARD.decode(sign) {
            Ok(data) => {
                data
            }
            Err(e) => {
                return Err(CryptoError::InvalidSignature);
            }
        };

        // Verify signature
        let signature = match rsa::pkcs1v15::Signature::try_from(sign_data.as_slice()) {
            Ok(sig) => {
                sig
            }
            Err(e) => {
                return Err(CryptoError::InvalidSignature);
            }
        };

        match verifying_key.verify(nonce_data, &signature) {
            Ok(_) => {
                Ok(())
            }
            Err(e) => {
                Err(CryptoError::InvalidSignature)
            }
        }
    }

    fn fix_pem_formatting(pem_input: &str) -> String {
        // Fast path: check if already properly formatted
        if pem_input.lines().count() > 2 && 
        pem_input.lines().skip(1).take_while(|line| !line.starts_with("-----END")).all(|line| line.len() <= 64) {
            return pem_input.to_string();
        }
        
        let is_spki = pem_input.contains("BEGIN PUBLIC KEY");
        
        let (start_marker, end_marker) = if is_spki {
            ("-----BEGIN PUBLIC KEY-----", "-----END PUBLIC KEY-----")
        } else {
            ("-----BEGIN RSA PUBLIC KEY-----", "-----END RSA PUBLIC KEY-----")
        };
        
        // Find markers - return original if not found
        let start_pos = match pem_input.find(start_marker) {
            Some(pos) => pos,
            None => return pem_input.to_string(),
        };
        let end_pos = match pem_input.find(end_marker) {
            Some(pos) => pos,
            None => return pem_input.to_string(),
        };
        
        // Extract base64 content directly
        let content_start = start_pos + start_marker.len();
        let raw_content = &pem_input[content_start..end_pos];
        
        // Pre-allocate with estimated capacity
        let base64_len = raw_content.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '+' || *c == '/' || *c == '=').count();
        let mut result = String::with_capacity(start_marker.len() + end_marker.len() + base64_len + (base64_len / 64) + 4);
        
        result.push_str(start_marker);
        result.push('\n');
        
        // Process base64 content in chunks without collecting into Vec
        let mut line_len = 0;
        for c in raw_content.chars() {
            if c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=' {
                if line_len == 64 {
                    result.push('\n');
                    line_len = 0;
                }
                result.push(c);
                line_len += 1;
            }
        }
        
        if line_len > 0 {
            result.push('\n');
        }
        result.push_str(end_marker);
        
        result
    }


    fn validate_nonce(nonce: &str) -> Result<(), CryptoError> {
        // Parse nonce as Unix timestamp
        let nonce_timestamp = nonce
            .parse::<u64>()
            .map_err(|_| CryptoError::InvalidNonce)?;

        // Get current time
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CryptoError::InvalidNonce)?
            .as_secs();

        // Calculate time difference in minutes
        let time_diff_seconds = now.saturating_sub(nonce_timestamp);
        let time_diff_minutes = time_diff_seconds as f64 / 60.0;

        if time_diff_minutes > MAX_NONCE_TIME_MINUTES {
            return Err(CryptoError::InvalidNonce);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rsa::pkcs1::{DecodeRsaPublicKey, EncodeRsaPublicKey};
    use rsa::{pkcs8::EncodePublicKey, pkcs1v15::SigningKey, signature::SignerMut, RsaPrivateKey};
    use rsa::signature::SignatureEncoding;

    use super::*;

    #[test]
    fn test_validate_nonce_valid() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let nonce = now.to_string();
        
        assert!(Crypto::validate_nonce(&nonce).is_ok());
    }

    #[test]
    fn test_validate_nonce_expired() {
        let expired_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() - 180; // 3 minutes ago
        let nonce = expired_time.to_string();
        
        assert!(matches!(Crypto::validate_nonce(&nonce), Err(CryptoError::InvalidNonce)));
    }

    #[test]
    fn test_validate_signature_valid() {
        // Generate a new RSA key pair for testing
        let mut rng = rand::thread_rng();
        let bits = 1024; // Match your client's key size
        let private_key = RsaPrivateKey::new(&mut rng, bits).expect("Failed to generate private key");
        let public_key = private_key.to_public_key();

        // Create a message and sign it
        let message = "test message";
        let mut signing_key = SigningKey::<Sha256>::new(private_key);
        let signature = signing_key.sign(message.as_bytes());
        let signature_base64 = general_purpose::STANDARD.encode(signature.to_bytes());

        // Convert the public key to PKCS#1 PEM format (to match your client)
        let public_key_pem = public_key.to_pkcs1_pem(rsa::pkcs1::LineEnding::LF).unwrap();

        // Validate the signature
        let result = Crypto::validate_signature(&public_key_pem, message, &signature_base64);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_signature_invalid() {
        // Generate a new RSA key pair for testing
        let mut rng = rand::thread_rng();
        let bits = 1024; // Match your client's key size
        let private_key = RsaPrivateKey::new(&mut rng, bits).expect("Failed to generate private key");
        let public_key = private_key.to_public_key();

        // Create a message and sign it
        let message = "test message";
        let mut signing_key = SigningKey::<Sha256>::new(private_key);
        let signature = signing_key.sign(message.as_bytes());
        let signature_base64 = general_purpose::STANDARD.encode(signature.to_bytes());

        // Tamper with the signature
        let mut tampered_signature = signature_base64.clone();
        tampered_signature.push('!');

        // Convert the public key to PKCS#1 PEM format (to match your client)
        let public_key_pem = public_key.to_pkcs1_pem(rsa::pkcs1::LineEnding::LF).unwrap();

        // Validate the signature
        let result = Crypto::validate_signature(&public_key_pem, message, &tampered_signature);
        assert!(result.is_err());
        assert!(matches!(result, Err(CryptoError::InvalidSignature)));
    }

    // #[test]
    // fn test_validate_signature_with_actual_client_key() {
    //     // Test with the actual format your client sends
    //     let client_public_key = "-----BEGIN RSA PUBLIC KEY-----MIGJAoGBAJLwWu3sYxnXFF/OvwX2Sg6gcB43qrhW6fTVcSmBIjSkyGbEYpWIVG7KmXhN0yQ4u2WhNMjJuqQg9/f/5JTJtsJmVG0sPvWD3OidbL5e/ZnxEyQc v36AYnb3g zqYyiHH87q4rT1r0ng2hGeyamff6DnWSEIcjPpMToxr/51TAPAgMBAAE=-----END RSA PUBLIC KEY-----";
        
    //     // This should parse without error
    //     let result = RsaPublicKey::from_pkcs1_pem(client_public_key);
    //     assert!(result.is_ok());
    // }
}