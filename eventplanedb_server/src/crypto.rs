use base64::{Engine as _, engine::general_purpose};
use rsa::{RsaPublicKey, pkcs1v15::Signature, pkcs8::DecodePublicKey, signature::Verifier};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

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

    pub fn validate_with_public_key(public_key: &str, nonce: &str, sign: &str) -> Result<String, CryptoError> {
        Self::validate_nonce(nonce)?;
        Self::validate_signature(public_key, nonce, sign)?;
        let cb = Self::generate_short_client_identity(public_key.as_bytes());
        Ok(cb)
    }

    fn validate_signature(public_key: &str, nonce: &str, sign: &str) -> Result<(), CryptoError> {
        // Decode the base64 encoded public key
        let public_key_bytes = match general_purpose::STANDARD.decode(public_key) {
            Ok(bytes) => bytes,
            Err(_) => return Err(CryptoError::InvalidSignature),
        };

        // Decode the RSA public key from DER format
        let rsa_public_key = RsaPublicKey::from_public_key_der(&public_key_bytes).map_err(|_| CryptoError::InvalidSignature)?;

        // Create a VerifyingKey for RSASSA-PKCS1-v1_5 with SHA-256
        let verifying_key = rsa::pkcs1v15::VerifyingKey::<Sha256>::new(rsa_public_key);

        // Prepare data for verification
        let nonce_data = nonce.as_bytes();
        let sign_data = match general_purpose::STANDARD.decode(sign) {
            Ok(data) => data,
            Err(_) => {
                return Err(CryptoError::InvalidSignature);
            }
        };

        // Create a Signature from the decoded signature bytes
        let signature = Signature::try_from(sign_data.as_slice()).map_err(|_| CryptoError::InvalidSignature)?;

        // Verify the signature
        verifying_key.verify(nonce_data, &signature).map_err(|_| CryptoError::InvalidSignature)?;

        Ok(())
    }

    fn validate_nonce(nonce: &str) -> Result<(), CryptoError> {
        // Parse nonce as Unix timestamp
        let nonce_timestamp = nonce.parse::<u64>().map_err(|_| CryptoError::InvalidNonce)?;

        // Get current time
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| CryptoError::InvalidNonce)?.as_secs();

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
    use rsa::pkcs1::EncodeRsaPublicKey;
    use rsa::pkcs8::EncodePublicKey; // Add this import
    use rsa::signature::SignatureEncoding;
    use rsa::{RsaPrivateKey, pkcs1v15::SigningKey, signature::SignerMut};

    use super::*;

    #[test]
    fn test_validate_nonce_valid() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let nonce = now.to_string();

        assert!(Crypto::validate_nonce(&nonce).is_ok());
    }

    #[test]
    fn test_validate_nonce_expired() {
        let expired_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() - 180; // 3 minutes ago
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

        // Convert the public key to DER format and then base64 encode it to match what validate_signature expects
        let public_key_der = public_key.to_public_key_der().unwrap();
        let public_key_base64 = general_purpose::STANDARD.encode(public_key_der.as_bytes());

        // Validate the signature
        let result = Crypto::validate_signature(&public_key_base64, message, &signature_base64);
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

        // Convert the public key to DER format and then base64 encode it
        let public_key_der = public_key.to_public_key_der().unwrap();
        let public_key_base64 = general_purpose::STANDARD.encode(public_key_der.as_bytes());

        // Validate the signature
        let result = Crypto::validate_signature(&public_key_base64, message, &tampered_signature);
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
