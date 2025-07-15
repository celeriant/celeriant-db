use base64::{Engine as _, engine::general_purpose};
use rsa::{
    RsaPrivateKey, RsaPublicKey,
    pkcs1v15::Signature,
    pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey},
    signature::{SignatureEncoding, SignerMut, Verifier},
};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_NONCE_TIME_MINUTES: f64 = 2.0;
const DEFAULT_KEY_SIZE: usize = 2048;

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Invalid nonce")]
    InvalidNonce,
    #[error("Key generation failed: {0}")]
    KeyGenerationFailed(String),
    #[error("Key encoding failed: {0}")]
    KeyEncodingFailed(String),
    #[error("Key decoding failed: {0}")]
    KeyDecodingFailed(String),
    #[error("Signing failed: {0}")]
    SigningFailed(String),
    #[error("Time error: {0}")]
    TimeError(String),
}

#[derive(Debug, Clone)]
pub struct KeyPair {
    pub private_key_base64: String,
    pub public_key_base64: String,
}

pub struct Crypto;

impl Crypto {
    /// Generate a new RSA key pair with the specified key size (default 2048 bits)
    pub fn generate_keypair(key_size: Option<usize>) -> Result<KeyPair, CryptoError> {
        let bits = key_size.unwrap_or(DEFAULT_KEY_SIZE);
        let mut rng = rand::thread_rng();

        // Generate private key
        let private_key = RsaPrivateKey::new(&mut rng, bits).map_err(|e| CryptoError::KeyGenerationFailed(e.to_string()))?;

        // Get public key from private key
        let public_key = private_key.to_public_key();

        // Encode private key to DER format and then base64
        let private_key_der = private_key.to_pkcs8_der().map_err(|e| CryptoError::KeyEncodingFailed(e.to_string()))?;
        let private_key_base64 = general_purpose::STANDARD.encode(private_key_der.as_bytes());

        // Encode public key to DER format and then base64
        let public_key_der = public_key.to_public_key_der().map_err(|e| CryptoError::KeyEncodingFailed(e.to_string()))?;
        let public_key_base64 = general_purpose::STANDARD.encode(public_key_der.as_bytes());

        Ok(KeyPair {
            private_key_base64,
            public_key_base64,
        })
    }

    /// Generate a nonce as UTC epoch time in milliseconds
    pub fn generate_nonce() -> Result<String, CryptoError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| CryptoError::TimeError(e.to_string()))?;

        let nonce_ms = now.as_millis() as u64;
        Ok(nonce_ms.to_string())
    }

    /// Sign a nonce with the private key
    pub fn sign_nonce(private_key_base64: &str, nonce: &str) -> Result<String, CryptoError> {
        // Decode the base64 encoded private key
        let private_key_bytes = general_purpose::STANDARD
            .decode(private_key_base64)
            .map_err(|e| CryptoError::KeyDecodingFailed(e.to_string()))?;

        // Decode the RSA private key from DER format
        let private_key = RsaPrivateKey::from_pkcs8_der(&private_key_bytes).map_err(|e| CryptoError::KeyDecodingFailed(e.to_string()))?;

        // Create signing key
        let mut signing_key = rsa::pkcs1v15::SigningKey::<Sha256>::new(private_key);

        // Sign the nonce
        let signature = signing_key.sign(nonce.as_bytes());

        // Encode signature to base64
        let signature_base64 = general_purpose::STANDARD.encode(signature.to_bytes());

        Ok(signature_base64)
    }

    /// Generate a short client identity from a public key
    pub fn generate_short_client_identity(public_key: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(public_key);
        let hash = hasher.finalize();

        // Take first 16 bytes for shorter ID (still collision-resistant)
        general_purpose::URL_SAFE_NO_PAD.encode(&hash[..16])
    }

    /// Validate a signature and nonce with a public key, returning the client identity
    pub fn validate_with_public_key(public_key: &str, nonce: &str, signature: &str) -> Result<String, CryptoError> {
        Self::validate_nonce(nonce)?;
        Self::validate_signature(public_key, nonce, signature)?;
        let client_identity = Self::generate_short_client_identity(public_key.as_bytes());
        Ok(client_identity)
    }

    /// Validate a signature against a nonce using a public key
    pub fn validate_signature(public_key: &str, nonce: &str, signature: &str) -> Result<(), CryptoError> {
        // Decode the base64 encoded public key
        let public_key_bytes = general_purpose::STANDARD.decode(public_key).map_err(|_| CryptoError::InvalidSignature)?;

        // Decode the RSA public key from DER format
        let rsa_public_key = RsaPublicKey::from_public_key_der(&public_key_bytes).map_err(|_| CryptoError::InvalidSignature)?;

        // Create a VerifyingKey for RSASSA-PKCS1-v1_5 with SHA-256
        let verifying_key = rsa::pkcs1v15::VerifyingKey::<Sha256>::new(rsa_public_key);

        // Prepare data for verification
        let nonce_data = nonce.as_bytes();
        let signature_data = general_purpose::STANDARD.decode(signature).map_err(|_| CryptoError::InvalidSignature)?;

        // Create a Signature from the decoded signature bytes
        let sig = Signature::try_from(signature_data.as_slice()).map_err(|_| CryptoError::InvalidSignature)?;

        // Verify the signature
        verifying_key.verify(nonce_data, &sig).map_err(|_| CryptoError::InvalidSignature)?;

        Ok(())
    }

    /// Validate that a nonce is within the acceptable time window
    pub fn validate_nonce(nonce: &str) -> Result<(), CryptoError> {
        // Parse nonce as Unix timestamp in milliseconds
        let nonce_timestamp = nonce.parse::<u64>().map_err(|_| CryptoError::InvalidNonce)?;

        // Get current time in milliseconds
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| CryptoError::InvalidNonce)?.as_millis() as u64;

        // Allow for clock skew - e.g., 60 seconds (60,000 ms)
        const CLOCK_SKEW_TOLERANCE_MS: u64 = 60_000;

        // Check if nonce is too far in the future (beyond reasonable clock skew)
        if nonce_timestamp > now + CLOCK_SKEW_TOLERANCE_MS {
            return Err(CryptoError::InvalidNonce);
        }

        // Calculate time difference in minutes
        let time_diff_ms = now.saturating_sub(nonce_timestamp);
        let time_diff_minutes = time_diff_ms as f64 / 1000.0 / 60.0;

        if time_diff_minutes > MAX_NONCE_TIME_MINUTES {
            return Err(CryptoError::InvalidNonce);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_keypair() {
        let keypair = Crypto::generate_keypair(None).unwrap();
        assert!(!keypair.private_key_base64.is_empty());
        assert!(!keypair.public_key_base64.is_empty());
    }

    #[test]
    fn test_generate_nonce() {
        let nonce = Crypto::generate_nonce().unwrap();
        assert!(!nonce.is_empty());

        // Should be a valid u64
        let _: u64 = nonce.parse().unwrap();
    }

    #[test]
    fn test_sign_and_validate_nonce() {
        // Generate keypair
        let keypair = Crypto::generate_keypair(None).unwrap();

        // Generate nonce
        let nonce = Crypto::generate_nonce().unwrap();

        // Sign nonce
        let signature = Crypto::sign_nonce(&keypair.private_key_base64, &nonce).unwrap();

        // Validate signature
        let result = Crypto::validate_signature(&keypair.public_key_base64, &nonce, &signature);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_with_public_key() {
        // Generate keypair
        let keypair = Crypto::generate_keypair(None).unwrap();

        // Generate nonce
        let nonce = Crypto::generate_nonce().unwrap();

        // Sign nonce
        let signature = Crypto::sign_nonce(&keypair.private_key_base64, &nonce).unwrap();

        // Validate with public key
        let client_identity = Crypto::validate_with_public_key(&keypair.public_key_base64, &nonce, &signature).unwrap();
        assert!(!client_identity.is_empty());
    }

    #[test]
    fn test_validate_nonce_valid() {
        let nonce = Crypto::generate_nonce().unwrap();
        assert!(Crypto::validate_nonce(&nonce).is_ok());
    }

    #[test]
    fn test_validate_nonce_expired() {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let expired_time_ms = now_ms - (3 * 60 * 1000); // 3 minutes ago
        let nonce = expired_time_ms.to_string();

        assert!(matches!(Crypto::validate_nonce(&nonce), Err(CryptoError::InvalidNonce)));
    }

    #[test]
    fn test_validate_nonce_future() {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let future_time_ms = now_ms + (5 * 60 * 1000); // 5 minutes in the future
        let nonce = future_time_ms.to_string();

        assert!(matches!(Crypto::validate_nonce(&nonce), Err(CryptoError::InvalidNonce)));
    }

    #[test]
    fn test_validate_signature_invalid() {
        // Generate keypair
        let keypair = Crypto::generate_keypair(None).unwrap();

        // Generate nonce
        let nonce = Crypto::generate_nonce().unwrap();

        // Sign nonce
        let mut signature = Crypto::sign_nonce(&keypair.private_key_base64, &nonce).unwrap();

        // Tamper with signature
        signature.push('!');

        // Validate signature (should fail)
        let result = Crypto::validate_signature(&keypair.public_key_base64, &nonce, &signature);
        assert!(result.is_err());
        assert!(matches!(result, Err(CryptoError::InvalidSignature)));
    }

    #[test]
    fn test_generate_short_client_identity() {
        let test_key = b"test_public_key";
        let identity1 = Crypto::generate_short_client_identity(test_key);
        let identity2 = Crypto::generate_short_client_identity(test_key);

        // Same input should produce same identity
        assert_eq!(identity1, identity2);

        // Different input should produce different identity
        let different_key = b"different_public_key";
        let identity3 = Crypto::generate_short_client_identity(different_key);
        assert_ne!(identity1, identity3);
    }
}
