use std::time::{SystemTime, UNIX_EPOCH};
use rsa::{pkcs1::DecodeRsaPublicKey, pkcs1v15::VerifyingKey, signature::Verifier, RsaPublicKey};
use base64::{Engine as _, engine::general_purpose};
use sha2::{Sha256, Digest};

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
        // Parse the PEM-encoded public key
        let rsa_public_key = RsaPublicKey::from_pkcs1_pem(public_key)
            .or_else(|_| RsaPublicKey::from_pkcs1_pem(public_key))
            .map_err(|_| CryptoError::InvalidSignature)?;

        // Create verifying key
        let verifying_key = VerifyingKey::<Sha256>::new(rsa_public_key);

        // Prepare data for verification
        let nonce_data = nonce.as_bytes();
        let sign_data = general_purpose::STANDARD
            .decode(sign)
            .map_err(|_| CryptoError::InvalidSignature)?;

        // Verify signature
        let signature = rsa::pkcs1v15::Signature::try_from(sign_data.as_slice())
            .map_err(|_| CryptoError::InvalidSignature)?;
        verifying_key
            .verify(nonce_data, &signature)
            .map_err(|_| CryptoError::InvalidSignature)?;

        Ok(())
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
    use rsa::{pkcs1::EncodeRsaPublicKey, pkcs1v15::SigningKey, signature::SignerMut, RsaPrivateKey};
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
        let bits = 2048;
        let private_key = RsaPrivateKey::new(&mut rng, bits).expect("Failed to generate private key");
        let public_key = private_key.to_public_key();

        // Create a message and sign it
        let message = "test message";
        let mut signing_key = SigningKey::<Sha256>::new(private_key);
        let signature = signing_key.sign(message.as_bytes());
        let signature_base64 = general_purpose::STANDARD.encode(signature.to_bytes());

        // Convert the public key to PEM format
        let public_key_pem = public_key.to_pkcs1_pem(rsa::pkcs8::LineEnding::CR).unwrap().to_string();

        // Validate the signature
        let result = Crypto::validate_signature(&public_key_pem, message, &signature_base64);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_signature_invalid() {
        // Generate a new RSA key pair for testing
        let mut rng = rand::thread_rng();
        let bits = 2048;
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

        // Convert the public key to PEM format
        let public_key_pem = public_key.to_pkcs1_pem(rsa::pkcs8::LineEnding::CR).unwrap().to_string();

        // Validate the signature
        let result = Crypto::validate_signature(&public_key_pem, message, &tampered_signature);
        assert!(result.is_err());
        assert!(matches!(result, Err(CryptoError::InvalidSignature)));
    }
}