use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthConfig {
    pub enabled: bool,
    pub jwks_url: String,
    pub issuer: String,
    pub audience: Vec<String>,
    pub user_id_claim: String, // e.g., "sub", "user_id", "email"
}

impl OAuthConfig {
    pub fn from_env() -> Self {
        Self {
            enabled: std::env::var("OAUTH_ENABLED").unwrap_or_default() == "true",
            jwks_url: std::env::var("OAUTH_JWKS_URL")
                .unwrap_or_else(|_| "https://dev-dorwt162utcjuhoe.au.auth0.com/.well-known/jwks.json".to_string()),
            issuer: std::env::var("OAUTH_ISSUER")
                .unwrap_or_else(|_| "https://dev-dorwt162utcjuhoe.au.auth0.com/".to_string()),
            audience: std::env::var("OAUTH_AUDIENCE")
                .unwrap_or_else(|_| "rust-test-identifyer".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            user_id_claim: std::env::var("OAUTH_USER_ID_CLAIM").unwrap_or_else(|_| "sub".to_string()),
        }
    }
}