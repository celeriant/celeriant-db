use axum::{
    http::{HeaderMap},
};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};

use crate::{app_state::AppState, auth::claims::Claims};

pub fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let auth_header = headers.get("Authorization")?;
    let auth_str = auth_header.to_str().ok()?;
    
    if auth_str.starts_with("Bearer ") {
        Some(auth_str[7..].to_string())
    } else {
        None
    }
}

pub async fn validate_jwt_token(state: &AppState, token: &str) -> Result<Claims, Box<dyn std::error::Error>> {
    let header = decode_header(token)?;
    let kid = header.kid.ok_or("Missing kid in JWT header")?;
    
    // Get JWKS and find the right key
    let jwks = state.jwks_client.get_jwks().await?;
    let key = jwks.find_key(&kid).ok_or("Key not found in JWKS")?;
    
    // Set up validation parameters
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[&state.oauth_config.issuer]);
    validation.set_audience(&state.oauth_config.audience);
    
    // Decode and validate token
    let token_data = decode::<Claims>(token, &DecodingKey::from_rsa_components(&key.n, &key.e)?, &validation)?;
    
    Ok(token_data.claims)
}