use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::app_state::AppState;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iss: String,
    pub aud: serde_json::Value,
    pub exp: usize,
    pub iat: usize,
    pub client_id: String,  // This is directly available in RFC 9068
    pub jti: Option<String>, // JWT ID, optional but part of RFC 9068
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

pub async fn oauth_middleware(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Check if OAuth is enabled
    if !state.oauth_config.enabled {
        return Ok(next.run(request).await);
    }

    // Extract Bearer token from Authorization header
    let token = match extract_bearer_token(&headers) {
        Some(token) => token,
        None => {
            // No OAuth token found, continue with existing crypto auth
            return Ok(next.run(request).await);
        }
    };

    // Validate JWT token
    match validate_jwt_token(&state, &token).await {
        Ok(claims) => {
            // Convert JWT claims to user hash for compatibility
            let user_hash = generate_user_hash_from_claims(&claims, &state.oauth_config.user_id_claim);
            
            // Add user hash to request extensions for route handlers
            request.extensions_mut().insert(user_hash);
            
            Ok(next.run(request).await)
        }
        Err(_) => Err(StatusCode::UNAUTHORIZED),
    }
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
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

fn generate_user_hash_from_claims(claims: &Claims, user_id_claim: &str) -> String {
    use sha2::{Sha256, Digest};
    
    // Extract user ID from the specified claim
    let user_id = if user_id_claim == "sub" {
        claims.sub.clone()
    } else {
        claims.extra.get(user_id_claim)
            .and_then(|v| v.as_str())
            .unwrap_or(&claims.sub)
            .to_string()
    };
    
    let combined = format!("oauth:{}:{}", claims.iss, user_id);
    
    let mut hasher = Sha256::new();
    hasher.update(combined.as_bytes());
    let result = hasher.finalize();
    
    base64::encode(result)
}