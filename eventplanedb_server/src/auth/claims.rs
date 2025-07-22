use serde::{Deserialize, Serialize};


#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iss: String,
    pub aud: serde_json::Value,
    pub exp: usize,
    pub iat: usize,
    pub jti: Option<String>,
    pub org_id: Option<String>,
    pub email: Option<String>,
    pub email_verified: Option<bool>,
    pub name: Option<String>,
    pub nickname: Option<String>,
    pub picture: Option<String>,
}