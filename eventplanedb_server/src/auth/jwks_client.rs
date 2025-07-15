use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Jwks {
    pub keys: Vec<JwkKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwkKey {
    pub kty: String,
    pub kid: String,
    #[serde(rename = "use")]
    pub use_: Option<String>,
    pub n: String,
    pub e: String,
    pub alg: Option<String>,
}

impl Jwks {
    pub fn find_key(&self, kid: &str) -> Option<&JwkKey> {
        self.keys.iter().find(|key| key.kid == kid)
    }
}

pub struct JwksClient {
    cache: Arc<RwLock<Option<(Jwks, Instant)>>>,
    client: reqwest::Client,
    cache_duration: Duration,
    jwks_url: String,
}

impl JwksClient {
    pub fn new(jwks_url: String) -> Self {
        Self {
            cache: Arc::new(RwLock::new(None)),
            client: reqwest::Client::new(),
            cache_duration: Duration::from_secs(3600), // 1 hour
            jwks_url,
        }
    }

    pub async fn get_jwks(&self) -> Result<Jwks, Box<dyn std::error::Error>> {
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some((jwks, cached_at)) = cache.as_ref() {
                if cached_at.elapsed() < self.cache_duration {
                    return Ok(jwks.clone());
                }
            }
        }

        // Fetch from remote using reqwest
        let response = self.client.get(&self.jwks_url).send().await?;
        let jwks: Jwks = response.json().await?;

        // Update cache
        {
            let mut cache = self.cache.write().await;
            *cache = Some((jwks.clone(), Instant::now()));
        }

        Ok(jwks)
    }
}