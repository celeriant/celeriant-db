use crossbeam::channel::Sender;
use eventplanedb_access::job_error::JobError;
use eventplanedb_crypto::Crypto;
use eventplanedb_thread_worker::{event_notifications::EventNotifier, job::Job, job_context::JobContext, process_jobs::create_thread_pool};
use std::sync::Arc;

use crate::auth::{
    claims::Claims,
    jwks_client::JwksClient,
    jwt_middleware::{extract_bearer_token, validate_jwt_token},
    oauth_config::OAuthConfig,
};

#[derive(Clone)]
pub struct AppState {
    pub workers: Arc<Vec<Sender<Job>>>,
    pub event_notifier: EventNotifier,
    pub base_path: String,
    pub read_max_bytes: usize,
    pub subscribe_cooldown_period_ms: u64,
    pub oauth_config: OAuthConfig,
    pub jwks_client: Arc<JwksClient>,
}

impl AppState {
    pub fn new(base_path: String) -> Self {
        let cores = core_affinity::get_core_ids().unwrap_or_else(|| vec![core_affinity::CoreId { id: 0 }]);
        let event_notifier = EventNotifier::new();
        let workers = create_thread_pool(cores.len(), event_notifier.clone());
        let oauth_config = OAuthConfig::from_env();
        let jwks_client = Arc::new(JwksClient::new(oauth_config.jwks_url.clone()));

        Self {
            workers: Arc::new(workers),
            event_notifier,
            base_path,
            read_max_bytes: 1024 * 1024, // 1MB
            subscribe_cooldown_period_ms: 300,
            oauth_config,
            jwks_client,
        }
    }

    pub async fn create_job_context(&self, aggregate_id: String, headers: &axum::http::HeaderMap) -> Result<JobContext, JobError> {
        let (current_user_id, current_org_id) = self
            .get_claims(headers)
            .await?
            .map(|claims| (Some(claims.sub), claims.org_id))
            .unwrap_or((None, None));

        let context = JobContext {
            aggregate_id: aggregate_id.clone(),
            file_path: self.get_file_path(&aggregate_id),
            current_client_id: self.get_client_id(headers)?,
            current_user_id,
            current_org_id,
            server_time: self.server_time(),
        };

        Ok(context)
    }

    pub fn server_time(&self) -> u64 {
        chrono::Utc::now().timestamp_millis() as u64
    }

    pub fn get_file_path(&self, aggregate_id: &str) -> String {
        format!("{}/{}.dat", self.base_path, aggregate_id)
    }

    pub fn get_client_id_direct(&self, public_key: &str, nonce: &str, signature: &str) -> Result<u128, JobError> {
        match Crypto::validate_with_public_key(&public_key, &nonce, &signature) {
            Ok(client_id) => Ok(client_id),
            Err(e) => Err(JobError::AuthenticationFailed(e.to_string())),
        }
    }

    pub async fn get_claims_direct(&self, token: Option<&str>) -> Result<Option<Claims>, JobError> {
        match token {
            Some(bearer_token) => match validate_jwt_token(self, bearer_token).await {
                Ok(claims) => Ok(Some(claims)),
                Err(err) => Err(JobError::AuthenticationFailed(err.to_string())),
            },
            None => Ok(None),
        }
    }

    pub async fn get_claims(&self, headers: &axum::http::HeaderMap) -> Result<Option<Claims>, JobError> {
        self.get_claims_direct(extract_bearer_token(headers).as_deref()).await
    }

    pub fn get_client_id(&self, headers: &axum::http::HeaderMap) -> Result<u128, JobError> {
        let public_key = match headers.get("X-Public-Key").and_then(|h| h.to_str().ok()) {
            Some(pk) => pk.to_string(),
            None => return Err(JobError::InvalidParameters("Missing header X-Public-Key".to_string())),
        };

        let nonce = match headers.get("X-Nonce").and_then(|h| h.to_str().ok()) {
            Some(n) => n.to_string(),
            None => return Err(JobError::InvalidParameters("Missing header X-Nonce".to_string())),
        };

        let sign = match headers.get("X-Signature").and_then(|h| h.to_str().ok()) {
            Some(s) => s.to_string(),
            None => return Err(JobError::InvalidParameters("Missing header X-Signature".to_string())),
        };

        self.get_client_id_direct(&public_key, &nonce, &sign)
    }
}
