use crossbeam::channel::Sender;
use eventplanedb_access::job_error::JobError;
use eventplanedb_crypto::Crypto;
use eventplanedb_thread_worker::{event_notifications::EventNotifier, job::Job, process_jobs::create_thread_pool};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub workers: Arc<Vec<Sender<Job>>>,
    pub event_notifier: EventNotifier,
    pub base_path: String,
    pub read_max_bytes: usize,
    pub subscribe_cooldown_period_ms: u64,
}

impl AppState {
    pub fn new(base_path: String) -> Self {
        let cores = core_affinity::get_core_ids().unwrap_or_else(|| vec![core_affinity::CoreId { id: 0 }]);
        let event_notifier = EventNotifier::new();
        let workers = create_thread_pool(cores.len(), event_notifier.clone());

        Self {
            workers: Arc::new(workers),
            event_notifier,
            base_path,
            read_max_bytes: 1024 * 1024, // 1MB
            subscribe_cooldown_period_ms: 300
        }
    }

    pub fn server_time(&self) -> u64 {
        chrono::Utc::now().timestamp_millis() as u64
    }

    pub fn get_file_path(&self, pi: &str) -> String {
        format!("{}/{}.dat", self.base_path, pi)
    }

    pub fn validate_auth_params(&self, public_key: &str, nonce: &str, signature: &str) -> Result<String, JobError> {
        match Crypto::validate_with_public_key(public_key, nonce, signature) {
            Ok(cb) => Ok(cb),
            Err(e) => Err(JobError::InvalidParameters(e.to_string())),
        }
    }

    pub fn validate_auth_headers(&self, headers: &axum::http::HeaderMap) -> Result<String, JobError> {
        let public_key = match headers.get("X-Public-Key").and_then(|h| h.to_str().ok()) {
            Some(pk) => pk.to_string(),
            None => return Err(JobError::InvalidParameters("Missing X-Public-Key header".to_string())),
        };

        let nonce = match headers.get("X-Nonce").and_then(|h| h.to_str().ok()) {
            Some(n) => n.to_string(),
            None => return Err(JobError::InvalidParameters("Missing X-Nonce header".to_string())),
        };

        let sign = match headers.get("X-Signature").and_then(|h| h.to_str().ok()) {
            Some(s) => s.to_string(),
            None => return Err(JobError::InvalidParameters("Missing X-Signature header".to_string())),
        };

        match Crypto::validate_with_public_key(&public_key, &nonce, &sign) {
            Ok(cb) => Ok(cb),
            Err(e) => Err(JobError::InvalidParameters(e.to_string())),
        }
    }
}