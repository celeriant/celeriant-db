use std::sync::Arc;
use crossbeam::channel::Sender;
use event_storage_threads::{job::Job, process_jobs::create_thread_pool};

#[derive(Clone)]
pub struct AppState {
    pub workers: Arc<Vec<Sender<Job>>>,
    pub base_path: String,
}

impl AppState {
    pub fn new(base_path: String) -> Self {
        let cores = core_affinity::get_core_ids().unwrap_or_else(|| vec![core_affinity::CoreId { id: 0 }]);
        let workers = create_thread_pool(cores.len());
        
        Self {
            workers: Arc::new(workers),
            base_path,
        }
    }

    pub fn get_file_path(&self, pi: &str) -> String {
        format!("{}/{}.dat", self.base_path, pi)
    }
    
    pub fn validate_auth_headers(&self, headers: &axum::http::HeaderMap) -> Result<String, (axum::http::StatusCode, String)> {
        let public_key = match headers.get("X-Public-Key").and_then(|h| h.to_str().ok()) {
            Some(pk) => pk.to_string(),
            None => return Err((axum::http::StatusCode::BAD_REQUEST, "Missing X-Public-Key header".to_string())),
        };

        let nonce = match headers.get("X-Nonce").and_then(|h| h.to_str().ok()) {
            Some(n) => n.to_string(),
            None => return Err((axum::http::StatusCode::BAD_REQUEST, "Missing X-Nonce header".to_string())),
        };

        let sign = match headers.get("X-Signature").and_then(|h| h.to_str().ok()) {
            Some(s) => s.to_string(),
            None => return Err((axum::http::StatusCode::BAD_REQUEST, "Missing X-Signature header".to_string())),
        };

        match crate::crypto::Crypto::validate_with_public_key(&public_key, &nonce, &sign) {
            Ok(cb) => Ok(cb),
            Err(e) => Err((axum::http::StatusCode::UNAUTHORIZED, e.to_string())),
        }
    }
}