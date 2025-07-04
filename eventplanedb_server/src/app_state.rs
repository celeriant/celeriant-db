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
}