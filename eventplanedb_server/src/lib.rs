use axum::{
    extract::{Path, Query},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use event_storage::event_item::EventItem;
use event_storage::event_batch_item::EventBatchItem;
use event_storage_threads::{create_thread_pool, read_async, write_async, Job};
use crossbeam::channel::Sender;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use core_affinity;

#[derive(Debug, Serialize, Deserialize)]
struct WriteRequest {
    events: Vec<EventItem>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WriteResponse {
    si: u64,
    events_written: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReadResponse {
    events: Vec<EventItem>,
    total_events: usize,
}

#[derive(Debug, Deserialize)]
struct ReadQuery {
    from_si: Option<u64>,
    max_bytes: Option<usize>,
}

#[derive(Clone)]
pub struct AppState {
    pub workers: Arc<Vec<Sender<Job>>>,
    pub file_paths: Arc<RwLock<HashMap<String, String>>>,
    pub base_path: String,
}

impl AppState {
    pub fn new(base_path: String) -> Self {
        let cores = core_affinity::get_core_ids().unwrap_or_else(|| vec![core_affinity::CoreId { id: 0 }]);
        let workers = create_thread_pool(cores.len());
        
        Self {
            workers: Arc::new(workers),
            file_paths: Arc::new(RwLock::new(HashMap::new())),
            base_path,
        }
    }

    fn get_file_path(&self, file_id: &str) -> String {
        format!("{}/{}.dat", self.base_path, file_id)
    }
}

// POST /events/{file_id} - Write events to a file
async fn write_events(
    Path(file_id): Path<String>,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(request): Json<WriteRequest>,
) -> Result<Json<WriteResponse>, (StatusCode, String)> {
    if request.events.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "No events provided".to_string()));
    }

    let file_path = state.get_file_path(&file_id);
    
    // Create an EventBatchItem from the events
    let event_batch = EventBatchItem {
        si: 0, // Will be assigned by the storage system
        cb: None,
        sd: chrono::Utc::now().timestamp_millis() as u64,
        events: request.events.clone(),
    };

    match write_async(&state.workers, file_path, true, event_batch).await {
        Ok(si) => {
            // Update file paths registry
            let mut paths = state.file_paths.write().await;
            paths.insert(file_id.clone(), state.get_file_path(&file_id));
            
            Ok(Json(WriteResponse {
                si,
                events_written: request.events.len(),
            }))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to write events: {}", e),
        )),
    }
}

// GET /events/{file_id} - Read events from a file
async fn read_events(
    Path(file_id): Path<String>,
    Query(params): Query<ReadQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<ReadResponse>, (StatusCode, String)> {
    let file_path = state.get_file_path(&file_id);
    let from_si = params.from_si.unwrap_or(0);
    let max_bytes = params.max_bytes.unwrap_or(usize::MAX);

    match read_async(&state.workers, file_path, from_si, max_bytes).await {
        Ok(result) => {
            // Flatten all events from all batches
            let all_events: Vec<EventItem> = result.event_batches.iter().flat_map(|batch| batch.events.iter().cloned()).collect();

            Ok(Json(ReadResponse {
                total_events: all_events.len(),
                events: all_events,
            }))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to read events: {}", e),
        )),
    }
}

// GET /files - List all available files
async fn list_files(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let paths = state.file_paths.read().await;
    let file_ids: Vec<String> = paths.keys().cloned().collect();
    Ok(Json(file_ids))
}

// Health check endpoint
async fn health_check() -> &'static str {
    "OK"
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/events/{file_id}", post(write_events))
        .route("/events/{file_id}", get(read_events))
        .route("/files", get(list_files))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum_test::TestServer;
    use tempfile::TempDir;
    use event_storage::event_item::EventItem;

    fn create_test_event() -> EventItem {
        let mut event = EventItem::new();
        event.ed = 12345;
        event.iv = Some("test_event".to_string());
        event.tp = 100;
        event.int_values = Some(vec![1, 2, 3, 4, 5]);
        event.string_values = Some(vec![Some("test".to_string()), None, Some("data".to_string())]);
        event
    }

    #[tokio::test]
    async fn test_write_and_read_events() {
        let temp_dir = TempDir::new().unwrap();
        let app_state = AppState::new(temp_dir.path().to_string_lossy().to_string());
        let app = create_router(app_state);
        let server = TestServer::new(app).unwrap();

        // Test write
        let write_request = WriteRequest {
            events: vec![create_test_event(), create_test_event()],
        };

        let response = server
            .post("/events/test_file")
            .json(&write_request)
            .await;

        assert_eq!(response.status_code(), StatusCode::OK);
        
        let write_response: WriteResponse = response.json();
        assert_eq!(write_response.events_written, 2);
        assert!(write_response.si > 0);

        // Test read
        let response = server
            .get("/events/test_file")
            .await;

        assert_eq!(response.status_code(), StatusCode::OK);
        
        let read_response: ReadResponse = response.json();
        assert_eq!(read_response.total_events, 2);
        assert_eq!(read_response.events.len(), 2);
    }

    #[tokio::test]
    async fn test_list_files() {
        let temp_dir = TempDir::new().unwrap();
        let app_state = AppState::new(temp_dir.path().to_string_lossy().to_string());
        let app = create_router(app_state);
        let server = TestServer::new(app).unwrap();

        // Write to a file first
        let write_request = WriteRequest {
            events: vec![create_test_event()],
        };

        let _response = server
            .post("/events/test_file")
            .json(&write_request)
            .await;

        // Test list files
        let response = server.get("/files").await;
        assert_eq!(response.status_code(), StatusCode::OK);
        
        let files: Vec<String> = response.json();
        assert!(files.contains(&"test_file".to_string()));
    }

    #[tokio::test]
    async fn test_empty_events_error() {
        let temp_dir = TempDir::new().unwrap();
        let app_state = AppState::new(temp_dir.path().to_string_lossy().to_string());
        let app = create_router(app_state);
        let server = TestServer::new(app).unwrap();

        let write_request = WriteRequest {
            events: vec![],
        };

        let response = server
            .post("/events/test_file")
            .json(&write_request)
            .await;

        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    }
}