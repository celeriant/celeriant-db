use axum::{
    extract::{Path, Query},
    http::{HeaderValue, Method, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use event_storage::event_item::EventItem;
use event_storage::event_batch_item::EventBatchItem;
use event_storage_threads::{create_thread_pool, read_async, write_async, Job};
use crossbeam::channel::Sender;
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use std::{collections::HashMap, time::Duration};
use std::sync::Arc;
use tokio::sync::RwLock;
use core_affinity;

#[derive(Debug, Serialize, Deserialize)]
struct WriteRequest {
    events: Vec<EventItem>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WriteResponse {
    #[serde(rename = "serverId")]
    server_id: u64,
    #[serde(rename = "eventDate")]
    event_date: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReadResponse {
    events: Vec<EventItem>,
    server_id: u64,
}


#[derive(Debug, Deserialize)]
struct ShareQuery {
    pi: String,
    #[serde(rename = "publicKey")]
    public_key: String,
    nonce: String,
    sign: String,
    #[serde(rename = "isOwner")]
    is_owner: bool,
    #[serde(rename = "singleUse")]
    single_use: bool,
    iv: Option<String>,
    description: Option<String>,
    #[serde(rename = "expiresOn")]
    expires_on: i64,
    #[serde(rename = "readOnly")]
    read_only: bool,
}

#[derive(Debug, Serialize)]
struct ShareResponse {
    #[serde(rename = "shareKey")]
    share_key: String,
    #[serde(rename = "shareEvent")]
    share_event: EventItem,
}

#[derive(Debug, Deserialize)]
struct ReadQuery {
    pi: String,
    #[serde(rename = "publicKey")]
    public_key: String,
    nonce: String,
    sign: String,
    #[serde(rename = "fromTime")]
    from_time: i64,
    #[serde(rename = "createIfNotExist")]
    create_if_not_exist: bool,
    #[serde(rename = "shareKey")]
    share_key: Option<String>,
    max_bytes: Option<usize>,
}


#[derive(Debug, Deserialize)]
struct WriteQuery {
    pi: String,
    #[serde(rename = "publicKey")]
    public_key: String,
    nonce: String,
    sign: String,
    #[serde(rename = "createIfNotExist")]
    create_if_not_exist: bool,
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

// POST /write - Write events with query parameters
async fn write_events(
    Query(params): Query<WriteQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(events): Json<Vec<EventItem>>,
) -> Result<Json<WriteResponse>, (StatusCode, String)> {
    if events.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "No events provided".to_string()));
    }

    // TODO: Implement authentication logic using:
    // - params.pi (project identifier)
    // - params.public_key
    // - params.nonce
    // - params.sign
    // - params.create_if_not_exist

    let file_path = state.get_file_path(&params.pi);
    let event_date = chrono::Utc::now().timestamp_millis();
    
    // Create an EventBatchItem from the events
    let event_batch = EventBatchItem {
        si: 0, // Will be assigned by the storage system
        cb: None,
        sd: event_date as u64,
        events: events.clone(),
    };

    match write_async(&state.workers, file_path, params.create_if_not_exist, event_batch).await {
        Ok(si) => {
            // Update file paths registry
            let mut paths = state.file_paths.write().await;
            paths.insert(params.pi.clone(), state.get_file_path(&params.pi));
            
            Ok(Json(WriteResponse {
                server_id: 12345, // TODO: Implement proper server ID logic
                event_date,
            }))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to write events: {}", e),
        )),
    }
}


// POST /share - Create a share link
async fn share(
    Query(params): Query<ShareQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<ShareResponse>, (StatusCode, String)> {
    // TODO: Implement authentication logic using:
    // - params.pi (project identifier)
    // - params.public_key
    // - params.nonce
    // - params.sign
    // - params.is_owner
    
    let share_key = nanoid::nanoid!();
    
    // Create EventItem with tp == 43 (share event type)
    let share_event = EventItem {
        tp: 43,
        ed: chrono::Utc::now().timestamp_millis() as u64,
        iv: None,
        int_values: None,
        uint_values: None,
        f32_values: None,
        f64_values: None,
        bool_values: None,
        string_values: None,
        byte_arrays: None,
    };

    Ok(Json(ShareResponse {
        share_key: share_key.clone(),
        share_event,
    }))
}

// GET /events/{file_id} - Read events from a file
async fn read_events(
    Query(params): Query<ReadQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<ReadResponse>, (StatusCode, String)> {
    // TODO: Implement authentication logic using:
    // - params.pi (project identifier)
    // - params.public_key
    // - params.nonce
    // - params.sign
    // - params.share_key (optional)
    
    // Use pi as the file_id
    let file_path = state.get_file_path(&params.pi);
    let from_si = params.from_time.max(0) as u64; // Convert fromTime to from_si
    let max_bytes = usize::MAX; // Default max bytes
    
    // TODO: Handle params.create_if_not_exist logic

    match read_async(&state.workers, file_path, from_si, max_bytes).await {
        Ok(result) => {
            // Flatten all events from all batches
            let all_events: Vec<EventItem> = result.event_batches.iter().flat_map(|batch| batch.events.iter().cloned()).collect();

            Ok(Json(ReadResponse {
                events: all_events,
                server_id: 12345, // TODO: Implement proper server ID logic
            }))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to read events: {}", e),
        )),
    }
}

pub fn create_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin([
            "http://localhost:5174".parse::<HeaderValue>().unwrap(),
            "https://colorsquare.org".parse::<HeaderValue>().unwrap(),
        ])
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
        .max_age(Duration::from_secs(86400)); // 24 hours

    let api = Router::new()
        .route("/read", get(read_events))
        .route("/write", post(write_events))
        .route("/share", post(share));

    Router::new()
        .nest("/api", api)
        .layer(cors)
        .with_state(state)
}
