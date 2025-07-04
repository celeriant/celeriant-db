use axum::{http::{HeaderValue, Method}, routing::{get, post}, Router};
use tower_http::cors::{Any, CorsLayer};
use std::{env, time::Duration};

use crate::{app_state::AppState, routes::{read::read_events, share::share, write::write_events}};

mod app_state;
mod routes;
mod crypto;

#[tokio::main]
async fn main() {
    // Get base path from environment variable or use default
    let base_path = env::var("DATA_PATH").unwrap_or_else(|_| "./data".to_string());
    
    // Create data directory if it doesn't exist
    std::fs::create_dir_all(&base_path).expect("Failed to create data directory");
    
    // Create application state
    let app_state = AppState::new(base_path);

    //TODO: Enforce maximum upload size

    //TODO: Rate limiting
    
    // Create the router
    let app = create_router(app_state);
    
    // Get port from environment or use default
    let port = env::var("PORT").unwrap_or_else(|_| "5198".to_string());
    let addr = format!("0.0.0.0:{}", port);
    
    println!("Starting EventPlaneDB server on {}", addr);
    
    // Start the server
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
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
