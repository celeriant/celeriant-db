use axum::{
    Router,
    http::{HeaderValue, Method},
    routing::{get, post},
};
use std::{env, time::Duration};
use tower_http::cors::{Any, CorsLayer};

use crate::{
    app_state::AppState,
    routes::{
        delete::delete, disable_share::disable_share, disable_user::disable_user, read::read_events, restore::restore, share::share,
        subscribe::subscribe_events, write::write_events,
    },
};

mod app_state;
mod crypto;
mod json_formatter;
mod routes;

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
    let addr = format!("0.0.0.0:{port}");

    println!("Starting EventPlaneDB server on {addr}");

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

    let api_v1 = Router::new()
        .route("/{pi}/read", get(read_events))
        .route("/{pi}/subscribe", get(subscribe_events))
        .route("/{pi}/write", post(write_events))
        .route("/{pi}/delete", post(delete))
        .route("/{pi}/restore", post(restore))
        .route("/{pi}/disableshare/{share_hash}", post(disable_share))
        .route("/{pi}/disableuser/{user_hash}", post(disable_user))
        .route("/{pi}/share", post(share));

    Router::new().nest("/api/v1", api_v1).layer(cors).with_state(state)
}
