use eventplanedb_server::{create_router, AppState};
use std::env;

#[tokio::main]
async fn main() {
    // Get base path from environment variable or use default
    let base_path = env::var("DATA_PATH").unwrap_or_else(|_| "./data".to_string());
    
    // Create data directory if it doesn't exist
    std::fs::create_dir_all(&base_path).expect("Failed to create data directory");
    
    // Create application state
    let app_state = AppState::new(base_path);
    
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