use axum::{extract::Request, middleware::Next, response::Response};
use tracing::info_span;
use uuid::Uuid;

// Correlation ID header name
pub const X_CORRELATION_ID: &str = "x-correlation-id";

// Middleware to handle correlation IDs
pub async fn correlation_id_middleware(request: Request, next: Next) -> Response {
    // Try to extract correlation ID from headers or generate a new one
    let correlation_id = request
        .headers()
        .get(X_CORRELATION_ID)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Create a span with the correlation ID
    let span = info_span!(
        "request",
        correlation_id = %correlation_id,
        method = %request.method(),
        uri = %request.uri(),
    );

    // Execute the rest of the stack inside the span
    let response = {
        let _guard = span.enter();
        tracing::debug!("Processing request");

        // Clone the request with a new header containing the correlation ID
        let mut request = request;
        request.headers_mut().insert(X_CORRELATION_ID, correlation_id.parse().unwrap());

        next.run(request).await
    };

    // Return response with correlation ID header added
    let mut response = response;
    response.headers_mut().insert(X_CORRELATION_ID, correlation_id.parse().unwrap());

    response
}
