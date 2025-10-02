use axum::{
    body::Body,
    http::{HeaderValue, Response, StatusCode, header},
    response::{IntoResponse, Response as AxumResponse},
};
use serde::Serialize;

pub struct CompactJson<T>(pub T);

impl<T> IntoResponse for CompactJson<T>
where
    T: Serialize,
{
    fn into_response(self) -> AxumResponse {
        match simd_json::to_string(&self.0) {
            Ok(json) => Response::builder()
                .header(header::CONTENT_TYPE, HeaderValue::from_static("application/json"))
                .body(Body::from(json))
                .unwrap(),
            Err(err) => {
                let body = format!("JSON serialization error: {}", err);
                Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from(body)).unwrap()
            }
        }
    }
}
