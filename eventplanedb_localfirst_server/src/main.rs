use axum::{
    Router,
    http::{HeaderValue, Method},
    middleware::from_fn,
    response::IntoResponse,
    routing::{get, post},
};
use axum_prometheus::{EndpointLabel, PrometheusMetricLayerBuilder};
use reqwest::StatusCode;
use std::{env, time::Duration};
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    limit::RequestBodyLimitLayer,
};
use tracing::{Level, info};
use tracing_subscriber::{EnvFilter, FmtSubscriber, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    app_state::AppState,
    correlation_id::correlation_id_middleware,
    // routes::{
    //     delete::delete, disable_client::disable_client, disable_share::disable_share,
    //     disable_user::disable_user, read::read_events, share::share, subscribe::subscribe_events,
    //     write::write_events,
    // },
};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod app_state;
mod correlation_id;
mod job_context;
mod job_error;
// mod error_response;
// mod internal_aggregates;
mod json_formatter;
// mod routes;

fn main() {
    println!("Hello, world!");
}
