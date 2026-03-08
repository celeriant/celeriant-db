use axum::{Router, routing::get, response::IntoResponse, http::StatusCode};
use metrics_exporter_prometheus::PrometheusHandle;
use std::net::SocketAddr;

use super::sidecar_config::SidecarConfig;

pub fn install_recorder() -> PrometheusHandle {
    let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("Failed to install Prometheus metrics recorder");
    register_metric_descriptions();
    handle
}

pub async fn run_metrics_server(config: SidecarConfig, handle: PrometheusHandle) {
    let num_shards = config.num_shards;
    let node_id = config.node_id;

    let app = Router::new()
        .route("/metrics", get(move || {
            let handle = handle.clone();
            async move { handle.render() }
        }))
        .route("/health", get(move || async move {
            (StatusCode::OK, axum::Json(serde_json::json!({
                "status": "ok",
                "node_id": format!("{node_id}"),
                "shards": num_shards
            })))
                .into_response()
        }));

    let addr = SocketAddr::from(([0, 0, 0, 0], config.metrics_port));
    tracing::info!("Metrics server listening on {}", addr);

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind metrics server to {}: {}", addr, e);
            return;
        }
    };

    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("Metrics server error: {}", e);
    }
}

fn register_metric_descriptions() {
    use metrics::{describe_counter, describe_gauge, describe_histogram};

    // Operations
    describe_counter!("celeriant_writes_total", "Successful write operations");
    describe_counter!("celeriant_reads_total", "Successful read operations");
    describe_counter!("celeriant_deletes_total", "Successful delete operations");
    describe_counter!("celeriant_trims_total", "Successful trim operations");
    describe_counter!("celeriant_write_errors_total", "Failed writes");
    describe_counter!("celeriant_read_errors_total", "Failed reads");
    describe_counter!("celeriant_write_events_total", "Total events written");
    describe_counter!("celeriant_write_bytes_total", "Total payload bytes written");
    describe_counter!("celeriant_read_bytes_total", "Total payload bytes read");

    // Latency
    describe_histogram!("celeriant_write_duration_seconds", "End-to-end write latency");
    describe_histogram!("celeriant_read_duration_seconds", "End-to-end read latency");
    describe_histogram!("celeriant_fsync_duration_seconds", "fsync batch duration");
    describe_histogram!("celeriant_replication_duration_seconds", "Replication batch duration");

    // Durability and replication
    describe_histogram!("celeriant_fsync_batch_size", "Writers per fsync batch");
    describe_histogram!("celeriant_replication_batch_size", "Writers per replication batch");
    describe_gauge!("celeriant_replication_pending_bytes", "Pending replication queue bytes");
    describe_counter!("celeriant_replication_s3_fallbacks_total", "Replication S3 fallbacks");
    describe_counter!("celeriant_replication_rollbacks_total", "Replication rollbacks");
    describe_counter!("celeriant_s3_catchup_rounds_total", "S3 catchup rounds executed");
    describe_counter!("celeriant_replication_applied_events_total", "Events applied via replication or S3 catchup");
    describe_counter!("celeriant_replication_applied_bytes_total", "Payload bytes applied via replication or S3 catchup");

    // Cache
    describe_gauge!("celeriant_cache_recent_write_bytes", "Recent write cache usage");
    describe_counter!("celeriant_cache_recent_write_hits_total", "Recent write cache hits");
    describe_counter!("celeriant_cache_recent_write_misses_total", "Recent write cache misses");
    describe_counter!("celeriant_cache_aggregate_snapshot_hits_total", "Aggregate snapshot LRU hits");
    describe_counter!("celeriant_cache_aggregate_snapshot_misses_total", "Aggregate snapshot LRU misses");
    describe_counter!("celeriant_cache_log_file_hits_total", "Log file LRU hits");
    describe_counter!("celeriant_cache_log_file_misses_total", "Log file LRU misses");

    // WAL and storage
    describe_gauge!("celeriant_wal_index", "Current WAL index");
    describe_gauge!("celeriant_log_segments_total", "Active log segment count");
    describe_counter!("celeriant_log_rotations_total", "Log file rotations");

    // Connections
    describe_gauge!("celeriant_client_connections_active", "Open client TCP connections");
    describe_counter!("celeriant_connection_redirects_total", "Cross-shard connection redirects");
    describe_gauge!("celeriant_watch_subscribers_active", "Active watch subscriptions");

    // Cluster
    describe_gauge!("celeriant_node_role", "1 = leader, 0 = follower");
    describe_counter!("celeriant_heartbeat_failures_total", "Failed heartbeats");
    describe_counter!("celeriant_leader_elections_total", "Leadership transitions");
    describe_gauge!("celeriant_clock_drift_ms", "Observed clock drift between nodes");
}
