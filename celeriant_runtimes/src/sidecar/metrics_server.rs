use axum::{Router, routing::get, response::IntoResponse, http::StatusCode};
use metrics_exporter_prometheus::PrometheusHandle;
use std::net::SocketAddr;

use super::sidecar_config::SidecarConfig;

pub fn install_recorder() -> PrometheusHandle {
    let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets(&[0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0])
        .expect("invalid bucket configuration")
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
    describe_gauge!("celeriant_replication_queue_bytes", "Replication queue bytes awaiting send");
    describe_gauge!("celeriant_replication_queue_high_water_bytes", "Replication queue threshold for S3 fallback");
    describe_gauge!("celeriant_replication_follower_pressured", "1 when follower is falling behind (S3 fallback imminent)");
    describe_counter!("celeriant_replication_s3_fallbacks_total", "Replication S3 fallbacks");
    describe_counter!("celeriant_replication_rollback_retries_total", "Replication rollback retry attempts");
    describe_counter!("celeriant_replication_rollback_io_error_total", "Replication rollback I/O failures");
    describe_counter!("celeriant_replication_rollback_lock_timeout_total", "Replication rollback aborted: could not acquire lock");
    describe_counter!("celeriant_replication_intra_batch_chain_break_total", "Chain break detected within a single replication batch");
    describe_counter!("celeriant_replication_tip_hash_mismatch_kick_total", "Follower tip-hash mismatch; kicked into S3 catchup");
    describe_counter!("celeriant_replicate_stale_lease_total", "Replication rejected by follower: stale leader lease");
    describe_counter!("celeriant_coordinator_gate_timeout_total", "Replication coordinator two-phase gate timeout");
    describe_counter!("celeriant_s3_catchup_rounds_total", "S3 catchup rounds executed");
    describe_counter!("celeriant_replication_applied_events_total", "Events applied via replication or S3 catchup");
    describe_counter!("celeriant_replication_applied_bytes_total", "Payload bytes applied via replication or S3 catchup");
    describe_counter!("celeriant_replication_snapshot_returned_to_queue_total", "Replication snapshot re-queued after BudgetExhausted (avoids rollback racing in-flight S3 PUT)");
    describe_counter!("celeriant_drain_role_change_total", "Pending-replication drain attempts on role change (labels: invariant_holds=true|false)");
    describe_counter!("celeriant_promotion_batch_budget_exceeded_total", "Promotion-batch upload skipped: scan exceeded max_promotion_batch_bytes");

    // Probe (reachability / gap-fill)
    describe_counter!("celeriant_probe_total", "Replication probe attempts");
    describe_counter!("celeriant_probe_outcome_current_total", "Probe outcome: follower at expected tip");
    describe_counter!("celeriant_probe_outcome_gap_detected_total", "Probe outcome: follower behind, gap to fill");
    describe_counter!("celeriant_probe_outcome_network_error_total", "Probe outcome: network error");
    describe_counter!("celeriant_probe_gap_send_success_total", "Probe gap-fill batch sent successfully");
    describe_counter!("celeriant_probe_gap_send_failed_total", "Probe gap-fill batch send failed");
    describe_histogram!("celeriant_probe_duration_seconds", "Probe round-trip duration");
    describe_histogram!("celeriant_probe_gap_size", "Detected probe gap size in WAL entries");

    // TCP catchup
    describe_counter!("celeriant_catchup_fallback_total", "TCP catchup abandoned for S3 fallback (label: reason)");
    describe_counter!("celeriant_catchup_fetch_error_total", "Catchup entry fetch errors (persistent growth while a follower lags = convergence livelock)");
    describe_counter!("celeriant_catchup_empty_fetch_total", "Catchup fetch returned no entries for a nonzero gap (livelock signature when nothing was compacted)");

    // Delete / recreate integrity signals
    describe_counter!("celeriant_tombstone_snapshot_regression_total", "Tombstone cache writes that regressed a newer cached version (stale-tombstone signature)");
    describe_counter!("celeriant_position_snapshot_stale_commit_total", "Committed batches carrying a version at or below the cached one");

    // Cache
    describe_gauge!("celeriant_cache_recent_write_bytes", "Recent write cache usage");
    describe_counter!("celeriant_cache_recent_write_hits_total", "Recent write cache hits");
    describe_counter!("celeriant_cache_recent_write_misses_total", "Recent write cache misses");
    describe_counter!("celeriant_cache_aggregate_snapshot_hits_total", "Aggregate snapshot LRU hits");
    describe_counter!("celeriant_cache_aggregate_snapshot_misses_total", "Aggregate snapshot LRU misses");
    describe_counter!("celeriant_cache_log_file_hits_total", "Log file LRU hits");
    describe_counter!("celeriant_cache_log_file_misses_total", "Log file LRU misses");

    // WAL and storage
    describe_gauge!("celeriant_wal_seq", "Current WAL sequence");
    describe_gauge!("celeriant_log_segments_total", "Active log segment count");
    describe_counter!("celeriant_log_rotations_total", "Log file rotations");
    describe_counter!("celeriant_log_segment_close_total", "Log segment file close events");
    describe_counter!("celeriant_orphan_segment_recovered_total", "Orphaned log segments cleaned up on boot");

    // Connections
    describe_gauge!("celeriant_client_connections_active", "Open client TCP connections");
    describe_counter!("celeriant_connection_redirects_total", "Cross-shard connection redirects");
    describe_counter!("celeriant_mesh_channel_full_total", "Mesh channel full events by message type");
    describe_gauge!("celeriant_watch_subscribers_active", "Active watch subscriptions");

    // Cluster
    describe_gauge!("celeriant_node_role", "1 = leader, 0 = follower");
    describe_counter!("celeriant_heartbeat_failures_total", "Failed heartbeats");
    describe_counter!("celeriant_heartbeat_kernel_blocked_total", "Heartbeat hard-timeout (kernel TCP send blocked)");
    describe_counter!("celeriant_heartbeat_attempts_total", "Heartbeat loop iterations (every send attempt, including happy ACKs)");
    describe_counter!("celeriant_heartbeat_acks_total", "Heartbeat ACKs received (happy path, no log emitted)");
    describe_counter!("celeriant_heartbeat_outcomes_total", "Heartbeat send outcomes by category (labels: shard_id, outcome=ack|rejected_not_follower|rejected_clock_drift|network_error|lock_timeout|hard_timeout)");
    describe_counter!("celeriant_heartbeat_received_total", "Heartbeats received on the follower path (counted before any classification)");
    describe_counter!("celeriant_heartbeat_received_outcomes_total", "Heartbeat receive outcomes (labels: shard_id, outcome=accepted_extended|accepted_no_extension|rejected_not_follower|rejected_clock_drift). Compare with celeriant_heartbeat_outcomes_total{outcome=ack} on the leader to detect asymmetries.");
    describe_counter!("celeriant_s3_lease_writes_total", "S3 lease record writes (labels: shard_id, reason=preemptive|proactive|discovery|challenge|post_catchup)");
    describe_gauge!("celeriant_s3_lease_age_seconds", "Informational: seconds since last successful S3 lease write. By design this drifts unbounded while heartbeats succeed (steady-state invariant — S3 is not touched).");
    describe_gauge!("celeriant_lease_remaining_ms", "Local lease TTL remaining in ms. Goes to zero when must_fence fires (labels: shard_id, role=leader|follower)");
    describe_counter!("celeriant_leader_self_fence_total", "Times the leader observed must_fence firing while raw status was still Leader (TTL exhausted before next renewal)");
    describe_counter!("celeriant_leader_elections_total", "Leadership transitions");
    describe_counter!("celeriant_follower_auto_fence_total", "Follower self-fenced (lease ownership lost mid-flight)");
    describe_counter!("celeriant_lease_budget_exhausted_total", "Lease-bounded operation aborted: budget exhausted");
    describe_gauge!("celeriant_clock_drift_ms", "Observed clock drift between nodes");
    describe_counter!("celeriant_s3_lease_renewal_requested_total", "On-demand lease-renewal nudges sent by data shards to shard 0 (labels: shard_id, result=sent|dropped)");
    describe_counter!("celeriant_s3_lease_renewal_handled_total", "Shard-0 on-demand renewal handler outcomes (labels: result=not_leader|debounced|attempted)");
    describe_histogram!("celeriant_s3_lease_cas_duration_seconds", "Latency of the run_election_to_acquire_s3_lease CAS round-trip (labels: reason)");
    describe_counter!("celeriant_s3_lease_superseded_total", "On-demand renewal concluded superseded / self-fence (labels: peer_present=true|false)");
    describe_counter!("celeriant_node_role_transitions_total", "Shard-0 leader<->follower role flips (labels: to=leader|follower)");

    // Stability
    describe_counter!("celeriant_node_starts_total", "Node boot and restart cycles");
    describe_counter!("celeriant_shard_panics_total", "Shard executor panics");
    describe_counter!("celeriant_shard_restarts_total", "Shard executor restart attempts");
}
