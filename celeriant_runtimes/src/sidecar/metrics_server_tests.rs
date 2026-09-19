use std::sync::{Arc, Mutex};

use metrics_exporter_prometheus::PrometheusBuilder;

use super::run_metrics_server;
use super::super::sidecar_config::SidecarConfig;

#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
    type Writer = SharedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[test]
fn no_dead_metric_descriptions_registered() {
    let source = include_str!("metrics_server.rs");

    let dead = [
        "celeriant_replication_follower_pressured",
        "celeriant_replication_rollback_retries_total",
        "celeriant_replication_rollback_io_error_total",
        "celeriant_replication_rollback_lock_timeout_total",
        "celeriant_replication_snapshot_returned_to_queue_total",
    ];

    for name in dead {
        assert!(
            !source.contains(name),
            "dead metric description {name} is still registered but never recorded"
        );
    }
}

#[test]
fn recorded_metrics_have_descriptions() {
    let source = include_str!("metrics_server.rs");

    // Every metric name the codebase records (gauge!/counter!/histogram!) must
    // have a matching describe_*! entry here, or it exports without # HELP.
    let recorded = [
        "celeriant_read_wal_seq",
        "celeriant_commit_notify_obligation_seq",
        "celeriant_replication_batch_rejected_total",
        "celeriant_s3_catchup_self_uploads_seen_total",
        "celeriant_replication_capture_outcome_total",
        "celeriant_replication_spin_timeout_total",
        "celeriant_replication_spin_fenced_total",
        "celeriant_replication_spin_terminal_total",
        "celeriant_replication_spin_retry_total",
        "celeriant_replication_confirm_loop_iterations",
        "celeriant_commit_notify_gave_up_total",
        "celeriant_cull_stale_client_seq_lru",
        "celeriant_cull_stale_agg_lru",
        "celeriant_take_pending_replication_dropped_batches",
        "celeriant_s3_fallback_lease_unconfirmed_total",
        "celeriant_barrier_sync_fsync_seconds",
        "celeriant_barrier_sync_fsync_total",
        "celeriant_barrier_sync_fsync_failed_total",
        "celeriant_probe_kick_total",
        "celeriant_s3_catchup_stall_bail_total",
        "celeriant_s3_catchup_via_s3_step_total",
        "celeriant_s3_catchup_via_s3_exhausted_total",
        "celeriant_s3_catchup_reframed_at_read_total",
        "celeriant_truncate_divergence_advanced_total",
        "celeriant_truncate_divergence_advanced_wal_seqs_total",
        "celeriant_truncate_dropped_self_acked_events_total",
        "celeriant_truncate_dropped_self_acked_wal_seqs_total",
        "celeriant_truncate_refused_due_to_ack_barrier_total",
        "celeriant_read_semaphore_wait_seconds",
        "celeriant_client_idempotency_inflight_total",
        "celeriant_client_idempotency_violations_total",
        "celeriant_client_seq_merge_on_apply_total",
        "celeriant_lease_orchestrator_unhandled_status_total",
        "celeriant_promotion_lease_renewed_total",
        "celeriant_promotion_superseded_during_catchup_total",
        "celeriant_s3_lease_on_demand_renewal_total",
        "celeriant_cache_aggregate_client_scan_found_total",
        "celeriant_cache_aggregate_client_scan_not_found_total",
        "celeriant_cache_aggregate_client_tip_hint_total",
        "celeriant_catchup_drain_late_files_total",
        "celeriant_deletes_rejected_backpressure_total",
        "celeriant_s3_catchup_live_tail_yield_total",
        "celeriant_s3_catchup_no_common_ancestor_total",
        "celeriant_s3_catchup_phantom_target_discarded_total",
        "celeriant_s3_catchup_round_cap_total",
        "celeriant_s3_catchup_same_epoch_divergence_total",
        "celeriant_s3_catchup_stalled_total",
        "celeriant_trims_rejected_backpressure_total",
        "celeriant_wal_divergent_truncations_total",
        "celeriant_writes_accepted_no_prior_client_seq_total",
        "celeriant_writes_rejected_backpressure_total",
        "celeriant_rotation_orphan_deleted_total",
    ];

    for name in recorded {
        assert!(
            source.contains(&format!("!(\"{name}\"")),
            "recorded metric {name} is not described in register_metric_descriptions()"
        );
    }
}

#[tokio::test]
async fn metrics_bind_failure_does_not_log_listening() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = occupied.local_addr().unwrap().port();

    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(SharedWriter(buffer.clone()))
        .with_max_level(tracing::Level::INFO)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let handle = PrometheusBuilder::new().build_recorder().handle();
    let config = SidecarConfig {
        worker_threads: 1,
        control_lane_capacity: 1,
        data_lane_capacity: 1,
        metrics_enabled: true,
        metrics_port: port,
        num_shards: 1,
        node_id: 0,
    };

    run_metrics_server(config, handle).await;

    let log = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    assert!(
        !log.contains("Metrics server listening"),
        "must not log 'listening' when bind fails; got: {log}"
    );
    assert!(
        log.contains("Failed to bind metrics server"),
        "must log the bind failure; got: {log}"
    );
}
