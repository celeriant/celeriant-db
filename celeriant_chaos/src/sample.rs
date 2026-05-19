use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::Serialize;

/// One snapshot of metrics from one node, scraped at one instant.
#[derive(Debug, Clone, Serialize)]
pub struct NodeSample {
    pub host: String,
    /// Milliseconds since the scraper started. Monotonic, not wall-clock.
    pub t_ms: u64,
    /// True if the HTTP scrape returned 200 with a parseable body.
    pub ok: bool,
    pub error: Option<String>,
    /// Sum of `celeriant_node_role` across all label sets. Expected 0 or 1.
    pub node_role: f64,
    pub wal_seq_max: u64,
    pub writes_total: u64,
    pub write_errors_total: u64,
    pub leader_elections_total: u64,
    pub heartbeat_failures_total: u64,
    pub s3_fallbacks_total: u64,
    pub rollbacks_total: u64,
    pub shard_panics_total: u64,
    pub node_starts_total: u64,
    /// Sum of `celeriant_client_connections_active` across all shards.
    /// Used to distinguish "listener never saw a TCP connection" from
    /// "listener saw it and rejected it" during post-promotion debugging.
    pub client_connections_active: u64,
    /// Items popped from `pending_replication_batches` but dropped on the
    /// floor because the rollback flag was set after the pop. Smoking gun
    /// for the orphan-snapshot missing-data hypothesis
    /// (`docs/missing-data.md`).
    pub capture_dropped_items_total: u64,
    pub capture_dropped_bytes_total: u64,
    /// Writes that passed idempotency validation because no prior
    /// client_seq was cached. Most are legitimate first-writes; a spike
    /// during/after a lease handover signals the duplicate-acceptance
    /// path in the failover bug.
    pub writes_accepted_no_prior_client_seq_total: u64,
    /// WAL scans that found NO metablock for the searched (aggregate, client).
    /// Used to characterise the cache miss path during/after rollback.
    pub cache_client_scan_not_found_total: u64,
    pub cache_client_scan_found_total: u64,
    /// truncate_wal dropped a committed metablock. Bytes past the read cursor
    /// that may have been acked to a client; each event is a likely false ack.
    pub truncate_dropped_committed_events_total: u64,
    /// Total bytes of committed metablocks dropped by truncate_wal.
    pub truncate_dropped_committed_bytes_total: u64,
    /// truncate_wal dropped wal_seqs this node acked as leader. Non-zero means
    /// real data loss.
    pub truncate_dropped_self_acked_events_total: u64,
    /// Sum of wal_seqs dropped across truncate_dropped_self_acked events.
    pub truncate_dropped_self_acked_wal_seqs_total: u64,
    /// S3 catchup skipped a batch this node uploaded. Counts how often the
    /// self-filter fires.
    pub s3_catchup_self_uploads_seen_total: u64,
    /// truncate_wal refused to truncate because divergent_wal_seq was at or
    /// below the cluster-wide ack barrier.
    pub truncate_refused_due_to_ack_barrier_total: u64,
    /// Subset of truncate_refused_due_to_ack_barrier where the follower signal
    /// (last_received_replication_wal_seq - 1) was the deciding blocker, i.e.
    /// it exceeded this node's last_self_acked.
    pub truncate_refused_by_follower_signal_total: u64,
}

impl NodeSample {
    pub fn unreachable(host: String, t_ms: u64, error: String) -> Self {
        Self {
            host,
            t_ms,
            ok: false,
            error: Some(error),
            node_role: 0.0,
            wal_seq_max: 0,
            writes_total: 0,
            write_errors_total: 0,
            leader_elections_total: 0,
            heartbeat_failures_total: 0,
            s3_fallbacks_total: 0,
            rollbacks_total: 0,
            shard_panics_total: 0,
            node_starts_total: 0,
            client_connections_active: 0,
            capture_dropped_items_total: 0,
            capture_dropped_bytes_total: 0,
            writes_accepted_no_prior_client_seq_total: 0,
            cache_client_scan_not_found_total: 0,
            cache_client_scan_found_total: 0,
            truncate_dropped_committed_events_total: 0,
            truncate_dropped_committed_bytes_total: 0,
            truncate_dropped_self_acked_events_total: 0,
            truncate_dropped_self_acked_wal_seqs_total: 0,
            s3_catchup_self_uploads_seen_total: 0,
            truncate_refused_due_to_ack_barrier_total: 0,
            truncate_refused_by_follower_signal_total: 0,
        }
    }
}

/// Parse a Prometheus text-format body into a NodeSample.
///
/// Only extracts the metrics the chaos runner cares about. All other lines
/// are ignored. Series with labels collapse via `agg`: sum for counters,
/// max for `wal_seq`, sum for the `node_role` gauge.
pub fn parse_metrics(host: String, t_ms: u64, body: &str) -> NodeSample {
    let mut sums: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut node_role = 0.0_f64;
    let mut wal_seq_max: u64 = 0;
    let mut client_connections_active: u64 = 0;

    const COUNTERS: &[&str] = &[
        "celeriant_writes_total",
        "celeriant_write_errors_total",
        "celeriant_leader_elections_total",
        "celeriant_heartbeat_failures_total",
        "celeriant_replication_s3_fallbacks_total",
        "celeriant_replication_rollbacks_total",
        "celeriant_shard_panics_total",
        "celeriant_node_starts_total",
        "celeriant_replication_capture_dropped_items_total",
        "celeriant_replication_capture_dropped_bytes_total",
        "celeriant_writes_accepted_no_prior_client_seq_total",
        "celeriant_cache_aggregate_client_scan_not_found_total",
        "celeriant_cache_aggregate_client_scan_found_total",
        "celeriant_truncate_dropped_committed_metablocks_events_total",
        "celeriant_truncate_dropped_committed_bytes_total",
        "celeriant_truncate_dropped_self_acked_events_total",
        "celeriant_truncate_dropped_self_acked_wal_seqs_total",
        "celeriant_s3_catchup_self_uploads_seen_total",
        "celeriant_truncate_refused_due_to_ack_barrier_total",
        "celeriant_truncate_refused_by_follower_signal_total",
    ];

    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // metric_name{labels} value [timestamp]   OR   metric_name value
        let Some((name_part, value_str)) = split_metric_line(line) else { continue };
        let name = strip_labels(name_part);

        // Histograms expand to _bucket / _sum / _count — we want neither.
        if name.ends_with("_bucket") || name.ends_with("_sum") || name.ends_with("_count") {
            continue;
        }

        if name == "celeriant_node_role" {
            if let Ok(v) = value_str.parse::<f64>() {
                node_role += v;
            }
            continue;
        }
        if name == "celeriant_wal_seq" {
            if let Ok(v) = value_str.parse::<f64>() {
                let v = v as u64;
                if v > wal_seq_max {
                    wal_seq_max = v;
                }
            }
            continue;
        }
        if name == "celeriant_client_connections_active" {
            if let Ok(v) = value_str.parse::<f64>() {
                client_connections_active = client_connections_active.saturating_add(v as u64);
            }
            continue;
        }
        if let Some(&counter) = COUNTERS.iter().find(|c| **c == name)
            && let Ok(v) = value_str.parse::<f64>()
        {
            *sums.entry(counter).or_insert(0) += v as u64;
        }
    }

    let get = |k: &str| -> u64 { sums.get(k).copied().unwrap_or(0) };

    NodeSample {
        host,
        t_ms,
        ok: true,
        error: None,
        node_role,
        wal_seq_max,
        writes_total: get("celeriant_writes_total"),
        write_errors_total: get("celeriant_write_errors_total"),
        leader_elections_total: get("celeriant_leader_elections_total"),
        heartbeat_failures_total: get("celeriant_heartbeat_failures_total"),
        s3_fallbacks_total: get("celeriant_replication_s3_fallbacks_total"),
        rollbacks_total: get("celeriant_replication_rollbacks_total"),
        shard_panics_total: get("celeriant_shard_panics_total"),
        node_starts_total: get("celeriant_node_starts_total"),
        client_connections_active,
        capture_dropped_items_total: get("celeriant_replication_capture_dropped_items_total"),
        capture_dropped_bytes_total: get("celeriant_replication_capture_dropped_bytes_total"),
        writes_accepted_no_prior_client_seq_total: get("celeriant_writes_accepted_no_prior_client_seq_total"),
        cache_client_scan_not_found_total: get("celeriant_cache_aggregate_client_scan_not_found_total"),
        cache_client_scan_found_total: get("celeriant_cache_aggregate_client_scan_found_total"),
        truncate_dropped_committed_events_total: get("celeriant_truncate_dropped_committed_metablocks_events_total"),
        truncate_dropped_committed_bytes_total: get("celeriant_truncate_dropped_committed_bytes_total"),
        truncate_dropped_self_acked_events_total: get("celeriant_truncate_dropped_self_acked_events_total"),
        truncate_dropped_self_acked_wal_seqs_total: get("celeriant_truncate_dropped_self_acked_wal_seqs_total"),
        s3_catchup_self_uploads_seen_total: get("celeriant_s3_catchup_self_uploads_seen_total"),
        truncate_refused_due_to_ack_barrier_total: get("celeriant_truncate_refused_due_to_ack_barrier_total"),
        truncate_refused_by_follower_signal_total: get("celeriant_truncate_refused_by_follower_signal_total"),
    }
}

fn split_metric_line(line: &str) -> Option<(&str, &str)> {
    // Tail-split on the first whitespace AFTER the value: name [TS]
    // Prometheus text uses spaces; format is "name value [timestamp]".
    let mut parts = line.splitn(3, ' ');
    let name = parts.next()?;
    let value = parts.next()?;
    Some((name, value))
}

fn strip_labels(name_part: &str) -> &str {
    match name_part.find('{') {
        Some(i) => &name_part[..i],
        None => name_part,
    }
}

/// Convert an `Instant` offset into milliseconds since `start`.
pub fn elapsed_ms(start: Instant, now: Instant) -> u64 {
    now.saturating_duration_since(start).as_millis() as u64
}

/// Sleep helper that uses the chosen scrape interval.
pub fn scrape_interval() -> Duration {
    Duration::from_millis(500)
}
