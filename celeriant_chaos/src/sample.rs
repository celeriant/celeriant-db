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
