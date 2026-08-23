use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Reheat age buckets the scenario stratifies by. Mirrors
/// `celeriant_bench::population::AgeBucket::ALL`.
pub const AGE_BUCKET_COUNT: usize = 4;

/// One snapshot of metrics from one node, scraped at one instant.
/// Deserialize + container default: the replay bin loads stored run JSONs
/// back through the checks, including runs recorded before newer fields.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
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
    /// Per-shard wal_seq from `celeriant_wal_seq{shard_id="N"}`. Label key is "shard_id".
    #[serde(default)]
    pub wal_seq_by_shard: BTreeMap<u32, u64>,
    /// Per-shard read cursor from `celeriant_read_wal_seq{shard_id="N"}`.
    /// A read cursor above the same tick's write cursor is a regression that
    /// can self-heal before quiesce — only visible here.
    #[serde(default)]
    pub read_wal_seq_by_shard: BTreeMap<u32, u64>,
    /// Per-shard follower parked-commit queue depth from
    /// `celeriant_parked_commit_queue_depth{shard_id="N"}`. A plateau above
    /// zero across ticks is a drain leak.
    #[serde(default)]
    pub parked_commit_depth_by_shard: BTreeMap<u32, u64>,
    /// Per-shard ack barrier from `celeriant_last_self_acked_wal_seq{shard_id="N"}`:
    /// the highest wal_seq this node acked as leader. Survives demotion and
    /// restart — NeverAhead accepts a follower read covered by it.
    #[serde(default)]
    pub last_self_acked_by_shard: BTreeMap<u32, u64>,
    /// Per-shard status from `celeriant_node_status_code{shard_id="N"}`
    /// (1 = steady Follower). NeverAhead audits a shard only while its
    /// follower is steady: catchup full-commits by design, so a catching-up
    /// shard's read legitimately outruns the leader's scraped view.
    #[serde(default)]
    pub node_status_code_by_shard: BTreeMap<u32, u64>,
    /// Per-shard EFFECTIVE status from `celeriant_node_status_effective_code{shard_id="N"}`:
    /// `0=BootCatchup 1=Follower 2=FollowerCatchingUp 3=Promoting 4=Leader
    /// 5=Fenced 6=Standalone`. THE gauge to diagnose a write outage from —
    /// `node_status_code` publishes `raw()` and read Leader on every shard
    /// through a total write freeze (F-37), and so did `node_role`.
    #[serde(default)]
    pub effective_status_by_shard: BTreeMap<u32, u64>,
    /// `celeriant_lease_remaining_ms` keyed by its raw `{shard_id,role}` label
    /// blob. Per-series rather than per-shard: a node that led and then demoted
    /// carries both roles' series for one shard, and merging them hides which
    /// view went stale. Signed — an expired lease reads below zero.
    #[serde(default)]
    pub lease_remaining_ms_by_series: BTreeMap<String, i64>,
    /// `celeriant_write_errors_total` split by `error_code`. The summed
    /// `write_errors_total` cannot tell `shard_cannot_accept_writes` — the
    /// fencing signature — from an ordinary OCC conflict.
    #[serde(default)]
    pub write_errors_by_code: BTreeMap<String, u64>,
    /// Per-shard unix-ms stamp from `celeriant_shard_executor_heartbeat_ms{shard_id="N"}`,
    /// refreshed every 1s. A value that stops advancing across samples means the
    /// executor made no progress at all, not just one mesh loop.
    #[serde(default)]
    pub executor_heartbeat_ms_by_shard: BTreeMap<u32, u64>,
    pub writes_total: u64,
    pub write_errors_total: u64,
    pub leader_elections_total: u64,
    pub heartbeat_failures_total: u64,
    pub s3_fallbacks_total: u64,
    /// On-demand S3 lease renewals (sum across result=renewed|superseded). >0 proves the
    /// demand-driven renewal fired: a data shard nudged shard 0 to re-CAS lease.json.
    #[serde(default)]
    pub s3_lease_on_demand_renewal_total: u64,
    /// S3-fallback uploads refused at the full-lease gate (stale CAS confirmation). >0
    /// means a shard hit the hard refuse and spin-waited for the green light.
    #[serde(default)]
    pub s3_fallback_lease_unconfirmed_total: u64,
    pub shard_panics_total: u64,
    pub node_starts_total: u64,
    /// Sum of `celeriant_client_connections_active` across all shards.
    /// Used to distinguish "listener never saw a TCP connection" from
    /// "listener saw it and rejected it" during post-promotion debugging.
    pub client_connections_active: u64,
    /// Sum of `celeriant_watch_subscribers_active` across all shards. The watch
    /// chaos scenario watches this drain to ~0 after the flood — a leaked watch
    /// session (the CLOSE-WAIT bug) shows up as a gauge that stays elevated.
    #[serde(default)]
    pub watch_subscribers_active: u64,
    /// Items popped from `pending_replication_batches` but dropped on the
    /// floor because the rollback flag was set after the pop. Nonzero means
    /// an orphaned snapshot: a false ack in the making.
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
    /// Replication failure returned the snapshot to pending instead of rolling back
    /// the write cursor. Prevents divergent S3 chains.
    pub replication_rollback_deferred_total: u64,
    /// truncate_wal dropped wal_seqs this node acked as leader. Non-zero means
    /// real data loss.
    pub truncate_dropped_self_acked_events_total: u64,
    /// Sum of wal_seqs dropped across truncate_dropped_self_acked events.
    pub truncate_dropped_self_acked_wal_seqs_total: u64,
    /// S3 catchup skipped a batch this node uploaded. Counts how often the
    /// self-filter fires.
    pub s3_catchup_self_uploads_seen_total: u64,
    /// truncate_wal refused because divergent_wal_seq is at or below last_self_acked.
    pub truncate_refused_due_to_ack_barrier_total: u64,
    /// Catchup saw two same-(lease_epoch) S3 batches at one start with divergent
    /// content — the content-immutability invariant violated (cull-skip regressed).
    /// MUST stay 0.
    pub s3_catchup_same_epoch_divergence_total: u64,
    /// Size of the OCC/idempotency LRU at cull time.
    pub cull_stale_client_seq_lru: u64,
    pub cull_stale_agg_lru: u64,
    pub client_idempotency_violations_total: u64,
    /// Same client_seq retried while the original is fsynced-but-not-yet-replicated.
    pub client_idempotency_inflight_total: u64,
    /// Batches dropped from pending_replication on cull.
    pub take_pending_replication_dropped_batches: u64,
    /// find_divergence advanced past byte-identical prefix of the matched S3 batch.
    pub truncate_divergence_advanced_total: u64,
    pub truncate_divergence_advanced_wal_seqs_total: u64,
    /// Sealed-segment read skipped because the bloom filter said not-present.
    pub read_bloom_short_circuit_total: u64,

    // ---- Read-path cost accounting (cardinality_pressure) ----
    // `read_bloom_gate_total` counts every keyed visit that reached the bloom, so it is
    // the denominator; `read_segments_walked_total` sits three gates further down and is
    // the cost side. They are not two names for the same number and the gap between them
    // is largest on the client dedup scan, which is the path this scenario leans on.
    /// Sealed-segment read skipped because the client-id bloom said not-present.
    pub read_client_bloom_short_circuit_total: u64,
    /// Chain scans that seeked straight to a summary tip instead of reverse-hunting.
    pub read_segment_hint_seek_total: u64,
    /// Sealed segments skipped by a summary hint during the client dedup scan.
    pub read_segment_hint_skip_total: u64,
    /// Cross-shard connection redirects. A request landing on the wrong shard
    /// migrates the whole TCP stream across the glommio mesh rather than
    /// forwarding the request, so a high count against writes means the shard
    /// affinity scheme is being defeated somewhere and every latency in the
    /// report carries migration cost. Measured at 163k against 216k writes on
    /// the first cardinality_pressure run.
    pub connection_redirects_total: u64,
    pub extension_redirects_total: u64,
    /// A full mesh channel answers SERVER_BUSY with no retry.
    pub mesh_channel_full_total: u64,
    /// Keyed segment visits that consulted the aggregate bloom. THE denominator for
    /// bloom effectiveness: `read_bloom_short_circuit_total / read_bloom_gate_total`.
    pub read_bloom_gate_total: u64,
    /// Segments a keyed reverse scan actually read metablocks from, having survived
    /// the client bloom, the segment hint and the reader lock as well. This is the
    /// cost side of the ledger and is NOT the bloom denominator — `gate` is.
    pub read_segments_walked_total: u64,
    /// Keyed visits where the aggregate bloom carried no information (missing or torn
    /// sidecar), so every key read as maybe-present. These are the scans that escaped
    /// bloom filtering entirely, and were indistinguishable from a bloom hit before.
    pub read_bloom_absent_total: u64,
    pub read_bytes_total: u64,
    pub cache_log_file_hits_total: u64,
    pub cache_log_file_misses_total: u64,

    // ---- Rotation and segment inventory ----
    pub log_rotations_total: u64,
    pub log_segments_total: u64,
    /// Rotation hit ENOSPC. The shard survives but every write needing rotation fails,
    /// which otherwise reads as an unexplained throughput collapse late in a fill.
    pub rotation_out_of_space_total: u64,

    // ---- Segment summary sidecar (the unbounded-memory suspect) ----
    /// Serialized size of the most recently sealed sidecar. The 4 MiB payload cap is
    /// soft — `trim_out_client_sets` degrades client sets to Unknown but never drops
    /// entries — so a value far above 4 MiB is the cap failing where anyone can see it.
    pub segment_summary_last_bytes: u64,
    pub segment_summary_last_aggregates: u64,
    /// Per-aggregate client sets dropped to Unknown at seal. Non-zero means the cap
    /// overflowed.
    pub segment_summary_client_sets_dropped_total: u64,
    /// Largest and widest sidecar since process start. The `last_*` pair above is
    /// last-writer-wins across shards and sampled at 2 Hz, so it misses most seals
    /// and is biased against exactly the outlier this scenario is hunting.
    pub segment_summary_max_bytes: u64,
    pub segment_summary_max_aggregates: u64,

    // ---- Negative-lookup bloom (the reheat write path) ----
    pub negative_lookup_short_circuit_total: u64,
    pub negative_lookup_evictions_total: u64,
    pub negative_lookup_false_positive_total: u64,
    pub negative_lookup_builds_started_total: u64,
    pub negative_lookup_builds_completed_total: u64,
    pub negative_lookup_stale_finish_total: u64,
    pub negative_lookup_build_refused_no_budget_total: u64,
    /// Header-only fsync at replication commit, persisting last_self_acked_wal_seq.
    pub barrier_sync_fsync_total: u64,
    pub barrier_sync_fsync_failed_total: u64,
    /// Reconciliation probe detected a behind follower (WalSeqMismatch on the tip send).
    pub probe_gap_detected_total: u64,
    /// Probe-triggered catchup outcomes: Caught vs FallbackToS3. A failed count that
    /// grows while a follower stays behind is the convergence-livelock signature.
    pub probe_gap_send_success_total: u64,
    pub probe_gap_send_failed_total: u64,
    /// Catchup fetch returned no entries for a nonzero gap (treated as Caught).
    pub catchup_empty_fetch_total: u64,
    /// Catchup gave up on TCP and fell back to S3 (sum over reasons).
    pub catchup_fallback_total: u64,
    /// Catchup fetch errored (ExtendedCatchupFailure path).
    pub catchup_fetch_error_total: u64,
    /// A tombstone cache put regressed a newer snapshot — stale-tombstone signature.
    pub tombstone_snapshot_regression_total: u64,
    /// A committed batch carried a version at/below the cached one on the write path.
    pub position_snapshot_stale_commit_total: u64,
    /// Post-burst commit-notify sends (leader side). sent(leader) ==
    /// received(follower) across a run means no notify was lost.
    pub commit_notify_sent_total: u64,
    /// Guard-passing empty-batch commit-notifies accepted (follower side).
    pub commit_notify_received_total: u64,
    /// S3 catchup abandoned the barrier wait on timeout.
    #[serde(default)]
    pub s3_catchup_barrier_timeout_total: u64,
    /// S3 catchup bailed because a shard stopped making progress.
    #[serde(default)]
    pub s3_catchup_stall_bail_total: u64,
    /// S3 catchup tasks spawned. Against the completion counters this separates
    /// "never started" from "started and never finished".
    #[serde(default)]
    pub s3_catchup_task_started_total: u64,
    /// StatusUpdate broadcasts dropped because the mesh channel was full — a
    /// shard that never learns a peer's status is one of the wedge causes.
    #[serde(default)]
    pub intrashard_status_broadcast_dropped_total: u64,
    /// Mesh sends abandoned by shard 0's `broadcast_message_to_other_shards`
    /// after retry exhaustion — the other broadcast-drop path, which
    /// `intrashard_status_broadcast_dropped_total` does NOT cover. Carries the
    /// lease-renewal StatusUpdate, so a drop here can strand a shard Fenced.
    #[serde(default)]
    pub intrashard_broadcast_dropped_total: u64,
    /// Catchup completions dropped instead of delivered. Non-zero means a shard
    /// finished catching up and nobody was told.
    #[serde(default)]
    pub s3_catchup_completion_dropped_total: u64,
    /// `celeriant_intrashard_handler_started_at_ms` series that are NON-ZERO, as
    /// `{labels}@value`. Filtered because the gauge is zero while idle and every
    /// one of the 60+ series would otherwise land in every sample. Each entry
    /// names a mesh loop currently inside a handler, which one, and since when.
    #[serde(default)]
    pub stuck_handlers: Vec<String>,
    /// `celeriant_intrashard_dequeued_total` keyed by its raw `{src_shard,shard_id}`
    /// label blob. Kept per-pair, not summed: a shard runs one mesh loop per
    /// PRODUCER, so a total would let a busy sibling loop mask a dead one. The
    /// mesh is FIFO per pair, so a counter that advanced past a message's send
    /// time proves that message was consumed.
    #[serde(default)]
    pub mesh_dequeued_by_pair: BTreeMap<String, u64>,
    /// Prometheus counter names (from the COUNTERS whitelist) that actually
    /// appeared in this scrape. A4: `unwrap_or(0)` at parse time makes an
    /// absent (renamed/removed) metric indistinguishable from a present-but-
    /// zero one; `check_counter` uses this set to fail closed on the former.
    #[serde(default)]
    /// Counter names actually seen in this scrape. `Arc` because the set is
    /// identical between consecutive scrapes in the overwhelming majority of
    /// ticks, and a `deep` run retains ~74,000 samples (2 Hz x 2 hosts x 5h).
    /// Rebuilding an owned 78-entry `BTreeSet<String>` per sample costs a few
    /// hundred MB of orchestrator RSS before `snapshot()` clones the whole Vec,
    /// two or three clones of which are live at once. The scraper interns the
    /// set so unchanged ticks share one allocation.
    pub metric_keys_present: std::sync::Arc<std::collections::BTreeSet<String>>,
}

impl NodeSample {
    pub fn unreachable(host: String, t_ms: u64, error: String) -> Self {
        Self { host, t_ms, ok: false, error: Some(error), ..Default::default() }
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
    let mut wal_seq_by_shard: BTreeMap<u32, u64> = BTreeMap::new();
    let mut read_wal_seq_by_shard: BTreeMap<u32, u64> = BTreeMap::new();
    let mut parked_commit_depth_by_shard: BTreeMap<u32, u64> = BTreeMap::new();
    let mut last_self_acked_by_shard: BTreeMap<u32, u64> = BTreeMap::new();
    let mut node_status_code_by_shard: BTreeMap<u32, u64> = BTreeMap::new();
    let mut effective_status_by_shard: BTreeMap<u32, u64> = BTreeMap::new();
    let mut lease_remaining_ms_by_series: BTreeMap<String, i64> = BTreeMap::new();
    let mut write_errors_by_code: BTreeMap<String, u64> = BTreeMap::new();
    let mut executor_heartbeat_ms_by_shard: BTreeMap<u32, u64> = BTreeMap::new();
    let mut stuck_handlers: Vec<String> = Vec::new();
    let mut mesh_dequeued: BTreeMap<String, u64> = BTreeMap::new();
    let mut client_connections_active: u64 = 0;
    let mut watch_subscribers_active: u64 = 0;

    const COUNTERS: &[&str] = &[
        "celeriant_writes_total",
        "celeriant_write_errors_total",
        "celeriant_leader_elections_total",
        "celeriant_heartbeat_failures_total",
        "celeriant_replication_s3_fallbacks_total",
        "celeriant_s3_lease_on_demand_renewal_total",
        "celeriant_s3_fallback_lease_unconfirmed_total",
        "celeriant_shard_panics_total",
        "celeriant_node_starts_total",
        "celeriant_replication_capture_dropped_items_total",
        "celeriant_replication_capture_dropped_bytes_total",
        "celeriant_writes_accepted_no_prior_client_seq_total",
        "celeriant_cache_aggregate_client_scan_not_found_total",
        "celeriant_cache_aggregate_client_scan_found_total",
        "celeriant_truncate_dropped_committed_metablocks_events_total",
        "celeriant_truncate_dropped_committed_bytes_total",
        "celeriant_replication_rollback_deferred_total",
        "celeriant_truncate_dropped_self_acked_events_total",
        "celeriant_truncate_dropped_self_acked_wal_seqs_total",
        "celeriant_s3_catchup_self_uploads_seen_total",
        "celeriant_truncate_refused_due_to_ack_barrier_total",
        "celeriant_s3_catchup_same_epoch_divergence_total",
        "celeriant_cull_stale_client_seq_lru",
        "celeriant_cull_stale_agg_lru",
        "celeriant_client_idempotency_violations_total",
        "celeriant_client_idempotency_inflight_total",
        "celeriant_take_pending_replication_dropped_batches",
        "celeriant_truncate_divergence_advanced_total",
        "celeriant_truncate_divergence_advanced_wal_seqs_total",
        "celeriant_read_bloom_short_circuit_total",
        "celeriant_read_client_bloom_short_circuit_total",
        "celeriant_read_segment_hint_seek_total",
        "celeriant_read_segment_hint_skip_total",
        "celeriant_connection_redirects_total",
        "celeriant_extension_redirects_total",
        "celeriant_mesh_channel_full_total",
        "celeriant_read_bloom_gate_total",
        "celeriant_read_segments_walked_total",
        "celeriant_read_bloom_absent_total",
        "celeriant_read_bytes_total",
        "celeriant_cache_log_file_hits_total",
        "celeriant_cache_log_file_misses_total",
        "celeriant_log_rotations_total",
        "celeriant_log_segments_total",
        "celeriant_rotation_out_of_space_total",
        "celeriant_segment_summary_last_bytes",
        "celeriant_segment_summary_last_aggregates",
        "celeriant_segment_summary_client_sets_dropped_total",
        "celeriant_segment_summary_max_bytes",
        "celeriant_segment_summary_max_aggregates",
        "celeriant_negative_lookup_short_circuit_total",
        "celeriant_negative_lookup_evictions_total",
        "celeriant_negative_lookup_false_positive_total",
        "celeriant_negative_lookup_builds_started_total",
        "celeriant_negative_lookup_builds_completed_total",
        "celeriant_negative_lookup_stale_finish_total",
        "celeriant_negative_lookup_build_refused_no_budget_total",
        "celeriant_barrier_sync_fsync_total",
        "celeriant_barrier_sync_fsync_failed_total",
        "celeriant_probe_outcome_gap_detected_total",
        "celeriant_probe_gap_send_success_total",
        "celeriant_probe_gap_send_failed_total",
        "celeriant_catchup_empty_fetch_total",
        "celeriant_catchup_fallback_total",
        "celeriant_catchup_fetch_error_total",
        "celeriant_tombstone_snapshot_regression_total",
        "celeriant_position_snapshot_stale_commit_total",
        "celeriant_commit_notify_sent_total",
        "celeriant_commit_notify_received_total",
        "celeriant_s3_catchup_barrier_timeout_total",
        "celeriant_s3_catchup_stall_bail_total",
        "celeriant_s3_catchup_task_started_total",
        "celeriant_intrashard_status_broadcast_dropped_total",
        "celeriant_intrashard_broadcast_dropped_total",
        "celeriant_s3_catchup_completion_dropped_total",
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
                if let Some(shard_id) = extract_label(name_part, "shard_id")
                    && let Ok(id) = shard_id.parse::<u32>()
                {
                    wal_seq_by_shard.insert(id, v);
                }
            }
            continue;
        }
        if name == "celeriant_read_wal_seq" {
            if let Ok(v) = value_str.parse::<f64>()
                && let Some(shard_id) = extract_label(name_part, "shard_id")
                && let Ok(id) = shard_id.parse::<u32>()
            {
                read_wal_seq_by_shard.insert(id, v as u64);
            }
            continue;
        }
        if name == "celeriant_parked_commit_queue_depth" {
            if let Ok(v) = value_str.parse::<f64>()
                && let Some(shard_id) = extract_label(name_part, "shard_id")
                && let Ok(id) = shard_id.parse::<u32>()
            {
                parked_commit_depth_by_shard.insert(id, v as u64);
            }
            continue;
        }
        if name == "celeriant_last_self_acked_wal_seq" {
            if let Ok(v) = value_str.parse::<f64>()
                && let Some(shard_id) = extract_label(name_part, "shard_id")
                && let Ok(id) = shard_id.parse::<u32>()
            {
                last_self_acked_by_shard.insert(id, v as u64);
            }
            continue;
        }
        if name == "celeriant_node_status_code" {
            if let Ok(v) = value_str.parse::<f64>()
                && let Some(shard_id) = extract_label(name_part, "shard_id")
                && let Ok(id) = shard_id.parse::<u32>()
            {
                node_status_code_by_shard.insert(id, v as u64);
            }
            continue;
        }
        if name == "celeriant_node_status_effective_code" {
            if let Ok(v) = value_str.parse::<f64>()
                && let Some(shard_id) = extract_label(name_part, "shard_id")
                && let Ok(id) = shard_id.parse::<u32>()
            {
                effective_status_by_shard.insert(id, v as u64);
            }
            continue;
        }
        if name == "celeriant_lease_remaining_ms" {
            if let Ok(v) = value_str.parse::<f64>()
                && let Some(labels) = name_part.find('{').map(|i| &name_part[i..])
            {
                lease_remaining_ms_by_series.insert(labels.to_string(), v as i64);
            }
            continue;
        }
        // No `continue`: the whitelist below still has to sum the same series
        // into `write_errors_total`. This branch only adds the per-code split.
        if name == "celeriant_write_errors_total"
            && let Some(code) = extract_label(name_part, "error_code")
            && let Ok(v) = value_str.parse::<f64>()
        {
            *write_errors_by_code.entry(code.to_string()).or_insert(0) += v as u64;
        }
        if name == "celeriant_intrashard_dequeued_total" {
            if let Ok(v) = value_str.parse::<f64>()
                && let Some(labels) = name_part.find('{').map(|i| &name_part[i..])
            {
                mesh_dequeued.insert(labels.to_string(), v as u64);
            }
            continue;
        }
        if name == "celeriant_shard_executor_heartbeat_ms" {
            if let Ok(v) = value_str.parse::<f64>()
                && let Some(shard_id) = extract_label(name_part, "shard_id")
                && let Ok(id) = shard_id.parse::<u32>()
            {
                executor_heartbeat_ms_by_shard.insert(id, v as u64);
            }
            continue;
        }
        if name == "celeriant_intrashard_handler_started_at_ms" {
            if let Ok(v) = value_str.parse::<f64>()
                && v != 0.0
            {
                let labels = &name_part[name.len()..];
                stuck_handlers.push(format!("{labels}@{}", v as u64));
            }
            continue;
        }
        if name == "celeriant_client_connections_active" {
            if let Ok(v) = value_str.parse::<f64>() {
                client_connections_active = client_connections_active.saturating_add(v as u64);
            }
            continue;
        }
        if name == "celeriant_watch_subscribers_active" {
            if let Ok(v) = value_str.parse::<f64>() {
                watch_subscribers_active = watch_subscribers_active.saturating_add(v as u64);
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
    let metric_keys_present: std::sync::Arc<std::collections::BTreeSet<String>> =
        std::sync::Arc::new(sums.keys().map(|k| k.to_string()).collect());

    NodeSample {
        host,
        t_ms,
        ok: true,
        error: None,
        node_role,
        wal_seq_max,
        wal_seq_by_shard,
        read_wal_seq_by_shard,
        parked_commit_depth_by_shard,
        last_self_acked_by_shard,
        node_status_code_by_shard,
        effective_status_by_shard,
        lease_remaining_ms_by_series,
        write_errors_by_code,
        executor_heartbeat_ms_by_shard,
        stuck_handlers,
        mesh_dequeued_by_pair: mesh_dequeued,
        writes_total: get("celeriant_writes_total"),
        write_errors_total: get("celeriant_write_errors_total"),
        leader_elections_total: get("celeriant_leader_elections_total"),
        heartbeat_failures_total: get("celeriant_heartbeat_failures_total"),
        s3_fallbacks_total: get("celeriant_replication_s3_fallbacks_total"),
        s3_lease_on_demand_renewal_total: get("celeriant_s3_lease_on_demand_renewal_total"),
        s3_fallback_lease_unconfirmed_total: get("celeriant_s3_fallback_lease_unconfirmed_total"),
        shard_panics_total: get("celeriant_shard_panics_total"),
        node_starts_total: get("celeriant_node_starts_total"),
        client_connections_active,
        watch_subscribers_active,
        capture_dropped_items_total: get("celeriant_replication_capture_dropped_items_total"),
        capture_dropped_bytes_total: get("celeriant_replication_capture_dropped_bytes_total"),
        writes_accepted_no_prior_client_seq_total: get("celeriant_writes_accepted_no_prior_client_seq_total"),
        cache_client_scan_not_found_total: get("celeriant_cache_aggregate_client_scan_not_found_total"),
        cache_client_scan_found_total: get("celeriant_cache_aggregate_client_scan_found_total"),
        truncate_dropped_committed_events_total: get("celeriant_truncate_dropped_committed_metablocks_events_total"),
        truncate_dropped_committed_bytes_total: get("celeriant_truncate_dropped_committed_bytes_total"),
        replication_rollback_deferred_total: get("celeriant_replication_rollback_deferred_total"),
        truncate_dropped_self_acked_events_total: get("celeriant_truncate_dropped_self_acked_events_total"),
        truncate_dropped_self_acked_wal_seqs_total: get("celeriant_truncate_dropped_self_acked_wal_seqs_total"),
        s3_catchup_self_uploads_seen_total: get("celeriant_s3_catchup_self_uploads_seen_total"),
        truncate_refused_due_to_ack_barrier_total: get("celeriant_truncate_refused_due_to_ack_barrier_total"),
        s3_catchup_same_epoch_divergence_total: get("celeriant_s3_catchup_same_epoch_divergence_total"),
        cull_stale_client_seq_lru: get("celeriant_cull_stale_client_seq_lru"),
        cull_stale_agg_lru: get("celeriant_cull_stale_agg_lru"),
        client_idempotency_violations_total: get("celeriant_client_idempotency_violations_total"),
        client_idempotency_inflight_total: get("celeriant_client_idempotency_inflight_total"),
        take_pending_replication_dropped_batches: get("celeriant_take_pending_replication_dropped_batches"),
        truncate_divergence_advanced_total: get("celeriant_truncate_divergence_advanced_total"),
        truncate_divergence_advanced_wal_seqs_total: get("celeriant_truncate_divergence_advanced_wal_seqs_total"),
        read_bloom_short_circuit_total: get("celeriant_read_bloom_short_circuit_total"),
        read_client_bloom_short_circuit_total: get("celeriant_read_client_bloom_short_circuit_total"),
        read_segment_hint_seek_total: get("celeriant_read_segment_hint_seek_total"),
        read_segment_hint_skip_total: get("celeriant_read_segment_hint_skip_total"),
        connection_redirects_total: get("celeriant_connection_redirects_total"),
        extension_redirects_total: get("celeriant_extension_redirects_total"),
        mesh_channel_full_total: get("celeriant_mesh_channel_full_total"),
        read_bloom_gate_total: get("celeriant_read_bloom_gate_total"),
        read_segments_walked_total: get("celeriant_read_segments_walked_total"),
        read_bloom_absent_total: get("celeriant_read_bloom_absent_total"),
        read_bytes_total: get("celeriant_read_bytes_total"),
        cache_log_file_hits_total: get("celeriant_cache_log_file_hits_total"),
        cache_log_file_misses_total: get("celeriant_cache_log_file_misses_total"),
        log_rotations_total: get("celeriant_log_rotations_total"),
        log_segments_total: get("celeriant_log_segments_total"),
        rotation_out_of_space_total: get("celeriant_rotation_out_of_space_total"),
        segment_summary_last_bytes: get("celeriant_segment_summary_last_bytes"),
        segment_summary_last_aggregates: get("celeriant_segment_summary_last_aggregates"),
        segment_summary_client_sets_dropped_total: get("celeriant_segment_summary_client_sets_dropped_total"),
        segment_summary_max_bytes: get("celeriant_segment_summary_max_bytes"),
        segment_summary_max_aggregates: get("celeriant_segment_summary_max_aggregates"),
        negative_lookup_short_circuit_total: get("celeriant_negative_lookup_short_circuit_total"),
        negative_lookup_evictions_total: get("celeriant_negative_lookup_evictions_total"),
        negative_lookup_false_positive_total: get("celeriant_negative_lookup_false_positive_total"),
        negative_lookup_builds_started_total: get("celeriant_negative_lookup_builds_started_total"),
        negative_lookup_builds_completed_total: get("celeriant_negative_lookup_builds_completed_total"),
        negative_lookup_stale_finish_total: get("celeriant_negative_lookup_stale_finish_total"),
        negative_lookup_build_refused_no_budget_total: get("celeriant_negative_lookup_build_refused_no_budget_total"),
        probe_gap_detected_total: get("celeriant_probe_outcome_gap_detected_total"),
        probe_gap_send_success_total: get("celeriant_probe_gap_send_success_total"),
        probe_gap_send_failed_total: get("celeriant_probe_gap_send_failed_total"),
        catchup_empty_fetch_total: get("celeriant_catchup_empty_fetch_total"),
        catchup_fallback_total: get("celeriant_catchup_fallback_total"),
        catchup_fetch_error_total: get("celeriant_catchup_fetch_error_total"),
        tombstone_snapshot_regression_total: get("celeriant_tombstone_snapshot_regression_total"),
        position_snapshot_stale_commit_total: get("celeriant_position_snapshot_stale_commit_total"),
        commit_notify_sent_total: get("celeriant_commit_notify_sent_total"),
        commit_notify_received_total: get("celeriant_commit_notify_received_total"),
        s3_catchup_barrier_timeout_total: get("celeriant_s3_catchup_barrier_timeout_total"),
        s3_catchup_stall_bail_total: get("celeriant_s3_catchup_stall_bail_total"),
        s3_catchup_task_started_total: get("celeriant_s3_catchup_task_started_total"),
        intrashard_status_broadcast_dropped_total: get("celeriant_intrashard_status_broadcast_dropped_total"),
        intrashard_broadcast_dropped_total: get("celeriant_intrashard_broadcast_dropped_total"),
        s3_catchup_completion_dropped_total: get("celeriant_s3_catchup_completion_dropped_total"),
        barrier_sync_fsync_total: get("celeriant_barrier_sync_fsync_total"),
        barrier_sync_fsync_failed_total: get("celeriant_barrier_sync_fsync_failed_total"),
        metric_keys_present,
    }
}

/// Extract the value of `key` from a Prometheus label set, e.g.
/// `metric{shard_id="3",other="x"}` → `extract_label(..., "shard_id")` → `Some("3")`.
fn extract_label<'a>(name_part: &'a str, key: &str) -> Option<&'a str> {
    let start = name_part.find('{')?;
    let end = name_part.rfind('}')?;
    let labels = &name_part[start + 1..end];
    for kv in labels.split(',') {
        let kv = kv.trim();
        if let Some(rest) = kv.strip_prefix(key) {
            if let Some(rest) = rest.strip_prefix("=\"") {
                if let Some(val) = rest.strip_suffix('"') {
                    return Some(val);
                }
            }
        }
    }
    None
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(body: &str) -> NodeSample {
        parse_metrics("h1".into(), 0, body)
    }

    #[test]
    fn stuck_handlers_keeps_only_nonzero() {
        let s = sample(
            "celeriant_intrashard_handler_started_at_ms{src_shard=\"0\",shard_id=\"2\",kind=\"enter_s3_catchup\"} 1785377615123\n\
             celeriant_intrashard_handler_started_at_ms{src_shard=\"1\",shard_id=\"2\",kind=\"status_update\"} 0\n",
        );
        assert_eq!(
            s.stuck_handlers,
            vec!["{src_shard=\"0\",shard_id=\"2\",kind=\"enter_s3_catchup\"}@1785377615123"]
        );
    }

    #[test]
    fn stuck_handlers_empty_when_all_idle() {
        let s = sample("celeriant_intrashard_handler_started_at_ms{src_shard=\"0\",shard_id=\"1\",kind=\"probe\"} 0\n");
        assert!(s.stuck_handlers.is_empty());
    }

    #[test]
    fn executor_heartbeat_parsed_per_shard() {
        let s = sample(
            "celeriant_shard_executor_heartbeat_ms{shard_id=\"0\"} 1785377615000\n\
             celeriant_shard_executor_heartbeat_ms{shard_id=\"3\"} 1785377614000\n",
        );
        assert_eq!(s.executor_heartbeat_ms_by_shard.get(&0), Some(&1785377615000));
        assert_eq!(s.executor_heartbeat_ms_by_shard.get(&3), Some(&1785377614000));
    }

    #[test]
    fn new_catchup_counters_summed_and_marked_present() {
        let s = sample(
            "celeriant_s3_catchup_barrier_timeout_total 2\n\
             celeriant_s3_catchup_stall_bail_total 1\n\
             celeriant_s3_catchup_task_started_total{shard_id=\"0\"} 4\n\
             celeriant_s3_catchup_task_started_total{shard_id=\"1\"} 5\n",
        );
        assert_eq!(s.s3_catchup_barrier_timeout_total, 2);
        assert_eq!(s.s3_catchup_stall_bail_total, 1);
        assert_eq!(s.s3_catchup_task_started_total, 9);
        assert_eq!(s.intrashard_status_broadcast_dropped_total, 0);
        assert!(!s.metric_keys_present.contains("celeriant_s3_catchup_completion_dropped_total"));
    }

    #[test]
    fn effective_status_is_parsed_separately_from_the_raw_gauge() {
        // The trap the whole wedge diagnosis turns on: raw reads Leader on
        // every shard while effective reads Fenced and writes are rejected.
        let s = sample(
            "celeriant_node_status_code{shard_id=\"1\"} 4\n\
             celeriant_node_status_code{shard_id=\"2\"} 4\n\
             celeriant_node_status_effective_code{shard_id=\"1\"} 5\n\
             celeriant_node_status_effective_code{shard_id=\"2\"} 5\n\
             celeriant_node_status_effective_code{shard_id=\"0\"} 4\n",
        );
        assert_eq!(s.node_status_code_by_shard.get(&1), Some(&4));
        assert_eq!(s.effective_status_by_shard.get(&1), Some(&5));
        assert_eq!(s.effective_status_by_shard.get(&2), Some(&5));
        assert_eq!(s.effective_status_by_shard.get(&0), Some(&4));
    }

    #[test]
    fn lease_remaining_keeps_both_role_series_and_accepts_a_negative() {
        let s = sample(
            "celeriant_lease_remaining_ms{shard_id=\"1\",role=\"leader\"} 4200\n\
             celeriant_lease_remaining_ms{shard_id=\"1\",role=\"follower\"} -1500\n",
        );
        assert_eq!(s.lease_remaining_ms_by_series.len(), 2, "{:?}", s.lease_remaining_ms_by_series);
        assert_eq!(
            s.lease_remaining_ms_by_series.get("{shard_id=\"1\",role=\"follower\"}"),
            Some(&-1500)
        );
    }

    #[test]
    fn write_errors_split_by_code_without_losing_the_total() {
        let s = sample(
            "celeriant_write_errors_total{shard_id=\"1\",error_code=\"shard_cannot_accept_writes\"} 434679\n\
             celeriant_write_errors_total{shard_id=\"2\",error_code=\"shard_cannot_accept_writes\"} 15352\n\
             celeriant_write_errors_total{shard_id=\"1\",error_code=\"occ_conflict\"} 12\n",
        );
        assert_eq!(s.write_errors_by_code.get("shard_cannot_accept_writes"), Some(&450_031));
        assert_eq!(s.write_errors_by_code.get("occ_conflict"), Some(&12));
        assert_eq!(s.write_errors_total, 450_043);
        assert!(s.metric_keys_present.contains("celeriant_write_errors_total"));
    }

    type Field = fn(&NodeSample) -> u64;

    /// Read/segment/negative-lookup metrics the comparator needs, paired with the
    /// field each must land in. Every table-driven test below walks this list.
    const READ_PATH_COUNTERS: &[(&str, Field)] = &[
        ("celeriant_read_client_bloom_short_circuit_total", |s| s.read_client_bloom_short_circuit_total),
        ("celeriant_read_segment_hint_seek_total", |s| s.read_segment_hint_seek_total),
        ("celeriant_read_segment_hint_skip_total", |s| s.read_segment_hint_skip_total),
        ("celeriant_cache_log_file_hits_total", |s| s.cache_log_file_hits_total),
        ("celeriant_cache_log_file_misses_total", |s| s.cache_log_file_misses_total),
        ("celeriant_read_bytes_total", |s| s.read_bytes_total),
        ("celeriant_log_rotations_total", |s| s.log_rotations_total),
        ("celeriant_log_segments_total", |s| s.log_segments_total),
        ("celeriant_rotation_out_of_space_total", |s| s.rotation_out_of_space_total),
        ("celeriant_segment_summary_last_bytes", |s| s.segment_summary_last_bytes),
        ("celeriant_segment_summary_last_aggregates", |s| s.segment_summary_last_aggregates),
        ("celeriant_segment_summary_client_sets_dropped_total", |s| s.segment_summary_client_sets_dropped_total),
        ("celeriant_read_segments_walked_total", |s| s.read_segments_walked_total),
        ("celeriant_read_bloom_absent_total", |s| s.read_bloom_absent_total),
        ("celeriant_negative_lookup_short_circuit_total", |s| s.negative_lookup_short_circuit_total),
        ("celeriant_negative_lookup_evictions_total", |s| s.negative_lookup_evictions_total),
        ("celeriant_negative_lookup_false_positive_total", |s| s.negative_lookup_false_positive_total),
        ("celeriant_negative_lookup_builds_started_total", |s| s.negative_lookup_builds_started_total),
        ("celeriant_negative_lookup_builds_completed_total", |s| s.negative_lookup_builds_completed_total),
        ("celeriant_negative_lookup_stale_finish_total", |s| s.negative_lookup_stale_finish_total),
        ("celeriant_negative_lookup_build_refused_no_budget_total", |s| s.negative_lookup_build_refused_no_budget_total),
        ("celeriant_client_idempotency_violations_total", |s| s.client_idempotency_violations_total),
    ];

    #[test]
    fn read_path_counters_sum_across_label_sets() {
        let mut body = String::new();
        for (i, (name, _)) in READ_PATH_COUNTERS.iter().enumerate() {
            let v = i as u64 + 1;
            body.push_str(&format!(
                "{name}{{shard_id=\"1\"}} {v}\n{name}{{shard_id=\"2\"}} {}\n",
                v * 10
            ));
        }
        let s = sample(&body);
        for (i, (name, get)) in READ_PATH_COUNTERS.iter().enumerate() {
            assert_eq!(get(&s), (i as u64 + 1) * 11, "{name}");
            if name.ends_with("_total") {
                assert!(s.metric_keys_present.contains(*name), "{name} not marked present");
            }
        }
    }

    #[test]
    fn absent_read_path_counters_are_zero() {
        let s = sample("celeriant_wal_seq{shard_id=\"0\"} 7\n");
        for (name, get) in READ_PATH_COUNTERS {
            assert_eq!(get(&s), 0, "{name}");
            assert!(!s.metric_keys_present.contains(*name), "{name} marked present while absent");
        }
    }

    #[test]
    fn read_path_counters_parse_float_exposition() {
        for text in ["1024", "1024.0"] {
            let body: String = READ_PATH_COUNTERS
                .iter()
                .map(|(name, _)| format!("{name} {text}\n"))
                .collect();
            let s = sample(&body);
            for (name, get) in READ_PATH_COUNTERS {
                assert_eq!(get(&s), 1024, "{name} written as {text}");
            }
        }
    }

    #[test]
    fn comments_and_histogram_suffixes_do_not_corrupt_read_path_counters() {
        let mut body = String::from(
            "# HELP celeriant_read_duration_seconds Read latency\n\
             # TYPE celeriant_read_duration_seconds histogram\n\
             celeriant_read_duration_seconds_bucket{le=\"0.1\"} 5\n\
             celeriant_read_duration_seconds_bucket{le=\"+Inf\"} 9\n\
             celeriant_read_duration_seconds_sum 0.42\n\
             celeriant_read_duration_seconds_count 9\n",
        );
        for (name, _) in READ_PATH_COUNTERS {
            body.push_str(&format!(
                "# TYPE {name} counter\n{name} 3\n\
                 {name}_bucket{{le=\"0.1\"}} 999\n{name}_sum 999\n{name}_count 999\n"
            ));
        }
        let s = sample(&body);
        for (name, get) in READ_PATH_COUNTERS {
            assert_eq!(get(&s), 3, "{name}");
        }
    }

    #[test]
    fn legacy_json_without_read_path_counters_deserializes() {
        let s: NodeSample =
            serde_json::from_str(r#"{"host":"h1","t_ms":500,"ok":true,"wal_seq_max":42}"#).unwrap();
        assert_eq!(s.wal_seq_max, 42);
        for (name, get) in READ_PATH_COUNTERS {
            assert_eq!(get(&s), 0, "{name}");
        }
    }

    #[test]
    fn read_path_counters_survive_serde_round_trip() {
        let body: String = READ_PATH_COUNTERS
            .iter()
            .enumerate()
            .map(|(i, (name, _))| format!("{name} {}\n", i + 1))
            .collect();
        let s = sample(&body);
        let back: NodeSample = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        for (i, (name, get)) in READ_PATH_COUNTERS.iter().enumerate() {
            assert_eq!(get(&back), i as u64 + 1, "{name}");
        }
        assert_eq!(back.metric_keys_present, s.metric_keys_present);
    }
}
