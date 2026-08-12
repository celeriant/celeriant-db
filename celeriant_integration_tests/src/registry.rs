use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Category {
    Core,
    Replication,
    Election,
    Fencing,
    Invariant,
    Metamorphic,
    Edge,
    Correctness,
    Durability,
    Performance,
    Operations,
    Security,
    Schema,
    Compaction,
    Debug,
    /// Requires kernel TLS (CONFIG_TLS). Excluded on Docker Desktop /
    /// LinuxKit kernels where kTLS isn't compiled in.
    RequiresKtls,
    /// Requires a remote RPi hardware cluster + provisioned PKI on disk.
    /// Opt-in only via `--include-or rpi`.
    Rpi,
}

impl Category {
    pub const ALL: &[Category] = &[
        Category::Core,
        Category::Replication,
        Category::Election,
        Category::Fencing,
        Category::Invariant,
        Category::Metamorphic,
        Category::Edge,
        Category::Correctness,
        Category::Durability,
        Category::Performance,
        Category::Operations,
        Category::Security,
        Category::Schema,
        Category::Compaction,
        Category::Debug,
        Category::RequiresKtls,
        Category::Rpi,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Category::Core => "core",
            Category::Replication => "replication",
            Category::Election => "election",
            Category::Fencing => "fencing",
            Category::Invariant => "invariant",
            Category::Metamorphic => "metamorphic",
            Category::Edge => "edge",
            Category::Correctness => "correctness",
            Category::Durability => "durability",
            Category::Performance => "performance",
            Category::Operations => "operations",
            Category::Security => "security",
            Category::Schema => "schema",
            Category::Compaction => "compaction",
            Category::Debug => "debug",
            Category::RequiresKtls => "requires_ktls",
            Category::Rpi => "rpi",
        }
    }

    pub fn from_str(s: &str) -> Option<Category> {
        match s {
            "core" => Some(Category::Core),
            "replication" => Some(Category::Replication),
            "election" => Some(Category::Election),
            "fencing" => Some(Category::Fencing),
            "invariant" => Some(Category::Invariant),
            "metamorphic" => Some(Category::Metamorphic),
            "edge" => Some(Category::Edge),
            "correctness" => Some(Category::Correctness),
            "durability" => Some(Category::Durability),
            "performance" => Some(Category::Performance),
            "operations" => Some(Category::Operations),
            "security" => Some(Category::Security),
            "schema" => Some(Category::Schema),
            "compaction" => Some(Category::Compaction),
            "debug" => Some(Category::Debug),
            "requires_ktls" => Some(Category::RequiresKtls),
            "rpi" => Some(Category::Rpi),
            _ => None,
        }
    }
}

impl fmt::Display for Category {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub struct TestEntry {
    pub name: &'static str,
    pub description: &'static str,
    pub estimated_secs: u32,
    pub categories: &'static [Category],
    pub distributed: bool,
}

pub fn all_tests() -> &'static [TestEntry] {
    use Category::*;
    &[
        TestEntry { name: "p1_append_reads_back_unchanged", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_reads_are_ordered_and_gap_free", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_version_tracks_batch_count", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_read_missing_aggregate_errors", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_empty_write_rejected", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_zero_event_type_rejected", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_write_no_create_rejected", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_pagination_streams_whole_aggregate", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_offset_filter_bounds_range", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_event_type_filter", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_client_id_filter", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_writes_survive_restart", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_exclude_client_id_filter", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_multi_event_batch_preserves_order", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p1_event_time_range_filter", description: "Contract suite (phase1)", estimated_secs: 10, categories: &[Core], distributed: false },
        TestEntry { name: "p2_occ_match_commits_and_advances", description: "Contract suite (phase2)", estimated_secs: 10, categories: &[Correctness], distributed: false },
        TestEntry { name: "p2_occ_stale_rejected_no_append", description: "Contract suite (phase2)", estimated_secs: 10, categories: &[Correctness], distributed: false },
        TestEntry { name: "p2_occ_create_guard_races_cleanly", description: "Contract suite (phase2)", estimated_secs: 10, categories: &[Correctness], distributed: false },
        TestEntry { name: "p2_idempotency_dedupes_replay", description: "Contract suite (phase2)", estimated_secs: 10, categories: &[Correctness], distributed: false },
        TestEntry { name: "p2_idempotency_scoped_per_client", description: "Contract suite (phase2)", estimated_secs: 10, categories: &[Correctness], distributed: false },
        TestEntry { name: "p2_multi_aggregate_atomic_rollback", description: "Contract suite (phase2)", estimated_secs: 10, categories: &[Correctness], distributed: false },
        TestEntry { name: "p2_multi_aggregate_cross_shard_rejected", description: "Contract suite (phase2)", estimated_secs: 10, categories: &[Correctness], distributed: false },
        TestEntry { name: "p2_occ_and_idempotency_compose", description: "Contract suite (phase2)", estimated_secs: 10, categories: &[Correctness], distributed: false },
        TestEntry { name: "p2_unconditional_write_appends", description: "Contract suite (phase2)", estimated_secs: 10, categories: &[Correctness], distributed: false },
        TestEntry { name: "p3_trim_drops_prefix", description: "Contract suite (phase3)", estimated_secs: 15, categories: &[Correctness], distributed: false },
        TestEntry { name: "p3_trim_preserves_remaining", description: "Contract suite (phase3)", estimated_secs: 15, categories: &[Correctness], distributed: false },
        TestEntry { name: "p3_trim_out_of_range_rejected", description: "Contract suite (phase3)", estimated_secs: 15, categories: &[Correctness], distributed: false },
        TestEntry { name: "p3_trim_missing_rejected", description: "Contract suite (phase3)", estimated_secs: 15, categories: &[Correctness], distributed: false },
        TestEntry { name: "p3_delete_removes_aggregate", description: "Contract suite (phase3)", estimated_secs: 15, categories: &[Correctness], distributed: false },
        TestEntry { name: "p3_delete_no_recreate_blocks_rewrite", description: "Contract suite (phase3)", estimated_secs: 15, categories: &[Correctness], distributed: false },
        TestEntry { name: "p3_delete_recreate_allows_rewrite", description: "Contract suite (phase3)", estimated_secs: 15, categories: &[Correctness], distributed: false },
        TestEntry { name: "p3_delete_conditional_guard", description: "Contract suite (phase3)", estimated_secs: 15, categories: &[Correctness], distributed: false },
        TestEntry { name: "p3_delete_missing_rejected", description: "Contract suite (phase3)", estimated_secs: 15, categories: &[Correctness], distributed: false },
        TestEntry { name: "p3_delete_sequence_continuation", description: "Contract suite (phase3)", estimated_secs: 15, categories: &[Correctness], distributed: false },
        TestEntry { name: "p3_delete_cross_shard_rejected", description: "Contract suite (phase3)", estimated_secs: 15, categories: &[Correctness], distributed: false },
        TestEntry { name: "p4_schema_rejects_bad_event", description: "Contract suite (phase4)", estimated_secs: 10, categories: &[Schema], distributed: false },
        TestEntry { name: "p4_schema_accepts_good_event", description: "Contract suite (phase4)", estimated_secs: 10, categories: &[Schema], distributed: false },
        TestEntry { name: "p4_schema_duplicate_register_rejected", description: "Contract suite (phase4)", estimated_secs: 10, categories: &[Schema], distributed: false },
        TestEntry { name: "p4_schema_invalid_register_rejected", description: "Contract suite (phase4)", estimated_secs: 10, categories: &[Schema], distributed: false },
        TestEntry { name: "p4_schema_unregistered_version_unvalidated", description: "Contract suite (phase4)", estimated_secs: 10, categories: &[Schema], distributed: false },
        TestEntry { name: "p5_watch_notifies_on_write", description: "Contract suite (phase5)", estimated_secs: 15, categories: &[Core], distributed: false },
        TestEntry { name: "p5_watch_incompatible_filter_rejected", description: "Contract suite (phase5)", estimated_secs: 15, categories: &[Core], distributed: false },
        TestEntry { name: "p5_watch_latency_too_high_rejected", description: "Contract suite (phase5)", estimated_secs: 15, categories: &[Core], distributed: false },
        TestEntry { name: "p5_watch_cursor_misses_no_events", description: "Contract suite (phase5)", estimated_secs: 15, categories: &[Core], distributed: false },
        TestEntry { name: "p5_watch_operations_are_distinct", description: "Contract suite (phase5)", estimated_secs: 15, categories: &[Core], distributed: false },
        TestEntry { name: "p5_watch_create_notification_carries_version_range", description: "Contract suite (phase5)", estimated_secs: 15, categories: &[Core], distributed: false },
        TestEntry { name: "p6_cluster_elects_single_leader", description: "Contract suite (phase6)", estimated_secs: 60, categories: &[Replication, Election], distributed: true },
        TestEntry { name: "p6_cluster_replicates_and_rejects_follower_write", description: "Contract suite (phase6)", estimated_secs: 60, categories: &[Replication, Election], distributed: true },
        TestEntry { name: "p6_follower_read_converges", description: "Contract suite (phase6)", estimated_secs: 60, categories: &[Replication, Election], distributed: true },
        TestEntry { name: "p6_failover_promotes_follower", description: "Contract suite (phase6)", estimated_secs: 60, categories: &[Replication, Election], distributed: true },
        TestEntry { name: "p7_concurrent_occ_writers_one_wins", description: "Contract suite (phase7)", estimated_secs: 25, categories: &[Correctness, Invariant], distributed: false },
        TestEntry { name: "p7_concurrent_creators_one_wins", description: "Contract suite (phase7)", estimated_secs: 25, categories: &[Correctness, Invariant], distributed: false },
        TestEntry { name: "p7_concurrent_idempotent_retries_dedupe", description: "Contract suite (phase7)", estimated_secs: 25, categories: &[Correctness, Invariant], distributed: false },
        TestEntry { name: "p7_per_aggregate_order_across_shards", description: "Contract suite (phase7)", estimated_secs: 25, categories: &[Correctness, Invariant], distributed: false },
        TestEntry { name: "p7_large_values_long_stream_intact", description: "Contract suite (phase7)", estimated_secs: 25, categories: &[Correctness, Invariant], distributed: false },
        TestEntry { name: "p8_replication_link_cut_acks_via_s3_and_heals", description: "Contract suite (phase8)", estimated_secs: 90, categories: &[Replication, Edge], distributed: true },
        TestEntry { name: "p8_s3_outage_leader_keeps_serving", description: "Contract suite (phase8)", estimated_secs: 90, categories: &[Replication, Edge], distributed: true },
        TestEntry { name: "p8_crash_follower_restart_data_survives", description: "Contract suite (phase8)", estimated_secs: 90, categories: &[Replication, Edge], distributed: true },
        TestEntry { name: "p8_exactly_once_across_failover", description: "Contract suite (phase8)", estimated_secs: 90, categories: &[Replication, Edge], distributed: true },
        TestEntry { name: "p8_leader_follower_read_parity", description: "Contract suite (phase8)", estimated_secs: 90, categories: &[Replication, Edge], distributed: true },
        TestEntry { name: "p9_mtls_require_good_client_connects", description: "Contract suite (phase9)", estimated_secs: 25, categories: &[Security], distributed: false },
        TestEntry { name: "p9_mtls_require_untrusted_ca_refused", description: "Contract suite (phase9)", estimated_secs: 25, categories: &[Security], distributed: false },
        TestEntry { name: "p9_mtls_require_no_cert_refused", description: "Contract suite (phase9)", estimated_secs: 25, categories: &[Security], distributed: false },
        TestEntry { name: "p9_tls_strict_refuses_plaintext", description: "Contract suite (phase9)", estimated_secs: 25, categories: &[Security], distributed: false },
        TestEntry { name: "p9_tls_client_auth_none_allows_anonymous", description: "Contract suite (phase9)", estimated_secs: 25, categories: &[Security], distributed: false },
        TestEntry { name: "p9_identity_required_but_absent_rejected", description: "Contract suite (phase9)", estimated_secs: 25, categories: &[Security], distributed: false },
        TestEntry { name: "p9_public_key_identity_accepted_and_deterministic", description: "Contract suite (phase9)", estimated_secs: 25, categories: &[Security], distributed: false },
        TestEntry { name: "p9_identity_bad_signature_rejected", description: "Contract suite (phase9)", estimated_secs: 25, categories: &[Security], distributed: false },
        TestEntry { name: "p9_identity_clientid_mismatch_rejected", description: "Contract suite (phase9)", estimated_secs: 25, categories: &[Security], distributed: false },
        TestEntry { name: "p10_avro_accepts_conforming_payload", description: "Contract suite (phase10)", estimated_secs: 20, categories: &[Schema], distributed: false },
        TestEntry { name: "p10_avro_rejects_nonconforming_payload", description: "Contract suite (phase10)", estimated_secs: 20, categories: &[Schema], distributed: false },
        TestEntry { name: "p10_avro_malformed_schema_rejected", description: "Contract suite (phase10)", estimated_secs: 20, categories: &[Schema], distributed: false },
        TestEntry { name: "p10_protobuf_accepts_conforming_payload", description: "Contract suite (phase10)", estimated_secs: 20, categories: &[Schema], distributed: false },
        TestEntry { name: "p10_protobuf_rejects_nonconforming_payload", description: "Contract suite (phase10)", estimated_secs: 20, categories: &[Schema], distributed: false },
        TestEntry { name: "p10_protobuf_malformed_schema_rejected", description: "Contract suite (phase10)", estimated_secs: 20, categories: &[Schema], distributed: false },
        TestEntry { name: "p10_avro_duplicate_register_rejected", description: "Contract suite (phase10)", estimated_secs: 20, categories: &[Schema], distributed: false },
        TestEntry { name: "p10_encrypted_payload_roundtrips_unchanged", description: "Contract suite (phase10)", estimated_secs: 20, categories: &[Schema], distributed: false },
        TestEntry { name: "p10_encrypted_payload_skips_schema_validation", description: "Contract suite (phase10)", estimated_secs: 20, categories: &[Schema], distributed: false },
        TestEntry { name: "p11_leader_self_fences_on_lost_lease", description: "Contract suite (phase11)", estimated_secs: 120, categories: &[Election, Fencing], distributed: true },
        TestEntry { name: "p11_notleader_redirect_carries_leader_address", description: "Contract suite (phase11)", estimated_secs: 120, categories: &[Election, Fencing], distributed: true },
        TestEntry { name: "p11_throttled_link_preserves_acked_writes", description: "Contract suite (phase11)", estimated_secs: 120, categories: &[Election, Fencing], distributed: true },
        TestEntry { name: "p11_compaction_reclaims_after_trim", description: "Contract suite (phase11)", estimated_secs: 120, categories: &[Election, Fencing], distributed: true },
        // ── Core ──
        TestEntry {
            name: "single",
            description: "Basic CRUD operations, idempotency, and listing functionality",
            estimated_secs: 10,
            categories: &[Core],
            distributed: false,
        },
        TestEntry {
            name: "batch",
            description: "Write throughput and latency benchmark in standalone and replicated modes",
            estimated_secs: 210,
            categories: &[Core, Performance, RequiresKtls],
            distributed: true,
        },
        TestEntry {
            name: "batch_standalone_cleartext",
            description: "Plaintext standalone write throughput and latency benchmark",
            estimated_secs: 60,
            categories: &[Core, Performance],
            distributed: false,
        },
        TestEntry {
            name: "batch_standalone_compressed",
            description: "Plaintext standalone write throughput with ~3 KiB compressed (ZstdDict) payloads",
            estimated_secs: 40,
            categories: &[Core, Performance],
            distributed: false,
        },
        TestEntry {
            name: "batch_replicated_cleartext",
            description: "Plaintext replicated write throughput and latency benchmark",
            estimated_secs: 60,
            categories: &[Core, Performance],
            distributed: true,
        },
        TestEntry {
            name: "read_list_benchmark",
            description: "Read and list performance benchmark with large WAL and concurrent ops",
            estimated_secs: 150,
            categories: &[Core, Performance],
            distributed: false,
        },
        TestEntry {
            name: "rpi_cluster_bench",
            description: "Write benchmark against a remote RPi cluster over mTLS",
            estimated_secs: 60,
            categories: &[Performance, Rpi],
            distributed: true,
        },
        TestEntry {
            name: "rpi_cluster_pool_bench",
            description: "Pool-based write benchmark with automatic leader failover",
            estimated_secs: 60,
            categories: &[Performance, Rpi],
            distributed: true,
        },
        TestEntry {
            name: "chaos",
            description: "Concurrent read/write testing with variable large payload sizes",
            estimated_secs: 31,
            categories: &[Core],
            distributed: false,
        },
        TestEntry {
            name: "chaos_delete",
            description: "Concurrent write/delete/read testing with verification",
            estimated_secs: 31,
            categories: &[Core],
            distributed: false,
        },
        TestEntry {
            name: "chaos_watch",
            description: "Adversarial watch lifecycle (churn, half-open, slow readers, multi-shard) under read/write load",
            estimated_secs: 30,
            categories: &[Core, Edge],
            distributed: false,
        },
        TestEntry {
            name: "watch_failover",
            description: "Watch delivery history stays consistent with the final read across a leader kill + promotion + reconnect",
            estimated_secs: 60,
            categories: &[Replication, Election, Edge],
            distributed: true,
        },
        TestEntry {
            name: "watch_follower_promotion",
            description: "Follower-side watch subscriber across a promotion: every acked event delivered exactly once, in order, parked tail events fire on the promotion commit",
            estimated_secs: 60,
            categories: &[Replication, Election, Edge],
            distributed: true,
        },
        TestEntry {
            name: "connection_test",
            description: "Connection lifecycle, pipelining, and shard routing",
            estimated_secs: 1,
            categories: &[Core],
            distributed: false,
        },
        TestEntry {
            name: "watch_test",
            description: "Watch API streaming subscriptions",
            estimated_secs: 9,
            categories: &[Core],
            distributed: false,
        },
        TestEntry {
            name: "multi_shard_watch_test",
            description: "Multi-shard watch API with fallback behavior and event merging",
            estimated_secs: 46,
            categories: &[Core],
            distributed: false,
        },
        TestEntry {
            name: "typed_operations",
            description: "Typed convenience methods, auto-compression, and error matching",
            estimated_secs: 1,
            categories: &[Core],
            distributed: false,
        },
        TestEntry {
            name: "pool_test",
            description: "Connection pool operations, lifecycle, and idle eviction",
            estimated_secs: 1,
            categories: &[Core],
            distributed: false,
        },
        // ── Replication ──
        TestEntry {
            name: "s3_fallback_catchup",
            description: "Full cycle: normal replication -> S3 fallback -> follower boot catchup",
            estimated_secs: 21,
            categories: &[Replication],
            distributed: true,
        },
        TestEntry {
            name: "s3_fallback_s3_down",
            description: "Writes rejected when both follower offline and S3 unreachable",
            estimated_secs: 40,
            categories: &[Replication],
            distributed: true,
        },
        TestEntry {
            name: "s3_fallback_createonly",
            description: "CreateOnly prevents overwrites of existing S3 fallback objects",
            estimated_secs: 11,
            categories: &[Replication],
            distributed: true,
        },
        TestEntry {
            name: "s3_degraded_segment_summaries",
            description: "Sealed-segment .summary sidecars land on local NVMe across rotations on both TCP and S3 fallback paths",
            estimated_secs: 200,
            categories: &[Replication, Durability],
            distributed: true,
        },
        TestEntry {
            name: "s3_follower_crash",
            description: "Leader detects heartbeat loss, pre-renews S3 lease, continues writes",
            estimated_secs: 25,
            categories: &[Replication],
            distributed: true,
        },
        TestEntry {
            name: "s3_follower_kick",
            description: "Coordinated kick: network block -> WAL gap -> kick -> S3 catchup",
            estimated_secs: 21,
            categories: &[Replication],
            distributed: true,
        },
        TestEntry {
            name: "s3_leader_solo",
            description: "Leader alone with S3 fallback, late-joining follower catches up via S3",
            estimated_secs: 18,
            categories: &[Replication],
            distributed: true,
        },
        TestEntry {
            name: "standalone_to_distributed",
            description: "Standalone-to-distributed WAL migration and replication convergence",
            estimated_secs: 41,
            categories: &[Replication, Operations],
            distributed: true,
        },
        TestEntry {
            name: "follower_notify_liveness",
            description: "Idle-tail commit-notify convergence bound (burst tail + lone write)",
            estimated_secs: 40,
            categories: &[Core],
            distributed: true,
        },
        TestEntry {
            name: "follower_read_snapshot",
            description: "Replicated data visibility and delete/trim operations on follower",
            estimated_secs: 13,
            categories: &[Replication],
            distributed: true,
        },
        TestEntry {
            name: "leader_read_visibility",
            description: "Data not visible until replication completes (read-after-write invariant)",
            estimated_secs: 23,
            categories: &[Replication],
            distributed: true,
        },
        // ── Election ──
        TestEntry {
            name: "s3_election",
            description: "Cold-start S3 election and split-brain prevention via CreateOnly race",
            estimated_secs: 10,
            categories: &[Election],
            distributed: true,
        },
        TestEntry {
            name: "s3_failover_and_recovery",
            description: "Complete failover cycle: leader crash, follower takeover, old leader recovers as follower",
            estimated_secs: 26,
            categories: &[Election, Replication],
            distributed: true,
        },
        TestEntry {
            name: "s3_failover_latency",
            description: "Failover latency measurement from leader crash to follower accepting writes",
            estimated_secs: 16,
            categories: &[Election, Performance],
            distributed: true,
        },
        TestEntry {
            name: "s3_stale_lease",
            description: "Stale lease node rejoins as follower instead of attempting takeover",
            estimated_secs: 19,
            categories: &[Election],
            distributed: true,
        },
        TestEntry {
            name: "s3_lease_monotonicity",
            description: "Lease epoch strictly monotonically increasing across consecutive failovers",
            estimated_secs: 24,
            categories: &[Election],
            distributed: true,
        },
        TestEntry {
            name: "s3_unreachable_failover",
            description: "Dual failure: both nodes fence when S3 + TCP down, both reject writes, recover when restored",
            estimated_secs: 30,
            categories: &[Election, Fencing],
            distributed: true,
        },
        TestEntry {
            name: "s3_lease_renewal_backoff",
            description: "S3 lease renewal backs off during follower outage instead of hammering at heartbeat rate",
            estimated_secs: 25,
            categories: &[Election],
            distributed: true,
        },
        // ── Fencing ──
        TestEntry {
            name: "s3_fencing_writes",
            description: "Fenced nodes reject all write operations",
            estimated_secs: 11,
            categories: &[Fencing],
            distributed: true,
        },
        TestEntry {
            name: "s3_concurrent_cas",
            description: "Partition + CAS race + reconvergence: proxy replication, fencing, S3 race, single leader",
            estimated_secs: 28,
            categories: &[Fencing, Election, Replication],
            distributed: true,
        },
        // ── Invariant ──
        TestEntry {
            name: "invariant_clock_drift_rejection",
            description: "Follower rejects replication when clock drift exceeds threshold",
            estimated_secs: 25,
            categories: &[Invariant, Replication],
            distributed: true,
        },
        TestEntry {
            name: "invariant_held_seq_sibling_recovery",
            description: "A held client_seq consumed by a sibling yields a 2002 that must be verified, not trusted",
            estimated_secs: 1,
            categories: &[Invariant, Correctness],
            distributed: false,
        },
        TestEntry {
            name: "invariant_occ_before_idempotency",
            description: "OCC check fires before idempotency when both would fail",
            estimated_secs: 1,
            categories: &[Invariant, Correctness],
            distributed: false,
        },
        TestEntry {
            name: "reference_account_service",
            description: "Reference BFF: request dedup across replicas, in-memory and Postgres projections",
            estimated_secs: 5,
            categories: &[Invariant, Correctness],
            distributed: false,
        },
        TestEntry {
            name: "invariant_read_count",
            description: "count_events accurately returns the exact number of events written",
            estimated_secs: 36,
            categories: &[Invariant],
            distributed: false,
        },
        TestEntry {
            name: "invariant_concurrent_write",
            description: "Concurrent writes produce correct event count matching successful responses",
            estimated_secs: 1,
            categories: &[Invariant],
            distributed: false,
        },
        TestEntry {
            name: "invariant_replication_convergence",
            description: "Leader and follower have identical event counts after replication settles",
            estimated_secs: 23,
            categories: &[Invariant, Replication],
            distributed: true,
        },
        TestEntry {
            name: "invariant_s3_fallback_dedup",
            description: "S3 boot catchup produces no duplicate events in multi-shard config",
            estimated_secs: 55,
            categories: &[Invariant, Replication],
            distributed: true,
        },
        TestEntry {
            name: "invariant_replication_queue_pressure",
            description: "Throttled follower saturates inflight cap, surfacing ServerBusy backpressure; cluster converges after unthrottle",
            estimated_secs: 37,
            categories: &[Invariant, Replication],
            distributed: true,
        },
        // ── Metamorphic ──
        TestEntry {
            name: "metamorphic_leader_follower_parity",
            description: "Leader and follower return byte-identical events, batch indices, and tip hash per aggregate",
            estimated_secs: 20,
            categories: &[Metamorphic, Replication],
            distributed: true,
        },
        TestEntry {
            name: "metamorphic_post_failover_parity",
            description: "Old-leader and new-leader byte-identical after kill, promotion, more writes, and rejoin/catchup",
            estimated_secs: 60,
            categories: &[Metamorphic, Replication, Election],
            distributed: true,
        },
        TestEntry {
            name: "metamorphic_delete_trim_parity",
            description: "Standalone and 2-node cluster make identical delete/trim accept/reject decisions; sequence-continuation recreate resumes at the same version",
            estimated_secs: 30,
            categories: &[Metamorphic],
            distributed: true,
        },
        TestEntry {
            name: "metamorphic_standalone_vs_cluster",
            description: "Standalone and 2-node cluster produce identical event lists modulo cross-run time/UUID artefacts",
            estimated_secs: 30,
            categories: &[Metamorphic],
            distributed: true,
        },
        TestEntry {
            name: "metamorphic_follower_crash_catchup_parity",
            description: "Leader and follower byte-identical after SIGKILL, S3-fallback writes, S3 boot catchup, and resumed TCP replication",
            estimated_secs: 45,
            categories: &[Metamorphic, Replication],
            distributed: true,
        },
        TestEntry {
            name: "metamorphic_divergence_recovery_parity",
            description: "Old leader with divergent WAL truncates + S3-catches-up when rejoining, ends byte-identical with new leader",
            estimated_secs: 130,
            categories: &[Metamorphic, Replication, Election],
            distributed: true,
        },
        TestEntry {
            name: "metamorphic_cull_parity",
            description: "Speculative-tail removal across all three paths — in-process demotion cull, boot divergence truncation, GC-forced reframe — ends byte-identical with no acked loss",
            estimated_secs: 180,
            categories: &[Metamorphic, Replication, Election],
            distributed: true,
        },
        // ── Edge ──
        TestEntry {
            name: "edge_empty_replication_batch",
            description: "Heartbeat + replication cycles with no writes produce no inconsistency",
            estimated_secs: 20,
            categories: &[Edge],
            distributed: true,
        },
        TestEntry {
            name: "edge_stale_cache_rotation",
            description: "Log rotation + LRU eviction correctly update cache pointers",
            estimated_secs: 35,
            categories: &[Edge],
            distributed: true,
        },
        TestEntry {
            name: "edge_s3_missing_batches",
            description: "Missing S3 batch detection triggers fatal WalSeqGap error",
            estimated_secs: 32,
            categories: &[Edge],
            distributed: true,
        },
        TestEntry {
            name: "edge_s3_batch_ordering",
            description: "S3 fallback batches correctly ordered during catchup",
            estimated_secs: 32,
            categories: &[Edge],
            distributed: true,
        },
        TestEntry {
            name: "edge_log_rotation_mid_replication",
            description: "Log rotation mid-replication doesn't lose data despite LRU eviction",
            estimated_secs: 90,
            categories: &[Edge],
            distributed: true,
        },
        TestEntry {
            name: "edge_log_eviction_before_s3",
            description: "Evicted log files re-opened transparently for S3 upload",
            estimated_secs: 40,
            categories: &[Edge],
            distributed: true,
        },
        TestEntry {
            name: "edge_heartbeat_lock_contention",
            description: "Split locking keeps heartbeats independent from replication under pressure",
            estimated_secs: 42,
            categories: &[Edge],
            distributed: true,
        },
        TestEntry {
            name: "edge_concurrent_heartbeat_replication_s3",
            description: "S3 uploads without lock contention allow heartbeat flow during queue pressure",
            estimated_secs: 28,
            categories: &[Edge],
            distributed: true,
        },
        TestEntry {
            name: "edge_split_brain_s3_unavailable",
            description: "Heartbeat TTL + S3 CAS prevent writes during dual unavailability",
            estimated_secs: 28,
            categories: &[Edge],
            distributed: true,
        },
        TestEntry {
            name: "edge_s3_batch_boundary_contiguity",
            description: "S3 fallback batch boundaries contiguous under load (WalSeqGap bug)",
            estimated_secs: 45,
            categories: &[Edge, Replication],
            distributed: true,
        },
        TestEntry {
            name: "edge_s3_catchup_after_partition",
            description: "S3 catchup after partition + failover: no WalSeqGap on rejoin",
            estimated_secs: 40,
            categories: &[Edge, Replication],
            distributed: true,
        },
        TestEntry {
            name: "edge_s3_overlap_after_partition",
            description: "Both leaders upload S3 batches during partition: overlapping WAL indices on catchup",
            estimated_secs: 50,
            categories: &[Edge, Replication],
            distributed: true,
        },
        TestEntry {
            name: "edge_corrupted_s3_batch",
            description: "S3 batch CRC32C catches corruption, triggers fatal shutdown",
            estimated_secs: 31,
            categories: &[Edge],
            distributed: true,
        },
        TestEntry {
            name: "edge_list_pagination_cache_eviction",
            description: "Paginated list returns correct results despite WAL sequence cache eviction",
            estimated_secs: 5,
            categories: &[Edge],
            distributed: false,
        },
        TestEntry {
            name: "regression_list_iterator_truncation",
            description: "Shard-listing iterators do not truncate when the discovery probe fails",
            estimated_secs: 15,
            categories: &[Edge],
            distributed: false,
        },
        TestEntry {
            name: "edge_wal_tip_hash_divergence",
            description: "TipHashMismatch detection triggers WAL truncation and S3 catchup auto-heal",
            estimated_secs: 33,
            categories: &[Edge],
            distributed: true,
        },
        TestEntry {
            name: "edge_wal_divergence_and_recovery",
            description: "Leader crash with unreplicated writes, follower diverges, auto-heal via S3 truncation",
            estimated_secs: 54,
            categories: &[Edge],
            distributed: true,
        },
        // ── Bug Reproducers ──
        TestEntry {
            name: "stale_lease_restart_split_brain",
            description: "Stale-lease restart split-brain: HB-Ack `continue` skips S3 renewal, restarting follower CAS-steals stale lease then split brain",
            estimated_secs: 30,
            categories: &[Correctness, Election, Fencing],
            distributed: true,
        },
        // ── Correctness (Pilot Phase 1) ──
        TestEntry {
            name: "p1_1_dcb_rollback",
            description: "Multi-aggregate atomic write rolls back completely on OCC failure",
            estimated_secs: 1,
            categories: &[Correctness],
            distributed: false,
        },
        TestEntry {
            name: "p1_2_concurrent_dcb",
            description: "Concurrent multi-aggregate writes handle OCC conflicts in two-phase commit",
            estimated_secs: 1,
            categories: &[Correctness],
            distributed: false,
        },
        TestEntry {
            name: "p1_3_cross_shard_rejection",
            description: "Multi-aggregate writes spanning shards are properly rejected",
            estimated_secs: 1,
            categories: &[Correctness],
            distributed: false,
        },
        TestEntry {
            name: "p1_4_exactly_once",
            description: "Exactly-once writes under connection failure with client-side idempotency",
            estimated_secs: 1,
            categories: &[Correctness],
            distributed: false,
        },
        TestEntry {
            name: "p1_6_ordering_verification",
            description: "Per-aggregate strict total ordering with contiguous monotonic batch indices",
            estimated_secs: 1,
            categories: &[Correctness],
            distributed: false,
        },
        TestEntry {
            name: "p1_7_multitenancy_isolation",
            description: "Multi-tenancy isolation: org A cannot see org B's data",
            estimated_secs: 1,
            categories: &[Correctness],
            distributed: false,
        },
        TestEntry {
            name: "segment_summary_correctness",
            description: "Cross-segment delete barriers, trim, multi-org/type listing, and recreate-after-delete with segment summaries",
            estimated_secs: 30,
            categories: &[Correctness],
            distributed: false,
        },
        // ── Durability (Pilot Phase 2) ──
        TestEntry {
            name: "p2_1_write_survival",
            description: "Acknowledged writes survive SIGKILL in distributed cluster",
            estimated_secs: 17,
            categories: &[Durability],
            distributed: true,
        },
        TestEntry {
            name: "p2_5_blackout_acked_writes_survive",
            description: "Concurrent acked writes survive an S3 blackout + follower kill + heal cycle on both nodes",
            estimated_secs: 120,
            categories: &[Durability, Replication, Metamorphic],
            distributed: true,
        },
        TestEntry {
            name: "idempotency_cold_reconstruction",
            description: "After restart, cold client-seq reverse-WAL reconstruction dedupes a replay across interleaved foreign aggregates",
            estimated_secs: 20,
            categories: &[Correctness, Invariant],
            distributed: false,
        },
        TestEntry {
            name: "idempotency_negative_scan_load",
            description: "Reproduces the negative client-idempotency lookup cost: fresh-producer first-writes to a deep aggregate vs a shallow one",
            estimated_secs: 30,
            categories: &[Performance, Debug],
            distributed: false,
        },
        TestEntry {
            name: "idempotency_negative_scan_present_client",
            description: "a segment-present client (bloom can't short-circuit) first-writing to a deep aggregate vs a shallow one",
            estimated_secs: 30,
            categories: &[Performance, Debug],
            distributed: false,
        },
        TestEntry {
            name: "idempotency_restart_fanin",
            description: "After kill+restart, a concurrent burst of every client retrying its last seq is fully deduped and a new client still lands",
            estimated_secs: 30,
            categories: &[Correctness, Durability],
            distributed: false,
        },
        TestEntry {
            name: "idempotency_trim_client_state",
            description: "TrimStart keeps per-client idempotency floors (warm and cold) even when a client's entire history is trimmed away",
            estimated_secs: 20,
            categories: &[Correctness, Durability],
            distributed: false,
        },
        TestEntry {
            name: "idempotency_delete_recreate",
            description: "Delete+recreate vs client seq floors: old-seq retries and new clients in both sequence-continuation modes, warm and cold",
            estimated_secs: 20,
            categories: &[Correctness, Durability],
            distributed: false,
        },
        TestEntry {
            name: "recovery_multiseg_read_amplification",
            description: "After kill -9 on a root with sealed segments, first-touch write cost must stay bounded — pins the EBS recovery wedge (schema-absence scan walks the whole WAL when the SchemaKey/AggregateKey bloom-hash collision defeats segment skipping); bytes-read assertion via /proc/<pid>/io",
            estimated_secs: 30,
            categories: &[Durability, Performance],
            distributed: false,
        },
        TestEntry {
            name: "crash_kill9_under_load_durability",
            description: "SIGKILL mid-load past segment rotation, 3 restart cycles on the same data root: acked writes readable exactly once and in order, in-flight writes never duplicated or torn, idempotency floors correct post-recovery",
            estimated_secs: 15,
            categories: &[Correctness, Durability],
            distributed: false,
        },
        TestEntry {
            name: "schema_sealed_segment_survives_crash",
            description: "A schema registered in a segment that then seals (2+ rotations) is still enforced and findable after kill -9 + restart, for pre-crash and new aggregates — including the SchemaKey/AggregateKey bloom-collision shape where segment skipping is most tempted to over-skip",
            estimated_secs: 30,
            categories: &[Correctness, Durability, Schema],
            distributed: false,
        },
        TestEntry {
            name: "sidecar_size_tracks_cardinality",
            description: "A sealed segment holding a small key population (~64 aggregates, 4 clients) must produce a .summary sidecar under 64 KiB — blooms right-sized at seal from true cardinality, not fixed ~393 KB — and the shrunken sidecars must still serve post-kill-9 recovery",
            estimated_secs: 30,
            categories: &[Operations, Performance],
            distributed: false,
        },
        TestEntry {
            name: "idempotency_across_seal",
            description: "Client seq floors survive segment rotation: pre-seal seq retries rejected and new clients accepted, warm and after restart",
            estimated_secs: 60,
            categories: &[Correctness, Durability],
            distributed: false,
        },
        TestEntry {
            name: "p2_2_dual_restart",
            description: "Both nodes restart simultaneously, S3 lease race resolves cleanly",
            estimated_secs: 14,
            categories: &[Durability],
            distributed: true,
        },
        TestEntry {
            name: "p2_3_wal_corruption",
            description: "WAL on-disk corruption detected via CRC32C mismatch on restart",
            estimated_secs: 6,
            categories: &[Durability],
            distributed: false,
        },
        TestEntry {
            name: "storage_corruption_header_recovery",
            description: "Corrupt primary WAL header → node recovers byte-identical from the backup header",
            estimated_secs: 8,
            categories: &[Durability],
            distributed: false,
        },
        TestEntry {
            name: "wal_mid_datablock_truncation",
            description: "Torn uncommitted datablock bytes are invisible on restart and safely overwritten",
            estimated_secs: 8,
            categories: &[Durability],
            distributed: false,
        },
        TestEntry {
            name: "s3_to_tcp_failback",
            description: "Follower booting into S3 catchup with S3 dead fails back to TCP bridging",
            estimated_secs: 60,
            categories: &[Replication, Durability],
            distributed: true,
        },
        TestEntry {
            name: "p2_4_s3_capacity",
            description: "S3 degraded-mode capacity with large-volume fallback and follower catchup",
            estimated_secs: 42,
            categories: &[Durability],
            distributed: true,
        },
        // ── Performance (Pilot Phase 3) ──
        TestEntry {
            name: "p3_1_cold_read_latency",
            description: "Cold read latency after cache eviction measures reverse WAL scan + LRU",
            estimated_secs: 22,
            categories: &[Performance],
            distributed: false,
        },
        TestEntry {
            name: "p3_2_bloom_filter",
            description: "Bloom filter prevents unnecessary disk scans for missing aggregates",
            estimated_secs: 42,
            categories: &[Performance],
            distributed: false,
        },
        TestEntry {
            name: "p3_3_sequential_cold_reads",
            description: "Sustained cold sequential read throughput for audit/replay access patterns",
            estimated_secs: 39,
            categories: &[Performance],
            distributed: false,
        },
        TestEntry {
            name: "p3_4_read_thundering_herd",
            description: "Concurrent distinct cold reads stress the reverse-scan path; verifies the read semaphore bounds NVMe load without wedging, and surfaces read amplification",
            estimated_secs: 90,
            categories: &[Performance],
            distributed: false,
        },
        // ── Operations (Pilot Phase 4) ──
        TestEntry {
            name: "p4_1_rolling_upgrade",
            description: "Rolling upgrade with zero downtime: writes continue through node restarts",
            estimated_secs: 19,
            categories: &[Operations],
            distributed: true,
        },
        // ── Security ──
        TestEntry {
            name: "mtls_test",
            description: "mTLS handshake, plaintext rejection, and certificate trust validation",
            estimated_secs: 2,
            categories: &[Security],
            distributed: false,
        },
        TestEntry {
            name: "identity_test",
            description: "Client identity handshake and enforcement",
            estimated_secs: 1,
            categories: &[Security],
            distributed: false,
        },
        TestEntry {
            name: "api_key_test",
            description: "API key auth with read-only and read-write permission enforcement",
            estimated_secs: 1,
            categories: &[Security],
            distributed: false,
        },
        // ── Schema ──
        TestEntry {
            name: "schema_validation",
            description: "Schema registration, write enforcement, and WAL recovery",
            estimated_secs: 1,
            categories: &[Schema],
            distributed: false,
        },
        TestEntry {
            name: "schema_zero_cache",
            description: "Schema enforcement with all caches disabled",
            estimated_secs: 1,
            categories: &[Schema],
            distributed: false,
        },
        TestEntry {
            name: "schema_follower_crash",
            description: "Schemas survive follower crash, restart, and promotion",
            estimated_secs: 28,
            categories: &[Schema],
            distributed: true,
        },
        TestEntry {
            name: "schema_old_leader_recovery",
            description: "Schemas survive full A->B->A leadership cycle",
            estimated_secs: 35,
            categories: &[Schema],
            distributed: true,
        },
        TestEntry {
            name: "schema_bank_bench",
            description: "Schema-validated atomic writes throughput and latency benchmark",
            estimated_secs: 180,
            categories: &[Schema, Performance, RequiresKtls],
            distributed: true,
        },
        // ── Compaction ──
        TestEntry {
            name: "compaction_standalone",
            description: "Space reclamation from deleted aggregates on standalone server",
            estimated_secs: 34,
            categories: &[Compaction],
            distributed: false,
        },
        TestEntry {
            name: "compaction_restart",
            description: "Compacted data remains readable after server restart",
            estimated_secs: 35,
            categories: &[Compaction],
            distributed: false,
        },
        TestEntry {
            name: "compaction_replicated",
            description: "Leader compaction doesn't corrupt follower data in replicated mode",
            estimated_secs: 50,
            categories: &[Compaction],
            distributed: true,
        },
        // ── Bug reproductions ──
        TestEntry {
            name: "bug_kick_after_restart",
            description: "BUG: kick not delivered after follower restart (follower_reachable=false)",
            estimated_secs: 40,
            categories: &[Replication, Edge],
            distributed: true,
        },
        // ── Debug ──
        TestEntry {
            name: "debug_follower_pressure",
            description: "Follower under replication queue pressure with external process launch",
            estimated_secs: 60,
            categories: &[Debug],
            distributed: true,
        },
        TestEntry {
            name: "debug_demotion_cull_acked_loss",
            description: "Fence-bounce demotion cull rewinds past peer-acked entries; reconciliation probe must re-supply them before the leader dies",
            estimated_secs: 100,
            categories: &[Debug, Durability],
            distributed: true,
        },
    ]
}

impl TestEntry {
    pub fn timeout_secs(&self) -> u32 {
        // 2x estimated time, minimum 60s
        (self.estimated_secs * 2).max(60)
    }

    pub fn has_category(&self, cat: Category) -> bool {
        self.categories.iter().any(|c| *c == cat)
    }

    pub fn has_all_categories(&self, cats: &[Category]) -> bool {
        cats.iter().all(|c| self.has_category(*c))
    }

    pub fn has_any_category(&self, cats: &[Category]) -> bool {
        cats.iter().any(|c| self.has_category(*c))
    }
}

#[derive(Default)]
pub struct TestFilter {
    /// Test must have ALL of these categories (AND)
    pub include: Vec<Category>,
    /// Test must have at least ONE of these categories (OR)
    pub include_or: Vec<Category>,
    /// Exclude tests that have ALL of these categories (AND)
    pub exclude: Vec<Category>,
    /// Exclude tests that have at least ONE of these categories (OR)
    pub exclude_or: Vec<Category>,
    /// Only distributed tests
    pub distributed: bool,
    /// Only standalone tests
    pub standalone: bool,
    /// Run a specific test by name
    pub test: Option<String>,
}

impl TestFilter {
    pub fn apply<'a>(&self, tests: &'a [TestEntry]) -> Vec<&'a TestEntry> {
        tests
            .iter()
            .filter(|t| {
                if let Some(ref name) = self.test {
                    return t.name == name;
                }
                if !self.include.is_empty() && !t.has_all_categories(&self.include) {
                    return false;
                }
                if !self.include_or.is_empty() && !t.has_any_category(&self.include_or) {
                    return false;
                }
                if !self.exclude.is_empty() && t.has_all_categories(&self.exclude) {
                    return false;
                }
                if !self.exclude_or.is_empty() && t.has_any_category(&self.exclude_or) {
                    return false;
                }
                if self.distributed && !t.distributed {
                    return false;
                }
                if self.standalone && t.distributed {
                    return false;
                }
                true
            })
            .collect()
    }
}

pub fn parse_categories(s: &str) -> Result<Vec<Category>, String> {
    s.split(',')
        .map(|part| {
            let trimmed = part.trim();
            Category::from_str(trimmed)
                .ok_or_else(|| format!("unknown category '{}'. valid: {}", trimmed, category_list()))
        })
        .collect()
}

pub fn category_list() -> String {
    Category::ALL
        .iter()
        .map(|c| c.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}
