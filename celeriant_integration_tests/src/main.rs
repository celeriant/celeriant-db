use celeriant_integration_tests::registry::{all_tests, parse_categories, Category, TestFilter};
use clap::Parser;
use std::fs::File;
use std::process::Command;
use std::time::Instant;
use wait_timeout::ChildExt;

#[derive(Parser)]
#[command(name = "celeriant-integration-tests", about = "Celeriant integration test runner")]
struct Cli {
    /// Run a specific test by name
    #[arg(long)]
    test: Option<String>,

    /// Include tests matching ALL of these categories (AND). Comma-separated.
    #[arg(long, value_delimiter = ',')]
    include: Vec<String>,

    /// Include tests matching ANY of these categories (OR). Comma-separated.
    #[arg(long, value_delimiter = ',')]
    include_or: Vec<String>,

    /// Exclude tests matching ALL of these categories (AND). Comma-separated.
    #[arg(long, value_delimiter = ',')]
    exclude: Vec<String>,

    /// Exclude tests matching ANY of these categories (OR). Comma-separated.
    #[arg(long, value_delimiter = ',')]
    exclude_or: Vec<String>,

    /// Only run distributed tests (require MinIO + multi-node)
    #[arg(long)]
    distributed: bool,

    /// Only run standalone tests (single node, no MinIO)
    #[arg(long)]
    standalone: bool,

    /// List matching tests without running them
    #[arg(long)]
    list: bool,

    /// List all available categories
    #[arg(long)]
    list_categories: bool,

    /// Per-test timeout in seconds (default: auto based on 2x estimated time, min 60s)
    #[arg(long)]
    timeout: Option<u32>,

    /// Internal: run a single test directly (used for subprocess dispatch)
    #[arg(long, hide = true)]
    run_test: Option<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Subprocess dispatch: run a single test directly
    if let Some(ref name) = cli.run_test {
        if let Err(e) = dispatch_test(name).await {
            eprintln!("{e}");
            std::process::exit(1);
        }
        return;
    }

    if cli.list_categories {
        println!("Available categories:");
        for cat in Category::ALL {
            let count = all_tests().iter().filter(|t| t.has_category(*cat)).count();
            println!("  {:<16} ({} tests)", cat.as_str(), count);
        }
        return;
    }

    if cli.distributed && cli.standalone {
        eprintln!("error: --distributed and --standalone are mutually exclusive");
        std::process::exit(1);
    }

    let filter = match build_filter(&cli) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

    let tests = all_tests();
    let selected = filter.apply(tests);

    if selected.is_empty() {
        println!("No tests match the given filters.");
        return;
    }

    if cli.list {
        print_test_list(&selected);
        return;
    }

    // Single test: run as subprocess, tee output to both terminal and log file
    if selected.len() == 1 {
        let name = selected[0].name;
        let self_exe = std::env::current_exe().expect("cannot resolve own executable path");
        let log_path = format!("/tmp/celeriant_test_{name}.log");

        println!("Running test: {name}");
        println!("Log: {log_path}\n");

        let log_file = File::create(&log_path).expect("cannot create log file");
        let log_file2 = log_file.try_clone().expect("cannot clone log handle");

        let mut child = Command::new(&self_exe)
            .args(["--run-test", name])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("failed to spawn test subprocess");

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        let t1 = std::thread::spawn(move || tee(stdout, std::io::stdout(), log_file));
        let t2 = std::thread::spawn(move || tee(stderr, std::io::stderr(), log_file2));

        let status = child.wait().expect("failed to wait on test subprocess");
        let _ = t1.join();
        let _ = t2.join();

        if status.success() {
            println!("\nPASS");
        } else {
            eprintln!("\nFAILED (exit {})", status.code().unwrap_or(-1));
            std::process::exit(1);
        }
        return;
    }

    run_tests(&selected, cli.timeout);
}

async fn dispatch_test(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    use celeriant_integration_tests::*;
    match name {
        "p1_append_reads_back_unchanged" => phase1::append_reads_back_unchanged().await,
        "p1_reads_are_ordered_and_gap_free" => phase1::reads_are_ordered_and_gap_free().await,
        "p1_version_tracks_batch_count" => phase1::version_tracks_batch_count().await,
        "p1_read_missing_aggregate_errors" => phase1::read_missing_aggregate_errors().await,
        "p1_empty_write_rejected" => phase1::empty_write_rejected().await,
        "p1_zero_event_type_rejected" => phase1::zero_event_type_rejected().await,
        "p1_write_no_create_rejected" => phase1::write_no_create_rejected().await,
        "p1_pagination_streams_whole_aggregate" => phase1::pagination_streams_whole_aggregate().await,
        "p1_offset_filter_bounds_range" => phase1::offset_filter_bounds_range().await,
        "p1_event_type_filter" => phase1::event_type_filter().await,
        "p1_client_id_filter" => phase1::client_id_filter().await,
        "p1_writes_survive_restart" => phase1::writes_survive_restart().await,
        "p1_exclude_client_id_filter" => phase1::exclude_client_id_filter().await,
        "p1_multi_event_batch_preserves_order" => phase1::multi_event_batch_preserves_order().await,
        "p1_event_time_range_filter" => phase1::event_time_range_filter().await,
        "p2_occ_match_commits_and_advances" => phase2::occ_match_commits_and_advances().await,
        "p2_occ_stale_rejected_no_append" => phase2::occ_stale_rejected_no_append().await,
        "p2_occ_create_guard_races_cleanly" => phase2::occ_create_guard_races_cleanly().await,
        "p2_idempotency_dedupes_replay" => phase2::idempotency_dedupes_replay().await,
        "p2_idempotency_scoped_per_client" => phase2::idempotency_scoped_per_client().await,
        "p2_multi_aggregate_atomic_rollback" => phase2::multi_aggregate_atomic_rollback().await,
        "p2_multi_aggregate_cross_shard_rejected" => phase2::multi_aggregate_cross_shard_rejected().await,
        "p2_occ_and_idempotency_compose" => phase2::occ_and_idempotency_compose().await,
        "p2_unconditional_write_appends" => phase2::unconditional_write_appends().await,
        "p3_trim_drops_prefix" => phase3::trim_drops_prefix().await,
        "p3_trim_preserves_remaining" => phase3::trim_preserves_remaining().await,
        "p3_trim_out_of_range_rejected" => phase3::trim_out_of_range_rejected().await,
        "p3_trim_missing_rejected" => phase3::trim_missing_rejected().await,
        "p3_delete_removes_aggregate" => phase3::delete_removes_aggregate().await,
        "p3_delete_no_recreate_blocks_rewrite" => phase3::delete_no_recreate_blocks_rewrite().await,
        "p3_delete_recreate_allows_rewrite" => phase3::delete_recreate_allows_rewrite().await,
        "p3_delete_conditional_guard" => phase3::delete_conditional_guard().await,
        "p3_delete_missing_rejected" => phase3::delete_missing_rejected().await,
        "p3_delete_sequence_continuation" => phase3::delete_sequence_continuation().await,
        "p3_delete_cross_shard_rejected" => phase3::delete_cross_shard_rejected().await,
        "p4_schema_rejects_bad_event" => phase4::schema_rejects_bad_event().await,
        "p4_schema_accepts_good_event" => phase4::schema_accepts_good_event().await,
        "p4_schema_duplicate_register_rejected" => phase4::schema_duplicate_register_rejected().await,
        "p4_schema_invalid_register_rejected" => phase4::schema_invalid_register_rejected().await,
        "p4_schema_unregistered_version_unvalidated" => phase4::schema_unregistered_version_unvalidated().await,
        "p5_watch_notifies_on_write" => phase5::watch_notifies_on_write().await,
        "p5_watch_incompatible_filter_rejected" => phase5::watch_incompatible_filter_rejected().await,
        "p5_watch_latency_too_high_rejected" => phase5::watch_latency_too_high_rejected().await,
        "p5_watch_cursor_misses_no_events" => phase5::watch_cursor_misses_no_events().await,
        "p5_watch_operations_are_distinct" => phase5::watch_operations_are_distinct().await,
        "p5_watch_create_notification_carries_version_range" => phase5::watch_create_notification_carries_version_range().await,
        "p6_cluster_elects_single_leader" => phase6::cluster_elects_single_leader().await,
        "p6_cluster_replicates_and_rejects_follower_write" => phase6::cluster_replicates_and_rejects_follower_write().await,
        "p6_follower_read_converges" => phase6::follower_read_converges().await,
        "p6_failover_promotes_follower" => phase6::failover_promotes_follower().await,
        "p7_concurrent_occ_writers_one_wins" => phase7::concurrent_occ_writers_one_wins().await,
        "p7_concurrent_creators_one_wins" => phase7::concurrent_creators_one_wins().await,
        "p7_concurrent_idempotent_retries_dedupe" => phase7::concurrent_idempotent_retries_dedupe().await,
        "p7_per_aggregate_order_across_shards" => phase7::per_aggregate_order_across_shards().await,
        "p7_large_values_long_stream_intact" => phase7::large_values_long_stream_intact().await,
        "p8_replication_link_cut_acks_via_s3_and_heals" => phase8::replication_link_cut_acks_via_s3_and_heals().await,
        "p8_s3_outage_leader_keeps_serving" => phase8::s3_outage_leader_keeps_serving().await,
        "p8_crash_follower_restart_data_survives" => phase8::crash_follower_restart_data_survives().await,
        "p8_exactly_once_across_failover" => phase8::exactly_once_across_failover().await,
        "p8_leader_follower_read_parity" => phase8::leader_follower_read_parity().await,
        "p9_mtls_require_good_client_connects" => phase9::mtls_require_good_client_connects().await,
        "p9_mtls_require_untrusted_ca_refused" => phase9::mtls_require_untrusted_ca_refused().await,
        "p9_mtls_require_no_cert_refused" => phase9::mtls_require_no_cert_refused().await,
        "p9_tls_strict_refuses_plaintext" => phase9::tls_strict_refuses_plaintext().await,
        "p9_tls_client_auth_none_allows_anonymous" => phase9::tls_client_auth_none_allows_anonymous().await,
        "p9_identity_required_but_absent_rejected" => phase9::identity_required_but_absent_rejected().await,
        "p9_public_key_identity_accepted_and_deterministic" => phase9::public_key_identity_accepted_and_deterministic().await,
        "p9_identity_bad_signature_rejected" => phase9::identity_bad_signature_rejected().await,
        "p9_identity_clientid_mismatch_rejected" => phase9::identity_clientid_mismatch_rejected().await,
        "p10_avro_accepts_conforming_payload" => phase10::avro_accepts_conforming_payload().await,
        "p10_avro_rejects_nonconforming_payload" => phase10::avro_rejects_nonconforming_payload().await,
        "p10_avro_malformed_schema_rejected" => phase10::avro_malformed_schema_rejected().await,
        "p10_protobuf_accepts_conforming_payload" => phase10::protobuf_accepts_conforming_payload().await,
        "p10_protobuf_rejects_nonconforming_payload" => phase10::protobuf_rejects_nonconforming_payload().await,
        "p10_protobuf_malformed_schema_rejected" => phase10::protobuf_malformed_schema_rejected().await,
        "p10_avro_duplicate_register_rejected" => phase10::avro_duplicate_register_rejected().await,
        "p10_encrypted_payload_roundtrips_unchanged" => phase10::encrypted_payload_roundtrips_unchanged().await,
        "p10_encrypted_payload_skips_schema_validation" => phase10::encrypted_payload_skips_schema_validation().await,
        "p11_leader_self_fences_on_lost_lease" => phase11::leader_self_fences_on_lost_lease().await,
        "p11_notleader_redirect_carries_leader_address" => phase11::notleader_redirect_carries_leader_address().await,
        "p11_throttled_link_preserves_acked_writes" => phase11::throttled_link_preserves_acked_writes().await,
        "p11_compaction_reclaims_after_trim" => phase11::compaction_reclaims_after_trim().await,
        "api_key_test" => api_key_test::run().await,
        "batch" => batch::run().await,
        "batch_standalone_cleartext" => batch_standalone_cleartext::run().await,
        "chaos" => chaos::run().await,
        "chaos_delete" => chaos_delete::run().await,
        "chaos_watch" => chaos_watch::run().await,
        "compaction_replicated" => compaction_replicated::run().await,
        "compaction_restart" => compaction_restart::run().await,
        "compaction_standalone" => compaction_standalone::run().await,
        "bug_kick_after_restart" => bug_kick_after_restart::run().await,
        "connection_test" => connection_test::run().await,
        "debug_demotion_cull_acked_loss" => debug_demotion_cull_acked_loss::run().await,
        "debug_follower_pressure" => debug_follower_pressure::run().await,
        "stale_lease_restart_split_brain" => stale_lease_restart_split_brain::run().await,
        "edge_concurrent_heartbeat_replication_s3" => edge_concurrent_heartbeat_replication_s3::run().await,
        "edge_corrupted_s3_batch" => edge_corrupted_s3_batch::run().await,
        "edge_s3_batch_boundary_contiguity" => edge_s3_batch_boundary_contiguity::run().await,
        "edge_s3_catchup_after_partition" => edge_s3_catchup_after_partition::run().await,
        "edge_s3_overlap_after_partition" => edge_s3_overlap_after_partition::run().await,
        "edge_empty_replication_batch" => edge_empty_replication_batch::run().await,
        "edge_heartbeat_lock_contention" => edge_heartbeat_lock_contention::run().await,
        "edge_wal_divergence_and_recovery" => edge_wal_divergence_and_recovery::run().await,
        "edge_list_pagination_cache_eviction" => edge_list_pagination_cache_eviction::run().await,
        "edge_log_eviction_before_s3" => edge_log_eviction_before_s3::run().await,
        "edge_log_rotation_mid_replication" => edge_log_rotation_mid_replication::run().await,
        "edge_s3_batch_ordering" => edge_s3_batch_ordering::run().await,
        "edge_s3_missing_batches" => edge_s3_missing_batches::run().await,
        "edge_split_brain_s3_unavailable" => edge_split_brain_s3_unavailable::run().await,
        "edge_stale_cache_rotation" => edge_stale_cache_rotation::run().await,
        "edge_wal_tip_hash_divergence" => edge_wal_tip_hash_divergence::run().await,
        "follower_read_snapshot" => follower_read_snapshot::run().await,
        "identity_test" => identity_test::run().await,
        "invariant_clock_drift_rejection" => invariant_clock_drift_rejection::run().await,
        "invariant_concurrent_write" => invariant_concurrent_write::run().await,
        "invariant_held_seq_sibling_recovery" => invariant_held_seq_sibling_recovery::run().await,
        "invariant_occ_before_idempotency" => invariant_occ_before_idempotency::run().await,
        "reference_account_service" => reference_account_service::run().await,
        "invariant_read_count" => invariant_read_count::run().await,
        "invariant_replication_convergence" => invariant_replication_convergence::run().await,
        "invariant_replication_queue_pressure" => invariant_replication_queue_pressure::run().await,
        "invariant_s3_fallback_dedup" => invariant_s3_fallback_dedup::run().await,
        "leader_read_visibility" => leader_read_visibility::run().await,
        "metamorphic_cull_parity" => metamorphic_cull_parity::run().await,
        "metamorphic_divergence_recovery_parity" => metamorphic_divergence_recovery_parity::run().await,
        "metamorphic_follower_crash_catchup_parity" => metamorphic_follower_crash_catchup_parity::run().await,
        "metamorphic_leader_follower_parity" => metamorphic_leader_follower_parity::run().await,
        "metamorphic_post_failover_parity" => metamorphic_post_failover_parity::run().await,
        "metamorphic_standalone_vs_cluster" => metamorphic_standalone_vs_cluster::run().await,
        "mtls_test" => mtls_test::run().await,
        "multi_shard_watch_test" => multi_shard_watch_test::run().await,
        "p1_1_dcb_rollback" => p1_1_dcb_rollback::run().await,
        "p1_2_concurrent_dcb" => p1_2_concurrent_dcb::run().await,
        "p1_3_cross_shard_rejection" => p1_3_cross_shard_rejection::run().await,
        "p1_4_exactly_once" => p1_4_exactly_once::run().await,
        "p1_6_ordering_verification" => p1_6_ordering_verification::run().await,
        "p1_7_multitenancy_isolation" => p1_7_multitenancy_isolation::run().await,
        "p2_1_write_survival" => p2_1_write_survival::run().await,
        "p2_5_blackout_acked_writes_survive" => p2_5_blackout_acked_writes_survive::run().await,
        "p2_2_dual_restart" => p2_2_dual_restart::run().await,
        "p2_3_wal_corruption" => p2_3_wal_corruption::run().await,
        "p2_4_s3_capacity" => p2_4_s3_capacity::run().await,
        "p3_1_cold_read_latency" => p3_1_cold_read_latency::run().await,
        "p3_2_bloom_filter" => p3_2_bloom_filter::run().await,
        "p3_3_sequential_cold_reads" => p3_3_sequential_cold_reads::run().await,
        "p3_4_read_thundering_herd" => p3_4_read_thundering_herd::run().await,
        "p4_1_rolling_upgrade" => p4_1_rolling_upgrade::run().await,
        "pool_test" => pool_test::run().await,
        "read_list_benchmark" => read_list_benchmark::run().await,
        "rpi_cluster_bench" => rpi_cluster_bench::run().await,
        "rpi_cluster_pool_bench" => rpi_cluster_pool_bench::run().await,
        "s3_concurrent_cas" => s3_concurrent_cas::run().await,
        "s3_degraded_segment_summaries" => s3_degraded_segment_summaries::run().await,
        "s3_election" => s3_election::run().await,
        "s3_failover_and_recovery" => s3_failover_and_recovery::run().await,
        "s3_failover_latency" => s3_failover_latency::run().await,
        "s3_fallback_catchup" => s3_fallback_catchup::run().await,
        "s3_fallback_createonly" => s3_fallback_createonly::run().await,
        "s3_fallback_s3_down" => s3_fallback_s3_down::run().await,
        "s3_fencing_writes" => s3_fencing_writes::run().await,
        "s3_follower_crash" => s3_follower_crash::run().await,
        "s3_follower_kick" => s3_follower_kick::run().await,
        "s3_leader_solo" => s3_leader_solo::run().await,
        "s3_lease_monotonicity" => s3_lease_monotonicity::run().await,
        "s3_lease_renewal_backoff" => s3_lease_renewal_backoff::run().await,
        "s3_stale_lease" => s3_stale_lease::run().await,
        "s3_unreachable_failover" => s3_unreachable_failover::run().await,
        "schema_bank_bench" => schema_bank_bench::run().await,
        "schema_follower_crash" => schema_follower_crash::run().await,
        "schema_old_leader_recovery" => schema_old_leader_recovery::run().await,
        "schema_validation" => schema_validation::run().await,
        "schema_zero_cache" => schema_zero_cache::run().await,
        "segment_summary_correctness" => segment_summary_correctness::run().await,
        "single" => single::run().await,
        "standalone_to_distributed" => standalone_to_distributed::run().await,
        "storage_corruption_header_recovery" => storage_corruption_header_recovery::run().await,
        "wal_mid_datablock_truncation" => wal_mid_datablock_truncation::run().await,
        "s3_to_tcp_failback" => s3_to_tcp_failback::run().await,
        "typed_operations" => typed_operations::run().await,
        "watch_failover" => watch_failover::run().await,
        "watch_test" => watch_test::run().await,
        _ => {
            eprintln!("unknown test: {name}");
            std::process::exit(1);
        }
    }
}

fn build_filter(cli: &Cli) -> Result<TestFilter, String> {
    let mut filter = TestFilter {
        distributed: cli.distributed,
        standalone: cli.standalone,
        test: cli.test.clone(),
        ..Default::default()
    };

    if !cli.include.is_empty() {
        filter.include = parse_categories(&cli.include.join(","))?;
    }
    if !cli.include_or.is_empty() {
        filter.include_or = parse_categories(&cli.include_or.join(","))?;
    }
    if !cli.exclude.is_empty() {
        filter.exclude = parse_categories(&cli.exclude.join(","))?;
    }
    if !cli.exclude_or.is_empty() {
        filter.exclude_or = parse_categories(&cli.exclude_or.join(","))?;
    }

    // Opt-in categories — appended to exclude_or unless the user has explicitly
    // opted in via --include-or / --include / --test. These need external setup:
    //   Debug — requires external process launch
    //   Rpi   — requires a remote hardware cluster + provisioned PKI
    if cli.test.is_none() {
        for cat in [Category::Debug, Category::Rpi] {
            let opted_in = filter.include_or.contains(&cat) || filter.include.contains(&cat);
            if !opted_in && !filter.exclude_or.contains(&cat) {
                filter.exclude_or.push(cat);
            }
        }
    }

    Ok(filter)
}

fn print_test_list(selected: &[&celeriant_integration_tests::registry::TestEntry]) {
    let est_total: u32 = selected.iter().map(|t| t.estimated_secs).sum();
    println!(
        "{} tests selected (est. {}m {}s)\n",
        selected.len(),
        est_total / 60,
        est_total % 60,
    );
    for t in selected {
        let mode = if t.distributed { "distributed" } else { "standalone" };
        let cats: Vec<&str> = t.categories.iter().map(|c| c.as_str()).collect();
        println!(
            "  {:<45} {:>4}s  {:<12} [{}]",
            t.name, t.estimated_secs, mode, cats.join(", "),
        );
    }
}

fn run_tests(
    tests: &[&celeriant_integration_tests::registry::TestEntry],
    timeout_override: Option<u32>,
) {
    let est_total: u32 = tests.iter().map(|t| t.estimated_secs).sum();
    println!(
        "Running {} tests (est. {}m {}s)\n",
        tests.len(),
        est_total / 60,
        est_total % 60,
    );

    let self_exe = std::env::current_exe().expect("cannot resolve own executable path");

    let mut passed = 0u32;
    let mut failed = 0u32;
    let mut timed_out = 0u32;
    let mut is_first = true;

    let suite_start = Instant::now();

    for test in tests {
        // Allow previous test's servers to fully tear down
        if !is_first {
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        is_first = false;

        let timeout = timeout_override.unwrap_or_else(|| test.timeout_secs());
        let log_path = format!("/tmp/celeriant_test_{}.log", test.name);

        print!("{:<50} ", test.name);

        // Write output to temp file to avoid pipe buffer deadlocks
        let log_file = match File::create(&log_path) {
            Ok(f) => f,
            Err(e) => {
                println!("ERROR  (cannot create log: {e})");
                failed += 1;
                continue;
            }
        };
        let stderr_file = match log_file.try_clone() {
            Ok(f) => f,
            Err(e) => {
                println!("ERROR  (cannot clone log handle: {e})");
                failed += 1;
                continue;
            }
        };

        let start = Instant::now();
        let result = Command::new(&self_exe)
            .args(["--run-test", test.name])
            .stdout(log_file)
            .stderr(stderr_file)
            .spawn();

        match result {
            Ok(mut child) => {
                let timeout_dur = std::time::Duration::from_secs(timeout as u64);
                match child.wait_timeout(timeout_dur) {
                    Ok(Some(status)) => {
                        let elapsed = start.elapsed();
                        if status.success() {
                            println!("PASS  ({:.1}s)", elapsed.as_secs_f64());
                            passed += 1;
                        } else {
                            println!(
                                "FAIL  (exit {}, {:.1}s)",
                                status.code().unwrap_or(-1),
                                elapsed.as_secs_f64(),
                            );
                            print_log_tail(&log_path, 5);
                            failed += 1;
                        }
                    }
                    Ok(None) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        println!("TIMEOUT  ({}s)", timeout);
                        timed_out += 1;
                    }
                    Err(e) => {
                        println!("ERROR  ({e})");
                        failed += 1;
                    }
                }
            }
            Err(e) => {
                println!("ERROR  (spawn failed: {e})");
                failed += 1;
            }
        }
    }

    let suite_elapsed = suite_start.elapsed();
    println!();
    println!("=== Summary ===");
    println!(
        "Passed: {}  Failed: {}  Timed out: {}  Total: {}  ({:.0}s)",
        passed, failed, timed_out, tests.len(), suite_elapsed.as_secs_f64(),
    );
    println!("Logs: /tmp/celeriant_test_*.log");

    if failed + timed_out > 0 {
        std::process::exit(1);
    }
}

fn print_log_tail(path: &str, n: usize) {
    use std::io::BufRead;
    let Ok(file) = File::open(path) else { return };
    let lines: Vec<String> = std::io::BufReader::new(file)
        .lines()
        .filter_map(|l| l.ok())
        .collect();
    let skip = lines.len().saturating_sub(n);
    for line in lines.into_iter().skip(skip) {
        println!("  {line}");
    }
}

/// Copy from `reader` to both `terminal` and `log_file` simultaneously.
fn tee(reader: impl std::io::Read, mut terminal: impl std::io::Write, mut log_file: File) {
    use std::io::Write;
    let mut buf = [0u8; 8192];
    let mut reader = reader;
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let _ = terminal.write_all(&buf[..n]);
                let _ = log_file.write_all(&buf[..n]);
            }
            Err(_) => break,
        }
    }
}
