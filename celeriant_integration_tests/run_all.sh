#!/usr/bin/env bash
set -euo pipefail

TIMEOUT=${1:-60}

# Per-test timeout overrides (8 benchmark scenarios need more time)
declare -A TIMEOUT_OVERRIDE
TIMEOUT_OVERRIDE[batch_main]=300
TIMEOUT_OVERRIDE[compaction_standalone_main]=120
TIMEOUT_OVERRIDE[compaction_restart_main]=120
TIMEOUT_OVERRIDE[compaction_replicated_main]=120
TIMEOUT_OVERRIDE[edge_leader_crash_divergent_wal_main]=120

TESTS=(
  single_main
  batch_main
  read_list_benchmark_main
  chaos_main
  chaos_delete_main
  connection_test_main
  watch_test_main
  multi_shard_watch_test_main
  s3_fallback_main
  s3_fallback_catchup_main
  s3_fallback_s3_down_main
  s3_fallback_createonly_main
  s3_election_main
  s3_follower_crash_main
  s3_leader_solo_main
  not_leader_error_main
  # --- Follower-side failover (watchdog) ---
  s3_failover_main
  s3_stale_lease_main
  s3_fencing_writes_main
  s3_lease_monotonicity_main
  s3_unreachable_failover_main
  s3_network_partition_main
  s3_reconvergence_main
  s3_old_leader_recovery_main
  s3_writes_during_fencing_main
  s3_concurrent_cas_main
  # --- Invariant tests ---
  invariant_read_count_main
  invariant_concurrent_write_main
  invariant_replication_convergence_main
  invariant_s3_fallback_dedup_main
  invariant_replication_queue_pressure_main
    # --- Follower-kick ---
  s3_follower_kick_main
  # --- Mode transition ---
  standalone_to_distributed_main
  # --- Edge cases ---
  edge_empty_replication_batch_main
  edge_stale_cache_rotation_main
  edge_s3_missing_batches_main
  edge_s3_batch_ordering_main
  edge_log_rotation_mid_replication_main
  edge_log_eviction_before_s3_main
  # --- Replication client locking regression guards (#14, #15) ---
  edge_heartbeat_lock_contention_main
  edge_concurrent_heartbeat_replication_s3_main
  # --- Batch 2 edge cases (#1, #4, #10) ---
  edge_split_brain_s3_unavailable_main
  edge_corrupted_s3_batch_main
  edge_list_pagination_cache_eviction_main
  # --- WAL tip hash divergence (#2, #3) ---
  edge_wal_tip_hash_divergence_main
  edge_wal_divergence_recovery_main
  edge_leader_crash_divergent_wal_main
  # --- Pilot Phase 1: Correctness ---
  p1_1_dcb_rollback_main
  p1_2_concurrent_dcb_main
  p1_3_cross_shard_rejection_main
  p1_4_exactly_once_main
  p1_6_ordering_verification_main
  p1_7_multitenancy_isolation_main
  # --- Pilot Phase 2: Durability ---
  p2_1_write_survival_main
  p2_2_dual_restart_main
  p2_3_wal_corruption_main
  p2_4_s3_capacity_main
  # --- Pilot Phase 3: Architecture ---
  p3_1_cold_read_latency_main
  p3_2_bloom_filter_main
  p3_3_sequential_cold_reads_main
  # --- Pilot Phase 4: Operational ---
  p4_1_rolling_upgrade_main
  # --- MTLS ---
  mtls_test_main
  # --- Client identity enforcement ---
  identity_test_main
  # --- Schema validation ---
  schema_validation_main
  schema_zero_cache_main
  schema_failover_main
  schema_follower_crash_main
  schema_old_leader_recovery_main
  # --- Tokio client typed operations ---
  typed_operations_main
  pool_test_main
  # --- Compaction ---
  compaction_standalone_main
  compaction_restart_main
  compaction_replicated_main
  # -- Read/Write Visibility ---
  follower_read_snapshot_main
  leader_read_visibility_main
)

# Pre-build everything so compilation isn't counted in per-test timeout
echo "Building all binaries (release)..."
cargo build --release
echo "Build complete."
echo ""

passed=0
failed=0
timed_out=0

for test in "${TESTS[@]}"; do
  test_timeout=${TIMEOUT_OVERRIDE[$test]:-$TIMEOUT}
  printf "%-40s " "$test"
  start=$(date +%s%N)
  if timeout "${test_timeout}s" cargo run --release --bin "$test" > /tmp/celeriant_test2_${test}.log 2>&1; then
    elapsed=$(( ($(date +%s%N) - start) / 1000000 ))
    printf "PASS  (%d.%ds)\n" $((elapsed/1000)) $((elapsed%1000/100))
    ((passed++)) || true
  else
    exit_code=$?
    elapsed=$(( ($(date +%s%N) - start) / 1000000 ))
    if [ $exit_code -eq 124 ]; then
      printf "TIMEOUT  (%ds)\n" $test_timeout
      ((timed_out++)) || true
    else
      printf "FAIL  (exit %d, %d.%ds)\n" $exit_code $((elapsed/1000)) $((elapsed%1000/100))
      tail -5 /tmp/celeriant_test2_${test}.log | sed 's/^/  /'
      ((failed++)) || true
    fi
  fi
done

echo ""
echo "=== Summary ==="
echo "Passed: $passed  Failed: $failed  Timed out: $timed_out  Total: ${#TESTS[@]}"
echo "Logs: /tmp/celeriant_test2_*.log"

[ $((failed + timed_out)) -eq 0 ] || exit 1
