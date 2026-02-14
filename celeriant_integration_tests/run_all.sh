#!/usr/bin/env bash
set -euo pipefail

TIMEOUT=${1:-60}
TESTS=(
  single_main
  batch_main
  chaos_main
  chaos_delete_main
  connection_test_main
  watch_test_main
  s3_fallback_main
  s3_fallback_catchup_main
  s3_fallback_s3_down_main
  s3_fallback_createonly_main
  s3_election_main
  s3_follower_crash_main
  s3_leader_solo_main
  # --- Require follower-side failover (not yet implemented) ---
  # s3_failover_main
  # s3_stale_lease_main
  # s3_fencing_writes_main
  # s3_lease_monotonicity_main
  # s3_unreachable_failover_main
  # s3_network_partition_main
  # s3_reconvergence_main
  # s3_old_leader_recovery_main
  # s3_writes_during_fencing_main
  # s3_concurrent_cas_main
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
  printf "%-40s " "$test"
  start=$(date +%s%N)
  if timeout "${TIMEOUT}s" cargo run --release --bin "$test" > /tmp/celeriant_test2_${test}.log 2>&1; then
    elapsed=$(( ($(date +%s%N) - start) / 1000000 ))
    printf "PASS  (%d.%ds)\n" $((elapsed/1000)) $((elapsed%1000/100))
    ((passed++)) || true
  else
    exit_code=$?
    elapsed=$(( ($(date +%s%N) - start) / 1000000 ))
    if [ $exit_code -eq 124 ]; then
      printf "TIMEOUT  (%ds)\n" $TIMEOUT
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
