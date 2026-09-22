---
name: chaos-iter
description: Delegate one rpi-cluster chaos iteration and summarize integrity and replication metrics during data-loss or failover investigations.
---

# Run one chaos iteration

Use the full harness output for setup or harness debugging. Use `--soak <secs>` for continuous runs.

Dispatch a `general-purpose` agent synchronously with the scenario, task count, repository path, and these instructions:

1. Run from the repository root with a 600-second timeout:

   ```bash
   bash deploy/rpi-cluster/chaos.sh --scenario <SCENARIO> --tasks <TASKS>
   ```

2. Capture the exit status and announced run directory. Read `<run_dir>/<SCENARIO>.json`; use `<run_dir>/harness.log` for failures. Do not substitute an older run if output is missing.
3. Report fewer than 200 words containing:
   - Run directory, exit status, and any timeout or missing output.
   - Integrity: `tasks_audited`, `tasks_with_gaps`, `total_missing_acks`, `tasks_unreadable`.
   - Deep audit: `aggregates_inspected`, `aggregates_with_duplicates`, `aggregates_unreadable`; count FP entries where `present_count >= max_acked && missing_seqs == []`, and REAL entries otherwise.
   - Per-host counters from each host's last `ok=true` sample: cs1=`192.168.88.214`, cs2=`192.168.88.213`.

Extract these counters:

```text
writes_total
rollbacks_total
cache_recent_write_hits_total
aggregate_details_snapshot_lag_total
write_validate_loop_crossed_rollback_total
write_rolled_back_pre_replicate_total
write_rolled_back_during_replicate_total
capture_dropped_items_total
fsync_capture_no_capture_race_total
```

Mark unavailable fields as missing. Omit sample streams, bench logs, and full audit entries.
