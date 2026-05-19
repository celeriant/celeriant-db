---
name: chaos-iter
description: Run one chaos scenario iteration on the rpi cluster and return a tight metric summary. Use during data-integrity investigations to avoid bloating the main conversation with bench output and JSON parsing.
---

# chaos-iter

Delegates the grunt work of running a chaos scenario, parsing its JSON, and returning the metrics that matter to a sub-agent. Saves ~200 lines of context per iteration.

## When to use

- Investigating missing-data / replication / failover bugs.
- Need to run `cargo run --release -p celeriant_chaos -- --scenario <name> --tasks <N>` and check a handful of fields in the resulting `runs/<ts>/<scenario>.json`.
- Don't need to retain the full bench output or sample stream in the main conversation.

## When NOT to use

- First-time setup or debugging the harness itself (you need the full output).
- Iteration count > 1 (use `--soak <secs>` directly so the harness manages iterations, or invoke this skill multiple times if you need per-iteration metrics).

## How to invoke

Dispatch a `general-purpose` agent with a self-contained prompt. Template:

```
Run this exact command and wait for it to complete (timeout: 600s):
  cargo run --release -p celeriant_chaos -- --scenario <SCENARIO> --tasks <TASKS>

After it completes:
1. Read the most recently created file under deploy/rpi-cluster/runs/<ts>/<SCENARIO>.json.
2. Extract and report (under 200 words):
   - integrity: tasks_audited, tasks_with_gaps, total_missing_acks, tasks_unreadable.
   - deep_audit: aggregates_inspected, aggregates_with_duplicates, aggregates_unreadable, FP-count (where present_count >= max_acked AND missing_seqs == []), REAL-count (rest).
   - Per-host (cs1=192.168.88.214, cs2=192.168.88.213) from the LAST `ok=true` sample, these counters:
     writes_total, rollbacks_total, cache_recent_write_hits_total,
     aggregate_details_snapshot_lag_total,
     write_validate_loop_crossed_rollback_total,
     write_rolled_back_pre_replicate_total,
     write_rolled_back_during_replicate_total,
     capture_dropped_items_total, fsync_capture_no_capture_race_total.
   - Run directory path (so the caller can do follow-up wal-inspect).

Do NOT include sample-by-sample data, bench logs, or full deep-audit entries. Just the summary above.
```

## Caller pattern

```
Agent(
  description: "Chaos iteration: <scenario>",
  subagent_type: "general-purpose",
  prompt: <the template above with scenario/tasks filled in>,
  run_in_background: false,  // usually want the result inline
)
```

Then act on the summary returned by the agent.
