---
name: wal-inspect-aggs
description: Run celeriant-wal-inspect on a list of failing aggregates across all shards on the rpi cluster and return a tabulated disk-truth report. Use during data-integrity investigations to cross-check audit claims against on-disk state without bloating the main conversation with raw wal-inspect output.
---

# wal-inspect-aggs

Delegates the grunt work of SSHing to the rpi data nodes, running `celeriant-wal-inspect client <org> <agg_type> <agg> <client>` per shard for each aggregate in a list, and aggregating the results into a single table. Saves ~30-50 lines of context per aggregate inspected.

## When to use

- Confirming or refuting an audit claim (e.g., "deep audit says no missing, but is the disk consistent?").
- Investigating agg_version reuse — wal-inspect shows the on-disk shape clearly (`missing in 1..=N` summary).
- Need disk-truth for multiple aggregates in one shot.

## When NOT to use

- One-off ad-hoc inspection of a single aggregate (just SSH and run directly — the overhead of dispatching isn't worth it).
- Need to read raw wal-inspect output line-by-line for some idiosyncratic pattern.

## Key facts the agent needs to know

- The tool is at `/usr/local/bin/celeriant-wal-inspect` on cs1 (192.168.88.214) and cs2 (192.168.88.213).
- Subcommand for a (aggregate, client_id): `client <org_id> <agg_type_id> <agg_id> <client_id>`. IDs are decimal `u128` or 32-char hex strings.
- The wal-inspect tool operates per-file, NOT per-shard. Each shard has one or more `log_N.wal` files in `/var/lib/celeriant/shard_<id>/`. Iterate over all `log_*.wal` files per shard to get full disk truth.
- Aggregates only live on shards 1..3 when `RESERVE_COORDINATOR_SHARD=true` (the default for the rpi cluster). Routing of aggregate_id → shard is non-obvious; the workaround is to inspect ALL non-zero shards and pick the one with `matched batches > 0`.
- The summary line `missing in 1..=N` is the headline: empty list = all seqs present on disk; otherwise it lists the gap.

## How to invoke

Dispatch a `general-purpose` agent with the list of aggregates. Template:

```
For each aggregate in the list below, SSH to the rpi data nodes and run
celeriant-wal-inspect on every log_*.wal file across shards 1..3 on cs1
(192.168.88.214). The cluster may be running or stopped — either is fine.

For each aggregate, report ONE line:
  client=<id> org=<o> type=<t> agg=<a>  shard=<n>  total_batches=<X>  within_read=<X>  max_agg_ver=<X>  distinct_seqs=<X>  missing=<list-or-none>

If matched batches == 0 on every shard, say "NOT FOUND on cs1".

Skip shard 0 (coordinator-reserved). Use:
  ssh 192.168.88.214 'for f in /var/lib/celeriant/shard_<n>/log_*.wal; do sudo /usr/local/bin/celeriant-wal-inspect "$f" client <org> <type> <agg> <client> 2>&1; done'

Aggregates to inspect:
- client=<C1> org=<O> type=<T> agg=<A1>
- client=<C2> org=<O> type=<T> agg=<A2>
- ...

Report under 300 words. No raw wal-inspect output, no per-batch lines —
just the tabulated summary line per aggregate.
```

## Caller pattern

```
Agent(
  description: "wal-inspect <N> aggregates",
  subagent_type: "general-purpose",
  prompt: <template above with aggregate list filled in>,
)
```

Use the returned table to decide whether to drill deeper on a specific aggregate (which would warrant a direct SSH for the raw output).
