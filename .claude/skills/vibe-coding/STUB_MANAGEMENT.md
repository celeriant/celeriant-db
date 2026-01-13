# Stub Management

Stubs are incomplete implementations that need to be finished later. **Silent stubs are bugs waiting to happen.** This document defines how to create, track, and eliminate stubs.

## Who Does What (Orchestrator/Implementer Split)

| Responsibility | Who |
|---------------|-----|
| Create stubs with `todo!("STUB...")` | Implementer |
| Report stubs created in phase summary | Implementer |
| Track stubs in SESSION_STATE.md | Orchestrator |
| Scan for stubs during validation | integration-validator |
| Decide when to resolve stubs | Orchestrator |

## The Problem

Bad stub patterns:
```rust
// BAD: Silent stub - returns wrong data, no failure
fn get_entries_for_catchup(&self, shard_id: u32, from_index: u64) -> Vec<WalEntry> {
    vec![]  // TODO: implement
}

// BAD: Silent stub - skips important work
fn replicate_to_follower(&self, batch: &Batch) -> Result<()> {
    // TODO: actually send to follower
    Ok(())
}

// BAD: Invisible stub - easy to miss
fn calculate_lease_duration(&self) -> Duration {
    Duration::from_secs(5)  // hardcoded for now
}
```

These stubs don't fail. They produce wrong behavior that's discovered later as a "bug."

## Rule: Stubs Must Fail Loudly

### Use `todo!()` Macro

```rust
// GOOD: Fails immediately when called
fn get_entries_for_catchup(&self, shard_id: u32, from_index: u64) -> Vec<WalEntry> {
    todo!("STUB: fetch WAL entries from disk for catch-up")
}

// GOOD: Fails with context
fn replicate_to_follower(&self, batch: &Batch) -> Result<()> {
    todo!("STUB: send batch over TCP to follower")
}
```

### Use `unimplemented!()` for Intentional Gaps

```rust
// GOOD: For features explicitly deferred
fn handle_three_node_cluster(&self) -> Result<()> {
    unimplemented!("Three-node clusters not supported in v1")
}
```

### Use Compile Errors for Critical Paths

```rust
// GOOD: Won't even compile - impossible to forget
fn apply_batch(&self, batch: &Batch) -> Result<()> {
    compile_error!("STUB: batch application not implemented")
}
```

## Rule: Stubs Must Be Trackable

### Consistent Marker Format

Every stub gets a searchable marker:

```rust
todo!("STUB(catch-up): fetch WAL entries for shard catch-up")
//     ^^^^^^^^^^^^^ category in parens
```

Categories:
- `STUB(wire)` - Network/protocol stubs
- `STUB(storage)` - Disk/WAL stubs
- `STUB(s3)` - S3/sidecar stubs
- `STUB(test)` - Test-only stubs
- `STUB(error)` - Error handling stubs

### Grep Command

Find all stubs:
```bash
rg "todo!\(\"STUB" --type rust
rg "STUB\(" --type rust
```

## Rule: Stubs Must Be Registered

### For Implementers
When you create a stub, **report it in your phase summary**:

```markdown
### Stubs created
- coordinator.rs:145 - STUB(catch-up): Fetch WAL entries for shard
- replication_loop.rs:89 - STUB(wire): Send batch over TCP
```

### For Orchestrators
When the implementer reports stubs, **add them to SESSION_STATE.md**:

```markdown
## Active Stubs

| Location | Category | Description | Blocker |
|----------|----------|-------------|---------|
| coordinator.rs:145 | catch-up | Fetch WAL entries for shard | Need ShardOps trait |
| replication_loop.rs:89 | wire | Send batch over TCP | Need connection setup |
| sidecar_bridge.rs:201 | s3 | List fallback batches | Need ObjectList support |
```

The "Blocker" column explains **why** it's stubbed - what needs to exist first.

## Rule: Stubs Are Tracked, Not Hidden

The `integration-validator` agent scans for stubs during validation. Stubs are allowed during development - they're reported as warnings, not failures.

### For Orchestrators
When committing with active stubs, acknowledge them in the commit message:

```
Phase 3: Wire protocol implementation

Implemented batch serialization and connection handling.

Contains STUB:
- coordinator.rs:145 - catch-up entries (blocked on ShardOps)
- replication_loop.rs:89 - TCP send (next phase)
```

This makes stubs visible in git history for the human doing replay.

## Stub Lifecycle

```
IMPLEMENTER                          ORCHESTRATOR
─────────────                        ────────────
┌─────────────────┐
│ Need to defer   │
│ some work       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 1. Use todo!()  │ ← Fails loudly if called
│    with STUB    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 2. Report in    │
│    phase summary│
└────────┬────────┘
         │
         └──────────────────────────▶ ┌─────────────────┐
                                      │ 3. Add to       │
                                      │ SESSION_STATE   │
                                      └────────┬────────┘
                                               │
                                               ▼
                                      ┌─────────────────┐
                                      │ 4. Include in   │
                                      │ commit message  │
                                      └────────┬────────┘
                                               │
                                               ▼
                                      ┌─────────────────┐
                                      │ 5. Plan phase   │
                                      │ to resolve stub │
                                      └────────┬────────┘
                                               │
         ┌─────────────────────────────────────┘
         ▼
┌─────────────────┐
│ 6. Implement    │
│    stub         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 7. Report in    │
│    phase summary│
└────────┬────────┘
         │
         └──────────────────────────▶ ┌─────────────────┐
                                      │ 8. Remove from  │
                                      │ SESSION_STATE   │
                                      └─────────────────┘
```

## Anti-Patterns

### Don't Use Silent Returns

```rust
// BAD
fn get_something(&self) -> Option<Thing> {
    None  // stub
}

// GOOD
fn get_something(&self) -> Option<Thing> {
    todo!("STUB(storage): load Thing from cache")
}
```

### Don't Use Placeholder Values

```rust
// BAD
fn calculate_timeout(&self) -> Duration {
    Duration::from_secs(30)  // placeholder
}

// GOOD
fn calculate_timeout(&self) -> Duration {
    todo!("STUB(config): calculate dynamic timeout from RTT")
}
```

### Don't Use Empty Implementations

```rust
// BAD
impl ShardOps for MyShard {
    fn apply_batch(&self, _batch: &Batch) -> Result<()> {
        Ok(())  // not implemented yet
    }
}

// GOOD
impl ShardOps for MyShard {
    fn apply_batch(&self, _batch: &Batch) -> Result<()> {
        todo!("STUB(storage): apply batch to WAL")
    }
}
```

## When Silent Stubs Are Acceptable

Only in tests, clearly marked:

```rust
#[cfg(test)]
mod tests {
    // Test double - intentionally returns empty
    struct MockShard;

    impl ShardOps for MockShard {
        fn get_entries(&self, _: u64) -> Vec<Entry> {
            vec![]  // Test stub - intentionally empty
        }
    }
}
```

## Stub Scanning (integration-validator)

The `integration-validator` agent automatically scans for stubs during validation:

```bash
# Part of validation
echo "=== Active Stubs ==="
rg "todo!\(\"STUB" --type rust -c || echo "No stubs found"

echo "=== Potential Silent Stubs ==="
rg "vec!\[\].*//|// TODO|// stub|// FIXME" --type rust -l || echo "none"
```

Stubs are reported as **warnings, not failures**. They're expected during development. The orchestrator uses this report to update SESSION_STATE.md and plan future phases.
