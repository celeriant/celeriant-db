# Evaluation: raft-rs Integration with Glommio-based Celeriant

## Executive Summary

**Integration is possible without forking**, but requires careful architectural decisions. The main challenges are:

1. raft-rs's `Storage` trait is synchronous, while glommio requires async I/O
2. raft-rs uses protobuf for messages, not your bincode/msgpack format
3. raft-rs is tick-driven, requiring periodic timer callbacks

None of these are blockers—they're design constraints you work around.

---

## Detailed Analysis

### 1. Runtime Compatibility (glommio)

**Good news**: raft-rs's `RawNode` is explicitly single-threaded:

```rust
// From raft/src/raw_node.rs
/// RawNode is a thread-unsafe Node.
pub struct RawNode<T: Storage> {
    pub raft: Raft<T>,
    // ...
}
```

This aligns perfectly with glommio's thread-per-core model. Each shard executor can own its own `RawNode` without synchronization.

**Challenge**: The `Storage` trait methods are synchronous:

```rust
pub trait Storage {
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>, 
               context: GetEntriesContext) -> Result<Vec<Entry>>;
    fn term(&self, idx: u64) -> Result<u64>;
    fn first_index(&self) -> Result<u64>;
    fn last_index(&self) -> Result<u64>;
    fn snapshot(&self, request_index: u64, to: u64) -> Result<Snapshot>;
}
```

You cannot `await` inside these methods. However, raft-rs provides an async escape hatch:

```rust
// From raft/src/storage.rs
pub fn can_async(&self) -> bool {
    match self.0 {
        GetEntriesFor::SendAppend { .. } => true,
        GetEntriesFor::Empty(can_async) => can_async,
        _ => false,
    }
}
```

When `context.can_async()` is true, you can return `StorageError::LogTemporarilyUnavailable`, then call `on_entries_fetched(context)` later when data is ready.

**Solution**: Implement a memory-backed `Storage` with async background persistence:

```rust
// Conceptual structure
pub struct RaftStorage {
    // Hot data - serves synchronous reads
    entries_cache: RefCell<BTreeMap<u64, Entry>>,
    hard_state: RefCell<HardState>,
    conf_state: RefCell<ConfState>,
    
    // Pending async fetches
    pending_fetches: RefCell<HashMap<u64, GetEntriesContext>>,
    
    // Reference to your existing WAL for persistence
    wal: Rc<ShardWriteAheadLog>,
}

impl Storage for RaftStorage {
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>, 
               context: GetEntriesContext) -> Result<Vec<Entry>> {
        // Try cache first
        if let Some(entries) = self.try_get_from_cache(low, high, max_size) {
            return Ok(entries);
        }
        
        // Not in cache - if async allowed, defer
        if context.can_async() {
            self.schedule_async_fetch(low, high, context);
            return Err(Error::Store(StorageError::LogTemporarilyUnavailable));
        }
        
        // Must have data - this is a bug if we reach here
        Err(Error::Store(StorageError::Unavailable))
    }
    // ...
}
```

### 2. WAL Integration

Your existing `ShardWriteAheadLog` stores event batches per aggregate. Raft needs a linear log of `Entry` records. These are different abstractions:

| Celeriant WAL | Raft Log |
|---------------|----------|
| Per-aggregate event batches | Global linear entry sequence |
| `EventBatchItem` with events | `Entry` with opaque `data` bytes |
| Indexed by `(AggregateKey, batch_index)` | Indexed by `log_index` |

**Integration approach**: Create a separate raft log that coexists with your event WAL:

```
data_root/
├── shard_0/
│   ├── log_1.wal           # Your existing event WAL
│   ├── log_2.wal
│   └── raft/               # New: Raft consensus log
│       ├── raft_log.wal
│       └── raft_meta.wal
├── shard_1/
│   └── ...
```

The raft log stores *proposals* (write requests), not the final events. Once committed, proposals are applied to your event WAL.

### 3. Wire Format

**raft-rs uses protobuf** for all messages:

```rust
// From raft/src/lib.rs
pub use raft_proto::eraftpb;  // protobuf-generated types

// Message types include:
// - MsgAppend, MsgAppendResponse
// - MsgRequestVote, MsgRequestVoteResponse  
// - MsgHeartbeat, MsgHeartbeatResponse
// - MsgSnapshot
// - etc.
```

**You have three options**:

1. **Use protobuf directly** (recommended): Add raft messages as a new message type in your protocol
2. **Wrap protobuf in your format**: Serialize protobuf bytes, then wrap in bincode/msgpack
3. **Re-serialize to your format**: Convert protobuf structs to serde structs (wasteful)

**Option 1 is cleanest**. Extend your wire header to distinguish raft messages:

```rust
// Extend your message types
pub enum RequestType {
    // Existing
    Write = 1,
    Read = 2,
    // ...
    
    // New: Raft consensus
    RaftMessage = 100,
}

// Raft messages use protobuf encoding, not bincode
impl Request {
    pub async fn read_request<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, WireError> {
        let header = WireHeader::from_reader(reader).await?;
        
        match header.request_type {
            RequestType::RaftMessage => {
                // Read protobuf-encoded raft message
                let raft_msg = read_protobuf_message(reader, header.payload_len).await?;
                Ok(Request::Raft(raft_msg))
            }
            _ => {
                // Existing bincode/msgpack path
            }
        }
    }
}
```

### 4. Tick-Based Model

raft-rs requires periodic `tick()` calls to drive elections and heartbeats:

```rust
// From raft/src/config.rs
pub struct Config {
    pub election_tick: usize,   // Ticks before election timeout
    pub heartbeat_tick: usize,  // Ticks between heartbeats
    // ...
}
```

**Integration with glommio**:

```rust
fn spawn_raft_ticker(raft_node: Rc<RefCell<RawNode<RaftStorage>>>, tick_interval: Duration) {
    glommio::spawn_local(async move {
        loop {
            glommio::timer::sleep(tick_interval).await;
            
            let has_ready = raft_node.borrow_mut().tick();
            if has_ready {
                // Process Ready state
                process_raft_ready(&raft_node).await;
            }
        }
    }).detach();
}
```

### 5. Message Transport

raft-rs gives you `Message` structs to send; you handle transport:

```rust
// From RawNode::ready()
let ready = node.ready();

// Messages to send to other nodes
for msg in ready.take_messages() {
    send_to_peer(msg.to, msg).await;  // Your transport
}

// Persisted messages (send after persistence)
for msg in ready.take_persisted_messages() {
    send_to_peer(msg.to, msg).await;
}
```

This fits your existing TCP architecture—you already have inter-shard communication via `IntrashardMessages`.

---

## Fork Assessment

### Is a Fork Required?

**No.** All integration challenges can be addressed through:

1. Memory-backed `Storage` implementation with async background I/O
2. Adding protobuf message support to your wire protocol
3. Proper glommio task structure for ticking and message handling

### What Would a Fork Enable?

If you did fork, you could:

| Change | Benefit | Complexity |
|--------|---------|------------|
| Async `Storage` trait | Native `async fn entries()` | High - touches core abstractions |
| Replace protobuf with serde | Use your existing format | Medium - many message types |
| Remove `slog` dependency | Use `tracing` consistently | Low - straightforward replacement |
| Optimize for single-shard | Remove some distributed checks | Medium - careful analysis needed |

**Recommendation**: Start without forking. Fork only if you hit performance issues that can't be solved otherwise.

---

## Architecture Proposal

```
┌─────────────────────────────────────────────────────────────────┐
│                         Shard Executor                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │  TCP Handler │───▶│ RaftNode     │───▶│ RaftStorage      │  │
│  │  (requests)  │    │ (consensus)  │    │ (memory + async) │  │
│  └──────────────┘    └──────────────┘    └──────────────────┘  │
│         │                   │                     │             │
│         │                   │                     ▼             │
│         │                   │            ┌──────────────────┐  │
│         │                   │            │ RaftLog (disk)   │  │
│         │                   │            │ (proposals)      │  │
│         │                   │            └──────────────────┘  │
│         │                   │                                   │
│         │                   ▼ (committed entries)               │
│         │            ┌──────────────────┐                      │
│         └───────────▶│ ShardWriteAhead  │                      │
│                      │ Log (events)     │                      │
│                      └──────────────────┘                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Data flow**:

1. Client sends `WriteRequest` 
2. Request is proposed to Raft as an `Entry`
3. Raft replicates to quorum
4. Once committed, `WriteRequest` is applied to `ShardWriteAheadLog`
5. Response sent to client

---

## Task Breakdown

### Phase 1: Foundation (1-2 weeks)

| Task | Description | Estimate |
|------|-------------|----------|
| 1.1 | Add `raft-rs` and `raft-proto` dependencies | 2h |
| 1.2 | Implement `RaftStorage` with in-memory cache | 2d |
| 1.3 | Create raft log file format (separate from event WAL) | 1d |
| 1.4 | Implement persistence for `HardState` and `ConfState` | 1d |
| 1.5 | Add protobuf message type to wire protocol | 4h |
| 1.6 | Unit tests for `RaftStorage` trait implementation | 1d |

### Phase 2: Single-Node Integration (1 week)

| Task | Description | Estimate |
|------|-------------|----------|
| 2.1 | Create `RaftNode` wrapper with glommio lifecycle | 1d |
| 2.2 | Implement tick timer task | 2h |
| 2.3 | Implement `Ready` processing loop | 1d |
| 2.4 | Connect proposals to existing `WriteRequest` flow | 1d |
| 2.5 | Apply committed entries to `ShardWriteAheadLog` | 1d |
| 2.6 | Integration tests: single node write/read | 1d |

### Phase 3: Multi-Node Replication (2 weeks)

| Task | Description | Estimate |
|------|-------------|----------|
| 3.1 | Design cluster membership configuration | 4h |
| 3.2 | Implement peer-to-peer message transport | 2d |
| 3.3 | Handle `MsgAppend`, `MsgHeartbeat` messages | 1d |
| 3.4 | Handle `MsgRequestVote`, election flow | 1d |
| 3.5 | Implement snapshot sending/receiving | 2d |
| 3.6 | Add leader redirection for client requests | 1d |
| 3.7 | Integration tests: 3-node cluster | 2d |
| 3.8 | Chaos testing: node failures, network partitions | 2d |

### Phase 4: Production Hardening (1-2 weeks)

| Task | Description | Estimate |
|------|-------------|----------|
| 4.1 | Log compaction and snapshotting | 2d |
| 4.2 | Dynamic membership changes (add/remove nodes) | 2d |
| 4.3 | Metrics and observability | 1d |
| 4.4 | Leader lease optimization for reads | 1d |
| 4.5 | Backpressure for slow followers | 1d |
| 4.6 | Recovery testing after crashes | 2d |

---

## Key Implementation Details

### RaftStorage Implementation Skeleton

```rust
use raft::{prelude::*, Storage, RaftState, GetEntriesContext};
use raft::eraftpb::{Entry, Snapshot, HardState, ConfState};
use std::cell::RefCell;
use std::rc::Rc;

pub struct RaftStorage {
    // In-memory state (serves synchronous reads)
    hard_state: RefCell<HardState>,
    conf_state: RefCell<ConfState>,
    entries: RefCell<Vec<Entry>>,  // Ring buffer or BTreeMap
    snapshot: RefCell<Snapshot>,
    
    // For async fetch pattern
    pending_contexts: RefCell<Vec<GetEntriesContext>>,
    
    // Disk persistence (your existing infrastructure)
    raft_log_file: Rc<RefCell<DmaFile>>,
}

impl Storage for RaftStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state.borrow().clone(),
            conf_state: self.conf_state.borrow().clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let max_size = max_size.into();
        let entries = self.entries.borrow();
        
        // Calculate offset from first entry
        let first = self.first_index()?;
        if low < first {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        
        let offset = (low - first) as usize;
        let end = ((high - first) as usize).min(entries.len());
        
        if offset >= entries.len() {
            // Need to fetch from disk
            if context.can_async() {
                self.pending_contexts.borrow_mut().push(context);
                return Err(raft::Error::Store(raft::StorageError::LogTemporarilyUnavailable));
            }
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }
        
        // Serve from memory
        let mut result: Vec<Entry> = entries[offset..end].to_vec();
        raft::util::limit_size(&mut result, max_size);
        Ok(result)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let entries = self.entries.borrow();
        let first = self.first_index()?;
        
        if idx < first {
            let snap = self.snapshot.borrow();
            if idx == snap.get_metadata().index {
                return Ok(snap.get_metadata().term);
            }
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        
        let offset = (idx - first) as usize;
        if offset >= entries.len() {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }
        
        Ok(entries[offset].term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        let snap = self.snapshot.borrow();
        Ok(snap.get_metadata().index + 1)
    }

    fn last_index(&self) -> raft::Result<u64> {
        let entries = self.entries.borrow();
        let snap = self.snapshot.borrow();
        Ok(snap.get_metadata().index + entries.len() as u64)
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let snap = self.snapshot.borrow();
        if snap.get_metadata().index < request_index {
            return Err(raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable));
        }
        Ok(snap.clone())
    }
}
```

### Ready Processing Loop

```rust
async fn process_raft_ready(
    raft_node: &Rc<RefCell<RawNode<RaftStorage>>>,
    storage: &Rc<RaftStorage>,
    peers: &HashMap<u64, TcpStream>,
    event_wal: &Rc<ShardWriteAheadLog>,
) -> Result<(), RaftError> {
    if !raft_node.borrow().has_ready() {
        return Ok(());
    }

    let mut ready = raft_node.borrow_mut().ready();

    // 1. Send messages (can be done before persistence for leader)
    for msg in ready.take_messages() {
        send_raft_message(&peers, msg).await?;
    }

    // 2. Persist snapshot if present
    if !ready.snapshot().is_empty() {
        storage.apply_snapshot(ready.snapshot().clone())?;
    }

    // 3. Persist entries
    if !ready.entries().is_empty() {
        storage.append_entries(ready.entries()).await?;
    }

    // 4. Persist hard state
    if let Some(hs) = ready.hs() {
        storage.set_hard_state(hs.clone()).await?;
    }

    // 5. Send persisted messages
    for msg in ready.take_persisted_messages() {
        send_raft_message(&peers, msg).await?;
    }

    // 6. Apply committed entries to your event WAL
    for entry in ready.take_committed_entries() {
        if entry.data.is_empty() {
            // Configuration change or empty entry
            continue;
        }
        
        // Deserialize the original WriteRequest
        let write_request: WriteRequest = deserialize_proposal(&entry.data)?;
        
        // Apply to your event WAL
        event_wal.write(entry.index, write_request).await?;
    }

    // 7. Advance the raft state machine
    let mut light_rd = raft_node.borrow_mut().advance(ready);
    
    // 8. Handle light ready (more committed entries, messages)
    for msg in light_rd.take_messages() {
        send_raft_message(&peers, msg).await?;
    }
    
    for entry in light_rd.take_committed_entries() {
        if !entry.data.is_empty() {
            let write_request: WriteRequest = deserialize_proposal(&entry.data)?;
            event_wal.write(entry.index, write_request).await?;
        }
    }
    
    raft_node.borrow_mut().advance_apply();

    Ok(())
}
```

---

## Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Memory pressure from entry cache | Medium | Implement compaction, tune cache size |
| Async fetch complexity | Medium | Start with generous cache, add async fetch later |
| protobuf version conflicts | Low | Pin versions, use workspace dependencies |
| Performance regression | Medium | Benchmark before/after, profile hot paths |
| Split-brain scenarios | Low | Thorough chaos testing, proper quorum configuration |

---

## Conclusion

raft-rs can be integrated with your glommio-based Celeriant without forking. The main work is:

1. **Storage adapter**: Memory-backed with async persistence to your WAL
2. **Wire protocol extension**: Add protobuf message type for raft
3. **Glommio integration**: Timer tasks and proper async boundaries

The architecture fits well—raft-rs's single-threaded `RawNode` aligns with glommio's thread-per-core model, and the callback-based async pattern can be adapted to work with glommio's executor.