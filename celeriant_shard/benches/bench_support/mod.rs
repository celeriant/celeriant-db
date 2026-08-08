//! Shared construction helpers for the `celeriant_shard` write benchmarks.
//!
//! Extracted from `write_benchmark.rs` when `write_probe.rs` was added, so the two targets build
//! the same `ShardWal` from the same config rather than diverging copies.
#![allow(dead_code)]

use std::cell::Cell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use celeriant_msg::request::requests::{ReplicationBatchItem, SingleAggregateWrite, WriteRequest};
use celeriant_msg::response::responses::HeartbeatResult;
use celeriant_shard::error::replication_to_follower_error::ReplicateToFollowerError;
use celeriant_shard::error::replication_to_s3_error::ReplicateToS3Error;
use celeriant_shard::error::send_heartbeat_error::SendHeartbeatError;
use celeriant_shard::internal_shard_config::InternalShardConfig;
use celeriant_shard::replication_client::{ReplicationClient, StubReplicationClient};
use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

/// The segment size `write_benchmark.rs` has always used.
pub const SEGMENT_SIZE_BYTES: u64 = 128 * 1024 * 1024;

/// Aggregate namespace `celeriant_bench` writes into, so the probe's routing and key sizes
/// match the load generator every session 3-5 number was taken with.
pub const WORKLOAD_ORG: u128 = 1;
pub const WORKLOAD_AGG_TYPE: u128 = 1;

pub fn create_config(
    shard_dir: PathBuf,
    fsync_delay: Duration,
    recent_write_cache_bytes: u64,
) -> InternalShardConfig {
    create_config_with_preallocate(
        shard_dir,
        fsync_delay,
        recent_write_cache_bytes,
        SEGMENT_SIZE_BYTES,
    )
}

pub fn create_config_with_preallocate(
    shard_dir: PathBuf,
    fsync_delay: Duration,
    recent_write_cache_bytes: u64,
    shard_log_preallocate_bytes: u64,
) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        max_open_files: 256,
        shard_log_preallocate_bytes,
        fsync_delay,
        replication_delay: Duration::from_millis(17),
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::from_millis(500),
        heartbeat_starve_threshold: Duration::ZERO,
        recent_write_cache_bytes,
        shard_dir,
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes: 1024,
        timestamp_config: TimestampConfig::default(),
        list_max_duration: Duration::from_millis(2000),
        list_page_size: 20000,
        list_max_concurrent: 16,
        read_max_concurrent: 64,
        schema_cache_bytes: 4 * 1024 * 1024,
        max_schema_size_bytes: 16384,
        max_catchup_gap_bytes: Some(104_857_600),
        max_promotion_batch_bytes: None,
        max_clock_drift_ms: 500,
        shard_id: 1,
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_compaction"),
        cache_warmup_max_duration: Duration::MAX,
        wal_compression_level: 3,
        dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

/// Constant-byte events, the shape `write_benchmark.rs` measures.
pub fn create_events(count: usize, size: usize, base_index: u64) -> Vec<DatablockAggregateEvent> {
    (0..count)
        .map(|i| DatablockAggregateEvent {
            client_seq: base_index + i as u64,
            event_seq: 0,
            event_id: None,
            event_timestamp: 1_700_000_000_000 + i as u64,
            event_type_major: 1,
            event_type_minor: 0,
            event_value: Arc::new(vec![0xABu8; size]),
            iv: None,
        })
        .collect()
}

/// One event carrying **byte-identical bytes to `celeriant_bench`'s default payload**
/// (`celeriant_bench/src/lib.rs:359`): `format!("[t-{id}-r-{seq}] hello")`, ~22-25 ASCII bytes.
///
/// This matters more than it looks. `create_events` builds 1,280 bytes of constant `0xAB`, and
/// the path it feeds runs zstd-3 with a builtin dictionary, crc32c and blake3 — none of which
/// cost the same on a highly compressible constant buffer as on short ASCII. Every anchor number
/// in this campaign was taken with the string below.
pub fn workload_event(id: usize, seq: u64) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: 0,
        event_seq: 0,
        event_id: None,
        event_timestamp: 0,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(format!("[t-{id}-r-{seq}] hello").into_bytes()),
        iv: None,
    }
}

/// The same event shape as `workload_event`, but taking an already-built payload. The server
/// decodes its payload off the wire and never formats a string, so a probe that calls `format!`
/// inside the measured loop is charging layer 1 for harness work.
pub fn workload_event_with(payload: Arc<Vec<u8>>) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: 0,
        event_seq: 0,
        event_id: None,
        event_timestamp: 0,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: payload,
        iv: None,
    }
}

pub fn create_write_request(
    aggregate_key: AggregateKey,
    events: Vec<DatablockAggregateEvent>,
    client_id: u128,
) -> WriteRequest {
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key,
        SingleAggregateWrite {
            events,
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    WriteRequest {
        correlation_id: None,
        client_id,
        user_id: None,
        writes,
    }
}

/// Call counts for the four `ReplicationClient` methods, shared with the caller after the
/// client itself has been moved into `ShardWal`.
#[derive(Clone, Default)]
pub struct ReplicationCallCounts {
    pub follower: Rc<Cell<u64>>,
    pub s3: Rc<Cell<u64>>,
    pub heartbeat: Rc<Cell<u64>>,
    pub kick: Rc<Cell<u64>>,
}

impl ReplicationCallCounts {
    pub fn total(&self) -> u64 {
        self.follower.get() + self.s3.get() + self.heartbeat.get() + self.kick.get()
    }
}

/// `StubReplicationClient` with the calls counted, and **nothing else changed** — every method
/// delegates, so the 30 ms / 230 ms / 100 ms sleeps in the stub still happen if they are reached.
///
/// It exists to turn "layer 1 has no follower" from an assumption into evidence. The stub's
/// sleeps are large enough that a per-write call would dominate any measurement, but a per-batch
/// or once-at-open call would not be obvious in a wall-clock number — and a counter reading zero
/// settles both cases. Costs one `Cell` increment on a path that is supposed never to run.
pub struct CountingReplicationClient {
    inner: StubReplicationClient,
    counts: ReplicationCallCounts,
}

impl CountingReplicationClient {
    pub fn new(counts: ReplicationCallCounts) -> Self {
        Self { inner: StubReplicationClient, counts }
    }
}

impl ReplicationClient for CountingReplicationClient {
    fn set_follower_address(&self, address: Option<String>) {
        self.inner.set_follower_address(address);
    }
    fn set_follower_reachable(&self, reachable: bool) {
        self.inner.set_follower_reachable(reachable);
    }
    fn is_follower_reachable(&self) -> bool {
        self.inner.is_follower_reachable()
    }
    fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> {
        self.inner.current_heartbeat_started_at_unix_ms()
    }
    fn set_heartbeat_in_flight(&self, unix_ms: Option<u64>) {
        self.inner.set_heartbeat_in_flight(unix_ms);
    }
    fn reset_heartbeat_state(&self) {
        self.inner.reset_heartbeat_state();
    }

    async fn replicate_to_follower(
        &self,
        batches: Vec<ReplicationBatchItem>,
        leader_confirmed_wal_seq: u64,
        sender_lease_epoch: u64,
    ) -> Result<(), ReplicateToFollowerError> {
        self.counts.follower.set(self.counts.follower.get() + 1);
        self.inner
            .replicate_to_follower(batches, leader_confirmed_wal_seq, sender_lease_epoch)
            .await
    }

    async fn replicate_to_s3(
        &self,
        batches: Vec<ReplicationBatchItem>,
    ) -> Result<(), ReplicateToS3Error> {
        self.counts.s3.set(self.counts.s3.get() + 1);
        self.inner.replicate_to_s3(batches).await
    }

    async fn send_heartbeat(
        &self,
        unix_epoch_now_ms: u64,
        lease_epoch: u64,
    ) -> Result<HeartbeatResult, SendHeartbeatError> {
        self.counts.heartbeat.set(self.counts.heartbeat.get() + 1);
        self.inner.send_heartbeat(unix_epoch_now_ms, lease_epoch).await
    }

    async fn send_kick(&self) -> Result<bool, SendHeartbeatError> {
        self.counts.kick.set(self.counts.kick.get() + 1);
        self.inner.send_kick().await
    }
}
