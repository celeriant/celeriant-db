//! Blind oracle: the WAL crash-window recovery contract that Phase 4 (C6) must
//! not break.
//!
//! Phase 4 changed `sync()` from three serial `write_at`s (datablocks,
//! metablocks, dual header) to a joined datablocks+metablocks pair followed by
//! the header, then `fdatasync`. The durability invariant is that the dual
//! header must never become durable before the blocks it points at: boot CRCs
//! only the header block and the chain-tip rebuild scan is CRC-free, so a
//! header over unlanded blocks is unrecoverable, undetectable corruption.
//!
//! The contract below is flag-independent: it holds for the serial shape and
//! for the joined one. What CI actually runs is narrower — the config literal
//! in this file sets `wal_join_data_meta_writes` to the shipped default
//! (`true`), so these tests exercise the joined position only. The serial
//! position was verified by manually flipping that literal to `false` (both
//! positions green, 533 tests); nothing here enforces it, so a future change to
//! the serial path is not covered by this file unless the flip is repeated.
//!
//! The contracted state a crash may legally leave behind:
//!
//!   datablocks + metablocks landed, dual header did NOT
//!     => boot recovers to the PRIOR header cursor, serves exactly what that
//!        header covers, invents no tail, and stays writable.
//!
//! The crash window is simulated on disk (goal.md "Falsification depth": the
//! window itself is not exercised, the resulting states are). A shard writes
//! batch A, is closed, and its two durable header blocks are captured. It then
//! writes batch B — whose datablocks, metablocks AND header land — and is
//! dropped SIGKILL-style. Restoring A's header blocks over B's, leaving every
//! other byte of the segment untouched, reproduces exactly "B's data and
//! metadata are on disk, B's header is not".
//!
//! Segment layout used (verified against `celeriant_wal::shard_log_header` and
//! `celeriant_rotating_log::log_segment_file`):
//!   - primary header block: `[0, HEADER_BLOCK_SIZE_BYTES)`
//!   - backup header block:  `[file_len - HEADER_BLOCK_SIZE_BYTES, file_len)`
//!   - metablocks grow up from `HEADER_BLOCK_SIZE_BYTES`, datablocks grow down
//!     from `file_len - HEADER_BLOCK_SIZE_BYTES`
//!   - segment path: `<shard_dir>/log_1.wal`, `file_len` == the preallocated size

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest};
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_msg::response::responses::ReadResponse;
use celeriant_rotating_log::errors::ready_up_error::ReadyUpError;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::shard_log_header::ShardLogHeader;
use celeriant_wire::disk::versioned_block::deserialise_shard_log_header;
use glommio::{LocalExecutorBuilder, Placement};

use crate::internal_shard_config::InternalShardConfig;
use crate::replication_client::StubReplicationClient;
use crate::s3_downloader::StubS3Downloader;
use crate::shard_wal::ShardWal;
use crate::timestamp_config::TimestampConfig;

type TestShard = ShardWal<StubReplicationClient, StubS3Downloader>;

const PREALLOCATE_BYTES: u64 = 4 * 1024 * 1024;

const TAG_A: u8 = 0xA1;
const TAG_B: u8 = 0xB2;
const TAG_C: u8 = 0xC3;

/// Per-event payload size. Large and incompressible so each batch is well over
/// `MINIBATCH_SIZE_BYTES` after zstd and therefore takes the datablock path —
/// a batch that fits a metablock minibatch would never exercise the data+meta
/// pair the join changes.
const EVENT_VALUE_BYTES: usize = 2048;

macro_rules! glommio_test {
    ($body:expr) => {
        LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move { $body })
            .unwrap()
            .join()
            .unwrap()
    };
}

fn test_dir() -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("shard");
    (tmp, dir)
}

fn test_config(dir: &Path) -> InternalShardConfig {
    InternalShardConfig {
        wal_join_data_meta_writes: true,
        node_id: 1,
        shard_id: 1,
        max_open_files: 4,
        shard_log_preallocate_bytes: PREALLOCATE_BYTES,
        fsync_delay: Duration::ZERO,
        replication_delay: Duration::ZERO,
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::ZERO,
        heartbeat_starve_threshold: Duration::ZERO,
        recent_write_cache_bytes: 64 * 1024 * 1024,
        shard_dir: dir.to_path_buf(),
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        negative_lookup_cache_bytes: 2 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes: 1024,
        timestamp_config: TimestampConfig::default(),
        list_page_size: 100,
        list_max_concurrent: 16,
        list_max_duration: Duration::from_secs(2),
        schema_cache_bytes: 4 * 1024 * 1024,
        max_schema_size_bytes: 16384,
        max_catchup_gap_bytes: Some(100 * 1024 * 1024),
        max_promotion_batch_bytes: None,
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: PathBuf::from("/tmp/test_compaction"),
        max_clock_drift_ms: 500,
        read_max_concurrent: 64,
        cache_warmup_max_duration: Duration::MAX,
        wal_compression_level: 3,
        dict_bytes: Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

async fn try_open_shard(dir: &Path) -> Result<TestShard, ReadyUpError> {
    ShardWal::open(
        test_config(dir),
        ValidatedNodeStatus::create_standalone(),
        StubReplicationClient,
        StubS3Downloader,
    )
    .await
}

async fn open_shard(dir: &Path) -> TestShard {
    try_open_shard(dir).await.expect("shard must open")
}

fn key(org: u128, atype: u128, id: u128) -> AggregateKey {
    AggregateKey::new(org, atype, id)
}

/// Deterministic, incompressible payload: xorshift bytes behind a leading tag
/// byte, so a mis-served batch is identifiable from its first byte and byte
/// equality is meaningful.
fn event_value(tag: u8, client_seq: u64) -> Vec<u8> {
    let mut state = ((tag as u64) << 56) | client_seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = Vec::with_capacity(EVENT_VALUE_BYTES + 8);
    out.push(tag);
    while out.len() < EVENT_VALUE_BYTES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(EVENT_VALUE_BYTES);
    out
}

fn batch(tag: u8, base_seq: u64, count: u64) -> Vec<DatablockAggregateEvent> {
    (0..count)
        .map(|i| {
            let client_seq = base_seq + i;
            DatablockAggregateEvent {
                client_seq,
                event_type_major: 1,
                event_value: Arc::new(event_value(tag, client_seq)),
                ..Default::default()
            }
        })
        .collect()
}

fn write_req(agg: AggregateKey, evts: Vec<DatablockAggregateEvent>) -> ClientRequest {
    let mut writes = HashMap::new();
    writes.insert(
        agg,
        SingleAggregateWrite {
            events: evts,
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );
    ClientRequest::Write(WriteRequest {
        correlation_id: None,
        client_id: 1,
        user_id: None,
        writes,
    })
}

fn read_req(agg: AggregateKey) -> ClientRequest {
    ClientRequest::Read(ReadRequest {
        correlation_id: None,
        aggregate_key: agg,
        filters: ReadFilters::new(0),
    })
}

/// `fsync_delay` is ZERO, so a successful write response means the batch is
/// fsynced: datablocks, metablocks and both header copies are durable.
async fn write_durable(shard: &TestShard, agg: &AggregateKey, tag: u8, base_seq: u64, count: u64) {
    let result = shard
        .process_client_request(write_req(agg.clone(), batch(tag, base_seq, count)))
        .await;
    assert!(
        matches!(result, Ok(ClientResponse::Write(_))),
        "write of batch {tag:#04x} must be acked (ack == durable): {:?}",
        result.err()
    );
}

async fn read_all(shard: &TestShard, agg: &AggregateKey) -> ReadResponse {
    match shard.process_client_request(read_req(agg.clone())).await {
        Ok(ClientResponse::Read(r)) => r,
        other => panic!("read after recovery must succeed, got {other:?}"),
    }
}

fn assert_batch(got: &AggregateEventBatch, version: u64, tag: u8, base_seq: u64, count: u64, what: &str) {
    assert_eq!(got.aggregate_version, version, "{what}: wrong aggregate_version");
    assert_eq!(got.events.len() as u64, count, "{what}: wrong event count");
    for (i, event) in got.events.iter().enumerate() {
        let client_seq = base_seq + i as u64;
        assert_eq!(
            event.event_value.first().copied(),
            Some(tag),
            "{what}: event {i} carries batch tag {:?}, expected {tag:#04x}",
            event.event_value.first()
        );
        assert_eq!(event.client_seq, client_seq, "{what}: event {i} wrong client_seq");
        assert_eq!(
            event.event_value.as_slice(),
            event_value(tag, client_seq).as_slice(),
            "{what}: event {i} payload differs byte-for-byte"
        );
    }
}

// ── on-disk manipulation ──

fn wal_path(dir: &Path) -> PathBuf {
    let path = dir.join("log_1.wal");
    assert!(path.exists(), "expected active segment at {}", path.display());
    path
}

fn backup_header_offset(path: &Path) -> u64 {
    let file_len = std::fs::metadata(path).unwrap().len();
    assert_eq!(file_len, PREALLOCATE_BYTES, "segment is preallocated to a fixed length");
    file_len - HEADER_BLOCK_SIZE_BYTES as u64
}

fn read_region(path: &Path, offset: u64, len: usize) -> Vec<u8> {
    let mut f = std::fs::File::open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf).unwrap();
    buf
}

/// `(primary @ 0, backup @ file_len - HEADER_BLOCK_SIZE_BYTES)`.
fn header_copies(path: &Path) -> (Vec<u8>, Vec<u8>) {
    (
        read_region(path, 0, HEADER_BLOCK_SIZE_BYTES),
        read_region(path, backup_header_offset(path), HEADER_BLOCK_SIZE_BYTES),
    )
}

/// `sync_all` matters: the shard reopens the segment with O_DIRECT, which must
/// not race dirty page cache from this buffered write.
fn write_region(path: &Path, offset: u64, bytes: &[u8]) {
    let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
}

fn parse_header(block: &[u8], what: &str) -> ShardLogHeader {
    deserialise_shard_log_header(block).unwrap_or_else(|e| panic!("{what} must parse: {e:?}"))
}

/// The metablock+datablock span between the two header blocks — everything a
/// header-only restore must leave untouched.
fn block_region(path: &Path) -> Vec<u8> {
    let start = HEADER_BLOCK_SIZE_BYTES as u64;
    let end = backup_header_offset(path);
    read_region(path, start, (end - start) as usize)
}

/// Writes A, closes cleanly, captures A's durable header copies; then writes B
/// and dies without close. Returns A's `(primary, backup)` header blocks.
async fn write_a_capture_header_then_write_b(dir: &Path, agg: &AggregateKey) -> (Vec<u8>, Vec<u8>) {
    {
        let shard = open_shard(dir).await;
        write_durable(&shard, agg, TAG_A, 1, 3).await;
        shard.close().await;
    }
    let path = wal_path(dir);
    let header_a = header_copies(&path);

    {
        let shard = open_shard(dir).await;
        write_durable(&shard, agg, TAG_B, 11, 2).await;
        // SIGKILL shape: no graceful close. Disk holds B's datablocks,
        // metablocks and header, all fsynced.
        drop(shard);
    }

    let header_b = header_copies(&path);
    let cursor_a = parse_header(&header_a.0, "header after A").write;
    let cursor_b = parse_header(&header_b.0, "header after B").write;
    assert!(
        cursor_b.wal_seq > cursor_a.wal_seq,
        "non-vacuity: B must have advanced the header wal_seq ({} -> {})",
        cursor_a.wal_seq,
        cursor_b.wal_seq
    );
    assert!(
        cursor_b.metablocks_position > cursor_a.metablocks_position,
        "non-vacuity: B must have written metablocks"
    );
    assert!(
        cursor_b.datablocks_position < cursor_a.datablocks_position,
        "non-vacuity: B must have written datablocks (a minibatch-only B would not exercise the data+meta pair)"
    );

    header_a
}

// ── Scenario 1: both header copies stale — data+meta landed, header did not ──

/// The exact Phase 4 hazard state. B's datablocks and metablocks are on disk;
/// neither header copy points at them. Boot must land on A's cursor: A's events
/// exactly, B invisible, no error, no invented tail — and the recovered cursor
/// must be usable, with the next batch (physically overwriting B's orphaned
/// blocks) landing and reading back clean.
#[test]
fn boot_recovers_prior_header_cursor_when_data_and_meta_landed_but_header_did_not() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let agg = key(1, 1, 1);

        let (header_a_primary, header_a_backup) = write_a_capture_header_then_write_b(&dir, &agg).await;
        let path = wal_path(&dir);
        let blocks_with_b = block_region(&path);

        // Crash window: roll BOTH header copies back to A, touch nothing else.
        write_region(&path, 0, &header_a_primary);
        write_region(&path, backup_header_offset(&path), &header_a_backup);

        assert_eq!(
            block_region(&path),
            blocks_with_b,
            "setup invalid: B's datablocks/metablocks must still be on disk — only the header blocks may change"
        );

        let shard = try_open_shard(&dir)
            .await
            .expect("boot over a stale header with newer unreferenced blocks must recover, not error");

        let read = read_all(&shard, &agg).await;
        assert_eq!(
            read.event_batches.len(),
            1,
            "must serve exactly what A's header covers — headerless blocks are invisible and no tail is invented"
        );
        assert_batch(&read.event_batches[0], 1, TAG_A, 1, 3, "recovered batch A");

        // The recovered cursor is usable, and C may physically reuse B's space.
        write_durable(&shard, &agg, TAG_C, 21, 4).await;
        let read = read_all(&shard, &agg).await;
        assert_eq!(read.event_batches.len(), 2, "post-recovery write must append to A, not to a phantom B");
        assert_batch(&read.event_batches[0], 1, TAG_A, 1, 3, "batch A after post-recovery write");
        assert_batch(&read.event_batches[1], 2, TAG_C, 21, 4, "batch C written after recovery");

        shard.close().await;

        // And it survives another restart: the recovered tail is durable, not
        // just an in-memory reconstruction.
        let shard = try_open_shard(&dir).await.expect("second boot after recovery must succeed");
        let read = read_all(&shard, &agg).await;
        assert_eq!(read.event_batches.len(), 2, "restart after recovery must serve A then C");
        assert_batch(&read.event_batches[0], 1, TAG_A, 1, 3, "batch A after restart");
        assert_batch(&read.event_batches[1], 2, TAG_C, 21, 4, "batch C after restart");
        shard.close().await;
    });
}

// ── Scenario 2: torn dual-header write — only the primary copy is stale ──

/// Half of the dual header survived the crash. Only the primary copy (offset 0)
/// is rolled back to A; the backup still describes B.
/// `load_header_detecting_corruption` reads offset 0 first and falls back to the
/// tail copy only when the front fails to deserialise, so the stale-but-valid
/// primary is the one that wins and the state recovered is A's — same contract
/// as scenario 1. If that loader order ever changes, relax the count assertion,
/// not the "boots and serves a consistent, uninvented prefix" one.
#[test]
fn boot_recovers_consistent_state_when_only_one_header_copy_is_stale() {
    glommio_test!({
        let (_tmp, dir) = test_dir();
        let agg = key(1, 1, 1);

        let (header_a_primary, _header_a_backup) = write_a_capture_header_then_write_b(&dir, &agg).await;
        let path = wal_path(&dir);
        let blocks_with_b = block_region(&path);

        // Torn dual-header write: primary reverted to A, backup still B's.
        write_region(&path, 0, &header_a_primary);

        let (primary, backup) = header_copies(&path);
        let primary_cursor = parse_header(&primary, "reverted primary header").write;
        let backup_cursor = parse_header(&backup, "surviving backup header").write;
        assert!(
            backup_cursor.wal_seq > primary_cursor.wal_seq,
            "setup invalid: the two header copies must now disagree (torn write)"
        );
        assert_eq!(
            block_region(&path),
            blocks_with_b,
            "setup invalid: only the primary header block may have changed"
        );

        let shard = try_open_shard(&dir)
            .await
            .expect("boot over a torn dual header must recover, not error");

        let read = read_all(&shard, &agg).await;
        assert_eq!(
            read.event_batches.len(),
            1,
            "primary header block wins (loader reads offset 0 first), so only A is visible"
        );
        assert_batch(&read.event_batches[0], 1, TAG_A, 1, 3, "recovered batch A after torn header");

        write_durable(&shard, &agg, TAG_C, 21, 4).await;
        let read = read_all(&shard, &agg).await;
        assert_eq!(read.event_batches.len(), 2, "the recovered cursor must be writable after a torn header");
        assert_batch(&read.event_batches[0], 1, TAG_A, 1, 3, "batch A after torn-header recovery write");
        assert_batch(&read.event_batches[1], 2, TAG_C, 21, 4, "batch C after torn-header recovery");

        shard.close().await;
    });
}
