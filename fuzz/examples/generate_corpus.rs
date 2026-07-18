//! Seed-corpus generator. Uses the real *serialise* paths (not hand-rolled
//! bytes) to produce valid encoded inputs for each harness, so AFL starts
//! from something the decoder actually accepts instead of wasting cycles
//! rediscovering the wire format from scratch.
//!
//! Run with: `cargo run --example generate_corpus`

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
use celeriant_wal::constants::{
    AGGREGATE_BLOOM_BYTES, CLIENT_BLOOM_BYTES, FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES,
    WIRE_VERSION_S3_FALLBACK_BATCH, WIRE_VERSION_SEGMENT_SUMMARY_BLOCK, WIRE_VERSION_WAL_METABLOCK,
    WIRE_VERSION_WAL_SHARD_LOG_HEADER,
};
use celeriant_wal::datablocks::datablock::Datablock;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
use celeriant_wal::datablocks::datablock_kind::DatablockKind;
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::s3::fallback_batch::FallbackBatch;
use celeriant_wal::segment_summary::{SegmentSummaryBlock, SegmentSummaryPayload};
use celeriant_wal::shard_log_header::{HeaderCursor, ShardLogHeader};
use celeriant_wire::codec::bincode::{fixed_serialise_heap, fixed_serialise_stack};
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::disk::serialised_datablock::{CompressionPolicy, SerialisedDatablock};
use celeriant_wire::disk::versioned_block::{serialize_versioned_message, HEADER_SIZE};
use celeriant_wire::network::wire_header::{
    wire_header_write_fixed_size, wire_header_write_variable_size_uncompressed,
    wire_header_write_variable_size_with_codec, PROTOCOL_VERSION_V2,
};
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};

fn write_seed(dir: &Path, name: &str, bytes: &[u8]) {
    fs::create_dir_all(dir).unwrap();
    fs::write(dir.join(name), bytes).unwrap();
}

fn sample_metablock() -> Metablock {
    Metablock::default_inline_event_batch_metadata(AggregateKey::new(1, 2, 3))
}

fn sample_datablock() -> Datablock {
    Datablock {
        datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
            aggregate_version: 1,
            events: vec![DatablockAggregateEvent {
                client_seq: 1,
                event_seq: 1,
                event_id: Some(42),
                event_timestamp: 1000,
                event_type_major: 1,
                event_type_minor: 0,
                event_value: Arc::new(vec![1, 2, 3, 4]),
                iv: None,
            }],
        }),
    }
}

/// A properly-sized `ShardLogHeader`, matching what `setup_new_file`
/// (celeriant_rotating_log/src/log_segment_file/log_segment_file.rs) actually persists:
/// full-width (not empty) bloom vectors, one whole number of SBBF blocks each.
fn sample_shard_log_header() -> ShardLogHeader {
    let cursor = HeaderCursor {
        metablocks_position: HEADER_BLOCK_SIZE_BYTES as u64,
        datablocks_position: 4 * 1024 * 1024,
        wal_seq: 7,
        tip_hash: [0x42u8; 32],
    };
    ShardLogHeader {
        write: cursor.clone(),
        aggregate_bloom: vec![0u64; AGGREGATE_BLOOM_BYTES / 8],
        client_bloom: vec![0u64; CLIENT_BLOOM_BYTES / 8],
        last_received_replication_wal_seq: 0,
        last_self_acked_wal_seq: 0,
        read: cursor,
    }
}

fn sample_write_request() -> WriteRequest {
    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(1, 2, 3),
        SingleAggregateWrite {
            events: vec![DatablockAggregateEvent {
                client_seq: 1,
                event_seq: 1,
                event_id: None,
                event_timestamp: 1000,
                event_type_major: 1,
                event_type_minor: 0,
                event_value: Arc::new(vec![9, 9, 9]),
                iv: None,
            }],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );
    WriteRequest {
        correlation_id: Some(1),
        client_id: 42,
        user_id: None,
        writes,
    }
}

fn gen_bincode_decode(root: &Path) {
    let dir = root.join("bincode_decode");
    let mb = fixed_serialise_heap(&sample_metablock()).unwrap();
    let db = fixed_serialise_heap(&sample_datablock()).unwrap();
    let wr = fixed_serialise_heap(&sample_write_request()).unwrap();

    let mut seed0 = vec![0u8];
    seed0.extend_from_slice(&mb);
    write_seed(&dir, "metablock.bin", &seed0);

    let mut seed1 = vec![1u8];
    seed1.extend_from_slice(&db);
    write_seed(&dir, "datablock.bin", &seed1);

    let mut seed2 = vec![2u8];
    seed2.extend_from_slice(&wr);
    write_seed(&dir, "write_request.bin", &seed2);
}

fn gen_wire_header(root: &Path) {
    let dir = root.join("wire_header");
    let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile");

    futures_lite::future::block_on(async {
        // Fixed-size frame.
        let mut buf = Vec::new();
        wire_header_write_fixed_size(&mut buf, &12345u64, 1, PROTOCOL_VERSION_V2)
            .await
            .unwrap();
        write_seed(&dir, "fixed.bin", &buf);

        // Variable uncompressed frame, big enough to skip the small-payload fast path.
        let mut buf = Vec::new();
        let payload = vec![7u8; 2000];
        wire_header_write_variable_size_uncompressed(&mut buf, &payload, 1, 1 << 20, PROTOCOL_VERSION_V2)
            .await
            .unwrap();
        write_seed(&dir, "variable_uncompressed.bin", &buf);

        // Variable frame compressed with the builtin dict.
        let mut buf = Vec::new();
        let payload = vec![9u8; 2000];
        wire_header_write_variable_size_with_codec(
            &mut buf,
            &payload,
            1,
            CompressionType::ZstdDict,
            1 << 20,
            PROTOCOL_VERSION_V2,
            &codec,
        )
        .await
        .unwrap();
        write_seed(&dir, "variable_zstd_dict.bin", &buf);

        // Small payload, no compression - exercises the stack fast path.
        let mut buf = Vec::new();
        let payload = vec![1u8, 2, 3];
        wire_header_write_variable_size_uncompressed(&mut buf, &payload, 1, 1 << 20, PROTOCOL_VERSION_V2)
            .await
            .unwrap();
        write_seed(&dir, "small_uncompressed.bin", &buf);
    });
}

fn gen_serialised_datablock(root: &Path) {
    let dir = root.join("serialised_datablock");
    let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile");

    // Small datablock -> inlines uncompressed. Reformat into the harness's
    // [compressed_size:u64][uncompressed_size:u64][compression_type:u8][minibatch bytes] layout.
    let small = SerialisedDatablock::new(
        &sample_datablock(),
        CompressionPolicy::Auto { compression_allowed: true },
        &codec,
    )
    .unwrap();
    if let DatablockStorageKind::Inline(inline) = &small.storage_kind {
        let mut seed = Vec::new();
        seed.extend_from_slice(&small.compressed_size.to_le_bytes());
        seed.extend_from_slice(&small.uncompressed_size.to_le_bytes());
        seed.push(small.compression_type);
        seed.extend_from_slice(&inline.minibatch);
        write_seed(&dir, "inline_uncompressed.bin", &seed);
    }

    // Larger, compressible payload -> inlines via zstd-dict compression.
    let large_payload = vec![0u8; 1500];
    let large_datablock = Datablock {
        datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
            aggregate_version: 1,
            events: vec![DatablockAggregateEvent {
                client_seq: 1,
                event_seq: 1,
                event_id: Some(1),
                event_timestamp: 1,
                event_type_major: 1,
                event_type_minor: 0,
                event_value: Arc::new(large_payload),
                iv: None,
            }],
        }),
    };
    let compressed = SerialisedDatablock::new(
        &large_datablock,
        CompressionPolicy::Auto { compression_allowed: true },
        &codec,
    )
    .unwrap();
    if let DatablockStorageKind::Inline(inline) = &compressed.storage_kind {
        let mut seed = Vec::new();
        seed.extend_from_slice(&compressed.compressed_size.to_le_bytes());
        seed.extend_from_slice(&compressed.uncompressed_size.to_le_bytes());
        seed.push(compressed.compression_type);
        seed.extend_from_slice(&inline.minibatch);
        write_seed(&dir, "inline_zstd_dict.bin", &seed);
    }
}

fn gen_versioned_block(root: &Path) {
    let dir = root.join("versioned_block");

    // selector 0: metablock body (post-header payload bytes only - the
    // harness rebuilds the header + CRC itself).
    let mb_body = fixed_serialise_stack_to_vec(&sample_metablock());
    let mut seed0 = vec![0u8];
    seed0.extend_from_slice(&mb_body);
    write_seed(&dir, "metablock.bin", &seed0);

    // selector 1: segment summary payload.
    let summary = SegmentSummaryBlock {
        payload: SegmentSummaryPayload {
            orgs: vec![1, 2],
            aggregate_types: vec![],
            aggregates: vec![],
        },
    };
    let summary_body = fixed_serialise_heap(&summary).unwrap();
    let mut seed1 = vec![1u8];
    seed1.extend_from_slice(&summary_body);
    write_seed(&dir, "segment_summary.bin", &seed1);

    // selector 2: fallback batch payload.
    let fallback = FallbackBatch::new(1, 100, 0, 42, 1, 1);
    let fallback_body = fixed_serialise_heap(&fallback).unwrap();
    let mut seed2 = vec![2u8];
    seed2.extend_from_slice(&fallback_body);
    write_seed(&dir, "fallback_batch.bin", &seed2);

    // Sanity: the constants used in serialise must match what the harness expects.
    let _ = WIRE_VERSION_WAL_METABLOCK;
    let _ = WIRE_VERSION_SEGMENT_SUMMARY_BLOCK;
    let _ = WIRE_VERSION_S3_FALLBACK_BATCH;
}

fn fixed_serialise_stack_to_vec<T: bincode::Encode>(message: &T) -> Vec<u8> {
    let mut buf = [0u8; FIXED_BLOCK_SIZE_BYTES];
    let n = fixed_serialise_stack(message, &mut buf).unwrap();
    buf[..n].to_vec()
}

fn gen_sbbf(root: &Path) {
    let dir = root.join("sbbf");
    // hash (8 bytes) + a properly-aligned 4-word (32-byte) block, matching
    // real usage (words.len() a multiple of 4).
    let mut seed = Vec::new();
    seed.extend_from_slice(&0xDEAD_BEEFu64.to_le_bytes());
    let mut words = [0u64; 4];
    celeriant_wal::sbbf::insert(&mut words, 0xDEAD_BEEF);
    for w in words {
        seed.extend_from_slice(&w.to_le_bytes());
    }
    write_seed(&dir, "aligned_4_words.bin", &seed);

    // Larger 16-word (128-byte) block, still aligned.
    let mut seed = Vec::new();
    seed.extend_from_slice(&12345u64.to_le_bytes());
    let mut words = [0u64; 16];
    for i in 0..10u64 {
        celeriant_wal::sbbf::insert(&mut words, i * 7919);
    }
    for w in words {
        seed.extend_from_slice(&w.to_le_bytes());
    }
    write_seed(&dir, "aligned_16_words.bin", &seed);
}

fn gen_metablock_bytes(root: &Path) {
    let dir = root.join("metablock_bytes");
    let mut buf = [0u8; FIXED_BLOCK_SIZE_BYTES];
    serialize_versioned_message(&sample_metablock(), WIRE_VERSION_WAL_METABLOCK, &mut buf).unwrap();
    write_seed(&dir, "metablock_full_block.bin", &buf);

    // A truncated-but-plausible variant (still full-sized field region for
    // the fields we read, just corrupted trailing bytes).
    let mut truncated = buf;
    truncated[FIXED_BLOCK_SIZE_BYTES - 1] ^= 0xFF;
    write_seed(&dir, "metablock_corrupted_tail.bin", &truncated);
}

fn gen_dual_header_recovery(root: &Path) {
    let dir = root.join("dual_header_recovery");
    let header = sample_shard_log_header();
    let mut buffer = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
    serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, &mut buffer).unwrap();

    // control=0: bytes are already a real, CRC-valid serialisation - no recompute needed.
    // Both slots valid; primary wins, matching the common "clean read" case.
    let mut seed = vec![0u8];
    seed.extend_from_slice(&buffer);
    seed.extend_from_slice(&buffer);
    write_seed(&dir, "both_valid.bin", &seed);

    // Primary slot is corrupted noise with no recompute (control bit0=0) - its embedded CRC
    // won't match, forcing the fallback path; backup slot is the real valid header
    // (control bit1=1 recomputes it isn't even needed since bytes are already valid, but
    // exercising the recompute path here too for corpus diversity).
    let mut seed = vec![0b0010u8];
    seed.extend_from_slice(&vec![0xAAu8; HEADER_BLOCK_SIZE_BYTES]);
    seed.extend_from_slice(&buffer);
    write_seed(&dir, "primary_corrupt_backup_valid.bin", &seed);

    // Both slots corrupted noise, no recompute - both decodes fail, exercises the
    // "propagate the backup error" path with zero panics.
    let mut seed = vec![0u8];
    seed.extend_from_slice(&vec![0x55u8; HEADER_BLOCK_SIZE_BYTES]);
    seed.extend_from_slice(&vec![0x66u8; HEADER_BLOCK_SIZE_BYTES]);
    write_seed(&dir, "both_corrupt.bin", &seed);
}

fn gen_multi_block_segment_scan(root: &Path) {
    let dir = root.join("multi_block_segment_scan");
    let mb_body = fixed_serialise_stack_to_vec(&sample_metablock());

    // Two well-formed blocks back to back - the harness recomputes CRC/version per block,
    // so only the post-header payload region needs to be real serialised bytes.
    let mut seed = vec![0u8; 2 * FIXED_BLOCK_SIZE_BYTES];
    for block_idx in 0..2 {
        let base = block_idx * FIXED_BLOCK_SIZE_BYTES;
        let n = mb_body.len().min(FIXED_BLOCK_SIZE_BYTES - HEADER_SIZE);
        seed[base + HEADER_SIZE..base + HEADER_SIZE + n].copy_from_slice(&mb_body[..n]);
    }
    write_seed(&dir, "two_valid_blocks.bin", &seed);

    // Same two blocks plus a ragged trailing partial block - proves the "discard the tail"
    // contract (`chunks_exact`) doesn't panic on a truncated last record.
    let mut ragged = seed.clone();
    ragged.extend_from_slice(&[0u8; 37]);
    write_seed(&dir, "two_valid_blocks_ragged_tail.bin", &ragged);
}

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");
    gen_bincode_decode(&root);
    gen_wire_header(&root);
    gen_serialised_datablock(&root);
    gen_versioned_block(&root);
    gen_sbbf(&root);
    gen_metablock_bytes(&root);
    gen_dual_header_recovery(&root);
    gen_multi_block_segment_scan(&root);
    println!("corpus generated under {}", root.display());
}
