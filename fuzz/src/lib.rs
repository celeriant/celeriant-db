//! Fuzz harness logic, kept AFL-agnostic: every target is a plain `run(data)`
//! function. The `afl::fuzz!` wrapper in `src/bin/*.rs` just calls into here,
//! so the same logic can be exercised from `cargo test` (crash-proof) or a
//! plain `cargo run --bin <name> <file>` reproducer without any AFL machinery.

use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES, MINIBATCH_SIZE_BYTES};
use celeriant_wal::datablocks::datablock::Datablock;
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::datablock_inline_data::DatablockInlineData;
use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
use celeriant_wire::codec::bincode::fixed_deserialise;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::disk::serialised_datablock::deserialise_datablock;
use celeriant_wire::disk::versioned_block::{deserialise_metablock, deserialise_segment_summary, deserialise_fallback_batch, deserialise_shard_log_header, CRC_SIZE, HEADER_SIZE};
use celeriant_wire::disk::metablock_bytes;
use celeriant_wire::network::wire_header::WireHeader;
use celeriant_msg::request::requests::WriteRequest;

// Building `DictCodec` trains/compiles a zstd dictionary and is not cheap;
// the real server builds one per executor at boot (see `DictCodec::new`
// doc comment) and reuses it. Doing that per-fuzz-iteration was skewing the
// serialised_datablock/wire_header exec rate and tripping AFL's calibration
// timeout as spurious "hangs". The AFL loop is single-threaded per process
// (persistent-mode `__afl_persistent_loop`), so a thread-local built once
// is safe and matches production reuse.
thread_local! {
    static DICT_CODEC: DictCodec = DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile");
}

// ==================== Target A: bincode_decode ====================
//
// `fixed_deserialise::<T>` is the untrusted-input entry point for every
// on-wire and on-disk bincode payload. `MAX_DECODE_BYTES` (bincode.rs) is
// meant to make it panic-free; this target monomorphizes over the three
// representative on-wire types and just checks it never panics/OOMs.

pub fn run_bincode_decode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let selector = data[0];
    let body = &data[1..];
    match selector % 3 {
        0 => {
            let _ = fixed_deserialise::<Metablock>(body);
        }
        1 => {
            let _ = fixed_deserialise::<Datablock>(body);
        }
        _ => {
            let _ = fixed_deserialise::<WriteRequest>(body);
        }
    }
}

// ==================== Target B: wire_header ====================
//
// Drives `WireHeader::from_reader` + the variable-size codec reader over an
// in-memory cursor (no async runtime needed, per rust-fuzz book guidance).
// Exercises the version gate, the compressed/uncompressed length guards
// (decompression-bomb defense), and the zstd-dict decompress path.

pub fn run_wire_header(data: &[u8]) {
    // Cap well below a real deployment's `internode_max_request_size` so a
    // pathological but *accepted* frame can't force multi-GB allocations
    // during fuzzing; the guard logic itself is exercised regardless since
    // corpus/mutated headers routinely claim lengths above this cap too.
    const MAX_SIZE_BYTES: u64 = 4 * 1024 * 1024;

    futures_lite::future::block_on(async {
        let mut reader = futures_lite::io::Cursor::new(data);
        let header = match WireHeader::from_reader(&mut reader, MAX_SIZE_BYTES).await {
            Ok(h) => h,
            Err(_) => return,
        };

        // The reader/decompress work here is entirely synchronous (in-memory
        // cursor, no real yielding), so a nested `block_on` inside the
        // thread-local borrow is safe and lets us keep the cached codec
        // scoped to `with` without fighting its non-'static closure bound.
        let _: Result<Vec<u8>, _> = DICT_CODEC.with(|codec| {
            futures_lite::future::block_on(header.read_variable_size_with_codec(&mut reader, codec))
        });
    });
}

// ==================== Target C: serialised_datablock (inline path) ====================
//
// `deserialise_datablock`'s `Inline` branch slices
// `&inline.minibatch[..compressed_size as usize]` with an on-disk
// `compressed_size` that is NOT bounds-checked against the fixed minibatch
// array before the slice. Feeds fuzzed (fully attacker-controlled)
// compressed_size/uncompressed_size/compression byte straight through.

pub fn run_serialised_datablock_inline(data: &[u8]) {
    if data.len() < 17 {
        return;
    }
    let compressed_size = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let uncompressed_size = u64::from_le_bytes(data[8..16].try_into().unwrap());
    // Keep the compression byte within the valid enum range so we reach the
    // slicing line instead of bouncing off `UnknownCompression` first - that
    // gate is intentional and not what we're targeting here.
    let compression_type_id = data[16] % 2;

    let mut minibatch = [0u8; MINIBATCH_SIZE_BYTES];
    let payload = &data[17..];
    let n = payload.len().min(MINIBATCH_SIZE_BYTES);
    minibatch[..n].copy_from_slice(&payload[..n]);

    let storage_kind = DatablockStorageKind::Inline(DatablockInlineData { minibatch });

    DICT_CODEC.with(|codec| {
        let _ = deserialise_datablock(
            uncompressed_size,
            compressed_size,
            celeriant_wal::constants::WIRE_VERSION_WAL_DATABLOCK,
            compression_type_id,
            &storage_kind,
            None,
            codec,
        );
    });
}

// ==================== Target D: versioned_block ====================
//
// `deserialise_metablock` / `deserialise_segment_summary` /
// `deserialise_fallback_batch` all sit behind a leading CRC32c gate
// (`validate_header`); naive random bytes bounce off `ChecksumMismatch`
// before reaching the decode logic. The harness recomputes the CRC over the
// mutated body so AFL's mutations reach the interesting bincode decode path
// instead of getting rejected at the checksum.

pub fn run_versioned_block(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let selector = data[0];
    let body = &data[1..];

    match selector % 3 {
        0 => {
            // Fixed FIXED_BLOCK_SIZE_BYTES array required by deserialise_metablock.
            let mut buf = [0u8; FIXED_BLOCK_SIZE_BYTES];
            let n = body.len().min(FIXED_BLOCK_SIZE_BYTES - HEADER_SIZE);
            buf[HEADER_SIZE..HEADER_SIZE + n].copy_from_slice(&body[..n]);
            buf[CRC_SIZE..HEADER_SIZE].copy_from_slice(&celeriant_wal::constants::WIRE_VERSION_WAL_METABLOCK.to_le_bytes());
            let crc = crc32c::crc32c(&buf[CRC_SIZE..]);
            buf[0..CRC_SIZE].copy_from_slice(&crc.to_le_bytes());
            let _ = deserialise_metablock(&buf);
        }
        1 => {
            let _ = recrc_and_call(body, celeriant_wal::constants::WIRE_VERSION_SEGMENT_SUMMARY_BLOCK, |b| {
                let _ = deserialise_segment_summary(b);
            });
        }
        _ => {
            let _ = recrc_and_call(body, celeriant_wal::constants::WIRE_VERSION_S3_FALLBACK_BATCH, |b| {
                let _ = deserialise_fallback_batch(b);
            });
        }
    }
}

fn recrc_and_call(body: &[u8], version: u32, f: impl FnOnce(&[u8])) -> () {
    let mut buf = vec![0u8; HEADER_SIZE + body.len()];
    buf[HEADER_SIZE..].copy_from_slice(body);
    buf[CRC_SIZE..HEADER_SIZE].copy_from_slice(&version.to_le_bytes());
    let crc = crc32c::crc32c(&buf[CRC_SIZE..]);
    buf[0..CRC_SIZE].copy_from_slice(&crc.to_le_bytes());
    f(&buf);
}

// ==================== Target E: sbbf ====================
//
// `insert`/`contains` only precondition their `words.len() % 4 == 0`
// invariant via `debug_assert!`. Note: `cargo afl build` keeps
// debug-assertions on (verified empirically - the assert itself fires when
// replaying the reproducer through the instrumented binary), so in THIS
// build the crash is the clean assertion message. In a true production
// release build (debug-assertions off, e.g. an actual deploy), the assert
// disappears silently and a short/misaligned `words` slice instead panics
// one line later on the raw indexing bounds check - the block-index
// arithmetic picks a block whose 4-word span runs past the end of the
// slice. Either way it's a real crash (Rust's bounds checks are never
// compiled out), but the intended guard provides no protection in prod.

pub fn run_sbbf(data: &[u8]) {
    if data.len() < 8 {
        return;
    }
    let hash = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let word_bytes = &data[8..];
    // Deliberately do NOT round to a multiple of 4 u64s - that's the whole point.
    let mut words: Vec<u64> = word_bytes
        .chunks(8)
        .map(|c| {
            let mut buf = [0u8; 8];
            buf[..c.len()].copy_from_slice(c);
            u64::from_le_bytes(buf)
        })
        .collect();
    if words.is_empty() {
        words.push(0);
    }
    celeriant_wal::sbbf::insert(&mut words, hash);
    let _ = celeriant_wal::sbbf::contains(&words, hash);
}

// ==================== Target F: metablock_bytes ====================
//
// Zero-copy byte-level accessors used for fast metablock scanning without
// full bincode decode. Every accessor does hardcoded `bytes[offset..offset+N]`
// slicing with no length check - a short/truncated on-disk blob panics.

pub fn run_metablock_bytes(data: &[u8]) {
    // Deliberately NOT panic-caught: an uncaught panic here is exactly what
    // should abort the AFL child process and register as a crash.
    let _ = metablock_bytes::read_metablock_kind_discriminant(data);
    let _ = metablock_bytes::read_wal_seq(data);
    let _ = metablock_bytes::read_server_timestamp(data);
    let _ = metablock_bytes::read_node_id(data);
    let _ = metablock_bytes::read_compressed_size(data);
    let _ = metablock_bytes::read_uncompressed_size(data);
}

// ==================== Target G: dual_header_recovery ====================
//
// Models `load_header_detecting_corruption` (celeriant_rotating_log/src/log_segment_file/
// log_segment_file.rs:391-419): try the primary (front-of-file) header slot; on any decode
// failure fall back to the backup (rear-of-file) slot. That function is `async fn` over a
// glommio `DmaFile`, and `celeriant_rotating_log` depends on glommio unconditionally, so it
// can't be pulled into this runtime-free workspace. The fallback SELECTION logic itself is
// pure (just two calls to `deserialise_shard_log_header`, which lives in `celeriant_wire` and
// has no glommio dependency), so it is replicated verbatim here instead.
//
// Also chains into the real next consumer step: `AggregateKeyBloom::from_bytes`
// (celeriant_rotating_log/src/log_segment_file/aggregate_key_bloom.rs:35, same glommio reason,
// not pulled in) treats the header's `aggregate_bloom`/`client_bloom` `Vec<u64>` as SBBF words
// and only `debug_assert_eq!`s `len() % 4 == 0` before indexing — replicated directly by
// calling the underlying `celeriant_wal::sbbf::contains` (glommio-free) on those same fields,
// exactly as `AggregateKeyBloom::may_contain`/`from_bytes` would. `deserialise_shard_log_header`
// does NOT itself enforce that invariant on a CRC-valid body: bincode's `Vec<u64>` decode
// (`Vec<T>::decode`, bincode-2.0.1/src/features/impl_alloc.rs:263-283) only checks the claimed
// length against `MAX_DECODE_BYTES` (celeriant_wire/src/codec/bincode.rs), not against any
// SBBF-block alignment. So a CRC-valid header whose bloom length isn't a multiple of 4 reaches
// `sbbf::contains`/`insert` with a misaligned slice — this is the `fuzz_sbbf` bug (target E)
// from Phase 1, shown here reachable one hop further: through a CRC-valid on-disk header,
// which the Phase-1 validation pass explicitly said was NOT the case ("length always a
// multiple of 4 by construction" — true only if the header was honestly written; the CRC
// gate alone does not guarantee it for a corrupted-but-still-checksum-valid header).

pub fn run_dual_header_recovery(data: &[u8]) {
    if data.len() < 2 {
        return;
    }
    let control = data[0];
    let body = &data[1..];
    let half = body.len() / 2;
    let (primary_src, backup_src) = body.split_at(half);

    let primary = build_header_slot(primary_src, control & 0b0001 != 0, control & 0b0100 != 0);
    let backup = build_header_slot(backup_src, control & 0b0010 != 0, control & 0b1000 != 0);

    let header = match deserialise_shard_log_header(&primary) {
        Ok(h) => h,
        Err(_primary_err) => match deserialise_shard_log_header(&backup) {
            Ok(h) => h,
            Err(_backup_err) => return,
        },
    };

    let _ = celeriant_wal::sbbf::contains(&header.aggregate_bloom, 0);
    let _ = celeriant_wal::sbbf::contains(&header.client_bloom, 0);
}

/// Builds one HEADER_BLOCK_SIZE_BYTES-sized header slot from fuzzer bytes. `recompute_crc`
/// mirrors target D's `recrc_and_call`: lets mutations reach decode instead of bouncing off
/// `ChecksumMismatch`. Left optional (unlike target D) so AFL can also explore the genuinely
/// CRC-invalid case, which is what drives the primary -> backup fallback branch.
fn build_header_slot(src: &[u8], recompute_crc: bool, force_valid_version: bool) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
    let n = src.len().min(HEADER_BLOCK_SIZE_BYTES);
    buf[..n].copy_from_slice(&src[..n]);
    if force_valid_version {
        buf[CRC_SIZE..HEADER_SIZE].copy_from_slice(&celeriant_wal::constants::WIRE_VERSION_WAL_SHARD_LOG_HEADER.to_le_bytes());
    }
    if recompute_crc {
        let crc = crc32c::crc32c(&buf[CRC_SIZE..]);
        buf[0..CRC_SIZE].copy_from_slice(&crc.to_le_bytes());
    }
    buf
}

// ==================== Target H: multi_block_segment_scan ====================
//
// Models the pure block-chunking contract of `read_fixed_records_visit_const`
// (celeriant_disk/src/files/read_fixed_records_visit_const.rs:18-45): the async/`DmaFile`-
// coupled (glommio) primitive every WAL scan (reverse_metablock_scanner.rs,
// shard_wal_compact.rs, shard_wal.rs) drives to turn a raw segment-file image into fixed-size
// metablock records, silently discarding a ragged trailing partial block
// (`valid_end = start + ((end-start)/n)*n`, `as_chunks::<N>()` drops the remainder). Both
// `celeriant_disk` and `celeriant_rotating_log` depend on glommio unconditionally, so the
// async file-read wrapper can't be pulled into this workspace; the pure chunking behaviour
// itself — `chunks_exact(N)`, ragged tail discarded — is replicated directly over an
// in-memory buffer, feeding each full `FIXED_BLOCK_SIZE_BYTES` chunk through the same
// CRC-recompute + `deserialise_metablock` shape target D already uses, but now with many
// blocks per single fuzz execution and an honest "read a file image" framing.
//
// Also chains into the real next step for an Inline-stored datablock: `collect_from_disk.rs`
// (`fetch_datablocks_for_metablocks`, celeriant_shard/src/collect_from_disk.rs:67) immediately
// hands a decoded metablock's `DatablockStorageKind::Inline` straight to `deserialise_datablock`
// with the exact same argument shape used here. This makes the target-C bugs (OOB
// `compressed_size` slice / unbounded `uncompressed_size` allocation, `fuzz_serialised_datablock`)
// reachable through a CRC-valid, multi-block-scanned `Metablock` — a step beyond target C's
// direct-call reachability, and beyond what the Phase-1 validation pass considered ("a CRC-valid
// metablock is always in range" assumes an honest writer, not a decoder/serializer bug that
// still round-trips through the CRC).

pub fn run_multi_block_segment_scan(data: &[u8]) {
    DICT_CODEC.with(|codec| {
        for chunk in data.chunks_exact(FIXED_BLOCK_SIZE_BYTES) {
            let mut block: [u8; FIXED_BLOCK_SIZE_BYTES] = chunk.try_into().unwrap();
            block[CRC_SIZE..HEADER_SIZE].copy_from_slice(&celeriant_wal::constants::WIRE_VERSION_WAL_METABLOCK.to_le_bytes());
            let crc = crc32c::crc32c(&block[CRC_SIZE..]);
            block[0..CRC_SIZE].copy_from_slice(&crc.to_le_bytes());

            let Ok(metablock) = deserialise_metablock(&block) else {
                continue;
            };

            if let DatablockStorageKind::Inline(_) = &metablock.datablock {
                let _ = deserialise_datablock(
                    metablock.uncompressed_size,
                    metablock.compressed_size,
                    metablock.datablock_version,
                    metablock.datablock_compression_type,
                    &metablock.datablock,
                    None,
                    codec,
                );
            }
        }
    });
}

#[cfg(test)]
mod crash_proof {
    use super::*;

    /// Target C: compressed_size claims more than the fixed minibatch array
    /// holds. `&inline.minibatch[..compressed_size]` panics before any
    /// decode is attempted. This is the exact known bug from the survey.
    #[test]
    #[should_panic]
    fn serialised_datablock_inline_oob_compressed_size() {
        let mut data = vec![0u8; 20];
        // compressed_size = MINIBATCH_SIZE_BYTES + 1000, LE u64 at data[0..8]
        let bad_size = (MINIBATCH_SIZE_BYTES as u64) + 1000;
        data[0..8].copy_from_slice(&bad_size.to_le_bytes());
        run_serialised_datablock_inline(&data);
    }

    /// Target E: words.len() in 1..=3 is non-multiple-of-4; block_index's
    /// `words.len() / 4 == 0` degenerate case still writes/reads lanes 0..=3,
    /// running past a 1-word slice.
    #[test]
    #[should_panic]
    fn sbbf_misaligned_words_oob() {
        // 8 bytes hash + exactly 8 bytes of word data => words.len() == 1.
        let data = vec![0xAAu8; 16];
        run_sbbf(&data);
    }

    /// Target F: empty buffer, the very first accessor call indexes past the
    /// end. This is the exact `run` function the AFL binary calls.
    #[test]
    #[should_panic]
    fn metablock_bytes_empty_buffer_panics() {
        run_metablock_bytes(&[]);
    }

    /// Target G: a real, CRC-valid (via the real `serialize_versioned_message` path, not the
    /// harness's optional recompute) `ShardLogHeader` whose `aggregate_bloom` length (3 u64s)
    /// isn't a multiple of 4. `deserialise_shard_log_header` decodes it successfully - nothing
    /// in bincode's `Vec<u64>` decode enforces SBBF-block alignment - so the harness's
    /// downstream `sbbf::contains` call (mirroring `AggregateKeyBloom::from_bytes`) hits the
    /// `debug_assert!(words.len() % 4 == 0)` in `celeriant_wal::sbbf`. Control byte 0: no
    /// recompute/force-version needed, the header bytes are already valid.
    #[test]
    #[should_panic]
    fn dual_header_recovery_misaligned_bloom_panics() {
        use celeriant_wal::constants::WIRE_VERSION_WAL_SHARD_LOG_HEADER;
        use celeriant_wal::shard_log_header::{HeaderCursor, ShardLogHeader};
        use celeriant_wire::disk::versioned_block::serialize_versioned_message;

        let header = ShardLogHeader {
            write: HeaderCursor::genesis(),
            aggregate_bloom: vec![0u64; 3], // NOT a multiple of 4 - violates the SBBF invariant
            client_bloom: vec![0u64; 4],
            last_received_replication_wal_seq: 0,
            last_self_acked_wal_seq: 0,
            read: HeaderCursor::genesis(),
        };
        let mut buffer = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&header, WIRE_VERSION_WAL_SHARD_LOG_HEADER, &mut buffer).unwrap();

        let mut data = vec![0u8]; // control: no recompute needed, buffer is already valid
        data.extend_from_slice(&buffer); // primary slot
        data.extend_from_slice(&buffer); // backup slot (unused - primary decodes fine)
        run_dual_header_recovery(&data);
    }

    /// Target H: a real, CRC-valid `Metablock` (via `deserialise_metablock`, reached through the
    /// harness's own CRC-recompute step) with `Inline` datablock storage and a `compressed_size`
    /// that exceeds the fixed minibatch array. This is the exact target-C bug
    /// (`serialised_datablock.rs:178`, `&inline.minibatch[..compressed_size]`), now proven
    /// reachable from a multi-block, through-the-Metablock-CRC-gate scan rather than a direct
    /// unguarded call.
    #[test]
    #[should_panic]
    fn multi_block_segment_scan_inline_oob_compressed_size_panics() {
        use celeriant_wal::aggregate_key::AggregateKey;
        use celeriant_wire::codec::bincode::fixed_serialise_stack;

        let mut metablock = Metablock::default_inline_event_batch_metadata(AggregateKey::new(1, 2, 3));
        metablock.compressed_size = (MINIBATCH_SIZE_BYTES as u64) + 1000; // OOB
        metablock.datablock_version = celeriant_wal::constants::WIRE_VERSION_WAL_DATABLOCK;
        metablock.datablock_compression_type = 0; // CompressionType::None

        let mut block = [0u8; FIXED_BLOCK_SIZE_BYTES];
        let n = fixed_serialise_stack(&metablock, &mut block[HEADER_SIZE..]).unwrap();
        assert!(HEADER_SIZE + n <= FIXED_BLOCK_SIZE_BYTES);

        run_multi_block_segment_scan(&block);
    }
}

/// Every generated seed must survive its own harness without panicking -
/// otherwise `generate_corpus` produced garbage instead of valid encodings.
#[cfg(test)]
mod corpus_sanity {
    use super::*;
    use std::fs;
    use std::path::Path;

    fn assert_seeds_do_not_panic(dir: &str, run: impl Fn(&[u8])) {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus").join(dir);
        let entries: Vec<_> = fs::read_dir(&root)
            .unwrap_or_else(|e| panic!("run generate_corpus first: {root:?}: {e}"))
            .map(|e| e.unwrap().path())
            .collect();
        assert!(!entries.is_empty(), "no seeds in {root:?}");
        for path in entries {
            let data = fs::read(&path).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&data)));
            assert!(result.is_ok(), "seed {path:?} panicked unexpectedly");
        }
    }

    #[test]
    fn bincode_decode_seeds_are_valid() {
        assert_seeds_do_not_panic("bincode_decode", run_bincode_decode);
    }

    #[test]
    fn wire_header_seeds_are_valid() {
        assert_seeds_do_not_panic("wire_header", run_wire_header);
    }

    #[test]
    fn serialised_datablock_seeds_are_valid() {
        assert_seeds_do_not_panic("serialised_datablock", run_serialised_datablock_inline);
    }

    #[test]
    fn versioned_block_seeds_are_valid() {
        assert_seeds_do_not_panic("versioned_block", run_versioned_block);
    }

    #[test]
    fn sbbf_seeds_are_valid() {
        assert_seeds_do_not_panic("sbbf", run_sbbf);
    }

    #[test]
    fn metablock_bytes_seeds_are_valid() {
        assert_seeds_do_not_panic("metablock_bytes", run_metablock_bytes);
    }

    #[test]
    fn dual_header_recovery_seeds_are_valid() {
        assert_seeds_do_not_panic("dual_header_recovery", run_dual_header_recovery);
    }

    #[test]
    fn multi_block_segment_scan_seeds_are_valid() {
        assert_seeds_do_not_panic("multi_block_segment_scan", run_multi_block_segment_scan);
    }

    /// Through-the-gate targets must start the fuzzer from a genuinely decodable state, not
    /// merely a non-panicking one - a seed that always bounces off `ChecksumMismatch` explores
    /// nothing interesting. `both_valid.bin`'s control byte is 0 (no recompute), so its primary
    /// slot bytes (offset 1..HEADER_BLOCK_SIZE_BYTES+1) are exactly what `generate_corpus`'s
    /// `serialize_versioned_message` produced - assert `deserialise_shard_log_header` accepts
    /// them directly.
    #[test]
    fn dual_header_recovery_both_valid_seed_decodes_ok() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus/dual_header_recovery/both_valid.bin");
        let data = fs::read(&root).unwrap_or_else(|e| panic!("run generate_corpus first: {root:?}: {e}"));
        let primary: &[u8] = &data[1..1 + HEADER_BLOCK_SIZE_BYTES];
        deserialise_shard_log_header(primary).expect("primary slot must decode Ok");
    }

    /// Same rationale for `two_valid_blocks.bin`: the harness recomputes each block's
    /// CRC/version itself, so a decode-Ok assertion here has to replicate exactly that step
    /// (matching `run_multi_block_segment_scan`'s own recompute) rather than calling
    /// `deserialise_metablock` on the raw seed bytes directly.
    #[test]
    fn multi_block_segment_scan_first_block_decodes_ok() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus/multi_block_segment_scan/two_valid_blocks.bin");
        let data = fs::read(&root).unwrap_or_else(|e| panic!("run generate_corpus first: {root:?}: {e}"));
        let mut block: [u8; FIXED_BLOCK_SIZE_BYTES] = data[0..FIXED_BLOCK_SIZE_BYTES].try_into().unwrap();
        block[CRC_SIZE..HEADER_SIZE].copy_from_slice(&celeriant_wal::constants::WIRE_VERSION_WAL_METABLOCK.to_le_bytes());
        let crc = crc32c::crc32c(&block[CRC_SIZE..]);
        block[0..CRC_SIZE].copy_from_slice(&crc.to_le_bytes());
        deserialise_metablock(&block).expect("first block must decode Ok");
    }
}

/// Second, independent bug found on the same target while investigating why
/// smoke-run exec/sec was low: `deserialise_datablock`'s zstd-dict branch
/// passes the fully attacker-controlled `uncompressed_size` straight into
/// `DictCodec::decompress` -> `zstd::bulk::Decompressor::decompress`, which
/// does `Vec::with_capacity(uncompressed_size)` with no upper bound. Unlike
/// the wire layer (`WireHeader::from_reader` caps `uncompressed_length`
/// against `max_size_bytes` before any decompressor ever runs -
/// `from_reader_rejects_decompression_bomb` in wire_header.rs), nothing
/// bounds this value when `deserialise_datablock` is called directly (e.g.
/// from a disk read path). A near-`isize::MAX` value aborts the process via
/// Rust's allocator-failure handler (`handle_alloc_error`, NOT a catchable
/// panic - `#[should_panic]` cannot observe this, hence the subprocess
/// check below); smaller-but-large values (100s of MB - several GB) are
/// what showed up as AFL "hangs" in the smoke run, not actual timeouts.
/// This is a real, disk-reachable decompression-bomb gap.
///
/// Reproduce manually: `cargo test --lib huge_uncompressed_size_aborts -- --ignored --nocapture`
/// (spawns a child `cargo test` process and checks it dies to SIGABRT).
#[cfg(test)]
mod crash_proof_uncompressed_size_bomb {
    #[test]
    #[ignore = "spawns a child process that intentionally OOM-aborts; run explicitly"]
    fn huge_uncompressed_size_aborts_via_allocator_failure() {
        // Re-invoke this same test binary, running only the (normally-ignored)
        // one-shot abort trigger below, and assert the child died to SIGABRT -
        // an allocator-failure abort is not a catchable panic, so this can't
        // be a plain #[should_panic] test.
        use std::os::unix::process::ExitStatusExt;
        let exe = std::env::current_exe().unwrap();
        let status = std::process::Command::new(exe)
            .args(["--exact", "crash_proof_uncompressed_size_bomb::trigger_abort", "--nocapture"])
            .env("CELERIANT_FUZZ_TRIGGER_ABORT", "1")
            .status()
            .unwrap();
        assert_eq!(status.signal(), Some(6 /* SIGABRT */), "expected child to abort, got {status:?}");
    }

    #[test]
    fn trigger_abort() {
        if std::env::var("CELERIANT_FUZZ_TRIGGER_ABORT").is_err() {
            return; // only runs when invoked by the test above
        }
        let mut data = vec![0u8; 200];
        data[0..8].copy_from_slice(&50u64.to_le_bytes()); // compressed_size
        data[8..16].copy_from_slice(&(u64::MAX / 2).to_le_bytes()); // uncompressed_size
        data[16] = 1; // ZstdDict
        super::run_serialised_datablock_inline(&data);      
    }
}
