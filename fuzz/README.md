# celeriant_fuzz

AFL++ coverage-guided fuzzing of Celeriant's untrusted-input decode paths: the on-wire and on-disk byte parsers that attacker- or corruption-controlled bytes reach first. A malformed WAL block or wire header must fail closed, never panic or read out of bounds. These targets prove that.

## Why a separate workspace

This crate is its own `[workspace]`, deliberately fenced off from the repo root. AFL instrumentation (`cargo afl build`) rewrites every compilation unit, and that cfg must not leak into the product build. It depends only on the runtime-free crates — `celeriant_wire`, `celeriant_wal`, `celeriant_msg` — none of which pull in glommio or tokio, so a target is a plain synchronous `fuzz!(|data| ...)` with no executor to stand up.

Consequence: a root `cargo build` skips `fuzz/` entirely, and you build the targets from inside `fuzz/` with a different command. Don't mix them.

## Build

The targets link AFL's runtime (`__afl_manual_init`, `__afl_fuzz_ptr`, …), which only `cargo afl build` injects. Plain `cargo build` compiles the `fuzz!` macro but leaves those symbols undefined and fails at link. Use:

```bash
cd fuzz
cargo afl build --release        # produces target/release/fuzz_*
```

The seed generator is an `example`, not a fuzz target, so it builds under plain cargo:

```bash
cargo run --example generate_corpus   # seeds corpus/ from the real serialise paths
```

Seeding from real encoders matters — AFL starts from bytes the decoders actually accept instead of burning the first hour rediscovering the wire format.

## Run

```bash
mkdir -p out
./run_campaigns.sh
```

Six campaigns launch in parallel and the script waits for all of them, so wall-clock is the longest one: **30 minutes**. Clean targets get 1800s (a crash there is a new bug); already-crashing targets get 300s (just cataloging distinct paths). Durations live in the `DUR` map — edit the seconds to change them. Logs land in `out/<target>.log`, crashes in `out/<target>/default/crashes/`, and a summary in `campaign_status.txt`.

Six AFL instances contend for cores. On fewer than ~6 free cores they still exit on the wall-clock timer, just at fewer execs/sec.

Run one target open-ended with the live UI:

```bash
cargo afl fuzz -i corpus/wire_header -o out/wire_header target/release/fuzz_wire_header
```

`dual_header_recovery` and `multi_block_segment_scan` aren't in `run_campaigns.sh` — run those individually or add them to `DUR`.

## Reproduce a crash

`known_crashes/` holds curated reproducers, the one thing here worth versioning. Replay any one by feeding it to the target on stdin — no AFL needed:

```bash
./target/release/fuzz_serialised_datablock < known_crashes/serialised_datablock_compressed_size_oob.bin
```

A fixed target exits clean; a live bug panics or aborts.

## Targets

Each is a thin `fuzz!` wrapper over a `run_*` entry in `src/lib.rs`.

- `bincode_decode` — `fixed_deserialise::<T>`, the untrusted-input entry for every on-wire and on-disk bincode payload. `MAX_DECODE_BYTES` is meant to keep it panic-free.
- `wire_header` — `WireHeader::from_reader` plus the variable-size codec reader; exercises the version gate and the compressed/uncompressed length guards.
- `serialised_datablock` — the `Inline` decode branch that slices `minibatch[..compressed_size]` with an on-disk `compressed_size` not bounds-checked against the fixed array.
- `versioned_block` — `deserialise_fallback_batch` behind its CRC32c gate; the harness recomputes the CRC over the mutated body so mutations reach the decode path instead of bouncing off `ChecksumMismatch`.
- `sbbf` — split-block bloom `contains`/`insert` over a misaligned or short `words` slice.
- `metablock_bytes` — the zero-copy `bytes[offset..offset+N]` accessors used for fast metablock scanning without a full decode.
- `dual_header_recovery` — the `sbbf` misalignment reached one hop further, through a CRC-valid on-disk header.
- `multi_block_segment_scan` — the `serialised_datablock` OOB reached through a CRC-valid, multi-block-scanned `Metablock`.

The last two exist to show a Phase-1 "unreachable" bug is in fact reachable once a CRC-valid header or metablock carries the hostile field — the CRC proves integrity, not honesty of the writer.

## Layout

- `src/bin/` — the eight `fuzz!` wrappers
- `src/lib.rs` — the `run_*` bodies under test
- `examples/generate_corpus.rs` — seed generator
- `known_crashes/` — curated reproducers (tracked)
- `corpus/`, `out/`, `target/` — generated, gitignored
