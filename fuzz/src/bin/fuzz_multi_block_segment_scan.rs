fn main() {
    afl::fuzz!(|data: &[u8]| {
        celeriant_fuzz::run_multi_block_segment_scan(data);
    });
}
