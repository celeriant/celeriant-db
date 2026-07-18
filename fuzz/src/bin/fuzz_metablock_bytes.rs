fn main() {
    afl::fuzz!(|data: &[u8]| {
        celeriant_fuzz::run_metablock_bytes(data);
    });
}
