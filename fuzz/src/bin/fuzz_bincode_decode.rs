fn main() {
    afl::fuzz!(|data: &[u8]| {
        celeriant_fuzz::run_bincode_decode(data);
    });
}
