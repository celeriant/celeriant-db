fn main() {
    afl::fuzz!(|data: &[u8]| {
        celeriant_fuzz::run_wire_header(data);
    });
}
