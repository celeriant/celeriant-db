fn main() {
    afl::fuzz!(|data: &[u8]| {
        celeriant_fuzz::run_serialised_datablock_inline(data);
    }); 
}
