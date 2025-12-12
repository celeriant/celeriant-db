use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use celeriant_lib::startup;

#[deny(clippy::disallowed_methods)]
fn main() -> Result<(), std::io::Error> {
    startup(std::env::args().collect())
}