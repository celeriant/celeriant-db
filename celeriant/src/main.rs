use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use celeriant_lib::cert_cmd::run_cert;
use celeriant_lib::startup;

#[deny(clippy::disallowed_methods)]
fn main() -> Result<(), std::io::Error> {
    let mut args: Vec<String> = std::env::args().collect();

    if args.get(1).map(|s| s.as_str()) == Some("cert") {
        let cert_argv: Vec<String> = args.drain(2..).collect();
        if let Err(e) = run_cert(cert_argv) {
            eprintln!("error: {e:?}");
            std::process::exit(1);
        }
        return Ok(());
    }

    startup(args)
}
