use clap::Parser;
use dotenvy::dotenv;
use tracing::{info, instrument};
use tracing_subscriber::EnvFilter;

use crate::server_config::ServerConfig;

mod server_config;

pub fn startup(args: Vec<String>) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    install_crash_handler();

    load_dotenv();

    let config = ServerConfig::parse_from(args);

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&config.default_log_level)),
        )
        .init();

    config.log_non_defaults();

    info!("Starting glommio executors...");

    glommio::LocalExecutorBuilder::default()
        .spawn(|| async move {
            info!("Shard started");
            handle_request().await;
        })
        .unwrap()
        .join()
        .unwrap();

    Ok(())
}

#[instrument]
async fn handle_request() {
    info!("Processing");
}

fn load_dotenv() {
    match dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            eprintln!("FATAL Error loading config from: {e}");
            eprintln!("Aborting");
            std::process::exit(1);
        }
    };
}

#[cfg(unix)]
fn install_crash_handler() {
    unsafe {
        set_signal_handler(libc::SIGBUS, signal_handler);
        set_signal_handler(libc::SIGSEGV, signal_handler);
        set_signal_handler(libc::SIGILL, signal_handler);
    }
}

#[cfg(unix)]
unsafe extern "C" fn signal_handler(_sig: i32) {
    std::process::abort();
}

#[cfg(unix)]
unsafe fn set_signal_handler(signal: libc::c_int, handler: unsafe extern "C" fn(libc::c_int)) {
    use libc::{sigaction, sigfillset, sighandler_t};
    let mut sigset = unsafe { std::mem::zeroed() };
    if unsafe { sigfillset(&mut sigset) } != -1 {
        let mut action: sigaction = unsafe { std::mem::zeroed() };
        action.sa_mask = sigset;
        action.sa_sigaction = handler as sighandler_t;

        unsafe {
            sigaction(signal, &action, std::ptr::null_mut());
        }
    }
}
