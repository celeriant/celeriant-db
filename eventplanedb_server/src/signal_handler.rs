use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::flag;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub enum Signal {
    Sigint,
    Sigterm,
}

pub struct SignalHandler {
    sigint_flag: Arc<AtomicBool>,
    sigterm_flag: Arc<AtomicBool>,
}

impl SignalHandler {
    pub fn new() -> Result<Self, std::io::Error> {
        let sigint_flag = Arc::new(AtomicBool::new(false));
        let sigterm_flag = Arc::new(AtomicBool::new(false));

        // Register signal handlers that set atomic flags
        flag::register(SIGINT, sigint_flag.clone())?;
        flag::register(SIGTERM, sigterm_flag.clone())?;

        Ok(SignalHandler {
            sigint_flag,
            sigterm_flag,
        })
    }

    /// Poll for received signals. Returns Some(Signal) if a signal was received,
    /// None if no signal is pending. This method is non-blocking and safe to call
    /// from async contexts.
    pub fn poll_signal(&mut self) -> Result<Option<Signal>, std::io::Error> {
        // Check and reset SIGINT flag atomically
        if self.sigint_flag.swap(false, Ordering::Relaxed) {
            return Ok(Some(Signal::Sigint));
        }
        
        // Check and reset SIGTERM flag atomically
        if self.sigterm_flag.swap(false, Ordering::Relaxed) {
            return Ok(Some(Signal::Sigterm));
        }

        Ok(None)
    }
}