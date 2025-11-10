use nix::libc;
use nix::sys::signal::{self, SigSet, Signal, SigHandler, SigAction};
use nix::sys::signalfd::{SignalFd};
use std::io;

extern "C" fn signal_noop(_: libc::c_int) {
    // This handler does nothing - we just need it to override the default handler
    // The actual signal will be read via signalfd
}

pub struct SignalHandler {
    signalfd: SignalFd,
}

impl SignalHandler {
    pub fn new() -> io::Result<Self> {
        // Install no-op signal handlers to override default termination behavior
        let handler = SigHandler::Handler(signal_noop);
        let action = SigAction::new(
            handler,
            signal::SaFlags::SA_RESTART,
            SigSet::empty(),
        );
        
        unsafe {
            signal::sigaction(Signal::SIGINT, &action)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            signal::sigaction(Signal::SIGTERM, &action)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }
        
        // Block the signals so they're queued for signalfd
        let mut mask = SigSet::empty();
        mask.add(Signal::SIGINT);
        mask.add(Signal::SIGTERM);
        
        signal::sigprocmask(signal::SigmaskHow::SIG_BLOCK, Some(&mask), None)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        
        // Create signalfd for these signals (non-blocking)
        let signalfd = SignalFd::new(&mask)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        
        Ok(SignalHandler { signalfd })
    }
    
    /// Poll for a signal. Returns Some(signal) if one was received, None if not.
    pub fn poll_signal(&mut self) -> io::Result<Option<Signal>> {
        match self.signalfd.read_signal() {
            Ok(Some(sig)) => {
                Ok(Some(Signal::try_from(sig.ssi_signo as i32).unwrap()))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }
}