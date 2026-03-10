/// Checks if the kernel clock is NTP-synchronized via the `adjtimex()` syscall.
///
/// NTP daemons (chrony, ntpd, systemd-timesyncd) periodically call `adjtimex()`
/// to discipline the host kernel clock. The kernel tracks whether it has been
/// disciplined recently: if no NTP daemon has called in for ~11 minutes, the
/// kernel sets `STA_UNSYNC` and `adjtimex()` returns `TIME_ERROR` (5).
///
/// This works in containers (Docker, K8s, ECS, EKS) because all containers
/// share the host kernel's clock — the NTP daemon runs on the host, and the
/// sync state is visible to any process via `adjtimex()`.
pub fn check_clock_synchronized() -> Result<(), String> {
    let mut timex: libc::timex = unsafe { std::mem::zeroed() };
    let result = unsafe { libc::adjtimex(&mut timex) };

    if result < 0 {
        return Err(format!(
            "adjtimex() syscall failed (errno {}). Cannot verify clock synchronization.",
            std::io::Error::last_os_error()
        ));
    }

    // adjtimex() returns the kernel clock state. States 0–4 all indicate
    // a disciplined clock (TIME_OK, or leap-second transitions). State 5
    // (TIME_ERROR) means STA_UNSYNC is set — no NTP discipline for ~11 min.
    const TIME_ERROR: i32 = 5;
    if result == TIME_ERROR {
        return Err(
            "System clock is not NTP-synchronized (kernel reports STA_UNSYNC). \
             Celeriant requires synchronized clocks for cluster operation. \
             Ensure an NTP daemon (chrony, systemd-timesyncd, or ntpd) is running on the host."
                .to_string(),
        );
    }

    Ok(())
}
