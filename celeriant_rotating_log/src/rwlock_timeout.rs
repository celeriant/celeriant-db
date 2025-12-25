use std::time::Duration;
use glommio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use glommio::timer::Timer;
use glommio::GlommioError;
use futures_lite::future::or;

const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub enum LockTimeoutError {
    PotentialDeadlock { duration: Duration, operation: &'static str },
    LockError(GlommioError<()>),
}

impl std::fmt::Display for LockTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PotentialDeadlock { duration, operation } => {
                write!(f, "POTENTIAL DEADLOCK: {operation} lock not acquired within {duration:?}")
            }
            Self::LockError(e) => write!(f, "Lock error: {e}"),
        }
    }
}

impl std::error::Error for LockTimeoutError {}

pub async fn read_with_timeout<T>(
    lock: &RwLock<T>,
) -> Result<RwLockReadGuard<'_, T>, LockTimeoutError> {
    let result = or(
        async { Some(lock.read().await) },
        async {
            Timer::new(DEADLOCK_TIMEOUT).await;
            None
        },
    )
    .await;

    match result {
        Some(Ok(guard)) => Ok(guard),
        Some(Err(e)) => Err(LockTimeoutError::LockError(e)),
        None => Err(LockTimeoutError::PotentialDeadlock {
            duration: DEADLOCK_TIMEOUT,
            operation: "read",
        }),
    }
}

pub async fn write_with_timeout<T>(
    lock: &RwLock<T>,
) -> Result<RwLockWriteGuard<'_, T>, LockTimeoutError> {
    let result = or(
        async { Some(lock.write().await) },
        async {
            Timer::new(DEADLOCK_TIMEOUT).await;
            None
        },
    )
    .await;

    match result {
        Some(Ok(guard)) => Ok(guard),
        Some(Err(e)) => Err(LockTimeoutError::LockError(e)),
        None => Err(LockTimeoutError::PotentialDeadlock {
            duration: DEADLOCK_TIMEOUT,
            operation: "write",
        }),
    }
}