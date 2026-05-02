use std::time::Duration;
use glommio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use glommio::timer::Timer;
use futures_lite::future::or;
use tracing::warn;

pub async fn with_budget<T>(budget: Duration, fut: impl std::future::Future<Output = T>) -> Option<T> {
    or(
        async { Some(fut.await) },
        async {
            Timer::new(budget).await;
            None
        },
    ).await
}

const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub enum LockTimeoutError {
    PotentialDeadlock { duration: Duration, operation: &'static str, location: &'static str },
    LockError(String),
}

impl std::fmt::Display for LockTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PotentialDeadlock { duration, operation, location } => {
                write!(f, "POTENTIAL DEADLOCK at {location}: {operation} lock not acquired within {duration:?}")
            }
            Self::LockError(e) => write!(f, "Lock error: {e}"),
        }
    }
}
impl std::error::Error for LockTimeoutError {}

pub async fn read_with_timeout<'a, T>(
    lock: &'a RwLock<T>,
    location: &'static str,
) -> Result<RwLockReadGuard<'a, T>, LockTimeoutError> {
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
        Some(Err(e)) => Err(LockTimeoutError::LockError(e.to_string())),
        None => {
            warn!(location, duration_ms = DEADLOCK_TIMEOUT.as_millis() as u64, "RwLock read acquisition timed out — potential deadlock");
            Err(LockTimeoutError::PotentialDeadlock {
                duration: DEADLOCK_TIMEOUT,
                operation: "read",
                location,
            })
        }
    }
}

pub async fn write_with_timeout<'a, T>(
    lock: &'a RwLock<T>,
    location: &'static str,
) -> Result<RwLockWriteGuard<'a, T>, LockTimeoutError> {
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
        Some(Err(e)) => Err(LockTimeoutError::LockError(e.to_string())),
        None => {
            warn!(location, duration_ms = DEADLOCK_TIMEOUT.as_millis() as u64, "RwLock write acquisition timed out — potential deadlock");
            Err(LockTimeoutError::PotentialDeadlock {
                duration: DEADLOCK_TIMEOUT,
                operation: "write",
                location,
            })
        }
    }
}