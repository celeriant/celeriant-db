use crate::amortisation::local_event::{LocalEvent, LocalEventListener};
use celeriant_disk::files::rwlock_timeout::{read_with_timeout, with_budget, write_with_timeout};
use glommio::sync::RwLock;
use std::cell::Cell;
use std::{rc::Rc, time::Duration};

const GATE_BUDGET: Duration = Duration::from_secs(60);

/// Result of a sync operation.
pub type SyncResult<E> = Result<(), E>;

/// Result of a capture operation in two-phase sync.
pub enum CaptureResult<T, E: Clone> {
    /// Data was captured, proceed with sync.
    Captured(T),
    /// Capture failed (e.g., rollback occurred, queue was emptied).
    Failed(E),
    /// Nothing to capture: a concurrent batch already drained the pending
    /// work, so the caller is already covered. Resolves Ok without any I/O.
    NoCaptureRaceButOk,
}

/// Coordinates delayed sync with writer coalescing.
///
/// Designed for the fsync batching pattern:
/// 1. Writer calls `request_sync()`
/// 2. First writer becomes leader, others become followers
/// 3. Leader sleeps for `delay` while more writers accumulate
/// 4. Leader calls the provided sync function
/// 5. All waiters receive the result
///
/// # Example
/// ```ignore
/// let coordinator: Coordinator<FsyncError> = Coordinator::new();
///
/// // Called from multiple concurrent writers
/// coordinator.request_sync(
///     Some(Duration::from_millis(5)),
///     FsyncError::GateTimeout,
///     || async { do_fsync().await },
/// ).await?;
/// ```
pub struct Coordinator<E: Clone> {
    lock_orchestrator: RwLock<Option<Rc<LocalEvent<SyncResult<E>>>>>,

    /// Orchestrator for single-phase `request_sync` callers (header-only barrier
    /// fsyncs). Kept separate from the two-phase orchestrator: a two-phase writer
    /// attaching to a single-phase cycle resolves Ok without its queue item ever
    /// being captured; a false ack.
    lock_orchestrator_single: RwLock<Option<Rc<LocalEvent<SyncResult<E>>>>>,

    /// Sync gate: serializes sync execution and supports rollback drain/gate.
    /// Fsync and rollback both acquire write lock - ensures one-at-a-time execution.
    /// Shared by both orchestrators: single- and two-phase cycles never overlap.
    sync_gate: RwLock<()>,

    /// Did the previous two-phase cycle coalesce followers? It is the load signal
    /// for the fast path: a free gate alone is not idle (under load the gate frees
    /// in brief windows between syncs, and fast-pathing a lone writer there
    /// fragments the batch). If the last cycle had followers we are under load, so
    /// the next leader sleeps `delay` to coalesce instead of fast-pathing.
    last_two_phase_batched: Cell<bool>,
}

impl<E: Clone> Coordinator<E> {
    pub fn new() -> Self {
        Self {
            lock_orchestrator: RwLock::new(None),
            lock_orchestrator_single: RwLock::new(None),
            sync_gate: RwLock::new(()),
            last_two_phase_batched: Cell::new(false),
        }
    }

    /// Acquire rollback lock. Waits for any in-flight fsync to complete,
    /// then blocks new fsyncs until the guard is dropped.
    pub async fn acquire_rollback_lock(&self) -> Option<glommio::sync::RwLockWriteGuard<'_, ()>> {
        match with_budget(GATE_BUDGET, self.sync_gate.write()).await {
            Some(Ok(g)) => Some(g),
            Some(Err(_)) => None,
            None => None,
        }
    }

    pub async fn request_sync<F, Fut>(&self, delay: Option<Duration>, gate_timeout_err: E, sync_fn: F) -> SyncResult<E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = SyncResult<E>>,
    {
        let delay = match delay {
            Some(d) if d.as_micros() > 0 => d,
            _ => Duration::from_millis(0),
        };

        // Followers register their listener under the guard: registration is
        // atomic with observing the event in the slot, so the leader's notify
        // cannot be missed (a listener registered after notify hangs).
        enum Acquired<E: Clone> {
            Leader(Rc<LocalEvent<SyncResult<E>>>),
            Follower(LocalEventListener<SyncResult<E>>),
            Retry,
        }

        loop {
            let acquired = {
                match self.lock_orchestrator_single.try_write() {
                    Ok(mut guard) => match guard.as_ref() {
                        Some(event) => Acquired::Follower(event.listen()),
                        None => {
                            let event = Rc::new(LocalEvent::new());
                            *guard = Some(event.clone());
                            Acquired::Leader(event)
                        }
                    },
                    Err(_) => match read_with_timeout(&self.lock_orchestrator_single, "request_sync_read_be_follower").await {
                        Ok(guard) => match guard.as_ref() {
                            Some(event) => Acquired::Follower(event.listen()),
                            None => Acquired::Retry,
                        },
                        Err(_) => Acquired::Retry,
                    },
                }
            };

            match acquired {
                Acquired::Leader(event) => {
                    glommio::timer::sleep(delay).await;

                    if let Ok(mut guard) = write_with_timeout(&self.lock_orchestrator_single, "request_sync_clear_orchestrator").await {
                        guard.take();
                    }

                    let _sync_guard = match with_budget(GATE_BUDGET, self.sync_gate.write()).await {
                        Some(Ok(g)) => g,
                        _ => {
                            metrics::counter!("celeriant_coordinator_gate_timeout_total", "path" => "request_sync").increment(1);
                            let result: SyncResult<E> = Err(gate_timeout_err);
                            event.notify(result.clone());
                            return result;
                        }
                    };
                    let result = sync_fn().await;
                    drop(_sync_guard);

                    event.notify(result.clone());
                    return result;
                }
                Acquired::Follower(listener) => return listener.await,
                Acquired::Retry => continue,
            }
        }
    }

    /// Sync for callers that only need `satisfied()` to become true (the
    /// replication barrier: a bump needs a header on disk that covers it).
    ///
    /// Every data fsync writes dual headers, so any two-phase cycle that
    /// captures after our bump does our work for free. Before paying for our
    /// own fsync, wait on in-flight cycles and re-check `satisfied()` after
    /// each. A cycle that did no I/O (NoCaptureRaceButOk) just fails the
    /// re-check, which is fine. After MAX_RIDES, or when there is nothing to
    /// ride, take the gate and run `sync_fn` ourselves.
    ///
    /// This path never publishes an event of its own, so two-phase writers
    /// cannot ride us. Letting them ride a cycle with no capture step is a
    /// false ack.
    pub async fn request_sync_until<F, Fut, P>(
        &self,
        gate_timeout_err: E,
        sync_fn: F,
        satisfied: P,
    ) -> SyncResult<E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = SyncResult<E>>,
        P: Fn() -> bool,
    {
        const MAX_RIDES: u32 = 3;
        let mut rides = 0;
        loop {
            if satisfied() {
                return Ok(());
            }
            if rides >= MAX_RIDES {
                break;
            }
            let ride = match self.lock_orchestrator.try_write() {
                Ok(guard) => guard.as_ref().map(|event| event.listen()),
                Err(_) => match read_with_timeout(&self.lock_orchestrator, "request_sync_until_ride").await {
                    Ok(guard) => guard.as_ref().map(|event| event.listen()),
                    Err(_) => None,
                },
            };
            match ride {
                // Event present implies pre-capture: its sync_fn (and header
                // write) hasn't started, so it will include the caller's bump.
                // The listener is registered under the guard, atomic with
                // observing the event, so the leader's notify cannot be missed.
                Some(listener) => {
                    rides += 1;
                    let _ = listener.await;
                }
                None => break,
            }
        }

        let _sync_guard = match with_budget(GATE_BUDGET, self.sync_gate.write()).await {
            Some(Ok(g)) => g,
            _ => {
                metrics::counter!("celeriant_coordinator_gate_timeout_total", "path" => "request_sync_until").increment(1);
                return Err(gate_timeout_err);
            }
        };
        // A cycle that completed while we queued on the gate may have covered us.
        if satisfied() {
            return Ok(());
        }
        sync_fn().await
    }

    /// Amortised fsync in two phases: capture the pending work, then sync it.
    /// Followers wait on the leader's event and take its result.
    ///
    /// The invariant that matters: a follower is always waiting on the batch
    /// that captured its item. Capture and orchestrator-clear are atomic under
    /// the write lock, so a late writer attaches to the next leader, never to
    /// a snapshot that already missed it. Breaking that ordering is a false
    /// ack (an Ok for an item the batch never carried).
    pub async fn request_sync_two_phase<C, S, T, Fut2>(
        &self,
        delay: Option<Duration>,
        gate_timeout_err: E,
        capture_fn: C,
        sync_fn: S,
    ) -> SyncResult<E>
    where
        C: FnOnce() -> CaptureResult<T, E>,
        S: FnOnce(T) -> Fut2,
        Fut2: std::future::Future<Output = SyncResult<E>>,
    {
        let delay = match delay {
            Some(d) if d.as_micros() > 0 => d,
            _ => Duration::from_millis(0),
        };

        // Follower listeners registered under the guard, same as request_sync.
        enum Acquired<E: Clone> {
            Leader(Rc<LocalEvent<SyncResult<E>>>),
            Follower(LocalEventListener<SyncResult<E>>),
            Retry,
        }

        loop {
            let acquired = {
                match self.lock_orchestrator.try_write() {
                    Ok(mut guard) => match guard.as_ref() {
                        Some(event) => Acquired::Follower(event.listen()),
                        None => {
                            let event = Rc::new(LocalEvent::new());
                            *guard = Some(event.clone());
                            Acquired::Leader(event)
                        }
                    },
                    Err(_) => match read_with_timeout(&self.lock_orchestrator, "request_sync_two_phase_read").await {
                        Ok(guard) => match guard.as_ref() {
                            Some(event) => Acquired::Follower(event.listen()),
                            None => Acquired::Retry,
                        },
                        Err(_) => Acquired::Retry,
                    },
                }
            };

            match acquired {
                Acquired::Leader(event) => {
                    // Fast path: the server is idle, so sync now and skip the amortisation delay.
                    // The idle signal is `sync_gate.try_write()` succeeding: nobody else holds the
                    // gate, so there is no in-flight sync to batch with and nothing to wait for.
                    // Slow path: a sync holds the gate (we are under load). Sleep `delay` so more
                    // writers accumulate behind us, then queue for the gate; the gate wait itself
                    // also batches. (A momentarily-free gate between busy syncs may fast-path a small
                    // batch, acceptable; under sustained load the gate is held and we batch.)
                    //
                    // NOTE: do NOT gate this on `Rc::strong_count(&event)`. The orchestrator holds a
                    // clone of `event` (stored above) and followers attach via `event.listen()`,
                    // which clones the inner listeners Rc, not this outer one, so the count is a
                    // constant 2 here and tells you nothing. A past `== 1` check made the fast path
                    // dead (idle writes paid the full delay); `two_phase_fast_path_fires_when_idle`
                    // guards against that regressing again.
                    // Fast path requires a free gate AND that the previous cycle did NOT
                    // coalesce followers (we are not under load). A free gate alone is not
                    // idle: under sustained load it frees in brief windows between syncs, and
                    // fast-pathing a lone writer there fragments the batch — de-amortisation
                    // that raises latency under load. `two_phase_amortises_under_sustained_staggered_load`
                    // guards this; `two_phase_fast_path_fires_when_idle` guards the idle win.
                    let fast_guard = if self.last_two_phase_batched.get() {
                        None
                    } else {
                        self.sync_gate.try_write().ok()
                    };
                    let sync_guard_opt = match fast_guard {
                        Some(guard) => Some(guard),
                        None => {
                            glommio::timer::sleep(delay).await;
                            match with_budget(GATE_BUDGET, self.sync_gate.write()).await {
                                Some(Ok(g)) => Some(g),
                                _ => None,
                            }
                        }
                    };
                    let _sync_guard = match sync_guard_opt {
                        Some(g) => g,
                        None => {
                            metrics::counter!("celeriant_coordinator_gate_timeout_total", "path" => "two_phase").increment(1);
                            // Clear orchestrator before returning so the next leader can register.
                            if let Ok(mut guard) = write_with_timeout(&self.lock_orchestrator, "two_phase_clear_on_timeout").await {
                                guard.take();
                            }
                            let result: SyncResult<E> = Err(gate_timeout_err);
                            event.notify(result.clone());
                            return result;
                        }
                    };

                    // Phase 1: capture + clear the orchestrator atomically under the
                    // write lock. capture_fn is sync, so no writer can attach to our
                    // event after its item missed the snapshot. The false-ack window
                    // (writer attaches between capture and clear, then receives this
                    // batch's Ok for an item the batch never carried) is closed.
                    let captured = match write_with_timeout(&self.lock_orchestrator, "two_phase_capture_and_clear").await {
                        Ok(mut guard) => {
                            let captured = capture_fn();
                            guard.take();
                            captured
                        }
                        Err(_) => {
                            metrics::counter!("celeriant_coordinator_gate_timeout_total", "path" => "two_phase_orchestrator").increment(1);
                            drop(_sync_guard);
                            let result: SyncResult<E> = Err(gate_timeout_err);
                            event.notify(result.clone());
                            return result;
                        }
                    };

                    // Phase 2: Process captured data
                    let result = match captured {
                        CaptureResult::Captured(data) => sync_fn(data).await,
                        CaptureResult::Failed(e) => Err(e),
                        CaptureResult::NoCaptureRaceButOk => Ok(()),
                    };

                    drop(_sync_guard);
                    // Load signal for the next leader: did this cycle coalesce followers?
                    self.last_two_phase_batched.set(event.listener_count() > 0);
                    event.notify(result.clone());
                    return result;
                }
                Acquired::Follower(listener) => return listener.await,
                Acquired::Retry => continue,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::yield_now;
    use glommio::{LocalExecutorBuilder, Placement, spawn_local};
    use std::cell::{Cell, RefCell};
    use std::future::Future;

    fn run_with_glommio<G, F, T>(fut_gen: G)
    where
        G: FnOnce() -> F + Send + 'static,
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let builder = LocalExecutorBuilder::new(Placement::Unbound);
        let handle = builder.name("test").spawn(fut_gen).unwrap();
        handle.join().unwrap();
    }

    // ==================== Basic Functionality ====================

    #[test]
    fn single_request_succeeds() {
        run_with_glommio(|| async {
            let coordinator: Coordinator<String> = Coordinator::new();
            let call_count = Rc::new(Cell::new(0));

            let cc = call_count.clone();
            let result = coordinator
                .request_sync(Some(Duration::from_millis(1)), "gate-timeout".to_string(), || async move {
                    cc.set(cc.get() + 1);
                    Ok(())
                })
                .await;

            assert!(result.is_ok());
            assert_eq!(call_count.get(), 1);
        });
    }

    #[test]
    fn immediate_sync_no_delay() {
        run_with_glommio(|| async {
            let coordinator: Coordinator<String> = Coordinator::new();
            let call_count = Rc::new(Cell::new(0));

            // None delay
            let cc = call_count.clone();
            let result = coordinator
                .request_sync(None, "gate-timeout".to_string(), || async move {
                    cc.set(cc.get() + 1);
                    Ok(())
                })
                .await;
            assert!(result.is_ok());
            assert_eq!(call_count.get(), 1);

            // Zero duration delay
            let cc = call_count.clone();
            let result = coordinator
                .request_sync(Some(Duration::ZERO), "gate-timeout".to_string(), || async move {
                    cc.set(cc.get() + 1);
                    Ok(())
                })
                .await;
            assert!(result.is_ok());
            assert_eq!(call_count.get(), 2);
        });
    }

    #[test]
    fn immediate_sync_bypasses_coalescing() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let call_count = Rc::new(Cell::new(0));

            // Multiple immediate syncs should each call sync_fn
            let mut handles = vec![];
            for _ in 0..5 {
                let coord = coordinator.clone();
                let cc = call_count.clone();
                handles.push(spawn_local(async move {
                    coord
                        .request_sync(None, "gate-timeout".to_string(), || async move {
                            cc.set(cc.get() + 1);
                            Ok(())
                        })
                        .await
                }));
            }

            for h in handles {
                h.await.unwrap();
            }

            // Each immediate sync runs independently
            assert_eq!(call_count.get(), 5);
        });
    }

    // ==================== Leader/Follower Logic ====================

    #[test]
    fn first_caller_becomes_leader() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let leader_id = Rc::new(Cell::new(0u32));
            let sync_called_by = Rc::new(Cell::new(0u32));

            let coord = coordinator.clone();
            let lid = leader_id.clone();
            let scb = sync_called_by.clone();

            // First request - should become leader
            let leader_handle = spawn_local(async move {
                lid.set(1);
                coord
                    .request_sync(Some(Duration::from_millis(10)), "gate-timeout".to_string(), || async move {
                        scb.set(1);
                        Ok(())
                    })
                    .await
            });

            yield_now().await; // Let leader start

            let coord = coordinator.clone();
            let scb = sync_called_by.clone();

            // Second request - should become follower
            let follower_handle = spawn_local(async move {
                coord
                    .request_sync(Some(Duration::from_millis(10)), "gate-timeout".to_string(), || async move {
                        scb.set(2); // Should NOT be called
                        Ok(())
                    })
                    .await
            });

            leader_handle.await.unwrap();
            follower_handle.await.unwrap();

            // Only leader's sync_fn should have been called
            assert_eq!(sync_called_by.get(), 1);
        });
    }

    #[test]
    fn only_one_leader_per_batch() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let sync_count = Rc::new(Cell::new(0));
            let concurrent_syncs = Rc::new(Cell::new(0));
            let max_concurrent = Rc::new(Cell::new(0));

            let mut handles = vec![];

            for _ in 0..10 {
                let coord = coordinator.clone();
                let sc = sync_count.clone();
                let cs = concurrent_syncs.clone();
                let mc = max_concurrent.clone();

                handles.push(spawn_local(async move {
                    coord
                        .request_sync(Some(Duration::from_millis(5)), "gate-timeout".to_string(), || async move {
                            let current = cs.get() + 1;
                            cs.set(current);
                            if current > mc.get() {
                                mc.set(current);
                            }

                            // Simulate sync work
                            yield_now().await;

                            cs.set(cs.get() - 1);
                            sc.set(sc.get() + 1);
                            Ok(())
                        })
                        .await
                }));

                // Stagger the requests slightly
                yield_now().await;
            }

            for h in handles {
                h.await.unwrap();
            }

            // Should have exactly one sync call for the batch
            assert_eq!(sync_count.get(), 1);
            // Never more than one concurrent sync
            assert_eq!(max_concurrent.get(), 1);
        });
    }

    #[test]
    fn all_followers_receive_same_result() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let results: Rc<RefCell<Vec<SyncResult<String>>>> = Rc::new(RefCell::new(vec![]));

            let mut handles = vec![];

            for _ in 0..5 {
                let coord = coordinator.clone();
                let res = results.clone();

                handles.push(spawn_local(async move {
                    let result = coord.request_sync(Some(Duration::from_millis(5)), "gate-timeout".to_string(), || async { Ok(()) }).await;
                    res.borrow_mut().push(result);
                }));

                yield_now().await;
            }

            for h in handles {
                h.await;
            }

            let results = results.borrow();
            assert_eq!(results.len(), 5);
            // All results should be Ok
            for r in results.iter() {
                assert!(r.is_ok());
            }
        });
    }

    // ==================== Coalescing Behavior ====================

    #[test]
    fn requests_during_delay_are_batched() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let sync_count = Rc::new(Cell::new(0));
            let completed_count = Rc::new(Cell::new(0));

            let mut handles = vec![];

            // Spawn many requests quickly
            for _ in 0..20 {
                let coord = coordinator.clone();
                let sc = sync_count.clone();
                let cc = completed_count.clone();

                handles.push(spawn_local(async move {
                    coord
                        .request_sync(Some(Duration::from_millis(10)), "gate-timeout".to_string(), || async move {
                            sc.set(sc.get() + 1);
                            Ok(())
                        })
                        .await
                        .unwrap();
                    cc.set(cc.get() + 1);
                }));

                yield_now().await;
            }

            for h in handles {
                h.await;
            }

            // All 20 requests completed
            assert_eq!(completed_count.get(), 20);
            // But sync was only called once
            assert_eq!(sync_count.get(), 1);
        });
    }

    #[test]
    fn multiple_batches_over_time() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let sync_count = Rc::new(Cell::new(0));

            // First batch
            {
                let mut handles = vec![];
                for _ in 0..3 {
                    let coord = coordinator.clone();
                    let sc = sync_count.clone();
                    handles.push(spawn_local(async move {
                        coord
                            .request_sync(Some(Duration::from_millis(5)), "gate-timeout".to_string(), || async move {
                                sc.set(sc.get() + 1);
                                Ok(())
                            })
                            .await
                    }));
                    yield_now().await;
                }
                for h in handles {
                    h.await.unwrap();
                }
            }

            assert_eq!(sync_count.get(), 1);

            // Wait for first batch to fully complete
            glommio::timer::sleep(Duration::from_millis(10)).await;

            // Second batch
            {
                let mut handles = vec![];
                for _ in 0..3 {
                    let coord = coordinator.clone();
                    let sc = sync_count.clone();
                    handles.push(spawn_local(async move {
                        coord
                            .request_sync(Some(Duration::from_millis(5)), "gate-timeout".to_string(), || async move {
                                sc.set(sc.get() + 1);
                                Ok(())
                            })
                            .await
                    }));
                    yield_now().await;
                }
                for h in handles {
                    h.await.unwrap();
                }
            }

            // Two batches = two sync calls
            assert_eq!(sync_count.get(), 2);
        });
    }

    // ==================== Error Propagation ====================

    #[test]
    fn error_propagates_to_leader() {
        run_with_glommio(|| async {
            let coordinator: Coordinator<String> = Coordinator::new();

            let result = coordinator
                .request_sync(Some(Duration::from_millis(1)), "gate-timeout".to_string(), || async { Err("sync failed".to_string()) })
                .await;

            assert!(result.is_err());
            assert_eq!(result.unwrap_err(), "sync failed");
        });
    }

    #[test]
    fn error_propagates_to_all_followers() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let results: Rc<RefCell<Vec<SyncResult<String>>>> = Rc::new(RefCell::new(vec![]));

            let mut handles = vec![];

            for i in 0..5 {
                let coord = coordinator.clone();
                let res = results.clone();

                handles.push(spawn_local(async move {
                    let result = coord
                        .request_sync(Some(Duration::from_millis(5)), "gate-timeout".to_string(), || async move { Err(format!("error from task {}", i)) })
                        .await;
                    res.borrow_mut().push(result);
                }));

                yield_now().await;
            }

            for h in handles {
                h.await;
            }

            let results = results.borrow();
            assert_eq!(results.len(), 5);

            // All should have the same error (from leader, task 0)
            for r in results.iter() {
                assert!(r.is_err());
                assert_eq!(r.as_ref().unwrap_err(), "error from task 0");
            }
        });
    }

    #[test]
    fn error_does_not_affect_subsequent_batches() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let should_fail = Rc::new(Cell::new(true));

            // First batch - fails
            let coord = coordinator.clone();
            let sf = should_fail.clone();
            let result = coord
                .request_sync(Some(Duration::from_millis(1)), "gate-timeout".to_string(), || async move {
                    if sf.get() { Err("first failure".to_string()) } else { Ok(()) }
                })
                .await;
            assert!(result.is_err());

            // Second batch - succeeds
            should_fail.set(false);
            glommio::timer::sleep(Duration::from_millis(5)).await;

            let result = coordinator.request_sync(Some(Duration::from_millis(1)), "gate-timeout".to_string(), || async { Ok(()) }).await;
            assert!(result.is_ok());
        });
    }

    // ==================== Edge Cases ====================

    #[test]
    fn request_arriving_as_leader_finishes() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let sync_count = Rc::new(Cell::new(0));

            // Very short delay to create race condition
            let coord = coordinator.clone();
            let sc = sync_count.clone();
            let h1 = spawn_local(async move {
                coord
                    .request_sync(Some(Duration::from_micros(100)), "gate-timeout".to_string(), || async move {
                        sc.set(sc.get() + 1);
                        Ok(())
                    })
                    .await
            });

            // Wait almost until delay expires
            glommio::timer::sleep(Duration::from_micros(90)).await;

            // Request right at the edge
            let coord = coordinator.clone();
            let sc = sync_count.clone();
            let h2 = spawn_local(async move {
                coord
                    .request_sync(Some(Duration::from_micros(100)), "gate-timeout".to_string(), || async move {
                        sc.set(sc.get() + 1);
                        Ok(())
                    })
                    .await
            });

            h1.await.unwrap();
            h2.await.unwrap();

            // Either 1 or 2 syncs depending on timing, but both complete successfully
            assert!(sync_count.get() >= 1 && sync_count.get() <= 2);
        });
    }

    #[test]
    fn rapid_sequential_batches() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let sync_count = Rc::new(Cell::new(0));
            let total_completed = Rc::new(Cell::new(0));

            // Run many sequential single-request batches
            for _ in 0..10 {
                let coord = coordinator.clone();
                let sc = sync_count.clone();
                let tc = total_completed.clone();

                coord
                    .request_sync(Some(Duration::from_micros(100)), "gate-timeout".to_string(), || async move {
                        sc.set(sc.get() + 1);
                        Ok(())
                    })
                    .await
                    .unwrap();

                tc.set(tc.get() + 1);

                // Small gap between batches
                glommio::timer::sleep(Duration::from_micros(200)).await;
            }

            assert_eq!(total_completed.get(), 10);
            assert_eq!(sync_count.get(), 10); // Each was its own batch
        });
    }

    #[test]
    fn stress_test_concurrent_requests() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let sync_count = Rc::new(Cell::new(0));
            let completed = Rc::new(Cell::new(0));

            let mut handles = vec![];

            // Spawn 100 concurrent requests
            for _ in 0..100 {
                let coord = coordinator.clone();
                let sc = sync_count.clone();
                let cc = completed.clone();

                handles.push(spawn_local(async move {
                    coord
                        .request_sync(Some(Duration::from_millis(2)), "gate-timeout".to_string(), || async move {
                            sc.set(sc.get() + 1);
                            // Simulate some sync work
                            yield_now().await;
                            Ok(())
                        })
                        .await
                        .unwrap();
                    cc.set(cc.get() + 1);
                }));
            }

            for h in handles {
                h.await;
            }

            assert_eq!(completed.get(), 100);
            // Should have far fewer syncs than requests due to batching
            assert!(sync_count.get() < 100);
            assert!(sync_count.get() >= 1);
        });
    }

    #[test]
    fn follower_sync_fn_never_called() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let follower_sync_called = Rc::new(Cell::new(false));

            let coord = coordinator.clone();
            let leader = spawn_local(async move { coord.request_sync(Some(Duration::from_millis(10)), "gate-timeout".to_string(), || async { Ok(()) }).await });

            yield_now().await;

            // Multiple followers
            let mut followers = vec![];
            for _ in 0..5 {
                let coord = coordinator.clone();
                let fsc = follower_sync_called.clone();
                followers.push(spawn_local(async move {
                    coord
                        .request_sync(Some(Duration::from_millis(10)), "gate-timeout".to_string(), || async move {
                            fsc.set(true);
                            Ok(())
                        })
                        .await
                }));
                yield_now().await;
            }

            leader.await.unwrap();
            for f in followers {
                f.await.unwrap();
            }

            // Follower's sync_fn should never have been invoked
            assert!(!follower_sync_called.get());
        });
    }

    #[test]
    fn coordinator_reusable_after_completion() {
        run_with_glommio(|| async {
            let coordinator: Coordinator<String> = Coordinator::new();

            for i in 0..5 {
                let result = coordinator.request_sync(Some(Duration::from_millis(1)), "gate-timeout".to_string(), || async { Ok(()) }).await;
                assert!(result.is_ok(), "Iteration {} failed", i);

                glommio::timer::sleep(Duration::from_millis(5)).await;
            }
        });
    }

    #[test]
    fn sync_result_value_propagated() {
        run_with_glommio(|| async {
            // Test with a coordinator that returns actual values
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let results: Rc<RefCell<Vec<SyncResult<String>>>> = Rc::new(RefCell::new(vec![]));

            let mut handles = vec![];

            for i in 0..3 {
                let coord = coordinator.clone();
                let res = results.clone();

                handles.push(spawn_local(async move {
                    let result = coord
                        .request_sync(Some(Duration::from_millis(5)), "gate-timeout".to_string(), || async move {
                            // Only leader (first one) sets this
                            Ok(())
                        })
                        .await;
                    res.borrow_mut().push(result);
                }));

                if i == 0 {
                    yield_now().await; // Ensure first is leader
                }
            }

            for h in handles {
                h.await;
            }

            // All should have received Ok(())
            let results = results.borrow();
            assert_eq!(results.len(), 3);
            for r in results.iter() {
                assert!(r.is_ok());
            }
        });
    }

    // ==================== Invariant Verification ====================

    #[test]
    fn never_two_concurrent_leaders() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let active_leaders = Rc::new(Cell::new(0u32));
            let max_leaders = Rc::new(Cell::new(0u32));
            let violations = Rc::new(Cell::new(0u32));

            let mut handles = vec![];

            for _ in 0..50 {
                let coord = coordinator.clone();
                let al = active_leaders.clone();
                let ml = max_leaders.clone();
                let v = violations.clone();

                handles.push(spawn_local(async move {
                    coord
                        .request_sync(Some(Duration::from_millis(2)), "gate-timeout".to_string(), || async move {
                            let current = al.get() + 1;
                            al.set(current);

                            if current > ml.get() {
                                ml.set(current);
                            }
                            if current > 1 {
                                v.set(v.get() + 1);
                            }

                            // Hold the "leader" position for a bit
                            yield_now().await;
                            glommio::timer::sleep(Duration::from_micros(100)).await;

                            al.set(al.get() - 1);
                            Ok(())
                        })
                        .await
                }));

                yield_now().await;
            }

            for h in handles {
                h.await.unwrap();
            }

            assert_eq!(violations.get(), 0, "Had {} concurrent leader violations", violations.get());
            assert_eq!(max_leaders.get(), 1, "Max concurrent leaders was {}", max_leaders.get());
        });
    }

    #[test]
    fn all_waiters_complete_even_with_errors() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let completed_count = Rc::new(Cell::new(0));

            let mut handles = vec![];

            for _ in 0..10 {
                let coord = coordinator.clone();
                let cc = completed_count.clone();

                handles.push(spawn_local(async move {
                    let _ = coord
                        .request_sync(Some(Duration::from_millis(5)), "gate-timeout".to_string(), || async { Err("intentional error".to_string()) })
                        .await;
                    // This should still execute
                    cc.set(cc.get() + 1);
                }));

                yield_now().await;
            }

            for h in handles {
                h.await;
            }

            // All waiters should have completed, not hung
            assert_eq!(completed_count.get(), 10);
        });
    }

    // ==================== Two-Phase Capture Atomicity ====================

    /// Ok means your item committed in a successful batch. That's the
    /// false-ack invariant.
    ///
    /// The queue models the replication queue: capture drains it, a failed
    /// commit returns items (return_to_pending_replication), an empty
    /// capture is NoCaptureRaceButOk.
    #[test]
    fn ok_resolution_implies_item_committed() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let queue: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(vec![]));
            let committed: Rc<RefCell<std::collections::HashSet<u32>>> = Rc::new(RefCell::new(Default::default()));
            let batch_no = Rc::new(Cell::new(0u32));

            let mut handles = vec![];
            for id in 0..100u32 {
                let coord = coordinator.clone();
                let queue = queue.clone();
                let committed = committed.clone();
                let batch_no = batch_no.clone();
                handles.push(spawn_local(async move {
                    loop {
                        queue.borrow_mut().push(id);
                        let q = queue.clone();
                        let c = committed.clone();
                        let b = batch_no.clone();
                        let result = coord
                            .request_sync_two_phase(
                                Some(Duration::from_micros(200)),
                                "gate-timeout".to_string(),
                                move || {
                                    let items = std::mem::take(&mut *q.borrow_mut());
                                    if items.is_empty() {
                                        CaptureResult::NoCaptureRaceButOk
                                    } else {
                                        CaptureResult::Captured((items, q))
                                    }
                                },
                                move |(items, q)| async move {
                                    yield_now().await; // commit phase yields, like real replication
                                    let n = b.get() + 1;
                                    b.set(n);
                                    if n % 3 == 0 {
                                        // Failed batch: items go back, like return_to_pending_replication.
                                        q.borrow_mut().extend(items);
                                        return Err("batch failed".to_string());
                                    }
                                    c.borrow_mut().extend(items);
                                    Ok(())
                                },
                            )
                            .await;
                        match result {
                            Ok(()) => {
                                assert!(
                                    committed.borrow().contains(&id),
                                    "writer {id} resolved Ok but its item was never committed (false ack)"
                                );
                                return;
                            }
                            Err(_) => {
                                // Retry: drop our requeued copy first so the retry's push is the only one.
                                queue.borrow_mut().retain(|&x| x != id);
                                yield_now().await;
                            }
                        }
                    }
                }));
                if id % 7 == 0 {
                    yield_now().await; // stagger to vary batch composition
                }
            }

            for h in handles {
                h.await;
            }
            assert_eq!(committed.borrow().len(), 100);
        });
    }

    /// Barrier fsyncs (`request_sync`) and writer data fsyncs
    /// (`request_sync_two_phase`) used to share one orchestrator, so a writer
    /// could attach to a barrier cycle and take its Ok. Barrier cycles have
    /// no capture step. The writer's item was never processed.
    #[test]
    fn two_phase_writer_must_not_attach_to_single_phase_cycle() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let queue: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(vec![]));
            let committed: Rc<RefCell<std::collections::HashSet<u32>>> = Rc::new(RefCell::new(Default::default()));

            // Barrier cycle: registers its event, then sleeps its delay.
            let coord = coordinator.clone();
            let barrier = spawn_local(async move {
                coord
                    .request_sync(Some(Duration::from_millis(10)), "gate-timeout".to_string(), || async { Ok(()) })
                    .await
            });
            yield_now().await; // barrier registers and starts its delay sleep

            // Writer: pushes its item, then requests a two-phase data sync.
            let coord = coordinator.clone();
            let q = queue.clone();
            let c = committed.clone();
            let writer = spawn_local(async move {
                q.borrow_mut().push(7);
                let q2 = q.clone();
                let c2 = c.clone();
                coord
                    .request_sync_two_phase(
                        Some(Duration::from_millis(1)),
                        "gate-timeout".to_string(),
                        move || {
                            let items = std::mem::take(&mut *q2.borrow_mut());
                            if items.is_empty() {
                                CaptureResult::NoCaptureRaceButOk
                            } else {
                                CaptureResult::Captured(items)
                            }
                        },
                        move |items| async move {
                            c2.borrow_mut().extend(items);
                            Ok(())
                        },
                    )
                    .await
            });

            barrier.await.unwrap();
            let writer_result = writer.await;

            if writer_result.is_ok() {
                assert!(
                    committed.borrow().contains(&7),
                    "writer resolved Ok via the barrier cycle but its item was never captured (false ack)"
                );
            }
        });
    }

    // ==================== request_sync_until (barrier ride) ====================

    #[test]
    fn sync_until_rides_two_phase_cycle_without_own_sync() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let header_synced = Rc::new(Cell::new(false));
            let own_sync_ran = Rc::new(Cell::new(false));

            // Data cycle: writes "headers" as a side effect.
            let coord = coordinator.clone();
            let hs = header_synced.clone();
            let data_cycle = spawn_local(async move {
                coord
                    .request_sync_two_phase(
                        Some(Duration::from_millis(5)),
                        "gate-timeout".to_string(),
                        || CaptureResult::Captured(()),
                        move |_| async move {
                            hs.set(true);
                            Ok(())
                        },
                    )
                    .await
            });
            yield_now().await; // data cycle registers

            let hs = header_synced.clone();
            let osr = own_sync_ran.clone();
            let result = coordinator
                .request_sync_until(
                    "gate-timeout".to_string(),
                    move || async move {
                        osr.set(true);
                        Ok(())
                    },
                    move || hs.get(),
                )
                .await;

            data_cycle.await.unwrap();
            assert!(result.is_ok());
            assert!(header_synced.get(), "ride target must have run");
            assert!(!own_sync_ran.get(), "barrier must ride the data cycle, not fsync itself");
        });
    }

    #[test]
    fn sync_until_falls_through_when_ride_does_no_io() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let own_sync_ran = Rc::new(Cell::new(false));

            // Data cycle resolves NoCaptureRaceButOk: Ok with no I/O performed.
            let coord = coordinator.clone();
            let data_cycle = spawn_local(async move {
                coord
                    .request_sync_two_phase(
                        Some(Duration::from_millis(5)),
                        "gate-timeout".to_string(),
                        || CaptureResult::<(), String>::NoCaptureRaceButOk,
                        |_| async { Ok(()) },
                    )
                    .await
            });
            yield_now().await;

            let osr = own_sync_ran.clone();
            let osr_check = own_sync_ran.clone();
            let result = coordinator
                .request_sync_until(
                    "gate-timeout".to_string(),
                    move || async move {
                        osr.set(true);
                        Ok(())
                    },
                    move || osr_check.get(), // only our own sync satisfies
                )
                .await;

            data_cycle.await.unwrap();
            assert!(result.is_ok());
            assert!(own_sync_ran.get(), "a no-IO ride must not satisfy the barrier");
        });
    }

    #[test]
    fn sync_until_runs_own_sync_when_idle() {
        run_with_glommio(|| async {
            let coordinator: Coordinator<String> = Coordinator::new();
            let own_sync_ran = Rc::new(Cell::new(false));
            let osr = own_sync_ran.clone();
            let osr_check = own_sync_ran.clone();
            let result = coordinator
                .request_sync_until(
                    "gate-timeout".to_string(),
                    move || async move {
                        osr.set(true);
                        Ok(())
                    },
                    move || osr_check.get(),
                )
                .await;
            assert!(result.is_ok());
            assert!(own_sync_ran.get());
        });
    }

    #[test]
    fn sync_until_skips_everything_when_already_satisfied() {
        run_with_glommio(|| async {
            let coordinator: Coordinator<String> = Coordinator::new();
            let own_sync_ran = Rc::new(Cell::new(false));
            let osr = own_sync_ran.clone();
            let result = coordinator
                .request_sync_until(
                    "gate-timeout".to_string(),
                    move || async move {
                        osr.set(true);
                        Ok(())
                    },
                    || true,
                )
                .await;
            assert!(result.is_ok());
            assert!(!own_sync_ran.get());
        });
    }

    // ==================== Rollback Gate Tests ====================

    #[test]
    fn rollback_gate_blocks_new_syncs() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let sync_count = Rc::new(Cell::new(0));

            let gate = coordinator.acquire_rollback_lock().await;

            let mut handles = vec![];
            for _ in 0..5 {
                let coord = coordinator.clone();
                let sc = sync_count.clone();
                handles.push(spawn_local(async move {
                    coord
                        .request_sync(Some(Duration::from_millis(1)), "gate-timeout".to_string(), || async move {
                            sc.set(sc.get() + 1);
                            Ok(())
                        })
                        .await
                }));
            }

            glommio::timer::sleep(Duration::from_millis(20)).await;
            assert_eq!(sync_count.get(), 0);

            drop(gate);

            for h in handles {
                h.await.unwrap();
            }
            assert_eq!(sync_count.get(), 1); // All batched
        });
    }

    #[test]
    fn rollback_gate_drains_inflight_sync() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let sync_started = Rc::new(Cell::new(false));
            let sync_done = Rc::new(Cell::new(false));

            let coord = coordinator.clone();
            let started = sync_started.clone();
            let done = sync_done.clone();
            let sync_handle = spawn_local(async move {
                coord
                    .request_sync(Some(Duration::from_millis(1)), "gate-timeout".to_string(), || async move {
                        started.set(true);
                        glommio::timer::sleep(Duration::from_millis(50)).await;
                        done.set(true);
                        Ok(())
                    })
                    .await
            });

            while !sync_started.get() {
                yield_now().await;
            }

            let coord = coordinator.clone();
            let done_check = sync_done.clone();
            let drain_handle = spawn_local(async move {
                let _guard = coord.acquire_rollback_lock().await;
                assert!(done_check.get()); // Sync must finish before we acquire
            });

            sync_handle.await.unwrap();
            drain_handle.await;
        });
    }

    // ==================== Two-Phase Fast Path ====================

    /// Regression guard for the idle fast path. An idle two-phase sync (gate free) must
    /// skip the amortisation delay entirely. A broken predicate (the historical
    /// `Rc::strong_count(&event) == 1`, which never matched because the orchestrator holds
    /// a clone) makes every idle sync pay the full delay. This asserts it does not.
    #[test]
    fn two_phase_fast_path_fires_when_idle() {
        run_with_glommio(|| async {
            let coordinator: Coordinator<String> = Coordinator::new();
            let committed = Rc::new(Cell::new(false));
            // Deliberately large: if the fast path fires we never sleep it; if it regresses
            // we block for ~1s and the elapsed assert fails.
            let delay = Duration::from_secs(1);

            let c = committed.clone();
            let start = std::time::Instant::now();
            let result = coordinator
                .request_sync_two_phase(
                    Some(delay),
                    "gate-timeout".to_string(),
                    || CaptureResult::Captured(()),
                    move |_| async move {
                        c.set(true);
                        Ok(())
                    },
                )
                .await;
            let elapsed = start.elapsed();

            assert!(result.is_ok());
            assert!(committed.get(), "fast path must still capture and commit");
            assert!(
                elapsed < Duration::from_millis(300),
                "idle two-phase sync took {elapsed:?}: the fast path is not firing; it must \
                 skip the {delay:?} amortisation delay when the gate is free"
            );
        });
    }

    /// When the gate is held (a sync in flight / rollback), the fast path's `try_write` must
    /// fail and the caller takes the slow path: it waits for the gate, then still captures.
    #[test]
    fn two_phase_takes_slow_path_when_gate_busy() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let committed = Rc::new(Cell::new(false));

            // Hold the gate so try_write() cannot succeed.
            let gate = coordinator.acquire_rollback_lock().await;

            let coord = coordinator.clone();
            let c = committed.clone();
            let writer = spawn_local(async move {
                coord
                    .request_sync_two_phase(
                        Some(Duration::from_millis(5)),
                        "gate-timeout".to_string(),
                        || CaptureResult::Captured(()),
                        move |_| async move {
                            c.set(true);
                            Ok(())
                        },
                    )
                    .await
            });

            glommio::timer::sleep(Duration::from_millis(20)).await;
            assert!(!committed.get(), "must not sync while the gate is held");

            drop(gate);
            let result = writer.await;
            assert!(result.is_ok());
            assert!(committed.get(), "slow path must capture once the gate frees");
        });
    }

    /// The fast path takes the gate via `try_write`, so it must still serialize: even under
    /// concurrent idle writers, no two sync_fn bodies run at once.
    #[test]
    fn two_phase_fast_path_still_serializes_through_gate() {
        run_with_glommio(|| async {
            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let concurrent = Rc::new(Cell::new(0i32));
            let max_concurrent = Rc::new(Cell::new(0i32));
            let completed = Rc::new(Cell::new(0));

            let mut handles = vec![];
            for _ in 0..20 {
                let coord = coordinator.clone();
                let cur = concurrent.clone();
                let mx = max_concurrent.clone();
                let done = completed.clone();
                handles.push(spawn_local(async move {
                    let cur2 = cur.clone();
                    let mx2 = mx.clone();
                    let r = coord
                        .request_sync_two_phase(
                            Some(Duration::from_millis(1)),
                            "gate-timeout".to_string(),
                            || CaptureResult::Captured(()),
                            move |_| async move {
                                let n = cur2.get() + 1;
                                cur2.set(n);
                                if n > mx2.get() {
                                    mx2.set(n);
                                }
                                yield_now().await; // hold the in-sync window open
                                cur2.set(cur2.get() - 1);
                                Ok(())
                            },
                        )
                        .await;
                    assert!(r.is_ok());
                    done.set(done.get() + 1);
                }));
            }
            for h in handles {
                h.await;
            }

            assert_eq!(completed.get(), 20);
            assert_eq!(max_concurrent.get(), 1, "gate must serialize syncs even on the fast path");
        });
    }

    /// Amortisation regression guard. Under SUSTAINED, STAGGERED concurrent load the
    /// coordinator must coalesce writers into shared syncs. The risk is the opposite:
    /// a fast path that fires whenever the gate is momentarily free de-amortises —
    /// it issues tiny batches per arrival, pushing the sync count toward one-per-request.
    /// That is the low-concurrency latency regression in disguise (a sync-count regime,
    /// observable without wall-clock timing). Lockstep arrival hides it (every version
    /// batches cleanly), so writers arrive with independent jitter. Ideal coalescing is
    /// one sync per round (`REQUESTS` total); we assert the count stays well under that,
    /// which the de-amortising fast path cannot satisfy.
    #[test]
    fn two_phase_amortises_under_sustained_staggered_load() {
        run_with_glommio(|| async {
            const WRITERS: usize = 32;
            const REQUESTS: usize = 20; // per writer
            const JITTER_MAX_US: u64 = 1000;
            // Coalescing window wide vs likely preemption: this test runs in parallel
            // with the rest of the suite, so a few-hundred-µs steal must not collapse the
            // batch. Ideal stays one sync per round (REQUESTS); the de-amortising fast
            // path roughly doubles it (~1.6x), which the threshold below catches.
            let delay = Duration::from_micros(3000);
            let fsync_sim = Duration::from_micros(200);

            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let syncs = Rc::new(Cell::new(0u64));

            let mut handles = vec![];
            for w in 0..WRITERS {
                let coord = coordinator.clone();
                let syncs = syncs.clone();
                handles.push(spawn_local(async move {
                    // Deterministic per-writer LCG: reproducible jitter so the guard does
                    // not move because the randomness moved.
                    let mut rng = (w as u64).wrapping_add(1);
                    for _ in 0..REQUESTS {
                        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                        let jitter = (rng >> 33) % JITTER_MAX_US;
                        if jitter > 0 {
                            glommio::timer::sleep(Duration::from_micros(jitter)).await;
                        }
                        let syncs = syncs.clone();
                        let r = coord
                            .request_sync_two_phase(
                                Some(delay),
                                "gate-timeout".to_string(),
                                || CaptureResult::Captured(()),
                                move |_| async move {
                                    syncs.set(syncs.get() + 1);
                                    glommio::timer::sleep(fsync_sim).await;
                                    Ok(())
                                },
                            )
                            .await;
                        assert!(r.is_ok());
                    }
                }));
            }
            for h in handles {
                h.await;
            }

            let total = (WRITERS * REQUESTS) as u64;
            let n_syncs = syncs.get();
            // Ideal is one sync per round = REQUESTS (+1 for the initial idle fast path).
            // Allow 40% slack for parallel-test preemption; a de-amortising fast path
            // roughly doubles the count and clears this comfortably.
            let max_syncs = (REQUESTS as u64 * 14) / 10;
            assert!(
                n_syncs <= max_syncs,
                "de-amortisation: {n_syncs} syncs for {total} requests across {WRITERS} writers \
                 (ideal ~{REQUESTS}, allowed ≤{max_syncs}); the fast path is firing tiny batches \
                 under load instead of coalescing"
            );
        });
    }

    /// A failed capture returns the error without running sync_fn, and must release both the
    /// orchestrator and the gate so the coordinator is immediately reusable.
    #[test]
    fn two_phase_capture_failed_propagates_and_recovers() {
        run_with_glommio(|| async {
            let coordinator: Coordinator<String> = Coordinator::new();

            let sync_ran = Rc::new(Cell::new(false));
            let sr = sync_ran.clone();
            let result = coordinator
                .request_sync_two_phase(
                    Some(Duration::from_millis(1)),
                    "gate-timeout".to_string(),
                    || CaptureResult::<(), String>::Failed("capture failed".to_string()),
                    move |_| async move {
                        sr.set(true);
                        Ok(())
                    },
                )
                .await;
            assert_eq!(result.unwrap_err(), "capture failed");
            assert!(!sync_ran.get(), "sync_fn must not run when capture fails");

            // Reusable: a clean cycle must work right after the Failed cycle.
            let committed = Rc::new(Cell::new(false));
            let c = committed.clone();
            let result = coordinator
                .request_sync_two_phase(
                    Some(Duration::from_millis(1)),
                    "gate-timeout".to_string(),
                    || CaptureResult::Captured(()),
                    move |_| async move {
                        c.set(true);
                        Ok(())
                    },
                )
                .await;
            assert!(result.is_ok());
            assert!(committed.get(), "coordinator must recover after a failed capture");
        });
    }

    /// Single-phase (barrier) and two-phase (data) cycles use separate orchestrators but share
    /// the one `sync_gate`. The invariant (coordinator.rs comment on `sync_gate`) is that their
    /// sync bodies never overlap. Interleave both kinds under concurrency and assert it holds.
    #[test]
    fn single_and_two_phase_never_overlap_on_gate() {
        run_with_glommio(|| async {
            async fn hold_and_track(cur: Rc<Cell<i32>>, mx: Rc<Cell<i32>>) -> SyncResult<String> {
                let n = cur.get() + 1;
                cur.set(n);
                if n > mx.get() {
                    mx.set(n);
                }
                yield_now().await;
                glommio::timer::sleep(Duration::from_millis(2)).await;
                cur.set(cur.get() - 1);
                Ok(())
            }

            let coordinator: Rc<Coordinator<String>> = Rc::new(Coordinator::new());
            let concurrent = Rc::new(Cell::new(0i32));
            let max_concurrent = Rc::new(Cell::new(0i32));

            let mut handles = vec![];
            for i in 0..20 {
                let coord = coordinator.clone();
                let cur = concurrent.clone();
                let mx = max_concurrent.clone();
                handles.push(spawn_local(async move {
                    if i % 2 == 0 {
                        coord
                            .request_sync(Some(Duration::from_millis(1)), "gt".to_string(), move || hold_and_track(cur, mx))
                            .await
                    } else {
                        coord
                            .request_sync_two_phase(
                                Some(Duration::from_millis(1)),
                                "gt".to_string(),
                                || CaptureResult::Captured(()),
                                move |_| hold_and_track(cur, mx),
                            )
                            .await
                    }
                }));
                yield_now().await;
            }
            for h in handles {
                h.await.unwrap();
            }

            assert_eq!(max_concurrent.get(), 1, "single- and two-phase syncs must never run at once (shared gate)");
        });
    }
}
