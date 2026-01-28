use crate::amortisation::local_event::LocalEvent;
use celeriant_disk::files::rwlock_timeout::{read_with_timeout, write_with_timeout};
use glommio::sync::RwLock;
use std::{rc::Rc, time::Duration};

/// Result of a sync operation.
pub type SyncResult<E> = Result<(), E>;

/// Result of a capture operation in two-phase sync.
pub enum CaptureResult<T, E: Clone> {
    /// Data was captured, proceed with sync.
    Captured(T),
    /// Capture failed (e.g., rollback occurred, queue was emptied).
    Failed(E),
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
/// let coordinator = SyncCoordinator::new();
///
/// // Called from multiple concurrent writers
/// coordinator.request_sync(
///     Some(Duration::from_millis(5)),
///     || async { do_fsync().await }
/// ).await?;
/// ```
pub struct Coordinator<E: Clone> {
    lock_orchestrator: RwLock<Option<Rc<LocalEvent<SyncResult<E>>>>>,

    /// Sync gate: serializes sync execution and supports rollback drain/gate.
    /// Fsync and rollback both acquire write lock - ensures one-at-a-time execution.
    sync_gate: RwLock<()>,
}

impl<E: Clone> Coordinator<E> {
    pub fn new() -> Self {
        Self {
            lock_orchestrator: RwLock::new(None),
            sync_gate: RwLock::new(()),
        }
    }

    /// Acquire rollback lock. Waits for any in-flight fsync to complete,
    /// then blocks new fsyncs until the guard is dropped.
    pub async fn acquire_rollback_lock(&self) -> Option<glommio::sync::RwLockWriteGuard<'_, ()>> {
        write_with_timeout(&self.sync_gate, "acquire_rollback_lock").await.ok()
    }

    pub async fn request_sync<F, Fut>(&self, delay: Option<Duration>, sync_fn: F) -> SyncResult<E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = SyncResult<E>>,
    {
        let delay = match delay {
            Some(d) if d.as_micros() > 0 => d,
            _ => Duration::from_millis(0),
        };

        enum Acquired<E: Clone> {
            Leader(Rc<LocalEvent<SyncResult<E>>>),
            Follower(Rc<LocalEvent<SyncResult<E>>>),
            Retry,
        }

        loop {
            let acquired = {
                match self.lock_orchestrator.try_write() {
                    Ok(mut guard) => match guard.as_ref() {
                        Some(event) => Acquired::Follower(event.clone()),
                        None => {
                            let event = Rc::new(LocalEvent::new());
                            *guard = Some(event.clone());
                            Acquired::Leader(event)
                        }
                    },
                    Err(_) => match read_with_timeout(&self.lock_orchestrator, "request_sync_read_be_follower").await {
                        Ok(guard) => match guard.as_ref() {
                            Some(event) => Acquired::Follower(event.clone()),
                            None => Acquired::Retry,
                        },
                        Err(_) => Acquired::Retry,
                    },
                }
            }; // Guards dropped here automatically

            match acquired {
                Acquired::Leader(event) => {
                    glommio::timer::sleep(delay).await;

                    if let Ok(mut guard) = write_with_timeout(&self.lock_orchestrator, "request_sync_clear_orchestrator").await {
                        guard.take();
                    }

                    let _sync_guard = write_with_timeout(&self.sync_gate, "fsync_sync_gate").await.ok();
                    let result = sync_fn().await;
                    drop(_sync_guard);

                    event.notify(result.clone());
                    return result;
                }
                Acquired::Follower(event) => return event.listen().await,
                Acquired::Retry => continue,
            }
        }
    }

    /// Two-phase sync: capture snapshot first, then commit.
    ///
    /// This fixes a race condition in the single-phase API where clearing the orchestrator
    /// before taking the snapshot can cause a subsequent leader to find an empty queue
    /// (because the previous leader's snapshot captured their items).
    ///
    /// The fix: take snapshot FIRST (while orchestrator still has event), THEN clear orchestrator.
    /// This ensures followers are correctly associated with the batch that captures their items.
    pub async fn request_sync_two_phase<C, S, T, Fut1, Fut2>(
        &self,
        delay: Option<Duration>,
        capture_fn: C,
        sync_fn: S,
    ) -> SyncResult<E>
    where
        C: FnOnce() -> Fut1,
        Fut1: std::future::Future<Output = CaptureResult<T, E>>,
        S: FnOnce(T) -> Fut2,
        Fut2: std::future::Future<Output = SyncResult<E>>,
    {
        let delay = match delay {
            Some(d) if d.as_micros() > 0 => d,
            _ => Duration::from_millis(0),
        };

        enum Acquired<E: Clone> {
            Leader(Rc<LocalEvent<SyncResult<E>>>),
            Follower(Rc<LocalEvent<SyncResult<E>>>),
            Retry,
        }

        loop {
            let acquired = {
                match self.lock_orchestrator.try_write() {
                    Ok(mut guard) => match guard.as_ref() {
                        Some(event) => Acquired::Follower(event.clone()),
                        None => {
                            let event = Rc::new(LocalEvent::new());
                            *guard = Some(event.clone());
                            Acquired::Leader(event)
                        }
                    },
                    Err(_) => match read_with_timeout(&self.lock_orchestrator, "request_sync_two_phase_read").await {
                        Ok(guard) => match guard.as_ref() {
                            Some(event) => Acquired::Follower(event.clone()),
                            None => Acquired::Retry,
                        },
                        Err(_) => Acquired::Retry,
                    },
                }
            };

            match acquired {
                Acquired::Leader(event) => {
                    glommio::timer::sleep(delay).await;

                    // Acquire sync gate BEFORE capture - serializes sync execution
                    let _sync_guard = write_with_timeout(&self.sync_gate, "two_phase_sync_gate").await.ok();

                    // Phase 1: Capture snapshot FIRST (while orchestrator still has our event)
                    // Any writer that arrives now sees our event and becomes a follower
                    let captured = capture_fn().await;

                    // NOW clear orchestrator - new leaders can only start for items added AFTER capture
                    if let Ok(mut guard) = write_with_timeout(&self.lock_orchestrator, "two_phase_clear_orchestrator").await {
                        guard.take();
                    }

                    // Phase 2: Process captured data
                    let result = match captured {
                        CaptureResult::Captured(data) => sync_fn(data).await,
                        CaptureResult::Failed(e) => Err(e),
                        CaptureResult::NoCaptureRaceButOk => Ok(()),
                    };

                    drop(_sync_guard);
                    event.notify(result.clone());
                    return result;
                }
                Acquired::Follower(event) => return event.listen().await,
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
                .request_sync(Some(Duration::from_millis(1)), || async move {
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
                .request_sync(None, || async move {
                    cc.set(cc.get() + 1);
                    Ok(())
                })
                .await;
            assert!(result.is_ok());
            assert_eq!(call_count.get(), 1);

            // Zero duration delay
            let cc = call_count.clone();
            let result = coordinator
                .request_sync(Some(Duration::ZERO), || async move {
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
                        .request_sync(None, || async move {
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
                    .request_sync(Some(Duration::from_millis(10)), || async move {
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
                    .request_sync(Some(Duration::from_millis(10)), || async move {
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
                        .request_sync(Some(Duration::from_millis(5)), || async move {
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
                    let result = coord.request_sync(Some(Duration::from_millis(5)), || async { Ok(()) }).await;
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
                        .request_sync(Some(Duration::from_millis(10)), || async move {
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
                            .request_sync(Some(Duration::from_millis(5)), || async move {
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
                            .request_sync(Some(Duration::from_millis(5)), || async move {
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
                .request_sync(Some(Duration::from_millis(1)), || async { Err("sync failed".to_string()) })
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
                        .request_sync(Some(Duration::from_millis(5)), || async move { Err(format!("error from task {}", i)) })
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
                .request_sync(Some(Duration::from_millis(1)), || async move {
                    if sf.get() { Err("first failure".to_string()) } else { Ok(()) }
                })
                .await;
            assert!(result.is_err());

            // Second batch - succeeds
            should_fail.set(false);
            glommio::timer::sleep(Duration::from_millis(5)).await;

            let result = coordinator.request_sync(Some(Duration::from_millis(1)), || async { Ok(()) }).await;
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
                    .request_sync(Some(Duration::from_micros(100)), || async move {
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
                    .request_sync(Some(Duration::from_micros(100)), || async move {
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
                    .request_sync(Some(Duration::from_micros(100)), || async move {
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
                        .request_sync(Some(Duration::from_millis(2)), || async move {
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
            let leader = spawn_local(async move { coord.request_sync(Some(Duration::from_millis(10)), || async { Ok(()) }).await });

            yield_now().await;

            // Multiple followers
            let mut followers = vec![];
            for _ in 0..5 {
                let coord = coordinator.clone();
                let fsc = follower_sync_called.clone();
                followers.push(spawn_local(async move {
                    coord
                        .request_sync(Some(Duration::from_millis(10)), || async move {
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
                let result = coordinator.request_sync(Some(Duration::from_millis(1)), || async { Ok(()) }).await;
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
                        .request_sync(Some(Duration::from_millis(5)), || async move {
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
                        .request_sync(Some(Duration::from_millis(2)), || async move {
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
                        .request_sync(Some(Duration::from_millis(5)), || async { Err("intentional error".to_string()) })
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
                        .request_sync(Some(Duration::from_millis(1)), || async move {
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
                    .request_sync(Some(Duration::from_millis(1)), || async move {
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
}
