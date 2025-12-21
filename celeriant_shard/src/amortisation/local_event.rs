use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

type SharedListeners<T> = Rc<RefCell<BTreeMap<u64, ListenerState<T>>>>;

struct ListenerState<T> {
    waker: Option<Waker>,
    result: Option<T>,
}

pub struct LocalEventListener<T> {
    id: u64,
    listeners: SharedListeners<T>,
}

impl<T: Clone> Future for LocalEventListener<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut listeners = self.listeners.borrow_mut();

        if let Some(state) = listeners.get(&self.id) {
            if let Some(result) = &state.result {
                let result = result.clone();
                listeners.remove(&self.id);
                return Poll::Ready(result);
            }
        }

        listeners.insert(
            self.id,
            ListenerState {
                waker: Some(cx.waker().clone()),
                result: None,
            },
        );

        Poll::Pending
    }
}

/// An async event that is optimized for single thread use.
pub struct LocalEvent<T = ()> {
    listeners: SharedListeners<T>,
    last_id: Cell<u64>,
}

impl<T> Default for LocalEvent<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> LocalEvent<T> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            listeners: Rc::new(RefCell::new(BTreeMap::new())),
            last_id: Cell::new(0),
        }
    }

    pub fn listen(&self) -> LocalEventListener<T> {
        let mut listeners = self.listeners.borrow_mut();
        let id = self.last_id.get();

        // Potential bug when an event has a listener with an id that already
        // exists. Because I don't expect there to be so many listeners on a
        // single event that exist for so long, I won't handle it.
        self.last_id.set(id.wrapping_add(1));

        listeners.insert(
            id,
            ListenerState {
                waker: None,
                result: None,
            },
        );

        LocalEventListener {
            id,
            listeners: self.listeners.clone(),
        }
    }

    pub fn notify(&self, result: T)
    where
        T: Clone,
    {
        let mut listeners = self.listeners.borrow_mut();
        for listener in listeners.values_mut() {
            listener.result = Some(result.clone());
            if let Some(waker) = listener.waker.take() {
                waker.wake();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use futures_lite::future::yield_now;
    use glommio::{spawn_local, LocalExecutorBuilder, Placement};

    use super::*;

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

    #[test]
    fn sticky_event() {
        run_with_glommio(|| async {
            let event = LocalEvent::new();
            let listener = event.listen();
            event.notify(());
            listener.await;
        });
    }

    #[test]
    fn sanity_event() {
        run_with_glommio(|| async {
            let set = Rc::new(Cell::new(false));
            let event = LocalEvent::new();
            let listener = event.listen();

            let cloned_set = set.clone();
            spawn_local(async move {
                yield_now().await;
                cloned_set.set(true);
                event.notify(());
            })
            .detach();

            listener.await;
            assert!(set.get());
        });
    }

    #[test]
    fn reuse_event() {
        run_with_glommio(|| async {
            let event = LocalEvent::new();

            let listener1 = event.listen();
            let listener2 = event.listen();

            event.notify(());
            listener1.await;

            let listener3 = event.listen();

            event.notify(());
            listener3.await;
            listener2.await;
        });
    }

    #[test]
    fn event_with_result() {
        run_with_glommio(|| async {
            let event: LocalEvent<Result<i32, String>> = LocalEvent::new();
            
            let listener1 = event.listen();
            let listener2 = event.listen();
            
            event.notify(Ok(42));
            
            assert_eq!(listener1.await, Ok(42));
            assert_eq!(listener2.await, Ok(42));
        });
    }

    #[test]
    fn event_with_error() {
        run_with_glommio(|| async {
            let event: LocalEvent<Result<(), String>> = LocalEvent::new();
            
            let listener1 = event.listen();
            let listener2 = event.listen();
            
            event.notify(Err("sync failed".to_string()));
            
            assert_eq!(listener1.await, Err("sync failed".to_string()));
            assert_eq!(listener2.await, Err("sync failed".to_string()));
        });
    }
}