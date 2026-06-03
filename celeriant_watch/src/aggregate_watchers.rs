use std::{cell::{Cell, RefCell}, collections::HashMap, rc::Rc};

use celeriant_msg::request::requests::WatchRequest;
use celeriant_wal::aggregate_key::AggregateKey;

use crate::{aggregate_watch_event::{AggregateWatchEvent, AggregateWatchEventOperation}, subscribed_client::SubscribedClient, watcher_handle::WatcherHandle};


pub struct AggregateWatchers {
    next_id: Cell<u64>,
    watcher_handles: RefCell<Vec<WatcherHandle>>,
}

impl AggregateWatchers {
    pub fn new() -> Self {
        Self {
            next_id: Cell::new(0),
            watcher_handles: RefCell::new(Vec::new()),
        }
    }

    /// Current number of active subscribers on this shard.
    pub fn subscriber_count(&self) -> usize {
        self.watcher_handles.borrow().len()
    }

    /// Like [`add_subscriber`] but refuses once the shard already holds
    /// `max_subscribers`. Each subscription eagerly reserves a ~0.8 MB event
    /// ring that is off the per-shard memory budget, so an uncapped subscriber
    /// count is an unbounded (and adversarially reachable) memory growth path.
    /// Returns `None` when at capacity; the caller maps that to a client error.
    pub fn add_subscriber_capped(
        &self,
        request: WatchRequest,
        max_subscribers: usize,
    ) -> Option<(u64, Rc<RefCell<SubscribedClient>>)> {
        if self.watcher_handles.borrow().len() >= max_subscribers {
            return None;
        }
        Some(self.add_subscriber(request))
    }

    /// Creates a unique monotonically increasing ID for a subscriber so it can remove itself
    /// and returns the SubscribedClient for the shard to receive events through the channel
    pub fn add_subscriber(&self,
        request: WatchRequest,
    ) -> (u64, Rc<RefCell<SubscribedClient>>) {
        let id = self.next_id.get();
        self.next_id.set(id.wrapping_add(1));
        
        let (client, local_sender_channel) = SubscribedClient::new(
            request.requested_latency_ms,
        );
        let subscribed_client = Rc::new(RefCell::new(client));
        
        self.watcher_handles.borrow_mut().push(WatcherHandle {
            id,
            local_sender_channel,
            orgs: request.orgs,
            aggregate_types: request.aggregate_types,
            aggregates: request.aggregates,
            operation_types: request.operation_types,
        });
        
        (id, subscribed_client)
    }

    /// Remove a subscriber by ID
    pub fn remove_subscriber(&self, id: u64) {
        self.watcher_handles.borrow_mut().retain(|h| h.id != id);
    }

    /// Check if there are any active subscribers
    pub fn is_empty(&self) -> bool {
        self.watcher_handles.borrow().is_empty()
    }

    /// Broadcast write events to watchers after durable sync.
    pub fn notify_watchers(
        &self,
        events: HashMap<AggregateKey, AggregateWatchEventOperation>,
    ) {
        for (aggregate_key, operation) in events {
            self.broadcast(AggregateWatchEvent {
                aggregate_key: aggregate_key.clone(),
                operation,
            });
        }
    }

    /// Broadcast an event to all subscribers
    /// In the WRITE hot path for the WAL, we just try send into channels and dont await
    /// If channel is full, remove the client as they are not keeping up
    pub fn broadcast(&self, event: AggregateWatchEvent) {
        let event_type = event.operation_as_u8();
        let mut handles = self.watcher_handles.borrow_mut();
        handles.retain(|handle| handle.notify_of_event(event_type, &event));
    }
}