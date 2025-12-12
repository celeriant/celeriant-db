use std::{cell::{Cell, RefCell}, rc::Rc};

use celeriant_msg::request::requests::WatchRequest;
use glommio::channels::local_channel::{LocalSender};

use crate::watch::{aggregate_watch_event::AggregateWatchEvent, subscribed_client::{SubscribedClient}};

pub struct WatcherHandle {
    pub id: u64,
    pub subscribe_to_event_types: Vec<u8>,
    pub sender: LocalSender<AggregateWatchEvent>,
    pub subscribed_client: Rc<RefCell<SubscribedClient>>,
}

pub struct AggregateWatchers {
    next_id: Cell<u64>,
    senders: RefCell<Vec<WatcherHandle>>,
}

impl AggregateWatchers {
    pub fn new() -> Self {
        Self {
            next_id: Cell::new(0),
            senders: RefCell::new(Vec::new()),
        }
    }

    /// Add a new subscriber, returns (id, receiver) for the subscriber's task
    pub fn add_subscriber(&self, 
        request: WatchRequest,
        max_response_size: Option<usize>,
    ) -> (u64, Rc<RefCell<SubscribedClient>>) {
        let id = self.next_id.get();
        self.next_id.set(id.wrapping_add(1));
        
        let (client, sender) = SubscribedClient::new(
            request.requested_latency_ms,
            request.requested_throughput_bs,
            max_response_size,
        );
        let subscribed_client = Rc::new(RefCell::new(client));
        
        self.senders.borrow_mut().push(WatcherHandle {
            id,
            sender,
            subscribe_to_event_types: request.subscribe_to_event_types,
            subscribed_client: subscribed_client.clone(),
        });
        
        (id, subscribed_client)
    }

    /// Remove a subscriber by ID
    pub fn remove_subscriber(&self, id: u64) {
        self.senders.borrow_mut().retain(|h| h.id != id);
    }

    /// Check if there are any active subscribers
    pub fn is_empty(&self) -> bool {
        self.senders.borrow().is_empty()
    }

    /// Broadcast an event to all subscribers
    pub fn broadcast(&self, event: AggregateWatchEvent) {
        let event_type = event.to_u8();
        let senders = self.senders.borrow();
        for handle in senders.iter() {
            if handle.subscribe_to_event_types.contains(&event_type) {
                // Use try_send - if a subscriber's channel is full or closed, skip it
                let _ = handle.sender.try_send(event.clone());
            }
        }
    }

    /// Get the number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.senders.borrow().len()
    }
}

impl Default for AggregateWatchers {
    fn default() -> Self {
        Self::new()
    }
}