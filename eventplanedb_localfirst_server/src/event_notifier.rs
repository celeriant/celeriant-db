use eventplanedb_storage_stateful::aggregate_key::AggregateKey;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct EventNotifier {
    channels: Arc<Mutex<HashMap<AggregateKey, broadcast::Sender<u128>>>>,
}

impl EventNotifier {
    pub fn new() -> Self {
        EventNotifier {
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // Get or create a channel for a specific file path
    pub fn subscribe(&self, aggregate_key: AggregateKey) -> broadcast::Receiver<u128> {
        let mut channels = self.channels.lock().unwrap();

        // Get or create the channel
        let sender = channels.entry(aggregate_key).or_insert_with(|| {
            // Create a new broadcast channel with capacity for 100 messages
            // TODO: What should we set this capacity to?
            let (tx, _) = broadcast::channel(100);
            tx
        });

        // Clone the sender to create a new receiver
        sender.subscribe()
    }

    // Notify all subscribers for a specific file path
    pub fn notify(&self, aggregate_key: &AggregateKey, client_id: u128) {
        let channels = self.channels.lock().unwrap();

        if let Some(sender) = channels.get(aggregate_key) {
            // Send the user hash that caused this notification
            let _ = sender.send(client_id);
        }
    }
}
