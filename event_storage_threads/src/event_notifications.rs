use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct EventNotifier {
    channels: Arc<Mutex<HashMap<String, broadcast::Sender<String>>>>, // Changed to send String (user hash)
}

impl EventNotifier {
    pub fn new() -> Self {
        EventNotifier {
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // Get or create a channel for a specific file path
    pub fn subscribe(&self, file_path: &str) -> broadcast::Receiver<String> {
        let mut channels = self.channels.lock().unwrap();

        // Get or create the channel
        let sender = channels.entry(file_path.to_string()).or_insert_with(|| {
            // Create a new broadcast channel with capacity for 100 messages
            let (tx, _) = broadcast::channel(100);
            tx
        });

        // Clone the sender to create a new receiver
        sender.subscribe()
    }

    // Notify all subscribers for a specific file path
    pub fn notify(&self, file_path: &str, user_hash: &str) {
        let channels = self.channels.lock().unwrap();

        if let Some(sender) = channels.get(file_path) {
            // Send the user hash that caused this notification
            let _ = sender.send(user_hash.to_string());
        }
    }
}
