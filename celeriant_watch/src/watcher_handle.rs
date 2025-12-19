use std::collections::HashSet;

use celeriant_wal::{aggregate_key::AggregateKey, aggregate_type_key::AggregateTypeKey};
use glommio::channels::local_channel::LocalSender;
use crate::aggregate_watch_event::AggregateWatchEvent;

/// Keeps a reference to the channel and what events
/// the receiver wants to know about
pub struct WatcherHandle {
    pub id: u64,
    pub local_sender_channel: LocalSender<AggregateWatchEvent>,

    pub orgs: Option<HashSet<u128>>,
    pub aggregate_types: Option<HashSet<AggregateTypeKey>>,
    pub aggregates: Option<HashSet<AggregateKey>>,
    pub operation_types: Option<HashSet<u8>>,
}

impl WatcherHandle {
    /// Returns false if client can't keep up (channel full)
    pub fn notify_of_event(&self, event_type: u8, event: &AggregateWatchEvent) -> bool {
        
        if self.operation_types.as_ref().is_some_and(|s| !s.contains(&event_type)) {
            return true;
        }

        let key = &event.aggregate_key;
        
        // If all filters are None, match everything
        // If any filter is Some and matches, notify
        // If filters exist but none match, skip
        let has_any_filter = self.orgs.is_some() || 
                             self.aggregate_types.is_some() || 
                             self.aggregates.is_some();

        if has_any_filter {
            let matches = self.orgs.as_ref().is_some_and(|s| s.contains(&key.org_id)) ||
                          self.aggregates.as_ref().is_some_and(|s| s.contains(key)) ||
                          self.aggregate_types.as_ref().is_some_and(|s| 
                              s.contains(&AggregateTypeKey::new(key.org_id, key.aggregate_type_id)));
            
            if !matches {
                return true;
            }
        }

        self.local_sender_channel.try_send(event.clone()).is_ok()
    }
}