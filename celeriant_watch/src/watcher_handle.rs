use std::collections::HashSet;

use glommio::channels::local_channel::LocalSender;
use crate::aggregate_watch_event::AggregateWatchEvent;

/// Keeps a reference to the channel and what events
/// the receiver wants to know about
pub struct WatcherHandle {
    pub id: u64,
    pub local_sender_channel: LocalSender<AggregateWatchEvent>,

    pub orgs: Option<HashSet<u128>>,
    pub aggregate_types: Option<HashSet<u128>>,
    pub aggregates: Option<HashSet<u128>>,
    pub operation_types: Option<HashSet<u8>>,
}

impl WatcherHandle {
    /// Returns false if client can't keep up (channel full)
    pub fn notify_of_event(&self, event_type: u8, event: &AggregateWatchEvent) -> bool {
        
        if self.operation_types.as_ref().is_some_and(|s| !s.contains(&event_type)) {
            return true;
        }

        let key = &event.aggregate_key;

        // AND across filter categories:
        // - if a filter is None => it does not restrict matching
        // - if a filter is Some(set) => the event must match that set (OR within the set)
        if self.orgs.as_ref().is_some_and(|s| !s.contains(&key.org_id)) {
            return true;
        }
        
        if self.aggregate_types.as_ref().is_some_and(|s| !s.contains(&key.aggregate_type_id)) {
            return true;
        }

        if self.aggregates.as_ref().is_some_and(|s| !s.contains(&key.aggregate_id)) {
            return true;
        }

        self.local_sender_channel.try_send(event.clone()).is_ok()
    }
}