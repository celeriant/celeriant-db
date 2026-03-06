use std::collections::HashMap;

use celeriant_msg::response::{responses::WatchResponse, watch_event::WatchResponseEvent};
use celeriant_wal::aggregate_key::AggregateKey;

#[derive(Debug, Clone)]
pub struct AggregateWatchEvent {
    pub aggregate_key: AggregateKey,
    pub operation: AggregateWatchEventOperation,
}

/// Event message passing via a local channel within a single shard
/// Used by ShardWriteAheadLog to notify SubscribedClients of aggregate actions
#[derive(Debug, Clone)]
pub enum AggregateWatchEventOperation {
    Delete {},
    Write {
        from_event_batch_index: u64,
        to_event_batch_index: u64,
    },
    Read {
        from_event_batch_index: u64,
        to_event_batch_index: Option<u64>,
    },
    TrimStart {
        keep_from_event_batch_index: u64,
    },
    AggregateDetails {},
    Create {},
}

impl AggregateWatchEvent {
    pub const DELETE: u8 = 0;
    pub const WRITE: u8 = 1;
    pub const READ: u8 = 2;
    pub const TRIM_START: u8 = 3;
    pub const DETAILS: u8 = 4;
    pub const CREATE: u8 = 5;

    pub fn operation_as_u8(&self) -> u8 {
        match self.operation {
            AggregateWatchEventOperation::Delete { .. } => Self::DELETE,
            AggregateWatchEventOperation::Write { .. } => Self::WRITE,
            AggregateWatchEventOperation::Read { .. } => Self::READ,
            AggregateWatchEventOperation::TrimStart { .. } => Self::TRIM_START,
            AggregateWatchEventOperation::AggregateDetails { .. } => Self::DETAILS,
            AggregateWatchEventOperation::Create { .. } => Self::CREATE,
        }
    }
}

/// Internal accumulator that merges watch events using hashmaps,
/// then flattens to a `WatchResponse` for the wire.
#[derive(Debug, Clone, Default)]
struct AccumulatedEvent {
    from_event_batch_index: Option<u64>,
    to_event_batch_index: Option<u64>,
    keep_from_event_batch_index: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct WatchEventAccumulator {
    events: HashMap<AggregateKey, HashMap<u8, AccumulatedEvent>>,
}

impl WatchEventAccumulator {
    pub fn accumulate(&mut self, event: AggregateWatchEvent) {
        let operation_key = event.operation_as_u8();
        let events_map = self.events.entry(event.aggregate_key).or_default();

        match event.operation {
            AggregateWatchEventOperation::Delete {}
            | AggregateWatchEventOperation::AggregateDetails {}
            | AggregateWatchEventOperation::Create {} => {
                events_map.entry(operation_key).or_default();
            }
            AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            } => {
                events_map
                    .entry(operation_key)
                    .and_modify(|e| {
                        e.from_event_batch_index = Some(
                            e.from_event_batch_index
                                .unwrap_or(from_event_batch_index)
                                .min(from_event_batch_index),
                        );
                        e.to_event_batch_index = Some(
                            e.to_event_batch_index
                                .unwrap_or(to_event_batch_index)
                                .max(to_event_batch_index),
                        );
                    })
                    .or_insert(AccumulatedEvent {
                        from_event_batch_index: Some(from_event_batch_index),
                        to_event_batch_index: Some(to_event_batch_index),
                        ..Default::default()
                    });
            }
            AggregateWatchEventOperation::Read {
                from_event_batch_index,
                to_event_batch_index,
            } => {
                events_map
                    .entry(operation_key)
                    .and_modify(|e| {
                        e.from_event_batch_index = Some(
                            e.from_event_batch_index
                                .unwrap_or(from_event_batch_index)
                                .min(from_event_batch_index),
                        );
                        e.to_event_batch_index = match (e.to_event_batch_index, to_event_batch_index) {
                            (Some(a), Some(b)) => Some(a.max(b)),
                            (a, b) => a.or(b),
                        };
                    })
                    .or_insert(AccumulatedEvent {
                        from_event_batch_index: Some(from_event_batch_index),
                        to_event_batch_index,
                        ..Default::default()
                    });
            }
            AggregateWatchEventOperation::TrimStart {
                keep_from_event_batch_index,
            } => {
                events_map.insert(operation_key, AccumulatedEvent {
                    keep_from_event_batch_index: Some(keep_from_event_batch_index),
                    ..Default::default()
                });
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn into_response(self) -> WatchResponse {
        let mut events = Vec::new();
        for (key, ops) in self.events {
            for (operation, acc) in ops {
                events.push(WatchResponseEvent {
                    org_id: key.org_id,
                    aggregate_type_id: key.aggregate_type_id,
                    aggregate_id: key.aggregate_id,
                    operation,
                    from_event_batch_index: acc.from_event_batch_index,
                    to_event_batch_index: acc.to_event_batch_index,
                    keep_from_event_batch_index: acc.keep_from_event_batch_index,
                });
            }
        }
        WatchResponse { events }
    }
}
