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
        from_aggregate_version: u64,
        to_aggregate_version: u64,
    },
    Read {
        from_aggregate_version: u64,
        to_aggregate_version: Option<u64>,
    },
    TrimStart {
        keep_from_aggregate_version: u64,
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
    from_aggregate_version: Option<u64>,
    to_aggregate_version: Option<u64>,
    keep_from_aggregate_version: Option<u64>,
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
                from_aggregate_version,
                to_aggregate_version,
            } => {
                events_map
                    .entry(operation_key)
                    .and_modify(|e| {
                        e.from_aggregate_version = Some(
                            e.from_aggregate_version
                                .unwrap_or(from_aggregate_version)
                                .min(from_aggregate_version),
                        );
                        e.to_aggregate_version = Some(
                            e.to_aggregate_version
                                .unwrap_or(to_aggregate_version)
                                .max(to_aggregate_version),
                        );
                    })
                    .or_insert(AccumulatedEvent {
                        from_aggregate_version: Some(from_aggregate_version),
                        to_aggregate_version: Some(to_aggregate_version),
                        ..Default::default()
                    });
            }
            AggregateWatchEventOperation::Read {
                from_aggregate_version,
                to_aggregate_version,
            } => {
                events_map
                    .entry(operation_key)
                    .and_modify(|e| {
                        e.from_aggregate_version = Some(
                            e.from_aggregate_version
                                .unwrap_or(from_aggregate_version)
                                .min(from_aggregate_version),
                        );
                        e.to_aggregate_version = match (e.to_aggregate_version, to_aggregate_version) {
                            (Some(a), Some(b)) => Some(a.max(b)),
                            (a, b) => a.or(b),
                        };
                    })
                    .or_insert(AccumulatedEvent {
                        from_aggregate_version: Some(from_aggregate_version),
                        to_aggregate_version,
                        ..Default::default()
                    });
            }
            AggregateWatchEventOperation::TrimStart {
                keep_from_aggregate_version,
            } => {
                events_map.insert(operation_key, AccumulatedEvent {
                    keep_from_aggregate_version: Some(keep_from_aggregate_version),
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
                    from_aggregate_version: acc.from_aggregate_version,
                    to_aggregate_version: acc.to_aggregate_version,
                    keep_from_aggregate_version: acc.keep_from_aggregate_version,
                });
            }
        }
        WatchResponse { events }
    }
}
