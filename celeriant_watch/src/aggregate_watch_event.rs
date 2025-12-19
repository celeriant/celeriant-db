use celeriant_msg::response::{responses::WatchResponse, watch_event::WatchEvent};
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
    Exists {},
}

impl AggregateWatchEvent {
    pub const DELETE: u8 = 0;
    pub const WRITE: u8 = 1;
    pub const READ: u8 = 2;
    pub const TRIM_START: u8 = 3;
    pub const EXISTS: u8 = 4;

    pub fn operation_as_u8(&self) -> u8 {
        match self.operation {
            AggregateWatchEventOperation::Delete { .. } => Self::DELETE,
            AggregateWatchEventOperation::Write { .. } => Self::WRITE,
            AggregateWatchEventOperation::Read { .. } => Self::READ,
            AggregateWatchEventOperation::TrimStart { .. } => Self::TRIM_START,
            AggregateWatchEventOperation::Exists { .. } => Self::EXISTS,
        }
    }

    pub fn add_to_response(self, watch_response: &mut Option<WatchResponse>) {
        let operation_key = self.operation_as_u8();

        let events_map = watch_response
            .get_or_insert_default()
            .events
            .get_or_insert_default()
            .entry(self.aggregate_key.clone())
            .or_default();

        match self.operation {
            AggregateWatchEventOperation::Delete {} | AggregateWatchEventOperation::Exists {} => {
                events_map.insert(operation_key, None);
            }
            AggregateWatchEventOperation::Write {
                from_event_batch_index,
                to_event_batch_index,
            } => {
                events_map
                    .entry(operation_key)
                    .and_modify(|existing| {
                        if let Some(e) = existing {
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
                        }
                    })
                    .or_insert_with(|| Some(WatchEvent {
                        from_event_batch_index: Some(from_event_batch_index),
                        to_event_batch_index: Some(to_event_batch_index),
                        ..Default::default()
                    }));
            }
            AggregateWatchEventOperation::Read {
                from_event_batch_index,
                to_event_batch_index,
            } => {
                events_map
                    .entry(operation_key)
                    .and_modify(|existing| {
                        if let Some(e) = existing {
                            e.from_event_batch_index = Some(
                                e.from_event_batch_index
                                    .unwrap_or(from_event_batch_index)
                                    .min(from_event_batch_index),
                            );
                            e.to_event_batch_index = match (e.to_event_batch_index, to_event_batch_index) {
                                (Some(a), Some(b)) => Some(a.max(b)),
                                (a, b) => a.or(b),
                            };
                        }
                    })
                    .or_insert_with(|| Some(WatchEvent {
                        from_event_batch_index: Some(from_event_batch_index),
                        to_event_batch_index,
                        ..Default::default()
                    }));
            }
            AggregateWatchEventOperation::TrimStart {
                keep_from_event_batch_index,
            } => {
                // TrimStart is destructive, always replace
                events_map.insert(operation_key, Some(WatchEvent {
                    keep_from_event_batch_index: Some(keep_from_event_batch_index),
                    ..Default::default()
                }));
            }
        }
    }

}
