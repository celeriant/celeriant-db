use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

/// Create a `DatablockAggregateEvent` by JSON-serializing `value`.
///
/// Sets `event_type_major` to `event_type`, `event_type_minor` to 0,
/// and all other fields to their defaults. Modify the returned event
/// to set additional fields (e.g. `event_timestamp`, `event_id`).
pub fn json_event<T: Serialize>(
    event_type: u64,
    value: &T,
) -> Result<DatablockAggregateEvent, serde_json::Error> {
    let bytes = serde_json::to_vec(value)?;
    Ok(DatablockAggregateEvent {
        client_seq: 0,
        event_seq: 0,
        event_id: None,
        event_timestamp: 0,
        event_type_major: event_type,
        event_type_minor: 0,
        event_value: Arc::new(bytes),
        iv: None,
    })
}

/// Deserialize the `event_value` JSON bytes of an event into a typed struct.
pub fn from_json<T: DeserializeOwned>(
    event: &DatablockAggregateEvent,
) -> Result<T, serde_json::Error> {
    serde_json::from_slice(&event.event_value)
}
