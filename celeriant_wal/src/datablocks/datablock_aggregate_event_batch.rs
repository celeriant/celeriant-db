use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

/// Events from clients are grouped into batches, compressed and stored in the WAL,
/// typically in datablocks which are variable length, but if < 256 bytes can
/// be stored directly in a metablock minibatch
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf, Default)]
pub struct DatablockAggregateEventBatch {
    /// Unique, incremented integer assigned to each event batch when persisted on the server
    pub event_batch_index: u64,

    /// Events present in this batch, all from the same client / user
    pub events: Vec<DatablockAggregateEvent>,
}
