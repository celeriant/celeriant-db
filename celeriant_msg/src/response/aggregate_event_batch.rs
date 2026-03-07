use bincode::{Decode, Encode};
use celeriant_wal::datablocks::datablock::Datablock;
use celeriant_wal::datablocks::datablock_kind::DatablockKind;
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use serde::{Deserialize, Serialize};

use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::serde::serde_u128_base64;
use celeriant_wal::serde::serde_option_u128_base64;

/// Events from clients are grouped into batches, compressed and stored in the WAL,
/// typically in datablocks which are variable length, but if < 256 bytes can
/// be stored directly in a metablock minibatch
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct AggregateEventBatch {
    /// Unique, incremented integer assigned to each event batch when persisted on the server
    #[serde(rename = "bx")]
    pub event_batch_index: u64,

    #[serde(with = "serde_u128_base64", rename = "ci")]
    /// Client ID that created this batch
    pub client_id: u128,

    /// Optional user ID
    #[serde(with = "serde_option_u128_base64", rename = "ui")]
    pub user_id: Option<u128>,

    /// Server timestamp when batch was processed
    #[serde(rename = "st")]
    pub server_timestamp: u64,

    /// Events present in this batch, all from the same client / user
    #[serde(rename = "ev")]
    pub events: Vec<DatablockAggregateEvent>,
}

impl AggregateEventBatch {
    pub fn from_wal(metablock: &Metablock, datablock: &Datablock) -> Option<Self> {
        let MetablockKind::EventBatchMetadata(metablock_event_batch) = &metablock.wal_metablock_type else {
            return None;
        };
        
        let DatablockKind::EventBatchItem(datablock_event_batch) = &datablock.datablock_kind else {
            return None;
        };

        Some(Self { 
            event_batch_index: metablock_event_batch.event_batch_index, 
            client_id: metablock_event_batch.client_id, 
            user_id: metablock_event_batch.user_id, 
            server_timestamp: metablock.server_timestamp, 
            events: datablock_event_batch.events.clone()
        })
    }
}