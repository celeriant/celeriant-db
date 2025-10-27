use bincode::{Decode, Encode};
use eventplanedb_structures::{read_filters::ReadFilters, read_result::ReadResult, append_result::AppendResult};
use eventplanedb_structures::{event_item::EventItem};
use serde::{Deserialize, Serialize};

/// Wire protocol requests
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum Request {
    AppendEvents {
        sync_delay_us: u64,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
        filter_duplicate_client_events: bool,
    },
    ReadFiltered {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: ReadFilters,
    },
    Exists {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },
    TrimStart {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        keep_from_event_batch_index: u64,
    },
    Delete {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },
}

/// Wire protocol responses
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum Response {
    AppendEventsResult(Result<AppendResult, String>),
    ReadFilteredResult(Result<ReadResult, String>),
    ExistsResult(Result<bool, String>),
    TrimStartResult(Result<(), String>),
    DeleteResult(Result<(), String>),
}

impl Request {
    pub fn aggregate_id(&self) -> &u128 {
        match self {
            Request::AppendEvents { aggregate_id, .. } => aggregate_id,
            Request::ReadFiltered { aggregate_id, .. } => aggregate_id,
            Request::Exists { aggregate_id, .. } => aggregate_id,
            Request::TrimStart { aggregate_id, .. } => aggregate_id,
            Request::Delete { aggregate_id, .. } => aggregate_id,
        }
    }
}
