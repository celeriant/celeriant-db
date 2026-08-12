use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::aggregate_key::AggregateKey;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub struct MetablockSoftDelete {
    pub aggregate_key: AggregateKey,
    pub allow_recreate: bool,
    pub allow_sequence_continuation: bool,
    pub aggregate_version: u64,
    pub event_seq: u64,
    pub client_id: u128,
    pub user_id: Option<u128>,
}

impl MetablockSoftDelete {
    // Wire format layout (bincode fixed-int encoding)
    // Update these if field order or types change!

    const WIRE_SIZE_AGGREGATE_KEY: usize = AggregateKey::WIRE_SIZE_TOTAL;
    const WIRE_SIZE_ALLOW_RECREATE: usize = 1;
    const WIRE_SIZE_ALLOW_SEQUENCE_CONTINUATION: usize = 1;
    const WIRE_SIZE_AGGREGATE_VERSION: usize = 8;
    const WIRE_SIZE_EVENT_SEQ: usize = 8;

    pub const OFFSET_AGGREGATE_KEY: usize = 0;

    pub const OFFSET_ALLOW_RECREATE: usize =
        Self::OFFSET_AGGREGATE_KEY + Self::WIRE_SIZE_AGGREGATE_KEY;

    pub const OFFSET_ALLOW_SEQUENCE_CONTINUATION: usize =
        Self::OFFSET_ALLOW_RECREATE + Self::WIRE_SIZE_ALLOW_RECREATE;

    pub const OFFSET_AGGREGATE_VERSION: usize =
        Self::OFFSET_ALLOW_SEQUENCE_CONTINUATION + Self::WIRE_SIZE_ALLOW_SEQUENCE_CONTINUATION;

    pub const OFFSET_EVENT_SEQ: usize =
        Self::OFFSET_AGGREGATE_VERSION + Self::WIRE_SIZE_AGGREGATE_VERSION;

    pub const OFFSET_CLIENT_ID: usize =
        Self::OFFSET_EVENT_SEQ + Self::WIRE_SIZE_EVENT_SEQ;
}
