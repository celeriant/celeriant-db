#[derive(Debug, Clone)]
pub enum AggregateWatchEvent {
    Delete {
        correlation_id: Option<u128>,
    },
    Write {
        from_event_batch_index: u64,
        to_event_batch_index: u64,
    },
    Read {
        correlation_id: Option<u128>,
        from_event_batch_index: u64,
        to_event_batch_index: Option<u64>,
        is_cached_read: bool,
    },
    TrimStart {
        correlation_id: Option<u128>,
        keep_from_event_batch_index: u64,
    },
    Exists {
        correlation_id: Option<u128>,
    },
    PrependBatches {
        correlation_id: Option<u128>,
        from_event_batch_index: u64,
        to_event_batch_index: u64
    }
}

impl AggregateWatchEvent {
    pub const DELETE: u8 = 0;
    pub const WRITE: u8 = 1;
    pub const READ: u8 = 2;
    pub const TRIM_START: u8 = 3;
    pub const EXISTS: u8 = 4;
    pub const PREPEND_BATCHES: u8 = 5;

    pub fn to_u8(&self) -> u8 {
        match self {
            AggregateWatchEvent::Delete { .. } => Self::DELETE,
            AggregateWatchEvent::Write { .. } => Self::WRITE,
            AggregateWatchEvent::Read { .. } => Self::READ,
            AggregateWatchEvent::TrimStart { .. } => Self::TRIM_START,
            AggregateWatchEvent::Exists { .. } => Self::EXISTS,
            AggregateWatchEvent::PrependBatches { .. } => Self::PREPEND_BATCHES,
        }
    }
}