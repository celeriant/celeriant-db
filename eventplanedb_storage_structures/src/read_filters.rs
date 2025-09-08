use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// Filters and pagination options for reading event batches
#[derive(Default, Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReadFilters {
    /// Starting server ID to begin reading from (inclusive). Will error if not found in stream.
    pub from_event_batch_index: u64,
    /// End reading event batches at this server id (inclusive). Will error if reached end of stream before this ID.
    pub to_event_batch_index: Option<u64>,
    /// Optional limit on the total response size in bytes to prevent large responses
    pub max_bytes: Option<usize>,
    /// Optional whitelist of event types to include in results
    pub include_event_types: Option<Vec<u64>>,
    /// Skip events created by this client
    pub exclude_client_id: Option<u128>,
    /// Only get events for this client
    pub include_client_id: Option<u128>,
    /// Skip events created by this user
    pub exclude_user_id: Option<u128>,
    /// Only get events for this user
    pub include_user_id: Option<u128>,
    /// Optional timestamp filter, only include batches after this time (exclusive)
    pub min_server_timestamp: Option<u64>,
    /// Optional timestamp filter, only include batches before this time (exclusive)
    pub max_server_timestamp: Option<u64>,
    /// Only include batches with max_local_index greater than or equal to this value
    pub min_client_event_index: Option<u64>,
    /// Only include batches with min_local_index less than or equal to this value
    pub max_client_event_index: Option<u64>,
    /// Only include batches with max_event_time greater than or equal to this value
    pub min_event_timestamp: Option<u64>,
    /// Only include batches with min_event_time less than or equal to this value
    pub max_event_timestamp: Option<u64>,
    /// Only include batches with event_index greater than or equal to this value
    pub min_event_index: Option<u64>,
    /// Only include batches with event_index less than or equal to this value
    pub max_event_index: Option<u64>,
}

impl ReadFilters {
    pub fn new(from_event_batch_index: u64) -> Self {
        Self {
            from_event_batch_index,
            ..Default::default()
        }
    }

    pub fn to_event_batch_index(mut self, event_batch_index: u64) -> Self {
        self.to_event_batch_index = Some(event_batch_index);
        self
    }

    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    pub fn include_event_types(mut self, event_types: Vec<u64>) -> Self {
        self.include_event_types = Some(event_types);
        self
    }

    pub fn exclude_client_id(mut self, client_id: u128) -> Self {
        self.exclude_client_id = Some(client_id);
        self
    }

    pub fn include_client_id(mut self, client_id: u128) -> Self {
        self.include_client_id = Some(client_id);
        self
    }

    pub fn exclude_user_id(mut self, user_id: u128) -> Self {
        self.exclude_user_id = Some(user_id);
        self
    }

    pub fn include_user_id(mut self, user_id: u128) -> Self {
        self.include_user_id = Some(user_id);
        self
    }

    pub fn min_server_timestamp(mut self, timestamp: u64) -> Self {
        self.min_server_timestamp = Some(timestamp);
        self
    }

    pub fn max_server_timestamp(mut self, timestamp: u64) -> Self {
        self.max_server_timestamp = Some(timestamp);
        self
    }

    pub fn time_range(mut self, after: u64, before: u64) -> Self {
        self.min_server_timestamp = Some(after);
        self.max_server_timestamp = Some(before);
        self
    }

    pub fn min_client_event_index(mut self, index: u64) -> Self {
        self.min_client_event_index = Some(index);
        self
    }

    pub fn max_client_event_index(mut self, index: u64) -> Self {
        self.max_client_event_index = Some(index);
        self
    }

    pub fn client_event_index_range(mut self, min: u64, max: u64) -> Self {
        self.min_client_event_index = Some(min);
        self.max_client_event_index = Some(max);
        self
    }

    pub fn min_event_timestamp(mut self, time: u64) -> Self {
        self.min_event_timestamp = Some(time);
        self
    }

    pub fn max_event_timestamp(mut self, time: u64) -> Self {
        self.max_event_timestamp = Some(time);
        self
    }

    pub fn event_time_range(mut self, min: u64, max: u64) -> Self {
        self.min_event_timestamp = Some(min);
        self.max_event_timestamp = Some(max);
        self
    }

    pub fn min_event_index(mut self, event_index: u64) -> Self {
        self.min_event_index = Some(event_index);
        self
    }

    pub fn max_event_index(mut self, event_index: u64) -> Self {
        self.max_event_index = Some(event_index);
        self
    }

    pub fn event_index_range(mut self, min_event_index: u64, max_event_index: u64) -> Self {
        self.min_event_index = Some(min_event_index);
        self.max_event_index = Some(max_event_index);
        self
    }
}
