/// Filters and pagination options for reading event batches
#[derive(Debug, Default)]
pub struct ReadFilters<'a> {
    /// Starting server ID to begin reading from (inclusive). Will error if not found in stream.
    pub from_server_id: Option<u64>,
    /// End reading event batches at this server id (inclusive). Will error if reached end of stream before this ID.
    pub to_server_id: Option<u64>,
    /// Optional limit on the total response size in bytes to prevent large responses
    pub max_bytes: Option<usize>,
    /// Optional whitelist of event types to include in results
    pub include_event_types: Option<&'a [u64]>,
    /// Optional client ID to exclude from results (useful for excluding own events)
    pub exclude_client_id: Option<u64>,
    /// Optional timestamp filter, only include batches after this time (exclusive)
    pub after_server_time: Option<u64>,
    /// Optional timestamp filter, only include batches before this time (exclusive)
    pub before_server_time: Option<u64>,
}

impl<'a> ReadFilters<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_server_id(mut self, id: u64) -> Self {
        self.from_server_id = Some(id);
        self
    }

    pub fn to_server_id(mut self, id: u64) -> Self {
        self.to_server_id = Some(id);
        self
    }

    pub fn server_id_range(mut self, from: u64, to: u64) -> Self {
        self.from_server_id = Some(from);
        self.to_server_id = Some(to);
        self
    }

    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    pub fn include_event_types(mut self, event_types: &'a [u64]) -> Self {
        self.include_event_types = Some(event_types);
        self
    }

    pub fn exclude_client_id(mut self, client_id: u64) -> Self {
        self.exclude_client_id = Some(client_id);
        self
    }

    pub fn after_server_time(mut self, timestamp: u64) -> Self {
        self.after_server_time = Some(timestamp);
        self
    }

    pub fn before_server_time(mut self, timestamp: u64) -> Self {
        self.before_server_time = Some(timestamp);
        self
    }

    pub fn time_range(mut self, after: u64, before: u64) -> Self {
        self.after_server_time = Some(after);
        self.before_server_time = Some(before);
        self
    }
}
