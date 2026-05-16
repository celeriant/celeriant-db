pub mod error_codes;
pub mod process_client_requests;
pub mod process_client_responses;
pub mod process_cluster_requests;
pub mod process_cluster_responses;
pub mod process_identify;
pub mod read_wire_data_error;
pub mod request;
pub mod response;

/// Minimum serialized payload size (bytes) before response compression is applied.
/// Below this threshold, the zstd frame overhead dominates any potential saving.
pub const RESPONSE_COMPRESSION_THRESHOLD_BYTES: usize = 1024;