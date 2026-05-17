use crate::intra_batch_chain::IntraBatchChainBreak;

#[derive(Debug, Clone)]
pub enum ReplicateToS3Error {
    S3NotConfigured,
    S3Unavailable,
    S3PutFailed {
        path: String,
        message: String,
    },
    SidecarUnavailable,
    SerializationFailed(String),
    BatchNotContiguous {
        at_index: usize,
        expected_wal_seq: u64,
        actual_wal_seq: u64,
    },
    LeaseIndexInconsistent {
        at_index: usize,
        first: u64,
        found: u64,
    },
    IntraBatchChainBreak(IntraBatchChainBreak),
}