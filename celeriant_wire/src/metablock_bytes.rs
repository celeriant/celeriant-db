//! Zero-copy byte-level access to serialized metablock fields.
//! Avoids full deserialization for fast scanning.

use celeriant_wal::buffer_read::{read_u64_le, read_u128_le};
use celeriant_wal::constants::WIRE_SIZE_ENUM_DISCRIMINANT;
use celeriant_wal::metablocks::{metablock::Metablock, metablock_event_batch::MetablockEventBatch};
use celeriant_wal::aggregate_key::AggregateKey;

use crate::version_aware_wire_format::HEADER_SIZE;

/// Discriminant value for MetablockKind::EventBatchMetadata
const DISCRIMINANT_EVENT_BATCH_METADATA: u8 = 0;

/// Discriminant value for MetablockKind::SoftDelete
const DISCRIMINANT_SOFT_DELETE: u8 = 4;

/// Base offset where MetablockEventBatch payload starts
const EVENT_BATCH_PAYLOAD_OFFSET: usize = 
    HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE + WIRE_SIZE_ENUM_DISCRIMINANT;

const SOFT_DELETE_PAYLOAD_OFFSET: usize = 
    HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE + WIRE_SIZE_ENUM_DISCRIMINANT;

/// Read the MetablockKind discriminant from raw bytes
#[inline]
pub fn read_metablock_kind_discriminant(bytes: &[u8]) -> u8 {
    bytes[HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE]
}

/// Read wal_index from metablock bytes
#[inline]
pub fn read_wal_index(bytes: &[u8]) -> u64 {
    let offset = HEADER_SIZE + Metablock::OFFSET_WAL_INDEX;
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

#[inline]
pub fn is_metablock_kind_soft_delete(bytes: &[u8]) -> bool {
    read_metablock_kind_discriminant(bytes) == DISCRIMINANT_SOFT_DELETE
}

#[inline]
pub fn is_metablock_kind_event_batch_metadata(bytes: &[u8]) -> bool {
    read_metablock_kind_discriminant(bytes) == DISCRIMINANT_EVENT_BATCH_METADATA
}

/// Check if this metablock is an EventBatchMetadata for the given aggregate
#[inline]
pub fn is_matches_aggregate_key(bytes: &[u8], target: &AggregateKey) -> bool {
    if read_metablock_kind_discriminant(bytes) != DISCRIMINANT_EVENT_BATCH_METADATA {
        return false;
    }

    let org_id = read_event_batch_org_id(bytes);
    let type_id = read_event_batch_aggregate_type_id(bytes);
    let agg_id = read_event_batch_aggregate_id(bytes);

    org_id == target.org_id 
        && type_id == target.aggregate_type_id 
        && agg_id == target.aggregate_id
}

/// Check if this metablock is a SoftDelete for the given aggregate
#[inline]
pub fn is_soft_delete_for_aggregate(bytes: &[u8], target: &AggregateKey) -> bool {
    if read_metablock_kind_discriminant(bytes) != DISCRIMINANT_SOFT_DELETE {
        return false;
    }

    // SoftDelete has same layout for aggregate_key at start of payload
    let org_id = read_soft_delete_org_id(bytes);
    let type_id = read_soft_delete_aggregate_type_id(bytes);
    let agg_id = read_soft_delete_aggregate_id(bytes);

    org_id == target.org_id 
        && type_id == target.aggregate_type_id 
        && agg_id == target.aggregate_id
}

// --- SoftDelete field readers ---

#[inline]
pub fn read_soft_delete_org_id(bytes: &[u8]) -> u128 {
    let offset = SOFT_DELETE_PAYLOAD_OFFSET + AggregateKey::OFFSET_ORG_ID;
    read_u128_le(bytes, offset)
}

#[inline]
pub fn read_soft_delete_aggregate_type_id(bytes: &[u8]) -> u128 {
    let offset = SOFT_DELETE_PAYLOAD_OFFSET + AggregateKey::OFFSET_AGGREGATE_TYPE_ID;
    read_u128_le(bytes, offset)
}

#[inline]
pub fn read_soft_delete_aggregate_id(bytes: &[u8]) -> u128 {
    let offset = SOFT_DELETE_PAYLOAD_OFFSET + AggregateKey::OFFSET_AGGREGATE_ID;
    read_u128_le(bytes, offset)
}

// --- EventBatch field readers ---

#[inline]
pub fn read_event_batch_org_id(bytes: &[u8]) -> u128 {
    let offset = EVENT_BATCH_PAYLOAD_OFFSET 
        + MetablockEventBatch::OFFSET_AGGREGATE_KEY 
        + AggregateKey::OFFSET_ORG_ID;
    read_u128_le(bytes, offset)
}

#[inline]
pub fn read_event_batch_aggregate_type_id(bytes: &[u8]) -> u128 {
    let offset = EVENT_BATCH_PAYLOAD_OFFSET 
        + MetablockEventBatch::OFFSET_AGGREGATE_KEY 
        + AggregateKey::OFFSET_AGGREGATE_TYPE_ID;
    read_u128_le(bytes, offset)
}

#[inline]
pub fn read_event_batch_aggregate_id(bytes: &[u8]) -> u128 {
    let offset = EVENT_BATCH_PAYLOAD_OFFSET 
        + MetablockEventBatch::OFFSET_AGGREGATE_KEY 
        + AggregateKey::OFFSET_AGGREGATE_ID;
    read_u128_le(bytes, offset)
}

#[inline]
pub fn read_event_batch_min_event_batch_index(bytes: &[u8]) -> u64 {
    let offset = EVENT_BATCH_PAYLOAD_OFFSET + MetablockEventBatch::OFFSET_MIN_EVENT_BATCH_INDEX;
    read_u64_le(bytes, offset)
}

#[inline]
pub fn read_event_batch_event_batch_index(bytes: &[u8]) -> u64 {
    let offset = EVENT_BATCH_PAYLOAD_OFFSET + MetablockEventBatch::OFFSET_EVENT_BATCH_INDEX;
    read_u64_le(bytes, offset)
}

#[inline]
pub fn read_event_batch_max_event_index(bytes: &[u8]) -> u64 {
    let offset = EVENT_BATCH_PAYLOAD_OFFSET + MetablockEventBatch::OFFSET_MAX_EVENT_INDEX;
    read_u64_le(bytes, offset)
}

#[inline]
pub fn read_event_batch_client_id(bytes: &[u8]) -> u128 {
    let offset = EVENT_BATCH_PAYLOAD_OFFSET + MetablockEventBatch::OFFSET_CLIENT_ID;
    read_u128_le(bytes, offset)
}

#[inline]
pub fn read_event_batch_max_client_event_index(bytes: &[u8]) -> u64 {
    let offset = EVENT_BATCH_PAYLOAD_OFFSET + MetablockEventBatch::OFFSET_MAX_CLIENT_EVENT_INDEX;
    read_u64_le(bytes, offset)
}

pub fn read_event_batch_aggregate_key(bytes: &[u8]) -> AggregateKey {
    AggregateKey::new(read_event_batch_org_id(bytes), read_event_batch_aggregate_type_id(bytes), read_event_batch_aggregate_id(bytes))
}