//! Zero-copy byte-level access to serialized metablock fields.
//! Avoids full deserialization for fast scanning.

use celeriant_wal::buffer_read::{read_u64_le, read_u128_le};
use celeriant_wal::constants::{WIRE_SIZE_ENUM_DISCRIMINANT};
use celeriant_wal::metablocks::{metablock::Metablock, metablock_event_batch::MetablockEventBatch};
use celeriant_wal::aggregate_key::AggregateKey;

use crate::version_aware_wire_format::HEADER_SIZE;

/// Discriminant value for MetablockKind::EventBatchMetadata
const DISCRIMINANT_EVENT_BATCH_METADATA: u8 = 0;

/// Discriminant value for MetablockKind::SoftDelete
const DISCRIMINANT_SOFT_DELETE: u8 = 4;

/// Discriminant value for MetablockKind::SoftTrim
const DISCRIMINANT_SOFT_TRIM: u8 = 5;

/// Base offset where MetablockEventBatch payload starts
const EVENT_BATCH_PAYLOAD_OFFSET: usize = 
    HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE + WIRE_SIZE_ENUM_DISCRIMINANT;

const SOFT_DELETE_PAYLOAD_OFFSET: usize = 
    HEADER_SIZE + Metablock::OFFSET_WAL_METABLOCK_TYPE + WIRE_SIZE_ENUM_DISCRIMINANT;

const SOFT_TRIM_PAYLOAD_OFFSET: usize = 
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

/// Read wal_index from metablock bytes
#[inline]
pub fn read_server_timestamp(bytes: &[u8]) -> u64 {
    let offset = HEADER_SIZE + Metablock::OFFSET_SERVER_TIMESTAMP;
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

/// Read wal_index from metablock bytes
#[inline]
pub fn read_compressed_size(bytes: &[u8]) -> u64 {
    let offset = HEADER_SIZE + Metablock::OFFSET_COMPRESSED_SIZE;
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

/// Read wal_index from metablock bytes
#[inline]
pub fn read_uncompressed_size(bytes: &[u8]) -> u64 {
    let offset = HEADER_SIZE + Metablock::OFFSET_UNCOMPRESSED_SIZE;
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

#[inline]
pub fn is_metablock_kind_soft_delete(bytes: &[u8]) -> bool {
    read_metablock_kind_discriminant(bytes) == DISCRIMINANT_SOFT_DELETE
}

#[inline]
pub fn is_metablock_kind_soft_trim(bytes: &[u8]) -> bool {
    read_metablock_kind_discriminant(bytes) == DISCRIMINANT_SOFT_TRIM
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

/// Read aggregate_key from SoftDelete metablock
pub fn read_soft_delete_aggregate_key(bytes: &[u8]) -> AggregateKey {    
    let org_id = read_soft_delete_org_id(bytes);
    let type_id = read_soft_delete_aggregate_type_id(bytes);
    let agg_id = read_soft_delete_aggregate_id(bytes);
    
    AggregateKey::new(org_id, type_id, agg_id)
}

/// Check if this metablock is a SoftTrim for the given aggregate
#[inline]
pub fn is_soft_trim_for_aggregate(bytes: &[u8], target: &AggregateKey) -> bool {
    if read_metablock_kind_discriminant(bytes) != DISCRIMINANT_SOFT_TRIM {
        return false;
    }

    let org_id = read_soft_trim_org_id(bytes);
    let type_id = read_soft_trim_aggregate_type_id(bytes);
    let agg_id = read_soft_trim_aggregate_id(bytes);

    org_id == target.org_id 
        && type_id == target.aggregate_type_id 
        && agg_id == target.aggregate_id
}

// --- SoftTrim field readers ---

#[inline]
pub fn read_soft_trim_org_id(bytes: &[u8]) -> u128 {
    let offset = SOFT_TRIM_PAYLOAD_OFFSET + AggregateKey::OFFSET_ORG_ID;
    read_u128_le(bytes, offset)
}

#[inline]
pub fn read_soft_trim_aggregate_type_id(bytes: &[u8]) -> u128 {
    let offset = SOFT_TRIM_PAYLOAD_OFFSET + AggregateKey::OFFSET_AGGREGATE_TYPE_ID;
    read_u128_le(bytes, offset)
}

#[inline]
pub fn read_soft_trim_aggregate_id(bytes: &[u8]) -> u128 {
    let offset = SOFT_TRIM_PAYLOAD_OFFSET + AggregateKey::OFFSET_AGGREGATE_ID;
    read_u128_le(bytes, offset)
}

#[inline]
pub fn read_soft_trim_keep_from_event_batch_index(bytes: &[u8]) -> u64 {
    // AggregateKey is 3 x u128 = 48 bytes
    let offset = SOFT_TRIM_PAYLOAD_OFFSET + AggregateKey::WIRE_SIZE_TOTAL;
    read_u64_le(bytes, offset)
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
pub fn read_event_batch_min_event_timestamp(bytes: &[u8]) -> u64 {
    let offset = EVENT_BATCH_PAYLOAD_OFFSET + MetablockEventBatch::OFFSET_MIN_EVENT_TIMESTAMP;
    read_u64_le(bytes, offset)
}

#[inline]
pub fn read_event_batch_max_event_timestamp(bytes: &[u8]) -> u64 {
    let offset = EVENT_BATCH_PAYLOAD_OFFSET + MetablockEventBatch::OFFSET_MAX_EVENT_TIMESTAMP;
    read_u64_le(bytes, offset)
}

#[inline]
pub fn read_event_batch_min_event_index(bytes: &[u8]) -> u64 {
    let offset = EVENT_BATCH_PAYLOAD_OFFSET + MetablockEventBatch::OFFSET_MIN_EVENT_INDEX;
    read_u64_le(bytes, offset)
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, GENESIS_HASH, WIRE_VERSION_WAL_METABLOCK};
    use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;
    use celeriant_wal::metablocks::metablock::Metablock;
    use celeriant_wal::metablocks::metablock_event_batch::{EventTypesKind, MetablockEventBatch};
    use celeriant_wal::metablocks::metablock_kind::MetablockKind;
    use celeriant_wal::metablocks::metablock_snapshot_org::MetablockSnapshotOrg;
    use celeriant_wal::metablocks::metablock_snapshot_aggregate::MetablockSnapshotAggregate;
    use celeriant_wal::metablocks::metablock_soft_delete::MetablockSoftDelete;
    use celeriant_wal::metablocks::metablock_soft_trim::MetablockSoftTrim;
    use crate::version_aware_wire_format::serialize_versioned_message;

    fn serialize_metablock(metablock: &Metablock) -> [u8; FIXED_BLOCK_SIZE_BYTES] {
        let mut buffer = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(metablock, WIRE_VERSION_WAL_METABLOCK, &mut buffer).unwrap();
        buffer
    }

    fn make_event_batch_metablock(
        wal_index: u64,
        server_timestamp: u64,
        _aggregate_key: AggregateKey,
        event_batch: MetablockEventBatch,
        datablock: DatablockStorageKind,
        compressed_size: u64,
        uncompressed_size: u64,
    ) -> Metablock {
        Metablock {
            wal_index,
            server_timestamp,
            lease_index: 1,
            node_id: 0xDEADBEEF,
            compressed_size,
            uncompressed_size,
            wal_metablock_type: MetablockKind::EventBatchMetadata(event_batch),
            datablock,
            previous_tip_hash: GENESIS_HASH,
        }
    }

    fn make_soft_delete_metablock(
        wal_index: u64,
        server_timestamp: u64,
        aggregate_key: AggregateKey,
        compressed_size: u64,
        uncompressed_size: u64,
    ) -> Metablock {
        Metablock {
            wal_index,
            server_timestamp,
            lease_index: 1,
            node_id: 0xCAFEBABE,
            compressed_size,
            uncompressed_size,
            wal_metablock_type: MetablockKind::SoftDelete(MetablockSoftDelete {
                aggregate_key,
                allow_recreate: true,
                allow_index_continuation: false,
                event_batch_index: 999,
                event_index: 888,
                client_id: 777,
                user_id: Some(666),
            }),
            datablock: DatablockStorageKind::None,
            previous_tip_hash: GENESIS_HASH,
        }
    }

    fn make_soft_trim_metablock(
        wal_index: u64,
        server_timestamp: u64,
        aggregate_key: AggregateKey,
        keep_from_event_batch_index: u64,
        compressed_size: u64,
        uncompressed_size: u64,
    ) -> Metablock {
        Metablock {
            wal_index,
            server_timestamp,
            lease_index: 1,
            node_id: 0xFEEDFACE,
            compressed_size,
            uncompressed_size,
            wal_metablock_type: MetablockKind::SoftTrim(MetablockSoftTrim {
                aggregate_key,
                keep_from_event_batch_index,
                client_id: 111,
                user_id: None,
            }),
            datablock: DatablockStorageKind::None,
            previous_tip_hash: GENESIS_HASH,
        }
    }

    fn make_snapshot_org_metablock(wal_index: u64, org_id: u128,
        compressed_size: u64,
        uncompressed_size: u64,) -> Metablock {
        Metablock {
            wal_index,
            server_timestamp: 12345,
            lease_index: 1,
            node_id: 0x1234,
            compressed_size,
            uncompressed_size,
            wal_metablock_type: MetablockKind::SnapshotOrg(MetablockSnapshotOrg { org_id }),
            datablock: DatablockStorageKind::None,
            previous_tip_hash: GENESIS_HASH,
        }
    }

    fn make_snapshot_aggregate_metablock(wal_index: u64, aggregate_key: AggregateKey,
        compressed_size: u64,
        uncompressed_size: u64,) -> Metablock {
        Metablock {
            wal_index,
            server_timestamp: 99999,
            lease_index: 2,
            node_id: 0x5678,
            compressed_size,
            uncompressed_size,
            wal_metablock_type: MetablockKind::SnapshotAggregate(MetablockSnapshotAggregate {
                aggregate_key,
                last_wal_index: 100,
                last_event_index: 200,
                last_event_batch_index: 50,
                min_available_event_index: 10,
                min_available_event_batch_index: 5,
                compressed_size_bytes: 1024,
                uncompressed_size_bytes: 4096,
                created_at: 1000,
                created_by_client_id: 2000,
                created_by_user_id: Some(3000),
            }),
            datablock: DatablockStorageKind::None,
            previous_tip_hash: GENESIS_HASH,
        }
    }

    // ==================== Discriminant Tests ====================

    #[test]
    fn read_discriminant_event_batch_metadata() {
        let key = AggregateKey::new(1, 2, 3);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([1, 2, 0, 0]),
        };
        let metablock = make_event_batch_metablock(1, 1000, key, batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_metablock_kind_discriminant(&bytes), DISCRIMINANT_EVENT_BATCH_METADATA);
        assert!(is_metablock_kind_event_batch_metadata(&bytes));
        assert!(!is_metablock_kind_soft_delete(&bytes));
        assert!(!is_metablock_kind_soft_trim(&bytes));
    }

    #[test]
    fn read_discriminant_snapshot_org() {
        let metablock = make_snapshot_org_metablock(5, 12345, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_metablock_kind_discriminant(&bytes), 1); // SnapshotOrg = 1
        assert!(!is_metablock_kind_event_batch_metadata(&bytes));
        assert!(!is_metablock_kind_soft_delete(&bytes));
        assert!(!is_metablock_kind_soft_trim(&bytes));
    }

    #[test]
    fn read_discriminant_snapshot_aggregate() {
        let key = AggregateKey::new(100, 200, 300);
        let metablock = make_snapshot_aggregate_metablock(10, key, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_metablock_kind_discriminant(&bytes), 3); // SnapshotAggregate = 3
    }

    #[test]
    fn read_discriminant_soft_delete() {
        let key = AggregateKey::new(1, 2, 3);
        let metablock = make_soft_delete_metablock(1, 1000, key, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_metablock_kind_discriminant(&bytes), DISCRIMINANT_SOFT_DELETE);
        assert!(is_metablock_kind_soft_delete(&bytes));
        assert!(!is_metablock_kind_event_batch_metadata(&bytes));
        assert!(!is_metablock_kind_soft_trim(&bytes));
    }

    #[test]
    fn read_discriminant_soft_trim() {
        let key = AggregateKey::new(1, 2, 3);
        let metablock = make_soft_trim_metablock(1, 1000, key, 50, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_metablock_kind_discriminant(&bytes), DISCRIMINANT_SOFT_TRIM);
        assert!(is_metablock_kind_soft_trim(&bytes));
        assert!(!is_metablock_kind_event_batch_metadata(&bytes));
        assert!(!is_metablock_kind_soft_delete(&bytes));
    }

    // ==================== Common Metablock Field Tests ====================

    #[test]
    fn read_wal_index_from_event_batch() {
        let key = AggregateKey::new(1, 2, 3);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let metablock = make_event_batch_metablock(0xDEAD_BEEF_CAFE_BABE, 1000, key, batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_wal_index(&bytes), 0xDEAD_BEEF_CAFE_BABE);
    }

    #[test]
    fn read_server_timestamp_from_event_batch() {
        let key = AggregateKey::new(1, 2, 3);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let metablock = make_event_batch_metablock(1, 0x1234_5678_9ABC_DEF0, key, batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_server_timestamp(&bytes), 0x1234_5678_9ABC_DEF0);
    }

    #[test]
    fn read_wal_index_from_soft_delete() {
        let key = AggregateKey::new(1, 2, 3);
        let metablock = make_soft_delete_metablock(42424242, 1000, key, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_wal_index(&bytes), 42424242);
    }

    #[test]
    fn read_wal_index_from_soft_trim() {
        let key = AggregateKey::new(1, 2, 3);
        let metablock = make_soft_trim_metablock(99887766, 1000, key, 50, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_wal_index(&bytes), 99887766);
    }

    // ==================== Event Batch Field Tests ====================

    #[test]
    fn read_event_batch_aggregate_key_fields() {
        let key = AggregateKey::new(
            0x1111_2222_3333_4444_5555_6666_7777_8888,
            0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_1111,
            0x9999_8888_7777_6666_5555_4444_3333_2222,
        );
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let metablock = make_event_batch_metablock(1, 1000, key.clone(), batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_event_batch_org_id(&bytes), 0x1111_2222_3333_4444_5555_6666_7777_8888);
        assert_eq!(read_event_batch_aggregate_type_id(&bytes), 0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_1111);
        assert_eq!(read_event_batch_aggregate_id(&bytes), 0x9999_8888_7777_6666_5555_4444_3333_2222);

        let read_key = read_event_batch_aggregate_key(&bytes);
        assert_eq!(read_key.org_id, key.org_id);
        assert_eq!(read_key.aggregate_type_id, key.aggregate_type_id);
        assert_eq!(read_key.aggregate_id, key.aggregate_id);
    }

    #[test]
    fn read_event_batch_index_fields() {
        let key = AggregateKey::new(1, 2, 3);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 0xFEDC_BA98_7654_3210,
            min_event_batch_index: 0x0123_4567_89AB_CDEF,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let metablock = make_event_batch_metablock(1, 1000, key, batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_event_batch_event_batch_index(&bytes), 0xFEDC_BA98_7654_3210);
        assert_eq!(read_event_batch_min_event_batch_index(&bytes), 0x0123_4567_89AB_CDEF);
    }

    #[test]
    fn read_event_batch_event_index_fields() {
        let key = AggregateKey::new(1, 2, 3);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0xAAAA_BBBB_CCCC_DDDD,
            max_event_index: 0xEEEE_FFFF_0000_1111,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let metablock = make_event_batch_metablock(1, 1000, key, batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_event_batch_min_event_index(&bytes), 0xAAAA_BBBB_CCCC_DDDD);
        assert_eq!(read_event_batch_max_event_index(&bytes), 0xEEEE_FFFF_0000_1111);
    }

    #[test]
    fn read_event_batch_timestamp_fields() {
        let key = AggregateKey::new(1, 2, 3);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 0x1234_5678_9ABC_DEF0,
            max_event_timestamp: 0xFEDC_BA98_7654_3210,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let metablock = make_event_batch_metablock(1, 1000, key, batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_event_batch_min_event_timestamp(&bytes), 0x1234_5678_9ABC_DEF0);
        assert_eq!(read_event_batch_max_event_timestamp(&bytes), 0xFEDC_BA98_7654_3210);
    }

    #[test]
    fn read_event_batch_client_fields() {
        let key = AggregateKey::new(1, 2, 3);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 0xABCD_EF01_2345_6789,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 0x1111_2222_3333_4444_5555_6666_7777_8888,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let metablock = make_event_batch_metablock(1, 1000, key, batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_event_batch_client_id(&bytes), 0x1111_2222_3333_4444_5555_6666_7777_8888);
        assert_eq!(read_event_batch_max_client_event_index(&bytes), 0xABCD_EF01_2345_6789);
    }

    // ==================== Aggregate Key Matching Tests ====================

    #[test]
    fn is_matches_aggregate_key_returns_true_for_matching_event_batch() {
        let key = AggregateKey::new(111, 222, 333);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let metablock = make_event_batch_metablock(1, 1000, key.clone(), batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert!(is_matches_aggregate_key(&bytes, &key));
    }

    #[test]
    fn is_matches_aggregate_key_returns_false_for_different_key() {
        let key = AggregateKey::new(111, 222, 333);
        let different_key = AggregateKey::new(111, 222, 999); // Different aggregate_id
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let metablock = make_event_batch_metablock(1, 1000, key, batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert!(!is_matches_aggregate_key(&bytes, &different_key));
    }

    #[test]
    fn is_matches_aggregate_key_returns_false_for_non_event_batch() {
        let key = AggregateKey::new(111, 222, 333);
        let metablock = make_soft_delete_metablock(1, 1000, key.clone(), 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert!(!is_matches_aggregate_key(&bytes, &key));
    }

    // ==================== Soft Delete Tests ====================

    #[test]
    fn read_soft_delete_aggregate_key_fields() {
        let key = AggregateKey::new(
            0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_1111,
            0x2222_3333_4444_5555_6666_7777_8888_9999,
            0x1234_5678_9ABC_DEF0_FEDC_BA98_7654_3210,
        );
        let metablock = make_soft_delete_metablock(1, 1000, key.clone(), 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_soft_delete_org_id(&bytes), key.org_id);
        assert_eq!(read_soft_delete_aggregate_type_id(&bytes), key.aggregate_type_id);
        assert_eq!(read_soft_delete_aggregate_id(&bytes), key.aggregate_id);

        let read_key = read_soft_delete_aggregate_key(&bytes);
        assert_eq!(read_key.org_id, key.org_id);
        assert_eq!(read_key.aggregate_type_id, key.aggregate_type_id);
        assert_eq!(read_key.aggregate_id, key.aggregate_id);
    }

    #[test]
    fn is_soft_delete_for_aggregate_returns_true_for_matching() {
        let key = AggregateKey::new(100, 200, 300);
        let metablock = make_soft_delete_metablock(1, 1000, key.clone(), 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert!(is_soft_delete_for_aggregate(&bytes, &key));
    }

    #[test]
    fn is_soft_delete_for_aggregate_returns_false_for_different_key() {
        let key = AggregateKey::new(100, 200, 300);
        let different_key = AggregateKey::new(100, 200, 999);
        let metablock = make_soft_delete_metablock(1, 1000, key, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert!(!is_soft_delete_for_aggregate(&bytes, &different_key));
    }

    #[test]
    fn is_soft_delete_for_aggregate_returns_false_for_non_soft_delete() {
        let key = AggregateKey::new(100, 200, 300);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let metablock = make_event_batch_metablock(1, 1000, key.clone(), batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert!(!is_soft_delete_for_aggregate(&bytes, &key));
    }

    // ==================== Soft Trim Tests ====================

    #[test]
    fn read_soft_trim_aggregate_key_fields() {
        let key = AggregateKey::new(
            0x1111_1111_1111_1111_1111_1111_1111_1111,
            0x2222_2222_2222_2222_2222_2222_2222_2222,
            0x3333_3333_3333_3333_3333_3333_3333_3333,
        );
        let metablock = make_soft_trim_metablock(1, 1000, key.clone(), 50, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_soft_trim_org_id(&bytes), key.org_id);
        assert_eq!(read_soft_trim_aggregate_type_id(&bytes), key.aggregate_type_id);
        assert_eq!(read_soft_trim_aggregate_id(&bytes), key.aggregate_id);
    }

    #[test]
    fn read_soft_trim_keep_from_event_batch_index_field() {
        let key = AggregateKey::new(1, 2, 3);
        let metablock = make_soft_trim_metablock(1, 1000, key, 0xDEAD_BEEF_CAFE_BABE, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_soft_trim_keep_from_event_batch_index(&bytes), 0xDEAD_BEEF_CAFE_BABE);
    }

    #[test]
    fn is_soft_trim_for_aggregate_returns_true_for_matching() {
        let key = AggregateKey::new(500, 600, 700);
        let metablock = make_soft_trim_metablock(1, 1000, key.clone(), 25, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert!(is_soft_trim_for_aggregate(&bytes, &key));
    }

    #[test]
    fn is_soft_trim_for_aggregate_returns_false_for_different_key() {
        let key = AggregateKey::new(500, 600, 700);
        let different_key = AggregateKey::new(500, 999, 700);
        let metablock = make_soft_trim_metablock(1, 1000, key, 25, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert!(!is_soft_trim_for_aggregate(&bytes, &different_key));
    }

    #[test]
    fn is_soft_trim_for_aggregate_returns_false_for_non_soft_trim() {
        let key = AggregateKey::new(500, 600, 700);
        let metablock = make_soft_delete_metablock(1, 1000, key.clone(), 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert!(!is_soft_trim_for_aggregate(&bytes, &key));
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn read_fields_with_max_values() {
        let key = AggregateKey::new(u128::MAX, u128::MAX, u128::MAX);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: u64::MAX,
            min_event_batch_index: u64::MAX,
            min_client_event_index: u64::MAX,
            max_client_event_index: u64::MAX,
            min_event_timestamp: u64::MAX,
            max_event_timestamp: u64::MAX,
            min_event_index: u64::MAX,
            max_event_index: u64::MAX,
            client_id: u128::MAX,
            user_id: Some(u128::MAX),
            event_types_data: EventTypesKind::Direct([u64::MAX; 4]),
        };
        let metablock = make_event_batch_metablock(u64::MAX, u64::MAX, key.clone(), batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_wal_index(&bytes), u64::MAX);
        assert_eq!(read_server_timestamp(&bytes), u64::MAX);
        assert_eq!(read_event_batch_org_id(&bytes), u128::MAX);
        assert_eq!(read_event_batch_aggregate_type_id(&bytes), u128::MAX);
        assert_eq!(read_event_batch_aggregate_id(&bytes), u128::MAX);
        assert_eq!(read_event_batch_event_batch_index(&bytes), u64::MAX);
        assert_eq!(read_event_batch_min_event_batch_index(&bytes), u64::MAX);
        assert_eq!(read_event_batch_min_event_timestamp(&bytes), u64::MAX);
        assert_eq!(read_event_batch_max_event_timestamp(&bytes), u64::MAX);
        assert_eq!(read_event_batch_min_event_index(&bytes), u64::MAX);
        assert_eq!(read_event_batch_max_event_index(&bytes), u64::MAX);
        assert_eq!(read_event_batch_client_id(&bytes), u128::MAX);
        assert_eq!(read_event_batch_max_client_event_index(&bytes), u64::MAX);
    }

    #[test]
    fn read_fields_with_zero_values() {
        let key = AggregateKey::new(0, 0, 0);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 0,
            min_event_batch_index: 0,
            min_client_event_index: 0,
            max_client_event_index: 0,
            min_event_timestamp: 0,
            max_event_timestamp: 0,
            min_event_index: 0,
            max_event_index: 0,
            client_id: 0,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let metablock = make_event_batch_metablock(0, 0, key.clone(), batch, DatablockStorageKind::None, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_wal_index(&bytes), 0);
        assert_eq!(read_server_timestamp(&bytes), 0);
        assert_eq!(read_event_batch_org_id(&bytes), 0);
        assert_eq!(read_event_batch_aggregate_type_id(&bytes), 0);
        assert_eq!(read_event_batch_aggregate_id(&bytes), 0);
        assert_eq!(read_event_batch_event_batch_index(&bytes), 0);
        assert_eq!(read_event_batch_client_id(&bytes), 0);
    }

    #[test]
    fn soft_delete_with_max_aggregate_key_values() {
        let key = AggregateKey::new(u128::MAX, u128::MAX, u128::MAX);
        let metablock = make_soft_delete_metablock(1, 1000, key.clone(), 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_soft_delete_org_id(&bytes), u128::MAX);
        assert_eq!(read_soft_delete_aggregate_type_id(&bytes), u128::MAX);
        assert_eq!(read_soft_delete_aggregate_id(&bytes), u128::MAX);

        let read_key = read_soft_delete_aggregate_key(&bytes);
        assert_eq!(read_key.org_id, u128::MAX);
        assert_eq!(read_key.aggregate_type_id, u128::MAX);
        assert_eq!(read_key.aggregate_id, u128::MAX);
    }

    #[test]
    fn soft_trim_with_max_values() {
        let key = AggregateKey::new(u128::MAX, u128::MAX, u128::MAX);
        let metablock = make_soft_trim_metablock(u64::MAX, u64::MAX, key.clone(), u64::MAX, 0, 0);
        let bytes = serialize_metablock(&metablock);

        assert_eq!(read_soft_trim_org_id(&bytes), u128::MAX);
        assert_eq!(read_soft_trim_aggregate_type_id(&bytes), u128::MAX);
        assert_eq!(read_soft_trim_aggregate_id(&bytes), u128::MAX);
        assert_eq!(read_soft_trim_keep_from_event_batch_index(&bytes), u64::MAX);
    }

    // ==================== Cross-type Verification Tests ====================

    #[test]
    fn all_discriminants_are_distinct() {
        let key = AggregateKey::new(1, 2, 3);

        // EventBatchMetadata
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let event_batch_bytes = serialize_metablock(&make_event_batch_metablock(1, 1000, key.clone(), batch, DatablockStorageKind::None, 0, 0));

        // SnapshotOrg
        let snapshot_org_bytes = serialize_metablock(&make_snapshot_org_metablock(1, 123, 0, 0));

        // SnapshotAggregate
        let snapshot_agg_bytes = serialize_metablock(&make_snapshot_aggregate_metablock(1, key.clone(), 0, 0));

        // SoftDelete
        let soft_delete_bytes = serialize_metablock(&make_soft_delete_metablock(1, 1000, key.clone(), 0, 0));

        // SoftTrim
        let soft_trim_bytes = serialize_metablock(&make_soft_trim_metablock(1, 1000, key.clone(), 50, 0, 0));

        let discriminants = [
            read_metablock_kind_discriminant(&event_batch_bytes),
            read_metablock_kind_discriminant(&snapshot_org_bytes),
            read_metablock_kind_discriminant(&snapshot_agg_bytes),
            read_metablock_kind_discriminant(&soft_delete_bytes),
            read_metablock_kind_discriminant(&soft_trim_bytes),
        ];

        // Verify expected values match MetablockKind enum
        assert_eq!(discriminants[0], 0); // EventBatchMetadata
        assert_eq!(discriminants[1], 1); // SnapshotOrg
        assert_eq!(discriminants[2], 3); // SnapshotAggregate
        assert_eq!(discriminants[3], 4); // SoftDelete
        assert_eq!(discriminants[4], 5); // SoftTrim

        // Verify all are unique
        let mut unique = discriminants.to_vec();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), discriminants.len(), "Discriminants should all be unique");
    }

    #[test]
    fn aggregate_key_matching_is_exact() {
        let key = AggregateKey::new(100, 200, 300);
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let bytes = serialize_metablock(&make_event_batch_metablock(1, 1000, key.clone(), batch, DatablockStorageKind::None, 0, 0));

        // Exact match
        assert!(is_matches_aggregate_key(&bytes, &key));

        // Off by one in each field
        assert!(!is_matches_aggregate_key(&bytes, &AggregateKey::new(99, 200, 300)));
        assert!(!is_matches_aggregate_key(&bytes, &AggregateKey::new(101, 200, 300)));
        assert!(!is_matches_aggregate_key(&bytes, &AggregateKey::new(100, 199, 300)));
        assert!(!is_matches_aggregate_key(&bytes, &AggregateKey::new(100, 201, 300)));
        assert!(!is_matches_aggregate_key(&bytes, &AggregateKey::new(100, 200, 299)));
        assert!(!is_matches_aggregate_key(&bytes, &AggregateKey::new(100, 200, 301)));
    }

    #[test]
    fn common_fields_consistent_across_metablock_types() {
        let key = AggregateKey::new(1, 2, 3);
        let wal_index = 0xABCD_EF01_2345_6789;
        let server_timestamp = 0x9876_5432_10FE_DCBA;

        // EventBatch
        let batch = MetablockEventBatch {
            aggregate_key: key.clone(),
            event_batch_index: 1,
            min_event_batch_index: 1,
            min_client_event_index: 0,
            max_client_event_index: 10,
            min_event_timestamp: 1000,
            max_event_timestamp: 2000,
            min_event_index: 0,
            max_event_index: 5,
            client_id: 100,
            user_id: None,
            event_types_data: EventTypesKind::Direct([0; 4]),
        };
        let event_batch_metablock = make_event_batch_metablock(wal_index, server_timestamp, key.clone(), batch, DatablockStorageKind::None, 0, 0);
        let event_batch_bytes = serialize_metablock(&event_batch_metablock);

        // SoftDelete
        let soft_delete_metablock = make_soft_delete_metablock(wal_index, server_timestamp, key.clone(), 0, 0);
        let soft_delete_bytes = serialize_metablock(&soft_delete_metablock);

        // SoftTrim
        let soft_trim_metablock = make_soft_trim_metablock(wal_index, server_timestamp, key.clone(), 50, 0, 0);
        let soft_trim_bytes = serialize_metablock(&soft_trim_metablock);

        // Verify wal_index is read correctly from all types
        assert_eq!(read_wal_index(&event_batch_bytes), wal_index);
        assert_eq!(read_wal_index(&soft_delete_bytes), wal_index);
        assert_eq!(read_wal_index(&soft_trim_bytes), wal_index);

        // Verify server_timestamp is read correctly from all types
        assert_eq!(read_server_timestamp(&event_batch_bytes), server_timestamp);
        assert_eq!(read_server_timestamp(&soft_delete_bytes), server_timestamp);
        assert_eq!(read_server_timestamp(&soft_trim_bytes), server_timestamp);
    }
}