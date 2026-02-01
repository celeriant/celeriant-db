//! S3 fallback replication for when follower is unreachable.
//!
//! When the follower becomes unreachable, the leader writes batches to S3
//! instead. A new leader must consume these batches before accepting writes.

pub use celeriant_wal::s3::fallback_batch::{FallbackBatch, FallbackItem};

use crate::paths;

/// Get the S3 path for a fallback batch.
pub fn fallback_batch_s3_path(batch: &FallbackBatch) -> String {
    paths::fallback_batch_path(batch.shard_id, batch.fallback_index, batch.end_wal_index)
}

/// Parse a fallback batch path to extract shard_id, start_index, and end_index.
/// Returns None if the path doesn't match the expected format.
pub fn parse_fallback_path(path: &str) -> Option<(u32, u64, u64)> {
    // Expected format: cluster/fallback/shard_XXX/batch_XXXXXXXXX_XXXXXXXXX.bin
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 3 {
        return None;
    }

    let shard_part = parts.iter().find(|p| p.starts_with("shard_"))?;
    let batch_part = parts.iter().find(|p| p.starts_with("batch_"))?;

    let shard_id: u32 = shard_part.strip_prefix("shard_")?.parse().ok()?;
    let batch_name = batch_part.strip_prefix("batch_")?.strip_suffix(".bin")?;

    let indices: Vec<&str> = batch_name.split('_').collect();
    if indices.len() != 2 {
        return None;
    }

    let start_index: u64 = indices[0].parse().ok()?;
    let end_index: u64 = indices[1].parse().ok()?;

    Some((shard_id, start_index, end_index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::datablocks::datablock::Datablock;
    use celeriant_wal::datablocks::datablock_kind::DatablockKind;
    use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
    use celeriant_wal::metablocks::metablock::Metablock;
    use celeriant_wal::metablocks::metablock_kind::MetablockKind;
    use celeriant_wal::metablocks::metablock_event_batch::MetablockEventBatch;
    use celeriant_wal::metablocks::datablock_storage_kind::DatablockStorageKind;

    #[test]
    fn test_fallback_batch_s3_path() {
        let batch = FallbackBatch::new(5, 10, 2);
        assert_eq!(fallback_batch_s3_path(&batch), "cluster/fallback/shard_002/batch_000000005_000000010.bin");
    }

    #[test]
    fn test_parse_fallback_path() {
        assert_eq!(
            parse_fallback_path("cluster/fallback/shard_002/batch_000000005_000000010.bin"),
            Some((2, 5, 10))
        );
        assert_eq!(
            parse_fallback_path("cluster/fallback/shard_015/batch_123456789_123456799.bin"),
            Some((15, 123456789, 123456799))
        );
        assert_eq!(parse_fallback_path("cluster/lease.bin"), None);
        assert_eq!(parse_fallback_path("invalid"), None);
        assert_eq!(parse_fallback_path("cluster/fallback/shard_002/batch_000000005.bin"), None);
    }

    #[test]
    fn test_fallback_batch_bincode_roundtrip() {
        let aggregate_key = AggregateKey::new(1, 2, 3);

        let metablock1 = Metablock {
            wal_index: 42,
            server_timestamp: 1000,
            lease_index: 5,
            node_id: 999,
            uncompressed_size: 1024,
            compressed_size: 512,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [1u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                event_batch_index: 10,
                min_event_batch_index: 1,
                min_client_event_index: 1,
                max_client_event_index: 5,
                min_event_timestamp: 100,
                max_event_timestamp: 500,
                min_event_index: 1,
                max_event_index: 5,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::Block(celeriant_wal::metablocks::datablock_block_ref::DatablockBlockRef {
                crc32c: 0,
                datablock_position: 1000,
            }),
        };

        let metablock2 = Metablock {
            wal_index: 43,
            server_timestamp: 2000,
            lease_index: 5,
            node_id: 999,
            uncompressed_size: 2048,
            compressed_size: 1024,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [2u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                event_batch_index: 11,
                min_event_batch_index: 1,
                min_client_event_index: 6,
                max_client_event_index: 10,
                min_event_timestamp: 600,
                max_event_timestamp: 1000,
                min_event_index: 6,
                max_event_index: 10,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::None,
        };

        let datablock1 = Some(Datablock {
            datablock_kind: DatablockKind::EventBatchItem(DatablockAggregateEventBatch {
                event_batch_index: 10,
                events: vec![],
            }),
        });

        let original_batch = FallbackBatch {
            fallback_index: 42,
            end_wal_index: 43,
            shard_id: 7,
            items: vec![
                FallbackItem {
                    metablock: metablock1,
                    datablock: datablock1,
                },
                FallbackItem {
                    metablock: metablock2,
                    datablock: None,
                },
            ],
        };

        let serialized = celeriant_wire::disk::versioned_block::serialize_versioned_message_heap(
            &original_batch,
            celeriant_wal::constants::WIRE_VERSION_S3_FALLBACK_BATCH,
        ).expect("serialization should succeed");

        let deserialized = celeriant_wire::disk::versioned_block::deserialise_fallback_batch(&serialized)
            .expect("deserialization should succeed");

        assert_eq!(deserialized.fallback_index, 42);
        assert_eq!(deserialized.end_wal_index, 43);
        assert_eq!(deserialized.shard_id, 7);
        assert_eq!(deserialized.items.len(), 2);

        assert_eq!(deserialized.items[0].metablock.wal_index, 42);
        assert_eq!(deserialized.items[0].metablock.server_timestamp, 1000);
        assert!(deserialized.items[0].datablock.is_some());

        assert_eq!(deserialized.items[1].metablock.wal_index, 43);
        assert_eq!(deserialized.items[1].metablock.server_timestamp, 2000);
        assert!(deserialized.items[1].datablock.is_none());
    }

    #[test]
    fn test_fallback_index_is_first_wal_index() {
        let aggregate_key = AggregateKey::new(1, 2, 3);

        let metablock_first = Metablock {
            wal_index: 100,
            server_timestamp: 1000,
            lease_index: 5,
            node_id: 999,
            uncompressed_size: 1024,
            compressed_size: 512,
            datablock_version: 1,
            datablock_compression_type: 1,
            previous_tip_hash: [1u8; 32],
            wal_metablock_type: MetablockKind::EventBatchMetadata(MetablockEventBatch {
                aggregate_key: aggregate_key.clone(),
                event_batch_index: 10,
                min_event_batch_index: 1,
                min_client_event_index: 1,
                max_client_event_index: 5,
                min_event_timestamp: 100,
                max_event_timestamp: 500,
                min_event_index: 1,
                max_event_index: 5,
                client_id: 123,
                user_id: None,
                event_types_data: celeriant_wal::metablocks::metablock_event_batch::EventTypesKind::Direct([7, 0, 0, 0]),
            }),
            datablock: DatablockStorageKind::None,
        };

        let metablock_second = Metablock {
            wal_index: 101,
            ..metablock_first.clone()
        };

        let batch = FallbackBatch {
            fallback_index: 100,
            end_wal_index: 101,
            shard_id: 5,
            items: vec![
                FallbackItem {
                    metablock: metablock_first,
                    datablock: None,
                },
                FallbackItem {
                    metablock: metablock_second,
                    datablock: None,
                },
            ],
        };

        assert_eq!(batch.fallback_index, batch.items[0].metablock.wal_index);
        assert_eq!(batch.end_wal_index, batch.items[batch.items.len() - 1].metablock.wal_index);
    }

    #[test]
    fn test_shard_id_narrowing() {
        let batch_0 = FallbackBatch::new(1, 5, 0);
        assert_eq!(
            fallback_batch_s3_path(&batch_0),
            "cluster/fallback/shard_000/batch_000000001_000000005.bin"
        );

        let batch_999 = FallbackBatch::new(1, 10, 999);
        assert_eq!(
            fallback_batch_s3_path(&batch_999),
            "cluster/fallback/shard_999/batch_000000001_000000010.bin"
        );

        assert!(u32::MAX > 999);
    }
}
