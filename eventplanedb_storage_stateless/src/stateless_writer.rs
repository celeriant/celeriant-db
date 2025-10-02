use eventplanedb_storage_structures::compression_type::CompressionType;
use eventplanedb_storage_structures::constants::{BINCODE_CONFIG_FIXED, BLOOM_BYTES};
use eventplanedb_storage_structures::event_batch_item::EventBatchItem;
use eventplanedb_storage_structures::event_batch_metadata::EventBatchMetadata;
use eventplanedb_storage_structures::event_item::EventItem;
use fastbloom::BloomFilter;

use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::io::{self};

use crate::stateless_engine::StatelessEngine;
use crate::wire_format::to_wire_format_variable;

pub trait StatelessWriter {
    fn append_event_batch(
        &self,
        event_batch_writer: &mut File,
        metadata_writer: &mut File,
        bloom_filter: &mut BloomFilter,
        event_type_dedup: &mut HashSet<u64>,
        compression_type: CompressionType,
        event_batch_item: &EventBatchItem,
    ) -> io::Result<EventBatchMetadata>;
}

impl StatelessWriter for StatelessEngine {
    fn append_event_batch(
        &self,
        event_batch_writer: &mut File,
        metadata_writer: &mut File,
        bloom_filter: &mut BloomFilter,
        event_type_dedup: &mut HashSet<u64>,
        compression_type: CompressionType,
        event_batch_item: &EventBatchItem,
    ) -> io::Result<EventBatchMetadata> {
        if event_batch_item.events.is_empty() {
            return Err(io::Error::other("Cannot write empty event batch"));
        }

        // Serialize and compress the event data
        let (uncompressed_size, compressed_event_batch_item) =
            to_wire_format_variable(&event_batch_item, compression_type)?;
        let events_crc = crc32fast::hash(&compressed_event_batch_item);

        // Determine event types data (bloom filter or direct array)
        let (event_types, use_bloom) = extract_unique_event_types(&event_batch_item.events);
        let event_types_data = if use_bloom {
            let bloom_bytes =
                create_bloom_filter_bytes(bloom_filter, event_type_dedup, &event_batch_item.events);
            eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Bloom(
                bloom_bytes,
            )
        } else {
            eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Direct(
                event_types,
            )
        };

        // Create and serialise metadata
        let metadata = eventplanedb_storage_structures::event_batch_metadata::EventBatchMetadata::from_batch_item(
            event_batch_item,
            uncompressed_size as u64,
            compressed_event_batch_item.len() as u64,
            compression_type,
            event_types_data,
            events_crc,
        );

        let metadata_bytes = bincode::encode_to_vec(&metadata, BINCODE_CONFIG_FIXED)
            .map_err(|e| io::Error::other(e.to_string()))?;

        // Write data to disk
        event_batch_writer.write_all(&compressed_event_batch_item)?;
        metadata_writer.write_all(&metadata_bytes)?;

        //TODO: Test for file corruption - pull out USB stick and see if corrupt, and if we can repair
        event_batch_writer.flush()?;
        metadata_writer.flush()?;

        Ok(metadata)
    }
}

fn extract_unique_event_types(events: &[EventItem]) -> ([u64; 4], bool) {
    let mut bloom_or_event_types = [u64::MAX, u64::MAX, u64::MAX, u64::MAX];
    let mut use_bloom = false;
    let mut unique_count = 0;

    for event in events {
        let event_type = event.event_type_major;

        // Check if we already have this event type
        if unique_count > 0 && bloom_or_event_types[0] == event_type {
            continue;
        }
        if unique_count > 1 && bloom_or_event_types[1] == event_type {
            continue;
        }
        if unique_count > 2 && bloom_or_event_types[2] == event_type {
            continue;
        }
        if unique_count > 3 && bloom_or_event_types[3] == event_type {
            continue;
        }

        // New unique event type
        if unique_count < 4 {
            bloom_or_event_types[unique_count] = event_type;
            unique_count += 1;
        } else {
            use_bloom = true;
            break;
        }
    }

    (bloom_or_event_types, use_bloom)
}

fn create_bloom_filter_bytes(
    filter: &mut BloomFilter,
    event_type_dedup: &mut HashSet<u64>,
    events: &[EventItem],
) -> [u64; BLOOM_BYTES / 8] {
    // Populate bloom filter with multiple event types
    filter.clear();
    event_type_dedup.clear();

    for event in events {
        event_type_dedup.insert(event.event_type_major);
    }

    for &event_type in event_type_dedup.iter() {
        filter.insert(&event_type.to_le_bytes());
    }

    filter.as_slice().try_into().expect("Conversion failed")
}
