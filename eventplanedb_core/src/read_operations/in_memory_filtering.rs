use eventplanedb_structures::{constants::{BLOOM_BYTES, BLOOM_HASH_COUNT, BLOOM_HASH_SEED}, event_batch_item::EventBatchItem, event_batch_metadata::{EventBatchMetadata, EventTypesData}, read_filters::ReadFilters};
use fastbloom::BloomFilter;

use crate::read_operations::{read_error::ReadError, read_structures::MetadataWithAbsolutePosition};

pub fn apply_event_filters(event_batch: &mut EventBatchItem, read_filters: &ReadFilters) {
    // Final event type filtering (bloom filter might have false positives)
    if let Some(event_types) = read_filters.include_event_types.as_deref() {
        event_batch
            .events
            .retain(|event| event_types.contains(&event.event_type_major));
    }

    // Final filtering for local_index
    if let Some(min_event_index) = read_filters.min_event_index {
        event_batch
            .events
            .retain(|event| event.event_index >= min_event_index);
    }

    if let Some(max_event_index) = read_filters.max_event_index {
        event_batch
            .events
            .retain(|event| event.event_index <= max_event_index);
    }

    // Final filtering for event_time
    if let Some(min_event_time) = read_filters.min_event_timestamp {
        event_batch
            .events
            .retain(|event| event.event_timestamp >= min_event_time);
    }

    if let Some(max_event_time) = read_filters.max_event_timestamp {
        event_batch
            .events
            .retain(|event| event.event_timestamp <= max_event_time);
    }

    // Final filtering for event index
    if let Some(min_client_event_index) = read_filters.min_client_event_index {
        event_batch
            .events
            .retain(|event| event.client_event_index >= min_client_event_index);
    }

    if let Some(max_client_event_index) = read_filters.max_client_event_index {
        event_batch
            .events
            .retain(|event| event.client_event_index <= max_client_event_index);
    }
}

pub fn is_include_batch(metadata: &EventBatchMetadata, filters: &ReadFilters) -> bool {
    if metadata.event_batch_index < filters.from_event_batch_index {
        return false;
    }

    if filters.to_event_batch_index.map_or(false, |to_server_id| {
        metadata.event_batch_index > to_server_id
    }) {
        return false;
    }

    if filters
        .min_server_timestamp
        .map_or(false, |before_server_time| {
            metadata.server_timestamp < before_server_time
        })
    {
        return false;
    }

    if filters
        .max_server_timestamp
        .map_or(false, |after_server_time| {
            metadata.server_timestamp > after_server_time
        })
    {
        return false;
    }

    if filters
        .exclude_client_id
        .map_or(false, |exclude_client_id| {
            metadata.client_id == exclude_client_id
        })
    {
        return false;
    }

    if filters
        .include_client_id
        .map_or(false, |include_client_id| {
            metadata.client_id != include_client_id
        })
    {
        return false;
    }

    if filters
        .exclude_user_id
        .map_or(false, |exclude_user_id| metadata.user_id == exclude_user_id)
    {
        return false;
    }

    if filters
        .include_user_id
        .map_or(false, |include_user_id| metadata.user_id != include_user_id)
    {
        return false;
    }

    if filters.min_client_event_index.map_or(false, |min_index| {
        metadata.max_client_event_index < min_index
    }) {
        return false;
    }

    if filters.max_client_event_index.map_or(false, |max_index| {
        metadata.min_client_event_index > max_index
    }) {
        return false;
    }

    if filters
        .min_event_timestamp
        .map_or(false, |min_time| metadata.max_event_timestamp < min_time)
    {
        return false;
    }

    if filters
        .max_event_timestamp
        .map_or(false, |max_time| metadata.min_event_timestamp > max_time)
    {
        return false;
    }

    if filters
        .min_event_index
        .map_or(false, |min_index| metadata.max_event_index < min_index)
    {
        return false;
    }

    if filters
        .max_event_index
        .map_or(false, |max_index| metadata.min_event_index > max_index)
    {
        return false;
    }

    if filters
        .include_event_types
        .as_ref()
        .map_or(false, |include_event_types| {
            //Is there at least one of the include_event_types in the event batch? If not, return true to skip
            let at_least_one_match =
                check_event_types_match(&metadata.event_types_data, &include_event_types);
            !at_least_one_match
        })
    {
        return false;
    }

    true
}

fn check_event_types_match(event_types_data: &EventTypesData, include_event_types: &[u64]) -> bool {
    match event_types_data {
        EventTypesData::Direct(event_types) => {
            // Check if any of the required types are in the direct array
            if event_types.len() < include_event_types.len() {
                event_types
                    .iter()
                    .any(|&batch_type| include_event_types.contains(&batch_type))
            } else {
                include_event_types
                    .iter()
                    .any(|&include_event_type| event_types.contains(&include_event_type))
            }
        }
        EventTypesData::Bloom(bloom_bytes) => {
            // Create bloom filter and test each required type
            let bloom = bloom_filter_from_bytes(bloom_bytes);
            include_event_types
                .iter()
                .any(|&include_event_type| bloom.contains(&include_event_type.to_le_bytes()))
        }
    }
}

fn bloom_filter_from_bytes(bloom_bytes: &[u64; BLOOM_BYTES / 8]) -> BloomFilter {
    BloomFilter::from_vec(bloom_bytes.to_vec())
        .seed(&BLOOM_HASH_SEED)
        .hashes(BLOOM_HASH_COUNT)
}


pub fn trim_end_if_exceeds_max_bytes(
    metadata_for_reading: &mut Vec<&MetadataWithAbsolutePosition>,
    read_filters: &ReadFilters,
    max_bytes: Option<usize>,
) -> Result<Option<u64>, ReadError> {
    // Only keep batches where include is true
    metadata_for_reading
        .retain(|batch| is_include_batch(&batch.event_batch_metadata, read_filters));

    // If no max_bytes limit is specified, we don't need to trim
    let max_bytes = match max_bytes {
        Some(limit) => limit as u64,
        None => return Ok(None),
    };

    // If after filtering we don't have any batches, return None
    if metadata_for_reading.is_empty() {
        return Ok(None);
    }

    // Calculate cumulative compressed size
    let mut cumulative_size: u64 = 0;
    let mut cut_index: Option<usize> = None;

    // Batches are sorted by event_batch_index (ascending)
    for (index, batch) in metadata_for_reading.iter().enumerate() {
        cumulative_size += batch.event_batch_metadata.compressed_size;

        // If we exceed the max_bytes limit, store this index as the cut point
        if cumulative_size > max_bytes {
            cut_index = Some(index);
            break;
        }
    }

    // If we need to trim
    if let Some(index) = cut_index {
        // Get the server_id of the first batch we're trimming
        let next_event_batch_index = if index < metadata_for_reading.len() {
            Some(
                metadata_for_reading[index]
                    .event_batch_metadata
                    .event_batch_index,
            )
        } else {
            None
        };

        // Keep only the batches that fit within the max_bytes limit
        metadata_for_reading.truncate(index);

        if metadata_for_reading.is_empty() {
            // Throw an error as max bytes was too small to return any event batches
            return Err(ReadError::MaxBytesTooSmall {
                current_max_bytes: max_bytes,
                required_max_bytes: cumulative_size,
            });
        }

        Ok(next_event_batch_index)
    } else {
        // No trimming needed, all batches fit within the limit
        Ok(None)
    }
}


pub fn apply_max_bytes_pagination(
    metadata_for_reading: &mut Vec<&MetadataWithAbsolutePosition>,
    max_bytes: Option<usize>,
) -> Result<Option<u64>, ReadError> {
    let max_bytes = match max_bytes {
        Some(limit) => limit as u64,
        None => return Ok(None),
    };

    if metadata_for_reading.is_empty() {
        return Ok(None);
    }

    let mut cumulative_size: u64 = 0;
    let mut cut_index: Option<usize> = None;

    for (index, batch) in metadata_for_reading.iter().enumerate() {
        cumulative_size += batch.event_batch_metadata.compressed_size;

        if cumulative_size > max_bytes {
            cut_index = Some(index);
            break;
        }
    }

    if let Some(index) = cut_index {
        let next_event_batch_index = if index < metadata_for_reading.len() {
            Some(
                metadata_for_reading[index]
                    .event_batch_metadata
                    .event_batch_index,
            )
        } else {
            None
        };

        metadata_for_reading.truncate(index);

        if metadata_for_reading.is_empty() {
            return Err(ReadError::MaxBytesTooSmall {
                current_max_bytes: max_bytes,
                required_max_bytes: cumulative_size,
            });
        }

        Ok(next_event_batch_index)
    } else {
        Ok(None)
    }
}
