use std::sync::Arc;
use eventplanedb_structures::{
    aggregate_key::AggregateKey,
    event_item::EventItem,
};
use crate::read_operations::{
    read_operations::ReadOperationsWithDmaFiles,
    read_structures::AggregateReadConfig,
};

/// Creates a test aggregate key with predictable values
pub fn create_test_aggregate_key() -> AggregateKey {
    AggregateKey::new(1, 1, 1)
}

/// Creates test events with sequential indexes
/// 
/// # Parameters
/// * `starting_client_event_index` - Base index for client event indexes
/// * `count` - Number of events to create
/// * `base_timestamp` - Base timestamp for events
pub fn create_test_events(
    starting_client_event_index: u64,
    count: usize,
    base_timestamp: u64,
) -> Vec<EventItem> {
    let mut events = Vec::with_capacity(count);
    
    for i in 0..count {
        let client_event_index = starting_client_event_index + i as u64;
        
        events.push(EventItem {
            client_event_index,
            event_index: 0, // Will be set by writer
            event_id: Some((client_event_index as u128) << 64 | i as u128),
            event_timestamp: base_timestamp + i as u64,
            event_type_major: 1 + (i % 3) as u64, // Vary event types 1, 2, 3
            event_type_minor: 0,
            event_value: Arc::new(format!("test_event_{}", i).into_bytes()),
            iv: None,
        });
    }
    
    events
}

/// Sets up a test aggregate with empty files
/// 
/// Returns (ReadOperationsWithDmaFiles, metadata_path, event_batches_path)
pub async fn setup_test_aggregate(
    base_path: &str,
    read_config: AggregateReadConfig,
) -> (ReadOperationsWithDmaFiles, String, String) {
    // Clean up any existing test data
    let _ = std::fs::remove_dir_all(base_path);
    
    // Create directory structure
    std::fs::create_dir_all(base_path).unwrap();
    
    let metadata_path = format!("{}/metadata.bin", base_path);
    let event_batches_path = format!("{}/event_batches.bin", base_path);
    
    // Open with create_if_not_exists=true
    let reader = ReadOperationsWithDmaFiles::open(
        base_path,
        &metadata_path,
        &event_batches_path,
        true, // create_if_not_exists
        read_config,
    )
    .await
    .unwrap();
    
    (reader, metadata_path, event_batches_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_events_generates_sequential_indexes() {
        let events = create_test_events(10, 5, 1000);
        
        assert_eq!(events.len(), 5);
        assert_eq!(events[0].client_event_index, 10);
        assert_eq!(events[4].client_event_index, 14);
        assert_eq!(events[0].event_timestamp, 1000);
        assert_eq!(events[4].event_timestamp, 1004);
    }

    #[test]
    fn test_create_test_events_varies_event_types() {
        let events = create_test_events(0, 6, 0);
        
        // Should cycle through types 1, 2, 3
        assert_eq!(events[0].event_type_major, 1);
        assert_eq!(events[1].event_type_major, 2);
        assert_eq!(events[2].event_type_major, 3);
        assert_eq!(events[3].event_type_major, 1);
        assert_eq!(events[4].event_type_major, 2);
        assert_eq!(events[5].event_type_major, 3);
    }

    #[test]
    fn test_create_test_aggregate_key() {
        let key = create_test_aggregate_key();
        assert_eq!(key.org_id, 1);
        assert_eq!(key.aggregate_type_id, 1);
        assert_eq!(key.aggregate_id, 1);
    }
}