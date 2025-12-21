use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

/// Result of extracting unique event types from a batch.
pub struct EventTypeExtraction {
    /// Up to 4 unique event types (u64::MAX for unused slots)
    pub event_types: [u64; 4],
    /// True if more than 4 unique types were found (need bloom filter)
    pub needs_bloom: bool,
}

/// Extract unique event types from events, determining storage strategy.
///
/// Returns up to 4 unique event types in the array. If more than 4 unique
/// types exist, `needs_bloom` is true and caller should use bloom filter.
///
/// # Example
/// ```ignore
/// let extraction = extract_unique_event_types(&events);
/// if extraction.needs_bloom {
///     let bloom_bytes = bloom_cache.create_bloom_bytes(&events);
///     EventTypesKind::Bloom(bloom_bytes)
/// } else {
///     EventTypesKind::Direct(extraction.event_types)
/// }
/// ```
pub fn extract_unique_event_types(events: &[DatablockAggregateEvent]) -> EventTypeExtraction {
    let mut event_types = [u64::MAX; 4];
    let mut unique_count = 0;

    for event in events {
        let event_type = event.event_type_major;

        // Check if we already have this event type
        let already_seen = (0..unique_count).any(|i| event_types[i] == event_type);
        if already_seen {
            continue;
        }

        // New unique event type
        if unique_count < 4 {
            event_types[unique_count] = event_type;
            unique_count += 1;
        } else {
            return EventTypeExtraction {
                event_types,
                needs_bloom: true,
            };
        }
    }

    EventTypeExtraction {
        event_types,
        needs_bloom: false,
    }
}


#[cfg(test)]
mod tests {
    use crate::bloom::bloom_filter_cache::BloomFilterCache;

    use super::*;
    use std::sync::Arc;

    fn make_event(event_type: u64) -> DatablockAggregateEvent {
        DatablockAggregateEvent {
            event_type_major: event_type,
            event_value: Arc::new(vec![]),
            ..Default::default()
        }
    }

    #[test]
    fn extract_single_event_type() {
        let events = vec![make_event(42), make_event(42), make_event(42)];
        let result = extract_unique_event_types(&events);

        assert!(!result.needs_bloom);
        assert_eq!(result.event_types[0], 42);
        assert_eq!(result.event_types[1], u64::MAX);
    }

    #[test]
    fn extract_four_event_types() {
        let events = vec![
            make_event(1),
            make_event(2),
            make_event(3),
            make_event(4),
            make_event(1), // duplicate
        ];
        let result = extract_unique_event_types(&events);

        assert!(!result.needs_bloom);
    }

    #[test]
    fn extract_five_event_types_needs_bloom() {
        let events = vec![
            make_event(1),
            make_event(2),
            make_event(3),
            make_event(4),
            make_event(5),
        ];
        let result = extract_unique_event_types(&events);

        assert!(result.needs_bloom);
    }

    #[test]
    fn extract_empty_events() {
        let events: Vec<DatablockAggregateEvent> = vec![];
        let result = extract_unique_event_types(&events);

        assert!(!result.needs_bloom);
    }

    #[test]
    fn bloom_cache_creates_valid_bytes() {
        let cache = BloomFilterCache::new();
        let events = vec![
            make_event(100),
            make_event(200),
            make_event(300),
            make_event(400),
            make_event(500),
        ];

        let bytes = cache.create_bloom_bytes(&events);

        // Should have some bits set (not all zeros)
        assert!(bytes.iter().any(|&b| b != 0));
    }

    #[test]
    fn bloom_cache_is_reusable() {
        let cache = BloomFilterCache::new();

        let events1 = vec![make_event(1), make_event(2)];
        let bytes1 = cache.create_bloom_bytes(&events1);

        let events2 = vec![make_event(100), make_event(200)];
        let bytes2 = cache.create_bloom_bytes(&events2);

        // Different events should produce different bloom filters
        assert_ne!(bytes1, bytes2);
    }
}
