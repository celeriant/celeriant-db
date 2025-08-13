use std::{
    collections::{HashMap, VecDeque},
    io,
};

use crate::{
    access_level::AccessLevel,
    aggregate_event_type::AggregateEventType,
    aggregate_to_share_links::{AggregateToShareLinks, ShareLinkAccessInfo},
};
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem, event_storage_cache::EventStorageCache};

pub struct ShareLinksCache {
    // The queue is used to evict the oldest files from the cache when the cache is full
    cache_queue: VecDeque<String>,

    // The cache maps a file to another hashmap of share_hash to share link data
    cache: HashMap<String, AggregateToShareLinks>,

    // The maximum number of projects to cache, currently we can have unlimited share links inside a project
    cache_max_aggregate_count: usize,
}

impl ShareLinksCache {
    pub fn new(cache_max_aggregate_count: usize) -> Self {
        Self {
            cache_queue: VecDeque::new(),
            cache: HashMap::new(),
            cache_max_aggregate_count,
        }
    }

    /// If we have exeeded the maximum nbr of projects in the cache, clear out the oldest ones
    fn clear_cache(&mut self) {
        if self.cache.len() < self.cache_max_aggregate_count {
            return;
        }

        while self.cache.len() > self.cache_max_aggregate_count {
            if let Some(file_path) = self.cache_queue.pop_front() {
                self.cache.remove(&file_path);
            } else {
                break;
            }
        }
    }

    /// Grab the current cache for a project, or build it if it doesn't exist and add it to the cache
    fn get_or_build_cache(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str) -> &mut AggregateToShareLinks {
        self.clear_cache();

        if self.cache.contains_key(file_path) {
            return self.cache.get_mut(file_path).unwrap();
        }

        self.cache.insert(file_path.to_string(), AggregateToShareLinks::new());
        self.cache_queue.push_back(file_path.to_string());

        self.populate_cache_for_aggregate(event_storage_cache, file_path);

        self.clear_cache();

        return self.cache.get_mut(file_path).unwrap();
    }

    /// Read all the ProvideAccess events for a project and build the cache for that project from the events found
    fn populate_cache_for_aggregate(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str) {
        let aggregate_to_user_access_level = self.cache.get_mut(file_path).unwrap();

        match event_storage_cache.read(
            file_path,
            0,
            usize::MAX,
            Some(&[AggregateEventType::ShareLinkCreated as u64, AggregateEventType::ShareLinkDisabled as u64]),
            None,
        ) {
            Ok(result) => {
                for event_batch_item in result.event_batches {
                    for event_item in event_batch_item.events.iter() {
                        // Share links are stored in the cache for quick lookup when a user tries to join
                        if event_item.event_type == AggregateEventType::ShareLinkCreated as u64
                            && event_item.uint_values.is_some()
                            && event_item.string_values.is_some()
                            && event_item.bool_values.is_some()
                            && event_item.byte_arrays.is_some()
                            && event_item.bool_values.as_ref().unwrap().len() > 0
                            && event_item.string_values.as_ref().unwrap().len() > 0
                            && event_item.uint_values.as_ref().unwrap().len() > 1
                            && event_item.byte_arrays.as_ref().unwrap().len() > 0
                        {
                            let share_id_bytes: &Vec<u8> = event_item.byte_arrays.as_ref().unwrap()[0].as_ref().unwrap();
                            let share_id: u128 = u128::from_le_bytes(share_id_bytes.as_slice().try_into().unwrap());

                            let share_link_access_info = ShareLinkAccessInfo::new(
                                AccessLevel::from(event_item.uint_values.as_ref().unwrap()[0]),
                                share_id,
                                event_item.bool_values.as_ref().unwrap()[0],
                                event_item.uint_values.as_ref().unwrap()[1],
                            );
                            aggregate_to_user_access_level.add_share_link(share_link_access_info.share_id, share_link_access_info);
                        }

                        // Share links can be disabled if used (single use link) or if an owner explicitly disables it
                        if event_item.event_type == AggregateEventType::ShareLinkDisabled as u64
                            && event_item.byte_arrays.is_some()
                            && event_item.byte_arrays.as_ref().unwrap().len() > 0
                        {
                            let share_id_bytes: &Vec<u8> = event_item.byte_arrays.as_ref().unwrap()[0].as_ref().unwrap();
                            let share_id: u128 = u128::from_le_bytes(share_id_bytes.as_slice().try_into().unwrap());
                            aggregate_to_user_access_level.remove_share_link(&share_id);
                        }
                    }
                }
            }

            // Fail to read, skip populating the cache for this project. Could be a new project or file deleted.
            Err(_) => {}
        }
    }

    /// Create a new share link with the hash of the share link code by adding a new event to the file.
    /// The actual share link code is never saved, only generated and returned to the client.
    /// Also updates the cache with the info for the new share link.
    pub fn create_share_link(
        &mut self,
        event_storage_cache: &mut EventStorageCache,
        file_path: &str,
        current_client_id: &u128,
        current_user_id: Option<&str>,
        share_id: u128,
        access_level: AccessLevel,
        is_single_use: bool,
        iv: Option<[u8; 12]>,
        description: Option<String>,
        expires_on: u64,
        server_time: u64,
    ) -> io::Result<EventBatchItem> {
        let mut event_item = EventItem::new();
        event_item.event_date = server_time;
        event_item.event_type = AggregateEventType::ShareLinkCreated as u64;
        event_item.iv_arrays = Some(vec![iv]);
        event_item.string_values = Some(vec![description]);
        event_item.uint_values = Some(vec![access_level as u64, expires_on]);
        event_item.bool_values = Some(vec![is_single_use]);
        event_item.byte_arrays = Some(vec![Some(share_id.to_le_bytes().to_vec())]);

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.events = vec![event_item];
        event_batch_item.client_id = *current_client_id;
        event_batch_item.user_id = current_user_id.map(|f| f.to_string());
        event_batch_item.server_date = server_time;

        // We don't allow auto create of the aggregate here as there should already be user access events added and file created
        event_batch_item.server_id = event_storage_cache.write(&file_path, false, true, event_batch_item.clone())?;

        let share_link_info = ShareLinkAccessInfo::new(access_level, share_id, is_single_use, expires_on);

        let cache = self.get_or_build_cache(event_storage_cache, &file_path);
        cache.add_share_link(share_id, share_link_info);

        Ok(event_batch_item)
    }

    /// Pulls the share link from the cache, returning a dto with
    /// the relevant info for access control decisions
    pub fn get_share_key_data_if_still_valid(
        &mut self,
        event_storage_cache: &mut EventStorageCache,
        file_path: &str,
        share_id: &u128,
    ) -> Option<&ShareLinkAccessInfo> {
        let aggregate_cache = self.get_or_build_cache(event_storage_cache, file_path);

        let is_expired = if let Some(share_link_access_info) = aggregate_cache.get_share_link(share_id) {
            share_link_access_info.expires_on > 0 && share_link_access_info.expires_on < chrono::Utc::now().timestamp_millis() as u64
        } else {
            return None; // Share link doesn't exist
        };

        // If expired, remove it from cache
        if is_expired {
            aggregate_cache.remove_share_link(share_id);
            return None;
        }

        // Return the valid share link (we know it exists since we checked above)
        aggregate_cache.get_share_link(share_id)
    }

    /// Creates a DisableShareLink event and writes it without
    /// checking permissions or the status of the existing share link.
    /// Also removes the share link from the cache
    pub fn disable_share_link(
        &mut self,
        event_storage_cache: &mut EventStorageCache,
        file_path: &str,
        current_client_id: &u128,
        current_user_id: Option<&str>,
        share_id: u128,
        server_time: u64,
    ) -> io::Result<EventBatchItem> {
        //TODO: Should we check if the share_id is valid? Could create duplicate disable events

        let mut event_item = EventItem::new();
        event_item.event_date = server_time;
        event_item.event_type = AggregateEventType::ShareLinkDisabled as u64;
        event_item.byte_arrays = Some(vec![Some(share_id.to_le_bytes().to_vec())]);

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.events = vec![event_item];
        event_batch_item.client_id = *current_client_id;
        event_batch_item.user_id = current_user_id.map(|f| f.to_string());
        event_batch_item.server_date = server_time;

        event_batch_item.server_id = event_storage_cache.write(&file_path, false, true, event_batch_item.clone())?;

        let cache = self.get_or_build_cache(event_storage_cache, &file_path);
        cache.remove_share_link(&share_id);

        Ok(event_batch_item)
    }

    pub fn clear_for_file_path(&mut self, file_path: &str) {
        self.cache.remove(file_path);
        self.cache_queue.retain(|path| path != file_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{access_level::AccessLevel, aggregate_event_type::AggregateEventType};
    use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem, event_storage_cache::EventStorageCache};
    use tempfile::TempDir;

    fn create_test_event_item() -> EventItem {
        let mut event1 = EventItem::new();

        event1.event_date = 443;
        event1.event_type = 4;
        event1.int_values = Some(vec![1, 2, 3]);

        event1
    }

    // Helper function to create a basic EventStorageCache for testing
    fn setup_cache(max_aggregates: usize) -> (ShareLinksCache, EventStorageCache, TempDir) {
        let share_links_cache = ShareLinksCache::new(max_aggregates);
        let event_storage_cache = EventStorageCache::new(30, 1000000, 10000);
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        (share_links_cache, event_storage_cache, temp_dir)
    }

    // Helper function to create a file path within the temp directory
    fn create_file_path(temp_dir: &TempDir, file_name: &str) -> String {
        let events_bin = temp_dir.path().join(file_name);
        events_bin.to_str().unwrap().to_string()
    }

    // Helper function to create a mock AddShareLink EventItem
    fn create_add_share_link_event(share_id: u128, access_level: AccessLevel, is_single_use: bool, expires_on: u64, description: Option<String>) -> EventItem {
        let current_time = chrono::Utc::now().timestamp_millis() as u64;

        let mut event_item = EventItem::new();
        event_item.event_date = current_time;
        event_item.event_type = AggregateEventType::ShareLinkCreated as u64;
        event_item.string_values = Some(vec![description]);
        event_item.uint_values = Some(vec![access_level as u64, expires_on]);
        event_item.bool_values = Some(vec![is_single_use]);
        event_item.byte_arrays = Some(vec![Some(share_id.to_le_bytes().to_vec())]);
        event_item
    }

    // Helper function to create a mock DisableShareLink EventItem
    fn create_disable_share_link_event(share_id: u128) -> EventItem {
        let current_time = chrono::Utc::now().timestamp_millis() as u64;

        let mut event_item = EventItem::new();
        event_item.event_date = current_time;
        event_item.event_type = AggregateEventType::ShareLinkDisabled as u64;
        event_item.byte_arrays = Some(vec![Some(share_id.to_le_bytes().to_vec())]);
        event_item
    }

    // Helper function to create a mock EventBatchItem
    fn create_event_batch_item_with_events(events: Vec<EventItem>, client_id: u128, user_id: &str) -> EventBatchItem {
        let current_time = chrono::Utc::now().timestamp_millis() as u64;

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.events = events;
        event_batch_item.client_id = client_id;
        event_batch_item.user_id = Some(user_id.to_string());
        event_batch_item.server_date = current_time;
        event_batch_item
    }

    #[test]
    fn test_new() {
        let cache = ShareLinksCache::new(5);
        assert_eq!(cache.cache_max_aggregate_count, 5);
        assert_eq!(cache.cache.len(), 0);
        assert_eq!(cache.cache_queue.len(), 0);

        let cache = ShareLinksCache::new(0);
        assert_eq!(cache.cache_max_aggregate_count, 0);
    }

    #[test]
    fn test_clear_cache_does_nothing_when_under_max_capacity() {
        let (mut share_links_cache, _, _) = setup_cache(3);
        share_links_cache.cache.insert("project1".to_string(), AggregateToShareLinks::new());
        share_links_cache.cache.insert("project2".to_string(), AggregateToShareLinks::new());
        share_links_cache.cache_queue.push_back("project1".to_string());
        share_links_cache.cache_queue.push_back("project2".to_string());

        share_links_cache.clear_cache();

        assert_eq!(share_links_cache.cache.len(), 2);
        assert_eq!(share_links_cache.cache_queue.len(), 2);
    }

    #[test]
    fn test_clear_cache_removes_oldest_projects_when_at_max_capacity() {
        let (mut share_links_cache, _, _) = setup_cache(2);
        share_links_cache.cache.insert("project1".to_string(), AggregateToShareLinks::new());
        share_links_cache.cache.insert("project2".to_string(), AggregateToShareLinks::new());
        share_links_cache.cache.insert("project3".to_string(), AggregateToShareLinks::new());

        //project1 is the oldest
        share_links_cache.cache_queue.push_back("project1".to_string());
        share_links_cache.cache_queue.push_back("project2".to_string());
        share_links_cache.cache_queue.push_back("project3".to_string());
        share_links_cache.clear_cache();

        assert_eq!(share_links_cache.cache.len(), 2);
        assert!(share_links_cache.cache.contains_key("project2"));
        assert!(share_links_cache.cache.contains_key("project3"));
    }

    #[test]
    fn test_clear_cache_removes_multiple_projects_when_significantly_over_capacity() {
        let (mut share_links_cache, _, _) = setup_cache(1);
        share_links_cache.cache.insert("project1".to_string(), AggregateToShareLinks::new());
        share_links_cache.cache.insert("project2".to_string(), AggregateToShareLinks::new());
        share_links_cache.cache.insert("project3".to_string(), AggregateToShareLinks::new());

        share_links_cache.cache_queue.push_back("project1".to_string());
        share_links_cache.cache_queue.push_back("project2".to_string());
        share_links_cache.cache_queue.push_back("project3".to_string());
        share_links_cache.clear_cache();

        assert_eq!(share_links_cache.cache.len(), 1);
        assert!(share_links_cache.cache.contains_key("project3"));
        assert_eq!(share_links_cache.cache_queue.len(), 1);
        assert_eq!(share_links_cache.cache_queue[0], "project3".to_string());
    }

    #[test]
    fn test_clear_cache_handles_empty_queue_gracefully() {
        let (mut share_links_cache, _, _) = setup_cache(1);
        share_links_cache.cache.insert("project1".to_string(), AggregateToShareLinks::new());
        share_links_cache.cache.insert("project2".to_string(), AggregateToShareLinks::new());
        share_links_cache.cache_queue.clear(); // Empty the queue

        share_links_cache.clear_cache();

        assert_eq!(share_links_cache.cache.len(), 2); // Should still contain the projects as queue is empty
    }

    #[test]
    fn test_cache_eviction_order_follows_fifo() {
        let (mut share_links_cache, _, _) = setup_cache(2);
        share_links_cache.cache_queue.push_back("project1".to_string());
        share_links_cache.cache_queue.push_back("project2".to_string());
        share_links_cache.cache.insert("project1".to_string(), AggregateToShareLinks::new());
        share_links_cache.cache.insert("project2".to_string(), AggregateToShareLinks::new());

        // Add a third project, which should evict "project1" (oldest)
        share_links_cache.cache.insert("project3".to_string(), AggregateToShareLinks::new());
        share_links_cache.cache_queue.push_back("project3".to_string());
        share_links_cache.clear_cache(); // Force eviction

        assert_eq!(share_links_cache.cache.len(), 2);
        assert!(!share_links_cache.cache.contains_key("project1"));
        assert!(share_links_cache.cache.contains_key("project2"));
        assert!(share_links_cache.cache.contains_key("project3"));
        assert_eq!(share_links_cache.cache_queue.len(), 2);
        assert_eq!(share_links_cache.cache_queue[0], "project2".to_string());
        assert_eq!(share_links_cache.cache_queue[1], "project3".to_string());
    }

    #[test]
    fn test_populate_cache_for_project_with_valid_add_share_link_events() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create some AddShareLink events
        let event1 = create_add_share_link_event(111, AccessLevel::Viewer, false, 0, Some("desc1".to_string()));
        let event2 = create_add_share_link_event(222, AccessLevel::Contributor, true, 1678886400000, Some("desc2".to_string()));

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], 123, "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], 123, "admin");

        event_storage_cache.write(&file_path, true, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, true, event_batch_2).unwrap();

        // Populate cache for the project
        share_links_cache.cache.insert(file_path.clone(), AggregateToShareLinks::new());
        share_links_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify cache content
        let project_cache = share_links_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 2);
        let share_link1 = project_cache.get_share_link(&111).unwrap();
        assert_eq!(share_link1.access_level, AccessLevel::Viewer);
        assert_eq!(share_link1.is_single_use, false);
        let share_link2 = project_cache.get_share_link(&222).unwrap();
        assert_eq!(share_link2.access_level, AccessLevel::Contributor);
        assert_eq!(share_link2.is_single_use, true);
        assert_eq!(share_link2.expires_on, 1678886400000);
    }

    #[test]
    fn test_populate_cache_for_project_with_valid_disable_share_link_events() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an AddShareLink event and a DisableShareLink event
        let event1 = create_add_share_link_event(111, AccessLevel::Viewer, false, 0, Some("desc1".to_string()));
        let event2 = create_disable_share_link_event(111);

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], 123, "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], 123, "admin");

        event_storage_cache.write(&file_path, true, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, true, event_batch_2).unwrap();

        // Populate cache for the project
        share_links_cache.cache.insert(file_path.clone(), AggregateToShareLinks::new());
        share_links_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify cache content
        let project_cache = share_links_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
        assert!(project_cache.get_share_link(&111).is_none());
    }

    #[test]
    fn test_populate_cache_for_project_with_mixed_event_types() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create AddShareLink and other event types
        let event1 = create_add_share_link_event(111, AccessLevel::Viewer, false, 0, Some("desc1".to_string()));
        let mut event2 = create_add_share_link_event(222, AccessLevel::Contributor, true, 0, Some("desc2".to_string()));
        event2.event_type = AggregateEventType::UserAccessUpdated as u64; // Change type to ProvideAccess
        let event3 = create_disable_share_link_event(111);

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], 123, "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], 123, "admin");
        let event_batch_3 = create_event_batch_item_with_events(vec![event3], 123, "admin");

        event_storage_cache.write(&file_path, true, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, true, event_batch_2).unwrap();
        event_storage_cache.write(&file_path, true, true, event_batch_3).unwrap();

        // Populate cache for the project
        share_links_cache.cache.insert(file_path.clone(), AggregateToShareLinks::new());
        share_links_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify cache content (only AddShareLink and DisableShareLink events should be processed)
        let project_cache = share_links_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
        assert!(project_cache.get_share_link(&111).is_none());
        assert!(project_cache.get_share_link(&222).is_none()); // Not processed
    }

    #[test]
    fn test_populate_cache_for_project_with_malformed_add_share_link_events_missing_fields() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create malformed events (missing fields)
        let mut event1 = create_add_share_link_event(111, AccessLevel::Viewer, false, 0, Some("desc1".to_string()));
        event1.string_values = None; // Missing string_values
        let mut event2 = create_add_share_link_event(222, AccessLevel::Contributor, true, 0, Some("desc2".to_string()));
        event2.uint_values = None; // Missing uint_values
        let mut event3 = create_add_share_link_event(333, AccessLevel::Owner, false, 0, Some("desc3".to_string()));
        event3.bool_values = None; // Missing bool_values

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], 123, "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], 123, "admin");
        let event_batch_3 = create_event_batch_item_with_events(vec![event3], 123, "admin");
        event_storage_cache.write(&file_path, true, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, true, event_batch_2).unwrap();
        event_storage_cache.write(&file_path, true, true, event_batch_3).unwrap();

        // Populate cache for the project
        share_links_cache.cache.insert(file_path.clone(), AggregateToShareLinks::new());
        share_links_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify cache content (malformed events should be ignored)
        let project_cache = share_links_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_when_file_doesnt_exist() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "nonexistent_project.bin");

        // Attempt to populate cache for a non-existent file
        share_links_cache.cache.insert(file_path.clone(), AggregateToShareLinks::new());
        share_links_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify that the cache remains empty
        let project_cache = share_links_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_with_empty_event_batches() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an empty event batch
        let event_batch = EventBatchItem {
            server_id: 0,
            client_id: 123,
            user_id: Some("admin".to_string()),
            server_date: chrono::Utc::now().timestamp_millis() as u64,
            events: vec![],
        };
        let result = event_storage_cache.write(&file_path, true, true, event_batch);
        assert!(result.is_err());

        // Populate the cache
        share_links_cache.cache.insert(file_path.clone(), AggregateToShareLinks::new());
        share_links_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify that the cache remains empty
        let project_cache = share_links_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_get_or_build_cache_returns_existing_cache_when_project_already_cached() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Insert a project into the cache
        share_links_cache.cache.insert(file_path.clone(), AggregateToShareLinks::new());

        // Get the cache for the project
        share_links_cache.get_or_build_cache(&mut event_storage_cache, &file_path);

        // Verify that the cache is the same one that was inserted
        assert!(share_links_cache.cache.contains_key(&file_path));
    }

    #[test]
    fn test_get_or_build_cache_creates_new_cache_when_project_not_cached() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Get the cache for the project
        share_links_cache.get_or_build_cache(&mut event_storage_cache, &file_path);

        // Verify that a new cache was created
        assert!(share_links_cache.cache.contains_key(&file_path));
    }

    #[test]
    fn test_get_or_build_cache_adds_project_to_cache_queue_when_creating_new_cache() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Get the cache for the project
        share_links_cache.get_or_build_cache(&mut event_storage_cache, &file_path);

        // Verify that the project was added to the cache queue
        assert!(share_links_cache.cache_queue.contains(&file_path));
    }

    #[test]
    fn test_get_or_build_cache_triggers_cache_clearing_when_at_capacity() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(1);
        let file_path1 = create_file_path(&temp_dir, "project1.bin");
        let file_path2 = create_file_path(&temp_dir, "project2.bin");

        // Fill the cache to capacity
        share_links_cache.cache.insert(file_path1.clone(), AggregateToShareLinks::new());
        share_links_cache.cache_queue.push_back(file_path1.clone());

        // Add a new project, which should trigger cache clearing
        share_links_cache.get_or_build_cache(&mut event_storage_cache, &file_path2);

        // Verify that the cache was cleared (at least one project was evicted)
        assert!(share_links_cache.cache.contains_key(&file_path2));
        assert!(!share_links_cache.cache.contains_key(&file_path1));
    }

    #[test]
    fn test_create_share_link_adds_event_and_updates_cache() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let current_user_hash = "user1";
        let share_id = 12345;
        let access_level = AccessLevel::Viewer;
        let is_single_use = true;
        let iv = Some([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let description = Some("test share link".to_string());
        let expires_on = 1678886400000;

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            server_id: 0,
            client_id: 123,
            user_id: None,
            server_date: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, true, first_batch).unwrap();

        let none_check = share_links_cache.get_share_key_data_if_still_valid(&mut event_storage_cache, &file_path, &share_id);
        assert!(none_check.is_none());

        // Create share link
        let result = share_links_cache.create_share_link(
            &mut event_storage_cache,
            &file_path,
            &123,
            Some(current_user_hash),
            share_id,
            access_level,
            is_single_use,
            iv,
            description,
            expires_on,
            654,
        );

        // Verify that the event was created successfully
        assert!(result.is_ok());

        // Verify that the cache was updated
        let project_cache = share_links_cache.get_or_build_cache(&mut event_storage_cache, &file_path);
        assert_eq!(project_cache.count(), 1);
        let share_link = project_cache.get_share_link(&share_id).unwrap();
        assert_eq!(share_link.access_level, access_level);
        assert_eq!(share_link.is_single_use, is_single_use);
        assert_eq!(share_link.expires_on, expires_on);
        assert_eq!(share_link.access_level, access_level);
        assert_eq!(share_link.is_single_use, is_single_use);
        assert_eq!(share_link.expires_on, expires_on);
    }

    #[test]
    fn test_get_share_key_data_if_still_valid_returns_share_link_if_valid() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let share_id = 12345;

        // Create a share link and add it to the cache
        let mut project_cache = AggregateToShareLinks::new();
        let share_link_info = ShareLinkAccessInfo::new(AccessLevel::Viewer, share_id, false, 0);
        project_cache.add_share_link(share_id, share_link_info);
        share_links_cache.cache.insert(file_path.clone(), project_cache);

        // Get the share link data
        let result = share_links_cache.get_share_key_data_if_still_valid(&mut event_storage_cache, &file_path, &share_id);

        // Verify that the share link data was returned
        assert!(result.is_some());
        assert_eq!(result.unwrap().access_level, AccessLevel::Viewer);
    }

    #[test]
    fn test_get_share_key_data_if_still_valid_returns_none_if_share_link_does_not_exist() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let share_id = 12345;

        // Get the share link data
        let result = share_links_cache.get_share_key_data_if_still_valid(&mut event_storage_cache, &file_path, &share_id);

        // Verify that None was returned
        assert!(result.is_none());
    }

    #[test]
    fn test_get_share_key_data_if_still_valid_returns_none_if_share_link_is_expired() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let share_id = 12345;

        // Create an expired share link and add it to the cache
        let mut project_cache = AggregateToShareLinks::new();
        let share_link_info = ShareLinkAccessInfo::new(
            AccessLevel::Viewer,
            share_id,
            false,
            1, // Expired timestamp
        );
        project_cache.add_share_link(share_id, share_link_info);
        share_links_cache.cache.insert(file_path.clone(), project_cache);

        // Get the share link data
        let result = share_links_cache.get_share_key_data_if_still_valid(&mut event_storage_cache, &file_path, &share_id);

        // Verify that None was returned
        assert!(result.is_none());

        // Verify that the share link was removed from the cache
        let project_cache = share_links_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_disable_share_link_adds_event_and_removes_from_cache() {
        let (mut share_links_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let current_user_hash = "user1";
        let share_id = 12345;

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            server_id: 0,
            client_id: 1,
            user_id: None,
            server_date: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, true, first_batch).unwrap();

        // Create a share link and add it to the cache
        let mut project_cache = AggregateToShareLinks::new();
        let share_link_info = ShareLinkAccessInfo::new(AccessLevel::Viewer, share_id, false, 0);
        project_cache.add_share_link(share_id, share_link_info);
        share_links_cache.cache.insert(file_path.clone(), project_cache);

        // Disable the share link
        let result = share_links_cache.disable_share_link(&mut event_storage_cache, &file_path, &1, Some(current_user_hash), share_id, 999);

        // Verify that the event was created successfully
        assert!(result.is_ok());

        // Verify that the share link was removed from the cache
        let project_cache = share_links_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
        assert!(project_cache.get_share_link(&share_id).is_none());
    }
}
