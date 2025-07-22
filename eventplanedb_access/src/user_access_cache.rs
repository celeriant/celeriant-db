use crate::{access_level::AccessLevel, aggregate_event_type::AggregateEventType, aggregate_to_user_access_level::AggregateToUserAccessLevel};
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem, event_storage_cache::EventStorageCache};
use std::{
    collections::{HashMap, VecDeque},
    io, usize,
};

pub struct UserAccessCache {
    // The queue is used to evict the oldest files from the cache when the cache is full
    cache_queue: VecDeque<String>,

    // The cache maps a file to another hashmap of user_hash to access level
    cache: HashMap<String, AggregateToUserAccessLevel>,

    // The maximum number of projects to cache, currently we can have unlimited users inside a project (str+u64)
    cache_max_aggregate_count: usize,
}

impl UserAccessCache {

    pub fn new( cache_max_aggregate_count: usize) -> Self {
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
    fn get_or_build_cache(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str) -> &mut AggregateToUserAccessLevel {
        self.clear_cache();

        if self.cache.contains_key(file_path) {
            return self.cache.get_mut(file_path).unwrap();
        }

        self.cache.insert(file_path.to_string(), AggregateToUserAccessLevel::new());
        self.cache_queue.push_back(file_path.to_string());

        self.populate_cache_for_aggregate(event_storage_cache, file_path);

        self.clear_cache();

        return self.cache.get_mut(file_path).unwrap();
    }

    /// Read all the ProvideAccess events for a project and build the cache for that project from the events found
    fn populate_cache_for_aggregate(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str) {
        let aggregate_to_user_access_level = self.cache.get_mut(file_path).unwrap();

        match event_storage_cache.read(file_path, 0, usize::MAX, Some(&[AggregateEventType::UserAccessUpdated as u64]), None) {
            Ok(result) => {
                for batch in result.event_batches {
                    for event in batch.events.iter() {
                        // Check the event is a ProvideAccess event and has the correct data
                        if event.event_type != AggregateEventType::UserAccessUpdated as u64
                            || event.string_values.as_ref().is_none()
                            || event.string_values.as_ref().unwrap().len() == 0
                            || event.uint_values.is_none()
                            || event.uint_values.as_ref().unwrap().len() == 0
                            || event.byte_arrays.as_ref().is_none()
                            || event.byte_arrays.as_ref().unwrap().len() == 0
                            || event.byte_arrays.as_ref().unwrap()[0].is_none() 
                        {
                            continue;
                        }

                        //Interpret the event to pull out relevant data
                        let user_id = event.string_values.as_ref().unwrap()[0].as_ref().map(|s| s.as_str());
                        let client_id_bytes: &Vec<u8> = event.byte_arrays.as_ref().unwrap()[0].as_ref().unwrap();
                        let client_id: u128 = u128::from_le_bytes(client_id_bytes.as_slice().try_into().unwrap());
                        let access_level = AccessLevel::from(event.uint_values.as_ref().unwrap()[0]);

                        // As we process events in chronological order, allow the users' access
                        // to upgrade OR downgrade depending on the event's access level
                        aggregate_to_user_access_level.update_cache_for_user(&client_id, user_id, access_level, true);
                    }
                }
            }

            // Fail to read, skip populating the cache for this project. Could be a new project or file deleted.
            Err(_) => {}
        }
    }

    /// Get the current access level for a user. Will build a cache if one does not exist.
    pub fn get_current_access_level(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str, client_id: &u128, user_id: Option<&str>) -> AccessLevel {
        let aggregate_to_user_access_level = self.get_or_build_cache(event_storage_cache, file_path);
        aggregate_to_user_access_level.get_access_level(client_id, user_id)
    }

    /// Change the access level for for_user_hash. Adds an event to the file and updates the cache.
    pub fn update_access_for_user(
        &mut self,
        event_storage_cache: &mut EventStorageCache,
        file_path: &str,
        current_client_id: &u128,
        current_user_id: Option<&str>,
        for_client_id: &u128,
        for_user_id: Option<&str>,
        potential_access_level: AccessLevel,
        allow_downgrade: bool,
        share_id: Option<u128>,
        server_time: u64,
    ) -> io::Result<Option<EventBatchItem>> {

        //TODO: This is really a programmer guard, not business logic
        //Not allowed to downgrade your own permissions - client id check.
        if allow_downgrade && *current_client_id == *for_client_id {
            return Ok(None);
        }

        //Not allowed to downgrade your own permissions - user id check
        if let Some(for_user_id) = for_user_id {
            if allow_downgrade && (current_user_id.is_some_and(|x| x == for_user_id)) {
                return Ok(None);
            }
        }

        let current_access_level = self.get_current_access_level(event_storage_cache, file_path, for_client_id, for_user_id);

        //No op as same permission level or lower level and not downgrading
        if current_access_level == potential_access_level
            || !allow_downgrade && !AccessLevel::increases_access_level(current_access_level, potential_access_level)
        {
            return Ok(None);
        }

        let mut event_item = EventItem::new();
        event_item.event_date = server_time;
        event_item.event_type = AggregateEventType::UserAccessUpdated as u64;
        event_item.string_values = Some(vec![for_user_id.as_ref().map(|f| f.to_string())]);
        event_item.uint_values = Some(vec![potential_access_level as u64]);
        event_item.byte_arrays = Some(vec![Some(for_client_id.to_le_bytes().to_vec()), share_id.map(|sk| sk.to_le_bytes().to_vec())]);

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.events = vec![event_item];
        event_batch_item.client_id = *current_client_id;
        event_batch_item.user_id = current_user_id.map(|f| f.to_string());
        event_batch_item.server_date = server_time;

        event_batch_item.server_id = event_storage_cache.write(file_path, false, event_batch_item.clone())?;

        let aggregate_to_user_access_level = self.get_or_build_cache(event_storage_cache, file_path);
        aggregate_to_user_access_level.update_cache_for_user(for_client_id, for_user_id, potential_access_level, allow_downgrade);

        Ok(Some(event_batch_item))
    }
}

#[cfg(test)]
mod tests {
    use crate::{access_level::AccessLevel, aggregate_event_type::AggregateEventType, aggregate_to_user_access_level::AggregateToUserAccessLevel};
    use eventplanedb_storage::event_item::EventItem;
    use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};
    use std::vec;
    use tempfile::TempDir;

    use super::*;

    // Helper function to create a basic EventStorageCache for testing
    fn setup_cache(max_projects: usize) -> (UserAccessCache, EventStorageCache, TempDir) {
        let user_access_cache = UserAccessCache::new(max_projects);
        let event_storage_cache = EventStorageCache::new(30, 1000000, 10000);
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        (user_access_cache, event_storage_cache, temp_dir)
    }

    // Helper function to create a file path within the temp directory
    fn create_file_path(temp_dir: &TempDir, file_name: &str) -> String {
        let events_bin = temp_dir.path().join(file_name);
        events_bin.to_str().unwrap().to_string()
    }

    // Helper function to create a mock ProvideAccess EventItem
    fn create_provide_access_event(client_id: &u128, user_hash: &str, access_level: AccessLevel, ed_override: Option<u64>) -> EventItem {
        let current_time = ed_override.unwrap_or(chrono::Utc::now().timestamp_millis() as u64);

        let mut event_item = EventItem::new();
        event_item.event_date = current_time;
        event_item.event_type = AggregateEventType::UserAccessUpdated as u64;
        event_item.string_values = Some(vec![Some(user_hash.to_string()), None]);
        event_item.uint_values = Some(vec![access_level as u64]);
        event_item.byte_arrays = Some(vec![Some(client_id.to_le_bytes().to_vec())]);
        event_item
    }

    // Helper function to create a mock ProvideAccess EventItem
    fn create_provide_access_event_only_client(client_id: &u128, access_level: AccessLevel, ed_override: Option<u64>) -> EventItem {
        let current_time = ed_override.unwrap_or(chrono::Utc::now().timestamp_millis() as u64);

        let mut event_item = EventItem::new();
        event_item.event_date = current_time;
        event_item.event_type = AggregateEventType::UserAccessUpdated as u64;
        event_item.string_values = Some(vec![None, None]);
        event_item.uint_values = Some(vec![access_level as u64]);
        event_item.byte_arrays = Some(vec![Some(client_id.to_le_bytes().to_vec())]);
        event_item
    }

    // Helper function to create a mock EventBatchItem
    fn create_event_batch_item_with_events(events: Vec<EventItem>, current_user_hash: &str) -> EventBatchItem {
        let current_time = chrono::Utc::now().timestamp_millis() as u64;

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.events = events;
        event_batch_item.client_id = 0;
        event_batch_item.user_id = Some(current_user_hash.to_string());
        event_batch_item.server_id = current_time;
        event_batch_item
    }

    #[test]
    fn test_new() {
        let cache = UserAccessCache::new(5);
        assert_eq!(cache.cache_max_aggregate_count, 5);
        assert_eq!(cache.cache.len(), 0);
        assert_eq!(cache.cache_queue.len(), 0);

        let cache = UserAccessCache::new(0);
        assert_eq!(cache.cache_max_aggregate_count, 0);
    }

    #[test]
    fn test_clear_cache_does_nothing_when_under_max_capacity() {
        let (mut user_access_cache, _, _) = setup_cache(3);
        user_access_cache.cache.insert("project1".to_string(), AggregateToUserAccessLevel::new());
        user_access_cache.cache.insert("project2".to_string(), AggregateToUserAccessLevel::new());
        user_access_cache.cache_queue.push_back("project1".to_string());
        user_access_cache.cache_queue.push_back("project2".to_string());

        user_access_cache.clear_cache();

        assert_eq!(user_access_cache.cache.len(), 2);
        assert_eq!(user_access_cache.cache_queue.len(), 2);
    }

    #[test]
    fn test_clear_cache_removes_oldest_projects_when_at_max_capacity() {
        let (mut user_access_cache, _, _) = setup_cache(2);
        user_access_cache.cache.insert("project1".to_string(), AggregateToUserAccessLevel::new());
        user_access_cache.cache.insert("project2".to_string(), AggregateToUserAccessLevel::new());
        user_access_cache.cache.insert("project3".to_string(), AggregateToUserAccessLevel::new());

        //project1 is the oldest
        user_access_cache.cache_queue.push_back("project1".to_string());
        user_access_cache.cache_queue.push_back("project2".to_string());
        user_access_cache.cache_queue.push_back("project3".to_string());
        user_access_cache.clear_cache();

        assert_eq!(user_access_cache.cache.len(), 2);
        assert!(user_access_cache.cache.contains_key("project2"));
        assert!(user_access_cache.cache.contains_key("project3"));
    }

    #[test]
    fn test_clear_cache_removes_multiple_projects_when_significantly_over_capacity() {
        let (mut user_access_cache, _, _) = setup_cache(1);
        user_access_cache.cache.insert("project1".to_string(), AggregateToUserAccessLevel::new());
        user_access_cache.cache.insert("project2".to_string(), AggregateToUserAccessLevel::new());
        user_access_cache.cache.insert("project3".to_string(), AggregateToUserAccessLevel::new());

        user_access_cache.cache_queue.push_back("project1".to_string());
        user_access_cache.cache_queue.push_back("project2".to_string());
        user_access_cache.cache_queue.push_back("project3".to_string());
        user_access_cache.clear_cache();

        assert_eq!(user_access_cache.cache.len(), 1);
        assert!(user_access_cache.cache.contains_key("project3"));
        assert_eq!(user_access_cache.cache_queue.len(), 1);
        assert_eq!(user_access_cache.cache_queue[0], "project3".to_string());
    }

    #[test]
    fn test_clear_cache_handles_empty_queue_gracefully() {
        let (mut user_access_cache, _, _) = setup_cache(1);
        user_access_cache.cache.insert("project1".to_string(), AggregateToUserAccessLevel::new());
        user_access_cache.cache.insert("project2".to_string(), AggregateToUserAccessLevel::new());
        user_access_cache.cache_queue.clear(); // Empty the queue

        user_access_cache.clear_cache();

        assert_eq!(user_access_cache.cache.len(), 2); // Should still contain the projects as queue is empty
    }

    #[test]
    fn test_cache_eviction_order_follows_fifo() {
        let (mut user_access_cache, _, _) = setup_cache(2);
        user_access_cache.cache_queue.push_back("project1".to_string());
        user_access_cache.cache_queue.push_back("project2".to_string());
        user_access_cache.cache.insert("project1".to_string(), AggregateToUserAccessLevel::new());
        user_access_cache.cache.insert("project2".to_string(), AggregateToUserAccessLevel::new());

        // Add a third project, which should evict "project1" (oldest)
        user_access_cache.cache.insert("project3".to_string(), AggregateToUserAccessLevel::new());
        user_access_cache.cache_queue.push_back("project3".to_string());
        user_access_cache.clear_cache(); // Force eviction

        assert_eq!(user_access_cache.cache.len(), 2);
        assert!(!user_access_cache.cache.contains_key("project1"));
        assert!(user_access_cache.cache.contains_key("project2"));
        assert!(user_access_cache.cache.contains_key("project3"));
        assert_eq!(user_access_cache.cache_queue.len(), 2);
        assert_eq!(user_access_cache.cache_queue[0], "project2".to_string());
        assert_eq!(user_access_cache.cache_queue[1], "project3".to_string());
    }

    #[test]
    fn test_populate_cache_for_project_with_valid_provide_access_events() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create some ProvideAccess events
        let event1 = create_provide_access_event(&0, "user1", AccessLevel::Owner, None);
        let event2 = create_provide_access_event(&0, "user2", AccessLevel::Contributor, None);
        let event3 = create_provide_access_event(&0, "user1", AccessLevel::Viewer, None); // Update user1

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], "admin");
        let event_batch_3 = create_event_batch_item_with_events(vec![event3], "admin");

        event_storage_cache.write(&file_path, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_2).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_3).unwrap();

        // Populate cache for the project
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify cache content
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.get_access_level_for_user("user1"), AccessLevel::Viewer);
        assert_eq!(project_cache.get_access_level_for_user("user2"), AccessLevel::Contributor);
        assert_eq!(project_cache.count(), 2);
    }

    #[test]
    fn test_populate_cache_for_project_with_mixed_event_types() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create ProvideAccess and other event types
        let event1 = create_provide_access_event(&0, "user1", AccessLevel::Owner, None);
        let mut event2 = create_provide_access_event(&0, "user2", AccessLevel::Contributor, None);
        event2.event_type = AggregateEventType::ShareLinkCreated as u64; // Change type to AddShareLink
        let event3 = create_provide_access_event(&0, "user1", AccessLevel::Viewer, None);

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], "admin");
        let event_batch_3 = create_event_batch_item_with_events(vec![event3], "admin");

        event_storage_cache.write(&file_path, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_2).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_3).unwrap();

        // Populate cache for the project
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify cache content (only ProvideAccess events should be processed)
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.get_access_level_for_user("user1"), AccessLevel::Viewer);
        assert_eq!(project_cache.get_access_level_for_user("user2"), AccessLevel::None); // Not processed
        assert_eq!(project_cache.count(), 1);
    }

    #[test]
    fn test_populate_cache_for_project_with_malformed_events_missing_fields() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create malformed events (missing fields)
        let mut event1 = create_provide_access_event(&0, "user1", AccessLevel::Owner, None);
        event1.string_values = None; // Missing string_values
        let mut event2 = create_provide_access_event(&0, "user2", AccessLevel::Contributor, None);
        event2.uint_values = None; // Missing uint_values

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], "admin");
        event_storage_cache.write(&file_path, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_2).unwrap();

        // Populate cache for the project
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify cache content (malformed events should be ignored)
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_with_events_missing_string_values() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an event with missing string_values
        let mut event = create_provide_access_event(&0, "user1", AccessLevel::Owner, None);
        event.string_values = None;

        let event_batch = create_event_batch_item_with_events(vec![event], "admin");
        event_storage_cache.write(&file_path, true, event_batch).unwrap();

        // Populate the cache
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify that the malformed event was ignored
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_with_events_missing_uint_values() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an event with missing uint_values
        let mut event = create_provide_access_event(&0, "user1", AccessLevel::Owner, None);
        event.uint_values = None;

        let event_batch = create_event_batch_item_with_events(vec![event], "admin");
        event_storage_cache.write(&file_path, true, event_batch).unwrap();

        // Populate the cache
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify that the malformed event was ignored
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_with_events_having_insufficient_array_lengths() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an event with string_values having insufficient length
        let mut event1 = create_provide_access_event(&0, "user1", AccessLevel::Owner, None);
        event1.string_values = Some(vec![]); // Shortened vector

        // Create an event with uint_values having insufficient length
        let mut event2 = create_provide_access_event(&0, "user2", AccessLevel::Contributor, None);
        event2.uint_values = Some(vec![]); // Empty vector

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], "admin");
        event_storage_cache.write(&file_path, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_2).unwrap();

        // Populate the cache
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify that the malformed events were ignored
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_only_client_id_still_valid_permission() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an event with None in the user_hash position (first string value)
        let mut event = create_provide_access_event_only_client(&123, AccessLevel::Owner, None);
        event.string_values = Some(vec![None, Some("share_key".to_string())]);

        let event_batch = create_event_batch_item_with_events(vec![event], "admin");
        event_storage_cache.write(&file_path, true, event_batch).unwrap();

        // Populate the cache
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify that the malformed event was ignored
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 1);
        assert_eq!(project_cache.get_access_level(&123, Some("user1")), AccessLevel::None);
        assert_eq!(project_cache.get_access_level(&123, None), AccessLevel::Owner);
    }

    #[test]
    fn test_populate_cache_for_project_with_events_having_none_values_in_required_positions() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an event with None in the user_hash position (first string value)
        let mut event = create_provide_access_event_only_client(&0, AccessLevel::Owner, None);
        event.byte_arrays = Some(vec![None]); // Ensure byte_arrays is malformed for the test

        let event_batch = create_event_batch_item_with_events(vec![event], "admin");
        event_storage_cache.write(&file_path, true, event_batch).unwrap();

        // Populate the cache
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify that the malformed event was ignored
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_when_file_doesnt_exist() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "nonexistent_project.bin");

        // Attempt to populate cache for a non-existent file
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify that the cache remains empty
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_with_empty_event_batches() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an empty event batch
        let event_batch = EventBatchItem {
            server_id: 0,
            client_id: 0,
            user_id: Some("admin".to_string()),
            server_date: chrono::Utc::now().timestamp_millis() as u64,
            events: vec![],
        };
        event_storage_cache.write(&file_path, true, event_batch).unwrap();

        // Populate the cache
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify that the cache remains empty
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_processes_events_in_chronological_order() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create ProvideAccess events with different timestamps
        let event1 = create_provide_access_event(&0, "user1", AccessLevel::Contributor, Some(2));
        let event2 = create_provide_access_event(&0, "user1", AccessLevel::Owner, Some(1)); // Earlier timestamp
        let event3 = create_provide_access_event(&0, "user1", AccessLevel::Viewer, Some(3)); // Later timestamp

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], "admin");
        let event_batch_3 = create_event_batch_item_with_events(vec![event3], "admin");

        event_storage_cache.write(&file_path, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_2).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_3).unwrap();

        // Populate cache for the project
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        // Verify that the latest event (highest timestamp) determines the access level
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.get_access_level_for_user("user1"), AccessLevel::Viewer);
    }

    #[test]
    fn test_get_or_build_cache_returns_existing_cache_when_project_already_cached() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Insert a project into the cache
        // user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());

        // Get the cache for the project
        {
            let _ = user_access_cache.get_or_build_cache(&mut event_storage_cache, &file_path);
        }

        // Verify that the cache is the same one that was inserted
        assert!(user_access_cache.cache.contains_key(&file_path));
    }

    #[test]
    fn test_get_or_build_cache_creates_new_cache_when_project_not_cached() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Get the cache for the project
        user_access_cache.get_or_build_cache(&mut event_storage_cache, &file_path);

        // Verify that a new cache was created
        assert!(user_access_cache.cache.contains_key(&file_path));
    }

    #[test]
    fn test_get_or_build_cache_adds_project_to_cache_queue_when_creating_new_cache() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Get the cache for the project
        user_access_cache.get_or_build_cache(&mut event_storage_cache, &file_path);

        // Verify that the project was added to the cache queue
        assert!(user_access_cache.cache_queue.contains(&file_path));
    }

    #[test]
    fn test_get_or_build_cache_triggers_cache_clearing_when_at_capacity() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(1);
        let file_path1 = create_file_path(&temp_dir, "project1.bin");
        let file_path2 = create_file_path(&temp_dir, "project2.bin");

        // Fill the cache to capacity
        user_access_cache.cache.insert(file_path1.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.cache_queue.push_back(file_path1.clone());

        // Add a new project, which should trigger cache clearing
        user_access_cache.get_or_build_cache(&mut event_storage_cache, &file_path2);

        // Verify that the cache was cleared (at least one project was evicted)
        assert!(user_access_cache.cache.contains_key(&file_path2));
        assert!(!user_access_cache.cache.contains_key(&file_path1));
    }

    #[test]
    fn test_get_current_access_level_for_existing_user_in_cached_project() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Insert a project into the cache with a user and access level
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&0, Some("user1"), AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Get the access level for the user
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &0, Some("user1"));

        // Verify that the access level is correct
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    #[test]
    fn test_get_current_access_level_for_non_existent_user_should_return_default() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Insert a project into the cache (no users)
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());

        // Get the access level for a non-existent user
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &0, Some("user1"));

        // Verify that the access level is the default (None)
        assert_eq!(access_level, AccessLevel::None);
    }

    #[test]
    fn test_get_current_access_level_for_new_project_builds_cache_first() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Get the access level for a project that is not in the cache
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &0, Some("user1"));

        // Verify that a new cache was created and the access level is the default (None)
        assert!(user_access_cache.cache.contains_key(&file_path));
        assert_eq!(access_level, AccessLevel::None);
    }

    #[test]
    fn test_get_current_access_level_with_various_access_levels() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create a project cache and add a user with different access levels
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&0, Some("owner"), AccessLevel::Owner, true);
        project_cache.update_cache_for_user(&0, Some("contributor"), AccessLevel::Contributor, true);
        project_cache.update_cache_for_user(&0, Some("viewer"), AccessLevel::Viewer, true);
        project_cache.update_cache_for_user(&0, Some("none"), AccessLevel::None, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Verify that the correct access levels are returned
        assert_eq!(
            user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &0, Some("owner")),
            AccessLevel::Owner
        );
        assert_eq!(
            user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &0, Some("contributor")),
            AccessLevel::Contributor
        );
        assert_eq!(
            user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &0, Some("viewer")),
            AccessLevel::Viewer
        );
        assert_eq!(
            user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &0, Some("none")),
            AccessLevel::None
        );
        assert_eq!(
            user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &0, Some("unknown")),
            AccessLevel::None
        ); // Non-existent user
    }

    #[test]
    fn test_update_access_for_user_prevents_self_downgrade_when_allow_downgrade_is_true() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";

        // Initialize user's access level to Contributor
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&0, Some(user_hash), AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Attempt to downgrade own access level
        let result = user_access_cache
            .update_access_for_user(
                &mut event_storage_cache,
                &file_path,
                &334,
                Some(user_hash),
                &334,
                Some(user_hash),
                AccessLevel::Viewer,
                true,
                None,
                654,
            )
            .unwrap();

        // Verify that the update was prevented (returns None)
        assert!(result.is_none());

        // Verify that the access level remains unchanged
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &0, Some(user_hash));
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    #[test]
    fn test_update_access_for_user_disallows_self_upgrade() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";

        // Initialize user's access level to Contributor
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&0, Some(user_hash), AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Attempt to upgrade own access level
        let result = user_access_cache
            .update_access_for_user(
                &mut event_storage_cache, 
                &file_path, 
                &33, 
                Some(user_hash), 
                &33,
                Some(user_hash),
                AccessLevel::Owner, 
                true, 
                None,
            343)
            .unwrap();

        // Verify that the update was successful (returns Some(EventItem))
        assert!(result.is_none());

        // Verify that the access level was updated
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &0, Some(user_hash));
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    #[test]
    fn test_update_access_for_user_returns_none_for_same_access_level_no_op() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";

        // Initialize user's access level to Contributor
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&0, Some(user_hash), AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Attempt to set same access level
        let result = user_access_cache
            .update_access_for_user(
                &mut event_storage_cache,
                &file_path,
                &345,
                Some(user_hash),
                &345,
                Some(user_hash),
                AccessLevel::Contributor,
                true,
                None,
                654
            )
            .unwrap();

        // Verify that the update was a no-op (returns None)
        assert!(result.is_none());

        // Verify that the access level remains unchanged
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &345, Some(user_hash));
        assert_eq!(access_level, AccessLevel::Contributor);
    }









    #[test]
    fn test_client_id_only_access_level_retrieval() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        // Create a project cache with client_id-only access (no user_id)
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&12345, None, AccessLevel::Owner, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Verify access level for client_id without user_id
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &12345, None);
        assert_eq!(access_level, AccessLevel::Owner);

        // Verify that same client_id with any user_id gets None (no OAuth override yet)
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &12345, Some("any_user"));
        assert_eq!(access_level, AccessLevel::None);
    }

    #[test]
    fn test_oauth_override_of_pki_client_id() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        // Start with PKI-only access
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&12345, None, AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Verify initial PKI access
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &12345, None);
        assert_eq!(access_level, AccessLevel::Contributor);

        // Now add OAuth user_id for same client_id (simulating user login)
        let result = user_access_cache.update_access_for_user(
            &mut event_storage_cache,
            &file_path,
            &99999, // Different admin client_id
            Some("admin"),
            &12345, // Same client_id being upgraded
            Some("oauth_user"), // Now has OAuth user_id
            AccessLevel::Owner,
            false, // No downgrade
            None,
            1000
        ).unwrap();

        assert!(result.is_some());

        // Verify that OAuth access now takes precedence
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &12345, Some("oauth_user"));
        assert_eq!(access_level, AccessLevel::Owner);

        // Verify that PKI-only access for same client_id is now None
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &12345, None);
        assert_eq!(access_level, AccessLevel::None);
    }

    #[test]
    fn test_different_oauth_users_same_client_id() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        let client_id = 54321;
        let mut project_cache = AggregateToUserAccessLevel::new();
        
        // Same client_id with different OAuth users
        project_cache.update_cache_for_user(&client_id, Some("user_a"), AccessLevel::Owner, true);
        project_cache.update_cache_for_user(&client_id, Some("user_b"), AccessLevel::Viewer, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Verify each OAuth user has their own access level
        let access_level_a = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &client_id, Some("user_a"));
        assert_eq!(access_level_a, AccessLevel::Owner);

        let access_level_b = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &client_id, Some("user_b"));
        assert_eq!(access_level_b, AccessLevel::Viewer);

        // PKI-only access should be None when OAuth users exist
        let pki_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &client_id, None);
        assert_eq!(pki_access, AccessLevel::None);
    }

    #[test]
    fn test_cross_device_access_with_oauth() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        let device1_client_id = 11111;
        let device2_client_id = 22222;
        let oauth_user = "cross_device_user";

        let mut project_cache = AggregateToUserAccessLevel::new();
        
        // Same OAuth user on different devices (different client_ids)
        project_cache.update_cache_for_user(&device1_client_id, Some(oauth_user), AccessLevel::Owner, true);
        project_cache.update_cache_for_user(&device2_client_id, Some(oauth_user), AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Device 1 can still access as long as has oauth token; however they now only have contributor access
        let device1_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &device1_client_id, Some(oauth_user));
        assert_eq!(device1_access, AccessLevel::Contributor);

        // device 1 without oauth can no longer access
        let device1_no_oauth = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &device1_client_id, None);
        assert_eq!(device1_no_oauth, AccessLevel::None);

        let device2_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &device2_client_id, Some(oauth_user));
        assert_eq!(device2_access, AccessLevel::Contributor);
    }

    #[test]
    fn test_prevent_oauth_user_downgrading_self_across_client_ids() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        let client_id1 = 11111;
        let client_id2 = 22222;
        let oauth_user = "same_oauth_user";

        // Set up initial access levels
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&client_id1, Some(oauth_user), AccessLevel::Owner, true);
        project_cache.update_cache_for_user(&client_id2, Some(oauth_user), AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Attempt to downgrade own access on different client_id (should be prevented)
        let result = user_access_cache.update_access_for_user(
            &mut event_storage_cache,
            &file_path,
            &client_id1, // Current client_id
            Some(oauth_user), // Current OAuth user
            &client_id2, // Target client_id (different)
            Some(oauth_user), // Same OAuth user
            AccessLevel::Viewer, // Downgrade attempt
            true, // Allow downgrade flag
            None,
            1000
        ).unwrap();

        // Should be prevented due to same OAuth user
        assert!(result.is_none());

        // Verify access levels remain unchanged
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &client_id2, Some(oauth_user));
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    fn initial_event() -> EventBatchItem {
        let mut event_item = EventItem::new();
        event_item.event_date = 3232;
        event_item.event_type = 1;

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.events = vec![event_item];
        event_batch_item.client_id = 1;
        event_batch_item.server_date = 3232;

        event_batch_item
    }

    #[test]
    fn test_allow_oauth_user_upgrade_across_client_ids() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        let client_id1 = 11111;
        let client_id2 = 22222;
        let oauth_user = "same_oauth_user";

        // Set up initial access levels
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&client_id1, Some(oauth_user), AccessLevel::Owner, true);
        project_cache.update_cache_for_user(&client_id2, Some(oauth_user), AccessLevel::Viewer, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Attempt to upgrade own access on different client_id (should be allowed)
        let result = user_access_cache.update_access_for_user(
            &mut event_storage_cache,
            &file_path,
            &client_id1, // Current client_id (has Owner)
            Some(oauth_user), // Current OAuth user
            &client_id2, // Target client_id (different)
            Some(oauth_user), // Same OAuth user
            AccessLevel::Contributor, // Upgrade from Viewer
            false, // No downgrade flag
            None,
            1000
        ).unwrap();

        // Should be allowed as it's an upgrade
        assert!(result.is_some());

        // Verify access level was upgraded
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &client_id2, Some(oauth_user));
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    #[test]
    fn test_pki_to_oauth_migration_workflow() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        let client_id = 12345;
        
        // Start with PKI-only access
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&client_id, None, AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Verify PKI access works
        let pki_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &client_id, None);
        assert_eq!(pki_access, AccessLevel::Contributor);

        // User decides to login with OAuth (migration event)
        let result = user_access_cache.update_access_for_user(
            &mut event_storage_cache,
            &file_path,
            &99999, // Admin client_id
            Some("admin"),
            &client_id, // Same client_id, now with OAuth
            Some("new_oauth_user"), // New OAuth identity
            AccessLevel::Contributor, // Same access level
            false,
            None,
            1000
        ).unwrap();

        assert!(result.is_some());

        // Now OAuth access should work
        let oauth_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &client_id, Some("new_oauth_user"));
        assert_eq!(oauth_access, AccessLevel::Contributor);

        // PKI access should no longer work for this client_id
        let pki_access_after = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &client_id, None);
        assert_eq!(pki_access_after, AccessLevel::None);
    }

    #[test]
    fn test_mixed_pki_and_oauth_users_in_same_project() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        let pki_client_id = 11111;
        let oauth_client_id = 22222;

        let mut project_cache = AggregateToUserAccessLevel::new();
        
        // PKI-only user
        project_cache.update_cache_for_user(&pki_client_id, None, AccessLevel::Owner, true);
        
        // OAuth user
        project_cache.update_cache_for_user(&oauth_client_id, Some("oauth_user"), AccessLevel::Contributor, true);
        
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Verify PKI user access
        let pki_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &pki_client_id, None);
        assert_eq!(pki_access, AccessLevel::Owner);

        // Verify OAuth user access
        let oauth_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &oauth_client_id, Some("oauth_user"));
        assert_eq!(oauth_access, AccessLevel::Contributor);

        // Verify cross-contamination doesn't occur
        let wrong_pki = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &oauth_client_id, None);
        assert_eq!(wrong_pki, AccessLevel::None);

        let wrong_oauth = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &pki_client_id, Some("oauth_user_wrong"));
        assert_eq!(wrong_oauth, AccessLevel::None);
    }

    #[test]
    fn test_populate_cache_handles_mixed_pki_oauth_events() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        // Create mixed PKI and OAuth events
        let pki_event = create_provide_access_event_only_client(&11111, AccessLevel::Owner, Some(1));
        let oauth_event1 = create_provide_access_event(&22222, "oauth_user", AccessLevel::Contributor, Some(2));
        let oauth_event2 = create_provide_access_event(&11111, "migrated_user", AccessLevel::Owner, Some(3)); // PKI->OAuth migration

        let event_batch_1 = create_event_batch_item_with_events(vec![pki_event], "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![oauth_event1], "admin");
        let event_batch_3 = create_event_batch_item_with_events(vec![oauth_event2], "admin");

        event_storage_cache.write(&file_path, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_2).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_3).unwrap();

        // Populate cache
        user_access_cache.cache.insert(file_path.clone(), AggregateToUserAccessLevel::new());
        user_access_cache.populate_cache_for_aggregate(&mut event_storage_cache, &file_path);

        let project_cache = user_access_cache.cache.get(&file_path).unwrap();

        // Verify PKI access is gone due to migration
        assert_eq!(project_cache.get_access_level(&11111, None), AccessLevel::None);

        // Verify OAuth user access
        assert_eq!(project_cache.get_access_level(&22222, Some("oauth_user")), AccessLevel::Contributor);
        assert_eq!(project_cache.get_access_level(&11111, Some("migrated_user")), AccessLevel::Owner);

        assert_eq!(project_cache.count(), 2);
    }

    #[test]
    fn test_admin_can_manage_both_pki_and_oauth_users() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        let admin_client_id = 99999;
        let pki_user_client_id = 11111;
        let oauth_user_client_id = 22222;

        // Set up admin with Owner access
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&admin_client_id, Some("admin"), AccessLevel::Owner, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Admin grants access to PKI user
        let result1 = user_access_cache.update_access_for_user(
            &mut event_storage_cache,
            &file_path,
            &admin_client_id,
            Some("admin"),
            &pki_user_client_id,
            None, // PKI user (no OAuth)
            AccessLevel::Contributor,
            false,
            None,
            1000
        ).unwrap();
        assert!(result1.is_some());

        // Admin grants access to OAuth user
        let result2 = user_access_cache.update_access_for_user(
            &mut event_storage_cache,
            &file_path,
            &admin_client_id,
            Some("admin"),
            &oauth_user_client_id,
            Some("oauth_user"), // OAuth user
            AccessLevel::Viewer,
            false,
            None,
            1001
        ).unwrap();
        assert!(result2.is_some());

        // Verify both users have access
        let pki_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &pki_user_client_id, None);
        assert_eq!(pki_access, AccessLevel::Contributor);

        let oauth_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &oauth_user_client_id, Some("oauth_user"));
        assert_eq!(oauth_access, AccessLevel::Viewer);
    }

    #[test]
    fn test_client_id_collision_with_different_oauth_users() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        let shared_client_id = 12345;
        
        let mut project_cache = AggregateToUserAccessLevel::new();
        
        // Hypothetical scenario: same client_id used by different OAuth users
        // (This might happen if someone shares a device or client_id generation collision)
        project_cache.update_cache_for_user(&shared_client_id, Some("user_a"), AccessLevel::Owner, true);
        project_cache.update_cache_for_user(&shared_client_id, Some("user_b"), AccessLevel::Viewer, true);
        
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Each OAuth user should maintain their own access level
        let user_a_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &shared_client_id, Some("user_a"));
        assert_eq!(user_a_access, AccessLevel::Owner);

        let user_b_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &shared_client_id, Some("user_b"));
        assert_eq!(user_b_access, AccessLevel::Viewer);

        // PKI access should be None when OAuth users exist
        let pki_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &shared_client_id, None);
        assert_eq!(pki_access, AccessLevel::None);
    }

    #[test]
    fn test_share_key_scenarios_with_mixed_auth() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let _ = event_storage_cache.write(&file_path, true, initial_event());

        let admin_client_id = 99999;
        let recipient_client_id = 12345;

        // Set up admin
        let mut project_cache = AggregateToUserAccessLevel::new();
        project_cache.update_cache_for_user(&admin_client_id, Some("admin"), AccessLevel::Owner, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Admin creates share link for PKI user
        let result = user_access_cache.update_access_for_user(
            &mut event_storage_cache,
            &file_path,
            &admin_client_id,
            Some("admin"),
            &recipient_client_id,
            None, // PKI recipient
            AccessLevel::Viewer,
            false,
            Some(34324),
            1000
        ).unwrap();
        assert!(result.is_some());

        // Later, the PKI user migrates to OAuth
        let oauth_result = user_access_cache.update_access_for_user(
            &mut event_storage_cache,
            &file_path,
            &admin_client_id,
            Some("admin"),
            &recipient_client_id,
            Some("oauth_migrated_user"), // Now OAuth
            AccessLevel::Viewer, // Same level
            false,
            Some(34324), // Same share key
            1001
        ).unwrap();
        assert!(oauth_result.is_some());

        // Verify OAuth access works and PKI doesn't
        let oauth_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &recipient_client_id, Some("oauth_migrated_user"));
        assert_eq!(oauth_access, AccessLevel::Viewer);

        let pki_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, &recipient_client_id, None);
        assert_eq!(pki_access, AccessLevel::None);
    }

}
