use std::{collections::{HashMap, VecDeque}, io, usize};
use event_storage::{event_batch_item::EventBatchItem, event_item::EventItem, event_storage_cache::{EventStorageCache}};
use crate::{access_level::AccessLevel, project_event_type::ProjectEventType, project_to_user_access_level::ProjectToUserAccessLevel};

pub struct UserAccessCache {
    // The queue is used to evict the oldest files from the cache when the cache is full
    cache_queue: VecDeque<String>,

    // The cache maps a file to another hashmap of user_hash to access level
    cache: HashMap<String, ProjectToUserAccessLevel>,

    // The maximum number of projects to cache, currently we can have unlimited users inside a project (str+u64)
    cache_max_project_count: usize,
}

impl UserAccessCache {
    pub fn new(
        cache_max_project_count: usize,
    ) -> Self {
        Self {
            cache_queue: VecDeque::new(),
            cache: HashMap::new(),
            cache_max_project_count,
        }
    }

    /// If we have exeeded the maximum nbr of projects in the cache, clear out the oldest ones
    fn clear_cache(&mut self) {
        
        if self.cache.len() < self.cache_max_project_count {
            return;
        }

        while self.cache.len() > self.cache_max_project_count {
            if let Some(file_path) = self.cache_queue.pop_front() {
                self.cache.remove(&file_path);
            } else {
                break;
            }
        }
    }

    /// Grab the current cache for a project, or build it if it doesn't exist and add it to the cache
    fn get_or_build_cache(
        &mut self,
        event_storage_cache: &mut EventStorageCache,
        file_path: &str,
    ) -> &mut ProjectToUserAccessLevel {
        self.clear_cache();

        if self.cache.contains_key(file_path) {
            return self.cache.get_mut(file_path).unwrap();
        }

        self.cache.insert(file_path.to_string(), ProjectToUserAccessLevel::new());
        self.cache_queue.push_back(file_path.to_string());

        self.populate_cache_for_project(event_storage_cache, file_path);

        self.clear_cache();

        return self.cache.get_mut(file_path).unwrap();
    }

    /// Read all the ProvideAccess events for a project and build the cache for that project from the events found
    fn populate_cache_for_project(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str) {
        let project_to_user_access_level = self.cache.get_mut(file_path).unwrap();

        match event_storage_cache.read(file_path, 0, usize::MAX, Some(&[ProjectEventType::ProvideAccess as u64])) {
            Ok(result) => {
                for batch in result.event_batches  {
                    for event in batch.events.iter() {

                        // Check the event is a ProvideAccess event and has the correct data
                        if event.tp != ProjectEventType::ProvideAccess as u64 ||
                        event.string_values.as_ref().is_none() ||
                        event.string_values.as_ref().unwrap().len() < 1 ||
                        event.string_values.as_ref().unwrap()[0].is_none() ||
                        event.uint_values.is_none() || 
                        event.uint_values.as_ref().unwrap().len() == 0{
                            continue;
                        }

                        // The events are tp: ProvideAccess and the access level is stored as a u64 in the uint_values array
                        // The user id is a hash of their public key stored unencrypted in the first item of the string value array
                        let user_hash = event.string_values.as_ref().unwrap()[0].as_ref().unwrap().clone();
                        let access_level = AccessLevel::from(event.uint_values.as_ref().unwrap()[0]);

                        // As we process events in chronological order, allow the users' access 
                        // to upgrade OR downgrade depending on the event's access level
                        project_to_user_access_level.update_cache_for_user(&user_hash, access_level, true);
                    }
                }
            },

            // Fail to read, skip populating the cache for this project. Could be a new project or file deleted.
            Err(_) => { }
        }
    }

    /// Get the current access level for a user. Will build a cache if one does not exist.
    pub fn get_current_access_level(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str, user_hash: &str) -> AccessLevel {
        let project_to_user_access_level = self.get_or_build_cache(event_storage_cache, file_path);
        project_to_user_access_level.current_access_level_for_user(user_hash)
    }

    /// Change the access level for for_user_hash. Adds an event to the file and updates the cache.
    pub fn update_access_for_user(&mut self, 
        event_storage_cache: &mut EventStorageCache, 
        file_path: &str, 
        current_user_hash: &str, 
        for_user_hash: &str,
        potential_access_level: AccessLevel,
        allow_downgrade: bool, 
        share_key_hash: Option<&str>, 
        ed_override: Option<u64>) -> io::Result<Option<EventItem>> {

        //Not allowed to downgrade your own permissions
        if allow_downgrade && current_user_hash == for_user_hash
        {
            return Ok(None);
        }

        let current_access_level = self.get_current_access_level(event_storage_cache, file_path, for_user_hash);

        //No op as same permission level or lower level and not downgrading
        if current_access_level == potential_access_level || 
        !allow_downgrade && !AccessLevel::increases_access_level(current_access_level, potential_access_level)
        {
            return Ok(None);
        }

        let current_time = ed_override.unwrap_or(chrono::Utc::now().timestamp_millis() as u64);

        let mut event_item = EventItem::new();
        event_item.ed = current_time;
        event_item.tp = ProjectEventType::ProvideAccess as u64;
        event_item.string_values = Some(vec![Some(for_user_hash.to_string()), share_key_hash.map_or(None,|f| Some(f.to_string()))]);
        event_item.uint_values = Some(vec![potential_access_level as u64]);

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.events = vec![event_item.clone()];
        event_batch_item.cb = Some(current_user_hash.to_string());
        event_batch_item.sd = current_time;

        event_storage_cache.write(file_path, false, event_batch_item)?;

        let project_to_user_access_level = self.get_or_build_cache(event_storage_cache, file_path);
        project_to_user_access_level.update_cache_for_user(for_user_hash, potential_access_level, allow_downgrade);
        
        Ok(Some(event_item))
    }
    
}

#[cfg(test)]
mod tests {
    use std::{io, vec};
    use event_storage::event_item::EventItem;
    use crate::{access_level::AccessLevel, project_event_type::ProjectEventType, project_to_user_access_level::ProjectToUserAccessLevel};
    use event_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};
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
    fn create_provide_access_event(user_hash: &str, access_level: AccessLevel, ed_override: Option<u64>) -> EventItem {
        let current_time = ed_override.unwrap_or(chrono::Utc::now().timestamp_millis() as u64);

        let mut event_item = EventItem::new();
        event_item.ed = current_time;
        event_item.tp = ProjectEventType::ProvideAccess as u64;
        event_item.string_values = Some(vec![Some(user_hash.to_string()), None]);
        event_item.uint_values = Some(vec![access_level as u64]);
        event_item
    }

    // Helper function to create a mock EventBatchItem
    fn create_event_batch_item_with_events(events: Vec<EventItem>, current_user_hash: &str) -> EventBatchItem {
        let current_time = chrono::Utc::now().timestamp_millis() as u64;

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.events = events;
        event_batch_item.cb = Some(current_user_hash.to_string());
        event_batch_item.sd = current_time;
        event_batch_item
    }

    #[test]
    fn test_new() {
        let cache = UserAccessCache::new(5);
        assert_eq!(cache.cache_max_project_count, 5);
        assert_eq!(cache.cache.len(), 0);
        assert_eq!(cache.cache_queue.len(), 0);

        let cache = UserAccessCache::new(0);
        assert_eq!(cache.cache_max_project_count, 0);
    }

    #[test]
    fn test_clear_cache_does_nothing_when_under_max_capacity() {
        let (mut user_access_cache, _, _) = setup_cache(3);
        user_access_cache.cache.insert("project1".to_string(), ProjectToUserAccessLevel::new());
        user_access_cache.cache.insert("project2".to_string(), ProjectToUserAccessLevel::new());
        user_access_cache.cache_queue.push_back("project1".to_string());
        user_access_cache.cache_queue.push_back("project2".to_string());

        user_access_cache.clear_cache();

        assert_eq!(user_access_cache.cache.len(), 2);
        assert_eq!(user_access_cache.cache_queue.len(), 2);
    }

    #[test]
    fn test_clear_cache_removes_oldest_projects_when_at_max_capacity() {
        let (mut user_access_cache, _, _) = setup_cache(2);
        user_access_cache.cache.insert("project1".to_string(), ProjectToUserAccessLevel::new());
        user_access_cache.cache.insert("project2".to_string(), ProjectToUserAccessLevel::new());
        user_access_cache.cache.insert("project3".to_string(), ProjectToUserAccessLevel::new());

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
        user_access_cache.cache.insert("project1".to_string(), ProjectToUserAccessLevel::new());
        user_access_cache.cache.insert("project2".to_string(), ProjectToUserAccessLevel::new());
        user_access_cache.cache.insert("project3".to_string(), ProjectToUserAccessLevel::new());

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
        user_access_cache.cache.insert("project1".to_string(), ProjectToUserAccessLevel::new());
        user_access_cache.cache.insert("project2".to_string(), ProjectToUserAccessLevel::new());
        user_access_cache.cache_queue.clear(); // Empty the queue

        user_access_cache.clear_cache();

        assert_eq!(user_access_cache.cache.len(), 2); // Should still contain the projects as queue is empty
    }

    #[test]
    fn test_cache_eviction_order_follows_fifo() {
        let (mut user_access_cache, _, _) = setup_cache(2);
        user_access_cache.cache_queue.push_back("project1".to_string());
        user_access_cache.cache_queue.push_back("project2".to_string());
        user_access_cache.cache.insert("project1".to_string(), ProjectToUserAccessLevel::new());
        user_access_cache.cache.insert("project2".to_string(), ProjectToUserAccessLevel::new());

        // Add a third project, which should evict "project1" (oldest)
        user_access_cache.cache.insert("project3".to_string(), ProjectToUserAccessLevel::new());
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
        let event1 = create_provide_access_event("user1", AccessLevel::Owner, None);
        let event2 = create_provide_access_event("user2", AccessLevel::Contributor, None);
        let event3 = create_provide_access_event("user1", AccessLevel::Viewer, None); // Update user1

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], "admin");
        let event_batch_3 = create_event_batch_item_with_events(vec![event3], "admin");

        event_storage_cache.write(&file_path, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_2).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_3).unwrap();

        // Populate cache for the project
        user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());
        user_access_cache.populate_cache_for_project(&mut event_storage_cache, &file_path);

        // Verify cache content
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.current_access_level_for_user("user1"), AccessLevel::Viewer);
        assert_eq!(project_cache.current_access_level_for_user("user2"), AccessLevel::Contributor);
        assert_eq!(project_cache.count(), 2);
    }

    #[test]
    fn test_populate_cache_for_project_with_mixed_event_types() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create ProvideAccess and other event types
        let event1 = create_provide_access_event("user1", AccessLevel::Owner, None);
        let mut event2 = create_provide_access_event("user2", AccessLevel::Contributor, None);
        event2.tp = ProjectEventType::AddShareLink as u64; // Change type to AddShareLink
        let event3 = create_provide_access_event("user1", AccessLevel::Viewer, None);

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], "admin");
        let event_batch_3 = create_event_batch_item_with_events(vec![event3], "admin");

        event_storage_cache.write(&file_path, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_2).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_3).unwrap();

        // Populate cache for the project
        user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());
        user_access_cache.populate_cache_for_project(&mut event_storage_cache, &file_path);

        // Verify cache content (only ProvideAccess events should be processed)
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.current_access_level_for_user("user1"), AccessLevel::Viewer);
        assert_eq!(project_cache.current_access_level_for_user("user2"), AccessLevel::None); // Not processed
        assert_eq!(project_cache.count(), 1);
    }

    #[test]
    fn test_populate_cache_for_project_with_malformed_events_missing_fields() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create malformed events (missing fields)
        let mut event1 = create_provide_access_event("user1", AccessLevel::Owner, None);
        event1.string_values = None; // Missing string_values
        let mut event2 = create_provide_access_event("user2", AccessLevel::Contributor, None);
        event2.uint_values = None; // Missing uint_values

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], "admin");
        event_storage_cache.write(&file_path, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_2).unwrap();

        // Populate cache for the project
        user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());
        user_access_cache.populate_cache_for_project(&mut event_storage_cache, &file_path);

        // Verify cache content (malformed events should be ignored)
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_with_events_missing_string_values() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an event with missing string_values
        let mut event = create_provide_access_event("user1", AccessLevel::Owner, None);
        event.string_values = None;

        let event_batch = create_event_batch_item_with_events(vec![event], "admin");
        event_storage_cache.write(&file_path, true, event_batch).unwrap();

        // Populate the cache
        user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());
        user_access_cache.populate_cache_for_project(&mut event_storage_cache, &file_path);

        // Verify that the malformed event was ignored
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_with_events_missing_uint_values() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an event with missing uint_values
        let mut event = create_provide_access_event("user1", AccessLevel::Owner, None);
        event.uint_values = None;

        let event_batch = create_event_batch_item_with_events(vec![event], "admin");
        event_storage_cache.write(&file_path, true, event_batch).unwrap();

        // Populate the cache
        user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());
        user_access_cache.populate_cache_for_project(&mut event_storage_cache, &file_path);

        // Verify that the malformed event was ignored
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_with_events_having_insufficient_array_lengths() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an event with string_values having insufficient length
        let mut event1 = create_provide_access_event("user1", AccessLevel::Owner, None);
        event1.string_values = Some(vec![]); // Shortened vector

        // Create an event with uint_values having insufficient length
        let mut event2 = create_provide_access_event("user2", AccessLevel::Contributor, None);
        event2.uint_values = Some(vec![]); // Empty vector

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], "admin");
        event_storage_cache.write(&file_path, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_2).unwrap();

        // Populate the cache
        user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());
        user_access_cache.populate_cache_for_project(&mut event_storage_cache, &file_path);

        // Verify that the malformed events were ignored
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_with_events_having_none_values_in_required_positions() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create an event with None in the user_hash position (first string value)
        let mut event = create_provide_access_event("user1", AccessLevel::Owner, None);
        event.string_values = Some(vec![None, Some("share_key".to_string())]);

        let event_batch = create_event_batch_item_with_events(vec![event], "admin");
        event_storage_cache.write(&file_path, true, event_batch).unwrap();

        // Populate the cache
        user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());
        user_access_cache.populate_cache_for_project(&mut event_storage_cache, &file_path);

        // Verify that the malformed event was ignored
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_when_file_doesnt_exist() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "nonexistent_project.bin");

        // Attempt to populate cache for a non-existent file
        user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());
        user_access_cache.populate_cache_for_project(&mut event_storage_cache, &file_path);

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
            si: 0,
            cb: Some("admin".to_string()),
            sd: chrono::Utc::now().timestamp_millis() as u64,
            events: vec![],
        };
        event_storage_cache.write(&file_path, true, event_batch).unwrap();

        // Populate the cache
        user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());
        user_access_cache.populate_cache_for_project(&mut event_storage_cache, &file_path);

        // Verify that the cache remains empty
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.count(), 0);
    }

    #[test]
    fn test_populate_cache_for_project_processes_events_in_chronological_order() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create ProvideAccess events with different timestamps
        let event1 = create_provide_access_event("user1", AccessLevel::Contributor, Some(2));
        let event2 = create_provide_access_event("user1", AccessLevel::Owner, Some(1)); // Earlier timestamp
        let event3 = create_provide_access_event("user1", AccessLevel::Viewer, Some(3)); // Later timestamp

        let event_batch_1 = create_event_batch_item_with_events(vec![event1], "admin");
        let event_batch_2 = create_event_batch_item_with_events(vec![event2], "admin");
        let event_batch_3 = create_event_batch_item_with_events(vec![event3], "admin");

        event_storage_cache.write(&file_path, true, event_batch_1).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_2).unwrap();
        event_storage_cache.write(&file_path, true, event_batch_3).unwrap();

        // Populate cache for the project
        user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());
        user_access_cache.populate_cache_for_project(&mut event_storage_cache, &file_path);

        // Verify that the latest event (highest timestamp) determines the access level
        let project_cache = user_access_cache.cache.get(&file_path).unwrap();
        assert_eq!(project_cache.current_access_level_for_user("user1"), AccessLevel::Viewer);
    }

    #[test]
    fn test_get_or_build_cache_returns_existing_cache_when_project_already_cached() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Insert a project into the cache
        // user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());

        // Get the cache for the project
        {
            let project_cache = user_access_cache.get_or_build_cache(&mut event_storage_cache, &file_path);
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
        user_access_cache.cache.insert(file_path1.clone(), ProjectToUserAccessLevel::new());
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
        let mut project_cache = ProjectToUserAccessLevel::new();
        project_cache.update_cache_for_user("user1", AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Get the access level for the user
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, "user1");

        // Verify that the access level is correct
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    #[test]
    fn test_get_current_access_level_for_non_existent_user_should_return_default() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Insert a project into the cache (no users)
        user_access_cache.cache.insert(file_path.clone(), ProjectToUserAccessLevel::new());

        // Get the access level for a non-existent user
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, "user1");

        // Verify that the access level is the default (None)
        assert_eq!(access_level, AccessLevel::None);
    }

    #[test]
    fn test_get_current_access_level_for_new_project_builds_cache_first() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Get the access level for a project that is not in the cache
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, "user1");

        // Verify that a new cache was created and the access level is the default (None)
        assert!(user_access_cache.cache.contains_key(&file_path));
        assert_eq!(access_level, AccessLevel::None);
    }

    #[test]
    fn test_get_current_access_level_with_various_access_levels() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");

        // Create a project cache and add a user with different access levels
        let mut project_cache = ProjectToUserAccessLevel::new();
        project_cache.update_cache_for_user("owner", AccessLevel::Owner, true);
        project_cache.update_cache_for_user("contributor", AccessLevel::Contributor, true);
        project_cache.update_cache_for_user("viewer", AccessLevel::Viewer, true);
        project_cache.update_cache_for_user("none", AccessLevel::None, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Verify that the correct access levels are returned
        assert_eq!(user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, "owner"), AccessLevel::Owner);
        assert_eq!(user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, "contributor"), AccessLevel::Contributor);
        assert_eq!(user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, "viewer"), AccessLevel::Viewer);
        assert_eq!(user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, "none"), AccessLevel::None);
        assert_eq!(user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, "unknown"), AccessLevel::None); // Non-existent user
    }

    #[test]
    fn test_update_access_for_user_prevents_self_downgrade_when_allow_downgrade_is_true() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";

        // Initialize user's access level to Contributor
        let mut project_cache = ProjectToUserAccessLevel::new();
        project_cache.update_cache_for_user(user_hash, AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Attempt to downgrade own access level
        let result = user_access_cache.update_access_for_user(&mut event_storage_cache, &file_path, user_hash, user_hash, AccessLevel::Viewer, true, None, None).unwrap();

        // Verify that the update was prevented (returns None)
        assert!(result.is_none());

        // Verify that the access level remains unchanged
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, user_hash);
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    #[test]
    fn test_update_access_for_user_disallows_self_upgrade() {
        let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";

        // Initialize user's access level to Contributor
        let mut project_cache = ProjectToUserAccessLevel::new();
        project_cache.update_cache_for_user(user_hash, AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Attempt to upgrade own access level
        let result = user_access_cache.update_access_for_user(&mut event_storage_cache, &file_path, user_hash, user_hash, AccessLevel::Owner, true, None, None).unwrap();

        // Verify that the update was successful (returns Some(EventItem))
        assert!(result.is_none());

        // Verify that the access level was updated
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, user_hash);
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    #[test]
    fn test_update_access_for_user_returns_none_for_same_access_level_no_op() {
       let (mut user_access_cache, mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";

        // Initialize user's access level to Contributor
        let mut project_cache = ProjectToUserAccessLevel::new();
        project_cache.update_cache_for_user(user_hash, AccessLevel::Contributor, true);
        user_access_cache.cache.insert(file_path.clone(), project_cache);

        // Attempt to set same access level
        let result = user_access_cache.update_access_for_user(&mut event_storage_cache, &file_path, "admin", user_hash, AccessLevel::Contributor, true, None, None).unwrap();

        // Verify that the update was a no-op (returns None)
        assert!(result.is_none());

        // Verify that the access level remains unchanged
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, user_hash);
        assert_eq!(access_level, AccessLevel::Contributor);
    }
}