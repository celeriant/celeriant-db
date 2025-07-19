// use crate::{access_level::AccessLevel, aggregate_event_type::AggregateEventType, aggregate_to_user_access_level::AggregateToUserAccessLevel, user_access_cache::UserAccessCache};
// use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem, event_storage_cache::EventStorageCache};
// use std::{
//     collections::{HashMap, VecDeque},
//     io, usize,
// };

// pub struct UserSubMappingCache {
//     // The queue is used to evict the oldest files from the cache when the cache is full
//     cache_queue: VecDeque<String>,

//     // The cache maps a file to another hashmap of sub to access level
//     cache: HashMap<String, AggregateToUserAccessLevel>,

//     // The maximum number of projects to cache
//     cache_max_aggregate_count: usize,
// }

// impl UserSubMappingCache {
//     pub fn new(cache_max_aggregate_count: usize) -> Self {
//         Self {
//             cache_queue: VecDeque::new(),
//             cache: HashMap::new(),
//             cache_max_aggregate_count,
//         }
//     }

//     /// If we have exceeded the maximum nbr of projects in the cache, clear out the oldest ones
//     fn clear_cache(&mut self) {
//         if self.cache.len() < self.cache_max_aggregate_count {
//             return;
//         }

//         while self.cache.len() > self.cache_max_aggregate_count {
//             if let Some(file_path) = self.cache_queue.pop_front() {
//                 self.cache.remove(&file_path);
//             } else {
//                 break;
//             }
//         }
//     }

//     /// Grab the current cache for a project, or build it if it doesn't exist and add it to the cache
//     fn get_or_build_cache(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str, user_access_cache: &mut UserAccessCache) -> &mut AggregateToUserAccessLevel {
//         self.clear_cache();

//         if self.cache.contains_key(file_path) {
//             return self.cache.get_mut(file_path).unwrap();
//         }

//         self.cache.insert(file_path.to_string(), AggregateToUserAccessLevel::new());
//         self.cache_queue.push_back(file_path.to_string());

//         self.populate_cache_for_aggregate(event_storage_cache, file_path, user_access_cache);

//         self.clear_cache();

//         return self.cache.get_mut(file_path).unwrap();
//     }

//     /// Read all the MapUserHashToSub events for a project and build the cache for that project from the events found
//     fn populate_cache_for_aggregate(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str, user_access_cache: &mut UserAccessCache) {
//         let aggregate_to_user_access_level = self.cache.get_mut(file_path).unwrap();

//         match event_storage_cache.read(file_path, 0, usize::MAX, Some(&[AggregateEventType::MapUserHashToSub as u64])) {
//             Ok(result) => {
//                 for batch in result.event_batches {
//                     for event in batch.events.iter() {
//                         // Check the event is a MapUserHashToSub event and has the correct data
//                         if event.tp != AggregateEventType::MapUserHashToSub as u64
//                             || event.string_values.as_ref().is_none()
//                             || event.string_values.as_ref().unwrap().len() < 2
//                             || event.string_values.as_ref().unwrap()[0].is_none()
//                             || event.string_values.as_ref().unwrap()[1].is_none()
//                         {
//                             continue;
//                         }

//                         // The events are tp: MapUserHashToSub
//                         // The sub is stored in the first item of the string value array
//                         // The user_hash is stored in the second item of the string value array
//                         let sub = event.string_values.as_ref().unwrap()[0].as_ref().unwrap().clone();
//                         let user_hash = event.string_values.as_ref().unwrap()[1].as_ref().unwrap().clone();

//                         // Get the current access level for the user_hash from the user access cache
//                         let access_level = user_access_cache.get_current_access_level(event_storage_cache, file_path, &user_hash);

//                         // Update the cache with the sub mapping to the access level
//                         // As we process events in chronological order, allow the mapping to be updated
//                         aggregate_to_user_access_level.update_cache_for_user(&sub, access_level, true);
//                     }
//                 }
//             }

//             // Fail to read, skip populating the cache for this project. Could be a new project or file deleted.
//             Err(_) => {}
//         }
//     }

//     /// Get the current access level for a sub. Will build a cache if one does not exist.
//     pub fn get_current_access_level(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str, sub: &str, user_access_cache: &mut UserAccessCache) -> AccessLevel {
//         let aggregate_to_user_access_level = self.get_or_build_cache(event_storage_cache, file_path, user_access_cache);
//         aggregate_to_user_access_level.current_access_level_for_user(sub)
//     }

//     pub fn upsert_access_level(
//         &mut self,
//         event_storage_cache: &mut EventStorageCache,
//         file_path: &str,
//         sub: &str,
//         user_hash: &str,
//         user_access_cache: &mut UserAccessCache,
//     ) {
//         // Get the current access level for the user_hash and update the cache
//         let access_level = user_access_cache.get_current_access_level(event_storage_cache, file_path, user_hash);
//         let aggregate_to_user_access_level = self.get_or_build_cache(event_storage_cache, file_path, user_access_cache);
//         aggregate_to_user_access_level.update_cache_for_user(sub, access_level, true);
//     }

//     /// Create a mapping between sub and user_hash. Adds an event to the file and updates the cache.
//     pub fn map_sub_to_user_hash(
//         &mut self,
//         event_storage_cache: &mut EventStorageCache,
//         file_path: &str,
//         current_user_hash: &str,
//         sub: &str,
//         user_hash: &str,
//         user_access_cache: &mut UserAccessCache,
//         server_time: u64,
//     ) -> io::Result<Option<EventBatchItem>> {
//         let mut event_item = EventItem::new();
//         event_item.ed = server_time;
//         event_item.tp = AggregateEventType::MapUserHashToSub as u64;
//         event_item.string_values = Some(vec![Some(sub.to_string()), Some(user_hash.to_string())]);

//         let mut event_batch_item = EventBatchItem::new();
//         event_batch_item.events = vec![event_item];
//         event_batch_item.cb = Some(current_user_hash.to_string());
//         event_batch_item.sd = server_time;

//         event_batch_item.si = event_storage_cache.write(file_path, false, event_batch_item.clone())?;

//         self.upsert_access_level(event_storage_cache, file_path, sub, user_hash, user_access_cache);
//         Ok(Some(event_batch_item))
//     }
// }

// #[cfg(test)]
// mod tests {
//     use crate::{access_level::AccessLevel, aggregate_event_type::AggregateEventType, user_access_cache::UserAccessCache};
//     use eventplanedb_storage::event_item::EventItem;
//     use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};
//     use std::vec;
//     use tempfile::TempDir;

//     use super::*;

//     // Helper function to create a basic EventStorageCache for testing
//     fn setup_cache(max_projects: usize) -> (UserSubMappingCache, EventStorageCache, UserAccessCache, TempDir) {
//         let user_sub_mapping_cache = UserSubMappingCache::new(max_projects);
//         let event_storage_cache = EventStorageCache::new(30, 1000000, 10000);
//         let user_access_cache = UserAccessCache::new(max_projects);
//         let temp_dir = TempDir::new().expect("Failed to create temp directory");
//         (user_sub_mapping_cache, event_storage_cache, user_access_cache, temp_dir)
//     }

//     // Helper function to create a file path within the temp directory
//     fn create_file_path(temp_dir: &TempDir, file_name: &str) -> String {
//         let events_bin = temp_dir.path().join(file_name);
//         events_bin.to_str().unwrap().to_string()
//     }

//     // Helper function to create a mock MapUserHashToSub EventItem
//     fn create_map_user_hash_to_sub_event(sub: &str, user_hash: &str, ed_override: Option<u64>) -> EventItem {
//         let current_time = ed_override.unwrap_or(chrono::Utc::now().timestamp_millis() as u64);

//         let mut event_item = EventItem::new();
//         event_item.ed = current_time;
//         event_item.tp = AggregateEventType::MapUserHashToSub as u64;
//         event_item.string_values = Some(vec![Some(sub.to_string()), Some(user_hash.to_string())]);
//         event_item
//     }

//     // Helper function to create a mock UserAccessUpdated EventItem
//     fn create_user_access_updated_event(user_hash: &str, access_level: AccessLevel, ed_override: Option<u64>) -> EventItem {
//         let current_time = ed_override.unwrap_or(chrono::Utc::now().timestamp_millis() as u64);

//         let mut event_item = EventItem::new();
//         event_item.ed = current_time;
//         event_item.tp = AggregateEventType::UserAccessUpdated as u64;
//         event_item.string_values = Some(vec![Some(user_hash.to_string()), None]);
//         event_item.uint_values = Some(vec![access_level as u64]);
//         event_item
//     }

//     // Helper function to create a mock EventBatchItem
//     fn create_event_batch_item_with_events(events: Vec<EventItem>, current_user_hash: &str) -> EventBatchItem {
//         let current_time = chrono::Utc::now().timestamp_millis() as u64;

//         let mut event_batch_item = EventBatchItem::new();
//         event_batch_item.events = events;
//         event_batch_item.cb = Some(current_user_hash.to_string());
//         event_batch_item.sd = current_time;
//         event_batch_item
//     }

//     #[test]
//     fn test_new() {
//         let cache = UserSubMappingCache::new(5);
//         assert_eq!(cache.cache_max_aggregate_count, 5);
//         assert_eq!(cache.cache.len(), 0);
//         assert_eq!(cache.cache_queue.len(), 0);
//     }

//     #[test]
//     fn test_populate_cache_with_valid_map_events_and_access_levels() {
//         let (mut user_sub_mapping_cache, mut event_storage_cache, mut user_access_cache, temp_dir) = setup_cache(5);
//         let file_path = create_file_path(&temp_dir, "project1.bin");

//         // First create user access events
//         let user_access_event1 = create_user_access_updated_event("user_hash_1", AccessLevel::Owner, Some(1));
//         let user_access_event2 = create_user_access_updated_event("user_hash_2", AccessLevel::Contributor, Some(2));

//         // Then create mapping events
//         let map_event1 = create_map_user_hash_to_sub_event("sub_123", "user_hash_1", Some(3));
//         let map_event2 = create_map_user_hash_to_sub_event("sub_456", "user_hash_2", Some(4));

//         let user_access_batch1 = create_event_batch_item_with_events(vec![user_access_event1], "admin");
//         let user_access_batch2 = create_event_batch_item_with_events(vec![user_access_event2], "admin");
//         let map_batch1 = create_event_batch_item_with_events(vec![map_event1], "admin");
//         let map_batch2 = create_event_batch_item_with_events(vec![map_event2], "admin");

//         event_storage_cache.write(&file_path, true, user_access_batch1).unwrap();
//         event_storage_cache.write(&file_path, true, user_access_batch2).unwrap();
//         event_storage_cache.write(&file_path, true, map_batch1).unwrap();
//         event_storage_cache.write(&file_path, true, map_batch2).unwrap();

//         // Get access levels by sub
//         let access_level1 = user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path, "sub_123", &mut user_access_cache);
//         let access_level2 = user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path, "sub_456", &mut user_access_cache);
//         let access_level3 = user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path, "sub_unknown", &mut user_access_cache);

//         // Verify cache content
//         assert_eq!(access_level1, AccessLevel::Owner);
//         assert_eq!(access_level2, AccessLevel::Contributor);
//         assert_eq!(access_level3, AccessLevel::None);
//     }

//     #[test]
//     fn test_populate_cache_with_malformed_map_events() {
//         let (mut user_sub_mapping_cache, mut event_storage_cache, mut user_access_cache, temp_dir) = setup_cache(5);
//         let file_path = create_file_path(&temp_dir, "project1.bin");

//         // Create malformed events
//         let mut malformed_event1 = create_map_user_hash_to_sub_event("sub_123", "user_hash_1", None);
//         malformed_event1.string_values = None; // Missing string_values

//         let mut malformed_event2 = create_map_user_hash_to_sub_event("sub_123", "user_hash_1", None);
//         malformed_event2.string_values = Some(vec![Some("sub_123".to_string())]); // Insufficient length

//         let mut malformed_event3 = create_map_user_hash_to_sub_event("sub_123", "user_hash_1", None);
//         malformed_event3.string_values = Some(vec![None, Some("user_hash_1".to_string())]); // None in required position

//         let batch1 = create_event_batch_item_with_events(vec![malformed_event1], "admin");
//         let batch2 = create_event_batch_item_with_events(vec![malformed_event2], "admin");
//         let batch3 = create_event_batch_item_with_events(vec![malformed_event3], "admin");

//         event_storage_cache.write(&file_path, true, batch1).unwrap();
//         event_storage_cache.write(&file_path, true, batch2).unwrap();
//         event_storage_cache.write(&file_path, true, batch3).unwrap();

//         // Get access level (should be None due to malformed events)
//         let access_level = user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path, "sub_123", &mut user_access_cache);
//         assert_eq!(access_level, AccessLevel::None);
//     }

//     #[test]
//     fn test_populate_cache_ignores_other_event_types() {
//         let (mut user_sub_mapping_cache, mut event_storage_cache, mut user_access_cache, temp_dir) = setup_cache(5);
//         let file_path = create_file_path(&temp_dir, "project1.bin");

//         // Create events of different types
//         let mut wrong_type_event = create_map_user_hash_to_sub_event("sub_123", "user_hash_1", None);
//         wrong_type_event.tp = AggregateEventType::ShareLinkCreated as u64; // Wrong type

//         let correct_event = create_map_user_hash_to_sub_event("sub_456", "user_hash_2", None);

//         // Create user access event for user_hash_2
//         let user_access_event = create_user_access_updated_event("user_hash_2", AccessLevel::Viewer, None);

//         let batch1 = create_event_batch_item_with_events(vec![user_access_event], "admin");
//         let batch2 = create_event_batch_item_with_events(vec![wrong_type_event], "admin");
//         let batch3 = create_event_batch_item_with_events(vec![correct_event], "admin");

//         event_storage_cache.write(&file_path, true, batch1).unwrap();
//         event_storage_cache.write(&file_path, true, batch2).unwrap();
//         event_storage_cache.write(&file_path, true, batch3).unwrap();

//         // Verify only the correct event was processed
//         let access_level1 = user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path, "sub_123", &mut user_access_cache);
//         let access_level2 = user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path, "sub_456", &mut user_access_cache);

//         assert_eq!(access_level1, AccessLevel::None); // Wrong type event ignored
//         assert_eq!(access_level2, AccessLevel::Viewer); // Correct event processed
//     }

//     #[test]
//     fn test_map_sub_to_user_hash_creates_event_and_updates_cache() {
//         let (mut user_sub_mapping_cache, mut event_storage_cache, mut user_access_cache, temp_dir) = setup_cache(5);
//         let file_path = create_file_path(&temp_dir, "project1.bin");

//         // First create a user access event
//         let user_access_event = create_user_access_updated_event("user_hash_1", AccessLevel::Contributor, None);
//         let user_access_batch = create_event_batch_item_with_events(vec![user_access_event], "admin");
//         event_storage_cache.write(&file_path, true, user_access_batch).unwrap();

//         // Map sub to user hash
//         let result = user_sub_mapping_cache
//             .map_sub_to_user_hash(&mut event_storage_cache, &file_path, "admin", "sub_123", "user_hash_1", &mut user_access_cache, 6565)
//             .unwrap();

//         // Verify event was created
//         assert!(result.is_some());

//         // Verify cache was updated
//         let access_level = user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path, "sub_123", &mut user_access_cache);
//         assert_eq!(access_level, AccessLevel::Contributor);
//     }

//     #[test]
//     fn test_map_sub_to_user_hash_with_no_existing_user_access() {
//         let (mut user_sub_mapping_cache, mut event_storage_cache, mut user_access_cache, temp_dir) = setup_cache(5);
//         let file_path = create_file_path(&temp_dir, "project1.bin");

//         // First create a user access event so the file exists
//         let user_access_event = create_user_access_updated_event("user_hash_1", AccessLevel::Contributor, None);
//         let user_access_batch = create_event_batch_item_with_events(vec![user_access_event], "admin");
//         event_storage_cache.write(&file_path, true, user_access_batch).unwrap();

//         // Map sub to user hash without any existing user access
//         let result = user_sub_mapping_cache
//             .map_sub_to_user_hash(&mut event_storage_cache, &file_path, "admin", "sub_123", "user_hash_unknown", &mut user_access_cache, 6565)
//             .unwrap();

//         // Verify event was created
//         assert!(result.is_some());

//         // Verify cache shows no access (since user_hash_unknown has no access)
//         let access_level = user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path, "sub_123", &mut user_access_cache);
//         assert_eq!(access_level, AccessLevel::None);
//     }

//     #[test]
//     fn test_cache_eviction_works_correctly() {
//         let (mut user_sub_mapping_cache, mut event_storage_cache, mut user_access_cache, temp_dir) = setup_cache(2);
//         let file_path1 = create_file_path(&temp_dir, "project1.bin");
//         let file_path2 = create_file_path(&temp_dir, "project2.bin");
//         let file_path3 = create_file_path(&temp_dir, "project3.bin");

//         // Fill cache to capacity
//         user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path1, "sub_1", &mut user_access_cache);
//         user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path2, "sub_2", &mut user_access_cache);

//         // Add third project, should evict first
//         user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path3, "sub_3", &mut user_access_cache);

//         // Verify eviction occurred
//         assert!(!user_sub_mapping_cache.cache.contains_key(&file_path1));
//         assert!(user_sub_mapping_cache.cache.contains_key(&file_path2));
//         assert!(user_sub_mapping_cache.cache.contains_key(&file_path3));
//     }

//     #[test]
//     fn test_mapping_updates_when_user_access_changes() {
//         let (mut user_sub_mapping_cache, mut event_storage_cache, mut user_access_cache, temp_dir) = setup_cache(5);
//         let file_path = create_file_path(&temp_dir, "project1.bin");

//         // Create initial user access and mapping
//         let user_access_event1 = create_user_access_updated_event("user_hash_1", AccessLevel::Viewer, Some(1));
//         let map_event = create_map_user_hash_to_sub_event("sub_123", "user_hash_1", Some(2));
//         let user_access_event2 = create_user_access_updated_event("user_hash_1", AccessLevel::Owner, Some(3));

//         let batch1 = create_event_batch_item_with_events(vec![user_access_event1], "admin");
//         let batch2 = create_event_batch_item_with_events(vec![map_event], "admin");
//         let batch3 = create_event_batch_item_with_events(vec![user_access_event2], "admin");

//         event_storage_cache.write(&file_path, true, batch1).unwrap();
//         event_storage_cache.write(&file_path, true, batch2).unwrap();
//         event_storage_cache.write(&file_path, true, batch3).unwrap();

//         // Clear cache to force repopulation
//         user_sub_mapping_cache.cache.clear();
//         user_sub_mapping_cache.cache_queue.clear();

//         // Get access level - should reflect the updated user access level
//         let access_level = user_sub_mapping_cache.get_current_access_level(&mut event_storage_cache, &file_path, "sub_123", &mut user_access_cache);
//         assert_eq!(access_level, AccessLevel::Owner);
//     }
// }