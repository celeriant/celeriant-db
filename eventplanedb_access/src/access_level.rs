use event_storage::{event_batch_item::EventBatchItem, event_item::EventItem, event_storage_cache::EventStorageCache};
use serde::{Deserialize, Serialize};

use crate::{job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::{self, UserAccessCache}};

#[derive(PartialEq, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AccessLevel {
    Owner = 0,
    Contributor = 1,
    Viewer = 2,
    None = 3,
}

impl From<u64> for AccessLevel {
    fn from(value: u64) -> Self {
        match value {
            0 => AccessLevel::Owner,
            1 => AccessLevel::Contributor,
            2 => AccessLevel::Viewer,
            3 => AccessLevel::None,
            _ => AccessLevel::None,
        }
    }
}


impl AccessLevel {
    pub fn increases_access_level(current_access_level: AccessLevel, potential_access_level: AccessLevel) -> bool {
        current_access_level as u64 > potential_access_level as u64
    }

    pub fn meets_required_access_level(current_access_level: AccessLevel, required_access_level: AccessLevel) -> bool {
        current_access_level as u64 <= required_access_level as u64
    }

    pub fn require_permission(
        event_storage_cache: &mut EventStorageCache,
        share_links_cache: &mut ShareLinksCache,
        user_access_cache: &mut UserAccessCache,
        file_path: &str,
        current_user_hash: &str,
        required_access_level: AccessLevel,
        potential_share_key_hash: Option<&str>,
    ) -> Result<Vec<EventBatchItem>, JobError> {
        
        let mut new_events = Vec::new();
        let mut current_acces_level = user_access_cache.get_current_access_level(event_storage_cache, file_path, current_user_hash);

        //Is there a share link provided that can increase the user's access level? 
        //If yes, use it (eager use of share links even when current action doesn't require that permission level)
        if let Some(share_key_hash) = potential_share_key_hash {
            if let Some(share_key_info) = share_links_cache.get_share_key_data_if_still_valid(event_storage_cache, file_path, share_key_hash) {
                let new_access_level_granted_by_share_link = share_key_info.access_level;

                //will the share link give the user more access? if yes, we can use it
                if AccessLevel::increases_access_level(current_acces_level, new_access_level_granted_by_share_link) {
                    current_acces_level = new_access_level_granted_by_share_link;

                    //The share link exists and can improve the users access level.
                    //Disable the share link if it is single use
                    if share_key_info.is_single_use {
                        let disable_event_item = share_links_cache.disable_share_link(event_storage_cache, file_path, current_user_hash.to_string(), share_key_hash.to_string())?;
                        new_events.push(disable_event_item);
                    }

                    //Upgrade the users permissions linking the share link that was used
                    let provide_access_event = user_access_cache.update_access_for_user(
                        event_storage_cache, 
                        file_path, 
                        current_user_hash, 
                        current_user_hash, 
                        new_access_level_granted_by_share_link, 
                        false,
                        Some(share_key_hash),
                        None)?;
                    new_events.push(provide_access_event.unwrap());
                }
            }
        }

        //Now the user has increased access, but is it enough to perform the action?
        if !AccessLevel::meets_required_access_level(current_acces_level, required_access_level) {
            return Err(JobError::PermissionDenied("User does not have permission to perform this action".to_string()));
        }

        Ok(new_events)
    }
    
}

// ... existing code ...

#[cfg(test)]
mod tests {
    use super::*;
    use event_storage::{event_batch_item::EventBatchItem, event_item::EventItem, event_storage_cache::EventStorageCache};
    use tempfile::TempDir;

    use crate::share_links_cache::ShareLinksCache;
    use crate::user_access_cache::UserAccessCache;

    // Helper function to create a basic EventStorageCache for testing
    fn setup_cache(max_projects: usize) -> (EventStorageCache, TempDir) {
        let event_storage_cache = EventStorageCache::new(30, 1000000, 10000);
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        (event_storage_cache, temp_dir)
    }

    // Helper function to create a file path within the temp directory
    fn create_file_path(temp_dir: &TempDir, file_name: &str) -> String {
        let events_bin = temp_dir.path().join(file_name);
        events_bin.to_str().unwrap().to_string()
    }

    fn create_test_event_item() -> EventItem {
        let mut event1 = EventItem::new();

        event1.ed = 443;
        event1.tp = 4;
        event1.int_values = Some(vec![1, 2, 3]);

        event1
    }

    #[test]
    fn test_require_permission_user_has_sufficient_access() {
        let (mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            si: 0,
            cb: None,
            sd: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Set user's access level to Contributor
        user_access_cache.update_access_for_user(&mut event_storage_cache, &file_path, "admin", user_hash, AccessLevel::Contributor, false, None, None).unwrap();

        // Require Viewer access (which Contributor meets)
        let result = AccessLevel::require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            user_hash,
            AccessLevel::Viewer,
            None,
        );

        // Verify that permission is granted
        assert!(result.is_ok());
    }

    #[test]
    fn test_require_permission_user_has_insufficient_access() {
        let (mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            si: 0,
            cb: None,
            sd: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Set user's access level to Viewer
        user_access_cache.update_access_for_user(&mut event_storage_cache, &file_path, "admin", user_hash, AccessLevel::Viewer, false, None, None).unwrap();

        // Require Contributor access (which Viewer does not meet)
        let result = AccessLevel::require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            user_hash,
            AccessLevel::Contributor,
            None,
        );

        // Verify that permission is denied
        assert!(result.is_err());
    }

    #[test]
    fn test_require_permission_user_has_sufficient_access_with_share_link() {
        let (mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";
        let share_key_hash = "share1";

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            si: 0,
            cb: None,
            sd: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Create a share link with Contributor access
        share_links_cache.create_share_link(
            &mut event_storage_cache,
            file_path.clone(),
            "admin".to_string(),
            share_key_hash.to_string(),
            AccessLevel::Contributor,
            false,
            None,
            None,
            0
        ).unwrap();

        // Require Contributor access, providing the share link
        let result = AccessLevel::require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            user_hash,
            AccessLevel::Contributor,
            Some(share_key_hash),
        );

        // Verify that permission is granted
        assert!(result.is_ok());

        // Verify that the user's access level has been updated
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, user_hash);
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    #[test]
    fn test_require_permission_user_has_insufficient_access_despite_share_link() {
        let (mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";
        let share_key_hash = "share1";

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            si: 0,
            cb: None,
            sd: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Set user's access level to Viewer
        user_access_cache.update_access_for_user(&mut event_storage_cache, &file_path, "admin", user_hash, AccessLevel::Viewer, false, None, None).unwrap();

        // Create a share link with Contributor access
        share_links_cache.create_share_link(
            &mut event_storage_cache,
            file_path.clone(),
            "admin".to_string(),
            share_key_hash.to_string(),
            AccessLevel::Contributor,
            false,
            None,
            None,
            0
        ).unwrap();

        // Require Owner access, providing the share link (Contributor is not enough)
        let result = AccessLevel::require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            user_hash,
            AccessLevel::Owner,
            Some(share_key_hash),
        );

        // Verify that permission is denied
        assert!(result.is_err());

        // Verify that the user's access level has been updated to the share link level
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, user_hash);
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    #[test]
    fn test_require_permission_share_link_is_single_use_and_disabled() {
        let (mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";
        let share_key_hash = "share1";

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            si: 0,
            cb: None,
            sd: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Create a single-use share link with Contributor access
        share_links_cache.create_share_link(
            &mut event_storage_cache,
            file_path.clone(),
            "admin".to_string(),
            share_key_hash.to_string(),
            AccessLevel::Contributor,
            true,
            None,
            None,
            0
        ).unwrap();

        // Require Contributor access, providing the share link
        let result = AccessLevel::require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            user_hash,
            AccessLevel::Contributor,
            Some(share_key_hash),
        );

        // Verify that permission is granted
        assert!(result.is_ok());

        // Verify that the share link has been disabled (removed from cache)
        assert!(share_links_cache.get_share_key_data_if_still_valid(&mut event_storage_cache, &file_path, share_key_hash).is_none());

        //Try to use the share link again, it should not work since single use
         let result2 = AccessLevel::require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            "another_user",
            AccessLevel::Contributor,
            Some(share_key_hash),
        );

        assert!(result2.is_err());
    }

    #[test]
    fn test_require_permission_share_link_does_not_increase_access() {
        let (mut event_storage_cache, temp_dir) = setup_cache(5);
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let user_hash = "user1";
        let share_key_hash = "share1";

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            si: 0,
            cb: None,
            sd: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Set user's access level to Contributor
        user_access_cache.update_access_for_user(&mut event_storage_cache, &file_path, "admin", user_hash, AccessLevel::Contributor, false, None, None).unwrap();

        // Create a share link with Viewer access (lower than current)
        share_links_cache.create_share_link(
            &mut event_storage_cache,
            file_path.clone(),
            "admin".to_string(),
            share_key_hash.to_string(),
            AccessLevel::Viewer,
            false,
            None,
            None,
            0
        ).unwrap();

        // Require Contributor access, providing the share link
        let result = AccessLevel::require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            user_hash,
            AccessLevel::Contributor,
            Some(share_key_hash),
        );

        // Verify that permission is granted (since user already has Contributor access)
        assert!(result.is_ok());

        // Verify that the user's access level remains unchanged
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, user_hash);
        assert_eq!(access_level, AccessLevel::Contributor);
    }
}