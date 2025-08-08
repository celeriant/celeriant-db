use eventplanedb_storage::{event_batch_item::EventBatchItem, event_storage_cache::EventStorageCache};

use crate::{access_level::AccessLevel, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::UserAccessCache};

pub fn require_permission(
    event_storage_cache: &mut EventStorageCache,
    share_links_cache: &mut ShareLinksCache,
    user_access_cache: &mut UserAccessCache,
    file_path: &str,
    client_id: &u128,
    user_id: Option<&str>,
    org_id: Option<&str>,
    server_time: u64,
    required_access_level: AccessLevel,
    share_id: Option<&u128>,
) -> Result<Vec<EventBatchItem>, JobError> {
    let mut new_events = Vec::new();

    let client_id_access_level = user_access_cache.get_current_access_level(event_storage_cache, file_path, Some(client_id), None);
    let user_id_access_level = user_access_cache.get_current_access_level(event_storage_cache, file_path, Some(client_id), user_id);
    let current_access_level = AccessLevel::greatest_access_level(client_id_access_level, user_id_access_level);

    let mut final_access_level = current_access_level;
    let mut share_link_used: Option<u128> = None;

    // Check if there's a share link that can increase access level
    if let Some(share_id) = share_id {
        if let Some(share_key_info) = share_links_cache.get_share_key_data_if_still_valid(event_storage_cache, file_path, share_id) {
            // Will the share link give the user more access?
            if AccessLevel::increases_access_level(current_access_level, share_key_info.access_level)
                && AccessLevel::meets_required_access_level(share_key_info.access_level, required_access_level)
            {
                final_access_level = share_key_info.access_level;
                share_link_used = Some(*share_id);

                // Disable the share link if it is single use
                if share_key_info.is_single_use {
                    let disable_event_item =
                        share_links_cache.disable_share_link(event_storage_cache, file_path, client_id, user_id, *share_id, server_time)?;
                    new_events.push(disable_event_item);
                }
            }
        }
    }

    // Check if final access level meets requirements
    if !AccessLevel::meets_required_access_level(final_access_level, required_access_level) {
        return Err(JobError::PermissionDenied("User does not have permission to perform this action".to_string()));
    }

    let requires_transition_to_user_id = user_id.is_some() && client_id_access_level != AccessLevel::None;
    let share_link_increased_access_level = final_access_level != current_access_level;

    if requires_transition_to_user_id {
        let disable_client_id_event = user_access_cache.update_access_for_user(
            event_storage_cache,
            file_path,
            client_id,
            user_id,
            Some(client_id),
            None,
            None,
            AccessLevel::None,
            true,
            None,
            server_time,
        )?;
        new_events.push(disable_client_id_event.unwrap());
    }

    if requires_transition_to_user_id || share_link_increased_access_level {
        let provide_access_event = user_access_cache.update_access_for_user(
            event_storage_cache,
            file_path,
            client_id,
            user_id,
            Some(client_id),
            user_id,
            org_id,
            final_access_level,
            false,
            share_link_used,
            server_time,
        )?;
        if let Some(provide_access_event) = provide_access_event {
            new_events.push(provide_access_event);
        }
    }

    Ok(new_events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem, event_storage_cache::EventStorageCache};
    use tempfile::TempDir;

    use crate::share_links_cache::ShareLinksCache;
    use crate::user_access_cache::UserAccessCache;

    // Helper function to create a basic EventStorageCache for testing
    fn setup_cache() -> (EventStorageCache, TempDir) {
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

        event1.event_date = 443;
        event1.event_type = 4;
        event1.int_values = Some(vec![1, 2, 3]);

        event1
    }

    #[test]
    fn test_require_permission_multiple_clients_same_user_id() {
        let (mut event_storage_cache, temp_dir) = setup_cache();
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let client_id1 = 12345u128;
        let client_id2 = 67890u128;
        let user_id = "user1";

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            server_id: 0,
            client_id: 0,
            user_id: None,
            server_date: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Set client_id1 access level to Contributor
        user_access_cache
            .update_access_for_user(
                &mut event_storage_cache,
                &file_path,
                &999u128,
                Some("admin"),
                Some(&client_id1),
                None,
                None,
                AccessLevel::Contributor,
                false,
                None,
                654,
            )
            .unwrap();

        // Set client_id2 access level to Contributor
        user_access_cache
            .update_access_for_user(
                &mut event_storage_cache,
                &file_path,
                &999u128,
                Some("admin"),
                Some(&client_id2),
                None,
                None,
                AccessLevel::Contributor,
                false,
                None,
                655,
            )
            .unwrap();

        // First, user logs in on client_id1 and transitions the permission
        let server_time = 1000;
        let result1 = require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            &client_id1,
            Some(user_id),
            None,
            server_time,
            AccessLevel::Viewer,
            None,
        );

        assert!(result1.is_ok());

        // Now user logs in on client_id2 - this should handle the case where the user already has access
        // The current implementation would panic here because provide_access_event returns None
        // (since user already has Contributor access from client_id1)
        let server_time = 1001;
        let result2 = require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            &client_id2,
            Some(user_id),
            None,
            server_time,
            AccessLevel::Viewer,
            None,
        );

        // This should not panic and should successfully grant permission
        assert!(result2.is_ok());

        // Verify client_id2 access is now None
        let client2_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, Some(&client_id2), None);
        assert_eq!(client2_access, AccessLevel::None);

        // User access should still be Contributor
        let user_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, Some(&client_id2), Some(user_id));
        assert_eq!(user_access, AccessLevel::Contributor);
    }

    #[test]
    fn test_require_permission_user_has_sufficient_access() {
        let (mut event_storage_cache, temp_dir) = setup_cache();
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let client_id = 12345u128;
        let user_id = "user1";

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            server_id: 0,
            client_id: 0,
            user_id: None,
            server_date: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Set user's access level to Contributor
        user_access_cache
            .update_access_for_user(
                &mut event_storage_cache,
                &file_path,
                &999u128,
                Some("admin"),
                Some(&client_id),
                Some(user_id),
                None,
                AccessLevel::Contributor,
                false,
                None,
                645,
            )
            .unwrap();

        let server_time = 1000;

        // Require Viewer access (which Contributor meets)
        let result = require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            &client_id,
            Some(user_id),
            None,
            server_time,
            AccessLevel::Viewer,
            None,
        );

        // Verify that permission is granted
        assert!(result.is_ok());
        let events = result.unwrap();
        assert!(events.is_empty()); // No new events needed
    }

    #[test]
    fn test_require_permission_user_has_insufficient_access() {
        let (mut event_storage_cache, temp_dir) = setup_cache();
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let client_id = 12345u128;
        let user_id = "user1";

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            server_id: 0,
            client_id: 0,
            user_id: None,
            server_date: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Set user's access level to Viewer
        user_access_cache
            .update_access_for_user(
                &mut event_storage_cache,
                &file_path,
                &999u128,
                Some("admin"),
                Some(&client_id),
                Some(user_id),
                None,
                AccessLevel::Viewer,
                false,
                None,
                654,
            )
            .unwrap();

        let server_time = 1000;
        // Require Contributor access (which Viewer does not meet)
        let result = require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            &client_id,
            Some(user_id),
            None,
            server_time,
            AccessLevel::Contributor,
            None,
        );

        // Verify that permission is denied
        assert!(result.is_err());
        match result {
            Err(JobError::PermissionDenied(_)) => (),
            _ => panic!("Expected PermissionDenied error"),
        }
    }

    #[test]
    fn test_require_permission_user_has_sufficient_access_with_share_link() {
        let (mut event_storage_cache, temp_dir) = setup_cache();
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let client_id = 12345u128;
        let user_id = "user1";
        let share_id = 67890u128;

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            server_id: 0,
            client_id: 0,
            user_id: None,
            server_date: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Create a share link with Contributor access
        share_links_cache
            .create_share_link(
                &mut event_storage_cache,
                &file_path,
                &999u128,
                Some("admin"),
                share_id,
                AccessLevel::Contributor,
                false,
                None,
                None,
                0,
                654,
            )
            .unwrap();

        let server_time = 1000;
        // Require Contributor access, providing the share link
        let result = require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            &client_id,
            Some(user_id),
            None,
            server_time,
            AccessLevel::Contributor,
            Some(&share_id),
        );

        // Verify that permission is granted
        assert!(result.is_ok());
        let events = result.unwrap();
        assert!(!events.is_empty()); // Should have access update event

        // Verify that the user's access level has been updated
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, Some(&client_id), Some(user_id));
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    #[test]
    fn test_require_permission_user_has_insufficient_access_despite_share_link() {
        let (mut event_storage_cache, temp_dir) = setup_cache();
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let client_id = 12345u128;
        let user_id = "user1";
        let share_id = 67890u128;

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            server_id: 0,
            client_id: 0,
            user_id: None,
            server_date: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Set user's access level to Viewer
        user_access_cache
            .update_access_for_user(
                &mut event_storage_cache,
                &file_path,
                &999u128,
                Some("admin"),
                Some(&client_id),
                Some(user_id),
                None,
                AccessLevel::Viewer,
                false,
                None,
                654,
            )
            .unwrap();

        // Create a share link with Contributor access
        share_links_cache
            .create_share_link(
                &mut event_storage_cache,
                &file_path,
                &999u128,
                Some("admin"),
                share_id,
                AccessLevel::Contributor,
                false,
                None,
                None,
                0,
                654,
            )
            .unwrap();

        let server_time = 1000;
        // Require Owner access, providing the share link (Contributor is not enough)
        let result = require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            &client_id,
            Some(user_id),
            None,
            server_time,
            AccessLevel::Owner,
            Some(&share_id),
        );

        // Verify that permission is denied
        assert!(result.is_err());
        match result {
            Err(JobError::PermissionDenied(_)) => (),
            _ => panic!("Expected PermissionDenied error"),
        }

        // Verify that the user's access level has NOT been updated to the share link level
        // We don't want to needlessly 'use' a share link if access is not enough anyway
        // And also we return 403 so client can't process the events generated if we were to use it
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, Some(&client_id), Some(user_id));
        assert_eq!(access_level, AccessLevel::Viewer);
    }

    #[test]
    fn test_require_permission_share_link_is_single_use_and_disabled() {
        let (mut event_storage_cache, temp_dir) = setup_cache();
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let client_id = 12345u128;
        let user_id = "user1";
        let share_id = 67890u128;

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            server_id: 0,
            client_id: 0,
            user_id: None,
            server_date: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Create a single-use share link with Contributor access
        share_links_cache
            .create_share_link(
                &mut event_storage_cache,
                &file_path,
                &999u128,
                Some("admin"),
                share_id,
                AccessLevel::Contributor,
                true,
                None,
                None,
                0,
                654,
            )
            .unwrap();

        let server_time = 1000;
        // Require Contributor access, providing the share link
        let result = require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            &client_id,
            Some(user_id),
            None,
            server_time,
            AccessLevel::Contributor,
            Some(&share_id),
        );

        // Verify that permission is granted
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 2); // Should have disable event and access update event

        // Verify that the share link has been disabled (removed from cache)
        assert!(
            share_links_cache
                .get_share_key_data_if_still_valid(&mut event_storage_cache, &file_path, &share_id)
                .is_none()
        );

        //Try to use the share link again, it should not work since single use
        let client_id2 = 54321u128;
        let result2 = require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            &client_id2,
            Some("another_user"),
            None,
            server_time,
            AccessLevel::Contributor,
            Some(&share_id),
        );

        assert!(result2.is_err());
        match result2 {
            Err(JobError::PermissionDenied(_)) => (),
            _ => panic!("Expected PermissionDenied error"),
        }
    }

    #[test]
    fn test_require_permission_share_link_does_not_increase_access() {
        let (mut event_storage_cache, temp_dir) = setup_cache();
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let client_id = 12345u128;
        let user_id = "user1";
        let share_id = 67890u128;

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            server_id: 0,
            client_id: 0,
            user_id: None,
            server_date: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Set user's access level to Contributor
        user_access_cache
            .update_access_for_user(
                &mut event_storage_cache,
                &file_path,
                &999u128,
                Some("admin"),
                Some(&client_id),
                Some(user_id),
                None,
                AccessLevel::Contributor,
                false,
                None,
                654,
            )
            .unwrap();

        // Create a share link with Viewer access (lower than current)
        share_links_cache
            .create_share_link(
                &mut event_storage_cache,
                &file_path,
                &999u128,
                Some("admin"),
                share_id,
                AccessLevel::Viewer,
                false,
                None,
                None,
                0,
                654,
            )
            .unwrap();

        let server_time = 1000;
        // Require Contributor access, providing the share link
        let result = require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            &client_id,
            Some(user_id),
            None,
            server_time,
            AccessLevel::Contributor,
            Some(&share_id),
        );

        // Verify that permission is granted (since user already has Contributor access)
        assert!(result.is_ok());
        let events = result.unwrap();
        assert!(events.is_empty()); // No events should be generated since no access change

        // Verify that the user's access level remains unchanged
        let access_level = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, Some(&client_id), Some(user_id));
        assert_eq!(access_level, AccessLevel::Contributor);
    }

    #[test]
    fn test_require_permission_access_transferred_from_client_id_to_user_id() {
        let (mut event_storage_cache, temp_dir) = setup_cache();
        let file_path = create_file_path(&temp_dir, "project1.bin");
        let client_id = 12345u128;
        let user_id = "user1";

        let first_event = create_test_event_item();
        let first_batch = EventBatchItem {
            server_id: 0,
            client_id: 0,
            user_id: None,
            server_date: 0,
            events: vec![first_event],
        };
        event_storage_cache.write(&file_path, true, true, first_batch).unwrap();

        let mut share_links_cache = ShareLinksCache::new(5);
        let mut user_access_cache = UserAccessCache::new(5);

        // Set client_id access level to Contributor (simulating PKI-based access)
        user_access_cache
            .update_access_for_user(
                &mut event_storage_cache,
                &file_path,
                &999u128,
                Some("admin"),
                Some(&client_id),
                None, // No user_id initially (PKI-only access)
                None,
                AccessLevel::Contributor,
                false,
                None,
                654,
            )
            .unwrap();

        // Verify client has Contributor access but user has None
        let client_access = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, Some(&client_id), None);
        let user_access_before = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, Some(&client_id), Some(user_id));
        assert_eq!(client_access, AccessLevel::Contributor);
        assert_eq!(user_access_before, AccessLevel::None);

        let server_time = 1000;

        // User logs in with OAuth2 and requires Viewer access
        // This should trigger access transfer from client_id to user_id
        let result = require_permission(
            &mut event_storage_cache,
            &mut share_links_cache,
            &mut user_access_cache,
            &file_path,
            &client_id,
            Some(user_id), // Now providing user_id (OAuth2 login)
            None,
            server_time,
            AccessLevel::Viewer,
            None,
        );

        // Verify that permission is granted
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 2); // Should have access update event and disable client_id event

        // Verify that the user now has the client's access level
        let user_access_after = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, Some(&client_id), Some(user_id));
        assert_eq!(user_access_after, AccessLevel::Contributor);

        // Verify the client no longer has access
        let client_access_after = user_access_cache.get_current_access_level(&mut event_storage_cache, &file_path, Some(&client_id), None);
        assert_eq!(client_access_after, AccessLevel::None);
    }
}
