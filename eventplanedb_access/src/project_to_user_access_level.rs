use std::collections::HashMap;

use crate::access_level::AccessLevel;

pub struct ProjectToUserAccessLevel {
    users: HashMap<String, AccessLevel>,
}

impl ProjectToUserAccessLevel {
    pub fn new() -> Self {
        Self {
            users: HashMap::new()
        }
    }

    pub fn update_cache_for_user(&mut self, current_user_hash: &str, proposed_access_level: AccessLevel, allow_override_existing: bool) {
        match self.users.get(current_user_hash) {

            //No op - Providing NO access and currently has NO access
            None if proposed_access_level == AccessLevel::None => (),

            //Currently has NO access but we are providing some form of access
            None => { 
                self.users.insert(current_user_hash.to_string(), proposed_access_level); 
            },

            //No op - has access already and not allowed to take it away
            Some(_) if proposed_access_level == AccessLevel::None && !allow_override_existing => (),

            //Currently has access and we want to take it away
            Some(_) if proposed_access_level == AccessLevel::None && allow_override_existing => {
                self.users.remove(current_user_hash);
            },

            //Currently has access and we can update it (either higher access level or can override existing entry)
            Some(current_access_level) if proposed_access_level != AccessLevel::None && 
                (AccessLevel::increases_access_level(*current_access_level, proposed_access_level) || allow_override_existing) => {

                self.users.insert(current_user_hash.to_string(), proposed_access_level);
            }

            _ => (),
        }
    }

    pub fn current_access_level_for_user(&self, current_user_hash: &str) -> AccessLevel {
        self.users.get(current_user_hash).unwrap_or(&AccessLevel::None).clone()
    }

    pub fn count(&self) -> usize {
        self.users.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_empty_cache() {
        let cache = ProjectToUserAccessLevel::new();
        assert_eq!(cache.count(), 0);
    }

    #[test]
    fn test_update_cache_no_op_no_access_to_no_access() {
        let mut cache = ProjectToUserAccessLevel::new();
        let user_hash = "user123";
        
        cache.update_cache_for_user(user_hash, AccessLevel::None, false);
        
        assert_eq!(cache.count(), 0);
        assert_eq!(cache.current_access_level_for_user(user_hash), AccessLevel::None);
    }

    #[test]
    fn test_update_cache_grant_access_to_new_user() {
        let mut cache = ProjectToUserAccessLevel::new();
        let user_hash = "user123";
        
        cache.update_cache_for_user(user_hash, AccessLevel::Viewer, false);
        
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.current_access_level_for_user(user_hash), AccessLevel::Viewer);
    }

    #[test]
    fn test_update_cache_no_op_has_access_cannot_remove() {
        let mut cache = ProjectToUserAccessLevel::new();
        let user_hash = "user123";
        
        // First grant access
        cache.update_cache_for_user(user_hash, AccessLevel::Contributor, false);
        
        // Try to remove access but don't allow override
        cache.update_cache_for_user(user_hash, AccessLevel::None, false);
        
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.current_access_level_for_user(user_hash), AccessLevel::Contributor);
    }

    #[test]
    fn test_update_cache_remove_access_with_override() {
        let mut cache = ProjectToUserAccessLevel::new();
        let user_hash = "user123";
        
        // First grant access
        cache.update_cache_for_user(user_hash, AccessLevel::Owner, false);
        
        // Remove access with override allowed
        cache.update_cache_for_user(user_hash, AccessLevel::None, true);
        
        assert_eq!(cache.count(), 0);
        assert_eq!(cache.current_access_level_for_user(user_hash), AccessLevel::None);
    }

    #[test]
    fn test_update_cache_increase_access_level() {
        let mut cache = ProjectToUserAccessLevel::new();
        let user_hash = "user123";
        
        // Grant viewer access
        cache.update_cache_for_user(user_hash, AccessLevel::Viewer, false);
        
        // Increase to contributor (should work without override since it's an increase)
        cache.update_cache_for_user(user_hash, AccessLevel::Contributor, false);
        
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.current_access_level_for_user(user_hash), AccessLevel::Contributor);
    }

    #[test]
    fn test_update_cache_decrease_access_level_without_override() {
        let mut cache = ProjectToUserAccessLevel::new();
        let user_hash = "user123";
        
        // Grant owner access
        cache.update_cache_for_user(user_hash, AccessLevel::Owner, false);
        
        // Try to decrease to viewer without override (should be no-op)
        cache.update_cache_for_user(user_hash, AccessLevel::Viewer, false);
        
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.current_access_level_for_user(user_hash), AccessLevel::Owner);
    }

    #[test]
    fn test_update_cache_decrease_access_level_with_override() {
        let mut cache = ProjectToUserAccessLevel::new();
        let user_hash = "user123";
        
        // Grant owner access
        cache.update_cache_for_user(user_hash, AccessLevel::Owner, false);
        
        // Decrease to viewer with override allowed
        cache.update_cache_for_user(user_hash, AccessLevel::Viewer, true);
        
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.current_access_level_for_user(user_hash), AccessLevel::Viewer);
    }

    #[test]
    fn test_update_cache_same_access_level_with_override() {
        let mut cache = ProjectToUserAccessLevel::new();
        let user_hash = "user123";
        
        // Grant contributor access
        cache.update_cache_for_user(user_hash, AccessLevel::Contributor, false);
        
        // Set same access level with override allowed
        cache.update_cache_for_user(user_hash, AccessLevel::Contributor, true);
        
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.current_access_level_for_user(user_hash), AccessLevel::Contributor);
    }

    #[test]
    fn test_multiple_users() {
        let mut cache = ProjectToUserAccessLevel::new();
        
        cache.update_cache_for_user("user1", AccessLevel::Owner, false);
        cache.update_cache_for_user("user2", AccessLevel::Contributor, false);
        cache.update_cache_for_user("user3", AccessLevel::Viewer, false);
        
        assert_eq!(cache.count(), 3);
        assert_eq!(cache.current_access_level_for_user("user1"), AccessLevel::Owner);
        assert_eq!(cache.current_access_level_for_user("user2"), AccessLevel::Contributor);
        assert_eq!(cache.current_access_level_for_user("user3"), AccessLevel::Viewer);
        assert_eq!(cache.current_access_level_for_user("user4"), AccessLevel::None);
    }

    #[test]
    fn test_current_access_level_for_nonexistent_user() {
        let cache = ProjectToUserAccessLevel::new();
        assert_eq!(cache.current_access_level_for_user("nonexistent"), AccessLevel::None);
    }

    #[test]
    fn test_count_reflects_additions_and_removals() {
        let mut cache = ProjectToUserAccessLevel::new();
        
        assert_eq!(cache.count(), 0);
        
        cache.update_cache_for_user("user1", AccessLevel::Viewer, false);
        assert_eq!(cache.count(), 1);
        
        cache.update_cache_for_user("user2", AccessLevel::Owner, false);
        assert_eq!(cache.count(), 2);
        
        cache.update_cache_for_user("user1", AccessLevel::None, true);
        assert_eq!(cache.count(), 1);
        
        cache.update_cache_for_user("user2", AccessLevel::None, true);
        assert_eq!(cache.count(), 0);
    }
}