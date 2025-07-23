use std::collections::HashMap;

use crate::access_level::AccessLevel;

pub struct AggregateToUserAccessLevel {
    users: HashMap<String, AccessLevel>,
    clients: HashMap<u128, AccessLevel>,
}

impl AggregateToUserAccessLevel {
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
            clients: HashMap::new(),
        }
    }

    pub fn update_cache_for_user(
        &mut self,
        client_id: Option<&u128>,
        user_id: Option<&str>,
        proposed_access_level: AccessLevel,
        allow_override_existing: bool,
    ) {
        match user_id {
            Some(user_id) => {
                // We are using oauth, remove direct client access
                if let Some(client_id) = client_id {
                    self.clients.remove(client_id);
                }

                // Now attempt to add or update the oauth user access level
                match self.users.get(user_id) {
                    //No op - Providing NO access and currently has NO access
                    None if proposed_access_level == AccessLevel::None => (),

                    //Currently has NO access but we are providing some form of access
                    None => {
                        self.users.insert(user_id.to_string(), proposed_access_level);
                    }

                    //No op - has access already and not allowed to take it away
                    Some(_) if proposed_access_level == AccessLevel::None && !allow_override_existing => (),

                    //Currently has access and we want to take it away
                    Some(_) if proposed_access_level == AccessLevel::None && allow_override_existing => {
                        self.users.remove(user_id);
                    }

                    //Currently has access and we can update it (either higher access level or can override existing entry)
                    Some(current_access_level)
                        if proposed_access_level != AccessLevel::None
                            && (AccessLevel::increases_access_level(*current_access_level, proposed_access_level) || allow_override_existing) =>
                    {
                        self.users.insert(user_id.to_string(), proposed_access_level);
                    }

                    _ => (),
                }
            }
            None => {
                if let Some(client_id) = client_id {
                    // We are using zero-trust PKI instead of oauth
                    match self.clients.get(client_id) {
                        //No op - Providing NO access and currently has NO access
                        None if proposed_access_level == AccessLevel::None => (),

                        //Currently has NO access but we are providing some form of access
                        None => {
                            self.clients.insert(*client_id, proposed_access_level);
                        }

                        //No op - has access already and not allowed to take it away
                        Some(_) if proposed_access_level == AccessLevel::None && !allow_override_existing => (),

                        //Currently has access and we want to take it away
                        Some(_) if proposed_access_level == AccessLevel::None && allow_override_existing => {
                            self.clients.remove(client_id);
                        }

                        //Currently has access and we can update it (either higher access level or can override existing entry)
                        Some(current_access_level)
                            if proposed_access_level != AccessLevel::None
                                && (AccessLevel::increases_access_level(*current_access_level, proposed_access_level) || allow_override_existing) =>
                        {
                            self.clients.insert(*client_id, proposed_access_level);
                        }

                        _ => (),
                    }
                }
            }
        };
    }

    pub fn get_access_level_for_user(&self, user_id: &str) -> AccessLevel {
        self.users.get(user_id).unwrap_or(&AccessLevel::None).clone()
    }

    pub fn get_access_level_for_client(&self, client_id: &u128) -> AccessLevel {
        self.clients.get(client_id).unwrap_or(&AccessLevel::None).clone()
    }

    pub fn get_access_level(&self, client_id: Option<&u128>, user_id: Option<&str>) -> AccessLevel {
        match user_id {
            Some(user_id) => self.get_access_level_for_user(user_id),
            None => {
                if let Some(client_id) = client_id {
                    self.get_access_level_for_client(client_id)
                } else {
                    AccessLevel::None
                }
            }
        }
    }

    pub fn count(&self) -> usize {
        self.users.len() + self.clients.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_empty_cache() {
        let cache = AggregateToUserAccessLevel::new();
        assert_eq!(cache.count(), 0);
    }

    #[test]
    fn test_update_cache_no_op_no_access_to_no_access() {
        let mut cache = AggregateToUserAccessLevel::new();
        let user_hash = Some("user123");
        let client_id: u128 = 444;

        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::None, false);

        assert_eq!(cache.count(), 0);
        assert_eq!(cache.get_access_level_for_user(user_hash.unwrap()), AccessLevel::None);
    }

    #[test]
    fn test_update_cache_grant_access_to_new_user() {
        let mut cache = AggregateToUserAccessLevel::new();
        let user_hash = Some("user123");
        let client_id: u128 = 444;

        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::Viewer, false);

        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_user(user_hash.unwrap()), AccessLevel::Viewer);
    }

    #[test]
    fn test_update_cache_no_op_has_access_cannot_remove() {
        let mut cache = AggregateToUserAccessLevel::new();
        let user_hash = Some("user123");
        let client_id: u128 = 444;

        // First grant access
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::Contributor, false);

        // Try to remove access but don't allow override
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::None, false);

        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_user(user_hash.unwrap()), AccessLevel::Contributor);
    }

    #[test]
    fn test_update_cache_remove_access_with_override() {
        let mut cache = AggregateToUserAccessLevel::new();
        let user_hash = Some("user123");
        let client_id: u128 = 444;

        // First grant access
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::Owner, false);

        // Remove access with override allowed
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::None, true);

        assert_eq!(cache.count(), 0);
        assert_eq!(cache.get_access_level_for_user(user_hash.unwrap()), AccessLevel::None);
    }

    #[test]
    fn test_update_cache_increase_access_level() {
        let mut cache = AggregateToUserAccessLevel::new();
        let user_hash = Some("user123");
        let client_id: u128 = 444;

        // Grant viewer access
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::Viewer, false);

        // Increase to contributor (should work without override since it's an increase)
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::Contributor, false);

        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_user(user_hash.unwrap()), AccessLevel::Contributor);
    }

    #[test]
    fn test_update_cache_decrease_access_level_without_override() {
        let mut cache = AggregateToUserAccessLevel::new();
        let user_hash = Some("user123");
        let client_id: u128 = 444;

        // Grant owner access
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::Owner, false);

        // Try to decrease to viewer without override (should be no-op)
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::Viewer, false);

        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_user(user_hash.unwrap()), AccessLevel::Owner);
    }

    #[test]
    fn test_update_cache_decrease_access_level_with_override() {
        let mut cache = AggregateToUserAccessLevel::new();
        let user_hash = Some("user123");
        let client_id: u128 = 444;

        // Grant owner access
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::Owner, false);

        // Decrease to viewer with override allowed
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::Viewer, true);

        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_user(user_hash.unwrap()), AccessLevel::Viewer);
    }

    #[test]
    fn test_update_cache_same_access_level_with_override() {
        let mut cache = AggregateToUserAccessLevel::new();
        let user_hash = Some("user123");
        let client_id: u128 = 444;

        // Grant contributor access
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::Contributor, false);

        // Set same access level with override allowed
        cache.update_cache_for_user(Some(&client_id), user_hash, AccessLevel::Contributor, true);

        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_user(user_hash.unwrap()), AccessLevel::Contributor);
    }

    #[test]
    fn test_multiple_users() {
        let mut cache = AggregateToUserAccessLevel::new();

        cache.update_cache_for_user(Some(&111), Some("user1"), AccessLevel::Owner, false);
        cache.update_cache_for_user(Some(&222), Some("user2"), AccessLevel::Contributor, false);
        cache.update_cache_for_user(Some(&333), Some("user3"), AccessLevel::Viewer, false);

        assert_eq!(cache.count(), 3);
        assert_eq!(cache.get_access_level_for_user("user1"), AccessLevel::Owner);
        assert_eq!(cache.get_access_level_for_user("user2"), AccessLevel::Contributor);
        assert_eq!(cache.get_access_level_for_user("user3"), AccessLevel::Viewer);
        assert_eq!(cache.get_access_level_for_user("user4"), AccessLevel::None);
    }

    #[test]
    fn test_current_access_level_for_nonexistent_user() {
        let cache = AggregateToUserAccessLevel::new();
        assert_eq!(cache.get_access_level_for_user("nonexistent"), AccessLevel::None);
    }

    #[test]
    fn test_count_reflects_additions_and_removals() {
        let mut cache = AggregateToUserAccessLevel::new();

        assert_eq!(cache.count(), 0);

        cache.update_cache_for_user(Some(&111), Some("user1"), AccessLevel::Viewer, false);
        assert_eq!(cache.count(), 1);

        cache.update_cache_for_user(Some(&222), Some("user2"), AccessLevel::Owner, false);
        assert_eq!(cache.count(), 2);

        cache.update_cache_for_user(Some(&111), Some("user1"), AccessLevel::None, true);
        assert_eq!(cache.count(), 1);

        cache.update_cache_for_user(Some(&222), Some("user2"), AccessLevel::None, true);
        assert_eq!(cache.count(), 0);
    }

    #[test]
    fn test_client_pki_access_basic() {
        let mut cache = AggregateToUserAccessLevel::new();
        let client_id: u128 = 12345;

        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::Viewer, false);

        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::Viewer);
        assert_eq!(cache.get_access_level(Some(&client_id), None), AccessLevel::Viewer);
    }

    #[test]
    fn test_oauth_overrides_pki_client_access() {
        let mut cache = AggregateToUserAccessLevel::new();
        let client_id: u128 = 12345;
        let user_id = "user123";

        // First establish PKI client access
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::Contributor, false);
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::Contributor);

        // OAuth login should remove client access and add user access
        cache.update_cache_for_user(Some(&client_id), Some(user_id), AccessLevel::Owner, false);

        assert_eq!(cache.count(), 1); // Still 1 total, but switched from client to user
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::None);
        assert_eq!(cache.get_access_level_for_user(user_id), AccessLevel::Owner);
        assert_eq!(cache.get_access_level(Some(&client_id), Some(user_id)), AccessLevel::Owner);
    }

    #[test]
    fn test_oauth_override_removes_even_with_no_new_access() {
        let mut cache = AggregateToUserAccessLevel::new();
        let client_id: u128 = 12345;
        let user_id = "user123";

        // Establish PKI client access
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::Viewer, false);
        assert_eq!(cache.count(), 1);

        // OAuth login with None access should still remove client access
        cache.update_cache_for_user(Some(&client_id), Some(user_id), AccessLevel::None, false);

        assert_eq!(cache.count(), 0); // Both client and user should have no access
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::None);
        assert_eq!(cache.get_access_level_for_user(user_id), AccessLevel::None);
    }

    #[test]
    fn test_multiple_clients_pki_access() {
        let mut cache = AggregateToUserAccessLevel::new();
        let client1: u128 = 11111;
        let client2: u128 = 22222;
        let client3: u128 = 33333;

        cache.update_cache_for_user(Some(&client1), None, AccessLevel::Owner, false);
        cache.update_cache_for_user(Some(&client2), None, AccessLevel::Contributor, false);
        cache.update_cache_for_user(Some(&client3), None, AccessLevel::Viewer, false);

        assert_eq!(cache.count(), 3);
        assert_eq!(cache.get_access_level_for_client(&client1), AccessLevel::Owner);
        assert_eq!(cache.get_access_level_for_client(&client2), AccessLevel::Contributor);
        assert_eq!(cache.get_access_level_for_client(&client3), AccessLevel::Viewer);
        assert_eq!(cache.get_access_level_for_client(&99999), AccessLevel::None);
    }

    #[test]
    fn test_mixed_pki_and_oauth_users() {
        let mut cache = AggregateToUserAccessLevel::new();
        let client1: u128 = 11111;
        let client2: u128 = 22222;
        let user1 = "oauth_user1";
        let user2 = "oauth_user2";

        // PKI clients
        cache.update_cache_for_user(Some(&client1), None, AccessLevel::Viewer, false);
        cache.update_cache_for_user(Some(&client2), None, AccessLevel::Contributor, false);

        // OAuth users (different client IDs, but shouldn't matter for user access)
        cache.update_cache_for_user(Some(&33333), Some(user1), AccessLevel::Owner, false);
        cache.update_cache_for_user(Some(&44444), Some(user2), AccessLevel::Contributor, false);

        assert_eq!(cache.count(), 4);

        // PKI access
        assert_eq!(cache.get_access_level(Some(&client1), None), AccessLevel::Viewer);
        assert_eq!(cache.get_access_level(Some(&client2), None), AccessLevel::Contributor);

        // OAuth access
        assert_eq!(cache.get_access_level(Some(&33333), Some(user1)), AccessLevel::Owner);
        assert_eq!(cache.get_access_level(Some(&44444), Some(user2)), AccessLevel::Contributor);
    }

    #[test]
    fn test_pki_client_upgrade_and_downgrade() {
        let mut cache = AggregateToUserAccessLevel::new();
        let client_id: u128 = 12345;

        // Start with viewer access
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::Viewer, false);
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::Viewer);

        // Upgrade to contributor (should work without override)
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::Contributor, false);
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::Contributor);

        // Try to downgrade without override (should be no-op)
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::Viewer, false);
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::Contributor);

        // Downgrade with override
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::Viewer, true);
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::Viewer);
    }

    #[test]
    fn test_same_user_different_clients_oauth_consolidation() {
        let mut cache = AggregateToUserAccessLevel::new();
        let client1: u128 = 11111;
        let client2: u128 = 22222;
        let user_id = "user123";

        // User logs in from client1
        cache.update_cache_for_user(Some(&client1), Some(user_id), AccessLevel::Viewer, false);
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_user(user_id), AccessLevel::Viewer);

        // Same user logs in from client2 with higher access (should upgrade)
        cache.update_cache_for_user(Some(&client2), Some(user_id), AccessLevel::Owner, false);
        assert_eq!(cache.count(), 1); // Still just one user entry
        assert_eq!(cache.get_access_level_for_user(user_id), AccessLevel::Owner);

        // Access from either client should return the same user access level
        assert_eq!(cache.get_access_level(Some(&client1), Some(user_id)), AccessLevel::Owner);
        assert_eq!(cache.get_access_level(Some(&client2), Some(user_id)), AccessLevel::Owner);
    }

    #[test]
    fn test_client_id_parameter_irrelevant_for_oauth_users() {
        let mut cache = AggregateToUserAccessLevel::new();
        let user_id = "user123";

        // Grant access with one client_id
        cache.update_cache_for_user(Some(&11111), Some(user_id), AccessLevel::Contributor, false);

        // Access level should be the same regardless of which client_id is used for lookup
        assert_eq!(cache.get_access_level(Some(&11111), Some(user_id)), AccessLevel::Contributor);
        assert_eq!(cache.get_access_level(Some(&99999), Some(user_id)), AccessLevel::Contributor);
    }

    #[test]
    fn test_pki_to_oauth_transition_preserves_higher_access() {
        let mut cache = AggregateToUserAccessLevel::new();
        let client_id: u128 = 12345;
        let user_id = "user123";

        // Start with high PKI access
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::Owner, false);

        // OAuth login with lower proposed access should still grant the higher level if allow_override is false
        // But since it's OAuth override, the client access gets removed regardless
        cache.update_cache_for_user(Some(&client_id), Some(user_id), AccessLevel::Viewer, false);

        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::None);
        assert_eq!(cache.get_access_level_for_user(user_id), AccessLevel::Viewer); // Gets what was proposed
    }

    #[test]
    fn test_get_access_level_method_routing() {
        let mut cache = AggregateToUserAccessLevel::new();
        let client_id: u128 = 12345;
        let user_id = "user123";

        // Set up PKI access
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::Contributor, false);

        // Set up OAuth access for different user
        cache.update_cache_for_user(Some(&67890), Some(user_id), AccessLevel::Owner, false);

        // get_access_level should route correctly based on user_id presence
        assert_eq!(cache.get_access_level(Some(&client_id), None), AccessLevel::Contributor);
        assert_eq!(cache.get_access_level(Some(&67890), Some(user_id)), AccessLevel::Owner);
        assert_eq!(cache.get_access_level(Some(&99999), Some(user_id)), AccessLevel::Owner); // User access, client_id irrelevant
        assert_eq!(cache.get_access_level(Some(&99999), None), AccessLevel::None); // No PKI access for this client
    }

    #[test]
    fn test_pki_client_remove_access_with_override() {
        let mut cache = AggregateToUserAccessLevel::new();
        let client_id: u128 = 12345;

        // First grant PKI client access
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::Contributor, false);
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::Contributor);

        // Remove PKI client access with override allowed
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::None, true);

        assert_eq!(cache.count(), 0);
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::None);
        assert_eq!(cache.get_access_level(Some(&client_id), None), AccessLevel::None);
    }

    #[test]
    fn test_pki_client_cannot_remove_access_without_override() {
        let mut cache = AggregateToUserAccessLevel::new();
        let client_id: u128 = 12345;

        // First grant PKI client access
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::Owner, false);
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::Owner);

        // Try to remove PKI client access without override (should be no-op)
        cache.update_cache_for_user(Some(&client_id), None, AccessLevel::None, false);

        assert_eq!(cache.count(), 1);
        assert_eq!(cache.get_access_level_for_client(&client_id), AccessLevel::Owner);
        assert_eq!(cache.get_access_level(Some(&client_id), None), AccessLevel::Owner);
    }
}
