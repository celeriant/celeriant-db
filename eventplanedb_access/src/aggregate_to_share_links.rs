use std::collections::HashMap;

use crate::access_level::AccessLevel;

pub struct ShareLinkAccessInfo {
    pub access_level: AccessLevel,
    pub share_key: String,
    pub is_single_use: bool,
    pub created_by: String,
    pub expires_on: u64,
}

impl ShareLinkAccessInfo {
    pub fn new(
        access_level: AccessLevel,
        share_key: String,
        is_single_use: bool,
        created_by: String,
        expires_on: u64
    ) -> Self {
        Self {
            access_level,
            share_key,
            is_single_use,
            created_by,
            expires_on
        }
    }
}

pub struct AggregateToShareLinks {
    share_links: HashMap<String, ShareLinkAccessInfo>,
}

impl AggregateToShareLinks {
    pub fn new() -> Self {
        Self {
            share_links: HashMap::new(),
        }
    }

    pub fn add_share_link(&mut self, share_hash: String, share_link_info: ShareLinkAccessInfo) {
        self.share_links.insert(share_hash, share_link_info);
    }

    pub fn get_share_link(&self, share_hash: &str) -> Option<&ShareLinkAccessInfo> {
        self.share_links.get(share_hash)
    }

    pub fn remove_share_link(&mut self, share_hash: &str) {
        self.share_links.remove(share_hash);
    }

    pub fn count(&self) -> usize {
        self.share_links.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_empty_share_links() {
        let aggregate_share_links = AggregateToShareLinks::new();
        assert_eq!(aggregate_share_links.count(), 0);
    }

    #[test]
    fn test_add_and_get_share_link() {
        let mut aggregate_share_links = AggregateToShareLinks::new();
        let share_hash = "share123".to_string();
        let share_link_info = ShareLinkAccessInfo {
            access_level: AccessLevel::Viewer,
            share_key: "key123".to_string(),
            is_single_use: false,
            created_by: "user123".to_string(),
            expires_on: 0
        };

        aggregate_share_links.add_share_link(share_hash.clone(), share_link_info);
        assert_eq!(aggregate_share_links.count(), 1);

        let retrieved_share_link = aggregate_share_links.get_share_link(&share_hash).unwrap();
        assert_eq!(retrieved_share_link.access_level, AccessLevel::Viewer);
        assert_eq!(retrieved_share_link.share_key, "key123");
        assert_eq!(retrieved_share_link.is_single_use, false);
        assert_eq!(retrieved_share_link.created_by, "user123");
    }

    #[test]
    fn test_remove_share_link() {
        let mut aggregate_share_links = AggregateToShareLinks::new();
        let share_hash = "share123".to_string();
        let share_link_info = ShareLinkAccessInfo {
            access_level: AccessLevel::Viewer,
            share_key: "key123".to_string(),
            is_single_use: false,
            created_by: "user123".to_string(),
            expires_on: 0
        };

        aggregate_share_links.add_share_link(share_hash.clone(), share_link_info);
        assert_eq!(aggregate_share_links.count(), 1);

        aggregate_share_links.remove_share_link(&share_hash);
        assert_eq!(aggregate_share_links.count(), 0);
        assert!(aggregate_share_links.get_share_link(&share_hash).is_none());
    }

    #[test]
    fn test_get_nonexistent_share_link() {
        let aggregate_share_links = AggregateToShareLinks::new();
        assert!(aggregate_share_links.get_share_link("nonexistent").is_none());
    }

    #[test]
    fn test_multiple_share_links() {
        let mut aggregate_share_links = AggregateToShareLinks::new();

        let share_link_info1 = ShareLinkAccessInfo {
            access_level: AccessLevel::Viewer,
            share_key: "key1".to_string(),
            is_single_use: false,
            created_by: "user1".to_string(),
            expires_on: 0
        };
        let share_link_info2 = ShareLinkAccessInfo {
            access_level: AccessLevel::Contributor,
            share_key: "key2".to_string(),
            is_single_use: true,
            created_by: "user2".to_string(),
            expires_on: 0
        };

        aggregate_share_links.add_share_link("share1".to_string(), share_link_info1);
        aggregate_share_links.add_share_link("share2".to_string(), share_link_info2);

        assert_eq!(aggregate_share_links.count(), 2);
        assert_eq!(aggregate_share_links.get_share_link("share1").unwrap().access_level, AccessLevel::Viewer);
        assert_eq!(aggregate_share_links.get_share_link("share2").unwrap().access_level, AccessLevel::Contributor);
    }
}