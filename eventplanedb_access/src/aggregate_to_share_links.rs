use std::collections::HashMap;

use crate::access_level::AccessLevel;

pub struct ShareLinkAccessInfo {
    pub access_level: AccessLevel,
    pub share_id: u128,
    pub is_single_use: bool,
    pub expires_on: u64,
}

impl ShareLinkAccessInfo {
    pub fn new(
        access_level: AccessLevel,
        share_id: u128,
        is_single_use: bool,
        expires_on: u64
    ) -> Self {
        Self {
            access_level,
            share_id,
            is_single_use,
            expires_on
        }
    }
}

pub struct AggregateToShareLinks {
    share_links: HashMap<u128, ShareLinkAccessInfo>,
}

impl AggregateToShareLinks {
    pub fn new() -> Self {
        Self {
            share_links: HashMap::new(),
        }
    }

    pub fn add_share_link(&mut self, share_id: u128, share_link_info: ShareLinkAccessInfo) {
        self.share_links.insert(share_id, share_link_info);
    }

    pub fn get_share_link(&self, share_id: &u128) -> Option<&ShareLinkAccessInfo> {
        self.share_links.get(share_id)
    }

    pub fn remove_share_link(&mut self, share_id: &u128) {
        self.share_links.remove(share_id);
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
        let share_id = 123;
        let share_link_info = ShareLinkAccessInfo {
            access_level: AccessLevel::Viewer,
            share_id,
            is_single_use: false,
            expires_on: 0
        };

        aggregate_share_links.add_share_link(share_id, share_link_info);
        assert_eq!(aggregate_share_links.count(), 1);

        let retrieved_share_link = aggregate_share_links.get_share_link(&share_id).unwrap();
        assert_eq!(retrieved_share_link.access_level, AccessLevel::Viewer);
        assert_eq!(retrieved_share_link.share_id, 123);
        assert_eq!(retrieved_share_link.is_single_use, false);
    }

    #[test]
    fn test_remove_share_link() {
        let mut aggregate_share_links = AggregateToShareLinks::new();
        let share_id = 123;
        let share_link_info = ShareLinkAccessInfo {
            access_level: AccessLevel::Viewer,
            share_id: 123,
            is_single_use: false,
            expires_on: 0
        };

        aggregate_share_links.add_share_link(share_id.clone(), share_link_info);
        assert_eq!(aggregate_share_links.count(), 1);

        aggregate_share_links.remove_share_link(&share_id);
        assert_eq!(aggregate_share_links.count(), 0);
        assert!(aggregate_share_links.get_share_link(&share_id).is_none());
    }

    #[test]
    fn test_get_nonexistent_share_link() {
        let aggregate_share_links = AggregateToShareLinks::new();
        assert!(aggregate_share_links.get_share_link(&666).is_none());
    }

    #[test]
    fn test_multiple_share_links() {
        let mut aggregate_share_links = AggregateToShareLinks::new();

        let share_link_info1 = ShareLinkAccessInfo {
            access_level: AccessLevel::Viewer,
            share_id: 1,
            is_single_use: false,
            expires_on: 0
        };
        let share_link_info2 = ShareLinkAccessInfo {
            access_level: AccessLevel::Contributor,
            share_id: 2,
            is_single_use: true,
            expires_on: 0
        };

        aggregate_share_links.add_share_link(1, share_link_info1);
        aggregate_share_links.add_share_link(2, share_link_info2);

        assert_eq!(aggregate_share_links.count(), 2);
        assert_eq!(aggregate_share_links.get_share_link(&1).unwrap().access_level, AccessLevel::Viewer);
        assert_eq!(aggregate_share_links.get_share_link(&2).unwrap().access_level, AccessLevel::Contributor);
    }
}