use event_storage::event_storage_cache::EventStorageCache;
use serde::{Deserialize, Serialize};

pub struct ShareLinksCache<'a> {
    event_storage_cache: &'a mut EventStorageCache,
}

impl<'a> ShareLinksCache<'a> {

    pub fn new(event_storage_cache: &'a mut EventStorageCache) -> Self {
        Self {
            event_storage_cache,
        }
    }

    pub fn create_share_link() {

    }

    pub fn check_share_link_valid() -> Option<ShareLinkAccessInfo>
    {
        None
    }

    pub fn use_share_link() {

    }
    
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AccessLevel {
    Owner,
    Contributor,
    Viewer,
}

pub struct ShareLinkAccessInfo {
    pub access_level: AccessLevel,
    pub share_key: String,
    pub is_single_use: bool,
    pub created_by: String,
}