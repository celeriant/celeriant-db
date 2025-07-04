use event_storage::{event_item::EventItem, event_storage_cache::EventStorageCache};
use serde::{Deserialize, Serialize};
use crate::{access_level::AccessLevel, job_error::JobError};

pub struct ShareLinksCache {
}

impl ShareLinksCache {
    pub fn new() -> Self {
        Self {
        }
    }

    pub fn create_share_link(&mut self,
        mut event_storage_cache: &mut EventStorageCache,
        file_path: String,
        cb: String,
        share_hash: String,
        access_level: AccessLevel,
        is_single_use: bool,
        iv: Option<String>,
        description: Option<String>,
        expires_on: Option<i64>,) -> Result<EventItem, JobError> {

        //TODO: Check user has write access or provide access using share link
        Err(JobError::NotFound("sdlfsd".to_string()))
    }

    pub fn check_share_link_valid() -> Option<ShareLinkAccessInfo>
    {
        None
    }

    pub fn use_share_link() {

    }
    
}



pub struct ShareLinkAccessInfo {
    pub access_level: AccessLevel,
    pub share_key: String,
    pub is_single_use: bool,
    pub created_by: String,
}