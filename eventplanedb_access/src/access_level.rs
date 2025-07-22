use serde::{Deserialize, Serialize};

// use crate::{claims::Claims, job_error::JobError, share_links_cache::ShareLinksCache, user_access_cache::{UserAccessCache, UserIdType}};

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
        (current_access_level as u64) > (potential_access_level as u64)
    }

    pub fn meets_required_access_level(current_access_level: AccessLevel, required_access_level: AccessLevel) -> bool {
        (current_access_level as u64) <= (required_access_level as u64)
    }

    pub fn greatest_access_level(access_level_1: AccessLevel, access_level_2: AccessLevel) -> AccessLevel {
        if (access_level_1 as u64) < (access_level_2 as u64) {
            access_level_1
        } else {
            access_level_2
        }
    }
}
