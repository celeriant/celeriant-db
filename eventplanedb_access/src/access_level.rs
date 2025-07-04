use serde::{Deserialize, Serialize};

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
}