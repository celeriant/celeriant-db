use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct MetadataConfig {
    pub base_path: PathBuf,
    pub orgs_target_schema_version: u32,
    pub users_target_schema_version: u32,
    pub aggregate_target_schema_version: u32,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("./metadata"),
            orgs_target_schema_version: 1,
            users_target_schema_version: 1,
            aggregate_target_schema_version: 1,
        }
    }
}

impl MetadataConfig {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            ..Default::default()
        }
    }

    /// Get database file path for organization metadata
    pub fn org_db_path(&self, org_id: u128) -> PathBuf {
        self.base_path.join("orgs").join(format!("{org_id}.db"))
    }

    /// Get database file path for user metadata
    pub fn user_db_path(&self, user_id: u128) -> PathBuf {
        self.base_path.join("users").join(format!("{user_id}.db"))
    }

    /// Get database file path for aggregate metadata
    pub fn aggregate_db_path(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> PathBuf {
        self.base_path
            .join("aggregates")
            .join(format!("{org_id}_{aggregate_type_id}_{aggregate_id}.db"))
    }
}
