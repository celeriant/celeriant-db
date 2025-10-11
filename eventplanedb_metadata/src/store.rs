use crate::{
    config::MetadataConfig,
    error::{MetadataError, MetadataResult},
    migration::{DatabaseType, MigrationManager},
};
use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use turso::{Builder, Connection, Database};

#[derive(Clone)]
pub struct MetadataStore {
    pub test: u64,
    config: MetadataConfig,
}

impl MetadataStore {
    pub fn new(config: MetadataConfig) -> Self {
        // Ensure base directories exist
        std::fs::create_dir_all(&config.base_path).ok();
        std::fs::create_dir_all(config.base_path.join("orgs")).ok();
        std::fs::create_dir_all(config.base_path.join("users")).ok();
        std::fs::create_dir_all(config.base_path.join("aggregates")).ok();

        Self { test: 99, config }
    }

    async fn get_database(&self, db_path: &str) -> MetadataResult<Database> {
        let builder = Builder::new_local(db_path);
        let db = builder
            .build()
            .await
            .map_err(MetadataError::DatabaseError)?;
        Ok(db)
    }

    /// Opens a connection to the org metadata store and ensures the schema is up to date
    pub async fn open_org_connection(&self, org_id: u128) -> MetadataResult<Connection> {
        let db_path = self
            .config
            .org_db_path(org_id)
            .to_string_lossy()
            .to_string();
        let db = self.get_database(&db_path).await?;
        let conn = db.connect().map_err(MetadataError::DatabaseError)?;

        // Run schema migration every time - ensure_schema should be idempotent
        MigrationManager::ensure_schema(&conn, DatabaseType::Org).await?;

        Ok(conn)
    }

    /// Deletes the aggregate metadata database file
    pub async fn delete_aggregate_database(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> MetadataResult<()> {
        let db_path = self
            .config
            .aggregate_db_path(org_id, aggregate_type_id, aggregate_id);

        // Remove the database file if it exists
        if db_path.exists() {
            std::fs::remove_file(&db_path).map_err(MetadataError::IoError)?;
        }

        Ok(())
    }

    /// Opens a connection to the user metadata store and ensures the schema is up to date
    pub async fn open_user_connection(&self, user_id: u128) -> MetadataResult<Connection> {
        let db_path = self
            .config
            .user_db_path(user_id)
            .to_string_lossy()
            .to_string();
        let db = self.get_database(&db_path).await?;
        let conn = db.connect().map_err(MetadataError::DatabaseError)?;

        // Run schema migration every time - ensure_schema should be idempotent
        MigrationManager::ensure_schema(&conn, DatabaseType::User).await?;

        Ok(conn)
    }

    /// Opens a connection to the aggregate metadata store and ensures the schema is up to date
    pub async fn open_aggregate_connection(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> MetadataResult<Connection> {
        let db_path = self
            .config
            .aggregate_db_path(org_id, aggregate_type_id, aggregate_id)
            .to_string_lossy()
            .to_string();
        let db = self.get_database(&db_path).await?;
        let conn = db.connect().map_err(MetadataError::DatabaseError)?;

        // Run schema migration every time - ensure_schema should be idempotent
        MigrationManager::ensure_schema(&conn, DatabaseType::Aggregate).await?;

        Ok(conn)
    }
}
