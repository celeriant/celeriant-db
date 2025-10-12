use crate::{
    config::MetadataConfig,
    error::{MetadataError, MetadataResult},
    migration::{DatabaseType, MigrationManager},
};
use async_sqlite::{Client, ClientBuilder, JournalMode};
use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

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

    async fn get_client(&self, db_path: &str) -> MetadataResult<Client> {
        let client = ClientBuilder::new()
            .path(db_path)
            .journal_mode(JournalMode::Wal)
            .open()
            .await
            .map_err(MetadataError::DatabaseError)?;
        Ok(client)
    }

    /// Opens a connection to the org metadata store and ensures the schema is up to date
    pub async fn open_org_connection(&self, org_id: u128) -> MetadataResult<Client> {
        let db_path = self
            .config
            .org_db_path(org_id)
            .to_string_lossy()
            .to_string();
        let client = self.get_client(&db_path).await?;

        // Run schema migration every time - ensure_schema should be idempotent
        MigrationManager::ensure_schema(&client, DatabaseType::Org).await?;

        Ok(client)
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
    pub async fn open_user_connection(&self, user_id: u128) -> MetadataResult<Client> {
        let db_path = self
            .config
            .user_db_path(user_id)
            .to_string_lossy()
            .to_string();
        let client = self.get_client(&db_path).await?;

        // Run schema migration every time - ensure_schema should be idempotent
        MigrationManager::ensure_schema(&client, DatabaseType::User).await?;

        Ok(client)
    }

    /// Opens a connection to the aggregate metadata store and ensures the schema is up to date
    pub async fn open_aggregate_connection(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> MetadataResult<Client> {
        let db_path = self
            .config
            .aggregate_db_path(org_id, aggregate_type_id, aggregate_id)
            .to_string_lossy()
            .to_string();
        let client = self.get_client(&db_path).await?;

        // Run schema migration every time - ensure_schema should be idempotent
        MigrationManager::ensure_schema(&client, DatabaseType::Aggregate).await?;

        Ok(client)
    }
}
