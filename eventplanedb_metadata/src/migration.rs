use crate::{MetadataError, MetadataResult, schema::*};
use async_sqlite::Client;

pub struct MigrationManager;

impl MigrationManager {
    pub async fn ensure_schema(client: &Client, db_type: DatabaseType) -> MetadataResult<()> {
        let current_version = Self::get_schema_version(client).await?;
        let target_version = Self::get_target_version(db_type);

        if current_version == 0 {
            Self::create_initial_schema(client, db_type).await?;
        } else if current_version < target_version {
            Self::migrate_schema(client, current_version, target_version, db_type).await?;
        }

        Ok(())
    }

    fn get_target_version(db_type: DatabaseType) -> u32 {
        match db_type {
            DatabaseType::Org => ORGS_CURRENT_SCHEMA_VERSION,
            DatabaseType::User => USERS_CURRENT_SCHEMA_VERSION,
            DatabaseType::Aggregate => AGGREGATES_CURRENT_SCHEMA_VERSION,
        }
    }

    async fn get_schema_version(client: &Client) -> MetadataResult<u32> {
        client
            .conn(|conn| {
                let mut stmt = match conn
                    .prepare("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
                {
                    Ok(stmt) => stmt,
                    Err(_) => {
                        // schema_version table doesn't exist yet, return 0
                        return Ok(0);
                    }
                };

                match stmt.query_row([], |row| {
                    let version: u32 = row.get(0)?;
                    Ok(version)
                }) {
                    Ok(v) => Ok(v),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
                    Err(e) => Err(e.into()), // Convert rusqlite::Error to async_sqlite::Error
                }
            })
            .await
            .map_err(MetadataError::from)
    }

    async fn create_initial_schema(client: &Client, db_type: DatabaseType) -> MetadataResult<()> {
        let schema = match db_type {
            DatabaseType::Org => ORG_SCHEMA_V1,
            DatabaseType::User => USER_SCHEMA_V1,
            DatabaseType::Aggregate => AGGREGATE_SCHEMA_V1,
        };

        let target_version = Self::get_target_version(db_type);

        client
            .conn(move |conn| {
                // Execute schema batch
                conn.execute_batch(schema)?;

                // Insert schema version
                conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)",
                    [target_version],
                )?;

                Ok(())
            })
            .await
            .map_err(|e| MetadataError::from(e))
    }

    async fn migrate_schema(
        client: &Client,
        from: u32,
        to: u32,
        db_type: DatabaseType,
    ) -> MetadataResult<()> {
        // Future migration logic here - now database-type aware
        // client.conn(|conn| {
        //     match (db_type, from, to) {
        //         (DatabaseType::Org, 1, 2) => {
        //             // Example org migration
        //             // conn.execute("ALTER TABLE user_permissions ADD COLUMN ...", ())?;
        //         }
        //         (DatabaseType::User, 1, 2) => {
        //             // Example user migration
        //             // conn.execute("ALTER TABLE user_aggregate_access ADD COLUMN ...", ())?;
        //         }
        //         (DatabaseType::Aggregate, 1, 2) => {
        //             // Example aggregate migration
        //             // conn.execute("ALTER TABLE users ADD COLUMN ...", ())?;
        //         }
        //         _ => return Err(MetadataError::UnsupportedMigration(from, to)),
        //     }
        //
        //     conn.execute("UPDATE schema_version SET version = ?", [to])?;
        //     Ok(())
        // }).await?;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DatabaseType {
    Org,
    User,
    Aggregate,
}
