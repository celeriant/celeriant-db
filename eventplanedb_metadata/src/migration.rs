use crate::{MetadataError, MetadataResult, schema::*};
use turso::Connection;

pub struct MigrationManager;

impl MigrationManager {
    pub async fn ensure_schema(conn: &Connection, db_type: DatabaseType) -> MetadataResult<()> {
        let current_version = Self::get_schema_version(conn).await?;
        let target_version = Self::get_target_version(db_type);

        if current_version == 0 {
            Self::create_initial_schema(conn, db_type).await?;
        } else if current_version < target_version {
            Self::migrate_schema(conn, current_version, target_version, db_type).await?;
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

    async fn get_schema_version(conn: &Connection) -> MetadataResult<u32> {
        let mut stmt = match conn
            .prepare("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
            .await
        {
            Ok(stmt) => stmt,
            Err(_) => {
                // schema_version table doesn't exist yet, return 0
                return Ok(0);
            }
        };

        let mut rows = stmt.query(()).await?;

        if let Some(row) = rows.next().await? {
            let version: u32 = row
                .get(0)
                .map_err(|_| MetadataError::row_parse_failed("Failed to parse schema version"))?;
            Ok(version)
        } else {
            Ok(0)
        }
    }

    async fn create_initial_schema(conn: &Connection, db_type: DatabaseType) -> MetadataResult<()> {
        let schema = match db_type {
            DatabaseType::Org => ORG_SCHEMA_V1,
            DatabaseType::User => USER_SCHEMA_V1,
            DatabaseType::Aggregate => AGGREGATE_SCHEMA_V1,
        };

        let target_version = Self::get_target_version(db_type);

        // Execute schema batch
        conn.execute_batch(schema).await?;

        // Insert schema version
        conn.execute(
            "INSERT INTO schema_version (version) VALUES (?)",
            [target_version],
        )
        .await?;

        Ok(())
    }

    async fn migrate_schema(
        conn: &Connection,
        from: u32,
        to: u32,
        db_type: DatabaseType,
    ) -> MetadataResult<()> {
        // Future migration logic here - now database-type aware
        // match (db_type, from, to) {
        //     (DatabaseType::Org, 1, 2) => {
        //         // Example org migration
        //         // conn.execute("ALTER TABLE user_permissions ADD COLUMN ...", ()).await?;
        //     }
        //     (DatabaseType::User, 1, 2) => {
        //         // Example user migration
        //         // conn.execute("ALTER TABLE user_aggregate_access ADD COLUMN ...", ()).await?;
        //     }
        //     (DatabaseType::Aggregate, 1, 2) => {
        //         // Example aggregate migration
        //         // conn.execute("ALTER TABLE users ADD COLUMN ...", ()).await?;
        //     }
        //     _ => return Err(MetadataError::UnsupportedMigration(from, to)),
        // }

        // conn.execute("UPDATE schema_version SET version = ?", [to])
        //     .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DatabaseType {
    Org,
    User,
    Aggregate,
}
