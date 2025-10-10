pub mod config;
pub mod error;
pub mod migration;
pub mod queries;
pub mod schema;
pub mod store;

// Re-export common types
pub use config::MetadataConfig;
pub use error::{MetadataError, MetadataResult};
pub use migration::{DatabaseType, MigrationManager};

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use turso::Builder;

    #[tokio::test]
    async fn test1() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        println!("{:?}", temp_dir.path()); // Use the temp directory

        let db_path: String = temp_dir
            .path()
            .join("test.db")
            .to_string_lossy()
            .to_string();

        let builder = Builder::new_local(&db_path);
        let db = builder.build().await?;

        let conn = db.connect()?;

        let is_auto_commit = conn.is_autocommit()?;

        assert!(is_auto_commit);

        conn.execute(
            "   CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL)",
            (),
        )
        .await?;

        conn.execute("INSERT INTO users (username) VALUES (?)", ("alice",))
            .await?;
        conn.execute("INSERT INTO users (username) VALUES (?)", ("bob",))
            .await?;

        let mut res = conn.query("SELECT * FROM users", ()).await?;

        // Iterate over the rows to print the data
        while let Some(row) = res.next().await? {
            let id: i64 = row.get(0)?;
            let username: String = row.get(1)?;
            println!("User: id={id}, username={username}");
        }

        Ok(())
    }
}
