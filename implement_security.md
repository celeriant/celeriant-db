# Implementation: SQLite Metadata Store with Turso

## Overview

Implement a separate metadata storage system using Turso (libsql) to handle permissions, shares, analytics, and aggregate metadata queries while keeping the existing event-sourced storage for data integrity.

## Architecture

```
eventplanedb_localfirst_server/
├── src/
│   ├── metadata/           # New metadata module
│   └── routes/            # Updated routes
└── Cargo.toml            # Add libsql dependency

eventplanedb_metadata/     # New crate
├── src/
│   ├── lib.rs
│   ├── config.rs
│   ├── store.rs
│   ├── schema.rs
│   ├── migrations.rs
│   ├── queries/
│   │   ├── mod.rs
│   │   ├── permissions.rs
│   │   ├── shares.rs
│   │   ├── analytics.rs
│   │   └── aggregates.rs
│   └── sync.rs
└── Cargo.toml
```

## Critical Implementation Steps

### 1. Create New Metadata Crate

**File: `eventplanedb_metadata/Cargo.toml`**

```toml
[package]
name = "eventplanedb_metadata"
version = "0.1.0"
edition = "2021"

[dependencies]
libsql = "0.4"
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "1.0"
tracing = "0.1"
async-trait = "0.1"
```

### 2. Configuration System

**File: `eventplanedb_metadata/src/config.rs`**

```rust
#[derive(Debug, Clone)]
pub struct MetadataConfig {
    /// Base directory for all metadata databases
    pub base_path: PathBuf,
    
    /// Schema version for migrations
    pub target_schema_version: u32,
    
    /// Connection pool settings
    pub max_connections_per_db: u32,
    
    /// Enable WAL mode for better concurrency
    pub enable_wal: bool,
    
    /// Sync interval for event-driven updates
    pub sync_interval_ms: u64,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("./metadata"),
            target_schema_version: 1,
            max_connections_per_db: 10,
            enable_wal: true,
            sync_interval_ms: 100,
        }
    }
}

impl MetadataConfig {
    /// Get database file path for organization metadata
    pub fn org_db_path(&self, org_id: u128) -> PathBuf {
        self.base_path.join("orgs").join(format!("{}.db", org_id))
    }
    
    /// Get database file path for user metadata
    pub fn user_db_path(&self, user_id: u128) -> PathBuf {
        self.base_path.join("users").join(format!("{}.db", user_id))
    }
    
    /// Get database file path for aggregate metadata
    pub fn aggregate_db_path(&self, org_id: u128, aggregate_type_id: u128) -> PathBuf {
        self.base_path
            .join("aggregates")
            .join(format!("{}_{}.db", org_id, aggregate_type_id))
    }
}
```

### 3. Schema Management

**File: `eventplanedb_metadata/src/schema.rs`**

```rust
pub const CURRENT_SCHEMA_VERSION: u32 = 1;

pub const ORG_SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS user_permissions (
    user_id INTEGER NOT NULL,
    aggregate_type_id INTEGER NOT NULL,
    aggregate_id INTEGER NOT NULL,
    access_level INTEGER NOT NULL,
    granted_at INTEGER NOT NULL,
    granted_by INTEGER,
    revoked_at INTEGER,
    PRIMARY KEY (user_id, aggregate_type_id, aggregate_id)
);

CREATE TABLE IF NOT EXISTS shares (
    share_id INTEGER PRIMARY KEY,
    aggregate_type_id INTEGER NOT NULL,
    aggregate_id INTEGER NOT NULL,
    created_by INTEGER NOT NULL,
    access_level INTEGER NOT NULL,
    expires_at INTEGER,
    is_single_use BOOLEAN NOT NULL DEFAULT 0,
    use_count INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    disabled_at INTEGER,
    disabled_by INTEGER
);

CREATE TABLE IF NOT EXISTS aggregate_activity (
    aggregate_type_id INTEGER NOT NULL,
    aggregate_id INTEGER NOT NULL,
    last_write INTEGER NOT NULL,
    write_count_24h INTEGER NOT NULL DEFAULT 0,
    unique_users_24h TEXT, -- JSON array
    last_read INTEGER,
    read_count_24h INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (aggregate_type_id, aggregate_id)
);

CREATE INDEX idx_user_permissions_user ON user_permissions(user_id);
CREATE INDEX idx_shares_active ON shares(disabled_at) WHERE disabled_at IS NULL;
CREATE INDEX idx_activity_last_write ON aggregate_activity(last_write DESC);
"#;

pub const USER_SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS user_org_membership (
    org_id INTEGER PRIMARY KEY,
    role INTEGER NOT NULL,
    joined_at INTEGER NOT NULL,
    last_active INTEGER
);

CREATE TABLE IF NOT EXISTS user_aggregate_access (
    org_id INTEGER NOT NULL,
    aggregate_type_id INTEGER NOT NULL,
    aggregate_id INTEGER NOT NULL,
    access_level INTEGER NOT NULL,
    last_accessed INTEGER,
    PRIMARY KEY (org_id, aggregate_type_id, aggregate_id)
);
"#;

pub const AGGREGATE_SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS aggregate_metadata (
    aggregate_id INTEGER PRIMARY KEY,
    owner_user_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    last_modified INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    event_count INTEGER NOT NULL DEFAULT 0,
    collaborator_count INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS aggregate_collaborators (
    aggregate_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    access_level INTEGER NOT NULL,
    added_at INTEGER NOT NULL,
    added_by INTEGER NOT NULL,
    PRIMARY KEY (aggregate_id, user_id)
);
"#;
```

### 4. Migration System

**File: `eventplanedb_metadata/src/migrations.rs`**

```rust
use libsql::Connection;
use crate::schema::*;

pub struct MigrationManager;

impl MigrationManager {
    pub async fn ensure_schema(conn: &Connection, db_type: DatabaseType) -> Result<(), MetadataError> {
        let current_version = Self::get_schema_version(conn).await?;
        
        if current_version == 0 {
            Self::create_initial_schema(conn, db_type).await?;
        } else if current_version < CURRENT_SCHEMA_VERSION {
            Self::migrate_schema(conn, current_version, CURRENT_SCHEMA_VERSION).await?;
        }
        
        Ok(())
    }
    
    async fn get_schema_version(conn: &Connection) -> Result<u32, MetadataError> {
        let result = conn
            .query("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1", ())
            .await;
            
        match result {
            Ok(mut rows) => {
                if let Some(row) = rows.next().await? {
                    Ok(row.get::<u32>(0)?)
                } else {
                    Ok(0)
                }
            }
            Err(_) => Ok(0), // Table doesn't exist
        }
    }
    
    async fn create_initial_schema(conn: &Connection, db_type: DatabaseType) -> Result<(), MetadataError> {
        let schema = match db_type {
            DatabaseType::Org => ORG_SCHEMA_V1,
            DatabaseType::User => USER_SCHEMA_V1,
            DatabaseType::Aggregate => AGGREGATE_SCHEMA_V1,
        };
        
        conn.execute_batch(schema).await?;
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", [CURRENT_SCHEMA_VERSION]).await?;
        
        Ok(())
    }
    
    async fn migrate_schema(conn: &Connection, from: u32, to: u32) -> Result<(), MetadataError> {
        tracing::info!("Migrating schema from version {} to {}", from, to);
        
        // Future migration logic here
        match (from, to) {
            (1, 2) => {
                // Example migration
                // conn.execute("ALTER TABLE ...", ()).await?;
            }
            _ => return Err(MetadataError::UnsupportedMigration(from, to)),
        }
        
        conn.execute("UPDATE schema_version SET version = ?", [to]).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DatabaseType {
    Org,
    User,
    Aggregate,
}
```

### 5. Main Store Implementation

**File: `eventplanedb_metadata/src/store.rs`**

```rust
use libsql::{Connection, Database};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct MetadataStore {
    config: MetadataConfig,
    
    // Connection pools for different database types
    org_connections: Arc<RwLock<HashMap<u128, Arc<Connection>>>>,
    user_connections: Arc<RwLock<HashMap<u128, Arc<Connection>>>>,
    aggregate_connections: Arc<RwLock<HashMap<(u128, u128), Arc<Connection>>>>,
}

impl MetadataStore {
    pub fn new(config: MetadataConfig) -> Self {
        // Ensure base directories exist
        std::fs::create_dir_all(&config.base_path).ok();
        std::fs::create_dir_all(&config.base_path.join("orgs")).ok();
        std::fs::create_dir_all(&config.base_path.join("users")).ok();
        std::fs::create_dir_all(&config.base_path.join("aggregates")).ok();
        
        Self {
            config,
            org_connections: Arc::new(RwLock::new(HashMap::new())),
            user_connections: Arc::new(RwLock::new(HashMap::new())),
            aggregate_connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Get or create connection to org database
    pub async fn get_org_connection(&self, org_id: u128) -> Result<Arc<Connection>, MetadataError> {
        {
            let connections = self.org_connections.read().await;
            if let Some(conn) = connections.get(&org_id) {
                return Ok(conn.clone());
            }
        }
        
        // Create new connection
        let db_path = self.config.org_db_path(org_id);
        let db = Database::open(&db_path).await?;
        let conn = db.connect()?;
        
        // Set up database
        if self.config.enable_wal {
            conn.execute("PRAGMA journal_mode = WAL", ()).await?;
        }
        
        // Ensure schema
        MigrationManager::ensure_schema(&conn, DatabaseType::Org).await?;
        
        let conn = Arc::new(conn);
        
        // Cache connection
        {
            let mut connections = self.org_connections.write().await;
            connections.insert(org_id, conn.clone());
        }
        
        Ok(conn)
    }
    
    // Similar methods for user and aggregate connections...
}
```

### 6. Query Modules

**File: `eventplanedb_metadata/src/queries/permissions.rs`**

```rust
use super::*;

impl MetadataStore {
    pub async fn user_can_access(
        &self,
        user_id: u128,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> Result<bool, MetadataError> {
        let conn = self.get_org_connection(org_id).await?;
        
        let mut stmt = conn
            .prepare("SELECT 1 FROM user_permissions 
                     WHERE user_id = ? AND aggregate_type_id = ? 
                     AND aggregate_id = ? AND revoked_at IS NULL")
            .await?;
        
        let mut rows = stmt.query([user_id as i64, aggregate_type_id as i64, aggregate_id as i64]).await?;
        Ok(rows.next().await?.is_some())
    }
    
    pub async fn grant_permission(
        &self,
        org_id: u128,
        user_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        access_level: AccessLevel,
        granted_by: u128,
    ) -> Result<(), MetadataError> {
        let conn = self.get_org_connection(org_id).await?;
        
        conn.execute(
            "INSERT OR REPLACE INTO user_permissions 
             (user_id, aggregate_type_id, aggregate_id, access_level, granted_at, granted_by)
             VALUES (?, ?, ?, ?, ?, ?)",
            [
                user_id as i64,
                aggregate_type_id as i64,
                aggregate_id as i64,
                access_level as i64,
                chrono::Utc::now().timestamp(),
                granted_by as i64,
            ],
        ).await?;
        
        Ok(())
    }
}
```

### 7. Analytics Queries

**File: `eventplanedb_metadata/src/queries/analytics.rs`**

```rust
impl MetadataStore {
    pub async fn most_active_aggregates(
        &self,
        org_id: u128,
        limit: u32,
    ) -> Result<Vec<AggregateActivity>, MetadataError> {
        let conn = self.get_org_connection(org_id).await?;
        
        let mut stmt = conn
            .prepare(
                "SELECT aggregate_type_id, aggregate_id, last_write, 
                        write_count_24h, unique_users_24h, read_count_24h
                 FROM aggregate_activity 
                 ORDER BY write_count_24h DESC 
                 LIMIT ?"
            ).await?;
        
        let mut rows = stmt.query([limit as i64]).await?;
        let mut results = Vec::new();
        
        while let Some(row) = rows.next().await? {
            results.push(AggregateActivity {
                aggregate_type_id: row.get::<i64>(0)? as u128,
                aggregate_id: row.get::<i64>(1)? as u128,
                last_write: row.get::<i64>(2)? as u64,
                write_count_24h: row.get::<i64>(3)? as u32,
                unique_users_24h: serde_json::from_str(
                    &row.get::<String>(4)?
                ).unwrap_or_default(),
                read_count_24h: row.get::<i64>(5)? as u32,
            });
        }
        
        Ok(results)
    }
    
    pub async fn user_accessible_aggregates(
        &self,
        user_id: u128,
        org_id: u128,
    ) -> Result<Vec<AggregateKey>, MetadataError> {
        let conn = self.get_org_connection(org_id).await?;
        
        let mut stmt = conn
            .prepare(
                "SELECT aggregate_type_id, aggregate_id 
                 FROM user_permissions 
                 WHERE user_id = ? AND revoked_at IS NULL"
            ).await?;
        
        let mut rows = stmt.query([user_id as i64]).await?;
        let mut results = Vec::new();
        
        while let Some(row) = rows.next().await? {
            results.push(AggregateKey::new(
                org_id,
                row.get::<i64>(0)? as u128,
                row.get::<i64>(1)? as u128,
            ));
        }
        
        Ok(results)
    }
}
```

### 8. Event Synchronization

**File: `eventplanedb_metadata/src/sync.rs`**

```rust
use tokio::sync::mpsc;

pub struct MetadataSync {
    store: Arc<MetadataStore>,
    event_receiver: mpsc::Receiver<MetadataEvent>,
}

#[derive(Debug, Clone)]
pub enum MetadataEvent {
    ShareCreated {
        org_id: u128,
        share_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        created_by: u128,
        access_level: AccessLevel,
        expires_at: Option<u64>,
        is_single_use: bool,
    },
    PermissionGranted {
        org_id: u128,
        user_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        access_level: AccessLevel,
        granted_by: u128,
    },
    AggregateActivity {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        activity_type: ActivityType,
        user_id: u128,
    },
}

impl MetadataSync {
    pub async fn run(&mut self) {
        while let Some(event) = self.event_receiver.recv().await {
            if let Err(e) = self.handle_event(event).await {
                tracing::error!("Failed to process metadata event: {}", e);
            }
        }
    }
    
    async fn handle_event(&self, event: MetadataEvent) -> Result<(), MetadataError> {
        match event {
            MetadataEvent::ShareCreated { org_id, share_id, .. } => {
                self.store.create_share(org_id, share_id, /* ... */).await?;
            }
            MetadataEvent::PermissionGranted { org_id, user_id, .. } => {
                self.store.grant_permission(org_id, user_id, /* ... */).await?;
            }
            MetadataEvent::AggregateActivity { org_id, aggregate_type_id, aggregate_id, .. } => {
                self.store.update_activity(org_id, aggregate_type_id, aggregate_id).await?;
            }
        }
        Ok(())
    }
}
```

### 9. Integration with LocalFirst Server

**File: `eventplanedb_localfirst_server/Cargo.toml`**

```toml
[dependencies]
# ... existing deps
eventplanedb_metadata = { path = "../eventplanedb_metadata" }
```

**File: `eventplanedb_localfirst_server/src/app_state.rs`**

```rust
// Add to imports
use eventplanedb_metadata::{MetadataStore, MetadataConfig, MetadataSync};

// Add to AppState struct
pub struct AppState {
    // ... existing fields
    pub metadata_store: Arc<MetadataStore>,
    pub metadata_event_sender: mpsc::Sender<MetadataEvent>,
}

impl AppState {
    pub fn new(base_path: String) -> Self {
        // ... existing setup
        
        let metadata_config = MetadataConfig {
            base_path: PathBuf::from(&base_path).join("metadata"),
            ..MetadataConfig::default()
        };
        let metadata_store = Arc::new(MetadataStore::new(metadata_config));
        
        let (metadata_event_sender, metadata_event_receiver) = mpsc::channel(1000);
        
        // Start metadata sync task
        let mut metadata_sync = MetadataSync::new(metadata_store.clone(), metadata_event_receiver);
        tokio::spawn(async move {
            metadata_sync.run().await;
        });
        
        Self {
            // ... existing fields
            metadata_store,
            metadata_event_sender,
        }
    }
}
```

## Testing Strategy

1. **Unit Tests**: Test each query module independently
2. **Integration Tests**: Test event sync with actual SQLite files
3. **Performance Tests**: Benchmark concurrent access patterns
4. **Migration Tests**: Test schema upgrades with sample data

## Deployment Considerations

1. **Backup Strategy**: Regular backups of metadata directories
2. **Monitoring**: Track database file sizes and query performance
3. **Cleanup**: Periodic cleanup of old shares and revoked permissions
4. **Sharding**: Consider splitting large org databases if needed

## Future Enhancements

1. **Read Replicas**: For high-read analytics workloads
2. **Compression**: SQLite database compression for storage efficiency
3. **Caching**: Redis layer for frequently accessed permissions
4. **Metrics**: Prometheus metrics for database operations
