pub const ORGS_CURRENT_SCHEMA_VERSION: u32 = 1;
pub const USERS_CURRENT_SCHEMA_VERSION: u32 = 1;
pub const AGGREGATES_CURRENT_SCHEMA_VERSION: u32 = 1;

pub const ORG_SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS user_aggregate_access (
    user_id BLOB NOT NULL,
    aggregate_type_id BLOB NOT NULL,
    aggregate_id BLOB NOT NULL,
    access_level INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    granted_from_share_id BLOB,
    PRIMARY KEY (user_id, aggregate_type_id, aggregate_id)
);

CREATE INDEX idx_user_permissions_user ON user_aggregate_access(user_id);
"#;

pub const USER_SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS user_aggregate_access (
    org_id BLOB NOT NULL,
    aggregate_type_id BLOB NOT NULL,
    aggregate_id BLOB NOT NULL,
    access_level INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    granted_from_share_id BLOB,
    PRIMARY KEY (org_id, aggregate_type_id, aggregate_id)
);
"#;

pub const AGGREGATE_SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS users_and_clients (
    id BLOB NOT NULL,
    is_user INTEGER NOT NULL DEFAULT 0,
    access_level INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    granted_from_share_id BLOB,
    PRIMARY KEY (id, is_user)
);

CREATE TABLE IF NOT EXISTS share_links (
    id BLOB PRIMARY KEY,
    created_by_client_id BLOB NOT NULL,
    created_by_user_id BLOB,
    created_at INTEGER NOT NULL,
    access_level INTEGER NOT NULL,
    expires_at INTEGER,
    is_single_use BOOLEAN NOT NULL DEFAULT 0,
    use_count INTEGER NOT NULL DEFAULT 0,
    disabled_at INTEGER,
    disabled_by_client_id BLOB,
    disabled_by_user_id BLOB
);
"#;
