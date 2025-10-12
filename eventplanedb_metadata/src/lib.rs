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
