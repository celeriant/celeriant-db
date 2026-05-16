pub mod client_error;
pub mod celeriant_client;
pub mod client_operations;
pub mod event_helpers;
pub mod list_operations;
pub mod pool;
pub mod pool_trait;
pub mod read_all_iterator;
pub mod server_error;
mod tokio_wire;
pub mod watch_connection;

pub use celeriant_client::{CachedDict, CeleriantClient, ClientIdentityConfig, ClientTlsConfig};
pub use client_operations::WriteEventsOptions;
pub use event_helpers::{from_json, json_event};
pub use client_error::ClientError;
pub use pool::{
    CeleriantPool, PoolOptions, PooledConnection, PooledListAggregateTypesIterator,
    PooledListAggregatesIterator, PooledListOrgsIterator, PooledReadAllIterator,
};
pub use pool_trait::CeleriantPoolApi;
pub use read_all_iterator::ReadAllIterator;
pub use server_error::*;
pub use watch_connection::{WatchConnection, WatchOptions};
