//! Object Store Sidecar Integration
//!
//! This module implements the Tokio sidecar pattern for integrating Apache Arrow's
//! `object_store` crate with the Glommio-based server.

pub mod config;
pub mod error;
pub mod gateway;
pub mod lease_ops;
pub mod ops;
pub mod runtime;

pub use config::{ObjectStoreRetryConfig, ObjectStoreRuntimeConfig};
pub use error::ObjectStoreError;
pub use gateway::ObjectStoreGateway;
pub use lease_ops::LeaseOps;
pub use ops::{ObjectStoreOp, ObjectStoreResult, QoSClass};
pub use runtime::{ObjectStoreRuntime, S3Config};