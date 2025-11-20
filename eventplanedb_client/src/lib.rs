use std::sync::Arc;
use tokio::sync::RwLock;

mod connection;
mod config;
mod error;

#[cfg(feature = "ffi")]
pub mod ffi;

pub use config::{ClientConfig, ConnectionPoolConfig};
pub use error::{ClientError, ClientResult};
pub use eventplanedb_structures::*;

use connection::{Connection, ConnectionPool};

/// High-performance EventPlaneDB client with connection pooling and pipelining
pub struct EventPlaneDBClient {
    pool: Arc<ConnectionPool>,
    config: ClientConfig,
}

impl EventPlaneDBClient {
    /// Create a new client with the given configuration
    pub async fn new(config: ClientConfig) -> ClientResult<Self> {
        let pool = ConnectionPool::new(config.clone()).await?;
        
        Ok(Self {
            pool: Arc::new(pool),
            config,
        })
    }

    /// Create a client with default configuration
    pub async fn connect(address: impl Into<String>) -> ClientResult<Self> {
        let config = ClientConfig::new(address.into());
        Self::new(config).await
    }

    /// List all organizations with optional filters
    pub async fn list_organisations(
        &self,
        filters: directory_filters::DirectoryFilters,
        correlation_id: Option<u128>,
    ) -> ClientResult<response::ListOrganisationsResponse> {
        let request = request::Request::ListOrganisations(
            request::ListOrganisationsRequest {
                correlation_id,
                filters,
            }
        );

        let response = self.execute_request(request).await?;
        
        match response {
            response::Response::ListOrganisations(resp) => Ok(resp),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// List aggregates for an organization
    pub async fn list_aggregates(
        &self,
        org_id: u128,
        aggregate_type_id: Option<u128>,
        filters: directory_filters::DirectoryFilters,
        correlation_id: Option<u128>,
    ) -> ClientResult<response::ListAggregatesResponse> {
        let request = request::Request::ListAggregates(
            request::ListAggregatesRequest {
                correlation_id,
                org_id,
                aggregate_type_id,
                filters,
            }
        );

        let response = self.execute_request(request).await?;
        
        match response {
            response::Response::ListAggregates(resp) => Ok(resp),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Check if an aggregate exists
    pub async fn exists(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        correlation_id: Option<u128>,
    ) -> ClientResult<response::ExistsResponse> {
        let request = request::Request::Exists(request::ExistsRequest {
            correlation_id,
            org_id,
            aggregate_type_id,
            aggregate_id,
        });

        let response = self.execute_request(request).await?;
        
        match response {
            response::Response::Exists(resp) => Ok(resp),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Read events from an aggregate
    pub async fn read(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: read_filters::ReadFilters,
        correlation_id: Option<u128>,
    ) -> ClientResult<response::ReadResponse> {
        let request = request::Request::Read(request::ReadRequest {
            correlation_id,
            org_id,
            aggregate_type_id,
            aggregate_id,
            filters,
        });

        let response = self.execute_request(request).await?;
        
        match response {
            response::Response::Read(resp) => Ok(resp),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Write events to an aggregate
    pub async fn write(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<event_item::EventItem>,
        allow_create: bool,
        expected_event_batch_index: Option<u64>,
        enforce_client_idempotency: bool,
        durable_write_with_delay_us: Option<u64>,
        compression_type: compression_type::CompressionType,
        correlation_id: Option<u128>,
    ) -> ClientResult<response::WriteResponse> {
        let request = request::Request::Write(request::WriteRequest {
            correlation_id,
            org_id,
            aggregate_type_id,
            aggregate_id,
            client_id,
            user_id,
            events,
            allow_create,
            expected_event_batch_index,
            enforce_client_idempotency,
            durable_write_with_delay_us,
            compression_type,
        });

        let response = self.execute_request(request).await?;
        
        match response {
            response::Response::Write(resp) => Ok(resp),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Write event batches to an aggregate
    pub async fn write_batches(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        batches: Vec<event_batch_item::EventBatchItem>,
        allow_create: bool,
        durable_write_with_delay_us: Option<u64>,
        compression_type: compression_type::CompressionType,
        correlation_id: Option<u128>,
    ) -> ClientResult<response::WriteBatchesResponse> {
        let request = request::Request::WriteBatches(request::WriteBatchesRequest {
            correlation_id,
            org_id,
            aggregate_type_id,
            aggregate_id,
            allow_create,
            durable_write_with_delay_us,
            compression_type,
            batches,
        });

        let response = self.execute_request(request).await?;
        
        match response {
            response::Response::WriteBatches(resp) => Ok(resp),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Trim events from the start of an aggregate
    pub async fn trim_start(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        keep_from_event_batch_index: u64,
        correlation_id: Option<u128>,
    ) -> ClientResult<response::TrimStartResponse> {
        let request = request::Request::TrimStart(request::TrimStartRequest {
            correlation_id,
            org_id,
            aggregate_type_id,
            aggregate_id,
            keep_from_event_batch_index,
        });

        let response = self.execute_request(request).await?;
        
        match response {
            response::Response::TrimStart(resp) => Ok(resp),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Delete an aggregate
    pub async fn delete(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        correlation_id: Option<u128>,
    ) -> ClientResult<response::DeleteResponse> {
        let request = request::Request::Delete(request::DeleteRequest {
            correlation_id,
            org_id,
            aggregate_type_id,
            aggregate_id,
        });

        let response = self.execute_request(request).await?;
        
        match response {
            response::Response::Delete(resp) => Ok(resp),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Update cache limits across all shards
    pub async fn update_cache_limits(
        &self,
        aggregate_write_max_data_cache_size_bytes: u64,
        correlation_id: Option<u128>,
    ) -> ClientResult<response::UpdateCacheLimitsResponse> {
        let request = request::Request::UpdateCacheLimits(
            request::UpdateCacheLimitsRequest {
                correlation_id,
                aggregate_write_max_data_cache_size_bytes,
            }
        );

        let response = self.execute_request(request).await?;
        
        match response {
            response::Response::UpdateCacheLimits(resp) => Ok(resp),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Execute a request with automatic retries and connection management
    async fn execute_request(
        &self,
        request: request::Request,
    ) -> ClientResult<response::Response> {
        let mut attempts = 0;
        let max_retries = self.config.max_retries;

        loop {
            attempts += 1;

            match self.try_execute_request(&request).await {
                Ok(response) => return Ok(response),
                Err(e) if attempts <= max_retries && e.is_retryable() => {
                    // Exponential backoff
                    let delay = std::time::Duration::from_millis(
                        self.config.retry_delay_ms * 2u64.pow(attempts - 1)
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn try_execute_request(
        &self,
        request: &request::Request,
    ) -> ClientResult<response::Response> {
        let mut conn = self.pool.get().await?;
        let result = conn.execute(request.clone()).await;
        
        // Return connection to pool if request was successful
        if result.is_ok() {
            self.pool.return_connection(conn).await;
        }
        
        result
    }

    /// Get client statistics
    pub async fn stats(&self) -> ConnectionStats {
        self.pool.stats().await
    }

    /// Close all connections gracefully
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionStats {
    pub active_connections: usize,
    pub idle_connections: usize,
    pub total_requests: u64,
    pub failed_requests: u64,
}