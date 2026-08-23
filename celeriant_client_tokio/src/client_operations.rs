use std::collections::HashMap;

use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::{
    AggregateDetailsRequest, DeleteRequest, ReadRequest, RegisterSchemaRequest, SingleAggregateWrite,
    TrimStartRequest, WriteRequest,
};
use celeriant_msg::response::responses::{
    AggregateDetailsResponse, DeleteResponse, ReadResponse, RegisterSchemaResponse, TrimStartResponse, WriteResponse,
};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

use crate::celeriant_client::CeleriantClient;
use crate::client_error::ClientError;

/// Options for `write_events_with`. All fields default to the same values used by `write_events`.
///
/// The `client_id` is deliberately not an option: it scopes client-seq idempotency, so the
/// caller must always supply it explicitly. The client library never invents one.
#[derive(Debug, Clone)]
pub struct WriteEventsOptions {
    pub allow_create: bool,
    pub expected_version: Option<u64>,
    pub enforce_client_idempotency: bool,
}

impl Default for WriteEventsOptions {
    fn default() -> Self {
        Self {
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        }
    }
}

impl CeleriantClient {
    pub async fn read(&mut self, request: ReadRequest) -> Result<ReadResponse, ClientError> {
        match self.send_owned(ClientRequest::Read(request)).await? {
            ClientResponse::Read(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn write(&mut self, request: WriteRequest) -> Result<WriteResponse, ClientError> {
        match self.send_owned(ClientRequest::Write(request)).await? {
            ClientResponse::Write(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn delete(&mut self, request: DeleteRequest) -> Result<DeleteResponse, ClientError> {
        match self.send_owned(ClientRequest::Delete(request)).await? {
            ClientResponse::Delete(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn trim_start(&mut self, request: TrimStartRequest) -> Result<TrimStartResponse, ClientError> {
        match self.send_owned(ClientRequest::TrimStart(request)).await? {
            ClientResponse::TrimStart(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn aggregate_details(
        &mut self,
        request: AggregateDetailsRequest,
    ) -> Result<AggregateDetailsResponse, ClientError> {
        match self.send_owned(ClientRequest::AggregateDetails(request)).await? {
            ClientResponse::AggregateDetails(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    /// Convenience method: write events to a single aggregate without constructing a `WriteRequest`.
    ///
    /// `client_id` scopes client-seq idempotency — use a stable id per logical writer, never a
    /// fresh random value per call.
    pub async fn write_events(
        &mut self,
        aggregate_key: AggregateKey,
        events: Vec<DatablockAggregateEvent>,
        client_id: u128,
    ) -> Result<WriteResponse, ClientError> {
        self.write_events_with(aggregate_key, events, client_id, WriteEventsOptions::default()).await
    }

    /// Like `write_events` but accepts options to control idempotency, optimistic concurrency, etc.
    pub async fn write_events_with(
        &mut self,
        aggregate_key: AggregateKey,
        events: Vec<DatablockAggregateEvent>,
        client_id: u128,
        options: WriteEventsOptions,
    ) -> Result<WriteResponse, ClientError> {
        let mut writes = HashMap::new();
        writes.insert(aggregate_key, SingleAggregateWrite {
            events,
            allow_create: options.allow_create,
            expected_version: options.expected_version,
            enforce_client_idempotency: options.enforce_client_idempotency,
        });
        self.write(WriteRequest {
            correlation_id: None,
            client_id,
            user_id: None,
            writes,
        })
        .await
    }

    pub async fn register_schema(
        &mut self,
        request: RegisterSchemaRequest,
    ) -> Result<RegisterSchemaResponse, ClientError> {
        match self.send_owned(ClientRequest::RegisterSchema(request)).await? {
            ClientResponse::RegisterSchema(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }
}
