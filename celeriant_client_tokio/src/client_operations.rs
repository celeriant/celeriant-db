use std::collections::HashMap;

use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::{
    AggregateDetailsRequest, DeleteRequest, ReadRequest, RegisterSchemaRequest, SingleAggregateWrite,
    TrimStartRequest, WriteRequest,
};
use celeriant_msg::response::responses::{AggregateDetailsResponse, ReadResponse, SuccessResponse};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

use crate::celeriant_client::CeleriantClient;
use crate::client_error::ClientError;

/// Options for `write_events_with`. All fields default to the same values used by `write_events`.
#[derive(Debug, Clone)]
pub struct WriteEventsOptions {
    pub client_id: u128,
    pub allow_create: bool,
    pub expected_event_batch_index: Option<u64>,
    pub enforce_client_idempotency: bool,
}

impl Default for WriteEventsOptions {
    fn default() -> Self {
        Self {
            client_id: 0,
            allow_create: true,
            expected_event_batch_index: None,
            enforce_client_idempotency: false,
        }
    }
}

impl CeleriantClient {
    pub async fn read(&mut self, request: ReadRequest) -> Result<ReadResponse, ClientError> {
        match self.send_request(&ClientRequest::Read(request)).await? {
            ClientResponse::Read(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn write(&mut self, request: WriteRequest) -> Result<SuccessResponse, ClientError> {
        match self.send_request(&ClientRequest::Write(request)).await? {
            ClientResponse::Write(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn delete(&mut self, request: DeleteRequest) -> Result<SuccessResponse, ClientError> {
        match self.send_request(&ClientRequest::Delete(request)).await? {
            ClientResponse::Delete(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn trim_start(&mut self, request: TrimStartRequest) -> Result<SuccessResponse, ClientError> {
        match self.send_request(&ClientRequest::TrimStart(request)).await? {
            ClientResponse::TrimStart(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn aggregate_details(
        &mut self,
        request: AggregateDetailsRequest,
    ) -> Result<AggregateDetailsResponse, ClientError> {
        match self.send_request(&ClientRequest::AggregateDetails(request)).await? {
            ClientResponse::AggregateDetails(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    /// Convenience method: write events to a single aggregate without constructing a `WriteRequest`.
    pub async fn write_events(
        &mut self,
        aggregate_key: AggregateKey,
        events: Vec<DatablockAggregateEvent>,
    ) -> Result<SuccessResponse, ClientError> {
        self.write_events_with(aggregate_key, events, WriteEventsOptions::default()).await
    }

    /// Like `write_events` but accepts options to control idempotency, optimistic concurrency, etc.
    pub async fn write_events_with(
        &mut self,
        aggregate_key: AggregateKey,
        events: Vec<DatablockAggregateEvent>,
        options: WriteEventsOptions,
    ) -> Result<SuccessResponse, ClientError> {
        let mut writes = HashMap::new();
        writes.insert(aggregate_key, SingleAggregateWrite {
            events,
            allow_create: options.allow_create,
            expected_event_batch_index: options.expected_event_batch_index,
            enforce_client_idempotency: options.enforce_client_idempotency,
        });
        self.write(WriteRequest {
            correlation_id: None,
            client_id: options.client_id,
            user_id: None,
            writes,
        })
        .await
    }

    pub async fn register_schema(
        &mut self,
        request: RegisterSchemaRequest,
    ) -> Result<SuccessResponse, ClientError> {
        match self.send_request(&ClientRequest::RegisterSchema(request)).await? {
            ClientResponse::RegisterSchema(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }
}
