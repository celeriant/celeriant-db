use std::future::Future;

use celeriant_msg::request::requests::{
    AggregateDetailsRequest, DeleteRequest, ReadRequest, RegisterSchemaRequest, TrimStartRequest,
    WatchRequest, WriteRequest,
};
use celeriant_msg::response::responses::{
    AggregateDetailsResponse, DeleteResponse, ReadResponse, RegisterSchemaResponse, TrimStartResponse, WriteResponse,
};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

use crate::client_error::ClientError;
use crate::client_operations::WriteEventsOptions;
use crate::pool::CeleriantPool;
use crate::watch_connection::{WatchConnection, WatchOptions};

pub trait CeleriantPoolApi: Send + Sync {
    fn read(&self, request: ReadRequest) -> impl Future<Output = Result<ReadResponse, ClientError>> + Send;
    fn write(&self, request: WriteRequest) -> impl Future<Output = Result<WriteResponse, ClientError>> + Send;
    fn delete(&self, request: DeleteRequest) -> impl Future<Output = Result<DeleteResponse, ClientError>> + Send;
    fn trim_start(&self, request: TrimStartRequest) -> impl Future<Output = Result<TrimStartResponse, ClientError>> + Send;
    fn aggregate_details(&self, request: AggregateDetailsRequest) -> impl Future<Output = Result<AggregateDetailsResponse, ClientError>> + Send;
    fn register_schema(&self, request: RegisterSchemaRequest) -> impl Future<Output = Result<RegisterSchemaResponse, ClientError>> + Send;
    fn watch(&self, request: WatchRequest, options: WatchOptions) -> impl Future<Output = Result<WatchConnection, ClientError>> + Send;
    fn write_events(&self, aggregate_key: AggregateKey, events: Vec<DatablockAggregateEvent>, client_id: u128) -> impl Future<Output = Result<WriteResponse, ClientError>> + Send;
    fn write_events_with(&self, aggregate_key: AggregateKey, events: Vec<DatablockAggregateEvent>, client_id: u128, options: WriteEventsOptions) -> impl Future<Output = Result<WriteResponse, ClientError>> + Send;
}

impl CeleriantPoolApi for CeleriantPool {
    fn read(&self, request: ReadRequest) -> impl Future<Output = Result<ReadResponse, ClientError>> + Send {
        self.read(request)
    }

    fn write(&self, request: WriteRequest) -> impl Future<Output = Result<WriteResponse, ClientError>> + Send {
        self.write(request)
    }

    fn delete(&self, request: DeleteRequest) -> impl Future<Output = Result<DeleteResponse, ClientError>> + Send {
        self.delete(request)
    }

    fn trim_start(&self, request: TrimStartRequest) -> impl Future<Output = Result<TrimStartResponse, ClientError>> + Send {
        self.trim_start(request)
    }

    fn aggregate_details(&self, request: AggregateDetailsRequest) -> impl Future<Output = Result<AggregateDetailsResponse, ClientError>> + Send {
        self.aggregate_details(request)
    }

    fn register_schema(&self, request: RegisterSchemaRequest) -> impl Future<Output = Result<RegisterSchemaResponse, ClientError>> + Send {
        self.register_schema(request)
    }

    fn watch(&self, request: WatchRequest, options: WatchOptions) -> impl Future<Output = Result<WatchConnection, ClientError>> + Send {
        self.watch(request, options)
    }

    fn write_events(&self, aggregate_key: AggregateKey, events: Vec<DatablockAggregateEvent>, client_id: u128) -> impl Future<Output = Result<WriteResponse, ClientError>> + Send {
        self.write_events(aggregate_key, events, client_id)
    }

    fn write_events_with(&self, aggregate_key: AggregateKey, events: Vec<DatablockAggregateEvent>, client_id: u128, options: WriteEventsOptions) -> impl Future<Output = Result<WriteResponse, ClientError>> + Send {
        self.write_events_with(aggregate_key, events, client_id, options)
    }
}
