use bincode::{Decode, Encode};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use thiserror::Error;

use eventplanedb_storage_structures::{
    constants::BINCODE_CONFIG_VARIABLE, event_batch_metadata::EventBatchMetadata, event_item::EventItem, read_filters::ReadFilters, read_result::ReadResult
};

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid message format")]
    InvalidFormat,
    #[error("Message too large: {0} bytes")]
    MessageTooLarge(u32),
    #[error("Connection closed unexpectedly")]
    ConnectionClosed,
}

/// Wire protocol requests
#[derive(Debug, Clone, Encode, Decode)]
pub enum Request {
    AppendEvents {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
    },
    ReadFiltered {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        filters: ReadFilters,
    },
    Exists {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },
    TrimStart {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        keep_from_event_batch_index: u64,
    },
    Delete {
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    },
}

/// Wire protocol responses
#[derive(Debug, Clone, Encode, Decode)]
pub enum Response {
    AppendEventsResult(Result<EventBatchMetadata, String>),
    ReadFilteredResult(Result<ReadResult, String>),
    ExistsResult(Result<bool, String>),
    TrimStartResult(Result<(), String>),
    DeleteResult(Result<(), String>),
}

impl Request {
    pub fn aggregate_id(&self) -> &u128 {
        match self {
            Request::AppendEvents { aggregate_id, .. } => aggregate_id,
            Request::ReadFiltered { aggregate_id, .. } => aggregate_id,
            Request::Exists { aggregate_id, ..} => aggregate_id,
            Request::TrimStart { aggregate_id, .. } => aggregate_id,
            Request::Delete { aggregate_id, .. } => aggregate_id,
        }
    }
}

/// Protocol constants
const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024; // 64MB max message size
const PROTOCOL_VERSION: u8 = 1;

/// Message framing: [version: u8][length: u32 LE][payload: bytes]
pub async fn write_message<T, W>(writer: &mut W, message: &T) -> Result<(), ProtocolError>
where
    T: Encode,
    W: AsyncWriteExt + Unpin,
{

    let mut tcp_buffer_2 = [0u8; 200]; // 200 bytes buffer
    let bytes = bincode::encode_into_slice(message, &mut tcp_buffer_2, BINCODE_CONFIG_VARIABLE)
        .map_err(|_| ProtocolError::InvalidFormat)?;
    writer.write_all(&tcp_buffer_2[..bytes]).await?;

    //Serialize to bincode - heap based
    // let encoded = bincode::encode_to_vec(message, BINCODE_CONFIG_VARIABLE)
    //     .map_err(|_| ProtocolError::InvalidFormat)?;

    // if encoded.len() > MAX_MESSAGE_SIZE as usize {
    //     return Err(ProtocolError::MessageTooLarge(encoded.len() as u32));
    // }
    
    // writer.write_all(&encoded).await?;

    Ok(())
}