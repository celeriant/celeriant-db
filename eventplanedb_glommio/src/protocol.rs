use bincode::{Decode, Encode};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use eventplanedb_storage_structures::{
    event_batch_metadata::EventBatchMetadata, event_item::EventItem, read_filters::ReadFilters,
    read_result::ReadResult,
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
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum Request {
    AppendEvents {
        aggregate_id: String,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>,
    },
    ReadFiltered {
        aggregate_id: String,
        filters: ReadFilters,
    },
    Exists {
        aggregate_id: String,
    },
    TrimStart {
        aggregate_id: String,
        keep_from_event_batch_index: u64,
    },
    Delete {
        aggregate_id: String,
    },
}

/// Wire protocol responses
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum Response {
    AppendEventsResult(Result<EventBatchMetadata, String>),
    ReadFilteredResult(Result<ReadResult, String>),
    ExistsResult(Result<bool, String>),
    TrimStartResult(Result<(), String>),
    DeleteResult(Result<(), String>),
}

impl Request {
    pub fn aggregate_id(&self) -> &str {
        match self {
            Request::AppendEvents { aggregate_id, .. } => aggregate_id,
            Request::ReadFiltered { aggregate_id, .. } => aggregate_id,
            Request::Exists { aggregate_id } => aggregate_id,
            Request::TrimStart { aggregate_id, .. } => aggregate_id,
            Request::Delete { aggregate_id } => aggregate_id,
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
    let encoded = bincode::encode_to_vec(message, bincode::config::standard())
        .map_err(|_| ProtocolError::InvalidFormat)?;
    
    if encoded.len() > MAX_MESSAGE_SIZE as usize {
        return Err(ProtocolError::MessageTooLarge(encoded.len() as u32));
    }

    // Write version
    writer.write_all(&[PROTOCOL_VERSION]).await?;
    
    // Write length
    let length = encoded.len() as u32;
    writer.write_all(&length.to_le_bytes()).await?;
    
    // Write payload
    writer.write_all(&encoded).await?;
    
    Ok(())
}

pub async fn read_message<T, R>(reader: &mut R) -> Result<T, ProtocolError>
where
    T: Decode<()>,
    R: AsyncReadExt + Unpin,
{
    // Read version
    let mut version_buf = [0u8; 1];
    reader.read_exact(&mut version_buf).await?;
    
    if version_buf[0] != PROTOCOL_VERSION {
        return Err(ProtocolError::InvalidFormat);
    }

    // Read length
    let mut length_buf = [0u8; 4];
    reader.read_exact(&mut length_buf).await?;
    
    let length = u32::from_le_bytes(length_buf);
    if length > MAX_MESSAGE_SIZE {
        return Err(ProtocolError::MessageTooLarge(length));
    }

    // Read payload
    let mut payload = vec![0u8; length as usize];
    reader.read_exact(&mut payload).await?;

    // Decode message
    let (message, _) = bincode::decode_from_slice(&payload, bincode::config::standard())
        .map_err(|_| ProtocolError::InvalidFormat)?;

    Ok(message)
}